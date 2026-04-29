begin;

alter table if exists public.governance_cases
  add column if not exists recurrence_group_id uuid,
  add column if not exists reopened_from_case_id uuid null references public.governance_cases(id),
  add column if not exists repeat_count integer not null default 1,
  add column if not exists reopened_at timestamptz null,
  add column if not exists reopen_reason text null,
  add column if not exists recurrence_match_basis jsonb not null default '{}'::jsonb;

create table if not exists public.governance_case_recurrence (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid null references public.watchlists(id) on delete cascade,
  recurrence_group_id uuid not null,
  source_case_id uuid not null references public.governance_cases(id) on delete cascade,
  matched_case_id uuid not null references public.governance_cases(id) on delete cascade,
  match_type text not null,
  match_score numeric not null default 1.0,
  matched_within_window boolean not null default true,
  match_basis jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_governance_case_recurrence_group
  on public.governance_case_recurrence (recurrence_group_id, created_at desc);

create index if not exists idx_governance_case_recurrence_source_case
  on public.governance_case_recurrence (source_case_id, created_at desc);

create index if not exists idx_governance_case_recurrence_matched_case
  on public.governance_case_recurrence (matched_case_id, created_at desc);

create or replace view public.governance_case_recurrence_summary as
with prior_rollup as (
  select
    c.id as case_id,
    c.workspace_id,
    c.recurrence_group_id,
    c.reopened_from_case_id,
    c.repeat_count,
    count(distinct r.matched_case_id)::int as prior_related_case_count,
    (
      array_remove(
        array_agg(m.id order by coalesce(m.closed_at, m.resolved_at, m.updated_at) desc nulls last),
        null
      )
    )[1] as latest_prior_case_id,
    max(coalesce(m.closed_at, m.resolved_at, m.updated_at)) as latest_prior_closed_at,
    (
      array_remove(
        array_agg(m.status order by coalesce(m.closed_at, m.resolved_at, m.updated_at) desc nulls last),
        null
      )
    )[1] as latest_prior_status
  from public.governance_cases c
  left join public.governance_case_recurrence r
    on r.source_case_id = c.id
  left join public.governance_cases m
    on m.id = r.matched_case_id
  group by c.id, c.workspace_id, c.recurrence_group_id, c.reopened_from_case_id, c.repeat_count
)
select
  case_id,
  workspace_id,
  recurrence_group_id,
  reopened_from_case_id,
  repeat_count,
  prior_related_case_count,
  latest_prior_case_id,
  latest_prior_closed_at,
  latest_prior_status,
  (reopened_from_case_id is not null) as is_reopened,
  (coalesce(prior_related_case_count, 0) > 0 or repeat_count > 1) as is_recurring
from prior_rollup;

create or replace view public.governance_case_summary as
with note_rollup as (
  select case_id, count(*)::int as note_count
  from public.governance_case_notes
  group by case_id
),
evidence_rollup as (
  select case_id, count(*)::int as evidence_count
  from public.governance_case_evidence
  group by case_id
),
event_rollup as (
  select
    case_id,
    count(*)::int as event_count,
    (
      array_remove(array_agg(event_type order by created_at desc, id desc), null)
    )[1] as last_event_type,
    max(created_at) as last_event_at
  from public.governance_case_events
  group by case_id
)
select
  c.id,
  c.workspace_id,
  ws.slug as workspace_slug,
  c.degradation_state_id,
  c.watchlist_id,
  w.slug as watchlist_slug,
  w.name as watchlist_name,
  c.version_tuple,
  c.status,
  c.severity,
  c.title,
  c.summary,
  c.opened_at,
  c.acknowledged_at,
  c.resolved_at,
  c.closed_at,
  c.reopened_count,
  c.current_assignee,
  c.current_team,
  c.metadata,
  coalesce(n.note_count, 0) as note_count,
  coalesce(e.evidence_count, 0) as evidence_count,
  coalesce(ev.event_count, 0) as event_count,
  ev.last_event_type,
  ev.last_event_at,
  c.recurrence_group_id,
  c.reopened_from_case_id,
  c.repeat_count,
  c.reopened_at,
  c.reopen_reason,
  c.recurrence_match_basis,
  coalesce(rs.prior_related_case_count, 0) as prior_related_case_count,
  rs.latest_prior_case_id,
  rs.latest_prior_closed_at,
  rs.latest_prior_status,
  coalesce(rs.is_reopened, false) as is_reopened,
  coalesce(rs.is_recurring, c.repeat_count > 1) as is_recurring
from public.governance_cases c
join public.workspaces ws on ws.id = c.workspace_id
left join public.watchlists w on w.id = c.watchlist_id
left join note_rollup n on n.case_id = c.id
left join evidence_rollup e on e.case_id = c.id
left join event_rollup ev on ev.case_id = c.id
left join public.governance_case_recurrence_summary rs on rs.case_id = c.id;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
    '0033',
    '0033_phase3_0B_case_recurrence',
    current_user,
    jsonb_build_object(
        'phase', '3.0B',
        'description', 'Recurring and reopened governance case intelligence'
    )
)
on conflict (version) do update
set
    name = excluded.name,
    applied_at = now(),
    applied_by = excluded.applied_by,
    metadata = excluded.metadata;

commit;
