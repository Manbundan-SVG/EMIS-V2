-- Phase 2.9 - Governance case management
-- Additive operator workflow on top of degradation lifecycle states.

begin;

create table if not exists public.governance_cases (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  degradation_state_id uuid null references public.governance_degradation_states(id) on delete set null,
  watchlist_id uuid null references public.watchlists(id) on delete set null,
  version_tuple text null,
  status text not null default 'open'
    check (status in ('open', 'acknowledged', 'in_progress', 'resolved', 'closed')),
  severity text not null default 'medium',
  title text not null,
  summary text null,
  opened_at timestamptz not null default now(),
  acknowledged_at timestamptz null,
  resolved_at timestamptz null,
  closed_at timestamptz null,
  reopened_count integer not null default 0,
  current_assignee text null,
  current_team text null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists governance_cases_workspace_status_idx
  on public.governance_cases (workspace_id, status, opened_at desc);

create unique index if not exists governance_cases_active_state_idx
  on public.governance_cases (degradation_state_id)
  where degradation_state_id is not null
    and status in ('open', 'acknowledged', 'in_progress');

create table if not exists public.governance_case_events (
  id uuid primary key default gen_random_uuid(),
  case_id uuid not null references public.governance_cases(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  event_type text not null,
  actor text null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists governance_case_events_case_idx
  on public.governance_case_events (case_id, created_at desc);

create table if not exists public.governance_case_notes (
  id uuid primary key default gen_random_uuid(),
  case_id uuid not null references public.governance_cases(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  author text null,
  note text not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.governance_case_evidence (
  id uuid primary key default gen_random_uuid(),
  case_id uuid not null references public.governance_cases(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  evidence_type text not null,
  reference_id text not null,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists governance_case_evidence_case_idx
  on public.governance_case_evidence (case_id, created_at desc);

create table if not exists public.governance_assignments (
  id uuid primary key default gen_random_uuid(),
  case_id uuid not null references public.governance_cases(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  assigned_to text null,
  assigned_team text null,
  assigned_by text null,
  reason text null,
  active boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  assigned_at timestamptz not null default now()
);

create index if not exists governance_assignments_case_idx
  on public.governance_assignments (case_id, assigned_at desc);

create unique index if not exists governance_assignments_active_case_idx
  on public.governance_assignments (case_id)
  where active = true;

create table if not exists public.governance_assignment_history (
  id uuid primary key default gen_random_uuid(),
  case_id uuid not null references public.governance_cases(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  previous_assignee text null,
  previous_team text null,
  new_assignee text null,
  new_team text null,
  changed_by text null,
  reason text null,
  metadata jsonb not null default '{}'::jsonb,
  changed_at timestamptz not null default now()
);

create index if not exists governance_assignment_history_case_idx
  on public.governance_assignment_history (case_id, changed_at desc);

create or replace view public.governance_case_summary as
select
  c.id,
  c.workspace_id,
  w.slug as workspace_slug,
  c.degradation_state_id,
  c.watchlist_id,
  wl.slug as watchlist_slug,
  wl.name as watchlist_name,
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
  last_event.event_type as last_event_type,
  last_event.created_at as last_event_at
from public.governance_cases c
join public.workspaces w
  on w.id = c.workspace_id
left join public.watchlists wl
  on wl.id = c.watchlist_id
left join (
  select case_id, count(*)::integer as note_count
  from public.governance_case_notes
  group by case_id
) n on n.case_id = c.id
left join (
  select case_id, count(*)::integer as evidence_count
  from public.governance_case_evidence
  group by case_id
) e on e.case_id = c.id
left join (
  select case_id, count(*)::integer as event_count
  from public.governance_case_events
  group by case_id
) ev on ev.case_id = c.id
left join lateral (
  select gce.event_type, gce.created_at
  from public.governance_case_events gce
  where gce.case_id = c.id
  order by gce.created_at desc
  limit 1
) last_event on true;

comment on view public.governance_case_summary is
  'Workspace-aware operator case summary with counts and latest case activity.';

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
select
  '0031_phase2_9_case_management',
  'phase2_9_case_management',
  'codex',
  jsonb_build_object(
    'tables', jsonb_build_array(
      'governance_cases',
      'governance_case_events',
      'governance_case_notes',
      'governance_case_evidence',
      'governance_assignments',
      'governance_assignment_history'
    ),
    'views', jsonb_build_array(
      'governance_case_summary'
    ),
    'features', jsonb_build_array(
      'case_tracking',
      'assignment_history',
      'case_evidence'
    )
  )
where not exists (
  select 1
  from public.schema_migration_ledger
  where version = '0031_phase2_9_case_management'
);

commit;
