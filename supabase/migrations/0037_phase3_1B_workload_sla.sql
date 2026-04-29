create extension if not exists pgcrypto;

create table if not exists public.governance_sla_policies (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  severity text not null,
  chronicity_class text null,
  ack_within_minutes integer not null check (ack_within_minutes > 0),
  resolve_within_minutes integer not null check (resolve_within_minutes > 0),
  enabled boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (workspace_id, severity, chronicity_class)
);

create table if not exists public.governance_sla_evaluations (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  case_id uuid not null unique references public.governance_cases(id) on delete cascade,
  policy_id uuid null references public.governance_sla_policies(id) on delete set null,
  chronicity_class text null,
  ack_due_at timestamptz null,
  resolve_due_at timestamptz null,
  ack_breached boolean not null default false,
  resolve_breached boolean not null default false,
  breach_severity text null,
  metadata jsonb not null default '{}'::jsonb,
  evaluated_at timestamptz not null default now()
);

insert into public.governance_sla_policies (
  workspace_id,
  severity,
  chronicity_class,
  ack_within_minutes,
  resolve_within_minutes,
  enabled,
  metadata
)
select
  w.id,
  seeded.severity,
  seeded.chronicity_class,
  seeded.ack_within_minutes,
  seeded.resolve_within_minutes,
  true,
  jsonb_build_object('seeded_by', '0037_phase3_1B_workload_sla')
from public.workspaces w
cross join (
  values
    ('critical', null::text, 15, 240),
    ('high', null::text, 30, 480),
    ('medium', null::text, 60, 1440),
    ('low', null::text, 240, 4320)
) as seeded(severity, chronicity_class, ack_within_minutes, resolve_within_minutes)
on conflict (workspace_id, severity, chronicity_class) do nothing;

create or replace view public.governance_case_aging_summary as
select
  c.id as case_id,
  c.workspace_id,
  w.slug as workspace_slug,
  c.watchlist_id,
  wl.slug as watchlist_slug,
  c.title,
  c.status,
  c.severity,
  c.current_assignee,
  c.current_team,
  c.recurrence_group_id,
  c.repeat_count,
  c.opened_at,
  c.acknowledged_at,
  c.resolved_at,
  c.closed_at,
  extract(epoch from (now() - c.opened_at)) / 60.0 as age_minutes
from public.governance_cases c
join public.workspaces w
  on w.id = c.workspace_id
left join public.watchlists wl
  on wl.id = c.watchlist_id;

create or replace view public.governance_case_sla_summary as
select
  c.id as case_id,
  c.workspace_id,
  w.slug as workspace_slug,
  c.watchlist_id,
  wl.slug as watchlist_slug,
  c.title,
  c.status,
  c.severity,
  c.current_assignee,
  c.current_team,
  c.repeat_count,
  c.opened_at,
  c.acknowledged_at,
  c.resolved_at,
  c.closed_at,
  e.policy_id,
  e.chronicity_class,
  e.ack_due_at,
  e.resolve_due_at,
  coalesce(e.ack_breached, false) as ack_breached,
  coalesce(e.resolve_breached, false) as resolve_breached,
  e.breach_severity,
  coalesce(e.metadata, '{}'::jsonb) as metadata,
  e.evaluated_at
from public.governance_cases c
join public.workspaces w
  on w.id = c.workspace_id
left join public.watchlists wl
  on wl.id = c.watchlist_id
left join public.governance_sla_evaluations e
  on e.case_id = c.id;

create or replace view public.governance_operator_workload_pressure as
select
  c.workspace_id,
  w.slug as workspace_slug,
  c.current_assignee as assigned_to,
  c.current_team as assigned_team,
  count(*) filter (where c.status in ('open', 'acknowledged', 'in_progress')) as open_case_count,
  count(*) filter (where c.status in ('open', 'acknowledged', 'in_progress') and c.repeat_count > 1) as recurring_case_count,
  count(*) filter (where c.status in ('open', 'acknowledged', 'in_progress') and c.severity in ('high', 'critical')) as severe_open_case_count,
  count(*) filter (where c.status in ('open', 'acknowledged', 'in_progress') and coalesce(s.ack_breached, false)) as ack_breached_case_count,
  count(*) filter (where c.status in ('open', 'acknowledged', 'in_progress') and coalesce(s.resolve_breached, false)) as resolve_breached_case_count,
  avg(extract(epoch from (now() - c.opened_at)) / 60.0) filter (where c.status in ('open', 'acknowledged', 'in_progress')) as avg_open_age_minutes,
  sum(
    case
      when c.status in ('open', 'acknowledged', 'in_progress') and c.severity = 'critical' then 5
      when c.status in ('open', 'acknowledged', 'in_progress') and c.severity = 'high' then 3
      when c.status in ('open', 'acknowledged', 'in_progress') and c.severity = 'medium' then 2
      when c.status in ('open', 'acknowledged', 'in_progress') then 1
      else 0
    end
  ) as severity_weighted_load
from public.governance_cases c
join public.workspaces w
  on w.id = c.workspace_id
left join public.governance_sla_evaluations s
  on s.case_id = c.id
where c.current_assignee is not null
group by 1, 2, 3, 4;

create or replace view public.governance_team_workload_pressure as
select
  c.workspace_id,
  w.slug as workspace_slug,
  c.current_team as assigned_team,
  count(*) filter (where c.status in ('open', 'acknowledged', 'in_progress')) as open_case_count,
  count(*) filter (where c.status in ('open', 'acknowledged', 'in_progress') and c.repeat_count > 1) as recurring_case_count,
  count(*) filter (where c.status in ('open', 'acknowledged', 'in_progress') and c.severity in ('high', 'critical')) as severe_open_case_count,
  count(*) filter (where c.status in ('open', 'acknowledged', 'in_progress') and coalesce(s.ack_breached, false)) as ack_breached_case_count,
  count(*) filter (where c.status in ('open', 'acknowledged', 'in_progress') and coalesce(s.resolve_breached, false)) as resolve_breached_case_count,
  avg(extract(epoch from (now() - c.opened_at)) / 60.0) filter (where c.status in ('open', 'acknowledged', 'in_progress')) as avg_open_age_minutes,
  sum(
    case
      when c.status in ('open', 'acknowledged', 'in_progress') and c.severity = 'critical' then 5
      when c.status in ('open', 'acknowledged', 'in_progress') and c.severity = 'high' then 3
      when c.status in ('open', 'acknowledged', 'in_progress') and c.severity = 'medium' then 2
      when c.status in ('open', 'acknowledged', 'in_progress') then 1
      else 0
    end
  ) as severity_weighted_load
from public.governance_cases c
join public.workspaces w
  on w.id = c.workspace_id
left join public.governance_sla_evaluations s
  on s.case_id = c.id
where c.current_team is not null
group by 1, 2, 3;

create or replace view public.governance_stale_case_summary as
select
  c.id as case_id,
  c.workspace_id,
  w.slug as workspace_slug,
  c.watchlist_id,
  wl.slug as watchlist_slug,
  c.title,
  c.status,
  c.severity,
  c.current_assignee,
  c.current_team,
  c.repeat_count,
  extract(epoch from (now() - c.opened_at)) / 60.0 as age_minutes,
  s.ack_due_at,
  s.resolve_due_at,
  coalesce(s.ack_breached, false) as ack_breached,
  coalesce(s.resolve_breached, false) as resolve_breached,
  s.breach_severity,
  s.evaluated_at
from public.governance_cases c
join public.workspaces w
  on w.id = c.workspace_id
left join public.watchlists wl
  on wl.id = c.watchlist_id
left join public.governance_sla_evaluations s
  on s.case_id = c.id
where c.status in ('open', 'acknowledged', 'in_progress')
  and (
    extract(epoch from (now() - c.opened_at)) / 60.0 > 240
    or coalesce(s.ack_breached, false)
    or coalesce(s.resolve_breached, false)
  );

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0037',
  '0037_phase3_1B_workload_sla',
  current_user,
  jsonb_build_object('phase', '3.1B', 'feature', 'workload_sla')
)
on conflict (version) do nothing;
