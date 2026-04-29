create extension if not exists pgcrypto;

create table if not exists public.governance_routing_rules (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  is_enabled boolean not null default true,
  priority integer not null default 100,
  root_cause_code text null,
  severity text null,
  watchlist_id uuid null references public.watchlists(id) on delete cascade,
  version_tuple text null,
  recurrence_min integer null,
  chronic_only boolean not null default false,
  assign_team text null,
  assign_user text null,
  fallback_team text null,
  routing_reason_template text null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_governance_routing_rules_workspace_priority
  on public.governance_routing_rules (workspace_id, is_enabled, priority asc, created_at desc);

create table if not exists public.governance_routing_overrides (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  case_id uuid null references public.governance_cases(id) on delete cascade,
  watchlist_id uuid null references public.watchlists(id) on delete cascade,
  root_cause_code text null,
  severity text null,
  version_tuple text null,
  assigned_team text null,
  assigned_user text null,
  reason text null,
  is_enabled boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_governance_routing_overrides_workspace_case
  on public.governance_routing_overrides (workspace_id, case_id, is_enabled, created_at desc);

create table if not exists public.governance_routing_decisions (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  case_id uuid not null references public.governance_cases(id) on delete cascade,
  routing_rule_id uuid null references public.governance_routing_rules(id) on delete set null,
  override_id uuid null references public.governance_routing_overrides(id) on delete set null,
  assigned_team text null,
  assigned_user text null,
  routing_reason text not null,
  workload_snapshot jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_governance_routing_decisions_workspace_created
  on public.governance_routing_decisions (workspace_id, created_at desc);

create or replace view public.governance_assignment_workload_summary as
with latest_assignments as (
  select distinct on (ga.case_id)
    ga.case_id,
    ga.workspace_id,
    ga.assigned_to,
    ga.assigned_team,
    ga.assigned_at
  from public.governance_assignments ga
  where ga.active = true
  order by ga.case_id, ga.assigned_at desc, ga.id desc
)
select
  c.workspace_id,
  coalesce(a.assigned_team, 'unassigned') as assigned_team,
  coalesce(a.assigned_to, 'unassigned') as assigned_to,
  count(*) filter (
    where c.status in ('open', 'acknowledged', 'in_progress')
  ) as open_case_count,
  count(*) filter (
    where c.status in ('open', 'acknowledged', 'in_progress')
      and c.severity in ('high', 'critical')
  ) as severe_open_case_count,
  avg(extract(epoch from (now() - c.opened_at)) / 3600.0) filter (
    where c.status in ('open', 'acknowledged', 'in_progress')
  ) as avg_open_age_hours,
  count(*) filter (
    where c.status in ('open', 'acknowledged', 'in_progress')
      and coalesce(c.repeat_count, 1) > 1
  ) as reopened_open_case_count,
  count(*) filter (
    where c.status in ('open', 'acknowledged', 'in_progress')
      and extract(epoch from (now() - c.opened_at)) / 3600.0 >= 24.0
  ) as stale_open_case_count
from public.governance_cases c
left join latest_assignments a
  on a.case_id = c.id
group by 1, 2, 3;

create or replace view public.governance_operator_case_metrics as
select
  workspace_id,
  assigned_to as operator_id,
  assigned_team,
  sum(open_case_count) as open_case_count,
  sum(severe_open_case_count) as severe_open_case_count,
  avg(avg_open_age_hours) as avg_open_age_hours,
  sum(reopened_open_case_count) as reopened_open_case_count,
  sum(stale_open_case_count) as stale_open_case_count
from public.governance_assignment_workload_summary
where assigned_to <> 'unassigned'
group by 1, 2, 3;

create or replace view public.governance_team_case_metrics as
select
  workspace_id,
  assigned_team,
  sum(open_case_count) as open_case_count,
  sum(severe_open_case_count) as severe_open_case_count,
  avg(avg_open_age_hours) as avg_open_age_hours,
  sum(reopened_open_case_count) as reopened_open_case_count,
  sum(stale_open_case_count) as stale_open_case_count
from public.governance_assignment_workload_summary
where assigned_team <> 'unassigned'
group by 1, 2;

create or replace view public.governance_routing_summary as
select
  d.id,
  d.workspace_id,
  w.slug as workspace_slug,
  d.case_id,
  c.watchlist_id,
  wl.slug as watchlist_slug,
  c.title as case_title,
  c.status as case_status,
  c.severity,
  c.version_tuple,
  gsl.root_cause_code,
  d.routing_rule_id,
  d.override_id,
  d.assigned_team,
  d.assigned_user,
  d.routing_reason,
  d.workload_snapshot,
  d.metadata,
  d.created_at
from public.governance_routing_decisions d
join public.governance_cases c
  on c.id = d.case_id
join public.workspaces w
  on w.id = d.workspace_id
left join public.watchlists wl
  on wl.id = c.watchlist_id
left join public.governance_case_summary_latest gsl
  on gsl.case_id = d.case_id;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0036',
  '0036_phase3_1A_assignment_routing',
  current_user,
  jsonb_build_object('phase', '3.1A', 'feature', 'assignment_routing')
)
on conflict (version) do nothing;
