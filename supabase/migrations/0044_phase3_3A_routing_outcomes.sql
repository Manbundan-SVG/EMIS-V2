-- Phase 3.3A: Routing outcomes + effectiveness summaries

create table if not exists public.governance_routing_outcomes (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null,
  case_id uuid not null,
  routing_decision_id uuid null,
  assignment_id uuid null,
  assigned_to text null,
  assigned_team text null,
  root_cause_code text null,
  severity text null,
  watchlist_id uuid null,
  compute_version text null,
  signal_registry_version text null,
  model_version text null,
  recurrence_group_id uuid null,
  repeat_count integer not null default 1,
  outcome_type text not null,
  outcome_value numeric null,
  outcome_context jsonb not null default '{}'::jsonb,
  occurred_at timestamptz not null default now()
);

create index if not exists governance_routing_outcomes_case_idx
  on public.governance_routing_outcomes(case_id, occurred_at desc);

create index if not exists governance_routing_outcomes_operator_idx
  on public.governance_routing_outcomes(workspace_id, assigned_to, occurred_at desc);

create index if not exists governance_routing_outcomes_team_idx
  on public.governance_routing_outcomes(workspace_id, assigned_team, occurred_at desc);

create or replace view public.governance_operator_effectiveness_summary as
with base as (
  select
    gro.workspace_id,
    w.slug as workspace_slug,
    gro.assigned_to,
    count(*) filter (where gro.outcome_type = 'assigned') as assignments,
    count(*) filter (where gro.outcome_type = 'acknowledged') as acknowledgments,
    count(*) filter (where gro.outcome_type = 'resolved') as resolutions,
    count(*) filter (where gro.outcome_type = 'reassigned') as reassignments,
    count(*) filter (where gro.outcome_type = 'escalated') as escalations,
    count(*) filter (where gro.outcome_type = 'reopened') as reopens,
    avg(gro.outcome_value) filter (where gro.outcome_type = 'time_to_ack_hours') as avg_ack_hours,
    avg(gro.outcome_value) filter (where gro.outcome_type = 'time_to_resolve_hours') as avg_resolve_hours,
    max(gro.occurred_at) as latest_outcome_at
  from public.governance_routing_outcomes gro
  join public.workspaces w
    on w.id = gro.workspace_id
  where gro.assigned_to is not null
  group by gro.workspace_id, w.slug, gro.assigned_to
)
select
  workspace_id,
  workspace_slug,
  assigned_to,
  assignments,
  acknowledgments,
  resolutions,
  reassignments,
  escalations,
  reopens,
  avg_ack_hours,
  avg_resolve_hours,
  latest_outcome_at,
  case when assignments > 0 then resolutions::numeric / assignments else null end as resolution_rate,
  case when assignments > 0 then reassignments::numeric / assignments else null end as reassignment_rate,
  case when assignments > 0 then escalations::numeric / assignments else null end as escalation_rate
from base;

create or replace view public.governance_team_effectiveness_summary as
with base as (
  select
    gro.workspace_id,
    w.slug as workspace_slug,
    gro.assigned_team,
    count(*) filter (where gro.outcome_type = 'assigned') as assignments,
    count(*) filter (where gro.outcome_type = 'acknowledged') as acknowledgments,
    count(*) filter (where gro.outcome_type = 'resolved') as resolutions,
    count(*) filter (where gro.outcome_type = 'reassigned') as reassignments,
    count(*) filter (where gro.outcome_type = 'escalated') as escalations,
    count(*) filter (where gro.outcome_type = 'reopened') as reopens,
    avg(gro.outcome_value) filter (where gro.outcome_type = 'time_to_ack_hours') as avg_ack_hours,
    avg(gro.outcome_value) filter (where gro.outcome_type = 'time_to_resolve_hours') as avg_resolve_hours,
    max(gro.occurred_at) as latest_outcome_at
  from public.governance_routing_outcomes gro
  join public.workspaces w
    on w.id = gro.workspace_id
  where gro.assigned_team is not null
  group by gro.workspace_id, w.slug, gro.assigned_team
)
select
  workspace_id,
  workspace_slug,
  assigned_team,
  assignments,
  acknowledgments,
  resolutions,
  reassignments,
  escalations,
  reopens,
  avg_ack_hours,
  avg_resolve_hours,
  latest_outcome_at,
  case when assignments > 0 then resolutions::numeric / assignments else null end as resolution_rate,
  case when assignments > 0 then reassignments::numeric / assignments else null end as reassignment_rate,
  case when assignments > 0 then escalations::numeric / assignments else null end as escalation_rate
from base;

create or replace view public.governance_routing_recommendation_inputs as
select
  gro.workspace_id,
  w.slug as workspace_slug,
  coalesce(gro.assigned_to, gro.assigned_team, 'unassigned') as routing_target,
  gro.root_cause_code,
  gro.severity,
  gro.compute_version,
  gro.signal_registry_version,
  gro.model_version,
  avg(gro.outcome_value) filter (where gro.outcome_type = 'time_to_ack_hours') as avg_ack_hours,
  avg(gro.outcome_value) filter (where gro.outcome_type = 'time_to_resolve_hours') as avg_resolve_hours,
  count(*) filter (where gro.outcome_type = 'resolved') as resolved_count,
  count(*) filter (where gro.outcome_type = 'reassigned') as reassigned_count,
  count(*) filter (where gro.outcome_type = 'escalated') as escalated_count,
  count(*) filter (where gro.outcome_type = 'reopened') as reopened_count,
  max(gro.occurred_at) as latest_outcome_at
from public.governance_routing_outcomes gro
join public.workspaces w
  on w.id = gro.workspace_id
group by
  gro.workspace_id,
  w.slug,
  coalesce(gro.assigned_to, gro.assigned_team, 'unassigned'),
  gro.root_cause_code,
  gro.severity,
  gro.compute_version,
  gro.signal_registry_version,
  gro.model_version;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0044',
  '0044_phase3_3A_routing_outcomes',
  current_user,
  jsonb_build_object('phase', '3.3A', 'feature', 'routing_outcomes')
)
on conflict (version) do nothing;
