create table if not exists public.governance_routing_feedback (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  case_id uuid not null references public.governance_cases(id) on delete cascade,
  routing_decision_id uuid null references public.governance_routing_decisions(id) on delete set null,
  feedback_type text not null check (
    feedback_type in (
      'accepted',
      'manual_reassign',
      'escalation_reroute',
      'workload_rebalance',
      'resolved_without_change'
    )
  ),
  feedback_status text not null default 'active' check (feedback_status in ('active', 'superseded')),
  assigned_to text null,
  assigned_team text null,
  prior_assigned_to text null,
  prior_assigned_team text null,
  root_cause_code text null,
  severity text null,
  recurrence_group_id uuid null,
  repeat_count integer not null default 1,
  reason text null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_governance_routing_feedback_workspace_created
  on public.governance_routing_feedback (workspace_id, created_at desc);

create index if not exists idx_governance_routing_feedback_case_created
  on public.governance_routing_feedback (case_id, created_at desc);

create table if not exists public.governance_reassignment_events (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  case_id uuid not null references public.governance_cases(id) on delete cascade,
  routing_decision_id uuid null references public.governance_routing_decisions(id) on delete set null,
  previous_assigned_to text null,
  previous_assigned_team text null,
  new_assigned_to text null,
  new_assigned_team text null,
  reassignment_type text not null check (
    reassignment_type in (
      'manual_override',
      'workload_balance',
      'escalation',
      'policy_change',
      'other'
    )
  ),
  reassignment_reason text null,
  minutes_since_open integer null,
  minutes_since_last_assignment integer null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_governance_reassignment_events_workspace_created
  on public.governance_reassignment_events (workspace_id, created_at desc);

create index if not exists idx_governance_reassignment_events_case_created
  on public.governance_reassignment_events (case_id, created_at desc);

create or replace view public.governance_routing_quality_summary as
select
  rf.workspace_id,
  w.slug as workspace_slug,
  coalesce(rf.root_cause_code, 'unknown') as root_cause_code,
  coalesce(rf.assigned_team, 'unassigned') as assigned_team,
  count(*) as feedback_count,
  count(*) filter (where rf.feedback_type = 'accepted') as accepted_count,
  count(*) filter (where rf.feedback_type in ('manual_reassign', 'workload_rebalance', 'escalation_reroute')) as rerouted_count,
  round(
    case
      when count(*) = 0 then 0
      else (count(*) filter (where rf.feedback_type = 'accepted'))::numeric / count(*)::numeric
    end,
    6
  ) as acceptance_rate,
  max(rf.created_at) as latest_feedback_at
from public.governance_routing_feedback rf
join public.workspaces w
  on w.id = rf.workspace_id
where rf.feedback_status = 'active'
group by rf.workspace_id, w.slug, coalesce(rf.root_cause_code, 'unknown'), coalesce(rf.assigned_team, 'unassigned');

create or replace view public.governance_reassignment_pressure_summary as
select
  re.workspace_id,
  w.slug as workspace_slug,
  coalesce(re.new_assigned_team, 'unassigned') as assigned_team,
  count(*) as reassignment_count,
  count(*) filter (where re.reassignment_type = 'manual_override') as manual_override_count,
  count(*) filter (where re.reassignment_type = 'escalation') as escalation_reassign_count,
  count(*) filter (where re.reassignment_type = 'workload_balance') as workload_rebalance_count,
  round(avg(re.minutes_since_open)::numeric, 2) as avg_minutes_since_open,
  round(avg(re.minutes_since_last_assignment)::numeric, 2) as avg_minutes_since_last_assignment,
  max(re.created_at) as latest_reassignment_at
from public.governance_reassignment_events re
join public.workspaces w
  on w.id = re.workspace_id
group by re.workspace_id, w.slug, coalesce(re.new_assigned_team, 'unassigned');

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0040',
  '0040_phase3_1D_routing_feedback',
  current_user,
  jsonb_build_object('phase', '3.1D', 'feature', 'routing_feedback')
)
on conflict (version) do nothing;
