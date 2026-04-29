begin;

create table if not exists public.governance_performance_snapshots (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  snapshot_at timestamptz not null default now(),
  operator_count integer not null default 0,
  team_count integer not null default 0,
  operator_case_mix_count integer not null default 0,
  team_case_mix_count integer not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists governance_performance_snapshots_workspace_idx
  on public.governance_performance_snapshots (workspace_id, snapshot_at desc);

create or replace view public.governance_operator_performance_summary as
with case_facts as (
  select
    c.id as case_id,
    c.workspace_id,
    w.slug as workspace_slug,
    c.current_assignee as operator_name,
    c.current_team as assigned_team,
    coalesce(gsl.root_cause_code, 'unknown') as root_cause_code,
    c.severity,
    coalesce(gds.regime, 'unknown') as regime,
    c.status,
    c.opened_at,
    c.acknowledged_at,
    c.resolved_at,
    c.closed_at,
    c.reopened_from_case_id,
    coalesce(c.repeat_count, 1) as repeat_count,
    case when coalesce(c.repeat_count, 1) >= 3 then true else false end as chronic_case,
    coalesce(assignment_rollup.assignment_count, 0) as assignment_count,
    case when escalation_state.case_id is not null then true else false end as escalated
  from public.governance_cases c
  join public.workspaces w
    on w.id = c.workspace_id
  left join public.governance_case_summary_latest gsl
    on gsl.case_id = c.id
  left join public.governance_degradation_states gds
    on gds.id = c.degradation_state_id
  left join lateral (
    select count(*)::int as assignment_count
    from public.governance_assignments ga
    where ga.case_id = c.id
  ) assignment_rollup on true
  left join lateral (
    select es.case_id
    from public.governance_escalation_state es
    where es.case_id = c.id
      and coalesce(es.status, 'active') <> 'cleared'
    limit 1
  ) escalation_state on true
)
select
  workspace_id,
  workspace_slug,
  operator_name,
  count(*)::int as assigned_case_count,
  count(*) filter (where status in ('open', 'acknowledged', 'in_progress'))::int as active_open_case_count,
  avg(extract(epoch from (acknowledged_at - opened_at))) filter (
    where acknowledged_at is not null and opened_at is not null
  ) as avg_ack_seconds,
  percentile_cont(0.5) within group (
    order by extract(epoch from (acknowledged_at - opened_at))
  ) filter (
    where acknowledged_at is not null and opened_at is not null
  ) as median_ack_seconds,
  avg(extract(epoch from (coalesce(closed_at, resolved_at) - opened_at))) filter (
    where coalesce(closed_at, resolved_at) is not null and opened_at is not null
  ) as avg_resolve_seconds,
  percentile_cont(0.5) within group (
    order by extract(epoch from (coalesce(closed_at, resolved_at) - opened_at))
  ) filter (
    where coalesce(closed_at, resolved_at) is not null and opened_at is not null
  ) as median_resolve_seconds,
  count(*) filter (where reopened_from_case_id is not null)::int as reopened_case_count,
  count(*) filter (where escalated)::int as escalated_case_count,
  count(*) filter (where chronic_case)::int as chronic_case_count,
  count(*) filter (where status in ('resolved', 'closed'))::int as resolved_case_count,
  count(*) filter (where assignment_count > 1)::int as reassigned_case_count,
  count(*) filter (where severity in ('high', 'critical'))::int as severe_case_count,
  case when count(*) > 0
    then (count(*) filter (where reopened_from_case_id is not null))::numeric / count(*)::numeric
    else 0
  end as reopen_rate,
  case when count(*) > 0
    then (count(*) filter (where escalated))::numeric / count(*)::numeric
    else 0
  end as escalation_rate,
  case when count(*) > 0
    then (count(*) filter (where assignment_count > 1))::numeric / count(*)::numeric
    else 0
  end as reassignment_rate,
  case when count(*) > 0
    then (count(*) filter (where status in ('resolved', 'closed')))::numeric / count(*)::numeric
    else 0
  end as resolution_rate,
  case when count(*) > 0 then greatest(
    least(
      (
        (count(*) filter (where status in ('resolved', 'closed')))::numeric / count(*)::numeric
      ) * (
        1 - (
          (count(*) filter (where reopened_from_case_id is not null))::numeric / count(*)::numeric
        )
      ),
      1
    ),
    0
  ) else 0 end as resolution_quality_proxy
from case_facts
where operator_name is not null
group by workspace_id, workspace_slug, operator_name;

create or replace view public.governance_team_performance_summary as
with case_facts as (
  select
    c.id as case_id,
    c.workspace_id,
    w.slug as workspace_slug,
    c.current_team as assigned_team,
    c.severity,
    c.status,
    c.opened_at,
    c.acknowledged_at,
    c.resolved_at,
    c.closed_at,
    c.reopened_from_case_id,
    coalesce(c.repeat_count, 1) as repeat_count,
    case when coalesce(c.repeat_count, 1) >= 3 then true else false end as chronic_case,
    coalesce(assignment_rollup.assignment_count, 0) as assignment_count,
    case when escalation_state.case_id is not null then true else false end as escalated
  from public.governance_cases c
  join public.workspaces w
    on w.id = c.workspace_id
  left join lateral (
    select count(*)::int as assignment_count
    from public.governance_assignments ga
    where ga.case_id = c.id
  ) assignment_rollup on true
  left join lateral (
    select es.case_id
    from public.governance_escalation_state es
    where es.case_id = c.id
      and coalesce(es.status, 'active') <> 'cleared'
    limit 1
  ) escalation_state on true
)
select
  workspace_id,
  workspace_slug,
  assigned_team,
  count(*)::int as assigned_case_count,
  count(*) filter (where status in ('open', 'acknowledged', 'in_progress'))::int as active_open_case_count,
  avg(extract(epoch from (acknowledged_at - opened_at))) filter (
    where acknowledged_at is not null and opened_at is not null
  ) as avg_ack_seconds,
  percentile_cont(0.5) within group (
    order by extract(epoch from (acknowledged_at - opened_at))
  ) filter (
    where acknowledged_at is not null and opened_at is not null
  ) as median_ack_seconds,
  avg(extract(epoch from (coalesce(closed_at, resolved_at) - opened_at))) filter (
    where coalesce(closed_at, resolved_at) is not null and opened_at is not null
  ) as avg_resolve_seconds,
  percentile_cont(0.5) within group (
    order by extract(epoch from (coalesce(closed_at, resolved_at) - opened_at))
  ) filter (
    where coalesce(closed_at, resolved_at) is not null and opened_at is not null
  ) as median_resolve_seconds,
  count(*) filter (where reopened_from_case_id is not null)::int as reopened_case_count,
  count(*) filter (where escalated)::int as escalated_case_count,
  count(*) filter (where chronic_case)::int as chronic_case_count,
  count(*) filter (where status in ('resolved', 'closed'))::int as resolved_case_count,
  count(*) filter (where assignment_count > 1)::int as reassigned_case_count,
  count(*) filter (where severity in ('high', 'critical'))::int as severe_case_count,
  case when count(*) > 0
    then (count(*) filter (where reopened_from_case_id is not null))::numeric / count(*)::numeric
    else 0
  end as reopen_rate,
  case when count(*) > 0
    then (count(*) filter (where escalated))::numeric / count(*)::numeric
    else 0
  end as escalation_rate,
  case when count(*) > 0
    then (count(*) filter (where assignment_count > 1))::numeric / count(*)::numeric
    else 0
  end as reassignment_rate,
  case when count(*) > 0
    then (count(*) filter (where status in ('resolved', 'closed')))::numeric / count(*)::numeric
    else 0
  end as resolution_rate,
  case when count(*) > 0 then greatest(
    least(
      (
        (count(*) filter (where status in ('resolved', 'closed')))::numeric / count(*)::numeric
      ) * (
        1 - (
          (count(*) filter (where reopened_from_case_id is not null))::numeric / count(*)::numeric
        )
      ),
      1
    ),
    0
  ) else 0 end as resolution_quality_proxy
from case_facts
where assigned_team is not null
group by workspace_id, workspace_slug, assigned_team;

create or replace view public.governance_operator_case_mix_summary as
with case_facts as (
  select
    c.workspace_id,
    w.slug as workspace_slug,
    c.current_assignee as actor_name,
    coalesce(gsl.root_cause_code, 'unknown') as root_cause_code,
    c.severity,
    coalesce(gds.regime, 'unknown') as regime,
    c.reopened_from_case_id,
    coalesce(c.repeat_count, 1) as repeat_count
  from public.governance_cases c
  join public.workspaces w
    on w.id = c.workspace_id
  left join public.governance_case_summary_latest gsl
    on gsl.case_id = c.id
  left join public.governance_degradation_states gds
    on gds.id = c.degradation_state_id
)
select
  workspace_id,
  workspace_slug,
  actor_name,
  root_cause_code,
  severity,
  regime,
  count(*)::int as case_count,
  count(*) filter (where repeat_count > 1)::int as recurring_case_count,
  count(*) filter (where repeat_count >= 3)::int as chronic_case_count,
  count(*) filter (where severity in ('high', 'critical'))::int as severe_case_count
from case_facts
where actor_name is not null
group by workspace_id, workspace_slug, actor_name, root_cause_code, severity, regime;

create or replace view public.governance_team_case_mix_summary as
with case_facts as (
  select
    c.workspace_id,
    w.slug as workspace_slug,
    c.current_team as actor_name,
    coalesce(gsl.root_cause_code, 'unknown') as root_cause_code,
    c.severity,
    coalesce(gds.regime, 'unknown') as regime,
    c.reopened_from_case_id,
    coalesce(c.repeat_count, 1) as repeat_count
  from public.governance_cases c
  join public.workspaces w
    on w.id = c.workspace_id
  left join public.governance_case_summary_latest gsl
    on gsl.case_id = c.id
  left join public.governance_degradation_states gds
    on gds.id = c.degradation_state_id
)
select
  workspace_id,
  workspace_slug,
  actor_name,
  root_cause_code,
  severity,
  regime,
  count(*)::int as case_count,
  count(*) filter (where repeat_count > 1)::int as recurring_case_count,
  count(*) filter (where repeat_count >= 3)::int as chronic_case_count,
  count(*) filter (where severity in ('high', 'critical'))::int as severe_case_count
from case_facts
where actor_name is not null
group by workspace_id, workspace_slug, actor_name, root_cause_code, severity, regime;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0049',
  '0049_phase3_4B_operator_team_performance',
  current_user,
  jsonb_build_object(
    'phase', '3.4B',
    'description', 'Operator and team performance intelligence with case mix summaries'
  )
)
on conflict (version) do update
set
  name = excluded.name,
  applied_at = now(),
  applied_by = excluded.applied_by,
  metadata = excluded.metadata;

commit;
