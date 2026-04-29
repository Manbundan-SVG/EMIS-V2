begin;

create table if not exists public.governance_manager_analytics_snapshots (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  snapshot_at timestamptz not null default now(),
  window_days integer not null default 30 check (window_days > 0),
  open_case_count integer not null default 0,
  recurring_case_count integer not null default 0,
  escalated_case_count integer not null default 0,
  chronic_watchlist_count integer not null default 0,
  degraded_promotion_count integer not null default 0,
  rollback_risk_count integer not null default 0,
  metadata jsonb not null default '{}'::jsonb
);

create index if not exists governance_manager_analytics_snapshot_workspace_idx
  on public.governance_manager_analytics_snapshots (workspace_id, snapshot_at desc);

create or replace view public.governance_manager_overview_summary as
select
  s.workspace_id,
  w.slug as workspace_slug,
  s.id as snapshot_id,
  s.snapshot_at,
  s.window_days,
  s.open_case_count,
  s.recurring_case_count,
  s.escalated_case_count,
  s.chronic_watchlist_count,
  s.degraded_promotion_count,
  s.rollback_risk_count,
  (s.open_case_count + s.recurring_case_count + s.escalated_case_count)::integer as total_operating_burden,
  s.metadata
from public.governance_manager_analytics_snapshots s
join public.workspaces w
  on w.id = s.workspace_id;

create or replace view public.governance_chronic_watchlist_summary as
with watchlist_activity as (
  select
    workspace_id,
    watchlist_id,
    max(updated_at) as latest_case_at
  from public.governance_cases
  where watchlist_id is not null
  group by workspace_id, watchlist_id
)
select
  r.workspace_id,
  w.slug as workspace_slug,
  r.watchlist_id,
  wl.slug as watchlist_slug,
  wl.name as watchlist_name,
  r.recurring_case_count,
  r.reopened_case_count,
  r.max_repeat_count,
  r.recurrence_group_count,
  a.latest_case_at
from public.governance_recurrence_burden_summary r
join public.workspaces w
  on w.id = r.workspace_id
left join public.watchlists wl
  on wl.id = r.watchlist_id
left join watchlist_activity a
  on a.workspace_id = r.workspace_id
 and a.watchlist_id = r.watchlist_id
where r.recurring_case_count > 0;

create or replace view public.governance_operator_team_comparison_summary as
with operator_rows as (
  select
    o.workspace_id,
    o.workspace_slug,
    'operator'::text as entity_type,
    o.operator_name as actor_name,
    null::text as team_name,
    o.assigned_case_count,
    o.active_open_case_count,
    o.resolution_quality_proxy,
    o.reopen_rate,
    o.escalation_rate,
    o.reassignment_rate,
    o.chronic_case_count,
    o.severe_case_count,
    o.avg_ack_seconds,
    o.avg_resolve_seconds,
    p.severity_weighted_load
  from public.governance_operator_performance_summary o
  left join public.governance_operator_workload_pressure p
    on p.workspace_id = o.workspace_id
   and p.assigned_to = o.operator_name
),
team_rows as (
  select
    t.workspace_id,
    t.workspace_slug,
    'team'::text as entity_type,
    t.assigned_team as actor_name,
    t.assigned_team as team_name,
    t.assigned_case_count,
    t.active_open_case_count,
    t.resolution_quality_proxy,
    t.reopen_rate,
    t.escalation_rate,
    t.reassignment_rate,
    t.chronic_case_count,
    t.severe_case_count,
    t.avg_ack_seconds,
    t.avg_resolve_seconds,
    p.severity_weighted_load
  from public.governance_team_performance_summary t
  left join public.governance_team_workload_pressure p
    on p.workspace_id = t.workspace_id
   and p.assigned_team = t.assigned_team
)
select * from operator_rows
union all
select * from team_rows;

create or replace view public.governance_promotion_health_overview as
select
  s.workspace_id,
  w.slug as workspace_slug,
  s.promotion_type,
  count(*)::integer as promotion_count,
  count(*) filter (where s.impact_classification = 'improved')::integer as improved_count,
  count(*) filter (where s.impact_classification = 'neutral')::integer as neutral_count,
  count(*) filter (where s.impact_classification = 'degraded')::integer as degraded_count,
  count(*) filter (where s.impact_classification = 'rollback_candidate')::integer as rollback_candidate_count,
  round(avg(s.rollback_risk_score)::numeric, 4) as avg_rollback_risk_score,
  max(s.rollback_risk_score) as max_rollback_risk_score,
  max(s.created_at) as latest_created_at
from public.governance_promotion_impact_snapshots s
join public.workspaces w
  on w.id = s.workspace_id
group by s.workspace_id, w.slug, s.promotion_type;

create or replace view public.governance_operating_risk_summary as
with latest_snapshot as (
  select distinct on (workspace_id)
    workspace_id,
    workspace_slug,
    snapshot_at,
    open_case_count,
    recurring_case_count,
    escalated_case_count,
    chronic_watchlist_count,
    degraded_promotion_count,
    rollback_risk_count,
    total_operating_burden
  from public.governance_manager_overview_summary
  order by workspace_id, snapshot_at desc
),
stale_counts as (
  select workspace_id, count(*)::integer as stale_case_count
  from public.governance_stale_case_summary
  group by workspace_id
),
top_root_cause as (
  select distinct on (workspace_id)
    workspace_id,
    root_cause_code,
    case_count
  from public.governance_root_cause_trend_summary
  order by workspace_id, case_count desc, reopened_count desc, root_cause_code asc
)
select
  s.workspace_id,
  s.workspace_slug,
  s.snapshot_at,
  case
    when s.rollback_risk_count > 0 or s.degraded_promotion_count > 0 then 'high'
    when s.escalated_case_count > 0 or coalesce(st.stale_case_count, 0) > 0 or s.recurring_case_count > 2 then 'medium'
    else 'low'
  end as operating_risk,
  jsonb_build_object(
    'openCaseCount', s.open_case_count,
    'recurringCaseCount', s.recurring_case_count,
    'escalatedCaseCount', s.escalated_case_count,
    'chronicWatchlistCount', s.chronic_watchlist_count,
    'degradedPromotionCount', s.degraded_promotion_count,
    'rollbackRiskCount', s.rollback_risk_count,
    'staleCaseCount', coalesce(st.stale_case_count, 0),
    'topRootCause', tr.root_cause_code,
    'topRootCauseCaseCount', tr.case_count
  ) as supporting_metrics
from latest_snapshot s
left join stale_counts st
  on st.workspace_id = s.workspace_id
left join top_root_cause tr
  on tr.workspace_id = s.workspace_id;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0051',
  '0051_phase3_4D_manager_overview',
  current_user,
  jsonb_build_object(
    'phase', '3.4D',
    'description', 'Manager-facing analytics and operating review surfaces'
  )
)
on conflict (version) do update
set
  name = excluded.name,
  applied_at = now(),
  applied_by = excluded.applied_by,
  metadata = excluded.metadata;

commit;
