begin;

create or replace view public.governance_review_priority_summary as
with current_case_rollup as (
  select
    c.workspace_id,
    c.id as case_id,
    c.watchlist_id,
    c.current_assignee,
    c.current_team,
    coalesce(gcs.root_cause_code, 'unknown') as root_cause_code,
    coalesce(gds.regime, 'unknown') as regime,
    c.status,
    c.severity,
    coalesce(c.repeat_count, 1) as repeat_count,
    case when es.case_id is not null then true else false end as is_escalated,
    case when sc.case_id is not null then true else false end as is_stale,
    c.updated_at
  from public.governance_cases c
  left join public.governance_case_summary_latest gcs
    on gcs.case_id = c.id
  left join public.governance_degradation_states gds
    on gds.id = c.degradation_state_id
  left join lateral (
    select state.case_id
    from public.governance_escalation_state state
    where state.case_id = c.id
      and coalesce(state.status, 'active') <> 'cleared'
    limit 1
  ) es on true
  left join public.governance_stale_case_summary sc
    on sc.case_id = c.id
),
watchlist_latest_context as (
  select distinct on (workspace_id, watchlist_id)
    workspace_id,
    watchlist_id,
    regime as latest_regime,
    root_cause_code as latest_root_cause
  from current_case_rollup
  where watchlist_id is not null
  order by workspace_id, watchlist_id, updated_at desc, case_id asc
),
watchlist_live_counts as (
  select
    workspace_id,
    watchlist_id,
    count(*) filter (where status in ('open', 'acknowledged', 'in_progress'))::integer as open_case_count,
    count(*) filter (
      where status in ('open', 'acknowledged', 'in_progress')
        and repeat_count > 1
    )::integer as recurring_case_count,
    count(*) filter (
      where status in ('open', 'acknowledged', 'in_progress')
        and is_escalated
    )::integer as escalated_case_count,
    count(*) filter (
      where status in ('open', 'acknowledged', 'in_progress')
        and is_stale
    )::integer as stale_case_count
  from current_case_rollup
  where watchlist_id is not null
  group by workspace_id, watchlist_id
),
watchlist_candidates as (
  select
    cw.workspace_id,
    'watchlist'::text as entity_type,
    coalesce(cw.watchlist_slug, cw.watchlist_id::text, 'workspace') as entity_key,
    coalesce(cw.watchlist_name, cw.watchlist_slug, cw.watchlist_id::text, 'Workspace watchlist') as entity_label,
    round(
      (
        coalesce(lc.recurring_case_count, cw.recurring_case_count, 0) * 8
        + coalesce(lc.stale_case_count, 0) * 7
        + coalesce(lc.escalated_case_count, 0) * 5
        + coalesce(lc.open_case_count, 0) * 3
        + coalesce(cw.reopened_case_count, 0) * 4
        + coalesce(cw.max_repeat_count, 0) * 2
      )::numeric,
      4
    ) as priority_score,
    case
      when coalesce(lc.stale_case_count, 0) >= greatest(
        coalesce(lc.recurring_case_count, 0),
        coalesce(lc.escalated_case_count, 0),
        coalesce(lc.open_case_count, 0),
        0
      ) and coalesce(lc.stale_case_count, 0) > 0 then 'stale_severe_backlog'
      when coalesce(lc.recurring_case_count, cw.recurring_case_count, 0) >= greatest(
        coalesce(lc.stale_case_count, 0),
        coalesce(lc.escalated_case_count, 0),
        coalesce(lc.open_case_count, 0),
        0
      ) and coalesce(lc.recurring_case_count, cw.recurring_case_count, 0) > 0 then 'recurring_burden_spike'
      when coalesce(lc.escalated_case_count, 0) >= greatest(
        coalesce(lc.stale_case_count, 0),
        coalesce(lc.recurring_case_count, cw.recurring_case_count, 0),
        coalesce(lc.open_case_count, 0),
        0
      ) and coalesce(lc.escalated_case_count, 0) > 0 then 'escalation_concentration'
      else 'open_burden_concentration'
    end as priority_reason_code,
    coalesce(lc.open_case_count, 0) as open_case_count,
    coalesce(lc.recurring_case_count, cw.recurring_case_count, 0) as recurring_case_count,
    coalesce(lc.escalated_case_count, 0) as escalated_case_count,
    0::integer as rollback_risk_count,
    coalesce(lc.stale_case_count, 0) as stale_case_count,
    ctx.latest_regime,
    ctx.latest_root_cause,
    now() as snapshot_at
  from public.governance_chronic_watchlist_summary cw
  left join watchlist_live_counts lc
    on lc.workspace_id = cw.workspace_id
   and lc.watchlist_id = cw.watchlist_id
  left join watchlist_latest_context ctx
    on ctx.workspace_id = cw.workspace_id
   and ctx.watchlist_id = cw.watchlist_id
),
operator_dominant_mix as (
  select distinct on (workspace_id, actor_name)
    workspace_id,
    actor_name,
    regime,
    root_cause_code
  from public.governance_operator_case_mix_summary
  order by workspace_id, actor_name, case_count desc, recurring_case_count desc, severe_case_count desc, root_cause_code asc, regime asc
),
team_dominant_mix as (
  select distinct on (workspace_id, actor_name)
    workspace_id,
    actor_name,
    regime,
    root_cause_code
  from public.governance_team_case_mix_summary
  order by workspace_id, actor_name, case_count desc, recurring_case_count desc, severe_case_count desc, root_cause_code asc, regime asc
),
operator_stale_counts as (
  select
    workspace_id,
    current_assignee as actor_name,
    count(*)::integer as stale_case_count
  from public.governance_stale_case_summary
  where current_assignee is not null
  group by workspace_id, current_assignee
),
team_stale_counts as (
  select
    workspace_id,
    current_team as actor_name,
    count(*)::integer as stale_case_count
  from public.governance_stale_case_summary
  where current_team is not null
  group by workspace_id, current_team
),
operator_candidates as (
  select
    cmp.workspace_id,
    'operator'::text as entity_type,
    cmp.actor_name as entity_key,
    cmp.actor_name as entity_label,
    round(
      (
        coalesce(p.recurring_case_count, 0) * 8
        + coalesce(st.stale_case_count, 0) * 7
        + round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer * 5
        + coalesce(p.open_case_count, cmp.active_open_case_count, 0) * 3
        + coalesce(p.severity_weighted_load, 0)
        + greatest(0::numeric, (1 - coalesce(cmp.resolution_quality_proxy, 0))) * 10
      )::numeric,
      4
    ) as priority_score,
    case
      when coalesce(st.stale_case_count, 0) >= greatest(
        coalesce(p.recurring_case_count, 0),
        round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer,
        coalesce(p.open_case_count, cmp.active_open_case_count, 0),
        0
      ) and coalesce(st.stale_case_count, 0) > 0 then 'stale_severe_backlog'
      when coalesce(p.recurring_case_count, 0) >= greatest(
        coalesce(st.stale_case_count, 0),
        round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer,
        coalesce(p.open_case_count, cmp.active_open_case_count, 0),
        0
      ) and coalesce(p.recurring_case_count, 0) > 0 then 'recurring_burden_spike'
      when coalesce(p.severity_weighted_load, 0) >= greatest(
        coalesce(st.stale_case_count, 0),
        coalesce(p.recurring_case_count, 0),
        round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer,
        coalesce(p.open_case_count, cmp.active_open_case_count, 0),
        0
      ) then 'operator_overload'
      when round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer > 0 then 'escalation_concentration'
      else 'open_burden_concentration'
    end as priority_reason_code,
    coalesce(p.open_case_count, cmp.active_open_case_count, 0) as open_case_count,
    coalesce(p.recurring_case_count, 0) as recurring_case_count,
    round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer as escalated_case_count,
    0::integer as rollback_risk_count,
    coalesce(st.stale_case_count, 0) as stale_case_count,
    mix.regime as latest_regime,
    mix.root_cause_code as latest_root_cause,
    now() as snapshot_at
  from public.governance_operator_team_comparison_summary cmp
  left join public.governance_operator_workload_pressure p
    on p.workspace_id = cmp.workspace_id
   and p.assigned_to = cmp.actor_name
  left join operator_stale_counts st
    on st.workspace_id = cmp.workspace_id
   and st.actor_name = cmp.actor_name
  left join operator_dominant_mix mix
    on mix.workspace_id = cmp.workspace_id
   and mix.actor_name = cmp.actor_name
  where cmp.entity_type = 'operator'
),
team_candidates as (
  select
    cmp.workspace_id,
    'team'::text as entity_type,
    cmp.actor_name as entity_key,
    cmp.actor_name as entity_label,
    round(
      (
        coalesce(p.recurring_case_count, 0) * 8
        + coalesce(st.stale_case_count, 0) * 7
        + round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer * 5
        + coalesce(p.open_case_count, cmp.active_open_case_count, 0) * 3
        + coalesce(p.severity_weighted_load, 0)
        + greatest(0::numeric, (1 - coalesce(cmp.resolution_quality_proxy, 0))) * 10
      )::numeric,
      4
    ) as priority_score,
    case
      when coalesce(st.stale_case_count, 0) >= greatest(
        coalesce(p.recurring_case_count, 0),
        round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer,
        coalesce(p.open_case_count, cmp.active_open_case_count, 0),
        0
      ) and coalesce(st.stale_case_count, 0) > 0 then 'stale_severe_backlog'
      when coalesce(p.recurring_case_count, 0) >= greatest(
        coalesce(st.stale_case_count, 0),
        round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer,
        coalesce(p.open_case_count, cmp.active_open_case_count, 0),
        0
      ) and coalesce(p.recurring_case_count, 0) > 0 then 'recurring_burden_spike'
      when coalesce(p.severity_weighted_load, 0) >= greatest(
        coalesce(st.stale_case_count, 0),
        coalesce(p.recurring_case_count, 0),
        round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer,
        coalesce(p.open_case_count, cmp.active_open_case_count, 0),
        0
      ) then 'team_overload'
      when round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer > 0 then 'escalation_concentration'
      else 'open_burden_concentration'
    end as priority_reason_code,
    coalesce(p.open_case_count, cmp.active_open_case_count, 0) as open_case_count,
    coalesce(p.recurring_case_count, 0) as recurring_case_count,
    round(cmp.assigned_case_count::numeric * cmp.escalation_rate)::integer as escalated_case_count,
    0::integer as rollback_risk_count,
    coalesce(st.stale_case_count, 0) as stale_case_count,
    mix.regime as latest_regime,
    mix.root_cause_code as latest_root_cause,
    now() as snapshot_at
  from public.governance_operator_team_comparison_summary cmp
  left join public.governance_team_workload_pressure p
    on p.workspace_id = cmp.workspace_id
   and p.assigned_team = cmp.actor_name
  left join team_stale_counts st
    on st.workspace_id = cmp.workspace_id
   and st.actor_name = cmp.actor_name
  left join team_dominant_mix mix
    on mix.workspace_id = cmp.workspace_id
   and mix.actor_name = cmp.actor_name
  where cmp.entity_type = 'team'
),
root_cause_latest_regime as (
  select distinct on (workspace_id, root_cause_code)
    workspace_id,
    root_cause_code,
    regime
  from current_case_rollup
  order by workspace_id, root_cause_code, updated_at desc, case_id asc
),
root_cause_live_counts as (
  select
    workspace_id,
    root_cause_code,
    count(*) filter (where status in ('open', 'acknowledged', 'in_progress'))::integer as open_case_count,
    count(*) filter (
      where status in ('open', 'acknowledged', 'in_progress')
        and repeat_count > 1
    )::integer as recurring_case_count,
    count(*) filter (
      where status in ('open', 'acknowledged', 'in_progress')
        and is_escalated
    )::integer as escalated_case_count,
    count(*) filter (
      where status in ('open', 'acknowledged', 'in_progress')
        and is_stale
    )::integer as stale_case_count
  from current_case_rollup
  group by workspace_id, root_cause_code
),
root_cause_candidates as (
  select
    rt.workspace_id,
    'root_cause'::text as entity_type,
    rt.root_cause_code as entity_key,
    initcap(replace(rt.root_cause_code, '_', ' ')) as entity_label,
    round(
      (
        coalesce(lc.recurring_case_count, rt.recurring_count, 0) * 8
        + coalesce(lc.stale_case_count, 0) * 7
        + coalesce(lc.escalated_case_count, 0) * 5
        + coalesce(lc.open_case_count, 0) * 3
        + coalesce(rt.case_count, 0) * 2
        + coalesce(rt.severe_count, 0) * 3
        + coalesce(rt.reopened_count, 0) * 4
      )::numeric,
      4
    ) as priority_score,
    case
      when coalesce(lc.stale_case_count, 0) >= greatest(
        coalesce(lc.recurring_case_count, rt.recurring_count, 0),
        coalesce(lc.escalated_case_count, 0),
        coalesce(lc.open_case_count, 0),
        0
      ) and coalesce(lc.stale_case_count, 0) > 0 then 'stale_severe_backlog'
      when coalesce(lc.recurring_case_count, rt.recurring_count, 0) >= greatest(
        coalesce(lc.stale_case_count, 0),
        coalesce(lc.escalated_case_count, 0),
        coalesce(lc.open_case_count, 0),
        0
      ) and coalesce(lc.recurring_case_count, rt.recurring_count, 0) > 0 then 'recurring_burden_spike'
      when coalesce(lc.escalated_case_count, 0) >= greatest(
        coalesce(lc.stale_case_count, 0),
        coalesce(lc.recurring_case_count, rt.recurring_count, 0),
        coalesce(lc.open_case_count, 0),
        0
      ) and coalesce(lc.escalated_case_count, 0) > 0 then 'escalation_concentration'
      else 'open_burden_concentration'
    end as priority_reason_code,
    coalesce(lc.open_case_count, 0) as open_case_count,
    coalesce(lc.recurring_case_count, rt.recurring_count, 0) as recurring_case_count,
    coalesce(lc.escalated_case_count, 0) as escalated_case_count,
    0::integer as rollback_risk_count,
    coalesce(lc.stale_case_count, 0) as stale_case_count,
    reg.regime as latest_regime,
    rt.root_cause_code as latest_root_cause,
    now() as snapshot_at
  from public.governance_root_cause_trend_summary rt
  left join root_cause_live_counts lc
    on lc.workspace_id = rt.workspace_id
   and lc.root_cause_code = rt.root_cause_code
  left join root_cause_latest_regime reg
    on reg.workspace_id = rt.workspace_id
   and reg.root_cause_code = rt.root_cause_code
),
promotion_candidates as (
  select
    ph.workspace_id,
    'promotion'::text as entity_type,
    ph.promotion_type as entity_key,
    initcap(ph.promotion_type) || ' promotions' as entity_label,
    round(
      (
        coalesce(ph.rollback_candidate_count, 0) * 9
        + coalesce(ph.degraded_count, 0) * 7
        + coalesce(ph.promotion_count, 0)
        + coalesce(ph.avg_rollback_risk_score, 0) * 10
      )::numeric,
      4
    ) as priority_score,
    case
      when coalesce(ph.rollback_candidate_count, 0) > 0 or coalesce(ph.degraded_count, 0) > 0 then 'degraded_promotion_health'
      else 'promotion_review'
    end as priority_reason_code,
    0::integer as open_case_count,
    0::integer as recurring_case_count,
    0::integer as escalated_case_count,
    coalesce(ph.rollback_candidate_count, 0) as rollback_risk_count,
    0::integer as stale_case_count,
    null::text as latest_regime,
    null::text as latest_root_cause,
    now() as snapshot_at
  from public.governance_promotion_health_overview ph
),
all_candidates as (
  select * from watchlist_candidates
  union all
  select * from operator_candidates
  union all
  select * from team_candidates
  union all
  select * from root_cause_candidates
  union all
  select * from promotion_candidates
),
ranked as (
  select
    workspace_id,
    row_number() over (
      partition by workspace_id
      order by priority_score desc, entity_type asc, entity_key asc
    )::integer as priority_rank,
    entity_type,
    entity_key,
    entity_label,
    priority_score,
    priority_reason_code,
    open_case_count,
    recurring_case_count,
    escalated_case_count,
    rollback_risk_count,
    stale_case_count,
    latest_regime,
    latest_root_cause,
    snapshot_at
  from all_candidates
  where priority_score > 0
)
select *
from ranked;

create or replace view public.governance_trend_window_summary as
with windows(window_label, window_days) as (
  values
    ('7d'::text, 7::integer),
    ('30d'::text, 30::integer),
    ('90d'::text, 90::integer)
),
manager_metric_rows as (
  select
    workspace_id,
    snapshot_at,
    'open_case_count'::text as metric_name,
    open_case_count::numeric as metric_value
  from public.governance_manager_analytics_snapshots
  union all
  select workspace_id, snapshot_at, 'recurring_case_count', recurring_case_count::numeric
  from public.governance_manager_analytics_snapshots
  union all
  select workspace_id, snapshot_at, 'escalated_case_count', escalated_case_count::numeric
  from public.governance_manager_analytics_snapshots
  union all
  select workspace_id, snapshot_at, 'stale_case_count', coalesce((metadata ->> 'stale_case_count')::numeric, 0)
  from public.governance_manager_analytics_snapshots
  union all
  select workspace_id, snapshot_at, 'rollback_risk_count', rollback_risk_count::numeric
  from public.governance_manager_analytics_snapshots
  union all
  select workspace_id, snapshot_at, 'operator_pressure_count', coalesce((metadata ->> 'operator_pressure_count')::numeric, 0)
  from public.governance_manager_analytics_snapshots
  union all
  select workspace_id, snapshot_at, 'team_pressure_count', coalesce((metadata ->> 'team_pressure_count')::numeric, 0)
  from public.governance_manager_analytics_snapshots
),
workspace_metrics as (
  select distinct workspace_id, metric_name
  from manager_metric_rows
),
windowed as (
  select
    wm.workspace_id,
    w.window_label,
    wm.metric_name,
    round(
      avg(m.metric_value) filter (
        where m.snapshot_at >= now() - make_interval(days => w.window_days)
          and m.snapshot_at < now()
      )::numeric,
      4
    ) as current_value,
    round(
      avg(m.metric_value) filter (
        where m.snapshot_at >= now() - make_interval(days => (w.window_days * 2))
          and m.snapshot_at < now() - make_interval(days => w.window_days)
      )::numeric,
      4
    ) as prior_value
  from workspace_metrics wm
  cross join windows w
  left join manager_metric_rows m
    on m.workspace_id = wm.workspace_id
   and m.metric_name = wm.metric_name
  group by wm.workspace_id, w.window_label, wm.metric_name
)
select
  workspace_id,
  window_label,
  metric_name,
  coalesce(current_value, 0)::numeric as current_value,
  coalesce(prior_value, 0)::numeric as prior_value,
  (coalesce(current_value, 0) - coalesce(prior_value, 0))::numeric as delta_abs,
  case
    when coalesce(prior_value, 0) = 0 then null::numeric
    else round((((coalesce(current_value, 0) - coalesce(prior_value, 0)) / prior_value) * 100)::numeric, 4)
  end as delta_pct,
  case
    when abs(coalesce(current_value, 0) - coalesce(prior_value, 0)) < 0.0001 then 'flat'
    when coalesce(current_value, 0) > coalesce(prior_value, 0) then 'up'
    else 'down'
  end as trend_direction,
  now() as computed_at
from windowed;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0052',
  '0052_phase3_4D_4_review_priority_and_trends',
  current_user,
  jsonb_build_object(
    'phase', '3.4D.4',
    'description', 'Review-priority ranking and trend-window summaries for manager overview'
  )
)
on conflict (version) do update
set
  name = excluded.name,
  applied_at = now(),
  applied_by = excluded.applied_by,
  metadata = excluded.metadata;

commit;
