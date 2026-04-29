begin;

create or replace view public.version_stability_summary as
select
  jr.workspace_id,
  coalesce(jr.compute_version, 'unknown') as compute_version,
  coalesce(jr.signal_registry_version, 'unknown') as signal_registry_version,
  coalesce(jr.model_version, 'unknown') as model_version,
  count(*)::bigint as run_count,
  avg(jr.runtime_ms)::double precision as avg_runtime_ms,
  avg(case when jr.status = 'completed' then 1.0 else 0.0 end)::double precision as completion_rate,
  avg(case when jr.status in ('failed', 'dead_lettered') then 1.0 else 0.0 end)::double precision as failure_rate,
  avg(coalesce(rsb.family_instability_score, 0))::double precision as avg_family_instability,
  avg(coalesce(rsb.regime_instability_score, 0))::double precision as avg_regime_instability,
  avg(coalesce(rsb.replay_consistency_risk_score, 0))::double precision as avg_replay_consistency_risk,
  max(jr.completed_at) as last_completed_at
from public.job_runs jr
left join public.run_stability_baselines rsb
  on rsb.run_id = jr.id
 and rsb.window_size = 7
where jr.is_replay = false
group by 1, 2, 3, 4;

create or replace view public.version_replay_consistency_summary as
select
  src.workspace_id,
  coalesce(src.compute_version, 'unknown') as compute_version,
  coalesce(src.signal_registry_version, 'unknown') as signal_registry_version,
  coalesce(src.model_version, 'unknown') as model_version,
  count(*)::bigint as replay_count,
  avg(coalesce(rdelta.input_match_score, 0))::double precision as avg_input_match_score,
  avg(coalesce(rdelta.composite_delta_abs, 0))::double precision as avg_replay_composite_delta_abs,
  avg(case when coalesce(rdelta.severity, 'low') in ('high', 'critical') then 1.0 else 0.0 end)::double precision as elevated_replay_rate,
  max(replay.completed_at) as last_replay_completed_at
from public.job_run_replay_deltas rdelta
join public.job_runs replay
  on replay.id = rdelta.replay_run_id
join public.job_runs src
  on src.id = rdelta.source_run_id
group by 1, 2, 3, 4;

create or replace view public.version_regime_behavior_summary as
select
  jr.workspace_id,
  coalesce(jr.compute_version, 'unknown') as compute_version,
  coalesce(jr.signal_registry_version, 'unknown') as signal_registry_version,
  coalesce(jr.model_version, 'unknown') as model_version,
  count(*) filter (where coalesce(rte.transition_detected, false))::bigint as transition_count,
  avg(case when rte.transition_classification = 'conflicting' then 1.0 else 0.0 end)
    filter (where coalesce(rte.transition_detected, false))::double precision as conflicting_transition_rate,
  avg(coalesce(rte.stability_score, 0))
    filter (where coalesce(rte.transition_detected, false))::double precision as avg_transition_stability_score,
  avg(coalesce(rte.anomaly_likelihood, 0))
    filter (where coalesce(rte.transition_detected, false))::double precision as avg_transition_anomaly_likelihood
from public.job_runs jr
left join public.regime_transition_events rte
  on rte.run_id = jr.id
where jr.is_replay = false
group by 1, 2, 3, 4;

create or replace view public.version_health_rankings as
select
  vss.workspace_id,
  ws.slug as workspace_slug,
  vss.compute_version,
  vss.signal_registry_version,
  vss.model_version,
  vss.run_count,
  vss.avg_runtime_ms,
  vss.completion_rate,
  vss.failure_rate,
  vss.avg_family_instability,
  vss.avg_regime_instability,
  vss.avg_replay_consistency_risk,
  coalesce(vrcs.replay_count, 0) as replay_count,
  coalesce(vrcs.avg_input_match_score, greatest(0.0, 1.0 - least(coalesce(vss.avg_replay_consistency_risk, 0), 1.0))) as avg_input_match_score,
  coalesce(vrcs.avg_replay_composite_delta_abs, 0) as avg_replay_composite_delta_abs,
  coalesce(vrcs.elevated_replay_rate, 0) as elevated_replay_rate,
  coalesce(vrbs.transition_count, 0) as transition_count,
  coalesce(vrbs.conflicting_transition_rate, 0) as conflicting_transition_rate,
  coalesce(vrbs.avg_transition_stability_score, 0) as avg_transition_stability_score,
  coalesce(vrbs.avg_transition_anomaly_likelihood, 0) as avg_transition_anomaly_likelihood,
  (
    (1 - least(coalesce(vss.failure_rate, 0), 1)) * 0.30 +
    (1 - least(coalesce(vss.avg_family_instability, 0), 1)) * 0.20 +
    (1 - least(coalesce(vss.avg_regime_instability, 0), 1)) * 0.15 +
    (1 - least(coalesce(vss.avg_replay_consistency_risk, 0), 1)) * 0.15 +
    least(coalesce(vrcs.avg_input_match_score, greatest(0.0, 1.0 - least(coalesce(vss.avg_replay_consistency_risk, 0), 1.0))), 1) * 0.10 +
    (1 - least(coalesce(vrbs.conflicting_transition_rate, 0), 1)) * 0.10
  )::double precision as governance_health_score,
  dense_rank() over (
    partition by vss.workspace_id
    order by (
      (1 - least(coalesce(vss.failure_rate, 0), 1)) * 0.30 +
      (1 - least(coalesce(vss.avg_family_instability, 0), 1)) * 0.20 +
      (1 - least(coalesce(vss.avg_regime_instability, 0), 1)) * 0.15 +
      (1 - least(coalesce(vss.avg_replay_consistency_risk, 0), 1)) * 0.15 +
      least(coalesce(vrcs.avg_input_match_score, greatest(0.0, 1.0 - least(coalesce(vss.avg_replay_consistency_risk, 0), 1.0))), 1) * 0.10 +
      (1 - least(coalesce(vrbs.conflicting_transition_rate, 0), 1)) * 0.10
    ) desc,
    vss.run_count desc,
    vss.last_completed_at desc nulls last
  ) as health_rank,
  vss.last_completed_at,
  vrcs.last_replay_completed_at
from public.version_stability_summary vss
join public.workspaces ws
  on ws.id = vss.workspace_id
left join public.version_replay_consistency_summary vrcs
  on vrcs.workspace_id = vss.workspace_id
 and vrcs.compute_version = vss.compute_version
 and vrcs.signal_registry_version = vss.signal_registry_version
 and vrcs.model_version = vss.model_version
left join public.version_regime_behavior_summary vrbs
  on vrbs.workspace_id = vss.workspace_id
 and vrbs.compute_version = vss.compute_version
 and vrbs.signal_registry_version = vss.signal_registry_version
 and vrbs.model_version = vss.model_version;

comment on view public.version_health_rankings is
  'Workspace-scoped governance ranking for version tuples using run stability, replay consistency, and regime behavior.';

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0022_phase2_6C_version_governance',
  'Add version governance views and health rankings',
  current_user,
  jsonb_build_object(
    'views', jsonb_build_array(
      'version_stability_summary',
      'version_replay_consistency_summary',
      'version_regime_behavior_summary',
      'version_health_rankings'
    ),
    'focus', jsonb_build_array(
      'version_stability',
      'replay_consistency_by_version',
      'regime_behavior_by_version',
      'governance_health_rankings'
    )
  )
)
on conflict (version) do nothing;

commit;
