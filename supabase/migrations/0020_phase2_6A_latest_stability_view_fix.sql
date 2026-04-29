begin;

create or replace view public.latest_stability_summary as
with latest as (
  select distinct on (rsb.workspace_id, rsb.watchlist_id, rsb.queue_name, rsb.window_size)
    rsb.*
  from public.run_stability_baselines rsb
  join public.job_runs jr
    on jr.id = rsb.run_id
  where jr.is_replay = false
  order by
    rsb.workspace_id,
    rsb.watchlist_id,
    rsb.queue_name,
    rsb.window_size,
    jr.completed_at desc nulls last,
    rsb.created_at desc,
    rsb.run_id desc
)
select
  latest.run_id,
  latest.workspace_id,
  ws.slug as workspace_slug,
  latest.watchlist_id,
  wl.slug as watchlist_slug,
  wl.name as watchlist_name,
  latest.queue_name,
  latest.window_size,
  latest.baseline_run_count,
  latest.composite_current,
  latest.composite_baseline,
  latest.composite_delta_abs,
  latest.composite_delta_pct,
  latest.composite_instability_score,
  latest.family_instability_score,
  latest.replay_consistency_risk_score,
  latest.regime_instability_score,
  latest.dominant_family,
  latest.dominant_family_changed,
  latest.dominant_regime,
  latest.regime_changed,
  latest.stability_classification,
  rcm.replay_runs_considered,
  rcm.mismatch_rate,
  rcm.avg_input_match_score,
  rcm.avg_composite_delta_abs,
  gsm.transitions_considered,
  gsm.conflicting_transition_count,
  gsm.abrupt_transition_count,
  coalesce((
    select jsonb_agg(
      jsonb_build_object(
        'signal_family', sf.signal_family,
        'family_score_current', sf.family_score_current,
        'family_score_baseline', sf.family_score_baseline,
        'family_delta_abs', sf.family_delta_abs,
        'family_delta_pct', sf.family_delta_pct,
        'instability_score', sf.instability_score,
        'family_rank', sf.family_rank,
        'metadata', sf.metadata
      )
      order by sf.family_rank asc
    )
    from public.signal_family_stability_metrics sf
    where sf.run_id = latest.run_id
  ), '[]'::jsonb) as family_rows,
  latest.metadata,
  latest.created_at
from latest
join public.workspaces ws on ws.id = latest.workspace_id
left join public.watchlists wl on wl.id = latest.watchlist_id
left join public.replay_consistency_metrics rcm on rcm.run_id = latest.run_id
left join public.regime_stability_metrics gsm on gsm.run_id = latest.run_id;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0020_phase2_6A_latest_stability_view_fix',
  'Fix latest stability summary ordering to prefer most recently completed primary run',
  current_user,
  jsonb_build_object(
    'views', jsonb_build_array('latest_stability_summary'),
    'focus', jsonb_build_array('latest_row_ordering', 'primary_run_preference')
  )
)
on conflict (version) do nothing;

commit;
