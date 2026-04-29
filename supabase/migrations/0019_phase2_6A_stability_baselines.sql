begin;

create table if not exists public.run_stability_baselines (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  queue_name text not null default 'recompute',
  window_size integer not null default 7,
  baseline_run_count integer not null default 0,
  composite_baseline double precision,
  composite_current double precision,
  composite_delta_abs double precision,
  composite_delta_pct double precision,
  composite_instability_score double precision not null default 0,
  family_instability_score double precision not null default 0,
  replay_consistency_risk_score double precision not null default 0,
  regime_instability_score double precision not null default 0,
  dominant_family text,
  dominant_family_changed boolean not null default false,
  dominant_regime text,
  regime_changed boolean not null default false,
  stability_classification text not null default 'stable',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (run_id, window_size)
);

create table if not exists public.signal_family_stability_metrics (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  signal_family text not null,
  family_score_current double precision,
  family_score_baseline double precision,
  family_delta_abs double precision,
  family_delta_pct double precision,
  instability_score double precision not null default 0,
  family_rank integer not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.replay_consistency_metrics (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null unique references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  queue_name text not null default 'recompute',
  replay_runs_considered integer not null default 0,
  mismatch_rate double precision,
  avg_input_match_score double precision,
  avg_composite_delta_abs double precision,
  instability_score double precision not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.regime_stability_metrics (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null unique references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  queue_name text not null default 'recompute',
  transitions_considered integer not null default 0,
  conflicting_transition_count integer not null default 0,
  abrupt_transition_count integer not null default 0,
  instability_score double precision not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_run_stability_baselines_workspace_watchlist
  on public.run_stability_baselines (workspace_id, watchlist_id, queue_name, created_at desc);

create index if not exists idx_signal_family_stability_metrics_run
  on public.signal_family_stability_metrics (run_id, family_rank);

create index if not exists idx_replay_consistency_metrics_workspace_watchlist
  on public.replay_consistency_metrics (workspace_id, watchlist_id, queue_name, created_at desc);

create index if not exists idx_regime_stability_metrics_workspace_watchlist
  on public.regime_stability_metrics (workspace_id, watchlist_id, queue_name, created_at desc);

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
    rsb.created_at desc
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

comment on view public.latest_stability_summary is
  'Latest long-window stability summary per workspace/watchlist/queue, including family, replay, and regime risk metrics.';

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0019_phase2_6A_stability_baselines',
  'Add long-window stability baselines, family stability metrics, and monitoring summary view',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array(
      'run_stability_baselines',
      'signal_family_stability_metrics',
      'replay_consistency_metrics',
      'regime_stability_metrics'
    ),
    'views', jsonb_build_array('latest_stability_summary'),
    'focus', jsonb_build_array('stability_baselines', 'family_instability', 'replay_risk', 'regime_instability')
  )
)
on conflict (version) do nothing;

commit;
