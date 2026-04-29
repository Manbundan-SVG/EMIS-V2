begin;

create table if not exists public.job_run_replay_deltas (
  replay_run_id uuid primary key references public.job_runs(id) on delete cascade,
  source_run_id uuid not null references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  input_match_score double precision not null default 0,
  input_match_details jsonb not null default '{}'::jsonb,
  version_match boolean not null default true,
  compute_version_changed boolean not null default false,
  signal_registry_version_changed boolean not null default false,
  model_version_changed boolean not null default false,
  regime_changed boolean not null default false,
  source_regime text,
  replay_regime text,
  source_composite double precision,
  replay_composite double precision,
  composite_delta double precision,
  composite_delta_abs double precision,
  largest_signal_deltas jsonb not null default '[]'::jsonb,
  largest_family_deltas jsonb not null default '[]'::jsonb,
  summary jsonb not null default '{}'::jsonb,
  severity text not null default 'low',
  created_at timestamptz not null default now()
);

create index if not exists idx_job_run_replay_deltas_source_run_id
  on public.job_run_replay_deltas(source_run_id);

create index if not exists idx_job_run_replay_deltas_workspace_watchlist
  on public.job_run_replay_deltas(workspace_id, watchlist_id, severity);

create or replace view public.job_run_version_behavior_comparison as
with drift as (
  select
    run_id,
    flagged_metric_count
  from public.job_run_drift_summary
),
replay as (
  select
    replay_run_id as run_id,
    input_match_score,
    composite_delta_abs,
    severity
  from public.job_run_replay_deltas
)
select
  jr.workspace_id,
  jr.watchlist_id,
  jr.queue_name,
  jr.compute_version,
  jr.signal_registry_version,
  jr.model_version,
  count(*) filter (where jr.status = 'completed')::bigint as run_count,
  count(*) filter (where jr.status = 'completed' and jr.is_replay = true)::bigint as replay_run_count,
  avg(jr.attribution_target_total) filter (where jr.status = 'completed')::double precision as avg_composite_score,
  avg(coalesce(drift.flagged_metric_count, 0)) filter (where jr.status = 'completed')::double precision as avg_flagged_drift_metrics,
  avg(replay.input_match_score) filter (where replay.run_id is not null)::double precision as avg_replay_input_match_score,
  avg(replay.composite_delta_abs) filter (where replay.run_id is not null)::double precision as avg_replay_composite_delta_abs,
  count(*) filter (where replay.severity = 'high')::bigint as high_severity_replay_count,
  max(coalesce(jr.completed_at, jr.finished_at, jr.created_at)) as latest_completed_at
from public.job_runs jr
left join drift on drift.run_id = jr.id
left join replay on replay.run_id = jr.id
group by
  jr.workspace_id,
  jr.watchlist_id,
  jr.queue_name,
  jr.compute_version,
  jr.signal_registry_version,
  jr.model_version;

comment on view public.job_run_version_behavior_comparison is
  'Version-level behavior summary across completed runs, including drift and replay consistency metrics.';

do $$
begin
  begin
    alter publication supabase_realtime add table public.job_run_replay_deltas;
  exception
    when duplicate_object then null;
    when undefined_object then null;
  end;
end $$;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0017_phase2_5C_replay_delta',
  'Add replay delta diagnostics and version behavior comparison view',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array('job_run_replay_deltas'),
    'views', jsonb_build_array('job_run_version_behavior_comparison'),
    'focus', jsonb_build_array('replay_delta', 'version_behavior')
  )
)
on conflict (version) do nothing;

commit;
