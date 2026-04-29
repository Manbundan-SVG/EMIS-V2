begin;

create table if not exists public.job_run_drift_metrics (
  id bigserial primary key,
  run_id uuid not null references public.job_runs(id) on delete cascade,
  comparison_run_id uuid references public.job_runs(id) on delete set null,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  metric_type text not null,
  entity_name text not null,
  current_value double precision,
  baseline_value double precision,
  delta_abs double precision,
  delta_pct double precision,
  z_score double precision,
  drift_flag boolean not null default false,
  severity text not null default 'low',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_job_run_drift_metrics_run_id
  on public.job_run_drift_metrics(run_id, drift_flag desc, severity asc);

create index if not exists idx_job_run_drift_metrics_comparison_run_id
  on public.job_run_drift_metrics(comparison_run_id);

create index if not exists idx_job_run_drift_metrics_workspace_watchlist
  on public.job_run_drift_metrics(workspace_id, watchlist_id, metric_type);

alter table public.job_runs
  add column if not exists drift_summary jsonb not null default '{}'::jsonb,
  add column if not exists drift_severity text,
  add column if not exists comparison_run_id uuid references public.job_runs(id) on delete set null;

create or replace view public.job_run_drift_summary as
select
  jr.id as run_id,
  jr.workspace_id,
  jr.watchlist_id,
  jr.comparison_run_id,
  jr.drift_severity,
  jr.drift_summary,
  jr.compute_version as current_compute_version,
  cmp.compute_version as comparison_compute_version,
  jr.signal_registry_version as current_signal_registry_version,
  cmp.signal_registry_version as comparison_signal_registry_version,
  jr.model_version as current_model_version,
  cmp.model_version as comparison_model_version,
  count(dm.id)::bigint as metric_count,
  count(dm.id) filter (where dm.drift_flag)::bigint as flagged_metric_count,
  max(dm.created_at) as computed_at
from public.job_runs jr
left join public.job_runs cmp on cmp.id = jr.comparison_run_id
left join public.job_run_drift_metrics dm on dm.run_id = jr.id
group by
  jr.id,
  jr.workspace_id,
  jr.watchlist_id,
  jr.comparison_run_id,
  jr.drift_severity,
  jr.drift_summary,
  jr.compute_version,
  cmp.compute_version,
  jr.signal_registry_version,
  cmp.signal_registry_version,
  jr.model_version,
  cmp.model_version;

comment on view public.job_run_drift_summary is
  'Run-level drift summary with comparator metadata and flagged metric counts.';

do $$
begin
  begin
    alter publication supabase_realtime add table public.job_run_drift_metrics;
  exception
    when duplicate_object then null;
    when undefined_object then null;
  end;
end $$;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0016_phase2_5B_drift',
  'Add run drift monitoring metrics and drift summary view',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array('job_run_drift_metrics'),
    'job_runs_columns', jsonb_build_array('drift_summary', 'drift_severity', 'comparison_run_id'),
    'views', jsonb_build_array('job_run_drift_summary')
  )
)
on conflict (version) do nothing;

commit;
