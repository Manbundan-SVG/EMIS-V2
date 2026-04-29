begin;

create table if not exists public.job_run_compute_scopes (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null unique references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  queue_name text not null default 'recompute',
  scope_version text not null default 'phase2.6B/v1',
  primary_assets jsonb not null default '[]'::jsonb,
  dependency_assets jsonb not null default '[]'::jsonb,
  asset_universe jsonb not null default '[]'::jsonb,
  primary_asset_count integer not null default 0,
  dependency_asset_count integer not null default 0,
  asset_universe_count integer not null default 0,
  dependency_policy jsonb not null default '{}'::jsonb,
  scope_hash text not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_job_run_compute_scopes_workspace_watchlist_created
  on public.job_run_compute_scopes (workspace_id, watchlist_id, queue_name, created_at desc);

alter table public.job_run_input_snapshots
  add column if not exists compute_scope_id uuid references public.job_run_compute_scopes(id) on delete set null,
  add column if not exists scope_hash text,
  add column if not exists scope_version text,
  add column if not exists primary_asset_count integer,
  add column if not exists dependency_asset_count integer,
  add column if not exists asset_universe_count integer;

create or replace view public.run_scope_inspection as
select
  jr.id as run_id,
  jr.workspace_id,
  ws.slug as workspace_slug,
  jr.watchlist_id,
  wl.slug as watchlist_slug,
  wl.name as watchlist_name,
  jr.queue_id,
  jr.queue_name,
  jr.status,
  jr.is_replay,
  jr.replayed_from_run_id,
  sc.id as compute_scope_id,
  sc.scope_version,
  sc.scope_hash,
  sc.primary_assets,
  sc.dependency_assets,
  sc.asset_universe,
  sc.primary_asset_count,
  sc.dependency_asset_count,
  sc.asset_universe_count,
  sc.dependency_policy,
  sc.metadata,
  sc.created_at as scope_created_at
from public.job_runs jr
join public.workspaces ws
  on ws.id = jr.workspace_id
left join public.watchlists wl
  on wl.id = jr.watchlist_id
left join public.job_run_compute_scopes sc
  on sc.run_id = jr.id;

comment on view public.run_scope_inspection is
  'Watchlist-scoped compute snapshot for each run, including persisted primary/dependency universe and scope hash.';

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0021_phase2_6B_watchlist_scoped_compute',
  'Add watchlist-scoped compute persistence and scope inspection view',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array('job_run_compute_scopes'),
    'views', jsonb_build_array('run_scope_inspection'),
    'columns', jsonb_build_array(
      'job_run_input_snapshots.compute_scope_id',
      'job_run_input_snapshots.scope_hash',
      'job_run_input_snapshots.scope_version',
      'job_run_input_snapshots.primary_asset_count',
      'job_run_input_snapshots.dependency_asset_count',
      'job_run_input_snapshots.asset_universe_count'
    )
  )
)
on conflict (version) do nothing;

commit;
