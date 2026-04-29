begin;

create table if not exists public.market_data_sync_runs (
  id uuid primary key default gen_random_uuid(),
  source text not null,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  status text not null check (status in ('running', 'completed', 'failed')),
  requested_symbols jsonb not null default '[]'::jsonb,
  synced_symbols jsonb not null default '[]'::jsonb,
  asset_count integer not null default 0,
  bar_count integer not null default 0,
  open_interest_count integer not null default 0,
  funding_count integer not null default 0,
  liquidation_count integer not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  error text,
  started_at timestamptz not null default now(),
  completed_at timestamptz
);

create index if not exists market_data_sync_runs_workspace_started_idx
  on public.market_data_sync_runs(workspace_id, started_at desc);

create index if not exists market_data_sync_runs_watchlist_started_idx
  on public.market_data_sync_runs(watchlist_id, started_at desc);

create unique index if not exists uidx_market_liquidations_aggregate_asset_ts_side_source
  on public.market_liquidations(asset_id, ts, side, source)
  where liquidation_notional_1h is not null;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0024_phase2_7_crypto_market_ingestion',
  'Add crypto market sync audit table and aggregate liquidation idempotency index',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array('market_data_sync_runs'),
    'indexes', jsonb_build_array(
      'market_data_sync_runs_workspace_started_idx',
      'market_data_sync_runs_watchlist_started_idx',
      'uidx_market_liquidations_aggregate_asset_ts_side_source'
    ),
    'focus', jsonb_build_array(
      'crypto_market_sync_audit',
      'idempotent_liquidation_aggregate_upserts'
    )
  )
)
on conflict (version) do nothing;

commit;
