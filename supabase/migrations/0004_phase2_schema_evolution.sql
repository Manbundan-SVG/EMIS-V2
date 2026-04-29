-- Evolve Phase 1 schema to Phase 2 feed and compute requirements.
-- Safe to apply on top of existing Phase 1 data — all changes are additive
-- or loosen constraints. Existing rows are preserved.

-- ── market_bars ──────────────────────────────────────────────────────────────
-- Phase 2 inserts bars without timeframe/source; make them optional.
alter table public.market_bars alter column timeframe drop not null;
alter table public.market_bars alter column source   drop not null;
-- Pre-computed 1h return and volume z-score stored alongside the bar.
alter table public.market_bars add column if not exists return_1h    numeric;
alter table public.market_bars add column if not exists volume_zscore numeric;

-- ── market_open_interest ─────────────────────────────────────────────────────
alter table public.market_open_interest alter column source drop not null;
alter table public.market_open_interest add column if not exists oi_change_1h numeric;

-- ── market_funding ───────────────────────────────────────────────────────────
alter table public.market_funding alter column source drop not null;

-- ── market_liquidations ──────────────────────────────────────────────────────
alter table public.market_liquidations alter column side   drop not null;
alter table public.market_liquidations alter column source drop not null;
alter table public.market_liquidations add column if not exists liquidation_notional_1h numeric;

-- ── macro_series_points ──────────────────────────────────────────────────────
alter table public.macro_series_points rename column series_key to series_code;
alter table public.macro_series_points alter column source drop not null;
alter table public.macro_series_points add column if not exists return_1d numeric;
alter table public.macro_series_points add column if not exists change_1d numeric;

-- ── watchlist_assets ─────────────────────────────────────────────────────────
alter table public.watchlist_assets add column if not exists sort_order integer not null default 0;

-- ── feature_values ───────────────────────────────────────────────────────────
alter table public.feature_values add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.feature_values rename column feature_key to feature_name;
alter table public.feature_values rename column as_of       to ts;
alter table public.feature_values rename column metadata    to meta;
alter table public.feature_values add column if not exists updated_at timestamptz not null default now();
-- Drop old constraint (column rename keeps OID, drop by auto-generated name).
alter table public.feature_values drop constraint if exists feature_values_asset_id_feature_key_as_of_key;
drop index if exists idx_feature_values_asset_key_asof;
-- Unique index for Phase 2 upserts (workspace_id always non-null in Phase 2).
create unique index if not exists uidx_feature_values_ws_asset_name_ts
  on public.feature_values(workspace_id, asset_id, feature_name, ts)
  where workspace_id is not null;
create index if not exists idx_feature_values_asset_name_ts
  on public.feature_values(asset_id, feature_name, ts desc);

-- ── signal_values ────────────────────────────────────────────────────────────
-- Drop FK to signal_registry before any structural changes.
alter table public.signal_values drop constraint if exists signal_values_signal_key_fkey;
alter table public.signal_values add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.signal_values rename column signal_key to signal_name;
alter table public.signal_values rename column as_of      to ts;
alter table public.signal_values rename column value      to score;
alter table public.signal_values drop   column if exists regime;
alter table public.signal_values add column if not exists updated_at timestamptz not null default now();
alter table public.signal_values drop constraint if exists signal_values_asset_id_signal_key_as_of_key;
drop index if exists idx_signal_values_asset_key_asof;
create unique index if not exists uidx_signal_values_ws_asset_name_ts
  on public.signal_values(workspace_id, asset_id, signal_name, ts)
  where workspace_id is not null;
create index if not exists idx_signal_values_asset_name_ts
  on public.signal_values(asset_id, signal_name, ts desc);

-- ── composite_scores ─────────────────────────────────────────────────────────
alter table public.composite_scores rename column as_of to timestamp;
alter table public.composite_scores add column if not exists confidence numeric;
-- Invalidators become a structured dict in Phase 2 (not an array).
alter table public.composite_scores alter column invalidators set default '{}'::jsonb;
alter table public.composite_scores drop constraint if exists composite_scores_asset_id_workspace_id_as_of_key;
drop index if exists idx_composite_scores_ws_asof;
create unique index if not exists uidx_composite_scores_ws_asset_ts
  on public.composite_scores(workspace_id, asset_id, timestamp);
create index if not exists idx_composite_scores_ws_ts
  on public.composite_scores(workspace_id, timestamp desc);
