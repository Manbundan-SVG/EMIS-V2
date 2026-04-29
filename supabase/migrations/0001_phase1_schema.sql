create extension if not exists pgcrypto;

create table if not exists workspaces (
  id uuid primary key default gen_random_uuid(),
  slug text unique not null,
  name text not null,
  created_at timestamptz not null default now()
);

create table if not exists assets (
  id uuid primary key default gen_random_uuid(),
  symbol text unique not null,
  name text not null,
  asset_class text not null,
  is_active boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists watchlists (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references workspaces(id) on delete cascade,
  slug text not null,
  name text not null,
  created_at timestamptz not null default now(),
  unique (workspace_id, slug)
);

create table if not exists watchlist_assets (
  watchlist_id uuid not null references watchlists(id) on delete cascade,
  asset_id uuid not null references assets(id) on delete cascade,
  created_at timestamptz not null default now(),
  primary key (watchlist_id, asset_id)
);

create table if not exists model_versions (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references workspaces(id) on delete cascade,
  model_key text not null,
  version text not null,
  status text not null default 'active',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (workspace_id, model_key, version)
);

create table if not exists signal_registry (
  key text primary key,
  family text not null,
  horizon text not null,
  description text not null,
  inputs jsonb not null default '[]'::jsonb,
  outputs jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists market_bars (
  id uuid primary key default gen_random_uuid(),
  asset_id uuid not null references assets(id) on delete cascade,
  timeframe text not null,
  ts timestamptz not null,
  open numeric not null,
  high numeric not null,
  low numeric not null,
  close numeric not null,
  volume numeric,
  source text not null,
  created_at timestamptz not null default now(),
  unique (asset_id, timeframe, ts, source)
);
create index if not exists idx_market_bars_asset_tf_ts on market_bars(asset_id, timeframe, ts desc);

create table if not exists market_open_interest (
  id uuid primary key default gen_random_uuid(),
  asset_id uuid not null references assets(id) on delete cascade,
  ts timestamptz not null,
  open_interest numeric not null,
  source text not null,
  created_at timestamptz not null default now(),
  unique (asset_id, ts, source)
);
create index if not exists idx_oi_asset_ts on market_open_interest(asset_id, ts desc);

create table if not exists market_funding (
  id uuid primary key default gen_random_uuid(),
  asset_id uuid not null references assets(id) on delete cascade,
  ts timestamptz not null,
  funding_rate numeric not null,
  source text not null,
  created_at timestamptz not null default now(),
  unique (asset_id, ts, source)
);
create index if not exists idx_funding_asset_ts on market_funding(asset_id, ts desc);

create table if not exists market_liquidations (
  id uuid primary key default gen_random_uuid(),
  asset_id uuid not null references assets(id) on delete cascade,
  ts timestamptz not null,
  side text not null,
  notional_usd numeric,
  reference_price numeric,
  source text not null,
  created_at timestamptz not null default now()
);
create index if not exists idx_liquidations_asset_ts on market_liquidations(asset_id, ts desc);

create table if not exists macro_series_points (
  id uuid primary key default gen_random_uuid(),
  series_key text not null,
  ts timestamptz not null,
  value numeric not null,
  source text not null,
  created_at timestamptz not null default now(),
  unique (series_key, ts, source)
);
create index if not exists idx_macro_series_key_ts on macro_series_points(series_key, ts desc);

create table if not exists feature_values (
  id uuid primary key default gen_random_uuid(),
  asset_id uuid not null references assets(id) on delete cascade,
  feature_key text not null,
  as_of timestamptz not null,
  value numeric not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (asset_id, feature_key, as_of)
);
create index if not exists idx_feature_values_asset_key_asof on feature_values(asset_id, feature_key, as_of desc);

create table if not exists signal_values (
  id uuid primary key default gen_random_uuid(),
  asset_id uuid not null references assets(id) on delete cascade,
  signal_key text not null references signal_registry(key),
  as_of timestamptz not null,
  value numeric not null,
  regime text not null,
  explanation jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (asset_id, signal_key, as_of)
);
create index if not exists idx_signal_values_asset_key_asof on signal_values(asset_id, signal_key, as_of desc);

create table if not exists composite_scores (
  id uuid primary key default gen_random_uuid(),
  asset_id uuid not null references assets(id) on delete cascade,
  workspace_id uuid not null references workspaces(id) on delete cascade,
  as_of timestamptz not null,
  long_score numeric not null,
  short_score numeric not null,
  regime text not null,
  invalidators jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  unique (asset_id, workspace_id, as_of)
);
create index if not exists idx_composite_scores_ws_asof on composite_scores(workspace_id, as_of desc);

insert into workspaces (slug, name)
values ('default', 'Default Workspace')
on conflict (slug) do nothing;

insert into assets (symbol, name, asset_class)
values
  ('BTC', 'Bitcoin', 'crypto'),
  ('ETH', 'Ethereum', 'crypto'),
  ('SOL', 'Solana', 'crypto'),
  ('XRP', 'XRP', 'crypto'),
  ('SPY', 'SPDR S&P 500 ETF Trust', 'equity_index_proxy'),
  ('QQQ', 'Invesco QQQ Trust', 'equity_index_proxy'),
  ('DXY', 'US Dollar Index', 'fx_index'),
  ('US02Y', 'US 2Y Yield', 'rates'),
  ('US10Y', 'US 10Y Yield', 'rates'),
  ('GLD', 'SPDR Gold Shares', 'commodity_proxy')
on conflict (symbol) do nothing;

insert into signal_registry (key, family, horizon, description, inputs, outputs)
values
  ('trend_structure', 'trend', 'swing', 'Trend persistence and breakout quality.', '["market_bars"]'::jsonb, '["structure_score","trend_regime","breakout_quality"]'::jsonb),
  ('oi_price_divergence', 'leverage', 'intraday', 'Price vs open interest divergence.', '["market_bars","market_open_interest"]'::jsonb, '["crowdedness_score","squeeze_probability"]'::jsonb),
  ('funding_stress', 'leverage', 'intraday', 'Funding and basis stress.', '["market_funding"]'::jsonb, '["funding_stress_score"]'::jsonb),
  ('liquidation_magnet_distance', 'liquidation', 'intraday', 'Distance to liquidation clusters.', '["market_bars","market_liquidations"]'::jsonb, '["liq_magnet_proximity"]'::jsonb),
  ('macro_alignment', 'macro', 'daily', 'Alignment with DXY, rates, and equity risk.', '["market_bars","macro_series_points"]'::jsonb, '["macro_alignment_score"]'::jsonb)
on conflict (key) do nothing;
