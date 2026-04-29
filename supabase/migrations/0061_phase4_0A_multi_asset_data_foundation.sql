-- Phase 4.0A: Multi-Asset Data Foundation
-- Additive extension of the crypto-first live schema to support equities,
-- indices, FX, rates/macro proxies, and commodity proxies. Does not replace
-- or modify existing tables or market data paths.

begin;

-- ── A. Asset Universe Catalog ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS asset_universe_catalog (
    id               uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    symbol           text        NOT NULL,
    canonical_symbol text        NOT NULL,
    asset_class      text        NOT NULL CHECK (asset_class IN (
                         'crypto','equity','index','fx','rates','commodity','macro_proxy'
                     )),
    venue            text,
    quote_currency   text,
    base_currency    text,
    region           text,
    is_active        boolean     NOT NULL DEFAULT true,
    metadata         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS asset_universe_catalog_canonical_idx
    ON asset_universe_catalog (canonical_symbol, asset_class, coalesce(venue, ''));

CREATE INDEX IF NOT EXISTS asset_universe_catalog_class_active_idx
    ON asset_universe_catalog (asset_class, is_active);

-- Seed new assets into the existing assets table (idempotent)
INSERT INTO assets (symbol, name, asset_class)
VALUES
    ('IWM',   'iShares Russell 2000 ETF',             'equity_index_proxy'),
    ('DIA',   'SPDR Dow Jones Industrial Average ETF','equity_index_proxy'),
    ('US02Y', 'US 2Y Treasury Yield',                 'rates'),
    ('USO',   'United States Oil Fund',               'commodity_proxy')
ON CONFLICT (symbol) DO NOTHING;

-- Seed asset_universe_catalog. metadata->>'series_code' matches the
-- macro_series_points.series_code used by the macro-backed asset classes
-- (fx, rates, macro_proxy). For bar-backed classes (crypto, index, commodity)
-- the catalog row resolves through assets.symbol → market_bars.asset_id.
INSERT INTO asset_universe_catalog
    (symbol, canonical_symbol, asset_class, venue, quote_currency, base_currency, region, metadata)
VALUES
    -- Crypto (existing OKX/Binance path)
    ('BTCUSDT','BTC',   'crypto',      'okx',          'USDT','BTC', 'global','{"series_code":null}'::jsonb),
    ('ETHUSDT','ETH',   'crypto',      'okx',          'USDT','ETH', 'global','{"series_code":null}'::jsonb),
    ('SOLUSDT','SOL',   'crypto',      'okx',          'USDT','SOL', 'global','{"series_code":null}'::jsonb),
    ('XRPUSDT','XRP',   'crypto',      'okx',          'USDT','XRP', 'global','{"series_code":null}'::jsonb),
    -- Equities / Indices (via Alpaca snapshots)
    ('SPY',    'SPY',   'index',       'alpaca',       'USD', 'SPY', 'us',    '{"series_code":null}'::jsonb),
    ('QQQ',    'QQQ',   'index',       'alpaca',       'USD', 'QQQ', 'us',    '{"series_code":null}'::jsonb),
    ('DIA',    'DIA',   'index',       'alpaca',       'USD', 'DIA', 'us',    '{"series_code":null}'::jsonb),
    ('IWM',    'IWM',   'index',       'alpaca',       'USD', 'IWM', 'us',    '{"series_code":null}'::jsonb),
    -- Commodity proxies (via Alpaca snapshots)
    ('GLD',    'GLD',   'commodity',   'alpaca',       'USD', 'GLD', 'global','{"series_code":null,"underlying":"gold"}'::jsonb),
    ('USO',    'USO',   'commodity',   'alpaca',       'USD', 'USO', 'global','{"series_code":null,"underlying":"crude_oil"}'::jsonb),
    -- FX (via CurrencyAPI, stored in macro_series_points)
    ('EURUSD', 'EURUSD','fx',          'currency_api', 'USD', 'EUR', 'global','{"series_code":"EURUSD"}'::jsonb),
    ('USDJPY', 'USDJPY','fx',          'currency_api', 'JPY', 'USD', 'global','{"series_code":"USDJPY"}'::jsonb),
    ('GBPUSD', 'GBPUSD','fx',          'currency_api', 'USD', 'GBP', 'global','{"series_code":"GBPUSD"}'::jsonb),
    ('USDCHF', 'USDCHF','fx',          'currency_api', 'CHF', 'USD', 'global','{"series_code":"USDCHF"}'::jsonb),
    ('USDCAD', 'USDCAD','fx',          'currency_api', 'CAD', 'USD', 'global','{"series_code":"USDCAD"}'::jsonb),
    ('AUDUSD', 'AUDUSD','fx',          'currency_api', 'USD', 'AUD', 'global','{"series_code":"AUDUSD"}'::jsonb),
    -- Rates (via FRED, stored in macro_series_points)
    ('US10Y',  'US10Y', 'rates',       'fred',         'USD', null,  'us',    '{"series_code":"US10Y","fred_series_id":"DGS10"}'::jsonb),
    ('US02Y',  'US02Y', 'rates',       'fred',         'USD', null,  'us',    '{"series_code":"US02Y","fred_series_id":"DGS2"}'::jsonb),
    -- Macro proxies (via FRED, stored in macro_series_points)
    ('DXY',    'DXY',   'macro_proxy', 'fred',         'USD', null,  'global','{"series_code":"DXY","fred_series_id":"DTWEXBGS","proxy_type":"dxy_basket"}'::jsonb),
    ('2S10S',  '2S10S', 'macro_proxy', 'fred',         'USD', null,  'us',    '{"series_code":"2S10S","fred_series_id":"T10Y2Y","proxy_type":"yield_spread"}'::jsonb)
ON CONFLICT (canonical_symbol, asset_class, coalesce(venue, ''))
DO UPDATE SET
    is_active = EXCLUDED.is_active,
    metadata  = asset_universe_catalog.metadata || EXCLUDED.metadata;

-- ── B. Multi-Asset Sync Health Summary view ──────────────────────────────
-- Latest sync run per (workspace, source). asset_class is derived from
-- metadata first (future multi-asset runs), then falls back to mapping the
-- existing crypto_market_sync / macro_market_sync sources.

CREATE OR REPLACE VIEW multi_asset_sync_health_summary AS
WITH ranked AS (
    SELECT
        r.workspace_id,
        r.source,
        r.status,
        r.requested_symbols,
        r.synced_symbols,
        r.started_at,
        r.completed_at,
        r.metadata,
        COALESCE(
            r.metadata->>'asset_class',
            CASE r.source
                WHEN 'crypto_market_sync'      THEN 'crypto'
                WHEN 'macro_market_sync'       THEN 'rates'
                WHEN 'multi_asset_equity_sync' THEN 'index'
                WHEN 'multi_asset_fx_sync'     THEN 'fx'
                WHEN 'multi_asset_rates_sync'  THEN 'rates'
                ELSE 'unknown'
            END
        ) AS derived_asset_class,
        COALESCE(
            r.metadata->>'provider_family',
            r.metadata->>'provider_mode',
            r.source
        ) AS derived_provider_family,
        row_number() OVER (
            PARTITION BY r.workspace_id, r.source
            ORDER BY r.started_at DESC
        ) AS rn
    FROM market_data_sync_runs r
)
SELECT
    workspace_id,
    derived_provider_family                                                      AS provider_family,
    derived_asset_class                                                          AS asset_class,
    jsonb_array_length(COALESCE(requested_symbols, '[]'::jsonb))                 AS requested_symbol_count,
    jsonb_array_length(COALESCE(synced_symbols,    '[]'::jsonb))                 AS synced_symbol_count,
    GREATEST(0,
        jsonb_array_length(COALESCE(requested_symbols, '[]'::jsonb))
        - jsonb_array_length(COALESCE(synced_symbols,  '[]'::jsonb))
    )                                                                            AS failed_symbol_count,
    started_at                                                                   AS latest_run_started_at,
    completed_at                                                                 AS latest_run_completed_at,
    status                                                                       AS latest_status,
    COALESCE(metadata->>'provider_mode', derived_provider_family)                AS latest_provider_mode,
    metadata                                                                     AS latest_metadata
FROM ranked
WHERE rn = 1;

-- ── C. Normalized Multi-Asset Market State view ──────────────────────────
-- Unified per-symbol latest-state surface across all asset classes.
-- Bar-backed path:  asset_universe_catalog → assets → market_bars
-- Macro-backed path: asset_universe_catalog → macro_series_points (via series_code)
-- workspace_id is populated via CROSS JOIN with workspaces since the catalog
-- is global and market_bars / macro_series_points are not workspace-scoped.

CREATE OR REPLACE VIEW normalized_multi_asset_market_state AS
WITH latest_bars AS (
    SELECT DISTINCT ON (asset_id)
        asset_id,
        close   AS price,
        ts      AS price_timestamp,
        volume  AS volume_24h,
        source
    FROM market_bars
    ORDER BY asset_id, ts DESC
),
latest_macro AS (
    SELECT DISTINCT ON (series_code)
        series_code,
        value,
        ts,
        source
    FROM macro_series_points
    ORDER BY series_code, ts DESC
),
bar_state AS (
    SELECT
        w.id                                        AS workspace_id,
        auc.symbol,
        auc.canonical_symbol,
        auc.asset_class,
        COALESCE(lb.source, auc.venue)              AS provider_family,
        lb.price::numeric                           AS price,
        lb.price_timestamp,
        lb.volume_24h::numeric                      AS volume_24h,
        NULL::numeric                               AS oi_change_1h,
        NULL::numeric                               AS funding_rate,
        NULL::numeric                               AS yield_value,
        NULL::numeric                               AS fx_return_1d,
        NULL::numeric                               AS macro_proxy_value,
        NULL::bigint                                AS liquidation_count,
        auc.metadata
    FROM workspaces w
    CROSS JOIN asset_universe_catalog auc
    JOIN assets a   ON a.symbol = auc.canonical_symbol
    JOIN latest_bars lb ON lb.asset_id = a.id
    WHERE auc.is_active
      AND auc.asset_class IN ('crypto', 'equity', 'index', 'commodity')
),
macro_state AS (
    SELECT
        w.id                                        AS workspace_id,
        auc.symbol,
        auc.canonical_symbol,
        auc.asset_class,
        COALESCE(lm.source, auc.venue)              AS provider_family,
        lm.value::numeric                           AS price,
        lm.ts                                       AS price_timestamp,
        NULL::numeric                               AS volume_24h,
        NULL::numeric                               AS oi_change_1h,
        NULL::numeric                               AS funding_rate,
        CASE WHEN auc.asset_class = 'rates'
             THEN lm.value::numeric END             AS yield_value,
        CASE WHEN auc.asset_class = 'fx'
             THEN lm.value::numeric END             AS fx_return_1d,
        CASE WHEN auc.asset_class = 'macro_proxy'
             THEN lm.value::numeric END             AS macro_proxy_value,
        NULL::bigint                                AS liquidation_count,
        auc.metadata
    FROM workspaces w
    CROSS JOIN asset_universe_catalog auc
    JOIN latest_macro lm
        ON lm.series_code = auc.metadata->>'series_code'
    WHERE auc.is_active
      AND auc.asset_class IN ('fx', 'rates', 'macro_proxy')
      AND auc.metadata->>'series_code' IS NOT NULL
)
SELECT * FROM bar_state
UNION ALL
SELECT * FROM macro_state;

-- ── D. Multi-Asset Family State Summary view ─────────────────────────────
-- Lightweight per-workspace/asset_class aggregation. family_key currently
-- mirrors asset_class but remains distinct so future phases (4.0B+) can split
-- within a class (e.g. us_equity vs ex_us_equity) without a view rewrite.

CREATE OR REPLACE VIEW multi_asset_family_state_summary AS
SELECT
    ms.workspace_id,
    ms.asset_class,
    ms.asset_class                       AS family_key,
    count(*)::int                        AS symbol_count,
    max(ms.price_timestamp)              AS latest_timestamp,
    NULL::numeric                        AS avg_return_1d,
    NULL::numeric                        AS avg_volatility_proxy,
    jsonb_build_object(
        'provider_families',
        COALESCE(
            jsonb_agg(DISTINCT ms.provider_family) FILTER (WHERE ms.provider_family IS NOT NULL),
            '[]'::jsonb
        ),
        'symbol_list',
        COALESCE(
            jsonb_agg(DISTINCT ms.canonical_symbol),
            '[]'::jsonb
        )
    )                                    AS metadata
FROM normalized_multi_asset_market_state ms
GROUP BY ms.workspace_id, ms.asset_class;

commit;
