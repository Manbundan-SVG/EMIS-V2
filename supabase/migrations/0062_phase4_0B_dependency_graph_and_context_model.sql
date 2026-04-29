-- Phase 4.0B: Dependency Graph + Context Model
-- Additive extension of the 4.0A multi-asset foundation. Introduces durable
-- asset-to-asset dependency relationships, per-watchlist dependency profiles,
-- canonical family mappings, and reproducible context snapshots. Does not
-- modify existing tables or the 4.0A views.

begin;

-- ── A. Asset Dependency Graph ────────────────────────────────────────────
-- Global, workspace-agnostic. Defines explicit directed relationships from
-- one canonical symbol to another. `weight` is structural, not yet a signal
-- weight — it is used for ranking during context assembly.

CREATE TABLE IF NOT EXISTS asset_dependency_graph (
    id                uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    from_symbol       text        NOT NULL,
    to_symbol         text        NOT NULL,
    dependency_type   text        NOT NULL CHECK (dependency_type IN (
                          'macro_driver','risk_proxy','liquidity_proxy','sector_proxy',
                          'beta_proxy','commodity_link','fx_link','rates_link',
                          'index_confirmation','crypto_cross','custom'
                      )),
    dependency_family text        NOT NULL CHECK (dependency_family IN (
                          'macro','fx','rates','equity_index','commodity','crypto_cross','risk'
                      )),
    priority          integer     NOT NULL DEFAULT 100,
    weight            numeric     NOT NULL DEFAULT 1.0,
    is_active         boolean     NOT NULL DEFAULT true,
    metadata          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT asset_dependency_graph_not_self CHECK (from_symbol <> to_symbol)
);

CREATE UNIQUE INDEX IF NOT EXISTS asset_dependency_graph_unique_edge_idx
    ON asset_dependency_graph (from_symbol, to_symbol, dependency_type);

CREATE INDEX IF NOT EXISTS asset_dependency_graph_from_idx
    ON asset_dependency_graph (from_symbol, is_active, priority DESC);

CREATE INDEX IF NOT EXISTS asset_dependency_graph_to_idx
    ON asset_dependency_graph (to_symbol, is_active);

-- ── B. Watchlist Dependency Profile ──────────────────────────────────────
-- One active profile per (workspace_id, watchlist_id). History preserved via
-- soft is_active flag; promotion to a new profile keeps previous rows around.

CREATE TABLE IF NOT EXISTS watchlist_dependency_profiles (
    id                     uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id           uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id           uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    profile_name           text        NOT NULL,
    include_macro          boolean     NOT NULL DEFAULT true,
    include_fx             boolean     NOT NULL DEFAULT true,
    include_rates          boolean     NOT NULL DEFAULT true,
    include_equity_index   boolean     NOT NULL DEFAULT true,
    include_commodity      boolean     NOT NULL DEFAULT true,
    include_crypto_cross   boolean     NOT NULL DEFAULT true,
    max_dependencies       integer     NOT NULL DEFAULT 25,
    is_active              boolean     NOT NULL DEFAULT true,
    metadata               jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS watchlist_dependency_profiles_scope_idx
    ON watchlist_dependency_profiles (workspace_id, watchlist_id, is_active);

-- Only one active profile per (workspace, watchlist)
CREATE UNIQUE INDEX IF NOT EXISTS watchlist_dependency_profiles_active_unique_idx
    ON watchlist_dependency_profiles (workspace_id, watchlist_id)
    WHERE is_active;

-- ── C. Asset Family Mappings ─────────────────────────────────────────────
-- Canonical per-symbol family identity. Distinct from asset_universe_catalog:
-- a symbol may belong to multiple family keys (e.g. SPY → us_equity_index AND
-- broad_market), so the unique key is (symbol, family_key).

CREATE TABLE IF NOT EXISTS asset_family_mappings (
    id             uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    symbol         text        NOT NULL,
    asset_class    text        NOT NULL,
    family_key     text        NOT NULL,
    family_label   text        NOT NULL,
    region         text,
    is_active      boolean     NOT NULL DEFAULT true,
    metadata       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS asset_family_mappings_symbol_family_idx
    ON asset_family_mappings (symbol, family_key);

CREATE INDEX IF NOT EXISTS asset_family_mappings_class_active_idx
    ON asset_family_mappings (asset_class, is_active);

-- ── D. Watchlist Context Snapshots ───────────────────────────────────────
-- Point-in-time dependency context assembled for a watchlist. Immutable.
-- context_hash is SHA256 over sorted inputs; reproducible runs produce
-- identical hashes so dedupe/lineage is cheap.

CREATE TABLE IF NOT EXISTS watchlist_context_snapshots (
    id                    uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id          uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id          uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    profile_id            uuid        REFERENCES watchlist_dependency_profiles(id) ON DELETE SET NULL,
    snapshot_at           timestamptz NOT NULL DEFAULT now(),
    primary_symbols       jsonb       NOT NULL DEFAULT '[]'::jsonb,
    dependency_symbols    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    dependency_families   jsonb       NOT NULL DEFAULT '[]'::jsonb,
    context_hash          text        NOT NULL,
    coverage_summary      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    metadata              jsonb       NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS watchlist_context_snapshots_scope_time_idx
    ON watchlist_context_snapshots (workspace_id, watchlist_id, snapshot_at DESC);

CREATE INDEX IF NOT EXISTS watchlist_context_snapshots_hash_idx
    ON watchlist_context_snapshots (context_hash);

-- ── Seed: family mappings for the 20 symbols introduced in 4.0A ─────────

INSERT INTO asset_family_mappings (symbol, asset_class, family_key, family_label, region, metadata)
VALUES
    -- Crypto
    ('BTC',    'crypto',      'crypto_major',               'Crypto Major',                   'global', '{"tier":"top_2"}'::jsonb),
    ('ETH',    'crypto',      'crypto_major',               'Crypto Major',                   'global', '{"tier":"top_2"}'::jsonb),
    ('SOL',    'crypto',      'crypto_layer1',              'Crypto Layer 1',                 'global', '{}'::jsonb),
    ('XRP',    'crypto',      'crypto_altcoin',             'Crypto Altcoin',                 'global', '{}'::jsonb),
    -- Equity / index
    ('SPY',    'index',       'us_equity_index_broad',      'US Broad Equity Index',          'us',     '{"benchmark":"sp500"}'::jsonb),
    ('QQQ',    'index',       'us_equity_index_growth',     'US Growth Equity Index',         'us',     '{"benchmark":"nasdaq100"}'::jsonb),
    ('DIA',    'index',       'us_equity_index_bluechip',   'US Blue-Chip Equity Index',      'us',     '{"benchmark":"dow30"}'::jsonb),
    ('IWM',    'index',       'us_equity_index_smallcap',   'US Small-Cap Equity Index',      'us',     '{"benchmark":"russell2000"}'::jsonb),
    -- Commodity proxies
    ('GLD',    'commodity',   'commodity_precious_metals',  'Precious Metals (Gold)',         'global', '{"underlying":"gold"}'::jsonb),
    ('USO',    'commodity',   'commodity_energy',           'Energy (Crude Oil)',             'global', '{"underlying":"wti_crude"}'::jsonb),
    -- FX majors
    ('EURUSD', 'fx',          'fx_major',                   'FX Major',                       'global', '{}'::jsonb),
    ('USDJPY', 'fx',          'fx_major',                   'FX Major',                       'global', '{}'::jsonb),
    ('GBPUSD', 'fx',          'fx_major',                   'FX Major',                       'global', '{}'::jsonb),
    ('USDCHF', 'fx',          'fx_major',                   'FX Major',                       'global', '{}'::jsonb),
    ('USDCAD', 'fx',          'fx_major',                   'FX Major',                       'global', '{}'::jsonb),
    ('AUDUSD', 'fx',          'fx_major',                   'FX Major',                       'global', '{}'::jsonb),
    -- Rates
    ('US02Y',  'rates',       'rates_us_short',             'US Short-End Treasury',          'us',     '{"tenor_years":2}'::jsonb),
    ('US10Y',  'rates',       'rates_us_long',              'US Long-End Treasury',           'us',     '{"tenor_years":10}'::jsonb),
    -- Macro proxies
    ('DXY',    'macro_proxy', 'macro_dxy_basket',           'US Dollar Index Basket',         'global', '{"proxy_type":"dxy_basket"}'::jsonb),
    ('2S10S',  'macro_proxy', 'macro_yield_spread',         'US 2s10s Yield Spread',          'us',     '{"proxy_type":"yield_spread"}'::jsonb)
ON CONFLICT (symbol, family_key) DO UPDATE SET
    family_label = EXCLUDED.family_label,
    region       = EXCLUDED.region,
    is_active    = EXCLUDED.is_active,
    metadata     = asset_family_mappings.metadata || EXCLUDED.metadata;

-- ── Seed: initial dependency graph ──────────────────────────────────────
-- Conservative starting set. Direct/one-hop relationships only. Priorities
-- descend 100→80 for less critical dependencies; weights are structural.

INSERT INTO asset_dependency_graph
    (from_symbol, to_symbol, dependency_type, dependency_family, priority, weight, metadata)
VALUES
    -- BTC dependencies
    ('BTC',  'SPY',   'risk_proxy',         'risk',         100, 1.0, '{"rationale":"risk_on_off_correlation"}'::jsonb),
    ('BTC',  'QQQ',   'risk_proxy',         'risk',          95, 1.0, '{"rationale":"tech_risk_correlation"}'::jsonb),
    ('BTC',  'DXY',   'fx_link',            'fx',            90, 1.0, '{"rationale":"inverse_dollar_correlation"}'::jsonb),
    ('BTC',  'US10Y', 'rates_link',         'rates',         85, 1.0, '{"rationale":"duration_sensitivity"}'::jsonb),
    ('BTC',  'GLD',   'commodity_link',     'commodity',     80, 1.0, '{"rationale":"store_of_value_peer"}'::jsonb),
    -- ETH dependencies
    ('ETH',  'BTC',   'crypto_cross',       'crypto_cross', 100, 1.0, '{"rationale":"crypto_beta"}'::jsonb),
    ('ETH',  'QQQ',   'risk_proxy',         'risk',          90, 1.0, '{"rationale":"tech_risk_correlation"}'::jsonb),
    ('ETH',  'US10Y', 'rates_link',         'rates',         80, 1.0, '{"rationale":"duration_sensitivity"}'::jsonb),
    -- SOL dependencies
    ('SOL',  'BTC',   'crypto_cross',       'crypto_cross', 100, 1.0, '{"rationale":"crypto_beta"}'::jsonb),
    ('SOL',  'ETH',   'crypto_cross',       'crypto_cross',  95, 1.0, '{"rationale":"layer1_peer"}'::jsonb),
    ('SOL',  'QQQ',   'risk_proxy',         'risk',          80, 1.0, '{"rationale":"tech_risk_correlation"}'::jsonb),
    -- XRP dependencies
    ('XRP',  'BTC',   'crypto_cross',       'crypto_cross', 100, 1.0, '{"rationale":"crypto_beta"}'::jsonb),
    -- SPY dependencies
    ('SPY',  'US10Y', 'rates_link',         'rates',         95, 1.0, '{"rationale":"discount_rate_sensitivity"}'::jsonb),
    ('SPY',  'DXY',   'fx_link',            'fx',            85, 1.0, '{"rationale":"dollar_earnings_impact"}'::jsonb),
    ('SPY',  'GLD',   'commodity_link',     'commodity',     70, 1.0, '{"rationale":"risk_off_hedge"}'::jsonb),
    -- QQQ dependencies
    ('QQQ',  'US10Y', 'rates_link',         'rates',        100, 1.0, '{"rationale":"growth_duration_sensitivity"}'::jsonb),
    ('QQQ',  'SPY',   'index_confirmation', 'equity_index',  90, 1.0, '{"rationale":"broad_market_confirmation"}'::jsonb)
ON CONFLICT (from_symbol, to_symbol, dependency_type) DO UPDATE SET
    dependency_family = EXCLUDED.dependency_family,
    priority          = EXCLUDED.priority,
    weight            = EXCLUDED.weight,
    is_active         = EXCLUDED.is_active,
    metadata          = asset_dependency_graph.metadata || EXCLUDED.metadata;

-- ── E. Dependency Coverage Summary view ─────────────────────────────────
-- Latest context snapshot per (workspace, watchlist), joined against
-- normalized_multi_asset_market_state for freshness/missing detection.
-- Freshness thresholds: macro/fx/rates = 72h; bar-backed = 48h.

CREATE OR REPLACE VIEW watchlist_dependency_coverage_summary AS
WITH latest_snapshot AS (
    SELECT DISTINCT ON (workspace_id, watchlist_id)
        id,
        workspace_id,
        watchlist_id,
        context_hash,
        snapshot_at,
        primary_symbols,
        dependency_symbols,
        dependency_families
    FROM watchlist_context_snapshots
    ORDER BY workspace_id, watchlist_id, snapshot_at DESC
),
dep_rows AS (
    SELECT
        ls.id            AS snapshot_id,
        ls.workspace_id,
        jsonb_array_elements_text(ls.dependency_symbols) AS symbol
    FROM latest_snapshot ls
    WHERE jsonb_array_length(ls.dependency_symbols) > 0
),
dep_status AS (
    SELECT
        d.snapshot_id,
        (ms.price IS NULL)            AS is_missing,
        CASE
            WHEN ms.price_timestamp IS NULL THEN false
            WHEN ms.asset_class IN ('fx','rates','macro_proxy')
                 AND ms.price_timestamp < now() - interval '72 hours' THEN true
            WHEN ms.price_timestamp < now() - interval '48 hours' THEN true
            ELSE false
        END                           AS is_stale
    FROM dep_rows d
    LEFT JOIN normalized_multi_asset_market_state ms
        ON ms.workspace_id = d.workspace_id
       AND ms.canonical_symbol = d.symbol
),
dep_counts AS (
    SELECT
        snapshot_id,
        count(*) FILTER (WHERE NOT is_missing AND NOT is_stale)::int AS covered,
        count(*) FILTER (WHERE is_missing)::int                      AS missing,
        count(*) FILTER (WHERE is_stale AND NOT is_missing)::int     AS stale
    FROM dep_status
    GROUP BY snapshot_id
)
SELECT
    ls.workspace_id,
    ls.watchlist_id,
    ls.context_hash,
    jsonb_array_length(ls.primary_symbols)     AS primary_symbol_count,
    jsonb_array_length(ls.dependency_symbols)  AS dependency_symbol_count,
    jsonb_array_length(ls.dependency_families) AS dependency_family_count,
    COALESCE(dc.covered, 0)                    AS covered_dependency_count,
    COALESCE(dc.missing, 0)                    AS missing_dependency_count,
    COALESCE(dc.stale, 0)                      AS stale_dependency_count,
    ls.snapshot_at                             AS latest_context_snapshot_at,
    CASE
        WHEN jsonb_array_length(ls.dependency_symbols) = 0 THEN NULL
        ELSE COALESCE(dc.covered, 0)::numeric
             / jsonb_array_length(ls.dependency_symbols)::numeric
    END                                        AS coverage_ratio,
    jsonb_build_object(
        'snapshot_id',           ls.id,
        'snapshot_age_seconds',  EXTRACT(EPOCH FROM (now() - ls.snapshot_at))::int
    )                                          AS metadata
FROM latest_snapshot ls
LEFT JOIN dep_counts dc ON dc.snapshot_id = ls.id;

-- ── F. Dependency Context Detail view ───────────────────────────────────
-- One row per symbol in the latest context (primary + dependencies), enriched
-- with asset class, family, and freshness flags. A symbol's dependency_type
-- is the highest-priority graph edge pointing to it; for primary symbols it
-- is null.

CREATE OR REPLACE VIEW watchlist_dependency_context_detail AS
WITH latest_snapshot AS (
    SELECT DISTINCT ON (workspace_id, watchlist_id)
        id,
        workspace_id,
        watchlist_id,
        context_hash,
        snapshot_at,
        primary_symbols,
        dependency_symbols
    FROM watchlist_context_snapshots
    ORDER BY workspace_id, watchlist_id, snapshot_at DESC
),
primary_rows AS (
    SELECT
        ls.workspace_id,
        ls.watchlist_id,
        ls.context_hash,
        ls.snapshot_at,
        jsonb_array_elements_text(ls.primary_symbols) AS symbol,
        true                                          AS is_primary
    FROM latest_snapshot ls
    WHERE jsonb_array_length(ls.primary_symbols) > 0
),
dependency_rows AS (
    SELECT
        ls.workspace_id,
        ls.watchlist_id,
        ls.context_hash,
        ls.snapshot_at,
        jsonb_array_elements_text(ls.dependency_symbols) AS symbol,
        false                                            AS is_primary
    FROM latest_snapshot ls
    WHERE jsonb_array_length(ls.dependency_symbols) > 0
),
all_rows AS (
    SELECT * FROM primary_rows
    UNION ALL
    SELECT * FROM dependency_rows
)
SELECT
    r.workspace_id,
    r.watchlist_id,
    r.context_hash,
    r.symbol,
    ms.asset_class,
    COALESCE(adg.dependency_family, afm.family_key, 'unknown') AS dependency_family,
    adg.dependency_type,
    adg.priority,
    adg.weight,
    r.is_primary,
    ms.price_timestamp                                         AS latest_timestamp,
    (ms.price IS NULL)                                         AS is_missing,
    CASE
        WHEN ms.price_timestamp IS NULL THEN false
        WHEN ms.asset_class IN ('fx','rates','macro_proxy')
             AND ms.price_timestamp < now() - interval '72 hours' THEN true
        WHEN ms.price_timestamp < now() - interval '48 hours' THEN true
        ELSE false
    END                                                        AS is_stale,
    jsonb_build_object(
        'snapshot_at',   r.snapshot_at,
        'family_label',  afm.family_label,
        'edge_rationale', adg.metadata->>'rationale'
    )                                                          AS metadata
FROM all_rows r
LEFT JOIN normalized_multi_asset_market_state ms
    ON ms.workspace_id    = r.workspace_id
   AND ms.canonical_symbol = r.symbol
LEFT JOIN asset_family_mappings afm
    ON afm.symbol = r.symbol
   AND afm.is_active
LEFT JOIN LATERAL (
    SELECT
        adg2.dependency_type,
        adg2.dependency_family,
        adg2.priority,
        adg2.weight,
        adg2.metadata
    FROM asset_dependency_graph adg2
    WHERE adg2.to_symbol = r.symbol
      AND adg2.is_active
      AND NOT r.is_primary
    ORDER BY adg2.priority DESC, adg2.weight DESC, adg2.from_symbol ASC
    LIMIT 1
) adg ON true;

-- ── G. Dependency Family State view ─────────────────────────────────────
-- Per-(workspace, watchlist, dependency_family) rollup of coverage. Uses the
-- detail view so freshness definitions stay consistent.

CREATE OR REPLACE VIEW watchlist_dependency_family_state AS
SELECT
    workspace_id,
    watchlist_id,
    context_hash,
    dependency_family,
    count(*)::int                                                   AS symbol_count,
    count(*) FILTER (WHERE NOT is_missing AND NOT is_stale)::int    AS covered_count,
    count(*) FILTER (WHERE is_missing)::int                         AS missing_count,
    count(*) FILTER (WHERE is_stale AND NOT is_missing)::int        AS stale_count,
    max(latest_timestamp)                                           AS latest_timestamp,
    jsonb_build_object(
        'symbols',  jsonb_agg(DISTINCT symbol ORDER BY symbol)
    )                                                               AS metadata
FROM watchlist_dependency_context_detail
WHERE NOT is_primary
GROUP BY workspace_id, watchlist_id, context_hash, dependency_family;

commit;
