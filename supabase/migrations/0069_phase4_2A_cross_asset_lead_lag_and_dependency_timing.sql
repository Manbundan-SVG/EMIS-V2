-- Phase 4.2A: Cross-Asset Lead/Lag and Dependency Timing Model
-- Additive timing-measurement layer. Persists deterministic pairwise
-- lag/strength observations between base symbols and their dependencies,
-- plus per-family aggregates and a run-level timing summary. Descriptive
-- only — no forecasting or causality inference.

begin;

-- ── A. Lead/Lag Pair Snapshots ──────────────────────────────────────────
-- One row per (base_symbol, dependency_symbol) measurement emission.

CREATE TABLE IF NOT EXISTS cross_asset_lead_lag_pair_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                          uuid,
    context_snapshot_id             uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    base_symbol                     text        NOT NULL,
    dependency_symbol               text        NOT NULL,
    dependency_family               text        NOT NULL,
    dependency_type                 text,
    lag_bucket                      text        NOT NULL
                                                CHECK (lag_bucket IN ('lead','coincident','lag','insufficient_data')),
    best_lag_hours                  integer,
    timing_strength                 numeric,
    correlation_at_best_lag         numeric,
    base_return_series_key          text,
    dependency_return_series_key    text,
    window_label                    text        NOT NULL DEFAULT '7d',
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_lead_lag_pair_scope_time_idx
    ON cross_asset_lead_lag_pair_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_lead_lag_pair_run_idx
    ON cross_asset_lead_lag_pair_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_lead_lag_pair_symbols_idx
    ON cross_asset_lead_lag_pair_snapshots (base_symbol, dependency_symbol);

-- ── B. Family Timing Snapshots ──────────────────────────────────────────
-- One row per (watchlist, run, dependency_family) timing aggregate.

CREATE TABLE IF NOT EXISTS cross_asset_family_timing_snapshots (
    id                          uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                      uuid,
    context_snapshot_id         uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dependency_family           text        NOT NULL,
    lead_pair_count             integer     NOT NULL DEFAULT 0,
    coincident_pair_count       integer     NOT NULL DEFAULT 0,
    lag_pair_count              integer     NOT NULL DEFAULT 0,
    avg_best_lag_hours          numeric,
    avg_timing_strength         numeric,
    dominant_timing_class       text        NOT NULL DEFAULT 'insufficient_data'
                                            CHECK (dominant_timing_class IN ('lead','coincident','lag','insufficient_data')),
    top_leading_symbols         jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_scope_time_idx
    ON cross_asset_family_timing_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_run_idx
    ON cross_asset_family_timing_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_family_idx
    ON cross_asset_family_timing_snapshots (dependency_family);

-- ── C. Pair Summary view ────────────────────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_lead_lag_pair_summary AS
WITH ranked AS (
    SELECT
        p.*,
        row_number() OVER (
            PARTITION BY COALESCE(p.run_id::text, '__none__'),
                         p.base_symbol, p.dependency_symbol
            ORDER BY p.created_at DESC
        ) AS rn
    FROM cross_asset_lead_lag_pair_snapshots p
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    base_symbol,
    dependency_symbol,
    dependency_family,
    dependency_type,
    lag_bucket,
    best_lag_hours,
    timing_strength,
    correlation_at_best_lag,
    window_label,
    created_at
FROM ranked
WHERE rn = 1;

-- ── D. Family Timing Summary view ───────────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_timing_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY COALESCE(f.run_id::text, '__none__'), f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_timing_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    lead_pair_count,
    coincident_pair_count,
    lag_pair_count,
    avg_best_lag_hours,
    avg_timing_strength,
    dominant_timing_class,
    top_leading_symbols,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Run Timing Summary view ──────────────────────────────────────────
-- One row per (run, watchlist) with aggregate counts, dominant leading
-- family, and strongest leading symbol. dominant_leading_family ranks
-- families by lead_pair_count desc → avg_timing_strength desc → family name.

CREATE OR REPLACE VIEW run_cross_asset_timing_summary AS
WITH family_ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY COALESCE(f.run_id::text, '__none__')
            ORDER BY
                f.lead_pair_count DESC,
                f.avg_timing_strength DESC NULLS LAST,
                f.dependency_family ASC
        ) AS family_rn
    FROM cross_asset_family_timing_summary f
),
dominant_family AS (
    SELECT run_id, workspace_id, watchlist_id, context_snapshot_id,
           dependency_family AS dominant_leading_family,
           top_leading_symbols
    FROM family_ranked
    WHERE family_rn = 1
),
strongest_symbol AS (
    SELECT DISTINCT ON (COALESCE(p.run_id::text, '__none__'))
        p.run_id, p.dependency_symbol AS strongest_leading_symbol
    FROM cross_asset_lead_lag_pair_summary p
    WHERE p.lag_bucket = 'lead'
      AND p.timing_strength IS NOT NULL
    ORDER BY COALESCE(p.run_id::text, '__none__'), p.timing_strength DESC, p.dependency_symbol ASC
),
run_counts AS (
    SELECT
        run_id, workspace_id, watchlist_id, context_snapshot_id,
        sum(lead_pair_count)::int                AS lead_pair_count,
        sum(coincident_pair_count)::int          AS coincident_pair_count,
        sum(lag_pair_count)::int                 AS lag_pair_count,
        avg(avg_timing_strength)::numeric        AS avg_timing_strength,
        max(created_at)                          AS created_at
    FROM cross_asset_family_timing_summary
    GROUP BY run_id, workspace_id, watchlist_id, context_snapshot_id
)
SELECT
    rc.run_id,
    rc.workspace_id,
    rc.watchlist_id,
    rc.context_snapshot_id,
    rc.lead_pair_count,
    rc.coincident_pair_count,
    rc.lag_pair_count,
    df.dominant_leading_family,
    ss.strongest_leading_symbol,
    rc.avg_timing_strength,
    rc.created_at
FROM run_counts rc
LEFT JOIN dominant_family   df ON df.run_id IS NOT DISTINCT FROM rc.run_id
LEFT JOIN strongest_symbol  ss ON ss.run_id IS NOT DISTINCT FROM rc.run_id
WHERE rc.run_id IS NOT NULL;

commit;
