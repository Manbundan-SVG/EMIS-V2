-- Phase 4.2B: Family-Level Lead/Lag Attribution
-- Additive timing-aware refinement layer on top of 4.1A/B/C attribution and
-- 4.2A lead/lag timing measurements. Applies profile-driven per-timing-class
-- weights to family and symbol attribution without modifying any upstream
-- attribution layer.

begin;

-- ── A. Timing Attribution Profiles ──────────────────────────────────────
-- One active profile per workspace. Partial UNIQUE index enforces the
-- "at most one active" constraint.

CREATE TABLE IF NOT EXISTS cross_asset_timing_attribution_profiles (
    id                             uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                   uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                   text        NOT NULL,
    is_active                      boolean     NOT NULL DEFAULT true,
    lead_weight                    numeric     NOT NULL DEFAULT 1.10,
    coincident_weight              numeric     NOT NULL DEFAULT 1.00,
    lag_weight                     numeric     NOT NULL DEFAULT 0.85,
    insufficient_data_weight       numeric     NOT NULL DEFAULT 0.75,
    lead_bonus_scale               numeric     NOT NULL DEFAULT 1.0,
    lag_penalty_scale              numeric     NOT NULL DEFAULT 1.0,
    family_weight_overrides        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    metadata                       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_timing_attribution_profiles_scope_idx
    ON cross_asset_timing_attribution_profiles (workspace_id, is_active, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS cross_asset_timing_attribution_profiles_active_unique_idx
    ON cross_asset_timing_attribution_profiles (workspace_id)
    WHERE is_active;

-- ── B. Timing-Aware Family Attribution Snapshots ────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_timing_attribution_snapshots (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                        uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                              uuid        NOT NULL,
    context_snapshot_id                 uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    timing_profile_id                   uuid        REFERENCES cross_asset_timing_attribution_profiles(id) ON DELETE SET NULL,
    dependency_family                   text        NOT NULL,
    raw_family_net_contribution         numeric,
    weighted_family_net_contribution    numeric,
    regime_adjusted_family_contribution numeric,
    dominant_timing_class               text        NOT NULL DEFAULT 'insufficient_data'
                                                    CHECK (dominant_timing_class IN ('lead','coincident','lag','insufficient_data')),
    lead_pair_count                     integer     NOT NULL DEFAULT 0,
    coincident_pair_count               integer     NOT NULL DEFAULT 0,
    lag_pair_count                      integer     NOT NULL DEFAULT 0,
    timing_class_weight                 numeric,
    timing_bonus                        numeric,
    timing_penalty                      numeric,
    timing_adjusted_family_contribution numeric,
    timing_family_rank                  integer,
    top_leading_symbols                 jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_attribution_scope_time_idx
    ON cross_asset_family_timing_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_attribution_run_idx
    ON cross_asset_family_timing_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_attribution_family_idx
    ON cross_asset_family_timing_attribution_snapshots (dependency_family);

-- ── C. Timing-Aware Symbol Attribution Snapshots ────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_symbol_timing_attribution_snapshots (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                        uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                              uuid        NOT NULL,
    context_snapshot_id                 uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    timing_profile_id                   uuid        REFERENCES cross_asset_timing_attribution_profiles(id) ON DELETE SET NULL,
    symbol                              text        NOT NULL,
    dependency_family                   text        NOT NULL,
    dependency_type                     text,
    lag_bucket                          text        NOT NULL DEFAULT 'insufficient_data'
                                                    CHECK (lag_bucket IN ('lead','coincident','lag','insufficient_data')),
    best_lag_hours                      integer,
    raw_symbol_score                    numeric,
    weighted_symbol_score               numeric,
    regime_adjusted_symbol_score        numeric,
    timing_class_weight                 numeric,
    timing_adjusted_symbol_score        numeric,
    symbol_rank                         integer,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_timing_attribution_scope_time_idx
    ON cross_asset_symbol_timing_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_timing_attribution_run_idx
    ON cross_asset_symbol_timing_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_timing_attribution_symbol_idx
    ON cross_asset_symbol_timing_attribution_snapshots (symbol);

-- ── D. Timing-Aware Family Attribution Summary view ─────────────────────

CREATE OR REPLACE VIEW cross_asset_family_timing_attribution_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_timing_attribution_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    raw_family_net_contribution,
    weighted_family_net_contribution,
    regime_adjusted_family_contribution,
    dominant_timing_class,
    lead_pair_count,
    coincident_pair_count,
    lag_pair_count,
    timing_class_weight,
    timing_bonus,
    timing_penalty,
    timing_adjusted_family_contribution,
    timing_family_rank,
    top_leading_symbols,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Timing-Aware Symbol Attribution Summary view ─────────────────────

CREATE OR REPLACE VIEW cross_asset_symbol_timing_attribution_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.run_id, s.symbol
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_symbol_timing_attribution_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    symbol,
    dependency_family,
    dependency_type,
    lag_bucket,
    best_lag_hours,
    raw_symbol_score,
    weighted_symbol_score,
    regime_adjusted_symbol_score,
    timing_class_weight,
    timing_adjusted_symbol_score,
    symbol_rank,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Timing-Aware Integration Bridge view ─────────────────────────────
-- Joins raw / weighted / regime / timing aggregates per run so inspection
-- can compare dominant family choices across all four layers.

CREATE OR REPLACE VIEW run_cross_asset_timing_attribution_summary AS
WITH timing_totals AS (
    SELECT
        run_id,
        workspace_id,
        watchlist_id,
        context_snapshot_id,
        sum(timing_adjusted_family_contribution)::numeric
            AS timing_adjusted_cross_asset_contribution
    FROM cross_asset_family_timing_attribution_summary
    GROUP BY run_id, workspace_id, watchlist_id, context_snapshot_id
),
timing_dominant AS (
    SELECT DISTINCT ON (run_id)
        run_id,
        dependency_family AS timing_dominant_dependency_family
    FROM cross_asset_family_timing_attribution_summary
    WHERE timing_family_rank = 1
    ORDER BY run_id, created_at DESC
)
SELECT
    a.run_id,
    a.workspace_id,
    a.watchlist_id,
    a.context_snapshot_id,
    a.cross_asset_net_contribution,
    w.weighted_cross_asset_net_contribution,
    r.regime_adjusted_cross_asset_contribution,
    tt.timing_adjusted_cross_asset_contribution,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    r.regime_dominant_dependency_family,
    td.timing_dominant_dependency_family,
    a.created_at
FROM cross_asset_attribution_summary a
LEFT JOIN run_cross_asset_weighted_integration_summary w
    ON w.run_id = a.run_id
LEFT JOIN run_cross_asset_regime_integration_summary r
    ON r.run_id = a.run_id
LEFT JOIN run_cross_asset_explanation_bridge bridge
    ON bridge.run_id = a.run_id
LEFT JOIN timing_totals   tt ON tt.run_id = a.run_id
LEFT JOIN timing_dominant td ON td.run_id = a.run_id;

commit;
