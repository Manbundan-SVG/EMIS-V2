-- Phase 4.1C: Regime-Aware Cross-Asset Interpretation
-- Additive refinement layer on top of 4.1B weighted attribution. Conditions
-- family/symbol contribution on the active regime (from 2.5D
-- regime_transition_events) using a profile table, and persists regime-
-- adjusted family/symbol rows side-by-side with raw 4.1A and weighted 4.1B
-- values. Does not modify existing attribution layers.

begin;

-- ── A. Regime Interpretation Profiles ───────────────────────────────────
-- One active profile per (workspace, regime_key). A partial UNIQUE index
-- enforces the "at most one active" constraint at the DB level.

CREATE TABLE IF NOT EXISTS regime_cross_asset_interpretation_profiles (
    id                       uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id             uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name             text        NOT NULL,
    regime_key               text        NOT NULL,
    is_active                boolean     NOT NULL DEFAULT true,
    family_weight_overrides  jsonb       NOT NULL DEFAULT '{}'::jsonb,
    type_weight_overrides    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    confirmation_scale       numeric     NOT NULL DEFAULT 1.0,
    contradiction_scale      numeric     NOT NULL DEFAULT 1.0,
    missing_penalty_scale    numeric     NOT NULL DEFAULT 1.0,
    stale_penalty_scale      numeric     NOT NULL DEFAULT 1.0,
    dominance_threshold      numeric     NOT NULL DEFAULT 0.05,
    metadata                 jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at               timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS regime_cross_asset_interpretation_profiles_scope_idx
    ON regime_cross_asset_interpretation_profiles
       (workspace_id, regime_key, is_active, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS regime_cross_asset_interpretation_profiles_active_unique_idx
    ON regime_cross_asset_interpretation_profiles (workspace_id, regime_key)
    WHERE is_active;

-- ── B. Regime-Aware Family Attribution Snapshots ────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_regime_attribution_snapshots (
    id                                     uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                           uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                           uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                 uuid        NOT NULL,
    context_snapshot_id                    uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    regime_key                             text        NOT NULL,
    interpretation_profile_id              uuid        REFERENCES regime_cross_asset_interpretation_profiles(id) ON DELETE SET NULL,
    dependency_family                      text        NOT NULL,
    raw_family_net_contribution            numeric,
    weighted_family_net_contribution       numeric,
    regime_family_weight                   numeric,
    regime_type_weight                     numeric,
    regime_confirmation_scale              numeric,
    regime_contradiction_scale             numeric,
    regime_missing_penalty_scale           numeric,
    regime_stale_penalty_scale             numeric,
    regime_adjusted_family_contribution    numeric,
    regime_family_rank                     integer,
    interpretation_state                   text        NOT NULL DEFAULT 'computed'
                                                        CHECK (interpretation_state IN (
                                                            'computed','partial','missing_regime','regime_mismatch'
                                                        )),
    top_symbols                            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                               jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_regime_scope_time_idx
    ON cross_asset_family_regime_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_regime_run_idx
    ON cross_asset_family_regime_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_regime_regime_idx
    ON cross_asset_family_regime_attribution_snapshots (regime_key);

CREATE INDEX IF NOT EXISTS cross_asset_family_regime_family_idx
    ON cross_asset_family_regime_attribution_snapshots (dependency_family);

-- ── C. Regime-Aware Symbol Attribution Snapshots ────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_symbol_regime_attribution_snapshots (
    id                                     uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                           uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                           uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                 uuid        NOT NULL,
    context_snapshot_id                    uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    regime_key                             text        NOT NULL,
    interpretation_profile_id              uuid        REFERENCES regime_cross_asset_interpretation_profiles(id) ON DELETE SET NULL,
    symbol                                 text        NOT NULL,
    dependency_family                      text        NOT NULL,
    dependency_type                        text,
    graph_priority                         integer,
    is_direct_dependency                   boolean     NOT NULL DEFAULT true,
    raw_symbol_score                       numeric,
    weighted_symbol_score                  numeric,
    regime_family_weight                   numeric,
    regime_type_weight                     numeric,
    regime_adjusted_symbol_score           numeric,
    symbol_rank                            integer,
    metadata                               jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_regime_scope_time_idx
    ON cross_asset_symbol_regime_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_regime_run_idx
    ON cross_asset_symbol_regime_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_regime_regime_idx
    ON cross_asset_symbol_regime_attribution_snapshots (regime_key);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_regime_symbol_idx
    ON cross_asset_symbol_regime_attribution_snapshots (symbol);

-- ── D. Regime-Aware Family Attribution Summary view ─────────────────────

CREATE OR REPLACE VIEW cross_asset_family_regime_attribution_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_regime_attribution_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    regime_key,
    dependency_family,
    raw_family_net_contribution,
    weighted_family_net_contribution,
    regime_family_weight,
    regime_type_weight,
    regime_confirmation_scale,
    regime_contradiction_scale,
    regime_missing_penalty_scale,
    regime_stale_penalty_scale,
    regime_adjusted_family_contribution,
    regime_family_rank,
    interpretation_state,
    top_symbols,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Regime-Aware Symbol Attribution Summary view ─────────────────────

CREATE OR REPLACE VIEW cross_asset_symbol_regime_attribution_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.run_id, s.symbol
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_symbol_regime_attribution_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    regime_key,
    symbol,
    dependency_family,
    dependency_type,
    graph_priority,
    is_direct_dependency,
    raw_symbol_score,
    weighted_symbol_score,
    regime_family_weight,
    regime_type_weight,
    regime_adjusted_symbol_score,
    symbol_rank,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Regime-Aware Integration Summary view ────────────────────────────
-- Bridges 4.1A raw attribution + 4.0D explanation bridge + 4.1B weighted
-- integration + 4.1C regime-adjusted aggregates so run inspection can see
-- raw / weighted / regime-adjusted side-by-side.

CREATE OR REPLACE VIEW run_cross_asset_regime_integration_summary AS
WITH regime_totals AS (
    SELECT
        run_id,
        workspace_id,
        watchlist_id,
        max(regime_key)                                          AS regime_key,
        sum(regime_adjusted_family_contribution)::numeric        AS regime_adjusted_total
    FROM cross_asset_family_regime_attribution_summary
    GROUP BY run_id, workspace_id, watchlist_id
),
regime_dominant AS (
    SELECT DISTINCT ON (run_id)
        run_id,
        dependency_family AS regime_dominant_dependency_family
    FROM cross_asset_family_regime_attribution_summary
    WHERE regime_family_rank = 1
    ORDER BY run_id, created_at DESC
)
SELECT
    a.run_id,
    a.workspace_id,
    a.watchlist_id,
    a.context_snapshot_id,
    COALESCE(rt.regime_key, 'missing_regime')                      AS regime_key,
    a.cross_asset_net_contribution,
    w.weighted_cross_asset_net_contribution,
    rt.regime_adjusted_total                                       AS regime_adjusted_cross_asset_contribution,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    rd.regime_dominant_dependency_family,
    bridge.cross_asset_confidence_score,
    a.created_at
FROM cross_asset_attribution_summary a
LEFT JOIN run_cross_asset_weighted_integration_summary w   ON w.run_id   = a.run_id
LEFT JOIN run_cross_asset_explanation_bridge        bridge ON bridge.run_id = a.run_id
LEFT JOIN regime_totals    rt ON rt.run_id = a.run_id
LEFT JOIN regime_dominant  rd ON rd.run_id = a.run_id;

commit;
