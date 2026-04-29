-- Phase 4.2C: Timing-Aware Composite Refinement
-- Final timing-aware integration layer. Starts from the most mature upstream
-- integrated composite (regime-adjusted → weighted → raw fallback), adds a
-- bounded timing-aware delta conditioned on dominant timing class, and
-- persists the result side-by-side with all upstream layers. Does not modify
-- any existing composite or attribution layer.

begin;

-- ── A. Timing Integration Profiles ──────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_timing_integration_profiles (
    id                               uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                     uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                     text        NOT NULL,
    is_active                        boolean     NOT NULL DEFAULT true,
    integration_mode                 text        NOT NULL DEFAULT 'timing_additive_guardrailed'
                                                CHECK (integration_mode IN (
                                                    'timing_additive_guardrailed',
                                                    'lead_confirmation_only',
                                                    'lag_suppression_only'
                                                )),
    integration_weight               numeric     NOT NULL DEFAULT 0.10,
    lead_weight_scale                numeric     NOT NULL DEFAULT 1.00,
    coincident_weight_scale          numeric     NOT NULL DEFAULT 1.00,
    lag_weight_scale                 numeric     NOT NULL DEFAULT 1.00,
    insufficient_data_weight_scale   numeric     NOT NULL DEFAULT 1.00,
    max_positive_contribution        numeric     NOT NULL DEFAULT 0.25,
    max_negative_contribution        numeric     NOT NULL DEFAULT 0.25,
    metadata                         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_timing_integration_profiles_scope_idx
    ON cross_asset_timing_integration_profiles (workspace_id, is_active, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS cross_asset_timing_integration_profiles_active_unique_idx
    ON cross_asset_timing_integration_profiles (workspace_id)
    WHERE is_active;

-- ── B. Timing-Aware Composite Snapshots ─────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_timing_composite_snapshots (
    id                                        uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                              uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                              uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                    uuid        NOT NULL,
    context_snapshot_id                       uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    timing_integration_profile_id             uuid        REFERENCES cross_asset_timing_integration_profiles(id) ON DELETE SET NULL,
    base_signal_score                         numeric,
    cross_asset_net_contribution              numeric,
    weighted_cross_asset_net_contribution     numeric,
    regime_adjusted_cross_asset_contribution  numeric,
    timing_adjusted_cross_asset_contribution  numeric,
    composite_pre_timing                      numeric,
    timing_net_contribution                   numeric,
    composite_post_timing                     numeric,
    dominant_timing_class                     text        NOT NULL DEFAULT 'insufficient_data'
                                                        CHECK (dominant_timing_class IN ('lead','coincident','lag','insufficient_data')),
    integration_mode                          text        NOT NULL,
    metadata                                  jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                                timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_timing_composite_scope_time_idx
    ON cross_asset_timing_composite_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_timing_composite_run_idx
    ON cross_asset_timing_composite_snapshots (run_id);

-- ── C. Family Timing Composite Contribution Snapshots ───────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_timing_composite_snapshots (
    id                                   uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                         uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                         uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                               uuid        NOT NULL,
    context_snapshot_id                  uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dependency_family                    text        NOT NULL,
    dominant_timing_class                text        NOT NULL DEFAULT 'insufficient_data'
                                                        CHECK (dominant_timing_class IN ('lead','coincident','lag','insufficient_data')),
    timing_adjusted_family_contribution  numeric,
    integration_weight_applied           numeric,
    timing_integration_contribution      numeric,
    family_rank                          integer,
    top_symbols                          jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                             jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_composite_scope_time_idx
    ON cross_asset_family_timing_composite_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_composite_run_idx
    ON cross_asset_family_timing_composite_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_composite_family_idx
    ON cross_asset_family_timing_composite_snapshots (dependency_family);

-- ── D. Timing Composite Summary view ────────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_timing_composite_summary AS
WITH ranked AS (
    SELECT
        c.*,
        row_number() OVER (
            PARTITION BY c.run_id
            ORDER BY c.created_at DESC
        ) AS rn
    FROM cross_asset_timing_composite_snapshots c
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    base_signal_score,
    cross_asset_net_contribution,
    weighted_cross_asset_net_contribution,
    regime_adjusted_cross_asset_contribution,
    timing_adjusted_cross_asset_contribution,
    composite_pre_timing,
    timing_net_contribution,
    composite_post_timing,
    dominant_timing_class,
    integration_mode,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Family Timing Composite Summary view ─────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_timing_composite_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_timing_composite_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    dominant_timing_class,
    timing_adjusted_family_contribution,
    integration_weight_applied,
    timing_integration_contribution,
    family_rank,
    top_symbols,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Final Integration Bridge view ────────────────────────────────────
-- Joins 4.1A raw integration, 4.1B weighted integration, 4.1C regime
-- integration, 4.2B timing attribution aggregates, and 4.2C timing
-- composite into a single row per run for run inspection.

CREATE OR REPLACE VIEW run_cross_asset_final_integration_summary AS
SELECT
    tc.run_id,
    tc.workspace_id,
    tc.watchlist_id,
    tc.context_snapshot_id,
    tc.cross_asset_net_contribution,
    tc.weighted_cross_asset_net_contribution,
    tc.regime_adjusted_cross_asset_contribution,
    tc.timing_adjusted_cross_asset_contribution,
    tc.timing_net_contribution,
    tc.composite_pre_timing,
    tc.composite_post_timing,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    r.regime_dominant_dependency_family,
    t.timing_dominant_dependency_family,
    tc.dominant_timing_class,
    tc.created_at
FROM cross_asset_timing_composite_summary tc
LEFT JOIN run_cross_asset_explanation_bridge       bridge ON bridge.run_id = tc.run_id
LEFT JOIN run_cross_asset_weighted_integration_summary w  ON w.run_id     = tc.run_id
LEFT JOIN run_cross_asset_regime_integration_summary   r  ON r.run_id     = tc.run_id
LEFT JOIN run_cross_asset_timing_attribution_summary   t  ON t.run_id     = tc.run_id;

commit;
