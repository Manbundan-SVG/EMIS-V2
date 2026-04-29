-- Phase 4.5C: Cluster-Aware Composite Refinement
-- Final cluster-aware integration layer. Starts from the most mature upstream
-- composite (4.4C composite_post_archetype → 4.3C composite_post_transition →
-- 4.2C composite_post_timing → regime equivalent → raw fallback), adds a
-- bounded cluster-aware delta conditioned on the run's cluster state, and
-- persists the result side-by-side with all upstream layers. Does not modify
-- any existing composite, attribution, archetype, or cluster surface.

begin;

-- ── A. Cluster Integration Profiles ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_cluster_integration_profiles (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                    text        NOT NULL,
    is_active                       boolean     NOT NULL DEFAULT true,
    integration_mode                text        NOT NULL DEFAULT 'cluster_additive_guardrailed'
                                                CHECK (integration_mode IN (
                                                    'cluster_additive_guardrailed',
                                                    'stable_confirmation_only',
                                                    'deterioration_suppression_only',
                                                    'rotation_sensitive'
                                                )),
    integration_weight              numeric     NOT NULL DEFAULT 0.10,
    stable_scale                    numeric     NOT NULL DEFAULT 1.08,
    recovering_scale                numeric     NOT NULL DEFAULT 1.03,
    rotating_scale                  numeric     NOT NULL DEFAULT 1.01,
    mixed_scale                     numeric     NOT NULL DEFAULT 0.92,
    deteriorating_scale             numeric     NOT NULL DEFAULT 0.82,
    insufficient_history_scale      numeric     NOT NULL DEFAULT 0.85,
    max_positive_contribution       numeric     NOT NULL DEFAULT 0.20,
    max_negative_contribution       numeric     NOT NULL DEFAULT 0.20,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_cluster_integration_profiles_scope_idx
    ON cross_asset_cluster_integration_profiles (workspace_id, is_active, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS cross_asset_cluster_integration_profiles_active_unique_idx
    ON cross_asset_cluster_integration_profiles (workspace_id)
    WHERE is_active;

-- ── B. Cluster-Aware Composite Snapshots ────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_cluster_composite_snapshots (
    id                                          uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                                uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                                uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                      uuid        NOT NULL,
    context_snapshot_id                         uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    cluster_integration_profile_id              uuid        REFERENCES cross_asset_cluster_integration_profiles(id) ON DELETE SET NULL,
    base_signal_score                           numeric,
    cross_asset_net_contribution                numeric,
    weighted_cross_asset_net_contribution       numeric,
    regime_adjusted_cross_asset_contribution    numeric,
    timing_adjusted_cross_asset_contribution    numeric,
    transition_adjusted_cross_asset_contribution numeric,
    archetype_adjusted_cross_asset_contribution numeric,
    cluster_adjusted_cross_asset_contribution   numeric,
    composite_pre_cluster                       numeric,
    cluster_net_contribution                    numeric,
    composite_post_cluster                      numeric,
    cluster_state                               text        NOT NULL DEFAULT 'insufficient_history'
                                                        CHECK (cluster_state IN (
                                                            'stable','rotating','deteriorating',
                                                            'recovering','mixed','insufficient_history'
                                                        )),
    dominant_archetype_key                      text        NOT NULL DEFAULT 'insufficient_history',
    integration_mode                            text        NOT NULL,
    metadata                                    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_cluster_composite_scope_time_idx
    ON cross_asset_cluster_composite_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_cluster_composite_run_idx
    ON cross_asset_cluster_composite_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_cluster_composite_state_idx
    ON cross_asset_cluster_composite_snapshots (cluster_state);

-- ── C. Family Cluster Composite Contribution Snapshots ──────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_cluster_composite_snapshots (
    id                                      uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                            uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                            uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                  uuid        NOT NULL,
    context_snapshot_id                     uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dependency_family                       text        NOT NULL,
    cluster_state                           text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (cluster_state IN (
                                                    'stable','rotating','deteriorating',
                                                    'recovering','mixed','insufficient_history'
                                                )),
    dominant_archetype_key                  text        NOT NULL DEFAULT 'insufficient_history',
    cluster_adjusted_family_contribution    numeric,
    integration_weight_applied              numeric,
    cluster_integration_contribution        numeric,
    family_rank                             integer,
    top_symbols                             jsonb       NOT NULL DEFAULT '[]'::jsonb,
    reason_codes                            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                                jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_cluster_composite_scope_time_idx
    ON cross_asset_family_cluster_composite_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_cluster_composite_run_idx
    ON cross_asset_family_cluster_composite_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_cluster_composite_family_idx
    ON cross_asset_family_cluster_composite_snapshots (dependency_family);

CREATE INDEX IF NOT EXISTS cross_asset_family_cluster_composite_state_idx
    ON cross_asset_family_cluster_composite_snapshots (cluster_state);

-- ── D. Cluster Composite Summary view ───────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_cluster_composite_summary AS
WITH ranked AS (
    SELECT
        c.*,
        row_number() OVER (
            PARTITION BY c.run_id
            ORDER BY c.created_at DESC
        ) AS rn
    FROM cross_asset_cluster_composite_snapshots c
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
    transition_adjusted_cross_asset_contribution,
    archetype_adjusted_cross_asset_contribution,
    cluster_adjusted_cross_asset_contribution,
    composite_pre_cluster,
    cluster_net_contribution,
    composite_post_cluster,
    cluster_state,
    dominant_archetype_key,
    integration_mode,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Family Cluster Composite Summary view ────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_cluster_composite_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_cluster_composite_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    cluster_state,
    dominant_archetype_key,
    cluster_adjusted_family_contribution,
    integration_weight_applied,
    cluster_integration_contribution,
    family_rank,
    top_symbols,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Final Cluster Integration Bridge view ────────────────────────────
-- Joins all upstream integration layers + 4.5C cluster composite into a
-- single row per run for run inspection.

CREATE OR REPLACE VIEW run_cross_asset_cluster_integration_summary AS
SELECT
    cc.run_id,
    cc.workspace_id,
    cc.watchlist_id,
    cc.context_snapshot_id,
    cc.cross_asset_net_contribution,
    cc.weighted_cross_asset_net_contribution,
    cc.regime_adjusted_cross_asset_contribution,
    cc.timing_adjusted_cross_asset_contribution,
    cc.transition_adjusted_cross_asset_contribution,
    cc.archetype_adjusted_cross_asset_contribution,
    cc.cluster_adjusted_cross_asset_contribution,
    cc.cluster_net_contribution,
    cc.composite_pre_cluster,
    cc.composite_post_cluster,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    r.regime_dominant_dependency_family,
    t.timing_dominant_dependency_family,
    tra.transition_dominant_dependency_family,
    aa.archetype_dominant_dependency_family,
    ca.cluster_dominant_dependency_family,
    cc.cluster_state,
    cc.dominant_archetype_key,
    cc.created_at
FROM cross_asset_cluster_composite_summary cc
LEFT JOIN run_cross_asset_explanation_bridge               bridge ON bridge.run_id = cc.run_id
LEFT JOIN run_cross_asset_weighted_integration_summary     w      ON w.run_id      = cc.run_id
LEFT JOIN run_cross_asset_regime_integration_summary       r      ON r.run_id      = cc.run_id
LEFT JOIN run_cross_asset_timing_attribution_summary       t      ON t.run_id      = cc.run_id
LEFT JOIN run_cross_asset_transition_attribution_summary   tra    ON tra.run_id    = cc.run_id
LEFT JOIN run_cross_asset_archetype_attribution_summary    aa     ON aa.run_id     = cc.run_id
LEFT JOIN run_cross_asset_cluster_attribution_summary      ca     ON ca.run_id     = cc.run_id;

commit;
