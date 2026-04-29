-- Phase 4.7C: Decay-Aware Composite Refinement
-- Final decay-aware integration layer. Starts from the most mature upstream
-- composite (4.6C composite_post_persistence → 4.5C composite_post_cluster →
-- 4.4C composite_post_archetype → 4.3C composite_post_transition → 4.2C
-- composite_post_timing → regime equivalent → raw fallback), adds a bounded
-- decay-aware delta conditioned on the run's freshness state + stale memory
-- and contradiction flags from 4.7A, and persists the result side-by-side
-- with all upstream layers. Does not modify any existing composite,
-- attribution, archetype, cluster, persistence, or decay surface.

begin;

-- ── A. Decay Integration Profiles ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_decay_integration_profiles (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                        text        NOT NULL,
    is_active                           boolean     NOT NULL DEFAULT true,
    integration_mode                    text        NOT NULL DEFAULT 'decay_additive_guardrailed'
                                                CHECK (integration_mode IN (
                                                    'decay_additive_guardrailed',
                                                    'fresh_confirmation_only',
                                                    'stale_suppression_only',
                                                    'contradiction_suppression_only'
                                                )),
    integration_weight                  numeric     NOT NULL DEFAULT 0.10,
    fresh_scale                         numeric     NOT NULL DEFAULT 1.08,
    decaying_scale                      numeric     NOT NULL DEFAULT 0.98,
    stale_scale                         numeric     NOT NULL DEFAULT 0.82,
    contradicted_scale                  numeric     NOT NULL DEFAULT 0.65,
    mixed_scale                         numeric     NOT NULL DEFAULT 0.88,
    insufficient_history_scale          numeric     NOT NULL DEFAULT 0.80,
    stale_extra_suppression             numeric     NOT NULL DEFAULT 0.02,
    contradiction_extra_suppression     numeric     NOT NULL DEFAULT 0.04,
    max_positive_contribution           numeric     NOT NULL DEFAULT 0.20,
    max_negative_contribution           numeric     NOT NULL DEFAULT 0.20,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_decay_integration_profiles_active_idx
    ON cross_asset_decay_integration_profiles (workspace_id, is_active, created_at DESC);

-- ── B. Decay-Aware Composite Snapshots ──────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_decay_composite_snapshots (
    id                                          uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                                uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                                uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                      uuid        NOT NULL,
    context_snapshot_id                         uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    decay_integration_profile_id                uuid        REFERENCES cross_asset_decay_integration_profiles(id) ON DELETE SET NULL,
    base_signal_score                           numeric,
    cross_asset_net_contribution                numeric,
    weighted_cross_asset_net_contribution       numeric,
    regime_adjusted_cross_asset_contribution    numeric,
    timing_adjusted_cross_asset_contribution    numeric,
    transition_adjusted_cross_asset_contribution numeric,
    archetype_adjusted_cross_asset_contribution numeric,
    cluster_adjusted_cross_asset_contribution   numeric,
    persistence_adjusted_cross_asset_contribution numeric,
    decay_adjusted_cross_asset_contribution     numeric,
    composite_pre_decay                         numeric,
    decay_net_contribution                      numeric,
    composite_post_decay                        numeric,
    freshness_state                             text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (freshness_state IN (
                                                    'fresh','decaying','stale','contradicted','mixed','insufficient_history'
                                                )),
    aggregate_decay_score                       numeric,
    stale_memory_flag                           boolean     NOT NULL DEFAULT false,
    contradiction_flag                          boolean     NOT NULL DEFAULT false,
    integration_mode                            text        NOT NULL,
    metadata                                    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_decay_composite_scope_time_idx
    ON cross_asset_decay_composite_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_decay_composite_run_idx
    ON cross_asset_decay_composite_snapshots (run_id);
CREATE INDEX IF NOT EXISTS cross_asset_decay_composite_freshness_idx
    ON cross_asset_decay_composite_snapshots (freshness_state);
CREATE INDEX IF NOT EXISTS cross_asset_decay_composite_stale_flag_idx
    ON cross_asset_decay_composite_snapshots (stale_memory_flag);
CREATE INDEX IF NOT EXISTS cross_asset_decay_composite_contradiction_flag_idx
    ON cross_asset_decay_composite_snapshots (contradiction_flag);

-- ── C. Family Decay Composite Contribution Snapshots ────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_decay_composite_snapshots (
    id                                      uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                            uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                            uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                  uuid        NOT NULL,
    context_snapshot_id                     uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dependency_family                       text        NOT NULL,
    freshness_state                         text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (freshness_state IN (
                                                    'fresh','decaying','stale','contradicted','mixed','insufficient_history'
                                                )),
    aggregate_decay_score                   numeric,
    family_decay_score                      numeric,
    stale_memory_flag                       boolean     NOT NULL DEFAULT false,
    contradiction_flag                      boolean     NOT NULL DEFAULT false,
    decay_adjusted_family_contribution      numeric,
    integration_weight_applied              numeric,
    decay_integration_contribution          numeric,
    family_rank                             integer,
    top_symbols                             jsonb       NOT NULL DEFAULT '[]'::jsonb,
    reason_codes                            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                                jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_decay_composite_scope_time_idx
    ON cross_asset_family_decay_composite_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_family_decay_composite_run_idx
    ON cross_asset_family_decay_composite_snapshots (run_id);
CREATE INDEX IF NOT EXISTS cross_asset_family_decay_composite_family_idx
    ON cross_asset_family_decay_composite_snapshots (dependency_family);
CREATE INDEX IF NOT EXISTS cross_asset_family_decay_composite_freshness_idx
    ON cross_asset_family_decay_composite_snapshots (freshness_state);

-- ── D. Decay Composite Summary view ─────────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_decay_composite_summary AS
WITH ranked AS (
    SELECT c.*, row_number() OVER (PARTITION BY c.run_id ORDER BY c.created_at DESC) AS rn
    FROM cross_asset_decay_composite_snapshots c
)
SELECT
    workspace_id, watchlist_id, run_id, context_snapshot_id,
    base_signal_score,
    cross_asset_net_contribution,
    weighted_cross_asset_net_contribution,
    regime_adjusted_cross_asset_contribution,
    timing_adjusted_cross_asset_contribution,
    transition_adjusted_cross_asset_contribution,
    archetype_adjusted_cross_asset_contribution,
    cluster_adjusted_cross_asset_contribution,
    persistence_adjusted_cross_asset_contribution,
    decay_adjusted_cross_asset_contribution,
    composite_pre_decay, decay_net_contribution, composite_post_decay,
    freshness_state, aggregate_decay_score,
    stale_memory_flag, contradiction_flag,
    integration_mode, created_at
FROM ranked
WHERE rn = 1;

-- ── E. Family Decay Composite Summary view ──────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_decay_composite_summary AS
WITH ranked AS (
    SELECT f.*,
        row_number() OVER (PARTITION BY f.run_id, f.dependency_family ORDER BY f.created_at DESC) AS rn
    FROM cross_asset_family_decay_composite_snapshots f
)
SELECT
    workspace_id, watchlist_id, run_id, context_snapshot_id,
    dependency_family,
    freshness_state, aggregate_decay_score, family_decay_score,
    stale_memory_flag, contradiction_flag,
    decay_adjusted_family_contribution,
    integration_weight_applied,
    decay_integration_contribution,
    family_rank, top_symbols, reason_codes, created_at
FROM ranked
WHERE rn = 1;

-- ── F. Final Decay Integration Bridge view ──────────────────────────────
-- Joins all upstream integration layers + 4.7B decay attribution + the
-- 4.7C decay composite into one row per run for run inspection.

CREATE OR REPLACE VIEW run_cross_asset_decay_integration_summary AS
SELECT
    dc.run_id,
    dc.workspace_id,
    dc.watchlist_id,
    dc.context_snapshot_id,
    dc.cross_asset_net_contribution,
    dc.weighted_cross_asset_net_contribution,
    dc.regime_adjusted_cross_asset_contribution,
    dc.timing_adjusted_cross_asset_contribution,
    dc.transition_adjusted_cross_asset_contribution,
    dc.archetype_adjusted_cross_asset_contribution,
    dc.cluster_adjusted_cross_asset_contribution,
    dc.persistence_adjusted_cross_asset_contribution,
    dc.decay_adjusted_cross_asset_contribution,
    dc.decay_net_contribution,
    dc.composite_pre_decay,
    dc.composite_post_decay,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    r.regime_dominant_dependency_family,
    t.timing_dominant_dependency_family,
    tra.transition_dominant_dependency_family,
    aa.archetype_dominant_dependency_family,
    ca.cluster_dominant_dependency_family,
    pa.persistence_dominant_dependency_family,
    da.decay_dominant_dependency_family,
    dc.freshness_state,
    dc.aggregate_decay_score,
    dc.stale_memory_flag,
    dc.contradiction_flag,
    dc.created_at
FROM cross_asset_decay_composite_summary dc
LEFT JOIN run_cross_asset_explanation_bridge                bridge ON bridge.run_id = dc.run_id
LEFT JOIN run_cross_asset_weighted_integration_summary      w      ON w.run_id      = dc.run_id
LEFT JOIN run_cross_asset_regime_integration_summary        r      ON r.run_id      = dc.run_id
LEFT JOIN run_cross_asset_timing_attribution_summary        t      ON t.run_id      = dc.run_id
LEFT JOIN run_cross_asset_transition_attribution_summary    tra    ON tra.run_id    = dc.run_id
LEFT JOIN run_cross_asset_archetype_attribution_summary     aa     ON aa.run_id     = dc.run_id
LEFT JOIN run_cross_asset_cluster_attribution_summary       ca     ON ca.run_id     = dc.run_id
LEFT JOIN run_cross_asset_persistence_attribution_summary   pa     ON pa.run_id     = dc.run_id
LEFT JOIN run_cross_asset_decay_attribution_summary         da     ON da.run_id     = dc.run_id;

commit;
