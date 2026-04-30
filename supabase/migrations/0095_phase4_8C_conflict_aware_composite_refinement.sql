-- Phase 4.8C: Conflict-Aware Composite Refinement
-- Final conflict-aware integration layer. Starts from the most mature
-- upstream composite (4.7C composite_post_decay -> 4.6C composite_post_persistence
-- -> 4.5C composite_post_cluster -> 4.4C composite_post_archetype -> 4.3C
-- composite_post_transition -> 4.2C composite_post_timing -> regime / raw
-- fallback), adds a bounded conflict-aware delta conditioned on the
-- 4.8A run-level layer-consensus state, agreement / conflict scores,
-- and dominant conflict source plus the per-family conflict-aware
-- contribution from 4.8B, and persists the result side-by-side with
-- every upstream layer. Does not modify any upstream attribution,
-- decay, persistence, cluster, archetype, transition, timing, regime,
-- weighted, or raw integration surface. Surfaces all replay-readiness
-- columns required by 4.8D.

begin;

-- ── A. Conflict Integration Profiles ────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_conflict_integration_profiles (
    id                                     uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                           uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                           text        NOT NULL,
    is_active                              boolean     NOT NULL DEFAULT true,
    integration_mode                       text        NOT NULL DEFAULT 'conflict_additive_guardrailed'
                                                       CHECK (integration_mode IN (
                                                           'conflict_additive_guardrailed',
                                                           'aligned_supportive_confirmation_only',
                                                           'conflict_suppression_only',
                                                           'unreliable_suppression_only')),
    integration_weight                     numeric     NOT NULL DEFAULT 0.10,
    aligned_supportive_scale               numeric     NOT NULL DEFAULT 1.08,
    aligned_suppressive_scale              numeric     NOT NULL DEFAULT 0.78,
    partial_agreement_scale                numeric     NOT NULL DEFAULT 0.96,
    conflicted_scale                       numeric     NOT NULL DEFAULT 0.72,
    unreliable_scale                       numeric     NOT NULL DEFAULT 0.65,
    insufficient_context_scale             numeric     NOT NULL DEFAULT 0.80,
    conflict_extra_suppression             numeric     NOT NULL DEFAULT 0.03,
    unreliable_extra_suppression           numeric     NOT NULL DEFAULT 0.04,
    dominant_conflict_source_suppression   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    max_positive_contribution              numeric     NOT NULL DEFAULT 0.20,
    max_negative_contribution              numeric     NOT NULL DEFAULT 0.20,
    metadata                               jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_conflict_integration_profiles_active_idx
    ON cross_asset_conflict_integration_profiles (workspace_id, is_active, created_at DESC);

-- ── B. Conflict-Aware Composite Snapshots (per-run / per-watchlist) ─────

CREATE TABLE IF NOT EXISTS cross_asset_conflict_composite_snapshots (
    id                                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                          uuid        NOT NULL,
    context_snapshot_id                             uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    conflict_integration_profile_id                 uuid        REFERENCES cross_asset_conflict_integration_profiles(id) ON DELETE SET NULL,
    base_signal_score                               numeric,
    cross_asset_net_contribution                    numeric,
    weighted_cross_asset_net_contribution           numeric,
    regime_adjusted_cross_asset_contribution        numeric,
    timing_adjusted_cross_asset_contribution        numeric,
    transition_adjusted_cross_asset_contribution    numeric,
    archetype_adjusted_cross_asset_contribution     numeric,
    cluster_adjusted_cross_asset_contribution       numeric,
    persistence_adjusted_cross_asset_contribution   numeric,
    decay_adjusted_cross_asset_contribution         numeric,
    conflict_adjusted_cross_asset_contribution      numeric,
    composite_pre_conflict                          numeric,
    conflict_net_contribution                       numeric,
    composite_post_conflict                         numeric,
    layer_consensus_state                           text        NOT NULL DEFAULT 'insufficient_context'
                                                                CHECK (layer_consensus_state IN (
                                                                    'aligned_supportive','aligned_suppressive',
                                                                    'partial_agreement','conflicted','unreliable',
                                                                    'insufficient_context')),
    agreement_score                                 numeric,
    conflict_score                                  numeric,
    dominant_conflict_source                        text,
    integration_mode                                text        NOT NULL,
    source_contribution_layer                       text,
    source_composite_layer                          text,
    scoring_version                                 text        NOT NULL DEFAULT '4.8C.v1',
    metadata                                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_conflict_composite_scope_time_idx
    ON cross_asset_conflict_composite_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_conflict_composite_run_idx
    ON cross_asset_conflict_composite_snapshots (run_id);
CREATE INDEX IF NOT EXISTS cross_asset_conflict_composite_consensus_idx
    ON cross_asset_conflict_composite_snapshots (layer_consensus_state);
CREATE INDEX IF NOT EXISTS cross_asset_conflict_composite_dominant_source_idx
    ON cross_asset_conflict_composite_snapshots (dominant_conflict_source);

-- ── C. Family Conflict Composite Snapshots ──────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_conflict_composite_snapshots (
    id                                          uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                                uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                                uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                      uuid        NOT NULL,
    context_snapshot_id                         uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dependency_family                           text        NOT NULL,
    family_consensus_state                      text        NOT NULL DEFAULT 'insufficient_context'
                                                            CHECK (family_consensus_state IN (
                                                                'aligned_supportive','aligned_suppressive',
                                                                'partial_agreement','conflicted','unreliable',
                                                                'insufficient_context')),
    agreement_score                             numeric,
    conflict_score                              numeric,
    dominant_conflict_source                    text,
    conflict_adjusted_family_contribution       numeric,
    integration_weight_applied                  numeric,
    conflict_integration_contribution           numeric,
    family_rank                                 integer,
    top_symbols                                 jsonb       NOT NULL DEFAULT '[]'::jsonb,
    reason_codes                                jsonb       NOT NULL DEFAULT '[]'::jsonb,
    source_contribution_layer                   text,
    scoring_version                             text        NOT NULL DEFAULT '4.8C.v1',
    metadata                                    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_composite_scope_time_idx
    ON cross_asset_family_conflict_composite_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_composite_run_idx
    ON cross_asset_family_conflict_composite_snapshots (run_id);
CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_composite_family_idx
    ON cross_asset_family_conflict_composite_snapshots (dependency_family);
CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_composite_consensus_idx
    ON cross_asset_family_conflict_composite_snapshots (family_consensus_state);

-- ── D. Conflict-Aware Composite Summary view ────────────────────────────

CREATE OR REPLACE VIEW cross_asset_conflict_composite_summary AS
WITH ranked AS (
    SELECT s.*,
        row_number() OVER (PARTITION BY s.run_id ORDER BY s.created_at DESC) AS rn
    FROM cross_asset_conflict_composite_snapshots s
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
    persistence_adjusted_cross_asset_contribution,
    decay_adjusted_cross_asset_contribution,
    conflict_adjusted_cross_asset_contribution,
    composite_pre_conflict,
    conflict_net_contribution,
    composite_post_conflict,
    layer_consensus_state,
    agreement_score,
    conflict_score,
    dominant_conflict_source,
    integration_mode,
    source_contribution_layer,
    source_composite_layer,
    scoring_version,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Family Conflict Composite Summary view ───────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_conflict_composite_summary AS
WITH ranked AS (
    SELECT f.*,
        row_number() OVER (PARTITION BY f.run_id, f.dependency_family ORDER BY f.created_at DESC) AS rn
    FROM cross_asset_family_conflict_composite_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    family_consensus_state,
    agreement_score,
    conflict_score,
    dominant_conflict_source,
    conflict_adjusted_family_contribution,
    integration_weight_applied,
    conflict_integration_contribution,
    family_rank,
    top_symbols,
    reason_codes,
    source_contribution_layer,
    scoring_version,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Final Conflict Integration Bridge view ───────────────────────────
-- Joins every upstream contribution layer with the latest conflict-aware
-- composite. The 4.8A run-level layer-conflict summary contributes
-- consensus state and dominant conflict source. 4.8B's existing run
-- bridge view contributes conflict-adjusted contribution and conflict
-- dominant family.

CREATE OR REPLACE VIEW run_cross_asset_conflict_integration_summary AS
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
    cc.persistence_adjusted_cross_asset_contribution,
    cc.decay_adjusted_cross_asset_contribution,
    cc.conflict_adjusted_cross_asset_contribution,
    cc.conflict_net_contribution,
    cc.composite_pre_conflict,
    cc.composite_post_conflict,
    bridge.dominant_dependency_family,
    rcw.weighted_dominant_dependency_family,
    rcr.regime_dominant_dependency_family,
    rct.timing_dominant_dependency_family,
    rctra.transition_dominant_dependency_family,
    rcaa.archetype_dominant_dependency_family,
    rcca.cluster_dominant_dependency_family,
    rcpa.persistence_dominant_dependency_family,
    rcda.decay_dominant_dependency_family,
    rcca8b.conflict_dominant_dependency_family,
    cc.layer_consensus_state,
    cc.agreement_score,
    cc.conflict_score,
    cc.dominant_conflict_source,
    cc.integration_mode,
    cc.source_contribution_layer,
    cc.source_composite_layer,
    cc.scoring_version,
    cc.created_at
FROM cross_asset_conflict_composite_summary cc
LEFT JOIN run_cross_asset_explanation_bridge              bridge   ON bridge.run_id = cc.run_id
LEFT JOIN run_cross_asset_weighted_integration_summary    rcw      ON rcw.run_id    = cc.run_id
LEFT JOIN run_cross_asset_regime_integration_summary      rcr      ON rcr.run_id    = cc.run_id
LEFT JOIN run_cross_asset_timing_attribution_summary      rct      ON rct.run_id    = cc.run_id
LEFT JOIN run_cross_asset_transition_attribution_summary  rctra    ON rctra.run_id  = cc.run_id
LEFT JOIN run_cross_asset_archetype_attribution_summary   rcaa     ON rcaa.run_id   = cc.run_id
LEFT JOIN run_cross_asset_cluster_attribution_summary     rcca     ON rcca.run_id   = cc.run_id
LEFT JOIN run_cross_asset_persistence_attribution_summary rcpa     ON rcpa.run_id   = cc.run_id
LEFT JOIN run_cross_asset_decay_attribution_summary       rcda     ON rcda.run_id   = cc.run_id
LEFT JOIN run_cross_asset_conflict_attribution_summary    rcca8b   ON rcca8b.run_id = cc.run_id;

commit;
