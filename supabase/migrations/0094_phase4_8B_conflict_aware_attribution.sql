-- Phase 4.8B: Conflict-Aware Attribution
-- Additive refinement layer on top of the live 4.7B decay-aware attribution
-- and the 4.8A cross-layer conflict / agreement diagnostics. Conditions
-- family and symbol contribution on layer-consensus state, agreement score,
-- conflict score, and dominant conflict source with bounded weights and
-- explicit bonuses/penalties. Does not modify any upstream attribution,
-- decay, persistence, or layer-conflict surface.

begin;

-- ── A. Conflict Attribution Profiles ────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_conflict_attribution_profiles (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                        text        NOT NULL,
    is_active                           boolean     NOT NULL DEFAULT true,
    aligned_supportive_weight           numeric     NOT NULL DEFAULT 1.08,
    aligned_suppressive_weight          numeric     NOT NULL DEFAULT 0.78,
    partial_agreement_weight            numeric     NOT NULL DEFAULT 0.96,
    conflicted_weight                   numeric     NOT NULL DEFAULT 0.72,
    unreliable_weight                   numeric     NOT NULL DEFAULT 0.65,
    insufficient_context_weight         numeric     NOT NULL DEFAULT 0.80,
    agreement_bonus_scale               numeric     NOT NULL DEFAULT 1.0,
    conflict_penalty_scale              numeric     NOT NULL DEFAULT 1.0,
    unreliable_penalty_scale            numeric     NOT NULL DEFAULT 1.0,
    dominant_conflict_source_penalties  jsonb       NOT NULL DEFAULT '{}'::jsonb,
    conflict_family_overrides           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_conflict_attribution_profiles_active_idx
    ON cross_asset_conflict_attribution_profiles (workspace_id, is_active, created_at DESC);

-- ── B. Family Conflict-Aware Attribution Snapshots ──────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_conflict_attribution_snapshots (
    id                                          uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                                uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                                uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                      uuid        NOT NULL,
    context_snapshot_id                         uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    conflict_profile_id                         uuid        REFERENCES cross_asset_conflict_attribution_profiles(id) ON DELETE SET NULL,
    dependency_family                           text        NOT NULL,
    raw_family_net_contribution                 numeric,
    weighted_family_net_contribution            numeric,
    regime_adjusted_family_contribution         numeric,
    timing_adjusted_family_contribution         numeric,
    transition_adjusted_family_contribution     numeric,
    archetype_adjusted_family_contribution      numeric,
    cluster_adjusted_family_contribution        numeric,
    persistence_adjusted_family_contribution    numeric,
    decay_adjusted_family_contribution          numeric,
    family_consensus_state                      text        NOT NULL DEFAULT 'insufficient_context'
                                                CHECK (family_consensus_state IN (
                                                    'aligned_supportive','aligned_suppressive',
                                                    'partial_agreement','conflicted','unreliable',
                                                    'insufficient_context')),
    agreement_score                             numeric,
    conflict_score                              numeric,
    dominant_conflict_source                    text,
    transition_direction                        text,
    archetype_direction                         text,
    cluster_direction                           text,
    persistence_direction                       text,
    decay_direction                             text,
    conflict_weight                             numeric,
    conflict_bonus                              numeric,
    conflict_penalty                            numeric,
    conflict_adjusted_family_contribution       numeric,
    conflict_family_rank                        integer,
    top_symbols                                 jsonb       NOT NULL DEFAULT '[]'::jsonb,
    reason_codes                                jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                                    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_attribution_scope_time_idx
    ON cross_asset_family_conflict_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_attribution_run_idx
    ON cross_asset_family_conflict_attribution_snapshots (run_id);
CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_attribution_family_idx
    ON cross_asset_family_conflict_attribution_snapshots (dependency_family);
CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_attribution_consensus_idx
    ON cross_asset_family_conflict_attribution_snapshots (family_consensus_state);
CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_attribution_dominant_source_idx
    ON cross_asset_family_conflict_attribution_snapshots (dominant_conflict_source);

-- ── C. Symbol Conflict-Aware Attribution Snapshots ──────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_symbol_conflict_attribution_snapshots (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                        uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                              uuid        NOT NULL,
    context_snapshot_id                 uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    conflict_profile_id                 uuid        REFERENCES cross_asset_conflict_attribution_profiles(id) ON DELETE SET NULL,
    symbol                              text        NOT NULL,
    dependency_family                   text        NOT NULL,
    dependency_type                     text,
    family_consensus_state              text        NOT NULL DEFAULT 'insufficient_context'
                                                CHECK (family_consensus_state IN (
                                                    'aligned_supportive','aligned_suppressive',
                                                    'partial_agreement','conflicted','unreliable',
                                                    'insufficient_context')),
    agreement_score                     numeric,
    conflict_score                      numeric,
    dominant_conflict_source            text,
    raw_symbol_score                    numeric,
    weighted_symbol_score               numeric,
    regime_adjusted_symbol_score        numeric,
    timing_adjusted_symbol_score        numeric,
    transition_adjusted_symbol_score    numeric,
    archetype_adjusted_symbol_score     numeric,
    cluster_adjusted_symbol_score       numeric,
    persistence_adjusted_symbol_score   numeric,
    decay_adjusted_symbol_score         numeric,
    conflict_weight                     numeric,
    conflict_adjusted_symbol_score      numeric,
    symbol_rank                         integer,
    reason_codes                        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_conflict_attribution_scope_time_idx
    ON cross_asset_symbol_conflict_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_symbol_conflict_attribution_run_idx
    ON cross_asset_symbol_conflict_attribution_snapshots (run_id);
CREATE INDEX IF NOT EXISTS cross_asset_symbol_conflict_attribution_symbol_idx
    ON cross_asset_symbol_conflict_attribution_snapshots (symbol);
CREATE INDEX IF NOT EXISTS cross_asset_symbol_conflict_attribution_consensus_idx
    ON cross_asset_symbol_conflict_attribution_snapshots (family_consensus_state);

-- ── D. Family Conflict-Aware Attribution Summary view ───────────────────

CREATE OR REPLACE VIEW cross_asset_family_conflict_attribution_summary AS
WITH ranked AS (
    SELECT f.*,
        row_number() OVER (PARTITION BY f.run_id, f.dependency_family ORDER BY f.created_at DESC) AS rn
    FROM cross_asset_family_conflict_attribution_snapshots f
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
    timing_adjusted_family_contribution,
    transition_adjusted_family_contribution,
    archetype_adjusted_family_contribution,
    cluster_adjusted_family_contribution,
    persistence_adjusted_family_contribution,
    decay_adjusted_family_contribution,
    family_consensus_state,
    agreement_score,
    conflict_score,
    dominant_conflict_source,
    transition_direction,
    archetype_direction,
    cluster_direction,
    persistence_direction,
    decay_direction,
    conflict_weight,
    conflict_bonus,
    conflict_penalty,
    conflict_adjusted_family_contribution,
    conflict_family_rank,
    top_symbols,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Symbol Conflict-Aware Attribution Summary view ───────────────────

CREATE OR REPLACE VIEW cross_asset_symbol_conflict_attribution_summary AS
WITH ranked AS (
    SELECT s.*,
        row_number() OVER (PARTITION BY s.run_id, s.symbol ORDER BY s.created_at DESC) AS rn
    FROM cross_asset_symbol_conflict_attribution_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    symbol,
    dependency_family,
    dependency_type,
    family_consensus_state,
    agreement_score,
    conflict_score,
    dominant_conflict_source,
    raw_symbol_score,
    weighted_symbol_score,
    regime_adjusted_symbol_score,
    timing_adjusted_symbol_score,
    transition_adjusted_symbol_score,
    archetype_adjusted_symbol_score,
    cluster_adjusted_symbol_score,
    persistence_adjusted_symbol_score,
    decay_adjusted_symbol_score,
    conflict_weight,
    conflict_adjusted_symbol_score,
    symbol_rank,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Conflict-Aware Integration Bridge view ───────────────────────────
-- Joins raw, weighted, regime, timing, transition, archetype, cluster,
-- persistence, decay, and conflict attribution layers + 4.8A run-level
-- layer conflict summary.

CREATE OR REPLACE VIEW run_cross_asset_conflict_attribution_summary AS
WITH conflict_totals AS (
    SELECT run_id, workspace_id, watchlist_id, context_snapshot_id,
        sum(conflict_adjusted_family_contribution)::numeric AS conflict_adjusted_cross_asset_contribution
    FROM cross_asset_family_conflict_attribution_summary
    GROUP BY run_id, workspace_id, watchlist_id, context_snapshot_id
),
conflict_dominant AS (
    SELECT DISTINCT ON (run_id) run_id,
        dependency_family AS conflict_dominant_dependency_family
    FROM cross_asset_family_conflict_attribution_summary
    WHERE conflict_family_rank = 1
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
    t.timing_adjusted_cross_asset_contribution,
    tra.transition_adjusted_cross_asset_contribution,
    aa.archetype_adjusted_cross_asset_contribution,
    ca.cluster_adjusted_cross_asset_contribution,
    pa.persistence_adjusted_cross_asset_contribution,
    da.decay_adjusted_cross_asset_contribution,
    ct.conflict_adjusted_cross_asset_contribution,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    r.regime_dominant_dependency_family,
    t.timing_dominant_dependency_family,
    tra.transition_dominant_dependency_family,
    aa.archetype_dominant_dependency_family,
    ca.cluster_dominant_dependency_family,
    pa.persistence_dominant_dependency_family,
    da.decay_dominant_dependency_family,
    cd.conflict_dominant_dependency_family,
    rcl.layer_consensus_state,
    rcl.agreement_score,
    rcl.conflict_score,
    rcl.dominant_conflict_source,
    a.created_at
FROM cross_asset_attribution_summary a
LEFT JOIN run_cross_asset_weighted_integration_summary    w   ON w.run_id   = a.run_id
LEFT JOIN run_cross_asset_regime_integration_summary      r   ON r.run_id   = a.run_id
LEFT JOIN run_cross_asset_timing_attribution_summary      t   ON t.run_id   = a.run_id
LEFT JOIN run_cross_asset_transition_attribution_summary  tra ON tra.run_id = a.run_id
LEFT JOIN run_cross_asset_archetype_attribution_summary   aa  ON aa.run_id  = a.run_id
LEFT JOIN run_cross_asset_cluster_attribution_summary     ca  ON ca.run_id  = a.run_id
LEFT JOIN run_cross_asset_persistence_attribution_summary pa  ON pa.run_id  = a.run_id
LEFT JOIN run_cross_asset_decay_attribution_summary       da  ON da.run_id  = a.run_id
LEFT JOIN run_cross_asset_explanation_bridge              bridge ON bridge.run_id = a.run_id
LEFT JOIN conflict_totals    ct  ON ct.run_id  = a.run_id
LEFT JOIN conflict_dominant  cd  ON cd.run_id  = a.run_id
LEFT JOIN run_cross_asset_layer_conflict_summary          rcl ON rcl.run_id = a.run_id;

commit;
