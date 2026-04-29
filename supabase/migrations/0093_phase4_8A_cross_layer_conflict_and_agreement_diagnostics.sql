-- Phase 4.8A: Cross-Layer Conflict and Agreement Diagnostics
-- Additive cross-layer conflict/agreement diagnostic layer over the live
-- 4.2A timing, 4.3A transition, 4.4A archetype, 4.5A cluster, 4.6A
-- persistence, and 4.7A decay surfaces. Provides per-run consensus state,
-- per-family consensus state, and discrete conflict events. Does not modify
-- any classification, attribution, composite, or replay-validation layer.

begin;

-- ── A. Cross-Layer Conflict Policy Profiles ─────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_conflict_policy_profiles (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                    text        NOT NULL,
    is_active                       boolean     NOT NULL DEFAULT true,
    timing_weight                   numeric     NOT NULL DEFAULT 0.15,
    transition_weight               numeric     NOT NULL DEFAULT 0.20,
    archetype_weight                numeric     NOT NULL DEFAULT 0.15,
    cluster_weight                  numeric     NOT NULL DEFAULT 0.20,
    persistence_weight              numeric     NOT NULL DEFAULT 0.15,
    decay_weight                    numeric     NOT NULL DEFAULT 0.15,
    agreement_threshold             numeric     NOT NULL DEFAULT 0.70,
    partial_agreement_threshold     numeric     NOT NULL DEFAULT 0.50,
    conflict_threshold              numeric     NOT NULL DEFAULT 0.35,
    unreliable_threshold            numeric     NOT NULL DEFAULT 0.20,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_conflict_policy_profiles_active_idx
    ON cross_asset_conflict_policy_profiles (workspace_id, is_active, created_at DESC);

-- ── B. Cross-Layer Agreement Snapshots ──────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_layer_agreement_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                          uuid        NOT NULL,
    context_snapshot_id             uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    conflict_policy_profile_id      uuid        REFERENCES cross_asset_conflict_policy_profiles(id) ON DELETE SET NULL,
    dominant_timing_class           text,
    dominant_transition_state       text,
    dominant_sequence_class         text,
    dominant_archetype_key          text,
    cluster_state                   text,
    persistence_state               text,
    freshness_state                 text,
    timing_direction                text,
    transition_direction            text,
    archetype_direction             text,
    cluster_direction               text,
    persistence_direction           text,
    decay_direction                 text,
    supportive_weight               numeric,
    suppressive_weight              numeric,
    neutral_weight                  numeric,
    missing_weight                  numeric,
    agreement_score                 numeric,
    conflict_score                  numeric,
    layer_consensus_state           text        NOT NULL DEFAULT 'insufficient_context'
                                                CHECK (layer_consensus_state IN (
                                                    'aligned_supportive','aligned_suppressive',
                                                    'partial_agreement','conflicted','unreliable',
                                                    'insufficient_context'
                                                )),
    dominant_conflict_source        text,
    conflict_reason_codes           jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_layer_agreement_scope_time_idx
    ON cross_asset_layer_agreement_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_layer_agreement_run_idx
    ON cross_asset_layer_agreement_snapshots (run_id);
CREATE INDEX IF NOT EXISTS cross_asset_layer_agreement_consensus_idx
    ON cross_asset_layer_agreement_snapshots (layer_consensus_state);
CREATE INDEX IF NOT EXISTS cross_asset_layer_agreement_dominant_conflict_idx
    ON cross_asset_layer_agreement_snapshots (dominant_conflict_source);

-- ── C. Family Layer Agreement Snapshots ─────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_layer_agreement_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                          uuid        NOT NULL,
    context_snapshot_id             uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dependency_family               text        NOT NULL,
    transition_state                text,
    dominant_sequence_class         text,
    archetype_key                   text,
    cluster_state                   text,
    persistence_state               text,
    freshness_state                 text,
    family_contribution             numeric,
    transition_direction            text,
    archetype_direction             text,
    cluster_direction               text,
    persistence_direction           text,
    decay_direction                 text,
    agreement_score                 numeric,
    conflict_score                  numeric,
    family_consensus_state          text        NOT NULL DEFAULT 'insufficient_context'
                                                CHECK (family_consensus_state IN (
                                                    'aligned_supportive','aligned_suppressive',
                                                    'partial_agreement','conflicted','unreliable',
                                                    'insufficient_context'
                                                )),
    dominant_conflict_source        text,
    family_rank                     integer,
    conflict_reason_codes           jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_layer_agreement_scope_time_idx
    ON cross_asset_family_layer_agreement_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_family_layer_agreement_run_idx
    ON cross_asset_family_layer_agreement_snapshots (run_id);
CREATE INDEX IF NOT EXISTS cross_asset_family_layer_agreement_family_idx
    ON cross_asset_family_layer_agreement_snapshots (dependency_family);
CREATE INDEX IF NOT EXISTS cross_asset_family_layer_agreement_consensus_idx
    ON cross_asset_family_layer_agreement_snapshots (family_consensus_state);

-- ── D. Cross-Layer Conflict Event Snapshots ─────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_layer_conflict_event_snapshots (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                        uuid        REFERENCES watchlists(id) ON DELETE CASCADE,
    source_run_id                       uuid,
    target_run_id                       uuid        NOT NULL,
    prior_consensus_state               text,
    current_consensus_state             text        NOT NULL,
    prior_dominant_conflict_source      text,
    current_dominant_conflict_source    text,
    prior_agreement_score               numeric,
    current_agreement_score             numeric,
    prior_conflict_score                numeric,
    current_conflict_score              numeric,
    event_type                          text        NOT NULL CHECK (event_type IN (
                                                        'agreement_strengthened','agreement_weakened',
                                                        'conflict_emerged','conflict_resolved',
                                                        'unreliable_stack_detected','insufficient_context'
                                                    )),
    reason_codes                        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_layer_conflict_event_scope_time_idx
    ON cross_asset_layer_conflict_event_snapshots (workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_layer_conflict_event_target_idx
    ON cross_asset_layer_conflict_event_snapshots (target_run_id);
CREATE INDEX IF NOT EXISTS cross_asset_layer_conflict_event_type_idx
    ON cross_asset_layer_conflict_event_snapshots (event_type);
CREATE INDEX IF NOT EXISTS cross_asset_layer_conflict_event_consensus_idx
    ON cross_asset_layer_conflict_event_snapshots (current_consensus_state);

-- ── E. Cross-Layer Agreement Summary view ───────────────────────────────

CREATE OR REPLACE VIEW cross_asset_layer_agreement_summary AS
WITH ranked AS (
    SELECT s.*, row_number() OVER (PARTITION BY s.run_id ORDER BY s.created_at DESC) AS rn
    FROM cross_asset_layer_agreement_snapshots s
)
SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
    dominant_timing_class, dominant_transition_state, dominant_sequence_class,
    dominant_archetype_key, cluster_state, persistence_state, freshness_state,
    timing_direction, transition_direction, archetype_direction,
    cluster_direction, persistence_direction, decay_direction,
    supportive_weight, suppressive_weight, neutral_weight, missing_weight,
    agreement_score, conflict_score,
    layer_consensus_state, dominant_conflict_source,
    conflict_reason_codes, created_at
FROM ranked WHERE rn = 1;

-- ── F. Family Layer Agreement Summary view ──────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_layer_agreement_summary AS
WITH ranked AS (
    SELECT f.*,
        row_number() OVER (PARTITION BY f.run_id, f.dependency_family ORDER BY f.created_at DESC) AS rn
    FROM cross_asset_family_layer_agreement_snapshots f
)
SELECT workspace_id, watchlist_id, run_id, context_snapshot_id, dependency_family,
    transition_state, dominant_sequence_class, archetype_key,
    cluster_state, persistence_state, freshness_state,
    family_contribution,
    transition_direction, archetype_direction, cluster_direction,
    persistence_direction, decay_direction,
    agreement_score, conflict_score,
    family_consensus_state, dominant_conflict_source, family_rank,
    conflict_reason_codes, created_at
FROM ranked WHERE rn = 1;

-- ── G. Cross-Layer Conflict Event Summary view ──────────────────────────

CREATE OR REPLACE VIEW cross_asset_layer_conflict_event_summary AS
WITH ranked AS (
    SELECT e.*,
        row_number() OVER (PARTITION BY e.target_run_id, e.event_type ORDER BY e.created_at DESC) AS rn
    FROM cross_asset_layer_conflict_event_snapshots e
)
SELECT workspace_id, watchlist_id, source_run_id, target_run_id,
    prior_consensus_state, current_consensus_state,
    prior_dominant_conflict_source, current_dominant_conflict_source,
    prior_agreement_score, current_agreement_score,
    prior_conflict_score, current_conflict_score,
    event_type, reason_codes, created_at
FROM ranked WHERE rn = 1;

-- ── H. Run Cross-Layer Conflict Bridge view ─────────────────────────────

CREATE OR REPLACE VIEW run_cross_asset_layer_conflict_summary AS
WITH latest_event AS (
    SELECT DISTINCT ON (target_run_id)
        target_run_id, event_type AS latest_conflict_event_type, created_at AS latest_conflict_event_at
    FROM cross_asset_layer_conflict_event_summary
    WHERE target_run_id IS NOT NULL
    ORDER BY target_run_id, created_at DESC
)
SELECT
    la.run_id, la.workspace_id, la.watchlist_id,
    la.layer_consensus_state, la.agreement_score, la.conflict_score,
    la.dominant_conflict_source,
    la.freshness_state, la.persistence_state, la.cluster_state,
    le.latest_conflict_event_type, la.created_at
FROM cross_asset_layer_agreement_summary la
LEFT JOIN latest_event le ON le.target_run_id = la.run_id;

commit;
