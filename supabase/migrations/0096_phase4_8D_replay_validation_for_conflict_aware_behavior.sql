-- Phase 4.8D: Replay Validation for Conflict-Aware Behavior
-- Mirrors the 4.7D decay replay-validation pattern but for the conflict
-- layer. Compares a source run's conflict-aware attribution + composite
-- to its replay counterpart, captures explicit drift diagnostics, and
-- exposes per-family stability and an aggregate trustworthiness summary.
-- All comparison logic is deterministic, side-by-side, and metadata-stamped.
-- Does not modify any upstream attribution, decay, persistence, cluster,
-- archetype, transition, timing, regime, weighted, raw, or 4.8A/B/C
-- conflict surface.

begin;

-- ── A. Conflict Replay Validation Snapshots ─────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_conflict_replay_validation_snapshots (
    id                                      uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                            uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                            uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    source_run_id                           uuid        NOT NULL,
    replay_run_id                           uuid        NOT NULL,
    -- upstream state
    source_context_snapshot_id              uuid,
    replay_context_snapshot_id              uuid,
    source_regime_key                       text,
    replay_regime_key                       text,
    source_dominant_timing_class            text,
    replay_dominant_timing_class            text,
    source_dominant_transition_state        text,
    replay_dominant_transition_state        text,
    source_dominant_sequence_class          text,
    replay_dominant_sequence_class          text,
    source_dominant_archetype_key           text,
    replay_dominant_archetype_key           text,
    source_cluster_state                    text,
    replay_cluster_state                    text,
    source_persistence_state                text,
    replay_persistence_state                text,
    source_freshness_state                  text,
    replay_freshness_state                  text,
    -- conflict state
    source_layer_consensus_state            text,
    replay_layer_consensus_state            text,
    source_agreement_score                  numeric,
    replay_agreement_score                  numeric,
    source_conflict_score                   numeric,
    replay_conflict_score                   numeric,
    source_dominant_conflict_source         text,
    replay_dominant_conflict_source         text,
    -- 4.8C replay-readiness fields
    source_contribution_layer               text,
    replay_contribution_layer               text,
    source_composite_layer                  text,
    replay_composite_layer                  text,
    source_scoring_version                  text,
    replay_scoring_version                  text,
    -- match flags
    context_hash_match                      boolean     NOT NULL DEFAULT false,
    regime_match                            boolean     NOT NULL DEFAULT false,
    timing_class_match                      boolean     NOT NULL DEFAULT false,
    transition_state_match                  boolean     NOT NULL DEFAULT false,
    sequence_class_match                    boolean     NOT NULL DEFAULT false,
    archetype_match                         boolean     NOT NULL DEFAULT false,
    cluster_state_match                     boolean     NOT NULL DEFAULT false,
    persistence_state_match                 boolean     NOT NULL DEFAULT false,
    freshness_state_match                   boolean     NOT NULL DEFAULT false,
    layer_consensus_state_match             boolean     NOT NULL DEFAULT false,
    agreement_score_match                   boolean     NOT NULL DEFAULT false,
    conflict_score_match                    boolean     NOT NULL DEFAULT false,
    dominant_conflict_source_match          boolean     NOT NULL DEFAULT false,
    source_contribution_layer_match         boolean     NOT NULL DEFAULT false,
    source_composite_layer_match            boolean     NOT NULL DEFAULT false,
    scoring_version_match                   boolean     NOT NULL DEFAULT false,
    conflict_attribution_match              boolean     NOT NULL DEFAULT false,
    conflict_composite_match                boolean     NOT NULL DEFAULT false,
    conflict_dominant_family_match          boolean     NOT NULL DEFAULT false,
    -- deltas / reasons / state
    conflict_delta                          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    conflict_composite_delta                jsonb       NOT NULL DEFAULT '{}'::jsonb,
    drift_reason_codes                      jsonb       NOT NULL DEFAULT '[]'::jsonb,
    validation_state                        text        NOT NULL DEFAULT 'validated'
                                                CHECK (validation_state IN (
                                                    'validated','drift_detected',
                                                    'insufficient_source','insufficient_replay',
                                                    'context_mismatch','timing_mismatch',
                                                    'transition_mismatch','archetype_mismatch',
                                                    'cluster_mismatch','persistence_mismatch',
                                                    'decay_mismatch','conflict_mismatch'
                                                )),
    metadata                                jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_conflict_replay_validation_scope_time_idx
    ON cross_asset_conflict_replay_validation_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_conflict_replay_validation_source_idx
    ON cross_asset_conflict_replay_validation_snapshots (source_run_id);
CREATE INDEX IF NOT EXISTS cross_asset_conflict_replay_validation_replay_idx
    ON cross_asset_conflict_replay_validation_snapshots (replay_run_id);
CREATE INDEX IF NOT EXISTS cross_asset_conflict_replay_validation_state_idx
    ON cross_asset_conflict_replay_validation_snapshots (validation_state);

-- ── B. Family Conflict Replay Stability Snapshots ───────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_conflict_replay_stability_snapshots (
    id                                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    source_run_id                                   uuid        NOT NULL,
    replay_run_id                                   uuid        NOT NULL,
    dependency_family                               text        NOT NULL,
    source_family_consensus_state                   text,
    replay_family_consensus_state                   text,
    source_agreement_score                          numeric,
    replay_agreement_score                          numeric,
    source_conflict_score                           numeric,
    replay_conflict_score                           numeric,
    source_dominant_conflict_source                 text,
    replay_dominant_conflict_source                 text,
    source_contribution_layer                       text,
    replay_contribution_layer                       text,
    source_scoring_version                          text,
    replay_scoring_version                          text,
    source_conflict_adjusted_contribution           numeric,
    replay_conflict_adjusted_contribution           numeric,
    source_conflict_integration_contribution        numeric,
    replay_conflict_integration_contribution        numeric,
    conflict_adjusted_delta                         numeric,
    conflict_integration_delta                      numeric,
    family_consensus_state_match                    boolean     NOT NULL DEFAULT false,
    agreement_score_match                           boolean     NOT NULL DEFAULT false,
    conflict_score_match                            boolean     NOT NULL DEFAULT false,
    dominant_conflict_source_match                  boolean     NOT NULL DEFAULT false,
    source_contribution_layer_match                 boolean     NOT NULL DEFAULT false,
    scoring_version_match                           boolean     NOT NULL DEFAULT false,
    conflict_family_rank_match                      boolean     NOT NULL DEFAULT false,
    conflict_composite_family_rank_match            boolean     NOT NULL DEFAULT false,
    drift_reason_codes                              jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_replay_scope_time_idx
    ON cross_asset_family_conflict_replay_stability_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_replay_pair_idx
    ON cross_asset_family_conflict_replay_stability_snapshots (source_run_id, replay_run_id);
CREATE INDEX IF NOT EXISTS cross_asset_family_conflict_replay_family_idx
    ON cross_asset_family_conflict_replay_stability_snapshots (dependency_family);

-- ── C. Conflict Replay Validation Summary view ──────────────────────────

CREATE OR REPLACE VIEW cross_asset_conflict_replay_validation_summary AS
WITH ranked AS (
    SELECT s.*,
        row_number() OVER (PARTITION BY s.source_run_id, s.replay_run_id ORDER BY s.created_at DESC) AS rn
    FROM cross_asset_conflict_replay_validation_snapshots s
)
SELECT
    workspace_id, watchlist_id,
    source_run_id, replay_run_id,
    source_context_snapshot_id, replay_context_snapshot_id,
    source_regime_key, replay_regime_key,
    source_dominant_timing_class, replay_dominant_timing_class,
    source_dominant_transition_state, replay_dominant_transition_state,
    source_dominant_sequence_class, replay_dominant_sequence_class,
    source_dominant_archetype_key, replay_dominant_archetype_key,
    source_cluster_state, replay_cluster_state,
    source_persistence_state, replay_persistence_state,
    source_freshness_state, replay_freshness_state,
    source_layer_consensus_state, replay_layer_consensus_state,
    source_agreement_score, replay_agreement_score,
    source_conflict_score, replay_conflict_score,
    source_dominant_conflict_source, replay_dominant_conflict_source,
    source_contribution_layer, replay_contribution_layer,
    source_composite_layer, replay_composite_layer,
    source_scoring_version, replay_scoring_version,
    context_hash_match, regime_match, timing_class_match,
    transition_state_match, sequence_class_match, archetype_match,
    cluster_state_match, persistence_state_match, freshness_state_match,
    layer_consensus_state_match, agreement_score_match, conflict_score_match,
    dominant_conflict_source_match,
    source_contribution_layer_match, source_composite_layer_match, scoring_version_match,
    conflict_attribution_match, conflict_composite_match, conflict_dominant_family_match,
    drift_reason_codes, validation_state, created_at
FROM ranked
WHERE rn = 1;

-- ── D. Family Conflict Replay Stability Summary view ────────────────────

CREATE OR REPLACE VIEW cross_asset_family_conflict_replay_stability_summary AS
WITH ranked AS (
    SELECT f.*,
        row_number() OVER (PARTITION BY f.source_run_id, f.replay_run_id, f.dependency_family ORDER BY f.created_at DESC) AS rn
    FROM cross_asset_family_conflict_replay_stability_snapshots f
)
SELECT
    workspace_id, watchlist_id,
    source_run_id, replay_run_id,
    dependency_family,
    source_family_consensus_state, replay_family_consensus_state,
    source_agreement_score, replay_agreement_score,
    source_conflict_score, replay_conflict_score,
    source_dominant_conflict_source, replay_dominant_conflict_source,
    source_contribution_layer, replay_contribution_layer,
    source_scoring_version, replay_scoring_version,
    source_conflict_adjusted_contribution, replay_conflict_adjusted_contribution,
    source_conflict_integration_contribution, replay_conflict_integration_contribution,
    conflict_adjusted_delta, conflict_integration_delta,
    family_consensus_state_match, agreement_score_match, conflict_score_match,
    dominant_conflict_source_match, source_contribution_layer_match, scoring_version_match,
    conflict_family_rank_match, conflict_composite_family_rank_match,
    drift_reason_codes, created_at
FROM ranked
WHERE rn = 1;

-- ── E. Conflict Replay Stability Aggregate view ─────────────────────────

CREATE OR REPLACE VIEW cross_asset_conflict_replay_stability_aggregate AS
WITH base AS (
    SELECT * FROM cross_asset_conflict_replay_validation_summary
)
SELECT
    workspace_id,
    count(*)::int                                                             AS validation_count,
    avg(CASE WHEN context_hash_match           THEN 1.0 ELSE 0.0 END)::numeric AS context_match_rate,
    avg(CASE WHEN regime_match                 THEN 1.0 ELSE 0.0 END)::numeric AS regime_match_rate,
    avg(CASE WHEN timing_class_match           THEN 1.0 ELSE 0.0 END)::numeric AS timing_class_match_rate,
    avg(CASE WHEN transition_state_match       THEN 1.0 ELSE 0.0 END)::numeric AS transition_state_match_rate,
    avg(CASE WHEN sequence_class_match         THEN 1.0 ELSE 0.0 END)::numeric AS sequence_class_match_rate,
    avg(CASE WHEN archetype_match              THEN 1.0 ELSE 0.0 END)::numeric AS archetype_match_rate,
    avg(CASE WHEN cluster_state_match          THEN 1.0 ELSE 0.0 END)::numeric AS cluster_state_match_rate,
    avg(CASE WHEN persistence_state_match      THEN 1.0 ELSE 0.0 END)::numeric AS persistence_state_match_rate,
    avg(CASE WHEN freshness_state_match        THEN 1.0 ELSE 0.0 END)::numeric AS freshness_state_match_rate,
    avg(CASE WHEN layer_consensus_state_match  THEN 1.0 ELSE 0.0 END)::numeric AS layer_consensus_state_match_rate,
    avg(CASE WHEN agreement_score_match        THEN 1.0 ELSE 0.0 END)::numeric AS agreement_score_match_rate,
    avg(CASE WHEN conflict_score_match         THEN 1.0 ELSE 0.0 END)::numeric AS conflict_score_match_rate,
    avg(CASE WHEN dominant_conflict_source_match THEN 1.0 ELSE 0.0 END)::numeric AS dominant_conflict_source_match_rate,
    avg(CASE WHEN source_contribution_layer_match THEN 1.0 ELSE 0.0 END)::numeric AS source_contribution_layer_match_rate,
    avg(CASE WHEN source_composite_layer_match THEN 1.0 ELSE 0.0 END)::numeric AS source_composite_layer_match_rate,
    avg(CASE WHEN scoring_version_match        THEN 1.0 ELSE 0.0 END)::numeric AS scoring_version_match_rate,
    avg(CASE WHEN conflict_attribution_match   THEN 1.0 ELSE 0.0 END)::numeric AS conflict_attribution_match_rate,
    avg(CASE WHEN conflict_composite_match     THEN 1.0 ELSE 0.0 END)::numeric AS conflict_composite_match_rate,
    avg(CASE WHEN conflict_dominant_family_match THEN 1.0 ELSE 0.0 END)::numeric AS conflict_dominant_family_match_rate,
    sum(CASE WHEN validation_state = 'drift_detected' THEN 1 ELSE 0 END)::int  AS drift_detected_count,
    max(created_at)                                                            AS latest_validated_at
FROM base
GROUP BY workspace_id;

commit;
