-- Phase 4.6D: Replay Validation for Persistence-Aware Composite Behavior
-- Additive replay-validation layer for the persistence stack (4.6A/B/C).
-- Mirrors 4.5D's cluster replay validation pattern but extends comparisons
-- to persistence_state, memory_score, state_age_runs, and latest_persistence_event_type,
-- with an explicit persistence_mismatch validation_state. Does not modify
-- any attribution, timing, sequencing, archetype, cluster, persistence, or
-- composite layer.

begin;

-- ── A. Persistence Replay Validation Snapshots ──────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_persistence_replay_validation_snapshots (
    id                                    uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                          uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                          uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    source_run_id                         uuid        NOT NULL,
    replay_run_id                         uuid        NOT NULL,
    source_context_snapshot_id            uuid,
    replay_context_snapshot_id            uuid,
    source_regime_key                     text,
    replay_regime_key                     text,
    source_dominant_timing_class          text,
    replay_dominant_timing_class          text,
    source_dominant_transition_state      text,
    replay_dominant_transition_state      text,
    source_dominant_sequence_class        text,
    replay_dominant_sequence_class        text,
    source_dominant_archetype_key         text,
    replay_dominant_archetype_key         text,
    source_cluster_state                  text,
    replay_cluster_state                  text,
    source_persistence_state              text,
    replay_persistence_state              text,
    source_memory_score                   numeric,
    replay_memory_score                   numeric,
    source_state_age_runs                 integer,
    replay_state_age_runs                 integer,
    source_latest_persistence_event_type  text,
    replay_latest_persistence_event_type  text,
    context_hash_match                    boolean     NOT NULL DEFAULT false,
    regime_match                          boolean     NOT NULL DEFAULT false,
    timing_class_match                    boolean     NOT NULL DEFAULT false,
    transition_state_match                boolean     NOT NULL DEFAULT false,
    sequence_class_match                  boolean     NOT NULL DEFAULT false,
    archetype_match                       boolean     NOT NULL DEFAULT false,
    cluster_state_match                   boolean     NOT NULL DEFAULT false,
    persistence_state_match               boolean     NOT NULL DEFAULT false,
    memory_score_match                    boolean     NOT NULL DEFAULT false,
    state_age_match                       boolean     NOT NULL DEFAULT false,
    persistence_event_match               boolean     NOT NULL DEFAULT false,
    persistence_attribution_match         boolean     NOT NULL DEFAULT false,
    persistence_composite_match           boolean     NOT NULL DEFAULT false,
    persistence_dominant_family_match     boolean     NOT NULL DEFAULT false,
    persistence_delta                     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    persistence_composite_delta           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    drift_reason_codes                    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    validation_state                      text        NOT NULL DEFAULT 'validated'
                                                CHECK (validation_state IN (
                                                    'validated','drift_detected',
                                                    'insufficient_source','insufficient_replay',
                                                    'context_mismatch','timing_mismatch',
                                                    'transition_mismatch','archetype_mismatch',
                                                    'cluster_mismatch','persistence_mismatch'
                                                )),
    metadata                              jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_replay_validation_scope_time_idx
    ON cross_asset_persistence_replay_validation_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_replay_validation_source_idx
    ON cross_asset_persistence_replay_validation_snapshots (source_run_id);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_replay_validation_replay_idx
    ON cross_asset_persistence_replay_validation_snapshots (replay_run_id);

-- ── B. Family Persistence Replay Stability Snapshots ────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_persistence_replay_stability_snapshots (
    id                                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    source_run_id                                   uuid        NOT NULL,
    replay_run_id                                   uuid        NOT NULL,
    dependency_family                               text        NOT NULL,
    source_persistence_state                        text,
    replay_persistence_state                        text,
    source_memory_score                             numeric,
    replay_memory_score                             numeric,
    source_state_age_runs                           integer,
    replay_state_age_runs                           integer,
    source_latest_persistence_event_type            text,
    replay_latest_persistence_event_type            text,
    source_persistence_adjusted_contribution        numeric,
    replay_persistence_adjusted_contribution        numeric,
    source_persistence_integration_contribution     numeric,
    replay_persistence_integration_contribution     numeric,
    persistence_adjusted_delta                      numeric,
    persistence_integration_delta                   numeric,
    persistence_state_match                         boolean     NOT NULL DEFAULT false,
    memory_score_match                              boolean     NOT NULL DEFAULT false,
    state_age_match                                 boolean     NOT NULL DEFAULT false,
    persistence_event_match                         boolean     NOT NULL DEFAULT false,
    persistence_family_rank_match                   boolean     NOT NULL DEFAULT false,
    persistence_composite_family_rank_match         boolean     NOT NULL DEFAULT false,
    drift_reason_codes                              jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_persistence_replay_stability_scope_time_idx
    ON cross_asset_family_persistence_replay_stability_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_persistence_replay_stability_runs_idx
    ON cross_asset_family_persistence_replay_stability_snapshots (source_run_id, replay_run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_persistence_replay_stability_family_idx
    ON cross_asset_family_persistence_replay_stability_snapshots (dependency_family);

-- ── C. Persistence Replay Validation Summary view ───────────────────────

CREATE OR REPLACE VIEW cross_asset_persistence_replay_validation_summary AS
WITH ranked AS (
    SELECT
        v.*,
        row_number() OVER (
            PARTITION BY v.source_run_id, v.replay_run_id
            ORDER BY v.created_at DESC
        ) AS rn
    FROM cross_asset_persistence_replay_validation_snapshots v
)
SELECT
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    source_context_snapshot_id,
    replay_context_snapshot_id,
    source_regime_key,
    replay_regime_key,
    source_dominant_timing_class,
    replay_dominant_timing_class,
    source_dominant_transition_state,
    replay_dominant_transition_state,
    source_dominant_sequence_class,
    replay_dominant_sequence_class,
    source_dominant_archetype_key,
    replay_dominant_archetype_key,
    source_cluster_state,
    replay_cluster_state,
    source_persistence_state,
    replay_persistence_state,
    source_memory_score,
    replay_memory_score,
    source_state_age_runs,
    replay_state_age_runs,
    source_latest_persistence_event_type,
    replay_latest_persistence_event_type,
    context_hash_match,
    regime_match,
    timing_class_match,
    transition_state_match,
    sequence_class_match,
    archetype_match,
    cluster_state_match,
    persistence_state_match,
    memory_score_match,
    state_age_match,
    persistence_event_match,
    persistence_attribution_match,
    persistence_composite_match,
    persistence_dominant_family_match,
    drift_reason_codes,
    validation_state,
    created_at
FROM ranked
WHERE rn = 1;

-- ── D. Family Persistence Replay Stability Summary view ─────────────────

CREATE OR REPLACE VIEW cross_asset_family_persistence_replay_stability_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.source_run_id, f.replay_run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_persistence_replay_stability_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    dependency_family,
    source_persistence_state,
    replay_persistence_state,
    source_memory_score,
    replay_memory_score,
    source_state_age_runs,
    replay_state_age_runs,
    source_latest_persistence_event_type,
    replay_latest_persistence_event_type,
    source_persistence_adjusted_contribution,
    replay_persistence_adjusted_contribution,
    source_persistence_integration_contribution,
    replay_persistence_integration_contribution,
    persistence_adjusted_delta,
    persistence_integration_delta,
    persistence_state_match,
    memory_score_match,
    state_age_match,
    persistence_event_match,
    persistence_family_rank_match,
    persistence_composite_family_rank_match,
    drift_reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Persistence Replay Stability Aggregate view ──────────────────────

CREATE OR REPLACE VIEW cross_asset_persistence_replay_stability_aggregate AS
SELECT
    s.workspace_id,
    count(*)::int                                                                                  AS validation_count,
    (avg(CASE WHEN s.context_hash_match                  THEN 1 ELSE 0 END))::numeric              AS context_match_rate,
    (avg(CASE WHEN s.regime_match                        THEN 1 ELSE 0 END))::numeric              AS regime_match_rate,
    (avg(CASE WHEN s.timing_class_match                  THEN 1 ELSE 0 END))::numeric              AS timing_class_match_rate,
    (avg(CASE WHEN s.transition_state_match              THEN 1 ELSE 0 END))::numeric              AS transition_state_match_rate,
    (avg(CASE WHEN s.sequence_class_match                THEN 1 ELSE 0 END))::numeric              AS sequence_class_match_rate,
    (avg(CASE WHEN s.archetype_match                     THEN 1 ELSE 0 END))::numeric              AS archetype_match_rate,
    (avg(CASE WHEN s.cluster_state_match                 THEN 1 ELSE 0 END))::numeric              AS cluster_state_match_rate,
    (avg(CASE WHEN s.persistence_state_match             THEN 1 ELSE 0 END))::numeric              AS persistence_state_match_rate,
    (avg(CASE WHEN s.memory_score_match                  THEN 1 ELSE 0 END))::numeric              AS memory_score_match_rate,
    (avg(CASE WHEN s.state_age_match                     THEN 1 ELSE 0 END))::numeric              AS state_age_match_rate,
    (avg(CASE WHEN s.persistence_event_match             THEN 1 ELSE 0 END))::numeric              AS persistence_event_match_rate,
    (avg(CASE WHEN s.persistence_attribution_match       THEN 1 ELSE 0 END))::numeric              AS persistence_attribution_match_rate,
    (avg(CASE WHEN s.persistence_composite_match         THEN 1 ELSE 0 END))::numeric              AS persistence_composite_match_rate,
    (avg(CASE WHEN s.persistence_dominant_family_match   THEN 1 ELSE 0 END))::numeric              AS persistence_dominant_family_match_rate,
    (count(*) FILTER (WHERE s.validation_state = 'drift_detected'))::int                           AS drift_detected_count,
    max(s.created_at)                                                                              AS latest_validated_at
FROM cross_asset_persistence_replay_validation_summary s
GROUP BY s.workspace_id;

commit;
