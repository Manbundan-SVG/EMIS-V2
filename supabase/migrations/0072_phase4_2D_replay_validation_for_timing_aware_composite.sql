-- Phase 4.2D: Replay Validation for Timing-Aware Composite Behavior
-- Additive replay-validation layer for the timing stack (4.2A/B/C). Mirrors
-- 4.1D's general replay validation pattern but focuses on timing-specific
-- layers: dominant timing class, timing-adjusted attribution, and timing-
-- aware composite integration. Does not modify any attribution or timing
-- layer.

begin;

-- ── A. Timing Replay Validation Snapshots ───────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_timing_replay_validation_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    source_run_id                   uuid        NOT NULL,
    replay_run_id                   uuid        NOT NULL,
    source_context_snapshot_id      uuid,
    replay_context_snapshot_id      uuid,
    source_regime_key               text,
    replay_regime_key               text,
    source_dominant_timing_class    text,
    replay_dominant_timing_class    text,
    context_hash_match              boolean     NOT NULL DEFAULT false,
    regime_match                    boolean     NOT NULL DEFAULT false,
    timing_class_match              boolean     NOT NULL DEFAULT false,
    timing_attribution_match        boolean     NOT NULL DEFAULT false,
    timing_composite_match          boolean     NOT NULL DEFAULT false,
    timing_dominant_family_match    boolean     NOT NULL DEFAULT false,
    timing_net_delta                jsonb       NOT NULL DEFAULT '{}'::jsonb,
    timing_composite_delta          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    drift_reason_codes              jsonb       NOT NULL DEFAULT '[]'::jsonb,
    validation_state                text        NOT NULL DEFAULT 'validated'
                                                CHECK (validation_state IN (
                                                    'validated','drift_detected',
                                                    'insufficient_source','insufficient_replay',
                                                    'context_mismatch','timing_mismatch'
                                                )),
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_timing_replay_validation_scope_time_idx
    ON cross_asset_timing_replay_validation_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_timing_replay_validation_source_idx
    ON cross_asset_timing_replay_validation_snapshots (source_run_id);

CREATE INDEX IF NOT EXISTS cross_asset_timing_replay_validation_replay_idx
    ON cross_asset_timing_replay_validation_snapshots (replay_run_id);

-- ── B. Family Timing Replay Stability Snapshots ─────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_timing_replay_stability_snapshots (
    id                                      uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                            uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                            uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    source_run_id                           uuid        NOT NULL,
    replay_run_id                           uuid        NOT NULL,
    dependency_family                       text        NOT NULL,
    source_dominant_timing_class            text,
    replay_dominant_timing_class            text,
    source_timing_adjusted_contribution     numeric,
    replay_timing_adjusted_contribution     numeric,
    source_timing_integration_contribution  numeric,
    replay_timing_integration_contribution  numeric,
    timing_adjusted_delta                   numeric,
    timing_integration_delta                numeric,
    timing_class_match                      boolean     NOT NULL DEFAULT false,
    timing_family_rank_match                boolean     NOT NULL DEFAULT false,
    timing_composite_family_rank_match      boolean     NOT NULL DEFAULT false,
    drift_reason_codes                      jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                                jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_replay_stability_scope_time_idx
    ON cross_asset_family_timing_replay_stability_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_replay_stability_runs_idx
    ON cross_asset_family_timing_replay_stability_snapshots (source_run_id, replay_run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_timing_replay_stability_family_idx
    ON cross_asset_family_timing_replay_stability_snapshots (dependency_family);

-- ── C. Timing Replay Validation Summary view ────────────────────────────

CREATE OR REPLACE VIEW cross_asset_timing_replay_validation_summary AS
WITH ranked AS (
    SELECT
        v.*,
        row_number() OVER (
            PARTITION BY v.source_run_id, v.replay_run_id
            ORDER BY v.created_at DESC
        ) AS rn
    FROM cross_asset_timing_replay_validation_snapshots v
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
    context_hash_match,
    regime_match,
    timing_class_match,
    timing_attribution_match,
    timing_composite_match,
    timing_dominant_family_match,
    drift_reason_codes,
    validation_state,
    created_at
FROM ranked
WHERE rn = 1;

-- ── D. Family Timing Replay Stability Summary view ──────────────────────

CREATE OR REPLACE VIEW cross_asset_family_timing_replay_stability_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.source_run_id, f.replay_run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_timing_replay_stability_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    dependency_family,
    source_dominant_timing_class,
    replay_dominant_timing_class,
    source_timing_adjusted_contribution,
    replay_timing_adjusted_contribution,
    source_timing_integration_contribution,
    replay_timing_integration_contribution,
    timing_adjusted_delta,
    timing_integration_delta,
    timing_class_match,
    timing_family_rank_match,
    timing_composite_family_rank_match,
    drift_reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Timing Replay Stability Aggregate view ───────────────────────────

CREATE OR REPLACE VIEW cross_asset_timing_replay_stability_aggregate AS
SELECT
    s.workspace_id,
    count(*)::int                                                                     AS validation_count,
    (avg(CASE WHEN s.context_hash_match            THEN 1 ELSE 0 END))::numeric AS context_match_rate,
    (avg(CASE WHEN s.regime_match                  THEN 1 ELSE 0 END))::numeric AS regime_match_rate,
    (avg(CASE WHEN s.timing_class_match            THEN 1 ELSE 0 END))::numeric AS timing_class_match_rate,
    (avg(CASE WHEN s.timing_attribution_match      THEN 1 ELSE 0 END))::numeric AS timing_attribution_match_rate,
    (avg(CASE WHEN s.timing_composite_match        THEN 1 ELSE 0 END))::numeric AS timing_composite_match_rate,
    (avg(CASE WHEN s.timing_dominant_family_match  THEN 1 ELSE 0 END))::numeric AS timing_dominant_family_match_rate,
    (count(*) FILTER (WHERE s.validation_state = 'drift_detected'))::int          AS drift_detected_count,
    max(s.created_at)                                                             AS latest_validated_at
FROM cross_asset_timing_replay_validation_summary s
GROUP BY s.workspace_id;

commit;
