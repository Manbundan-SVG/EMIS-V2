-- Phase 4.1D: Cross-Asset Replay + Stability Validation
-- Additive validation layer on top of 4.1A/B/C. Persists deterministic
-- comparisons between a source run and its replay across raw / weighted /
-- regime-aware attribution layers, along with per-family deltas and
-- explicit drift reason codes. Does not modify any attribution layer.

begin;

-- ── A. Cross-Asset Replay Validation Snapshots ──────────────────────────
-- One row per (source_run, replay_run) comparison emission.

CREATE TABLE IF NOT EXISTS cross_asset_replay_validation_snapshots (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                        uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    source_run_id                       uuid        NOT NULL,
    replay_run_id                       uuid        NOT NULL,
    source_context_snapshot_id          uuid,
    replay_context_snapshot_id          uuid,
    source_regime_key                   text,
    replay_regime_key                   text,
    context_hash_match                  boolean     NOT NULL DEFAULT false,
    regime_match                        boolean     NOT NULL DEFAULT false,
    raw_attribution_match               boolean     NOT NULL DEFAULT false,
    weighted_attribution_match          boolean     NOT NULL DEFAULT false,
    regime_attribution_match            boolean     NOT NULL DEFAULT false,
    dominant_family_match               boolean     NOT NULL DEFAULT false,
    weighted_dominant_family_match      boolean     NOT NULL DEFAULT false,
    regime_dominant_family_match        boolean     NOT NULL DEFAULT false,
    raw_delta                           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    weighted_delta                      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    regime_delta                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    drift_reason_codes                  jsonb       NOT NULL DEFAULT '[]'::jsonb,
    validation_state                    text        NOT NULL DEFAULT 'validated'
                                                    CHECK (validation_state IN (
                                                        'validated','drift_detected',
                                                        'insufficient_source','insufficient_replay',
                                                        'context_mismatch'
                                                    )),
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_replay_validation_scope_time_idx
    ON cross_asset_replay_validation_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_replay_validation_source_idx
    ON cross_asset_replay_validation_snapshots (source_run_id);

CREATE INDEX IF NOT EXISTS cross_asset_replay_validation_replay_idx
    ON cross_asset_replay_validation_snapshots (replay_run_id);

-- ── B. Family Replay Stability Snapshots ────────────────────────────────
-- One row per (source_run, replay_run, dependency_family) comparison.

CREATE TABLE IF NOT EXISTS cross_asset_family_replay_stability_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    source_run_id                   uuid        NOT NULL,
    replay_run_id                   uuid        NOT NULL,
    dependency_family               text        NOT NULL,
    source_raw_contribution         numeric,
    replay_raw_contribution         numeric,
    source_weighted_contribution    numeric,
    replay_weighted_contribution    numeric,
    source_regime_contribution      numeric,
    replay_regime_contribution      numeric,
    raw_delta                       numeric,
    weighted_delta                  numeric,
    regime_delta                    numeric,
    family_rank_match               boolean     NOT NULL DEFAULT false,
    weighted_family_rank_match      boolean     NOT NULL DEFAULT false,
    regime_family_rank_match        boolean     NOT NULL DEFAULT false,
    drift_reason_codes              jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_replay_stability_scope_time_idx
    ON cross_asset_family_replay_stability_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_replay_stability_runs_idx
    ON cross_asset_family_replay_stability_snapshots (source_run_id, replay_run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_replay_stability_family_idx
    ON cross_asset_family_replay_stability_snapshots (dependency_family);

-- ── C. Replay Validation Summary view ───────────────────────────────────
-- Latest validation per (source_run, replay_run).

CREATE OR REPLACE VIEW cross_asset_replay_validation_summary AS
WITH ranked AS (
    SELECT
        v.*,
        row_number() OVER (
            PARTITION BY v.source_run_id, v.replay_run_id
            ORDER BY v.created_at DESC
        ) AS rn
    FROM cross_asset_replay_validation_snapshots v
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
    context_hash_match,
    regime_match,
    raw_attribution_match,
    weighted_attribution_match,
    regime_attribution_match,
    dominant_family_match,
    weighted_dominant_family_match,
    regime_dominant_family_match,
    drift_reason_codes,
    validation_state,
    created_at
FROM ranked
WHERE rn = 1;

-- ── D. Family Replay Stability Summary view ─────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_replay_stability_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.source_run_id, f.replay_run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_replay_stability_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    dependency_family,
    source_raw_contribution,
    replay_raw_contribution,
    source_weighted_contribution,
    replay_weighted_contribution,
    source_regime_contribution,
    replay_regime_contribution,
    raw_delta,
    weighted_delta,
    regime_delta,
    family_rank_match,
    weighted_family_rank_match,
    regime_family_rank_match,
    drift_reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Replay Stability Aggregate view ──────────────────────────────────
-- Workspace-level rollup over the latest batch of validations. Match rates
-- are computed per-workspace from cross_asset_replay_validation_summary
-- so each (source, replay) pair contributes exactly once.

CREATE OR REPLACE VIEW cross_asset_replay_stability_aggregate AS
SELECT
    s.workspace_id,
    count(*)::int                                                          AS validation_count,
    (avg(CASE WHEN s.context_hash_match                 THEN 1 ELSE 0 END))::numeric AS context_match_rate,
    (avg(CASE WHEN s.regime_match                       THEN 1 ELSE 0 END))::numeric AS regime_match_rate,
    (avg(CASE WHEN s.raw_attribution_match              THEN 1 ELSE 0 END))::numeric AS raw_match_rate,
    (avg(CASE WHEN s.weighted_attribution_match         THEN 1 ELSE 0 END))::numeric AS weighted_match_rate,
    (avg(CASE WHEN s.regime_attribution_match           THEN 1 ELSE 0 END))::numeric AS regime_match_rate_attribution,
    (avg(CASE WHEN s.dominant_family_match              THEN 1 ELSE 0 END))::numeric AS dominant_family_match_rate,
    (avg(CASE WHEN s.weighted_dominant_family_match     THEN 1 ELSE 0 END))::numeric AS weighted_dominant_family_match_rate,
    (avg(CASE WHEN s.regime_dominant_family_match       THEN 1 ELSE 0 END))::numeric AS regime_dominant_family_match_rate,
    (count(*) FILTER (WHERE s.validation_state = 'drift_detected'))::int   AS drift_detected_count,
    max(s.created_at)                                                      AS latest_validated_at
FROM cross_asset_replay_validation_summary s
GROUP BY s.workspace_id;

commit;
