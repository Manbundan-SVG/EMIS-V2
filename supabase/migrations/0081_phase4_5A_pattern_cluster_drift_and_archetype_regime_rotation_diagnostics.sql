-- Phase 4.5A: Pattern-Cluster Drift and Archetype Regime Rotation Diagnostics
-- Additive cluster/drift/rotation diagnostic layer over the live 4.4A archetype
-- summaries, 4.4B archetype-aware attribution, and 4.4C archetype-aware
-- composite. Provides recent-window archetype distributions, regime-conditioned
-- archetype rotation summaries, and discrete drift-event records. Does not
-- modify any classification, attribution, composite, or replay-validation layer.

begin;

-- ── A. Archetype Cluster Snapshots ──────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_archetype_cluster_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                          uuid        NOT NULL,
    context_snapshot_id             uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    regime_key                      text,
    window_label                    text        NOT NULL DEFAULT 'recent_10',
    dominant_archetype_key          text        NOT NULL DEFAULT 'insufficient_history',
    archetype_mix                   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    reinforcement_share             numeric,
    recovery_share                  numeric,
    rotation_share                  numeric,
    degradation_share               numeric,
    mixed_share                     numeric,
    pattern_entropy                 numeric,
    cluster_state                   text        NOT NULL DEFAULT 'stable'
                                                CHECK (cluster_state IN (
                                                    'stable','rotating','deteriorating',
                                                    'recovering','mixed','insufficient_history'
                                                )),
    drift_score                     numeric,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_archetype_cluster_scope_time_idx
    ON cross_asset_archetype_cluster_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_archetype_cluster_run_idx
    ON cross_asset_archetype_cluster_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_archetype_cluster_regime_idx
    ON cross_asset_archetype_cluster_snapshots (regime_key);

CREATE INDEX IF NOT EXISTS cross_asset_archetype_cluster_archetype_idx
    ON cross_asset_archetype_cluster_snapshots (dominant_archetype_key);

-- ── B. Archetype Regime Rotation Snapshots ──────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_archetype_regime_rotation_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    regime_key                      text        NOT NULL,
    window_label                    text        NOT NULL DEFAULT 'recent_25',
    prior_dominant_archetype_key    text,
    current_dominant_archetype_key  text,
    rotation_count                  integer     NOT NULL DEFAULT 0,
    reinforcement_run_count         integer     NOT NULL DEFAULT 0,
    recovery_run_count              integer     NOT NULL DEFAULT 0,
    degradation_run_count           integer     NOT NULL DEFAULT 0,
    mixed_run_count                 integer     NOT NULL DEFAULT 0,
    rotation_state                  text        NOT NULL DEFAULT 'stable'
                                                CHECK (rotation_state IN (
                                                    'stable','rotating','deteriorating',
                                                    'recovering','mixed','insufficient_history'
                                                )),
    regime_drift_score              numeric,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_archetype_regime_rotation_scope_idx
    ON cross_asset_archetype_regime_rotation_snapshots (workspace_id, regime_key, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_archetype_regime_rotation_regime_idx
    ON cross_asset_archetype_regime_rotation_snapshots (regime_key);

CREATE INDEX IF NOT EXISTS cross_asset_archetype_regime_rotation_archetype_idx
    ON cross_asset_archetype_regime_rotation_snapshots (current_dominant_archetype_key);

-- ── C. Pattern Drift Event Snapshots ────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_pattern_drift_event_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                    uuid        REFERENCES watchlists(id) ON DELETE CASCADE,
    source_run_id                   uuid,
    target_run_id                   uuid,
    regime_key                      text,
    prior_cluster_state             text,
    current_cluster_state           text        NOT NULL,
    prior_dominant_archetype_key    text,
    current_dominant_archetype_key  text        NOT NULL,
    drift_event_type                text        NOT NULL
                                                CHECK (drift_event_type IN (
                                                    'archetype_rotation','reinforcement_break',
                                                    'recovery_break','degradation_acceleration',
                                                    'mixed_noise_increase','stabilization',
                                                    'insufficient_history'
                                                )),
    drift_score                     numeric,
    reason_codes                    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_pattern_drift_event_scope_time_idx
    ON cross_asset_pattern_drift_event_snapshots (workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_pattern_drift_event_target_idx
    ON cross_asset_pattern_drift_event_snapshots (target_run_id);

CREATE INDEX IF NOT EXISTS cross_asset_pattern_drift_event_regime_idx
    ON cross_asset_pattern_drift_event_snapshots (regime_key);

CREATE INDEX IF NOT EXISTS cross_asset_pattern_drift_event_type_idx
    ON cross_asset_pattern_drift_event_snapshots (drift_event_type);

-- ── D. Archetype Cluster Summary view ───────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_archetype_cluster_summary AS
WITH ranked AS (
    SELECT
        c.*,
        row_number() OVER (
            PARTITION BY c.run_id
            ORDER BY c.created_at DESC
        ) AS rn
    FROM cross_asset_archetype_cluster_snapshots c
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    regime_key,
    window_label,
    dominant_archetype_key,
    archetype_mix,
    reinforcement_share,
    recovery_share,
    rotation_share,
    degradation_share,
    mixed_share,
    pattern_entropy,
    cluster_state,
    drift_score,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Archetype Regime Rotation Summary view ───────────────────────────

CREATE OR REPLACE VIEW cross_asset_archetype_regime_rotation_summary AS
WITH ranked AS (
    SELECT
        r.*,
        row_number() OVER (
            PARTITION BY r.workspace_id, r.regime_key, r.window_label
            ORDER BY r.created_at DESC
        ) AS rn
    FROM cross_asset_archetype_regime_rotation_snapshots r
)
SELECT
    workspace_id,
    regime_key,
    window_label,
    prior_dominant_archetype_key,
    current_dominant_archetype_key,
    rotation_count,
    reinforcement_run_count,
    recovery_run_count,
    degradation_run_count,
    mixed_run_count,
    rotation_state,
    regime_drift_score,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Pattern Drift Event Summary view ─────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_pattern_drift_event_summary AS
WITH ranked AS (
    SELECT
        d.*,
        row_number() OVER (
            PARTITION BY d.target_run_id, d.drift_event_type
            ORDER BY d.created_at DESC
        ) AS rn
    FROM cross_asset_pattern_drift_event_snapshots d
)
SELECT
    workspace_id,
    watchlist_id,
    source_run_id,
    target_run_id,
    regime_key,
    prior_cluster_state,
    current_cluster_state,
    prior_dominant_archetype_key,
    current_dominant_archetype_key,
    drift_event_type,
    drift_score,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── G. Run Pattern-Cluster Bridge view ──────────────────────────────────
-- Joins the 4.5A cluster summary, latest drift event for the run, and the
-- regime rotation state for the run's regime into one row per run.

CREATE OR REPLACE VIEW run_cross_asset_pattern_cluster_summary AS
WITH latest_drift AS (
    SELECT DISTINCT ON (target_run_id)
        target_run_id,
        drift_event_type AS latest_drift_event_type,
        created_at       AS latest_drift_event_at
    FROM cross_asset_pattern_drift_event_summary
    WHERE target_run_id IS NOT NULL
    ORDER BY target_run_id, created_at DESC
)
SELECT
    cs.run_id,
    cs.workspace_id,
    cs.watchlist_id,
    cs.regime_key,
    cs.dominant_archetype_key,
    cs.cluster_state,
    cs.drift_score,
    cs.pattern_entropy,
    rr.rotation_state              AS current_rotation_state,
    ld.latest_drift_event_type,
    cs.created_at
FROM cross_asset_archetype_cluster_summary cs
LEFT JOIN cross_asset_archetype_regime_rotation_summary rr
       ON rr.workspace_id = cs.workspace_id
      AND rr.regime_key   = cs.regime_key
LEFT JOIN latest_drift ld
       ON ld.target_run_id = cs.run_id;

commit;
