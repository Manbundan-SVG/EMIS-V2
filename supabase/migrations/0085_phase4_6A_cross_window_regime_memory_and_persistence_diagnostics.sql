-- Phase 4.6A: Cross-Window Regime Memory and Persistence Diagnostics
-- Additive cross-window memory + persistence diagnostic layer over the live
-- 4.1C regime, 4.2A timing, 4.3A transition, 4.4A archetype, 4.5A pattern-
-- cluster, and 4.5C cluster composite surfaces. Provides per-run state
-- persistence ratios + regime memory + persistence transition events. Does
-- not modify any classification, attribution, composite, or replay-
-- validation layer.

begin;

-- ── A. State Persistence Snapshots ──────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_state_persistence_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                          uuid        NOT NULL,
    context_snapshot_id             uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    window_label                    text        NOT NULL DEFAULT 'recent_20',
    regime_key                      text,
    dominant_timing_class           text,
    dominant_transition_state       text,
    dominant_sequence_class         text,
    dominant_archetype_key          text,
    cluster_state                   text,
    current_state_signature         text        NOT NULL,
    state_age_runs                  integer     NOT NULL DEFAULT 1,
    same_state_count                integer     NOT NULL DEFAULT 1,
    state_persistence_ratio         numeric,
    regime_persistence_ratio        numeric,
    cluster_persistence_ratio       numeric,
    archetype_persistence_ratio     numeric,
    persistence_state               text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (persistence_state IN (
                                                    'persistent','fragile','rotating',
                                                    'breaking_down','recovering','mixed',
                                                    'insufficient_history'
                                                )),
    memory_score                    numeric,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_state_persistence_scope_time_idx
    ON cross_asset_state_persistence_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_state_persistence_run_idx
    ON cross_asset_state_persistence_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_state_persistence_regime_idx
    ON cross_asset_state_persistence_snapshots (regime_key);

CREATE INDEX IF NOT EXISTS cross_asset_state_persistence_cluster_idx
    ON cross_asset_state_persistence_snapshots (cluster_state);

CREATE INDEX IF NOT EXISTS cross_asset_state_persistence_archetype_idx
    ON cross_asset_state_persistence_snapshots (dominant_archetype_key);

-- ── B. Regime Memory Snapshots ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_regime_memory_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    regime_key                      text        NOT NULL,
    window_label                    text        NOT NULL DEFAULT 'recent_50',
    run_count                       integer     NOT NULL DEFAULT 0,
    same_regime_streak_count        integer     NOT NULL DEFAULT 0,
    regime_switch_count             integer     NOT NULL DEFAULT 0,
    avg_regime_duration_runs        numeric,
    max_regime_duration_runs        integer,
    regime_memory_score             numeric,
    dominant_cluster_state          text,
    dominant_archetype_key          text,
    persistence_state               text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (persistence_state IN (
                                                    'persistent','fragile','rotating',
                                                    'breaking_down','recovering','mixed',
                                                    'insufficient_history'
                                                )),
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_regime_memory_scope_idx
    ON cross_asset_regime_memory_snapshots (workspace_id, regime_key, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_regime_memory_state_idx
    ON cross_asset_regime_memory_snapshots (persistence_state);

CREATE INDEX IF NOT EXISTS cross_asset_regime_memory_cluster_idx
    ON cross_asset_regime_memory_snapshots (dominant_cluster_state);

-- ── C. Persistence Transition Event Snapshots ───────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_persistence_transition_event_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                    uuid        REFERENCES watchlists(id) ON DELETE CASCADE,
    source_run_id                   uuid,
    target_run_id                   uuid        NOT NULL,
    regime_key                      text,
    prior_state_signature           text,
    current_state_signature         text        NOT NULL,
    prior_persistence_state         text,
    current_persistence_state       text        NOT NULL,
    prior_memory_score              numeric,
    current_memory_score            numeric,
    memory_score_delta              numeric,
    event_type                      text        NOT NULL
                                                CHECK (event_type IN (
                                                    'persistence_gain','persistence_loss',
                                                    'regime_memory_break','cluster_memory_break',
                                                    'archetype_memory_break',
                                                    'state_rotation','stabilization',
                                                    'insufficient_history'
                                                )),
    reason_codes                    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_transition_event_scope_time_idx
    ON cross_asset_persistence_transition_event_snapshots (workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_transition_event_target_idx
    ON cross_asset_persistence_transition_event_snapshots (target_run_id);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_transition_event_regime_idx
    ON cross_asset_persistence_transition_event_snapshots (regime_key);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_transition_event_type_idx
    ON cross_asset_persistence_transition_event_snapshots (event_type);

-- ── D. State Persistence Summary view ───────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_state_persistence_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.run_id
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_state_persistence_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    window_label,
    regime_key,
    dominant_timing_class,
    dominant_transition_state,
    dominant_sequence_class,
    dominant_archetype_key,
    cluster_state,
    current_state_signature,
    state_age_runs,
    same_state_count,
    state_persistence_ratio,
    regime_persistence_ratio,
    cluster_persistence_ratio,
    archetype_persistence_ratio,
    persistence_state,
    memory_score,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Regime Memory Summary view ───────────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_regime_memory_summary AS
WITH ranked AS (
    SELECT
        r.*,
        row_number() OVER (
            PARTITION BY r.workspace_id, r.regime_key, r.window_label
            ORDER BY r.created_at DESC
        ) AS rn
    FROM cross_asset_regime_memory_snapshots r
)
SELECT
    workspace_id,
    regime_key,
    window_label,
    run_count,
    same_regime_streak_count,
    regime_switch_count,
    avg_regime_duration_runs,
    max_regime_duration_runs,
    regime_memory_score,
    dominant_cluster_state,
    dominant_archetype_key,
    persistence_state,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Persistence Transition Event Summary view ────────────────────────

CREATE OR REPLACE VIEW cross_asset_persistence_transition_event_summary AS
WITH ranked AS (
    SELECT
        e.*,
        row_number() OVER (
            PARTITION BY e.target_run_id, e.event_type
            ORDER BY e.created_at DESC
        ) AS rn
    FROM cross_asset_persistence_transition_event_snapshots e
)
SELECT
    workspace_id,
    watchlist_id,
    source_run_id,
    target_run_id,
    regime_key,
    prior_state_signature,
    current_state_signature,
    prior_persistence_state,
    current_persistence_state,
    prior_memory_score,
    current_memory_score,
    memory_score_delta,
    event_type,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── G. Run Persistence Bridge view ──────────────────────────────────────
-- Joins the 4.6A state-persistence summary + latest persistence event for
-- the run into one row per run for run inspection.

CREATE OR REPLACE VIEW run_cross_asset_persistence_summary AS
WITH latest_event AS (
    SELECT DISTINCT ON (target_run_id)
        target_run_id,
        event_type AS latest_persistence_event_type,
        created_at AS latest_persistence_event_at
    FROM cross_asset_persistence_transition_event_summary
    WHERE target_run_id IS NOT NULL
    ORDER BY target_run_id, created_at DESC
)
SELECT
    sp.run_id,
    sp.workspace_id,
    sp.watchlist_id,
    sp.regime_key,
    sp.cluster_state,
    sp.dominant_archetype_key,
    sp.persistence_state,
    sp.memory_score,
    sp.state_age_runs,
    sp.state_persistence_ratio,
    le.latest_persistence_event_type,
    sp.created_at
FROM cross_asset_state_persistence_summary sp
LEFT JOIN latest_event le ON le.target_run_id = sp.run_id;

commit;
