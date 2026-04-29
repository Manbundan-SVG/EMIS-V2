-- Phase 4.7A: Signal Decay and Stale-Memory Diagnostics
-- Additive decay-diagnostic layer over the live 4.6A persistence summaries,
-- 4.6B persistence-aware attribution, 4.6C persistence-aware composite, and
-- 4.6D persistence replay-validation surfaces. Adds explicit freshness /
-- stale-memory / contradiction state per run and per dependency family,
-- plus discrete stale-memory event snapshots. Does not modify any
-- attribution, timing, sequencing, archetype, cluster, persistence, or
-- composite layer. Decay logic is deterministic, bounded, half-life based,
-- and metadata-stamped — no predictive forecasting.

begin;

-- ── A. Signal Decay Policy Profiles ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_signal_decay_policy_profiles (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                        text        NOT NULL,
    is_active                           boolean     NOT NULL DEFAULT true,
    regime_half_life_runs               integer     NOT NULL DEFAULT 12,
    timing_half_life_runs               integer     NOT NULL DEFAULT 8,
    transition_half_life_runs           integer     NOT NULL DEFAULT 6,
    archetype_half_life_runs            integer     NOT NULL DEFAULT 10,
    cluster_half_life_runs              integer     NOT NULL DEFAULT 10,
    persistence_half_life_runs          integer     NOT NULL DEFAULT 12,
    fresh_memory_threshold              numeric     NOT NULL DEFAULT 0.75,
    decaying_memory_threshold           numeric     NOT NULL DEFAULT 0.50,
    stale_memory_threshold              numeric     NOT NULL DEFAULT 0.30,
    contradiction_penalty_threshold     numeric     NOT NULL DEFAULT 0.50,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_signal_decay_policy_profiles_active_idx
    ON cross_asset_signal_decay_policy_profiles (workspace_id, is_active, created_at DESC);

-- ── B. Signal Decay Snapshots (per run/watchlist) ───────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_signal_decay_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                          uuid        NOT NULL,
    context_snapshot_id             uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    decay_policy_profile_id         uuid        REFERENCES cross_asset_signal_decay_policy_profiles(id) ON DELETE SET NULL,
    regime_key                      text,
    dominant_timing_class           text,
    dominant_transition_state       text,
    dominant_sequence_class         text,
    dominant_archetype_key          text,
    cluster_state                   text,
    persistence_state               text,
    current_state_signature         text        NOT NULL,
    state_age_runs                  integer,
    memory_score                    numeric,
    regime_decay_score              numeric,
    timing_decay_score              numeric,
    transition_decay_score          numeric,
    archetype_decay_score           numeric,
    cluster_decay_score             numeric,
    persistence_decay_score         numeric,
    aggregate_decay_score           numeric,
    freshness_state                 text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (freshness_state IN (
                                                    'fresh','decaying','stale',
                                                    'contradicted','mixed',
                                                    'insufficient_history'
                                                )),
    stale_memory_flag               boolean     NOT NULL DEFAULT false,
    contradiction_flag              boolean     NOT NULL DEFAULT false,
    reason_codes                    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_signal_decay_scope_time_idx
    ON cross_asset_signal_decay_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_signal_decay_run_idx
    ON cross_asset_signal_decay_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_signal_decay_freshness_idx
    ON cross_asset_signal_decay_snapshots (freshness_state);

CREATE INDEX IF NOT EXISTS cross_asset_signal_decay_stale_flag_idx
    ON cross_asset_signal_decay_snapshots (stale_memory_flag);

CREATE INDEX IF NOT EXISTS cross_asset_signal_decay_contradiction_flag_idx
    ON cross_asset_signal_decay_snapshots (contradiction_flag);

-- ── C. Family Signal Decay Snapshots (per run/family) ───────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_signal_decay_snapshots (
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
    family_rank                     integer,
    family_contribution             numeric,
    family_state_age_runs           integer,
    family_memory_score             numeric,
    family_decay_score              numeric,
    family_freshness_state          text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (family_freshness_state IN (
                                                    'fresh','decaying','stale',
                                                    'contradicted','mixed',
                                                    'insufficient_history'
                                                )),
    stale_family_memory_flag        boolean     NOT NULL DEFAULT false,
    contradicted_family_flag        boolean     NOT NULL DEFAULT false,
    reason_codes                    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_signal_decay_scope_time_idx
    ON cross_asset_family_signal_decay_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_signal_decay_run_idx
    ON cross_asset_family_signal_decay_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_signal_decay_family_idx
    ON cross_asset_family_signal_decay_snapshots (dependency_family);

CREATE INDEX IF NOT EXISTS cross_asset_family_signal_decay_freshness_idx
    ON cross_asset_family_signal_decay_snapshots (family_freshness_state);

-- ── D. Stale-Memory Event Snapshots ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_stale_memory_event_snapshots (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                        uuid        REFERENCES watchlists(id) ON DELETE CASCADE,
    source_run_id                       uuid,
    target_run_id                       uuid        NOT NULL,
    regime_key                          text,
    prior_freshness_state               text,
    current_freshness_state             text        NOT NULL,
    prior_state_signature               text,
    current_state_signature             text        NOT NULL,
    prior_memory_score                  numeric,
    current_memory_score                numeric,
    prior_aggregate_decay_score         numeric,
    current_aggregate_decay_score       numeric,
    event_type                          text        NOT NULL
                                                    CHECK (event_type IN (
                                                        'memory_freshened',
                                                        'memory_decayed',
                                                        'memory_became_stale',
                                                        'memory_contradicted',
                                                        'memory_reconfirmed',
                                                        'insufficient_history'
                                                    )),
    reason_codes                        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_stale_memory_event_scope_time_idx
    ON cross_asset_stale_memory_event_snapshots (workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_stale_memory_event_target_idx
    ON cross_asset_stale_memory_event_snapshots (target_run_id);

CREATE INDEX IF NOT EXISTS cross_asset_stale_memory_event_regime_idx
    ON cross_asset_stale_memory_event_snapshots (regime_key);

CREATE INDEX IF NOT EXISTS cross_asset_stale_memory_event_type_idx
    ON cross_asset_stale_memory_event_snapshots (event_type);

-- ── E. Signal Decay Summary view ────────────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_signal_decay_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.run_id
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_signal_decay_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    regime_key,
    dominant_timing_class,
    dominant_transition_state,
    dominant_sequence_class,
    dominant_archetype_key,
    cluster_state,
    persistence_state,
    current_state_signature,
    state_age_runs,
    memory_score,
    regime_decay_score,
    timing_decay_score,
    transition_decay_score,
    archetype_decay_score,
    cluster_decay_score,
    persistence_decay_score,
    aggregate_decay_score,
    freshness_state,
    stale_memory_flag,
    contradiction_flag,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Family Signal Decay Summary view ─────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_signal_decay_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_signal_decay_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    transition_state,
    dominant_sequence_class,
    archetype_key,
    cluster_state,
    persistence_state,
    family_rank,
    family_contribution,
    family_state_age_runs,
    family_memory_score,
    family_decay_score,
    family_freshness_state,
    stale_family_memory_flag,
    contradicted_family_flag,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── G. Stale-Memory Event Summary view ──────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_stale_memory_event_summary AS
WITH ranked AS (
    SELECT
        e.*,
        row_number() OVER (
            PARTITION BY e.target_run_id, e.event_type
            ORDER BY e.created_at DESC
        ) AS rn
    FROM cross_asset_stale_memory_event_snapshots e
)
SELECT
    workspace_id,
    watchlist_id,
    source_run_id,
    target_run_id,
    regime_key,
    prior_freshness_state,
    current_freshness_state,
    prior_state_signature,
    current_state_signature,
    prior_memory_score,
    current_memory_score,
    prior_aggregate_decay_score,
    current_aggregate_decay_score,
    event_type,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── H. Run Signal Decay Bridge view ─────────────────────────────────────
-- One compact run-linked decay row joining the 4.7A signal decay summary
-- with the latest stale-memory event for the same run.

CREATE OR REPLACE VIEW run_cross_asset_signal_decay_summary AS
WITH latest_event AS (
    SELECT DISTINCT ON (target_run_id)
        target_run_id,
        event_type AS latest_stale_memory_event_type,
        created_at AS latest_stale_memory_event_at
    FROM cross_asset_stale_memory_event_summary
    WHERE target_run_id IS NOT NULL
    ORDER BY target_run_id, created_at DESC
)
SELECT
    sd.run_id,
    sd.workspace_id,
    sd.watchlist_id,
    sd.regime_key,
    sd.persistence_state,
    sd.memory_score,
    sd.freshness_state,
    sd.aggregate_decay_score,
    sd.stale_memory_flag,
    sd.contradiction_flag,
    le.latest_stale_memory_event_type,
    sd.created_at
FROM cross_asset_signal_decay_summary sd
LEFT JOIN latest_event le ON le.target_run_id = sd.run_id;

commit;
