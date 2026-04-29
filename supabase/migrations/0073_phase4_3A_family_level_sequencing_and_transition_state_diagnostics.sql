-- Phase 4.3A: Family-Level Sequencing and Transition-State Diagnostics
-- Additive transition-state layer on top of 4.0C/4.0D/4.1A-4.2D. Persists
-- per-family state observations per run, discrete transition events vs the
-- prior run, and a short sequence signature per family over a recent window.
-- Descriptive-only — no forecasting or causal inference.

begin;

-- ── A. Family Transition-State Snapshots ────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_transition_state_snapshots (
    id                                 uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                       uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                       uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                             uuid        NOT NULL,
    context_snapshot_id                uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dependency_family                  text        NOT NULL,
    regime_key                         text,
    dominant_timing_class              text,
    signal_state                       text        NOT NULL
                                                    CHECK (signal_state IN (
                                                        'confirmed','unconfirmed','contradicted',
                                                        'missing_context','stale_context','insufficient_data'
                                                    )),
    transition_state                   text        NOT NULL
                                                    CHECK (transition_state IN (
                                                        'reinforcing','deteriorating','recovering',
                                                        'rotating_in','rotating_out','stable','insufficient_history'
                                                    )),
    family_contribution                numeric,
    timing_adjusted_contribution       numeric,
    timing_integration_contribution    numeric,
    family_rank                        integer,
    metadata                           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_transition_state_scope_time_idx
    ON cross_asset_family_transition_state_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_transition_state_run_idx
    ON cross_asset_family_transition_state_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_transition_state_family_idx
    ON cross_asset_family_transition_state_snapshots (dependency_family);

-- ── B. Family Transition-Event Snapshots ────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_transition_event_snapshots (
    id                           uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                 uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                 uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    source_run_id                uuid,
    target_run_id                uuid        NOT NULL,
    dependency_family            text        NOT NULL,
    prior_signal_state           text,
    current_signal_state         text        NOT NULL,
    prior_transition_state       text,
    current_transition_state     text        NOT NULL,
    prior_family_rank            integer,
    current_family_rank          integer,
    rank_delta                   integer,
    prior_family_contribution    numeric,
    current_family_contribution  numeric,
    contribution_delta           numeric,
    event_type                   text        NOT NULL
                                                CHECK (event_type IN (
                                                    'state_shift','rank_shift','dominance_gain','dominance_loss',
                                                    'recovery','degradation','timing_shift','regime_shift'
                                                )),
    metadata                     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_transition_event_scope_time_idx
    ON cross_asset_family_transition_event_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_transition_event_target_idx
    ON cross_asset_family_transition_event_snapshots (target_run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_transition_event_family_idx
    ON cross_asset_family_transition_event_snapshots (dependency_family);

-- ── C. Family Sequence Summary Snapshots ────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_sequence_summary_snapshots (
    id                         uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id               uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id               uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                     uuid,
    dependency_family          text        NOT NULL,
    window_label               text        NOT NULL DEFAULT 'recent_5',
    sequence_signature         text        NOT NULL,
    sequence_length            integer     NOT NULL,
    dominant_sequence_class    text        NOT NULL
                                            CHECK (dominant_sequence_class IN (
                                                'reinforcing_path','deteriorating_path','recovery_path',
                                                'rotation_path','mixed_path','insufficient_history'
                                            )),
    sequence_confidence        numeric,
    metadata                   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                 timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_sequence_summary_scope_time_idx
    ON cross_asset_family_sequence_summary_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_sequence_summary_run_idx
    ON cross_asset_family_sequence_summary_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_sequence_summary_family_idx
    ON cross_asset_family_sequence_summary_snapshots (dependency_family);

-- ── D. Transition-State Summary view ────────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_transition_state_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.run_id, s.dependency_family
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_family_transition_state_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    regime_key,
    dominant_timing_class,
    signal_state,
    transition_state,
    family_contribution,
    timing_adjusted_contribution,
    timing_integration_contribution,
    family_rank,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Transition-Event Summary view ────────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_transition_event_summary AS
WITH ranked AS (
    SELECT
        e.*,
        row_number() OVER (
            PARTITION BY e.target_run_id, e.dependency_family
            ORDER BY e.created_at DESC
        ) AS rn
    FROM cross_asset_family_transition_event_snapshots e
)
SELECT
    workspace_id,
    watchlist_id,
    source_run_id,
    target_run_id,
    dependency_family,
    prior_signal_state,
    current_signal_state,
    prior_transition_state,
    current_transition_state,
    prior_family_rank,
    current_family_rank,
    rank_delta,
    prior_family_contribution,
    current_family_contribution,
    contribution_delta,
    event_type,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Sequence Summary view ────────────────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_sequence_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY COALESCE(s.run_id::text, '__none__'), s.dependency_family
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_family_sequence_summary_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    dependency_family,
    window_label,
    sequence_signature,
    sequence_length,
    dominant_sequence_class,
    sequence_confidence,
    created_at
FROM ranked
WHERE rn = 1;

-- ── G. Run Transition Diagnostics Summary view ──────────────────────────
-- Aggregates transition-state + event + sequence data into one row per
-- run. dominant_transition_state / dominant_sequence_class are picked from
-- the family that holds the run's dominant_dependency_family slot.

CREATE OR REPLACE VIEW run_cross_asset_transition_diagnostics_summary AS
WITH event_counts AS (
    SELECT
        target_run_id AS run_id,
        count(*) FILTER (
            WHERE event_type IN ('dominance_gain','dominance_loss','rank_shift')
        )::int AS rotation_event_count,
        count(*) FILTER (WHERE event_type = 'degradation')::int AS degradation_event_count,
        count(*) FILTER (WHERE event_type = 'recovery')::int    AS recovery_event_count
    FROM cross_asset_family_transition_event_summary
    GROUP BY target_run_id
),
dom_state AS (
    SELECT
        s.run_id,
        s.workspace_id,
        s.watchlist_id,
        s.dependency_family  AS dominant_dependency_family,
        s.dominant_timing_class,
        s.transition_state   AS dominant_transition_state,
        s.created_at         AS state_created_at
    FROM cross_asset_family_transition_state_summary s
    WHERE s.family_rank = 1
),
dom_sequence AS (
    SELECT DISTINCT ON (run_id, dependency_family)
        run_id, dependency_family,
        dominant_sequence_class
    FROM cross_asset_family_sequence_summary
    ORDER BY run_id, dependency_family, created_at DESC
),
dom_joined AS (
    SELECT
        d.run_id,
        d.workspace_id,
        d.watchlist_id,
        d.dominant_dependency_family,
        d.dominant_timing_class,
        d.dominant_transition_state,
        ds.dominant_sequence_class,
        d.state_created_at
    FROM dom_state d
    LEFT JOIN dom_sequence ds
        ON ds.run_id = d.run_id
       AND ds.dependency_family = d.dominant_dependency_family
),
prior_dom AS (
    SELECT DISTINCT ON (target_run_id)
        target_run_id AS run_id,
        dependency_family AS prior_dominant_dependency_family
    FROM cross_asset_family_transition_event_summary
    WHERE event_type = 'dominance_loss'
    ORDER BY target_run_id, created_at DESC
)
SELECT
    d.run_id,
    d.workspace_id,
    d.watchlist_id,
    d.dominant_dependency_family,
    pd.prior_dominant_dependency_family,
    d.dominant_timing_class,
    d.dominant_transition_state,
    d.dominant_sequence_class,
    COALESCE(ec.rotation_event_count,    0) AS rotation_event_count,
    COALESCE(ec.degradation_event_count, 0) AS degradation_event_count,
    COALESCE(ec.recovery_event_count,    0) AS recovery_event_count,
    d.state_created_at                      AS created_at
FROM dom_joined d
LEFT JOIN event_counts  ec ON ec.run_id = d.run_id
LEFT JOIN prior_dom     pd ON pd.run_id = d.run_id;

commit;
