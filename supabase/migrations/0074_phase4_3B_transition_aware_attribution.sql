-- Phase 4.3B: Transition-Aware Attribution
-- Additive refinement layer on top of 4.1A raw / 4.1B weighted / 4.1C regime /
-- 4.2B timing-aware attribution. Conditions family and symbol contributions
-- on 4.3A transition state + sequence class, with bounded multipliers and
-- explicit bonuses/penalties. Does not modify any upstream attribution.

begin;

-- ── A. Transition Attribution Profiles ──────────────────────────────────
-- One active profile per workspace. Partial UNIQUE enforces at-most-one-active.

CREATE TABLE IF NOT EXISTS cross_asset_transition_attribution_profiles (
    id                             uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                   uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                   text        NOT NULL,
    is_active                      boolean     NOT NULL DEFAULT true,
    reinforcing_weight             numeric     NOT NULL DEFAULT 1.10,
    stable_weight                  numeric     NOT NULL DEFAULT 1.00,
    recovering_weight              numeric     NOT NULL DEFAULT 1.03,
    rotating_in_weight             numeric     NOT NULL DEFAULT 1.08,
    rotating_out_weight            numeric     NOT NULL DEFAULT 0.90,
    deteriorating_weight           numeric     NOT NULL DEFAULT 0.85,
    insufficient_history_weight    numeric     NOT NULL DEFAULT 0.80,
    recovery_bonus_scale           numeric     NOT NULL DEFAULT 1.0,
    degradation_penalty_scale      numeric     NOT NULL DEFAULT 1.0,
    rotation_bonus_scale           numeric     NOT NULL DEFAULT 1.0,
    sequence_class_overrides       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    family_weight_overrides        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    metadata                       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_transition_attribution_profiles_scope_idx
    ON cross_asset_transition_attribution_profiles (workspace_id, is_active, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS cross_asset_transition_attribution_profiles_active_unique_idx
    ON cross_asset_transition_attribution_profiles (workspace_id)
    WHERE is_active;

-- ── B. Family Transition-Aware Attribution Snapshots ────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_transition_attribution_snapshots (
    id                                    uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                          uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                          uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                uuid        NOT NULL,
    context_snapshot_id                   uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    transition_profile_id                 uuid        REFERENCES cross_asset_transition_attribution_profiles(id) ON DELETE SET NULL,
    dependency_family                     text        NOT NULL,
    raw_family_net_contribution           numeric,
    weighted_family_net_contribution      numeric,
    regime_adjusted_family_contribution   numeric,
    timing_adjusted_family_contribution   numeric,
    transition_state                      text        NOT NULL DEFAULT 'insufficient_history'
                                                        CHECK (transition_state IN (
                                                            'reinforcing','deteriorating','recovering',
                                                            'rotating_in','rotating_out','stable','insufficient_history'
                                                        )),
    dominant_sequence_class               text        NOT NULL DEFAULT 'insufficient_history'
                                                        CHECK (dominant_sequence_class IN (
                                                            'reinforcing_path','deteriorating_path','recovery_path',
                                                            'rotation_path','mixed_path','insufficient_history'
                                                        )),
    transition_state_weight               numeric,
    sequence_class_weight                 numeric,
    transition_bonus                      numeric,
    transition_penalty                    numeric,
    transition_adjusted_family_contribution numeric,
    transition_family_rank                integer,
    top_symbols                           jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                              jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_transition_attribution_scope_time_idx
    ON cross_asset_family_transition_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_transition_attribution_run_idx
    ON cross_asset_family_transition_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_transition_attribution_family_idx
    ON cross_asset_family_transition_attribution_snapshots (dependency_family);

-- ── C. Symbol Transition-Aware Attribution Snapshots ────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_symbol_transition_attribution_snapshots (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                        uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                              uuid        NOT NULL,
    context_snapshot_id                 uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    transition_profile_id               uuid        REFERENCES cross_asset_transition_attribution_profiles(id) ON DELETE SET NULL,
    symbol                              text        NOT NULL,
    dependency_family                   text        NOT NULL,
    dependency_type                     text,
    transition_state                    text        NOT NULL DEFAULT 'insufficient_history'
                                                        CHECK (transition_state IN (
                                                            'reinforcing','deteriorating','recovering',
                                                            'rotating_in','rotating_out','stable','insufficient_history'
                                                        )),
    dominant_sequence_class             text        NOT NULL DEFAULT 'insufficient_history'
                                                        CHECK (dominant_sequence_class IN (
                                                            'reinforcing_path','deteriorating_path','recovery_path',
                                                            'rotation_path','mixed_path','insufficient_history'
                                                        )),
    raw_symbol_score                    numeric,
    weighted_symbol_score               numeric,
    regime_adjusted_symbol_score        numeric,
    timing_adjusted_symbol_score        numeric,
    transition_state_weight             numeric,
    sequence_class_weight               numeric,
    transition_adjusted_symbol_score    numeric,
    symbol_rank                         integer,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_transition_attribution_scope_time_idx
    ON cross_asset_symbol_transition_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_transition_attribution_run_idx
    ON cross_asset_symbol_transition_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_transition_attribution_symbol_idx
    ON cross_asset_symbol_transition_attribution_snapshots (symbol);

-- ── D. Family Transition-Aware Attribution Summary view ─────────────────

CREATE OR REPLACE VIEW cross_asset_family_transition_attribution_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_transition_attribution_snapshots f
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
    transition_state,
    dominant_sequence_class,
    transition_state_weight,
    sequence_class_weight,
    transition_bonus,
    transition_penalty,
    transition_adjusted_family_contribution,
    transition_family_rank,
    top_symbols,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Symbol Transition-Aware Attribution Summary view ─────────────────

CREATE OR REPLACE VIEW cross_asset_symbol_transition_attribution_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.run_id, s.symbol
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_symbol_transition_attribution_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    symbol,
    dependency_family,
    dependency_type,
    transition_state,
    dominant_sequence_class,
    raw_symbol_score,
    weighted_symbol_score,
    regime_adjusted_symbol_score,
    timing_adjusted_symbol_score,
    transition_state_weight,
    sequence_class_weight,
    transition_adjusted_symbol_score,
    symbol_rank,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Transition-Aware Integration Bridge view ─────────────────────────
-- Joins raw / weighted / regime / timing aggregates + 4.3B transition
-- totals per run for run inspection.

CREATE OR REPLACE VIEW run_cross_asset_transition_attribution_summary AS
WITH transition_totals AS (
    SELECT
        run_id,
        workspace_id,
        watchlist_id,
        context_snapshot_id,
        sum(transition_adjusted_family_contribution)::numeric
            AS transition_adjusted_cross_asset_contribution
    FROM cross_asset_family_transition_attribution_summary
    GROUP BY run_id, workspace_id, watchlist_id, context_snapshot_id
),
transition_dominant AS (
    SELECT DISTINCT ON (run_id)
        run_id,
        dependency_family AS transition_dominant_dependency_family,
        transition_state  AS dominant_transition_state,
        dominant_sequence_class
    FROM cross_asset_family_transition_attribution_summary
    WHERE transition_family_rank = 1
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
    tt.transition_adjusted_cross_asset_contribution,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    r.regime_dominant_dependency_family,
    t.timing_dominant_dependency_family,
    td.transition_dominant_dependency_family,
    td.dominant_transition_state,
    td.dominant_sequence_class,
    a.created_at
FROM cross_asset_attribution_summary a
LEFT JOIN run_cross_asset_weighted_integration_summary   w  ON w.run_id  = a.run_id
LEFT JOIN run_cross_asset_regime_integration_summary     r  ON r.run_id  = a.run_id
LEFT JOIN run_cross_asset_timing_attribution_summary     t  ON t.run_id  = a.run_id
LEFT JOIN run_cross_asset_explanation_bridge             bridge ON bridge.run_id = a.run_id
LEFT JOIN transition_totals    tt ON tt.run_id = a.run_id
LEFT JOIN transition_dominant  td ON td.run_id = a.run_id;

commit;
