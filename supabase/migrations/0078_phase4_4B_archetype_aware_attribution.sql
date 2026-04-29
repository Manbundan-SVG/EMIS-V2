-- Phase 4.4B: Archetype-Aware Attribution
-- Additive refinement layer on top of 4.1A raw / 4.1B weighted / 4.1C regime /
-- 4.2B timing / 4.3B transition attribution. Conditions family and symbol
-- contribution on the 4.4A archetype classification with bounded archetype
-- weights + explicit archetype-specific bonuses and penalties. Does not
-- modify any upstream attribution, transition, or archetype surface.

begin;

-- ── A. Archetype Attribution Profiles ───────────────────────────────────
-- One active profile per workspace. Partial UNIQUE enforces at-most-one-active.

CREATE TABLE IF NOT EXISTS cross_asset_archetype_attribution_profiles (
    id                                uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                      uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                      text        NOT NULL,
    is_active                         boolean     NOT NULL DEFAULT true,
    rotation_handoff_weight           numeric     NOT NULL DEFAULT 1.03,
    reinforcing_continuation_weight   numeric     NOT NULL DEFAULT 1.10,
    recovering_reentry_weight         numeric     NOT NULL DEFAULT 1.05,
    deteriorating_breakdown_weight    numeric     NOT NULL DEFAULT 0.82,
    mixed_transition_noise_weight     numeric     NOT NULL DEFAULT 0.90,
    insufficient_history_weight       numeric     NOT NULL DEFAULT 0.80,
    recovery_bonus_scale              numeric     NOT NULL DEFAULT 1.0,
    breakdown_penalty_scale           numeric     NOT NULL DEFAULT 1.0,
    rotation_bonus_scale              numeric     NOT NULL DEFAULT 1.0,
    archetype_family_overrides        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    metadata                          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_archetype_attribution_profiles_scope_idx
    ON cross_asset_archetype_attribution_profiles (workspace_id, is_active, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS cross_asset_archetype_attribution_profiles_active_unique_idx
    ON cross_asset_archetype_attribution_profiles (workspace_id)
    WHERE is_active;

-- ── B. Family Archetype-Aware Attribution Snapshots ─────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_archetype_attribution_snapshots (
    id                                      uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                            uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                            uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                  uuid        NOT NULL,
    context_snapshot_id                     uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    archetype_profile_id                    uuid        REFERENCES cross_asset_archetype_attribution_profiles(id) ON DELETE SET NULL,
    dependency_family                       text        NOT NULL,
    raw_family_net_contribution             numeric,
    weighted_family_net_contribution        numeric,
    regime_adjusted_family_contribution     numeric,
    timing_adjusted_family_contribution     numeric,
    transition_adjusted_family_contribution numeric,
    archetype_key                           text        NOT NULL DEFAULT 'insufficient_history',
    transition_state                        text        NOT NULL DEFAULT 'insufficient_history'
                                                        CHECK (transition_state IN (
                                                            'reinforcing','deteriorating','recovering',
                                                            'rotating_in','rotating_out','stable','insufficient_history'
                                                        )),
    dominant_sequence_class                 text        NOT NULL DEFAULT 'insufficient_history'
                                                        CHECK (dominant_sequence_class IN (
                                                            'reinforcing_path','deteriorating_path','recovery_path',
                                                            'rotation_path','mixed_path','insufficient_history'
                                                        )),
    archetype_weight                        numeric,
    archetype_bonus                         numeric,
    archetype_penalty                       numeric,
    archetype_adjusted_family_contribution  numeric,
    archetype_family_rank                   integer,
    top_symbols                             jsonb       NOT NULL DEFAULT '[]'::jsonb,
    classification_reason_codes             jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                                jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_archetype_attribution_scope_time_idx
    ON cross_asset_family_archetype_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_archetype_attribution_run_idx
    ON cross_asset_family_archetype_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_archetype_attribution_family_idx
    ON cross_asset_family_archetype_attribution_snapshots (dependency_family);

CREATE INDEX IF NOT EXISTS cross_asset_family_archetype_attribution_archetype_idx
    ON cross_asset_family_archetype_attribution_snapshots (archetype_key);

-- ── C. Symbol Archetype-Aware Attribution Snapshots ─────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_symbol_archetype_attribution_snapshots (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                        uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                              uuid        NOT NULL,
    context_snapshot_id                 uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    archetype_profile_id                uuid        REFERENCES cross_asset_archetype_attribution_profiles(id) ON DELETE SET NULL,
    symbol                              text        NOT NULL,
    dependency_family                   text        NOT NULL,
    dependency_type                     text,
    archetype_key                       text        NOT NULL DEFAULT 'insufficient_history',
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
    transition_adjusted_symbol_score    numeric,
    archetype_weight                    numeric,
    archetype_adjusted_symbol_score     numeric,
    symbol_rank                         integer,
    classification_reason_codes         jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_archetype_attribution_scope_time_idx
    ON cross_asset_symbol_archetype_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_archetype_attribution_run_idx
    ON cross_asset_symbol_archetype_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_archetype_attribution_symbol_idx
    ON cross_asset_symbol_archetype_attribution_snapshots (symbol);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_archetype_attribution_archetype_idx
    ON cross_asset_symbol_archetype_attribution_snapshots (archetype_key);

-- ── D. Family Archetype-Aware Attribution Summary view ──────────────────

CREATE OR REPLACE VIEW cross_asset_family_archetype_attribution_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_archetype_attribution_snapshots f
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
    transition_adjusted_family_contribution,
    archetype_key,
    transition_state,
    dominant_sequence_class,
    archetype_weight,
    archetype_bonus,
    archetype_penalty,
    archetype_adjusted_family_contribution,
    archetype_family_rank,
    top_symbols,
    classification_reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Symbol Archetype-Aware Attribution Summary view ──────────────────

CREATE OR REPLACE VIEW cross_asset_symbol_archetype_attribution_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.run_id, s.symbol
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_symbol_archetype_attribution_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    symbol,
    dependency_family,
    dependency_type,
    archetype_key,
    transition_state,
    dominant_sequence_class,
    raw_symbol_score,
    weighted_symbol_score,
    regime_adjusted_symbol_score,
    timing_adjusted_symbol_score,
    transition_adjusted_symbol_score,
    archetype_weight,
    archetype_adjusted_symbol_score,
    symbol_rank,
    classification_reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Archetype-Aware Integration Bridge view ──────────────────────────

CREATE OR REPLACE VIEW run_cross_asset_archetype_attribution_summary AS
WITH archetype_totals AS (
    SELECT
        run_id,
        workspace_id,
        watchlist_id,
        context_snapshot_id,
        sum(archetype_adjusted_family_contribution)::numeric
            AS archetype_adjusted_cross_asset_contribution
    FROM cross_asset_family_archetype_attribution_summary
    GROUP BY run_id, workspace_id, watchlist_id, context_snapshot_id
),
archetype_dominant AS (
    SELECT DISTINCT ON (run_id)
        run_id,
        dependency_family AS archetype_dominant_dependency_family
    FROM cross_asset_family_archetype_attribution_summary
    WHERE archetype_family_rank = 1
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
    tra.transition_adjusted_cross_asset_contribution,
    at.archetype_adjusted_cross_asset_contribution,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    r.regime_dominant_dependency_family,
    t.timing_dominant_dependency_family,
    tra.transition_dominant_dependency_family,
    ad.archetype_dominant_dependency_family,
    rs.dominant_archetype_key,
    a.created_at
FROM cross_asset_attribution_summary a
LEFT JOIN run_cross_asset_weighted_integration_summary    w   ON w.run_id   = a.run_id
LEFT JOIN run_cross_asset_regime_integration_summary      r   ON r.run_id   = a.run_id
LEFT JOIN run_cross_asset_timing_attribution_summary      t   ON t.run_id   = a.run_id
LEFT JOIN run_cross_asset_transition_attribution_summary  tra ON tra.run_id = a.run_id
LEFT JOIN run_cross_asset_explanation_bridge              bridge ON bridge.run_id = a.run_id
LEFT JOIN archetype_totals    at ON at.run_id = a.run_id
LEFT JOIN archetype_dominant  ad ON ad.run_id = a.run_id
LEFT JOIN cross_asset_run_archetype_summary               rs  ON rs.run_id  = a.run_id;

commit;
