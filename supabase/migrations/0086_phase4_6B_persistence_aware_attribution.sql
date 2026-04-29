-- Phase 4.6B: Persistence-Aware Attribution
-- Additive refinement layer on top of 4.1A raw / 4.1B weighted / 4.1C regime /
-- 4.2B timing / 4.3B transition / 4.4B archetype / 4.5B cluster attribution.
-- Conditions family and symbol contribution on the 4.6A persistence
-- diagnostics (persistence_state, memory_score, state_age_runs,
-- latest_persistence_event_type) with bounded persistence weights and
-- explicit memory-driven bonuses/penalties. Does not modify any upstream
-- attribution, archetype, cluster, or persistence surface.

begin;

-- ── A. Persistence Attribution Profiles ─────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_persistence_attribution_profiles (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                    text        NOT NULL,
    is_active                       boolean     NOT NULL DEFAULT true,
    persistent_weight               numeric     NOT NULL DEFAULT 1.08,
    recovering_weight               numeric     NOT NULL DEFAULT 1.04,
    rotating_weight                 numeric     NOT NULL DEFAULT 0.98,
    fragile_weight                  numeric     NOT NULL DEFAULT 0.88,
    breaking_down_weight            numeric     NOT NULL DEFAULT 0.80,
    mixed_weight                    numeric     NOT NULL DEFAULT 0.90,
    insufficient_history_weight     numeric     NOT NULL DEFAULT 0.80,
    memory_score_boost_scale        numeric     NOT NULL DEFAULT 1.0,
    memory_break_penalty_scale      numeric     NOT NULL DEFAULT 1.0,
    stabilization_bonus_scale       numeric     NOT NULL DEFAULT 1.0,
    state_age_bonus_scale           numeric     NOT NULL DEFAULT 1.0,
    persistence_family_overrides    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_attribution_profiles_scope_idx
    ON cross_asset_persistence_attribution_profiles (workspace_id, is_active, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS cross_asset_persistence_attribution_profiles_active_unique_idx
    ON cross_asset_persistence_attribution_profiles (workspace_id)
    WHERE is_active;

-- ── B. Family Persistence-Aware Attribution Snapshots ───────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_persistence_attribution_snapshots (
    id                                          uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                                uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                                uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                      uuid        NOT NULL,
    context_snapshot_id                         uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    persistence_profile_id                      uuid        REFERENCES cross_asset_persistence_attribution_profiles(id) ON DELETE SET NULL,
    dependency_family                           text        NOT NULL,
    raw_family_net_contribution                 numeric,
    weighted_family_net_contribution            numeric,
    regime_adjusted_family_contribution         numeric,
    timing_adjusted_family_contribution         numeric,
    transition_adjusted_family_contribution     numeric,
    archetype_adjusted_family_contribution      numeric,
    cluster_adjusted_family_contribution        numeric,
    persistence_state                           text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (persistence_state IN (
                                                    'persistent','fragile','rotating',
                                                    'breaking_down','recovering','mixed',
                                                    'insufficient_history'
                                                )),
    memory_score                                numeric,
    state_age_runs                              integer,
    state_persistence_ratio                     numeric,
    regime_persistence_ratio                    numeric,
    cluster_persistence_ratio                   numeric,
    archetype_persistence_ratio                 numeric,
    latest_persistence_event_type               text,
    persistence_weight                          numeric,
    persistence_bonus                           numeric,
    persistence_penalty                         numeric,
    persistence_adjusted_family_contribution    numeric,
    persistence_family_rank                     integer,
    top_symbols                                 jsonb       NOT NULL DEFAULT '[]'::jsonb,
    reason_codes                                jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                                    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_persistence_attribution_scope_time_idx
    ON cross_asset_family_persistence_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_persistence_attribution_run_idx
    ON cross_asset_family_persistence_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_persistence_attribution_family_idx
    ON cross_asset_family_persistence_attribution_snapshots (dependency_family);

CREATE INDEX IF NOT EXISTS cross_asset_family_persistence_attribution_state_idx
    ON cross_asset_family_persistence_attribution_snapshots (persistence_state);

-- ── C. Symbol Persistence-Aware Attribution Snapshots ───────────────────

CREATE TABLE IF NOT EXISTS cross_asset_symbol_persistence_attribution_snapshots (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                        uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                              uuid        NOT NULL,
    context_snapshot_id                 uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    persistence_profile_id              uuid        REFERENCES cross_asset_persistence_attribution_profiles(id) ON DELETE SET NULL,
    symbol                              text        NOT NULL,
    dependency_family                   text        NOT NULL,
    dependency_type                     text,
    persistence_state                   text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (persistence_state IN (
                                                    'persistent','fragile','rotating',
                                                    'breaking_down','recovering','mixed',
                                                    'insufficient_history'
                                                )),
    memory_score                        numeric,
    state_age_runs                      integer,
    latest_persistence_event_type       text,
    raw_symbol_score                    numeric,
    weighted_symbol_score               numeric,
    regime_adjusted_symbol_score        numeric,
    timing_adjusted_symbol_score        numeric,
    transition_adjusted_symbol_score    numeric,
    archetype_adjusted_symbol_score     numeric,
    cluster_adjusted_symbol_score       numeric,
    persistence_weight                  numeric,
    persistence_adjusted_symbol_score   numeric,
    symbol_rank                         integer,
    reason_codes                        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_persistence_attribution_scope_time_idx
    ON cross_asset_symbol_persistence_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_persistence_attribution_run_idx
    ON cross_asset_symbol_persistence_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_persistence_attribution_symbol_idx
    ON cross_asset_symbol_persistence_attribution_snapshots (symbol);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_persistence_attribution_state_idx
    ON cross_asset_symbol_persistence_attribution_snapshots (persistence_state);

-- ── D. Family Persistence-Aware Attribution Summary view ────────────────

CREATE OR REPLACE VIEW cross_asset_family_persistence_attribution_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_persistence_attribution_snapshots f
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
    archetype_adjusted_family_contribution,
    cluster_adjusted_family_contribution,
    persistence_state,
    memory_score,
    state_age_runs,
    state_persistence_ratio,
    regime_persistence_ratio,
    cluster_persistence_ratio,
    archetype_persistence_ratio,
    latest_persistence_event_type,
    persistence_weight,
    persistence_bonus,
    persistence_penalty,
    persistence_adjusted_family_contribution,
    persistence_family_rank,
    top_symbols,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Symbol Persistence-Aware Attribution Summary view ────────────────

CREATE OR REPLACE VIEW cross_asset_symbol_persistence_attribution_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.run_id, s.symbol
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_symbol_persistence_attribution_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    symbol,
    dependency_family,
    dependency_type,
    persistence_state,
    memory_score,
    state_age_runs,
    latest_persistence_event_type,
    raw_symbol_score,
    weighted_symbol_score,
    regime_adjusted_symbol_score,
    timing_adjusted_symbol_score,
    transition_adjusted_symbol_score,
    archetype_adjusted_symbol_score,
    cluster_adjusted_symbol_score,
    persistence_weight,
    persistence_adjusted_symbol_score,
    symbol_rank,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Persistence-Aware Integration Bridge view ────────────────────────

CREATE OR REPLACE VIEW run_cross_asset_persistence_attribution_summary AS
WITH persistence_totals AS (
    SELECT
        run_id,
        workspace_id,
        watchlist_id,
        context_snapshot_id,
        sum(persistence_adjusted_family_contribution)::numeric
            AS persistence_adjusted_cross_asset_contribution
    FROM cross_asset_family_persistence_attribution_summary
    GROUP BY run_id, workspace_id, watchlist_id, context_snapshot_id
),
persistence_dominant AS (
    SELECT DISTINCT ON (run_id)
        run_id,
        dependency_family AS persistence_dominant_dependency_family
    FROM cross_asset_family_persistence_attribution_summary
    WHERE persistence_family_rank = 1
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
    aa.archetype_adjusted_cross_asset_contribution,
    ca.cluster_adjusted_cross_asset_contribution,
    pt.persistence_adjusted_cross_asset_contribution,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    r.regime_dominant_dependency_family,
    t.timing_dominant_dependency_family,
    tra.transition_dominant_dependency_family,
    aa.archetype_dominant_dependency_family,
    ca.cluster_dominant_dependency_family,
    pd.persistence_dominant_dependency_family,
    rps.persistence_state,
    rps.memory_score,
    rps.state_age_runs,
    rps.latest_persistence_event_type,
    a.created_at
FROM cross_asset_attribution_summary a
LEFT JOIN run_cross_asset_weighted_integration_summary    w   ON w.run_id   = a.run_id
LEFT JOIN run_cross_asset_regime_integration_summary      r   ON r.run_id   = a.run_id
LEFT JOIN run_cross_asset_timing_attribution_summary      t   ON t.run_id   = a.run_id
LEFT JOIN run_cross_asset_transition_attribution_summary  tra ON tra.run_id = a.run_id
LEFT JOIN run_cross_asset_archetype_attribution_summary   aa  ON aa.run_id  = a.run_id
LEFT JOIN run_cross_asset_cluster_attribution_summary     ca  ON ca.run_id  = a.run_id
LEFT JOIN run_cross_asset_explanation_bridge              bridge ON bridge.run_id = a.run_id
LEFT JOIN persistence_totals     pt ON pt.run_id = a.run_id
LEFT JOIN persistence_dominant   pd ON pd.run_id = a.run_id
LEFT JOIN run_cross_asset_persistence_summary             rps ON rps.run_id = a.run_id;

commit;
