-- Phase 4.5B: Cluster-Aware Attribution
-- Additive refinement layer on top of 4.1A raw / 4.1B weighted / 4.1C regime /
-- 4.2B timing / 4.3B transition / 4.4B archetype attribution. Conditions
-- family and symbol contribution on the 4.5A pattern-cluster diagnostics
-- (cluster_state, dominant_archetype_key, drift_score, pattern_entropy) with
-- bounded cluster weights and explicit cluster bonuses/penalties. Does not
-- modify any upstream attribution, archetype, or cluster surface.

begin;

-- ── A. Cluster Attribution Profiles ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_cluster_attribution_profiles (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                    text        NOT NULL,
    is_active                       boolean     NOT NULL DEFAULT true,
    stable_weight                   numeric     NOT NULL DEFAULT 1.08,
    rotating_weight                 numeric     NOT NULL DEFAULT 1.02,
    recovering_weight               numeric     NOT NULL DEFAULT 1.04,
    deteriorating_weight            numeric     NOT NULL DEFAULT 0.82,
    mixed_weight                    numeric     NOT NULL DEFAULT 0.90,
    insufficient_history_weight     numeric     NOT NULL DEFAULT 0.80,
    drift_penalty_scale             numeric     NOT NULL DEFAULT 1.0,
    rotation_bonus_scale            numeric     NOT NULL DEFAULT 1.0,
    recovery_bonus_scale            numeric     NOT NULL DEFAULT 1.0,
    entropy_penalty_scale           numeric     NOT NULL DEFAULT 1.0,
    cluster_family_overrides        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_cluster_attribution_profiles_scope_idx
    ON cross_asset_cluster_attribution_profiles (workspace_id, is_active, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS cross_asset_cluster_attribution_profiles_active_unique_idx
    ON cross_asset_cluster_attribution_profiles (workspace_id)
    WHERE is_active;

-- ── B. Family Cluster-Aware Attribution Snapshots ───────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_cluster_attribution_snapshots (
    id                                      uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                            uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                            uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                  uuid        NOT NULL,
    context_snapshot_id                     uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    cluster_profile_id                      uuid        REFERENCES cross_asset_cluster_attribution_profiles(id) ON DELETE SET NULL,
    dependency_family                       text        NOT NULL,
    raw_family_net_contribution             numeric,
    weighted_family_net_contribution        numeric,
    regime_adjusted_family_contribution     numeric,
    timing_adjusted_family_contribution     numeric,
    transition_adjusted_family_contribution numeric,
    archetype_adjusted_family_contribution  numeric,
    cluster_state                           text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (cluster_state IN (
                                                    'stable','rotating','deteriorating',
                                                    'recovering','mixed','insufficient_history'
                                                )),
    dominant_archetype_key                  text        NOT NULL DEFAULT 'insufficient_history',
    drift_score                             numeric,
    pattern_entropy                         numeric,
    cluster_weight                          numeric,
    cluster_bonus                           numeric,
    cluster_penalty                         numeric,
    cluster_adjusted_family_contribution    numeric,
    cluster_family_rank                     integer,
    top_symbols                             jsonb       NOT NULL DEFAULT '[]'::jsonb,
    reason_codes                            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                                jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_cluster_attribution_scope_time_idx
    ON cross_asset_family_cluster_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_cluster_attribution_run_idx
    ON cross_asset_family_cluster_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_cluster_attribution_family_idx
    ON cross_asset_family_cluster_attribution_snapshots (dependency_family);

CREATE INDEX IF NOT EXISTS cross_asset_family_cluster_attribution_state_idx
    ON cross_asset_family_cluster_attribution_snapshots (cluster_state);

-- ── C. Symbol Cluster-Aware Attribution Snapshots ───────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_symbol_cluster_attribution_snapshots (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                        uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                              uuid        NOT NULL,
    context_snapshot_id                 uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    cluster_profile_id                  uuid        REFERENCES cross_asset_cluster_attribution_profiles(id) ON DELETE SET NULL,
    symbol                              text        NOT NULL,
    dependency_family                   text        NOT NULL,
    dependency_type                     text,
    cluster_state                       text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (cluster_state IN (
                                                    'stable','rotating','deteriorating',
                                                    'recovering','mixed','insufficient_history'
                                                )),
    dominant_archetype_key              text        NOT NULL DEFAULT 'insufficient_history',
    raw_symbol_score                    numeric,
    weighted_symbol_score               numeric,
    regime_adjusted_symbol_score        numeric,
    timing_adjusted_symbol_score        numeric,
    transition_adjusted_symbol_score    numeric,
    archetype_adjusted_symbol_score     numeric,
    cluster_weight                      numeric,
    cluster_adjusted_symbol_score       numeric,
    symbol_rank                         integer,
    reason_codes                        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_cluster_attribution_scope_time_idx
    ON cross_asset_symbol_cluster_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_cluster_attribution_run_idx
    ON cross_asset_symbol_cluster_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_cluster_attribution_symbol_idx
    ON cross_asset_symbol_cluster_attribution_snapshots (symbol);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_cluster_attribution_state_idx
    ON cross_asset_symbol_cluster_attribution_snapshots (cluster_state);

-- ── D. Family Cluster-Aware Attribution Summary view ────────────────────

CREATE OR REPLACE VIEW cross_asset_family_cluster_attribution_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_cluster_attribution_snapshots f
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
    cluster_state,
    dominant_archetype_key,
    drift_score,
    pattern_entropy,
    cluster_weight,
    cluster_bonus,
    cluster_penalty,
    cluster_adjusted_family_contribution,
    cluster_family_rank,
    top_symbols,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Symbol Cluster-Aware Attribution Summary view ────────────────────

CREATE OR REPLACE VIEW cross_asset_symbol_cluster_attribution_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.run_id, s.symbol
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_symbol_cluster_attribution_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    symbol,
    dependency_family,
    dependency_type,
    cluster_state,
    dominant_archetype_key,
    raw_symbol_score,
    weighted_symbol_score,
    regime_adjusted_symbol_score,
    timing_adjusted_symbol_score,
    transition_adjusted_symbol_score,
    archetype_adjusted_symbol_score,
    cluster_weight,
    cluster_adjusted_symbol_score,
    symbol_rank,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Cluster-Aware Integration Bridge view ────────────────────────────

CREATE OR REPLACE VIEW run_cross_asset_cluster_attribution_summary AS
WITH cluster_totals AS (
    SELECT
        run_id,
        workspace_id,
        watchlist_id,
        context_snapshot_id,
        sum(cluster_adjusted_family_contribution)::numeric
            AS cluster_adjusted_cross_asset_contribution
    FROM cross_asset_family_cluster_attribution_summary
    GROUP BY run_id, workspace_id, watchlist_id, context_snapshot_id
),
cluster_dominant AS (
    SELECT DISTINCT ON (run_id)
        run_id,
        dependency_family AS cluster_dominant_dependency_family
    FROM cross_asset_family_cluster_attribution_summary
    WHERE cluster_family_rank = 1
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
    ct.cluster_adjusted_cross_asset_contribution,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    r.regime_dominant_dependency_family,
    t.timing_dominant_dependency_family,
    tra.transition_dominant_dependency_family,
    aa.archetype_dominant_dependency_family,
    cd.cluster_dominant_dependency_family,
    rpc.cluster_state,
    rpc.dominant_archetype_key,
    a.created_at
FROM cross_asset_attribution_summary a
LEFT JOIN run_cross_asset_weighted_integration_summary    w   ON w.run_id   = a.run_id
LEFT JOIN run_cross_asset_regime_integration_summary      r   ON r.run_id   = a.run_id
LEFT JOIN run_cross_asset_timing_attribution_summary      t   ON t.run_id   = a.run_id
LEFT JOIN run_cross_asset_transition_attribution_summary  tra ON tra.run_id = a.run_id
LEFT JOIN run_cross_asset_archetype_attribution_summary   aa  ON aa.run_id  = a.run_id
LEFT JOIN run_cross_asset_explanation_bridge              bridge ON bridge.run_id = a.run_id
LEFT JOIN cluster_totals    ct ON ct.run_id = a.run_id
LEFT JOIN cluster_dominant  cd ON cd.run_id = a.run_id
LEFT JOIN run_cross_asset_pattern_cluster_summary         rpc ON rpc.run_id = a.run_id;

commit;
