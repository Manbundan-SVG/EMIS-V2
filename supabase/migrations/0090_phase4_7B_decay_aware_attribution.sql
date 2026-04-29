-- Phase 4.7B: Decay-Aware Attribution
-- Additive refinement layer on top of the live 4.6B persistence-aware
-- attribution and 4.7A signal-decay diagnostics. Conditions family and
-- symbol contribution on freshness state, aggregate decay score, stale-memory
-- flag, and contradiction flag with bounded weights and explicit
-- bonuses/penalties. Does not modify any upstream attribution, persistence,
-- or signal-decay surface.

begin;

-- ── A. Decay Attribution Profiles ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_decay_attribution_profiles (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                    text        NOT NULL,
    is_active                       boolean     NOT NULL DEFAULT true,
    fresh_weight                    numeric     NOT NULL DEFAULT 1.08,
    decaying_weight                 numeric     NOT NULL DEFAULT 0.98,
    stale_weight                    numeric     NOT NULL DEFAULT 0.82,
    contradicted_weight             numeric     NOT NULL DEFAULT 0.65,
    mixed_weight                    numeric     NOT NULL DEFAULT 0.88,
    insufficient_history_weight     numeric     NOT NULL DEFAULT 0.80,
    freshness_bonus_scale           numeric     NOT NULL DEFAULT 1.0,
    stale_penalty_scale             numeric     NOT NULL DEFAULT 1.0,
    contradiction_penalty_scale     numeric     NOT NULL DEFAULT 1.0,
    decay_score_penalty_scale       numeric     NOT NULL DEFAULT 1.0,
    decay_family_overrides          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_decay_attribution_profiles_active_idx
    ON cross_asset_decay_attribution_profiles (workspace_id, is_active, created_at DESC);

-- ── B. Family Decay-Aware Attribution Snapshots ─────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_decay_attribution_snapshots (
    id                                          uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                                uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                                uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                      uuid        NOT NULL,
    context_snapshot_id                         uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    decay_profile_id                            uuid        REFERENCES cross_asset_decay_attribution_profiles(id) ON DELETE SET NULL,
    dependency_family                           text        NOT NULL,
    raw_family_net_contribution                 numeric,
    weighted_family_net_contribution            numeric,
    regime_adjusted_family_contribution         numeric,
    timing_adjusted_family_contribution         numeric,
    transition_adjusted_family_contribution     numeric,
    archetype_adjusted_family_contribution      numeric,
    cluster_adjusted_family_contribution        numeric,
    persistence_adjusted_family_contribution    numeric,
    freshness_state                             text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (freshness_state IN (
                                                    'fresh','decaying','stale','contradicted','mixed','insufficient_history')),
    aggregate_decay_score                       numeric,
    family_decay_score                          numeric,
    memory_score                                numeric,
    state_age_runs                              integer,
    stale_memory_flag                           boolean     NOT NULL DEFAULT false,
    contradiction_flag                          boolean     NOT NULL DEFAULT false,
    decay_weight                                numeric,
    decay_bonus                                 numeric,
    decay_penalty                               numeric,
    decay_adjusted_family_contribution          numeric,
    decay_family_rank                           integer,
    top_symbols                                 jsonb       NOT NULL DEFAULT '[]'::jsonb,
    reason_codes                                jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                                    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_decay_attribution_scope_time_idx
    ON cross_asset_family_decay_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_family_decay_attribution_run_idx
    ON cross_asset_family_decay_attribution_snapshots (run_id);
CREATE INDEX IF NOT EXISTS cross_asset_family_decay_attribution_family_idx
    ON cross_asset_family_decay_attribution_snapshots (dependency_family);
CREATE INDEX IF NOT EXISTS cross_asset_family_decay_attribution_freshness_idx
    ON cross_asset_family_decay_attribution_snapshots (freshness_state);
CREATE INDEX IF NOT EXISTS cross_asset_family_decay_attribution_stale_flag_idx
    ON cross_asset_family_decay_attribution_snapshots (stale_memory_flag);
CREATE INDEX IF NOT EXISTS cross_asset_family_decay_attribution_contradiction_flag_idx
    ON cross_asset_family_decay_attribution_snapshots (contradiction_flag);

-- ── C. Symbol Decay-Aware Attribution Snapshots ─────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_symbol_decay_attribution_snapshots (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                        uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                              uuid        NOT NULL,
    context_snapshot_id                 uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    decay_profile_id                    uuid        REFERENCES cross_asset_decay_attribution_profiles(id) ON DELETE SET NULL,
    symbol                              text        NOT NULL,
    dependency_family                   text        NOT NULL,
    dependency_type                     text,
    freshness_state                     text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (freshness_state IN (
                                                    'fresh','decaying','stale','contradicted','mixed','insufficient_history')),
    aggregate_decay_score               numeric,
    family_decay_score                  numeric,
    memory_score                        numeric,
    state_age_runs                      integer,
    stale_memory_flag                   boolean     NOT NULL DEFAULT false,
    contradiction_flag                  boolean     NOT NULL DEFAULT false,
    raw_symbol_score                    numeric,
    weighted_symbol_score               numeric,
    regime_adjusted_symbol_score        numeric,
    timing_adjusted_symbol_score        numeric,
    transition_adjusted_symbol_score    numeric,
    archetype_adjusted_symbol_score     numeric,
    cluster_adjusted_symbol_score       numeric,
    persistence_adjusted_symbol_score   numeric,
    decay_weight                        numeric,
    decay_adjusted_symbol_score         numeric,
    symbol_rank                         integer,
    reason_codes                        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_decay_attribution_scope_time_idx
    ON cross_asset_symbol_decay_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);
CREATE INDEX IF NOT EXISTS cross_asset_symbol_decay_attribution_run_idx
    ON cross_asset_symbol_decay_attribution_snapshots (run_id);
CREATE INDEX IF NOT EXISTS cross_asset_symbol_decay_attribution_symbol_idx
    ON cross_asset_symbol_decay_attribution_snapshots (symbol);
CREATE INDEX IF NOT EXISTS cross_asset_symbol_decay_attribution_freshness_idx
    ON cross_asset_symbol_decay_attribution_snapshots (freshness_state);
CREATE INDEX IF NOT EXISTS cross_asset_symbol_decay_attribution_contradiction_flag_idx
    ON cross_asset_symbol_decay_attribution_snapshots (contradiction_flag);

-- ── D. Family Decay-Aware Attribution Summary view ──────────────────────

CREATE OR REPLACE VIEW cross_asset_family_decay_attribution_summary AS
WITH ranked AS (
    SELECT f.*,
        row_number() OVER (PARTITION BY f.run_id, f.dependency_family ORDER BY f.created_at DESC) AS rn
    FROM cross_asset_family_decay_attribution_snapshots f
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
    persistence_adjusted_family_contribution,
    freshness_state,
    aggregate_decay_score,
    family_decay_score,
    memory_score,
    state_age_runs,
    stale_memory_flag,
    contradiction_flag,
    decay_weight,
    decay_bonus,
    decay_penalty,
    decay_adjusted_family_contribution,
    decay_family_rank,
    top_symbols,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Symbol Decay-Aware Attribution Summary view ──────────────────────

CREATE OR REPLACE VIEW cross_asset_symbol_decay_attribution_summary AS
WITH ranked AS (
    SELECT s.*,
        row_number() OVER (PARTITION BY s.run_id, s.symbol ORDER BY s.created_at DESC) AS rn
    FROM cross_asset_symbol_decay_attribution_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    symbol,
    dependency_family,
    dependency_type,
    freshness_state,
    aggregate_decay_score,
    family_decay_score,
    memory_score,
    state_age_runs,
    stale_memory_flag,
    contradiction_flag,
    raw_symbol_score,
    weighted_symbol_score,
    regime_adjusted_symbol_score,
    timing_adjusted_symbol_score,
    transition_adjusted_symbol_score,
    archetype_adjusted_symbol_score,
    cluster_adjusted_symbol_score,
    persistence_adjusted_symbol_score,
    decay_weight,
    decay_adjusted_symbol_score,
    symbol_rank,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Decay-Aware Integration Bridge view ──────────────────────────────
-- Joins all nine attribution layers + 4.7A run signal decay summary.

CREATE OR REPLACE VIEW run_cross_asset_decay_attribution_summary AS
WITH decay_totals AS (
    SELECT run_id, workspace_id, watchlist_id, context_snapshot_id,
        sum(decay_adjusted_family_contribution)::numeric AS decay_adjusted_cross_asset_contribution
    FROM cross_asset_family_decay_attribution_summary
    GROUP BY run_id, workspace_id, watchlist_id, context_snapshot_id
),
decay_dominant AS (
    SELECT DISTINCT ON (run_id) run_id,
        dependency_family AS decay_dominant_dependency_family
    FROM cross_asset_family_decay_attribution_summary
    WHERE decay_family_rank = 1
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
    pa.persistence_adjusted_cross_asset_contribution,
    dt.decay_adjusted_cross_asset_contribution,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    r.regime_dominant_dependency_family,
    t.timing_dominant_dependency_family,
    tra.transition_dominant_dependency_family,
    aa.archetype_dominant_dependency_family,
    ca.cluster_dominant_dependency_family,
    pa.persistence_dominant_dependency_family,
    dd.decay_dominant_dependency_family,
    rsd.freshness_state,
    rsd.aggregate_decay_score,
    rsd.stale_memory_flag,
    rsd.contradiction_flag,
    a.created_at
FROM cross_asset_attribution_summary a
LEFT JOIN run_cross_asset_weighted_integration_summary    w   ON w.run_id   = a.run_id
LEFT JOIN run_cross_asset_regime_integration_summary      r   ON r.run_id   = a.run_id
LEFT JOIN run_cross_asset_timing_attribution_summary      t   ON t.run_id   = a.run_id
LEFT JOIN run_cross_asset_transition_attribution_summary  tra ON tra.run_id = a.run_id
LEFT JOIN run_cross_asset_archetype_attribution_summary   aa  ON aa.run_id  = a.run_id
LEFT JOIN run_cross_asset_cluster_attribution_summary     ca  ON ca.run_id  = a.run_id
LEFT JOIN run_cross_asset_persistence_attribution_summary pa  ON pa.run_id  = a.run_id
LEFT JOIN run_cross_asset_explanation_bridge              bridge ON bridge.run_id = a.run_id
LEFT JOIN decay_totals    dt  ON dt.run_id  = a.run_id
LEFT JOIN decay_dominant  dd  ON dd.run_id  = a.run_id
LEFT JOIN run_cross_asset_signal_decay_summary            rsd ON rsd.run_id = a.run_id;

commit;
