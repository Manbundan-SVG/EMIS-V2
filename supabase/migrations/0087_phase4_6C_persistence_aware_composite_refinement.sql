-- Phase 4.6C: Persistence-Aware Composite Refinement
-- Final persistence-aware integration layer. Starts from the most mature
-- upstream composite (4.5C composite_post_cluster → 4.4C composite_post_archetype
-- → 4.3C composite_post_transition → 4.2C composite_post_timing → regime
-- equivalent → raw fallback), adds a bounded persistence-aware delta
-- conditioned on the run's persistence state + memory-break events, and
-- persists the result side-by-side with all upstream layers. Does not
-- modify any existing composite, attribution, archetype, cluster, or
-- persistence surface.

begin;

-- ── A. Persistence Integration Profiles ─────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_persistence_integration_profiles (
    id                                  uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                        uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                        text        NOT NULL,
    is_active                           boolean     NOT NULL DEFAULT true,
    integration_mode                    text        NOT NULL DEFAULT 'persistence_additive_guardrailed'
                                                CHECK (integration_mode IN (
                                                    'persistence_additive_guardrailed',
                                                    'persistent_confirmation_only',
                                                    'memory_break_suppression_only',
                                                    'recovery_sensitive'
                                                )),
    integration_weight                  numeric     NOT NULL DEFAULT 0.10,
    persistent_scale                    numeric     NOT NULL DEFAULT 1.08,
    recovering_scale                    numeric     NOT NULL DEFAULT 1.03,
    rotating_scale                      numeric     NOT NULL DEFAULT 0.98,
    fragile_scale                       numeric     NOT NULL DEFAULT 0.88,
    breaking_down_scale                 numeric     NOT NULL DEFAULT 0.80,
    mixed_scale                         numeric     NOT NULL DEFAULT 0.90,
    insufficient_history_scale          numeric     NOT NULL DEFAULT 0.85,
    memory_break_extra_suppression      numeric     NOT NULL DEFAULT 0.02,
    max_positive_contribution           numeric     NOT NULL DEFAULT 0.20,
    max_negative_contribution           numeric     NOT NULL DEFAULT 0.20,
    metadata                            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_integration_profiles_scope_idx
    ON cross_asset_persistence_integration_profiles (workspace_id, is_active, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS cross_asset_persistence_integration_profiles_active_unique_idx
    ON cross_asset_persistence_integration_profiles (workspace_id)
    WHERE is_active;

-- ── B. Persistence-Aware Composite Snapshots ────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_persistence_composite_snapshots (
    id                                          uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                                uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                                uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                      uuid        NOT NULL,
    context_snapshot_id                         uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    persistence_integration_profile_id          uuid        REFERENCES cross_asset_persistence_integration_profiles(id) ON DELETE SET NULL,
    base_signal_score                           numeric,
    cross_asset_net_contribution                numeric,
    weighted_cross_asset_net_contribution       numeric,
    regime_adjusted_cross_asset_contribution    numeric,
    timing_adjusted_cross_asset_contribution    numeric,
    transition_adjusted_cross_asset_contribution numeric,
    archetype_adjusted_cross_asset_contribution numeric,
    cluster_adjusted_cross_asset_contribution   numeric,
    persistence_adjusted_cross_asset_contribution numeric,
    composite_pre_persistence                   numeric,
    persistence_net_contribution                numeric,
    composite_post_persistence                  numeric,
    persistence_state                           text        NOT NULL DEFAULT 'insufficient_history'
                                                        CHECK (persistence_state IN (
                                                            'persistent','fragile','rotating',
                                                            'breaking_down','recovering','mixed',
                                                            'insufficient_history'
                                                        )),
    memory_score                                numeric,
    state_age_runs                              integer,
    latest_persistence_event_type               text,
    integration_mode                            text        NOT NULL,
    metadata                                    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                                  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_composite_scope_time_idx
    ON cross_asset_persistence_composite_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_composite_run_idx
    ON cross_asset_persistence_composite_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_persistence_composite_state_idx
    ON cross_asset_persistence_composite_snapshots (persistence_state);

-- ── C. Family Persistence Composite Contribution Snapshots ──────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_persistence_composite_snapshots (
    id                                      uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                            uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                            uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                                  uuid        NOT NULL,
    context_snapshot_id                     uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dependency_family                       text        NOT NULL,
    persistence_state                       text        NOT NULL DEFAULT 'insufficient_history'
                                                CHECK (persistence_state IN (
                                                    'persistent','fragile','rotating',
                                                    'breaking_down','recovering','mixed',
                                                    'insufficient_history'
                                                )),
    memory_score                            numeric,
    state_age_runs                          integer,
    latest_persistence_event_type           text,
    persistence_adjusted_family_contribution numeric,
    integration_weight_applied              numeric,
    persistence_integration_contribution    numeric,
    family_rank                             integer,
    top_symbols                             jsonb       NOT NULL DEFAULT '[]'::jsonb,
    reason_codes                            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                                jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_persistence_composite_scope_time_idx
    ON cross_asset_family_persistence_composite_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_persistence_composite_run_idx
    ON cross_asset_family_persistence_composite_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_persistence_composite_family_idx
    ON cross_asset_family_persistence_composite_snapshots (dependency_family);

CREATE INDEX IF NOT EXISTS cross_asset_family_persistence_composite_state_idx
    ON cross_asset_family_persistence_composite_snapshots (persistence_state);

-- ── D. Persistence Composite Summary view ───────────────────────────────

CREATE OR REPLACE VIEW cross_asset_persistence_composite_summary AS
WITH ranked AS (
    SELECT
        c.*,
        row_number() OVER (
            PARTITION BY c.run_id
            ORDER BY c.created_at DESC
        ) AS rn
    FROM cross_asset_persistence_composite_snapshots c
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    base_signal_score,
    cross_asset_net_contribution,
    weighted_cross_asset_net_contribution,
    regime_adjusted_cross_asset_contribution,
    timing_adjusted_cross_asset_contribution,
    transition_adjusted_cross_asset_contribution,
    archetype_adjusted_cross_asset_contribution,
    cluster_adjusted_cross_asset_contribution,
    persistence_adjusted_cross_asset_contribution,
    composite_pre_persistence,
    persistence_net_contribution,
    composite_post_persistence,
    persistence_state,
    memory_score,
    state_age_runs,
    latest_persistence_event_type,
    integration_mode,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Family Persistence Composite Summary view ────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_persistence_composite_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_persistence_composite_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    persistence_state,
    memory_score,
    state_age_runs,
    latest_persistence_event_type,
    persistence_adjusted_family_contribution,
    integration_weight_applied,
    persistence_integration_contribution,
    family_rank,
    top_symbols,
    reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Final Persistence Integration Bridge view ────────────────────────

CREATE OR REPLACE VIEW run_cross_asset_persistence_integration_summary AS
SELECT
    pc.run_id,
    pc.workspace_id,
    pc.watchlist_id,
    pc.context_snapshot_id,
    pc.cross_asset_net_contribution,
    pc.weighted_cross_asset_net_contribution,
    pc.regime_adjusted_cross_asset_contribution,
    pc.timing_adjusted_cross_asset_contribution,
    pc.transition_adjusted_cross_asset_contribution,
    pc.archetype_adjusted_cross_asset_contribution,
    pc.cluster_adjusted_cross_asset_contribution,
    pc.persistence_adjusted_cross_asset_contribution,
    pc.persistence_net_contribution,
    pc.composite_pre_persistence,
    pc.composite_post_persistence,
    bridge.dominant_dependency_family,
    w.weighted_dominant_dependency_family,
    r.regime_dominant_dependency_family,
    t.timing_dominant_dependency_family,
    tra.transition_dominant_dependency_family,
    aa.archetype_dominant_dependency_family,
    ca.cluster_dominant_dependency_family,
    pa.persistence_dominant_dependency_family,
    pc.persistence_state,
    pc.memory_score,
    pc.state_age_runs,
    pc.latest_persistence_event_type,
    pc.created_at
FROM cross_asset_persistence_composite_summary pc
LEFT JOIN run_cross_asset_explanation_bridge                bridge ON bridge.run_id = pc.run_id
LEFT JOIN run_cross_asset_weighted_integration_summary      w      ON w.run_id      = pc.run_id
LEFT JOIN run_cross_asset_regime_integration_summary        r      ON r.run_id      = pc.run_id
LEFT JOIN run_cross_asset_timing_attribution_summary        t      ON t.run_id      = pc.run_id
LEFT JOIN run_cross_asset_transition_attribution_summary    tra    ON tra.run_id    = pc.run_id
LEFT JOIN run_cross_asset_archetype_attribution_summary     aa     ON aa.run_id     = pc.run_id
LEFT JOIN run_cross_asset_cluster_attribution_summary       ca     ON ca.run_id     = pc.run_id
LEFT JOIN run_cross_asset_persistence_attribution_summary   pa     ON pa.run_id     = pc.run_id;

commit;
