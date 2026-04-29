-- Phase 4.1B: Dependency-Priority-Aware Ranking and Contribution Weighting
-- Additive refinement layer on top of 4.1A raw attribution. Introduces an
-- explicit weighting profile, weighted family attribution rows, weighted
-- symbol attribution rows, and summary views so run inspection can compare
-- raw vs weighted contributions. Does not modify 4.1A raw attribution.

begin;

-- ── A. Dependency Weighting Profiles ────────────────────────────────────
-- One active profile per workspace. History preserved via is_active flag;
-- a partial UNIQUE index enforces "at most one active" at the DB level.

CREATE TABLE IF NOT EXISTS dependency_weighting_profiles (
    id                             uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                   uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    profile_name                   text        NOT NULL,
    is_active                      boolean     NOT NULL DEFAULT true,
    priority_weight_scale          numeric     NOT NULL DEFAULT 1.0,
    direct_dependency_bonus        numeric     NOT NULL DEFAULT 0.20,
    secondary_dependency_penalty   numeric     NOT NULL DEFAULT 0.10,
    missing_penalty_scale          numeric     NOT NULL DEFAULT 1.0,
    stale_penalty_scale            numeric     NOT NULL DEFAULT 1.0,
    family_weight_overrides        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    type_weight_overrides          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    metadata                       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dependency_weighting_profiles_scope_idx
    ON dependency_weighting_profiles (workspace_id, is_active, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS dependency_weighting_profiles_active_unique_idx
    ON dependency_weighting_profiles (workspace_id)
    WHERE is_active;

-- ── B. Weighted Family Attribution Snapshots ────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_weighted_attribution_snapshots (
    id                                 uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                       uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                       uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                             uuid        NOT NULL,
    context_snapshot_id                uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    weighting_profile_id               uuid        REFERENCES dependency_weighting_profiles(id) ON DELETE SET NULL,
    dependency_family                  text        NOT NULL,
    raw_family_net_contribution        numeric,
    priority_weight                    numeric,
    family_weight                      numeric,
    type_weight                        numeric,
    coverage_weight                    numeric,
    weighted_family_net_contribution   numeric,
    weighted_family_rank               integer,
    top_symbols                        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_weighted_scope_time_idx
    ON cross_asset_family_weighted_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_weighted_run_idx
    ON cross_asset_family_weighted_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_weighted_family_idx
    ON cross_asset_family_weighted_attribution_snapshots (dependency_family);

-- ── C. Weighted Symbol Attribution Snapshots ────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_symbol_weighted_attribution_snapshots (
    id                                 uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                       uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                       uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                             uuid        NOT NULL,
    context_snapshot_id                uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    weighting_profile_id               uuid        REFERENCES dependency_weighting_profiles(id) ON DELETE SET NULL,
    symbol                             text        NOT NULL,
    dependency_family                  text        NOT NULL,
    dependency_type                    text,
    graph_priority                     integer,
    is_direct_dependency               boolean     NOT NULL DEFAULT true,
    raw_symbol_score                   numeric,
    priority_weight                    numeric,
    family_weight                      numeric,
    type_weight                        numeric,
    coverage_weight                    numeric,
    weighted_symbol_score              numeric,
    symbol_rank                        integer,
    metadata                           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_weighted_scope_time_idx
    ON cross_asset_symbol_weighted_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_weighted_run_idx
    ON cross_asset_symbol_weighted_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_symbol_weighted_symbol_idx
    ON cross_asset_symbol_weighted_attribution_snapshots (symbol);

-- ── D. Weighted Family Attribution Summary view ─────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_weighted_attribution_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_weighted_attribution_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    raw_family_net_contribution,
    priority_weight,
    family_weight,
    type_weight,
    coverage_weight,
    weighted_family_net_contribution,
    weighted_family_rank,
    top_symbols,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Weighted Symbol Attribution Summary view ─────────────────────────

CREATE OR REPLACE VIEW cross_asset_symbol_weighted_attribution_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.run_id, s.symbol
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_symbol_weighted_attribution_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    symbol,
    dependency_family,
    dependency_type,
    graph_priority,
    is_direct_dependency,
    raw_symbol_score,
    priority_weight,
    family_weight,
    type_weight,
    coverage_weight,
    weighted_symbol_score,
    symbol_rank,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Weighted Integration Summary view ────────────────────────────────
-- Bridges raw 4.1A attribution with weighted 4.1B family aggregates so run
-- inspection can see (1) the 4.1A net contribution and dominant family
-- side-by-side with (2) the weighted net contribution (sum of weighted
-- family contributions) and the weighted dominant family (rank-1 under
-- weighting).

CREATE OR REPLACE VIEW run_cross_asset_weighted_integration_summary AS
WITH weighted_totals AS (
    SELECT
        run_id,
        workspace_id,
        watchlist_id,
        context_snapshot_id,
        sum(weighted_family_net_contribution)::numeric AS weighted_cross_asset_net_contribution
    FROM cross_asset_family_weighted_attribution_summary
    GROUP BY run_id, workspace_id, watchlist_id, context_snapshot_id
),
weighted_dominant AS (
    SELECT DISTINCT ON (run_id)
        run_id,
        dependency_family AS weighted_dominant_dependency_family
    FROM cross_asset_family_weighted_attribution_summary
    WHERE weighted_family_rank = 1
    ORDER BY run_id, created_at DESC
)
SELECT
    a.run_id,
    a.workspace_id,
    a.watchlist_id,
    a.context_snapshot_id,
    a.base_signal_score,
    a.cross_asset_net_contribution,
    wt.weighted_cross_asset_net_contribution,
    bridge.dominant_dependency_family,
    wd.weighted_dominant_dependency_family,
    a.created_at
FROM cross_asset_attribution_summary a
LEFT JOIN weighted_totals   wt     ON wt.run_id     = a.run_id
LEFT JOIN weighted_dominant wd     ON wd.run_id     = a.run_id
LEFT JOIN run_cross_asset_explanation_bridge bridge ON bridge.run_id = a.run_id;

commit;
