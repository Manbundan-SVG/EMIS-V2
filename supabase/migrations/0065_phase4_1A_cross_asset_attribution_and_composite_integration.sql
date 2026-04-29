-- Phase 4.1A: Cross-Asset Attribution + Composite Integration
-- Additive extension of the 4.0D explainability layer. Persists run-linked
-- cross-asset attribution (base score, cross-asset score, penalties, net
-- contribution) and a pre/post composite view so run inspection can see
-- what cross-asset context did to the composite. Does not modify existing
-- tables or the 4.0A/B/C/D artifacts.

begin;

-- ── A. Cross-Asset Attribution Snapshots ────────────────────────────────
-- One row per (run, watchlist) attribution emission. run_id is NOT NULL
-- because attribution is always scoped to a specific run; workspace-only
-- attribution has no meaningful pre/post anchor.

CREATE TABLE IF NOT EXISTS cross_asset_attribution_snapshots (
    id                                uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                      uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                      uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                            uuid        NOT NULL,
    context_snapshot_id               uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    base_signal_score                 numeric,
    cross_asset_signal_score          numeric,
    cross_asset_confirmation_score    numeric,
    cross_asset_contradiction_penalty numeric,
    cross_asset_missing_penalty       numeric,
    cross_asset_stale_penalty         numeric,
    cross_asset_net_contribution      numeric,
    composite_pre_cross_asset         numeric,
    composite_post_cross_asset        numeric,
    integration_mode                  text        NOT NULL DEFAULT 'additive_guardrailed'
                                                    CHECK (integration_mode IN (
                                                        'additive_guardrailed',
                                                        'confirmation_only',
                                                        'suppression_only'
                                                    )),
    metadata                          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_attribution_snapshots_scope_time_idx
    ON cross_asset_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_attribution_snapshots_run_idx
    ON cross_asset_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_attribution_snapshots_context_idx
    ON cross_asset_attribution_snapshots (context_snapshot_id);

-- ── B. Family Attribution Snapshots ─────────────────────────────────────
-- One row per (run, watchlist, dependency_family) attribution emission.

CREATE TABLE IF NOT EXISTS cross_asset_family_attribution_snapshots (
    id                              uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                    uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                    uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                          uuid        NOT NULL,
    context_snapshot_id             uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dependency_family               text        NOT NULL,
    family_signal_score             numeric,
    family_confirmation_score       numeric,
    family_contradiction_penalty    numeric,
    family_missing_penalty          numeric,
    family_stale_penalty            numeric,
    family_net_contribution         numeric,
    family_rank                     integer,
    top_symbols                     jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_attribution_snapshots_scope_time_idx
    ON cross_asset_family_attribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_attribution_snapshots_run_idx
    ON cross_asset_family_attribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_attribution_snapshots_family_idx
    ON cross_asset_family_attribution_snapshots (dependency_family);

-- ── C. Cross-Asset Attribution Summary view ─────────────────────────────
-- Latest attribution row per run.

CREATE OR REPLACE VIEW cross_asset_attribution_summary AS
WITH ranked AS (
    SELECT
        a.*,
        row_number() OVER (
            PARTITION BY a.run_id
            ORDER BY a.created_at DESC
        ) AS rn
    FROM cross_asset_attribution_snapshots a
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    base_signal_score,
    cross_asset_signal_score,
    cross_asset_confirmation_score,
    cross_asset_contradiction_penalty,
    cross_asset_missing_penalty,
    cross_asset_stale_penalty,
    cross_asset_net_contribution,
    composite_pre_cross_asset,
    composite_post_cross_asset,
    integration_mode,
    created_at
FROM ranked
WHERE rn = 1;

-- ── D. Family Attribution Summary view ──────────────────────────────────
-- Latest row per (run, dependency_family).

CREATE OR REPLACE VIEW cross_asset_family_attribution_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_attribution_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    family_signal_score,
    family_confirmation_score,
    family_contradiction_penalty,
    family_missing_penalty,
    family_stale_penalty,
    family_net_contribution,
    family_rank,
    top_symbols,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Composite Integration Summary view ───────────────────────────────
-- Bridges the 4.1A attribution surface with the 4.0D explanation bridge so
-- run inspection can see both the pre/post composite and the dominant
-- family / confidence score in one row.

CREATE OR REPLACE VIEW run_composite_integration_summary AS
SELECT
    a.run_id,
    a.workspace_id,
    a.watchlist_id,
    a.base_signal_score,
    a.cross_asset_signal_score,
    a.cross_asset_net_contribution,
    a.composite_pre_cross_asset,
    a.composite_post_cross_asset,
    b.dominant_dependency_family,
    b.cross_asset_confidence_score,
    a.created_at
FROM cross_asset_attribution_summary a
LEFT JOIN run_cross_asset_explanation_bridge b
    ON b.run_id = a.run_id;

commit;
