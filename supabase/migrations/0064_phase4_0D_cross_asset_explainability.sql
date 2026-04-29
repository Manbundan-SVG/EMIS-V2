-- Phase 4.0D: Cross-Asset Explainability
-- Additive extension of the 4.0C cross-asset signal expansion. Introduces
-- run/watchlist-linked explanation artifacts: a compact snapshot of
-- confidence / confirmation / contradiction / missing / stale scores, plus
-- per-family contribution rows and ranked symbol lists. Does not modify
-- existing 4.0A/4.0B/4.0C tables or views.

begin;

-- ── A. Cross-Asset Explanation Snapshots ────────────────────────────────
-- One row per (workspace, watchlist, run) explanation emission. Ranked
-- symbol lists preserve deterministic ordering (see service).

CREATE TABLE IF NOT EXISTS cross_asset_explanation_snapshots (
    id                            uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                  uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                  uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                        uuid,
    context_snapshot_id           uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dominant_dependency_family    text,
    cross_asset_confidence_score  numeric,
    confirmation_score            numeric,
    contradiction_score           numeric,
    missing_context_score         numeric,
    stale_context_score           numeric,
    top_confirming_symbols        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    top_contradicting_symbols     jsonb       NOT NULL DEFAULT '[]'::jsonb,
    missing_dependency_symbols    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    stale_dependency_symbols      jsonb       NOT NULL DEFAULT '[]'::jsonb,
    explanation_state             text        NOT NULL DEFAULT 'computed'
                                                CHECK (explanation_state IN (
                                                    'computed','partial','missing_context','stale_context'
                                                )),
    metadata                      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_explanation_snapshots_scope_time_idx
    ON cross_asset_explanation_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_explanation_snapshots_run_idx
    ON cross_asset_explanation_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_explanation_snapshots_context_idx
    ON cross_asset_explanation_snapshots (context_snapshot_id);

-- ── B. Family Contribution Snapshots ────────────────────────────────────
-- One row per (workspace, watchlist, run, dependency_family) per explanation
-- emission. Enables per-family attribution without re-aggregating 4.0C data.

CREATE TABLE IF NOT EXISTS cross_asset_family_contribution_snapshots (
    id                            uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                  uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                  uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                        uuid,
    context_snapshot_id           uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dependency_family             text        NOT NULL,
    family_signal_count           integer     NOT NULL DEFAULT 0,
    confirmed_count               integer     NOT NULL DEFAULT 0,
    contradicted_count            integer     NOT NULL DEFAULT 0,
    missing_count                 integer     NOT NULL DEFAULT 0,
    stale_count                   integer     NOT NULL DEFAULT 0,
    family_confidence_score       numeric,
    family_support_score          numeric,
    family_contradiction_score    numeric,
    top_symbols                   jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_contribution_snapshots_scope_time_idx
    ON cross_asset_family_contribution_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_contribution_snapshots_run_idx
    ON cross_asset_family_contribution_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_contribution_snapshots_family_idx
    ON cross_asset_family_contribution_snapshots (dependency_family);

-- ── C. Cross-Asset Explanation Summary view ─────────────────────────────
-- Latest explanation per (workspace, watchlist, run). Workspace-level
-- explanations without a run use COALESCE to bucket them together.

CREATE OR REPLACE VIEW cross_asset_explanation_summary AS
WITH ranked AS (
    SELECT
        e.*,
        row_number() OVER (
            PARTITION BY e.workspace_id, e.watchlist_id,
                         COALESCE(e.run_id::text, '__none__')
            ORDER BY e.created_at DESC
        ) AS rn
    FROM cross_asset_explanation_snapshots e
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dominant_dependency_family,
    cross_asset_confidence_score,
    confirmation_score,
    contradiction_score,
    missing_context_score,
    stale_context_score,
    top_confirming_symbols,
    top_contradicting_symbols,
    missing_dependency_symbols,
    stale_dependency_symbols,
    explanation_state,
    created_at
FROM ranked
WHERE rn = 1;

-- ── D. Family Explanation Summary view ──────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_explanation_summary AS
WITH ranked AS (
    SELECT
        c.*,
        row_number() OVER (
            PARTITION BY c.workspace_id, c.watchlist_id,
                         COALESCE(c.run_id::text, '__none__'),
                         c.dependency_family
            ORDER BY c.created_at DESC
        ) AS rn
    FROM cross_asset_family_contribution_snapshots c
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    family_signal_count,
    confirmed_count,
    contradicted_count,
    missing_count,
    stale_count,
    family_confidence_score,
    family_support_score,
    family_contradiction_score,
    top_symbols,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Run Bridge view ──────────────────────────────────────────────────
-- Compact one-row-per-run explanation surface for future run inspection
-- integration. Only includes explanations actually linked to a run.

CREATE OR REPLACE VIEW run_cross_asset_explanation_bridge AS
WITH ranked AS (
    SELECT
        e.*,
        row_number() OVER (
            PARTITION BY e.run_id
            ORDER BY e.created_at DESC
        ) AS rn
    FROM cross_asset_explanation_snapshots e
    WHERE e.run_id IS NOT NULL
)
SELECT
    run_id,
    workspace_id,
    watchlist_id,
    context_snapshot_id,
    dominant_dependency_family,
    cross_asset_confidence_score,
    confirmation_score,
    contradiction_score,
    missing_context_score,
    stale_context_score,
    explanation_state,
    created_at
FROM ranked
WHERE rn = 1;

commit;
