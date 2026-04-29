-- Phase 4.0C: Cross-Asset Signal Expansion
-- Additive extension of the 4.0A data substrate and 4.0B context model.
-- Introduces storage surfaces for cross-asset features and signals that let
-- dependency context materially influence downstream feature/signal behavior.
-- Does not modify existing tables or the 4.0A/4.0B artifacts.

begin;

-- ── A. Cross-Asset Feature Snapshots ─────────────────────────────────────
-- One row per computed cross-asset feature. feature_state is explicit — a
-- numeric value of NULL combined with a state like 'missing_dependency' or
-- 'stale_dependency' is the correct representation; never substitute zero.

CREATE TABLE IF NOT EXISTS cross_asset_feature_snapshots (
    id                    uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id          uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id          uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                uuid,
    context_snapshot_id   uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    feature_family        text        NOT NULL CHECK (feature_family IN (
                              'risk_context','macro_confirmation','fx_pressure',
                              'rates_pressure','commodity_context','cross_asset_divergence'
                          )),
    feature_key           text        NOT NULL,
    feature_value         numeric,
    feature_state         text        NOT NULL DEFAULT 'computed' CHECK (feature_state IN (
                              'computed','missing_dependency','stale_dependency',
                              'insufficient_context','invalidated'
                          )),
    dependency_symbols    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    dependency_families   jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata              jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_feature_snapshots_scope_time_idx
    ON cross_asset_feature_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_feature_snapshots_run_idx
    ON cross_asset_feature_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_feature_snapshots_context_idx
    ON cross_asset_feature_snapshots (context_snapshot_id);

CREATE INDEX IF NOT EXISTS cross_asset_feature_snapshots_family_key_idx
    ON cross_asset_feature_snapshots (feature_family, feature_key);

-- ── B. Cross-Asset Signal Snapshots ──────────────────────────────────────
-- One row per derived cross-asset signal. Kept separate from raw features so
-- explanations and attribution can cite signals without re-deriving from
-- features. signal_state captures the confirmation/contradiction relationship
-- against the base_symbol's direction.

CREATE TABLE IF NOT EXISTS cross_asset_signal_snapshots (
    id                    uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id          uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id          uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                uuid,
    context_snapshot_id   uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    signal_family         text        NOT NULL CHECK (signal_family IN (
                              'macro_confirmation','risk_context','fx_pressure',
                              'rates_pressure','commodity_context','cross_asset_divergence'
                          )),
    signal_key            text        NOT NULL,
    signal_value          numeric,
    signal_direction      text        CHECK (signal_direction IS NULL OR signal_direction IN (
                              'bullish','bearish','neutral'
                          )),
    signal_state          text        NOT NULL DEFAULT 'computed' CHECK (signal_state IN (
                              'computed','confirmed','unconfirmed','contradicted',
                              'missing_context','stale_context'
                          )),
    base_symbol           text,
    dependency_symbols    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    dependency_families   jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata              jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_signal_snapshots_scope_time_idx
    ON cross_asset_signal_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_signal_snapshots_run_idx
    ON cross_asset_signal_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_signal_snapshots_family_key_idx
    ON cross_asset_signal_snapshots (signal_family, signal_key);

CREATE INDEX IF NOT EXISTS cross_asset_signal_snapshots_base_symbol_idx
    ON cross_asset_signal_snapshots (base_symbol);

-- ── C. Cross-Asset Signal Summary view ───────────────────────────────────
-- Latest signal per (workspace, watchlist, signal_family, signal_key, base_symbol).

CREATE OR REPLACE VIEW cross_asset_signal_summary AS
WITH ranked AS (
    SELECT
        s.*,
        row_number() OVER (
            PARTITION BY s.workspace_id, s.watchlist_id,
                         s.signal_family, s.signal_key,
                         COALESCE(s.base_symbol, '')
            ORDER BY s.created_at DESC
        ) AS rn
    FROM cross_asset_signal_snapshots s
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    signal_family,
    signal_key,
    signal_value,
    signal_direction,
    signal_state,
    base_symbol,
    jsonb_array_length(COALESCE(dependency_symbols,  '[]'::jsonb)) AS dependency_symbol_count,
    jsonb_array_length(COALESCE(dependency_families, '[]'::jsonb)) AS dependency_family_count,
    created_at
FROM ranked
WHERE rn = 1;

-- ── D. Cross-Asset Dependency Health Summary view ────────────────────────
-- Feature + signal rollup per (workspace, watchlist, context_snapshot_id,
-- dependency_family). Uses jsonb_array_elements_text to attribute a row to
-- every family it claims; a feature/signal touching both 'risk' and 'rates'
-- contributes to both family rows.

CREATE OR REPLACE VIEW cross_asset_dependency_health_summary AS
WITH feature_fam AS (
    SELECT
        f.workspace_id,
        f.watchlist_id,
        f.context_snapshot_id,
        jsonb_array_elements_text(f.dependency_families) AS dependency_family,
        f.feature_state,
        f.created_at,
        1 AS is_feature,
        0 AS is_signal,
        0 AS is_confirmed,
        0 AS is_contradicted,
        CASE WHEN f.feature_state = 'missing_dependency' THEN 1 ELSE 0 END AS is_missing,
        CASE WHEN f.feature_state = 'stale_dependency'   THEN 1 ELSE 0 END AS is_stale
    FROM cross_asset_feature_snapshots f
    WHERE jsonb_array_length(f.dependency_families) > 0
),
signal_fam AS (
    SELECT
        s.workspace_id,
        s.watchlist_id,
        s.context_snapshot_id,
        jsonb_array_elements_text(s.dependency_families) AS dependency_family,
        s.signal_state,
        s.created_at,
        0 AS is_feature,
        1 AS is_signal,
        CASE WHEN s.signal_state = 'confirmed'       THEN 1 ELSE 0 END AS is_confirmed,
        CASE WHEN s.signal_state = 'contradicted'    THEN 1 ELSE 0 END AS is_contradicted,
        CASE WHEN s.signal_state = 'missing_context' THEN 1 ELSE 0 END AS is_missing,
        CASE WHEN s.signal_state = 'stale_context'   THEN 1 ELSE 0 END AS is_stale
    FROM cross_asset_signal_snapshots s
    WHERE jsonb_array_length(s.dependency_families) > 0
),
combined AS (
    SELECT workspace_id, watchlist_id, context_snapshot_id, dependency_family,
           created_at, is_feature, is_signal, is_confirmed, is_contradicted,
           is_missing, is_stale
    FROM feature_fam
    UNION ALL
    SELECT workspace_id, watchlist_id, context_snapshot_id, dependency_family,
           created_at, is_feature, is_signal, is_confirmed, is_contradicted,
           is_missing, is_stale
    FROM signal_fam
)
SELECT
    workspace_id,
    watchlist_id,
    context_snapshot_id,
    dependency_family,
    sum(is_feature)::int       AS feature_count,
    sum(is_signal)::int        AS signal_count,
    sum(is_missing)::int       AS missing_dependency_count,
    sum(is_stale)::int         AS stale_dependency_count,
    sum(is_confirmed)::int     AS confirmed_count,
    sum(is_contradicted)::int  AS contradicted_count,
    max(created_at)            AS latest_created_at
FROM combined
GROUP BY workspace_id, watchlist_id, context_snapshot_id, dependency_family;

-- ── E. Run-Linked Cross-Asset Context Summary view ───────────────────────
-- One row per run that has any cross-asset feature or signal attached.
-- dominant_dependency_family is the family with the largest count on that run.

CREATE OR REPLACE VIEW run_cross_asset_context_summary AS
WITH run_features AS (
    SELECT
        f.run_id,
        f.workspace_id,
        f.watchlist_id,
        f.context_snapshot_id,
        count(*)::int AS feature_count,
        max(f.created_at) AS latest_feature_at
    FROM cross_asset_feature_snapshots f
    WHERE f.run_id IS NOT NULL
    GROUP BY f.run_id, f.workspace_id, f.watchlist_id, f.context_snapshot_id
),
run_signals AS (
    SELECT
        s.run_id,
        count(*)::int                                                   AS signal_count,
        count(*) FILTER (WHERE s.signal_state = 'confirmed')::int       AS confirmed_signal_count,
        count(*) FILTER (WHERE s.signal_state = 'contradicted')::int    AS contradicted_signal_count,
        count(*) FILTER (WHERE s.signal_state IN ('missing_context'))::int AS missing_context_count,
        count(*) FILTER (WHERE s.signal_state = 'stale_context')::int   AS stale_context_count,
        max(s.created_at)                                               AS latest_signal_at
    FROM cross_asset_signal_snapshots s
    WHERE s.run_id IS NOT NULL
    GROUP BY s.run_id
),
family_counts AS (
    SELECT
        s.run_id,
        jsonb_array_elements_text(s.dependency_families) AS dependency_family,
        count(*)::int AS fam_count
    FROM cross_asset_signal_snapshots s
    WHERE s.run_id IS NOT NULL
      AND jsonb_array_length(s.dependency_families) > 0
    GROUP BY s.run_id, jsonb_array_elements_text(s.dependency_families)
),
dominant_family AS (
    SELECT DISTINCT ON (run_id)
        run_id, dependency_family AS dominant_dependency_family
    FROM family_counts
    ORDER BY run_id, fam_count DESC, dependency_family ASC
)
SELECT
    rf.run_id,
    rf.workspace_id,
    rf.watchlist_id,
    rf.context_snapshot_id,
    rf.feature_count                      AS cross_asset_feature_count,
    COALESCE(rs.signal_count, 0)          AS cross_asset_signal_count,
    COALESCE(rs.confirmed_signal_count, 0)    AS confirmed_signal_count,
    COALESCE(rs.contradicted_signal_count, 0) AS contradicted_signal_count,
    COALESCE(rs.missing_context_count, 0)     AS missing_context_count,
    COALESCE(rs.stale_context_count, 0)       AS stale_context_count,
    df.dominant_dependency_family,
    GREATEST(rf.latest_feature_at, COALESCE(rs.latest_signal_at, rf.latest_feature_at))
        AS created_at
FROM run_features rf
LEFT JOIN run_signals    rs ON rs.run_id = rf.run_id
LEFT JOIN dominant_family df ON df.run_id = rf.run_id;

commit;
