-- Phase 4.4A: Sequencing Pattern Registry and Regime-Conditioned Transition Archetypes
-- Additive pattern-registry + archetype-classification layer on top of the
-- sequencing stack (4.3A/B/C/D). Introduces a controlled vocabulary of named
-- transition/sequence archetypes and per-run/family classifications. Does not
-- modify any diagnostics, attribution, composite, or replay-validation layer.

begin;

-- ── A. Transition Archetype Registry ────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_transition_archetype_registry (
    id                     uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    archetype_key          text        NOT NULL UNIQUE,
    archetype_label        text        NOT NULL,
    archetype_family       text        NOT NULL
                                CHECK (archetype_family IN (
                                    'rotation','recovery','degradation',
                                    'reinforcement','mixed','none'
                                )),
    description            text        NOT NULL,
    classification_rules   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    is_active              boolean     NOT NULL DEFAULT true,
    metadata               jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_transition_archetype_registry_active_idx
    ON cross_asset_transition_archetype_registry (is_active, archetype_family, archetype_key);

-- Seed initial archetypes. Rules JSON is advisory; authoritative classification
-- logic lives in the pattern service.
INSERT INTO cross_asset_transition_archetype_registry
    (archetype_key, archetype_label, archetype_family, description, classification_rules)
VALUES
    (
        'rotation_handoff', 'Rotation Handoff', 'rotation',
        'Family rotating in or out with rotation-path sequence and visible rank/dominance change.',
        '{"transition_state_in":["rotating_in","rotating_out"],"sequence_class_in":["rotation_path"]}'::jsonb
    ),
    (
        'reinforcing_continuation', 'Reinforcing Continuation', 'reinforcement',
        'Family in reinforcing state with reinforcing-path sequence; contribution holds or strengthens.',
        '{"transition_state_in":["reinforcing"],"sequence_class_in":["reinforcing_path"]}'::jsonb
    ),
    (
        'recovering_reentry', 'Recovering Reentry', 'recovery',
        'Family returning to confirmed evidence after prior contradiction; contribution improving.',
        '{"transition_state_in":["recovering"],"sequence_class_in":["recovery_path"]}'::jsonb
    ),
    (
        'deteriorating_breakdown', 'Deteriorating Breakdown', 'degradation',
        'Family losing confirmation with deteriorating-path sequence; contribution/rank weakening.',
        '{"transition_state_in":["deteriorating"],"sequence_class_in":["deteriorating_path"]}'::jsonb
    ),
    (
        'mixed_transition_noise', 'Mixed Transition Noise', 'mixed',
        'Transition state and sequence class disagree or evidence is fragmented.',
        '{"sequence_class_in":["mixed_path"]}'::jsonb
    ),
    (
        'insufficient_history', 'Insufficient History', 'none',
        'Not enough sequence evidence to classify into a meaningful archetype.',
        '{"transition_state_in":["insufficient_history"],"sequence_class_in":["insufficient_history"]}'::jsonb
    )
ON CONFLICT (archetype_key) DO NOTHING;

-- ── B. Family Archetype Snapshots ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_family_archetype_snapshots (
    id                             uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                   uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                   uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                         uuid        NOT NULL,
    context_snapshot_id            uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    dependency_family              text        NOT NULL,
    regime_key                     text,
    archetype_key                  text        NOT NULL,
    transition_state               text        NOT NULL DEFAULT 'insufficient_history'
                                        CHECK (transition_state IN (
                                            'reinforcing','deteriorating','recovering',
                                            'rotating_in','rotating_out','stable','insufficient_history'
                                        )),
    dominant_sequence_class        text        NOT NULL DEFAULT 'insufficient_history'
                                        CHECK (dominant_sequence_class IN (
                                            'reinforcing_path','deteriorating_path','recovery_path',
                                            'rotation_path','mixed_path','insufficient_history'
                                        )),
    dominant_timing_class          text,
    family_rank                    integer,
    family_contribution            numeric,
    archetype_confidence           numeric,
    classification_reason_codes    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_family_archetype_scope_time_idx
    ON cross_asset_family_archetype_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_family_archetype_run_idx
    ON cross_asset_family_archetype_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_family_archetype_family_idx
    ON cross_asset_family_archetype_snapshots (dependency_family);

CREATE INDEX IF NOT EXISTS cross_asset_family_archetype_key_idx
    ON cross_asset_family_archetype_snapshots (archetype_key);

-- ── C. Run Archetype Snapshots ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cross_asset_run_archetype_snapshots (
    id                             uuid        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    workspace_id                   uuid        NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    watchlist_id                   uuid        NOT NULL REFERENCES watchlists(id)  ON DELETE CASCADE,
    run_id                         uuid        NOT NULL,
    context_snapshot_id            uuid        REFERENCES watchlist_context_snapshots(id) ON DELETE SET NULL,
    regime_key                     text,
    dominant_archetype_key         text        NOT NULL,
    dominant_dependency_family     text,
    dominant_transition_state      text,
    dominant_sequence_class        text,
    archetype_confidence           numeric,
    rotation_event_count           integer     NOT NULL DEFAULT 0,
    recovery_event_count           integer     NOT NULL DEFAULT 0,
    degradation_event_count        integer     NOT NULL DEFAULT 0,
    mixed_event_count              integer     NOT NULL DEFAULT 0,
    classification_reason_codes    jsonb       NOT NULL DEFAULT '[]'::jsonb,
    metadata                       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at                     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cross_asset_run_archetype_scope_time_idx
    ON cross_asset_run_archetype_snapshots (workspace_id, watchlist_id, created_at DESC);

CREATE INDEX IF NOT EXISTS cross_asset_run_archetype_run_idx
    ON cross_asset_run_archetype_snapshots (run_id);

CREATE INDEX IF NOT EXISTS cross_asset_run_archetype_dominant_key_idx
    ON cross_asset_run_archetype_snapshots (dominant_archetype_key);

-- ── D. Family Archetype Summary view ────────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_family_archetype_summary AS
WITH ranked AS (
    SELECT
        f.*,
        row_number() OVER (
            PARTITION BY f.run_id, f.dependency_family
            ORDER BY f.created_at DESC
        ) AS rn
    FROM cross_asset_family_archetype_snapshots f
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    regime_key,
    archetype_key,
    transition_state,
    dominant_sequence_class,
    dominant_timing_class,
    family_rank,
    family_contribution,
    archetype_confidence,
    classification_reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── E. Run Archetype Summary view ───────────────────────────────────────

CREATE OR REPLACE VIEW cross_asset_run_archetype_summary AS
WITH ranked AS (
    SELECT
        r.*,
        row_number() OVER (
            PARTITION BY r.run_id
            ORDER BY r.created_at DESC
        ) AS rn
    FROM cross_asset_run_archetype_snapshots r
)
SELECT
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    regime_key,
    dominant_archetype_key,
    dominant_dependency_family,
    dominant_transition_state,
    dominant_sequence_class,
    archetype_confidence,
    rotation_event_count,
    recovery_event_count,
    degradation_event_count,
    mixed_event_count,
    classification_reason_codes,
    created_at
FROM ranked
WHERE rn = 1;

-- ── F. Regime-Conditioned Archetype Summary view ────────────────────────

CREATE OR REPLACE VIEW cross_asset_regime_archetype_summary AS
SELECT
    workspace_id,
    regime_key,
    dominant_archetype_key                  AS archetype_key,
    count(*)::int                           AS run_count,
    avg(archetype_confidence)::numeric      AS avg_confidence,
    max(created_at)                         AS latest_seen_at
FROM cross_asset_run_archetype_summary
GROUP BY workspace_id, regime_key, dominant_archetype_key;

-- ── G. Run Pattern Bridge view ──────────────────────────────────────────

CREATE OR REPLACE VIEW run_cross_asset_pattern_summary AS
SELECT
    run_id,
    workspace_id,
    watchlist_id,
    regime_key,
    dominant_archetype_key,
    dominant_dependency_family,
    dominant_transition_state,
    dominant_sequence_class,
    archetype_confidence,
    created_at
FROM cross_asset_run_archetype_summary;

commit;
