"""Phase 4.8D repository helpers — Conflict Replay Validation.

Sibling module to ``repositories.py`` matching the 4.7B/4.7C/4.7D/4.8A/4.8B/4.8C
pattern. The 4.8D service imports from here for persistence and reads.
"""

from __future__ import annotations

import json
from typing import Any  # noqa: F401

from src.db.repositories import _json_compatible  # noqa: F401  (re-used helper)


# ── A. Validation snapshot insert ───────────────────────────────────────

def insert_cross_asset_conflict_replay_validation_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    source_context_snapshot_id,
    replay_context_snapshot_id,
    source_regime_key,
    replay_regime_key,
    source_dominant_timing_class,
    replay_dominant_timing_class,
    source_dominant_transition_state,
    replay_dominant_transition_state,
    source_dominant_sequence_class,
    replay_dominant_sequence_class,
    source_dominant_archetype_key,
    replay_dominant_archetype_key,
    source_cluster_state,
    replay_cluster_state,
    source_persistence_state,
    replay_persistence_state,
    source_freshness_state,
    replay_freshness_state,
    source_layer_consensus_state,
    replay_layer_consensus_state,
    source_agreement_score,
    replay_agreement_score,
    source_conflict_score,
    replay_conflict_score,
    source_dominant_conflict_source,
    replay_dominant_conflict_source,
    source_contribution_layer,
    replay_contribution_layer,
    source_composite_layer,
    replay_composite_layer,
    source_scoring_version,
    replay_scoring_version,
    context_hash_match,
    regime_match,
    timing_class_match,
    transition_state_match,
    sequence_class_match,
    archetype_match,
    cluster_state_match,
    persistence_state_match,
    freshness_state_match,
    layer_consensus_state_match,
    agreement_score_match,
    conflict_score_match,
    dominant_conflict_source_match,
    source_contribution_layer_match,
    source_composite_layer_match,
    scoring_version_match,
    conflict_attribution_match,
    conflict_composite_match,
    conflict_dominant_family_match,
    conflict_delta,
    conflict_composite_delta,
    drift_reason_codes,
    validation_state,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_conflict_replay_validation_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_state, replay_cluster_state,
                source_persistence_state, replay_persistence_state,
                source_freshness_state, replay_freshness_state,
                source_layer_consensus_state, replay_layer_consensus_state,
                source_agreement_score, replay_agreement_score,
                source_conflict_score, replay_conflict_score,
                source_dominant_conflict_source, replay_dominant_conflict_source,
                source_contribution_layer, replay_contribution_layer,
                source_composite_layer, replay_composite_layer,
                source_scoring_version, replay_scoring_version,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match, archetype_match,
                cluster_state_match, persistence_state_match, freshness_state_match,
                layer_consensus_state_match, agreement_score_match, conflict_score_match,
                dominant_conflict_source_match,
                source_contribution_layer_match, source_composite_layer_match, scoring_version_match,
                conflict_attribution_match, conflict_composite_match, conflict_dominant_family_match,
                conflict_delta, conflict_composite_delta,
                drift_reason_codes, validation_state, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s::uuid,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s,
                %s, %s, %s,
                %s, %s, %s,
                %s::jsonb, %s::jsonb,
                %s::jsonb, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_state, replay_cluster_state,
                source_persistence_state, replay_persistence_state,
                source_freshness_state, replay_freshness_state,
                source_layer_consensus_state, replay_layer_consensus_state,
                source_agreement_score, replay_agreement_score,
                source_conflict_score, replay_conflict_score,
                source_dominant_conflict_source, replay_dominant_conflict_source,
                source_contribution_layer, replay_contribution_layer,
                source_composite_layer, replay_composite_layer,
                source_scoring_version, replay_scoring_version,
                bool(context_hash_match), bool(regime_match), bool(timing_class_match),
                bool(transition_state_match), bool(sequence_class_match), bool(archetype_match),
                bool(cluster_state_match), bool(persistence_state_match), bool(freshness_state_match),
                bool(layer_consensus_state_match), bool(agreement_score_match), bool(conflict_score_match),
                bool(dominant_conflict_source_match),
                bool(source_contribution_layer_match), bool(source_composite_layer_match), bool(scoring_version_match),
                bool(conflict_attribution_match), bool(conflict_composite_match), bool(conflict_dominant_family_match),
                json.dumps(_json_compatible(conflict_delta or {})),
                json.dumps(_json_compatible(conflict_composite_delta or {})),
                json.dumps(list(drift_reason_codes or [])),
                validation_state,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                "cross_asset_conflict_replay_validation_snapshots insert returned no row"
            )
        return dict(row)


# ── B. Family stability snapshots insert ────────────────────────────────

def insert_cross_asset_family_conflict_replay_stability_snapshots(
    conn, *, rows: list[dict],
):
    if not rows:
        return []
    out: list[dict] = []
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                insert into public.cross_asset_family_conflict_replay_stability_snapshots (
                    workspace_id, watchlist_id, source_run_id, replay_run_id,
                    dependency_family,
                    source_family_consensus_state, replay_family_consensus_state,
                    source_agreement_score, replay_agreement_score,
                    source_conflict_score, replay_conflict_score,
                    source_dominant_conflict_source, replay_dominant_conflict_source,
                    source_contribution_layer, replay_contribution_layer,
                    source_scoring_version, replay_scoring_version,
                    source_conflict_adjusted_contribution, replay_conflict_adjusted_contribution,
                    source_conflict_integration_contribution, replay_conflict_integration_contribution,
                    conflict_adjusted_delta, conflict_integration_delta,
                    family_consensus_state_match, agreement_score_match, conflict_score_match,
                    dominant_conflict_source_match, source_contribution_layer_match, scoring_version_match,
                    conflict_family_rank_match, conflict_composite_family_rank_match,
                    drift_reason_codes, metadata
                )
                values (
                    %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                    %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s::jsonb, %s::jsonb
                )
                returning id::text as id, created_at
                """,
                (
                    row["workspace_id"], row["watchlist_id"],
                    row["source_run_id"], row["replay_run_id"],
                    row["dependency_family"],
                    row.get("source_family_consensus_state"), row.get("replay_family_consensus_state"),
                    row.get("source_agreement_score"), row.get("replay_agreement_score"),
                    row.get("source_conflict_score"), row.get("replay_conflict_score"),
                    row.get("source_dominant_conflict_source"), row.get("replay_dominant_conflict_source"),
                    row.get("source_contribution_layer"), row.get("replay_contribution_layer"),
                    row.get("source_scoring_version"), row.get("replay_scoring_version"),
                    row.get("source_conflict_adjusted_contribution"), row.get("replay_conflict_adjusted_contribution"),
                    row.get("source_conflict_integration_contribution"), row.get("replay_conflict_integration_contribution"),
                    row.get("conflict_adjusted_delta"), row.get("conflict_integration_delta"),
                    bool(row.get("family_consensus_state_match", False)),
                    bool(row.get("agreement_score_match", False)),
                    bool(row.get("conflict_score_match", False)),
                    bool(row.get("dominant_conflict_source_match", False)),
                    bool(row.get("source_contribution_layer_match", False)),
                    bool(row.get("scoring_version_match", False)),
                    bool(row.get("conflict_family_rank_match", False)),
                    bool(row.get("conflict_composite_family_rank_match", False)),
                    json.dumps(list(row.get("drift_reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                ),
            )
            r = cur.fetchone()
            if r:
                out.append(dict(r))
    return out


# ── C. Summary fetchers ─────────────────────────────────────────────────

def get_cross_asset_conflict_replay_validation_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text  as workspace_id,
                watchlist_id::text  as watchlist_id,
                source_run_id::text as source_run_id,
                replay_run_id::text as replay_run_id,
                source_context_snapshot_id::text as source_context_snapshot_id,
                replay_context_snapshot_id::text as replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_state, replay_cluster_state,
                source_persistence_state, replay_persistence_state,
                source_freshness_state, replay_freshness_state,
                source_layer_consensus_state, replay_layer_consensus_state,
                source_agreement_score, replay_agreement_score,
                source_conflict_score, replay_conflict_score,
                source_dominant_conflict_source, replay_dominant_conflict_source,
                source_contribution_layer, replay_contribution_layer,
                source_composite_layer, replay_composite_layer,
                source_scoring_version, replay_scoring_version,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match, archetype_match,
                cluster_state_match, persistence_state_match, freshness_state_match,
                layer_consensus_state_match, agreement_score_match, conflict_score_match,
                dominant_conflict_source_match,
                source_contribution_layer_match, source_composite_layer_match, scoring_version_match,
                conflict_attribution_match, conflict_composite_match, conflict_dominant_family_match,
                drift_reason_codes, validation_state, created_at
            from public.cross_asset_conflict_replay_validation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 100
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_conflict_replay_stability_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text  as workspace_id,
                watchlist_id::text  as watchlist_id,
                source_run_id::text as source_run_id,
                replay_run_id::text as replay_run_id,
                dependency_family,
                source_family_consensus_state, replay_family_consensus_state,
                source_agreement_score, replay_agreement_score,
                source_conflict_score, replay_conflict_score,
                source_dominant_conflict_source, replay_dominant_conflict_source,
                source_contribution_layer, replay_contribution_layer,
                source_scoring_version, replay_scoring_version,
                source_conflict_adjusted_contribution, replay_conflict_adjusted_contribution,
                source_conflict_integration_contribution, replay_conflict_integration_contribution,
                conflict_adjusted_delta, conflict_integration_delta,
                family_consensus_state_match, agreement_score_match, conflict_score_match,
                dominant_conflict_source_match, source_contribution_layer_match, scoring_version_match,
                conflict_family_rank_match, conflict_composite_family_rank_match,
                drift_reason_codes, created_at
            from public.cross_asset_family_conflict_replay_stability_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_conflict_replay_stability_aggregate(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                validation_count,
                context_match_rate, regime_match_rate, timing_class_match_rate,
                transition_state_match_rate, sequence_class_match_rate, archetype_match_rate,
                cluster_state_match_rate, persistence_state_match_rate, freshness_state_match_rate,
                layer_consensus_state_match_rate, agreement_score_match_rate, conflict_score_match_rate,
                dominant_conflict_source_match_rate,
                source_contribution_layer_match_rate, source_composite_layer_match_rate, scoring_version_match_rate,
                conflict_attribution_match_rate, conflict_composite_match_rate, conflict_dominant_family_match_rate,
                drift_detected_count, latest_validated_at
            from public.cross_asset_conflict_replay_stability_aggregate
            where workspace_id = %s::uuid
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


# ── D. Source/replay loaders ────────────────────────────────────────────

def get_cross_asset_conflict_attribution_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
                   context_snapshot_id::text as context_snapshot_id,
                   conflict_adjusted_cross_asset_contribution,
                   layer_consensus_state, agreement_score, conflict_score,
                   dominant_conflict_source, created_at
            from public.run_cross_asset_conflict_attribution_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_cross_asset_conflict_composite_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
                   context_snapshot_id::text as context_snapshot_id,
                   conflict_adjusted_cross_asset_contribution,
                   conflict_net_contribution,
                   composite_pre_conflict, composite_post_conflict,
                   layer_consensus_state, agreement_score, conflict_score,
                   dominant_conflict_source,
                   integration_mode, source_contribution_layer, source_composite_layer,
                   scoring_version, created_at
            from public.cross_asset_conflict_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_family_conflict_attribution_for_run(conn, *, run_id):
    """List of family rows from 4.8B for the given run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family,
                   conflict_adjusted_family_contribution,
                   family_consensus_state,
                   agreement_score, conflict_score,
                   dominant_conflict_source,
                   conflict_family_rank,
                   created_at
            from public.cross_asset_family_conflict_attribution_summary
            where run_id = %s::uuid
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_family_conflict_composite_for_run(conn, *, run_id):
    """List of family rows from 4.8C for the given run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family,
                   family_consensus_state,
                   agreement_score, conflict_score,
                   dominant_conflict_source,
                   conflict_adjusted_family_contribution,
                   integration_weight_applied,
                   conflict_integration_contribution,
                   family_rank,
                   source_contribution_layer,
                   scoring_version,
                   created_at
            from public.cross_asset_family_conflict_composite_summary
            where run_id = %s::uuid
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_conflict_state_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    """Run-level layer-conflict context (4.8A)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   layer_consensus_state, agreement_score, conflict_score,
                   dominant_conflict_source, created_at
            from public.run_cross_asset_layer_conflict_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_context_snapshot_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    """Run-level context (regime/timing/transition/archetype/cluster/persistence/freshness)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select sd.run_id::text as run_id,
                   sd.regime_key,
                   sd.dominant_timing_class,
                   sd.dominant_transition_state,
                   sd.dominant_sequence_class,
                   sd.dominant_archetype_key,
                   sd.cluster_state,
                   sd.persistence_state,
                   sd.freshness_state
            from public.cross_asset_signal_decay_summary sd
            where sd.run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_replay_pair_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    """If this run is a replay, return the source run id. Otherwise None."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text          as run_id,
                   workspace_id::text as workspace_id,
                   replayed_from_run_id::text as source_run_id
            from public.job_runs
            where id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
