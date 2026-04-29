"""Phase 4.7D repository helpers (sibling module to repositories.py)."""

from __future__ import annotations

import json
from typing import Any

from src.db.repositories import _json_compatible  # noqa: F401  (re-used helper)


def insert_cross_asset_decay_replay_validation_snapshot(
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
    source_memory_score,
    replay_memory_score,
    source_freshness_state,
    replay_freshness_state,
    source_aggregate_decay_score,
    replay_aggregate_decay_score,
    source_stale_memory_flag,
    replay_stale_memory_flag,
    source_contradiction_flag,
    replay_contradiction_flag,
    context_hash_match,
    regime_match,
    timing_class_match,
    transition_state_match,
    sequence_class_match,
    archetype_match,
    cluster_state_match,
    persistence_state_match,
    memory_score_match,
    freshness_state_match,
    aggregate_decay_score_match,
    stale_memory_flag_match,
    contradiction_flag_match,
    decay_attribution_match,
    decay_composite_match,
    decay_dominant_family_match,
    decay_delta,
    decay_composite_delta,
    drift_reason_codes,
    validation_state,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_decay_replay_validation_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_state, replay_cluster_state,
                source_persistence_state, replay_persistence_state,
                source_memory_score, replay_memory_score,
                source_freshness_state, replay_freshness_state,
                source_aggregate_decay_score, replay_aggregate_decay_score,
                source_stale_memory_flag, replay_stale_memory_flag,
                source_contradiction_flag, replay_contradiction_flag,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match, archetype_match,
                cluster_state_match, persistence_state_match,
                memory_score_match, freshness_state_match,
                aggregate_decay_score_match,
                stale_memory_flag_match, contradiction_flag_match,
                decay_attribution_match, decay_composite_match, decay_dominant_family_match,
                decay_delta, decay_composite_delta,
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
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s,
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
                source_memory_score, replay_memory_score,
                source_freshness_state, replay_freshness_state,
                source_aggregate_decay_score, replay_aggregate_decay_score,
                source_stale_memory_flag, replay_stale_memory_flag,
                source_contradiction_flag, replay_contradiction_flag,
                bool(context_hash_match), bool(regime_match), bool(timing_class_match),
                bool(transition_state_match), bool(sequence_class_match), bool(archetype_match),
                bool(cluster_state_match), bool(persistence_state_match),
                bool(memory_score_match), bool(freshness_state_match),
                bool(aggregate_decay_score_match),
                bool(stale_memory_flag_match), bool(contradiction_flag_match),
                bool(decay_attribution_match), bool(decay_composite_match), bool(decay_dominant_family_match),
                json.dumps(_json_compatible(decay_delta or {})),
                json.dumps(_json_compatible(decay_composite_delta or {})),
                json.dumps(list(drift_reason_codes or [])),
                validation_state,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_decay_replay_validation_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_decay_replay_stability_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    dependency_family,
    source_freshness_state,
    replay_freshness_state,
    source_aggregate_decay_score,
    replay_aggregate_decay_score,
    source_family_decay_score,
    replay_family_decay_score,
    source_stale_memory_flag,
    replay_stale_memory_flag,
    source_contradiction_flag,
    replay_contradiction_flag,
    source_decay_adjusted_contribution,
    replay_decay_adjusted_contribution,
    source_decay_integration_contribution,
    replay_decay_integration_contribution,
    decay_adjusted_delta,
    decay_integration_delta,
    freshness_state_match,
    aggregate_decay_score_match,
    family_decay_score_match,
    stale_memory_flag_match,
    contradiction_flag_match,
    decay_family_rank_match,
    decay_composite_family_rank_match,
    drift_reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_family_decay_replay_stability_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                dependency_family,
                source_freshness_state, replay_freshness_state,
                source_aggregate_decay_score, replay_aggregate_decay_score,
                source_family_decay_score, replay_family_decay_score,
                source_stale_memory_flag, replay_stale_memory_flag,
                source_contradiction_flag, replay_contradiction_flag,
                source_decay_adjusted_contribution, replay_decay_adjusted_contribution,
                source_decay_integration_contribution, replay_decay_integration_contribution,
                decay_adjusted_delta, decay_integration_delta,
                freshness_state_match, aggregate_decay_score_match,
                family_decay_score_match,
                stale_memory_flag_match, contradiction_flag_match,
                decay_family_rank_match, decay_composite_family_rank_match,
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
                %s,
                %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                dependency_family,
                source_freshness_state, replay_freshness_state,
                source_aggregate_decay_score, replay_aggregate_decay_score,
                source_family_decay_score, replay_family_decay_score,
                source_stale_memory_flag, replay_stale_memory_flag,
                source_contradiction_flag, replay_contradiction_flag,
                source_decay_adjusted_contribution, replay_decay_adjusted_contribution,
                source_decay_integration_contribution, replay_decay_integration_contribution,
                decay_adjusted_delta, decay_integration_delta,
                bool(freshness_state_match), bool(aggregate_decay_score_match),
                bool(family_decay_score_match),
                bool(stale_memory_flag_match), bool(contradiction_flag_match),
                bool(decay_family_rank_match), bool(decay_composite_family_rank_match),
                json.dumps(list(drift_reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_family_decay_replay_stability_snapshots insert returned no row")
        return dict(row)


def get_cross_asset_decay_replay_validation_summary(conn, *, workspace_id, watchlist_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text                  as workspace_id,
                watchlist_id::text                  as watchlist_id,
                source_run_id::text                 as source_run_id,
                replay_run_id::text                 as replay_run_id,
                source_context_snapshot_id::text    as source_context_snapshot_id,
                replay_context_snapshot_id::text    as replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_state, replay_cluster_state,
                source_persistence_state, replay_persistence_state,
                source_memory_score, replay_memory_score,
                source_freshness_state, replay_freshness_state,
                source_aggregate_decay_score, replay_aggregate_decay_score,
                source_stale_memory_flag, replay_stale_memory_flag,
                source_contradiction_flag, replay_contradiction_flag,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match, archetype_match,
                cluster_state_match, persistence_state_match,
                memory_score_match, freshness_state_match,
                aggregate_decay_score_match,
                stale_memory_flag_match, contradiction_flag_match,
                decay_attribution_match, decay_composite_match, decay_dominant_family_match,
                drift_reason_codes, validation_state, created_at
            from public.cross_asset_decay_replay_validation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 100
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_decay_replay_stability_summary(conn, *, workspace_id, watchlist_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text  as workspace_id,
                watchlist_id::text  as watchlist_id,
                source_run_id::text as source_run_id,
                replay_run_id::text as replay_run_id,
                dependency_family,
                source_freshness_state, replay_freshness_state,
                source_aggregate_decay_score, replay_aggregate_decay_score,
                source_family_decay_score, replay_family_decay_score,
                source_stale_memory_flag, replay_stale_memory_flag,
                source_contradiction_flag, replay_contradiction_flag,
                source_decay_adjusted_contribution, replay_decay_adjusted_contribution,
                source_decay_integration_contribution, replay_decay_integration_contribution,
                decay_adjusted_delta, decay_integration_delta,
                freshness_state_match, aggregate_decay_score_match,
                family_decay_score_match,
                stale_memory_flag_match, contradiction_flag_match,
                decay_family_rank_match, decay_composite_family_rank_match,
                drift_reason_codes, created_at
            from public.cross_asset_family_decay_replay_stability_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 300
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_decay_replay_stability_aggregate(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select workspace_id::text as workspace_id,
                   validation_count,
                   context_match_rate, regime_match_rate, timing_class_match_rate,
                   transition_state_match_rate, sequence_class_match_rate,
                   archetype_match_rate, cluster_state_match_rate,
                   persistence_state_match_rate, memory_score_match_rate,
                   freshness_state_match_rate, aggregate_decay_score_match_rate,
                   stale_memory_flag_match_rate, contradiction_flag_match_rate,
                   decay_attribution_match_rate, decay_composite_match_rate,
                   decay_dominant_family_match_rate,
                   drift_detected_count, latest_validated_at
            from public.cross_asset_decay_replay_stability_aggregate
            where workspace_id = %s::uuid
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_cross_asset_decay_attribution_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   freshness_state, aggregate_decay_score,
                   stale_memory_flag, contradiction_flag,
                   decay_adjusted_cross_asset_contribution,
                   decay_dominant_dependency_family, created_at
            from public.run_cross_asset_decay_attribution_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_cross_asset_decay_composite_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   composite_pre_decay, decay_net_contribution, composite_post_decay,
                   freshness_state, aggregate_decay_score,
                   stale_memory_flag, contradiction_flag,
                   integration_mode, created_at
            from public.cross_asset_decay_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_decay_state_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   freshness_state, aggregate_decay_score,
                   memory_score, persistence_state,
                   stale_memory_flag, contradiction_flag,
                   latest_stale_memory_event_type, created_at
            from public.run_cross_asset_signal_decay_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_context_snapshot_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select context_snapshot_id::text as context_snapshot_id
            from public.cross_asset_attribution_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_replay_pair_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as replay_run_id,
                   replayed_from_run_id::text as source_run_id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id
            from public.job_runs
            where id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
