"""Phase 4.8A repository helpers (sibling module to repositories.py)."""

from __future__ import annotations

import json
from typing import Any

from src.db.repositories import _json_compatible  # noqa: F401  (re-used helper)


def list_cross_asset_conflict_policy_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active,
                   timing_weight, transition_weight, archetype_weight,
                   cluster_weight, persistence_weight, decay_weight,
                   agreement_threshold, partial_agreement_threshold,
                   conflict_threshold, unreliable_threshold,
                   metadata, created_at
            from public.cross_asset_conflict_policy_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_conflict_policy_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active,
                   timing_weight, transition_weight, archetype_weight,
                   cluster_weight, persistence_weight, decay_weight,
                   agreement_threshold, partial_agreement_threshold,
                   conflict_threshold, unreliable_threshold,
                   metadata, created_at
            from public.cross_asset_conflict_policy_profiles
            where workspace_id = %s::uuid
              and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_layer_agreement_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    conflict_policy_profile_id,
    dominant_timing_class,
    dominant_transition_state,
    dominant_sequence_class,
    dominant_archetype_key,
    cluster_state,
    persistence_state,
    freshness_state,
    timing_direction,
    transition_direction,
    archetype_direction,
    cluster_direction,
    persistence_direction,
    decay_direction,
    supportive_weight,
    suppressive_weight,
    neutral_weight,
    missing_weight,
    agreement_score,
    conflict_score,
    layer_consensus_state,
    dominant_conflict_source,
    conflict_reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_layer_agreement_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                conflict_policy_profile_id,
                dominant_timing_class, dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state, persistence_state, freshness_state,
                timing_direction, transition_direction, archetype_direction,
                cluster_direction, persistence_direction, decay_direction,
                supportive_weight, suppressive_weight, neutral_weight, missing_weight,
                agreement_score, conflict_score,
                layer_consensus_state, dominant_conflict_source,
                conflict_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                conflict_policy_profile_id,
                dominant_timing_class, dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state, persistence_state, freshness_state,
                timing_direction, transition_direction, archetype_direction,
                cluster_direction, persistence_direction, decay_direction,
                supportive_weight, suppressive_weight, neutral_weight, missing_weight,
                agreement_score, conflict_score,
                layer_consensus_state, dominant_conflict_source,
                json.dumps(list(conflict_reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_layer_agreement_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_layer_agreement_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    transition_state,
    dominant_sequence_class,
    archetype_key,
    cluster_state,
    persistence_state,
    freshness_state,
    family_contribution,
    transition_direction,
    archetype_direction,
    cluster_direction,
    persistence_direction,
    decay_direction,
    agreement_score,
    conflict_score,
    family_consensus_state,
    dominant_conflict_source,
    family_rank,
    conflict_reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_family_layer_agreement_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                transition_state, dominant_sequence_class, archetype_key,
                cluster_state, persistence_state, freshness_state,
                family_contribution,
                transition_direction, archetype_direction, cluster_direction,
                persistence_direction, decay_direction,
                agreement_score, conflict_score,
                family_consensus_state, dominant_conflict_source,
                family_rank, conflict_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s, %s,
                %s, %s, %s,
                %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                transition_state, dominant_sequence_class, archetype_key,
                cluster_state, persistence_state, freshness_state,
                family_contribution,
                transition_direction, archetype_direction, cluster_direction,
                persistence_direction, decay_direction,
                agreement_score, conflict_score,
                family_consensus_state, dominant_conflict_source,
                int(family_rank) if family_rank is not None else None,
                json.dumps(list(conflict_reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_family_layer_agreement_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_layer_conflict_event_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    target_run_id,
    prior_consensus_state,
    current_consensus_state,
    prior_dominant_conflict_source,
    current_dominant_conflict_source,
    prior_agreement_score,
    current_agreement_score,
    prior_conflict_score,
    current_conflict_score,
    event_type,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_layer_conflict_event_snapshots (
                workspace_id, watchlist_id, source_run_id, target_run_id,
                prior_consensus_state, current_consensus_state,
                prior_dominant_conflict_source, current_dominant_conflict_source,
                prior_agreement_score, current_agreement_score,
                prior_conflict_score, current_conflict_score,
                event_type, reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, target_run_id,
                prior_consensus_state, current_consensus_state,
                prior_dominant_conflict_source, current_dominant_conflict_source,
                prior_agreement_score, current_agreement_score,
                prior_conflict_score, current_conflict_score,
                event_type,
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_layer_conflict_event_snapshots insert returned no row")
        return dict(row)


def get_cross_asset_layer_agreement_summary(conn, *, workspace_id, watchlist_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dominant_timing_class, dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state, persistence_state, freshness_state,
                timing_direction, transition_direction, archetype_direction,
                cluster_direction, persistence_direction, decay_direction,
                supportive_weight, suppressive_weight, neutral_weight, missing_weight,
                agreement_score, conflict_score,
                layer_consensus_state, dominant_conflict_source,
                conflict_reason_codes, created_at
            from public.cross_asset_layer_agreement_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_layer_agreement_summary(conn, *, workspace_id, watchlist_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                transition_state, dominant_sequence_class, archetype_key,
                cluster_state, persistence_state, freshness_state,
                family_contribution,
                transition_direction, archetype_direction, cluster_direction,
                persistence_direction, decay_direction,
                agreement_score, conflict_score,
                family_consensus_state, dominant_conflict_source, family_rank,
                conflict_reason_codes, created_at
            from public.cross_asset_family_layer_agreement_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, family_rank asc nulls last
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_layer_conflict_event_summary(conn, *, workspace_id, watchlist_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text  as workspace_id,
                watchlist_id::text  as watchlist_id,
                source_run_id::text as source_run_id,
                target_run_id::text as target_run_id,
                prior_consensus_state, current_consensus_state,
                prior_dominant_conflict_source, current_dominant_conflict_source,
                prior_agreement_score, current_agreement_score,
                prior_conflict_score, current_conflict_score,
                event_type, reason_codes, created_at
            from public.cross_asset_layer_conflict_event_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 100
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_layer_conflict_summary(conn, *, run_id=None, workspace_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text       as run_id,
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                layer_consensus_state, agreement_score, conflict_score,
                dominant_conflict_source,
                freshness_state, persistence_state, cluster_state,
                latest_conflict_event_type, created_at
            from public.run_cross_asset_layer_conflict_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_current_layer_conflict_context(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   layer_consensus_state, agreement_score, conflict_score,
                   dominant_conflict_source, created_at
            from public.cross_asset_layer_agreement_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_family_layer_conflict_context(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family, family_consensus_state,
                   agreement_score, conflict_score, dominant_conflict_source
            from public.cross_asset_family_layer_agreement_summary
            where run_id = %s::uuid
            order by family_rank asc nulls last
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_prior_layer_agreement_for_watchlist(
    conn, *, workspace_id, watchlist_id, before_run_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   layer_consensus_state, dominant_conflict_source,
                   agreement_score, conflict_score, created_at
            from public.cross_asset_layer_agreement_summary
            where workspace_id = %s::uuid
              and watchlist_id = %s::uuid
              and (%s::uuid is null or run_id <> %s::uuid)
            order by created_at desc
            limit 1
            """,
            (workspace_id, watchlist_id, before_run_id, before_run_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None
