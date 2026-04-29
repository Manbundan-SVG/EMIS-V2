"""Phase 4.7C repository helpers (sibling module to repositories.py)."""

from __future__ import annotations

import json
from typing import Any

from src.db.repositories import _json_compatible  # noqa: F401  (re-used helper)


def list_cross_asset_decay_integration_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active, integration_mode,
                   integration_weight,
                   fresh_scale, decaying_scale, stale_scale,
                   contradicted_scale, mixed_scale, insufficient_history_scale,
                   stale_extra_suppression, contradiction_extra_suppression,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at
            from public.cross_asset_decay_integration_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_decay_integration_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active, integration_mode,
                   integration_weight,
                   fresh_scale, decaying_scale, stale_scale,
                   contradicted_scale, mixed_scale, insufficient_history_scale,
                   stale_extra_suppression, contradiction_extra_suppression,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at
            from public.cross_asset_decay_integration_profiles
            where workspace_id = %s::uuid
              and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_decay_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    decay_integration_profile_id,
    base_signal_score,
    cross_asset_net_contribution,
    weighted_cross_asset_net_contribution,
    regime_adjusted_cross_asset_contribution,
    timing_adjusted_cross_asset_contribution,
    transition_adjusted_cross_asset_contribution,
    archetype_adjusted_cross_asset_contribution,
    cluster_adjusted_cross_asset_contribution,
    persistence_adjusted_cross_asset_contribution,
    decay_adjusted_cross_asset_contribution,
    composite_pre_decay,
    decay_net_contribution,
    composite_post_decay,
    freshness_state,
    aggregate_decay_score,
    stale_memory_flag,
    contradiction_flag,
    integration_mode,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_decay_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                decay_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution, weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution, timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution, archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution, persistence_adjusted_cross_asset_contribution,
                decay_adjusted_cross_asset_contribution,
                composite_pre_decay, decay_net_contribution, composite_post_decay,
                freshness_state, aggregate_decay_score,
                stale_memory_flag, contradiction_flag,
                integration_mode, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                decay_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution, weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution, timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution, archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution, persistence_adjusted_cross_asset_contribution,
                decay_adjusted_cross_asset_contribution,
                composite_pre_decay, decay_net_contribution, composite_post_decay,
                freshness_state, aggregate_decay_score,
                bool(stale_memory_flag), bool(contradiction_flag),
                integration_mode,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_decay_composite_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_decay_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    freshness_state,
    aggregate_decay_score,
    family_decay_score,
    stale_memory_flag,
    contradiction_flag,
    decay_adjusted_family_contribution,
    integration_weight_applied,
    decay_integration_contribution,
    family_rank,
    top_symbols,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_family_decay_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                freshness_state, aggregate_decay_score, family_decay_score,
                stale_memory_flag, contradiction_flag,
                decay_adjusted_family_contribution,
                integration_weight_applied,
                decay_integration_contribution,
                family_rank,
                top_symbols, reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s, %s,
                %s, %s,
                %s,
                %s,
                %s,
                %s,
                %s::jsonb, %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                freshness_state, aggregate_decay_score, family_decay_score,
                bool(stale_memory_flag), bool(contradiction_flag),
                decay_adjusted_family_contribution,
                integration_weight_applied,
                decay_integration_contribution,
                int(family_rank) if family_rank is not None else None,
                json.dumps(list(top_symbols or [])),
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_family_decay_composite_snapshots insert returned no row")
        return dict(row)


def get_cross_asset_decay_composite_summary(conn, *, workspace_id, watchlist_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                persistence_adjusted_cross_asset_contribution,
                decay_adjusted_cross_asset_contribution,
                composite_pre_decay, decay_net_contribution, composite_post_decay,
                freshness_state, aggregate_decay_score,
                stale_memory_flag, contradiction_flag,
                integration_mode, created_at
            from public.cross_asset_decay_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_decay_composite_summary(conn, *, workspace_id, watchlist_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                freshness_state, aggregate_decay_score, family_decay_score,
                stale_memory_flag, contradiction_flag,
                decay_adjusted_family_contribution,
                integration_weight_applied,
                decay_integration_contribution,
                family_rank, top_symbols, reason_codes, created_at
            from public.cross_asset_family_decay_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, family_rank asc nulls last
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_decay_integration_summary(conn, *, run_id=None, workspace_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text       as run_id,
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                persistence_adjusted_cross_asset_contribution,
                decay_adjusted_cross_asset_contribution,
                decay_net_contribution,
                composite_pre_decay, composite_post_decay,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                transition_dominant_dependency_family,
                archetype_dominant_dependency_family,
                cluster_dominant_dependency_family,
                persistence_dominant_dependency_family,
                decay_dominant_dependency_family,
                freshness_state, aggregate_decay_score,
                stale_memory_flag, contradiction_flag,
                created_at
            from public.run_cross_asset_decay_integration_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_decay_attribution_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
                   freshness_state, aggregate_decay_score,
                   stale_memory_flag, contradiction_flag,
                   decay_adjusted_cross_asset_contribution,
                   decay_dominant_dependency_family,
                   created_at
            from public.run_cross_asset_decay_attribution_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
