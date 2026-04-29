"""Phase 4.7B repository helpers.

Lives alongside ``repositories.py`` to avoid editing the very large
``repositories.py`` module. The 4.7B service imports from here and from
``repositories`` for the shared ``_json_compatible`` helper.
"""

from __future__ import annotations

import json
from typing import Any

from src.db.repositories import _json_compatible  # noqa: F401  (re-used helper)


def list_cross_asset_decay_attribution_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active,
                   fresh_weight, decaying_weight, stale_weight,
                   contradicted_weight, mixed_weight, insufficient_history_weight,
                   freshness_bonus_scale, stale_penalty_scale,
                   contradiction_penalty_scale, decay_score_penalty_scale,
                   decay_family_overrides, metadata, created_at
            from public.cross_asset_decay_attribution_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_decay_attribution_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active,
                   fresh_weight, decaying_weight, stale_weight,
                   contradicted_weight, mixed_weight, insufficient_history_weight,
                   freshness_bonus_scale, stale_penalty_scale,
                   contradiction_penalty_scale, decay_score_penalty_scale,
                   decay_family_overrides, metadata, created_at
            from public.cross_asset_decay_attribution_profiles
            where workspace_id = %s::uuid
              and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_family_decay_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    decay_profile_id,
    dependency_family,
    raw_family_net_contribution,
    weighted_family_net_contribution,
    regime_adjusted_family_contribution,
    timing_adjusted_family_contribution,
    transition_adjusted_family_contribution,
    archetype_adjusted_family_contribution,
    cluster_adjusted_family_contribution,
    persistence_adjusted_family_contribution,
    freshness_state,
    aggregate_decay_score,
    family_decay_score,
    memory_score,
    state_age_runs,
    stale_memory_flag,
    contradiction_flag,
    decay_weight,
    decay_bonus,
    decay_penalty,
    decay_adjusted_family_contribution,
    decay_family_rank,
    top_symbols,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_family_decay_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                decay_profile_id, dependency_family,
                raw_family_net_contribution, weighted_family_net_contribution,
                regime_adjusted_family_contribution, timing_adjusted_family_contribution,
                transition_adjusted_family_contribution, archetype_adjusted_family_contribution,
                cluster_adjusted_family_contribution, persistence_adjusted_family_contribution,
                freshness_state, aggregate_decay_score, family_decay_score,
                memory_score, state_age_runs,
                stale_memory_flag, contradiction_flag,
                decay_weight, decay_bonus, decay_penalty,
                decay_adjusted_family_contribution, decay_family_rank,
                top_symbols, reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                decay_profile_id, dependency_family,
                raw_family_net_contribution, weighted_family_net_contribution,
                regime_adjusted_family_contribution, timing_adjusted_family_contribution,
                transition_adjusted_family_contribution, archetype_adjusted_family_contribution,
                cluster_adjusted_family_contribution, persistence_adjusted_family_contribution,
                freshness_state, aggregate_decay_score, family_decay_score,
                memory_score,
                int(state_age_runs) if state_age_runs is not None else None,
                bool(stale_memory_flag), bool(contradiction_flag),
                decay_weight, decay_bonus, decay_penalty,
                decay_adjusted_family_contribution,
                int(decay_family_rank) if decay_family_rank is not None else None,
                json.dumps(list(top_symbols or [])),
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_family_decay_attribution_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_symbol_decay_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    decay_profile_id,
    symbol,
    dependency_family,
    dependency_type,
    freshness_state,
    aggregate_decay_score,
    family_decay_score,
    memory_score,
    state_age_runs,
    stale_memory_flag,
    contradiction_flag,
    raw_symbol_score,
    weighted_symbol_score,
    regime_adjusted_symbol_score,
    timing_adjusted_symbol_score,
    transition_adjusted_symbol_score,
    archetype_adjusted_symbol_score,
    cluster_adjusted_symbol_score,
    persistence_adjusted_symbol_score,
    decay_weight,
    decay_adjusted_symbol_score,
    symbol_rank,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_symbol_decay_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                decay_profile_id, symbol, dependency_family, dependency_type,
                freshness_state, aggregate_decay_score, family_decay_score,
                memory_score, state_age_runs,
                stale_memory_flag, contradiction_flag,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score, archetype_adjusted_symbol_score,
                cluster_adjusted_symbol_score, persistence_adjusted_symbol_score,
                decay_weight, decay_adjusted_symbol_score, symbol_rank,
                reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                decay_profile_id, symbol, dependency_family, dependency_type,
                freshness_state, aggregate_decay_score, family_decay_score,
                memory_score,
                int(state_age_runs) if state_age_runs is not None else None,
                bool(stale_memory_flag), bool(contradiction_flag),
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score, archetype_adjusted_symbol_score,
                cluster_adjusted_symbol_score, persistence_adjusted_symbol_score,
                decay_weight, decay_adjusted_symbol_score,
                int(symbol_rank) if symbol_rank is not None else None,
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_symbol_decay_attribution_snapshots insert returned no row")
        return dict(row)


def get_cross_asset_family_decay_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                raw_family_net_contribution, weighted_family_net_contribution,
                regime_adjusted_family_contribution, timing_adjusted_family_contribution,
                transition_adjusted_family_contribution, archetype_adjusted_family_contribution,
                cluster_adjusted_family_contribution, persistence_adjusted_family_contribution,
                freshness_state, aggregate_decay_score, family_decay_score,
                memory_score, state_age_runs,
                stale_memory_flag, contradiction_flag,
                decay_weight, decay_bonus, decay_penalty,
                decay_adjusted_family_contribution, decay_family_rank,
                top_symbols, reason_codes, created_at
            from public.cross_asset_family_decay_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, decay_family_rank asc nulls last
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_symbol_decay_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                symbol, dependency_family, dependency_type,
                freshness_state, aggregate_decay_score, family_decay_score,
                memory_score, state_age_runs,
                stale_memory_flag, contradiction_flag,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score, archetype_adjusted_symbol_score,
                cluster_adjusted_symbol_score, persistence_adjusted_symbol_score,
                decay_weight, decay_adjusted_symbol_score, symbol_rank,
                reason_codes, created_at
            from public.cross_asset_symbol_decay_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, symbol_rank asc nulls last
            limit 300
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_decay_attribution_summary(
    conn, *, run_id=None, workspace_id=None,
):
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
            from public.run_cross_asset_decay_attribution_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_decay_context_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
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
