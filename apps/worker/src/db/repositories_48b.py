"""Phase 4.8B repository helpers — Conflict-Aware Attribution.

Lives alongside ``repositories.py`` (and the 4.7B/4.7C/4.7D/4.8A sibling
helpers) so we don't have to edit the very large ``repositories.py``
module. The 4.8B service imports from here for persistence and from
``repositories`` for the shared ``_json_compatible`` helper.
"""

from __future__ import annotations

import json
from typing import Any  # noqa: F401

from src.db.repositories import _json_compatible  # noqa: F401  (re-used helper)


# ── A. Profiles ─────────────────────────────────────────────────────────

def list_cross_asset_conflict_attribution_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active,
                   aligned_supportive_weight, aligned_suppressive_weight,
                   partial_agreement_weight, conflicted_weight,
                   unreliable_weight, insufficient_context_weight,
                   agreement_bonus_scale, conflict_penalty_scale,
                   unreliable_penalty_scale,
                   dominant_conflict_source_penalties,
                   conflict_family_overrides,
                   metadata, created_at
            from public.cross_asset_conflict_attribution_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_conflict_attribution_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active,
                   aligned_supportive_weight, aligned_suppressive_weight,
                   partial_agreement_weight, conflicted_weight,
                   unreliable_weight, insufficient_context_weight,
                   agreement_bonus_scale, conflict_penalty_scale,
                   unreliable_penalty_scale,
                   dominant_conflict_source_penalties,
                   conflict_family_overrides,
                   metadata, created_at
            from public.cross_asset_conflict_attribution_profiles
            where workspace_id = %s::uuid
              and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


# ── B. Family snapshot insert ───────────────────────────────────────────

def insert_cross_asset_family_conflict_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    conflict_profile_id,
    dependency_family,
    raw_family_net_contribution,
    weighted_family_net_contribution,
    regime_adjusted_family_contribution,
    timing_adjusted_family_contribution,
    transition_adjusted_family_contribution,
    archetype_adjusted_family_contribution,
    cluster_adjusted_family_contribution,
    persistence_adjusted_family_contribution,
    decay_adjusted_family_contribution,
    family_consensus_state,
    agreement_score,
    conflict_score,
    dominant_conflict_source,
    transition_direction,
    archetype_direction,
    cluster_direction,
    persistence_direction,
    decay_direction,
    conflict_weight,
    conflict_bonus,
    conflict_penalty,
    conflict_adjusted_family_contribution,
    conflict_family_rank,
    top_symbols,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_family_conflict_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                conflict_profile_id, dependency_family,
                raw_family_net_contribution, weighted_family_net_contribution,
                regime_adjusted_family_contribution, timing_adjusted_family_contribution,
                transition_adjusted_family_contribution, archetype_adjusted_family_contribution,
                cluster_adjusted_family_contribution, persistence_adjusted_family_contribution,
                decay_adjusted_family_contribution,
                family_consensus_state, agreement_score, conflict_score, dominant_conflict_source,
                transition_direction, archetype_direction, cluster_direction,
                persistence_direction, decay_direction,
                conflict_weight, conflict_bonus, conflict_penalty,
                conflict_adjusted_family_contribution, conflict_family_rank,
                top_symbols, reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                conflict_profile_id, dependency_family,
                raw_family_net_contribution, weighted_family_net_contribution,
                regime_adjusted_family_contribution, timing_adjusted_family_contribution,
                transition_adjusted_family_contribution, archetype_adjusted_family_contribution,
                cluster_adjusted_family_contribution, persistence_adjusted_family_contribution,
                decay_adjusted_family_contribution,
                family_consensus_state, agreement_score, conflict_score, dominant_conflict_source,
                transition_direction, archetype_direction, cluster_direction,
                persistence_direction, decay_direction,
                conflict_weight, conflict_bonus, conflict_penalty,
                conflict_adjusted_family_contribution,
                int(conflict_family_rank) if conflict_family_rank is not None else None,
                json.dumps(list(top_symbols or [])),
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                "cross_asset_family_conflict_attribution_snapshots insert returned no row"
            )
        return dict(row)


# ── C. Symbol snapshot insert ───────────────────────────────────────────

def insert_cross_asset_symbol_conflict_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    conflict_profile_id,
    symbol,
    dependency_family,
    dependency_type,
    family_consensus_state,
    agreement_score,
    conflict_score,
    dominant_conflict_source,
    raw_symbol_score,
    weighted_symbol_score,
    regime_adjusted_symbol_score,
    timing_adjusted_symbol_score,
    transition_adjusted_symbol_score,
    archetype_adjusted_symbol_score,
    cluster_adjusted_symbol_score,
    persistence_adjusted_symbol_score,
    decay_adjusted_symbol_score,
    conflict_weight,
    conflict_adjusted_symbol_score,
    symbol_rank,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_symbol_conflict_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                conflict_profile_id, symbol, dependency_family, dependency_type,
                family_consensus_state, agreement_score, conflict_score, dominant_conflict_source,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score, archetype_adjusted_symbol_score,
                cluster_adjusted_symbol_score, persistence_adjusted_symbol_score,
                decay_adjusted_symbol_score,
                conflict_weight, conflict_adjusted_symbol_score, symbol_rank,
                reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s, %s,
                %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                conflict_profile_id, symbol, dependency_family, dependency_type,
                family_consensus_state, agreement_score, conflict_score, dominant_conflict_source,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score, archetype_adjusted_symbol_score,
                cluster_adjusted_symbol_score, persistence_adjusted_symbol_score,
                decay_adjusted_symbol_score,
                conflict_weight, conflict_adjusted_symbol_score,
                int(symbol_rank) if symbol_rank is not None else None,
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                "cross_asset_symbol_conflict_attribution_snapshots insert returned no row"
            )
        return dict(row)


# ── D. Family summary fetch ─────────────────────────────────────────────

def get_cross_asset_family_conflict_attribution_summary(
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
                decay_adjusted_family_contribution,
                family_consensus_state, agreement_score, conflict_score, dominant_conflict_source,
                transition_direction, archetype_direction, cluster_direction,
                persistence_direction, decay_direction,
                conflict_weight, conflict_bonus, conflict_penalty,
                conflict_adjusted_family_contribution, conflict_family_rank,
                top_symbols, reason_codes, created_at
            from public.cross_asset_family_conflict_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, conflict_family_rank asc nulls last
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ── E. Symbol summary fetch ─────────────────────────────────────────────

def get_cross_asset_symbol_conflict_attribution_summary(
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
                family_consensus_state, agreement_score, conflict_score, dominant_conflict_source,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score, archetype_adjusted_symbol_score,
                cluster_adjusted_symbol_score, persistence_adjusted_symbol_score,
                decay_adjusted_symbol_score,
                conflict_weight, conflict_adjusted_symbol_score, symbol_rank,
                reason_codes, created_at
            from public.cross_asset_symbol_conflict_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, symbol_rank asc nulls last
            limit 300
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ── F. Run-level integration summary fetch ──────────────────────────────

def get_run_cross_asset_conflict_attribution_summary(
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
                conflict_adjusted_cross_asset_contribution,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                transition_dominant_dependency_family,
                archetype_dominant_dependency_family,
                cluster_dominant_dependency_family,
                persistence_dominant_dependency_family,
                decay_dominant_dependency_family,
                conflict_dominant_dependency_family,
                layer_consensus_state, agreement_score, conflict_score, dominant_conflict_source,
                created_at
            from public.run_cross_asset_conflict_attribution_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ── G. Conflict context for a run (4.8A run-level layer summary) ────────

def get_cross_asset_conflict_context_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
                   layer_consensus_state, agreement_score, conflict_score,
                   dominant_conflict_source,
                   freshness_state, persistence_state, cluster_state,
                   latest_conflict_event_type, created_at
            from public.run_cross_asset_layer_conflict_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_cross_asset_family_conflict_context_for_run(conn, *, run_id):
    """Pull family-level conflict context for the given run.

    Returns a dict keyed by ``dependency_family``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family,
                   family_consensus_state, agreement_score, conflict_score,
                   dominant_conflict_source,
                   transition_direction, archetype_direction, cluster_direction,
                   persistence_direction, decay_direction,
                   created_at
            from public.cross_asset_family_layer_agreement_summary
            where run_id = %s::uuid
            """,
            (run_id,),
        )
        out: dict[str, dict] = {}
        for r in cur.fetchall():
            row = dict(r)
            out[row["dependency_family"]] = row
        return out
