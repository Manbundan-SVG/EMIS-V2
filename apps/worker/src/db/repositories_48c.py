"""Phase 4.8C repository helpers — Conflict-Aware Composite Refinement.

Lives alongside ``repositories.py`` (and the 4.7B/4.7C/4.7D/4.8A/4.8B
sibling helpers) so we don't have to edit the very large
``repositories.py`` module. The 4.8C service imports from here for
persistence and from ``repositories`` for the shared
``_json_compatible`` helper.
"""

from __future__ import annotations

import json
from typing import Any  # noqa: F401

from src.db.repositories import _json_compatible  # noqa: F401  (re-used helper)


# ── A. Conflict Integration Profiles ────────────────────────────────────

def list_cross_asset_conflict_integration_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active, integration_mode, integration_weight,
                   aligned_supportive_scale, aligned_suppressive_scale,
                   partial_agreement_scale, conflicted_scale,
                   unreliable_scale, insufficient_context_scale,
                   conflict_extra_suppression, unreliable_extra_suppression,
                   dominant_conflict_source_suppression,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at
            from public.cross_asset_conflict_integration_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_conflict_integration_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active, integration_mode, integration_weight,
                   aligned_supportive_scale, aligned_suppressive_scale,
                   partial_agreement_scale, conflicted_scale,
                   unreliable_scale, insufficient_context_scale,
                   conflict_extra_suppression, unreliable_extra_suppression,
                   dominant_conflict_source_suppression,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at
            from public.cross_asset_conflict_integration_profiles
            where workspace_id = %s::uuid
              and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


# ── B. Run-level conflict composite snapshot insert ─────────────────────

def insert_cross_asset_conflict_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    conflict_integration_profile_id,
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
    conflict_adjusted_cross_asset_contribution,
    composite_pre_conflict,
    conflict_net_contribution,
    composite_post_conflict,
    layer_consensus_state,
    agreement_score,
    conflict_score,
    dominant_conflict_source,
    integration_mode,
    source_contribution_layer,
    source_composite_layer,
    scoring_version,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_conflict_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                conflict_integration_profile_id, base_signal_score,
                cross_asset_net_contribution, weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution, timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution, archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution, persistence_adjusted_cross_asset_contribution,
                decay_adjusted_cross_asset_contribution, conflict_adjusted_cross_asset_contribution,
                composite_pre_conflict, conflict_net_contribution, composite_post_conflict,
                layer_consensus_state, agreement_score, conflict_score, dominant_conflict_source,
                integration_mode, source_contribution_layer, source_composite_layer,
                scoring_version, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                conflict_integration_profile_id, base_signal_score,
                cross_asset_net_contribution, weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution, timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution, archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution, persistence_adjusted_cross_asset_contribution,
                decay_adjusted_cross_asset_contribution, conflict_adjusted_cross_asset_contribution,
                composite_pre_conflict, conflict_net_contribution, composite_post_conflict,
                layer_consensus_state, agreement_score, conflict_score, dominant_conflict_source,
                integration_mode, source_contribution_layer, source_composite_layer,
                scoring_version,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                "cross_asset_conflict_composite_snapshots insert returned no row"
            )
        return dict(row)


# ── C. Family conflict composite snapshot insert ────────────────────────

def insert_cross_asset_family_conflict_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    family_consensus_state,
    agreement_score,
    conflict_score,
    dominant_conflict_source,
    conflict_adjusted_family_contribution,
    integration_weight_applied,
    conflict_integration_contribution,
    family_rank,
    top_symbols,
    reason_codes,
    source_contribution_layer,
    scoring_version,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_family_conflict_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family, family_consensus_state,
                agreement_score, conflict_score, dominant_conflict_source,
                conflict_adjusted_family_contribution, integration_weight_applied,
                conflict_integration_contribution, family_rank,
                top_symbols, reason_codes,
                source_contribution_layer, scoring_version, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb,
                %s, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family, family_consensus_state,
                agreement_score, conflict_score, dominant_conflict_source,
                conflict_adjusted_family_contribution, integration_weight_applied,
                conflict_integration_contribution,
                int(family_rank) if family_rank is not None else None,
                json.dumps(list(top_symbols or [])),
                json.dumps(list(reason_codes or [])),
                source_contribution_layer, scoring_version,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                "cross_asset_family_conflict_composite_snapshots insert returned no row"
            )
        return dict(row)


# ── D. Conflict composite summary fetch ─────────────────────────────────

def get_cross_asset_conflict_composite_summary(
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
                conflict_adjusted_cross_asset_contribution,
                composite_pre_conflict,
                conflict_net_contribution,
                composite_post_conflict,
                layer_consensus_state, agreement_score, conflict_score,
                dominant_conflict_source,
                integration_mode, source_contribution_layer, source_composite_layer,
                scoring_version, created_at
            from public.cross_asset_conflict_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 100
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ── E. Family conflict composite summary fetch ─────────────────────────

def get_cross_asset_family_conflict_composite_summary(
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
                family_consensus_state, agreement_score, conflict_score,
                dominant_conflict_source,
                conflict_adjusted_family_contribution,
                integration_weight_applied,
                conflict_integration_contribution,
                family_rank, top_symbols, reason_codes,
                source_contribution_layer, scoring_version, created_at
            from public.cross_asset_family_conflict_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, family_rank asc nulls last
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ── F. Final conflict integration summary fetch ─────────────────────────

def get_run_cross_asset_conflict_integration_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
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
                conflict_net_contribution,
                composite_pre_conflict,
                composite_post_conflict,
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
                layer_consensus_state, agreement_score, conflict_score,
                dominant_conflict_source,
                integration_mode, source_contribution_layer, source_composite_layer,
                scoring_version, created_at
            from public.run_cross_asset_conflict_integration_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


# ── G. Conflict-aware attribution for a run (4.8B input) ───────────────

def get_cross_asset_conflict_attribution_for_run(conn, *, run_id, workspace_id):  # noqa: ARG001
    """Pull the run-level conflict-aware attribution row from 4.8B's bridge view.

    Returns a single dict or None.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text       as run_id,
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


def get_cross_asset_family_conflict_attribution_for_run(conn, *, run_id):
    """Pull family-level conflict-aware attribution rows for the given run.

    Returns a list of dicts (one per family).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family,
                   conflict_adjusted_family_contribution,
                   family_consensus_state,
                   agreement_score, conflict_score,
                   dominant_conflict_source,
                   conflict_family_rank,
                   top_symbols,
                   reason_codes,
                   created_at
            from public.cross_asset_family_conflict_attribution_summary
            where run_id = %s::uuid
            order by conflict_family_rank asc nulls last
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]


# ── H. Composite-pre-conflict fallback chain readers ────────────────────

def get_run_decay_composite(conn, *, run_id):
    """4.7C — primary fallback for composite_pre_conflict."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select composite_post_decay, base_signal_score, context_snapshot_id
            from public.cross_asset_decay_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_run_persistence_composite(conn, *, run_id):
    """4.6C fallback."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select composite_post_persistence, base_signal_score, context_snapshot_id
            from public.cross_asset_persistence_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_run_cluster_composite(conn, *, run_id):
    """4.5C fallback."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select composite_post_cluster, base_signal_score, context_snapshot_id
            from public.cross_asset_cluster_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_run_archetype_composite(conn, *, run_id):
    """4.4C fallback."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select composite_post_archetype, base_signal_score, context_snapshot_id
            from public.cross_asset_archetype_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_run_transition_composite(conn, *, run_id):
    """4.3C fallback."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select composite_post_transition, base_signal_score, context_snapshot_id
            from public.cross_asset_transition_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_run_timing_composite(conn, *, run_id):
    """4.2C fallback."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select composite_post_timing, base_signal_score, context_snapshot_id
            from public.cross_asset_timing_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
