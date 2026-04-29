"""Phase 4.6C: Persistence-Aware Composite Refinement Service.

Reads 4.6B persistence-aware attribution and the most mature upstream
composite (4.5C composite_post_cluster → 4.4C composite_post_archetype →
4.3C composite_post_transition → 4.2C composite_post_timing → 4.1C regime
equivalent → 4.1B weighted equivalent → 4.1A raw composite post) and emits
a bounded persistence-aware delta that refines the final integrated score.

Persists:
  * one cross_asset_persistence_composite_snapshots row per run
  * one cross_asset_family_persistence_composite_snapshots row per family

All adjustments are deterministic; persistence-aware net contribution is
clipped to a conservative band and integration weight is small and
explicit. Memory-break events apply additional sign-aware suppression.
Persistence-aware integration never dominates upstream cross-asset
integration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.6C.v1"

_VALID_MODES = frozenset({
    "persistence_additive_guardrailed",
    "persistent_confirmation_only",
    "memory_break_suppression_only",
    "recovery_sensitive",
})

_MEMORY_BREAK_EVENTS = frozenset({
    "persistence_loss",
    "regime_memory_break",
    "cluster_memory_break",
    "archetype_memory_break",
})

_DEFAULT_PROFILE: dict[str, Any] = {
    "profile_name":                   "default_persistence_integration",
    "integration_mode":               "persistence_additive_guardrailed",
    "integration_weight":             0.10,
    "persistent_scale":               1.08,
    "recovering_scale":               1.03,
    "rotating_scale":                 0.98,
    "fragile_scale":                  0.88,
    "breaking_down_scale":            0.80,
    "mixed_scale":                    0.90,
    "insufficient_history_scale":    0.85,
    "memory_break_extra_suppression": 0.02,
    "max_positive_contribution":      0.20,
    "max_negative_contribution":      0.20,
}

# Conservative first-pass clamp.
_NET_BOUND_DEFAULT = 0.15

# Persistence states each integration mode admits.
_MODE_ADMITS: dict[str, frozenset[str]] = {
    "persistence_additive_guardrailed": frozenset({
        "persistent", "fragile", "rotating", "breaking_down",
        "recovering", "mixed", "insufficient_history",
    }),
    "persistent_confirmation_only": frozenset({
        "persistent", "recovering",
    }),
    "memory_break_suppression_only": frozenset({
        "fragile", "breaking_down", "mixed",
    }),
    "recovery_sensitive": frozenset({
        "recovering", "persistent",
    }),
}


@dataclass
class FamilyPersistenceComposite:
    dependency_family: str
    persistence_state: str
    memory_score: float | None
    state_age_runs: int | None
    latest_persistence_event_type: str | None
    persistence_adjusted_family_contribution: float | None
    integration_weight_applied: float
    persistence_integration_contribution: float | None
    family_rank: int | None = None
    top_symbols: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PersistenceCompositeResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    persistence_integration_profile_id: str | None
    profile_name: str
    default_profile_used: bool
    integration_mode: str
    base_signal_score: float | None
    cross_asset_net_contribution: float | None
    weighted_cross_asset_net_contribution: float | None
    regime_adjusted_cross_asset_contribution: float | None
    timing_adjusted_cross_asset_contribution: float | None
    transition_adjusted_cross_asset_contribution: float | None
    archetype_adjusted_cross_asset_contribution: float | None
    cluster_adjusted_cross_asset_contribution: float | None
    persistence_adjusted_cross_asset_contribution: float | None
    composite_pre_persistence: float | None
    persistence_net_contribution: float | None
    composite_post_persistence: float | None
    persistence_state: str
    memory_score: float | None
    state_age_runs: int | None
    latest_persistence_event_type: str | None
    metadata: dict[str, Any]
    family_rows: list[FamilyPersistenceComposite]


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


class CrossAssetPersistenceCompositeService:
    """Deterministic persistence-aware final composite integration."""

    # ── profile loading ─────────────────────────────────────────────────
    def get_active_persistence_integration_profile(
        self, conn, *, workspace_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, profile_name, is_active, integration_mode,
                       integration_weight,
                       persistent_scale, recovering_scale, rotating_scale,
                       fragile_scale, breaking_down_scale, mixed_scale,
                       insufficient_history_scale,
                       memory_break_extra_suppression,
                       max_positive_contribution, max_negative_contribution,
                       metadata, created_at
                from public.cross_asset_persistence_integration_profiles
                where workspace_id = %s::uuid and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ── scale primitive ─────────────────────────────────────────────────
    def compute_persistence_integration_scale(
        self, *, persistence_state: str, profile: dict[str, Any],
    ) -> float:
        key_map = {
            "persistent":           "persistent_scale",
            "recovering":           "recovering_scale",
            "rotating":             "rotating_scale",
            "fragile":              "fragile_scale",
            "breaking_down":        "breaking_down_scale",
            "mixed":                "mixed_scale",
            "insufficient_history": "insufficient_history_scale",
        }
        scale = _as_float(profile.get(key_map.get(persistence_state, "insufficient_history_scale")))
        return scale if scale is not None else 1.0

    @staticmethod
    def _mode_gate_state(mode: str, persistence_state: str) -> bool:
        admits = _MODE_ADMITS.get(mode)
        if admits is None:
            return True
        return persistence_state in admits

    # ── input loading ───────────────────────────────────────────────────
    def _load_run_context(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, Any]:
        """Load all upstream contributions + persistence_pre fallback chain +
        run persistence context."""
        ctx: dict[str, Any] = {
            "context_snapshot_id":         None,
            "base_signal_score":           None,
            "raw_net":                     None,
            "weighted_net":                None,
            "regime_net":                  None,
            "timing_net":                  None,
            "transition_net":              None,
            "archetype_net":               None,
            "cluster_net":                 None,
            "persistence_net":             None,
            "timing_composite_post":       None,
            "transition_composite_post":   None,
            "archetype_composite_post":    None,
            "cluster_composite_post":      None,
            "composite_pre_persistence":   None,
            "composite_pre_source":        None,
            "persistence_state":           "insufficient_history",
            "memory_score":                None,
            "state_age_runs":              None,
            "latest_persistence_event_type": None,
        }
        with conn.cursor() as cur:
            cur.execute(
                """
                select context_snapshot_id::text as context_snapshot_id,
                       base_signal_score,
                       cross_asset_net_contribution,
                       composite_post_cross_asset
                from public.cross_asset_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            raw_composite_post: float | None = None
            if row:
                d = dict(row)
                ctx["context_snapshot_id"] = d.get("context_snapshot_id")
                ctx["base_signal_score"]   = _as_float(d.get("base_signal_score"))
                ctx["raw_net"]             = _as_float(d.get("cross_asset_net_contribution"))
                raw_composite_post         = _as_float(d.get("composite_post_cross_asset"))

            cur.execute(
                """
                select weighted_cross_asset_net_contribution
                from public.run_cross_asset_weighted_integration_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["weighted_net"] = _as_float(dict(row).get("weighted_cross_asset_net_contribution"))

            cur.execute(
                """
                select regime_adjusted_cross_asset_contribution
                from public.run_cross_asset_regime_integration_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["regime_net"] = _as_float(dict(row).get("regime_adjusted_cross_asset_contribution"))

            cur.execute(
                """
                select timing_adjusted_cross_asset_contribution
                from public.run_cross_asset_timing_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["timing_net"] = _as_float(dict(row).get("timing_adjusted_cross_asset_contribution"))

            cur.execute(
                """
                select transition_adjusted_cross_asset_contribution
                from public.run_cross_asset_transition_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["transition_net"] = _as_float(dict(row).get("transition_adjusted_cross_asset_contribution"))

            cur.execute(
                """
                select archetype_adjusted_cross_asset_contribution
                from public.run_cross_asset_archetype_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["archetype_net"] = _as_float(dict(row).get("archetype_adjusted_cross_asset_contribution"))

            cur.execute(
                """
                select cluster_adjusted_cross_asset_contribution
                from public.run_cross_asset_cluster_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["cluster_net"] = _as_float(dict(row).get("cluster_adjusted_cross_asset_contribution"))

            cur.execute(
                """
                select persistence_adjusted_cross_asset_contribution,
                       persistence_state, memory_score, state_age_runs,
                       latest_persistence_event_type
                from public.run_cross_asset_persistence_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                d = dict(row)
                ctx["persistence_net"] = _as_float(d.get("persistence_adjusted_cross_asset_contribution"))
                if d.get("persistence_state"):
                    ctx["persistence_state"] = d.get("persistence_state")
                ctx["memory_score"]                    = _as_float(d.get("memory_score"))
                ctx["state_age_runs"]                  = d.get("state_age_runs")
                ctx["latest_persistence_event_type"]   = d.get("latest_persistence_event_type")

            cur.execute(
                """
                select composite_post_timing
                from public.cross_asset_timing_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["timing_composite_post"] = _as_float(dict(row).get("composite_post_timing"))

            cur.execute(
                """
                select composite_post_transition
                from public.cross_asset_transition_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["transition_composite_post"] = _as_float(dict(row).get("composite_post_transition"))

            cur.execute(
                """
                select composite_post_archetype
                from public.cross_asset_archetype_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["archetype_composite_post"] = _as_float(dict(row).get("composite_post_archetype"))

            cur.execute(
                """
                select composite_post_cluster
                from public.cross_asset_cluster_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["cluster_composite_post"] = _as_float(dict(row).get("composite_post_cluster"))

            # If persistence context still empty, fall back to 4.6A persistence
            # bridge view directly.
            if ctx["persistence_state"] == "insufficient_history":
                cur.execute(
                    """
                    select persistence_state, memory_score, state_age_runs,
                           latest_persistence_event_type
                    from public.run_cross_asset_persistence_summary
                    where run_id = %s::uuid
                    limit 1
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
                if row:
                    d = dict(row)
                    if d.get("persistence_state"):
                        ctx["persistence_state"] = d.get("persistence_state")
                    if ctx["memory_score"] is None:
                        ctx["memory_score"] = _as_float(d.get("memory_score"))
                    if ctx["state_age_runs"] is None:
                        ctx["state_age_runs"] = d.get("state_age_runs")
                    if ctx["latest_persistence_event_type"] is None:
                        ctx["latest_persistence_event_type"] = d.get("latest_persistence_event_type")

        # composite_pre_persistence fallback chain
        INTEGRATION_WEIGHT = 0.10
        base = ctx["base_signal_score"]
        if ctx["cluster_composite_post"] is not None:
            ctx["composite_pre_persistence"] = ctx["cluster_composite_post"]
            ctx["composite_pre_source"] = "cluster_composite_post"
        elif ctx["archetype_composite_post"] is not None:
            ctx["composite_pre_persistence"] = ctx["archetype_composite_post"]
            ctx["composite_pre_source"] = "archetype_composite_post"
        elif ctx["transition_composite_post"] is not None:
            ctx["composite_pre_persistence"] = ctx["transition_composite_post"]
            ctx["composite_pre_source"] = "transition_composite_post"
        elif ctx["timing_composite_post"] is not None:
            ctx["composite_pre_persistence"] = ctx["timing_composite_post"]
            ctx["composite_pre_source"] = "timing_composite_post"
        elif base is not None and ctx["regime_net"] is not None:
            ctx["composite_pre_persistence"] = base + ctx["regime_net"] * INTEGRATION_WEIGHT
            ctx["composite_pre_source"] = "regime_post_integration_equivalent"
        elif base is not None and ctx["weighted_net"] is not None:
            ctx["composite_pre_persistence"] = base + ctx["weighted_net"] * INTEGRATION_WEIGHT
            ctx["composite_pre_source"] = "weighted_post_integration_equivalent"
        elif raw_composite_post is not None:
            ctx["composite_pre_persistence"] = raw_composite_post
            ctx["composite_pre_source"] = "raw_composite_post_cross_asset"
        elif base is not None:
            ctx["composite_pre_persistence"] = base
            ctx["composite_pre_source"] = "base_signal_score"
        return ctx

    def _load_persistence_family_rows(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       persistence_state,
                       memory_score,
                       state_age_runs,
                       latest_persistence_event_type,
                       persistence_adjusted_family_contribution,
                       persistence_family_rank,
                       top_symbols,
                       reason_codes
                from public.cross_asset_family_persistence_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [dict(r) for r in cur.fetchall()]

    # ── core builders ───────────────────────────────────────────────────
    def compute_persistence_net_contribution(
        self,
        *,
        persistence_adjusted_total: float | None,
        persistence_state: str,
        latest_event_type: str | None,
        profile: dict[str, Any],
    ) -> float | None:
        if persistence_adjusted_total is None:
            return None
        mode = profile.get("integration_mode", "persistence_additive_guardrailed")
        if not self._mode_gate_state(mode, persistence_state):
            return 0.0
        base = persistence_adjusted_total
        if mode == "memory_break_suppression_only" and persistence_state in (
            "fragile", "breaking_down", "mixed",
        ):
            base = -abs(base)

        scale = self.compute_persistence_integration_scale(
            persistence_state=persistence_state, profile=profile,
        )
        integration_weight = _as_float(profile.get("integration_weight")) or 0.10
        raw = base * integration_weight * scale

        # Memory-break event extra suppression: subtract a sign-aware fixed
        # amount to push contribution toward zero.
        if latest_event_type in _MEMORY_BREAK_EVENTS:
            extra = _as_float(profile.get("memory_break_extra_suppression")) or 0.02
            sign = 1.0 if raw >= 0 else -1.0
            raw -= sign * abs(extra)

        max_pos = _as_float(profile.get("max_positive_contribution")) or _NET_BOUND_DEFAULT
        max_neg = _as_float(profile.get("max_negative_contribution")) or _NET_BOUND_DEFAULT
        lower = -min(max_neg, _NET_BOUND_DEFAULT)
        upper =  min(max_pos, _NET_BOUND_DEFAULT)
        return _clip(raw, lower, upper)

    def integrate_persistence_with_composite(
        self,
        *,
        composite_pre_persistence: float | None,
        persistence_net_contribution: float | None,
    ) -> float | None:
        if composite_pre_persistence is None:
            return None
        if persistence_net_contribution is None:
            return composite_pre_persistence
        return composite_pre_persistence + persistence_net_contribution

    def compute_family_persistence_integration(
        self,
        family_rows: Sequence[dict[str, Any]],
        *,
        profile: dict[str, Any],
    ) -> list[FamilyPersistenceComposite]:
        integration_weight = _as_float(profile.get("integration_weight")) or 0.10
        mode = profile.get("integration_mode", "persistence_additive_guardrailed")
        max_pos = _as_float(profile.get("max_positive_contribution")) or _NET_BOUND_DEFAULT
        max_neg = _as_float(profile.get("max_negative_contribution")) or _NET_BOUND_DEFAULT
        lower = -min(max_neg, _NET_BOUND_DEFAULT)
        upper =  min(max_pos, _NET_BOUND_DEFAULT)
        memory_break_extra = _as_float(profile.get("memory_break_extra_suppression")) or 0.02

        out: list[FamilyPersistenceComposite] = []
        for r in family_rows:
            fam            = str(r["dependency_family"])
            persist_state  = str(r.get("persistence_state") or "insufficient_history")
            memory_score   = _as_float(r.get("memory_score"))
            state_age      = r.get("state_age_runs")
            latest_event   = r.get("latest_persistence_event_type")
            persist_adj    = _as_float(r.get("persistence_adjusted_family_contribution"))

            scale = self.compute_persistence_integration_scale(
                persistence_state=persist_state, profile=profile,
            )

            top_syms_raw = r.get("top_symbols") or []
            if isinstance(top_syms_raw, str):
                import json
                try:
                    top_syms_raw = json.loads(top_syms_raw)
                except json.JSONDecodeError:
                    top_syms_raw = []
            top_syms = [str(s) for s in top_syms_raw]

            reason_raw = r.get("reason_codes") or []
            if isinstance(reason_raw, str):
                import json
                try:
                    reason_raw = json.loads(reason_raw)
                except json.JSONDecodeError:
                    reason_raw = []
            reason_codes = [str(c) for c in reason_raw]

            contribution: float | None
            if persist_adj is None:
                contribution = None
            else:
                gated = persist_adj
                if not self._mode_gate_state(mode, persist_state):
                    gated = 0.0
                elif mode == "memory_break_suppression_only" and persist_state in (
                    "fragile", "breaking_down", "mixed",
                ):
                    gated = -abs(persist_adj)
                raw = gated * integration_weight * scale
                if latest_event in _MEMORY_BREAK_EVENTS:
                    sign = 1.0 if raw >= 0 else -1.0
                    raw -= sign * abs(memory_break_extra)
                contribution = _clip(raw, lower, upper)

            out.append(FamilyPersistenceComposite(
                dependency_family=fam,
                persistence_state=persist_state,
                memory_score=memory_score,
                state_age_runs=state_age,
                latest_persistence_event_type=latest_event,
                persistence_adjusted_family_contribution=persist_adj,
                integration_weight_applied=integration_weight * scale,
                persistence_integration_contribution=contribution,
                top_symbols=top_syms,
                reason_codes=reason_codes,
                metadata={
                    "scoring_version":          _SCORING_VERSION,
                    "persistence_scale":        scale,
                    "integration_mode":         mode,
                    "integration_weight":       integration_weight,
                    "source_family_rank":       r.get("persistence_family_rank"),
                    "memory_break_event":       latest_event in _MEMORY_BREAK_EVENTS,
                },
            ))
        return out

    def rank_family_persistence_integration(
        self, items: list[FamilyPersistenceComposite],
    ) -> list[FamilyPersistenceComposite]:
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.persistence_integration_contribution or 0.0),
                -abs(fa.persistence_integration_contribution or 0.0),
                fa.dependency_family,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.family_rank = i
        return ranked

    # ── orchestration ───────────────────────────────────────────────────
    def build_persistence_composite_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> PersistenceCompositeResult | None:
        family_rows_raw = self._load_persistence_family_rows(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not family_rows_raw:
            return None

        profile_row = self.get_active_persistence_integration_profile(
            conn, workspace_id=workspace_id,
        )
        default_profile_used = profile_row is None
        profile = profile_row if profile_row is not None else dict(_DEFAULT_PROFILE)
        profile_id = profile_row["id"] if profile_row is not None else None
        profile_name = profile.get("profile_name", _DEFAULT_PROFILE["profile_name"])
        mode = str(profile.get("integration_mode", "persistence_additive_guardrailed"))
        if mode not in _VALID_MODES:
            raise ValueError(f"invalid integration_mode: {mode!r}")

        ctx = self._load_run_context(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        persist_state = ctx["persistence_state"]

        persistence_net = self.compute_persistence_net_contribution(
            persistence_adjusted_total=ctx["persistence_net"],
            persistence_state=persist_state,
            latest_event_type=ctx.get("latest_persistence_event_type"),
            profile=profile,
        )
        composite_post = self.integrate_persistence_with_composite(
            composite_pre_persistence=ctx["composite_pre_persistence"],
            persistence_net_contribution=persistence_net,
        )

        family_rows = self.compute_family_persistence_integration(family_rows_raw, profile=profile)
        family_rows = self.rank_family_persistence_integration(family_rows)

        metadata: dict[str, Any] = {
            "scoring_version":                              _SCORING_VERSION,
            "default_persistence_integration_profile_used": default_profile_used,
            "profile_name":                                 profile_name,
            "composite_pre_source":                         ctx.get("composite_pre_source"),
            "integration_weight":                           _as_float(profile.get("integration_weight")),
            "max_positive_contribution":                    _as_float(profile.get("max_positive_contribution")),
            "max_negative_contribution":                    _as_float(profile.get("max_negative_contribution")),
            "memory_break_extra_suppression":               _as_float(profile.get("memory_break_extra_suppression")),
            "net_bound_default":                            _NET_BOUND_DEFAULT,
        }
        for fa in family_rows:
            fa.metadata.setdefault("profile_name", profile_name)
            fa.metadata.setdefault("default_persistence_integration_profile_used", default_profile_used)

        return PersistenceCompositeResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=ctx.get("context_snapshot_id"),
            persistence_integration_profile_id=profile_id,
            profile_name=profile_name,
            default_profile_used=default_profile_used,
            integration_mode=mode,
            base_signal_score=ctx.get("base_signal_score"),
            cross_asset_net_contribution=ctx.get("raw_net"),
            weighted_cross_asset_net_contribution=ctx.get("weighted_net"),
            regime_adjusted_cross_asset_contribution=ctx.get("regime_net"),
            timing_adjusted_cross_asset_contribution=ctx.get("timing_net"),
            transition_adjusted_cross_asset_contribution=ctx.get("transition_net"),
            archetype_adjusted_cross_asset_contribution=ctx.get("archetype_net"),
            cluster_adjusted_cross_asset_contribution=ctx.get("cluster_net"),
            persistence_adjusted_cross_asset_contribution=ctx.get("persistence_net"),
            composite_pre_persistence=ctx.get("composite_pre_persistence"),
            persistence_net_contribution=persistence_net,
            composite_post_persistence=composite_post,
            persistence_state=persist_state,
            memory_score=ctx.get("memory_score"),
            state_age_runs=ctx.get("state_age_runs"),
            latest_persistence_event_type=ctx.get("latest_persistence_event_type"),
            metadata=metadata,
            family_rows=family_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_persistence_composite(
        self, conn, *, result: PersistenceCompositeResult,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_persistence_composite_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            persistence_integration_profile_id=result.persistence_integration_profile_id,
            base_signal_score=result.base_signal_score,
            cross_asset_net_contribution=result.cross_asset_net_contribution,
            weighted_cross_asset_net_contribution=result.weighted_cross_asset_net_contribution,
            regime_adjusted_cross_asset_contribution=result.regime_adjusted_cross_asset_contribution,
            timing_adjusted_cross_asset_contribution=result.timing_adjusted_cross_asset_contribution,
            transition_adjusted_cross_asset_contribution=result.transition_adjusted_cross_asset_contribution,
            archetype_adjusted_cross_asset_contribution=result.archetype_adjusted_cross_asset_contribution,
            cluster_adjusted_cross_asset_contribution=result.cluster_adjusted_cross_asset_contribution,
            persistence_adjusted_cross_asset_contribution=result.persistence_adjusted_cross_asset_contribution,
            composite_pre_persistence=result.composite_pre_persistence,
            persistence_net_contribution=result.persistence_net_contribution,
            composite_post_persistence=result.composite_post_persistence,
            persistence_state=result.persistence_state,
            memory_score=result.memory_score,
            state_age_runs=result.state_age_runs,
            latest_persistence_event_type=result.latest_persistence_event_type,
            integration_mode=result.integration_mode,
            metadata=result.metadata,
        )
        return str(row["id"])

    def persist_family_persistence_composite(
        self, conn, *, result: PersistenceCompositeResult,
    ) -> int:
        if not result.family_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_persistence_composite_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            rows=[
                {
                    "dependency_family":                       fa.dependency_family,
                    "persistence_state":                       fa.persistence_state,
                    "memory_score":                            fa.memory_score,
                    "state_age_runs":                          fa.state_age_runs,
                    "latest_persistence_event_type":           fa.latest_persistence_event_type,
                    "persistence_adjusted_family_contribution": fa.persistence_adjusted_family_contribution,
                    "integration_weight_applied":              fa.integration_weight_applied,
                    "persistence_integration_contribution":    fa.persistence_integration_contribution,
                    "family_rank":                             fa.family_rank,
                    "top_symbols":                             fa.top_symbols,
                    "reason_codes":                            fa.reason_codes,
                    "metadata":                                fa.metadata,
                }
                for fa in result.family_rows
            ],
        )

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> PersistenceCompositeResult | None:
        result = self.build_persistence_composite_for_run(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if result is None:
            return None
        self.persist_persistence_composite(conn, result=result)
        self.persist_family_persistence_composite(conn, result=result)
        return result

    def refresh_workspace_persistence_composite(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[PersistenceCompositeResult]:
        """Emit persistence-aware composite for every watchlist. Commits
        per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[PersistenceCompositeResult] = []
        for wid in watchlist_ids:
            try:
                r = self.build_and_persist(
                    conn, workspace_id=workspace_id, watchlist_id=wid, run_id=run_id,
                )
                if r is not None:
                    conn.commit()
                    results.append(r)
            except Exception as exc:
                logger.warning(
                    "cross_asset_persistence_composite: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
