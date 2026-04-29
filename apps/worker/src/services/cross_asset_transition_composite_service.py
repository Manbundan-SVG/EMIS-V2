"""Phase 4.3C: Sequencing-Aware Composite Refinement Service.

Reads 4.3B transition-aware attribution and the most mature upstream composite
(4.2C timing composite → 4.1C regime equivalent → 4.1B weighted equivalent →
4.1A raw composite post fallback) and emits a bounded transition-aware delta
that refines the final integrated score. Persists:

  * one cross_asset_transition_composite_snapshots row per run
  * one cross_asset_family_transition_composite_snapshots row per family

All adjustments are deterministic; the transition-aware net contribution is
clipped to a conservative band and the integration weight is small and
explicit. Sequencing-aware integration never dominates upstream cross-asset
integration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.3C.v1"

_VALID_MODES = frozenset({
    "transition_additive_guardrailed",
    "reinforcing_confirmation_only",
    "deteriorating_suppression_only",
    "rotation_handoff_sensitive",
})

_DEFAULT_PROFILE: dict[str, Any] = {
    "profile_name":                      "default_transition_integration",
    "integration_mode":                  "transition_additive_guardrailed",
    "integration_weight":                0.10,
    "reinforcing_weight_scale":          1.05,
    "stable_weight_scale":               1.00,
    "recovering_weight_scale":           1.02,
    "rotating_in_weight_scale":          1.08,
    "rotating_out_weight_scale":         0.92,
    "deteriorating_weight_scale":        0.85,
    "insufficient_history_weight_scale": 0.90,
    "max_positive_contribution":         0.20,
    "max_negative_contribution":         0.20,
}

# Conservative first-pass clamp per spec §11.
_NET_BOUND_DEFAULT = 0.15


@dataclass
class FamilyTransitionComposite:
    dependency_family: str
    transition_state: str
    dominant_sequence_class: str
    transition_adjusted_family_contribution: float | None
    integration_weight_applied: float
    transition_integration_contribution: float | None
    family_rank: int | None = None
    top_symbols: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TransitionCompositeResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    transition_integration_profile_id: str | None
    profile_name: str
    default_profile_used: bool
    integration_mode: str
    base_signal_score: float | None
    cross_asset_net_contribution: float | None
    weighted_cross_asset_net_contribution: float | None
    regime_adjusted_cross_asset_contribution: float | None
    timing_adjusted_cross_asset_contribution: float | None
    transition_adjusted_cross_asset_contribution: float | None
    composite_pre_transition: float | None
    transition_net_contribution: float | None
    composite_post_transition: float | None
    dominant_transition_state: str
    metadata: dict[str, Any]
    family_rows: list[FamilyTransitionComposite]


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


class CrossAssetTransitionCompositeService:
    """Deterministic transition-aware final composite integration."""

    # ── profile loading ─────────────────────────────────────────────────
    def get_active_transition_integration_profile(
        self, conn, *, workspace_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, profile_name, is_active, integration_mode,
                       integration_weight,
                       reinforcing_weight_scale, stable_weight_scale,
                       recovering_weight_scale, rotating_in_weight_scale,
                       rotating_out_weight_scale, deteriorating_weight_scale,
                       insufficient_history_weight_scale,
                       max_positive_contribution, max_negative_contribution,
                       metadata, created_at
                from public.cross_asset_transition_integration_profiles
                where workspace_id = %s::uuid and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ── weight primitive ────────────────────────────────────────────────
    def compute_transition_state_integration_weight(
        self, *, transition_state: str, profile: dict[str, Any],
    ) -> float:
        key_map = {
            "reinforcing":          "reinforcing_weight_scale",
            "stable":               "stable_weight_scale",
            "recovering":           "recovering_weight_scale",
            "rotating_in":          "rotating_in_weight_scale",
            "rotating_out":         "rotating_out_weight_scale",
            "deteriorating":        "deteriorating_weight_scale",
            "insufficient_history": "insufficient_history_weight_scale",
        }
        scale = _as_float(profile.get(key_map.get(transition_state, "stable_weight_scale")))
        return scale if scale is not None else 1.0

    # ── input loading ───────────────────────────────────────────────────
    def _load_run_context(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, Any]:
        """Load all upstream contributions + the composite_pre_transition
        fallback chain. Returns keys: context_snapshot_id, base_signal_score,
        raw_net, weighted_net, regime_net, timing_net, transition_net,
        composite_pre_transition, composite_pre_source."""
        ctx: dict[str, Any] = {
            "context_snapshot_id":        None,
            "base_signal_score":          None,
            "raw_net":                    None,
            "weighted_net":               None,
            "regime_net":                 None,
            "timing_net":                 None,
            "transition_net":             None,
            "timing_composite_post":      None,
            "composite_pre_transition":   None,
            "composite_pre_source":       None,
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

        # Resolve composite_pre_transition via fallback chain:
        #  4.2C composite_post_timing
        #  → 4.1C regime post-integration equivalent
        #  → 4.1B weighted post-integration equivalent
        #  → 4.1A raw composite_post_cross_asset
        #  → base_signal_score
        INTEGRATION_WEIGHT = 0.10
        base = ctx["base_signal_score"]
        if ctx["timing_composite_post"] is not None:
            ctx["composite_pre_transition"] = ctx["timing_composite_post"]
            ctx["composite_pre_source"] = "timing_composite_post"
        elif base is not None and ctx["regime_net"] is not None:
            ctx["composite_pre_transition"] = base + ctx["regime_net"] * INTEGRATION_WEIGHT
            ctx["composite_pre_source"] = "regime_post_integration_equivalent"
        elif base is not None and ctx["weighted_net"] is not None:
            ctx["composite_pre_transition"] = base + ctx["weighted_net"] * INTEGRATION_WEIGHT
            ctx["composite_pre_source"] = "weighted_post_integration_equivalent"
        elif raw_composite_post is not None:
            ctx["composite_pre_transition"] = raw_composite_post
            ctx["composite_pre_source"] = "raw_composite_post_cross_asset"
        elif base is not None:
            ctx["composite_pre_transition"] = base
            ctx["composite_pre_source"] = "base_signal_score"
        return ctx

    def _load_transition_family_rows(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       transition_state,
                       dominant_sequence_class,
                       transition_adjusted_family_contribution,
                       transition_family_rank,
                       top_symbols
                from public.cross_asset_family_transition_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def _mode_gate_state(mode: str, transition_state: str) -> bool:
        """Return True if this state is admitted for a given mode, False if it
        should be zeroed out."""
        if mode == "reinforcing_confirmation_only":
            return transition_state in ("reinforcing", "recovering")
        if mode == "deteriorating_suppression_only":
            return transition_state in ("deteriorating", "rotating_out")
        if mode == "rotation_handoff_sensitive":
            return transition_state in ("rotating_in", "rotating_out", "reinforcing", "recovering")
        return True  # transition_additive_guardrailed admits all states

    # ── core builders ───────────────────────────────────────────────────
    def compute_transition_net_contribution(
        self,
        *,
        transition_adjusted_total: float | None,
        dominant_transition_state: str,
        profile: dict[str, Any],
    ) -> float | None:
        if transition_adjusted_total is None:
            return None
        mode = profile.get("integration_mode", "transition_additive_guardrailed")

        if not self._mode_gate_state(mode, dominant_transition_state):
            return 0.0
        base = transition_adjusted_total
        if mode == "deteriorating_suppression_only" and dominant_transition_state in (
            "deteriorating", "rotating_out",
        ):
            # Suppression-only mode: enforce downward (negative) contribution.
            base = -abs(base)

        state_scale = self.compute_transition_state_integration_weight(
            transition_state=dominant_transition_state, profile=profile,
        )
        integration_weight = _as_float(profile.get("integration_weight")) or 0.10
        raw = base * integration_weight * state_scale

        max_pos = _as_float(profile.get("max_positive_contribution")) or _NET_BOUND_DEFAULT
        max_neg = _as_float(profile.get("max_negative_contribution")) or _NET_BOUND_DEFAULT
        lower = -min(max_neg, _NET_BOUND_DEFAULT)
        upper =  min(max_pos, _NET_BOUND_DEFAULT)
        return _clip(raw, lower, upper)

    def integrate_transition_with_composite(
        self,
        *,
        composite_pre_transition: float | None,
        transition_net_contribution: float | None,
    ) -> float | None:
        if composite_pre_transition is None:
            return None
        if transition_net_contribution is None:
            return composite_pre_transition
        return composite_pre_transition + transition_net_contribution

    def compute_family_transition_integration(
        self,
        family_rows: Sequence[dict[str, Any]],
        *,
        profile: dict[str, Any],
    ) -> list[FamilyTransitionComposite]:
        integration_weight = _as_float(profile.get("integration_weight")) or 0.10
        mode = profile.get("integration_mode", "transition_additive_guardrailed")
        max_pos = _as_float(profile.get("max_positive_contribution")) or _NET_BOUND_DEFAULT
        max_neg = _as_float(profile.get("max_negative_contribution")) or _NET_BOUND_DEFAULT
        lower = -min(max_neg, _NET_BOUND_DEFAULT)
        upper =  min(max_pos, _NET_BOUND_DEFAULT)

        out: list[FamilyTransitionComposite] = []
        for r in family_rows:
            fam = str(r["dependency_family"])
            state = str(r.get("transition_state") or "insufficient_history")
            seq_class = str(r.get("dominant_sequence_class") or "insufficient_history")
            trans_adj = _as_float(r.get("transition_adjusted_family_contribution"))

            state_scale = self.compute_transition_state_integration_weight(
                transition_state=state, profile=profile,
            )

            top_syms_raw = r.get("top_symbols") or []
            if isinstance(top_syms_raw, str):
                import json
                try:
                    top_syms_raw = json.loads(top_syms_raw)
                except json.JSONDecodeError:
                    top_syms_raw = []
            top_syms = [str(s) for s in top_syms_raw]

            contribution: float | None
            if trans_adj is None:
                contribution = None
            else:
                gated = trans_adj
                if not self._mode_gate_state(mode, state):
                    gated = 0.0
                elif mode == "deteriorating_suppression_only" and state in (
                    "deteriorating", "rotating_out",
                ):
                    gated = -abs(trans_adj)
                raw = gated * integration_weight * state_scale
                contribution = _clip(raw, lower, upper)

            out.append(FamilyTransitionComposite(
                dependency_family=fam,
                transition_state=state,
                dominant_sequence_class=seq_class,
                transition_adjusted_family_contribution=trans_adj,
                integration_weight_applied=integration_weight * state_scale,
                transition_integration_contribution=contribution,
                top_symbols=top_syms,
                metadata={
                    "scoring_version":    _SCORING_VERSION,
                    "state_scale":        state_scale,
                    "integration_mode":   mode,
                    "integration_weight": integration_weight,
                    "source_family_rank": r.get("transition_family_rank"),
                },
            ))
        return out

    def rank_family_transition_integration(
        self, items: list[FamilyTransitionComposite],
    ) -> list[FamilyTransitionComposite]:
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.transition_integration_contribution or 0.0),
                -abs(fa.transition_integration_contribution or 0.0),
                fa.dependency_family,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.family_rank = i
        return ranked

    @staticmethod
    def _determine_run_dominant_transition_state(
        family_rows: Sequence[dict[str, Any]],
    ) -> str:
        if not family_rows:
            return "insufficient_history"
        ranked = sorted(
            family_rows,
            key=lambda r: r.get("transition_family_rank") or 10**6,
        )
        top = ranked[0]
        return str(top.get("transition_state") or "insufficient_history")

    # ── orchestration ───────────────────────────────────────────────────
    def build_transition_composite_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> TransitionCompositeResult | None:
        family_rows_raw = self._load_transition_family_rows(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not family_rows_raw:
            return None

        profile_row = self.get_active_transition_integration_profile(
            conn, workspace_id=workspace_id,
        )
        default_profile_used = profile_row is None
        profile = profile_row if profile_row is not None else dict(_DEFAULT_PROFILE)
        profile_id = profile_row["id"] if profile_row is not None else None
        profile_name = profile.get("profile_name", _DEFAULT_PROFILE["profile_name"])
        mode = str(profile.get("integration_mode", "transition_additive_guardrailed"))
        if mode not in _VALID_MODES:
            raise ValueError(f"invalid integration_mode: {mode!r}")

        ctx = self._load_run_context(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        dominant_state = self._determine_run_dominant_transition_state(family_rows_raw)

        transition_net = self.compute_transition_net_contribution(
            transition_adjusted_total=ctx["transition_net"],
            dominant_transition_state=dominant_state,
            profile=profile,
        )
        composite_post = self.integrate_transition_with_composite(
            composite_pre_transition=ctx["composite_pre_transition"],
            transition_net_contribution=transition_net,
        )

        family_rows = self.compute_family_transition_integration(family_rows_raw, profile=profile)
        family_rows = self.rank_family_transition_integration(family_rows)

        metadata: dict[str, Any] = {
            "scoring_version":                           _SCORING_VERSION,
            "default_transition_integration_profile_used": default_profile_used,
            "profile_name":                              profile_name,
            "composite_pre_source":                      ctx.get("composite_pre_source"),
            "integration_weight":                        _as_float(profile.get("integration_weight")),
            "max_positive_contribution":                 _as_float(profile.get("max_positive_contribution")),
            "max_negative_contribution":                 _as_float(profile.get("max_negative_contribution")),
            "net_bound_default":                         _NET_BOUND_DEFAULT,
        }
        for fa in family_rows:
            fa.metadata.setdefault("profile_name", profile_name)
            fa.metadata.setdefault("default_transition_integration_profile_used", default_profile_used)

        return TransitionCompositeResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=ctx.get("context_snapshot_id"),
            transition_integration_profile_id=profile_id,
            profile_name=profile_name,
            default_profile_used=default_profile_used,
            integration_mode=mode,
            base_signal_score=ctx.get("base_signal_score"),
            cross_asset_net_contribution=ctx.get("raw_net"),
            weighted_cross_asset_net_contribution=ctx.get("weighted_net"),
            regime_adjusted_cross_asset_contribution=ctx.get("regime_net"),
            timing_adjusted_cross_asset_contribution=ctx.get("timing_net"),
            transition_adjusted_cross_asset_contribution=ctx.get("transition_net"),
            composite_pre_transition=ctx.get("composite_pre_transition"),
            transition_net_contribution=transition_net,
            composite_post_transition=composite_post,
            dominant_transition_state=dominant_state,
            metadata=metadata,
            family_rows=family_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_transition_composite(
        self, conn, *, result: TransitionCompositeResult,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_transition_composite_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            transition_integration_profile_id=result.transition_integration_profile_id,
            base_signal_score=result.base_signal_score,
            cross_asset_net_contribution=result.cross_asset_net_contribution,
            weighted_cross_asset_net_contribution=result.weighted_cross_asset_net_contribution,
            regime_adjusted_cross_asset_contribution=result.regime_adjusted_cross_asset_contribution,
            timing_adjusted_cross_asset_contribution=result.timing_adjusted_cross_asset_contribution,
            transition_adjusted_cross_asset_contribution=result.transition_adjusted_cross_asset_contribution,
            composite_pre_transition=result.composite_pre_transition,
            transition_net_contribution=result.transition_net_contribution,
            composite_post_transition=result.composite_post_transition,
            dominant_transition_state=result.dominant_transition_state,
            integration_mode=result.integration_mode,
            metadata=result.metadata,
        )
        return str(row["id"])

    def persist_family_transition_composite(
        self, conn, *, result: TransitionCompositeResult,
    ) -> int:
        if not result.family_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_transition_composite_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            rows=[
                {
                    "dependency_family":                       fa.dependency_family,
                    "transition_state":                        fa.transition_state,
                    "dominant_sequence_class":                 fa.dominant_sequence_class,
                    "transition_adjusted_family_contribution": fa.transition_adjusted_family_contribution,
                    "integration_weight_applied":              fa.integration_weight_applied,
                    "transition_integration_contribution":     fa.transition_integration_contribution,
                    "family_rank":                             fa.family_rank,
                    "top_symbols":                             fa.top_symbols,
                    "metadata":                                fa.metadata,
                }
                for fa in result.family_rows
            ],
        )

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> TransitionCompositeResult | None:
        result = self.build_transition_composite_for_run(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
        )
        if result is None:
            return None
        self.persist_transition_composite(conn, result=result)
        self.persist_family_transition_composite(conn, result=result)
        return result

    def refresh_workspace_transition_composite(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[TransitionCompositeResult]:
        """Emit transition-aware composite for every watchlist in the
        workspace. Commits per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[TransitionCompositeResult] = []
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
                    "cross_asset_transition_composite: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
