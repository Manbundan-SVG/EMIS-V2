"""Phase 4.2C: Timing-Aware Composite Refinement Service.

Reads 4.2B timing-aware attribution and the most mature upstream composite
(regime-adjusted → weighted → raw fallback) and emits a bounded timing-
aware delta that refines the final integrated score. Persists:

  * one cross_asset_timing_composite_snapshots row per run
  * one cross_asset_family_timing_composite_snapshots row per family

All adjustments are deterministic; the timing-aware net contribution is
clipped to a conservative band and the integration weight is small and
explicit.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.2C.v1"

_VALID_MODES = frozenset({
    "timing_additive_guardrailed",
    "lead_confirmation_only",
    "lag_suppression_only",
})

_DEFAULT_PROFILE: dict[str, Any] = {
    "profile_name":                     "default_timing_integration",
    "integration_mode":                 "timing_additive_guardrailed",
    "integration_weight":               0.10,
    "lead_weight_scale":                1.00,
    "coincident_weight_scale":          1.00,
    "lag_weight_scale":                 1.00,
    "insufficient_data_weight_scale":   1.00,
    "max_positive_contribution":        0.25,
    "max_negative_contribution":        0.25,
}

# First-pass conservative clamp recommended by the spec.
_NET_BOUND_DEFAULT = 0.15


@dataclass
class FamilyTimingComposite:
    dependency_family: str
    dominant_timing_class: str
    timing_adjusted_family_contribution: float | None
    integration_weight_applied: float
    timing_integration_contribution: float | None
    family_rank: int | None = None
    top_symbols: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TimingCompositeResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    timing_integration_profile_id: str | None
    profile_name: str
    default_profile_used: bool
    integration_mode: str
    base_signal_score: float | None
    cross_asset_net_contribution: float | None
    weighted_cross_asset_net_contribution: float | None
    regime_adjusted_cross_asset_contribution: float | None
    timing_adjusted_cross_asset_contribution: float | None
    composite_pre_timing: float | None
    timing_net_contribution: float | None
    composite_post_timing: float | None
    dominant_timing_class: str
    metadata: dict[str, Any]
    family_rows: list[FamilyTimingComposite]


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


class CrossAssetTimingCompositeService:
    """Deterministic timing-aware final composite integration."""

    # ── profile loading ─────────────────────────────────────────────────
    def get_active_timing_integration_profile(
        self, conn, *, workspace_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, profile_name, is_active, integration_mode,
                       integration_weight,
                       lead_weight_scale, coincident_weight_scale,
                       lag_weight_scale, insufficient_data_weight_scale,
                       max_positive_contribution, max_negative_contribution,
                       metadata, created_at
                from public.cross_asset_timing_integration_profiles
                where workspace_id = %s::uuid and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ── weight primitive ────────────────────────────────────────────────
    def compute_timing_class_integration_weight(
        self, *, timing_class: str, profile: dict[str, Any],
    ) -> float:
        key_map = {
            "lead":              "lead_weight_scale",
            "coincident":        "coincident_weight_scale",
            "lag":               "lag_weight_scale",
            "insufficient_data": "insufficient_data_weight_scale",
        }
        scale = _as_float(profile.get(key_map.get(timing_class, "coincident_weight_scale")))
        return scale if scale is not None else 1.0

    # ── input loading ───────────────────────────────────────────────────
    def _load_run_context(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, Any]:
        """Load all upstream contributions + the composite_pre_timing fallback
        chain. Returns a dict with keys: context_snapshot_id, base_signal_score,
        raw_net, weighted_net, regime_net, timing_net, composite_pre_timing,
        composite_pre_source."""
        ctx: dict[str, Any] = {
            "context_snapshot_id":   None,
            "base_signal_score":     None,
            "raw_net":               None,
            "weighted_net":          None,
            "regime_net":            None,
            "timing_net":            None,
            "composite_pre_timing":  None,
            "composite_pre_source":  None,
        }
        with conn.cursor() as cur:
            # 4.1A attribution (contains base_signal_score + composite_post)
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

            # 4.1B weighted integration summary
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

            # 4.1C regime integration summary
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

            # 4.2B timing attribution integration summary
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

        # Resolve composite_pre_timing via fallback chain.
        # Since none of the upstream phases persist a standalone
        # "composite_post_regime/weighted", we reconstruct the most mature
        # pre-timing composite from base + the deepest available net × 4.1A
        # integration_weight (0.10, the documented default). If the 4.1A
        # composite_post_cross_asset row exists, we also consider it as a
        # safe last-resort anchor.
        INTEGRATION_WEIGHT = 0.10
        base = ctx["base_signal_score"]
        if base is not None and ctx["regime_net"] is not None:
            ctx["composite_pre_timing"] = base + ctx["regime_net"] * INTEGRATION_WEIGHT
            ctx["composite_pre_source"] = "regime_post_integration_equivalent"
        elif base is not None and ctx["weighted_net"] is not None:
            ctx["composite_pre_timing"] = base + ctx["weighted_net"] * INTEGRATION_WEIGHT
            ctx["composite_pre_source"] = "weighted_post_integration_equivalent"
        elif raw_composite_post is not None:
            ctx["composite_pre_timing"] = raw_composite_post
            ctx["composite_pre_source"] = "raw_composite_post_cross_asset"
        elif base is not None:
            ctx["composite_pre_timing"] = base
            ctx["composite_pre_source"] = "base_signal_score"
        return ctx

    def _load_timing_family_rows(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       dominant_timing_class,
                       timing_adjusted_family_contribution,
                       timing_family_rank,
                       top_leading_symbols
                from public.cross_asset_family_timing_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [dict(r) for r in cur.fetchall()]

    # ── core builders ───────────────────────────────────────────────────
    def compute_timing_net_contribution(
        self,
        *,
        timing_adjusted_total: float | None,
        dominant_timing_class: str,
        profile: dict[str, Any],
    ) -> float | None:
        if timing_adjusted_total is None:
            return None
        mode = profile.get("integration_mode", "timing_additive_guardrailed")

        # Mode gating
        if mode == "lead_confirmation_only" and dominant_timing_class != "lead":
            return 0.0
        if mode == "lag_suppression_only" and dominant_timing_class != "lag":
            return 0.0
        if mode == "lag_suppression_only" and dominant_timing_class == "lag":
            # Suppression only: force contribution to be negative (towards
            # zero of upstream composite). Preserve sign-aware magnitude.
            timing_adjusted_total = -abs(timing_adjusted_total)

        class_scale = self.compute_timing_class_integration_weight(
            timing_class=dominant_timing_class, profile=profile,
        )
        integration_weight = _as_float(profile.get("integration_weight")) or 0.10
        raw = timing_adjusted_total * integration_weight * class_scale

        max_pos = _as_float(profile.get("max_positive_contribution")) or _NET_BOUND_DEFAULT
        max_neg = _as_float(profile.get("max_negative_contribution")) or _NET_BOUND_DEFAULT
        # Final clamp uses min(configured, conservative band) per spec §11.
        lower = -min(max_neg, _NET_BOUND_DEFAULT)
        upper =  min(max_pos, _NET_BOUND_DEFAULT)
        return _clip(raw, lower, upper)

    def integrate_timing_with_composite(
        self,
        *,
        composite_pre_timing: float | None,
        timing_net_contribution: float | None,
    ) -> float | None:
        if composite_pre_timing is None:
            return None
        if timing_net_contribution is None:
            return composite_pre_timing
        return composite_pre_timing + timing_net_contribution

    def compute_family_timing_integration(
        self,
        family_rows: Sequence[dict[str, Any]],
        *,
        profile: dict[str, Any],
    ) -> list[FamilyTimingComposite]:
        integration_weight = _as_float(profile.get("integration_weight")) or 0.10
        mode = profile.get("integration_mode", "timing_additive_guardrailed")
        max_pos = _as_float(profile.get("max_positive_contribution")) or _NET_BOUND_DEFAULT
        max_neg = _as_float(profile.get("max_negative_contribution")) or _NET_BOUND_DEFAULT
        lower = -min(max_neg, _NET_BOUND_DEFAULT)
        upper =  min(max_pos, _NET_BOUND_DEFAULT)

        out: list[FamilyTimingComposite] = []
        for r in family_rows:
            fam = str(r["dependency_family"])
            timing_class = str(r.get("dominant_timing_class") or "insufficient_data")
            timing_adj = _as_float(r.get("timing_adjusted_family_contribution"))

            class_scale = self.compute_timing_class_integration_weight(
                timing_class=timing_class, profile=profile,
            )

            top_syms_raw = r.get("top_leading_symbols") or []
            if isinstance(top_syms_raw, str):
                import json
                try:
                    top_syms_raw = json.loads(top_syms_raw)
                except json.JSONDecodeError:
                    top_syms_raw = []
            top_syms = [str(s) for s in top_syms_raw]

            # Apply mode gating at the family level too so per-family
            # contributions mirror the run-level logic.
            contribution: float | None
            if timing_adj is None:
                contribution = None
            else:
                gated_base = timing_adj
                if mode == "lead_confirmation_only" and timing_class != "lead":
                    gated_base = 0.0
                elif mode == "lag_suppression_only":
                    if timing_class != "lag":
                        gated_base = 0.0
                    else:
                        gated_base = -abs(timing_adj)
                raw = gated_base * integration_weight * class_scale
                contribution = _clip(raw, lower, upper)

            out.append(FamilyTimingComposite(
                dependency_family=fam,
                dominant_timing_class=timing_class,
                timing_adjusted_family_contribution=timing_adj,
                integration_weight_applied=integration_weight * class_scale,
                timing_integration_contribution=contribution,
                top_symbols=top_syms,
                metadata={
                    "scoring_version":   _SCORING_VERSION,
                    "class_scale":       class_scale,
                    "integration_mode":  mode,
                    "integration_weight": integration_weight,
                    "source_family_rank": r.get("timing_family_rank"),
                },
            ))
        return out

    def rank_family_timing_integration(
        self, items: list[FamilyTimingComposite],
    ) -> list[FamilyTimingComposite]:
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.timing_integration_contribution or 0.0),
                -abs(fa.timing_integration_contribution or 0.0),
                fa.dependency_family,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.family_rank = i
        return ranked

    @staticmethod
    def _determine_run_dominant_timing_class(
        family_rows: Sequence[dict[str, Any]],
    ) -> str:
        if not family_rows:
            return "insufficient_data"
        # The rank-1 family (by 4.2B timing_family_rank) carries the run's
        # dominant timing class.
        ranked = sorted(
            family_rows,
            key=lambda r: r.get("timing_family_rank") or 10**6,
        )
        top = ranked[0]
        return str(top.get("dominant_timing_class") or "insufficient_data")

    # ── orchestration ───────────────────────────────────────────────────
    def build_timing_composite_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> TimingCompositeResult | None:
        family_rows_raw = self._load_timing_family_rows(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not family_rows_raw:
            return None

        profile_row = self.get_active_timing_integration_profile(
            conn, workspace_id=workspace_id,
        )
        default_profile_used = profile_row is None
        profile = profile_row if profile_row is not None else dict(_DEFAULT_PROFILE)
        profile_id = profile_row["id"] if profile_row is not None else None
        profile_name = profile.get("profile_name", _DEFAULT_PROFILE["profile_name"])
        mode = str(profile.get("integration_mode", "timing_additive_guardrailed"))
        if mode not in _VALID_MODES:
            raise ValueError(f"invalid integration_mode: {mode!r}")

        ctx = self._load_run_context(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        dominant_class = self._determine_run_dominant_timing_class(family_rows_raw)

        timing_net = self.compute_timing_net_contribution(
            timing_adjusted_total=ctx["timing_net"],
            dominant_timing_class=dominant_class,
            profile=profile,
        )
        composite_post = self.integrate_timing_with_composite(
            composite_pre_timing=ctx["composite_pre_timing"],
            timing_net_contribution=timing_net,
        )

        family_rows = self.compute_family_timing_integration(family_rows_raw, profile=profile)
        family_rows = self.rank_family_timing_integration(family_rows)

        metadata: dict[str, Any] = {
            "scoring_version":                       _SCORING_VERSION,
            "default_timing_integration_profile_used": default_profile_used,
            "profile_name":                          profile_name,
            "composite_pre_source":                  ctx.get("composite_pre_source"),
            "integration_weight":                    _as_float(profile.get("integration_weight")),
            "max_positive_contribution":             _as_float(profile.get("max_positive_contribution")),
            "max_negative_contribution":             _as_float(profile.get("max_negative_contribution")),
            "net_bound_default":                     _NET_BOUND_DEFAULT,
        }
        for fa in family_rows:
            fa.metadata.setdefault("profile_name", profile_name)
            fa.metadata.setdefault("default_timing_integration_profile_used", default_profile_used)

        return TimingCompositeResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=ctx.get("context_snapshot_id"),
            timing_integration_profile_id=profile_id,
            profile_name=profile_name,
            default_profile_used=default_profile_used,
            integration_mode=mode,
            base_signal_score=ctx.get("base_signal_score"),
            cross_asset_net_contribution=ctx.get("raw_net"),
            weighted_cross_asset_net_contribution=ctx.get("weighted_net"),
            regime_adjusted_cross_asset_contribution=ctx.get("regime_net"),
            timing_adjusted_cross_asset_contribution=ctx.get("timing_net"),
            composite_pre_timing=ctx.get("composite_pre_timing"),
            timing_net_contribution=timing_net,
            composite_post_timing=composite_post,
            dominant_timing_class=dominant_class,
            metadata=metadata,
            family_rows=family_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_timing_composite(
        self, conn, *, result: TimingCompositeResult,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_timing_composite_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            timing_integration_profile_id=result.timing_integration_profile_id,
            base_signal_score=result.base_signal_score,
            cross_asset_net_contribution=result.cross_asset_net_contribution,
            weighted_cross_asset_net_contribution=result.weighted_cross_asset_net_contribution,
            regime_adjusted_cross_asset_contribution=result.regime_adjusted_cross_asset_contribution,
            timing_adjusted_cross_asset_contribution=result.timing_adjusted_cross_asset_contribution,
            composite_pre_timing=result.composite_pre_timing,
            timing_net_contribution=result.timing_net_contribution,
            composite_post_timing=result.composite_post_timing,
            dominant_timing_class=result.dominant_timing_class,
            integration_mode=result.integration_mode,
            metadata=result.metadata,
        )
        return str(row["id"])

    def persist_family_timing_composite(
        self, conn, *, result: TimingCompositeResult,
    ) -> int:
        if not result.family_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_timing_composite_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            rows=[
                {
                    "dependency_family":                  fa.dependency_family,
                    "dominant_timing_class":              fa.dominant_timing_class,
                    "timing_adjusted_family_contribution": fa.timing_adjusted_family_contribution,
                    "integration_weight_applied":         fa.integration_weight_applied,
                    "timing_integration_contribution":    fa.timing_integration_contribution,
                    "family_rank":                        fa.family_rank,
                    "top_symbols":                        fa.top_symbols,
                    "metadata":                           fa.metadata,
                }
                for fa in result.family_rows
            ],
        )

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> TimingCompositeResult | None:
        result = self.build_timing_composite_for_run(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
        )
        if result is None:
            return None
        self.persist_timing_composite(conn, result=result)
        self.persist_family_timing_composite(conn, result=result)
        return result

    def refresh_workspace_timing_composite(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[TimingCompositeResult]:
        """Emit timing-aware composite for every watchlist in the workspace.
        Commits per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[TimingCompositeResult] = []
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
                    "cross_asset_timing_composite: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
