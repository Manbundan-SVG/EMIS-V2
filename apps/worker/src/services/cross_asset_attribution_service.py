"""Phase 4.1A: Cross-Asset Attribution + Composite Integration Service.

Reads:
  * existing base composite output from composite_scores (for the workspace)
  * 4.0C cross-asset signal snapshots (per run)
  * 4.0D explanation summary + family contributions (per run)

Computes deterministic attribution scores, integrates a guarded cross-asset
contribution into the existing composite, and persists:
  * one cross_asset_attribution_snapshots row per run
  * one cross_asset_family_attribution_snapshots row per dependency family

Integration is conservative:
  * cross_asset_net_contribution is clipped to [-0.25, +0.25]
  * applied to the composite via a small integration_weight (default 0.1)
  * never dominates the base composite

All formulas are transparent and auditable through metadata.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_INTEGRATION_WEIGHT       = 0.10
_NET_CONTRIBUTION_BOUND   = 0.25  # clipped to [-0.25, +0.25]
_MISSING_PENALTY_WEIGHT   = 0.5
_STALE_PENALTY_WEIGHT     = 0.5
_SCORING_VERSION          = "4.1A.v1"

_VALID_INTEGRATION_MODES = frozenset({
    "additive_guardrailed",
    "confirmation_only",
    "suppression_only",
})

# Structural priority tie-break for family ranking (same intent as 4.0D).
_FAMILY_PRIORITY: dict[str, int] = {
    "macro":        100,
    "rates":         95,
    "fx":            90,
    "equity_index":  85,
    "risk":          85,
    "crypto_cross":  75,
    "commodity":     70,
}


@dataclass
class FamilyAttribution:
    dependency_family: str
    family_signal_score: float | None
    family_confirmation_score: float | None
    family_contradiction_penalty: float | None
    family_missing_penalty: float | None
    family_stale_penalty: float | None
    family_net_contribution: float | None
    family_rank: int | None
    top_symbols: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AttributionResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    base_signal_score: float | None
    cross_asset_signal_score: float | None
    cross_asset_confirmation_score: float | None
    cross_asset_contradiction_penalty: float | None
    cross_asset_missing_penalty: float | None
    cross_asset_stale_penalty: float | None
    cross_asset_net_contribution: float | None
    composite_pre_cross_asset: float | None
    composite_post_cross_asset: float | None
    integration_mode: str
    metadata: dict[str, Any]
    family_attribution: list[FamilyAttribution]


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class CrossAssetAttributionService:
    """Deterministic cross-asset attribution + composite integration."""

    # ── input loading ───────────────────────────────────────────────────
    def get_latest_base_run_scores(
        self, conn, *, workspace_id: str, run_id: str,  # noqa: ARG002 (reserved; not keyed by run_id today)
    ) -> dict[str, Any]:
        """Derive a workspace-level base_signal_score from composite_scores.
        composite_scores is not run-keyed in the current schema, so we use
        the most recent as_of per asset and average (long_score - short_score)
        across the workspace. Returns {base_signal_score, composite_pre,
        as_of, asset_count} with NULLs if unavailable."""
        with conn.cursor() as cur:
            cur.execute(
                """
                with latest_per_asset as (
                    select distinct on (asset_id)
                        asset_id, long_score, short_score, as_of
                    from public.composite_scores
                    where workspace_id = %s::uuid
                    order by asset_id, as_of desc
                )
                select
                    avg(long_score - short_score)::numeric as avg_net,
                    max(as_of)                            as latest_as_of,
                    count(*)                              as asset_count
                from latest_per_asset
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            if not row:
                return {"base_signal_score": None, "composite_pre": None,
                        "as_of": None, "asset_count": 0}
            d = dict(row)
            avg_net = _as_float(d.get("avg_net"))
            return {
                "base_signal_score":  avg_net,
                "composite_pre":      avg_net,
                "as_of":              d.get("latest_as_of"),
                "asset_count":        int(d.get("asset_count") or 0),
            }

    def get_latest_cross_asset_explanation_for_run(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    context_snapshot_id::text as context_snapshot_id,
                    dominant_dependency_family,
                    cross_asset_confidence_score,
                    confirmation_score,
                    contradiction_score,
                    missing_context_score,
                    stale_context_score,
                    top_confirming_symbols,
                    top_contradicting_symbols,
                    missing_dependency_symbols,
                    stale_dependency_symbols,
                    explanation_state,
                    created_at
                from public.cross_asset_explanation_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                order by created_at desc
                limit 1
                """,
                (workspace_id, watchlist_id, run_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_family_contributions_for_run(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    dependency_family,
                    family_signal_count,
                    confirmed_count,
                    contradicted_count,
                    missing_count,
                    stale_count,
                    family_confidence_score,
                    family_support_score,
                    family_contradiction_score,
                    top_symbols
                from public.cross_asset_family_explanation_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [dict(r) for r in cur.fetchall()]

    # ── top-level scoring helpers ───────────────────────────────────────
    def compute_cross_asset_signal_score(
        self, explanation: dict[str, Any] | None,
    ) -> float | None:
        """Core "signal energy" from cross-asset context:
        confirmation − contradiction. Reflects whether the backdrop is
        actively supportive or opposing, independent of missing-data
        penalties."""
        if explanation is None:
            return None
        conf = _as_float(explanation.get("confirmation_score"))
        contra = _as_float(explanation.get("contradiction_score"))
        if conf is None and contra is None:
            return None
        return (conf or 0.0) - (contra or 0.0)

    def compute_cross_asset_confirmation_score(
        self, explanation: dict[str, Any] | None,
    ) -> float | None:
        if explanation is None:
            return None
        return _as_float(explanation.get("confirmation_score"))

    def compute_cross_asset_contradiction_penalty(
        self, explanation: dict[str, Any] | None,
    ) -> float | None:
        if explanation is None:
            return None
        return _as_float(explanation.get("contradiction_score"))

    def compute_cross_asset_missing_penalty(
        self, explanation: dict[str, Any] | None,
    ) -> float | None:
        if explanation is None:
            return None
        m = _as_float(explanation.get("missing_context_score"))
        return (m or 0.0) * _MISSING_PENALTY_WEIGHT if m is not None else None

    def compute_cross_asset_stale_penalty(
        self, explanation: dict[str, Any] | None,
    ) -> float | None:
        if explanation is None:
            return None
        s = _as_float(explanation.get("stale_context_score"))
        return (s or 0.0) * _STALE_PENALTY_WEIGHT if s is not None else None

    def compute_cross_asset_net_contribution(
        self,
        *,
        confirmation: float | None,
        contradiction_penalty: float | None,
        missing_penalty: float | None,
        stale_penalty: float | None,
        integration_mode: str,
    ) -> float | None:
        if confirmation is None and contradiction_penalty is None \
                and missing_penalty is None and stale_penalty is None:
            return None
        c  = confirmation or 0.0
        xp = contradiction_penalty or 0.0
        mp = missing_penalty or 0.0
        sp = stale_penalty or 0.0

        if integration_mode == "confirmation_only":
            raw = c  # penalties ignored
        elif integration_mode == "suppression_only":
            raw = -(xp + mp + sp)  # positive contribution ignored
        else:  # additive_guardrailed (default)
            raw = c - xp - mp - sp
        return _clip(raw, -_NET_CONTRIBUTION_BOUND, _NET_CONTRIBUTION_BOUND)

    def integrate_with_base_composite(
        self,
        *,
        composite_pre: float | None,
        net_contribution: float | None,
        integration_weight: float = _INTEGRATION_WEIGHT,
    ) -> float | None:
        if composite_pre is None:
            # Attribution without a base is still informative but has no
            # pre/post delta to report.
            return None
        if net_contribution is None:
            return composite_pre
        return composite_pre + net_contribution * integration_weight

    # ── family attribution ──────────────────────────────────────────────
    def compute_family_attribution(
        self,
        family_rows: Sequence[dict[str, Any]],
        *,
        integration_mode: str,
    ) -> list[FamilyAttribution]:
        items: list[FamilyAttribution] = []
        for r in family_rows:
            total = int(r.get("family_signal_count") or 0)
            support = _as_float(r.get("family_support_score"))
            contra  = _as_float(r.get("family_contradiction_score"))
            missing_n = int(r.get("missing_count") or 0)
            stale_n   = int(r.get("stale_count") or 0)
            missing_ratio = (missing_n / total) if total > 0 else 0.0
            stale_ratio   = (stale_n / total)   if total > 0 else 0.0

            confirmation = support
            contradiction_penalty = contra
            missing_penalty = missing_ratio * _MISSING_PENALTY_WEIGHT
            stale_penalty   = stale_ratio   * _STALE_PENALTY_WEIGHT

            signal_score = None
            if support is not None or contra is not None:
                signal_score = (support or 0.0) - (contra or 0.0)

            if integration_mode == "confirmation_only":
                raw_net = (confirmation or 0.0)
            elif integration_mode == "suppression_only":
                raw_net = -((contradiction_penalty or 0.0) + missing_penalty + stale_penalty)
            else:
                raw_net = (
                    (confirmation or 0.0)
                    - (contradiction_penalty or 0.0)
                    - missing_penalty
                    - stale_penalty
                )
            net = _clip(raw_net, -_NET_CONTRIBUTION_BOUND, _NET_CONTRIBUTION_BOUND)

            top_syms_raw = r.get("top_symbols") or []
            if isinstance(top_syms_raw, str):
                import json
                try:
                    top_syms_raw = json.loads(top_syms_raw)
                except json.JSONDecodeError:
                    top_syms_raw = []
            top_symbols = [str(s) for s in top_syms_raw]

            items.append(FamilyAttribution(
                dependency_family=str(r["dependency_family"]),
                family_signal_score=signal_score,
                family_confirmation_score=confirmation,
                family_contradiction_penalty=contradiction_penalty,
                family_missing_penalty=missing_penalty,
                family_stale_penalty=stale_penalty,
                family_net_contribution=net,
                family_rank=None,  # assigned after sort
                top_symbols=top_symbols,
                metadata={
                    "signal_count":   total,
                    "missing_count":  missing_n,
                    "stale_count":    stale_n,
                    "raw_net":        raw_net,
                    "clipped":        abs(raw_net) > _NET_CONTRIBUTION_BOUND,
                },
            ))

        # Deterministic rank: net contribution DESC, |net| DESC, priority DESC,
        # family name ASC
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.family_net_contribution or 0.0),
                -abs(fa.family_net_contribution or 0.0),
                -_FAMILY_PRIORITY.get(fa.dependency_family, 0),
                fa.dependency_family,
            ),
        )
        for rank_i, item in enumerate(ranked, start=1):
            item.family_rank = rank_i
        return ranked

    # ── main builder ────────────────────────────────────────────────────
    def build_attribution_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
        integration_mode: str = "additive_guardrailed",
        integration_weight: float = _INTEGRATION_WEIGHT,
    ) -> AttributionResult | None:
        if integration_mode not in _VALID_INTEGRATION_MODES:
            raise ValueError(f"invalid integration_mode: {integration_mode!r}")

        explanation = self.get_latest_cross_asset_explanation_for_run(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
        )
        # Still emit a row if explanation is missing; attribution will be
        # NULL-dominated but metadata records the missing-context state.
        base = self.get_latest_base_run_scores(
            conn, workspace_id=workspace_id, run_id=run_id,
        )
        family_rows = self._load_family_contributions_for_run(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
        )

        confirmation = self.compute_cross_asset_confirmation_score(explanation)
        contra_pen   = self.compute_cross_asset_contradiction_penalty(explanation)
        missing_pen  = self.compute_cross_asset_missing_penalty(explanation)
        stale_pen    = self.compute_cross_asset_stale_penalty(explanation)
        signal_score = self.compute_cross_asset_signal_score(explanation)
        net_contrib  = self.compute_cross_asset_net_contribution(
            confirmation=confirmation,
            contradiction_penalty=contra_pen,
            missing_penalty=missing_pen,
            stale_penalty=stale_pen,
            integration_mode=integration_mode,
        )
        post = self.integrate_with_base_composite(
            composite_pre=base.get("composite_pre"),
            net_contribution=net_contrib,
            integration_weight=integration_weight,
        )

        families = self.compute_family_attribution(
            family_rows, integration_mode=integration_mode,
        )

        metadata: dict[str, Any] = {
            "scoring_version":       _SCORING_VERSION,
            "integration_weight":    integration_weight,
            "net_contribution_bound": _NET_CONTRIBUTION_BOUND,
            "missing_penalty_weight": _MISSING_PENALTY_WEIGHT,
            "stale_penalty_weight":   _STALE_PENALTY_WEIGHT,
            "has_base_composite":     base.get("base_signal_score") is not None,
            "base_as_of":             base.get("as_of").isoformat() if base.get("as_of") else None,
            "base_asset_count":       base.get("asset_count", 0),
            "has_explanation":        explanation is not None,
            "explanation_state":      (explanation or {}).get("explanation_state"),
            "family_count":           len(families),
        }

        return AttributionResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=(explanation or {}).get("context_snapshot_id"),
            base_signal_score=base.get("base_signal_score"),
            cross_asset_signal_score=signal_score,
            cross_asset_confirmation_score=confirmation,
            cross_asset_contradiction_penalty=contra_pen,
            cross_asset_missing_penalty=missing_pen,
            cross_asset_stale_penalty=stale_pen,
            cross_asset_net_contribution=net_contrib,
            composite_pre_cross_asset=base.get("composite_pre"),
            composite_post_cross_asset=post,
            integration_mode=integration_mode,
            metadata=metadata,
            family_attribution=families,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_cross_asset_attribution(
        self, conn, *, attribution: AttributionResult,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_attribution_snapshot(
            conn,
            workspace_id=attribution.workspace_id,
            watchlist_id=attribution.watchlist_id,
            run_id=attribution.run_id,
            context_snapshot_id=attribution.context_snapshot_id,
            base_signal_score=attribution.base_signal_score,
            cross_asset_signal_score=attribution.cross_asset_signal_score,
            cross_asset_confirmation_score=attribution.cross_asset_confirmation_score,
            cross_asset_contradiction_penalty=attribution.cross_asset_contradiction_penalty,
            cross_asset_missing_penalty=attribution.cross_asset_missing_penalty,
            cross_asset_stale_penalty=attribution.cross_asset_stale_penalty,
            cross_asset_net_contribution=attribution.cross_asset_net_contribution,
            composite_pre_cross_asset=attribution.composite_pre_cross_asset,
            composite_post_cross_asset=attribution.composite_post_cross_asset,
            integration_mode=attribution.integration_mode,
            metadata=attribution.metadata,
        )
        return str(row["id"])

    def persist_family_attribution(
        self, conn, *, attribution: AttributionResult,
    ) -> int:
        if not attribution.family_attribution:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_attribution_snapshots(
            conn,
            workspace_id=attribution.workspace_id,
            watchlist_id=attribution.watchlist_id,
            run_id=attribution.run_id,
            context_snapshot_id=attribution.context_snapshot_id,
            rows=[
                {
                    "dependency_family":           fa.dependency_family,
                    "family_signal_score":         fa.family_signal_score,
                    "family_confirmation_score":   fa.family_confirmation_score,
                    "family_contradiction_penalty": fa.family_contradiction_penalty,
                    "family_missing_penalty":      fa.family_missing_penalty,
                    "family_stale_penalty":        fa.family_stale_penalty,
                    "family_net_contribution":     fa.family_net_contribution,
                    "family_rank":                 fa.family_rank,
                    "top_symbols":                 fa.top_symbols,
                    "metadata":                    fa.metadata,
                }
                for fa in attribution.family_attribution
            ],
        )

    def build_and_persist(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
        integration_mode: str = "additive_guardrailed",
    ) -> AttributionResult | None:
        attribution = self.build_attribution_for_run(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            integration_mode=integration_mode,
        )
        if attribution is None:
            return None
        self.persist_cross_asset_attribution(conn, attribution=attribution)
        self.persist_family_attribution(conn, attribution=attribution)
        return attribution

    def refresh_workspace_attribution(
        self,
        conn,
        *,
        workspace_id: str,
        run_id: str,
        integration_mode: str = "additive_guardrailed",
    ) -> list[AttributionResult]:
        """Emit attribution for every watchlist in the workspace for a given
        run. Commits per-watchlist so a single failure does not abort the
        batch."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        emitted: list[AttributionResult] = []
        for wid in watchlist_ids:
            try:
                result = self.build_and_persist(
                    conn,
                    workspace_id=workspace_id,
                    watchlist_id=wid,
                    run_id=run_id,
                    integration_mode=integration_mode,
                )
                if result is not None:
                    conn.commit()
                    emitted.append(result)
            except Exception as exc:
                logger.warning(
                    "cross_asset_attribution: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return emitted
