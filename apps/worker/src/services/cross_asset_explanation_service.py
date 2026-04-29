"""Phase 4.0D: Cross-Asset Explanation Service.

Reads 4.0C cross-asset signal snapshots for a given (workspace, watchlist,
run), aggregates them into compact explanation scores and ranked symbol
lists, and persists:

  * one cross_asset_explanation_snapshots row
  * one cross_asset_family_contribution_snapshots row per dependency family

All scoring is deterministic. Missing/stale context is represented as an
explicit explanation_state rather than being silently zeroed.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_TOP_SYMBOL_LIMIT = 10

# Per-family structural priority for dominance tie-breaks. Higher = more
# structurally important (macro beats commodity, etc.).
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
class FamilyContribution:
    dependency_family: str
    family_signal_count: int = 0
    confirmed_count: int = 0
    contradicted_count: int = 0
    missing_count: int = 0
    stale_count: int = 0
    top_symbols: list[str] = field(default_factory=list)

    @property
    def family_support_score(self) -> float | None:
        if self.family_signal_count == 0:
            return None
        return self.confirmed_count / self.family_signal_count

    @property
    def family_contradiction_score(self) -> float | None:
        if self.family_signal_count == 0:
            return None
        return self.contradicted_count / self.family_signal_count

    @property
    def family_confidence_score(self) -> float | None:
        if self.family_signal_count == 0:
            return None
        deficit = (self.missing_count + self.stale_count) / self.family_signal_count
        raw = (
            (self.family_support_score or 0.0)
            - (self.family_contradiction_score or 0.0)
            - 0.5 * deficit
        )
        # Clip to [0, 1]
        return max(0.0, min(1.0, raw))

    @property
    def net_score(self) -> float:
        return (self.family_support_score or 0.0) - (self.family_contradiction_score or 0.0)


@dataclass(frozen=True)
class CrossAssetExplanation:
    workspace_id: str
    watchlist_id: str
    run_id: str | None
    context_snapshot_id: str | None
    dominant_dependency_family: str | None
    cross_asset_confidence_score: float | None
    confirmation_score: float | None
    contradiction_score: float | None
    missing_context_score: float | None
    stale_context_score: float | None
    top_confirming_symbols: list[str]
    top_contradicting_symbols: list[str]
    missing_dependency_symbols: list[str]
    stale_dependency_symbols: list[str]
    explanation_state: str
    metadata: dict[str, Any]
    family_contributions: list[FamilyContribution]


class CrossAssetExplanationService:
    """Deterministic cross-asset explanation assembly."""

    # ── signal loading ──────────────────────────────────────────────────
    def load_signals_for_scope(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str | None,
    ) -> list[dict[str, Any]]:
        """Load cross_asset_signal_snapshots for the exact (workspace,
        watchlist, run). When run_id is None, fall back to the latest batch
        of rows for that watchlist that share the same context_snapshot_id."""
        with conn.cursor() as cur:
            if run_id is not None:
                cur.execute(
                    """
                    select
                        signal_family, signal_key, signal_value,
                        signal_direction, signal_state, base_symbol,
                        dependency_symbols, dependency_families,
                        context_snapshot_id::text as context_snapshot_id,
                        created_at
                    from public.cross_asset_signal_snapshots
                    where workspace_id = %s::uuid
                      and watchlist_id = %s::uuid
                      and run_id       = %s::uuid
                    """,
                    (workspace_id, watchlist_id, run_id),
                )
            else:
                cur.execute(
                    """
                    with latest_ctx as (
                        select context_snapshot_id, max(created_at) as last_seen
                        from public.cross_asset_signal_snapshots
                        where workspace_id = %s::uuid
                          and watchlist_id = %s::uuid
                          and run_id is null
                        group by context_snapshot_id
                        order by last_seen desc
                        limit 1
                    )
                    select
                        s.signal_family, s.signal_key, s.signal_value,
                        s.signal_direction, s.signal_state, s.base_symbol,
                        s.dependency_symbols, s.dependency_families,
                        s.context_snapshot_id::text as context_snapshot_id,
                        s.created_at
                    from public.cross_asset_signal_snapshots s
                    join latest_ctx lc
                        on s.context_snapshot_id is not distinct from lc.context_snapshot_id
                    where s.workspace_id = %s::uuid
                      and s.watchlist_id = %s::uuid
                      and s.run_id is null
                    """,
                    (workspace_id, watchlist_id, workspace_id, watchlist_id),
                )
            return [dict(r) for r in cur.fetchall()]

    # ── scoring helpers ─────────────────────────────────────────────────
    @staticmethod
    def _safe_ratio(numerator: int, denominator: int) -> float | None:
        if denominator <= 0:
            return None
        return numerator / denominator

    def rank_confirming_symbols(
        self, signals: Sequence[dict[str, Any]], limit: int = _TOP_SYMBOL_LIMIT,
    ) -> list[str]:
        return self._rank_symbols_by_state(signals, "confirmed", limit)

    def rank_contradicting_symbols(
        self, signals: Sequence[dict[str, Any]], limit: int = _TOP_SYMBOL_LIMIT,
    ) -> list[str]:
        return self._rank_symbols_by_state(signals, "contradicted", limit)

    def rank_missing_symbols(
        self, signals: Sequence[dict[str, Any]], limit: int = _TOP_SYMBOL_LIMIT,
    ) -> list[str]:
        return self._rank_symbols_by_state(signals, "missing_context", limit)

    def rank_stale_symbols(
        self, signals: Sequence[dict[str, Any]], limit: int = _TOP_SYMBOL_LIMIT,
    ) -> list[str]:
        return self._rank_symbols_by_state(signals, "stale_context", limit)

    @staticmethod
    def _rank_symbols_by_state(
        signals: Sequence[dict[str, Any]], target_state: str, limit: int,
    ) -> list[str]:
        counts: dict[str, int] = defaultdict(int)
        for s in signals:
            if s.get("signal_state") != target_state:
                continue
            for sym in (s.get("dependency_symbols") or []):
                if sym:
                    counts[sym] += 1
        ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        return [sym for sym, _ in ranked[:limit]]

    def compute_family_contributions(
        self, signals: Sequence[dict[str, Any]],
    ) -> list[FamilyContribution]:
        by_family: dict[str, FamilyContribution] = {}
        family_symbol_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        for s in signals:
            state = s.get("signal_state")
            fams = s.get("dependency_families") or []
            if not fams:
                continue
            for fam in fams:
                fc = by_family.setdefault(fam, FamilyContribution(dependency_family=fam))
                fc.family_signal_count += 1
                if state == "confirmed":
                    fc.confirmed_count += 1
                elif state == "contradicted":
                    fc.contradicted_count += 1
                elif state == "missing_context":
                    fc.missing_count += 1
                elif state == "stale_context":
                    fc.stale_count += 1
                # Track symbol appearances per family for top_symbols
                for sym in (s.get("dependency_symbols") or []):
                    if sym:
                        family_symbol_counts[fam][sym] += 1

        for fam, fc in by_family.items():
            ranked_syms = sorted(
                family_symbol_counts[fam].items(),
                key=lambda kv: (-kv[1], kv[0]),
            )
            fc.top_symbols = [sym for sym, _ in ranked_syms[:_TOP_SYMBOL_LIMIT]]
        return list(by_family.values())

    def determine_dominant_dependency_family(
        self, contributions: Sequence[FamilyContribution],
    ) -> str | None:
        if not contributions:
            return None
        # Rank by net score DESC, signal count DESC, structural priority DESC,
        # family name ASC (stable tie-break).
        ranked = sorted(
            contributions,
            key=lambda f: (
                -f.net_score,
                -f.family_signal_count,
                -_FAMILY_PRIORITY.get(f.dependency_family, 0),
                f.dependency_family,
            ),
        )
        top = ranked[0]
        if top.family_signal_count == 0:
            return None
        return top.dependency_family

    def compute_cross_asset_confidence_score(
        self,
        *,
        confirmation: float | None,
        contradiction: float | None,
        missing: float | None,
        stale: float | None,
    ) -> float | None:
        if confirmation is None:
            return None
        c  = confirmation or 0.0
        x  = contradiction or 0.0
        m  = missing or 0.0
        s  = stale or 0.0
        raw = c - x - 0.5 * m - 0.5 * s
        # Clip to [0, 1]. A large negative confirmation-minus-contradiction
        # still means "no confidence" rather than a negative confidence.
        return max(0.0, min(1.0, raw))

    @staticmethod
    def _determine_explanation_state(
        *,
        total_signals: int,
        missing_score: float | None,
        stale_score: float | None,
    ) -> str:
        if total_signals == 0:
            return "missing_context"
        m = missing_score or 0.0
        s = stale_score or 0.0
        if s > 0.5:
            return "stale_context"
        if m > 0.5:
            return "missing_context"
        if (m + s) > 0.25:
            return "partial"
        return "computed"

    # ── main builder ────────────────────────────────────────────────────
    def build_cross_asset_explanation(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str | None = None,
    ) -> CrossAssetExplanation:
        signals = self.load_signals_for_scope(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
        )
        total = len(signals)

        context_snapshot_id: str | None = None
        if signals:
            # All signals for a (run, watchlist) should share a snapshot id;
            # pick the most common to be robust.
            ctx_counts: dict[str | None, int] = defaultdict(int)
            for s in signals:
                ctx_counts[s.get("context_snapshot_id")] += 1
            context_snapshot_id = max(ctx_counts.items(), key=lambda kv: kv[1])[0]

        confirmed_n    = sum(1 for s in signals if s.get("signal_state") == "confirmed")
        contradicted_n = sum(1 for s in signals if s.get("signal_state") == "contradicted")
        missing_n      = sum(1 for s in signals if s.get("signal_state") == "missing_context")
        stale_n        = sum(1 for s in signals if s.get("signal_state") == "stale_context")

        confirmation_score  = self._safe_ratio(confirmed_n,    total)
        contradiction_score = self._safe_ratio(contradicted_n, total)
        missing_score       = self._safe_ratio(missing_n,      total)
        stale_score         = self._safe_ratio(stale_n,        total)

        confidence_score = self.compute_cross_asset_confidence_score(
            confirmation=confirmation_score,
            contradiction=contradiction_score,
            missing=missing_score,
            stale=stale_score,
        )

        top_confirming    = self.rank_confirming_symbols(signals)
        top_contradicting = self.rank_contradicting_symbols(signals)
        missing_symbols   = self.rank_missing_symbols(signals)
        stale_symbols     = self.rank_stale_symbols(signals)

        contributions = self.compute_family_contributions(signals)
        dominant_family = self.determine_dominant_dependency_family(contributions)

        explanation_state = self._determine_explanation_state(
            total_signals=total,
            missing_score=missing_score,
            stale_score=stale_score,
        )

        metadata: dict[str, Any] = {
            "signal_total":        total,
            "signal_counts": {
                "confirmed":        confirmed_n,
                "contradicted":     contradicted_n,
                "missing_context":  missing_n,
                "stale_context":    stale_n,
                "other":            total - confirmed_n - contradicted_n - missing_n - stale_n,
            },
            "family_count":        len(contributions),
            "scoring_version":     "4.0D.v1",
        }

        return CrossAssetExplanation(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snapshot_id,
            dominant_dependency_family=dominant_family,
            cross_asset_confidence_score=confidence_score,
            confirmation_score=confirmation_score,
            contradiction_score=contradiction_score,
            missing_context_score=missing_score,
            stale_context_score=stale_score,
            top_confirming_symbols=top_confirming,
            top_contradicting_symbols=top_contradicting,
            missing_dependency_symbols=missing_symbols,
            stale_dependency_symbols=stale_symbols,
            explanation_state=explanation_state,
            metadata=metadata,
            family_contributions=contributions,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_cross_asset_explanation(
        self, conn, *, explanation: CrossAssetExplanation,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_explanation_snapshot(
            conn,
            workspace_id=explanation.workspace_id,
            watchlist_id=explanation.watchlist_id,
            run_id=explanation.run_id,
            context_snapshot_id=explanation.context_snapshot_id,
            dominant_dependency_family=explanation.dominant_dependency_family,
            cross_asset_confidence_score=explanation.cross_asset_confidence_score,
            confirmation_score=explanation.confirmation_score,
            contradiction_score=explanation.contradiction_score,
            missing_context_score=explanation.missing_context_score,
            stale_context_score=explanation.stale_context_score,
            top_confirming_symbols=explanation.top_confirming_symbols,
            top_contradicting_symbols=explanation.top_contradicting_symbols,
            missing_dependency_symbols=explanation.missing_dependency_symbols,
            stale_dependency_symbols=explanation.stale_dependency_symbols,
            explanation_state=explanation.explanation_state,
            metadata=explanation.metadata,
        )
        return str(row["id"])

    def persist_family_contributions(
        self, conn, *, explanation: CrossAssetExplanation,
    ) -> int:
        if not explanation.family_contributions:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_contribution_snapshots(
            conn,
            workspace_id=explanation.workspace_id,
            watchlist_id=explanation.watchlist_id,
            run_id=explanation.run_id,
            context_snapshot_id=explanation.context_snapshot_id,
            rows=[
                {
                    "dependency_family":        fc.dependency_family,
                    "family_signal_count":      fc.family_signal_count,
                    "confirmed_count":          fc.confirmed_count,
                    "contradicted_count":       fc.contradicted_count,
                    "missing_count":            fc.missing_count,
                    "stale_count":              fc.stale_count,
                    "family_confidence_score":  fc.family_confidence_score,
                    "family_support_score":     fc.family_support_score,
                    "family_contradiction_score": fc.family_contradiction_score,
                    "top_symbols":              fc.top_symbols,
                    "metadata":                 {"net_score": fc.net_score},
                }
                for fc in explanation.family_contributions
            ],
        )

    # ── orchestration ───────────────────────────────────────────────────
    def build_and_persist(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str | None = None,
    ) -> CrossAssetExplanation:
        explanation = self.build_cross_asset_explanation(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
        )
        self.persist_cross_asset_explanation(conn, explanation=explanation)
        self.persist_family_contributions(conn, explanation=explanation)
        return explanation

    def refresh_workspace_explanations(
        self, conn, *, workspace_id: str, run_id: str | None = None,
    ) -> list[CrossAssetExplanation]:
        """Emit one explanation per watchlist in the workspace. Commits per-
        watchlist to isolate failures."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        emitted: list[CrossAssetExplanation] = []
        for wid in watchlist_ids:
            try:
                explanation = self.build_and_persist(
                    conn,
                    workspace_id=workspace_id,
                    watchlist_id=wid,
                    run_id=run_id,
                )
                conn.commit()
                emitted.append(explanation)
            except Exception as exc:
                logger.warning(
                    "cross_asset_explanation: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return emitted
