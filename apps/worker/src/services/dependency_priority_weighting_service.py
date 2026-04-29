"""Phase 4.1B: Dependency-Priority-Aware Weighting Service.

Refines 4.1A raw attribution by applying deterministic multiplier weights
derived from:
  * graph priority (from 4.0B asset_dependency_graph)
  * dependency type (via weighting profile overrides)
  * dependency family (via weighting profile overrides)
  * coverage / freshness (from 4.0B watchlist_dependency_coverage_summary)
  * direct vs secondary dependency role

All weights are clipped so no single multiplier channel can amplify a raw
contribution beyond a conservative bound. Raw attribution is preserved
alongside weighted attribution.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

# Weighting bounds
_TOTAL_MULTIPLIER_MIN = 0.5
_TOTAL_MULTIPLIER_MAX = 1.25

# Priority band: scores below 70 or above 100 are clamped before shaping.
_PRIORITY_FLOOR = 70
_PRIORITY_CEIL  = 100
_PRIORITY_WEIGHT_FLOOR = 0.85
_PRIORITY_WEIGHT_CEIL  = 1.00

# Coverage band
_COVERAGE_WEIGHT_FLOOR = 0.50
_COVERAGE_WEIGHT_CEIL  = 1.00

_SCORING_VERSION = "4.1B.v1"

_DEFAULT_PROFILE: dict[str, Any] = {
    "profile_name":                 "default_deterministic",
    "priority_weight_scale":        1.0,
    "direct_dependency_bonus":      0.20,
    "secondary_dependency_penalty": 0.10,
    "missing_penalty_scale":        1.0,
    "stale_penalty_scale":          1.0,
    "family_weight_overrides":      {},
    "type_weight_overrides":        {},
}


@dataclass
class WeightedFamilyAttribution:
    dependency_family: str
    raw_family_net_contribution: float | None
    priority_weight: float
    family_weight: float
    type_weight: float
    coverage_weight: float
    weighted_family_net_contribution: float | None
    weighted_family_rank: int | None = None
    top_symbols: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class WeightedSymbolAttribution:
    symbol: str
    dependency_family: str
    dependency_type: str | None
    graph_priority: int | None
    is_direct_dependency: bool
    raw_symbol_score: float | None
    priority_weight: float
    family_weight: float
    type_weight: float
    coverage_weight: float
    weighted_symbol_score: float | None
    symbol_rank: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WeightingResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    weighting_profile_id: str | None
    profile_name: str
    default_profile_used: bool
    family_rows: list[WeightedFamilyAttribution]
    symbol_rows: list[WeightedSymbolAttribution]


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class DependencyPriorityWeightingService:
    """Deterministic per-family and per-symbol weighting refinement."""

    # ── profile loading ─────────────────────────────────────────────────
    def get_active_weighting_profile(
        self, conn, *, workspace_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, profile_name,
                       priority_weight_scale,
                       direct_dependency_bonus,
                       secondary_dependency_penalty,
                       missing_penalty_scale,
                       stale_penalty_scale,
                       family_weight_overrides,
                       type_weight_overrides,
                       metadata,
                       created_at
                from public.dependency_weighting_profiles
                where workspace_id = %s::uuid
                  and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ── weight primitives ───────────────────────────────────────────────
    def compute_priority_weight(
        self, *, graph_priority: int | None, profile: dict[str, Any],
    ) -> float:
        """Priority 70 → 0.85, priority 100 → 1.00. Values outside [70, 100]
        are clamped. `priority_weight_scale` further scales the resulting
        weight multiplicatively; result is clipped to [0.5, 1.25] to keep
        the scale control from causing runaway amplification."""
        scale = _as_float(profile.get("priority_weight_scale")) or 1.0
        if graph_priority is None:
            return _clip(1.0 * scale, _TOTAL_MULTIPLIER_MIN, _TOTAL_MULTIPLIER_MAX)
        p = _clip(float(graph_priority), _PRIORITY_FLOOR, _PRIORITY_CEIL)
        # Linear interpolation between floor priority and ceil priority
        ratio = (p - _PRIORITY_FLOOR) / (_PRIORITY_CEIL - _PRIORITY_FLOOR)
        base = _PRIORITY_WEIGHT_FLOOR + ratio * (_PRIORITY_WEIGHT_CEIL - _PRIORITY_WEIGHT_FLOOR)
        return _clip(base * scale, _TOTAL_MULTIPLIER_MIN, _TOTAL_MULTIPLIER_MAX)

    def compute_family_weight(
        self, *, dependency_family: str, profile: dict[str, Any],
    ) -> float:
        overrides = profile.get("family_weight_overrides") or {}
        if isinstance(overrides, str):
            import json
            try:
                overrides = json.loads(overrides)
            except json.JSONDecodeError:
                overrides = {}
        raw = _as_float(overrides.get(dependency_family))
        if raw is None:
            return 1.0
        return _clip(raw, _TOTAL_MULTIPLIER_MIN, _TOTAL_MULTIPLIER_MAX)

    def compute_type_weight(
        self, *, dependency_type: str | None, profile: dict[str, Any],
    ) -> float:
        if dependency_type is None:
            return 1.0
        overrides = profile.get("type_weight_overrides") or {}
        if isinstance(overrides, str):
            import json
            try:
                overrides = json.loads(overrides)
            except json.JSONDecodeError:
                overrides = {}
        raw = _as_float(overrides.get(dependency_type))
        if raw is None:
            return 1.0
        return _clip(raw, _TOTAL_MULTIPLIER_MIN, _TOTAL_MULTIPLIER_MAX)

    def compute_coverage_weight(
        self, *, coverage_ratio: float | None,
    ) -> float:
        """Maps 4.0B coverage_ratio [0, 1] → [0.5, 1.0]. Null → neutral 1.0."""
        if coverage_ratio is None:
            return 1.0
        ratio = _clip(coverage_ratio, 0.0, 1.0)
        return _COVERAGE_WEIGHT_FLOOR + ratio * (_COVERAGE_WEIGHT_CEIL - _COVERAGE_WEIGHT_FLOOR)

    def compute_direct_adjustment(
        self, *, is_direct: bool, profile: dict[str, Any],
    ) -> float:
        bonus = _as_float(profile.get("direct_dependency_bonus")) or 0.0
        penalty = _as_float(profile.get("secondary_dependency_penalty")) or 0.0
        if is_direct:
            return 1.0 + max(0.0, bonus)
        return max(0.0, 1.0 - max(0.0, penalty))

    # ── input loading ───────────────────────────────────────────────────
    def _load_raw_family_attribution(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    dependency_family,
                    family_net_contribution,
                    family_rank,
                    top_symbols
                from public.cross_asset_family_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_graph_edges_for_dependencies(
        self,
        conn,
        *,
        dependency_symbols: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        """Return, for each dependency symbol, the highest-priority active
        edge pointing TO that symbol. Used to resolve graph_priority and
        dependency_type per symbol."""
        if not dependency_symbols:
            return {}
        with conn.cursor() as cur:
            cur.execute(
                """
                with ranked as (
                    select to_symbol, dependency_type, dependency_family,
                           priority, weight,
                           row_number() over (
                               partition by to_symbol
                               order by priority desc, weight desc, from_symbol asc
                           ) as rn
                    from public.asset_dependency_graph
                    where is_active = true
                      and to_symbol = any(%s::text[])
                )
                select to_symbol as symbol, dependency_type, dependency_family,
                       priority, weight
                from ranked
                where rn = 1
                """,
                (list(dependency_symbols),),
            )
            return {dict(r)["symbol"]: dict(r) for r in cur.fetchall()}

    def _load_latest_coverage_ratio(
        self, conn, *, workspace_id: str, watchlist_id: str,
    ) -> float | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select coverage_ratio
                from public.watchlist_dependency_coverage_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                limit 1
                """,
                (workspace_id, watchlist_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            return _as_float(dict(row).get("coverage_ratio"))

    # ── weighted family + symbol construction ───────────────────────────
    def compute_weighted_family_attribution(
        self,
        raw_rows: Sequence[dict[str, Any]],
        *,
        profile: dict[str, Any],
        coverage_ratio: float | None,
        edges_by_symbol: dict[str, dict[str, Any]],
    ) -> list[WeightedFamilyAttribution]:
        """One weighted row per raw family row. Per-family priority_weight is
        the average of member-symbol priority weights; type_weight is the
        majority member type weight (falling back to 1.0)."""
        coverage_w = self.compute_coverage_weight(coverage_ratio=coverage_ratio)

        out: list[WeightedFamilyAttribution] = []
        for r in raw_rows:
            family = str(r["dependency_family"])
            raw_net = _as_float(r.get("family_net_contribution"))
            top_syms_raw = r.get("top_symbols") or []
            if isinstance(top_syms_raw, str):
                import json
                try:
                    top_syms_raw = json.loads(top_syms_raw)
                except json.JSONDecodeError:
                    top_syms_raw = []
            top_symbols = [str(s) for s in top_syms_raw]

            # Priority weight: avg across member symbols' edges (or neutral)
            priority_weights = []
            type_weights = []
            for sym in top_symbols:
                edge = edges_by_symbol.get(sym) or {}
                p = edge.get("priority")
                priority_weights.append(
                    self.compute_priority_weight(graph_priority=int(p) if p is not None else None, profile=profile)
                )
                type_weights.append(
                    self.compute_type_weight(dependency_type=edge.get("dependency_type"), profile=profile)
                )
            priority_w = sum(priority_weights) / len(priority_weights) if priority_weights else 1.0
            type_w     = sum(type_weights) / len(type_weights)         if type_weights     else 1.0
            family_w   = self.compute_family_weight(dependency_family=family, profile=profile)

            total = priority_w * family_w * type_w * coverage_w
            total_clipped = _clip(total, _TOTAL_MULTIPLIER_MIN, _TOTAL_MULTIPLIER_MAX)
            weighted_net = raw_net * total_clipped if raw_net is not None else None

            out.append(WeightedFamilyAttribution(
                dependency_family=family,
                raw_family_net_contribution=raw_net,
                priority_weight=priority_w,
                family_weight=family_w,
                type_weight=type_w,
                coverage_weight=coverage_w,
                weighted_family_net_contribution=weighted_net,
                top_symbols=top_symbols,
                metadata={
                    "raw_family_rank":  r.get("family_rank"),
                    "member_count":     len(top_symbols),
                    "total_multiplier": total_clipped,
                    "clipped":          (total != total_clipped),
                },
            ))
        return out

    def rank_weighted_families(
        self, items: list[WeightedFamilyAttribution],
    ) -> list[WeightedFamilyAttribution]:
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.weighted_family_net_contribution or 0.0),
                -abs(fa.weighted_family_net_contribution or 0.0),
                fa.dependency_family,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.weighted_family_rank = i
        return ranked

    def compute_weighted_symbol_attribution(
        self,
        weighted_families: Sequence[WeightedFamilyAttribution],
        *,
        profile: dict[str, Any],
        coverage_ratio: float | None,
        edges_by_symbol: dict[str, dict[str, Any]],
        primary_symbols: set[str],
    ) -> list[WeightedSymbolAttribution]:
        coverage_w = self.compute_coverage_weight(coverage_ratio=coverage_ratio)
        rows: list[WeightedSymbolAttribution] = []

        for fa in weighted_families:
            # Raw symbol score: each member symbol inherits the family's raw
            # contribution, weighted by the symbol's structural edge weight
            # relative to siblings. With neutral weights (default), each
            # symbol's raw share is raw_family_net / member_count.
            members = fa.top_symbols or []
            if not members:
                continue
            # Compute edge-weight shares; default equal share if no edges
            edge_weights = []
            for sym in members:
                edge = edges_by_symbol.get(sym) or {}
                w = _as_float(edge.get("weight"))
                edge_weights.append(w if w is not None else 1.0)
            total_edge_weight = sum(edge_weights) if sum(edge_weights) > 0 else float(len(members))

            for idx, sym in enumerate(members):
                edge = edges_by_symbol.get(sym) or {}
                edge_priority = edge.get("priority")
                edge_type     = edge.get("dependency_type")
                # Share of the raw family contribution this symbol carries
                share = (edge_weights[idx] / total_edge_weight) if total_edge_weight > 0 else 0.0
                raw_score = (
                    (fa.raw_family_net_contribution or 0.0) * share
                    if fa.raw_family_net_contribution is not None else None
                )

                priority_w = self.compute_priority_weight(
                    graph_priority=int(edge_priority) if edge_priority is not None else None,
                    profile=profile,
                )
                family_w = fa.family_weight
                type_w   = self.compute_type_weight(
                    dependency_type=edge_type, profile=profile,
                )
                # "direct" = this symbol is an immediate dependency of some
                # primary watchlist symbol. In 4.0B one-hop expansion, every
                # dependency symbol in the context is direct; the column
                # remains for future multi-hop expansion.
                is_direct = sym not in primary_symbols
                direct_adj = self.compute_direct_adjustment(is_direct=True, profile=profile)

                total = priority_w * family_w * type_w * coverage_w * direct_adj
                total_clipped = _clip(total, _TOTAL_MULTIPLIER_MIN, _TOTAL_MULTIPLIER_MAX)
                weighted = raw_score * total_clipped if raw_score is not None else None

                rows.append(WeightedSymbolAttribution(
                    symbol=sym,
                    dependency_family=fa.dependency_family,
                    dependency_type=edge_type,
                    graph_priority=int(edge_priority) if edge_priority is not None else None,
                    is_direct_dependency=is_direct,
                    raw_symbol_score=raw_score,
                    priority_weight=priority_w,
                    family_weight=family_w,
                    type_weight=type_w,
                    coverage_weight=coverage_w,
                    weighted_symbol_score=weighted,
                    metadata={
                        "edge_weight_share": share,
                        "direct_adjustment": direct_adj,
                        "total_multiplier":  total_clipped,
                        "clipped":           (total != total_clipped),
                    },
                ))
        return rows

    def rank_weighted_symbols(
        self, rows: list[WeightedSymbolAttribution],
    ) -> list[WeightedSymbolAttribution]:
        ranked = sorted(
            rows,
            key=lambda r: (
                -(r.weighted_symbol_score or 0.0),
                -abs(r.weighted_symbol_score or 0.0),
                -(r.graph_priority or 0),
                r.symbol,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.symbol_rank = i
        return ranked

    # ── orchestration ───────────────────────────────────────────────────
    def build_weighting_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> WeightingResult | None:
        raw_rows = self._load_raw_family_attribution(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not raw_rows:
            return None

        profile_row = self.get_active_weighting_profile(conn, workspace_id=workspace_id)
        default_profile_used = profile_row is None
        profile = profile_row if profile_row is not None else _DEFAULT_PROFILE
        profile_id = profile_row["id"] if profile_row is not None else None
        profile_name = profile.get("profile_name", _DEFAULT_PROFILE["profile_name"])

        all_top_symbols: set[str] = set()
        for r in raw_rows:
            top = r.get("top_symbols") or []
            if isinstance(top, str):
                import json
                try:
                    top = json.loads(top)
                except json.JSONDecodeError:
                    top = []
            for s in top:
                if s:
                    all_top_symbols.add(str(s))
        edges_by_symbol = self._load_graph_edges_for_dependencies(
            conn, dependency_symbols=sorted(all_top_symbols),
        )
        coverage_ratio = self._load_latest_coverage_ratio(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
        )

        # primary watchlist symbols (used for direct/secondary flagging)
        primary_symbols: set[str] = set()
        with conn.cursor() as cur:
            cur.execute(
                """
                select distinct a.symbol as symbol
                from public.watchlist_assets wa
                join public.assets a on a.id = wa.asset_id
                where wa.watchlist_id = %s::uuid
                """,
                (watchlist_id,),
            )
            for row in cur.fetchall():
                primary_symbols.add(dict(row)["symbol"])

        weighted_families = self.compute_weighted_family_attribution(
            raw_rows,
            profile=profile,
            coverage_ratio=coverage_ratio,
            edges_by_symbol=edges_by_symbol,
        )
        weighted_families = self.rank_weighted_families(weighted_families)

        weighted_symbols = self.compute_weighted_symbol_attribution(
            weighted_families,
            profile=profile,
            coverage_ratio=coverage_ratio,
            edges_by_symbol=edges_by_symbol,
            primary_symbols=primary_symbols,
        )
        weighted_symbols = self.rank_weighted_symbols(weighted_symbols)

        # Carry context_snapshot_id from the raw attribution row (all rows
        # for the run share one).
        context_snapshot_id: str | None = None
        with conn.cursor() as cur:
            cur.execute(
                """
                select context_snapshot_id::text as context_snapshot_id
                from public.cross_asset_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                context_snapshot_id = dict(row).get("context_snapshot_id")

        for fa in weighted_families:
            fa.metadata["scoring_version"]      = _SCORING_VERSION
            fa.metadata["default_profile_used"] = default_profile_used
            fa.metadata["profile_name"]         = profile_name
        for sa in weighted_symbols:
            sa.metadata["scoring_version"]      = _SCORING_VERSION
            sa.metadata["default_profile_used"] = default_profile_used
            sa.metadata["profile_name"]         = profile_name

        return WeightingResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snapshot_id,
            weighting_profile_id=profile_id,
            profile_name=profile_name,
            default_profile_used=default_profile_used,
            family_rows=weighted_families,
            symbol_rows=weighted_symbols,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_weighted_attribution(
        self, conn, *, result: WeightingResult,
    ) -> dict[str, int]:
        import src.db.repositories as repo
        fam_count = repo.insert_cross_asset_family_weighted_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            weighting_profile_id=result.weighting_profile_id,
            rows=[
                {
                    "dependency_family":                fa.dependency_family,
                    "raw_family_net_contribution":      fa.raw_family_net_contribution,
                    "priority_weight":                  fa.priority_weight,
                    "family_weight":                    fa.family_weight,
                    "type_weight":                      fa.type_weight,
                    "coverage_weight":                  fa.coverage_weight,
                    "weighted_family_net_contribution": fa.weighted_family_net_contribution,
                    "weighted_family_rank":             fa.weighted_family_rank,
                    "top_symbols":                      fa.top_symbols,
                    "metadata":                         fa.metadata,
                }
                for fa in result.family_rows
            ],
        )
        sym_count = repo.insert_cross_asset_symbol_weighted_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            weighting_profile_id=result.weighting_profile_id,
            rows=[
                {
                    "symbol":                sa.symbol,
                    "dependency_family":     sa.dependency_family,
                    "dependency_type":       sa.dependency_type,
                    "graph_priority":        sa.graph_priority,
                    "is_direct_dependency":  sa.is_direct_dependency,
                    "raw_symbol_score":      sa.raw_symbol_score,
                    "priority_weight":       sa.priority_weight,
                    "family_weight":         sa.family_weight,
                    "type_weight":           sa.type_weight,
                    "coverage_weight":       sa.coverage_weight,
                    "weighted_symbol_score": sa.weighted_symbol_score,
                    "symbol_rank":           sa.symbol_rank,
                    "metadata":              sa.metadata,
                }
                for sa in result.symbol_rows
            ],
        )
        return {"family_rows": fam_count, "symbol_rows": sym_count}

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> WeightingResult | None:
        result = self.build_weighting_for_run(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
        )
        if result is None:
            return None
        self.persist_weighted_attribution(conn, result=result)
        return result

    def refresh_workspace_weighting(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[WeightingResult]:
        """Emit weighted attribution for every watchlist with 4.1A raw
        attribution for the given run. Commits per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[WeightingResult] = []
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
                    "dependency_priority_weighting: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
