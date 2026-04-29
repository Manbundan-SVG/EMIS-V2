"""Phase 4.1C: Regime-Aware Cross-Asset Interpretation Service.

Conditions 4.1B weighted attribution on the active regime (from 2.5D
regime_transition_events). Applies per-regime family/type multipliers and
direction-dependent confirmation/contradiction scales. Preserves raw 4.1A
values + weighted 4.1B values alongside regime-adjusted values.

All regime adjustments are deterministic; multipliers are clipped to a
conservative band so no single channel can amplify contribution beyond
reasonable bounds.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

# Regime-driven multiplier band (spec §11: conservative [0.75, 1.25]).
_REGIME_MULTIPLIER_MIN = 0.75
_REGIME_MULTIPLIER_MAX = 1.25

# Direction-scale band — confirmation/contradiction scales are applied
# conditionally (positive vs negative net) so each is clipped independently.
_DIRECTION_SCALE_MIN = 0.50
_DIRECTION_SCALE_MAX = 1.50

_SCORING_VERSION = "4.1C.v1"

# Structural priority tie-break for dominant family selection (matches 4.1B).
_FAMILY_PRIORITY: dict[str, int] = {
    "macro":        100,
    "rates":         95,
    "fx":            90,
    "equity_index":  85,
    "risk":          85,
    "crypto_cross":  75,
    "commodity":     70,
}

_DEFAULT_PROFILE: dict[str, Any] = {
    "profile_name":            "default_neutral",
    "family_weight_overrides": {},
    "type_weight_overrides":   {},
    "confirmation_scale":      1.0,
    "contradiction_scale":     1.0,
    "missing_penalty_scale":   1.0,
    "stale_penalty_scale":     1.0,
    "dominance_threshold":     0.05,
}


@dataclass
class RegimeFamilyAttribution:
    dependency_family: str
    raw_family_net_contribution: float | None
    weighted_family_net_contribution: float | None
    regime_family_weight: float
    regime_type_weight: float
    regime_confirmation_scale: float
    regime_contradiction_scale: float
    regime_missing_penalty_scale: float
    regime_stale_penalty_scale: float
    regime_adjusted_family_contribution: float | None
    regime_family_rank: int | None = None
    interpretation_state: str = "computed"
    top_symbols: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RegimeSymbolAttribution:
    symbol: str
    dependency_family: str
    dependency_type: str | None
    graph_priority: int | None
    is_direct_dependency: bool
    raw_symbol_score: float | None
    weighted_symbol_score: float | None
    regime_family_weight: float
    regime_type_weight: float
    regime_adjusted_symbol_score: float | None
    symbol_rank: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RegimeInterpretationResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    regime_key: str
    interpretation_profile_id: str | None
    profile_name: str
    default_profile_used: bool
    interpretation_state: str
    family_rows: list[RegimeFamilyAttribution]
    symbol_rows: list[RegimeSymbolAttribution]


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_overrides(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        import json
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


class RegimeAwareCrossAssetService:
    """Deterministic regime-conditioned refinement of 4.1B weighted attribution."""

    # ── regime + profile loading ────────────────────────────────────────
    def get_active_regime(
        self,
        conn,
        *,
        run_id: str,
    ) -> tuple[str, str]:
        """Return (regime_key, interpretation_state). Falls back to
        'missing_regime' when the run has no regime_transition_events row or
        to_regime is null."""
        with conn.cursor() as cur:
            cur.execute(
                """
                select to_regime, from_regime, transition_classification
                from public.regime_transition_events
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
        if not row:
            return ("missing_regime", "missing_regime")
        d = dict(row)
        key = d.get("to_regime") or d.get("from_regime")
        if not key:
            return ("missing_regime", "missing_regime")
        return (str(key), "computed")

    def get_active_interpretation_profile(
        self, conn, *, workspace_id: str, regime_key: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, profile_name, regime_key, is_active,
                       family_weight_overrides, type_weight_overrides,
                       confirmation_scale, contradiction_scale,
                       missing_penalty_scale, stale_penalty_scale,
                       dominance_threshold, metadata, created_at
                from public.regime_cross_asset_interpretation_profiles
                where workspace_id = %s::uuid
                  and regime_key   = %s
                  and is_active    = true
                order by created_at desc
                limit 1
                """,
                (workspace_id, regime_key),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ── weight primitives ───────────────────────────────────────────────
    def compute_regime_family_weight(
        self, *, dependency_family: str, profile: dict[str, Any],
    ) -> float:
        overrides = _parse_overrides(profile.get("family_weight_overrides"))
        raw = _as_float(overrides.get(dependency_family))
        if raw is None:
            return 1.0
        return _clip(raw, _REGIME_MULTIPLIER_MIN, _REGIME_MULTIPLIER_MAX)

    def compute_regime_type_weight(
        self, *, dependency_type: str | None, profile: dict[str, Any],
    ) -> float:
        if dependency_type is None:
            return 1.0
        overrides = _parse_overrides(profile.get("type_weight_overrides"))
        raw = _as_float(overrides.get(dependency_type))
        if raw is None:
            return 1.0
        return _clip(raw, _REGIME_MULTIPLIER_MIN, _REGIME_MULTIPLIER_MAX)

    def compute_regime_confirmation_scale(self, profile: dict[str, Any]) -> float:
        v = _as_float(profile.get("confirmation_scale"))
        if v is None:
            return 1.0
        return _clip(v, _DIRECTION_SCALE_MIN, _DIRECTION_SCALE_MAX)

    def compute_regime_contradiction_scale(self, profile: dict[str, Any]) -> float:
        v = _as_float(profile.get("contradiction_scale"))
        if v is None:
            return 1.0
        return _clip(v, _DIRECTION_SCALE_MIN, _DIRECTION_SCALE_MAX)

    def compute_regime_missing_penalty_scale(self, profile: dict[str, Any]) -> float:
        v = _as_float(profile.get("missing_penalty_scale"))
        if v is None:
            return 1.0
        return _clip(v, _DIRECTION_SCALE_MIN, _DIRECTION_SCALE_MAX)

    def compute_regime_stale_penalty_scale(self, profile: dict[str, Any]) -> float:
        v = _as_float(profile.get("stale_penalty_scale"))
        if v is None:
            return 1.0
        return _clip(v, _DIRECTION_SCALE_MIN, _DIRECTION_SCALE_MAX)

    # ── input loading ───────────────────────────────────────────────────
    def _load_weighted_family_attribution(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    dependency_family,
                    raw_family_net_contribution,
                    weighted_family_net_contribution,
                    top_symbols
                from public.cross_asset_family_weighted_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_weighted_symbol_attribution(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    symbol, dependency_family, dependency_type,
                    graph_priority, is_direct_dependency,
                    raw_symbol_score, weighted_symbol_score
                from public.cross_asset_symbol_weighted_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_context_snapshot_id(
        self, conn, *, run_id: str,
    ) -> str | None:
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
            return dict(row).get("context_snapshot_id") if row else None

    # ── core attribution builders ───────────────────────────────────────
    def compute_regime_adjusted_family_attribution(
        self,
        weighted_rows: Sequence[dict[str, Any]],
        *,
        profile: dict[str, Any],
        regime_key: str,
        interpretation_state: str,
        edge_type_by_family: dict[str, str | None],
    ) -> list[RegimeFamilyAttribution]:
        confirmation_scale   = self.compute_regime_confirmation_scale(profile)
        contradiction_scale  = self.compute_regime_contradiction_scale(profile)
        missing_scale        = self.compute_regime_missing_penalty_scale(profile)
        stale_scale          = self.compute_regime_stale_penalty_scale(profile)

        out: list[RegimeFamilyAttribution] = []
        for r in weighted_rows:
            family = str(r["dependency_family"])
            raw_net      = _as_float(r.get("raw_family_net_contribution"))
            weighted_net = _as_float(r.get("weighted_family_net_contribution"))

            top_syms_raw = r.get("top_symbols") or []
            if isinstance(top_syms_raw, str):
                import json
                try:
                    top_syms_raw = json.loads(top_syms_raw)
                except json.JSONDecodeError:
                    top_syms_raw = []
            top_symbols = [str(s) for s in top_syms_raw]

            family_w = self.compute_regime_family_weight(
                dependency_family=family, profile=profile,
            )
            dep_type = edge_type_by_family.get(family)
            type_w = self.compute_regime_type_weight(
                dependency_type=dep_type, profile=profile,
            )

            if weighted_net is None:
                regime_adjusted: float | None = None
            else:
                direction_scale = confirmation_scale if weighted_net >= 0 else contradiction_scale
                combined = family_w * type_w * direction_scale
                combined_clipped = _clip(combined, _REGIME_MULTIPLIER_MIN, _REGIME_MULTIPLIER_MAX)
                regime_adjusted = weighted_net * combined_clipped

            out.append(RegimeFamilyAttribution(
                dependency_family=family,
                raw_family_net_contribution=raw_net,
                weighted_family_net_contribution=weighted_net,
                regime_family_weight=family_w,
                regime_type_weight=type_w,
                regime_confirmation_scale=confirmation_scale,
                regime_contradiction_scale=contradiction_scale,
                regime_missing_penalty_scale=missing_scale,
                regime_stale_penalty_scale=stale_scale,
                regime_adjusted_family_contribution=regime_adjusted,
                interpretation_state=interpretation_state,
                top_symbols=top_symbols,
                metadata={
                    "regime_key":          regime_key,
                    "dependency_type":     dep_type,
                    "direction_sign":      (1 if (weighted_net or 0) >= 0 else -1),
                    "total_multiplier":    (
                        _clip(
                            family_w * type_w
                            * (confirmation_scale if (weighted_net or 0) >= 0 else contradiction_scale),
                            _REGIME_MULTIPLIER_MIN, _REGIME_MULTIPLIER_MAX,
                        ) if weighted_net is not None else None
                    ),
                },
            ))
        return out

    def rank_regime_families(
        self, items: list[RegimeFamilyAttribution],
    ) -> list[RegimeFamilyAttribution]:
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.regime_adjusted_family_contribution or 0.0),
                -abs(fa.regime_adjusted_family_contribution or 0.0),
                -_FAMILY_PRIORITY.get(fa.dependency_family, 0),
                fa.dependency_family,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.regime_family_rank = i
        return ranked

    def compute_regime_adjusted_symbol_attribution(
        self,
        weighted_symbols: Sequence[dict[str, Any]],
        *,
        profile: dict[str, Any],
    ) -> list[RegimeSymbolAttribution]:
        confirmation_scale  = self.compute_regime_confirmation_scale(profile)
        contradiction_scale = self.compute_regime_contradiction_scale(profile)

        rows: list[RegimeSymbolAttribution] = []
        for r in weighted_symbols:
            family = str(r["dependency_family"])
            dep_type = r.get("dependency_type")
            weighted_score = _as_float(r.get("weighted_symbol_score"))
            raw_score      = _as_float(r.get("raw_symbol_score"))

            family_w = self.compute_regime_family_weight(
                dependency_family=family, profile=profile,
            )
            type_w = self.compute_regime_type_weight(
                dependency_type=dep_type, profile=profile,
            )

            if weighted_score is None:
                regime_adjusted: float | None = None
            else:
                direction_scale = confirmation_scale if weighted_score >= 0 else contradiction_scale
                combined = family_w * type_w * direction_scale
                combined_clipped = _clip(combined, _REGIME_MULTIPLIER_MIN, _REGIME_MULTIPLIER_MAX)
                regime_adjusted = weighted_score * combined_clipped

            rows.append(RegimeSymbolAttribution(
                symbol=str(r["symbol"]),
                dependency_family=family,
                dependency_type=dep_type,
                graph_priority=(int(r["graph_priority"]) if r.get("graph_priority") is not None else None),
                is_direct_dependency=bool(r.get("is_direct_dependency", True)),
                raw_symbol_score=raw_score,
                weighted_symbol_score=weighted_score,
                regime_family_weight=family_w,
                regime_type_weight=type_w,
                regime_adjusted_symbol_score=regime_adjusted,
                metadata={
                    "direction_sign": (1 if (weighted_score or 0) >= 0 else -1),
                },
            ))
        return rows

    def rank_regime_symbols(
        self, rows: list[RegimeSymbolAttribution],
    ) -> list[RegimeSymbolAttribution]:
        ranked = sorted(
            rows,
            key=lambda r: (
                -(r.regime_adjusted_symbol_score or 0.0),
                -abs(r.regime_adjusted_symbol_score or 0.0),
                -(r.graph_priority or 0),
                r.symbol,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.symbol_rank = i
        return ranked

    # ── orchestration ───────────────────────────────────────────────────
    def build_regime_interpretation_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> RegimeInterpretationResult | None:
        weighted_rows = self._load_weighted_family_attribution(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not weighted_rows:
            return None
        weighted_symbols = self._load_weighted_symbol_attribution(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )

        regime_key, interpretation_state = self.get_active_regime(conn, run_id=run_id)
        profile_row = self.get_active_interpretation_profile(
            conn, workspace_id=workspace_id, regime_key=regime_key,
        )
        default_profile_used = profile_row is None
        profile = profile_row if profile_row is not None else dict(_DEFAULT_PROFILE, regime_key=regime_key)
        profile_id = profile_row["id"] if profile_row is not None else None
        profile_name = profile.get("profile_name", _DEFAULT_PROFILE["profile_name"])

        # Map family → majority dependency_type across its symbols (for family-
        # level type weight). Falls back to None when not resolvable.
        edge_type_by_family: dict[str, str | None] = {}
        family_type_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for s in weighted_symbols:
            fam = str(s["dependency_family"])
            t   = s.get("dependency_type")
            if t:
                family_type_counts[fam][t] += 1
        for fam, counts in family_type_counts.items():
            if counts:
                edge_type_by_family[fam] = max(counts.items(), key=lambda kv: kv[1])[0]

        family_rows = self.compute_regime_adjusted_family_attribution(
            weighted_rows,
            profile=profile,
            regime_key=regime_key,
            interpretation_state=interpretation_state,
            edge_type_by_family=edge_type_by_family,
        )
        family_rows = self.rank_regime_families(family_rows)

        symbol_rows = self.compute_regime_adjusted_symbol_attribution(
            weighted_symbols, profile=profile,
        )
        symbol_rows = self.rank_regime_symbols(symbol_rows)

        context_snapshot_id = self._load_context_snapshot_id(conn, run_id=run_id)

        for fa in family_rows:
            fa.metadata.update({
                "scoring_version":              _SCORING_VERSION,
                "default_profile_used":         default_profile_used,
                "profile_name":                 profile_name,
                "dominance_threshold":          _as_float(profile.get("dominance_threshold")) or 0.05,
            })
        for sa in symbol_rows:
            sa.metadata.update({
                "scoring_version":              _SCORING_VERSION,
                "default_profile_used":         default_profile_used,
                "profile_name":                 profile_name,
                "regime_key":                   regime_key,
            })

        return RegimeInterpretationResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snapshot_id,
            regime_key=regime_key,
            interpretation_profile_id=profile_id,
            profile_name=profile_name,
            default_profile_used=default_profile_used,
            interpretation_state=interpretation_state,
            family_rows=family_rows,
            symbol_rows=symbol_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_regime_attribution(
        self, conn, *, result: RegimeInterpretationResult,
    ) -> dict[str, int]:
        import src.db.repositories as repo
        fam_count = repo.insert_cross_asset_family_regime_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            regime_key=result.regime_key,
            interpretation_profile_id=result.interpretation_profile_id,
            rows=[
                {
                    "dependency_family":                    fa.dependency_family,
                    "raw_family_net_contribution":          fa.raw_family_net_contribution,
                    "weighted_family_net_contribution":     fa.weighted_family_net_contribution,
                    "regime_family_weight":                 fa.regime_family_weight,
                    "regime_type_weight":                   fa.regime_type_weight,
                    "regime_confirmation_scale":            fa.regime_confirmation_scale,
                    "regime_contradiction_scale":           fa.regime_contradiction_scale,
                    "regime_missing_penalty_scale":         fa.regime_missing_penalty_scale,
                    "regime_stale_penalty_scale":           fa.regime_stale_penalty_scale,
                    "regime_adjusted_family_contribution":  fa.regime_adjusted_family_contribution,
                    "regime_family_rank":                   fa.regime_family_rank,
                    "interpretation_state":                 fa.interpretation_state,
                    "top_symbols":                          fa.top_symbols,
                    "metadata":                             fa.metadata,
                }
                for fa in result.family_rows
            ],
        )
        sym_count = repo.insert_cross_asset_symbol_regime_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            regime_key=result.regime_key,
            interpretation_profile_id=result.interpretation_profile_id,
            rows=[
                {
                    "symbol":                       sa.symbol,
                    "dependency_family":            sa.dependency_family,
                    "dependency_type":              sa.dependency_type,
                    "graph_priority":               sa.graph_priority,
                    "is_direct_dependency":         sa.is_direct_dependency,
                    "raw_symbol_score":             sa.raw_symbol_score,
                    "weighted_symbol_score":        sa.weighted_symbol_score,
                    "regime_family_weight":         sa.regime_family_weight,
                    "regime_type_weight":           sa.regime_type_weight,
                    "regime_adjusted_symbol_score": sa.regime_adjusted_symbol_score,
                    "symbol_rank":                  sa.symbol_rank,
                    "metadata":                     sa.metadata,
                }
                for sa in result.symbol_rows
            ],
        )
        return {"family_rows": fam_count, "symbol_rows": sym_count}

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> RegimeInterpretationResult | None:
        result = self.build_regime_interpretation_for_run(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
        )
        if result is None:
            return None
        self.persist_regime_attribution(conn, result=result)
        return result

    def refresh_workspace_regime_interpretation(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[RegimeInterpretationResult]:
        """Emit regime-aware attribution for every watchlist with 4.1B weighted
        attribution for the given run. Commits per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[RegimeInterpretationResult] = []
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
                    "regime_aware_cross_asset: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
