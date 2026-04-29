"""Phase 4.2B: Family-Level Lead/Lag Attribution Service.

Conditions family and symbol attribution on timing class from 4.2A. For each
family, picks the most informed base contribution (regime-adjusted → weighted
→ raw) and applies a timing-class multiplier plus explicit bonus (lead) or
penalty (lag) from a profile. Symbol-level uses the pair-level lag bucket.

All adjustments are deterministic; multipliers are clipped to a conservative
band [0.75, 1.15] so timing cannot dominate upstream contribution.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_TIMING_MULTIPLIER_MIN = 0.75
_TIMING_MULTIPLIER_MAX = 1.15
_BONUS_PENALTY_BASE    = 0.05  # base magnitude; scaled by profile {lead_bonus_scale,lag_penalty_scale}
_SCORING_VERSION       = "4.2B.v1"

# Timing-class preference for dominant-family tie-break: lead > coincident > lag > insufficient_data
_TIMING_CLASS_PREFERENCE: dict[str, int] = {
    "lead":              4,
    "coincident":        3,
    "lag":               2,
    "insufficient_data": 1,
}

# Structural priority tie-break (matches 4.1B/4.1C).
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
    "profile_name":             "default_timing",
    "lead_weight":              1.10,
    "coincident_weight":        1.00,
    "lag_weight":               0.85,
    "insufficient_data_weight": 0.75,
    "lead_bonus_scale":         1.0,
    "lag_penalty_scale":        1.0,
    "family_weight_overrides":  {},
}


@dataclass
class TimingFamilyAttribution:
    dependency_family: str
    raw_family_net_contribution: float | None
    weighted_family_net_contribution: float | None
    regime_adjusted_family_contribution: float | None
    dominant_timing_class: str
    lead_pair_count: int
    coincident_pair_count: int
    lag_pair_count: int
    timing_class_weight: float
    timing_bonus: float
    timing_penalty: float
    timing_adjusted_family_contribution: float | None
    timing_family_rank: int | None = None
    top_leading_symbols: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TimingSymbolAttribution:
    symbol: str
    dependency_family: str
    dependency_type: str | None
    lag_bucket: str
    best_lag_hours: int | None
    raw_symbol_score: float | None
    weighted_symbol_score: float | None
    regime_adjusted_symbol_score: float | None
    timing_class_weight: float
    timing_adjusted_symbol_score: float | None
    symbol_rank: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TimingAttributionResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    timing_profile_id: str | None
    profile_name: str
    default_profile_used: bool
    family_rows: list[TimingFamilyAttribution]
    symbol_rows: list[TimingSymbolAttribution]


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


class CrossAssetTimingAttributionService:
    """Deterministic timing-class refinement of 4.1A/B/C family + symbol
    contributions."""

    # ── profile loading ─────────────────────────────────────────────────
    def get_active_timing_profile(
        self, conn, *, workspace_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, profile_name, is_active,
                       lead_weight, coincident_weight, lag_weight,
                       insufficient_data_weight,
                       lead_bonus_scale, lag_penalty_scale,
                       family_weight_overrides, metadata, created_at
                from public.cross_asset_timing_attribution_profiles
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
    def compute_timing_class_weight(
        self, *, timing_class: str, profile: dict[str, Any],
        dependency_family: str | None = None,
    ) -> float:
        # Per-class base weight
        key_map = {
            "lead":              "lead_weight",
            "coincident":        "coincident_weight",
            "lag":               "lag_weight",
            "insufficient_data": "insufficient_data_weight",
        }
        base = _as_float(profile.get(key_map.get(timing_class, "coincident_weight")))
        base = base if base is not None else 1.0

        # Per-family override (applied multiplicatively on top of class weight)
        overrides = _parse_overrides(profile.get("family_weight_overrides"))
        fam_override = _as_float(overrides.get(dependency_family)) if dependency_family else None
        combined = base * (fam_override if fam_override is not None else 1.0)
        return _clip(combined, _TIMING_MULTIPLIER_MIN, _TIMING_MULTIPLIER_MAX)

    def compute_timing_bonus(
        self, *, timing_class: str, base_contribution: float | None,
        profile: dict[str, Any],
    ) -> float:
        if timing_class != "lead" or base_contribution is None:
            return 0.0
        scale = _as_float(profile.get("lead_bonus_scale")) or 1.0
        # Bonus is a small additive term proportional to base magnitude —
        # preserves sign of base_contribution.
        sign = 1.0 if base_contribution >= 0 else -1.0
        return sign * abs(base_contribution) * _BONUS_PENALTY_BASE * max(0.0, scale)

    def compute_timing_penalty(
        self, *, timing_class: str, base_contribution: float | None,
        profile: dict[str, Any],
    ) -> float:
        """Returns a non-negative magnitude to SUBTRACT from the adjusted
        contribution. Sign-aware so subtracting it moves magnitude toward
        zero rather than flipping direction."""
        if timing_class != "lag" or base_contribution is None:
            return 0.0
        scale = _as_float(profile.get("lag_penalty_scale")) or 1.0
        # Penalty always reduces magnitude — sign-aware
        sign = 1.0 if base_contribution >= 0 else -1.0
        return sign * abs(base_contribution) * _BONUS_PENALTY_BASE * max(0.0, scale)

    # ── input loading ───────────────────────────────────────────────────
    def _load_family_timing(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       lead_pair_count, coincident_pair_count, lag_pair_count,
                       avg_best_lag_hours, avg_timing_strength,
                       dominant_timing_class, top_leading_symbols
                from public.cross_asset_family_timing_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_family_base_contributions(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        """Returns per-family: {raw, weighted, regime, top_symbols, dependency_type}."""
        out: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "raw": None, "weighted": None, "regime": None,
            "top_symbols": [], "dependency_type": None,
        })
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family, family_net_contribution, top_symbols
                from public.cross_asset_family_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                out[fam]["raw"] = _as_float(d.get("family_net_contribution"))
                top = d.get("top_symbols") or []
                if isinstance(top, str):
                    import json
                    try:
                        top = json.loads(top)
                    except json.JSONDecodeError:
                        top = []
                out[fam]["top_symbols"] = [str(s) for s in top]

            cur.execute(
                """
                select dependency_family, weighted_family_net_contribution
                from public.cross_asset_family_weighted_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                out[fam]["weighted"] = _as_float(d.get("weighted_family_net_contribution"))

            cur.execute(
                """
                select dependency_family, regime_adjusted_family_contribution
                from public.cross_asset_family_regime_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                out[fam]["regime"] = _as_float(d.get("regime_adjusted_family_contribution"))
        return out

    def _load_pair_timing(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select base_symbol, dependency_symbol,
                       dependency_family, dependency_type,
                       lag_bucket, best_lag_hours,
                       timing_strength, correlation_at_best_lag
                from public.cross_asset_lead_lag_pair_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_symbol_base_scores(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        """Returns per-symbol: {raw, weighted, regime, dependency_family, dependency_type}."""
        out: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "raw": None, "weighted": None, "regime": None,
            "dependency_family": None, "dependency_type": None,
        })
        with conn.cursor() as cur:
            cur.execute(
                """
                select symbol, dependency_family, dependency_type,
                       raw_symbol_score, weighted_symbol_score
                from public.cross_asset_symbol_weighted_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                sym = str(d["symbol"])
                out[sym]["raw"]               = _as_float(d.get("raw_symbol_score"))
                out[sym]["weighted"]          = _as_float(d.get("weighted_symbol_score"))
                out[sym]["dependency_family"] = d.get("dependency_family")
                out[sym]["dependency_type"]   = d.get("dependency_type")

            cur.execute(
                """
                select symbol, regime_adjusted_symbol_score
                from public.cross_asset_symbol_regime_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                sym = str(d["symbol"])
                out[sym]["regime"] = _as_float(d.get("regime_adjusted_symbol_score"))
        return out

    # ── family + symbol builders ────────────────────────────────────────
    def compute_timing_adjusted_family_attribution(
        self,
        family_timing_rows: Sequence[dict[str, Any]],
        base_contribs: dict[str, dict[str, Any]],
        *,
        profile: dict[str, Any],
    ) -> list[TimingFamilyAttribution]:
        items: list[TimingFamilyAttribution] = []
        seen_families: set[str] = set()

        for ft in family_timing_rows:
            fam = str(ft["dependency_family"])
            seen_families.add(fam)
            dominant = str(ft.get("dominant_timing_class") or "insufficient_data")
            lead_n  = int(ft.get("lead_pair_count") or 0)
            coinc_n = int(ft.get("coincident_pair_count") or 0)
            lag_n   = int(ft.get("lag_pair_count") or 0)

            top_leading = ft.get("top_leading_symbols") or []
            if isinstance(top_leading, str):
                import json
                try:
                    top_leading = json.loads(top_leading)
                except json.JSONDecodeError:
                    top_leading = []
            top_leading = [str(s) for s in top_leading]

            base_info = base_contribs.get(fam, {})
            raw      = _as_float(base_info.get("raw"))
            weighted = _as_float(base_info.get("weighted"))
            regime   = _as_float(base_info.get("regime"))
            base_contrib = regime if regime is not None else (weighted if weighted is not None else raw)

            timing_w = self.compute_timing_class_weight(
                timing_class=dominant, profile=profile, dependency_family=fam,
            )
            bonus   = self.compute_timing_bonus(
                timing_class=dominant, base_contribution=base_contrib, profile=profile,
            )
            penalty = self.compute_timing_penalty(
                timing_class=dominant, base_contribution=base_contrib, profile=profile,
            )

            timing_adjusted: float | None = None
            if base_contrib is not None:
                timing_adjusted = base_contrib * timing_w + bonus - penalty

            items.append(TimingFamilyAttribution(
                dependency_family=fam,
                raw_family_net_contribution=raw,
                weighted_family_net_contribution=weighted,
                regime_adjusted_family_contribution=regime,
                dominant_timing_class=dominant,
                lead_pair_count=lead_n,
                coincident_pair_count=coinc_n,
                lag_pair_count=lag_n,
                timing_class_weight=timing_w,
                timing_bonus=bonus,
                timing_penalty=penalty,
                timing_adjusted_family_contribution=timing_adjusted,
                top_leading_symbols=top_leading,
                metadata={
                    "scoring_version":        _SCORING_VERSION,
                    "base_contribution_source": (
                        "regime" if regime is not None
                        else ("weighted" if weighted is not None else "raw")
                    ),
                    "base_contribution":     base_contrib,
                },
            ))

        # Families with attribution but no timing row → apply
        # insufficient_data weight / no bonus.
        for fam, info in base_contribs.items():
            if fam in seen_families:
                continue
            raw = _as_float(info.get("raw"))
            weighted = _as_float(info.get("weighted"))
            regime = _as_float(info.get("regime"))
            base_contrib = regime if regime is not None else (weighted if weighted is not None else raw)
            if base_contrib is None:
                continue
            timing_w = self.compute_timing_class_weight(
                timing_class="insufficient_data", profile=profile, dependency_family=fam,
            )
            items.append(TimingFamilyAttribution(
                dependency_family=fam,
                raw_family_net_contribution=raw,
                weighted_family_net_contribution=weighted,
                regime_adjusted_family_contribution=regime,
                dominant_timing_class="insufficient_data",
                lead_pair_count=0,
                coincident_pair_count=0,
                lag_pair_count=0,
                timing_class_weight=timing_w,
                timing_bonus=0.0,
                timing_penalty=0.0,
                timing_adjusted_family_contribution=base_contrib * timing_w,
                top_leading_symbols=[],
                metadata={
                    "scoring_version":        _SCORING_VERSION,
                    "base_contribution_source": (
                        "regime" if regime is not None
                        else ("weighted" if weighted is not None else "raw")
                    ),
                    "base_contribution":     base_contrib,
                    "no_timing_row":         True,
                },
            ))
        return items

    def rank_timing_families(
        self, items: list[TimingFamilyAttribution],
    ) -> list[TimingFamilyAttribution]:
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.timing_adjusted_family_contribution or 0.0),
                -abs(fa.timing_adjusted_family_contribution or 0.0),
                -_TIMING_CLASS_PREFERENCE.get(fa.dominant_timing_class, 0),
                -_FAMILY_PRIORITY.get(fa.dependency_family, 0),
                fa.dependency_family,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.timing_family_rank = i
        return ranked

    def compute_timing_adjusted_symbol_attribution(
        self,
        pair_rows: Sequence[dict[str, Any]],
        symbol_scores: dict[str, dict[str, Any]],
        *,
        profile: dict[str, Any],
    ) -> list[TimingSymbolAttribution]:
        # A symbol may appear in multiple pair rows (one per base_symbol).
        # Collapse to one row per dependency_symbol using the strongest pair
        # (prefer lead > coincident > lag > insufficient_data, then highest
        # |timing_strength|).
        best_pair_by_symbol: dict[str, dict[str, Any]] = {}
        for p in pair_rows:
            sym = str(p["dependency_symbol"])
            current = best_pair_by_symbol.get(sym)
            bucket = p.get("lag_bucket") or "insufficient_data"
            strength = _as_float(p.get("timing_strength")) or 0.0
            if current is None:
                best_pair_by_symbol[sym] = p
                continue
            cur_bucket   = current.get("lag_bucket") or "insufficient_data"
            cur_strength = _as_float(current.get("timing_strength")) or 0.0
            if (_TIMING_CLASS_PREFERENCE.get(bucket, 0),     abs(strength)) \
             > (_TIMING_CLASS_PREFERENCE.get(cur_bucket, 0), abs(cur_strength)):
                best_pair_by_symbol[sym] = p

        rows: list[TimingSymbolAttribution] = []
        for sym, pair in best_pair_by_symbol.items():
            bucket       = str(pair.get("lag_bucket") or "insufficient_data")
            family       = str(pair.get("dependency_family") or "unknown")
            dep_type     = pair.get("dependency_type")
            best_lag     = pair.get("best_lag_hours")
            scores = symbol_scores.get(sym, {})
            raw      = _as_float(scores.get("raw"))
            weighted = _as_float(scores.get("weighted"))
            regime   = _as_float(scores.get("regime"))
            base_score = regime if regime is not None else (weighted if weighted is not None else raw)

            timing_w = self.compute_timing_class_weight(
                timing_class=bucket, profile=profile, dependency_family=family,
            )
            timing_adjusted: float | None = None
            if base_score is not None:
                bonus = self.compute_timing_bonus(
                    timing_class=bucket, base_contribution=base_score, profile=profile,
                )
                penalty = self.compute_timing_penalty(
                    timing_class=bucket, base_contribution=base_score, profile=profile,
                )
                timing_adjusted = base_score * timing_w + bonus - penalty

            rows.append(TimingSymbolAttribution(
                symbol=sym,
                dependency_family=family,
                dependency_type=dep_type,
                lag_bucket=bucket,
                best_lag_hours=int(best_lag) if best_lag is not None else None,
                raw_symbol_score=raw,
                weighted_symbol_score=weighted,
                regime_adjusted_symbol_score=regime,
                timing_class_weight=timing_w,
                timing_adjusted_symbol_score=timing_adjusted,
                metadata={
                    "scoring_version":          _SCORING_VERSION,
                    "base_score_source": (
                        "regime" if regime is not None
                        else ("weighted" if weighted is not None else "raw")
                    ),
                    "base_score":              base_score,
                    "timing_strength":         _as_float(pair.get("timing_strength")),
                    "correlation_at_best_lag": _as_float(pair.get("correlation_at_best_lag")),
                },
            ))
        return rows

    def rank_timing_symbols(
        self, rows: list[TimingSymbolAttribution],
    ) -> list[TimingSymbolAttribution]:
        ranked = sorted(
            rows,
            key=lambda r: (
                -(r.timing_adjusted_symbol_score or 0.0),
                -abs(r.timing_adjusted_symbol_score or 0.0),
                -_TIMING_CLASS_PREFERENCE.get(r.lag_bucket, 0),
                r.symbol,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.symbol_rank = i
        return ranked

    # ── orchestration ───────────────────────────────────────────────────
    def build_timing_attribution_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> TimingAttributionResult | None:
        family_timing = self._load_family_timing(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        base_contribs = self._load_family_base_contributions(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not family_timing and not base_contribs:
            return None

        profile_row = self.get_active_timing_profile(conn, workspace_id=workspace_id)
        default_profile_used = profile_row is None
        profile = profile_row if profile_row is not None else dict(_DEFAULT_PROFILE)
        profile_id = profile_row["id"] if profile_row is not None else None
        profile_name = profile.get("profile_name", _DEFAULT_PROFILE["profile_name"])

        family_rows = self.compute_timing_adjusted_family_attribution(
            family_timing, base_contribs, profile=profile,
        )
        family_rows = self.rank_timing_families(family_rows)

        pair_rows = self._load_pair_timing(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        symbol_scores = self._load_symbol_base_scores(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        symbol_rows = self.compute_timing_adjusted_symbol_attribution(
            pair_rows, symbol_scores, profile=profile,
        )
        symbol_rows = self.rank_timing_symbols(symbol_rows)

        # context_snapshot_id from upstream attribution
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

        for fa in family_rows:
            fa.metadata.update({
                "profile_name":         profile_name,
                "default_profile_used": default_profile_used,
            })
        for sa in symbol_rows:
            sa.metadata.update({
                "profile_name":         profile_name,
                "default_profile_used": default_profile_used,
            })

        return TimingAttributionResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snapshot_id,
            timing_profile_id=profile_id,
            profile_name=profile_name,
            default_profile_used=default_profile_used,
            family_rows=family_rows,
            symbol_rows=symbol_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_timing_attribution(
        self, conn, *, result: TimingAttributionResult,
    ) -> dict[str, int]:
        import src.db.repositories as repo
        fam_count = repo.insert_cross_asset_family_timing_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            timing_profile_id=result.timing_profile_id,
            rows=[
                {
                    "dependency_family":                    fa.dependency_family,
                    "raw_family_net_contribution":          fa.raw_family_net_contribution,
                    "weighted_family_net_contribution":     fa.weighted_family_net_contribution,
                    "regime_adjusted_family_contribution":  fa.regime_adjusted_family_contribution,
                    "dominant_timing_class":                fa.dominant_timing_class,
                    "lead_pair_count":                      fa.lead_pair_count,
                    "coincident_pair_count":                fa.coincident_pair_count,
                    "lag_pair_count":                       fa.lag_pair_count,
                    "timing_class_weight":                  fa.timing_class_weight,
                    "timing_bonus":                         fa.timing_bonus,
                    "timing_penalty":                       fa.timing_penalty,
                    "timing_adjusted_family_contribution":  fa.timing_adjusted_family_contribution,
                    "timing_family_rank":                   fa.timing_family_rank,
                    "top_leading_symbols":                  fa.top_leading_symbols,
                    "metadata":                             fa.metadata,
                }
                for fa in result.family_rows
            ],
        )
        sym_count = repo.insert_cross_asset_symbol_timing_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            timing_profile_id=result.timing_profile_id,
            rows=[
                {
                    "symbol":                       sa.symbol,
                    "dependency_family":            sa.dependency_family,
                    "dependency_type":              sa.dependency_type,
                    "lag_bucket":                   sa.lag_bucket,
                    "best_lag_hours":               sa.best_lag_hours,
                    "raw_symbol_score":             sa.raw_symbol_score,
                    "weighted_symbol_score":        sa.weighted_symbol_score,
                    "regime_adjusted_symbol_score": sa.regime_adjusted_symbol_score,
                    "timing_class_weight":          sa.timing_class_weight,
                    "timing_adjusted_symbol_score": sa.timing_adjusted_symbol_score,
                    "symbol_rank":                  sa.symbol_rank,
                    "metadata":                     sa.metadata,
                }
                for sa in result.symbol_rows
            ],
        )
        return {"family_rows": fam_count, "symbol_rows": sym_count}

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> TimingAttributionResult | None:
        result = self.build_timing_attribution_for_run(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if result is None:
            return None
        self.persist_timing_attribution(conn, result=result)
        return result

    def refresh_workspace_timing_attribution(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[TimingAttributionResult]:
        """Emit timing-aware attribution for every watchlist. Commits per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[TimingAttributionResult] = []
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
                    "cross_asset_timing_attribution: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
