"""Phase 4.5B: Cluster-Aware Attribution Service.

Reads 4.5A pattern-cluster diagnostics and the most mature upstream family
contribution (archetype → transition → timing → regime → weighted → raw
fallback), then applies cluster-state weights + drift/entropy-aware
penalties + state-specific bonuses (rotating with low drift, recovering).
Persists:

  * one cross_asset_family_cluster_attribution_snapshots row per family
  * one cross_asset_symbol_cluster_attribution_snapshots row per symbol

All adjustments are deterministic; cluster weight clipped to a conservative
band [0.75, 1.20] so cluster evidence cannot dominate the upstream chain.
Cluster-aware attribution is a refinement layer, not a replacement.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_WEIGHT_MIN            = 0.75
_WEIGHT_MAX            = 1.20
_BONUS_PENALTY_BASE    = 0.05
_DRIFT_PENALTY_THRESHOLD   = 0.20  # below this, drift contributes nothing
_ENTROPY_PENALTY_THRESHOLD = 0.70  # below this, entropy contributes nothing
_ROTATION_DRIFT_CEILING    = 0.50  # rotation bonus suppressed above this
_SCORING_VERSION       = "4.5B.v1"

_CLUSTER_PREFERENCE: dict[str, int] = {
    "stable":               5,
    "recovering":           4,
    "rotating":             3,
    "mixed":                2,
    "deteriorating":        2,
    "insufficient_history": 1,
}

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
    "profile_name":                  "default_cluster_attribution",
    "stable_weight":                 1.08,
    "rotating_weight":               1.02,
    "recovering_weight":             1.04,
    "deteriorating_weight":          0.82,
    "mixed_weight":                  0.90,
    "insufficient_history_weight":   0.80,
    "drift_penalty_scale":           1.0,
    "rotation_bonus_scale":          1.0,
    "recovery_bonus_scale":          1.0,
    "entropy_penalty_scale":         1.0,
    "cluster_family_overrides":      {},
}


@dataclass
class ClusterFamilyAttribution:
    dependency_family: str
    raw_family_net_contribution: float | None
    weighted_family_net_contribution: float | None
    regime_adjusted_family_contribution: float | None
    timing_adjusted_family_contribution: float | None
    transition_adjusted_family_contribution: float | None
    archetype_adjusted_family_contribution: float | None
    cluster_state: str
    dominant_archetype_key: str
    drift_score: float | None
    pattern_entropy: float | None
    cluster_weight: float
    cluster_bonus: float
    cluster_penalty: float
    cluster_adjusted_family_contribution: float | None
    cluster_family_rank: int | None = None
    top_symbols: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ClusterSymbolAttribution:
    symbol: str
    dependency_family: str
    dependency_type: str | None
    cluster_state: str
    dominant_archetype_key: str
    raw_symbol_score: float | None
    weighted_symbol_score: float | None
    regime_adjusted_symbol_score: float | None
    timing_adjusted_symbol_score: float | None
    transition_adjusted_symbol_score: float | None
    archetype_adjusted_symbol_score: float | None
    cluster_weight: float
    cluster_adjusted_symbol_score: float | None
    symbol_rank: int | None = None
    reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ClusterAttributionResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    cluster_profile_id: str | None
    profile_name: str
    default_profile_used: bool
    cluster_state: str
    dominant_archetype_key: str
    drift_score: float | None
    pattern_entropy: float | None
    family_rows: list[ClusterFamilyAttribution]
    symbol_rows: list[ClusterSymbolAttribution]


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


class CrossAssetClusterAttributionService:
    """Deterministic cluster-aware refinement of 4.1A/B/C + 4.2B + 4.3B + 4.4B
    family and symbol contributions."""

    # ── profile loading ─────────────────────────────────────────────────
    def get_active_cluster_profile(
        self, conn, *, workspace_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, profile_name, is_active,
                       stable_weight, rotating_weight, recovering_weight,
                       deteriorating_weight, mixed_weight, insufficient_history_weight,
                       drift_penalty_scale, rotation_bonus_scale,
                       recovery_bonus_scale, entropy_penalty_scale,
                       cluster_family_overrides, metadata, created_at
                from public.cross_asset_cluster_attribution_profiles
                where workspace_id = %s::uuid and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ── weight primitives ───────────────────────────────────────────────
    def compute_cluster_weight(
        self, *, cluster_state: str, dependency_family: str | None,
        profile: dict[str, Any],
    ) -> float:
        key_map = {
            "stable":               "stable_weight",
            "rotating":             "rotating_weight",
            "recovering":           "recovering_weight",
            "deteriorating":        "deteriorating_weight",
            "mixed":                "mixed_weight",
            "insufficient_history": "insufficient_history_weight",
        }
        base = _as_float(profile.get(key_map.get(cluster_state, "insufficient_history_weight")))
        base = base if base is not None else 1.0

        # Per-family override multiplies on top (bounded later).
        overrides = _parse_overrides(profile.get("cluster_family_overrides"))
        fam_override = None
        if dependency_family:
            fam_override = _as_float(overrides.get(dependency_family))
            if fam_override is None:
                nested = overrides.get(dependency_family) if isinstance(overrides.get(dependency_family), dict) else None
                if isinstance(nested, dict):
                    fam_override = _as_float(nested.get(cluster_state))

        combined = base * (fam_override if fam_override is not None else 1.0)
        return _clip(combined, _WEIGHT_MIN, _WEIGHT_MAX)

    def compute_cluster_bonus(
        self, *, cluster_state: str, base_contribution: float | None,
        drift_score: float | None, profile: dict[str, Any],
    ) -> float:
        if base_contribution is None:
            return 0.0
        sign = 1.0 if base_contribution >= 0 else -1.0
        magnitude = abs(base_contribution) * _BONUS_PENALTY_BASE
        if cluster_state == "recovering":
            scale = _as_float(profile.get("recovery_bonus_scale")) or 1.0
            return sign * magnitude * max(0.0, scale) * 0.4  # ~2% of base
        if cluster_state == "rotating":
            # Rotation bonus only when drift is not excessive.
            d = drift_score or 0.0
            if d >= _ROTATION_DRIFT_CEILING:
                return 0.0
            scale = _as_float(profile.get("rotation_bonus_scale")) or 1.0
            return sign * magnitude * max(0.0, scale) * 0.4  # ~2% of base
        return 0.0

    def compute_cluster_penalty(
        self,
        *,
        cluster_state: str,
        base_contribution: float | None,
        drift_score: float | None,
        pattern_entropy: float | None,
        profile: dict[str, Any],
    ) -> float:
        """Sign-aware penalty (subtracts toward zero). Combines cluster-state
        baseline penalty + drift penalty + entropy penalty."""
        if base_contribution is None:
            return 0.0
        sign = 1.0 if base_contribution >= 0 else -1.0
        magnitude = abs(base_contribution) * _BONUS_PENALTY_BASE

        state_pen = 0.0
        if cluster_state == "deteriorating":
            state_pen = 0.6  # ~3% of base
        elif cluster_state == "mixed":
            state_pen = 0.3  # ~1.5% of base

        # Drift penalty: scales linearly above _DRIFT_PENALTY_THRESHOLD.
        drift_pen = 0.0
        d = drift_score or 0.0
        if d > _DRIFT_PENALTY_THRESHOLD:
            scale = _as_float(profile.get("drift_penalty_scale")) or 1.0
            # 0 at threshold, ramping to 0.4 at drift=1.0.
            drift_pen = (d - _DRIFT_PENALTY_THRESHOLD) / (1.0 - _DRIFT_PENALTY_THRESHOLD)
            drift_pen = _clip(drift_pen, 0.0, 1.0) * 0.4 * max(0.0, scale)

        # Entropy penalty: scales linearly above _ENTROPY_PENALTY_THRESHOLD.
        entropy_pen = 0.0
        e = pattern_entropy or 0.0
        if e > _ENTROPY_PENALTY_THRESHOLD:
            scale = _as_float(profile.get("entropy_penalty_scale")) or 1.0
            entropy_pen = (e - _ENTROPY_PENALTY_THRESHOLD) / (1.0 - _ENTROPY_PENALTY_THRESHOLD)
            entropy_pen = _clip(entropy_pen, 0.0, 1.0) * 0.3 * max(0.0, scale)

        total = state_pen + drift_pen + entropy_pen
        return sign * magnitude * total

    # ── input loading ───────────────────────────────────────────────────
    def _load_run_cluster_context(
        self, conn, *, run_id: str,
    ) -> dict[str, Any]:
        """Pull cluster-state and run-level metrics from the 4.5A run pattern
        cluster summary."""
        ctx: dict[str, Any] = {
            "cluster_state":            "insufficient_history",
            "dominant_archetype_key":   "insufficient_history",
            "drift_score":              None,
            "pattern_entropy":          None,
            "regime_key":               None,
            "context_snapshot_id":      None,
        }
        with conn.cursor() as cur:
            cur.execute(
                """
                select cluster_state, dominant_archetype_key,
                       drift_score, pattern_entropy, regime_key
                from public.run_cross_asset_pattern_cluster_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                d = dict(r)
                ctx["cluster_state"]          = d.get("cluster_state") or "insufficient_history"
                ctx["dominant_archetype_key"] = d.get("dominant_archetype_key") or "insufficient_history"
                ctx["drift_score"]            = _as_float(d.get("drift_score"))
                ctx["pattern_entropy"]        = _as_float(d.get("pattern_entropy"))
                ctx["regime_key"]             = d.get("regime_key")

            cur.execute(
                """
                select context_snapshot_id::text as context_snapshot_id
                from public.cross_asset_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                ctx["context_snapshot_id"] = dict(r).get("context_snapshot_id")
        return ctx

    def _load_family_base_contributions(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "raw": None, "weighted": None, "regime": None, "timing": None,
            "transition": None, "archetype": None, "top_symbols": [],
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

            cur.execute(
                """
                select dependency_family, timing_adjusted_family_contribution
                from public.cross_asset_family_timing_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                out[fam]["timing"] = _as_float(d.get("timing_adjusted_family_contribution"))

            cur.execute(
                """
                select dependency_family, transition_adjusted_family_contribution,
                       top_symbols
                from public.cross_asset_family_transition_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                out[fam]["transition"] = _as_float(d.get("transition_adjusted_family_contribution"))
                top = d.get("top_symbols") or []
                if isinstance(top, str):
                    import json
                    try:
                        top = json.loads(top)
                    except json.JSONDecodeError:
                        top = []
                if top:
                    out[fam]["top_symbols"] = [str(s) for s in top]

            cur.execute(
                """
                select dependency_family, archetype_adjusted_family_contribution,
                       top_symbols
                from public.cross_asset_family_archetype_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                out[fam]["archetype"] = _as_float(d.get("archetype_adjusted_family_contribution"))
                top = d.get("top_symbols") or []
                if isinstance(top, str):
                    import json
                    try:
                        top = json.loads(top)
                    except json.JSONDecodeError:
                        top = []
                if top:
                    out[fam]["top_symbols"] = [str(s) for s in top]
        return out

    def _load_symbol_base_scores(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "raw": None, "weighted": None, "regime": None, "timing": None,
            "transition": None, "archetype": None,
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

            cur.execute(
                """
                select symbol, timing_adjusted_symbol_score
                from public.cross_asset_symbol_timing_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                sym = str(d["symbol"])
                out[sym]["timing"] = _as_float(d.get("timing_adjusted_symbol_score"))

            cur.execute(
                """
                select symbol, transition_adjusted_symbol_score
                from public.cross_asset_symbol_transition_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                sym = str(d["symbol"])
                out[sym]["transition"] = _as_float(d.get("transition_adjusted_symbol_score"))

            cur.execute(
                """
                select symbol, archetype_adjusted_symbol_score
                from public.cross_asset_symbol_archetype_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                sym = str(d["symbol"])
                out[sym]["archetype"] = _as_float(d.get("archetype_adjusted_symbol_score"))
        return out

    @staticmethod
    def _pick_base_contribution(contribs: dict[str, Any]) -> tuple[float | None, str]:
        """Fallback chain: archetype → transition → timing → regime → weighted → raw."""
        for key in ("archetype", "transition", "timing", "regime", "weighted", "raw"):
            v = contribs.get(key)
            if v is not None:
                return (v, key)
        return (None, "none")

    @staticmethod
    def _build_reason_codes(
        *, cluster_state: str, drift_score: float | None,
        pattern_entropy: float | None, dominant_archetype_key: str,
        rotation_drift_suppressed: bool,
    ) -> list[str]:
        codes = [
            f"cluster_state:{cluster_state}",
            f"dominant_archetype:{dominant_archetype_key}",
        ]
        if drift_score is not None and drift_score > _DRIFT_PENALTY_THRESHOLD:
            codes.append("drift_penalty_applied")
        if pattern_entropy is not None and pattern_entropy > _ENTROPY_PENALTY_THRESHOLD:
            codes.append("entropy_penalty_applied")
        if cluster_state == "deteriorating":
            codes.append("deteriorating_state_penalty")
        if cluster_state == "mixed":
            codes.append("mixed_state_suppression")
        if cluster_state == "rotating" and rotation_drift_suppressed:
            codes.append("rotation_bonus_suppressed_high_drift")
        if cluster_state == "rotating" and not rotation_drift_suppressed:
            codes.append("rotation_bonus_applied")
        if cluster_state == "recovering":
            codes.append("recovery_bonus_applied")
        if cluster_state == "insufficient_history":
            codes.append("insufficient_history_suppression")
        return codes

    # ── family + symbol builders ────────────────────────────────────────
    def compute_cluster_adjusted_family_attribution(
        self,
        base_contribs: dict[str, dict[str, Any]],
        *,
        cluster_state: str,
        dominant_archetype_key: str,
        drift_score: float | None,
        pattern_entropy: float | None,
        profile: dict[str, Any],
    ) -> list[ClusterFamilyAttribution]:
        items: list[ClusterFamilyAttribution] = []

        rotation_drift_suppressed = (
            cluster_state == "rotating"
            and (drift_score or 0.0) >= _ROTATION_DRIFT_CEILING
        )

        for fam, info in sorted(base_contribs.items()):
            base_val, base_source = self._pick_base_contribution(info)
            weight  = self.compute_cluster_weight(
                cluster_state=cluster_state, dependency_family=fam, profile=profile,
            )
            bonus   = self.compute_cluster_bonus(
                cluster_state=cluster_state, base_contribution=base_val,
                drift_score=drift_score, profile=profile,
            )
            penalty = self.compute_cluster_penalty(
                cluster_state=cluster_state, base_contribution=base_val,
                drift_score=drift_score, pattern_entropy=pattern_entropy,
                profile=profile,
            )

            if base_val is None:
                adjusted: float | None = None
            else:
                adjusted = base_val * weight + bonus - penalty

            reason_codes = self._build_reason_codes(
                cluster_state=cluster_state, drift_score=drift_score,
                pattern_entropy=pattern_entropy,
                dominant_archetype_key=dominant_archetype_key,
                rotation_drift_suppressed=rotation_drift_suppressed,
            )

            items.append(ClusterFamilyAttribution(
                dependency_family=fam,
                raw_family_net_contribution=_as_float(info.get("raw")),
                weighted_family_net_contribution=_as_float(info.get("weighted")),
                regime_adjusted_family_contribution=_as_float(info.get("regime")),
                timing_adjusted_family_contribution=_as_float(info.get("timing")),
                transition_adjusted_family_contribution=_as_float(info.get("transition")),
                archetype_adjusted_family_contribution=_as_float(info.get("archetype")),
                cluster_state=cluster_state,
                dominant_archetype_key=dominant_archetype_key,
                drift_score=drift_score,
                pattern_entropy=pattern_entropy,
                cluster_weight=weight,
                cluster_bonus=bonus,
                cluster_penalty=penalty,
                cluster_adjusted_family_contribution=adjusted,
                top_symbols=list(info.get("top_symbols") or []),
                reason_codes=reason_codes,
                metadata={
                    "scoring_version":          _SCORING_VERSION,
                    "base_contribution_source": base_source,
                    "base_contribution":        base_val,
                },
            ))
        return items

    def rank_cluster_families(
        self, items: list[ClusterFamilyAttribution],
    ) -> list[ClusterFamilyAttribution]:
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.cluster_adjusted_family_contribution or 0.0),
                -abs(fa.cluster_adjusted_family_contribution or 0.0),
                -_CLUSTER_PREFERENCE.get(fa.cluster_state, 0),
                -_FAMILY_PRIORITY.get(fa.dependency_family, 0),
                fa.dependency_family,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.cluster_family_rank = i
        return ranked

    def compute_cluster_adjusted_symbol_attribution(
        self,
        symbol_scores: dict[str, dict[str, Any]],
        *,
        cluster_state: str,
        dominant_archetype_key: str,
        drift_score: float | None,
        pattern_entropy: float | None,
        profile: dict[str, Any],
    ) -> list[ClusterSymbolAttribution]:
        rotation_drift_suppressed = (
            cluster_state == "rotating"
            and (drift_score or 0.0) >= _ROTATION_DRIFT_CEILING
        )
        rows: list[ClusterSymbolAttribution] = []
        for sym, info in symbol_scores.items():
            family = info.get("dependency_family") or "unknown"
            dep_type = info.get("dependency_type")
            base_val, base_source = self._pick_base_contribution(info)
            weight = self.compute_cluster_weight(
                cluster_state=cluster_state, dependency_family=str(family),
                profile=profile,
            )
            if base_val is None:
                adjusted: float | None = None
            else:
                bonus = self.compute_cluster_bonus(
                    cluster_state=cluster_state, base_contribution=base_val,
                    drift_score=drift_score, profile=profile,
                )
                penalty = self.compute_cluster_penalty(
                    cluster_state=cluster_state, base_contribution=base_val,
                    drift_score=drift_score, pattern_entropy=pattern_entropy,
                    profile=profile,
                )
                adjusted = base_val * weight + bonus - penalty

            reason_codes = self._build_reason_codes(
                cluster_state=cluster_state, drift_score=drift_score,
                pattern_entropy=pattern_entropy,
                dominant_archetype_key=dominant_archetype_key,
                rotation_drift_suppressed=rotation_drift_suppressed,
            )

            rows.append(ClusterSymbolAttribution(
                symbol=str(sym),
                dependency_family=str(family),
                dependency_type=dep_type,
                cluster_state=cluster_state,
                dominant_archetype_key=dominant_archetype_key,
                raw_symbol_score=_as_float(info.get("raw")),
                weighted_symbol_score=_as_float(info.get("weighted")),
                regime_adjusted_symbol_score=_as_float(info.get("regime")),
                timing_adjusted_symbol_score=_as_float(info.get("timing")),
                transition_adjusted_symbol_score=_as_float(info.get("transition")),
                archetype_adjusted_symbol_score=_as_float(info.get("archetype")),
                cluster_weight=weight,
                cluster_adjusted_symbol_score=adjusted,
                reason_codes=reason_codes,
                metadata={
                    "scoring_version":     _SCORING_VERSION,
                    "base_score_source":   base_source,
                    "base_score":          base_val,
                },
            ))
        return rows

    def rank_cluster_symbols(
        self, rows: list[ClusterSymbolAttribution],
    ) -> list[ClusterSymbolAttribution]:
        ranked = sorted(
            rows,
            key=lambda r: (
                -(r.cluster_adjusted_symbol_score or 0.0),
                -abs(r.cluster_adjusted_symbol_score or 0.0),
                -_CLUSTER_PREFERENCE.get(r.cluster_state, 0),
                r.symbol,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.symbol_rank = i
        return ranked

    # ── orchestration ───────────────────────────────────────────────────
    def build_cluster_attribution_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> ClusterAttributionResult | None:
        cluster_ctx = self._load_run_cluster_context(conn, run_id=run_id)
        base_contribs = self._load_family_base_contributions(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not base_contribs:
            return None

        profile_row = self.get_active_cluster_profile(conn, workspace_id=workspace_id)
        default_profile_used = profile_row is None
        profile = profile_row if profile_row is not None else dict(_DEFAULT_PROFILE)
        profile_id = profile_row["id"] if profile_row is not None else None
        profile_name = profile.get("profile_name", _DEFAULT_PROFILE["profile_name"])

        cluster_state = cluster_ctx["cluster_state"]
        dominant_archetype_key = cluster_ctx["dominant_archetype_key"]
        drift_score = cluster_ctx["drift_score"]
        pattern_entropy = cluster_ctx["pattern_entropy"]

        family_rows = self.compute_cluster_adjusted_family_attribution(
            base_contribs,
            cluster_state=cluster_state,
            dominant_archetype_key=dominant_archetype_key,
            drift_score=drift_score, pattern_entropy=pattern_entropy,
            profile=profile,
        )
        family_rows = self.rank_cluster_families(family_rows)

        symbol_scores = self._load_symbol_base_scores(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        symbol_rows = self.compute_cluster_adjusted_symbol_attribution(
            symbol_scores,
            cluster_state=cluster_state,
            dominant_archetype_key=dominant_archetype_key,
            drift_score=drift_score, pattern_entropy=pattern_entropy,
            profile=profile,
        )
        symbol_rows = self.rank_cluster_symbols(symbol_rows)

        for fa in family_rows:
            fa.metadata.update({
                "profile_name":               profile_name,
                "default_cluster_profile_used": default_profile_used,
            })
        for sa in symbol_rows:
            sa.metadata.update({
                "profile_name":               profile_name,
                "default_cluster_profile_used": default_profile_used,
            })

        return ClusterAttributionResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=cluster_ctx.get("context_snapshot_id"),
            cluster_profile_id=profile_id,
            profile_name=profile_name,
            default_profile_used=default_profile_used,
            cluster_state=cluster_state,
            dominant_archetype_key=dominant_archetype_key,
            drift_score=drift_score,
            pattern_entropy=pattern_entropy,
            family_rows=family_rows,
            symbol_rows=symbol_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_cluster_attribution(
        self, conn, *, result: ClusterAttributionResult,
    ) -> dict[str, int]:
        import src.db.repositories as repo
        fam_count = repo.insert_cross_asset_family_cluster_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            cluster_profile_id=result.cluster_profile_id,
            rows=[
                {
                    "dependency_family":                       fa.dependency_family,
                    "raw_family_net_contribution":             fa.raw_family_net_contribution,
                    "weighted_family_net_contribution":        fa.weighted_family_net_contribution,
                    "regime_adjusted_family_contribution":     fa.regime_adjusted_family_contribution,
                    "timing_adjusted_family_contribution":     fa.timing_adjusted_family_contribution,
                    "transition_adjusted_family_contribution": fa.transition_adjusted_family_contribution,
                    "archetype_adjusted_family_contribution":  fa.archetype_adjusted_family_contribution,
                    "cluster_state":                           fa.cluster_state,
                    "dominant_archetype_key":                  fa.dominant_archetype_key,
                    "drift_score":                             fa.drift_score,
                    "pattern_entropy":                         fa.pattern_entropy,
                    "cluster_weight":                          fa.cluster_weight,
                    "cluster_bonus":                           fa.cluster_bonus,
                    "cluster_penalty":                         fa.cluster_penalty,
                    "cluster_adjusted_family_contribution":    fa.cluster_adjusted_family_contribution,
                    "cluster_family_rank":                     fa.cluster_family_rank,
                    "top_symbols":                             fa.top_symbols,
                    "reason_codes":                            fa.reason_codes,
                    "metadata":                                fa.metadata,
                }
                for fa in result.family_rows
            ],
        )
        sym_count = repo.insert_cross_asset_symbol_cluster_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            cluster_profile_id=result.cluster_profile_id,
            rows=[
                {
                    "symbol":                           sa.symbol,
                    "dependency_family":                sa.dependency_family,
                    "dependency_type":                  sa.dependency_type,
                    "cluster_state":                    sa.cluster_state,
                    "dominant_archetype_key":           sa.dominant_archetype_key,
                    "raw_symbol_score":                 sa.raw_symbol_score,
                    "weighted_symbol_score":            sa.weighted_symbol_score,
                    "regime_adjusted_symbol_score":     sa.regime_adjusted_symbol_score,
                    "timing_adjusted_symbol_score":     sa.timing_adjusted_symbol_score,
                    "transition_adjusted_symbol_score": sa.transition_adjusted_symbol_score,
                    "archetype_adjusted_symbol_score":  sa.archetype_adjusted_symbol_score,
                    "cluster_weight":                   sa.cluster_weight,
                    "cluster_adjusted_symbol_score":    sa.cluster_adjusted_symbol_score,
                    "symbol_rank":                      sa.symbol_rank,
                    "reason_codes":                     sa.reason_codes,
                    "metadata":                         sa.metadata,
                }
                for sa in result.symbol_rows
            ],
        )
        return {"family_rows": fam_count, "symbol_rows": sym_count}

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> ClusterAttributionResult | None:
        result = self.build_cluster_attribution_for_run(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if result is None:
            return None
        self.persist_cluster_attribution(conn, result=result)
        return result

    def refresh_workspace_cluster_attribution(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[ClusterAttributionResult]:
        """Emit cluster-aware attribution for every watchlist. Commits
        per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[ClusterAttributionResult] = []
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
                    "cross_asset_cluster_attribution: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
