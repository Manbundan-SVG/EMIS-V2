"""Phase 4.6B: Persistence-Aware Attribution Service.

Reads 4.6A persistence diagnostics and the most mature upstream family
contribution (cluster → archetype → transition → timing → regime → weighted →
raw fallback), then applies persistence-state weights + memory-score boosts +
state-age bonuses + memory-break penalties. Persists:

  * one cross_asset_family_persistence_attribution_snapshots row per family
  * one cross_asset_symbol_persistence_attribution_snapshots row per symbol

All adjustments are deterministic; persistence weight clipped to a
conservative band [0.75, 1.20] so persistence evidence cannot dominate the
upstream chain. Persistence-aware attribution is a refinement layer, not a
replacement.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_WEIGHT_MIN                = 0.75
_WEIGHT_MAX                = 1.20
_BONUS_PENALTY_BASE        = 0.05
_MEMORY_BOOST_THRESHOLD    = 0.65   # below this, no memory-score boost
_STATE_AGE_BONUS_THRESHOLD = 3      # state_age_runs ≥ this → state-age bonus
_SCORING_VERSION           = "4.6B.v1"

# Constructive states that admit memory + state-age bonuses.
_CONSTRUCTIVE_STATES = frozenset({"persistent", "recovering"})

# States that block memory boosts entirely (regardless of memory score).
_NON_CONSTRUCTIVE_STATES = frozenset({
    "fragile", "breaking_down", "insufficient_history",
})

# Persistence-state preference for tie-break.
_PERSISTENCE_PREFERENCE: dict[str, int] = {
    "persistent":           5,
    "recovering":           4,
    "rotating":             3,
    "mixed":                2,
    "fragile":              2,
    "breaking_down":        1,
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

# Memory-break event types that trigger penalty.
_MEMORY_BREAK_EVENTS = frozenset({
    "persistence_loss",
    "regime_memory_break",
    "cluster_memory_break",
    "archetype_memory_break",
})

# Stabilization / gain events admit a small bonus.
_STABILIZATION_EVENTS = frozenset({
    "stabilization",
    "persistence_gain",
})

_DEFAULT_PROFILE: dict[str, Any] = {
    "profile_name":                 "default_persistence_attribution",
    "persistent_weight":            1.08,
    "recovering_weight":            1.04,
    "rotating_weight":              0.98,
    "fragile_weight":               0.88,
    "breaking_down_weight":         0.80,
    "mixed_weight":                 0.90,
    "insufficient_history_weight":  0.80,
    "memory_score_boost_scale":     1.0,
    "memory_break_penalty_scale":   1.0,
    "stabilization_bonus_scale":    1.0,
    "state_age_bonus_scale":        1.0,
    "persistence_family_overrides": {},
}


@dataclass
class PersistenceFamilyAttribution:
    dependency_family: str
    raw_family_net_contribution: float | None
    weighted_family_net_contribution: float | None
    regime_adjusted_family_contribution: float | None
    timing_adjusted_family_contribution: float | None
    transition_adjusted_family_contribution: float | None
    archetype_adjusted_family_contribution: float | None
    cluster_adjusted_family_contribution: float | None
    persistence_state: str
    memory_score: float | None
    state_age_runs: int | None
    state_persistence_ratio: float | None
    regime_persistence_ratio: float | None
    cluster_persistence_ratio: float | None
    archetype_persistence_ratio: float | None
    latest_persistence_event_type: str | None
    persistence_weight: float
    persistence_bonus: float
    persistence_penalty: float
    persistence_adjusted_family_contribution: float | None
    persistence_family_rank: int | None = None
    top_symbols: list[str] = field(default_factory=list)
    reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PersistenceSymbolAttribution:
    symbol: str
    dependency_family: str
    dependency_type: str | None
    persistence_state: str
    memory_score: float | None
    state_age_runs: int | None
    latest_persistence_event_type: str | None
    raw_symbol_score: float | None
    weighted_symbol_score: float | None
    regime_adjusted_symbol_score: float | None
    timing_adjusted_symbol_score: float | None
    transition_adjusted_symbol_score: float | None
    archetype_adjusted_symbol_score: float | None
    cluster_adjusted_symbol_score: float | None
    persistence_weight: float
    persistence_adjusted_symbol_score: float | None
    symbol_rank: int | None = None
    reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PersistenceAttributionResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    persistence_profile_id: str | None
    profile_name: str
    default_profile_used: bool
    persistence_state: str
    memory_score: float | None
    state_age_runs: int | None
    latest_persistence_event_type: str | None
    family_rows: list[PersistenceFamilyAttribution]
    symbol_rows: list[PersistenceSymbolAttribution]


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


class CrossAssetPersistenceAttributionService:
    """Deterministic persistence-aware refinement of upstream family + symbol
    contributions."""

    # ── profile loading ─────────────────────────────────────────────────
    def get_active_persistence_profile(
        self, conn, *, workspace_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, profile_name, is_active,
                       persistent_weight, recovering_weight, rotating_weight,
                       fragile_weight, breaking_down_weight, mixed_weight,
                       insufficient_history_weight,
                       memory_score_boost_scale, memory_break_penalty_scale,
                       stabilization_bonus_scale, state_age_bonus_scale,
                       persistence_family_overrides, metadata, created_at
                from public.cross_asset_persistence_attribution_profiles
                where workspace_id = %s::uuid and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ── weight primitives ───────────────────────────────────────────────
    def compute_persistence_weight(
        self, *, persistence_state: str, dependency_family: str | None,
        profile: dict[str, Any],
    ) -> float:
        key_map = {
            "persistent":           "persistent_weight",
            "recovering":           "recovering_weight",
            "rotating":             "rotating_weight",
            "fragile":              "fragile_weight",
            "breaking_down":        "breaking_down_weight",
            "mixed":                "mixed_weight",
            "insufficient_history": "insufficient_history_weight",
        }
        base = _as_float(profile.get(key_map.get(persistence_state, "insufficient_history_weight")))
        base = base if base is not None else 1.0

        # Per-family override.
        overrides = _parse_overrides(profile.get("persistence_family_overrides"))
        fam_override = None
        if dependency_family:
            fam_override = _as_float(overrides.get(dependency_family))
            if fam_override is None:
                nested = overrides.get(dependency_family) if isinstance(overrides.get(dependency_family), dict) else None
                if isinstance(nested, dict):
                    fam_override = _as_float(nested.get(persistence_state))

        combined = base * (fam_override if fam_override is not None else 1.0)
        return _clip(combined, _WEIGHT_MIN, _WEIGHT_MAX)

    def compute_memory_score_bonus(
        self, *, persistence_state: str, base_contribution: float | None,
        memory_score: float | None, profile: dict[str, Any],
    ) -> float:
        if base_contribution is None or memory_score is None:
            return 0.0
        if persistence_state in _NON_CONSTRUCTIVE_STATES:
            return 0.0
        if persistence_state not in _CONSTRUCTIVE_STATES:
            return 0.0
        if memory_score < _MEMORY_BOOST_THRESHOLD:
            return 0.0
        sign = 1.0 if base_contribution >= 0 else -1.0
        magnitude = abs(base_contribution) * _BONUS_PENALTY_BASE
        scale = _as_float(profile.get("memory_score_boost_scale")) or 1.0
        # 0% at threshold, ramping to full at memory_score=1.0.
        ramp = (memory_score - _MEMORY_BOOST_THRESHOLD) / (1.0 - _MEMORY_BOOST_THRESHOLD)
        ramp = _clip(ramp, 0.0, 1.0)
        return sign * magnitude * max(0.0, scale) * ramp * 0.5  # ~2.5% of base at full

    def compute_state_age_bonus(
        self, *, persistence_state: str, base_contribution: float | None,
        state_age_runs: int | None, profile: dict[str, Any],
    ) -> float:
        if base_contribution is None or state_age_runs is None:
            return 0.0
        if persistence_state not in _CONSTRUCTIVE_STATES:
            return 0.0
        if state_age_runs < _STATE_AGE_BONUS_THRESHOLD:
            return 0.0
        sign = 1.0 if base_contribution >= 0 else -1.0
        magnitude = abs(base_contribution) * _BONUS_PENALTY_BASE
        scale = _as_float(profile.get("state_age_bonus_scale")) or 1.0
        # Saturating bonus: 0 at threshold, full at state_age=10.
        ramp = (state_age_runs - _STATE_AGE_BONUS_THRESHOLD) / max(1, 10 - _STATE_AGE_BONUS_THRESHOLD)
        ramp = _clip(ramp, 0.0, 1.0)
        return sign * magnitude * max(0.0, scale) * ramp * 0.3  # ~1.5% of base at full

    def compute_persistence_penalty(
        self, *, persistence_state: str, base_contribution: float | None,
        latest_event_type: str | None, profile: dict[str, Any],
    ) -> float:
        """Sign-aware penalty (subtracts toward zero). Combines state-baseline
        penalty + memory-break event penalty."""
        if base_contribution is None:
            return 0.0
        sign = 1.0 if base_contribution >= 0 else -1.0
        magnitude = abs(base_contribution) * _BONUS_PENALTY_BASE

        state_pen = 0.0
        if persistence_state == "breaking_down":
            state_pen = 0.6  # ~3% of base
        elif persistence_state == "fragile":
            state_pen = 0.4  # ~2% of base
        elif persistence_state == "mixed":
            state_pen = 0.2  # ~1% of base

        # Memory-break event penalty.
        event_pen = 0.0
        if latest_event_type in _MEMORY_BREAK_EVENTS:
            scale = _as_float(profile.get("memory_break_penalty_scale")) or 1.0
            event_pen = 0.4 * max(0.0, scale)  # ~2% of base

        total = state_pen + event_pen
        return sign * magnitude * total

    @staticmethod
    def _compute_stabilization_bonus(
        *, base_contribution: float | None,
        latest_event_type: str | None, profile: dict[str, Any],
    ) -> float:
        if base_contribution is None or latest_event_type not in _STABILIZATION_EVENTS:
            return 0.0
        sign = 1.0 if base_contribution >= 0 else -1.0
        magnitude = abs(base_contribution) * _BONUS_PENALTY_BASE
        scale = _as_float(profile.get("stabilization_bonus_scale")) or 1.0
        return sign * magnitude * max(0.0, scale) * 0.3  # ~1.5% of base

    # ── input loading ───────────────────────────────────────────────────
    def _load_run_persistence_context(
        self, conn, *, run_id: str,
    ) -> dict[str, Any]:
        ctx: dict[str, Any] = {
            "context_snapshot_id":            None,
            "persistence_state":              "insufficient_history",
            "memory_score":                   None,
            "state_age_runs":                 None,
            "state_persistence_ratio":        None,
            "regime_persistence_ratio":       None,
            "cluster_persistence_ratio":      None,
            "archetype_persistence_ratio":    None,
            "latest_persistence_event_type":  None,
        }
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
            r = cur.fetchone()
            if r:
                ctx["context_snapshot_id"] = dict(r).get("context_snapshot_id")

            cur.execute(
                """
                select persistence_state, memory_score, state_age_runs,
                       state_persistence_ratio, regime_persistence_ratio,
                       cluster_persistence_ratio, archetype_persistence_ratio
                from public.cross_asset_state_persistence_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                d = dict(r)
                if d.get("persistence_state"):
                    ctx["persistence_state"] = d.get("persistence_state")
                ctx["memory_score"]                = _as_float(d.get("memory_score"))
                ctx["state_age_runs"]              = d.get("state_age_runs")
                ctx["state_persistence_ratio"]     = _as_float(d.get("state_persistence_ratio"))
                ctx["regime_persistence_ratio"]    = _as_float(d.get("regime_persistence_ratio"))
                ctx["cluster_persistence_ratio"]   = _as_float(d.get("cluster_persistence_ratio"))
                ctx["archetype_persistence_ratio"] = _as_float(d.get("archetype_persistence_ratio"))

            cur.execute(
                """
                select latest_persistence_event_type
                from public.run_cross_asset_persistence_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                ctx["latest_persistence_event_type"] = dict(r).get("latest_persistence_event_type")
        return ctx

    def _load_family_base_contributions(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "raw": None, "weighted": None, "regime": None, "timing": None,
            "transition": None, "archetype": None, "cluster": None,
            "top_symbols": [],
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

            for source_key, source_table, contrib_col, top_col in (
                ("weighted",   "cross_asset_family_weighted_attribution_summary",   "weighted_family_net_contribution",          None),
                ("regime",     "cross_asset_family_regime_attribution_summary",    "regime_adjusted_family_contribution",      None),
                ("timing",     "cross_asset_family_timing_attribution_summary",    "timing_adjusted_family_contribution",      None),
                ("transition", "cross_asset_family_transition_attribution_summary", "transition_adjusted_family_contribution", "top_symbols"),
                ("archetype",  "cross_asset_family_archetype_attribution_summary",  "archetype_adjusted_family_contribution",  "top_symbols"),
                ("cluster",    "cross_asset_family_cluster_attribution_summary",    "cluster_adjusted_family_contribution",    "top_symbols"),
            ):
                cur.execute(
                    f"""
                    select dependency_family, {contrib_col}{f", {top_col}" if top_col else ""}
                    from public.{source_table}
                    where workspace_id = %s::uuid
                      and watchlist_id = %s::uuid
                      and run_id       = %s::uuid
                    """,
                    (workspace_id, watchlist_id, run_id),
                )
                for r in cur.fetchall():
                    d = dict(r)
                    fam = str(d["dependency_family"])
                    out[fam][source_key] = _as_float(d.get(contrib_col))
                    if top_col:
                        top = d.get(top_col) or []
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
            "transition": None, "archetype": None, "cluster": None,
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

            for source_key, source_table, score_col in (
                ("regime",     "cross_asset_symbol_regime_attribution_summary",     "regime_adjusted_symbol_score"),
                ("timing",     "cross_asset_symbol_timing_attribution_summary",     "timing_adjusted_symbol_score"),
                ("transition", "cross_asset_symbol_transition_attribution_summary", "transition_adjusted_symbol_score"),
                ("archetype",  "cross_asset_symbol_archetype_attribution_summary",  "archetype_adjusted_symbol_score"),
                ("cluster",    "cross_asset_symbol_cluster_attribution_summary",    "cluster_adjusted_symbol_score"),
            ):
                cur.execute(
                    f"""
                    select symbol, {score_col}
                    from public.{source_table}
                    where workspace_id = %s::uuid
                      and watchlist_id = %s::uuid
                      and run_id       = %s::uuid
                    """,
                    (workspace_id, watchlist_id, run_id),
                )
                for r in cur.fetchall():
                    d = dict(r)
                    sym = str(d["symbol"])
                    out[sym][source_key] = _as_float(d.get(score_col))
        return out

    @staticmethod
    def _pick_base_contribution(contribs: dict[str, Any]) -> tuple[float | None, str]:
        """Fallback chain: cluster → archetype → transition → timing → regime → weighted → raw."""
        for key in ("cluster", "archetype", "transition", "timing", "regime", "weighted", "raw"):
            v = contribs.get(key)
            if v is not None:
                return (v, key)
        return (None, "none")

    @staticmethod
    def _build_reason_codes(
        *, persistence_state: str, memory_score: float | None,
        state_age_runs: int | None,
        latest_event_type: str | None,
        memory_boost_applied: bool, state_age_bonus_applied: bool,
        stabilization_bonus_applied: bool,
        memory_break_penalty_applied: bool,
    ) -> list[str]:
        codes = [f"persistence_state:{persistence_state}"]
        if memory_score is not None:
            codes.append(f"memory_score:{memory_score:.3f}")
        if state_age_runs is not None:
            codes.append(f"state_age:{state_age_runs}")
        if latest_event_type:
            codes.append(f"latest_event:{latest_event_type}")
        if memory_boost_applied:
            codes.append("memory_score_boost_applied")
        if state_age_bonus_applied:
            codes.append("state_age_bonus_applied")
        if stabilization_bonus_applied:
            codes.append("stabilization_bonus_applied")
        if memory_break_penalty_applied:
            codes.append("memory_break_penalty_applied")
        if persistence_state == "breaking_down":
            codes.append("breaking_down_state_penalty")
        if persistence_state == "fragile":
            codes.append("fragile_state_penalty")
        if persistence_state == "mixed":
            codes.append("mixed_state_suppression")
        if persistence_state == "insufficient_history":
            codes.append("insufficient_history_suppression")
        return codes

    # ── family + symbol builders ────────────────────────────────────────
    def compute_persistence_adjusted_family_attribution(
        self,
        base_contribs: dict[str, dict[str, Any]],
        *,
        persistence_state: str,
        memory_score: float | None,
        state_age_runs: int | None,
        ratios: dict[str, float | None],
        latest_event_type: str | None,
        profile: dict[str, Any],
    ) -> list[PersistenceFamilyAttribution]:
        items: list[PersistenceFamilyAttribution] = []

        for fam, info in sorted(base_contribs.items()):
            base_val, base_source = self._pick_base_contribution(info)
            weight = self.compute_persistence_weight(
                persistence_state=persistence_state, dependency_family=fam,
                profile=profile,
            )
            mem_bonus    = self.compute_memory_score_bonus(
                persistence_state=persistence_state, base_contribution=base_val,
                memory_score=memory_score, profile=profile,
            )
            age_bonus    = self.compute_state_age_bonus(
                persistence_state=persistence_state, base_contribution=base_val,
                state_age_runs=state_age_runs, profile=profile,
            )
            stab_bonus   = self._compute_stabilization_bonus(
                base_contribution=base_val,
                latest_event_type=latest_event_type, profile=profile,
            )
            penalty      = self.compute_persistence_penalty(
                persistence_state=persistence_state, base_contribution=base_val,
                latest_event_type=latest_event_type, profile=profile,
            )

            bonus_total = mem_bonus + age_bonus + stab_bonus
            adjusted: float | None
            if base_val is None:
                adjusted = None
            else:
                adjusted = base_val * weight + bonus_total - penalty

            reason_codes = self._build_reason_codes(
                persistence_state=persistence_state,
                memory_score=memory_score,
                state_age_runs=state_age_runs,
                latest_event_type=latest_event_type,
                memory_boost_applied=(abs(mem_bonus) > 0.0),
                state_age_bonus_applied=(abs(age_bonus) > 0.0),
                stabilization_bonus_applied=(abs(stab_bonus) > 0.0),
                memory_break_penalty_applied=(latest_event_type in _MEMORY_BREAK_EVENTS),
            )

            items.append(PersistenceFamilyAttribution(
                dependency_family=fam,
                raw_family_net_contribution=_as_float(info.get("raw")),
                weighted_family_net_contribution=_as_float(info.get("weighted")),
                regime_adjusted_family_contribution=_as_float(info.get("regime")),
                timing_adjusted_family_contribution=_as_float(info.get("timing")),
                transition_adjusted_family_contribution=_as_float(info.get("transition")),
                archetype_adjusted_family_contribution=_as_float(info.get("archetype")),
                cluster_adjusted_family_contribution=_as_float(info.get("cluster")),
                persistence_state=persistence_state,
                memory_score=memory_score,
                state_age_runs=state_age_runs,
                state_persistence_ratio=ratios.get("state"),
                regime_persistence_ratio=ratios.get("regime"),
                cluster_persistence_ratio=ratios.get("cluster"),
                archetype_persistence_ratio=ratios.get("archetype"),
                latest_persistence_event_type=latest_event_type,
                persistence_weight=weight,
                persistence_bonus=bonus_total,
                persistence_penalty=penalty,
                persistence_adjusted_family_contribution=adjusted,
                top_symbols=list(info.get("top_symbols") or []),
                reason_codes=reason_codes,
                metadata={
                    "scoring_version":          _SCORING_VERSION,
                    "base_contribution_source": base_source,
                    "base_contribution":        base_val,
                    "memory_bonus":             mem_bonus,
                    "state_age_bonus":          age_bonus,
                    "stabilization_bonus":      stab_bonus,
                },
            ))
        return items

    def rank_persistence_families(
        self, items: list[PersistenceFamilyAttribution],
    ) -> list[PersistenceFamilyAttribution]:
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.persistence_adjusted_family_contribution or 0.0),
                -abs(fa.persistence_adjusted_family_contribution or 0.0),
                -_PERSISTENCE_PREFERENCE.get(fa.persistence_state, 0),
                -_FAMILY_PRIORITY.get(fa.dependency_family, 0),
                fa.dependency_family,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.persistence_family_rank = i
        return ranked

    def compute_persistence_adjusted_symbol_attribution(
        self,
        symbol_scores: dict[str, dict[str, Any]],
        *,
        persistence_state: str,
        memory_score: float | None,
        state_age_runs: int | None,
        latest_event_type: str | None,
        profile: dict[str, Any],
    ) -> list[PersistenceSymbolAttribution]:
        rows: list[PersistenceSymbolAttribution] = []
        for sym, info in symbol_scores.items():
            family = info.get("dependency_family") or "unknown"
            dep_type = info.get("dependency_type")
            base_val, base_source = self._pick_base_contribution(info)
            weight = self.compute_persistence_weight(
                persistence_state=persistence_state, dependency_family=str(family),
                profile=profile,
            )

            adjusted: float | None
            mem_bonus = age_bonus = stab_bonus = 0.0
            penalty = 0.0
            if base_val is None:
                adjusted = None
            else:
                mem_bonus  = self.compute_memory_score_bonus(
                    persistence_state=persistence_state, base_contribution=base_val,
                    memory_score=memory_score, profile=profile,
                )
                age_bonus  = self.compute_state_age_bonus(
                    persistence_state=persistence_state, base_contribution=base_val,
                    state_age_runs=state_age_runs, profile=profile,
                )
                stab_bonus = self._compute_stabilization_bonus(
                    base_contribution=base_val,
                    latest_event_type=latest_event_type, profile=profile,
                )
                penalty    = self.compute_persistence_penalty(
                    persistence_state=persistence_state, base_contribution=base_val,
                    latest_event_type=latest_event_type, profile=profile,
                )
                adjusted = base_val * weight + (mem_bonus + age_bonus + stab_bonus) - penalty

            reason_codes = self._build_reason_codes(
                persistence_state=persistence_state,
                memory_score=memory_score,
                state_age_runs=state_age_runs,
                latest_event_type=latest_event_type,
                memory_boost_applied=(abs(mem_bonus) > 0.0),
                state_age_bonus_applied=(abs(age_bonus) > 0.0),
                stabilization_bonus_applied=(abs(stab_bonus) > 0.0),
                memory_break_penalty_applied=(latest_event_type in _MEMORY_BREAK_EVENTS),
            )

            rows.append(PersistenceSymbolAttribution(
                symbol=str(sym),
                dependency_family=str(family),
                dependency_type=dep_type,
                persistence_state=persistence_state,
                memory_score=memory_score,
                state_age_runs=state_age_runs,
                latest_persistence_event_type=latest_event_type,
                raw_symbol_score=_as_float(info.get("raw")),
                weighted_symbol_score=_as_float(info.get("weighted")),
                regime_adjusted_symbol_score=_as_float(info.get("regime")),
                timing_adjusted_symbol_score=_as_float(info.get("timing")),
                transition_adjusted_symbol_score=_as_float(info.get("transition")),
                archetype_adjusted_symbol_score=_as_float(info.get("archetype")),
                cluster_adjusted_symbol_score=_as_float(info.get("cluster")),
                persistence_weight=weight,
                persistence_adjusted_symbol_score=adjusted,
                reason_codes=reason_codes,
                metadata={
                    "scoring_version":     _SCORING_VERSION,
                    "base_score_source":   base_source,
                    "base_score":          base_val,
                },
            ))
        return rows

    def rank_persistence_symbols(
        self, rows: list[PersistenceSymbolAttribution],
    ) -> list[PersistenceSymbolAttribution]:
        ranked = sorted(
            rows,
            key=lambda r: (
                -(r.persistence_adjusted_symbol_score or 0.0),
                -abs(r.persistence_adjusted_symbol_score or 0.0),
                -_PERSISTENCE_PREFERENCE.get(r.persistence_state, 0),
                r.symbol,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.symbol_rank = i
        return ranked

    # ── orchestration ───────────────────────────────────────────────────
    def build_persistence_attribution_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> PersistenceAttributionResult | None:
        ctx = self._load_run_persistence_context(conn, run_id=run_id)
        base_contribs = self._load_family_base_contributions(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not base_contribs:
            return None

        profile_row = self.get_active_persistence_profile(conn, workspace_id=workspace_id)
        default_profile_used = profile_row is None
        profile = profile_row if profile_row is not None else dict(_DEFAULT_PROFILE)
        profile_id = profile_row["id"] if profile_row is not None else None
        profile_name = profile.get("profile_name", _DEFAULT_PROFILE["profile_name"])

        persistence_state = ctx["persistence_state"]
        memory_score      = ctx["memory_score"]
        state_age_runs    = ctx["state_age_runs"]
        latest_event_type = ctx["latest_persistence_event_type"]
        ratios = {
            "state":     ctx.get("state_persistence_ratio"),
            "regime":    ctx.get("regime_persistence_ratio"),
            "cluster":   ctx.get("cluster_persistence_ratio"),
            "archetype": ctx.get("archetype_persistence_ratio"),
        }

        family_rows = self.compute_persistence_adjusted_family_attribution(
            base_contribs,
            persistence_state=persistence_state,
            memory_score=memory_score,
            state_age_runs=state_age_runs,
            ratios=ratios,
            latest_event_type=latest_event_type,
            profile=profile,
        )
        family_rows = self.rank_persistence_families(family_rows)

        symbol_scores = self._load_symbol_base_scores(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        symbol_rows = self.compute_persistence_adjusted_symbol_attribution(
            symbol_scores,
            persistence_state=persistence_state,
            memory_score=memory_score,
            state_age_runs=state_age_runs,
            latest_event_type=latest_event_type,
            profile=profile,
        )
        symbol_rows = self.rank_persistence_symbols(symbol_rows)

        for fa in family_rows:
            fa.metadata.update({
                "profile_name":                   profile_name,
                "default_persistence_profile_used": default_profile_used,
            })
        for sa in symbol_rows:
            sa.metadata.update({
                "profile_name":                   profile_name,
                "default_persistence_profile_used": default_profile_used,
            })

        return PersistenceAttributionResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=ctx.get("context_snapshot_id"),
            persistence_profile_id=profile_id,
            profile_name=profile_name,
            default_profile_used=default_profile_used,
            persistence_state=persistence_state,
            memory_score=memory_score,
            state_age_runs=state_age_runs,
            latest_persistence_event_type=latest_event_type,
            family_rows=family_rows,
            symbol_rows=symbol_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_persistence_attribution(
        self, conn, *, result: PersistenceAttributionResult,
    ) -> dict[str, int]:
        import src.db.repositories as repo
        fam_count = repo.insert_cross_asset_family_persistence_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            persistence_profile_id=result.persistence_profile_id,
            rows=[
                {
                    "dependency_family":                       fa.dependency_family,
                    "raw_family_net_contribution":             fa.raw_family_net_contribution,
                    "weighted_family_net_contribution":        fa.weighted_family_net_contribution,
                    "regime_adjusted_family_contribution":     fa.regime_adjusted_family_contribution,
                    "timing_adjusted_family_contribution":     fa.timing_adjusted_family_contribution,
                    "transition_adjusted_family_contribution": fa.transition_adjusted_family_contribution,
                    "archetype_adjusted_family_contribution":  fa.archetype_adjusted_family_contribution,
                    "cluster_adjusted_family_contribution":    fa.cluster_adjusted_family_contribution,
                    "persistence_state":                       fa.persistence_state,
                    "memory_score":                            fa.memory_score,
                    "state_age_runs":                          fa.state_age_runs,
                    "state_persistence_ratio":                 fa.state_persistence_ratio,
                    "regime_persistence_ratio":                fa.regime_persistence_ratio,
                    "cluster_persistence_ratio":               fa.cluster_persistence_ratio,
                    "archetype_persistence_ratio":             fa.archetype_persistence_ratio,
                    "latest_persistence_event_type":           fa.latest_persistence_event_type,
                    "persistence_weight":                      fa.persistence_weight,
                    "persistence_bonus":                       fa.persistence_bonus,
                    "persistence_penalty":                     fa.persistence_penalty,
                    "persistence_adjusted_family_contribution": fa.persistence_adjusted_family_contribution,
                    "persistence_family_rank":                 fa.persistence_family_rank,
                    "top_symbols":                             fa.top_symbols,
                    "reason_codes":                            fa.reason_codes,
                    "metadata":                                fa.metadata,
                }
                for fa in result.family_rows
            ],
        )
        sym_count = repo.insert_cross_asset_symbol_persistence_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            persistence_profile_id=result.persistence_profile_id,
            rows=[
                {
                    "symbol":                           sa.symbol,
                    "dependency_family":                sa.dependency_family,
                    "dependency_type":                  sa.dependency_type,
                    "persistence_state":                sa.persistence_state,
                    "memory_score":                     sa.memory_score,
                    "state_age_runs":                   sa.state_age_runs,
                    "latest_persistence_event_type":    sa.latest_persistence_event_type,
                    "raw_symbol_score":                 sa.raw_symbol_score,
                    "weighted_symbol_score":            sa.weighted_symbol_score,
                    "regime_adjusted_symbol_score":     sa.regime_adjusted_symbol_score,
                    "timing_adjusted_symbol_score":     sa.timing_adjusted_symbol_score,
                    "transition_adjusted_symbol_score": sa.transition_adjusted_symbol_score,
                    "archetype_adjusted_symbol_score":  sa.archetype_adjusted_symbol_score,
                    "cluster_adjusted_symbol_score":    sa.cluster_adjusted_symbol_score,
                    "persistence_weight":               sa.persistence_weight,
                    "persistence_adjusted_symbol_score": sa.persistence_adjusted_symbol_score,
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
    ) -> PersistenceAttributionResult | None:
        result = self.build_persistence_attribution_for_run(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if result is None:
            return None
        self.persist_persistence_attribution(conn, result=result)
        return result

    def refresh_workspace_persistence_attribution(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[PersistenceAttributionResult]:
        """Emit persistence-aware attribution for every watchlist. Commits
        per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[PersistenceAttributionResult] = []
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
                    "cross_asset_persistence_attribution: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
