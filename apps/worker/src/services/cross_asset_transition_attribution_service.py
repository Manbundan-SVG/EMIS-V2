"""Phase 4.3B: Transition-Aware Attribution Service.

Conditions family and symbol attribution on 4.3A transition state + sequence
class. Picks the most informed base contribution (timing → regime → weighted
→ raw fallback chain) and applies transition-state × sequence-class
multipliers plus explicit state-specific bonuses (recovering, rotating_in)
and penalties (deteriorating, rotating_out). Symbol-level inherits the
family's state/class and same adjustments.

All adjustments are deterministic; total multiplier is clipped to the
conservative band [0.75, 1.20] so transition evidence cannot dominate
upstream contribution.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_TOTAL_MULTIPLIER_MIN = 0.75
_TOTAL_MULTIPLIER_MAX = 1.20
_BONUS_PENALTY_BASE   = 0.05  # base magnitude scaled by profile
_SCORING_VERSION      = "4.3B.v1"

# Tie-break preference for dominant family selection.
_TRANSITION_STATE_PREFERENCE: dict[str, int] = {
    "rotating_in":          3,
    "reinforcing":          3,
    "recovering":           3,
    "stable":               2,
    "rotating_out":         1,
    "deteriorating":        1,
    "insufficient_history": 0,
}

# Structural priority tie-break (matches 4.1B/4.1C/4.2B).
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
    "profile_name":                "default_transition",
    "reinforcing_weight":          1.10,
    "stable_weight":               1.00,
    "recovering_weight":           1.03,
    "rotating_in_weight":          1.08,
    "rotating_out_weight":         0.90,
    "deteriorating_weight":        0.85,
    "insufficient_history_weight": 0.80,
    "recovery_bonus_scale":        1.0,
    "degradation_penalty_scale":   1.0,
    "rotation_bonus_scale":        1.0,
    "sequence_class_overrides":    {},
    "family_weight_overrides":     {},
}

_DEFAULT_SEQUENCE_WEIGHTS: dict[str, float] = {
    "reinforcing_path":     1.05,
    "recovery_path":        1.02,
    "rotation_path":        1.03,
    "mixed_path":           1.00,
    "deteriorating_path":   0.90,
    "insufficient_history": 0.95,
}


@dataclass
class TransitionFamilyAttribution:
    dependency_family: str
    raw_family_net_contribution: float | None
    weighted_family_net_contribution: float | None
    regime_adjusted_family_contribution: float | None
    timing_adjusted_family_contribution: float | None
    transition_state: str
    dominant_sequence_class: str
    transition_state_weight: float
    sequence_class_weight: float
    transition_bonus: float
    transition_penalty: float
    transition_adjusted_family_contribution: float | None
    transition_family_rank: int | None = None
    top_symbols: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TransitionSymbolAttribution:
    symbol: str
    dependency_family: str
    dependency_type: str | None
    transition_state: str
    dominant_sequence_class: str
    raw_symbol_score: float | None
    weighted_symbol_score: float | None
    regime_adjusted_symbol_score: float | None
    timing_adjusted_symbol_score: float | None
    transition_state_weight: float
    sequence_class_weight: float
    transition_adjusted_symbol_score: float | None
    symbol_rank: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TransitionAttributionResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    transition_profile_id: str | None
    profile_name: str
    default_profile_used: bool
    family_rows: list[TransitionFamilyAttribution]
    symbol_rows: list[TransitionSymbolAttribution]


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


class CrossAssetTransitionAttributionService:
    """Deterministic transition-state + sequence-class refinement of
    4.1A/B/C/4.2B family + symbol contributions."""

    # ── profile loading ─────────────────────────────────────────────────
    def get_active_transition_profile(
        self, conn, *, workspace_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, profile_name, is_active,
                       reinforcing_weight, stable_weight, recovering_weight,
                       rotating_in_weight, rotating_out_weight,
                       deteriorating_weight, insufficient_history_weight,
                       recovery_bonus_scale, degradation_penalty_scale, rotation_bonus_scale,
                       sequence_class_overrides, family_weight_overrides,
                       metadata, created_at
                from public.cross_asset_transition_attribution_profiles
                where workspace_id = %s::uuid and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ── weight primitives ───────────────────────────────────────────────
    def compute_transition_state_weight(
        self, *, transition_state: str, profile: dict[str, Any],
        dependency_family: str | None = None,
    ) -> float:
        key_map = {
            "reinforcing":          "reinforcing_weight",
            "stable":               "stable_weight",
            "recovering":           "recovering_weight",
            "rotating_in":          "rotating_in_weight",
            "rotating_out":         "rotating_out_weight",
            "deteriorating":        "deteriorating_weight",
            "insufficient_history": "insufficient_history_weight",
        }
        base = _as_float(profile.get(key_map.get(transition_state, "stable_weight")))
        base = base if base is not None else 1.0
        # Optional per-family override multiplies on top
        overrides = _parse_overrides(profile.get("family_weight_overrides"))
        fam_override = _as_float(overrides.get(dependency_family)) if dependency_family else None
        combined = base * (fam_override if fam_override is not None else 1.0)
        return _clip(combined, _TOTAL_MULTIPLIER_MIN, _TOTAL_MULTIPLIER_MAX)

    def compute_sequence_class_weight(
        self, *, sequence_class: str, profile: dict[str, Any],
    ) -> float:
        overrides = _parse_overrides(profile.get("sequence_class_overrides"))
        raw = _as_float(overrides.get(sequence_class))
        if raw is None:
            raw = _DEFAULT_SEQUENCE_WEIGHTS.get(sequence_class, 1.0)
        return _clip(raw, _TOTAL_MULTIPLIER_MIN, _TOTAL_MULTIPLIER_MAX)

    def compute_transition_bonus(
        self, *, transition_state: str, base_contribution: float | None,
        profile: dict[str, Any],
    ) -> float:
        if base_contribution is None:
            return 0.0
        sign = 1.0 if base_contribution >= 0 else -1.0
        magnitude = abs(base_contribution) * _BONUS_PENALTY_BASE
        if transition_state == "recovering":
            scale = _as_float(profile.get("recovery_bonus_scale")) or 1.0
            return sign * magnitude * max(0.0, scale) * 0.4  # ~2% of base
        if transition_state == "rotating_in":
            scale = _as_float(profile.get("rotation_bonus_scale")) or 1.0
            return sign * magnitude * max(0.0, scale) * 0.5  # ~2.5% of base
        return 0.0

    def compute_transition_penalty(
        self, *, transition_state: str, base_contribution: float | None,
        profile: dict[str, Any],
    ) -> float:
        """Returns a sign-aware penalty magnitude to SUBTRACT. Always reduces
        magnitude toward zero (does not flip sign of base contribution)."""
        if base_contribution is None:
            return 0.0
        sign = 1.0 if base_contribution >= 0 else -1.0
        magnitude = abs(base_contribution) * _BONUS_PENALTY_BASE
        if transition_state == "deteriorating":
            scale = _as_float(profile.get("degradation_penalty_scale")) or 1.0
            return sign * magnitude * max(0.0, scale) * 0.6  # ~3% of base
        if transition_state == "rotating_out":
            scale = _as_float(profile.get("rotation_bonus_scale")) or 1.0
            return sign * magnitude * max(0.0, scale) * 0.3  # ~1.5% of base
        return 0.0

    # ── input loading ───────────────────────────────────────────────────
    def _load_transition_state_rows(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family, transition_state, family_rank
                from public.cross_asset_family_transition_state_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_sequence_class_by_family(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, str]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family, dominant_sequence_class
                from public.cross_asset_family_sequence_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return {str(dict(r)["dependency_family"]): str(dict(r)["dominant_sequence_class"])
                    for r in cur.fetchall()}

    def _load_family_base_contributions(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "raw": None, "weighted": None, "regime": None, "timing": None,
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
        return out

    def _load_symbol_base_scores(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "raw": None, "weighted": None, "regime": None, "timing": None,
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
                out[sym]["raw"]                = _as_float(d.get("raw_symbol_score"))
                out[sym]["weighted"]           = _as_float(d.get("weighted_symbol_score"))
                out[sym]["dependency_family"]  = d.get("dependency_family")
                out[sym]["dependency_type"]    = d.get("dependency_type")

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
        return out

    @staticmethod
    def _pick_base_contribution(contribs: dict[str, Any]) -> tuple[float | None, str]:
        """Fallback chain: timing → regime → weighted → raw."""
        for key in ("timing", "regime", "weighted", "raw"):
            v = contribs.get(key)
            if v is not None:
                return (v, key)
        return (None, "none")

    # ── family + symbol builders ────────────────────────────────────────
    def compute_transition_adjusted_family_attribution(
        self,
        transition_rows: Sequence[dict[str, Any]],
        base_contribs: dict[str, dict[str, Any]],
        sequence_by_family: dict[str, str],
        *,
        profile: dict[str, Any],
    ) -> list[TransitionFamilyAttribution]:
        items: list[TransitionFamilyAttribution] = []
        seen_families: set[str] = set()

        for tr in transition_rows:
            fam = str(tr["dependency_family"])
            seen_families.add(fam)
            state = str(tr.get("transition_state") or "insufficient_history")
            seq_class = sequence_by_family.get(fam, "insufficient_history")

            info = base_contribs.get(fam, {})
            base_val, base_source = self._pick_base_contribution(info)

            state_w = self.compute_transition_state_weight(
                transition_state=state, profile=profile, dependency_family=fam,
            )
            seq_w = self.compute_sequence_class_weight(
                sequence_class=seq_class, profile=profile,
            )
            bonus = self.compute_transition_bonus(
                transition_state=state, base_contribution=base_val, profile=profile,
            )
            penalty = self.compute_transition_penalty(
                transition_state=state, base_contribution=base_val, profile=profile,
            )

            if base_val is None:
                adjusted: float | None = None
                total_multiplier: float | None = None
            else:
                raw_multiplier = state_w * seq_w
                total_multiplier = _clip(raw_multiplier, _TOTAL_MULTIPLIER_MIN, _TOTAL_MULTIPLIER_MAX)
                adjusted = base_val * total_multiplier + bonus - penalty

            items.append(TransitionFamilyAttribution(
                dependency_family=fam,
                raw_family_net_contribution=_as_float(info.get("raw")),
                weighted_family_net_contribution=_as_float(info.get("weighted")),
                regime_adjusted_family_contribution=_as_float(info.get("regime")),
                timing_adjusted_family_contribution=_as_float(info.get("timing")),
                transition_state=state,
                dominant_sequence_class=seq_class,
                transition_state_weight=state_w,
                sequence_class_weight=seq_w,
                transition_bonus=bonus,
                transition_penalty=penalty,
                transition_adjusted_family_contribution=adjusted,
                top_symbols=list(info.get("top_symbols") or []),
                metadata={
                    "scoring_version":           _SCORING_VERSION,
                    "base_contribution_source":  base_source,
                    "base_contribution":         base_val,
                    "total_multiplier":          total_multiplier,
                    "source_family_rank":        tr.get("family_rank"),
                },
            ))

        # Families with base contribution but no 4.3A state row → apply
        # insufficient_history weight + suppression
        for fam, info in base_contribs.items():
            if fam in seen_families:
                continue
            base_val, base_source = self._pick_base_contribution(info)
            if base_val is None:
                continue
            state = "insufficient_history"
            seq_class = "insufficient_history"
            state_w = self.compute_transition_state_weight(
                transition_state=state, profile=profile, dependency_family=fam,
            )
            seq_w = self.compute_sequence_class_weight(
                sequence_class=seq_class, profile=profile,
            )
            total_multiplier = _clip(state_w * seq_w, _TOTAL_MULTIPLIER_MIN, _TOTAL_MULTIPLIER_MAX)
            adjusted = base_val * total_multiplier
            items.append(TransitionFamilyAttribution(
                dependency_family=fam,
                raw_family_net_contribution=_as_float(info.get("raw")),
                weighted_family_net_contribution=_as_float(info.get("weighted")),
                regime_adjusted_family_contribution=_as_float(info.get("regime")),
                timing_adjusted_family_contribution=_as_float(info.get("timing")),
                transition_state=state,
                dominant_sequence_class=seq_class,
                transition_state_weight=state_w,
                sequence_class_weight=seq_w,
                transition_bonus=0.0,
                transition_penalty=0.0,
                transition_adjusted_family_contribution=adjusted,
                top_symbols=list(info.get("top_symbols") or []),
                metadata={
                    "scoring_version":          _SCORING_VERSION,
                    "base_contribution_source": base_source,
                    "base_contribution":        base_val,
                    "total_multiplier":         total_multiplier,
                    "no_transition_row":        True,
                },
            ))
        return items

    def rank_transition_families(
        self, items: list[TransitionFamilyAttribution],
    ) -> list[TransitionFamilyAttribution]:
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.transition_adjusted_family_contribution or 0.0),
                -abs(fa.transition_adjusted_family_contribution or 0.0),
                -_TRANSITION_STATE_PREFERENCE.get(fa.transition_state, 0),
                -_FAMILY_PRIORITY.get(fa.dependency_family, 0),
                fa.dependency_family,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.transition_family_rank = i
        return ranked

    def compute_transition_adjusted_symbol_attribution(
        self,
        symbol_scores: dict[str, dict[str, Any]],
        family_rows: Sequence[TransitionFamilyAttribution],
        *,
        profile: dict[str, Any],
    ) -> list[TransitionSymbolAttribution]:
        state_by_family = {fa.dependency_family: fa.transition_state for fa in family_rows}
        sequence_by_family = {fa.dependency_family: fa.dominant_sequence_class for fa in family_rows}

        rows: list[TransitionSymbolAttribution] = []
        for sym, info in symbol_scores.items():
            family = info.get("dependency_family") or "unknown"
            dep_type = info.get("dependency_type")
            state = state_by_family.get(family, "insufficient_history")
            seq_class = sequence_by_family.get(family, "insufficient_history")
            base_val, base_source = self._pick_base_contribution(info)

            state_w = self.compute_transition_state_weight(
                transition_state=state, profile=profile, dependency_family=family,
            )
            seq_w = self.compute_sequence_class_weight(
                sequence_class=seq_class, profile=profile,
            )

            if base_val is None:
                adjusted: float | None = None
                total_multiplier: float | None = None
            else:
                raw_multiplier = state_w * seq_w
                total_multiplier = _clip(raw_multiplier, _TOTAL_MULTIPLIER_MIN, _TOTAL_MULTIPLIER_MAX)
                bonus = self.compute_transition_bonus(
                    transition_state=state, base_contribution=base_val, profile=profile,
                )
                penalty = self.compute_transition_penalty(
                    transition_state=state, base_contribution=base_val, profile=profile,
                )
                adjusted = base_val * total_multiplier + bonus - penalty

            rows.append(TransitionSymbolAttribution(
                symbol=str(sym),
                dependency_family=str(family),
                dependency_type=dep_type,
                transition_state=state,
                dominant_sequence_class=seq_class,
                raw_symbol_score=_as_float(info.get("raw")),
                weighted_symbol_score=_as_float(info.get("weighted")),
                regime_adjusted_symbol_score=_as_float(info.get("regime")),
                timing_adjusted_symbol_score=_as_float(info.get("timing")),
                transition_state_weight=state_w,
                sequence_class_weight=seq_w,
                transition_adjusted_symbol_score=adjusted,
                metadata={
                    "scoring_version":         _SCORING_VERSION,
                    "base_score_source":       base_source,
                    "base_score":              base_val,
                    "total_multiplier":        total_multiplier,
                },
            ))
        return rows

    def rank_transition_symbols(
        self, rows: list[TransitionSymbolAttribution],
    ) -> list[TransitionSymbolAttribution]:
        ranked = sorted(
            rows,
            key=lambda r: (
                -(r.transition_adjusted_symbol_score or 0.0),
                -abs(r.transition_adjusted_symbol_score or 0.0),
                -_TRANSITION_STATE_PREFERENCE.get(r.transition_state, 0),
                r.symbol,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.symbol_rank = i
        return ranked

    # ── orchestration ───────────────────────────────────────────────────
    def build_transition_attribution_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> TransitionAttributionResult | None:
        transition_rows = self._load_transition_state_rows(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        base_contribs = self._load_family_base_contributions(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not transition_rows and not base_contribs:
            return None

        profile_row = self.get_active_transition_profile(conn, workspace_id=workspace_id)
        default_profile_used = profile_row is None
        profile = profile_row if profile_row is not None else dict(_DEFAULT_PROFILE)
        profile_id = profile_row["id"] if profile_row is not None else None
        profile_name = profile.get("profile_name", _DEFAULT_PROFILE["profile_name"])

        sequence_by_family = self._load_sequence_class_by_family(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )

        family_rows = self.compute_transition_adjusted_family_attribution(
            transition_rows, base_contribs, sequence_by_family, profile=profile,
        )
        family_rows = self.rank_transition_families(family_rows)

        symbol_scores = self._load_symbol_base_scores(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        symbol_rows = self.compute_transition_adjusted_symbol_attribution(
            symbol_scores, family_rows, profile=profile,
        )
        symbol_rows = self.rank_transition_symbols(symbol_rows)

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
                "profile_name":                   profile_name,
                "default_transition_profile_used": default_profile_used,
            })
        for sa in symbol_rows:
            sa.metadata.update({
                "profile_name":                   profile_name,
                "default_transition_profile_used": default_profile_used,
            })

        return TransitionAttributionResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snapshot_id,
            transition_profile_id=profile_id,
            profile_name=profile_name,
            default_profile_used=default_profile_used,
            family_rows=family_rows,
            symbol_rows=symbol_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_transition_attribution(
        self, conn, *, result: TransitionAttributionResult,
    ) -> dict[str, int]:
        import src.db.repositories as repo
        fam_count = repo.insert_cross_asset_family_transition_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            transition_profile_id=result.transition_profile_id,
            rows=[
                {
                    "dependency_family":                      fa.dependency_family,
                    "raw_family_net_contribution":            fa.raw_family_net_contribution,
                    "weighted_family_net_contribution":       fa.weighted_family_net_contribution,
                    "regime_adjusted_family_contribution":    fa.regime_adjusted_family_contribution,
                    "timing_adjusted_family_contribution":    fa.timing_adjusted_family_contribution,
                    "transition_state":                       fa.transition_state,
                    "dominant_sequence_class":                fa.dominant_sequence_class,
                    "transition_state_weight":                fa.transition_state_weight,
                    "sequence_class_weight":                  fa.sequence_class_weight,
                    "transition_bonus":                       fa.transition_bonus,
                    "transition_penalty":                     fa.transition_penalty,
                    "transition_adjusted_family_contribution": fa.transition_adjusted_family_contribution,
                    "transition_family_rank":                 fa.transition_family_rank,
                    "top_symbols":                            fa.top_symbols,
                    "metadata":                               fa.metadata,
                }
                for fa in result.family_rows
            ],
        )
        sym_count = repo.insert_cross_asset_symbol_transition_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            transition_profile_id=result.transition_profile_id,
            rows=[
                {
                    "symbol":                           sa.symbol,
                    "dependency_family":                sa.dependency_family,
                    "dependency_type":                  sa.dependency_type,
                    "transition_state":                 sa.transition_state,
                    "dominant_sequence_class":          sa.dominant_sequence_class,
                    "raw_symbol_score":                 sa.raw_symbol_score,
                    "weighted_symbol_score":            sa.weighted_symbol_score,
                    "regime_adjusted_symbol_score":     sa.regime_adjusted_symbol_score,
                    "timing_adjusted_symbol_score":     sa.timing_adjusted_symbol_score,
                    "transition_state_weight":          sa.transition_state_weight,
                    "sequence_class_weight":            sa.sequence_class_weight,
                    "transition_adjusted_symbol_score": sa.transition_adjusted_symbol_score,
                    "symbol_rank":                      sa.symbol_rank,
                    "metadata":                         sa.metadata,
                }
                for sa in result.symbol_rows
            ],
        )
        return {"family_rows": fam_count, "symbol_rows": sym_count}

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> TransitionAttributionResult | None:
        result = self.build_transition_attribution_for_run(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if result is None:
            return None
        self.persist_transition_attribution(conn, result=result)
        return result

    def refresh_workspace_transition_attribution(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[TransitionAttributionResult]:
        """Emit transition-aware attribution for every watchlist. Commits per-
        watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[TransitionAttributionResult] = []
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
                    "cross_asset_transition_attribution: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
