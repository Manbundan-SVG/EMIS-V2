"""Phase 4.4B: Archetype-Aware Attribution Service.

Reads 4.4A archetype classifications and the most mature upstream family
contribution (transition → timing → regime → weighted → raw fallback), then
applies archetype-specific weights + bonuses + penalties. Persists:

  * one cross_asset_family_archetype_attribution_snapshots row per family
  * one cross_asset_symbol_archetype_attribution_snapshots row per symbol

All adjustments are deterministic; the archetype weight is clipped to a
conservative band [0.75, 1.20] so archetype evidence cannot dominate the
upstream attribution chain.
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
_SCORING_VERSION       = "4.4B.v1"

_ARCHETYPE_PREFERENCE: dict[str, int] = {
    "reinforcing_continuation": 5,
    "rotation_handoff":         4,
    "recovering_reentry":       4,
    "deteriorating_breakdown":  3,
    "mixed_transition_noise":   2,
    "insufficient_history":     1,
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
    "profile_name":                     "default_archetype_attribution",
    "rotation_handoff_weight":          1.03,
    "reinforcing_continuation_weight":  1.10,
    "recovering_reentry_weight":        1.05,
    "deteriorating_breakdown_weight":   0.82,
    "mixed_transition_noise_weight":    0.90,
    "insufficient_history_weight":      0.80,
    "recovery_bonus_scale":             1.0,
    "breakdown_penalty_scale":          1.0,
    "rotation_bonus_scale":             1.0,
    "archetype_family_overrides":       {},
}


@dataclass
class ArchetypeFamilyAttribution:
    dependency_family: str
    raw_family_net_contribution: float | None
    weighted_family_net_contribution: float | None
    regime_adjusted_family_contribution: float | None
    timing_adjusted_family_contribution: float | None
    transition_adjusted_family_contribution: float | None
    archetype_key: str
    transition_state: str
    dominant_sequence_class: str
    archetype_weight: float
    archetype_bonus: float
    archetype_penalty: float
    archetype_adjusted_family_contribution: float | None
    archetype_family_rank: int | None = None
    top_symbols: list[str] = field(default_factory=list)
    classification_reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ArchetypeSymbolAttribution:
    symbol: str
    dependency_family: str
    dependency_type: str | None
    archetype_key: str
    transition_state: str
    dominant_sequence_class: str
    raw_symbol_score: float | None
    weighted_symbol_score: float | None
    regime_adjusted_symbol_score: float | None
    timing_adjusted_symbol_score: float | None
    transition_adjusted_symbol_score: float | None
    archetype_weight: float
    archetype_adjusted_symbol_score: float | None
    symbol_rank: int | None = None
    classification_reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ArchetypeAttributionResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    archetype_profile_id: str | None
    profile_name: str
    default_profile_used: bool
    family_rows: list[ArchetypeFamilyAttribution]
    symbol_rows: list[ArchetypeSymbolAttribution]


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


class CrossAssetArchetypeAttributionService:
    """Deterministic archetype-aware refinement of 4.1A/B/C + 4.2B + 4.3B
    family and symbol contributions."""

    # ── profile loading ─────────────────────────────────────────────────
    def get_active_archetype_profile(
        self, conn, *, workspace_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, profile_name, is_active,
                       rotation_handoff_weight, reinforcing_continuation_weight,
                       recovering_reentry_weight, deteriorating_breakdown_weight,
                       mixed_transition_noise_weight, insufficient_history_weight,
                       recovery_bonus_scale, breakdown_penalty_scale, rotation_bonus_scale,
                       archetype_family_overrides, metadata, created_at
                from public.cross_asset_archetype_attribution_profiles
                where workspace_id = %s::uuid and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ── weight primitives ───────────────────────────────────────────────
    def compute_archetype_weight(
        self, *, archetype_key: str, dependency_family: str | None,
        profile: dict[str, Any],
    ) -> float:
        key_map = {
            "rotation_handoff":          "rotation_handoff_weight",
            "reinforcing_continuation":  "reinforcing_continuation_weight",
            "recovering_reentry":        "recovering_reentry_weight",
            "deteriorating_breakdown":   "deteriorating_breakdown_weight",
            "mixed_transition_noise":    "mixed_transition_noise_weight",
            "insufficient_history":      "insufficient_history_weight",
        }
        base = _as_float(profile.get(key_map.get(archetype_key, "insufficient_history_weight")))
        base = base if base is not None else 1.0

        # Per-family override multiplies on top (bounded later by clip).
        overrides = _parse_overrides(profile.get("archetype_family_overrides"))
        fam_override = None
        if dependency_family:
            fam_override = _as_float(overrides.get(dependency_family))
            if fam_override is None:
                # Allow nested {family: {archetype_key: weight}} override form
                nested = overrides.get(dependency_family) if isinstance(overrides.get(dependency_family), dict) else None
                if isinstance(nested, dict):
                    fam_override = _as_float(nested.get(archetype_key))

        combined = base * (fam_override if fam_override is not None else 1.0)
        return _clip(combined, _WEIGHT_MIN, _WEIGHT_MAX)

    def compute_archetype_bonus(
        self, *, archetype_key: str, base_contribution: float | None,
        profile: dict[str, Any],
    ) -> float:
        if base_contribution is None:
            return 0.0
        sign = 1.0 if base_contribution >= 0 else -1.0
        magnitude = abs(base_contribution) * _BONUS_PENALTY_BASE
        if archetype_key == "recovering_reentry":
            scale = _as_float(profile.get("recovery_bonus_scale")) or 1.0
            return sign * magnitude * max(0.0, scale) * 0.4  # ~2% of base
        if archetype_key == "rotation_handoff":
            scale = _as_float(profile.get("rotation_bonus_scale")) or 1.0
            return sign * magnitude * max(0.0, scale) * 0.5  # ~2.5% of base
        return 0.0

    def compute_archetype_penalty(
        self, *, archetype_key: str, base_contribution: float | None,
        profile: dict[str, Any],
    ) -> float:
        """Sign-aware penalty to SUBTRACT. Always reduces magnitude toward zero
        (does not flip the sign of base contribution)."""
        if base_contribution is None:
            return 0.0
        sign = 1.0 if base_contribution >= 0 else -1.0
        magnitude = abs(base_contribution) * _BONUS_PENALTY_BASE
        if archetype_key == "deteriorating_breakdown":
            scale = _as_float(profile.get("breakdown_penalty_scale")) or 1.0
            return sign * magnitude * max(0.0, scale) * 0.6  # ~3% of base
        return 0.0

    # ── input loading ───────────────────────────────────────────────────
    def _load_family_archetypes(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        """Keyed by dependency_family; carries archetype_key, transition_state,
        sequence_class, rank, contribution, and reason codes from 4.4A."""
        out: dict[str, dict[str, Any]] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family, archetype_key,
                       transition_state, dominant_sequence_class,
                       dominant_timing_class, family_rank,
                       family_contribution, archetype_confidence,
                       classification_reason_codes
                from public.cross_asset_family_archetype_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                reasons_raw = d.get("classification_reason_codes") or []
                if isinstance(reasons_raw, str):
                    import json
                    try:
                        reasons_raw = json.loads(reasons_raw)
                    except json.JSONDecodeError:
                        reasons_raw = []
                out[fam] = {
                    "archetype_key":           d.get("archetype_key") or "insufficient_history",
                    "transition_state":        d.get("transition_state") or "insufficient_history",
                    "dominant_sequence_class": d.get("dominant_sequence_class") or "insufficient_history",
                    "dominant_timing_class":   d.get("dominant_timing_class"),
                    "family_rank":             d.get("family_rank"),
                    "family_contribution":     _as_float(d.get("family_contribution")),
                    "archetype_confidence":    _as_float(d.get("archetype_confidence")),
                    "classification_reason_codes": [str(c) for c in reasons_raw],
                }
        return out

    def _load_family_base_contributions(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = defaultdict(lambda: {
            "raw": None, "weighted": None, "regime": None, "timing": None,
            "transition": None, "top_symbols": [],
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
                # 4.3B top_symbols takes precedence if populated
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
            "transition": None,
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
        return out

    @staticmethod
    def _pick_base_contribution(contribs: dict[str, Any]) -> tuple[float | None, str]:
        """Fallback chain: transition → timing → regime → weighted → raw."""
        for key in ("transition", "timing", "regime", "weighted", "raw"):
            v = contribs.get(key)
            if v is not None:
                return (v, key)
        return (None, "none")

    # ── family + symbol builders ────────────────────────────────────────
    def compute_archetype_adjusted_family_attribution(
        self,
        archetype_rows: dict[str, dict[str, Any]],
        base_contribs: dict[str, dict[str, Any]],
        *,
        profile: dict[str, Any],
    ) -> list[ArchetypeFamilyAttribution]:
        items: list[ArchetypeFamilyAttribution] = []
        seen: set[str] = set()

        for fam, arche in archetype_rows.items():
            seen.add(fam)
            archetype_key  = str(arche.get("archetype_key") or "insufficient_history")
            state          = str(arche.get("transition_state") or "insufficient_history")
            seq_class      = str(arche.get("dominant_sequence_class") or "insufficient_history")
            reason_codes   = list(arche.get("classification_reason_codes") or [])

            info = base_contribs.get(fam, {})
            base_val, base_source = self._pick_base_contribution(info)

            weight  = self.compute_archetype_weight(
                archetype_key=archetype_key, dependency_family=fam, profile=profile,
            )
            bonus   = self.compute_archetype_bonus(
                archetype_key=archetype_key, base_contribution=base_val, profile=profile,
            )
            penalty = self.compute_archetype_penalty(
                archetype_key=archetype_key, base_contribution=base_val, profile=profile,
            )

            if base_val is None:
                adjusted: float | None = None
            else:
                adjusted = base_val * weight + bonus - penalty

            items.append(ArchetypeFamilyAttribution(
                dependency_family=fam,
                raw_family_net_contribution=_as_float(info.get("raw")),
                weighted_family_net_contribution=_as_float(info.get("weighted")),
                regime_adjusted_family_contribution=_as_float(info.get("regime")),
                timing_adjusted_family_contribution=_as_float(info.get("timing")),
                transition_adjusted_family_contribution=_as_float(info.get("transition")),
                archetype_key=archetype_key,
                transition_state=state,
                dominant_sequence_class=seq_class,
                archetype_weight=weight,
                archetype_bonus=bonus,
                archetype_penalty=penalty,
                archetype_adjusted_family_contribution=adjusted,
                top_symbols=list(info.get("top_symbols") or []),
                classification_reason_codes=reason_codes,
                metadata={
                    "scoring_version":          _SCORING_VERSION,
                    "base_contribution_source": base_source,
                    "base_contribution":        base_val,
                    "source_family_rank":       arche.get("family_rank"),
                    "archetype_confidence":     arche.get("archetype_confidence"),
                },
            ))

        # Families with base contribution but no 4.4A archetype row →
        # insufficient_history + suppression (no fabricated bonus).
        for fam, info in base_contribs.items():
            if fam in seen:
                continue
            base_val, base_source = self._pick_base_contribution(info)
            if base_val is None:
                continue
            archetype_key = "insufficient_history"
            weight = self.compute_archetype_weight(
                archetype_key=archetype_key, dependency_family=fam, profile=profile,
            )
            adjusted = base_val * weight
            items.append(ArchetypeFamilyAttribution(
                dependency_family=fam,
                raw_family_net_contribution=_as_float(info.get("raw")),
                weighted_family_net_contribution=_as_float(info.get("weighted")),
                regime_adjusted_family_contribution=_as_float(info.get("regime")),
                timing_adjusted_family_contribution=_as_float(info.get("timing")),
                transition_adjusted_family_contribution=_as_float(info.get("transition")),
                archetype_key=archetype_key,
                transition_state="insufficient_history",
                dominant_sequence_class="insufficient_history",
                archetype_weight=weight,
                archetype_bonus=0.0,
                archetype_penalty=0.0,
                archetype_adjusted_family_contribution=adjusted,
                top_symbols=list(info.get("top_symbols") or []),
                classification_reason_codes=["no_archetype_row"],
                metadata={
                    "scoring_version":          _SCORING_VERSION,
                    "base_contribution_source": base_source,
                    "base_contribution":        base_val,
                    "no_archetype_row":         True,
                },
            ))
        return items

    def rank_archetype_families(
        self, items: list[ArchetypeFamilyAttribution],
    ) -> list[ArchetypeFamilyAttribution]:
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.archetype_adjusted_family_contribution or 0.0),
                -abs(fa.archetype_adjusted_family_contribution or 0.0),
                -_ARCHETYPE_PREFERENCE.get(fa.archetype_key, 0),
                -_FAMILY_PRIORITY.get(fa.dependency_family, 0),
                fa.dependency_family,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.archetype_family_rank = i
        return ranked

    def compute_archetype_adjusted_symbol_attribution(
        self,
        symbol_scores: dict[str, dict[str, Any]],
        family_rows: Sequence[ArchetypeFamilyAttribution],
        *,
        profile: dict[str, Any],
    ) -> list[ArchetypeSymbolAttribution]:
        by_fam = {fa.dependency_family: fa for fa in family_rows}
        rows: list[ArchetypeSymbolAttribution] = []
        for sym, info in symbol_scores.items():
            family = info.get("dependency_family") or "unknown"
            dep_type = info.get("dependency_type")
            fam_row = by_fam.get(str(family))
            archetype_key = fam_row.archetype_key if fam_row else "insufficient_history"
            state         = fam_row.transition_state if fam_row else "insufficient_history"
            seq_class     = fam_row.dominant_sequence_class if fam_row else "insufficient_history"
            reason_codes  = list(fam_row.classification_reason_codes) if fam_row else ["no_archetype_row"]

            base_val, base_source = self._pick_base_contribution(info)
            weight = self.compute_archetype_weight(
                archetype_key=archetype_key, dependency_family=str(family), profile=profile,
            )
            if base_val is None:
                adjusted: float | None = None
            else:
                bonus = self.compute_archetype_bonus(
                    archetype_key=archetype_key, base_contribution=base_val, profile=profile,
                )
                penalty = self.compute_archetype_penalty(
                    archetype_key=archetype_key, base_contribution=base_val, profile=profile,
                )
                adjusted = base_val * weight + bonus - penalty

            rows.append(ArchetypeSymbolAttribution(
                symbol=str(sym),
                dependency_family=str(family),
                dependency_type=dep_type,
                archetype_key=archetype_key,
                transition_state=state,
                dominant_sequence_class=seq_class,
                raw_symbol_score=_as_float(info.get("raw")),
                weighted_symbol_score=_as_float(info.get("weighted")),
                regime_adjusted_symbol_score=_as_float(info.get("regime")),
                timing_adjusted_symbol_score=_as_float(info.get("timing")),
                transition_adjusted_symbol_score=_as_float(info.get("transition")),
                archetype_weight=weight,
                archetype_adjusted_symbol_score=adjusted,
                classification_reason_codes=reason_codes,
                metadata={
                    "scoring_version":     _SCORING_VERSION,
                    "base_score_source":   base_source,
                    "base_score":          base_val,
                },
            ))
        return rows

    def rank_archetype_symbols(
        self, rows: list[ArchetypeSymbolAttribution],
    ) -> list[ArchetypeSymbolAttribution]:
        ranked = sorted(
            rows,
            key=lambda r: (
                -(r.archetype_adjusted_symbol_score or 0.0),
                -abs(r.archetype_adjusted_symbol_score or 0.0),
                -_ARCHETYPE_PREFERENCE.get(r.archetype_key, 0),
                r.symbol,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.symbol_rank = i
        return ranked

    # ── orchestration ───────────────────────────────────────────────────
    def build_archetype_attribution_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> ArchetypeAttributionResult | None:
        archetype_rows = self._load_family_archetypes(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        base_contribs = self._load_family_base_contributions(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not archetype_rows and not base_contribs:
            return None

        profile_row = self.get_active_archetype_profile(conn, workspace_id=workspace_id)
        default_profile_used = profile_row is None
        profile = profile_row if profile_row is not None else dict(_DEFAULT_PROFILE)
        profile_id = profile_row["id"] if profile_row is not None else None
        profile_name = profile.get("profile_name", _DEFAULT_PROFILE["profile_name"])

        family_rows = self.compute_archetype_adjusted_family_attribution(
            archetype_rows, base_contribs, profile=profile,
        )
        family_rows = self.rank_archetype_families(family_rows)

        symbol_scores = self._load_symbol_base_scores(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        symbol_rows = self.compute_archetype_adjusted_symbol_attribution(
            symbol_scores, family_rows, profile=profile,
        )
        symbol_rows = self.rank_archetype_symbols(symbol_rows)

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
                "profile_name":                    profile_name,
                "default_archetype_profile_used":  default_profile_used,
            })
        for sa in symbol_rows:
            sa.metadata.update({
                "profile_name":                    profile_name,
                "default_archetype_profile_used":  default_profile_used,
            })

        return ArchetypeAttributionResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snapshot_id,
            archetype_profile_id=profile_id,
            profile_name=profile_name,
            default_profile_used=default_profile_used,
            family_rows=family_rows,
            symbol_rows=symbol_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_archetype_attribution(
        self, conn, *, result: ArchetypeAttributionResult,
    ) -> dict[str, int]:
        import src.db.repositories as repo
        fam_count = repo.insert_cross_asset_family_archetype_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            archetype_profile_id=result.archetype_profile_id,
            rows=[
                {
                    "dependency_family":                       fa.dependency_family,
                    "raw_family_net_contribution":             fa.raw_family_net_contribution,
                    "weighted_family_net_contribution":        fa.weighted_family_net_contribution,
                    "regime_adjusted_family_contribution":     fa.regime_adjusted_family_contribution,
                    "timing_adjusted_family_contribution":     fa.timing_adjusted_family_contribution,
                    "transition_adjusted_family_contribution": fa.transition_adjusted_family_contribution,
                    "archetype_key":                           fa.archetype_key,
                    "transition_state":                        fa.transition_state,
                    "dominant_sequence_class":                 fa.dominant_sequence_class,
                    "archetype_weight":                        fa.archetype_weight,
                    "archetype_bonus":                         fa.archetype_bonus,
                    "archetype_penalty":                       fa.archetype_penalty,
                    "archetype_adjusted_family_contribution":  fa.archetype_adjusted_family_contribution,
                    "archetype_family_rank":                   fa.archetype_family_rank,
                    "top_symbols":                             fa.top_symbols,
                    "classification_reason_codes":             fa.classification_reason_codes,
                    "metadata":                                fa.metadata,
                }
                for fa in result.family_rows
            ],
        )
        sym_count = repo.insert_cross_asset_symbol_archetype_attribution_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            archetype_profile_id=result.archetype_profile_id,
            rows=[
                {
                    "symbol":                           sa.symbol,
                    "dependency_family":                sa.dependency_family,
                    "dependency_type":                  sa.dependency_type,
                    "archetype_key":                    sa.archetype_key,
                    "transition_state":                 sa.transition_state,
                    "dominant_sequence_class":          sa.dominant_sequence_class,
                    "raw_symbol_score":                 sa.raw_symbol_score,
                    "weighted_symbol_score":            sa.weighted_symbol_score,
                    "regime_adjusted_symbol_score":     sa.regime_adjusted_symbol_score,
                    "timing_adjusted_symbol_score":     sa.timing_adjusted_symbol_score,
                    "transition_adjusted_symbol_score": sa.transition_adjusted_symbol_score,
                    "archetype_weight":                 sa.archetype_weight,
                    "archetype_adjusted_symbol_score":  sa.archetype_adjusted_symbol_score,
                    "symbol_rank":                      sa.symbol_rank,
                    "classification_reason_codes":      sa.classification_reason_codes,
                    "metadata":                         sa.metadata,
                }
                for sa in result.symbol_rows
            ],
        )
        return {"family_rows": fam_count, "symbol_rows": sym_count}

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> ArchetypeAttributionResult | None:
        result = self.build_archetype_attribution_for_run(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if result is None:
            return None
        self.persist_archetype_attribution(conn, result=result)
        return result

    def refresh_workspace_archetype_attribution(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[ArchetypeAttributionResult]:
        """Emit archetype-aware attribution for every watchlist. Commits
        per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[ArchetypeAttributionResult] = []
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
                    "cross_asset_archetype_attribution: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
