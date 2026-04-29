"""Phase 4.8B: Conflict-Aware Attribution.

Refines cross-asset family + symbol contribution by conditioning on the
layer-consensus state, agreement score, conflict score, and dominant
conflict source from the live 4.8A diagnostics. Sits on top of the 4.7B
decay-aware attribution (and falls back through persistence / cluster /
archetype / transition / timing / regime / weighted / raw layers).

Persists:
  * cross_asset_family_conflict_attribution_snapshots (per-run/family)
  * cross_asset_symbol_conflict_attribution_snapshots (per-run/symbol)

All weights, bonuses, penalties, and bounds are deterministic and
metadata-stamped. No predictive forecasting in this phase.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.8B.v1"

# Default profile values mirror migration defaults so the service is safe
# against fresh installs lacking an active profile row.
_DEFAULT_ALIGNED_SUPPORTIVE_WEIGHT       = 1.08
_DEFAULT_ALIGNED_SUPPRESSIVE_WEIGHT      = 0.78
_DEFAULT_PARTIAL_AGREEMENT_WEIGHT        = 0.96
_DEFAULT_CONFLICTED_WEIGHT               = 0.72
_DEFAULT_UNRELIABLE_WEIGHT               = 0.65
_DEFAULT_INSUFFICIENT_CONTEXT_WEIGHT     = 0.80
_DEFAULT_AGREEMENT_BONUS_SCALE           = 1.0
_DEFAULT_CONFLICT_PENALTY_SCALE          = 1.0
_DEFAULT_UNRELIABLE_PENALTY_SCALE        = 1.0

# Conservative multiplier band — keeps conflict influence bounded.
_MIN_CONFLICT_MULTIPLIER  = 0.60
_MAX_CONFLICT_MULTIPLIER  = 1.20

# Bonus / penalty bases — small, explicit constants.
_AGREEMENT_BONUS_BASE        = 0.02
_CONFLICT_PENALTY_BASE       = 0.06
_UNRELIABLE_PENALTY_BASE     = 0.05
_DOMINANT_SOURCE_PENALTY     = 0.02

# Soft floor for conflicted / unreliable contributions — never invert sign,
# just damp magnitude.
_CONFLICT_FLOOR_SCALE = 0.55

# Consensus preference order for tie-breaks (higher = preferred).
_CONSENSUS_PREFERENCE = {
    "aligned_supportive":   5,
    "partial_agreement":    4,
    "aligned_suppressive":  3,
    "conflicted":           2,
    "insufficient_context": 1,
    "unreliable":           0,
}

_VALID_CONSENSUS_STATES = set(_CONSENSUS_PREFERENCE.keys())


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _as_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _safe_consensus_state(s: Any) -> str:
    raw = str(s or "").strip()
    if raw in _VALID_CONSENSUS_STATES:
        return raw
    return "insufficient_context"


@dataclass
class ConflictAttributionProfile:
    id: str | None
    workspace_id: str
    profile_name: str
    is_active: bool
    aligned_supportive_weight: float
    aligned_suppressive_weight: float
    partial_agreement_weight: float
    conflicted_weight: float
    unreliable_weight: float
    insufficient_context_weight: float
    agreement_bonus_scale: float
    conflict_penalty_scale: float
    unreliable_penalty_scale: float
    dominant_conflict_source_penalties: dict[str, Any] = field(default_factory=dict)
    conflict_family_overrides: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def default(cls, workspace_id: str) -> "ConflictAttributionProfile":
        return cls(
            id=None,
            workspace_id=workspace_id,
            profile_name="default",
            is_active=True,
            aligned_supportive_weight=_DEFAULT_ALIGNED_SUPPORTIVE_WEIGHT,
            aligned_suppressive_weight=_DEFAULT_ALIGNED_SUPPRESSIVE_WEIGHT,
            partial_agreement_weight=_DEFAULT_PARTIAL_AGREEMENT_WEIGHT,
            conflicted_weight=_DEFAULT_CONFLICTED_WEIGHT,
            unreliable_weight=_DEFAULT_UNRELIABLE_WEIGHT,
            insufficient_context_weight=_DEFAULT_INSUFFICIENT_CONTEXT_WEIGHT,
            agreement_bonus_scale=_DEFAULT_AGREEMENT_BONUS_SCALE,
            conflict_penalty_scale=_DEFAULT_CONFLICT_PENALTY_SCALE,
            unreliable_penalty_scale=_DEFAULT_UNRELIABLE_PENALTY_SCALE,
            dominant_conflict_source_penalties={},
            conflict_family_overrides={},
            metadata={"default_conflict_profile_used": True},
        )


@dataclass
class FamilyConflictAttributionSnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    conflict_profile_id: str | None
    dependency_family: str
    raw_family_net_contribution: float | None
    weighted_family_net_contribution: float | None
    regime_adjusted_family_contribution: float | None
    timing_adjusted_family_contribution: float | None
    transition_adjusted_family_contribution: float | None
    archetype_adjusted_family_contribution: float | None
    cluster_adjusted_family_contribution: float | None
    persistence_adjusted_family_contribution: float | None
    decay_adjusted_family_contribution: float | None
    family_consensus_state: str
    agreement_score: float | None
    conflict_score: float | None
    dominant_conflict_source: str | None
    transition_direction: str | None
    archetype_direction: str | None
    cluster_direction: str | None
    persistence_direction: str | None
    decay_direction: str | None
    conflict_weight: float | None
    conflict_bonus: float | None
    conflict_penalty: float | None
    conflict_adjusted_family_contribution: float | None
    conflict_family_rank: int | None
    top_symbols: list[str]
    reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SymbolConflictAttributionSnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    conflict_profile_id: str | None
    symbol: str
    dependency_family: str
    dependency_type: str | None
    family_consensus_state: str
    agreement_score: float | None
    conflict_score: float | None
    dominant_conflict_source: str | None
    raw_symbol_score: float | None
    weighted_symbol_score: float | None
    regime_adjusted_symbol_score: float | None
    timing_adjusted_symbol_score: float | None
    transition_adjusted_symbol_score: float | None
    archetype_adjusted_symbol_score: float | None
    cluster_adjusted_symbol_score: float | None
    persistence_adjusted_symbol_score: float | None
    decay_adjusted_symbol_score: float | None
    conflict_weight: float | None
    conflict_adjusted_symbol_score: float | None
    symbol_rank: int | None
    reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


class CrossAssetConflictAttributionService:
    """Deterministic conflict-aware family + symbol attribution refinement."""

    # ── profile ─────────────────────────────────────────────────────────
    def get_active_conflict_profile(
        self, conn, *, workspace_id: str,
    ) -> ConflictAttributionProfile:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, workspace_id::text as workspace_id,
                       profile_name, is_active,
                       aligned_supportive_weight, aligned_suppressive_weight,
                       partial_agreement_weight, conflicted_weight,
                       unreliable_weight, insufficient_context_weight,
                       agreement_bonus_scale, conflict_penalty_scale,
                       unreliable_penalty_scale,
                       dominant_conflict_source_penalties,
                       conflict_family_overrides,
                       metadata
                from public.cross_asset_conflict_attribution_profiles
                where workspace_id = %s::uuid
                  and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            if not row:
                return ConflictAttributionProfile.default(workspace_id)
            d = dict(row)
            return ConflictAttributionProfile(
                id=d.get("id"),
                workspace_id=d.get("workspace_id") or workspace_id,
                profile_name=d.get("profile_name") or "active",
                is_active=bool(d.get("is_active", True)),
                aligned_supportive_weight=float(
                    d.get("aligned_supportive_weight") or _DEFAULT_ALIGNED_SUPPORTIVE_WEIGHT
                ),
                aligned_suppressive_weight=float(
                    d.get("aligned_suppressive_weight") or _DEFAULT_ALIGNED_SUPPRESSIVE_WEIGHT
                ),
                partial_agreement_weight=float(
                    d.get("partial_agreement_weight") or _DEFAULT_PARTIAL_AGREEMENT_WEIGHT
                ),
                conflicted_weight=float(
                    d.get("conflicted_weight") or _DEFAULT_CONFLICTED_WEIGHT
                ),
                unreliable_weight=float(
                    d.get("unreliable_weight") or _DEFAULT_UNRELIABLE_WEIGHT
                ),
                insufficient_context_weight=float(
                    d.get("insufficient_context_weight") or _DEFAULT_INSUFFICIENT_CONTEXT_WEIGHT
                ),
                agreement_bonus_scale=float(
                    d.get("agreement_bonus_scale") or _DEFAULT_AGREEMENT_BONUS_SCALE
                ),
                conflict_penalty_scale=float(
                    d.get("conflict_penalty_scale") or _DEFAULT_CONFLICT_PENALTY_SCALE
                ),
                unreliable_penalty_scale=float(
                    d.get("unreliable_penalty_scale") or _DEFAULT_UNRELIABLE_PENALTY_SCALE
                ),
                dominant_conflict_source_penalties=dict(
                    d.get("dominant_conflict_source_penalties") or {}
                ),
                conflict_family_overrides=dict(d.get("conflict_family_overrides") or {}),
                metadata=dict(d.get("metadata") or {}),
            )

    # ── primitives ──────────────────────────────────────────────────────
    @staticmethod
    def compute_conflict_weight(
        *,
        consensus_state: str,
        profile: ConflictAttributionProfile,
        family: str | None = None,
    ) -> float:
        """Per-state weight, optionally overridden per-family in the profile."""
        state = _safe_consensus_state(consensus_state)
        overrides = (profile.conflict_family_overrides or {}).get(family or "", {}) if family else {}
        weights = {
            "aligned_supportive":   profile.aligned_supportive_weight,
            "aligned_suppressive":  profile.aligned_suppressive_weight,
            "partial_agreement":    profile.partial_agreement_weight,
            "conflicted":           profile.conflicted_weight,
            "unreliable":           profile.unreliable_weight,
            "insufficient_context": profile.insufficient_context_weight,
        }
        base = float(overrides.get(state, weights.get(state, profile.insufficient_context_weight)))
        return _clip(base, _MIN_CONFLICT_MULTIPLIER, _MAX_CONFLICT_MULTIPLIER)

    @staticmethod
    def compute_agreement_bonus(
        *,
        consensus_state: str,
        agreement_score: float | None,
        profile: ConflictAttributionProfile,
    ) -> float:
        """Only aligned_supportive stacks earn an agreement bonus, scaled by
        the agreement score. All other states return 0.

        Aligned suppressive, conflicted, unreliable, partial_agreement, and
        insufficient_context never receive a constructive boost — by design.
        """
        if _safe_consensus_state(consensus_state) != "aligned_supportive":
            return 0.0
        score = agreement_score if agreement_score is not None else 0.0
        bonus = _AGREEMENT_BONUS_BASE * max(0.0, min(1.0, score)) * profile.agreement_bonus_scale
        return max(0.0, bonus)

    @staticmethod
    def compute_conflict_penalty(
        *,
        consensus_state: str,
        conflict_score: float | None,
        dominant_conflict_source: str | None,
        profile: ConflictAttributionProfile,
    ) -> float:
        """Conflict-driven penalty.

        Fires for ``conflicted`` (always) and for any state where
        conflict_score is materially > 0. Dominant-conflict-source penalty
        adds a small explicit hit when configured.
        """
        state = _safe_consensus_state(consensus_state)
        penalty = 0.0
        score = conflict_score if conflict_score is not None else 0.0
        if state in ("conflicted", "aligned_suppressive", "partial_agreement"):
            penalty += _CONFLICT_PENALTY_BASE * max(0.0, min(1.0, score)) * profile.conflict_penalty_scale
        elif state == "unreliable" and score > 0.0:
            penalty += _CONFLICT_PENALTY_BASE * max(0.0, min(1.0, score)) * profile.conflict_penalty_scale * 0.5
        # Dominant-conflict-source penalty (small, explicit, configurable).
        if dominant_conflict_source:
            src_pen = profile.dominant_conflict_source_penalties.get(
                str(dominant_conflict_source), _DOMINANT_SOURCE_PENALTY,
            )
            try:
                penalty += float(src_pen)
            except (TypeError, ValueError):
                penalty += _DOMINANT_SOURCE_PENALTY
        return max(0.0, penalty)

    @staticmethod
    def compute_unreliable_penalty(
        *, consensus_state: str, profile: ConflictAttributionProfile,
    ) -> float:
        """Strongest penalty — unreliable stacks always get an explicit hit."""
        if _safe_consensus_state(consensus_state) != "unreliable":
            return 0.0
        return _UNRELIABLE_PENALTY_BASE * profile.unreliable_penalty_scale

    # ── inputs ──────────────────────────────────────────────────────────
    def _load_run_layer_conflict(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text       as run_id,
                       watchlist_id::text as watchlist_id,
                       layer_consensus_state, agreement_score, conflict_score,
                       dominant_conflict_source,
                       freshness_state, persistence_state, cluster_state,
                       latest_conflict_event_type
                from public.run_cross_asset_layer_conflict_summary
                where run_id = %s::uuid
                  and workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                limit 1
                """,
                (run_id, workspace_id, watchlist_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_family_layer_agreement(
        self, conn, *, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       family_consensus_state, agreement_score, conflict_score,
                       dominant_conflict_source,
                       transition_direction, archetype_direction, cluster_direction,
                       persistence_direction, decay_direction,
                       family_rank
                from public.cross_asset_family_layer_agreement_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = d.get("dependency_family")
                if fam:
                    out[str(fam)] = d
        return out

    def _load_family_decay_attribution(
        self, conn, *, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       context_snapshot_id::text as context_snapshot_id,
                       raw_family_net_contribution,
                       weighted_family_net_contribution,
                       regime_adjusted_family_contribution,
                       timing_adjusted_family_contribution,
                       transition_adjusted_family_contribution,
                       archetype_adjusted_family_contribution,
                       cluster_adjusted_family_contribution,
                       persistence_adjusted_family_contribution,
                       decay_adjusted_family_contribution,
                       top_symbols
                from public.cross_asset_family_decay_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_symbol_decay_attribution(
        self, conn, *, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select symbol, dependency_family, dependency_type,
                       context_snapshot_id::text as context_snapshot_id,
                       raw_symbol_score, weighted_symbol_score,
                       regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                       transition_adjusted_symbol_score, archetype_adjusted_symbol_score,
                       cluster_adjusted_symbol_score, persistence_adjusted_symbol_score,
                       decay_adjusted_symbol_score
                from public.cross_asset_symbol_decay_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            return [dict(r) for r in cur.fetchall()]

    # ── core computation ────────────────────────────────────────────────
    @staticmethod
    def _pick_base_contribution(row: dict[str, Any]) -> float | None:
        """Walk the upstream attribution stack newest-to-oldest until we find
        a usable numeric contribution."""
        for key in (
            "decay_adjusted_family_contribution",
            "persistence_adjusted_family_contribution",
            "cluster_adjusted_family_contribution",
            "archetype_adjusted_family_contribution",
            "transition_adjusted_family_contribution",
            "timing_adjusted_family_contribution",
            "regime_adjusted_family_contribution",
            "weighted_family_net_contribution",
            "raw_family_net_contribution",
        ):
            v = _as_float(row.get(key))
            if v is not None:
                return v
        return None

    @staticmethod
    def _pick_base_symbol_score(row: dict[str, Any]) -> float | None:
        for key in (
            "decay_adjusted_symbol_score",
            "persistence_adjusted_symbol_score",
            "cluster_adjusted_symbol_score",
            "archetype_adjusted_symbol_score",
            "transition_adjusted_symbol_score",
            "timing_adjusted_symbol_score",
            "regime_adjusted_symbol_score",
            "weighted_symbol_score",
            "raw_symbol_score",
        ):
            v = _as_float(row.get(key))
            if v is not None:
                return v
        return None

    def compute_conflict_adjusted_family_attribution(
        self,
        *,
        family_row: dict[str, Any],
        run_conflict: dict[str, Any] | None,
        family_conflict: dict[str, Any] | None,
        profile: ConflictAttributionProfile,
    ) -> dict[str, Any]:
        family = str(family_row.get("dependency_family") or "")

        # Prefer family-level layer agreement; fall back to run-level.
        if family_conflict:
            consensus_state = _safe_consensus_state(family_conflict.get("family_consensus_state"))
            agreement_score = _as_float(family_conflict.get("agreement_score"))
            conflict_score  = _as_float(family_conflict.get("conflict_score"))
            dominant_source = family_conflict.get("dominant_conflict_source")
            transition_dir  = family_conflict.get("transition_direction")
            archetype_dir   = family_conflict.get("archetype_direction")
            cluster_dir     = family_conflict.get("cluster_direction")
            persistence_dir = family_conflict.get("persistence_direction")
            decay_dir       = family_conflict.get("decay_direction")
        else:
            consensus_state = "insufficient_context"
            agreement_score = None
            conflict_score  = None
            dominant_source = None
            transition_dir = archetype_dir = cluster_dir = None
            persistence_dir = decay_dir = None

        # If family-level missing but run-level layer consensus is known,
        # inherit run-level state.
        if consensus_state == "insufficient_context" and run_conflict:
            run_state = _safe_consensus_state(run_conflict.get("layer_consensus_state"))
            if run_state != "insufficient_context":
                consensus_state = run_state
                if agreement_score is None:
                    agreement_score = _as_float(run_conflict.get("agreement_score"))
                if conflict_score is None:
                    conflict_score = _as_float(run_conflict.get("conflict_score"))
                if not dominant_source:
                    dominant_source = run_conflict.get("dominant_conflict_source")

        conflict_weight = self.compute_conflict_weight(
            consensus_state=consensus_state, profile=profile, family=family,
        )
        conflict_bonus = self.compute_agreement_bonus(
            consensus_state=consensus_state,
            agreement_score=agreement_score,
            profile=profile,
        )
        conflict_penalty = (
            self.compute_conflict_penalty(
                consensus_state=consensus_state,
                conflict_score=conflict_score,
                dominant_conflict_source=dominant_source,
                profile=profile,
            )
            + self.compute_unreliable_penalty(
                consensus_state=consensus_state, profile=profile,
            )
        )

        base = self._pick_base_contribution(family_row)
        conflict_adjusted = base
        if base is not None:
            conflict_adjusted = base * conflict_weight + conflict_bonus - conflict_penalty
            # Soft-floor conflicted / unreliable contributions toward zero.
            if consensus_state in ("conflicted", "unreliable"):
                if base > 0:
                    conflict_adjusted = min(conflict_adjusted, base * _CONFLICT_FLOOR_SCALE)
                elif base < 0:
                    conflict_adjusted = max(conflict_adjusted, base * _CONFLICT_FLOOR_SCALE)

        reason_codes: list[str] = []
        if consensus_state == "aligned_supportive":
            reason_codes.append("aligned_supportive_supports_contribution")
        if consensus_state == "aligned_suppressive":
            reason_codes.append("aligned_suppressive_caution")
        if consensus_state == "partial_agreement":
            reason_codes.append("partial_agreement_mild_caution")
        if consensus_state == "conflicted":
            reason_codes.append("conflict_penalty_applied")
            reason_codes.append("conflict_floor_applied")
        if consensus_state == "unreliable":
            reason_codes.append("unreliable_penalty_applied")
            reason_codes.append("conflict_floor_applied")
        if consensus_state == "insufficient_context":
            reason_codes.append("insufficient_context_neutral_safe_suppression")
        if dominant_source:
            reason_codes.append(f"dominant_conflict_source:{dominant_source}")
        if base is None:
            reason_codes.append("no_upstream_contribution_available")
        if profile.id is None:
            reason_codes.append("default_conflict_profile_used")

        top_symbols_raw = family_row.get("top_symbols") or []
        if isinstance(top_symbols_raw, str):
            top_symbols: list[str] = [top_symbols_raw]
        else:
            top_symbols = [str(s) for s in list(top_symbols_raw)[:8]]

        return {
            "dependency_family":                       family,
            "raw_family_net_contribution":             _as_float(family_row.get("raw_family_net_contribution")),
            "weighted_family_net_contribution":        _as_float(family_row.get("weighted_family_net_contribution")),
            "regime_adjusted_family_contribution":     _as_float(family_row.get("regime_adjusted_family_contribution")),
            "timing_adjusted_family_contribution":     _as_float(family_row.get("timing_adjusted_family_contribution")),
            "transition_adjusted_family_contribution": _as_float(family_row.get("transition_adjusted_family_contribution")),
            "archetype_adjusted_family_contribution":  _as_float(family_row.get("archetype_adjusted_family_contribution")),
            "cluster_adjusted_family_contribution":    _as_float(family_row.get("cluster_adjusted_family_contribution")),
            "persistence_adjusted_family_contribution": _as_float(family_row.get("persistence_adjusted_family_contribution")),
            "decay_adjusted_family_contribution":      _as_float(family_row.get("decay_adjusted_family_contribution")),
            "context_snapshot_id":                     family_row.get("context_snapshot_id"),
            "family_consensus_state":                  consensus_state,
            "agreement_score":                         agreement_score,
            "conflict_score":                          conflict_score,
            "dominant_conflict_source":                dominant_source,
            "transition_direction":                    transition_dir,
            "archetype_direction":                     archetype_dir,
            "cluster_direction":                       cluster_dir,
            "persistence_direction":                   persistence_dir,
            "decay_direction":                         decay_dir,
            "conflict_weight":                         conflict_weight,
            "conflict_bonus":                          conflict_bonus,
            "conflict_penalty":                        conflict_penalty,
            "conflict_adjusted_family_contribution":   conflict_adjusted,
            "top_symbols":                             top_symbols,
            "reason_codes":                            list(dict.fromkeys(reason_codes)),
            "base_contribution_used":                  base,
        }

    def compute_conflict_adjusted_symbol_attribution(
        self,
        *,
        symbol_row: dict[str, Any],
        family_conflict: dict[str, dict[str, Any]],
        run_conflict: dict[str, Any] | None,
        profile: ConflictAttributionProfile,
    ) -> dict[str, Any]:
        family = str(symbol_row.get("dependency_family") or "")
        fam_ctx = family_conflict.get(family) or {}

        if fam_ctx:
            consensus_state = _safe_consensus_state(fam_ctx.get("family_consensus_state"))
            agreement_score = _as_float(fam_ctx.get("agreement_score"))
            conflict_score  = _as_float(fam_ctx.get("conflict_score"))
            dominant_source = fam_ctx.get("dominant_conflict_source")
        else:
            consensus_state = "insufficient_context"
            agreement_score = None
            conflict_score  = None
            dominant_source = None

        if consensus_state == "insufficient_context" and run_conflict:
            run_state = _safe_consensus_state(run_conflict.get("layer_consensus_state"))
            if run_state != "insufficient_context":
                consensus_state = run_state
                if agreement_score is None:
                    agreement_score = _as_float(run_conflict.get("agreement_score"))
                if conflict_score is None:
                    conflict_score = _as_float(run_conflict.get("conflict_score"))
                if not dominant_source:
                    dominant_source = run_conflict.get("dominant_conflict_source")

        conflict_weight = self.compute_conflict_weight(
            consensus_state=consensus_state, profile=profile, family=family,
        )
        conflict_bonus = self.compute_agreement_bonus(
            consensus_state=consensus_state,
            agreement_score=agreement_score,
            profile=profile,
        )
        conflict_penalty = (
            self.compute_conflict_penalty(
                consensus_state=consensus_state,
                conflict_score=conflict_score,
                dominant_conflict_source=dominant_source,
                profile=profile,
            )
            + self.compute_unreliable_penalty(
                consensus_state=consensus_state, profile=profile,
            )
        )

        base = self._pick_base_symbol_score(symbol_row)
        conflict_adjusted = base
        if base is not None:
            conflict_adjusted = base * conflict_weight + conflict_bonus - conflict_penalty
            if consensus_state in ("conflicted", "unreliable"):
                if base > 0:
                    conflict_adjusted = min(conflict_adjusted, base * _CONFLICT_FLOOR_SCALE)
                elif base < 0:
                    conflict_adjusted = max(conflict_adjusted, base * _CONFLICT_FLOOR_SCALE)

        reason_codes: list[str] = []
        if consensus_state == "aligned_supportive":
            reason_codes.append("aligned_supportive_supports_contribution")
        if consensus_state == "aligned_suppressive":
            reason_codes.append("aligned_suppressive_caution")
        if consensus_state == "conflicted":
            reason_codes.append("conflict_penalty_applied")
        if consensus_state == "unreliable":
            reason_codes.append("unreliable_penalty_applied")
        if base is None:
            reason_codes.append("no_upstream_symbol_score_available")
        if profile.id is None:
            reason_codes.append("default_conflict_profile_used")

        return {
            "symbol":                            str(symbol_row.get("symbol") or ""),
            "dependency_family":                 family,
            "dependency_type":                   symbol_row.get("dependency_type"),
            "context_snapshot_id":               symbol_row.get("context_snapshot_id"),
            "family_consensus_state":            consensus_state,
            "agreement_score":                   agreement_score,
            "conflict_score":                    conflict_score,
            "dominant_conflict_source":          dominant_source,
            "raw_symbol_score":                  _as_float(symbol_row.get("raw_symbol_score")),
            "weighted_symbol_score":             _as_float(symbol_row.get("weighted_symbol_score")),
            "regime_adjusted_symbol_score":      _as_float(symbol_row.get("regime_adjusted_symbol_score")),
            "timing_adjusted_symbol_score":      _as_float(symbol_row.get("timing_adjusted_symbol_score")),
            "transition_adjusted_symbol_score":  _as_float(symbol_row.get("transition_adjusted_symbol_score")),
            "archetype_adjusted_symbol_score":   _as_float(symbol_row.get("archetype_adjusted_symbol_score")),
            "cluster_adjusted_symbol_score":     _as_float(symbol_row.get("cluster_adjusted_symbol_score")),
            "persistence_adjusted_symbol_score": _as_float(symbol_row.get("persistence_adjusted_symbol_score")),
            "decay_adjusted_symbol_score":       _as_float(symbol_row.get("decay_adjusted_symbol_score")),
            "conflict_weight":                   conflict_weight,
            "conflict_adjusted_symbol_score":    conflict_adjusted,
            "reason_codes":                      list(dict.fromkeys(reason_codes)),
            "base_score_used":                   base,
        }

    @staticmethod
    def rank_conflict_families(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Deterministic ranking by conflict-adjusted contribution desc,
        absolute contribution desc, consensus-state preference desc,
        family name asc."""
        def _key(r: dict[str, Any]) -> tuple:
            v = r.get("conflict_adjusted_family_contribution")
            v_f = float(v) if v is not None else float("-inf")
            absv = abs(v_f) if v_f != float("-inf") else 0.0
            pref = _CONSENSUS_PREFERENCE.get(_safe_consensus_state(r.get("family_consensus_state")), 0)
            return (-v_f, -absv, -pref, str(r.get("dependency_family") or ""))
        return sorted(rows, key=_key)

    @staticmethod
    def rank_conflict_symbols(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        def _key(r: dict[str, Any]) -> tuple:
            v = r.get("conflict_adjusted_symbol_score")
            v_f = float(v) if v is not None else float("-inf")
            absv = abs(v_f) if v_f != float("-inf") else 0.0
            pref = _CONSENSUS_PREFERENCE.get(_safe_consensus_state(r.get("family_consensus_state")), 0)
            return (-v_f, -absv, -pref, str(r.get("symbol") or ""))
        return sorted(rows, key=_key)

    # ── persistence ─────────────────────────────────────────────────────
    def _persist_family_snapshots(
        self, conn, *, snaps: list[FamilyConflictAttributionSnapshot],
    ) -> list[str]:
        if not snaps:
            return []
        import src.db.repositories_48b as repo
        ids: list[str] = []
        for snap in snaps:
            row = repo.insert_cross_asset_family_conflict_attribution_snapshots(
                conn,
                workspace_id=snap.workspace_id,
                watchlist_id=snap.watchlist_id,
                run_id=snap.run_id,
                context_snapshot_id=snap.context_snapshot_id,
                conflict_profile_id=snap.conflict_profile_id,
                dependency_family=snap.dependency_family,
                raw_family_net_contribution=snap.raw_family_net_contribution,
                weighted_family_net_contribution=snap.weighted_family_net_contribution,
                regime_adjusted_family_contribution=snap.regime_adjusted_family_contribution,
                timing_adjusted_family_contribution=snap.timing_adjusted_family_contribution,
                transition_adjusted_family_contribution=snap.transition_adjusted_family_contribution,
                archetype_adjusted_family_contribution=snap.archetype_adjusted_family_contribution,
                cluster_adjusted_family_contribution=snap.cluster_adjusted_family_contribution,
                persistence_adjusted_family_contribution=snap.persistence_adjusted_family_contribution,
                decay_adjusted_family_contribution=snap.decay_adjusted_family_contribution,
                family_consensus_state=snap.family_consensus_state,
                agreement_score=snap.agreement_score,
                conflict_score=snap.conflict_score,
                dominant_conflict_source=snap.dominant_conflict_source,
                transition_direction=snap.transition_direction,
                archetype_direction=snap.archetype_direction,
                cluster_direction=snap.cluster_direction,
                persistence_direction=snap.persistence_direction,
                decay_direction=snap.decay_direction,
                conflict_weight=snap.conflict_weight,
                conflict_bonus=snap.conflict_bonus,
                conflict_penalty=snap.conflict_penalty,
                conflict_adjusted_family_contribution=snap.conflict_adjusted_family_contribution,
                conflict_family_rank=snap.conflict_family_rank,
                top_symbols=snap.top_symbols,
                reason_codes=snap.reason_codes,
                metadata=snap.metadata,
            )
            ids.append(str(row["id"]))
        return ids

    def _persist_symbol_snapshots(
        self, conn, *, snaps: list[SymbolConflictAttributionSnapshot],
    ) -> list[str]:
        if not snaps:
            return []
        import src.db.repositories_48b as repo
        ids: list[str] = []
        for snap in snaps:
            row = repo.insert_cross_asset_symbol_conflict_attribution_snapshots(
                conn,
                workspace_id=snap.workspace_id,
                watchlist_id=snap.watchlist_id,
                run_id=snap.run_id,
                context_snapshot_id=snap.context_snapshot_id,
                conflict_profile_id=snap.conflict_profile_id,
                symbol=snap.symbol,
                dependency_family=snap.dependency_family,
                dependency_type=snap.dependency_type,
                family_consensus_state=snap.family_consensus_state,
                agreement_score=snap.agreement_score,
                conflict_score=snap.conflict_score,
                dominant_conflict_source=snap.dominant_conflict_source,
                raw_symbol_score=snap.raw_symbol_score,
                weighted_symbol_score=snap.weighted_symbol_score,
                regime_adjusted_symbol_score=snap.regime_adjusted_symbol_score,
                timing_adjusted_symbol_score=snap.timing_adjusted_symbol_score,
                transition_adjusted_symbol_score=snap.transition_adjusted_symbol_score,
                archetype_adjusted_symbol_score=snap.archetype_adjusted_symbol_score,
                cluster_adjusted_symbol_score=snap.cluster_adjusted_symbol_score,
                persistence_adjusted_symbol_score=snap.persistence_adjusted_symbol_score,
                decay_adjusted_symbol_score=snap.decay_adjusted_symbol_score,
                conflict_weight=snap.conflict_weight,
                conflict_adjusted_symbol_score=snap.conflict_adjusted_symbol_score,
                symbol_rank=snap.symbol_rank,
                reason_codes=snap.reason_codes,
                metadata=snap.metadata,
            )
            ids.append(str(row["id"]))
        return ids

    def persist_conflict_attribution(
        self,
        conn,
        *,
        family_snaps: list[FamilyConflictAttributionSnapshot],
        symbol_snaps: list[SymbolConflictAttributionSnapshot],
    ) -> tuple[list[str], list[str]]:
        return (
            self._persist_family_snapshots(conn, snaps=family_snaps),
            self._persist_symbol_snapshots(conn, snaps=symbol_snaps),
        )

    # ── orchestration ───────────────────────────────────────────────────
    def build_and_persist_for_run(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> tuple[int, int]:
        profile = self.get_active_conflict_profile(conn, workspace_id=workspace_id)
        run_conflict    = self._load_run_layer_conflict(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        family_conflict = self._load_family_layer_agreement(conn, run_id=run_id)
        family_rows     = self._load_family_decay_attribution(conn, run_id=run_id)
        symbol_rows     = self._load_symbol_decay_attribution(conn, run_id=run_id)

        # Conflict-aware attribution should only run after 4.8A surfaces exist.
        # If no conflict surfaces exist at all, skip cleanly.
        if not run_conflict and not family_conflict:
            logger.debug(
                "conflict_attribution: no 4.8A conflict surfaces for workspace=%s "
                "watchlist=%s run=%s — skipping",
                workspace_id, watchlist_id, run_id,
            )
            return (0, 0)

        if not family_rows and not symbol_rows:
            logger.debug(
                "conflict_attribution: no upstream attribution for workspace=%s watchlist=%s run=%s",
                workspace_id, watchlist_id, run_id,
            )
            return (0, 0)

        # ── families ────────────────────────────────────────────────────
        family_dicts: list[dict[str, Any]] = []
        for fr in family_rows:
            d = self.compute_conflict_adjusted_family_attribution(
                family_row=fr, run_conflict=run_conflict,
                family_conflict=family_conflict.get(str(fr.get("dependency_family") or "")),
                profile=profile,
            )
            family_dicts.append(d)

        ranked_families = self.rank_conflict_families(family_dicts)
        for i, d in enumerate(ranked_families, start=1):
            d["conflict_family_rank"] = i

        family_snaps: list[FamilyConflictAttributionSnapshot] = []
        common_meta = {
            "scoring_version": _SCORING_VERSION,
            "policy_profile_id": profile.id,
            "policy_profile_name": profile.profile_name,
            "default_conflict_profile_used": profile.id is None,
            "min_conflict_multiplier": _MIN_CONFLICT_MULTIPLIER,
            "max_conflict_multiplier": _MAX_CONFLICT_MULTIPLIER,
            "agreement_bonus_base": _AGREEMENT_BONUS_BASE,
            "conflict_penalty_base": _CONFLICT_PENALTY_BASE,
            "unreliable_penalty_base": _UNRELIABLE_PENALTY_BASE,
            "dominant_source_penalty_base": _DOMINANT_SOURCE_PENALTY,
            "conflict_floor_scale": _CONFLICT_FLOOR_SCALE,
        }
        for d in ranked_families:
            family_snaps.append(FamilyConflictAttributionSnapshot(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                run_id=run_id,
                context_snapshot_id=d.get("context_snapshot_id"),
                conflict_profile_id=profile.id,
                dependency_family=d["dependency_family"],
                raw_family_net_contribution=d["raw_family_net_contribution"],
                weighted_family_net_contribution=d["weighted_family_net_contribution"],
                regime_adjusted_family_contribution=d["regime_adjusted_family_contribution"],
                timing_adjusted_family_contribution=d["timing_adjusted_family_contribution"],
                transition_adjusted_family_contribution=d["transition_adjusted_family_contribution"],
                archetype_adjusted_family_contribution=d["archetype_adjusted_family_contribution"],
                cluster_adjusted_family_contribution=d["cluster_adjusted_family_contribution"],
                persistence_adjusted_family_contribution=d["persistence_adjusted_family_contribution"],
                decay_adjusted_family_contribution=d["decay_adjusted_family_contribution"],
                family_consensus_state=d["family_consensus_state"],
                agreement_score=d["agreement_score"],
                conflict_score=d["conflict_score"],
                dominant_conflict_source=d["dominant_conflict_source"],
                transition_direction=d["transition_direction"],
                archetype_direction=d["archetype_direction"],
                cluster_direction=d["cluster_direction"],
                persistence_direction=d["persistence_direction"],
                decay_direction=d["decay_direction"],
                conflict_weight=d["conflict_weight"],
                conflict_bonus=d["conflict_bonus"],
                conflict_penalty=d["conflict_penalty"],
                conflict_adjusted_family_contribution=d["conflict_adjusted_family_contribution"],
                conflict_family_rank=d["conflict_family_rank"],
                top_symbols=d["top_symbols"],
                reason_codes=d["reason_codes"],
                metadata={**common_meta, "base_contribution_used": d["base_contribution_used"]},
            ))

        # ── symbols ─────────────────────────────────────────────────────
        symbol_dicts: list[dict[str, Any]] = []
        for sr in symbol_rows:
            sd = self.compute_conflict_adjusted_symbol_attribution(
                symbol_row=sr, family_conflict=family_conflict,
                run_conflict=run_conflict, profile=profile,
            )
            symbol_dicts.append(sd)

        ranked_symbols = self.rank_conflict_symbols(symbol_dicts)
        for i, sd in enumerate(ranked_symbols, start=1):
            sd["symbol_rank"] = i

        symbol_snaps: list[SymbolConflictAttributionSnapshot] = []
        for sd in ranked_symbols:
            symbol_snaps.append(SymbolConflictAttributionSnapshot(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                run_id=run_id,
                context_snapshot_id=sd.get("context_snapshot_id"),
                conflict_profile_id=profile.id,
                symbol=sd["symbol"],
                dependency_family=sd["dependency_family"],
                dependency_type=sd["dependency_type"],
                family_consensus_state=sd["family_consensus_state"],
                agreement_score=sd["agreement_score"],
                conflict_score=sd["conflict_score"],
                dominant_conflict_source=sd["dominant_conflict_source"],
                raw_symbol_score=sd["raw_symbol_score"],
                weighted_symbol_score=sd["weighted_symbol_score"],
                regime_adjusted_symbol_score=sd["regime_adjusted_symbol_score"],
                timing_adjusted_symbol_score=sd["timing_adjusted_symbol_score"],
                transition_adjusted_symbol_score=sd["transition_adjusted_symbol_score"],
                archetype_adjusted_symbol_score=sd["archetype_adjusted_symbol_score"],
                cluster_adjusted_symbol_score=sd["cluster_adjusted_symbol_score"],
                persistence_adjusted_symbol_score=sd["persistence_adjusted_symbol_score"],
                decay_adjusted_symbol_score=sd["decay_adjusted_symbol_score"],
                conflict_weight=sd["conflict_weight"],
                conflict_adjusted_symbol_score=sd["conflict_adjusted_symbol_score"],
                symbol_rank=sd["symbol_rank"],
                reason_codes=sd["reason_codes"],
                metadata={**common_meta, "base_score_used": sd["base_score_used"]},
            ))

        fids, sids = self.persist_conflict_attribution(
            conn, family_snaps=family_snaps, symbol_snaps=symbol_snaps,
        )
        return (len(fids), len(sids))

    def refresh_workspace_conflict_attribution(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> int:
        """Emit conflict-aware attribution for every watchlist on this run."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        total = 0
        for wid in watchlist_ids:
            try:
                fcount, scount = self.build_and_persist_for_run(
                    conn, workspace_id=workspace_id, watchlist_id=wid, run_id=run_id,
                )
                if fcount or scount:
                    conn.commit()
                    total += fcount
            except Exception as exc:
                logger.warning(
                    "cross_asset_conflict_attribution: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return total
