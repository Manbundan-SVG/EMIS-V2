"""Phase 4.7B: Decay-Aware Attribution.

Refines cross-asset family + symbol contribution by conditioning on signal
freshness state, aggregate decay score, stale-memory flag, and contradiction
flag from the live 4.7A diagnostics. Sits on top of the 4.6B persistence-aware
attribution (and falls back through cluster / archetype / transition / timing
/ regime / weighted / raw layers).

Persists:
  * cross_asset_family_decay_attribution_snapshots (per-run/family)
  * cross_asset_symbol_decay_attribution_snapshots (per-run/symbol)

All weights, bonuses, penalties, and bounds are deterministic and metadata-
stamped. No predictive forecasting in this phase.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.7B.v1"

# Default profile values mirror migration defaults so the service is safe
# against fresh installs lacking an active profile row.
_DEFAULT_FRESH_WEIGHT             = 1.08
_DEFAULT_DECAYING_WEIGHT          = 0.98
_DEFAULT_STALE_WEIGHT             = 0.82
_DEFAULT_CONTRADICTED_WEIGHT      = 0.65
_DEFAULT_MIXED_WEIGHT             = 0.88
_DEFAULT_INSUFFICIENT_HISTORY_WEIGHT = 0.80
_DEFAULT_FRESHNESS_BONUS_SCALE    = 1.0
_DEFAULT_STALE_PENALTY_SCALE      = 1.0
_DEFAULT_CONTRADICTION_PENALTY_SCALE = 1.0
_DEFAULT_DECAY_SCORE_PENALTY_SCALE   = 1.0

# Conservative multiplier band — keeps decay influence bounded.
_MIN_DECAY_MULTIPLIER  = 0.60
_MAX_DECAY_MULTIPLIER  = 1.20

# Bonus / penalty bases — small, explicit constants.
_FRESH_BONUS_BASE                = 0.02
_DECAY_SCORE_PENALTY_BASE        = 0.05
_STALE_PENALTY_BASE              = 0.04
_CONTRADICTION_PENALTY_BASE      = 0.08

# Freshness preference order for tie-breaks (higher = preferred).
_FRESHNESS_PREFERENCE = {
    "fresh":                5,
    "decaying":             4,
    "mixed":                3,
    "stale":                2,
    "contradicted":         1,
    "insufficient_history": 0,
}

# Contradiction may optionally floor the contribution toward zero. We use
# a soft floor — never invert the sign, just damp magnitude.
_CONTRADICTION_FLOOR_SCALE = 0.50


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


@dataclass
class DecayAttributionProfile:
    id: str | None
    workspace_id: str
    profile_name: str
    is_active: bool
    fresh_weight: float
    decaying_weight: float
    stale_weight: float
    contradicted_weight: float
    mixed_weight: float
    insufficient_history_weight: float
    freshness_bonus_scale: float
    stale_penalty_scale: float
    contradiction_penalty_scale: float
    decay_score_penalty_scale: float
    decay_family_overrides: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def default(cls, workspace_id: str) -> "DecayAttributionProfile":
        return cls(
            id=None,
            workspace_id=workspace_id,
            profile_name="default_decay_attribution_profile",
            is_active=True,
            fresh_weight=_DEFAULT_FRESH_WEIGHT,
            decaying_weight=_DEFAULT_DECAYING_WEIGHT,
            stale_weight=_DEFAULT_STALE_WEIGHT,
            contradicted_weight=_DEFAULT_CONTRADICTED_WEIGHT,
            mixed_weight=_DEFAULT_MIXED_WEIGHT,
            insufficient_history_weight=_DEFAULT_INSUFFICIENT_HISTORY_WEIGHT,
            freshness_bonus_scale=_DEFAULT_FRESHNESS_BONUS_SCALE,
            stale_penalty_scale=_DEFAULT_STALE_PENALTY_SCALE,
            contradiction_penalty_scale=_DEFAULT_CONTRADICTION_PENALTY_SCALE,
            decay_score_penalty_scale=_DEFAULT_DECAY_SCORE_PENALTY_SCALE,
            decay_family_overrides={},
            metadata={"source": "default", "scoring_version": _SCORING_VERSION},
        )


@dataclass
class FamilyDecayAttributionSnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    decay_profile_id: str | None
    dependency_family: str
    raw_family_net_contribution: float | None
    weighted_family_net_contribution: float | None
    regime_adjusted_family_contribution: float | None
    timing_adjusted_family_contribution: float | None
    transition_adjusted_family_contribution: float | None
    archetype_adjusted_family_contribution: float | None
    cluster_adjusted_family_contribution: float | None
    persistence_adjusted_family_contribution: float | None
    freshness_state: str
    aggregate_decay_score: float | None
    family_decay_score: float | None
    memory_score: float | None
    state_age_runs: int | None
    stale_memory_flag: bool
    contradiction_flag: bool
    decay_weight: float | None
    decay_bonus: float | None
    decay_penalty: float | None
    decay_adjusted_family_contribution: float | None
    decay_family_rank: int | None
    top_symbols: list[str]
    reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SymbolDecayAttributionSnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    decay_profile_id: str | None
    symbol: str
    dependency_family: str
    dependency_type: str | None
    freshness_state: str
    aggregate_decay_score: float | None
    family_decay_score: float | None
    memory_score: float | None
    state_age_runs: int | None
    stale_memory_flag: bool
    contradiction_flag: bool
    raw_symbol_score: float | None
    weighted_symbol_score: float | None
    regime_adjusted_symbol_score: float | None
    timing_adjusted_symbol_score: float | None
    transition_adjusted_symbol_score: float | None
    archetype_adjusted_symbol_score: float | None
    cluster_adjusted_symbol_score: float | None
    persistence_adjusted_symbol_score: float | None
    decay_weight: float | None
    decay_adjusted_symbol_score: float | None
    symbol_rank: int | None
    reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


class CrossAssetDecayAttributionService:
    """Deterministic decay-aware family + symbol attribution refinement."""

    # ── profile ─────────────────────────────────────────────────────────
    def get_active_decay_profile(
        self, conn, *, workspace_id: str,
    ) -> DecayAttributionProfile:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, workspace_id::text as workspace_id,
                       profile_name, is_active,
                       fresh_weight, decaying_weight, stale_weight,
                       contradicted_weight, mixed_weight, insufficient_history_weight,
                       freshness_bonus_scale, stale_penalty_scale,
                       contradiction_penalty_scale, decay_score_penalty_scale,
                       decay_family_overrides, metadata
                from public.cross_asset_decay_attribution_profiles
                where workspace_id = %s::uuid
                  and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            if not row:
                return DecayAttributionProfile.default(workspace_id)
            d = dict(row)
            return DecayAttributionProfile(
                id=d.get("id"),
                workspace_id=d.get("workspace_id") or workspace_id,
                profile_name=d.get("profile_name") or "active",
                is_active=bool(d.get("is_active", True)),
                fresh_weight=float(d.get("fresh_weight") or _DEFAULT_FRESH_WEIGHT),
                decaying_weight=float(d.get("decaying_weight") or _DEFAULT_DECAYING_WEIGHT),
                stale_weight=float(d.get("stale_weight") or _DEFAULT_STALE_WEIGHT),
                contradicted_weight=float(d.get("contradicted_weight") or _DEFAULT_CONTRADICTED_WEIGHT),
                mixed_weight=float(d.get("mixed_weight") or _DEFAULT_MIXED_WEIGHT),
                insufficient_history_weight=float(
                    d.get("insufficient_history_weight") or _DEFAULT_INSUFFICIENT_HISTORY_WEIGHT
                ),
                freshness_bonus_scale=float(d.get("freshness_bonus_scale") or _DEFAULT_FRESHNESS_BONUS_SCALE),
                stale_penalty_scale=float(d.get("stale_penalty_scale") or _DEFAULT_STALE_PENALTY_SCALE),
                contradiction_penalty_scale=float(
                    d.get("contradiction_penalty_scale") or _DEFAULT_CONTRADICTION_PENALTY_SCALE
                ),
                decay_score_penalty_scale=float(
                    d.get("decay_score_penalty_scale") or _DEFAULT_DECAY_SCORE_PENALTY_SCALE
                ),
                decay_family_overrides=dict(d.get("decay_family_overrides") or {}),
                metadata=dict(d.get("metadata") or {}),
            )

    # ── primitives ──────────────────────────────────────────────────────
    @staticmethod
    def compute_decay_weight(
        *, freshness_state: str, profile: DecayAttributionProfile, family: str | None = None,
    ) -> float:
        """Per-state weight, optionally overridden per-family in the profile."""
        overrides = (profile.decay_family_overrides or {}).get(family or "", {}) if family else {}
        weights = {
            "fresh":                profile.fresh_weight,
            "decaying":             profile.decaying_weight,
            "stale":                profile.stale_weight,
            "contradicted":         profile.contradicted_weight,
            "mixed":                profile.mixed_weight,
            "insufficient_history": profile.insufficient_history_weight,
        }
        base = float(overrides.get(freshness_state, weights.get(freshness_state, profile.insufficient_history_weight)))
        return _clip(base, _MIN_DECAY_MULTIPLIER, _MAX_DECAY_MULTIPLIER)

    @staticmethod
    def compute_freshness_bonus(
        *,
        freshness_state: str,
        aggregate_decay_score: float | None,
        profile: DecayAttributionProfile,
    ) -> float:
        """Only fresh memory gets a bonus, scaled by the aggregate decay score."""
        if freshness_state != "fresh":
            return 0.0
        score = aggregate_decay_score if aggregate_decay_score is not None else 0.0
        bonus = _FRESH_BONUS_BASE * score * profile.freshness_bonus_scale
        return max(0.0, bonus)

    @staticmethod
    def compute_decay_penalty(
        *,
        freshness_state: str,
        aggregate_decay_score: float | None,
        stale_memory_flag: bool,
        profile: DecayAttributionProfile,
    ) -> float:
        """Penalty for stale memory + low decay score in supportive states."""
        penalty = 0.0
        score = aggregate_decay_score if aggregate_decay_score is not None else 1.0
        # Decay-score-driven penalty fires when score is below 0.5 and the
        # state is decaying / stale / mixed / insufficient_history (NOT for
        # contradiction — that has its own dedicated channel).
        if freshness_state in ("decaying", "stale", "mixed", "insufficient_history") and score < 0.5:
            penalty += _DECAY_SCORE_PENALTY_BASE * (0.5 - score) * profile.decay_score_penalty_scale
        # Explicit stale-memory penalty when the flag is set.
        if stale_memory_flag:
            penalty += _STALE_PENALTY_BASE * profile.stale_penalty_scale
        return max(0.0, penalty)

    @staticmethod
    def compute_contradiction_penalty(
        *, contradiction_flag: bool, profile: DecayAttributionProfile,
    ) -> float:
        if not contradiction_flag:
            return 0.0
        return _CONTRADICTION_PENALTY_BASE * profile.contradiction_penalty_scale

    # ── inputs ──────────────────────────────────────────────────────────
    def _load_run_signal_decay(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text         as run_id,
                       watchlist_id::text   as watchlist_id,
                       freshness_state, aggregate_decay_score,
                       memory_score, state_age_runs,
                       stale_memory_flag, contradiction_flag,
                       persistence_state, regime_key
                from public.cross_asset_signal_decay_summary
                where run_id = %s::uuid
                  and workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                limit 1
                """,
                (run_id, workspace_id, watchlist_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_family_signal_decay(
        self, conn, *, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family, family_freshness_state,
                       family_decay_score, family_memory_score,
                       family_state_age_runs, stale_family_memory_flag,
                       contradicted_family_flag, family_rank, family_contribution
                from public.cross_asset_family_signal_decay_summary
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

    def _load_family_persistence_attribution(
        self, conn, *, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family, context_snapshot_id::text as context_snapshot_id,
                       raw_family_net_contribution,
                       weighted_family_net_contribution,
                       regime_adjusted_family_contribution,
                       timing_adjusted_family_contribution,
                       transition_adjusted_family_contribution,
                       archetype_adjusted_family_contribution,
                       cluster_adjusted_family_contribution,
                       persistence_adjusted_family_contribution,
                       persistence_state, memory_score, state_age_runs,
                       latest_persistence_event_type, top_symbols
                from public.cross_asset_family_persistence_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_symbol_persistence_attribution(
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
                       persistence_state, memory_score, state_age_runs,
                       latest_persistence_event_type
                from public.cross_asset_symbol_persistence_attribution_summary
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

    def compute_decay_adjusted_family_attribution(
        self,
        *,
        family_row: dict[str, Any],
        run_decay: dict[str, Any] | None,
        family_decay: dict[str, Any] | None,
        profile: DecayAttributionProfile,
    ) -> dict[str, Any]:
        """Return a dict with all the fields needed to construct a
        FamilyDecayAttributionSnapshot for one family."""
        family = str(family_row.get("dependency_family") or "")

        # Prefer family-level signal-decay context, fall back to run-level.
        if family_decay:
            freshness_state = str(family_decay.get("family_freshness_state") or "insufficient_history")
            stale_flag      = bool(family_decay.get("stale_family_memory_flag") or False)
            contradiction   = bool(family_decay.get("contradicted_family_flag") or False)
            family_decay_score = _as_float(family_decay.get("family_decay_score"))
            family_memory   = _as_float(family_decay.get("family_memory_score"))
            family_age      = _as_int(family_decay.get("family_state_age_runs"))
        else:
            freshness_state = "insufficient_history"
            stale_flag      = False
            contradiction   = False
            family_decay_score = None
            family_memory   = None
            family_age      = None

        aggregate_decay = _as_float(run_decay.get("aggregate_decay_score")) if run_decay else None
        run_memory      = _as_float(run_decay.get("memory_score")) if run_decay else None
        memory          = family_memory if family_memory is not None else run_memory
        run_state_age   = _as_int(run_decay.get("state_age_runs")) if run_decay else None
        state_age       = family_age if family_age is not None else run_state_age

        # If freshness is missing at the family level but available at the
        # run level, inherit run-level state (with explicit reason code).
        run_freshness = str((run_decay or {}).get("freshness_state") or "")
        if freshness_state == "insufficient_history" and run_freshness:
            freshness_state = run_freshness
            stale_flag = stale_flag or bool((run_decay or {}).get("stale_memory_flag") or False)
            contradiction = contradiction or bool((run_decay or {}).get("contradiction_flag") or False)

        decay_weight = self.compute_decay_weight(
            freshness_state=freshness_state, profile=profile, family=family,
        )
        decay_bonus = self.compute_freshness_bonus(
            freshness_state=freshness_state,
            aggregate_decay_score=aggregate_decay,
            profile=profile,
        )
        decay_penalty = (
            self.compute_decay_penalty(
                freshness_state=freshness_state,
                aggregate_decay_score=aggregate_decay,
                stale_memory_flag=stale_flag,
                profile=profile,
            )
            + self.compute_contradiction_penalty(
                contradiction_flag=contradiction, profile=profile,
            )
        )

        base = self._pick_base_contribution(family_row)
        decay_adjusted = base
        if base is not None:
            decay_adjusted = base * decay_weight + decay_bonus - decay_penalty
            # Soft-floor contradiction-flagged contributions toward zero.
            if contradiction:
                if base > 0:
                    decay_adjusted = min(decay_adjusted, base * _CONTRADICTION_FLOOR_SCALE)
                elif base < 0:
                    decay_adjusted = max(decay_adjusted, base * _CONTRADICTION_FLOOR_SCALE)

        reason_codes: list[str] = []
        if freshness_state == "fresh":
            reason_codes.append("fresh_memory_supports_contribution")
        if freshness_state == "decaying":
            reason_codes.append("decaying_memory_caution")
        if freshness_state == "mixed":
            reason_codes.append("mixed_freshness_reduces_conviction")
        if stale_flag:
            reason_codes.append("stale_memory_penalty_applied")
        if contradiction:
            reason_codes.append("contradiction_penalty_applied")
            reason_codes.append("contradiction_floor_applied")
        if base is None:
            reason_codes.append("no_upstream_contribution_available")
        if freshness_state == "insufficient_history":
            reason_codes.append("insufficient_history_neutral_safe_suppression")
        if profile.id is None:
            reason_codes.append("default_decay_profile_used")

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
            "context_snapshot_id":                     family_row.get("context_snapshot_id"),
            "freshness_state":                         freshness_state,
            "aggregate_decay_score":                   aggregate_decay,
            "family_decay_score":                      family_decay_score,
            "memory_score":                            memory,
            "state_age_runs":                          state_age,
            "stale_memory_flag":                       stale_flag,
            "contradiction_flag":                      contradiction,
            "decay_weight":                            decay_weight,
            "decay_bonus":                             decay_bonus,
            "decay_penalty":                           decay_penalty,
            "decay_adjusted_family_contribution":      decay_adjusted,
            "top_symbols":                             top_symbols,
            "reason_codes":                            list(dict.fromkeys(reason_codes)),
            "base_contribution_used":                  base,
        }

    def compute_decay_adjusted_symbol_attribution(
        self,
        *,
        symbol_row: dict[str, Any],
        family_decay: dict[str, dict[str, Any]],
        run_decay: dict[str, Any] | None,
        profile: DecayAttributionProfile,
    ) -> dict[str, Any]:
        family = str(symbol_row.get("dependency_family") or "")
        fam_ctx = family_decay.get(family) or {}

        if fam_ctx:
            freshness_state = str(fam_ctx.get("family_freshness_state") or "insufficient_history")
            stale_flag      = bool(fam_ctx.get("stale_family_memory_flag") or False)
            contradiction   = bool(fam_ctx.get("contradicted_family_flag") or False)
            family_decay_score = _as_float(fam_ctx.get("family_decay_score"))
            family_memory   = _as_float(fam_ctx.get("family_memory_score"))
            family_age      = _as_int(fam_ctx.get("family_state_age_runs"))
        else:
            freshness_state = "insufficient_history"
            stale_flag      = False
            contradiction   = False
            family_decay_score = None
            family_memory   = None
            family_age      = None

        aggregate_decay = _as_float(run_decay.get("aggregate_decay_score")) if run_decay else None
        run_memory      = _as_float(run_decay.get("memory_score")) if run_decay else None
        memory          = family_memory if family_memory is not None else run_memory
        run_state_age   = _as_int(run_decay.get("state_age_runs")) if run_decay else None
        state_age       = family_age if family_age is not None else run_state_age

        run_freshness = str((run_decay or {}).get("freshness_state") or "")
        if freshness_state == "insufficient_history" and run_freshness:
            freshness_state = run_freshness
            stale_flag = stale_flag or bool((run_decay or {}).get("stale_memory_flag") or False)
            contradiction = contradiction or bool((run_decay or {}).get("contradiction_flag") or False)

        decay_weight = self.compute_decay_weight(
            freshness_state=freshness_state, profile=profile, family=family,
        )
        decay_bonus = self.compute_freshness_bonus(
            freshness_state=freshness_state,
            aggregate_decay_score=aggregate_decay,
            profile=profile,
        )
        decay_penalty = (
            self.compute_decay_penalty(
                freshness_state=freshness_state,
                aggregate_decay_score=aggregate_decay,
                stale_memory_flag=stale_flag,
                profile=profile,
            )
            + self.compute_contradiction_penalty(
                contradiction_flag=contradiction, profile=profile,
            )
        )

        base = self._pick_base_symbol_score(symbol_row)
        decay_adjusted = base
        if base is not None:
            decay_adjusted = base * decay_weight + decay_bonus - decay_penalty
            if contradiction:
                if base > 0:
                    decay_adjusted = min(decay_adjusted, base * _CONTRADICTION_FLOOR_SCALE)
                elif base < 0:
                    decay_adjusted = max(decay_adjusted, base * _CONTRADICTION_FLOOR_SCALE)

        reason_codes: list[str] = []
        if freshness_state == "fresh":
            reason_codes.append("fresh_memory_supports_contribution")
        if stale_flag:
            reason_codes.append("stale_memory_penalty_applied")
        if contradiction:
            reason_codes.append("contradiction_penalty_applied")
        if base is None:
            reason_codes.append("no_upstream_symbol_score_available")
        if profile.id is None:
            reason_codes.append("default_decay_profile_used")

        return {
            "symbol":                            str(symbol_row.get("symbol") or ""),
            "dependency_family":                 family,
            "dependency_type":                   symbol_row.get("dependency_type"),
            "context_snapshot_id":               symbol_row.get("context_snapshot_id"),
            "freshness_state":                   freshness_state,
            "aggregate_decay_score":             aggregate_decay,
            "family_decay_score":                family_decay_score,
            "memory_score":                      memory,
            "state_age_runs":                    state_age,
            "stale_memory_flag":                 stale_flag,
            "contradiction_flag":                contradiction,
            "raw_symbol_score":                  _as_float(symbol_row.get("raw_symbol_score")),
            "weighted_symbol_score":             _as_float(symbol_row.get("weighted_symbol_score")),
            "regime_adjusted_symbol_score":      _as_float(symbol_row.get("regime_adjusted_symbol_score")),
            "timing_adjusted_symbol_score":      _as_float(symbol_row.get("timing_adjusted_symbol_score")),
            "transition_adjusted_symbol_score":  _as_float(symbol_row.get("transition_adjusted_symbol_score")),
            "archetype_adjusted_symbol_score":   _as_float(symbol_row.get("archetype_adjusted_symbol_score")),
            "cluster_adjusted_symbol_score":     _as_float(symbol_row.get("cluster_adjusted_symbol_score")),
            "persistence_adjusted_symbol_score": _as_float(symbol_row.get("persistence_adjusted_symbol_score")),
            "decay_weight":                      decay_weight,
            "decay_adjusted_symbol_score":       decay_adjusted,
            "reason_codes":                      list(dict.fromkeys(reason_codes)),
            "base_score_used":                   base,
        }

    @staticmethod
    def rank_decay_families(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Deterministic ranking by decay-adjusted contribution desc, absolute
        contribution desc, freshness preference desc, family name asc."""
        def _key(r: dict[str, Any]) -> tuple:
            v = r.get("decay_adjusted_family_contribution")
            v_f = float(v) if v is not None else float("-inf")
            absv = abs(v_f) if v_f != float("-inf") else 0.0
            pref = _FRESHNESS_PREFERENCE.get(str(r.get("freshness_state") or ""), 0)
            return (-v_f, -absv, -pref, str(r.get("dependency_family") or ""))
        return sorted(rows, key=_key)

    @staticmethod
    def rank_decay_symbols(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        def _key(r: dict[str, Any]) -> tuple:
            v = r.get("decay_adjusted_symbol_score")
            v_f = float(v) if v is not None else float("-inf")
            absv = abs(v_f) if v_f != float("-inf") else 0.0
            pref = _FRESHNESS_PREFERENCE.get(str(r.get("freshness_state") or ""), 0)
            return (-v_f, -absv, -pref, str(r.get("symbol") or ""))
        return sorted(rows, key=_key)

    # ── persistence ─────────────────────────────────────────────────────
    def _persist_family_snapshots(
        self, conn, *, snaps: list[FamilyDecayAttributionSnapshot],
    ) -> list[str]:
        if not snaps:
            return []
        import src.db.repositories_47b as repo
        ids: list[str] = []
        for snap in snaps:
            row = repo.insert_cross_asset_family_decay_attribution_snapshots(
                conn,
                workspace_id=snap.workspace_id,
                watchlist_id=snap.watchlist_id,
                run_id=snap.run_id,
                context_snapshot_id=snap.context_snapshot_id,
                decay_profile_id=snap.decay_profile_id,
                dependency_family=snap.dependency_family,
                raw_family_net_contribution=snap.raw_family_net_contribution,
                weighted_family_net_contribution=snap.weighted_family_net_contribution,
                regime_adjusted_family_contribution=snap.regime_adjusted_family_contribution,
                timing_adjusted_family_contribution=snap.timing_adjusted_family_contribution,
                transition_adjusted_family_contribution=snap.transition_adjusted_family_contribution,
                archetype_adjusted_family_contribution=snap.archetype_adjusted_family_contribution,
                cluster_adjusted_family_contribution=snap.cluster_adjusted_family_contribution,
                persistence_adjusted_family_contribution=snap.persistence_adjusted_family_contribution,
                freshness_state=snap.freshness_state,
                aggregate_decay_score=snap.aggregate_decay_score,
                family_decay_score=snap.family_decay_score,
                memory_score=snap.memory_score,
                state_age_runs=snap.state_age_runs,
                stale_memory_flag=snap.stale_memory_flag,
                contradiction_flag=snap.contradiction_flag,
                decay_weight=snap.decay_weight,
                decay_bonus=snap.decay_bonus,
                decay_penalty=snap.decay_penalty,
                decay_adjusted_family_contribution=snap.decay_adjusted_family_contribution,
                decay_family_rank=snap.decay_family_rank,
                top_symbols=snap.top_symbols,
                reason_codes=snap.reason_codes,
                metadata=snap.metadata,
            )
            ids.append(str(row["id"]))
        return ids

    def _persist_symbol_snapshots(
        self, conn, *, snaps: list[SymbolDecayAttributionSnapshot],
    ) -> list[str]:
        if not snaps:
            return []
        import src.db.repositories_47b as repo
        ids: list[str] = []
        for snap in snaps:
            row = repo.insert_cross_asset_symbol_decay_attribution_snapshots(
                conn,
                workspace_id=snap.workspace_id,
                watchlist_id=snap.watchlist_id,
                run_id=snap.run_id,
                context_snapshot_id=snap.context_snapshot_id,
                decay_profile_id=snap.decay_profile_id,
                symbol=snap.symbol,
                dependency_family=snap.dependency_family,
                dependency_type=snap.dependency_type,
                freshness_state=snap.freshness_state,
                aggregate_decay_score=snap.aggregate_decay_score,
                family_decay_score=snap.family_decay_score,
                memory_score=snap.memory_score,
                state_age_runs=snap.state_age_runs,
                stale_memory_flag=snap.stale_memory_flag,
                contradiction_flag=snap.contradiction_flag,
                raw_symbol_score=snap.raw_symbol_score,
                weighted_symbol_score=snap.weighted_symbol_score,
                regime_adjusted_symbol_score=snap.regime_adjusted_symbol_score,
                timing_adjusted_symbol_score=snap.timing_adjusted_symbol_score,
                transition_adjusted_symbol_score=snap.transition_adjusted_symbol_score,
                archetype_adjusted_symbol_score=snap.archetype_adjusted_symbol_score,
                cluster_adjusted_symbol_score=snap.cluster_adjusted_symbol_score,
                persistence_adjusted_symbol_score=snap.persistence_adjusted_symbol_score,
                decay_weight=snap.decay_weight,
                decay_adjusted_symbol_score=snap.decay_adjusted_symbol_score,
                symbol_rank=snap.symbol_rank,
                reason_codes=snap.reason_codes,
                metadata=snap.metadata,
            )
            ids.append(str(row["id"]))
        return ids

    def persist_decay_attribution(
        self,
        conn,
        *,
        family_snaps: list[FamilyDecayAttributionSnapshot],
        symbol_snaps: list[SymbolDecayAttributionSnapshot],
    ) -> tuple[list[str], list[str]]:
        return (
            self._persist_family_snapshots(conn, snaps=family_snaps),
            self._persist_symbol_snapshots(conn, snaps=symbol_snaps),
        )

    # ── orchestration ───────────────────────────────────────────────────
    def build_and_persist_for_run(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> tuple[int, int]:
        profile = self.get_active_decay_profile(conn, workspace_id=workspace_id)
        run_decay = self._load_run_signal_decay(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        family_decay = self._load_family_signal_decay(conn, run_id=run_id)
        family_rows  = self._load_family_persistence_attribution(conn, run_id=run_id)
        symbol_rows  = self._load_symbol_persistence_attribution(conn, run_id=run_id)

        if not family_rows and not symbol_rows:
            logger.debug(
                "decay_attribution: no upstream attribution for workspace=%s watchlist=%s run=%s",
                workspace_id, watchlist_id, run_id,
            )
            return (0, 0)

        # ── families ────────────────────────────────────────────────────
        family_dicts: list[dict[str, Any]] = []
        for fr in family_rows:
            d = self.compute_decay_adjusted_family_attribution(
                family_row=fr, run_decay=run_decay,
                family_decay=family_decay.get(str(fr.get("dependency_family") or "")),
                profile=profile,
            )
            family_dicts.append(d)

        ranked_families = self.rank_decay_families(family_dicts)
        for i, d in enumerate(ranked_families, start=1):
            d["decay_family_rank"] = i

        family_snaps: list[FamilyDecayAttributionSnapshot] = []
        common_meta = {
            "scoring_version": _SCORING_VERSION,
            "policy_profile_id": profile.id,
            "policy_profile_name": profile.profile_name,
            "default_decay_profile_used": profile.id is None,
            "min_decay_multiplier": _MIN_DECAY_MULTIPLIER,
            "max_decay_multiplier": _MAX_DECAY_MULTIPLIER,
            "fresh_bonus_base": _FRESH_BONUS_BASE,
            "decay_score_penalty_base": _DECAY_SCORE_PENALTY_BASE,
            "stale_penalty_base": _STALE_PENALTY_BASE,
            "contradiction_penalty_base": _CONTRADICTION_PENALTY_BASE,
            "contradiction_floor_scale": _CONTRADICTION_FLOOR_SCALE,
        }
        for d in ranked_families:
            family_snaps.append(FamilyDecayAttributionSnapshot(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                run_id=run_id,
                context_snapshot_id=d.get("context_snapshot_id"),
                decay_profile_id=profile.id,
                dependency_family=d["dependency_family"],
                raw_family_net_contribution=d["raw_family_net_contribution"],
                weighted_family_net_contribution=d["weighted_family_net_contribution"],
                regime_adjusted_family_contribution=d["regime_adjusted_family_contribution"],
                timing_adjusted_family_contribution=d["timing_adjusted_family_contribution"],
                transition_adjusted_family_contribution=d["transition_adjusted_family_contribution"],
                archetype_adjusted_family_contribution=d["archetype_adjusted_family_contribution"],
                cluster_adjusted_family_contribution=d["cluster_adjusted_family_contribution"],
                persistence_adjusted_family_contribution=d["persistence_adjusted_family_contribution"],
                freshness_state=d["freshness_state"],
                aggregate_decay_score=d["aggregate_decay_score"],
                family_decay_score=d["family_decay_score"],
                memory_score=d["memory_score"],
                state_age_runs=d["state_age_runs"],
                stale_memory_flag=d["stale_memory_flag"],
                contradiction_flag=d["contradiction_flag"],
                decay_weight=d["decay_weight"],
                decay_bonus=d["decay_bonus"],
                decay_penalty=d["decay_penalty"],
                decay_adjusted_family_contribution=d["decay_adjusted_family_contribution"],
                decay_family_rank=d["decay_family_rank"],
                top_symbols=d["top_symbols"],
                reason_codes=d["reason_codes"],
                metadata={**common_meta, "base_contribution_used": d["base_contribution_used"]},
            ))

        # ── symbols ─────────────────────────────────────────────────────
        symbol_dicts: list[dict[str, Any]] = []
        for sr in symbol_rows:
            sd = self.compute_decay_adjusted_symbol_attribution(
                symbol_row=sr, family_decay=family_decay,
                run_decay=run_decay, profile=profile,
            )
            symbol_dicts.append(sd)

        ranked_symbols = self.rank_decay_symbols(symbol_dicts)
        for i, sd in enumerate(ranked_symbols, start=1):
            sd["symbol_rank"] = i

        symbol_snaps: list[SymbolDecayAttributionSnapshot] = []
        for sd in ranked_symbols:
            symbol_snaps.append(SymbolDecayAttributionSnapshot(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                run_id=run_id,
                context_snapshot_id=sd.get("context_snapshot_id"),
                decay_profile_id=profile.id,
                symbol=sd["symbol"],
                dependency_family=sd["dependency_family"],
                dependency_type=sd["dependency_type"],
                freshness_state=sd["freshness_state"],
                aggregate_decay_score=sd["aggregate_decay_score"],
                family_decay_score=sd["family_decay_score"],
                memory_score=sd["memory_score"],
                state_age_runs=sd["state_age_runs"],
                stale_memory_flag=sd["stale_memory_flag"],
                contradiction_flag=sd["contradiction_flag"],
                raw_symbol_score=sd["raw_symbol_score"],
                weighted_symbol_score=sd["weighted_symbol_score"],
                regime_adjusted_symbol_score=sd["regime_adjusted_symbol_score"],
                timing_adjusted_symbol_score=sd["timing_adjusted_symbol_score"],
                transition_adjusted_symbol_score=sd["transition_adjusted_symbol_score"],
                archetype_adjusted_symbol_score=sd["archetype_adjusted_symbol_score"],
                cluster_adjusted_symbol_score=sd["cluster_adjusted_symbol_score"],
                persistence_adjusted_symbol_score=sd["persistence_adjusted_symbol_score"],
                decay_weight=sd["decay_weight"],
                decay_adjusted_symbol_score=sd["decay_adjusted_symbol_score"],
                symbol_rank=sd["symbol_rank"],
                reason_codes=sd["reason_codes"],
                metadata={**common_meta, "base_score_used": sd["base_score_used"]},
            ))

        fids, sids = self.persist_decay_attribution(
            conn, family_snaps=family_snaps, symbol_snaps=symbol_snaps,
        )
        return (len(fids), len(sids))

    def refresh_workspace_decay_attribution(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> int:
        """Emit decay-aware attribution for every watchlist on this run."""
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
                    "cross_asset_decay_attribution: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return total
