"""Phase 4.7A: Signal Decay and Stale-Memory Diagnostics.

Computes deterministic per-run, per-family decay scores and freshness state
classifications on top of the live 4.6A persistence summaries (state,
regime memory), 4.6B persistence-aware family attribution, 4.6C persistence-
aware composite, 4.5A cluster, 4.4A archetype, 4.3A transition, and 4.2A
timing surfaces.

Persists:
  * cross_asset_signal_decay_snapshots (one per run/watchlist)
  * cross_asset_family_signal_decay_snapshots (one per run/family)
  * cross_asset_stale_memory_event_snapshots (per detected event)

All decay logic is deterministic, bounded to [0, 1], half-life-based, and
metadata-stamped. No predictive forecasting in this phase — windows and
weights are fixed.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.7A.v1"

# Default policy values used when no active policy profile exists for the
# workspace. These mirror the migration defaults so the service can run
# against fresh installs.
_DEFAULT_REGIME_HALF_LIFE       = 12
_DEFAULT_TIMING_HALF_LIFE       = 8
_DEFAULT_TRANSITION_HALF_LIFE   = 6
_DEFAULT_ARCHETYPE_HALF_LIFE    = 10
_DEFAULT_CLUSTER_HALF_LIFE      = 10
_DEFAULT_PERSISTENCE_HALF_LIFE  = 12
_DEFAULT_FRESH_THRESHOLD        = 0.75
_DEFAULT_DECAYING_THRESHOLD     = 0.50
_DEFAULT_STALE_THRESHOLD        = 0.30
_DEFAULT_CONTRADICTION_THRESH   = 0.50

# Aggregate weights — fixed and metadata-stamped.
_AGG_W_REGIME        = 0.20
_AGG_W_TIMING        = 0.15
_AGG_W_TRANSITION    = 0.15
_AGG_W_ARCHETYPE     = 0.15
_AGG_W_CLUSTER       = 0.20
_AGG_W_PERSISTENCE   = 0.15

# Layer blend: half-life decay vs current persistence-ratio support.
_LAYER_DECAY_W       = 0.70
_LAYER_RATIO_W       = 0.30

# History window lookups
_HISTORY_LIMIT       = 50
_MIN_HISTORY_FOR_CLASSIFY = 3

# Suppressive cluster/archetype values used for contradiction detection.
_SUPPRESSIVE_CLUSTERS   = {"deteriorating", "mixed"}
_SUPPRESSIVE_ARCHETYPES = {"deteriorating_breakdown", "mixed_transition_noise"}


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


def _clip(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _half_life_decay(age_runs: int | float | None, half_life_runs: int | float) -> float:
    """Bounded [0, 1] half-life decay. age=0 → 1.0; age=half_life → 0.5."""
    if age_runs is None:
        return 0.0
    age = max(0.0, float(age_runs))
    hl  = max(1.0, float(half_life_runs or 1))
    return _clip(math.pow(0.5, age / hl), 0.0, 1.0)


def _layer_score(age_runs: int | float | None, half_life: int | float, support_ratio: float | None) -> float:
    """Blend half-life decay and a [0, 1] persistence/support ratio."""
    decay = _half_life_decay(age_runs, half_life)
    ratio = _clip(support_ratio if support_ratio is not None else 0.0, 0.0, 1.0)
    return _clip(_LAYER_DECAY_W * decay + _LAYER_RATIO_W * ratio, 0.0, 1.0)


@dataclass
class DecayPolicyProfile:
    id: str | None
    workspace_id: str
    profile_name: str
    is_active: bool
    regime_half_life_runs: int
    timing_half_life_runs: int
    transition_half_life_runs: int
    archetype_half_life_runs: int
    cluster_half_life_runs: int
    persistence_half_life_runs: int
    fresh_memory_threshold: float
    decaying_memory_threshold: float
    stale_memory_threshold: float
    contradiction_penalty_threshold: float
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def default(cls, workspace_id: str) -> "DecayPolicyProfile":
        return cls(
            id=None,
            workspace_id=workspace_id,
            profile_name="default_decay_policy",
            is_active=True,
            regime_half_life_runs=_DEFAULT_REGIME_HALF_LIFE,
            timing_half_life_runs=_DEFAULT_TIMING_HALF_LIFE,
            transition_half_life_runs=_DEFAULT_TRANSITION_HALF_LIFE,
            archetype_half_life_runs=_DEFAULT_ARCHETYPE_HALF_LIFE,
            cluster_half_life_runs=_DEFAULT_CLUSTER_HALF_LIFE,
            persistence_half_life_runs=_DEFAULT_PERSISTENCE_HALF_LIFE,
            fresh_memory_threshold=_DEFAULT_FRESH_THRESHOLD,
            decaying_memory_threshold=_DEFAULT_DECAYING_THRESHOLD,
            stale_memory_threshold=_DEFAULT_STALE_THRESHOLD,
            contradiction_penalty_threshold=_DEFAULT_CONTRADICTION_THRESH,
            metadata={"source": "default", "scoring_version": _SCORING_VERSION},
        )


@dataclass
class SignalDecaySnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    decay_policy_profile_id: str | None
    regime_key: str | None
    dominant_timing_class: str | None
    dominant_transition_state: str | None
    dominant_sequence_class: str | None
    dominant_archetype_key: str | None
    cluster_state: str | None
    persistence_state: str | None
    current_state_signature: str
    state_age_runs: int | None
    memory_score: float | None
    regime_decay_score: float | None
    timing_decay_score: float | None
    transition_decay_score: float | None
    archetype_decay_score: float | None
    cluster_decay_score: float | None
    persistence_decay_score: float | None
    aggregate_decay_score: float | None
    freshness_state: str
    stale_memory_flag: bool
    contradiction_flag: bool
    reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FamilySignalDecaySnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    dependency_family: str
    transition_state: str | None
    dominant_sequence_class: str | None
    archetype_key: str | None
    cluster_state: str | None
    persistence_state: str | None
    family_rank: int | None
    family_contribution: float | None
    family_state_age_runs: int | None
    family_memory_score: float | None
    family_decay_score: float | None
    family_freshness_state: str
    stale_family_memory_flag: bool
    contradicted_family_flag: bool
    reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class StaleMemoryEvent:
    workspace_id: str
    watchlist_id: str | None
    source_run_id: str | None
    target_run_id: str
    regime_key: str | None
    prior_freshness_state: str | None
    current_freshness_state: str
    prior_state_signature: str | None
    current_state_signature: str
    prior_memory_score: float | None
    current_memory_score: float | None
    prior_aggregate_decay_score: float | None
    current_aggregate_decay_score: float | None
    event_type: str
    reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


class CrossAssetSignalDecayService:
    """Deterministic signal-decay + stale-memory diagnostics."""

    # ── policy ──────────────────────────────────────────────────────────
    def get_active_decay_policy(
        self, conn, *, workspace_id: str,
    ) -> DecayPolicyProfile:
        """Return the active decay policy profile for the workspace, or a
        deterministic default when none exists."""
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, workspace_id::text as workspace_id,
                       profile_name, is_active,
                       regime_half_life_runs, timing_half_life_runs,
                       transition_half_life_runs, archetype_half_life_runs,
                       cluster_half_life_runs, persistence_half_life_runs,
                       fresh_memory_threshold, decaying_memory_threshold,
                       stale_memory_threshold, contradiction_penalty_threshold,
                       metadata
                from public.cross_asset_signal_decay_policy_profiles
                where workspace_id = %s::uuid
                  and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            if not row:
                return DecayPolicyProfile.default(workspace_id)
            d = dict(row)
            return DecayPolicyProfile(
                id=d.get("id"),
                workspace_id=d.get("workspace_id") or workspace_id,
                profile_name=d.get("profile_name") or "active",
                is_active=bool(d.get("is_active", True)),
                regime_half_life_runs=int(d.get("regime_half_life_runs") or _DEFAULT_REGIME_HALF_LIFE),
                timing_half_life_runs=int(d.get("timing_half_life_runs") or _DEFAULT_TIMING_HALF_LIFE),
                transition_half_life_runs=int(d.get("transition_half_life_runs") or _DEFAULT_TRANSITION_HALF_LIFE),
                archetype_half_life_runs=int(d.get("archetype_half_life_runs") or _DEFAULT_ARCHETYPE_HALF_LIFE),
                cluster_half_life_runs=int(d.get("cluster_half_life_runs") or _DEFAULT_CLUSTER_HALF_LIFE),
                persistence_half_life_runs=int(d.get("persistence_half_life_runs") or _DEFAULT_PERSISTENCE_HALF_LIFE),
                fresh_memory_threshold=float(d.get("fresh_memory_threshold") or _DEFAULT_FRESH_THRESHOLD),
                decaying_memory_threshold=float(d.get("decaying_memory_threshold") or _DEFAULT_DECAYING_THRESHOLD),
                stale_memory_threshold=float(d.get("stale_memory_threshold") or _DEFAULT_STALE_THRESHOLD),
                contradiction_penalty_threshold=float(
                    d.get("contradiction_penalty_threshold") or _DEFAULT_CONTRADICTION_THRESH
                ),
                metadata=dict(d.get("metadata") or {}),
            )

    # ── inputs ──────────────────────────────────────────────────────────
    def load_current_decay_context(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,  # noqa: ARG002
    ) -> dict[str, Any] | None:
        """Read the 4.6A run persistence summary as the source of truth for
        current state signature / persistence / memory / age."""
        with conn.cursor() as cur:
            cur.execute(
                """
                select sp.run_id::text                  as run_id,
                       sp.workspace_id::text            as workspace_id,
                       sp.watchlist_id::text            as watchlist_id,
                       sp.context_snapshot_id::text     as context_snapshot_id,
                       sp.regime_key,
                       sp.dominant_timing_class,
                       sp.dominant_transition_state,
                       sp.dominant_sequence_class,
                       sp.dominant_archetype_key,
                       sp.cluster_state,
                       sp.persistence_state,
                       sp.current_state_signature,
                       sp.state_age_runs,
                       sp.memory_score,
                       sp.state_persistence_ratio,
                       sp.regime_persistence_ratio,
                       sp.cluster_persistence_ratio,
                       sp.archetype_persistence_ratio,
                       sp.created_at
                from public.cross_asset_state_persistence_summary sp
                where sp.run_id      = %s::uuid
                  and sp.workspace_id = %s::uuid
                  and sp.watchlist_id = %s::uuid
                limit 1
                """,
                (run_id, workspace_id, watchlist_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def load_recent_decay_history(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str | None = None,
        limit: int = _HISTORY_LIMIT,
    ) -> list[dict[str, Any]]:
        """Recent signal decay history (newest-first) for this workspace/watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text         as run_id,
                       workspace_id::text   as workspace_id,
                       watchlist_id::text   as watchlist_id,
                       freshness_state,
                       memory_score,
                       aggregate_decay_score,
                       current_state_signature,
                       regime_key,
                       stale_memory_flag,
                       contradiction_flag,
                       created_at
                from public.cross_asset_signal_decay_summary
                where workspace_id = %s::uuid
                  and (%s::uuid is null or watchlist_id = %s::uuid)
                order by created_at desc
                limit %s
                """,
                (workspace_id, watchlist_id, watchlist_id, int(limit)),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_prior_decay_snapshot(
        self, conn, *, workspace_id: str, watchlist_id: str, before_run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text             as run_id,
                       freshness_state,
                       current_state_signature,
                       memory_score,
                       aggregate_decay_score,
                       regime_key,
                       stale_memory_flag,
                       contradiction_flag,
                       created_at
                from public.cross_asset_signal_decay_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id      <> %s::uuid
                order by created_at desc
                limit 1
                """,
                (workspace_id, watchlist_id, before_run_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_family_persistence_rows(
        self, conn, *, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       persistence_state,
                       memory_score,
                       state_age_runs,
                       state_persistence_ratio,
                       regime_persistence_ratio,
                       cluster_persistence_ratio,
                       archetype_persistence_ratio,
                       persistence_adjusted_family_contribution,
                       persistence_family_rank,
                       latest_persistence_event_type
                from public.cross_asset_family_persistence_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_family_transition_state_rows(
        self, conn, *, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        """Per-family transition/sequence/archetype state from the 4.4A
        family archetype summary, indexed by dependency_family."""
        out: dict[str, dict[str, Any]] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       transition_state,
                       dominant_sequence_class,
                       archetype_key
                from public.cross_asset_family_archetype_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = d.get("dependency_family")
                if fam:
                    out[str(fam)] = {
                        "transition_state":        d.get("transition_state"),
                        "dominant_sequence_class": d.get("dominant_sequence_class"),
                        "archetype_key":           d.get("archetype_key"),
                    }
        return out

    # ── primitives ──────────────────────────────────────────────────────
    @staticmethod
    def compute_layer_decay_score(
        *, age_runs: int | float | None, half_life_runs: int | float, support_ratio: float | None,
    ) -> float:
        return _layer_score(age_runs, half_life_runs, support_ratio)

    @staticmethod
    def compute_aggregate_decay_score(
        *,
        regime: float | None,
        timing: float | None,
        transition: float | None,
        archetype: float | None,
        cluster: float | None,
        persistence: float | None,
    ) -> float:
        score = (
            _AGG_W_REGIME      * _clip(regime      or 0.0)
            + _AGG_W_TIMING      * _clip(timing      or 0.0)
            + _AGG_W_TRANSITION  * _clip(transition  or 0.0)
            + _AGG_W_ARCHETYPE   * _clip(archetype   or 0.0)
            + _AGG_W_CLUSTER     * _clip(cluster     or 0.0)
            + _AGG_W_PERSISTENCE * _clip(persistence or 0.0)
        )
        return _clip(score, 0.0, 1.0)

    @staticmethod
    def _is_contradicted(
        *,
        memory_score: float | None,
        persistence_state: str | None,
        cluster_state: str | None,
        archetype_key: str | None,
        contradiction_threshold: float,
    ) -> tuple[bool, list[str]]:
        """Deterministic contradiction detection. Returns (flag, reason_codes)."""
        reasons: list[str] = []
        memory = memory_score if memory_score is not None else 0.0
        # Strong memory but cluster shifted into a suppressive state.
        if memory >= contradiction_threshold and cluster_state in _SUPPRESSIVE_CLUSTERS:
            reasons.append("high_memory_low_current_support")
            reasons.append("cluster_shift_to_mixed" if cluster_state == "mixed"
                           else "persistent_state_now_deteriorating")
        # Strong memory but archetype shifted into suppressive archetype.
        if memory >= contradiction_threshold and archetype_key in _SUPPRESSIVE_ARCHETYPES:
            reasons.append("archetype_shift_to_breakdown")
        # Persistent state classification but cluster says deteriorating/mixed.
        if persistence_state == "persistent" and cluster_state in _SUPPRESSIVE_CLUSTERS:
            reasons.append("persistent_state_now_deteriorating")
        return (len(reasons) > 0, list(dict.fromkeys(reasons)))

    @staticmethod
    def classify_freshness_state(
        *,
        history_size: int,
        aggregate_decay_score: float,
        layer_scores: dict[str, float],
        contradiction_flag: bool,
        fresh_threshold: float,
        decaying_threshold: float,
        stale_threshold: float,
    ) -> tuple[str, list[str]]:
        """Deterministic freshness classification with reason codes."""
        reasons: list[str] = []
        if history_size < _MIN_HISTORY_FOR_CLASSIFY:
            reasons.append("insufficient_history")
            return "insufficient_history", reasons
        if contradiction_flag:
            reasons.append("contradiction_detected")
            return "contradicted", reasons

        # Mixed = layer disagreement: at least one layer is fresh AND at
        # least one is at/below the stale threshold (deterministic tie-break).
        layer_values = [v for v in layer_scores.values() if v is not None]
        if layer_values:
            any_fresh = any(v >= fresh_threshold for v in layer_values)
            any_stale = any(v <= stale_threshold for v in layer_values)
            if any_fresh and any_stale:
                reasons.append("layer_disagreement")
                return "mixed", reasons

        if aggregate_decay_score >= fresh_threshold:
            reasons.append("aggregate_decay_above_fresh_threshold")
            return "fresh", reasons
        if aggregate_decay_score >= decaying_threshold:
            reasons.append("aggregate_decay_below_fresh_threshold")
            return "decaying", reasons
        if aggregate_decay_score <= stale_threshold:
            reasons.append("aggregate_decay_below_stale_threshold")
            return "stale", reasons
        # Between decaying and stale → still decaying.
        reasons.append("aggregate_decay_below_fresh_threshold")
        return "decaying", reasons

    # ── per-run computation ─────────────────────────────────────────────
    def build_signal_decay_snapshot(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
        policy: DecayPolicyProfile | None = None,
    ) -> SignalDecaySnapshot | None:
        ctx = self.load_current_decay_context(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if ctx is None:
            logger.debug(
                "signal_decay: no persistence context for workspace=%s watchlist=%s run=%s",
                workspace_id, watchlist_id, run_id,
            )
            return None

        pol = policy or self.get_active_decay_policy(conn, workspace_id=workspace_id)

        state_age          = _as_int(ctx.get("state_age_runs"))
        memory_score       = _as_float(ctx.get("memory_score"))
        regime_ratio       = _as_float(ctx.get("regime_persistence_ratio"))
        cluster_ratio      = _as_float(ctx.get("cluster_persistence_ratio"))
        archetype_ratio    = _as_float(ctx.get("archetype_persistence_ratio"))
        state_ratio        = _as_float(ctx.get("state_persistence_ratio"))
        persistence_state  = ctx.get("persistence_state")

        # Layer decay scores. Layers without a discrete persistence ratio
        # fall back to the global state_persistence_ratio for support.
        regime_decay      = self.compute_layer_decay_score(
            age_runs=state_age, half_life_runs=pol.regime_half_life_runs,
            support_ratio=regime_ratio,
        )
        timing_decay      = self.compute_layer_decay_score(
            age_runs=state_age, half_life_runs=pol.timing_half_life_runs,
            support_ratio=state_ratio,
        )
        transition_decay  = self.compute_layer_decay_score(
            age_runs=state_age, half_life_runs=pol.transition_half_life_runs,
            support_ratio=state_ratio,
        )
        archetype_decay   = self.compute_layer_decay_score(
            age_runs=state_age, half_life_runs=pol.archetype_half_life_runs,
            support_ratio=archetype_ratio,
        )
        cluster_decay     = self.compute_layer_decay_score(
            age_runs=state_age, half_life_runs=pol.cluster_half_life_runs,
            support_ratio=cluster_ratio,
        )
        persistence_decay = self.compute_layer_decay_score(
            age_runs=state_age, half_life_runs=pol.persistence_half_life_runs,
            support_ratio=memory_score,
        )
        aggregate_decay = self.compute_aggregate_decay_score(
            regime=regime_decay, timing=timing_decay, transition=transition_decay,
            archetype=archetype_decay, cluster=cluster_decay, persistence=persistence_decay,
        )

        # Contradiction detection.
        contradiction_flag, contradiction_reasons = self._is_contradicted(
            memory_score=memory_score,
            persistence_state=persistence_state,
            cluster_state=ctx.get("cluster_state"),
            archetype_key=ctx.get("dominant_archetype_key"),
            contradiction_threshold=pol.contradiction_penalty_threshold,
        )

        # History size for classification gating.
        recent = self.load_recent_decay_history(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            limit=_HISTORY_LIMIT,
        )
        history_size = len([r for r in recent if r.get("run_id") != run_id]) + 1

        layer_scores = {
            "regime":      regime_decay,
            "timing":      timing_decay,
            "transition":  transition_decay,
            "archetype":   archetype_decay,
            "cluster":     cluster_decay,
            "persistence": persistence_decay,
        }
        freshness_state, reason_codes = self.classify_freshness_state(
            history_size=history_size,
            aggregate_decay_score=aggregate_decay,
            layer_scores=layer_scores,
            contradiction_flag=contradiction_flag,
            fresh_threshold=pol.fresh_memory_threshold,
            decaying_threshold=pol.decaying_memory_threshold,
            stale_threshold=pol.stale_memory_threshold,
        )

        if persistence_state == "persistent" and freshness_state in ("fresh", "decaying"):
            reason_codes.append("memory_reconfirmed")
        if freshness_state == "fresh":
            reason_codes.append("decay_score_recovered")
        reason_codes.extend(contradiction_reasons)
        # De-duplicate while preserving order.
        reason_codes = list(dict.fromkeys(reason_codes))

        stale_memory_flag = freshness_state == "stale"

        return SignalDecaySnapshot(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=ctx.get("context_snapshot_id"),
            decay_policy_profile_id=pol.id,
            regime_key=ctx.get("regime_key"),
            dominant_timing_class=ctx.get("dominant_timing_class"),
            dominant_transition_state=ctx.get("dominant_transition_state"),
            dominant_sequence_class=ctx.get("dominant_sequence_class"),
            dominant_archetype_key=ctx.get("dominant_archetype_key"),
            cluster_state=ctx.get("cluster_state"),
            persistence_state=persistence_state,
            current_state_signature=str(ctx.get("current_state_signature") or ""),
            state_age_runs=state_age,
            memory_score=memory_score,
            regime_decay_score=regime_decay,
            timing_decay_score=timing_decay,
            transition_decay_score=transition_decay,
            archetype_decay_score=archetype_decay,
            cluster_decay_score=cluster_decay,
            persistence_decay_score=persistence_decay,
            aggregate_decay_score=aggregate_decay,
            freshness_state=freshness_state,
            stale_memory_flag=stale_memory_flag,
            contradiction_flag=contradiction_flag,
            reason_codes=reason_codes,
            metadata={
                "scoring_version":         _SCORING_VERSION,
                "history_size":            history_size,
                "policy_profile_id":       pol.id,
                "policy_profile_name":     pol.profile_name,
                "fresh_threshold":         pol.fresh_memory_threshold,
                "decaying_threshold":      pol.decaying_memory_threshold,
                "stale_threshold":         pol.stale_memory_threshold,
                "contradiction_threshold": pol.contradiction_penalty_threshold,
                "regime_half_life_runs":   pol.regime_half_life_runs,
                "timing_half_life_runs":   pol.timing_half_life_runs,
                "transition_half_life_runs": pol.transition_half_life_runs,
                "archetype_half_life_runs":  pol.archetype_half_life_runs,
                "cluster_half_life_runs":  pol.cluster_half_life_runs,
                "persistence_half_life_runs": pol.persistence_half_life_runs,
                "aggregate_weights": {
                    "regime":      _AGG_W_REGIME,
                    "timing":      _AGG_W_TIMING,
                    "transition":  _AGG_W_TRANSITION,
                    "archetype":   _AGG_W_ARCHETYPE,
                    "cluster":     _AGG_W_CLUSTER,
                    "persistence": _AGG_W_PERSISTENCE,
                },
                "layer_blend_weights": {
                    "decay": _LAYER_DECAY_W, "ratio": _LAYER_RATIO_W,
                },
            },
        )

    # ── per-family computation ──────────────────────────────────────────
    def compute_family_decay_rows(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
        run_snap: SignalDecaySnapshot,
        policy: DecayPolicyProfile,
    ) -> list[FamilySignalDecaySnapshot]:
        rows = self._load_family_persistence_rows(conn, run_id=run_id)
        if not rows:
            return []
        family_states = self._load_family_transition_state_rows(conn, run_id=run_id)

        out: list[FamilySignalDecaySnapshot] = []
        for r in rows:
            family = str(r.get("dependency_family") or "")
            if not family:
                continue
            fam_persistence = r.get("persistence_state")
            fam_memory      = _as_float(r.get("memory_score"))
            fam_age         = _as_int(r.get("state_age_runs"))
            fam_state_ratio = _as_float(r.get("state_persistence_ratio"))
            fam_contribution = _as_float(r.get("persistence_adjusted_family_contribution"))
            fam_rank         = _as_int(r.get("persistence_family_rank"))

            # Per-family decay: blend persistence half-life decay with
            # state-persistence ratio support. Single deterministic score.
            family_decay = self.compute_layer_decay_score(
                age_runs=fam_age,
                half_life_runs=policy.persistence_half_life_runs,
                support_ratio=fam_state_ratio if fam_state_ratio is not None else fam_memory,
            )

            extras = family_states.get(family, {}) if family_states else {}
            transition_state        = extras.get("transition_state")
            dominant_sequence_class = extras.get("dominant_sequence_class")
            archetype_key           = extras.get("archetype_key")

            # Family contradiction detection mirrors run-level rules but
            # uses the run's cluster_state (a family-level cluster_state is
            # not always available).
            fam_contradiction, fam_contradiction_reasons = self._is_contradicted(
                memory_score=fam_memory,
                persistence_state=fam_persistence,
                cluster_state=run_snap.cluster_state,
                archetype_key=archetype_key or run_snap.dominant_archetype_key,
                contradiction_threshold=policy.contradiction_penalty_threshold,
            )

            # Classify freshness for family using the same thresholds.
            fam_freshness, fam_reasons = self.classify_freshness_state(
                history_size=_MIN_HISTORY_FOR_CLASSIFY,  # families inherit gating
                aggregate_decay_score=family_decay,
                layer_scores={"family": family_decay},
                contradiction_flag=fam_contradiction,
                fresh_threshold=policy.fresh_memory_threshold,
                decaying_threshold=policy.decaying_memory_threshold,
                stale_threshold=policy.stale_memory_threshold,
            )
            fam_reasons.extend(fam_contradiction_reasons)
            if fam_persistence == "persistent" and fam_freshness in ("fresh", "decaying"):
                fam_reasons.append("memory_reconfirmed")
            fam_reasons = list(dict.fromkeys(fam_reasons))

            out.append(FamilySignalDecaySnapshot(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                run_id=run_id,
                context_snapshot_id=run_snap.context_snapshot_id,
                dependency_family=family,
                transition_state=transition_state,
                dominant_sequence_class=dominant_sequence_class,
                archetype_key=archetype_key,
                cluster_state=run_snap.cluster_state,
                persistence_state=fam_persistence,
                family_rank=fam_rank,
                family_contribution=fam_contribution,
                family_state_age_runs=fam_age,
                family_memory_score=fam_memory,
                family_decay_score=family_decay,
                family_freshness_state=fam_freshness,
                stale_family_memory_flag=fam_freshness == "stale",
                contradicted_family_flag=fam_contradiction,
                reason_codes=fam_reasons,
                metadata={
                    "scoring_version":           _SCORING_VERSION,
                    "policy_profile_id":         policy.id,
                    "policy_profile_name":       policy.profile_name,
                    "persistence_half_life_runs": policy.persistence_half_life_runs,
                    "fresh_threshold":           policy.fresh_memory_threshold,
                    "decaying_threshold":        policy.decaying_memory_threshold,
                    "stale_threshold":           policy.stale_memory_threshold,
                    "contradiction_threshold":   policy.contradiction_penalty_threshold,
                },
            ))
        return out

    # ── stale-memory event detection ────────────────────────────────────
    def detect_stale_memory_event(
        self,
        *,
        workspace_id: str,
        watchlist_id: str,
        prior: dict[str, Any] | None,
        current_run_id: str,
        current_snap: SignalDecaySnapshot,
    ) -> StaleMemoryEvent:
        """Deterministic event classification across a freshness transition.

        Always returns an event so the operator can audit transitions; when
        no prior exists the event_type is ``insufficient_history``.
        """
        if prior is None:
            return StaleMemoryEvent(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                source_run_id=None,
                target_run_id=current_run_id,
                regime_key=current_snap.regime_key,
                prior_freshness_state=None,
                current_freshness_state=current_snap.freshness_state,
                prior_state_signature=None,
                current_state_signature=current_snap.current_state_signature,
                prior_memory_score=None,
                current_memory_score=current_snap.memory_score,
                prior_aggregate_decay_score=None,
                current_aggregate_decay_score=current_snap.aggregate_decay_score,
                event_type="insufficient_history",
                reason_codes=["insufficient_history"],
                metadata={
                    "scoring_version": _SCORING_VERSION,
                    "no_prior_window": True,
                },
            )

        prior_state   = str(prior.get("freshness_state") or "insufficient_history")
        prior_sig     = prior.get("current_state_signature")
        prior_memory  = _as_float(prior.get("memory_score"))
        prior_aggreg  = _as_float(prior.get("aggregate_decay_score"))
        prior_run_id  = prior.get("run_id")
        current_state = current_snap.freshness_state

        reason_codes: list[str] = []
        if prior_sig != current_snap.current_state_signature:
            reason_codes.append("state_signature_changed")
        if prior_memory is not None and current_snap.memory_score is not None:
            delta = current_snap.memory_score - prior_memory
            if delta >= 0.10:
                reason_codes.append("memory_score_increased")
            elif delta <= -0.10:
                reason_codes.append("memory_score_decreased")
        if prior_aggreg is not None and current_snap.aggregate_decay_score is not None:
            agg_delta = current_snap.aggregate_decay_score - prior_aggreg
            if agg_delta <= -0.10:
                reason_codes.append("aggregate_decay_below_fresh_threshold")
            if agg_delta >= 0.10:
                reason_codes.append("decay_score_recovered")

        # Event-type rules — explicit and deterministic.
        if current_state == "contradicted":
            event_type = "memory_contradicted"
            reason_codes.append("contradiction_detected")
        elif prior_state in ("stale", "decaying", "contradicted") and current_state == "fresh":
            event_type = "memory_freshened"
            reason_codes.append("decay_score_recovered")
        elif prior_state == "fresh" and current_state == "decaying":
            event_type = "memory_decayed"
            reason_codes.append("aggregate_decay_below_fresh_threshold")
        elif prior_state in ("fresh", "decaying") and current_state == "stale":
            event_type = "memory_became_stale"
            reason_codes.append("aggregate_decay_below_stale_threshold")
        elif prior_state == "stale" and current_state == "decaying":
            event_type = "memory_freshened"
            reason_codes.append("decay_score_recovered")
        elif prior_state == "fresh" and current_state == "fresh":
            event_type = "memory_reconfirmed"
            reason_codes.append("memory_reconfirmed")
        elif current_state == "insufficient_history" or prior_state == "insufficient_history":
            event_type = "insufficient_history"
            reason_codes.append("insufficient_history")
        else:
            # Fallback: classify by aggregate decay direction.
            if (
                current_snap.aggregate_decay_score is not None
                and prior_aggreg is not None
                and current_snap.aggregate_decay_score < prior_aggreg
            ):
                event_type = "memory_decayed"
                reason_codes.append("aggregate_decay_below_fresh_threshold")
            else:
                event_type = "memory_reconfirmed"
                reason_codes.append("memory_reconfirmed")

        reason_codes = list(dict.fromkeys(reason_codes)) or [event_type]

        return StaleMemoryEvent(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            source_run_id=str(prior_run_id) if prior_run_id else None,
            target_run_id=current_run_id,
            regime_key=current_snap.regime_key,
            prior_freshness_state=prior_state,
            current_freshness_state=current_state,
            prior_state_signature=prior_sig,
            current_state_signature=current_snap.current_state_signature,
            prior_memory_score=prior_memory,
            current_memory_score=current_snap.memory_score,
            prior_aggregate_decay_score=prior_aggreg,
            current_aggregate_decay_score=current_snap.aggregate_decay_score,
            event_type=event_type,
            reason_codes=reason_codes,
            metadata={
                "scoring_version": _SCORING_VERSION,
                "prior_run_id":    prior_run_id,
            },
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_signal_decay_snapshot(
        self, conn, *, snap: SignalDecaySnapshot,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_signal_decay_snapshots(
            conn,
            workspace_id=snap.workspace_id,
            watchlist_id=snap.watchlist_id,
            run_id=snap.run_id,
            context_snapshot_id=snap.context_snapshot_id,
            decay_policy_profile_id=snap.decay_policy_profile_id,
            regime_key=snap.regime_key,
            dominant_timing_class=snap.dominant_timing_class,
            dominant_transition_state=snap.dominant_transition_state,
            dominant_sequence_class=snap.dominant_sequence_class,
            dominant_archetype_key=snap.dominant_archetype_key,
            cluster_state=snap.cluster_state,
            persistence_state=snap.persistence_state,
            current_state_signature=snap.current_state_signature,
            state_age_runs=snap.state_age_runs,
            memory_score=snap.memory_score,
            regime_decay_score=snap.regime_decay_score,
            timing_decay_score=snap.timing_decay_score,
            transition_decay_score=snap.transition_decay_score,
            archetype_decay_score=snap.archetype_decay_score,
            cluster_decay_score=snap.cluster_decay_score,
            persistence_decay_score=snap.persistence_decay_score,
            aggregate_decay_score=snap.aggregate_decay_score,
            freshness_state=snap.freshness_state,
            stale_memory_flag=snap.stale_memory_flag,
            contradiction_flag=snap.contradiction_flag,
            reason_codes=snap.reason_codes,
            metadata=snap.metadata,
        )
        return str(row["id"])

    def persist_family_signal_decay_snapshots(
        self, conn, *, snaps: list[FamilySignalDecaySnapshot],
    ) -> list[str]:
        if not snaps:
            return []
        import src.db.repositories as repo
        ids: list[str] = []
        for snap in snaps:
            row = repo.insert_cross_asset_family_signal_decay_snapshots(
                conn,
                workspace_id=snap.workspace_id,
                watchlist_id=snap.watchlist_id,
                run_id=snap.run_id,
                context_snapshot_id=snap.context_snapshot_id,
                dependency_family=snap.dependency_family,
                transition_state=snap.transition_state,
                dominant_sequence_class=snap.dominant_sequence_class,
                archetype_key=snap.archetype_key,
                cluster_state=snap.cluster_state,
                persistence_state=snap.persistence_state,
                family_rank=snap.family_rank,
                family_contribution=snap.family_contribution,
                family_state_age_runs=snap.family_state_age_runs,
                family_memory_score=snap.family_memory_score,
                family_decay_score=snap.family_decay_score,
                family_freshness_state=snap.family_freshness_state,
                stale_family_memory_flag=snap.stale_family_memory_flag,
                contradicted_family_flag=snap.contradicted_family_flag,
                reason_codes=snap.reason_codes,
                metadata=snap.metadata,
            )
            ids.append(str(row["id"]))
        return ids

    def persist_stale_memory_event(
        self, conn, *, event: StaleMemoryEvent,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_stale_memory_event_snapshots(
            conn,
            workspace_id=event.workspace_id,
            watchlist_id=event.watchlist_id,
            source_run_id=event.source_run_id,
            target_run_id=event.target_run_id,
            regime_key=event.regime_key,
            prior_freshness_state=event.prior_freshness_state,
            current_freshness_state=event.current_freshness_state,
            prior_state_signature=event.prior_state_signature,
            current_state_signature=event.current_state_signature,
            prior_memory_score=event.prior_memory_score,
            current_memory_score=event.current_memory_score,
            prior_aggregate_decay_score=event.prior_aggregate_decay_score,
            current_aggregate_decay_score=event.current_aggregate_decay_score,
            event_type=event.event_type,
            reason_codes=event.reason_codes,
            metadata=event.metadata,
        )
        return str(row["id"])

    # ── orchestration ───────────────────────────────────────────────────
    def build_and_persist(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> SignalDecaySnapshot | None:
        policy = self.get_active_decay_policy(conn, workspace_id=workspace_id)
        snap = self.build_signal_decay_snapshot(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            policy=policy,
        )
        if snap is None:
            return None

        prior_snap = self._load_prior_decay_snapshot(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            before_run_id=run_id,
        )
        self.persist_signal_decay_snapshot(conn, snap=snap)

        family_snaps = self.compute_family_decay_rows(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            run_snap=snap,
            policy=policy,
        )
        if family_snaps:
            self.persist_family_signal_decay_snapshots(conn, snaps=family_snaps)

        event = self.detect_stale_memory_event(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            prior=prior_snap,
            current_run_id=run_id,
            current_snap=snap,
        )
        if event is not None:
            self.persist_stale_memory_event(conn, event=event)

        return snap

    def refresh_workspace_signal_decay(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[SignalDecaySnapshot]:
        """Emit signal-decay diagnostics for every watchlist. Commits per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[SignalDecaySnapshot] = []
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
                    "cross_asset_signal_decay: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
