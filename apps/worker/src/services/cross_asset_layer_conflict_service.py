"""Phase 4.8A: Cross-Layer Conflict and Agreement Diagnostics.

Maps each interpretation layer (timing, transition, archetype, cluster,
persistence, decay) into a simple direction class
(``supportive``/``suppressive``/``neutral``/``missing``), then computes a
weighted agreement score, a weighted conflict score, and a deterministic
consensus state per run and per family. Detects discrete conflict events
across consecutive runs.

Persists:
  * cross_asset_layer_agreement_snapshots (per run/watchlist)
  * cross_asset_family_layer_agreement_snapshots (per run/family)
  * cross_asset_layer_conflict_event_snapshots (per detected event)

Diagnostic only — no attribution or composite mutation, no forecasting.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.8A.v1"

_DEFAULT_TIMING_WEIGHT       = 0.15
_DEFAULT_TRANSITION_WEIGHT   = 0.20
_DEFAULT_ARCHETYPE_WEIGHT    = 0.15
_DEFAULT_CLUSTER_WEIGHT      = 0.20
_DEFAULT_PERSISTENCE_WEIGHT  = 0.15
_DEFAULT_DECAY_WEIGHT        = 0.15
_DEFAULT_AGREEMENT_THRESHOLD         = 0.70
_DEFAULT_PARTIAL_AGREEMENT_THRESHOLD = 0.50
_DEFAULT_CONFLICT_THRESHOLD          = 0.35
_DEFAULT_UNRELIABLE_THRESHOLD        = 0.20

# Minimum non-missing weight before we trust a non-`insufficient_context`
# classification. Below this we tag `insufficient_context`.
_MIN_NON_MISSING_WEIGHT = 0.30

# Score deltas that count as material agreement movement for events.
_AGREEMENT_DELTA_MATERIAL = 0.10


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _norm(s: Any) -> str | None:
    if s is None:
        return None
    return str(s)


@dataclass
class ConflictPolicyProfile:
    id: str | None
    workspace_id: str
    profile_name: str
    is_active: bool
    timing_weight: float
    transition_weight: float
    archetype_weight: float
    cluster_weight: float
    persistence_weight: float
    decay_weight: float
    agreement_threshold: float
    partial_agreement_threshold: float
    conflict_threshold: float
    unreliable_threshold: float
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def default(cls, workspace_id: str) -> "ConflictPolicyProfile":
        return cls(
            id=None,
            workspace_id=workspace_id,
            profile_name="default_conflict_policy",
            is_active=True,
            timing_weight=_DEFAULT_TIMING_WEIGHT,
            transition_weight=_DEFAULT_TRANSITION_WEIGHT,
            archetype_weight=_DEFAULT_ARCHETYPE_WEIGHT,
            cluster_weight=_DEFAULT_CLUSTER_WEIGHT,
            persistence_weight=_DEFAULT_PERSISTENCE_WEIGHT,
            decay_weight=_DEFAULT_DECAY_WEIGHT,
            agreement_threshold=_DEFAULT_AGREEMENT_THRESHOLD,
            partial_agreement_threshold=_DEFAULT_PARTIAL_AGREEMENT_THRESHOLD,
            conflict_threshold=_DEFAULT_CONFLICT_THRESHOLD,
            unreliable_threshold=_DEFAULT_UNRELIABLE_THRESHOLD,
            metadata={"source": "default", "scoring_version": _SCORING_VERSION},
        )


@dataclass
class LayerAgreementSnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    conflict_policy_profile_id: str | None
    dominant_timing_class: str | None
    dominant_transition_state: str | None
    dominant_sequence_class: str | None
    dominant_archetype_key: str | None
    cluster_state: str | None
    persistence_state: str | None
    freshness_state: str | None
    timing_direction: str | None
    transition_direction: str | None
    archetype_direction: str | None
    cluster_direction: str | None
    persistence_direction: str | None
    decay_direction: str | None
    supportive_weight: float | None
    suppressive_weight: float | None
    neutral_weight: float | None
    missing_weight: float | None
    agreement_score: float | None
    conflict_score: float | None
    layer_consensus_state: str
    dominant_conflict_source: str | None
    conflict_reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FamilyLayerAgreementSnapshot:
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
    freshness_state: str | None
    family_contribution: float | None
    transition_direction: str | None
    archetype_direction: str | None
    cluster_direction: str | None
    persistence_direction: str | None
    decay_direction: str | None
    agreement_score: float | None
    conflict_score: float | None
    family_consensus_state: str
    dominant_conflict_source: str | None
    family_rank: int | None
    conflict_reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LayerConflictEvent:
    workspace_id: str
    watchlist_id: str | None
    source_run_id: str | None
    target_run_id: str
    prior_consensus_state: str | None
    current_consensus_state: str
    prior_dominant_conflict_source: str | None
    current_dominant_conflict_source: str | None
    prior_agreement_score: float | None
    current_agreement_score: float | None
    prior_conflict_score: float | None
    current_conflict_score: float | None
    event_type: str
    reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


class CrossAssetLayerConflictService:
    """Deterministic cross-layer agreement / conflict diagnostics."""

    # ── policy ──────────────────────────────────────────────────────────
    def get_active_conflict_policy(
        self, conn, *, workspace_id: str,
    ) -> ConflictPolicyProfile:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, workspace_id::text as workspace_id,
                       profile_name, is_active,
                       timing_weight, transition_weight, archetype_weight,
                       cluster_weight, persistence_weight, decay_weight,
                       agreement_threshold, partial_agreement_threshold,
                       conflict_threshold, unreliable_threshold,
                       metadata
                from public.cross_asset_conflict_policy_profiles
                where workspace_id = %s::uuid
                  and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            if not row:
                return ConflictPolicyProfile.default(workspace_id)
            d = dict(row)
            return ConflictPolicyProfile(
                id=d.get("id"),
                workspace_id=d.get("workspace_id") or workspace_id,
                profile_name=d.get("profile_name") or "active",
                is_active=bool(d.get("is_active", True)),
                timing_weight=float(d.get("timing_weight") or _DEFAULT_TIMING_WEIGHT),
                transition_weight=float(d.get("transition_weight") or _DEFAULT_TRANSITION_WEIGHT),
                archetype_weight=float(d.get("archetype_weight") or _DEFAULT_ARCHETYPE_WEIGHT),
                cluster_weight=float(d.get("cluster_weight") or _DEFAULT_CLUSTER_WEIGHT),
                persistence_weight=float(d.get("persistence_weight") or _DEFAULT_PERSISTENCE_WEIGHT),
                decay_weight=float(d.get("decay_weight") or _DEFAULT_DECAY_WEIGHT),
                agreement_threshold=float(d.get("agreement_threshold") or _DEFAULT_AGREEMENT_THRESHOLD),
                partial_agreement_threshold=float(
                    d.get("partial_agreement_threshold") or _DEFAULT_PARTIAL_AGREEMENT_THRESHOLD
                ),
                conflict_threshold=float(d.get("conflict_threshold") or _DEFAULT_CONFLICT_THRESHOLD),
                unreliable_threshold=float(d.get("unreliable_threshold") or _DEFAULT_UNRELIABLE_THRESHOLD),
                metadata=dict(d.get("metadata") or {}),
            )

    # ── inputs ──────────────────────────────────────────────────────────
    def load_current_layer_context(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, Any]:
        """Stitch together the run-level layer context from existing
        diagnostic summaries."""
        ctx: dict[str, Any] = {
            "context_snapshot_id":         None,
            "dominant_timing_class":       None,
            "dominant_transition_state":   None,
            "dominant_sequence_class":     None,
            "dominant_archetype_key":      None,
            "cluster_state":               None,
            "persistence_state":           None,
            "freshness_state":             None,
            "decay_aggregate_score":       None,
            "stale_memory_flag":           None,
            "contradiction_flag":          None,
            "regime_key":                  None,
        }
        with conn.cursor() as cur:
            # Persistence + regime + cluster + archetype context.
            cur.execute(
                """
                select context_snapshot_id::text as context_snapshot_id,
                       regime_key, cluster_state,
                       dominant_archetype_key, persistence_state
                from public.cross_asset_state_persistence_summary
                where run_id = %s::uuid and workspace_id = %s::uuid and watchlist_id = %s::uuid
                limit 1
                """,
                (run_id, workspace_id, watchlist_id),
            )
            r = cur.fetchone()
            if r:
                d = dict(r)
                ctx["context_snapshot_id"]    = d.get("context_snapshot_id")
                ctx["regime_key"]             = d.get("regime_key")
                ctx["cluster_state"]          = d.get("cluster_state")
                ctx["dominant_archetype_key"] = d.get("dominant_archetype_key")
                ctx["persistence_state"]      = d.get("persistence_state")

            # Transition diagnostics → timing class + transition state + sequence class.
            cur.execute(
                """
                select dominant_timing_class, dominant_transition_state, dominant_sequence_class
                from public.run_cross_asset_transition_diagnostics_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                d = dict(r)
                ctx["dominant_timing_class"]     = d.get("dominant_timing_class")
                ctx["dominant_transition_state"] = d.get("dominant_transition_state")
                ctx["dominant_sequence_class"]   = d.get("dominant_sequence_class")

            # Decay state.
            cur.execute(
                """
                select freshness_state, aggregate_decay_score,
                       stale_memory_flag, contradiction_flag
                from public.run_cross_asset_signal_decay_summary
                where run_id = %s::uuid and workspace_id = %s::uuid
                limit 1
                """,
                (run_id, workspace_id),
            )
            r = cur.fetchone()
            if r:
                d = dict(r)
                ctx["freshness_state"]       = d.get("freshness_state")
                ctx["decay_aggregate_score"] = _as_float(d.get("aggregate_decay_score"))
                ctx["stale_memory_flag"]     = bool(d.get("stale_memory_flag")) if d.get("stale_memory_flag") is not None else None
                ctx["contradiction_flag"]    = bool(d.get("contradiction_flag")) if d.get("contradiction_flag") is not None else None

        return ctx

    def load_family_layer_context(
        self, conn, *, run_id: str,
    ) -> list[dict[str, Any]]:
        """Per-family stitched context from family-level summaries."""
        out: dict[str, dict[str, Any]] = {}
        with conn.cursor() as cur:
            # Family archetype carries transition_state, sequence_class, archetype_key.
            cur.execute(
                """
                select dependency_family, context_snapshot_id::text as context_snapshot_id,
                       transition_state, dominant_sequence_class,
                       archetype_key, family_contribution, family_rank
                from public.cross_asset_family_archetype_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = d.get("dependency_family")
                if not fam:
                    continue
                out[str(fam)] = {
                    "context_snapshot_id":     d.get("context_snapshot_id"),
                    "transition_state":        d.get("transition_state"),
                    "dominant_sequence_class": d.get("dominant_sequence_class"),
                    "archetype_key":           d.get("archetype_key"),
                    "family_contribution":     _as_float(d.get("family_contribution")),
                    "family_rank":             d.get("family_rank"),
                    "cluster_state":           None,
                    "persistence_state":       None,
                    "freshness_state":         None,
                }

            # Family persistence attribution carries persistence_state and cluster proxy.
            cur.execute(
                """
                select dependency_family, persistence_state
                from public.cross_asset_family_persistence_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d.get("dependency_family") or "")
                if not fam:
                    continue
                out.setdefault(fam, {})["persistence_state"] = d.get("persistence_state")

            # Family signal decay summary → freshness_state per family.
            cur.execute(
                """
                select dependency_family, family_freshness_state, cluster_state
                from public.cross_asset_family_signal_decay_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d.get("dependency_family") or "")
                if not fam:
                    continue
                out.setdefault(fam, {})["freshness_state"] = d.get("family_freshness_state")
                if d.get("cluster_state"):
                    out[fam]["cluster_state"] = d.get("cluster_state")

        return [{"dependency_family": k, **v} for k, v in out.items()]

    def _load_prior_layer_agreement(
        self, conn, *, workspace_id: str, watchlist_id: str, before_run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text as run_id,
                       layer_consensus_state,
                       dominant_conflict_source,
                       agreement_score, conflict_score, created_at
                from public.cross_asset_layer_agreement_summary
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

    # ── direction mapping ───────────────────────────────────────────────
    @staticmethod
    def map_timing_direction(timing_class: str | None) -> str:
        if timing_class is None:
            return "missing"
        s = str(timing_class).lower()
        if s == "lead":
            return "supportive"
        if s == "lag":
            return "suppressive"
        if s == "coincident":
            return "neutral"
        if s in ("insufficient_data", "insufficient_history", ""):
            return "missing"
        return "neutral"

    @staticmethod
    def map_transition_direction(transition_state: str | None) -> str:
        if transition_state is None:
            return "missing"
        s = str(transition_state).lower()
        if s in ("reinforcing", "recovering", "rotating_in"):
            return "supportive"
        if s in ("deteriorating", "rotating_out"):
            return "suppressive"
        if s == "stable":
            return "neutral"
        return "missing"

    @staticmethod
    def map_archetype_direction(archetype_key: str | None) -> str:
        if archetype_key is None:
            return "missing"
        s = str(archetype_key).lower()
        if s in ("reinforcing_continuation", "recovering_reentry", "rotation_handoff"):
            return "supportive"
        if s in ("deteriorating_breakdown", "mixed_transition_noise"):
            return "suppressive"
        return "missing"

    @staticmethod
    def map_cluster_direction(cluster_state: str | None) -> str:
        if cluster_state is None:
            return "missing"
        s = str(cluster_state).lower()
        if s in ("stable", "recovering"):
            return "supportive"
        if s in ("deteriorating", "mixed"):
            return "suppressive"
        if s == "rotating":
            return "neutral"
        return "missing"

    @staticmethod
    def map_persistence_direction(persistence_state: str | None) -> str:
        if persistence_state is None:
            return "missing"
        s = str(persistence_state).lower()
        if s in ("persistent", "recovering"):
            return "supportive"
        if s in ("fragile", "breaking_down", "mixed"):
            return "suppressive"
        if s == "rotating":
            return "neutral"
        return "missing"

    @staticmethod
    def map_decay_direction(freshness_state: str | None) -> str:
        if freshness_state is None:
            return "missing"
        s = str(freshness_state).lower()
        if s == "fresh":
            return "supportive"
        if s == "decaying":
            return "neutral"
        if s in ("stale", "contradicted", "mixed"):
            return "suppressive"
        return "missing"

    # ── scoring + classification ────────────────────────────────────────
    def compute_weighted_agreement(
        self, *, directions: dict[str, str], profile: ConflictPolicyProfile,
    ) -> dict[str, Any]:
        """Return weighted shares + agreement / conflict score + dominant direction."""
        weight_map = {
            "timing":      profile.timing_weight,
            "transition":  profile.transition_weight,
            "archetype":   profile.archetype_weight,
            "cluster":     profile.cluster_weight,
            "persistence": profile.persistence_weight,
            "decay":       profile.decay_weight,
        }
        total = sum(weight_map.values()) or 1.0
        # Normalise weights so they sum to 1 even if profile drifts.
        norm = {k: (v / total) for k, v in weight_map.items()}

        supportive_w  = 0.0
        suppressive_w = 0.0
        neutral_w     = 0.0
        missing_w     = 0.0
        per_layer: dict[str, tuple[str, float]] = {}

        for layer, direction in directions.items():
            w = norm.get(layer, 0.0)
            per_layer[layer] = (direction, w)
            if direction == "supportive":
                supportive_w += w
            elif direction == "suppressive":
                suppressive_w += w
            elif direction == "neutral":
                neutral_w += w
            else:
                missing_w += w

        non_missing = supportive_w + suppressive_w + neutral_w
        if non_missing <= 0.0:
            return {
                "supportive_weight":  0.0,
                "suppressive_weight": 0.0,
                "neutral_weight":     0.0,
                "missing_weight":     missing_w,
                "non_missing_weight": 0.0,
                "agreement_score":    None,
                "conflict_score":     None,
                "dominant_direction": "missing",
                "per_layer":          per_layer,
            }

        # Dominant non-missing direction by weight.
        candidates = {
            "supportive":  supportive_w,
            "suppressive": suppressive_w,
            "neutral":     neutral_w,
        }
        dominant_direction = max(candidates, key=lambda k: (candidates[k], k))
        dominant_w = candidates[dominant_direction]
        agreement_score = dominant_w / non_missing

        # Conflict: simultaneous supportive + suppressive evidence.
        conflict_score = min(supportive_w, suppressive_w) / non_missing

        return {
            "supportive_weight":  supportive_w,
            "suppressive_weight": suppressive_w,
            "neutral_weight":     neutral_w,
            "missing_weight":     missing_w,
            "non_missing_weight": non_missing,
            "agreement_score":    agreement_score,
            "conflict_score":     conflict_score,
            "dominant_direction": dominant_direction,
            "per_layer":          per_layer,
        }

    def classify_consensus_state(
        self,
        *,
        scores: dict[str, Any],
        profile: ConflictPolicyProfile,
    ) -> str:
        non_missing = float(scores.get("non_missing_weight") or 0.0)
        if non_missing < _MIN_NON_MISSING_WEIGHT:
            return "insufficient_context"

        agreement = scores.get("agreement_score")
        conflict = scores.get("conflict_score")
        dominant = scores.get("dominant_direction") or "missing"

        # Conflicted overrides aligned states when conflict is high.
        if conflict is not None and conflict >= profile.conflict_threshold:
            return "conflicted"
        if agreement is None:
            return "insufficient_context"
        if agreement <= profile.unreliable_threshold:
            return "unreliable"
        if agreement >= profile.agreement_threshold:
            if dominant == "supportive":
                return "aligned_supportive"
            if dominant == "suppressive":
                return "aligned_suppressive"
            # Dominant neutral with high agreement → partial agreement.
            return "partial_agreement"
        if agreement >= profile.partial_agreement_threshold:
            return "partial_agreement"
        return "unreliable"

    @staticmethod
    def _identify_dominant_conflict_source(
        per_layer: dict[str, tuple[str, float]],
    ) -> tuple[str | None, list[str]]:
        """Pick which layer carries the largest minority direction relative to
        the dominant direction. Returns (dominant_conflict_source, reason_codes)."""
        # Tally direction → list of (layer, weight)
        by_direction: dict[str, list[tuple[str, float]]] = {
            "supportive": [], "suppressive": [], "neutral": [], "missing": [],
        }
        for layer, (direction, w) in per_layer.items():
            by_direction.setdefault(direction, []).append((layer, w))

        sup = sum(w for _, w in by_direction["supportive"])
        sup_pen = sum(w for _, w in by_direction["suppressive"])
        if sup == 0 and sup_pen == 0:
            return (None, [])

        # The conflict source is the layer in the minority direction with the
        # largest weight. Choose minority direction = the one with smaller
        # total non-zero weight (tie-break: prefer suppressive).
        if sup >= sup_pen:
            minority_layers = sorted(by_direction["suppressive"], key=lambda t: (-t[1], t[0]))
        else:
            minority_layers = sorted(by_direction["supportive"], key=lambda t: (-t[1], t[0]))
        if not minority_layers:
            return (None, [])
        dominant_source = minority_layers[0][0]

        reasons: list[str] = []
        sup_layers = {layer for layer, _ in by_direction["supportive"]}
        sup_pen_layers = {layer for layer, _ in by_direction["suppressive"]}
        if sup > 0 and sup_pen > 0:
            reasons.append("supportive_and_suppressive_layers_present")
        # Pairwise reason codes for known frictions.
        if "timing" in sup_layers and "decay" in sup_pen_layers:
            reasons.append("timing_vs_decay_conflict")
        if "decay" in sup_layers and "timing" in sup_pen_layers:
            reasons.append("timing_vs_decay_conflict")
        if "transition" in sup_layers and "persistence" in sup_pen_layers:
            reasons.append("transition_vs_persistence_conflict")
        if "persistence" in sup_layers and "transition" in sup_pen_layers:
            reasons.append("transition_vs_persistence_conflict")
        if "archetype" in sup_layers and "cluster" in sup_pen_layers:
            reasons.append("archetype_vs_cluster_conflict")
        if "cluster" in sup_layers and "archetype" in sup_pen_layers:
            reasons.append("archetype_vs_cluster_conflict")
        if "cluster" in sup_layers and "decay" in sup_pen_layers:
            reasons.append("cluster_vs_decay_conflict")
        if "decay" in sup_layers and "cluster" in sup_pen_layers:
            reasons.append("cluster_vs_decay_conflict")
        return (dominant_source, list(dict.fromkeys(reasons)))

    # ── family-level agreement ──────────────────────────────────────────
    def compute_family_layer_agreement(
        self,
        *,
        family_ctx: dict[str, Any],
        run_freshness_state: str | None,
        run_persistence_state: str | None,
        run_cluster_state: str | None,
        profile: ConflictPolicyProfile,
    ) -> dict[str, Any]:
        # Family layers exclude timing (which is run-level only in the live
        # surfaces), so we use a 5-layer scoring with the timing weight folded
        # into the other layer weights proportionally.
        cluster_state = family_ctx.get("cluster_state") or run_cluster_state
        persistence_state = family_ctx.get("persistence_state") or run_persistence_state
        freshness_state = family_ctx.get("freshness_state") or run_freshness_state

        directions = {
            "transition":  self.map_transition_direction(family_ctx.get("transition_state")),
            "archetype":   self.map_archetype_direction(family_ctx.get("archetype_key")),
            "cluster":     self.map_cluster_direction(cluster_state),
            "persistence": self.map_persistence_direction(persistence_state),
            "decay":       self.map_decay_direction(freshness_state),
        }

        # Reuse run-level scoring helper but with a profile that has timing=0.
        family_profile = ConflictPolicyProfile(
            id=profile.id, workspace_id=profile.workspace_id,
            profile_name=profile.profile_name, is_active=profile.is_active,
            timing_weight=0.0,
            transition_weight=profile.transition_weight,
            archetype_weight=profile.archetype_weight,
            cluster_weight=profile.cluster_weight,
            persistence_weight=profile.persistence_weight,
            decay_weight=profile.decay_weight,
            agreement_threshold=profile.agreement_threshold,
            partial_agreement_threshold=profile.partial_agreement_threshold,
            conflict_threshold=profile.conflict_threshold,
            unreliable_threshold=profile.unreliable_threshold,
            metadata=profile.metadata,
        )
        scores = self.compute_weighted_agreement(
            directions={"timing": "missing", **directions},
            profile=family_profile,
        )
        consensus = self.classify_consensus_state(scores=scores, profile=family_profile)
        dominant_source, reason_codes = self._identify_dominant_conflict_source(
            scores["per_layer"]
        )
        return {
            "transition_direction":   directions["transition"],
            "archetype_direction":    directions["archetype"],
            "cluster_direction":      directions["cluster"],
            "persistence_direction":  directions["persistence"],
            "decay_direction":        directions["decay"],
            "agreement_score":        scores["agreement_score"],
            "conflict_score":         scores["conflict_score"],
            "family_consensus_state": consensus,
            "dominant_conflict_source": dominant_source,
            "conflict_reason_codes":  reason_codes,
            "cluster_state":          cluster_state,
            "persistence_state":      persistence_state,
            "freshness_state":        freshness_state,
        }

    # ── conflict event detection ────────────────────────────────────────
    def detect_conflict_event(
        self,
        *,
        workspace_id: str,
        watchlist_id: str,
        prior: dict[str, Any] | None,
        current_run_id: str,
        current_snap: LayerAgreementSnapshot,
    ) -> LayerConflictEvent:
        if prior is None:
            return LayerConflictEvent(
                workspace_id=workspace_id, watchlist_id=watchlist_id,
                source_run_id=None, target_run_id=current_run_id,
                prior_consensus_state=None,
                current_consensus_state=current_snap.layer_consensus_state,
                prior_dominant_conflict_source=None,
                current_dominant_conflict_source=current_snap.dominant_conflict_source,
                prior_agreement_score=None,
                current_agreement_score=current_snap.agreement_score,
                prior_conflict_score=None,
                current_conflict_score=current_snap.conflict_score,
                event_type="insufficient_context",
                reason_codes=["insufficient_context"],
                metadata={"scoring_version": _SCORING_VERSION, "no_prior_window": True},
            )

        prior_state    = str(prior.get("layer_consensus_state") or "insufficient_context")
        prior_source   = prior.get("dominant_conflict_source")
        prior_agree    = _as_float(prior.get("agreement_score"))
        prior_conflict = _as_float(prior.get("conflict_score"))
        prior_run_id   = prior.get("run_id")

        current_state    = current_snap.layer_consensus_state
        current_source   = current_snap.dominant_conflict_source
        current_agree    = current_snap.agreement_score
        current_conflict = current_snap.conflict_score

        agree_delta = (
            None if prior_agree is None or current_agree is None
            else current_agree - prior_agree
        )

        reasons: list[str] = []
        if (
            agree_delta is not None
            and abs(agree_delta) >= _AGREEMENT_DELTA_MATERIAL
        ):
            if agree_delta > 0:
                reasons.append("agreement_score_improved")
            else:
                reasons.append("agreement_score_deteriorated")
        if current_state == "insufficient_context":
            reasons.append("insufficient_context")
        if current_state == "unreliable":
            reasons.append("low_non_missing_weight")
        for code in current_snap.conflict_reason_codes or []:
            reasons.append(code)

        # Event-type rules — explicit and deterministic.
        if current_state == "unreliable":
            event_type = "unreliable_stack_detected"
        elif current_state == "conflicted" and prior_state != "conflicted":
            event_type = "conflict_emerged"
        elif prior_state == "conflicted" and current_state != "conflicted":
            event_type = "conflict_resolved"
        elif current_state == "insufficient_context" or prior_state == "insufficient_context":
            event_type = "insufficient_context"
        elif agree_delta is not None and agree_delta >= _AGREEMENT_DELTA_MATERIAL:
            event_type = "agreement_strengthened"
        elif agree_delta is not None and agree_delta <= -_AGREEMENT_DELTA_MATERIAL:
            event_type = "agreement_weakened"
        else:
            # Fallback: classify by dominant direction continuity.
            if current_state in ("aligned_supportive", "aligned_suppressive", "partial_agreement"):
                event_type = "agreement_strengthened" if (agree_delta or 0.0) >= 0 else "agreement_weakened"
            else:
                event_type = "insufficient_context"

        return LayerConflictEvent(
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=str(prior_run_id) if prior_run_id else None,
            target_run_id=current_run_id,
            prior_consensus_state=prior_state,
            current_consensus_state=current_state,
            prior_dominant_conflict_source=_norm(prior_source),
            current_dominant_conflict_source=_norm(current_source),
            prior_agreement_score=prior_agree,
            current_agreement_score=current_agree,
            prior_conflict_score=prior_conflict,
            current_conflict_score=current_conflict,
            event_type=event_type,
            reason_codes=list(dict.fromkeys(reasons)) or [event_type],
            metadata={"scoring_version": _SCORING_VERSION, "prior_run_id": prior_run_id},
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_layer_agreement_snapshot(
        self, conn, *, snap: LayerAgreementSnapshot,
    ) -> str:
        import src.db.repositories_48a as repo
        row = repo.insert_cross_asset_layer_agreement_snapshot(
            conn,
            workspace_id=snap.workspace_id,
            watchlist_id=snap.watchlist_id,
            run_id=snap.run_id,
            context_snapshot_id=snap.context_snapshot_id,
            conflict_policy_profile_id=snap.conflict_policy_profile_id,
            dominant_timing_class=snap.dominant_timing_class,
            dominant_transition_state=snap.dominant_transition_state,
            dominant_sequence_class=snap.dominant_sequence_class,
            dominant_archetype_key=snap.dominant_archetype_key,
            cluster_state=snap.cluster_state,
            persistence_state=snap.persistence_state,
            freshness_state=snap.freshness_state,
            timing_direction=snap.timing_direction,
            transition_direction=snap.transition_direction,
            archetype_direction=snap.archetype_direction,
            cluster_direction=snap.cluster_direction,
            persistence_direction=snap.persistence_direction,
            decay_direction=snap.decay_direction,
            supportive_weight=snap.supportive_weight,
            suppressive_weight=snap.suppressive_weight,
            neutral_weight=snap.neutral_weight,
            missing_weight=snap.missing_weight,
            agreement_score=snap.agreement_score,
            conflict_score=snap.conflict_score,
            layer_consensus_state=snap.layer_consensus_state,
            dominant_conflict_source=snap.dominant_conflict_source,
            conflict_reason_codes=snap.conflict_reason_codes,
            metadata=snap.metadata,
        )
        return str(row["id"])

    def persist_family_layer_agreement_snapshots(
        self, conn, *, snaps: list[FamilyLayerAgreementSnapshot],
    ) -> list[str]:
        if not snaps:
            return []
        import src.db.repositories_48a as repo
        ids: list[str] = []
        for snap in snaps:
            row = repo.insert_cross_asset_family_layer_agreement_snapshot(
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
                freshness_state=snap.freshness_state,
                family_contribution=snap.family_contribution,
                transition_direction=snap.transition_direction,
                archetype_direction=snap.archetype_direction,
                cluster_direction=snap.cluster_direction,
                persistence_direction=snap.persistence_direction,
                decay_direction=snap.decay_direction,
                agreement_score=snap.agreement_score,
                conflict_score=snap.conflict_score,
                family_consensus_state=snap.family_consensus_state,
                dominant_conflict_source=snap.dominant_conflict_source,
                family_rank=snap.family_rank,
                conflict_reason_codes=snap.conflict_reason_codes,
                metadata=snap.metadata,
            )
            ids.append(str(row["id"]))
        return ids

    def persist_layer_conflict_event(
        self, conn, *, event: LayerConflictEvent,
    ) -> str:
        import src.db.repositories_48a as repo
        row = repo.insert_cross_asset_layer_conflict_event_snapshot(
            conn,
            workspace_id=event.workspace_id,
            watchlist_id=event.watchlist_id,
            source_run_id=event.source_run_id,
            target_run_id=event.target_run_id,
            prior_consensus_state=event.prior_consensus_state,
            current_consensus_state=event.current_consensus_state,
            prior_dominant_conflict_source=event.prior_dominant_conflict_source,
            current_dominant_conflict_source=event.current_dominant_conflict_source,
            prior_agreement_score=event.prior_agreement_score,
            current_agreement_score=event.current_agreement_score,
            prior_conflict_score=event.prior_conflict_score,
            current_conflict_score=event.current_conflict_score,
            event_type=event.event_type,
            reason_codes=event.reason_codes,
            metadata=event.metadata,
        )
        return str(row["id"])

    # ── orchestration ───────────────────────────────────────────────────
    def build_and_persist_for_run(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> bool:
        profile = self.get_active_conflict_policy(conn, workspace_id=workspace_id)
        ctx = self.load_current_layer_context(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        directions = {
            "timing":      self.map_timing_direction(ctx.get("dominant_timing_class")),
            "transition":  self.map_transition_direction(ctx.get("dominant_transition_state")),
            "archetype":   self.map_archetype_direction(ctx.get("dominant_archetype_key")),
            "cluster":     self.map_cluster_direction(ctx.get("cluster_state")),
            "persistence": self.map_persistence_direction(ctx.get("persistence_state")),
            "decay":       self.map_decay_direction(ctx.get("freshness_state")),
        }
        scores = self.compute_weighted_agreement(directions=directions, profile=profile)
        consensus = self.classify_consensus_state(scores=scores, profile=profile)
        dominant_source, reasons = self._identify_dominant_conflict_source(scores["per_layer"])

        common_meta = {
            "scoring_version":            _SCORING_VERSION,
            "policy_profile_id":          profile.id,
            "policy_profile_name":        profile.profile_name,
            "default_conflict_profile_used": profile.id is None,
            "agreement_threshold":        profile.agreement_threshold,
            "partial_agreement_threshold": profile.partial_agreement_threshold,
            "conflict_threshold":         profile.conflict_threshold,
            "unreliable_threshold":       profile.unreliable_threshold,
            "min_non_missing_weight":     _MIN_NON_MISSING_WEIGHT,
            "agreement_delta_material":   _AGREEMENT_DELTA_MATERIAL,
        }

        run_snap = LayerAgreementSnapshot(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=ctx.get("context_snapshot_id"),
            conflict_policy_profile_id=profile.id,
            dominant_timing_class=ctx.get("dominant_timing_class"),
            dominant_transition_state=ctx.get("dominant_transition_state"),
            dominant_sequence_class=ctx.get("dominant_sequence_class"),
            dominant_archetype_key=ctx.get("dominant_archetype_key"),
            cluster_state=ctx.get("cluster_state"),
            persistence_state=ctx.get("persistence_state"),
            freshness_state=ctx.get("freshness_state"),
            timing_direction=directions["timing"],
            transition_direction=directions["transition"],
            archetype_direction=directions["archetype"],
            cluster_direction=directions["cluster"],
            persistence_direction=directions["persistence"],
            decay_direction=directions["decay"],
            supportive_weight=scores["supportive_weight"],
            suppressive_weight=scores["suppressive_weight"],
            neutral_weight=scores["neutral_weight"],
            missing_weight=scores["missing_weight"],
            agreement_score=scores["agreement_score"],
            conflict_score=scores["conflict_score"],
            layer_consensus_state=consensus,
            dominant_conflict_source=dominant_source,
            conflict_reason_codes=list(reasons),
            metadata=common_meta,
        )
        prior = self._load_prior_layer_agreement(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            before_run_id=run_id,
        )
        self.persist_layer_agreement_snapshot(conn, snap=run_snap)

        # Family-level agreement.
        fam_rows = self.load_family_layer_context(conn, run_id=run_id)
        family_snaps: list[FamilyLayerAgreementSnapshot] = []
        for fctx in fam_rows:
            fam_result = self.compute_family_layer_agreement(
                family_ctx=fctx,
                run_freshness_state=ctx.get("freshness_state"),
                run_persistence_state=ctx.get("persistence_state"),
                run_cluster_state=ctx.get("cluster_state"),
                profile=profile,
            )
            family_snaps.append(FamilyLayerAgreementSnapshot(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                run_id=run_id,
                context_snapshot_id=fctx.get("context_snapshot_id") or ctx.get("context_snapshot_id"),
                dependency_family=str(fctx.get("dependency_family") or ""),
                transition_state=fctx.get("transition_state"),
                dominant_sequence_class=fctx.get("dominant_sequence_class"),
                archetype_key=fctx.get("archetype_key"),
                cluster_state=fam_result["cluster_state"],
                persistence_state=fam_result["persistence_state"],
                freshness_state=fam_result["freshness_state"],
                family_contribution=fctx.get("family_contribution"),
                transition_direction=fam_result["transition_direction"],
                archetype_direction=fam_result["archetype_direction"],
                cluster_direction=fam_result["cluster_direction"],
                persistence_direction=fam_result["persistence_direction"],
                decay_direction=fam_result["decay_direction"],
                agreement_score=fam_result["agreement_score"],
                conflict_score=fam_result["conflict_score"],
                family_consensus_state=fam_result["family_consensus_state"],
                dominant_conflict_source=fam_result["dominant_conflict_source"],
                family_rank=fctx.get("family_rank"),
                conflict_reason_codes=fam_result["conflict_reason_codes"],
                metadata=common_meta,
            ))
        if family_snaps:
            self.persist_family_layer_agreement_snapshots(conn, snaps=family_snaps)

        # Conflict event.
        event = self.detect_conflict_event(
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            prior=prior, current_run_id=run_id, current_snap=run_snap,
        )
        if event is not None:
            self.persist_layer_conflict_event(conn, event=event)
        return True

    def refresh_workspace_layer_conflict(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> int:
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]
        total = 0
        for wid in watchlist_ids:
            try:
                ok = self.build_and_persist_for_run(
                    conn, workspace_id=workspace_id, watchlist_id=wid, run_id=run_id,
                )
                if ok:
                    conn.commit()
                    total += 1
            except Exception as exc:
                logger.warning(
                    "cross_asset_layer_conflict: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return total
