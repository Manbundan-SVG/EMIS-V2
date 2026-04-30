"""Phase 4.8C: Conflict-Aware Composite Refinement.

Final conflict-aware integration layer. Starts from the most mature
upstream composite (4.7C composite_post_decay -> 4.6C composite_post_persistence
-> 4.5C composite_post_cluster -> 4.4C composite_post_archetype -> 4.3C
composite_post_transition -> 4.2C composite_post_timing -> regime / raw
fallback), adds a bounded conflict-aware delta conditioned on the run's
layer-consensus state, agreement score, conflict score, and dominant
conflict source from 4.8A and the per-family conflict-aware contribution
from 4.8B, and persists the result side-by-side with all upstream
layers.

Persists:
  * cross_asset_conflict_composite_snapshots (per-run/watchlist)
  * cross_asset_family_conflict_composite_snapshots (per-run/family)

All weights, scales, suppression terms, and bounds are deterministic and
metadata-stamped. No predictive forecasting in this phase. Replay-readiness
columns required by 4.8D (source_contribution_layer, source_composite_layer,
scoring_version, layer_consensus_state, agreement/conflict score, dominant
conflict source) are persisted explicitly so 4.8D can compare source vs
replay without recomputing upstream logic.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.8C.v1"

# Default profile values mirror migration defaults.
_DEFAULT_INTEGRATION_MODE              = "conflict_additive_guardrailed"
_DEFAULT_INTEGRATION_WEIGHT            = 0.10
_DEFAULT_ALIGNED_SUPPORTIVE_SCALE      = 1.08
_DEFAULT_ALIGNED_SUPPRESSIVE_SCALE     = 0.78
_DEFAULT_PARTIAL_AGREEMENT_SCALE       = 0.96
_DEFAULT_CONFLICTED_SCALE              = 0.72
_DEFAULT_UNRELIABLE_SCALE              = 0.65
_DEFAULT_INSUFFICIENT_CONTEXT_SCALE    = 0.80
_DEFAULT_CONFLICT_EXTRA_SUPPRESSION    = 0.03
_DEFAULT_UNRELIABLE_EXTRA_SUPPRESSION  = 0.04
_DEFAULT_MAX_POSITIVE                  = 0.20
_DEFAULT_MAX_NEGATIVE                  = 0.20

# Conservative net-contribution band — keeps conflict refinement bounded.
_NET_CONTRIBUTION_FLOOR  = -0.15
_NET_CONTRIBUTION_CEIL   =  0.15


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _suppress_toward_zero(v: float, magnitude: float) -> float:
    """Reduce magnitude by `magnitude` without inverting sign."""
    if v == 0 or magnitude <= 0:
        return v
    if v > 0:
        return max(0.0, v - magnitude)
    return min(0.0, v + magnitude)


@dataclass
class ConflictIntegrationProfile:
    id: str | None
    workspace_id: str
    profile_name: str
    is_active: bool
    integration_mode: str
    integration_weight: float
    aligned_supportive_scale: float
    aligned_suppressive_scale: float
    partial_agreement_scale: float
    conflicted_scale: float
    unreliable_scale: float
    insufficient_context_scale: float
    conflict_extra_suppression: float
    unreliable_extra_suppression: float
    dominant_conflict_source_suppression: dict[str, Any]
    max_positive_contribution: float
    max_negative_contribution: float
    metadata: dict[str, Any]

    @classmethod
    def default(cls, workspace_id: str) -> "ConflictIntegrationProfile":
        return cls(
            id=None,
            workspace_id=workspace_id,
            profile_name="default-conflict-integration",
            is_active=True,
            integration_mode=_DEFAULT_INTEGRATION_MODE,
            integration_weight=_DEFAULT_INTEGRATION_WEIGHT,
            aligned_supportive_scale=_DEFAULT_ALIGNED_SUPPORTIVE_SCALE,
            aligned_suppressive_scale=_DEFAULT_ALIGNED_SUPPRESSIVE_SCALE,
            partial_agreement_scale=_DEFAULT_PARTIAL_AGREEMENT_SCALE,
            conflicted_scale=_DEFAULT_CONFLICTED_SCALE,
            unreliable_scale=_DEFAULT_UNRELIABLE_SCALE,
            insufficient_context_scale=_DEFAULT_INSUFFICIENT_CONTEXT_SCALE,
            conflict_extra_suppression=_DEFAULT_CONFLICT_EXTRA_SUPPRESSION,
            unreliable_extra_suppression=_DEFAULT_UNRELIABLE_EXTRA_SUPPRESSION,
            dominant_conflict_source_suppression={},
            max_positive_contribution=_DEFAULT_MAX_POSITIVE,
            max_negative_contribution=_DEFAULT_MAX_NEGATIVE,
            metadata={"default_conflict_integration_profile_used": True},
        )


@dataclass
class ConflictCompositeSnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    conflict_integration_profile_id: str | None
    base_signal_score: float | None
    cross_asset_net_contribution: float | None
    weighted_cross_asset_net_contribution: float | None
    regime_adjusted_cross_asset_contribution: float | None
    timing_adjusted_cross_asset_contribution: float | None
    transition_adjusted_cross_asset_contribution: float | None
    archetype_adjusted_cross_asset_contribution: float | None
    cluster_adjusted_cross_asset_contribution: float | None
    persistence_adjusted_cross_asset_contribution: float | None
    decay_adjusted_cross_asset_contribution: float | None
    conflict_adjusted_cross_asset_contribution: float | None
    composite_pre_conflict: float | None
    conflict_net_contribution: float | None
    composite_post_conflict: float | None
    layer_consensus_state: str
    agreement_score: float | None
    conflict_score: float | None
    dominant_conflict_source: str | None
    integration_mode: str
    source_contribution_layer: str | None
    source_composite_layer: str | None
    scoring_version: str = _SCORING_VERSION
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FamilyConflictCompositeSnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    dependency_family: str
    family_consensus_state: str
    agreement_score: float | None
    conflict_score: float | None
    dominant_conflict_source: str | None
    conflict_adjusted_family_contribution: float | None
    integration_weight_applied: float | None
    conflict_integration_contribution: float | None
    family_rank: int | None
    top_symbols: list[str]
    reason_codes: list[str]
    source_contribution_layer: str | None
    scoring_version: str = _SCORING_VERSION
    metadata: dict[str, Any] = field(default_factory=dict)


class CrossAssetConflictCompositeService:
    """Phase 4.8C — conflict-aware composite refinement service."""

    # ── profiles ────────────────────────────────────────────────────────
    def get_active_conflict_integration_profile(
        self, conn, *, workspace_id: str,
    ) -> ConflictIntegrationProfile:
        import src.db.repositories_48c as repo
        row = repo.get_active_cross_asset_conflict_integration_profile(
            conn, workspace_id=workspace_id,
        )
        if not row:
            return ConflictIntegrationProfile.default(workspace_id)

        def _f(key: str, default: float) -> float:
            v = row.get(key)
            return float(v) if v is not None else default

        return ConflictIntegrationProfile(
            id=row.get("id"),
            workspace_id=str(row.get("workspace_id") or workspace_id),
            profile_name=str(row.get("profile_name") or "active-profile"),
            is_active=bool(row.get("is_active", True)),
            integration_mode=str(row.get("integration_mode") or _DEFAULT_INTEGRATION_MODE),
            integration_weight=_f("integration_weight", _DEFAULT_INTEGRATION_WEIGHT),
            aligned_supportive_scale=_f("aligned_supportive_scale", _DEFAULT_ALIGNED_SUPPORTIVE_SCALE),
            aligned_suppressive_scale=_f("aligned_suppressive_scale", _DEFAULT_ALIGNED_SUPPRESSIVE_SCALE),
            partial_agreement_scale=_f("partial_agreement_scale", _DEFAULT_PARTIAL_AGREEMENT_SCALE),
            conflicted_scale=_f("conflicted_scale", _DEFAULT_CONFLICTED_SCALE),
            unreliable_scale=_f("unreliable_scale", _DEFAULT_UNRELIABLE_SCALE),
            insufficient_context_scale=_f("insufficient_context_scale", _DEFAULT_INSUFFICIENT_CONTEXT_SCALE),
            conflict_extra_suppression=_f("conflict_extra_suppression", _DEFAULT_CONFLICT_EXTRA_SUPPRESSION),
            unreliable_extra_suppression=_f("unreliable_extra_suppression", _DEFAULT_UNRELIABLE_EXTRA_SUPPRESSION),
            dominant_conflict_source_suppression=dict(row.get("dominant_conflict_source_suppression") or {}),
            max_positive_contribution=_f("max_positive_contribution", _DEFAULT_MAX_POSITIVE),
            max_negative_contribution=_f("max_negative_contribution", _DEFAULT_MAX_NEGATIVE),
            metadata=dict(row.get("metadata") or {}),
        )

    # ── upstream loaders (composite_pre_conflict fallback chain) ────────
    def _load_run_conflict_attribution(
        self, conn, *, run_id: str, workspace_id: str,
    ) -> dict[str, Any] | None:
        import src.db.repositories_48c as repo
        return repo.get_cross_asset_conflict_attribution_for_run(
            conn, run_id=run_id, workspace_id=workspace_id,
        )

    def _load_decay_composite_pre(self, conn, *, run_id: str) -> tuple[float | None, float | None, str | None]:
        import src.db.repositories_48c as repo
        row = repo.get_run_decay_composite(conn, run_id=run_id)
        if row and row.get("composite_post_decay") is not None:
            return _as_float(row["composite_post_decay"]), _as_float(row.get("base_signal_score")), "decay_4_7C"
        return None, None, None

    def _load_persistence_composite_pre(self, conn, *, run_id: str) -> tuple[float | None, float | None, str | None]:
        import src.db.repositories_48c as repo
        row = repo.get_run_persistence_composite(conn, run_id=run_id)
        if row and row.get("composite_post_persistence") is not None:
            return _as_float(row["composite_post_persistence"]), _as_float(row.get("base_signal_score")), "persistence_4_6C"
        return None, None, None

    def _load_cluster_composite_pre(self, conn, *, run_id: str) -> tuple[float | None, float | None, str | None]:
        import src.db.repositories_48c as repo
        row = repo.get_run_cluster_composite(conn, run_id=run_id)
        if row and row.get("composite_post_cluster") is not None:
            return _as_float(row["composite_post_cluster"]), _as_float(row.get("base_signal_score")), "cluster_4_5C"
        return None, None, None

    def _load_archetype_composite_pre(self, conn, *, run_id: str) -> tuple[float | None, float | None, str | None]:
        import src.db.repositories_48c as repo
        row = repo.get_run_archetype_composite(conn, run_id=run_id)
        if row and row.get("composite_post_archetype") is not None:
            return _as_float(row["composite_post_archetype"]), _as_float(row.get("base_signal_score")), "archetype_4_4C"
        return None, None, None

    def _load_transition_composite_pre(self, conn, *, run_id: str) -> tuple[float | None, float | None, str | None]:
        import src.db.repositories_48c as repo
        row = repo.get_run_transition_composite(conn, run_id=run_id)
        if row and row.get("composite_post_transition") is not None:
            return _as_float(row["composite_post_transition"]), _as_float(row.get("base_signal_score")), "transition_4_3C"
        return None, None, None

    def _load_timing_composite_pre(self, conn, *, run_id: str) -> tuple[float | None, float | None, str | None]:
        import src.db.repositories_48c as repo
        row = repo.get_run_timing_composite(conn, run_id=run_id)
        if row and row.get("composite_post_timing") is not None:
            return _as_float(row["composite_post_timing"]), _as_float(row.get("base_signal_score")), "timing_4_2C"
        return None, None, None

    def _resolve_composite_pre_conflict(
        self, conn, *, run_id: str, run_attribution: dict[str, Any] | None,
    ) -> tuple[float | None, float | None, str]:
        """Walk fallback chain for composite_pre_conflict.

        Returns (composite_value, base_signal_score, source_label).
        """
        for loader in (
            self._load_decay_composite_pre,
            self._load_persistence_composite_pre,
            self._load_cluster_composite_pre,
            self._load_archetype_composite_pre,
            self._load_transition_composite_pre,
            self._load_timing_composite_pre,
        ):
            value, base, label = loader(conn, run_id=run_id)
            if value is not None:
                return value, base, label

        # Final fallback: derive from current attribution-stack contribution
        # so we don't return None and skip 4.8C entirely. We use the most
        # mature contribution available in the 4.8B run summary.
        if run_attribution:
            for key, label in (
                ("decay_adjusted_cross_asset_contribution",       "fallback_decay_attribution_4_7B"),
                ("persistence_adjusted_cross_asset_contribution", "fallback_persistence_attribution_4_6B"),
                ("cluster_adjusted_cross_asset_contribution",     "fallback_cluster_attribution_4_5B"),
                ("archetype_adjusted_cross_asset_contribution",   "fallback_archetype_attribution_4_4B"),
                ("transition_adjusted_cross_asset_contribution",  "fallback_transition_attribution_4_3B"),
                ("timing_adjusted_cross_asset_contribution",      "fallback_timing_attribution_4_2B"),
                ("regime_adjusted_cross_asset_contribution",      "fallback_regime_attribution_4_1C"),
                ("weighted_cross_asset_net_contribution",         "fallback_weighted_attribution_4_1B"),
                ("cross_asset_net_contribution",                  "fallback_raw_attribution_4_1A"),
            ):
                v = _as_float(run_attribution.get(key))
                if v is not None:
                    return v, None, label

        return None, None, "no_upstream_composite_available"

    # ── source_contribution_layer chooser ───────────────────────────────
    @staticmethod
    def _resolve_source_contribution_layer(run_attribution: dict[str, Any] | None) -> tuple[float | None, str]:
        """Pick the most mature contribution to use as the conflict-input.

        Priority: conflict 4.8B -> decay 4.7B -> persistence 4.6B -> cluster
        4.5B -> archetype 4.4B -> transition 4.3B -> timing 4.2B -> regime
        4.1C -> weighted 4.1B -> raw 4.1A.
        """
        if not run_attribution:
            return None, "no_upstream_attribution_available"
        for key, label in (
            ("conflict_adjusted_cross_asset_contribution",    "conflict_4_8B"),
            ("decay_adjusted_cross_asset_contribution",       "decay_4_7B"),
            ("persistence_adjusted_cross_asset_contribution", "persistence_4_6B"),
            ("cluster_adjusted_cross_asset_contribution",     "cluster_4_5B"),
            ("archetype_adjusted_cross_asset_contribution",   "archetype_4_4B"),
            ("transition_adjusted_cross_asset_contribution",  "transition_4_3B"),
            ("timing_adjusted_cross_asset_contribution",      "timing_4_2B"),
            ("regime_adjusted_cross_asset_contribution",      "regime_4_1C"),
            ("weighted_cross_asset_net_contribution",         "weighted_4_1B"),
            ("cross_asset_net_contribution",                  "raw_4_1A"),
        ):
            v = _as_float(run_attribution.get(key))
            if v is not None:
                return v, label
        return None, "no_upstream_attribution_available"

    # ── primitives ──────────────────────────────────────────────────────
    @staticmethod
    def compute_conflict_integration_scale(
        *, layer_consensus_state: str, profile: ConflictIntegrationProfile,
    ) -> float:
        """Per-state integration scale."""
        scales = {
            "aligned_supportive":   profile.aligned_supportive_scale,
            "aligned_suppressive":  profile.aligned_suppressive_scale,
            "partial_agreement":    profile.partial_agreement_scale,
            "conflicted":           profile.conflicted_scale,
            "unreliable":           profile.unreliable_scale,
            "insufficient_context": profile.insufficient_context_scale,
        }
        return float(scales.get(layer_consensus_state, profile.insufficient_context_scale))

    @staticmethod
    def compute_conflict_event_suppression(
        *,
        layer_consensus_state: str,
        dominant_conflict_source: str | None,
        profile: ConflictIntegrationProfile,
    ) -> float:
        suppression = 0.0
        if layer_consensus_state == "conflicted":
            suppression += profile.conflict_extra_suppression
        if layer_consensus_state == "unreliable":
            suppression += profile.unreliable_extra_suppression
        if dominant_conflict_source:
            extra = profile.dominant_conflict_source_suppression.get(
                dominant_conflict_source
            )
            if extra is not None:
                try:
                    suppression += float(extra)
                except (TypeError, ValueError):
                    pass
        return max(0.0, suppression)

    def compute_conflict_net_contribution(
        self,
        *,
        conflict_adjusted: float | None,
        layer_consensus_state: str,
        dominant_conflict_source: str | None,
        profile: ConflictIntegrationProfile,
    ) -> tuple[float | None, dict[str, Any]]:
        """Return (conflict_net_contribution, breakdown_metadata)."""
        if conflict_adjusted is None:
            return None, {
                "no_conflict_adjusted_contribution": True,
                "applied_integration_weight": None,
                "applied_consensus_scale": None,
                "applied_event_suppression": None,
            }

        scale = self.compute_conflict_integration_scale(
            layer_consensus_state=layer_consensus_state, profile=profile,
        )
        weight = profile.integration_weight

        # Mode-specific behavior. Conservative — never amplifies.
        if profile.integration_mode == "aligned_supportive_confirmation_only" \
                and layer_consensus_state != "aligned_supportive":
            return 0.0, {
                "applied_integration_weight": 0.0,
                "applied_consensus_scale": scale,
                "applied_event_suppression": 0.0,
                "mode_zeroed": True,
            }
        if profile.integration_mode == "conflict_suppression_only" \
                and layer_consensus_state not in ("conflicted",):
            return 0.0, {
                "applied_integration_weight": 0.0,
                "applied_consensus_scale": scale,
                "applied_event_suppression": 0.0,
                "mode_zeroed": True,
            }
        if profile.integration_mode == "unreliable_suppression_only" \
                and layer_consensus_state not in ("unreliable",):
            return 0.0, {
                "applied_integration_weight": 0.0,
                "applied_consensus_scale": scale,
                "applied_event_suppression": 0.0,
                "mode_zeroed": True,
            }

        suppression = self.compute_conflict_event_suppression(
            layer_consensus_state=layer_consensus_state,
            dominant_conflict_source=dominant_conflict_source,
            profile=profile,
        )

        delta = conflict_adjusted * weight * scale

        # In suppression-only modes, force the delta non-positive (sign-aware).
        if profile.integration_mode in (
            "conflict_suppression_only", "unreliable_suppression_only",
        ):
            if delta > 0:
                delta = 0.0

        # Aligned-suppressive must NEVER receive aligned-supportive style boost.
        if layer_consensus_state == "aligned_suppressive" and delta > 0:
            delta = _suppress_toward_zero(delta, suppression + 0.02)

        # Suppression always reduces magnitude toward zero — never inverts.
        delta = _suppress_toward_zero(delta, suppression)

        # Profile-level + global guardrails.
        delta = _clip(delta, -profile.max_negative_contribution, profile.max_positive_contribution)
        delta = _clip(delta, _NET_CONTRIBUTION_FLOOR, _NET_CONTRIBUTION_CEIL)

        return delta, {
            "applied_integration_weight": weight,
            "applied_consensus_scale": scale,
            "applied_event_suppression": suppression,
            "mode_zeroed": False,
        }

    def integrate_conflict_with_composite(
        self,
        *,
        composite_pre_conflict: float | None,
        conflict_net_contribution: float | None,
    ) -> float | None:
        if composite_pre_conflict is None and conflict_net_contribution is None:
            return None
        base = composite_pre_conflict or 0.0
        delta = conflict_net_contribution or 0.0
        return base + delta

    # ── family-level integration ────────────────────────────────────────
    def compute_family_conflict_integration(
        self,
        *,
        family_row: dict[str, Any],
        run_consensus_state: str,
        run_dominant_conflict_source: str | None,
        profile: ConflictIntegrationProfile,
    ) -> dict[str, Any]:
        family_consensus = str(
            family_row.get("family_consensus_state")
            or run_consensus_state
            or "insufficient_context"
        )
        family_dominant = (
            family_row.get("dominant_conflict_source")
            or run_dominant_conflict_source
        )
        conflict_adjusted = _as_float(family_row.get("conflict_adjusted_family_contribution"))

        delta, breakdown = self.compute_conflict_net_contribution(
            conflict_adjusted=conflict_adjusted,
            layer_consensus_state=family_consensus,
            dominant_conflict_source=family_dominant,
            profile=profile,
        )

        reason_codes: list[str] = []
        if family_consensus == "aligned_supportive":
            reason_codes.append("aligned_supportive_supports_constructive_integration")
        if family_consensus == "aligned_suppressive":
            reason_codes.append("aligned_suppressive_blocks_constructive_boost")
        if family_consensus in ("conflicted", "unreliable"):
            reason_codes.append("consensus_state_suppressed_integration")
        if family_consensus == "unreliable":
            reason_codes.append("unreliable_extra_suppression")
        if family_consensus == "insufficient_context":
            reason_codes.append("insufficient_context_neutral_safe")
        if conflict_adjusted is None:
            reason_codes.append("no_conflict_attribution_for_family")
        if family_dominant:
            reason_codes.append(f"dominant_conflict_source:{family_dominant}")
        if profile.id is None:
            reason_codes.append("default_conflict_integration_profile_used")
        if breakdown.get("mode_zeroed"):
            reason_codes.append("mode_zeroed_family_integration")

        top_symbols_raw = family_row.get("top_symbols") or []
        if isinstance(top_symbols_raw, str):
            top_symbols: list[str] = [top_symbols_raw]
        else:
            top_symbols = [str(s) for s in list(top_symbols_raw)[:8]]

        return {
            "dependency_family":                    str(family_row.get("dependency_family") or ""),
            "context_snapshot_id":                  family_row.get("context_snapshot_id"),
            "family_consensus_state":               family_consensus,
            "agreement_score":                      _as_float(family_row.get("agreement_score")),
            "conflict_score":                       _as_float(family_row.get("conflict_score")),
            "dominant_conflict_source":             family_dominant,
            "conflict_adjusted_family_contribution": conflict_adjusted,
            "integration_weight_applied":           breakdown.get("applied_integration_weight"),
            "conflict_integration_contribution":    delta,
            "top_symbols":                          top_symbols,
            "reason_codes":                         list(dict.fromkeys(reason_codes)),
            "breakdown":                            breakdown,
        }

    @staticmethod
    def rank_family_conflict_integration(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Rank by absolute conflict-integration contribution desc → consensus
        preference (aligned_supportive > partial > aligned_suppressive >
        conflicted > insufficient_context > unreliable) → family name asc.
        """
        pref = {
            "aligned_supportive":   5,
            "partial_agreement":    4,
            "aligned_suppressive":  3,
            "conflicted":           2,
            "insufficient_context": 1,
            "unreliable":           0,
        }
        def _key(r: dict[str, Any]) -> tuple:
            v = r.get("conflict_integration_contribution")
            absv = abs(float(v)) if v is not None else 0.0
            p = pref.get(str(r.get("family_consensus_state") or ""), 0)
            return (-absv, -p, str(r.get("dependency_family") or ""))
        return sorted(rows, key=_key)

    # ── persistence ─────────────────────────────────────────────────────
    def persist_conflict_composite(
        self, conn, *, snap: ConflictCompositeSnapshot,
    ) -> str:
        import src.db.repositories_48c as repo
        row = repo.insert_cross_asset_conflict_composite_snapshots(
            conn,
            workspace_id=snap.workspace_id,
            watchlist_id=snap.watchlist_id,
            run_id=snap.run_id,
            context_snapshot_id=snap.context_snapshot_id,
            conflict_integration_profile_id=snap.conflict_integration_profile_id,
            base_signal_score=snap.base_signal_score,
            cross_asset_net_contribution=snap.cross_asset_net_contribution,
            weighted_cross_asset_net_contribution=snap.weighted_cross_asset_net_contribution,
            regime_adjusted_cross_asset_contribution=snap.regime_adjusted_cross_asset_contribution,
            timing_adjusted_cross_asset_contribution=snap.timing_adjusted_cross_asset_contribution,
            transition_adjusted_cross_asset_contribution=snap.transition_adjusted_cross_asset_contribution,
            archetype_adjusted_cross_asset_contribution=snap.archetype_adjusted_cross_asset_contribution,
            cluster_adjusted_cross_asset_contribution=snap.cluster_adjusted_cross_asset_contribution,
            persistence_adjusted_cross_asset_contribution=snap.persistence_adjusted_cross_asset_contribution,
            decay_adjusted_cross_asset_contribution=snap.decay_adjusted_cross_asset_contribution,
            conflict_adjusted_cross_asset_contribution=snap.conflict_adjusted_cross_asset_contribution,
            composite_pre_conflict=snap.composite_pre_conflict,
            conflict_net_contribution=snap.conflict_net_contribution,
            composite_post_conflict=snap.composite_post_conflict,
            layer_consensus_state=snap.layer_consensus_state,
            agreement_score=snap.agreement_score,
            conflict_score=snap.conflict_score,
            dominant_conflict_source=snap.dominant_conflict_source,
            integration_mode=snap.integration_mode,
            source_contribution_layer=snap.source_contribution_layer,
            source_composite_layer=snap.source_composite_layer,
            scoring_version=snap.scoring_version,
            metadata=snap.metadata,
        )
        return str(row["id"])

    def persist_family_conflict_composite(
        self, conn, *, snaps: list[FamilyConflictCompositeSnapshot],
    ) -> list[str]:
        if not snaps:
            return []
        import src.db.repositories_48c as repo
        ids: list[str] = []
        for snap in snaps:
            row = repo.insert_cross_asset_family_conflict_composite_snapshots(
                conn,
                workspace_id=snap.workspace_id,
                watchlist_id=snap.watchlist_id,
                run_id=snap.run_id,
                context_snapshot_id=snap.context_snapshot_id,
                dependency_family=snap.dependency_family,
                family_consensus_state=snap.family_consensus_state,
                agreement_score=snap.agreement_score,
                conflict_score=snap.conflict_score,
                dominant_conflict_source=snap.dominant_conflict_source,
                conflict_adjusted_family_contribution=snap.conflict_adjusted_family_contribution,
                integration_weight_applied=snap.integration_weight_applied,
                conflict_integration_contribution=snap.conflict_integration_contribution,
                family_rank=snap.family_rank,
                top_symbols=snap.top_symbols,
                reason_codes=snap.reason_codes,
                source_contribution_layer=snap.source_contribution_layer,
                scoring_version=snap.scoring_version,
                metadata=snap.metadata,
            )
            ids.append(str(row["id"]))
        return ids

    # ── orchestration ───────────────────────────────────────────────────
    def build_and_persist_for_run(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> tuple[bool, int]:
        import src.db.repositories_48c as repo

        profile = self.get_active_conflict_integration_profile(
            conn, workspace_id=workspace_id,
        )

        run_attribution = self._load_run_conflict_attribution(
            conn, run_id=run_id, workspace_id=workspace_id,
        )
        if not run_attribution or run_attribution.get("watchlist_id") != watchlist_id:
            logger.debug(
                "conflict_composite: no conflict attribution for workspace=%s watchlist=%s run=%s",
                workspace_id, watchlist_id, run_id,
            )
            return (False, 0)

        consensus_state    = str(run_attribution.get("layer_consensus_state") or "insufficient_context")
        agreement_score    = _as_float(run_attribution.get("agreement_score"))
        conflict_score     = _as_float(run_attribution.get("conflict_score"))
        dominant_conflict  = run_attribution.get("dominant_conflict_source")
        context_snap_id    = run_attribution.get("context_snapshot_id")

        conflict_adjusted_total, source_contribution_layer = \
            self._resolve_source_contribution_layer(run_attribution)

        # Resolve composite_pre_conflict from the upstream stack.
        composite_pre_conflict, base_signal, source_composite_layer = \
            self._resolve_composite_pre_conflict(
                conn, run_id=run_id, run_attribution=run_attribution,
            )

        # Compute conflict net contribution and final composite.
        delta, breakdown = self.compute_conflict_net_contribution(
            conflict_adjusted=conflict_adjusted_total,
            layer_consensus_state=consensus_state,
            dominant_conflict_source=dominant_conflict,
            profile=profile,
        )
        composite_post_conflict = self.integrate_conflict_with_composite(
            composite_pre_conflict=composite_pre_conflict,
            conflict_net_contribution=delta,
        )

        # Build run-level snapshot.
        run_meta: dict[str, Any] = {
            "scoring_version": _SCORING_VERSION,
            "integration_mode": profile.integration_mode,
            "source_contribution_layer": source_contribution_layer,
            "source_composite_layer": source_composite_layer,
            "default_conflict_integration_profile_used": profile.id is None,
            "thresholds": {
                "integration_weight": profile.integration_weight,
                "aligned_supportive_scale": profile.aligned_supportive_scale,
                "aligned_suppressive_scale": profile.aligned_suppressive_scale,
                "partial_agreement_scale": profile.partial_agreement_scale,
                "conflicted_scale": profile.conflicted_scale,
                "unreliable_scale": profile.unreliable_scale,
                "insufficient_context_scale": profile.insufficient_context_scale,
                "conflict_extra_suppression": profile.conflict_extra_suppression,
                "unreliable_extra_suppression": profile.unreliable_extra_suppression,
                "max_positive_contribution": profile.max_positive_contribution,
                "max_negative_contribution": profile.max_negative_contribution,
                "net_contribution_floor": _NET_CONTRIBUTION_FLOOR,
                "net_contribution_ceil":  _NET_CONTRIBUTION_CEIL,
            },
            "breakdown": breakdown,
        }

        snap = ConflictCompositeSnapshot(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snap_id,
            conflict_integration_profile_id=profile.id,
            base_signal_score=base_signal,
            cross_asset_net_contribution=_as_float(run_attribution.get("cross_asset_net_contribution")),
            weighted_cross_asset_net_contribution=_as_float(run_attribution.get("weighted_cross_asset_net_contribution")),
            regime_adjusted_cross_asset_contribution=_as_float(run_attribution.get("regime_adjusted_cross_asset_contribution")),
            timing_adjusted_cross_asset_contribution=_as_float(run_attribution.get("timing_adjusted_cross_asset_contribution")),
            transition_adjusted_cross_asset_contribution=_as_float(run_attribution.get("transition_adjusted_cross_asset_contribution")),
            archetype_adjusted_cross_asset_contribution=_as_float(run_attribution.get("archetype_adjusted_cross_asset_contribution")),
            cluster_adjusted_cross_asset_contribution=_as_float(run_attribution.get("cluster_adjusted_cross_asset_contribution")),
            persistence_adjusted_cross_asset_contribution=_as_float(run_attribution.get("persistence_adjusted_cross_asset_contribution")),
            decay_adjusted_cross_asset_contribution=_as_float(run_attribution.get("decay_adjusted_cross_asset_contribution")),
            conflict_adjusted_cross_asset_contribution=_as_float(run_attribution.get("conflict_adjusted_cross_asset_contribution")),
            composite_pre_conflict=composite_pre_conflict,
            conflict_net_contribution=delta,
            composite_post_conflict=composite_post_conflict,
            layer_consensus_state=consensus_state,
            agreement_score=agreement_score,
            conflict_score=conflict_score,
            dominant_conflict_source=dominant_conflict,
            integration_mode=profile.integration_mode,
            source_contribution_layer=source_contribution_layer,
            source_composite_layer=source_composite_layer,
            scoring_version=_SCORING_VERSION,
            metadata=run_meta,
        )
        self.persist_conflict_composite(conn, snap=snap)

        # Family-level integration.
        family_rows = repo.get_cross_asset_family_conflict_attribution_for_run(
            conn, run_id=run_id,
        )
        family_results = [
            self.compute_family_conflict_integration(
                family_row=fr,
                run_consensus_state=consensus_state,
                run_dominant_conflict_source=dominant_conflict,
                profile=profile,
            )
            for fr in family_rows
        ]
        ranked = self.rank_family_conflict_integration(family_results)

        family_snaps: list[FamilyConflictCompositeSnapshot] = []
        for idx, fr in enumerate(ranked, start=1):
            family_snaps.append(FamilyConflictCompositeSnapshot(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                run_id=run_id,
                context_snapshot_id=fr.get("context_snapshot_id") or context_snap_id,
                dependency_family=fr["dependency_family"],
                family_consensus_state=fr["family_consensus_state"],
                agreement_score=fr.get("agreement_score"),
                conflict_score=fr.get("conflict_score"),
                dominant_conflict_source=fr.get("dominant_conflict_source"),
                conflict_adjusted_family_contribution=fr.get("conflict_adjusted_family_contribution"),
                integration_weight_applied=fr.get("integration_weight_applied"),
                conflict_integration_contribution=fr.get("conflict_integration_contribution"),
                family_rank=idx,
                top_symbols=fr.get("top_symbols") or [],
                reason_codes=fr.get("reason_codes") or [],
                source_contribution_layer=source_contribution_layer,
                scoring_version=_SCORING_VERSION,
                metadata={
                    "scoring_version": _SCORING_VERSION,
                    "integration_mode": profile.integration_mode,
                    "default_conflict_integration_profile_used": profile.id is None,
                    "breakdown": fr.get("breakdown") or {},
                },
            ))
        self.persist_family_conflict_composite(conn, snaps=family_snaps)

        return (True, len(family_snaps))

    def refresh_workspace_conflict_composite(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> int:
        """Emit conflict-aware composite for every watchlist on this run."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        total = 0
        for wid in watchlist_ids:
            try:
                ok, fcount = self.build_and_persist_for_run(
                    conn, workspace_id=workspace_id, watchlist_id=wid, run_id=run_id,
                )
                if ok:
                    conn.commit()
                    total += 1
            except Exception as exc:
                logger.warning(
                    "cross_asset_conflict_composite: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return total
