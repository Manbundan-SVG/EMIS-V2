"""Phase 4.7C: Decay-Aware Composite Refinement.

Final decay-aware integration layer. Starts from the most mature upstream
composite (4.6C composite_post_persistence → 4.5C composite_post_cluster →
4.4C composite_post_archetype → 4.3C composite_post_transition → 4.2C
composite_post_timing → regime equivalent → raw fallback), adds a bounded
decay-aware delta conditioned on the run's freshness state + stale-memory
and contradiction flags from 4.7A and the per-family decay-aware
contribution from 4.7B, and persists the result side-by-side with all
upstream layers.

Persists:
  * cross_asset_decay_composite_snapshots (per-run/watchlist)
  * cross_asset_family_decay_composite_snapshots (per-run/family)

All weights, scales, suppression terms, and bounds are deterministic and
metadata-stamped. No predictive forecasting in this phase.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.7C.v1"

# Default profile values mirror migration defaults.
_DEFAULT_INTEGRATION_MODE          = "decay_additive_guardrailed"
_DEFAULT_INTEGRATION_WEIGHT        = 0.10
_DEFAULT_FRESH_SCALE               = 1.08
_DEFAULT_DECAYING_SCALE            = 0.98
_DEFAULT_STALE_SCALE               = 0.82
_DEFAULT_CONTRADICTED_SCALE        = 0.65
_DEFAULT_MIXED_SCALE               = 0.88
_DEFAULT_INSUFFICIENT_HIST_SCALE   = 0.80
_DEFAULT_STALE_EXTRA_SUPPRESSION   = 0.02
_DEFAULT_CONTRADICTION_SUPPRESSION = 0.04
_DEFAULT_MAX_POSITIVE              = 0.20
_DEFAULT_MAX_NEGATIVE              = 0.20

# Conservative net-contribution band — keeps decay refinement bounded.
_NET_CONTRIBUTION_FLOOR  = -0.15
_NET_CONTRIBUTION_CEIL   = 0.15


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
    """Reduce |v| by magnitude (clipped at 0). Sign preserved."""
    if v >= 0:
        return max(0.0, v - magnitude)
    return min(0.0, v + magnitude)


@dataclass
class DecayIntegrationProfile:
    id: str | None
    workspace_id: str
    profile_name: str
    is_active: bool
    integration_mode: str
    integration_weight: float
    fresh_scale: float
    decaying_scale: float
    stale_scale: float
    contradicted_scale: float
    mixed_scale: float
    insufficient_history_scale: float
    stale_extra_suppression: float
    contradiction_extra_suppression: float
    max_positive_contribution: float
    max_negative_contribution: float
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def default(cls, workspace_id: str) -> "DecayIntegrationProfile":
        return cls(
            id=None,
            workspace_id=workspace_id,
            profile_name="default_decay_integration_profile",
            is_active=True,
            integration_mode=_DEFAULT_INTEGRATION_MODE,
            integration_weight=_DEFAULT_INTEGRATION_WEIGHT,
            fresh_scale=_DEFAULT_FRESH_SCALE,
            decaying_scale=_DEFAULT_DECAYING_SCALE,
            stale_scale=_DEFAULT_STALE_SCALE,
            contradicted_scale=_DEFAULT_CONTRADICTED_SCALE,
            mixed_scale=_DEFAULT_MIXED_SCALE,
            insufficient_history_scale=_DEFAULT_INSUFFICIENT_HIST_SCALE,
            stale_extra_suppression=_DEFAULT_STALE_EXTRA_SUPPRESSION,
            contradiction_extra_suppression=_DEFAULT_CONTRADICTION_SUPPRESSION,
            max_positive_contribution=_DEFAULT_MAX_POSITIVE,
            max_negative_contribution=_DEFAULT_MAX_NEGATIVE,
            metadata={"source": "default", "scoring_version": _SCORING_VERSION},
        )


@dataclass
class DecayCompositeSnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    decay_integration_profile_id: str | None
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
    composite_pre_decay: float | None
    decay_net_contribution: float | None
    composite_post_decay: float | None
    freshness_state: str
    aggregate_decay_score: float | None
    stale_memory_flag: bool
    contradiction_flag: bool
    integration_mode: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FamilyDecayCompositeSnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    dependency_family: str
    freshness_state: str
    aggregate_decay_score: float | None
    family_decay_score: float | None
    stale_memory_flag: bool
    contradiction_flag: bool
    decay_adjusted_family_contribution: float | None
    integration_weight_applied: float | None
    decay_integration_contribution: float | None
    family_rank: int | None
    top_symbols: list[str]
    reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


class CrossAssetDecayCompositeService:
    """Deterministic decay-aware composite integration."""

    # ── profile ─────────────────────────────────────────────────────────
    def get_active_decay_integration_profile(
        self, conn, *, workspace_id: str,
    ) -> DecayIntegrationProfile:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, workspace_id::text as workspace_id,
                       profile_name, is_active, integration_mode,
                       integration_weight,
                       fresh_scale, decaying_scale, stale_scale,
                       contradicted_scale, mixed_scale, insufficient_history_scale,
                       stale_extra_suppression, contradiction_extra_suppression,
                       max_positive_contribution, max_negative_contribution,
                       metadata
                from public.cross_asset_decay_integration_profiles
                where workspace_id = %s::uuid
                  and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            if not row:
                return DecayIntegrationProfile.default(workspace_id)
            d = dict(row)
            return DecayIntegrationProfile(
                id=d.get("id"),
                workspace_id=d.get("workspace_id") or workspace_id,
                profile_name=d.get("profile_name") or "active",
                is_active=bool(d.get("is_active", True)),
                integration_mode=str(d.get("integration_mode") or _DEFAULT_INTEGRATION_MODE),
                integration_weight=float(d.get("integration_weight") or _DEFAULT_INTEGRATION_WEIGHT),
                fresh_scale=float(d.get("fresh_scale") or _DEFAULT_FRESH_SCALE),
                decaying_scale=float(d.get("decaying_scale") or _DEFAULT_DECAYING_SCALE),
                stale_scale=float(d.get("stale_scale") or _DEFAULT_STALE_SCALE),
                contradicted_scale=float(d.get("contradicted_scale") or _DEFAULT_CONTRADICTED_SCALE),
                mixed_scale=float(d.get("mixed_scale") or _DEFAULT_MIXED_SCALE),
                insufficient_history_scale=float(
                    d.get("insufficient_history_scale") or _DEFAULT_INSUFFICIENT_HIST_SCALE
                ),
                stale_extra_suppression=float(
                    d.get("stale_extra_suppression") or _DEFAULT_STALE_EXTRA_SUPPRESSION
                ),
                contradiction_extra_suppression=float(
                    d.get("contradiction_extra_suppression") or _DEFAULT_CONTRADICTION_SUPPRESSION
                ),
                max_positive_contribution=float(
                    d.get("max_positive_contribution") or _DEFAULT_MAX_POSITIVE
                ),
                max_negative_contribution=float(
                    d.get("max_negative_contribution") or _DEFAULT_MAX_NEGATIVE
                ),
                metadata=dict(d.get("metadata") or {}),
            )

    # ── inputs ──────────────────────────────────────────────────────────
    def _load_run_decay_attribution(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text as run_id,
                       workspace_id::text as workspace_id,
                       watchlist_id::text as watchlist_id,
                       context_snapshot_id::text as context_snapshot_id,
                       cross_asset_net_contribution,
                       weighted_cross_asset_net_contribution,
                       regime_adjusted_cross_asset_contribution,
                       timing_adjusted_cross_asset_contribution,
                       transition_adjusted_cross_asset_contribution,
                       archetype_adjusted_cross_asset_contribution,
                       cluster_adjusted_cross_asset_contribution,
                       persistence_adjusted_cross_asset_contribution,
                       decay_adjusted_cross_asset_contribution,
                       freshness_state, aggregate_decay_score,
                       stale_memory_flag, contradiction_flag,
                       decay_dominant_dependency_family
                from public.run_cross_asset_decay_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_persistence_composite_pre(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select base_signal_score, composite_post_persistence,
                       composite_pre_persistence
                from public.cross_asset_persistence_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_cluster_composite_pre(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select composite_post_cluster, composite_pre_cluster, base_signal_score
                from public.cross_asset_cluster_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_archetype_composite_pre(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select composite_post_archetype, composite_pre_archetype, base_signal_score
                from public.cross_asset_archetype_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_transition_composite_pre(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select composite_post_transition, composite_pre_transition, base_signal_score
                from public.cross_asset_transition_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_timing_composite_pre(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select composite_post_timing, composite_pre_timing, base_signal_score
                from public.cross_asset_timing_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_attribution_composite_pre(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select base_signal_score, composite_post_cross_asset, composite_pre_cross_asset
                from public.cross_asset_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _resolve_composite_pre_decay(
        self, conn, *, run_id: str,
    ) -> tuple[float | None, float | None, str]:
        """Walk the upstream composite stack and return
        ``(composite_pre_decay, base_signal_score, source_label)``."""
        sources: list[tuple[str, str, str]] = [
            ("composite_post_persistence", "_load_persistence_composite_pre", "4.6C"),
            ("composite_post_cluster",     "_load_cluster_composite_pre",     "4.5C"),
            ("composite_post_archetype",   "_load_archetype_composite_pre",   "4.4C"),
            ("composite_post_transition",  "_load_transition_composite_pre",  "4.3C"),
            ("composite_post_timing",      "_load_timing_composite_pre",      "4.2C"),
            ("composite_post_cross_asset", "_load_attribution_composite_pre", "4.1A"),
        ]
        for col, loader_name, label in sources:
            row = getattr(self, loader_name)(conn, run_id=run_id)
            if row is None:
                continue
            v = _as_float(row.get(col))
            if v is not None:
                return v, _as_float(row.get("base_signal_score")), label
        return None, None, "none"

    def _load_family_decay_attribution(
        self, conn, *, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       context_snapshot_id::text as context_snapshot_id,
                       freshness_state,
                       aggregate_decay_score,
                       family_decay_score,
                       stale_memory_flag,
                       contradiction_flag,
                       decay_adjusted_family_contribution,
                       decay_family_rank,
                       top_symbols
                from public.cross_asset_family_decay_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            return [dict(r) for r in cur.fetchall()]

    # ── primitives ──────────────────────────────────────────────────────
    @staticmethod
    def compute_decay_integration_scale(
        *, freshness_state: str, profile: DecayIntegrationProfile,
    ) -> float:
        """Per-state integration scale."""
        scales = {
            "fresh":                profile.fresh_scale,
            "decaying":             profile.decaying_scale,
            "stale":                profile.stale_scale,
            "contradicted":         profile.contradicted_scale,
            "mixed":                profile.mixed_scale,
            "insufficient_history": profile.insufficient_history_scale,
        }
        return float(scales.get(freshness_state, profile.insufficient_history_scale))

    @staticmethod
    def compute_decay_event_suppression(
        *,
        stale_memory_flag: bool,
        contradiction_flag: bool,
        profile: DecayIntegrationProfile,
    ) -> float:
        suppression = 0.0
        if stale_memory_flag:
            suppression += profile.stale_extra_suppression
        if contradiction_flag:
            suppression += profile.contradiction_extra_suppression
        return max(0.0, suppression)

    def compute_decay_net_contribution(
        self,
        *,
        decay_adjusted: float | None,
        freshness_state: str,
        stale_memory_flag: bool,
        contradiction_flag: bool,
        profile: DecayIntegrationProfile,
    ) -> tuple[float | None, dict[str, Any]]:
        """Return (decay_net_contribution, breakdown_metadata)."""
        if decay_adjusted is None:
            return None, {
                "no_decay_adjusted_contribution": True,
                "applied_integration_weight": None,
                "applied_freshness_scale": None,
                "applied_event_suppression": None,
            }

        scale = self.compute_decay_integration_scale(
            freshness_state=freshness_state, profile=profile,
        )
        weight = profile.integration_weight

        # Mode-specific behaviour. Conservative — never amplifies.
        if profile.integration_mode == "fresh_confirmation_only" and freshness_state != "fresh":
            return 0.0, {
                "applied_integration_weight": 0.0,
                "applied_freshness_scale": scale,
                "applied_event_suppression": 0.0,
                "mode_zeroed": True,
            }
        if profile.integration_mode == "stale_suppression_only" and not (
            stale_memory_flag or freshness_state == "stale"
        ):
            return 0.0, {
                "applied_integration_weight": 0.0,
                "applied_freshness_scale": scale,
                "applied_event_suppression": 0.0,
                "mode_zeroed": True,
            }
        if profile.integration_mode == "contradiction_suppression_only" and not (
            contradiction_flag or freshness_state == "contradicted"
        ):
            return 0.0, {
                "applied_integration_weight": 0.0,
                "applied_freshness_scale": scale,
                "applied_event_suppression": 0.0,
                "mode_zeroed": True,
            }

        suppression = self.compute_decay_event_suppression(
            stale_memory_flag=stale_memory_flag,
            contradiction_flag=contradiction_flag,
            profile=profile,
        )

        delta = decay_adjusted * weight * scale

        # In suppression-only modes, force the delta non-positive (sign-aware).
        if profile.integration_mode in ("stale_suppression_only", "contradiction_suppression_only"):
            if delta > 0:
                delta = 0.0

        # Suppression always reduces magnitude toward zero — never inverts.
        delta = _suppress_toward_zero(delta, suppression)

        # Profile-level + global guardrails.
        delta = _clip(delta, -profile.max_negative_contribution, profile.max_positive_contribution)
        delta = _clip(delta, _NET_CONTRIBUTION_FLOOR, _NET_CONTRIBUTION_CEIL)

        return delta, {
            "applied_integration_weight": weight,
            "applied_freshness_scale": scale,
            "applied_event_suppression": suppression,
            "mode_zeroed": False,
        }

    def integrate_decay_with_composite(
        self,
        *,
        composite_pre_decay: float | None,
        decay_net_contribution: float | None,
    ) -> float | None:
        if composite_pre_decay is None and decay_net_contribution is None:
            return None
        base = composite_pre_decay or 0.0
        delta = decay_net_contribution or 0.0
        return base + delta

    # ── family-level integration ────────────────────────────────────────
    def compute_family_decay_integration(
        self,
        *,
        family_row: dict[str, Any],
        run_freshness: str,
        run_stale_flag: bool,
        run_contradiction_flag: bool,
        profile: DecayIntegrationProfile,
    ) -> dict[str, Any]:
        family_freshness = str(family_row.get("freshness_state") or run_freshness or "insufficient_history")
        family_stale     = bool(family_row.get("stale_memory_flag") or run_stale_flag)
        family_contradiction = bool(family_row.get("contradiction_flag") or run_contradiction_flag)
        decay_adjusted   = _as_float(family_row.get("decay_adjusted_family_contribution"))

        delta, breakdown = self.compute_decay_net_contribution(
            decay_adjusted=decay_adjusted,
            freshness_state=family_freshness,
            stale_memory_flag=family_stale,
            contradiction_flag=family_contradiction,
            profile=profile,
        )

        reason_codes: list[str] = []
        if family_freshness == "fresh":
            reason_codes.append("fresh_supports_constructive_integration")
        if family_freshness in ("stale", "contradicted"):
            reason_codes.append("freshness_state_suppressed_integration")
        if family_stale:
            reason_codes.append("stale_memory_extra_suppression")
        if family_contradiction:
            reason_codes.append("contradiction_extra_suppression")
        if decay_adjusted is None:
            reason_codes.append("no_decay_attribution_for_family")
        if profile.id is None:
            reason_codes.append("default_decay_integration_profile_used")
        if breakdown.get("mode_zeroed"):
            reason_codes.append("mode_zeroed_family_integration")

        top_symbols_raw = family_row.get("top_symbols") or []
        if isinstance(top_symbols_raw, str):
            top_symbols: list[str] = [top_symbols_raw]
        else:
            top_symbols = [str(s) for s in list(top_symbols_raw)[:8]]

        return {
            "dependency_family":                  str(family_row.get("dependency_family") or ""),
            "context_snapshot_id":                family_row.get("context_snapshot_id"),
            "freshness_state":                    family_freshness,
            "aggregate_decay_score":              _as_float(family_row.get("aggregate_decay_score")),
            "family_decay_score":                 _as_float(family_row.get("family_decay_score")),
            "stale_memory_flag":                  family_stale,
            "contradiction_flag":                 family_contradiction,
            "decay_adjusted_family_contribution": decay_adjusted,
            "integration_weight_applied":         breakdown.get("applied_integration_weight"),
            "decay_integration_contribution":     delta,
            "top_symbols":                        top_symbols,
            "reason_codes":                       list(dict.fromkeys(reason_codes)),
            "breakdown":                          breakdown,
        }

    @staticmethod
    def rank_family_decay_integration(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Rank by absolute decay-integration contribution desc → freshness
        preference (fresh > decaying > mixed > stale > contradicted >
        insufficient) → family name asc."""
        pref = {
            "fresh":                5,
            "decaying":             4,
            "mixed":                3,
            "stale":                2,
            "contradicted":         1,
            "insufficient_history": 0,
        }
        def _key(r: dict[str, Any]) -> tuple:
            v = r.get("decay_integration_contribution")
            absv = abs(float(v)) if v is not None else 0.0
            p = pref.get(str(r.get("freshness_state") or ""), 0)
            return (-absv, -p, str(r.get("dependency_family") or ""))
        return sorted(rows, key=_key)

    # ── persistence ─────────────────────────────────────────────────────
    def persist_decay_composite(
        self, conn, *, snap: DecayCompositeSnapshot,
    ) -> str:
        import src.db.repositories_47c as repo
        row = repo.insert_cross_asset_decay_composite_snapshots(
            conn,
            workspace_id=snap.workspace_id,
            watchlist_id=snap.watchlist_id,
            run_id=snap.run_id,
            context_snapshot_id=snap.context_snapshot_id,
            decay_integration_profile_id=snap.decay_integration_profile_id,
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
            composite_pre_decay=snap.composite_pre_decay,
            decay_net_contribution=snap.decay_net_contribution,
            composite_post_decay=snap.composite_post_decay,
            freshness_state=snap.freshness_state,
            aggregate_decay_score=snap.aggregate_decay_score,
            stale_memory_flag=snap.stale_memory_flag,
            contradiction_flag=snap.contradiction_flag,
            integration_mode=snap.integration_mode,
            metadata=snap.metadata,
        )
        return str(row["id"])

    def persist_family_decay_composite(
        self, conn, *, snaps: list[FamilyDecayCompositeSnapshot],
    ) -> list[str]:
        if not snaps:
            return []
        import src.db.repositories_47c as repo
        ids: list[str] = []
        for snap in snaps:
            row = repo.insert_cross_asset_family_decay_composite_snapshots(
                conn,
                workspace_id=snap.workspace_id,
                watchlist_id=snap.watchlist_id,
                run_id=snap.run_id,
                context_snapshot_id=snap.context_snapshot_id,
                dependency_family=snap.dependency_family,
                freshness_state=snap.freshness_state,
                aggregate_decay_score=snap.aggregate_decay_score,
                family_decay_score=snap.family_decay_score,
                stale_memory_flag=snap.stale_memory_flag,
                contradiction_flag=snap.contradiction_flag,
                decay_adjusted_family_contribution=snap.decay_adjusted_family_contribution,
                integration_weight_applied=snap.integration_weight_applied,
                decay_integration_contribution=snap.decay_integration_contribution,
                family_rank=snap.family_rank,
                top_symbols=snap.top_symbols,
                reason_codes=snap.reason_codes,
                metadata=snap.metadata,
            )
            ids.append(str(row["id"]))
        return ids

    # ── orchestration ───────────────────────────────────────────────────
    def build_and_persist_for_run(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> tuple[bool, int]:
        profile = self.get_active_decay_integration_profile(conn, workspace_id=workspace_id)
        run_decay = self._load_run_decay_attribution(conn, run_id=run_id)

        if not run_decay or run_decay.get("watchlist_id") != watchlist_id:
            logger.debug(
                "decay_composite: no decay attribution for workspace=%s watchlist=%s run=%s",
                workspace_id, watchlist_id, run_id,
            )
            return (False, 0)

        decay_adjusted = _as_float(run_decay.get("decay_adjusted_cross_asset_contribution"))
        freshness_state = str(run_decay.get("freshness_state") or "insufficient_history")
        stale_flag      = bool(run_decay.get("stale_memory_flag") or False)
        contradiction   = bool(run_decay.get("contradiction_flag") or False)
        aggregate_decay = _as_float(run_decay.get("aggregate_decay_score"))
        context_snap_id = run_decay.get("context_snapshot_id")

        # Resolve composite_pre_decay from upstream stack.
        composite_pre_decay, base_signal, source_label = self._resolve_composite_pre_decay(
            conn, run_id=run_id,
        )

        # Compute decay net contribution and final composite.
        delta, breakdown = self.compute_decay_net_contribution(
            decay_adjusted=decay_adjusted,
            freshness_state=freshness_state,
            stale_memory_flag=stale_flag,
            contradiction_flag=contradiction,
            profile=profile,
        )
        composite_post_decay = self.integrate_decay_with_composite(
            composite_pre_decay=composite_pre_decay,
            decay_net_contribution=delta,
        )

        common_meta = {
            "scoring_version":             _SCORING_VERSION,
            "policy_profile_id":           profile.id,
            "policy_profile_name":         profile.profile_name,
            "default_decay_integration_profile_used": profile.id is None,
            "composite_pre_decay_source":  source_label,
            "net_contribution_floor":      _NET_CONTRIBUTION_FLOOR,
            "net_contribution_ceil":       _NET_CONTRIBUTION_CEIL,
            "applied_integration_weight":  breakdown.get("applied_integration_weight"),
            "applied_freshness_scale":     breakdown.get("applied_freshness_scale"),
            "applied_event_suppression":   breakdown.get("applied_event_suppression"),
            "mode_zeroed":                 breakdown.get("mode_zeroed", False),
            "integration_mode":            profile.integration_mode,
        }

        run_snap = DecayCompositeSnapshot(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snap_id,
            decay_integration_profile_id=profile.id,
            base_signal_score=base_signal,
            cross_asset_net_contribution=_as_float(run_decay.get("cross_asset_net_contribution")),
            weighted_cross_asset_net_contribution=_as_float(run_decay.get("weighted_cross_asset_net_contribution")),
            regime_adjusted_cross_asset_contribution=_as_float(run_decay.get("regime_adjusted_cross_asset_contribution")),
            timing_adjusted_cross_asset_contribution=_as_float(run_decay.get("timing_adjusted_cross_asset_contribution")),
            transition_adjusted_cross_asset_contribution=_as_float(run_decay.get("transition_adjusted_cross_asset_contribution")),
            archetype_adjusted_cross_asset_contribution=_as_float(run_decay.get("archetype_adjusted_cross_asset_contribution")),
            cluster_adjusted_cross_asset_contribution=_as_float(run_decay.get("cluster_adjusted_cross_asset_contribution")),
            persistence_adjusted_cross_asset_contribution=_as_float(run_decay.get("persistence_adjusted_cross_asset_contribution")),
            decay_adjusted_cross_asset_contribution=decay_adjusted,
            composite_pre_decay=composite_pre_decay,
            decay_net_contribution=delta,
            composite_post_decay=composite_post_decay,
            freshness_state=freshness_state,
            aggregate_decay_score=aggregate_decay,
            stale_memory_flag=stale_flag,
            contradiction_flag=contradiction,
            integration_mode=profile.integration_mode,
            metadata=common_meta,
        )
        self.persist_decay_composite(conn, snap=run_snap)

        # ── family-level integration ────────────────────────────────────
        family_rows = self._load_family_decay_attribution(conn, run_id=run_id)
        family_dicts: list[dict[str, Any]] = []
        for fr in family_rows:
            fd = self.compute_family_decay_integration(
                family_row=fr,
                run_freshness=freshness_state,
                run_stale_flag=stale_flag,
                run_contradiction_flag=contradiction,
                profile=profile,
            )
            family_dicts.append(fd)

        ranked = self.rank_family_decay_integration(family_dicts)
        for i, fd in enumerate(ranked, start=1):
            fd["family_rank"] = i

        family_snaps: list[FamilyDecayCompositeSnapshot] = []
        for fd in ranked:
            family_snaps.append(FamilyDecayCompositeSnapshot(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                run_id=run_id,
                context_snapshot_id=fd.get("context_snapshot_id") or context_snap_id,
                dependency_family=fd["dependency_family"],
                freshness_state=fd["freshness_state"],
                aggregate_decay_score=fd["aggregate_decay_score"],
                family_decay_score=fd["family_decay_score"],
                stale_memory_flag=fd["stale_memory_flag"],
                contradiction_flag=fd["contradiction_flag"],
                decay_adjusted_family_contribution=fd["decay_adjusted_family_contribution"],
                integration_weight_applied=fd["integration_weight_applied"],
                decay_integration_contribution=fd["decay_integration_contribution"],
                family_rank=fd["family_rank"],
                top_symbols=fd["top_symbols"],
                reason_codes=fd["reason_codes"],
                metadata={**common_meta, "breakdown": fd["breakdown"]},
            ))
        family_ids = self.persist_family_decay_composite(conn, snaps=family_snaps)
        return (True, len(family_ids))

    def refresh_workspace_decay_composite(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> int:
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        total_runs = 0
        for wid in watchlist_ids:
            try:
                ok, _fc = self.build_and_persist_for_run(
                    conn, workspace_id=workspace_id, watchlist_id=wid, run_id=run_id,
                )
                if ok:
                    conn.commit()
                    total_runs += 1
            except Exception as exc:
                logger.warning(
                    "cross_asset_decay_composite: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return total_runs
