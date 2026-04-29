"""Phase 4.5A: Pattern-Cluster Drift and Archetype Regime Rotation Diagnostics.

Computes recent-window archetype distribution summaries, regime-conditioned
archetype rotation summaries, and discrete drift-event records on top of the
4.4A archetype classifications.

Persists:
  * cross_asset_archetype_cluster_snapshots (one per run/window)
  * cross_asset_archetype_regime_rotation_snapshots (one per regime/window)
  * cross_asset_pattern_drift_event_snapshots (per detected event)

All clustering logic is deterministic, bounded, and metadata-stamped. No
unsupervised ML is used — windows are fixed and the rules are explicit.
"""

from __future__ import annotations

import logging
import math
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.5A.v1"

# Window sizes; metadata-stamped.
_RUN_WINDOW_SIZE         = 10   # cluster window for a single run
_REGIME_WINDOW_SIZE      = 25   # rotation window per regime
_MIN_HISTORY_FOR_CLUSTER = 3    # below this we mark insufficient_history

# Archetype → archetype_family bucket (matches 4.4A registry).
_ARCHETYPE_BUCKET: dict[str, str] = {
    "rotation_handoff":         "rotation",
    "reinforcing_continuation": "reinforcement",
    "recovering_reentry":       "recovery",
    "deteriorating_breakdown":  "degradation",
    "mixed_transition_noise":   "mixed",
    "insufficient_history":     "none",
}

# Bucket priority for tie-break when multiple archetype buckets tie.
_BUCKET_PRIORITY: dict[str, int] = {
    "reinforcement": 5,
    "rotation":      4,
    "recovery":      4,
    "degradation":   3,
    "mixed":         2,
    "none":          1,
}

# Archetype priority for picking dominant archetype within a window.
_ARCHETYPE_PRIORITY: dict[str, int] = {
    "reinforcing_continuation": 5,
    "rotation_handoff":         4,
    "recovering_reentry":       4,
    "deteriorating_breakdown":  3,
    "mixed_transition_noise":   2,
    "insufficient_history":     1,
}


@dataclass
class ClusterSnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    regime_key: str | None
    window_label: str
    dominant_archetype_key: str
    archetype_mix: dict[str, float]
    reinforcement_share: float | None
    recovery_share: float | None
    rotation_share: float | None
    degradation_share: float | None
    mixed_share: float | None
    pattern_entropy: float | None
    cluster_state: str
    drift_score: float | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RegimeRotationSnapshot:
    workspace_id: str
    regime_key: str
    window_label: str
    prior_dominant_archetype_key: str | None
    current_dominant_archetype_key: str | None
    rotation_count: int
    reinforcement_run_count: int
    recovery_run_count: int
    degradation_run_count: int
    mixed_run_count: int
    rotation_state: str
    regime_drift_score: float | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PatternDriftEvent:
    workspace_id: str
    watchlist_id: str | None
    source_run_id: str | None
    target_run_id: str | None
    regime_key: str | None
    prior_cluster_state: str | None
    current_cluster_state: str
    prior_dominant_archetype_key: str | None
    current_dominant_archetype_key: str
    drift_event_type: str
    drift_score: float | None
    reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


class CrossAssetPatternClusterService:
    """Deterministic pattern-cluster + drift + regime-rotation diagnostics."""

    # ── input loading ───────────────────────────────────────────────────
    def _load_run_context(self, conn, *, run_id: str) -> dict[str, Any]:
        ctx: dict[str, Any] = {"context_snapshot_id": None, "regime_key": None}
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
                select regime_key
                from public.run_cross_asset_regime_integration_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                ctx["regime_key"] = dict(r).get("regime_key")
        return ctx

    def _load_run_family_archetypes(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[str]:
        """Return the list of archetype_keys for the run's families."""
        with conn.cursor() as cur:
            cur.execute(
                """
                select archetype_key
                from public.cross_asset_family_archetype_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [str(dict(r)["archetype_key"]) for r in cur.fetchall()]

    def load_recent_run_archetypes(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str | None = None,
        regime_key: str | None = None,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        """Return recent run archetype rows ordered newest-first."""
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text       as run_id,
                       workspace_id::text as workspace_id,
                       watchlist_id::text as watchlist_id,
                       regime_key,
                       dominant_archetype_key,
                       archetype_confidence,
                       created_at
                from public.cross_asset_run_archetype_summary
                where workspace_id = %s::uuid
                  and (%s::uuid is null or watchlist_id = %s::uuid)
                  and (%s::text is null or regime_key   = %s::text)
                order by created_at desc
                limit %s
                """,
                (workspace_id, watchlist_id, watchlist_id,
                 regime_key, regime_key, int(limit)),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_prior_cluster_snapshot(
        self, conn, *, workspace_id: str, watchlist_id: str, before_run_id: str,
    ) -> dict[str, Any] | None:
        """Return the most recent prior cluster snapshot for the watchlist
        (ordered by created_at desc, excluding the target run)."""
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text as run_id,
                       cluster_state, dominant_archetype_key,
                       drift_score, pattern_entropy,
                       reinforcement_share, recovery_share,
                       rotation_share, degradation_share, mixed_share,
                       created_at
                from public.cross_asset_archetype_cluster_summary
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

    # ── cluster computation primitives ──────────────────────────────────
    @staticmethod
    def compute_archetype_mix(
        archetype_keys: list[str],
    ) -> tuple[dict[str, float], dict[str, float]]:
        """Returns (archetype_mix_by_key, share_by_bucket)."""
        if not archetype_keys:
            return ({}, {
                "reinforcement": 0.0, "rotation": 0.0, "recovery": 0.0,
                "degradation":   0.0, "mixed":    0.0, "none":     0.0,
            })
        n = len(archetype_keys)
        key_counts = Counter(archetype_keys)
        archetype_mix = {k: round(v / n, 6) for k, v in key_counts.items()}

        bucket_counts: Counter[str] = Counter()
        for k, c in key_counts.items():
            bucket_counts[_ARCHETYPE_BUCKET.get(k, "mixed")] += c
        bucket_share = {
            b: round(bucket_counts.get(b, 0) / n, 6)
            for b in ("reinforcement", "rotation", "recovery",
                      "degradation", "mixed", "none")
        }
        return archetype_mix, bucket_share

    @staticmethod
    def compute_pattern_entropy(archetype_mix: dict[str, float]) -> float:
        """Shannon entropy normalized to [0, 1] over the archetype mix."""
        if not archetype_mix:
            return 0.0
        # Filter to positive shares only.
        shares = [s for s in archetype_mix.values() if s > 0.0]
        if not shares:
            return 0.0
        ent = -sum(s * math.log(s, 2) for s in shares if s > 0.0)
        max_ent = math.log(max(2, len(shares)), 2)
        if max_ent <= 0.0:
            return 0.0
        return _clip(ent / max_ent, 0.0, 1.0)

    @staticmethod
    def _pick_dominant_archetype(
        archetype_keys: list[str],
    ) -> str:
        if not archetype_keys:
            return "insufficient_history"
        counts = Counter(archetype_keys)
        # Sort by count desc, then archetype priority desc, then alpha asc
        ranked = sorted(
            counts.items(),
            key=lambda kv: (
                -kv[1],
                -_ARCHETYPE_PRIORITY.get(kv[0], 0),
                kv[0],
            ),
        )
        return ranked[0][0]

    def classify_cluster_state(
        self,
        *,
        bucket_share: dict[str, float],
        pattern_entropy: float,
        history_size: int,
        prior_dominant: str | None,
        current_dominant: str,
    ) -> str:
        if history_size < _MIN_HISTORY_FOR_CLUSTER:
            return "insufficient_history"
        rein = bucket_share.get("reinforcement", 0.0)
        rec  = bucket_share.get("recovery", 0.0)
        rot  = bucket_share.get("rotation", 0.0)
        deg  = bucket_share.get("degradation", 0.0)
        mix  = bucket_share.get("mixed", 0.0)

        # Deterioration takes priority if it dominates the mix.
        if deg >= 0.50:
            return "deteriorating"
        # Recovery dominates if it exceeds degradation and is broad.
        if rec >= 0.40 and rec > deg:
            return "recovering"
        # Rotation: rotation share elevated AND dominant archetype changed.
        if rot >= 0.40 or (rot >= 0.25 and prior_dominant is not None
                            and prior_dominant != current_dominant):
            return "rotating"
        # High entropy with no clear bucket → mixed.
        if mix >= 0.40 or pattern_entropy >= 0.85:
            return "mixed"
        # Reinforcement dominant + low drift → stable.
        if rein >= 0.40 and pattern_entropy <= 0.80:
            return "stable"
        return "mixed"

    def compute_drift_score(
        self,
        *,
        prior_bucket_share: dict[str, float] | None,
        current_bucket_share: dict[str, float],
        prior_entropy: float | None,
        current_entropy: float,
        dominant_changed: bool,
    ) -> float:
        """Bounded [0, 1] drift score from prior → current cluster state."""
        if prior_bucket_share is None:
            return 0.0
        # Sum of absolute share deltas / 2 (total-variation distance).
        keys = ("reinforcement", "rotation", "recovery", "degradation", "mixed", "none")
        tvd = sum(abs(current_bucket_share.get(k, 0.0) - prior_bucket_share.get(k, 0.0))
                  for k in keys) / 2.0
        # Entropy delta (clipped) and dominant-change boost.
        ent_delta = abs(current_entropy - (prior_entropy or 0.0))
        boost = 0.20 if dominant_changed else 0.0
        score = 0.6 * tvd + 0.2 * _clip(ent_delta, 0.0, 1.0) + boost
        return _clip(score, 0.0, 1.0)

    # ── regime rotation summary ─────────────────────────────────────────
    def compute_regime_rotation_summary(
        self,
        *,
        workspace_id: str,
        regime_key: str,
        window_label: str,
        recent_runs: list[dict[str, Any]],
    ) -> RegimeRotationSnapshot:
        if not recent_runs:
            return RegimeRotationSnapshot(
                workspace_id=workspace_id, regime_key=regime_key,
                window_label=window_label,
                prior_dominant_archetype_key=None,
                current_dominant_archetype_key=None,
                rotation_count=0,
                reinforcement_run_count=0, recovery_run_count=0,
                degradation_run_count=0, mixed_run_count=0,
                rotation_state="insufficient_history",
                regime_drift_score=0.0,
                metadata={"scoring_version": _SCORING_VERSION,
                          "window_size": _REGIME_WINDOW_SIZE},
            )

        # recent_runs is newest-first. current = newest, prior = second-newest.
        archetype_seq = [str(r.get("dominant_archetype_key") or "insufficient_history")
                         for r in recent_runs]
        current_dom = archetype_seq[0]
        prior_dom = archetype_seq[1] if len(archetype_seq) > 1 else None

        # Rotation count = transitions where archetype key changes between
        # adjacent runs (chronologically, so reverse the seq when counting).
        chrono = list(reversed(archetype_seq))
        rotation_count = sum(
            1 for a, b in zip(chrono, chrono[1:]) if a != b
        )

        # Bucket counts
        rein = 0
        rec  = 0
        deg  = 0
        mix  = 0
        for a in archetype_seq:
            bucket = _ARCHETYPE_BUCKET.get(a, "mixed")
            if bucket == "reinforcement":
                rein += 1
            elif bucket == "recovery":
                rec += 1
            elif bucket == "degradation":
                deg += 1
            elif bucket == "mixed":
                mix += 1
            # rotation handoffs counted in rotation_count, not a run-bucket here

        n = len(archetype_seq)
        history_short = n < _MIN_HISTORY_FOR_CLUSTER

        if history_short:
            rotation_state = "insufficient_history"
        elif deg / n >= 0.50:
            rotation_state = "deteriorating"
        elif rec / n >= 0.40:
            rotation_state = "recovering"
        elif rotation_count >= max(2, n // 3):
            rotation_state = "rotating"
        elif mix / n >= 0.40:
            rotation_state = "mixed"
        elif rein / n >= 0.40 and rotation_count <= 1:
            rotation_state = "stable"
        else:
            rotation_state = "mixed"

        # Regime drift score: rotation density × 0.5 + (deg − rein density)
        # boost when degradation is rising.
        rot_density = rotation_count / max(1, n - 1) if n > 1 else 0.0
        deg_minus_rein = (deg - rein) / max(1, n)
        regime_drift_score = _clip(
            0.5 * rot_density + 0.5 * max(0.0, deg_minus_rein),
            0.0, 1.0,
        )

        return RegimeRotationSnapshot(
            workspace_id=workspace_id, regime_key=regime_key,
            window_label=window_label,
            prior_dominant_archetype_key=prior_dom,
            current_dominant_archetype_key=current_dom,
            rotation_count=rotation_count,
            reinforcement_run_count=rein, recovery_run_count=rec,
            degradation_run_count=deg, mixed_run_count=mix,
            rotation_state=rotation_state,
            regime_drift_score=regime_drift_score,
            metadata={
                "scoring_version":     _SCORING_VERSION,
                "window_size":         _REGIME_WINDOW_SIZE,
                "history_size":        n,
                "rotation_density":    rot_density,
            },
        )

    # ── drift event detection ───────────────────────────────────────────
    def detect_pattern_drift_event(
        self,
        *,
        workspace_id: str,
        watchlist_id: str,
        regime_key: str | None,
        prior: dict[str, Any] | None,
        current_run_id: str,
        current_cluster: ClusterSnapshot,
    ) -> PatternDriftEvent | None:
        if prior is None:
            # No prior window — emit explicit insufficient_history event so
            # operators see the first cluster snapshot for the watchlist.
            return PatternDriftEvent(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                source_run_id=None,
                target_run_id=current_run_id,
                regime_key=regime_key,
                prior_cluster_state=None,
                current_cluster_state=current_cluster.cluster_state,
                prior_dominant_archetype_key=None,
                current_dominant_archetype_key=current_cluster.dominant_archetype_key,
                drift_event_type="insufficient_history",
                drift_score=0.0,
                reason_codes=["insufficient_history"],
                metadata={
                    "scoring_version": _SCORING_VERSION,
                    "no_prior_window": True,
                },
            )

        prior_state    = str(prior.get("cluster_state") or "insufficient_history")
        prior_dom      = str(prior.get("dominant_archetype_key") or "insufficient_history")
        current_state  = current_cluster.cluster_state
        current_dom    = current_cluster.dominant_archetype_key
        prior_run_id   = prior.get("run_id")

        prior_bucket_share = {
            "reinforcement": _as_float(prior.get("reinforcement_share")) or 0.0,
            "rotation":      _as_float(prior.get("rotation_share"))      or 0.0,
            "recovery":      _as_float(prior.get("recovery_share"))      or 0.0,
            "degradation":   _as_float(prior.get("degradation_share"))   or 0.0,
            "mixed":         _as_float(prior.get("mixed_share"))         or 0.0,
            "none":          0.0,
        }
        current_bucket_share = {
            "reinforcement": current_cluster.reinforcement_share or 0.0,
            "rotation":      current_cluster.rotation_share      or 0.0,
            "recovery":      current_cluster.recovery_share      or 0.0,
            "degradation":   current_cluster.degradation_share   or 0.0,
            "mixed":         current_cluster.mixed_share         or 0.0,
            "none":          0.0,
        }

        reason_codes: list[str] = []
        if prior_dom != current_dom:
            reason_codes.append("dominant_archetype_changed")
        if current_bucket_share["rotation"] - prior_bucket_share["rotation"] >= 0.10:
            reason_codes.append("rotation_share_increase")
        if current_bucket_share["degradation"] - prior_bucket_share["degradation"] >= 0.10:
            reason_codes.append("degradation_share_increase")
        if current_bucket_share["mixed"] - prior_bucket_share["mixed"] >= 0.10:
            reason_codes.append("mixed_share_increase")
        prior_entropy = _as_float(prior.get("pattern_entropy"))
        if prior_entropy is not None and (current_cluster.pattern_entropy or 0.0) - prior_entropy >= 0.10:
            reason_codes.append("entropy_increase")
        if prior_state in ("deteriorating", "rotating", "mixed") and current_state == "stable":
            reason_codes.append("stabilization_detected")

        # Pick the most specific drift event type using priority order.
        if current_state == "deteriorating" and prior_state != "deteriorating":
            drift_event_type = "degradation_acceleration"
        elif current_state == "rotating" or (prior_dom != current_dom and current_state != "deteriorating"):
            drift_event_type = "archetype_rotation"
        elif prior_state == "stable" and current_state in ("rotating", "deteriorating", "mixed"):
            drift_event_type = "reinforcement_break"
        elif prior_state == "recovering" and current_state in ("deteriorating", "mixed", "rotating"):
            drift_event_type = "recovery_break"
        elif current_state == "mixed" and prior_state != "mixed":
            drift_event_type = "mixed_noise_increase"
        elif current_state == "stable" and prior_state in ("rotating", "deteriorating", "mixed", "recovering"):
            drift_event_type = "stabilization"
        else:
            # No meaningful drift detected.
            return None

        drift_score = self.compute_drift_score(
            prior_bucket_share=prior_bucket_share,
            current_bucket_share=current_bucket_share,
            prior_entropy=prior_entropy,
            current_entropy=current_cluster.pattern_entropy or 0.0,
            dominant_changed=(prior_dom != current_dom),
        )

        return PatternDriftEvent(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            source_run_id=prior_run_id,
            target_run_id=current_run_id,
            regime_key=regime_key,
            prior_cluster_state=prior_state,
            current_cluster_state=current_state,
            prior_dominant_archetype_key=prior_dom,
            current_dominant_archetype_key=current_dom,
            drift_event_type=drift_event_type,
            drift_score=drift_score,
            reason_codes=reason_codes if reason_codes else [drift_event_type],
            metadata={
                "scoring_version":   _SCORING_VERSION,
                "prior_run_id":      prior_run_id,
                "prior_window_age":  None,
            },
        )

    # ── orchestration ───────────────────────────────────────────────────
    def build_cluster_snapshot(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> ClusterSnapshot | None:
        """Build a per-run cluster snapshot from the run's family archetypes
        plus a recent run-archetype window for context."""
        ctx = self._load_run_context(conn, run_id=run_id)
        family_archetypes = self._load_run_family_archetypes(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not family_archetypes:
            return None

        archetype_mix, bucket_share = self.compute_archetype_mix(family_archetypes)
        entropy = self.compute_pattern_entropy(archetype_mix)
        current_dom = self._pick_dominant_archetype(family_archetypes)

        # Pull recent run archetype history (excluding this run, newest first)
        recent = self.load_recent_run_archetypes(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            limit=_RUN_WINDOW_SIZE + 1,
        )
        # Drop the current run if present.
        recent_filtered = [r for r in recent if r.get("run_id") != run_id][:_RUN_WINDOW_SIZE]
        prior_dom: str | None = None
        if recent_filtered:
            prior_dom = str(recent_filtered[0].get("dominant_archetype_key") or "insufficient_history")

        history_size = len(family_archetypes)
        cluster_state = self.classify_cluster_state(
            bucket_share=bucket_share, pattern_entropy=entropy,
            history_size=history_size,
            prior_dominant=prior_dom, current_dominant=current_dom,
        )

        # Drift score against the most recent prior cluster snapshot.
        prior_snap = self._load_prior_cluster_snapshot(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            before_run_id=run_id,
        )
        if prior_snap is not None:
            prior_bucket_share = {
                "reinforcement": _as_float(prior_snap.get("reinforcement_share")) or 0.0,
                "rotation":      _as_float(prior_snap.get("rotation_share"))      or 0.0,
                "recovery":      _as_float(prior_snap.get("recovery_share"))      or 0.0,
                "degradation":   _as_float(prior_snap.get("degradation_share"))   or 0.0,
                "mixed":         _as_float(prior_snap.get("mixed_share"))         or 0.0,
                "none":          0.0,
            }
            drift_score = self.compute_drift_score(
                prior_bucket_share=prior_bucket_share,
                current_bucket_share=bucket_share,
                prior_entropy=_as_float(prior_snap.get("pattern_entropy")),
                current_entropy=entropy,
                dominant_changed=(prior_snap.get("dominant_archetype_key") != current_dom),
            )
        else:
            drift_score = 0.0

        return ClusterSnapshot(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=ctx.get("context_snapshot_id"),
            regime_key=ctx.get("regime_key"),
            window_label=f"recent_{_RUN_WINDOW_SIZE}",
            dominant_archetype_key=current_dom,
            archetype_mix=archetype_mix,
            reinforcement_share=bucket_share.get("reinforcement"),
            recovery_share=bucket_share.get("recovery"),
            rotation_share=bucket_share.get("rotation"),
            degradation_share=bucket_share.get("degradation"),
            mixed_share=bucket_share.get("mixed"),
            pattern_entropy=entropy,
            cluster_state=cluster_state,
            drift_score=drift_score,
            metadata={
                "scoring_version":   _SCORING_VERSION,
                "history_size":      history_size,
                "run_window_size":   _RUN_WINDOW_SIZE,
                "prior_run_archetype": prior_dom,
            },
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_archetype_cluster_snapshot(
        self, conn, *, snap: ClusterSnapshot,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_archetype_cluster_snapshots(
            conn,
            workspace_id=snap.workspace_id,
            watchlist_id=snap.watchlist_id,
            run_id=snap.run_id,
            context_snapshot_id=snap.context_snapshot_id,
            regime_key=snap.regime_key,
            window_label=snap.window_label,
            dominant_archetype_key=snap.dominant_archetype_key,
            archetype_mix=snap.archetype_mix,
            reinforcement_share=snap.reinforcement_share,
            recovery_share=snap.recovery_share,
            rotation_share=snap.rotation_share,
            degradation_share=snap.degradation_share,
            mixed_share=snap.mixed_share,
            pattern_entropy=snap.pattern_entropy,
            cluster_state=snap.cluster_state,
            drift_score=snap.drift_score,
            metadata=snap.metadata,
        )
        return str(row["id"])

    def persist_regime_rotation_snapshot(
        self, conn, *, snap: RegimeRotationSnapshot,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_archetype_regime_rotation_snapshots(
            conn,
            workspace_id=snap.workspace_id,
            regime_key=snap.regime_key,
            window_label=snap.window_label,
            prior_dominant_archetype_key=snap.prior_dominant_archetype_key,
            current_dominant_archetype_key=snap.current_dominant_archetype_key,
            rotation_count=snap.rotation_count,
            reinforcement_run_count=snap.reinforcement_run_count,
            recovery_run_count=snap.recovery_run_count,
            degradation_run_count=snap.degradation_run_count,
            mixed_run_count=snap.mixed_run_count,
            rotation_state=snap.rotation_state,
            regime_drift_score=snap.regime_drift_score,
            metadata=snap.metadata,
        )
        return str(row["id"])

    def persist_pattern_drift_event(
        self, conn, *, event: PatternDriftEvent,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_pattern_drift_event_snapshots(
            conn,
            workspace_id=event.workspace_id,
            watchlist_id=event.watchlist_id,
            source_run_id=event.source_run_id,
            target_run_id=event.target_run_id,
            regime_key=event.regime_key,
            prior_cluster_state=event.prior_cluster_state,
            current_cluster_state=event.current_cluster_state,
            prior_dominant_archetype_key=event.prior_dominant_archetype_key,
            current_dominant_archetype_key=event.current_dominant_archetype_key,
            drift_event_type=event.drift_event_type,
            drift_score=event.drift_score,
            reason_codes=event.reason_codes,
            metadata=event.metadata,
        )
        return str(row["id"])

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> ClusterSnapshot | None:
        snap = self.build_cluster_snapshot(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if snap is None:
            return None

        # Persist cluster snapshot
        prior_snap = self._load_prior_cluster_snapshot(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            before_run_id=run_id,
        )
        self.persist_archetype_cluster_snapshot(conn, snap=snap)

        # Detect + persist drift event
        event = self.detect_pattern_drift_event(
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            regime_key=snap.regime_key, prior=prior_snap,
            current_run_id=run_id, current_cluster=snap,
        )
        if event is not None:
            self.persist_pattern_drift_event(conn, event=event)

        # Refresh regime rotation summary if the run carries a regime_key
        if snap.regime_key:
            recent = self.load_recent_run_archetypes(
                conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
                regime_key=snap.regime_key, limit=_REGIME_WINDOW_SIZE,
            )
            rotation = self.compute_regime_rotation_summary(
                workspace_id=workspace_id, regime_key=snap.regime_key,
                window_label=f"recent_{_REGIME_WINDOW_SIZE}",
                recent_runs=recent,
            )
            self.persist_regime_rotation_snapshot(conn, snap=rotation)

        return snap

    def refresh_workspace_pattern_clusters(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[ClusterSnapshot]:
        """Emit pattern-cluster diagnostics for every watchlist. Commits
        per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[ClusterSnapshot] = []
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
                    "cross_asset_pattern_cluster: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
