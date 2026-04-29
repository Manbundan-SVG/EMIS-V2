"""Phase 4.6A: Cross-Window Regime Memory and Persistence Diagnostics.

Computes per-run state persistence ratios, regime memory summaries, and
discrete persistence transition events on top of the 4.1C regime, 4.3A
transition diagnostics, 4.4A archetype, and 4.5A pattern-cluster surfaces.

Persists:
  * cross_asset_state_persistence_snapshots (one per run/window)
  * cross_asset_regime_memory_snapshots (one per regime/window)
  * cross_asset_persistence_transition_event_snapshots (per detected event)

All persistence logic is deterministic, bounded, and metadata-stamped. No
predictive forecasting — windows are fixed and the rules are explicit.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.6A.v1"

_RUN_WINDOW_SIZE         = 20  # state persistence window
_REGIME_WINDOW_SIZE      = 50  # regime memory window
_MIN_HISTORY_FOR_PERSIST = 3   # below this we mark insufficient_history
_PERSISTENT_STATE_AGE    = 3   # state age threshold for persistent classification
_PERSISTENT_MEMORY_SCORE = 0.65
_FRAGILE_MEMORY_SCORE    = 0.30


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _norm(s: Any) -> str:
    if s is None:
        return "null"
    return str(s)


@dataclass
class StatePersistenceSnapshot:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    window_label: str
    regime_key: str | None
    dominant_timing_class: str | None
    dominant_transition_state: str | None
    dominant_sequence_class: str | None
    dominant_archetype_key: str | None
    cluster_state: str | None
    current_state_signature: str
    state_age_runs: int
    same_state_count: int
    state_persistence_ratio: float | None
    regime_persistence_ratio: float | None
    cluster_persistence_ratio: float | None
    archetype_persistence_ratio: float | None
    persistence_state: str
    memory_score: float | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RegimeMemorySnapshot:
    workspace_id: str
    regime_key: str
    window_label: str
    run_count: int
    same_regime_streak_count: int
    regime_switch_count: int
    avg_regime_duration_runs: float | None
    max_regime_duration_runs: int | None
    regime_memory_score: float | None
    dominant_cluster_state: str | None
    dominant_archetype_key: str | None
    persistence_state: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PersistenceTransitionEvent:
    workspace_id: str
    watchlist_id: str | None
    source_run_id: str | None
    target_run_id: str
    regime_key: str | None
    prior_state_signature: str | None
    current_state_signature: str
    prior_persistence_state: str | None
    current_persistence_state: str
    prior_memory_score: float | None
    current_memory_score: float | None
    memory_score_delta: float | None
    event_type: str
    reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


class CrossAssetPersistenceService:
    """Deterministic cross-window state-persistence + regime-memory diagnostics."""

    # ── input loading ───────────────────────────────────────────────────
    def _load_run_state(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,  # noqa: ARG002
    ) -> dict[str, Any]:
        """Load the current run's regime + timing + transition + sequence +
        archetype + cluster state for state-signature construction."""
        ctx: dict[str, Any] = {
            "context_snapshot_id":         None,
            "regime_key":                  None,
            "dominant_timing_class":       None,
            "dominant_transition_state":   None,
            "dominant_sequence_class":     None,
            "dominant_archetype_key":      None,
            "cluster_state":               None,
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

            cur.execute(
                """
                select dominant_timing_class
                from public.run_cross_asset_transition_diagnostics_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                ctx["dominant_timing_class"] = dict(r).get("dominant_timing_class")

            cur.execute(
                """
                select dominant_transition_state, dominant_sequence_class
                from public.run_cross_asset_transition_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                d = dict(r)
                ctx["dominant_transition_state"] = d.get("dominant_transition_state")
                ctx["dominant_sequence_class"]   = d.get("dominant_sequence_class")

            cur.execute(
                """
                select dominant_archetype_key
                from public.cross_asset_run_archetype_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                ctx["dominant_archetype_key"] = dict(r).get("dominant_archetype_key")

            cur.execute(
                """
                select cluster_state
                from public.cross_asset_archetype_cluster_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                ctx["cluster_state"] = dict(r).get("cluster_state")
        return ctx

    def load_recent_state_history(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str | None = None,
        regime_key: str | None = None,
        limit: int = _RUN_WINDOW_SIZE,
    ) -> list[dict[str, Any]]:
        """Return recent state rows (newest first) joining regime + cluster +
        archetype summaries by run."""
        with conn.cursor() as cur:
            cur.execute(
                """
                with attribution_recent as (
                    select run_id, workspace_id, watchlist_id,
                           created_at
                    from public.cross_asset_attribution_summary
                    where workspace_id = %s::uuid
                      and (%s::uuid is null or watchlist_id = %s::uuid)
                )
                select
                    a.run_id::text as run_id,
                    a.workspace_id::text as workspace_id,
                    a.watchlist_id::text as watchlist_id,
                    rgi.regime_key,
                    diag.dominant_timing_class,
                    tra.dominant_transition_state,
                    tra.dominant_sequence_class,
                    arch.dominant_archetype_key,
                    cs.cluster_state,
                    a.created_at
                from attribution_recent a
                left join public.run_cross_asset_regime_integration_summary    rgi  on rgi.run_id  = a.run_id
                left join public.run_cross_asset_transition_diagnostics_summary diag on diag.run_id = a.run_id
                left join public.run_cross_asset_transition_attribution_summary tra  on tra.run_id  = a.run_id
                left join public.cross_asset_run_archetype_summary             arch on arch.run_id = a.run_id
                left join public.cross_asset_archetype_cluster_summary         cs   on cs.run_id   = a.run_id
                where (%s::text is null or rgi.regime_key = %s::text)
                order by a.created_at desc
                limit %s
                """,
                (workspace_id, watchlist_id, watchlist_id,
                 regime_key, regime_key, int(limit)),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_prior_persistence_snapshot(
        self, conn, *, workspace_id: str, watchlist_id: str, before_run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text as run_id,
                       persistence_state, current_state_signature,
                       memory_score, state_age_runs,
                       state_persistence_ratio, regime_persistence_ratio,
                       cluster_persistence_ratio, archetype_persistence_ratio,
                       regime_key, cluster_state, dominant_archetype_key,
                       created_at
                from public.cross_asset_state_persistence_summary
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

    # ── primitives ──────────────────────────────────────────────────────
    @staticmethod
    def build_state_signature(state: dict[str, Any]) -> str:
        """Compact deterministic signature with explicit nulls."""
        return (
            f"regime={_norm(state.get('regime_key'))}|"
            f"timing={_norm(state.get('dominant_timing_class'))}|"
            f"transition={_norm(state.get('dominant_transition_state'))}|"
            f"sequence={_norm(state.get('dominant_sequence_class'))}|"
            f"archetype={_norm(state.get('dominant_archetype_key'))}|"
            f"cluster={_norm(state.get('cluster_state'))}"
        )

    @staticmethod
    def compute_state_age_runs(
        current_signature: str, recent_signatures: list[str],
    ) -> int:
        """Number of contiguous recent runs (newest-first) matching current
        signature, including the current run."""
        age = 1  # current run counts as 1
        for sig in recent_signatures:
            if sig == current_signature:
                age += 1
            else:
                break
        return age

    @staticmethod
    def compute_persistence_ratios(
        *,
        current_signature: str,
        current_regime: str | None,
        current_cluster: str | None,
        current_archetype: str | None,
        recent: list[dict[str, Any]],
    ) -> dict[str, float]:
        """Bounded [0, 1] ratios across the recent window (excluding the
        current run from the recent list — caller is responsible)."""
        n = len(recent) + 1  # include current run in denominator
        if n <= 1:
            return {
                "state":     1.0,
                "regime":    1.0,
                "cluster":   1.0,
                "archetype": 1.0,
            }
        state_match = sum(
            1 for r in recent
            if CrossAssetPersistenceService.build_state_signature(r) == current_signature
        ) + 1
        regime_match = sum(
            1 for r in recent
            if (r.get("regime_key") or None) == (current_regime or None)
        ) + 1
        cluster_match = sum(
            1 for r in recent
            if (r.get("cluster_state") or None) == (current_cluster or None)
        ) + 1
        archetype_match = sum(
            1 for r in recent
            if (r.get("dominant_archetype_key") or None) == (current_archetype or None)
        ) + 1
        return {
            "state":     _clip(state_match / n, 0.0, 1.0),
            "regime":    _clip(regime_match / n, 0.0, 1.0),
            "cluster":   _clip(cluster_match / n, 0.0, 1.0),
            "archetype": _clip(archetype_match / n, 0.0, 1.0),
        }

    @staticmethod
    def compute_memory_score(ratios: dict[str, float]) -> float:
        """40% state + 25% regime + 20% cluster + 15% archetype, clipped."""
        score = (
            0.40 * ratios.get("state",     0.0)
            + 0.25 * ratios.get("regime",    0.0)
            + 0.20 * ratios.get("cluster",   0.0)
            + 0.15 * ratios.get("archetype", 0.0)
        )
        return _clip(score, 0.0, 1.0)

    @staticmethod
    def classify_persistence_state(
        *,
        history_size: int,
        cluster_state: str | None,
        archetype_key: str | None,
        memory_score: float,
        state_age_runs: int,
    ) -> str:
        if history_size < _MIN_HISTORY_FOR_PERSIST:
            return "insufficient_history"
        # Cluster state takes precedence when it indicates a directional
        # persistence regime.
        if cluster_state == "deteriorating":
            return "breaking_down"
        if cluster_state == "recovering":
            return "recovering"
        if cluster_state == "rotating":
            return "rotating"
        if cluster_state == "mixed":
            return "mixed"
        if memory_score >= _PERSISTENT_MEMORY_SCORE and state_age_runs >= _PERSISTENT_STATE_AGE:
            return "persistent"
        if memory_score < _FRAGILE_MEMORY_SCORE:
            return "fragile"
        # Archetype hint when cluster is stable but memory is intermediate.
        if archetype_key == "rotation_handoff":
            return "rotating"
        if archetype_key == "deteriorating_breakdown":
            return "breaking_down"
        if archetype_key == "recovering_reentry":
            return "recovering"
        if archetype_key == "mixed_transition_noise":
            return "mixed"
        return "fragile"

    # ── regime memory ───────────────────────────────────────────────────
    def compute_regime_memory_summary(
        self,
        *,
        workspace_id: str,
        regime_key: str,
        window_label: str,
        recent_runs: list[dict[str, Any]],
    ) -> RegimeMemorySnapshot:
        """recent_runs: full recent history newest-first (across regimes), so
        we can count regime switches and contiguous durations."""
        if not recent_runs:
            return RegimeMemorySnapshot(
                workspace_id=workspace_id, regime_key=regime_key,
                window_label=window_label,
                run_count=0,
                same_regime_streak_count=0,
                regime_switch_count=0,
                avg_regime_duration_runs=None,
                max_regime_duration_runs=None,
                regime_memory_score=0.0,
                dominant_cluster_state=None,
                dominant_archetype_key=None,
                persistence_state="insufficient_history",
                metadata={"scoring_version": _SCORING_VERSION,
                          "window_size": _REGIME_WINDOW_SIZE,
                          "history_size": 0},
            )

        regime_seq_chrono = list(reversed([
            (r.get("regime_key") or None) for r in recent_runs
        ]))
        # Contiguous current-regime streak counted from newest end.
        streak = 0
        for r in regime_seq_chrono[::-1]:
            if r == regime_key:
                streak += 1
            else:
                break

        switches = sum(
            1 for a, b in zip(regime_seq_chrono, regime_seq_chrono[1:])
            if a != b
        )
        # Compute contiguous-duration runs of regime_key
        durations: list[int] = []
        current = 0
        for r in regime_seq_chrono:
            if r == regime_key:
                current += 1
            else:
                if current > 0:
                    durations.append(current)
                current = 0
        if current > 0:
            durations.append(current)

        avg_dur: float | None = (sum(durations) / len(durations)) if durations else None
        max_dur: int | None   = max(durations) if durations else None

        # Run count restricted to the regime
        regime_runs = [r for r in recent_runs if (r.get("regime_key") or None) == regime_key]
        run_count = len(regime_runs)
        n_total = len(recent_runs)

        # Memory score: fraction in regime × (1 − switch density)
        in_regime_share = run_count / n_total if n_total > 0 else 0.0
        switch_density = switches / max(1, n_total - 1) if n_total > 1 else 0.0
        regime_memory_score = _clip(in_regime_share * (1.0 - 0.5 * switch_density), 0.0, 1.0)

        # Dominant cluster + archetype within this regime (across regime_runs)
        cluster_counts = Counter(
            (r.get("cluster_state") or None) for r in regime_runs
            if r.get("cluster_state") is not None
        )
        archetype_counts = Counter(
            (r.get("dominant_archetype_key") or None) for r in regime_runs
            if r.get("dominant_archetype_key") is not None
        )
        dominant_cluster   = cluster_counts.most_common(1)[0][0] if cluster_counts else None
        dominant_archetype = archetype_counts.most_common(1)[0][0] if archetype_counts else None

        # Persistence state for the regime
        if n_total < _MIN_HISTORY_FOR_PERSIST:
            persistence_state = "insufficient_history"
        elif dominant_cluster == "deteriorating":
            persistence_state = "breaking_down"
        elif dominant_cluster == "recovering":
            persistence_state = "recovering"
        elif dominant_cluster == "rotating":
            persistence_state = "rotating"
        elif dominant_cluster == "mixed":
            persistence_state = "mixed"
        elif regime_memory_score >= _PERSISTENT_MEMORY_SCORE and streak >= _PERSISTENT_STATE_AGE:
            persistence_state = "persistent"
        elif regime_memory_score < _FRAGILE_MEMORY_SCORE:
            persistence_state = "fragile"
        else:
            persistence_state = "fragile"

        return RegimeMemorySnapshot(
            workspace_id=workspace_id, regime_key=regime_key,
            window_label=window_label,
            run_count=run_count,
            same_regime_streak_count=streak,
            regime_switch_count=switches,
            avg_regime_duration_runs=avg_dur,
            max_regime_duration_runs=max_dur,
            regime_memory_score=regime_memory_score,
            dominant_cluster_state=dominant_cluster,
            dominant_archetype_key=dominant_archetype,
            persistence_state=persistence_state,
            metadata={
                "scoring_version":    _SCORING_VERSION,
                "window_size":        _REGIME_WINDOW_SIZE,
                "history_size":       n_total,
                "in_regime_share":    in_regime_share,
                "switch_density":     switch_density,
            },
        )

    # ── persistence event detection ─────────────────────────────────────
    def detect_persistence_transition_event(
        self,
        *,
        workspace_id: str,
        watchlist_id: str,
        regime_key: str | None,
        prior: dict[str, Any] | None,
        current_run_id: str,
        current_snap: StatePersistenceSnapshot,
    ) -> PersistenceTransitionEvent | None:
        if prior is None:
            return PersistenceTransitionEvent(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                source_run_id=None,
                target_run_id=current_run_id,
                regime_key=regime_key,
                prior_state_signature=None,
                current_state_signature=current_snap.current_state_signature,
                prior_persistence_state=None,
                current_persistence_state=current_snap.persistence_state,
                prior_memory_score=None,
                current_memory_score=current_snap.memory_score,
                memory_score_delta=None,
                event_type="insufficient_history",
                reason_codes=["insufficient_history"],
                metadata={"scoring_version": _SCORING_VERSION,
                          "no_prior_window": True},
            )

        prior_state    = str(prior.get("persistence_state") or "insufficient_history")
        prior_sig      = prior.get("current_state_signature")
        prior_memory   = _as_float(prior.get("memory_score"))
        prior_run_id   = prior.get("run_id")
        prior_regime   = prior.get("regime_key")
        prior_cluster  = prior.get("cluster_state")
        prior_archetype = prior.get("dominant_archetype_key")

        current_state  = current_snap.persistence_state
        current_sig    = current_snap.current_state_signature
        current_memory = current_snap.memory_score
        memory_delta: float | None
        if prior_memory is not None and current_memory is not None:
            memory_delta = current_memory - prior_memory
        elif current_memory is not None and prior_memory is None:
            memory_delta = current_memory
        else:
            memory_delta = None

        reason_codes: list[str] = []
        if prior_sig != current_sig:
            reason_codes.append("state_signature_changed")
        if (prior_regime or None) != (current_snap.regime_key or None):
            reason_codes.append("regime_changed")
        if (prior_cluster or None) != (current_snap.cluster_state or None):
            reason_codes.append("cluster_state_changed")
        if (prior_archetype or None) != (current_snap.dominant_archetype_key or None):
            reason_codes.append("archetype_changed")
        if memory_delta is not None and memory_delta >= 0.10:
            reason_codes.append("memory_score_increased")
        if memory_delta is not None and memory_delta <= -0.10:
            reason_codes.append("memory_score_decreased")
        if current_snap.state_age_runs >= _PERSISTENT_STATE_AGE:
            reason_codes.append("state_age_threshold_met")

        # Pick most specific event type
        if (prior_regime or None) != (current_snap.regime_key or None):
            event_type = "regime_memory_break"
        elif (prior_cluster or None) != (current_snap.cluster_state or None):
            event_type = "cluster_memory_break"
        elif (prior_archetype or None) != (current_snap.dominant_archetype_key or None):
            event_type = "archetype_memory_break"
        elif current_state == "rotating" and prior_state != "rotating":
            event_type = "state_rotation"
        elif current_state == "persistent" and prior_state in ("fragile", "rotating", "mixed", "breaking_down", "recovering"):
            event_type = "stabilization"
        elif memory_delta is not None and memory_delta >= 0.10:
            event_type = "persistence_gain"
        elif memory_delta is not None and memory_delta <= -0.10:
            event_type = "persistence_loss"
        else:
            return None  # no meaningful event

        return PersistenceTransitionEvent(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            source_run_id=prior_run_id,
            target_run_id=current_run_id,
            regime_key=regime_key,
            prior_state_signature=prior_sig,
            current_state_signature=current_sig,
            prior_persistence_state=prior_state,
            current_persistence_state=current_state,
            prior_memory_score=prior_memory,
            current_memory_score=current_memory,
            memory_score_delta=memory_delta,
            event_type=event_type,
            reason_codes=reason_codes if reason_codes else [event_type],
            metadata={
                "scoring_version": _SCORING_VERSION,
                "prior_run_id":    prior_run_id,
            },
        )

    # ── orchestration ───────────────────────────────────────────────────
    def build_state_persistence_snapshot(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> StatePersistenceSnapshot | None:
        ctx = self._load_run_state(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        signature = self.build_state_signature(ctx)

        # Recent run history (newest-first), excluding current run.
        recent = self.load_recent_state_history(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            limit=_RUN_WINDOW_SIZE + 1,
        )
        recent_filtered = [r for r in recent if r.get("run_id") != run_id][:_RUN_WINDOW_SIZE]

        recent_signatures = [self.build_state_signature(r) for r in recent_filtered]
        state_age = self.compute_state_age_runs(signature, recent_signatures)
        same_state_count = sum(1 for s in recent_signatures if s == signature) + 1

        ratios = self.compute_persistence_ratios(
            current_signature=signature,
            current_regime=ctx.get("regime_key"),
            current_cluster=ctx.get("cluster_state"),
            current_archetype=ctx.get("dominant_archetype_key"),
            recent=recent_filtered,
        )
        memory_score = self.compute_memory_score(ratios)

        history_size = len(recent_filtered) + 1
        persistence_state = self.classify_persistence_state(
            history_size=history_size,
            cluster_state=ctx.get("cluster_state"),
            archetype_key=ctx.get("dominant_archetype_key"),
            memory_score=memory_score,
            state_age_runs=state_age,
        )

        return StatePersistenceSnapshot(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=ctx.get("context_snapshot_id"),
            window_label=f"recent_{_RUN_WINDOW_SIZE}",
            regime_key=ctx.get("regime_key"),
            dominant_timing_class=ctx.get("dominant_timing_class"),
            dominant_transition_state=ctx.get("dominant_transition_state"),
            dominant_sequence_class=ctx.get("dominant_sequence_class"),
            dominant_archetype_key=ctx.get("dominant_archetype_key"),
            cluster_state=ctx.get("cluster_state"),
            current_state_signature=signature,
            state_age_runs=state_age,
            same_state_count=same_state_count,
            state_persistence_ratio=ratios.get("state"),
            regime_persistence_ratio=ratios.get("regime"),
            cluster_persistence_ratio=ratios.get("cluster"),
            archetype_persistence_ratio=ratios.get("archetype"),
            persistence_state=persistence_state,
            memory_score=memory_score,
            metadata={
                "scoring_version":      _SCORING_VERSION,
                "history_size":         history_size,
                "run_window_size":      _RUN_WINDOW_SIZE,
                "persistent_threshold": _PERSISTENT_MEMORY_SCORE,
                "fragile_threshold":    _FRAGILE_MEMORY_SCORE,
            },
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_state_persistence_snapshot(
        self, conn, *, snap: StatePersistenceSnapshot,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_state_persistence_snapshots(
            conn,
            workspace_id=snap.workspace_id,
            watchlist_id=snap.watchlist_id,
            run_id=snap.run_id,
            context_snapshot_id=snap.context_snapshot_id,
            window_label=snap.window_label,
            regime_key=snap.regime_key,
            dominant_timing_class=snap.dominant_timing_class,
            dominant_transition_state=snap.dominant_transition_state,
            dominant_sequence_class=snap.dominant_sequence_class,
            dominant_archetype_key=snap.dominant_archetype_key,
            cluster_state=snap.cluster_state,
            current_state_signature=snap.current_state_signature,
            state_age_runs=snap.state_age_runs,
            same_state_count=snap.same_state_count,
            state_persistence_ratio=snap.state_persistence_ratio,
            regime_persistence_ratio=snap.regime_persistence_ratio,
            cluster_persistence_ratio=snap.cluster_persistence_ratio,
            archetype_persistence_ratio=snap.archetype_persistence_ratio,
            persistence_state=snap.persistence_state,
            memory_score=snap.memory_score,
            metadata=snap.metadata,
        )
        return str(row["id"])

    def persist_regime_memory_snapshot(
        self, conn, *, snap: RegimeMemorySnapshot,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_regime_memory_snapshots(
            conn,
            workspace_id=snap.workspace_id,
            regime_key=snap.regime_key,
            window_label=snap.window_label,
            run_count=snap.run_count,
            same_regime_streak_count=snap.same_regime_streak_count,
            regime_switch_count=snap.regime_switch_count,
            avg_regime_duration_runs=snap.avg_regime_duration_runs,
            max_regime_duration_runs=snap.max_regime_duration_runs,
            regime_memory_score=snap.regime_memory_score,
            dominant_cluster_state=snap.dominant_cluster_state,
            dominant_archetype_key=snap.dominant_archetype_key,
            persistence_state=snap.persistence_state,
            metadata=snap.metadata,
        )
        return str(row["id"])

    def persist_persistence_transition_event(
        self, conn, *, event: PersistenceTransitionEvent,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_persistence_transition_event_snapshots(
            conn,
            workspace_id=event.workspace_id,
            watchlist_id=event.watchlist_id,
            source_run_id=event.source_run_id,
            target_run_id=event.target_run_id,
            regime_key=event.regime_key,
            prior_state_signature=event.prior_state_signature,
            current_state_signature=event.current_state_signature,
            prior_persistence_state=event.prior_persistence_state,
            current_persistence_state=event.current_persistence_state,
            prior_memory_score=event.prior_memory_score,
            current_memory_score=event.current_memory_score,
            memory_score_delta=event.memory_score_delta,
            event_type=event.event_type,
            reason_codes=event.reason_codes,
            metadata=event.metadata,
        )
        return str(row["id"])

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> StatePersistenceSnapshot | None:
        snap = self.build_state_persistence_snapshot(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if snap is None:
            return None

        prior_snap = self._load_prior_persistence_snapshot(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            before_run_id=run_id,
        )
        self.persist_state_persistence_snapshot(conn, snap=snap)

        # Detect + persist persistence event
        event = self.detect_persistence_transition_event(
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            regime_key=snap.regime_key, prior=prior_snap,
            current_run_id=run_id, current_snap=snap,
        )
        if event is not None:
            self.persist_persistence_transition_event(conn, event=event)

        # Refresh regime memory summary if regime_key present
        if snap.regime_key:
            recent = self.load_recent_state_history(
                conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
                limit=_REGIME_WINDOW_SIZE,
            )
            regime_summary = self.compute_regime_memory_summary(
                workspace_id=workspace_id, regime_key=snap.regime_key,
                window_label=f"recent_{_REGIME_WINDOW_SIZE}",
                recent_runs=recent,
            )
            self.persist_regime_memory_snapshot(conn, snap=regime_summary)

        return snap

    def refresh_workspace_persistence(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[StatePersistenceSnapshot]:
        """Emit persistence diagnostics for every watchlist. Commits
        per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[StatePersistenceSnapshot] = []
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
                    "cross_asset_persistence: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
