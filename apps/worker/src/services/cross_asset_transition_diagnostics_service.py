"""Phase 4.3A: Family-Level Sequencing and Transition-State Diagnostics Service.

Classifies each dependency family's current state (derived from 4.1C regime-
adjusted contribution + 4.2B timing class + context coverage) and transition
state vs the most recent prior run. Emits one primary transition event per
family per run and builds a short fixed-window sequence signature.

All classification is deterministic; repeated runs on unchanged inputs
produce identical states, events, and signatures.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_SCORING_VERSION          = "4.3A.v1"
_SEQUENCE_WINDOW          = 5         # last N observations per family
_MIN_SEQUENCE_OBSERVATIONS = 3         # below → insufficient_history
_CONTRIBUTION_CONFIRMED_THRESHOLD    = 0.02
_CONTRIBUTION_CONTRADICTED_THRESHOLD = -0.02
_CONTRIBUTION_MATERIAL_DELTA         = 0.025
_RANK_MATERIAL_DELTA                 = 1

# Event-type priority when multiple conditions fire simultaneously. Higher
# takes precedence.
_EVENT_PRIORITY: dict[str, int] = {
    "dominance_gain":   100,
    "dominance_loss":    95,
    "regime_shift":      90,
    "timing_shift":      85,
    "degradation":       80,
    "recovery":          75,
    "rank_shift":        70,
    "state_shift":       60,
}


@dataclass
class FamilyStateRow:
    dependency_family: str
    regime_key: str | None
    dominant_timing_class: str | None
    signal_state: str
    transition_state: str
    family_contribution: float | None
    timing_adjusted_contribution: float | None
    timing_integration_contribution: float | None
    family_rank: int | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FamilyTransitionEvent:
    dependency_family: str
    prior_signal_state: str | None
    current_signal_state: str
    prior_transition_state: str | None
    current_transition_state: str
    prior_family_rank: int | None
    current_family_rank: int | None
    rank_delta: int | None
    prior_family_contribution: float | None
    current_family_contribution: float | None
    contribution_delta: float | None
    event_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FamilySequenceRow:
    dependency_family: str
    window_label: str
    sequence_signature: str
    sequence_length: int
    dominant_sequence_class: str
    sequence_confidence: float | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TransitionDiagnosticsResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    prior_run_id: str | None
    state_rows: list[FamilyStateRow]
    event_rows: list[FamilyTransitionEvent]
    sequence_rows: list[FamilySequenceRow]


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _num_delta(a: float | None, b: float | None) -> float | None:
    if a is None and b is None:
        return None
    return (b or 0.0) - (a or 0.0)


class CrossAssetTransitionDiagnosticsService:
    """Deterministic per-family transition-state classification + sequencing."""

    # ── state derivation ───────────────────────────────────────────────
    def classify_signal_state(
        self,
        *,
        family_contribution: float | None,
        missing_count: int,
        stale_count: int,
        signal_count: int,
    ) -> str:
        """Derive signal state from the 4.1C family attribution's contribution
        sign and the 4.0D family count metadata (missing/stale)."""
        if signal_count == 0:
            return "insufficient_data"
        if missing_count >= max(1, signal_count // 2):
            return "missing_context"
        if stale_count >= max(1, signal_count // 2):
            return "stale_context"
        if family_contribution is None:
            return "insufficient_data"
        if family_contribution >= _CONTRIBUTION_CONFIRMED_THRESHOLD:
            return "confirmed"
        if family_contribution <= _CONTRIBUTION_CONTRADICTED_THRESHOLD:
            return "contradicted"
        return "unconfirmed"

    def classify_transition_state(
        self,
        *,
        prior: FamilyStateRow | None,
        current_signal_state: str,
        current_contribution: float | None,
        current_rank: int | None,
    ) -> str:
        if prior is None:
            return "insufficient_history"

        prior_contrib = prior.family_contribution
        contrib_delta = _num_delta(prior_contrib, current_contribution)
        rank_delta = None
        if prior.family_rank is not None and current_rank is not None:
            rank_delta = current_rank - prior.family_rank

        # dominance → rotating_in
        if prior.family_rank != 1 and current_rank == 1:
            return "rotating_in"
        # lost dominance → rotating_out
        if prior.family_rank == 1 and current_rank != 1:
            return "rotating_out"
        # material rank improvement → rotating_in; material rank slip → rotating_out
        if rank_delta is not None:
            if rank_delta <= -_RANK_MATERIAL_DELTA:
                return "rotating_in"
            if rank_delta >= _RANK_MATERIAL_DELTA:
                return "rotating_out"

        # recovery: prior weak/missing/stale + current confirmed/unconfirmed
        weak_states = {"contradicted", "missing_context", "stale_context", "insufficient_data"}
        positive_states = {"confirmed", "unconfirmed"}
        if prior.signal_state in weak_states and current_signal_state in positive_states:
            return "recovering"
        # degradation: prior confirmed/stable + current contradicted/weak
        if prior.signal_state == "confirmed" and current_signal_state in weak_states:
            return "deteriorating"
        # reinforcing: both confirmed + contribution holding or improving
        if prior.signal_state == "confirmed" and current_signal_state == "confirmed":
            if contrib_delta is not None and contrib_delta >= -_CONTRIBUTION_MATERIAL_DELTA:
                return "reinforcing"
        # deteriorating: contribution fell materially below neutral band
        if (contrib_delta is not None and contrib_delta <= -_CONTRIBUTION_MATERIAL_DELTA
                and current_signal_state != "confirmed"):
            return "deteriorating"

        return "stable"

    # ── input loading ───────────────────────────────────────────────────
    def _load_current_state(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        """Returns per-family current state inputs: regime, timing class,
        rank, raw/weighted/regime/timing-adjusted/timing-integration
        contributions, and missing/stale counts (from 4.0D if present)."""
        out: dict[str, dict[str, Any]] = {}
        with conn.cursor() as cur:
            # 4.1C regime family rows (authoritative for regime_key,
            # family_contribution, family_rank at regime layer)
            cur.execute(
                """
                select dependency_family, regime_key,
                       regime_adjusted_family_contribution, regime_family_rank
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
                out.setdefault(fam, {})
                out[fam]["regime_key"]                  = d.get("regime_key")
                out[fam]["family_contribution"]         = _as_float(d.get("regime_adjusted_family_contribution"))
                out[fam]["family_rank"]                 = d.get("regime_family_rank")

            # 4.2B timing-aware family (dominant_timing_class + timing_adjusted)
            cur.execute(
                """
                select dependency_family, dominant_timing_class,
                       timing_adjusted_family_contribution
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
                out.setdefault(fam, {})
                out[fam]["dominant_timing_class"]        = d.get("dominant_timing_class")
                out[fam]["timing_adjusted_contribution"] = _as_float(d.get("timing_adjusted_family_contribution"))

            # 4.2C timing composite family (timing_integration_contribution)
            cur.execute(
                """
                select dependency_family, timing_integration_contribution
                from public.cross_asset_family_timing_composite_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                out.setdefault(fam, {})
                out[fam]["timing_integration_contribution"] = _as_float(d.get("timing_integration_contribution"))

            # 4.0D family explanation counts (for missing/stale/signal_count)
            cur.execute(
                """
                select dependency_family,
                       family_signal_count, confirmed_count, contradicted_count,
                       missing_count, stale_count
                from public.cross_asset_family_explanation_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                out.setdefault(fam, {})
                out[fam]["signal_count"]     = int(d.get("family_signal_count") or 0)
                out[fam]["missing_count"]    = int(d.get("missing_count") or 0)
                out[fam]["stale_count"]      = int(d.get("stale_count") or 0)
                out[fam]["confirmed_count"]  = int(d.get("confirmed_count") or 0)

            # context_snapshot_id carry-through
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
                out.setdefault("__run__", {})["context_snapshot_id"] = dict(r).get("context_snapshot_id")

        return out

    def _load_prior_run_id(
        self, conn, *, workspace_id: str, watchlist_id: str, current_run_id: str,
    ) -> str | None:
        """Pick the most recent prior transition-state run for this watchlist
        before the current run's first state row."""
        with conn.cursor() as cur:
            cur.execute(
                """
                select distinct run_id::text as run_id, max(created_at) as latest
                from public.cross_asset_family_transition_state_snapshots
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       <> %s::uuid
                group by run_id
                order by latest desc
                limit 1
                """,
                (workspace_id, watchlist_id, current_run_id),
            )
            row = cur.fetchone()
            return dict(row).get("run_id") if row else None

    def _load_prior_family_states(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, FamilyStateRow]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family, regime_key, dominant_timing_class,
                       signal_state, transition_state,
                       family_contribution,
                       timing_adjusted_contribution,
                       timing_integration_contribution,
                       family_rank, metadata
                from public.cross_asset_family_transition_state_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            out: dict[str, FamilyStateRow] = {}
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                out[fam] = FamilyStateRow(
                    dependency_family=fam,
                    regime_key=d.get("regime_key"),
                    dominant_timing_class=d.get("dominant_timing_class"),
                    signal_state=d.get("signal_state"),
                    transition_state=d.get("transition_state"),
                    family_contribution=_as_float(d.get("family_contribution")),
                    timing_adjusted_contribution=_as_float(d.get("timing_adjusted_contribution")),
                    timing_integration_contribution=_as_float(d.get("timing_integration_contribution")),
                    family_rank=d.get("family_rank"),
                )
            return out

    def get_recent_family_state_history(
        self, conn, *, workspace_id: str, watchlist_id: str,
        dependency_family: str, limit: int = _SEQUENCE_WINDOW,
    ) -> list[FamilyStateRow]:
        """Load the last N transition-state rows for a given family, newest first."""
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family, regime_key, dominant_timing_class,
                       signal_state, transition_state,
                       family_contribution,
                       timing_adjusted_contribution,
                       timing_integration_contribution,
                       family_rank, created_at
                from public.cross_asset_family_transition_state_snapshots
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and dependency_family = %s
                order by created_at desc
                limit %s
                """,
                (workspace_id, watchlist_id, dependency_family, limit),
            )
            out: list[FamilyStateRow] = []
            for r in cur.fetchall():
                d = dict(r)
                out.append(FamilyStateRow(
                    dependency_family=dependency_family,
                    regime_key=d.get("regime_key"),
                    dominant_timing_class=d.get("dominant_timing_class"),
                    signal_state=d.get("signal_state"),
                    transition_state=d.get("transition_state"),
                    family_contribution=_as_float(d.get("family_contribution")),
                    timing_adjusted_contribution=_as_float(d.get("timing_adjusted_contribution")),
                    timing_integration_contribution=_as_float(d.get("timing_integration_contribution")),
                    family_rank=d.get("family_rank"),
                ))
            return out

    # ── event derivation ────────────────────────────────────────────────
    def compute_transition_event(
        self,
        *,
        family: str,
        prior: FamilyStateRow | None,
        current: FamilyStateRow,
    ) -> FamilyTransitionEvent | None:
        if prior is None:
            return None

        rank_delta    = None
        if prior.family_rank is not None and current.family_rank is not None:
            rank_delta = current.family_rank - prior.family_rank
        contrib_delta = _num_delta(prior.family_contribution, current.family_contribution)

        candidates: list[str] = []
        # dominance
        if prior.family_rank != 1 and current.family_rank == 1:
            candidates.append("dominance_gain")
        elif prior.family_rank == 1 and current.family_rank != 1:
            candidates.append("dominance_loss")
        # regime shift
        if prior.regime_key is not None and current.regime_key is not None \
                and prior.regime_key != current.regime_key:
            candidates.append("regime_shift")
        # timing shift
        if (prior.dominant_timing_class is not None and current.dominant_timing_class is not None
                and prior.dominant_timing_class != current.dominant_timing_class):
            candidates.append("timing_shift")
        # degradation / recovery
        weak = {"contradicted", "missing_context", "stale_context", "insufficient_data"}
        if prior.signal_state in weak and current.signal_state in ("confirmed", "unconfirmed"):
            candidates.append("recovery")
        if prior.signal_state == "confirmed" and current.signal_state in weak:
            candidates.append("degradation")
        # rank shift
        if rank_delta is not None and abs(rank_delta) >= _RANK_MATERIAL_DELTA \
                and "dominance_gain" not in candidates and "dominance_loss" not in candidates:
            candidates.append("rank_shift")
        # state shift
        if (prior.signal_state != current.signal_state
                or prior.transition_state != current.transition_state):
            candidates.append("state_shift")

        if not candidates:
            return None

        # Pick highest-priority event type
        event_type = max(candidates, key=lambda t: _EVENT_PRIORITY.get(t, 0))
        return FamilyTransitionEvent(
            dependency_family=family,
            prior_signal_state=prior.signal_state,
            current_signal_state=current.signal_state,
            prior_transition_state=prior.transition_state,
            current_transition_state=current.transition_state,
            prior_family_rank=prior.family_rank,
            current_family_rank=current.family_rank,
            rank_delta=rank_delta,
            prior_family_contribution=prior.family_contribution,
            current_family_contribution=current.family_contribution,
            contribution_delta=contrib_delta,
            event_type=event_type,
            metadata={
                "scoring_version":   _SCORING_VERSION,
                "candidate_events":  candidates,
            },
        )

    # ── sequence derivation ─────────────────────────────────────────────
    def build_sequence_signature(
        self, history: Sequence[FamilyStateRow],
    ) -> str:
        """Produces a compact signature like:
        'confirmed>reinforcing | confirmed>stable | contradicted>deteriorating'
        Ordered oldest → newest."""
        if not history:
            return ""
        ordered = list(reversed(history))  # history is newest-first; signature reads old→new
        parts = [f"{h.signal_state}>{h.transition_state}" for h in ordered]
        return " | ".join(parts)

    def classify_sequence_path(
        self, history: Sequence[FamilyStateRow],
    ) -> str:
        if len(history) < _MIN_SEQUENCE_OBSERVATIONS:
            return "insufficient_history"
        transitions = [h.transition_state for h in history]
        c_reinforcing  = sum(1 for t in transitions if t == "reinforcing")
        c_deteriorating = sum(1 for t in transitions if t == "deteriorating")
        c_recovering   = sum(1 for t in transitions if t == "recovering")
        c_rotation     = sum(1 for t in transitions if t in ("rotating_in", "rotating_out"))
        total = len(transitions)

        # Deterministic classification with stable tie-break priority:
        # rotation > recovery > deteriorating > reinforcing > mixed
        if c_rotation > 0:
            return "rotation_path"
        if c_recovering >= max(1, total // 3):
            return "recovery_path"
        if c_deteriorating >= max(1, total // 2):
            return "deteriorating_path"
        if c_reinforcing >= max(1, total // 2):
            return "reinforcing_path"
        return "mixed_path"

    def compute_sequence_confidence(
        self, history: Sequence[FamilyStateRow], sequence_class: str,
    ) -> float | None:
        if not history:
            return None
        n = len(history)
        if n < _MIN_SEQUENCE_OBSERVATIONS:
            return max(0.0, min(1.0, n / _MIN_SEQUENCE_OBSERVATIONS))
        transitions = [h.transition_state for h in history]
        match_count = sum(1 for t in transitions if self._matches_class(t, sequence_class))
        # confidence = match ratio × window fullness
        fullness = n / _SEQUENCE_WINDOW
        return max(0.0, min(1.0, (match_count / n) * fullness))

    @staticmethod
    def _matches_class(transition: str, sequence_class: str) -> bool:
        if sequence_class == "reinforcing_path":  return transition == "reinforcing"
        if sequence_class == "deteriorating_path": return transition == "deteriorating"
        if sequence_class == "recovery_path":     return transition == "recovering"
        if sequence_class == "rotation_path":     return transition in ("rotating_in", "rotating_out")
        return False  # mixed / insufficient

    # ── orchestration ───────────────────────────────────────────────────
    def build_transition_diagnostics_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> TransitionDiagnosticsResult | None:
        current_inputs = self._load_current_state(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        run_meta = current_inputs.pop("__run__", {})
        context_snapshot_id = run_meta.get("context_snapshot_id") if isinstance(run_meta, dict) else None
        if not current_inputs:
            return None

        prior_run_id = self._load_prior_run_id(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, current_run_id=run_id,
        )
        prior_states: dict[str, FamilyStateRow] = {}
        if prior_run_id is not None:
            prior_states = self._load_prior_family_states(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                run_id=prior_run_id,
            )

        state_rows: list[FamilyStateRow] = []
        event_rows: list[FamilyTransitionEvent] = []
        sequence_rows: list[FamilySequenceRow] = []

        for fam, inp in sorted(current_inputs.items()):
            contrib      = _as_float(inp.get("family_contribution"))
            signal_state = self.classify_signal_state(
                family_contribution=contrib,
                missing_count=int(inp.get("missing_count") or 0),
                stale_count=int(inp.get("stale_count") or 0),
                signal_count=int(inp.get("signal_count") or 0),
            )
            prior = prior_states.get(fam)
            transition_state = self.classify_transition_state(
                prior=prior,
                current_signal_state=signal_state,
                current_contribution=contrib,
                current_rank=inp.get("family_rank"),
            )
            state_row = FamilyStateRow(
                dependency_family=fam,
                regime_key=inp.get("regime_key"),
                dominant_timing_class=inp.get("dominant_timing_class"),
                signal_state=signal_state,
                transition_state=transition_state,
                family_contribution=contrib,
                timing_adjusted_contribution=_as_float(inp.get("timing_adjusted_contribution")),
                timing_integration_contribution=_as_float(inp.get("timing_integration_contribution")),
                family_rank=inp.get("family_rank"),
                metadata={
                    "scoring_version":   _SCORING_VERSION,
                    "signal_count":      int(inp.get("signal_count") or 0),
                    "missing_count":     int(inp.get("missing_count") or 0),
                    "stale_count":       int(inp.get("stale_count") or 0),
                    "has_prior":         prior is not None,
                    "prior_run_id":      prior_run_id,
                },
            )
            state_rows.append(state_row)

            event = self.compute_transition_event(
                family=fam, prior=prior, current=state_row,
            )
            if event is not None:
                event_rows.append(event)

            # Sequence: combine prior history with current row
            history = self.get_recent_family_state_history(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                dependency_family=fam,
                limit=_SEQUENCE_WINDOW - 1,  # leave room for current
            )
            # Prepend current (it's newer than stored history)
            combined = [state_row] + history
            combined = combined[:_SEQUENCE_WINDOW]
            sequence_class = self.classify_sequence_path(combined)
            signature      = self.build_sequence_signature(combined)
            confidence     = self.compute_sequence_confidence(combined, sequence_class)
            sequence_rows.append(FamilySequenceRow(
                dependency_family=fam,
                window_label=f"recent_{_SEQUENCE_WINDOW}",
                sequence_signature=signature,
                sequence_length=len(combined),
                dominant_sequence_class=sequence_class,
                sequence_confidence=confidence,
                metadata={
                    "scoring_version":    _SCORING_VERSION,
                    "window_size":        _SEQUENCE_WINDOW,
                    "min_observations":   _MIN_SEQUENCE_OBSERVATIONS,
                    "history_length":     len(history),
                },
            ))

        return TransitionDiagnosticsResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snapshot_id,
            prior_run_id=prior_run_id,
            state_rows=state_rows,
            event_rows=event_rows,
            sequence_rows=sequence_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_transition_state_snapshots(
        self, conn, *, result: TransitionDiagnosticsResult,
    ) -> int:
        if not result.state_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_transition_state_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            rows=[
                {
                    "dependency_family":               s.dependency_family,
                    "regime_key":                      s.regime_key,
                    "dominant_timing_class":           s.dominant_timing_class,
                    "signal_state":                    s.signal_state,
                    "transition_state":                s.transition_state,
                    "family_contribution":             s.family_contribution,
                    "timing_adjusted_contribution":    s.timing_adjusted_contribution,
                    "timing_integration_contribution": s.timing_integration_contribution,
                    "family_rank":                     s.family_rank,
                    "metadata":                        s.metadata,
                }
                for s in result.state_rows
            ],
        )

    def persist_transition_event_snapshots(
        self, conn, *, result: TransitionDiagnosticsResult,
    ) -> int:
        if not result.event_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_transition_event_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            source_run_id=result.prior_run_id,
            target_run_id=result.run_id,
            rows=[
                {
                    "dependency_family":           e.dependency_family,
                    "prior_signal_state":          e.prior_signal_state,
                    "current_signal_state":        e.current_signal_state,
                    "prior_transition_state":      e.prior_transition_state,
                    "current_transition_state":    e.current_transition_state,
                    "prior_family_rank":           e.prior_family_rank,
                    "current_family_rank":         e.current_family_rank,
                    "rank_delta":                  e.rank_delta,
                    "prior_family_contribution":   e.prior_family_contribution,
                    "current_family_contribution": e.current_family_contribution,
                    "contribution_delta":          e.contribution_delta,
                    "event_type":                  e.event_type,
                    "metadata":                    e.metadata,
                }
                for e in result.event_rows
            ],
        )

    def persist_sequence_summary_snapshots(
        self, conn, *, result: TransitionDiagnosticsResult,
    ) -> int:
        if not result.sequence_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_sequence_summary_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            rows=[
                {
                    "dependency_family":       s.dependency_family,
                    "window_label":            s.window_label,
                    "sequence_signature":      s.sequence_signature,
                    "sequence_length":         s.sequence_length,
                    "dominant_sequence_class": s.dominant_sequence_class,
                    "sequence_confidence":     s.sequence_confidence,
                    "metadata":                s.metadata,
                }
                for s in result.sequence_rows
            ],
        )

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> TransitionDiagnosticsResult | None:
        result = self.build_transition_diagnostics_for_run(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if result is None:
            return None
        self.persist_transition_state_snapshots(conn, result=result)
        self.persist_transition_event_snapshots(conn, result=result)
        self.persist_sequence_summary_snapshots(conn, result=result)
        return result

    def refresh_workspace_transition_diagnostics(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[TransitionDiagnosticsResult]:
        """Emit transition diagnostics for every watchlist. Commits per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[TransitionDiagnosticsResult] = []
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
                    "cross_asset_transition_diagnostics: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
