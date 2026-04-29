"""Phase 4.6D: Replay Validation for Persistence-Aware Composite Behavior.

Compares a replay run against its source across the persistence stack:
  * 4.0B context hash
  * 2.5D regime key
  * 4.2B dominant timing class (inherited)
  * 4.3A dominant transition state + dominant sequence class
  * 4.4A dominant archetype key
  * 4.5A cluster state
  * 4.6A persistence state + memory score + state age + latest persistence event
  * 4.6B persistence-adjusted attribution (run aggregate + per-family)
  * 4.6C persistence-aware composite (persistence_net + pre/post composite +
    family integration contribution)
  * persistence dominant family from the 4.6B integration summary

Persists:
  * one cross_asset_persistence_replay_validation_snapshots row per pair
  * one cross_asset_family_persistence_replay_stability_snapshots row per family
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_NUMERIC_TOLERANCE        = 1e-9
_MEMORY_SCORE_TOLERANCE   = 1e-6
_SCORING_VERSION          = "4.6D.v1"


@dataclass
class PersistenceRunSnapshot:
    """Compact container for a single run's persistence-layer state."""
    run_id: str
    context_snapshot_id: str | None
    context_hash: str | None
    regime_key: str | None
    dominant_timing_class: str | None
    dominant_transition_state: str | None
    dominant_sequence_class: str | None
    dominant_archetype_key: str | None
    cluster_state: str | None
    persistence_state: str | None
    memory_score: float | None
    state_age_runs: int | None
    latest_persistence_event_type: str | None
    persistence_adjusted_total: float | None
    persistence_dominant_family: str | None
    persistence_net_contribution: float | None
    composite_pre_persistence: float | None
    composite_post_persistence: float | None
    # family_key -> {persistence_state, memory_score, state_age_runs,
    #                latest_persistence_event_type,
    #                persistence_adjusted_contribution, persistence_family_rank,
    #                persistence_integration_contribution,
    #                persistence_composite_family_rank}
    family: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class FamilyPersistenceStabilityRow:
    dependency_family: str
    source_persistence_state: str | None
    replay_persistence_state: str | None
    source_memory_score: float | None
    replay_memory_score: float | None
    source_state_age_runs: int | None
    replay_state_age_runs: int | None
    source_latest_persistence_event_type: str | None
    replay_latest_persistence_event_type: str | None
    source_persistence_adjusted_contribution: float | None
    replay_persistence_adjusted_contribution: float | None
    source_persistence_integration_contribution: float | None
    replay_persistence_integration_contribution: float | None
    persistence_adjusted_delta: float | None
    persistence_integration_delta: float | None
    persistence_state_match: bool
    memory_score_match: bool
    state_age_match: bool
    persistence_event_match: bool
    persistence_family_rank_match: bool
    persistence_composite_family_rank_match: bool
    drift_reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PersistenceReplayValidationResult:
    workspace_id: str
    watchlist_id: str
    source_run_id: str
    replay_run_id: str
    source_context_snapshot_id: str | None
    replay_context_snapshot_id: str | None
    source_regime_key: str | None
    replay_regime_key: str | None
    source_dominant_timing_class: str | None
    replay_dominant_timing_class: str | None
    source_dominant_transition_state: str | None
    replay_dominant_transition_state: str | None
    source_dominant_sequence_class: str | None
    replay_dominant_sequence_class: str | None
    source_dominant_archetype_key: str | None
    replay_dominant_archetype_key: str | None
    source_cluster_state: str | None
    replay_cluster_state: str | None
    source_persistence_state: str | None
    replay_persistence_state: str | None
    source_memory_score: float | None
    replay_memory_score: float | None
    source_state_age_runs: int | None
    replay_state_age_runs: int | None
    source_latest_persistence_event_type: str | None
    replay_latest_persistence_event_type: str | None
    context_hash_match: bool
    regime_match: bool
    timing_class_match: bool
    transition_state_match: bool
    sequence_class_match: bool
    archetype_match: bool
    cluster_state_match: bool
    persistence_state_match: bool
    memory_score_match: bool
    state_age_match: bool
    persistence_event_match: bool
    persistence_attribution_match: bool
    persistence_composite_match: bool
    persistence_dominant_family_match: bool
    persistence_delta: dict[str, Any]
    persistence_composite_delta: dict[str, Any]
    drift_reason_codes: list[str]
    validation_state: str
    metadata: dict[str, Any]
    family_rows: list[FamilyPersistenceStabilityRow]


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _num_match(a: float | None, b: float | None, tol: float = _NUMERIC_TOLERANCE) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) <= tol


def _num_delta(a: float | None, b: float | None) -> float | None:
    if a is None and b is None:
        return None
    return (b or 0.0) - (a or 0.0)


class CrossAssetPersistenceReplayValidationService:
    """Deterministic replay comparison across the 4.6A/4.6B/4.6C persistence stack."""

    # ── lineage ─────────────────────────────────────────────────────────
    def load_source_and_replay_runs(
        self, conn, *, replay_run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    id::text                   as replay_run_id,
                    replayed_from_run_id::text as source_run_id,
                    workspace_id::text         as workspace_id,
                    watchlist_id::text         as watchlist_id,
                    is_replay
                from public.job_runs
                where id = %s::uuid
                limit 1
                """,
                (replay_run_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        d = dict(row)
        if not d.get("source_run_id"):
            return None
        return {
            "source_run_id":  d["source_run_id"],
            "replay_run_id":  d["replay_run_id"],
            "workspace_id":   d["workspace_id"],
            "watchlist_id":   d["watchlist_id"],
            "is_replay":      bool(d.get("is_replay")),
        }

    # ── per-run state loading ───────────────────────────────────────────
    def _load_run_state(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,  # noqa: ARG002
    ) -> PersistenceRunSnapshot:
        snap = PersistenceRunSnapshot(
            run_id=run_id, context_snapshot_id=None, context_hash=None,
            regime_key=None, dominant_timing_class=None,
            dominant_transition_state=None, dominant_sequence_class=None,
            dominant_archetype_key=None,
            cluster_state=None,
            persistence_state=None, memory_score=None,
            state_age_runs=None, latest_persistence_event_type=None,
            persistence_adjusted_total=None, persistence_dominant_family=None,
            persistence_net_contribution=None,
            composite_pre_persistence=None, composite_post_persistence=None,
        )

        with conn.cursor() as cur:
            # 4.6C composite summary — primary source for context + persistence
            # state + composite pre/post + persistence_net + memory + age + event.
            cur.execute(
                """
                select
                    context_snapshot_id::text as context_snapshot_id,
                    persistence_adjusted_cross_asset_contribution,
                    composite_pre_persistence,
                    persistence_net_contribution,
                    composite_post_persistence,
                    persistence_state,
                    memory_score,
                    state_age_runs,
                    latest_persistence_event_type
                from public.cross_asset_persistence_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                d = dict(row)
                snap.context_snapshot_id              = d.get("context_snapshot_id")
                snap.persistence_adjusted_total       = _as_float(d.get("persistence_adjusted_cross_asset_contribution"))
                snap.composite_pre_persistence        = _as_float(d.get("composite_pre_persistence"))
                snap.persistence_net_contribution     = _as_float(d.get("persistence_net_contribution"))
                snap.composite_post_persistence       = _as_float(d.get("composite_post_persistence"))
                snap.persistence_state                = d.get("persistence_state")
                snap.memory_score                     = _as_float(d.get("memory_score"))
                snap.state_age_runs                   = d.get("state_age_runs")
                snap.latest_persistence_event_type    = d.get("latest_persistence_event_type")

            # Context hash
            if snap.context_snapshot_id:
                cur.execute(
                    "select context_hash from public.watchlist_context_snapshots where id = %s::uuid",
                    (snap.context_snapshot_id,),
                )
                r = cur.fetchone()
                if r:
                    snap.context_hash = dict(r)["context_hash"]

            # Regime key
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
                snap.regime_key = dict(r).get("regime_key")

            # Dominant timing class from 4.3A diagnostics summary
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
                snap.dominant_timing_class = dict(r).get("dominant_timing_class")

            # Dominant transition state + sequence class from 4.3B summary
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
                snap.dominant_transition_state = d.get("dominant_transition_state")
                snap.dominant_sequence_class   = d.get("dominant_sequence_class")

            # Dominant archetype key from 4.4A run archetype summary
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
                snap.dominant_archetype_key = dict(r).get("dominant_archetype_key")

            # Cluster state from 4.5A archetype-cluster summary
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
                snap.cluster_state = dict(r).get("cluster_state")

            # If composite summary did not provide persistence context, fall
            # back to 4.6A persistence bridge.
            if snap.persistence_state is None:
                cur.execute(
                    """
                    select persistence_state, memory_score, state_age_runs,
                           latest_persistence_event_type
                    from public.run_cross_asset_persistence_summary
                    where run_id = %s::uuid
                    limit 1
                    """,
                    (run_id,),
                )
                r = cur.fetchone()
                if r:
                    d = dict(r)
                    snap.persistence_state             = d.get("persistence_state")
                    if snap.memory_score is None:
                        snap.memory_score              = _as_float(d.get("memory_score"))
                    if snap.state_age_runs is None:
                        snap.state_age_runs            = d.get("state_age_runs")
                    if snap.latest_persistence_event_type is None:
                        snap.latest_persistence_event_type = d.get("latest_persistence_event_type")

            # Persistence dominant family from 4.6B attribution summary
            cur.execute(
                """
                select persistence_dominant_dependency_family
                from public.run_cross_asset_persistence_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                snap.persistence_dominant_family = dict(r).get("persistence_dominant_dependency_family")

            # Per-family 4.6B persistence-adjusted contributions + state +
            # memory + age + event + rank.
            cur.execute(
                """
                select dependency_family,
                       persistence_state,
                       memory_score,
                       state_age_runs,
                       latest_persistence_event_type,
                       persistence_adjusted_family_contribution,
                       persistence_family_rank
                from public.cross_asset_family_persistence_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                snap.family.setdefault(fam, {})
                snap.family[fam]["persistence_state"]                  = d.get("persistence_state")
                snap.family[fam]["memory_score"]                       = _as_float(d.get("memory_score"))
                snap.family[fam]["state_age_runs"]                     = d.get("state_age_runs")
                snap.family[fam]["latest_persistence_event_type"]      = d.get("latest_persistence_event_type")
                snap.family[fam]["persistence_adjusted_contribution"]  = _as_float(d.get("persistence_adjusted_family_contribution"))
                snap.family[fam]["persistence_family_rank"]            = d.get("persistence_family_rank")

            # Per-family 4.6C persistence integration contributions + rank
            cur.execute(
                """
                select dependency_family,
                       persistence_state,
                       memory_score,
                       state_age_runs,
                       latest_persistence_event_type,
                       persistence_integration_contribution,
                       family_rank
                from public.cross_asset_family_persistence_composite_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                snap.family.setdefault(fam, {})
                if snap.family[fam].get("persistence_state") is None:
                    snap.family[fam]["persistence_state"] = d.get("persistence_state")
                if snap.family[fam].get("memory_score") is None:
                    snap.family[fam]["memory_score"] = _as_float(d.get("memory_score"))
                if snap.family[fam].get("state_age_runs") is None:
                    snap.family[fam]["state_age_runs"] = d.get("state_age_runs")
                if snap.family[fam].get("latest_persistence_event_type") is None:
                    snap.family[fam]["latest_persistence_event_type"] = d.get("latest_persistence_event_type")
                snap.family[fam]["persistence_integration_contribution"] = _as_float(d.get("persistence_integration_contribution"))
                snap.family[fam]["persistence_composite_family_rank"]    = d.get("family_rank")

        return snap

    # ── pairwise comparisons ────────────────────────────────────────────
    @staticmethod
    def compare_context_hashes(a: PersistenceRunSnapshot, b: PersistenceRunSnapshot) -> bool:
        if a.context_hash is None and b.context_hash is None:
            return True
        return a.context_hash == b.context_hash

    @staticmethod
    def compare_regime_keys(a: PersistenceRunSnapshot, b: PersistenceRunSnapshot) -> bool:
        if a.regime_key is None and b.regime_key is None:
            return True
        return a.regime_key == b.regime_key

    @staticmethod
    def compare_dominant_timing_classes(a: PersistenceRunSnapshot, b: PersistenceRunSnapshot) -> bool:
        if a.dominant_timing_class is None and b.dominant_timing_class is None:
            return True
        return a.dominant_timing_class == b.dominant_timing_class

    @staticmethod
    def compare_transition_states(a: PersistenceRunSnapshot, b: PersistenceRunSnapshot) -> bool:
        if a.dominant_transition_state is None and b.dominant_transition_state is None:
            return True
        return a.dominant_transition_state == b.dominant_transition_state

    @staticmethod
    def compare_sequence_classes(a: PersistenceRunSnapshot, b: PersistenceRunSnapshot) -> bool:
        if a.dominant_sequence_class is None and b.dominant_sequence_class is None:
            return True
        return a.dominant_sequence_class == b.dominant_sequence_class

    @staticmethod
    def compare_archetype_keys(a: PersistenceRunSnapshot, b: PersistenceRunSnapshot) -> bool:
        if a.dominant_archetype_key is None and b.dominant_archetype_key is None:
            return True
        return a.dominant_archetype_key == b.dominant_archetype_key

    @staticmethod
    def compare_cluster_states(a: PersistenceRunSnapshot, b: PersistenceRunSnapshot) -> bool:
        if a.cluster_state is None and b.cluster_state is None:
            return True
        return a.cluster_state == b.cluster_state

    @staticmethod
    def compare_persistence_states(a: PersistenceRunSnapshot, b: PersistenceRunSnapshot) -> bool:
        if a.persistence_state is None and b.persistence_state is None:
            return True
        return a.persistence_state == b.persistence_state

    @staticmethod
    def compare_memory_scores(a: PersistenceRunSnapshot, b: PersistenceRunSnapshot) -> bool:
        return _num_match(a.memory_score, b.memory_score, tol=_MEMORY_SCORE_TOLERANCE)

    @staticmethod
    def compare_state_age_runs(a: PersistenceRunSnapshot, b: PersistenceRunSnapshot) -> bool:
        if a.state_age_runs is None and b.state_age_runs is None:
            return True
        return a.state_age_runs == b.state_age_runs

    @staticmethod
    def compare_persistence_events(a: PersistenceRunSnapshot, b: PersistenceRunSnapshot) -> bool:
        if a.latest_persistence_event_type is None and b.latest_persistence_event_type is None:
            return True
        return a.latest_persistence_event_type == b.latest_persistence_event_type

    def compare_persistence_attribution(
        self, source: PersistenceRunSnapshot, replay: PersistenceRunSnapshot,
    ) -> tuple[bool, dict[str, Any]]:
        net_match = _num_match(source.persistence_adjusted_total, replay.persistence_adjusted_total)
        per_family: dict[str, Any] = {}
        fam_all_match = True
        all_fams = sorted(set(source.family) | set(replay.family))
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            sv = _as_float(s.get("persistence_adjusted_contribution"))
            rv = _as_float(r.get("persistence_adjusted_contribution"))
            m = _num_match(sv, rv)
            per_family[fam] = {
                "source":      sv,
                "replay":      rv,
                "delta":       _num_delta(sv, rv),
                "source_rank": s.get("persistence_family_rank"),
                "replay_rank": r.get("persistence_family_rank"),
                "rank_match":  (s.get("persistence_family_rank") == r.get("persistence_family_rank")),
                "source_persistence_state": s.get("persistence_state"),
                "replay_persistence_state": r.get("persistence_state"),
                "source_memory_score":      s.get("memory_score"),
                "replay_memory_score":      r.get("memory_score"),
                "source_state_age_runs":    s.get("state_age_runs"),
                "replay_state_age_runs":    r.get("state_age_runs"),
                "source_latest_event":      s.get("latest_persistence_event_type"),
                "replay_latest_event":      r.get("latest_persistence_event_type"),
                "match":       m,
            }
            if not m:
                fam_all_match = False
        return (net_match and fam_all_match, {
            "net": {
                "source": source.persistence_adjusted_total,
                "replay": replay.persistence_adjusted_total,
                "delta":  _num_delta(source.persistence_adjusted_total, replay.persistence_adjusted_total),
                "match":  net_match,
            },
            "families": per_family,
        })

    def compare_persistence_composite(
        self, source: PersistenceRunSnapshot, replay: PersistenceRunSnapshot,
    ) -> tuple[bool, dict[str, Any]]:
        net_match  = _num_match(source.persistence_net_contribution, replay.persistence_net_contribution)
        pre_match  = _num_match(source.composite_pre_persistence, replay.composite_pre_persistence)
        post_match = _num_match(source.composite_post_persistence, replay.composite_post_persistence)
        per_family: dict[str, Any] = {}
        fam_all_match = True
        all_fams = sorted(set(source.family) | set(replay.family))
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            sv = _as_float(s.get("persistence_integration_contribution"))
            rv = _as_float(r.get("persistence_integration_contribution"))
            m = _num_match(sv, rv)
            per_family[fam] = {
                "source":      sv,
                "replay":      rv,
                "delta":       _num_delta(sv, rv),
                "source_rank": s.get("persistence_composite_family_rank"),
                "replay_rank": r.get("persistence_composite_family_rank"),
                "rank_match":  (s.get("persistence_composite_family_rank") == r.get("persistence_composite_family_rank")),
                "match":       m,
            }
            if not m:
                fam_all_match = False
        return (net_match and pre_match and post_match and fam_all_match, {
            "persistence_net": {
                "source": source.persistence_net_contribution,
                "replay": replay.persistence_net_contribution,
                "delta":  _num_delta(source.persistence_net_contribution, replay.persistence_net_contribution),
                "match":  net_match,
            },
            "composite_pre": {
                "source": source.composite_pre_persistence,
                "replay": replay.composite_pre_persistence,
                "delta":  _num_delta(source.composite_pre_persistence, replay.composite_pre_persistence),
                "match":  pre_match,
            },
            "composite_post": {
                "source": source.composite_post_persistence,
                "replay": replay.composite_post_persistence,
                "delta":  _num_delta(source.composite_post_persistence, replay.composite_post_persistence),
                "match":  post_match,
            },
            "families": per_family,
        })

    @staticmethod
    def compare_persistence_dominant_family(
        a: PersistenceRunSnapshot, b: PersistenceRunSnapshot,
    ) -> bool:
        if a.persistence_dominant_family is None and b.persistence_dominant_family is None:
            return True
        return a.persistence_dominant_family == b.persistence_dominant_family

    # ── drift reason code derivation ────────────────────────────────────
    def derive_persistence_drift_reason_codes(
        self,
        *,
        context_match: bool,
        regime_match: bool,
        timing_class_match: bool,
        transition_state_match: bool,
        sequence_class_match: bool,
        archetype_match: bool,
        cluster_state_match: bool,
        persistence_state_match: bool,
        memory_score_match: bool,
        state_age_match: bool,
        persistence_event_match: bool,
        persistence_attribution_match: bool,
        persistence_composite_match: bool,
        persistence_dominant_family_match: bool,
        persistence_attr_delta: dict[str, Any],
        persistence_composite_delta: dict[str, Any],  # noqa: ARG002
        missing_source_layers: list[str],
        missing_replay_layers: list[str],
    ) -> list[str]:
        codes: list[str] = []
        if not context_match:
            codes.append("context_hash_mismatch")
        if not regime_match:
            codes.append("regime_key_mismatch")
        if not timing_class_match:
            codes.append("timing_class_mismatch")
        if not transition_state_match:
            codes.append("transition_state_mismatch")
        if not sequence_class_match:
            codes.append("sequence_class_mismatch")
        if not archetype_match:
            codes.append("archetype_key_mismatch")
        if not cluster_state_match:
            codes.append("cluster_state_mismatch")
        if not persistence_state_match:
            codes.append("persistence_state_mismatch")
        if not memory_score_match:
            codes.append("memory_score_mismatch")
        if not state_age_match:
            codes.append("state_age_mismatch")
        if not persistence_event_match:
            codes.append("persistence_event_mismatch")
        if not persistence_attribution_match:
            for fam, d in (persistence_attr_delta.get("families") or {}).items():
                if not d.get("match"):
                    codes.append(f"persistence_family_delta:{fam}")
                    break
            else:
                codes.append("persistence_family_delta")
        if not persistence_composite_match:
            codes.append("persistence_integration_delta")
        if not persistence_dominant_family_match:
            codes.append("persistence_dominant_family_shift")
        for layer in missing_source_layers:
            codes.append(f"missing_source_persistence_layer:{layer}")
        for layer in missing_replay_layers:
            codes.append(f"missing_replay_persistence_layer:{layer}")
        seen: set[str] = set()
        out: list[str] = []
        for c in codes:
            if c not in seen:
                seen.add(c)
                out.append(c)
        return out

    # ── family stability rows ───────────────────────────────────────────
    def compute_family_persistence_stability(
        self, source: PersistenceRunSnapshot, replay: PersistenceRunSnapshot,
    ) -> list[FamilyPersistenceStabilityRow]:
        all_fams = sorted(set(source.family) | set(replay.family))
        rows: list[FamilyPersistenceStabilityRow] = []
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            s_state  = s.get("persistence_state")
            r_state  = r.get("persistence_state")
            s_mem    = _as_float(s.get("memory_score"))
            r_mem    = _as_float(r.get("memory_score"))
            s_age    = s.get("state_age_runs")
            r_age    = r.get("state_age_runs")
            s_event  = s.get("latest_persistence_event_type")
            r_event  = r.get("latest_persistence_event_type")
            s_attr   = _as_float(s.get("persistence_adjusted_contribution"))
            r_attr   = _as_float(r.get("persistence_adjusted_contribution"))
            s_int    = _as_float(s.get("persistence_integration_contribution"))
            r_int    = _as_float(r.get("persistence_integration_contribution"))
            s_rank   = s.get("persistence_family_rank")
            r_rank   = r.get("persistence_family_rank")
            s_crank  = s.get("persistence_composite_family_rank")
            r_crank  = r.get("persistence_composite_family_rank")

            mem_match = _num_match(s_mem, r_mem, tol=_MEMORY_SCORE_TOLERANCE)
            age_match = (s_age == r_age)
            evt_match = (s_event == r_event)

            codes: list[str] = []
            if not _num_match(s_attr, r_attr):
                codes.append("persistence_family_delta")
            if not _num_match(s_int, r_int):
                codes.append("persistence_integration_delta")
            if s_state != r_state:
                codes.append("persistence_state_mismatch")
            if not mem_match:
                codes.append("memory_score_mismatch")
            if not age_match:
                codes.append("state_age_mismatch")
            if not evt_match:
                codes.append("persistence_event_mismatch")
            if s_rank != r_rank:
                codes.append("persistence_family_rank_shift")
            if s_crank != r_crank:
                codes.append("persistence_composite_family_rank_shift")

            rows.append(FamilyPersistenceStabilityRow(
                dependency_family=fam,
                source_persistence_state=s_state,
                replay_persistence_state=r_state,
                source_memory_score=s_mem,
                replay_memory_score=r_mem,
                source_state_age_runs=s_age,
                replay_state_age_runs=r_age,
                source_latest_persistence_event_type=s_event,
                replay_latest_persistence_event_type=r_event,
                source_persistence_adjusted_contribution=s_attr,
                replay_persistence_adjusted_contribution=r_attr,
                source_persistence_integration_contribution=s_int,
                replay_persistence_integration_contribution=r_int,
                persistence_adjusted_delta=_num_delta(s_attr, r_attr),
                persistence_integration_delta=_num_delta(s_int, r_int),
                persistence_state_match=(s_state == r_state),
                memory_score_match=mem_match,
                state_age_match=age_match,
                persistence_event_match=evt_match,
                persistence_family_rank_match=(s_rank == r_rank),
                persistence_composite_family_rank_match=(s_crank == r_crank),
                drift_reason_codes=codes,
                metadata={
                    "source_persistence_family_rank":      s_rank,
                    "replay_persistence_family_rank":      r_rank,
                    "source_persistence_composite_rank":   s_crank,
                    "replay_persistence_composite_rank":   r_crank,
                    "memory_score_tolerance":              _MEMORY_SCORE_TOLERANCE,
                },
            ))
        return rows

    # ── main builder ────────────────────────────────────────────────────
    def build_replay_validation_for_run(
        self,
        conn,
        *,
        replay_run_id: str,
    ) -> PersistenceReplayValidationResult | None:
        lineage = self.load_source_and_replay_runs(conn, replay_run_id=replay_run_id)
        if not lineage:
            return None

        source = self._load_run_state(
            conn,
            workspace_id=lineage["workspace_id"],
            watchlist_id=lineage["watchlist_id"],
            run_id=lineage["source_run_id"],
        )
        replay = self._load_run_state(
            conn,
            workspace_id=lineage["workspace_id"],
            watchlist_id=lineage["watchlist_id"],
            run_id=lineage["replay_run_id"],
        )

        missing_source_layers: list[str] = []
        missing_replay_layers: list[str] = []
        if source.persistence_adjusted_total is None and not source.family:
            missing_source_layers.append("attribution")
        if source.persistence_net_contribution is None and source.composite_post_persistence is None:
            missing_source_layers.append("composite")
        if replay.persistence_adjusted_total is None and not replay.family:
            missing_replay_layers.append("attribution")
        if replay.persistence_net_contribution is None and replay.composite_post_persistence is None:
            missing_replay_layers.append("composite")

        context_match            = self.compare_context_hashes(source, replay)
        regime_match             = self.compare_regime_keys(source, replay)
        timing_class_match       = self.compare_dominant_timing_classes(source, replay)
        transition_state_match   = self.compare_transition_states(source, replay)
        sequence_class_match     = self.compare_sequence_classes(source, replay)
        archetype_match          = self.compare_archetype_keys(source, replay)
        cluster_state_match      = self.compare_cluster_states(source, replay)
        persistence_state_match  = self.compare_persistence_states(source, replay)
        memory_score_match       = self.compare_memory_scores(source, replay)
        state_age_match          = self.compare_state_age_runs(source, replay)
        persistence_event_match  = self.compare_persistence_events(source, replay)
        attr_match, attr_delta         = self.compare_persistence_attribution(source, replay)
        comp_match, composite_delta    = self.compare_persistence_composite(source, replay)
        dominant_family_match          = self.compare_persistence_dominant_family(source, replay)

        drift_codes = self.derive_persistence_drift_reason_codes(
            context_match=context_match,
            regime_match=regime_match,
            timing_class_match=timing_class_match,
            transition_state_match=transition_state_match,
            sequence_class_match=sequence_class_match,
            archetype_match=archetype_match,
            cluster_state_match=cluster_state_match,
            persistence_state_match=persistence_state_match,
            memory_score_match=memory_score_match,
            state_age_match=state_age_match,
            persistence_event_match=persistence_event_match,
            persistence_attribution_match=attr_match,
            persistence_composite_match=comp_match,
            persistence_dominant_family_match=dominant_family_match,
            persistence_attr_delta=attr_delta,
            persistence_composite_delta=composite_delta,
            missing_source_layers=missing_source_layers,
            missing_replay_layers=missing_replay_layers,
        )

        # Validation state priority:
        # insufficient_source/replay → context_mismatch → timing_mismatch →
        # transition_mismatch → archetype_mismatch → cluster_mismatch →
        # persistence_mismatch → drift_detected → validated
        if missing_source_layers == ["attribution", "composite"]:
            validation_state = "insufficient_source"
        elif missing_replay_layers == ["attribution", "composite"]:
            validation_state = "insufficient_replay"
        elif not context_match and not any(
            c for c in drift_codes
            if c.startswith((
                "persistence_family_delta", "persistence_integration_delta",
                "persistence_dominant_family_shift",
                "persistence_state_mismatch",
                "memory_score_mismatch", "state_age_mismatch",
                "persistence_event_mismatch",
                "cluster_state_mismatch", "archetype_key_mismatch",
                "transition_state_mismatch", "sequence_class_mismatch",
                "timing_class_mismatch", "regime_key_mismatch",
            ))
        ):
            validation_state = "context_mismatch"
        elif not timing_class_match and context_match and regime_match and \
             transition_state_match and sequence_class_match and \
             archetype_match and cluster_state_match and \
             persistence_state_match and memory_score_match and \
             state_age_match and persistence_event_match and \
             attr_match and comp_match:
            validation_state = "timing_mismatch"
        elif (not transition_state_match or not sequence_class_match) and \
             context_match and regime_match and timing_class_match and \
             archetype_match and cluster_state_match and \
             persistence_state_match and memory_score_match and \
             state_age_match and persistence_event_match and \
             attr_match and comp_match:
            validation_state = "transition_mismatch"
        elif (not archetype_match) and \
             context_match and regime_match and timing_class_match and \
             transition_state_match and sequence_class_match and \
             cluster_state_match and \
             persistence_state_match and memory_score_match and \
             state_age_match and persistence_event_match and \
             attr_match and comp_match:
            validation_state = "archetype_mismatch"
        elif (not cluster_state_match) and \
             context_match and regime_match and timing_class_match and \
             transition_state_match and sequence_class_match and \
             archetype_match and \
             persistence_state_match and memory_score_match and \
             state_age_match and persistence_event_match and \
             attr_match and comp_match:
            validation_state = "cluster_mismatch"
        elif (not persistence_state_match or not memory_score_match or
              not state_age_match or not persistence_event_match) and \
             context_match and regime_match and timing_class_match and \
             transition_state_match and sequence_class_match and \
             archetype_match and cluster_state_match and \
             attr_match and comp_match:
            validation_state = "persistence_mismatch"
        elif drift_codes:
            validation_state = "drift_detected"
        else:
            validation_state = "validated"

        family_rows = self.compute_family_persistence_stability(source, replay)

        metadata: dict[str, Any] = {
            "scoring_version":           _SCORING_VERSION,
            "numeric_tolerance":         _NUMERIC_TOLERANCE,
            "memory_score_tolerance":    _MEMORY_SCORE_TOLERANCE,
            "missing_source_layers":     missing_source_layers,
            "missing_replay_layers":     missing_replay_layers,
            "is_replay":                 lineage.get("is_replay"),
        }

        return PersistenceReplayValidationResult(
            workspace_id=lineage["workspace_id"],
            watchlist_id=lineage["watchlist_id"],
            source_run_id=lineage["source_run_id"],
            replay_run_id=lineage["replay_run_id"],
            source_context_snapshot_id=source.context_snapshot_id,
            replay_context_snapshot_id=replay.context_snapshot_id,
            source_regime_key=source.regime_key,
            replay_regime_key=replay.regime_key,
            source_dominant_timing_class=source.dominant_timing_class,
            replay_dominant_timing_class=replay.dominant_timing_class,
            source_dominant_transition_state=source.dominant_transition_state,
            replay_dominant_transition_state=replay.dominant_transition_state,
            source_dominant_sequence_class=source.dominant_sequence_class,
            replay_dominant_sequence_class=replay.dominant_sequence_class,
            source_dominant_archetype_key=source.dominant_archetype_key,
            replay_dominant_archetype_key=replay.dominant_archetype_key,
            source_cluster_state=source.cluster_state,
            replay_cluster_state=replay.cluster_state,
            source_persistence_state=source.persistence_state,
            replay_persistence_state=replay.persistence_state,
            source_memory_score=source.memory_score,
            replay_memory_score=replay.memory_score,
            source_state_age_runs=source.state_age_runs,
            replay_state_age_runs=replay.state_age_runs,
            source_latest_persistence_event_type=source.latest_persistence_event_type,
            replay_latest_persistence_event_type=replay.latest_persistence_event_type,
            context_hash_match=context_match,
            regime_match=regime_match,
            timing_class_match=timing_class_match,
            transition_state_match=transition_state_match,
            sequence_class_match=sequence_class_match,
            archetype_match=archetype_match,
            cluster_state_match=cluster_state_match,
            persistence_state_match=persistence_state_match,
            memory_score_match=memory_score_match,
            state_age_match=state_age_match,
            persistence_event_match=persistence_event_match,
            persistence_attribution_match=attr_match,
            persistence_composite_match=comp_match,
            persistence_dominant_family_match=dominant_family_match,
            persistence_delta=attr_delta,
            persistence_composite_delta=composite_delta,
            drift_reason_codes=drift_codes,
            validation_state=validation_state,
            metadata=metadata,
            family_rows=family_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_persistence_replay_validation(
        self, conn, *, result: PersistenceReplayValidationResult,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_persistence_replay_validation_snapshot(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            source_run_id=result.source_run_id,
            replay_run_id=result.replay_run_id,
            source_context_snapshot_id=result.source_context_snapshot_id,
            replay_context_snapshot_id=result.replay_context_snapshot_id,
            source_regime_key=result.source_regime_key,
            replay_regime_key=result.replay_regime_key,
            source_dominant_timing_class=result.source_dominant_timing_class,
            replay_dominant_timing_class=result.replay_dominant_timing_class,
            source_dominant_transition_state=result.source_dominant_transition_state,
            replay_dominant_transition_state=result.replay_dominant_transition_state,
            source_dominant_sequence_class=result.source_dominant_sequence_class,
            replay_dominant_sequence_class=result.replay_dominant_sequence_class,
            source_dominant_archetype_key=result.source_dominant_archetype_key,
            replay_dominant_archetype_key=result.replay_dominant_archetype_key,
            source_cluster_state=result.source_cluster_state,
            replay_cluster_state=result.replay_cluster_state,
            source_persistence_state=result.source_persistence_state,
            replay_persistence_state=result.replay_persistence_state,
            source_memory_score=result.source_memory_score,
            replay_memory_score=result.replay_memory_score,
            source_state_age_runs=result.source_state_age_runs,
            replay_state_age_runs=result.replay_state_age_runs,
            source_latest_persistence_event_type=result.source_latest_persistence_event_type,
            replay_latest_persistence_event_type=result.replay_latest_persistence_event_type,
            context_hash_match=result.context_hash_match,
            regime_match=result.regime_match,
            timing_class_match=result.timing_class_match,
            transition_state_match=result.transition_state_match,
            sequence_class_match=result.sequence_class_match,
            archetype_match=result.archetype_match,
            cluster_state_match=result.cluster_state_match,
            persistence_state_match=result.persistence_state_match,
            memory_score_match=result.memory_score_match,
            state_age_match=result.state_age_match,
            persistence_event_match=result.persistence_event_match,
            persistence_attribution_match=result.persistence_attribution_match,
            persistence_composite_match=result.persistence_composite_match,
            persistence_dominant_family_match=result.persistence_dominant_family_match,
            persistence_delta=result.persistence_delta,
            persistence_composite_delta=result.persistence_composite_delta,
            drift_reason_codes=result.drift_reason_codes,
            validation_state=result.validation_state,
            metadata=result.metadata,
        )
        return str(row["id"])

    def persist_family_persistence_stability(
        self, conn, *, result: PersistenceReplayValidationResult,
    ) -> int:
        if not result.family_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_persistence_replay_stability_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            source_run_id=result.source_run_id,
            replay_run_id=result.replay_run_id,
            rows=[
                {
                    "dependency_family":                                fr.dependency_family,
                    "source_persistence_state":                         fr.source_persistence_state,
                    "replay_persistence_state":                         fr.replay_persistence_state,
                    "source_memory_score":                              fr.source_memory_score,
                    "replay_memory_score":                              fr.replay_memory_score,
                    "source_state_age_runs":                            fr.source_state_age_runs,
                    "replay_state_age_runs":                            fr.replay_state_age_runs,
                    "source_latest_persistence_event_type":             fr.source_latest_persistence_event_type,
                    "replay_latest_persistence_event_type":             fr.replay_latest_persistence_event_type,
                    "source_persistence_adjusted_contribution":         fr.source_persistence_adjusted_contribution,
                    "replay_persistence_adjusted_contribution":         fr.replay_persistence_adjusted_contribution,
                    "source_persistence_integration_contribution":      fr.source_persistence_integration_contribution,
                    "replay_persistence_integration_contribution":      fr.replay_persistence_integration_contribution,
                    "persistence_adjusted_delta":                       fr.persistence_adjusted_delta,
                    "persistence_integration_delta":                    fr.persistence_integration_delta,
                    "persistence_state_match":                          fr.persistence_state_match,
                    "memory_score_match":                               fr.memory_score_match,
                    "state_age_match":                                  fr.state_age_match,
                    "persistence_event_match":                          fr.persistence_event_match,
                    "persistence_family_rank_match":                    fr.persistence_family_rank_match,
                    "persistence_composite_family_rank_match":          fr.persistence_composite_family_rank_match,
                    "drift_reason_codes":                               fr.drift_reason_codes,
                    "metadata":                                         fr.metadata,
                }
                for fr in result.family_rows
            ],
        )

    def build_and_persist(
        self, conn, *, replay_run_id: str,
    ) -> PersistenceReplayValidationResult | None:
        result = self.build_replay_validation_for_run(
            conn, replay_run_id=replay_run_id,
        )
        if result is None:
            return None
        self.persist_persistence_replay_validation(conn, result=result)
        self.persist_family_persistence_stability(conn, result=result)
        return result

    def refresh_persistence_replay_validation_for_run(
        self, conn, *, replay_run_id: str,
    ) -> PersistenceReplayValidationResult | None:
        """Consumer entrypoint. Only acts when the run is a replay with a
        resolvable source. Commits on success."""
        try:
            result = self.build_and_persist(conn, replay_run_id=replay_run_id)
            if result is not None:
                conn.commit()
            return result
        except Exception as exc:
            logger.warning(
                "cross_asset_persistence_replay_validation: run=%s build/persist failed: %s",
                replay_run_id, exc,
            )
            conn.rollback()
            return None
