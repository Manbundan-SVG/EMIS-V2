"""Phase 4.7D: Replay Validation for Decay-Aware Composite Behavior.

Compares a source run's decay-aware attribution + composite to its replay
counterpart and persists explicit drift diagnostics. Mirrors the 4.6D
persistence replay-validation pattern but extends comparisons to freshness
state, aggregate decay score, stale-memory flag, contradiction flag,
decay-aware attribution, decay-aware composite, and decay dominant family.

Persists:
  * cross_asset_decay_replay_validation_snapshots
  * cross_asset_family_decay_replay_stability_snapshots

All comparison logic is deterministic, explicit, and metadata-stamped.
Numeric tolerances are stamped on every row for audit. No predictive
behavior in this phase.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.7D.v1"

# Explicit numeric tolerances.
_CONTRIBUTION_TOLERANCE = 1e-9     # decay-adjusted / decay integration contributions
_MEMORY_SCORE_TOLERANCE = 1e-6     # memory_score
_DECAY_SCORE_TOLERANCE  = 1e-6     # aggregate_decay_score / family_decay_score


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _as_bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "t", "1", "yes"):
            return True
        if s in ("false", "f", "0", "no"):
            return False
    return None


def _eq_str(a: Any, b: Any) -> bool:
    aa = a if a is None else str(a)
    bb = b if b is None else str(b)
    return aa == bb


def _eq_num(a: Any, b: Any, tol: float) -> bool:
    af = _as_float(a)
    bf = _as_float(b)
    if af is None and bf is None:
        return True
    if af is None or bf is None:
        return False
    return abs(af - bf) <= tol


def _eq_bool(a: Any, b: Any) -> bool:
    aa = _as_bool(a)
    bb = _as_bool(b)
    return aa == bb


@dataclass
class DecayReplayValidationSnapshot:
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
    source_freshness_state: str | None
    replay_freshness_state: str | None
    source_aggregate_decay_score: float | None
    replay_aggregate_decay_score: float | None
    source_stale_memory_flag: bool | None
    replay_stale_memory_flag: bool | None
    source_contradiction_flag: bool | None
    replay_contradiction_flag: bool | None
    context_hash_match: bool
    regime_match: bool
    timing_class_match: bool
    transition_state_match: bool
    sequence_class_match: bool
    archetype_match: bool
    cluster_state_match: bool
    persistence_state_match: bool
    memory_score_match: bool
    freshness_state_match: bool
    aggregate_decay_score_match: bool
    stale_memory_flag_match: bool
    contradiction_flag_match: bool
    decay_attribution_match: bool
    decay_composite_match: bool
    decay_dominant_family_match: bool
    decay_delta: dict[str, Any]
    decay_composite_delta: dict[str, Any]
    drift_reason_codes: list[str]
    validation_state: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FamilyDecayReplayStabilitySnapshot:
    workspace_id: str
    watchlist_id: str
    source_run_id: str
    replay_run_id: str
    dependency_family: str
    source_freshness_state: str | None
    replay_freshness_state: str | None
    source_aggregate_decay_score: float | None
    replay_aggregate_decay_score: float | None
    source_family_decay_score: float | None
    replay_family_decay_score: float | None
    source_stale_memory_flag: bool | None
    replay_stale_memory_flag: bool | None
    source_contradiction_flag: bool | None
    replay_contradiction_flag: bool | None
    source_decay_adjusted_contribution: float | None
    replay_decay_adjusted_contribution: float | None
    source_decay_integration_contribution: float | None
    replay_decay_integration_contribution: float | None
    decay_adjusted_delta: float | None
    decay_integration_delta: float | None
    freshness_state_match: bool
    aggregate_decay_score_match: bool
    family_decay_score_match: bool
    stale_memory_flag_match: bool
    contradiction_flag_match: bool
    decay_family_rank_match: bool
    decay_composite_family_rank_match: bool
    drift_reason_codes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)


class CrossAssetDecayReplayValidationService:
    """Deterministic decay-layer replay validation."""

    # ── lineage ─────────────────────────────────────────────────────────
    def load_source_and_replay_runs(
        self, conn, *, replay_run_id: str,
    ) -> tuple[str | None, str | None, str | None, str | None]:
        """Return ``(source_run_id, source_workspace_id, replay_workspace_id,
        watchlist_id)`` if the run is a replay, else (None, None, None, None)."""
        with conn.cursor() as cur:
            cur.execute(
                """
                select replayed_from_run_id::text as source_run_id,
                       workspace_id::text         as workspace_id,
                       watchlist_id::text         as watchlist_id
                from public.job_runs
                where id = %s::uuid
                  and replayed_from_run_id is not null
                limit 1
                """,
                (replay_run_id,),
            )
            row = cur.fetchone()
            if not row:
                return (None, None, None, None)
            d = dict(row)
            return (
                d.get("source_run_id"),
                d.get("workspace_id"),
                d.get("workspace_id"),
                d.get("watchlist_id"),
            )

    # ── per-run context loaders (decay attribution + decay composite) ───
    def _load_decay_state_for_run(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text as run_id,
                       freshness_state, aggregate_decay_score,
                       memory_score, persistence_state,
                       stale_memory_flag, contradiction_flag,
                       latest_stale_memory_event_type
                from public.run_cross_asset_signal_decay_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_decay_attribution_for_run(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text as run_id,
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

    def _load_decay_composite_for_run(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select run_id::text as run_id,
                       context_snapshot_id::text as context_snapshot_id,
                       decay_adjusted_cross_asset_contribution,
                       composite_pre_decay,
                       decay_net_contribution,
                       composite_post_decay,
                       freshness_state, aggregate_decay_score,
                       stale_memory_flag, contradiction_flag
                from public.cross_asset_decay_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_run_persistence_state(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select persistence_state, memory_score, regime_key,
                       cluster_state, dominant_archetype_key
                from public.run_cross_asset_persistence_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_run_transition_diagnostics(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dominant_timing_class, dominant_transition_state,
                       dominant_sequence_class
                from public.run_cross_asset_transition_diagnostics_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_run_pattern_cluster(
        self, conn, *, run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dominant_archetype_key, cluster_state
                from public.run_cross_asset_pattern_cluster_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def _load_family_decay_attribution(
        self, conn, *, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       freshness_state, aggregate_decay_score, family_decay_score,
                       stale_memory_flag, contradiction_flag,
                       decay_adjusted_family_contribution, decay_family_rank
                from public.cross_asset_family_decay_attribution_summary
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

    def _load_family_decay_composite(
        self, conn, *, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       decay_integration_contribution, family_rank
                from public.cross_asset_family_decay_composite_summary
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

    # ── primitive comparators ───────────────────────────────────────────
    @staticmethod
    def compare_context_hashes(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_regime_keys(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_dominant_timing_classes(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_transition_states(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_sequence_classes(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_archetype_keys(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_cluster_states(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_persistence_states(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_memory_scores(src: Any, rep: Any) -> bool:
        return _eq_num(src, rep, _MEMORY_SCORE_TOLERANCE)

    @staticmethod
    def compare_freshness_states(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_aggregate_decay_scores(src: Any, rep: Any) -> bool:
        return _eq_num(src, rep, _DECAY_SCORE_TOLERANCE)

    @staticmethod
    def compare_stale_memory_flags(src: Any, rep: Any) -> bool:
        return _eq_bool(src, rep)

    @staticmethod
    def compare_contradiction_flags(src: Any, rep: Any) -> bool:
        return _eq_bool(src, rep)

    @staticmethod
    def compare_decay_attribution(src: dict | None, rep: dict | None) -> bool:
        if src is None and rep is None:
            return True
        if src is None or rep is None:
            return False
        return _eq_num(
            src.get("decay_adjusted_cross_asset_contribution"),
            rep.get("decay_adjusted_cross_asset_contribution"),
            _CONTRIBUTION_TOLERANCE,
        )

    @staticmethod
    def compare_decay_composite(src: dict | None, rep: dict | None) -> bool:
        if src is None and rep is None:
            return True
        if src is None or rep is None:
            return False
        return (
            _eq_num(src.get("decay_net_contribution"),
                    rep.get("decay_net_contribution"),
                    _CONTRIBUTION_TOLERANCE)
            and _eq_num(src.get("composite_pre_decay"),
                        rep.get("composite_pre_decay"),
                        _CONTRIBUTION_TOLERANCE)
            and _eq_num(src.get("composite_post_decay"),
                        rep.get("composite_post_decay"),
                        _CONTRIBUTION_TOLERANCE)
        )

    @staticmethod
    def compare_decay_dominant_family(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    # ── family-level stability ──────────────────────────────────────────
    def compute_family_decay_stability(
        self,
        *,
        workspace_id: str,
        watchlist_id: str,
        source_run_id: str,
        replay_run_id: str,
        source_attr: dict[str, dict[str, Any]],
        replay_attr: dict[str, dict[str, Any]],
        source_comp: dict[str, dict[str, Any]],
        replay_comp: dict[str, dict[str, Any]],
    ) -> list[FamilyDecayReplayStabilitySnapshot]:
        families = sorted(
            set(source_attr.keys()) | set(replay_attr.keys())
            | set(source_comp.keys()) | set(replay_comp.keys())
        )
        out: list[FamilyDecayReplayStabilitySnapshot] = []
        for fam in families:
            sa = source_attr.get(fam) or {}
            ra = replay_attr.get(fam) or {}
            sc = source_comp.get(fam) or {}
            rc = replay_comp.get(fam) or {}

            src_decay_adj = _as_float(sa.get("decay_adjusted_family_contribution"))
            rep_decay_adj = _as_float(ra.get("decay_adjusted_family_contribution"))
            src_decay_int = _as_float(sc.get("decay_integration_contribution"))
            rep_decay_int = _as_float(rc.get("decay_integration_contribution"))

            adj_delta = (
                None if src_decay_adj is None or rep_decay_adj is None
                else rep_decay_adj - src_decay_adj
            )
            int_delta = (
                None if src_decay_int is None or rep_decay_int is None
                else rep_decay_int - src_decay_int
            )

            freshness_match     = _eq_str(sa.get("freshness_state"),     ra.get("freshness_state"))
            aggregate_match     = _eq_num(sa.get("aggregate_decay_score"), ra.get("aggregate_decay_score"), _DECAY_SCORE_TOLERANCE)
            family_decay_match  = _eq_num(sa.get("family_decay_score"),  ra.get("family_decay_score"),  _DECAY_SCORE_TOLERANCE)
            stale_match         = _eq_bool(sa.get("stale_memory_flag"),  ra.get("stale_memory_flag"))
            contradiction_match = _eq_bool(sa.get("contradiction_flag"), ra.get("contradiction_flag"))
            attr_rank_match     = (sa.get("decay_family_rank") == ra.get("decay_family_rank"))
            comp_rank_match     = (sc.get("family_rank")       == rc.get("family_rank"))

            reason_codes: list[str] = []
            if not freshness_match:
                reason_codes.append("freshness_state_mismatch")
            if not aggregate_match:
                reason_codes.append("aggregate_decay_score_mismatch")
            if not family_decay_match:
                reason_codes.append("family_decay_score_mismatch")
            if not stale_match:
                reason_codes.append("stale_memory_flag_mismatch")
            if not contradiction_match:
                reason_codes.append("contradiction_flag_mismatch")
            if adj_delta is not None and abs(adj_delta) > _CONTRIBUTION_TOLERANCE:
                reason_codes.append("decay_family_delta")
            if int_delta is not None and abs(int_delta) > _CONTRIBUTION_TOLERANCE:
                reason_codes.append("decay_integration_delta")
            if not attr_rank_match:
                reason_codes.append("decay_family_rank_mismatch")
            if not comp_rank_match:
                reason_codes.append("decay_composite_family_rank_mismatch")
            if not sa and ra:
                reason_codes.append("missing_source_decay_layer:attribution")
            if sa and not ra:
                reason_codes.append("missing_replay_decay_layer:attribution")
            if not sc and rc:
                reason_codes.append("missing_source_decay_layer:composite")
            if sc and not rc:
                reason_codes.append("missing_replay_decay_layer:composite")

            out.append(FamilyDecayReplayStabilitySnapshot(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                source_run_id=source_run_id,
                replay_run_id=replay_run_id,
                dependency_family=fam,
                source_freshness_state=sa.get("freshness_state"),
                replay_freshness_state=ra.get("freshness_state"),
                source_aggregate_decay_score=_as_float(sa.get("aggregate_decay_score")),
                replay_aggregate_decay_score=_as_float(ra.get("aggregate_decay_score")),
                source_family_decay_score=_as_float(sa.get("family_decay_score")),
                replay_family_decay_score=_as_float(ra.get("family_decay_score")),
                source_stale_memory_flag=_as_bool(sa.get("stale_memory_flag")),
                replay_stale_memory_flag=_as_bool(ra.get("stale_memory_flag")),
                source_contradiction_flag=_as_bool(sa.get("contradiction_flag")),
                replay_contradiction_flag=_as_bool(ra.get("contradiction_flag")),
                source_decay_adjusted_contribution=src_decay_adj,
                replay_decay_adjusted_contribution=rep_decay_adj,
                source_decay_integration_contribution=src_decay_int,
                replay_decay_integration_contribution=rep_decay_int,
                decay_adjusted_delta=adj_delta,
                decay_integration_delta=int_delta,
                freshness_state_match=freshness_match,
                aggregate_decay_score_match=aggregate_match,
                family_decay_score_match=family_decay_match,
                stale_memory_flag_match=stale_match,
                contradiction_flag_match=contradiction_match,
                decay_family_rank_match=attr_rank_match,
                decay_composite_family_rank_match=comp_rank_match,
                drift_reason_codes=list(dict.fromkeys(reason_codes)),
                metadata={
                    "scoring_version":            _SCORING_VERSION,
                    "contribution_tolerance":     _CONTRIBUTION_TOLERANCE,
                    "decay_score_tolerance":      _DECAY_SCORE_TOLERANCE,
                },
            ))
        return out

    # ── drift reason aggregation ────────────────────────────────────────
    @staticmethod
    def derive_decay_drift_reason_codes(
        *,
        context_match: bool,
        regime_match: bool,
        timing_match: bool,
        transition_match: bool,
        sequence_match: bool,
        archetype_match: bool,
        cluster_match: bool,
        persistence_match: bool,
        memory_match: bool,
        freshness_match: bool,
        aggregate_decay_match: bool,
        stale_match: bool,
        contradiction_match: bool,
        decay_attr_match: bool,
        decay_comp_match: bool,
        decay_dom_match: bool,
        source_decay_attr: dict | None,
        replay_decay_attr: dict | None,
        source_decay_comp: dict | None,
        replay_decay_comp: dict | None,
    ) -> list[str]:
        codes: list[str] = []
        if not context_match:
            codes.append("context_hash_mismatch")
        if not regime_match:
            codes.append("regime_key_mismatch")
        if not timing_match:
            codes.append("timing_class_mismatch")
        if not transition_match:
            codes.append("transition_state_mismatch")
        if not sequence_match:
            codes.append("sequence_class_mismatch")
        if not archetype_match:
            codes.append("archetype_key_mismatch")
        if not cluster_match:
            codes.append("cluster_state_mismatch")
        if not persistence_match:
            codes.append("persistence_state_mismatch")
        if not memory_match:
            codes.append("memory_score_mismatch")
        if not freshness_match:
            codes.append("freshness_state_mismatch")
        if not aggregate_decay_match:
            codes.append("aggregate_decay_score_mismatch")
        if not stale_match:
            codes.append("stale_memory_flag_mismatch")
        if not contradiction_match:
            codes.append("contradiction_flag_mismatch")
        if not decay_attr_match:
            codes.append("decay_family_delta")
        if not decay_comp_match:
            codes.append("decay_integration_delta")
        if not decay_dom_match:
            codes.append("decay_dominant_family_shift")
        if source_decay_attr is None and replay_decay_attr is not None:
            codes.append("missing_source_decay_layer:attribution")
        if replay_decay_attr is None and source_decay_attr is not None:
            codes.append("missing_replay_decay_layer:attribution")
        if source_decay_comp is None and replay_decay_comp is not None:
            codes.append("missing_source_decay_layer:composite")
        if replay_decay_comp is None and source_decay_comp is not None:
            codes.append("missing_replay_decay_layer:composite")
        return list(dict.fromkeys(codes))

    @staticmethod
    def _classify_validation_state(
        *,
        source_decay_attr: dict | None,
        replay_decay_attr: dict | None,
        source_decay_comp: dict | None,
        replay_decay_comp: dict | None,
        context_match: bool,
        regime_match: bool,
        timing_match: bool,
        transition_match: bool,
        sequence_match: bool,
        archetype_match: bool,
        cluster_match: bool,
        persistence_match: bool,
        memory_match: bool,
        freshness_match: bool,
        aggregate_decay_match: bool,
        stale_match: bool,
        contradiction_match: bool,
        decay_attr_match: bool,
        decay_comp_match: bool,
        decay_dom_match: bool,
    ) -> str:
        if source_decay_attr is None and source_decay_comp is None:
            return "insufficient_source"
        if replay_decay_attr is None and replay_decay_comp is None:
            return "insufficient_replay"
        if not context_match:
            return "context_mismatch"
        if not (timing_match):
            # Only flag timing_mismatch when broader upstream context already matched.
            if regime_match:
                return "timing_mismatch"
        if not transition_match or not sequence_match:
            if regime_match and timing_match:
                return "transition_mismatch"
        if not archetype_match:
            if regime_match and timing_match and transition_match and sequence_match:
                return "archetype_mismatch"
        if not cluster_match:
            if (regime_match and timing_match and transition_match
                    and sequence_match and archetype_match):
                return "cluster_mismatch"
        if not persistence_match or not memory_match:
            if (regime_match and timing_match and transition_match
                    and sequence_match and archetype_match and cluster_match):
                return "persistence_mismatch"
        if not (freshness_match and aggregate_decay_match
                and stale_match and contradiction_match):
            if (regime_match and timing_match and transition_match
                    and sequence_match and archetype_match and cluster_match
                    and persistence_match and memory_match):
                return "decay_mismatch"
        if decay_attr_match and decay_comp_match and decay_dom_match:
            return "validated"
        return "drift_detected"

    # ── persistence ─────────────────────────────────────────────────────
    def persist_decay_replay_validation(
        self, conn, *, snap: DecayReplayValidationSnapshot,
    ) -> str:
        import src.db.repositories_47d as repo
        row = repo.insert_cross_asset_decay_replay_validation_snapshot(
            conn,
            workspace_id=snap.workspace_id,
            watchlist_id=snap.watchlist_id,
            source_run_id=snap.source_run_id,
            replay_run_id=snap.replay_run_id,
            source_context_snapshot_id=snap.source_context_snapshot_id,
            replay_context_snapshot_id=snap.replay_context_snapshot_id,
            source_regime_key=snap.source_regime_key,
            replay_regime_key=snap.replay_regime_key,
            source_dominant_timing_class=snap.source_dominant_timing_class,
            replay_dominant_timing_class=snap.replay_dominant_timing_class,
            source_dominant_transition_state=snap.source_dominant_transition_state,
            replay_dominant_transition_state=snap.replay_dominant_transition_state,
            source_dominant_sequence_class=snap.source_dominant_sequence_class,
            replay_dominant_sequence_class=snap.replay_dominant_sequence_class,
            source_dominant_archetype_key=snap.source_dominant_archetype_key,
            replay_dominant_archetype_key=snap.replay_dominant_archetype_key,
            source_cluster_state=snap.source_cluster_state,
            replay_cluster_state=snap.replay_cluster_state,
            source_persistence_state=snap.source_persistence_state,
            replay_persistence_state=snap.replay_persistence_state,
            source_memory_score=snap.source_memory_score,
            replay_memory_score=snap.replay_memory_score,
            source_freshness_state=snap.source_freshness_state,
            replay_freshness_state=snap.replay_freshness_state,
            source_aggregate_decay_score=snap.source_aggregate_decay_score,
            replay_aggregate_decay_score=snap.replay_aggregate_decay_score,
            source_stale_memory_flag=snap.source_stale_memory_flag,
            replay_stale_memory_flag=snap.replay_stale_memory_flag,
            source_contradiction_flag=snap.source_contradiction_flag,
            replay_contradiction_flag=snap.replay_contradiction_flag,
            context_hash_match=snap.context_hash_match,
            regime_match=snap.regime_match,
            timing_class_match=snap.timing_class_match,
            transition_state_match=snap.transition_state_match,
            sequence_class_match=snap.sequence_class_match,
            archetype_match=snap.archetype_match,
            cluster_state_match=snap.cluster_state_match,
            persistence_state_match=snap.persistence_state_match,
            memory_score_match=snap.memory_score_match,
            freshness_state_match=snap.freshness_state_match,
            aggregate_decay_score_match=snap.aggregate_decay_score_match,
            stale_memory_flag_match=snap.stale_memory_flag_match,
            contradiction_flag_match=snap.contradiction_flag_match,
            decay_attribution_match=snap.decay_attribution_match,
            decay_composite_match=snap.decay_composite_match,
            decay_dominant_family_match=snap.decay_dominant_family_match,
            decay_delta=snap.decay_delta,
            decay_composite_delta=snap.decay_composite_delta,
            drift_reason_codes=snap.drift_reason_codes,
            validation_state=snap.validation_state,
            metadata=snap.metadata,
        )
        return str(row["id"])

    def persist_family_decay_stability(
        self, conn, *, snaps: list[FamilyDecayReplayStabilitySnapshot],
    ) -> list[str]:
        if not snaps:
            return []
        import src.db.repositories_47d as repo
        ids: list[str] = []
        for snap in snaps:
            row = repo.insert_cross_asset_family_decay_replay_stability_snapshots(
                conn,
                workspace_id=snap.workspace_id,
                watchlist_id=snap.watchlist_id,
                source_run_id=snap.source_run_id,
                replay_run_id=snap.replay_run_id,
                dependency_family=snap.dependency_family,
                source_freshness_state=snap.source_freshness_state,
                replay_freshness_state=snap.replay_freshness_state,
                source_aggregate_decay_score=snap.source_aggregate_decay_score,
                replay_aggregate_decay_score=snap.replay_aggregate_decay_score,
                source_family_decay_score=snap.source_family_decay_score,
                replay_family_decay_score=snap.replay_family_decay_score,
                source_stale_memory_flag=snap.source_stale_memory_flag,
                replay_stale_memory_flag=snap.replay_stale_memory_flag,
                source_contradiction_flag=snap.source_contradiction_flag,
                replay_contradiction_flag=snap.replay_contradiction_flag,
                source_decay_adjusted_contribution=snap.source_decay_adjusted_contribution,
                replay_decay_adjusted_contribution=snap.replay_decay_adjusted_contribution,
                source_decay_integration_contribution=snap.source_decay_integration_contribution,
                replay_decay_integration_contribution=snap.replay_decay_integration_contribution,
                decay_adjusted_delta=snap.decay_adjusted_delta,
                decay_integration_delta=snap.decay_integration_delta,
                freshness_state_match=snap.freshness_state_match,
                aggregate_decay_score_match=snap.aggregate_decay_score_match,
                family_decay_score_match=snap.family_decay_score_match,
                stale_memory_flag_match=snap.stale_memory_flag_match,
                contradiction_flag_match=snap.contradiction_flag_match,
                decay_family_rank_match=snap.decay_family_rank_match,
                decay_composite_family_rank_match=snap.decay_composite_family_rank_match,
                drift_reason_codes=snap.drift_reason_codes,
                metadata=snap.metadata,
            )
            ids.append(str(row["id"]))
        return ids

    # ── orchestration ───────────────────────────────────────────────────
    def refresh_decay_replay_validation_for_run(
        self, conn, *, replay_run_id: str,
    ) -> bool:
        source_run_id, workspace_id, _, watchlist_id = self.load_source_and_replay_runs(
            conn, replay_run_id=replay_run_id,
        )
        if not (source_run_id and workspace_id and watchlist_id):
            return False  # not a replay

        src_decay_attr  = self._load_decay_attribution_for_run(conn, run_id=source_run_id)
        rep_decay_attr  = self._load_decay_attribution_for_run(conn, run_id=replay_run_id)
        src_decay_comp  = self._load_decay_composite_for_run(conn, run_id=source_run_id)
        rep_decay_comp  = self._load_decay_composite_for_run(conn, run_id=replay_run_id)

        if (src_decay_attr is None and src_decay_comp is None) and (
            rep_decay_attr is None and rep_decay_comp is None
        ):
            logger.debug(
                "decay_replay_validation: no decay layers present for source=%s replay=%s",
                source_run_id, replay_run_id,
            )
            return False

        # Upstream context loaders.
        src_decay_state = self._load_decay_state_for_run(conn, run_id=source_run_id)
        rep_decay_state = self._load_decay_state_for_run(conn, run_id=replay_run_id)
        src_persistence = self._load_run_persistence_state(conn, run_id=source_run_id)
        rep_persistence = self._load_run_persistence_state(conn, run_id=replay_run_id)
        src_transition  = self._load_run_transition_diagnostics(conn, run_id=source_run_id)
        rep_transition  = self._load_run_transition_diagnostics(conn, run_id=replay_run_id)
        src_pattern     = self._load_run_pattern_cluster(conn, run_id=source_run_id)
        rep_pattern     = self._load_run_pattern_cluster(conn, run_id=replay_run_id)

        # Field-by-field comparisons.
        src_ctx_id  = (src_decay_attr or {}).get("context_snapshot_id") or (src_decay_comp or {}).get("context_snapshot_id")
        rep_ctx_id  = (rep_decay_attr or {}).get("context_snapshot_id") or (rep_decay_comp or {}).get("context_snapshot_id")
        src_regime  = (src_persistence or {}).get("regime_key")
        rep_regime  = (rep_persistence or {}).get("regime_key")
        src_timing  = (src_transition or {}).get("dominant_timing_class")
        rep_timing  = (rep_transition or {}).get("dominant_timing_class")
        src_trans   = (src_transition or {}).get("dominant_transition_state")
        rep_trans   = (rep_transition or {}).get("dominant_transition_state")
        src_seq     = (src_transition or {}).get("dominant_sequence_class")
        rep_seq     = (rep_transition or {}).get("dominant_sequence_class")
        src_arch    = (src_pattern or {}).get("dominant_archetype_key") or (src_persistence or {}).get("dominant_archetype_key")
        rep_arch    = (rep_pattern or {}).get("dominant_archetype_key") or (rep_persistence or {}).get("dominant_archetype_key")
        src_cluster = (src_pattern or {}).get("cluster_state") or (src_persistence or {}).get("cluster_state")
        rep_cluster = (rep_pattern or {}).get("cluster_state") or (rep_persistence or {}).get("cluster_state")
        src_persist = (src_persistence or {}).get("persistence_state") or (src_decay_state or {}).get("persistence_state")
        rep_persist = (rep_persistence or {}).get("persistence_state") or (rep_decay_state or {}).get("persistence_state")
        src_memory  = (src_persistence or {}).get("memory_score") or (src_decay_state or {}).get("memory_score")
        rep_memory  = (rep_persistence or {}).get("memory_score") or (rep_decay_state or {}).get("memory_score")
        src_freshness = (src_decay_attr or {}).get("freshness_state") or (src_decay_state or {}).get("freshness_state")
        rep_freshness = (rep_decay_attr or {}).get("freshness_state") or (rep_decay_state or {}).get("freshness_state")
        src_aggregate = (src_decay_attr or {}).get("aggregate_decay_score") or (src_decay_state or {}).get("aggregate_decay_score")
        rep_aggregate = (rep_decay_attr or {}).get("aggregate_decay_score") or (rep_decay_state or {}).get("aggregate_decay_score")
        src_stale     = (src_decay_attr or {}).get("stale_memory_flag")
        rep_stale     = (rep_decay_attr or {}).get("stale_memory_flag")
        src_contra    = (src_decay_attr or {}).get("contradiction_flag")
        rep_contra    = (rep_decay_attr or {}).get("contradiction_flag")
        src_dom_fam   = (src_decay_attr or {}).get("decay_dominant_dependency_family")
        rep_dom_fam   = (rep_decay_attr or {}).get("decay_dominant_dependency_family")

        context_match     = self.compare_context_hashes(src_ctx_id, rep_ctx_id)
        regime_match      = self.compare_regime_keys(src_regime, rep_regime)
        timing_match      = self.compare_dominant_timing_classes(src_timing, rep_timing)
        transition_match  = self.compare_transition_states(src_trans, rep_trans)
        sequence_match    = self.compare_sequence_classes(src_seq, rep_seq)
        archetype_match   = self.compare_archetype_keys(src_arch, rep_arch)
        cluster_match     = self.compare_cluster_states(src_cluster, rep_cluster)
        persistence_match = self.compare_persistence_states(src_persist, rep_persist)
        memory_match      = self.compare_memory_scores(src_memory, rep_memory)
        freshness_match   = self.compare_freshness_states(src_freshness, rep_freshness)
        aggregate_match   = self.compare_aggregate_decay_scores(src_aggregate, rep_aggregate)
        stale_match       = self.compare_stale_memory_flags(src_stale, rep_stale)
        contradiction_match = self.compare_contradiction_flags(src_contra, rep_contra)
        decay_attr_match  = self.compare_decay_attribution(src_decay_attr, rep_decay_attr)
        decay_comp_match  = self.compare_decay_composite(src_decay_comp, rep_decay_comp)
        decay_dom_match   = self.compare_decay_dominant_family(src_dom_fam, rep_dom_fam)

        decay_delta = {
            "decay_adjusted_cross_asset_contribution": {
                "source": _as_float((src_decay_attr or {}).get("decay_adjusted_cross_asset_contribution")),
                "replay": _as_float((rep_decay_attr or {}).get("decay_adjusted_cross_asset_contribution")),
            },
        }
        composite_delta = {
            "decay_net_contribution": {
                "source": _as_float((src_decay_comp or {}).get("decay_net_contribution")),
                "replay": _as_float((rep_decay_comp or {}).get("decay_net_contribution")),
            },
            "composite_pre_decay": {
                "source": _as_float((src_decay_comp or {}).get("composite_pre_decay")),
                "replay": _as_float((rep_decay_comp or {}).get("composite_pre_decay")),
            },
            "composite_post_decay": {
                "source": _as_float((src_decay_comp or {}).get("composite_post_decay")),
                "replay": _as_float((rep_decay_comp or {}).get("composite_post_decay")),
            },
        }

        drift_codes = self.derive_decay_drift_reason_codes(
            context_match=context_match, regime_match=regime_match,
            timing_match=timing_match, transition_match=transition_match,
            sequence_match=sequence_match, archetype_match=archetype_match,
            cluster_match=cluster_match, persistence_match=persistence_match,
            memory_match=memory_match, freshness_match=freshness_match,
            aggregate_decay_match=aggregate_match,
            stale_match=stale_match, contradiction_match=contradiction_match,
            decay_attr_match=decay_attr_match, decay_comp_match=decay_comp_match,
            decay_dom_match=decay_dom_match,
            source_decay_attr=src_decay_attr, replay_decay_attr=rep_decay_attr,
            source_decay_comp=src_decay_comp, replay_decay_comp=rep_decay_comp,
        )
        validation_state = self._classify_validation_state(
            source_decay_attr=src_decay_attr, replay_decay_attr=rep_decay_attr,
            source_decay_comp=src_decay_comp, replay_decay_comp=rep_decay_comp,
            context_match=context_match, regime_match=regime_match,
            timing_match=timing_match, transition_match=transition_match,
            sequence_match=sequence_match, archetype_match=archetype_match,
            cluster_match=cluster_match, persistence_match=persistence_match,
            memory_match=memory_match, freshness_match=freshness_match,
            aggregate_decay_match=aggregate_match,
            stale_match=stale_match, contradiction_match=contradiction_match,
            decay_attr_match=decay_attr_match, decay_comp_match=decay_comp_match,
            decay_dom_match=decay_dom_match,
        )

        snap = DecayReplayValidationSnapshot(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            source_run_id=source_run_id,
            replay_run_id=replay_run_id,
            source_context_snapshot_id=src_ctx_id,
            replay_context_snapshot_id=rep_ctx_id,
            source_regime_key=src_regime,           replay_regime_key=rep_regime,
            source_dominant_timing_class=src_timing,replay_dominant_timing_class=rep_timing,
            source_dominant_transition_state=src_trans,replay_dominant_transition_state=rep_trans,
            source_dominant_sequence_class=src_seq, replay_dominant_sequence_class=rep_seq,
            source_dominant_archetype_key=src_arch, replay_dominant_archetype_key=rep_arch,
            source_cluster_state=src_cluster,       replay_cluster_state=rep_cluster,
            source_persistence_state=src_persist,   replay_persistence_state=rep_persist,
            source_memory_score=_as_float(src_memory), replay_memory_score=_as_float(rep_memory),
            source_freshness_state=src_freshness,   replay_freshness_state=rep_freshness,
            source_aggregate_decay_score=_as_float(src_aggregate),
            replay_aggregate_decay_score=_as_float(rep_aggregate),
            source_stale_memory_flag=_as_bool(src_stale),
            replay_stale_memory_flag=_as_bool(rep_stale),
            source_contradiction_flag=_as_bool(src_contra),
            replay_contradiction_flag=_as_bool(rep_contra),
            context_hash_match=context_match, regime_match=regime_match,
            timing_class_match=timing_match, transition_state_match=transition_match,
            sequence_class_match=sequence_match, archetype_match=archetype_match,
            cluster_state_match=cluster_match, persistence_state_match=persistence_match,
            memory_score_match=memory_match, freshness_state_match=freshness_match,
            aggregate_decay_score_match=aggregate_match,
            stale_memory_flag_match=stale_match,
            contradiction_flag_match=contradiction_match,
            decay_attribution_match=decay_attr_match,
            decay_composite_match=decay_comp_match,
            decay_dominant_family_match=decay_dom_match,
            decay_delta=decay_delta,
            decay_composite_delta=composite_delta,
            drift_reason_codes=drift_codes,
            validation_state=validation_state,
            metadata={
                "scoring_version":         _SCORING_VERSION,
                "contribution_tolerance":  _CONTRIBUTION_TOLERANCE,
                "memory_score_tolerance":  _MEMORY_SCORE_TOLERANCE,
                "decay_score_tolerance":   _DECAY_SCORE_TOLERANCE,
            },
        )
        self.persist_decay_replay_validation(conn, snap=snap)

        # Family-level stability rows.
        src_fam_attr = self._load_family_decay_attribution(conn, run_id=source_run_id)
        rep_fam_attr = self._load_family_decay_attribution(conn, run_id=replay_run_id)
        src_fam_comp = self._load_family_decay_composite(conn, run_id=source_run_id)
        rep_fam_comp = self._load_family_decay_composite(conn, run_id=replay_run_id)
        family_snaps = self.compute_family_decay_stability(
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=source_run_id, replay_run_id=replay_run_id,
            source_attr=src_fam_attr, replay_attr=rep_fam_attr,
            source_comp=src_fam_comp, replay_comp=rep_fam_comp,
        )
        if family_snaps:
            self.persist_family_decay_stability(conn, snaps=family_snaps)

        return True
