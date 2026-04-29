"""Phase 4.3D: Replay Validation for Sequencing-Aware Composite Behavior.

Compares a replay run against its source across the sequencing stack:
  * 4.0B context hash
  * 2.5D regime key
  * 4.2B dominant timing class (inherited)
  * 4.3A dominant transition state + dominant sequence class
  * 4.3B transition-adjusted attribution (run aggregate + per-family)
  * 4.3C transition-aware composite (transition_net + pre/post composite +
    family integration contribution)
  * transition dominant family from the 4.3B integration summary

Persists:
  * one cross_asset_transition_replay_validation_snapshots row per pair
  * one cross_asset_family_transition_replay_stability_snapshots row per family
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_NUMERIC_TOLERANCE = 1e-9
_SCORING_VERSION   = "4.3D.v1"


@dataclass
class TransitionRunSnapshot:
    """Compact container for a single run's sequencing-layer state."""
    run_id: str
    context_snapshot_id: str | None
    context_hash: str | None
    regime_key: str | None
    dominant_timing_class: str | None
    dominant_transition_state: str | None
    dominant_sequence_class: str | None
    transition_adjusted_total: float | None
    transition_dominant_family: str | None
    transition_net_contribution: float | None
    composite_pre_transition: float | None
    composite_post_transition: float | None
    # family_key -> {transition_state, sequence_class,
    #                transition_adjusted_contribution, transition_family_rank,
    #                transition_integration_contribution, transition_composite_family_rank}
    family: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class FamilyTransitionStabilityRow:
    dependency_family: str
    source_transition_state: str | None
    replay_transition_state: str | None
    source_sequence_class: str | None
    replay_sequence_class: str | None
    source_transition_adjusted_contribution: float | None
    replay_transition_adjusted_contribution: float | None
    source_transition_integration_contribution: float | None
    replay_transition_integration_contribution: float | None
    transition_adjusted_delta: float | None
    transition_integration_delta: float | None
    transition_state_match: bool
    sequence_class_match: bool
    transition_family_rank_match: bool
    transition_composite_family_rank_match: bool
    drift_reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TransitionReplayValidationResult:
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
    context_hash_match: bool
    regime_match: bool
    timing_class_match: bool
    transition_state_match: bool
    sequence_class_match: bool
    transition_attribution_match: bool
    transition_composite_match: bool
    transition_dominant_family_match: bool
    transition_delta: dict[str, Any]
    transition_composite_delta: dict[str, Any]
    drift_reason_codes: list[str]
    validation_state: str
    metadata: dict[str, Any]
    family_rows: list[FamilyTransitionStabilityRow]


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


class CrossAssetTransitionReplayValidationService:
    """Deterministic replay comparison across the 4.3A/4.3B/4.3C sequencing stack."""

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
    ) -> TransitionRunSnapshot:
        snap = TransitionRunSnapshot(
            run_id=run_id, context_snapshot_id=None, context_hash=None,
            regime_key=None, dominant_timing_class=None,
            dominant_transition_state=None, dominant_sequence_class=None,
            transition_adjusted_total=None, transition_dominant_family=None,
            transition_net_contribution=None,
            composite_pre_transition=None, composite_post_transition=None,
        )

        with conn.cursor() as cur:
            # 4.3C composite summary — primary source for context +
            # transition state + composite pre/post.
            cur.execute(
                """
                select
                    context_snapshot_id::text as context_snapshot_id,
                    transition_adjusted_cross_asset_contribution,
                    composite_pre_transition,
                    transition_net_contribution,
                    composite_post_transition,
                    dominant_transition_state
                from public.cross_asset_transition_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                d = dict(row)
                snap.context_snapshot_id           = d.get("context_snapshot_id")
                snap.transition_adjusted_total     = _as_float(d.get("transition_adjusted_cross_asset_contribution"))
                snap.composite_pre_transition      = _as_float(d.get("composite_pre_transition"))
                snap.transition_net_contribution   = _as_float(d.get("transition_net_contribution"))
                snap.composite_post_transition     = _as_float(d.get("composite_post_transition"))
                snap.dominant_transition_state     = d.get("dominant_transition_state")

            # Context hash from 4.0B (via snapshot id)
            if snap.context_snapshot_id:
                cur.execute(
                    "select context_hash from public.watchlist_context_snapshots where id = %s::uuid",
                    (snap.context_snapshot_id,),
                )
                r = cur.fetchone()
                if r:
                    snap.context_hash = dict(r)["context_hash"]

            # Regime key from 4.1C integration summary
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

            # Dominant timing class + timing dominant family from 4.2B summary
            cur.execute(
                """
                select timing_dominant_dependency_family
                from public.run_cross_asset_timing_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            timing_dom_family = None
            if r:
                timing_dom_family = dict(r).get("timing_dominant_dependency_family")

            # Pull dominant timing class from the 4.3A transition diagnostics
            # run summary (carries dominant_timing_class for the run).
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

            # Transition dominant family + dominant sequence class from 4.3B
            # integration summary.
            cur.execute(
                """
                select transition_dominant_dependency_family,
                       dominant_sequence_class
                from public.run_cross_asset_transition_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                d = dict(r)
                snap.transition_dominant_family = d.get("transition_dominant_dependency_family")
                snap.dominant_sequence_class    = d.get("dominant_sequence_class")

            # Fallback: reuse timing dominant family when no transition family
            # is resolvable but we still need something for mirror lookups.
            if snap.transition_dominant_family is None and timing_dom_family is not None:
                snap.metadata_timing_dom_family = timing_dom_family  # type: ignore[attr-defined]

            # Per-family 4.3B transition-adjusted contributions + rank +
            # state + sequence class
            cur.execute(
                """
                select dependency_family,
                       transition_state,
                       dominant_sequence_class,
                       transition_adjusted_family_contribution,
                       transition_family_rank
                from public.cross_asset_family_transition_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                snap.family.setdefault(fam, {})
                snap.family[fam]["transition_state"]                     = d.get("transition_state")
                snap.family[fam]["sequence_class"]                       = d.get("dominant_sequence_class")
                snap.family[fam]["transition_adjusted_contribution"]     = _as_float(d.get("transition_adjusted_family_contribution"))
                snap.family[fam]["transition_family_rank"]               = d.get("transition_family_rank")

            # Per-family 4.3C transition integration contributions + rank
            cur.execute(
                """
                select dependency_family,
                       transition_state,
                       dominant_sequence_class,
                       transition_integration_contribution,
                       family_rank
                from public.cross_asset_family_transition_composite_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                snap.family.setdefault(fam, {})
                if snap.family[fam].get("transition_state") is None:
                    snap.family[fam]["transition_state"] = d.get("transition_state")
                if snap.family[fam].get("sequence_class") is None:
                    snap.family[fam]["sequence_class"] = d.get("dominant_sequence_class")
                snap.family[fam]["transition_integration_contribution"]  = _as_float(d.get("transition_integration_contribution"))
                snap.family[fam]["transition_composite_family_rank"]     = d.get("family_rank")

        return snap

    # ── pairwise comparisons ────────────────────────────────────────────
    @staticmethod
    def compare_context_hashes(
        source: TransitionRunSnapshot, replay: TransitionRunSnapshot,
    ) -> bool:
        if source.context_hash is None and replay.context_hash is None:
            return True
        return source.context_hash == replay.context_hash

    @staticmethod
    def compare_regime_keys(
        source: TransitionRunSnapshot, replay: TransitionRunSnapshot,
    ) -> bool:
        if source.regime_key is None and replay.regime_key is None:
            return True
        return source.regime_key == replay.regime_key

    @staticmethod
    def compare_dominant_timing_classes(
        source: TransitionRunSnapshot, replay: TransitionRunSnapshot,
    ) -> bool:
        if source.dominant_timing_class is None and replay.dominant_timing_class is None:
            return True
        return source.dominant_timing_class == replay.dominant_timing_class

    @staticmethod
    def compare_transition_states(
        source: TransitionRunSnapshot, replay: TransitionRunSnapshot,
    ) -> bool:
        if source.dominant_transition_state is None and replay.dominant_transition_state is None:
            return True
        return source.dominant_transition_state == replay.dominant_transition_state

    @staticmethod
    def compare_sequence_classes(
        source: TransitionRunSnapshot, replay: TransitionRunSnapshot,
    ) -> bool:
        if source.dominant_sequence_class is None and replay.dominant_sequence_class is None:
            return True
        return source.dominant_sequence_class == replay.dominant_sequence_class

    def compare_transition_attribution(
        self, source: TransitionRunSnapshot, replay: TransitionRunSnapshot,
    ) -> tuple[bool, dict[str, Any]]:
        net_match = _num_match(source.transition_adjusted_total, replay.transition_adjusted_total)
        per_family: dict[str, Any] = {}
        fam_all_match = True
        all_fams = sorted(set(source.family) | set(replay.family))
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            sv = _as_float(s.get("transition_adjusted_contribution"))
            rv = _as_float(r.get("transition_adjusted_contribution"))
            m = _num_match(sv, rv)
            per_family[fam] = {
                "source":      sv,
                "replay":      rv,
                "delta":       _num_delta(sv, rv),
                "source_rank": s.get("transition_family_rank"),
                "replay_rank": r.get("transition_family_rank"),
                "rank_match":  (s.get("transition_family_rank") == r.get("transition_family_rank")),
                "match":       m,
            }
            if not m:
                fam_all_match = False
        return (net_match and fam_all_match, {
            "net": {
                "source": source.transition_adjusted_total,
                "replay": replay.transition_adjusted_total,
                "delta":  _num_delta(source.transition_adjusted_total, replay.transition_adjusted_total),
                "match":  net_match,
            },
            "families": per_family,
        })

    def compare_transition_composite(
        self, source: TransitionRunSnapshot, replay: TransitionRunSnapshot,
    ) -> tuple[bool, dict[str, Any]]:
        net_match  = _num_match(source.transition_net_contribution, replay.transition_net_contribution)
        pre_match  = _num_match(source.composite_pre_transition, replay.composite_pre_transition)
        post_match = _num_match(source.composite_post_transition, replay.composite_post_transition)
        per_family: dict[str, Any] = {}
        fam_all_match = True
        all_fams = sorted(set(source.family) | set(replay.family))
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            sv = _as_float(s.get("transition_integration_contribution"))
            rv = _as_float(r.get("transition_integration_contribution"))
            m = _num_match(sv, rv)
            per_family[fam] = {
                "source":      sv,
                "replay":      rv,
                "delta":       _num_delta(sv, rv),
                "source_rank": s.get("transition_composite_family_rank"),
                "replay_rank": r.get("transition_composite_family_rank"),
                "rank_match":  (s.get("transition_composite_family_rank") == r.get("transition_composite_family_rank")),
                "match":       m,
            }
            if not m:
                fam_all_match = False
        return (net_match and pre_match and post_match and fam_all_match, {
            "transition_net": {
                "source": source.transition_net_contribution,
                "replay": replay.transition_net_contribution,
                "delta":  _num_delta(source.transition_net_contribution, replay.transition_net_contribution),
                "match":  net_match,
            },
            "composite_pre": {
                "source": source.composite_pre_transition,
                "replay": replay.composite_pre_transition,
                "delta":  _num_delta(source.composite_pre_transition, replay.composite_pre_transition),
                "match":  pre_match,
            },
            "composite_post": {
                "source": source.composite_post_transition,
                "replay": replay.composite_post_transition,
                "delta":  _num_delta(source.composite_post_transition, replay.composite_post_transition),
                "match":  post_match,
            },
            "families": per_family,
        })

    @staticmethod
    def compare_transition_dominant_family(
        source: TransitionRunSnapshot, replay: TransitionRunSnapshot,
    ) -> bool:
        if source.transition_dominant_family is None and replay.transition_dominant_family is None:
            return True
        return source.transition_dominant_family == replay.transition_dominant_family

    # ── drift reason code derivation ────────────────────────────────────
    def derive_transition_drift_reason_codes(
        self,
        *,
        context_match: bool,
        regime_match: bool,
        timing_class_match: bool,
        transition_state_match: bool,
        sequence_class_match: bool,
        transition_attribution_match: bool,
        transition_composite_match: bool,
        transition_dominant_family_match: bool,
        transition_attr_delta: dict[str, Any],
        transition_composite_delta: dict[str, Any],  # noqa: ARG002
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
        if not transition_attribution_match:
            for fam, d in (transition_attr_delta.get("families") or {}).items():
                if not d.get("match"):
                    codes.append(f"transition_family_delta:{fam}")
                    break
            else:
                codes.append("transition_family_delta")
        if not transition_composite_match:
            codes.append("transition_integration_delta")
        if not transition_dominant_family_match:
            codes.append("transition_dominant_family_shift")
        for layer in missing_source_layers:
            codes.append(f"missing_source_transition_layer:{layer}")
        for layer in missing_replay_layers:
            codes.append(f"missing_replay_transition_layer:{layer}")
        seen: set[str] = set()
        out: list[str] = []
        for c in codes:
            if c not in seen:
                seen.add(c)
                out.append(c)
        return out

    # ── family stability rows ───────────────────────────────────────────
    def compute_family_transition_stability(
        self, source: TransitionRunSnapshot, replay: TransitionRunSnapshot,
    ) -> list[FamilyTransitionStabilityRow]:
        all_fams = sorted(set(source.family) | set(replay.family))
        rows: list[FamilyTransitionStabilityRow] = []
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            s_state  = s.get("transition_state")
            r_state  = r.get("transition_state")
            s_seq    = s.get("sequence_class")
            r_seq    = r.get("sequence_class")
            s_attr   = _as_float(s.get("transition_adjusted_contribution"))
            r_attr   = _as_float(r.get("transition_adjusted_contribution"))
            s_int    = _as_float(s.get("transition_integration_contribution"))
            r_int    = _as_float(r.get("transition_integration_contribution"))
            s_rank   = s.get("transition_family_rank")
            r_rank   = r.get("transition_family_rank")
            s_crank  = s.get("transition_composite_family_rank")
            r_crank  = r.get("transition_composite_family_rank")

            codes: list[str] = []
            if not _num_match(s_attr, r_attr):
                codes.append("transition_family_delta")
            if not _num_match(s_int, r_int):
                codes.append("transition_integration_delta")
            if s_state != r_state:
                codes.append("transition_state_mismatch")
            if s_seq != r_seq:
                codes.append("sequence_class_mismatch")
            if s_rank != r_rank:
                codes.append("transition_family_rank_shift")
            if s_crank != r_crank:
                codes.append("transition_composite_family_rank_shift")

            rows.append(FamilyTransitionStabilityRow(
                dependency_family=fam,
                source_transition_state=s_state,
                replay_transition_state=r_state,
                source_sequence_class=s_seq,
                replay_sequence_class=r_seq,
                source_transition_adjusted_contribution=s_attr,
                replay_transition_adjusted_contribution=r_attr,
                source_transition_integration_contribution=s_int,
                replay_transition_integration_contribution=r_int,
                transition_adjusted_delta=_num_delta(s_attr, r_attr),
                transition_integration_delta=_num_delta(s_int, r_int),
                transition_state_match=(s_state == r_state),
                sequence_class_match=(s_seq == r_seq),
                transition_family_rank_match=(s_rank == r_rank),
                transition_composite_family_rank_match=(s_crank == r_crank),
                drift_reason_codes=codes,
                metadata={
                    "source_transition_family_rank":       s_rank,
                    "replay_transition_family_rank":       r_rank,
                    "source_transition_composite_rank":    s_crank,
                    "replay_transition_composite_rank":    r_crank,
                },
            ))
        return rows

    # ── main builder ────────────────────────────────────────────────────
    def build_replay_validation_for_run(
        self,
        conn,
        *,
        replay_run_id: str,
    ) -> TransitionReplayValidationResult | None:
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
        if source.transition_adjusted_total is None and not source.family:
            missing_source_layers.append("attribution")
        if source.transition_net_contribution is None and source.composite_post_transition is None:
            missing_source_layers.append("composite")
        if replay.transition_adjusted_total is None and not replay.family:
            missing_replay_layers.append("attribution")
        if replay.transition_net_contribution is None and replay.composite_post_transition is None:
            missing_replay_layers.append("composite")

        context_match          = self.compare_context_hashes(source, replay)
        regime_match           = self.compare_regime_keys(source, replay)
        timing_class_match     = self.compare_dominant_timing_classes(source, replay)
        transition_state_match = self.compare_transition_states(source, replay)
        sequence_class_match   = self.compare_sequence_classes(source, replay)
        attr_match, attr_delta         = self.compare_transition_attribution(source, replay)
        comp_match, composite_delta    = self.compare_transition_composite(source, replay)
        dominant_family_match          = self.compare_transition_dominant_family(source, replay)

        drift_codes = self.derive_transition_drift_reason_codes(
            context_match=context_match,
            regime_match=regime_match,
            timing_class_match=timing_class_match,
            transition_state_match=transition_state_match,
            sequence_class_match=sequence_class_match,
            transition_attribution_match=attr_match,
            transition_composite_match=comp_match,
            transition_dominant_family_match=dominant_family_match,
            transition_attr_delta=attr_delta,
            transition_composite_delta=composite_delta,
            missing_source_layers=missing_source_layers,
            missing_replay_layers=missing_replay_layers,
        )

        # Validation state priority:
        # insufficient_source/replay → context_mismatch → timing_mismatch →
        # transition_mismatch → drift_detected → validated
        if missing_source_layers == ["attribution", "composite"]:
            validation_state = "insufficient_source"
        elif missing_replay_layers == ["attribution", "composite"]:
            validation_state = "insufficient_replay"
        elif not context_match and not any(
            c for c in drift_codes
            if c.startswith((
                "transition_family_delta", "transition_integration_delta",
                "transition_dominant_family_shift",
                "transition_state_mismatch", "sequence_class_mismatch",
                "timing_class_mismatch", "regime_key_mismatch",
            ))
        ):
            validation_state = "context_mismatch"
        elif not timing_class_match and context_match and regime_match and \
             transition_state_match and sequence_class_match and \
             attr_match and comp_match:
            validation_state = "timing_mismatch"
        elif (not transition_state_match or not sequence_class_match) and \
             context_match and regime_match and timing_class_match and \
             attr_match and comp_match:
            validation_state = "transition_mismatch"
        elif drift_codes:
            validation_state = "drift_detected"
        else:
            validation_state = "validated"

        family_rows = self.compute_family_transition_stability(source, replay)

        metadata: dict[str, Any] = {
            "scoring_version":        _SCORING_VERSION,
            "numeric_tolerance":      _NUMERIC_TOLERANCE,
            "missing_source_layers":  missing_source_layers,
            "missing_replay_layers":  missing_replay_layers,
            "is_replay":              lineage.get("is_replay"),
        }

        return TransitionReplayValidationResult(
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
            context_hash_match=context_match,
            regime_match=regime_match,
            timing_class_match=timing_class_match,
            transition_state_match=transition_state_match,
            sequence_class_match=sequence_class_match,
            transition_attribution_match=attr_match,
            transition_composite_match=comp_match,
            transition_dominant_family_match=dominant_family_match,
            transition_delta=attr_delta,
            transition_composite_delta=composite_delta,
            drift_reason_codes=drift_codes,
            validation_state=validation_state,
            metadata=metadata,
            family_rows=family_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_transition_replay_validation(
        self, conn, *, result: TransitionReplayValidationResult,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_transition_replay_validation_snapshot(
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
            context_hash_match=result.context_hash_match,
            regime_match=result.regime_match,
            timing_class_match=result.timing_class_match,
            transition_state_match=result.transition_state_match,
            sequence_class_match=result.sequence_class_match,
            transition_attribution_match=result.transition_attribution_match,
            transition_composite_match=result.transition_composite_match,
            transition_dominant_family_match=result.transition_dominant_family_match,
            transition_delta=result.transition_delta,
            transition_composite_delta=result.transition_composite_delta,
            drift_reason_codes=result.drift_reason_codes,
            validation_state=result.validation_state,
            metadata=result.metadata,
        )
        return str(row["id"])

    def persist_family_transition_stability(
        self, conn, *, result: TransitionReplayValidationResult,
    ) -> int:
        if not result.family_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_transition_replay_stability_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            source_run_id=result.source_run_id,
            replay_run_id=result.replay_run_id,
            rows=[
                {
                    "dependency_family":                            fr.dependency_family,
                    "source_transition_state":                      fr.source_transition_state,
                    "replay_transition_state":                      fr.replay_transition_state,
                    "source_sequence_class":                        fr.source_sequence_class,
                    "replay_sequence_class":                        fr.replay_sequence_class,
                    "source_transition_adjusted_contribution":      fr.source_transition_adjusted_contribution,
                    "replay_transition_adjusted_contribution":      fr.replay_transition_adjusted_contribution,
                    "source_transition_integration_contribution":   fr.source_transition_integration_contribution,
                    "replay_transition_integration_contribution":   fr.replay_transition_integration_contribution,
                    "transition_adjusted_delta":                    fr.transition_adjusted_delta,
                    "transition_integration_delta":                 fr.transition_integration_delta,
                    "transition_state_match":                       fr.transition_state_match,
                    "sequence_class_match":                         fr.sequence_class_match,
                    "transition_family_rank_match":                 fr.transition_family_rank_match,
                    "transition_composite_family_rank_match":       fr.transition_composite_family_rank_match,
                    "drift_reason_codes":                           fr.drift_reason_codes,
                    "metadata":                                     fr.metadata,
                }
                for fr in result.family_rows
            ],
        )

    def build_and_persist(
        self, conn, *, replay_run_id: str,
    ) -> TransitionReplayValidationResult | None:
        result = self.build_replay_validation_for_run(
            conn, replay_run_id=replay_run_id,
        )
        if result is None:
            return None
        self.persist_transition_replay_validation(conn, result=result)
        self.persist_family_transition_stability(conn, result=result)
        return result

    def refresh_transition_replay_validation_for_run(
        self, conn, *, replay_run_id: str,
    ) -> TransitionReplayValidationResult | None:
        """Consumer entrypoint. Only acts when the run is a replay with a
        resolvable source. Commits on success."""
        try:
            result = self.build_and_persist(conn, replay_run_id=replay_run_id)
            if result is not None:
                conn.commit()
            return result
        except Exception as exc:
            logger.warning(
                "cross_asset_transition_replay_validation: run=%s build/persist failed: %s",
                replay_run_id, exc,
            )
            conn.rollback()
            return None
