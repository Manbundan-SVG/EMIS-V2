"""Phase 4.4D: Replay Validation for Archetype-Aware Composite Behavior.

Compares a replay run against its source across the archetype stack:
  * 4.0B context hash
  * 2.5D regime key
  * 4.2B dominant timing class (inherited)
  * 4.3A dominant transition state + dominant sequence class
  * 4.4A dominant archetype key
  * 4.4B archetype-adjusted attribution (run aggregate + per-family)
  * 4.4C archetype-aware composite (archetype_net + pre/post composite +
    family integration contribution)
  * archetype dominant family from the 4.4B integration summary

Persists:
  * one cross_asset_archetype_replay_validation_snapshots row per pair
  * one cross_asset_family_archetype_replay_stability_snapshots row per family
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_NUMERIC_TOLERANCE = 1e-9
_SCORING_VERSION   = "4.4D.v1"


@dataclass
class ArchetypeRunSnapshot:
    """Compact container for a single run's archetype-layer state."""
    run_id: str
    context_snapshot_id: str | None
    context_hash: str | None
    regime_key: str | None
    dominant_timing_class: str | None
    dominant_transition_state: str | None
    dominant_sequence_class: str | None
    dominant_archetype_key: str | None
    archetype_adjusted_total: float | None
    archetype_dominant_family: str | None
    archetype_net_contribution: float | None
    composite_pre_archetype: float | None
    composite_post_archetype: float | None
    # family_key -> {transition_state, sequence_class, archetype_key,
    #                archetype_adjusted_contribution, archetype_family_rank,
    #                archetype_integration_contribution,
    #                archetype_composite_family_rank}
    family: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class FamilyArchetypeStabilityRow:
    dependency_family: str
    source_transition_state: str | None
    replay_transition_state: str | None
    source_sequence_class: str | None
    replay_sequence_class: str | None
    source_archetype_key: str | None
    replay_archetype_key: str | None
    source_archetype_adjusted_contribution: float | None
    replay_archetype_adjusted_contribution: float | None
    source_archetype_integration_contribution: float | None
    replay_archetype_integration_contribution: float | None
    archetype_adjusted_delta: float | None
    archetype_integration_delta: float | None
    transition_state_match: bool
    sequence_class_match: bool
    archetype_match: bool
    archetype_family_rank_match: bool
    archetype_composite_family_rank_match: bool
    drift_reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ArchetypeReplayValidationResult:
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
    context_hash_match: bool
    regime_match: bool
    timing_class_match: bool
    transition_state_match: bool
    sequence_class_match: bool
    archetype_match: bool
    archetype_attribution_match: bool
    archetype_composite_match: bool
    archetype_dominant_family_match: bool
    archetype_delta: dict[str, Any]
    archetype_composite_delta: dict[str, Any]
    drift_reason_codes: list[str]
    validation_state: str
    metadata: dict[str, Any]
    family_rows: list[FamilyArchetypeStabilityRow]


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


class CrossAssetArchetypeReplayValidationService:
    """Deterministic replay comparison across the 4.4A/4.4B/4.4C archetype stack."""

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
    ) -> ArchetypeRunSnapshot:
        snap = ArchetypeRunSnapshot(
            run_id=run_id, context_snapshot_id=None, context_hash=None,
            regime_key=None, dominant_timing_class=None,
            dominant_transition_state=None, dominant_sequence_class=None,
            dominant_archetype_key=None,
            archetype_adjusted_total=None, archetype_dominant_family=None,
            archetype_net_contribution=None,
            composite_pre_archetype=None, composite_post_archetype=None,
        )

        with conn.cursor() as cur:
            # 4.4C composite summary — primary source for context + archetype
            # key + composite pre/post + archetype_net.
            cur.execute(
                """
                select
                    context_snapshot_id::text as context_snapshot_id,
                    archetype_adjusted_cross_asset_contribution,
                    composite_pre_archetype,
                    archetype_net_contribution,
                    composite_post_archetype,
                    dominant_archetype_key
                from public.cross_asset_archetype_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                d = dict(row)
                snap.context_snapshot_id         = d.get("context_snapshot_id")
                snap.archetype_adjusted_total    = _as_float(d.get("archetype_adjusted_cross_asset_contribution"))
                snap.composite_pre_archetype     = _as_float(d.get("composite_pre_archetype"))
                snap.archetype_net_contribution  = _as_float(d.get("archetype_net_contribution"))
                snap.composite_post_archetype    = _as_float(d.get("composite_post_archetype"))
                snap.dominant_archetype_key      = d.get("dominant_archetype_key")

            # Context hash
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

            # Dominant transition state + sequence class from 4.3B integration
            # summary.
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

            # Archetype dominant family from 4.4B integration summary.
            cur.execute(
                """
                select archetype_dominant_dependency_family
                from public.run_cross_asset_archetype_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                snap.archetype_dominant_family = dict(r).get("archetype_dominant_dependency_family")

            # Per-family 4.4B archetype-adjusted contributions + rank + state +
            # sequence class + archetype key.
            cur.execute(
                """
                select dependency_family,
                       transition_state,
                       dominant_sequence_class,
                       archetype_key,
                       archetype_adjusted_family_contribution,
                       archetype_family_rank
                from public.cross_asset_family_archetype_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                snap.family.setdefault(fam, {})
                snap.family[fam]["transition_state"]                   = d.get("transition_state")
                snap.family[fam]["sequence_class"]                     = d.get("dominant_sequence_class")
                snap.family[fam]["archetype_key"]                      = d.get("archetype_key")
                snap.family[fam]["archetype_adjusted_contribution"]    = _as_float(d.get("archetype_adjusted_family_contribution"))
                snap.family[fam]["archetype_family_rank"]              = d.get("archetype_family_rank")

            # Per-family 4.4C archetype integration contributions + rank
            cur.execute(
                """
                select dependency_family,
                       archetype_key,
                       transition_state,
                       dominant_sequence_class,
                       archetype_integration_contribution,
                       family_rank
                from public.cross_asset_family_archetype_composite_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                snap.family.setdefault(fam, {})
                if snap.family[fam].get("archetype_key") is None:
                    snap.family[fam]["archetype_key"] = d.get("archetype_key")
                if snap.family[fam].get("transition_state") is None:
                    snap.family[fam]["transition_state"] = d.get("transition_state")
                if snap.family[fam].get("sequence_class") is None:
                    snap.family[fam]["sequence_class"] = d.get("dominant_sequence_class")
                snap.family[fam]["archetype_integration_contribution"] = _as_float(d.get("archetype_integration_contribution"))
                snap.family[fam]["archetype_composite_family_rank"]    = d.get("family_rank")

        return snap

    # ── pairwise comparisons ────────────────────────────────────────────
    @staticmethod
    def compare_context_hashes(a: ArchetypeRunSnapshot, b: ArchetypeRunSnapshot) -> bool:
        if a.context_hash is None and b.context_hash is None:
            return True
        return a.context_hash == b.context_hash

    @staticmethod
    def compare_regime_keys(a: ArchetypeRunSnapshot, b: ArchetypeRunSnapshot) -> bool:
        if a.regime_key is None and b.regime_key is None:
            return True
        return a.regime_key == b.regime_key

    @staticmethod
    def compare_dominant_timing_classes(a: ArchetypeRunSnapshot, b: ArchetypeRunSnapshot) -> bool:
        if a.dominant_timing_class is None and b.dominant_timing_class is None:
            return True
        return a.dominant_timing_class == b.dominant_timing_class

    @staticmethod
    def compare_transition_states(a: ArchetypeRunSnapshot, b: ArchetypeRunSnapshot) -> bool:
        if a.dominant_transition_state is None and b.dominant_transition_state is None:
            return True
        return a.dominant_transition_state == b.dominant_transition_state

    @staticmethod
    def compare_sequence_classes(a: ArchetypeRunSnapshot, b: ArchetypeRunSnapshot) -> bool:
        if a.dominant_sequence_class is None and b.dominant_sequence_class is None:
            return True
        return a.dominant_sequence_class == b.dominant_sequence_class

    @staticmethod
    def compare_archetype_keys(a: ArchetypeRunSnapshot, b: ArchetypeRunSnapshot) -> bool:
        if a.dominant_archetype_key is None and b.dominant_archetype_key is None:
            return True
        return a.dominant_archetype_key == b.dominant_archetype_key

    def compare_archetype_attribution(
        self, source: ArchetypeRunSnapshot, replay: ArchetypeRunSnapshot,
    ) -> tuple[bool, dict[str, Any]]:
        net_match = _num_match(source.archetype_adjusted_total, replay.archetype_adjusted_total)
        per_family: dict[str, Any] = {}
        fam_all_match = True
        all_fams = sorted(set(source.family) | set(replay.family))
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            sv = _as_float(s.get("archetype_adjusted_contribution"))
            rv = _as_float(r.get("archetype_adjusted_contribution"))
            m = _num_match(sv, rv)
            per_family[fam] = {
                "source":      sv,
                "replay":      rv,
                "delta":       _num_delta(sv, rv),
                "source_rank": s.get("archetype_family_rank"),
                "replay_rank": r.get("archetype_family_rank"),
                "rank_match":  (s.get("archetype_family_rank") == r.get("archetype_family_rank")),
                "source_archetype_key": s.get("archetype_key"),
                "replay_archetype_key": r.get("archetype_key"),
                "match":       m,
            }
            if not m:
                fam_all_match = False
        return (net_match and fam_all_match, {
            "net": {
                "source": source.archetype_adjusted_total,
                "replay": replay.archetype_adjusted_total,
                "delta":  _num_delta(source.archetype_adjusted_total, replay.archetype_adjusted_total),
                "match":  net_match,
            },
            "families": per_family,
        })

    def compare_archetype_composite(
        self, source: ArchetypeRunSnapshot, replay: ArchetypeRunSnapshot,
    ) -> tuple[bool, dict[str, Any]]:
        net_match  = _num_match(source.archetype_net_contribution, replay.archetype_net_contribution)
        pre_match  = _num_match(source.composite_pre_archetype, replay.composite_pre_archetype)
        post_match = _num_match(source.composite_post_archetype, replay.composite_post_archetype)
        per_family: dict[str, Any] = {}
        fam_all_match = True
        all_fams = sorted(set(source.family) | set(replay.family))
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            sv = _as_float(s.get("archetype_integration_contribution"))
            rv = _as_float(r.get("archetype_integration_contribution"))
            m = _num_match(sv, rv)
            per_family[fam] = {
                "source":      sv,
                "replay":      rv,
                "delta":       _num_delta(sv, rv),
                "source_rank": s.get("archetype_composite_family_rank"),
                "replay_rank": r.get("archetype_composite_family_rank"),
                "rank_match":  (s.get("archetype_composite_family_rank") == r.get("archetype_composite_family_rank")),
                "match":       m,
            }
            if not m:
                fam_all_match = False
        return (net_match and pre_match and post_match and fam_all_match, {
            "archetype_net": {
                "source": source.archetype_net_contribution,
                "replay": replay.archetype_net_contribution,
                "delta":  _num_delta(source.archetype_net_contribution, replay.archetype_net_contribution),
                "match":  net_match,
            },
            "composite_pre": {
                "source": source.composite_pre_archetype,
                "replay": replay.composite_pre_archetype,
                "delta":  _num_delta(source.composite_pre_archetype, replay.composite_pre_archetype),
                "match":  pre_match,
            },
            "composite_post": {
                "source": source.composite_post_archetype,
                "replay": replay.composite_post_archetype,
                "delta":  _num_delta(source.composite_post_archetype, replay.composite_post_archetype),
                "match":  post_match,
            },
            "families": per_family,
        })

    @staticmethod
    def compare_archetype_dominant_family(
        a: ArchetypeRunSnapshot, b: ArchetypeRunSnapshot,
    ) -> bool:
        if a.archetype_dominant_family is None and b.archetype_dominant_family is None:
            return True
        return a.archetype_dominant_family == b.archetype_dominant_family

    # ── drift reason code derivation ────────────────────────────────────
    def derive_archetype_drift_reason_codes(
        self,
        *,
        context_match: bool,
        regime_match: bool,
        timing_class_match: bool,
        transition_state_match: bool,
        sequence_class_match: bool,
        archetype_match: bool,
        archetype_attribution_match: bool,
        archetype_composite_match: bool,
        archetype_dominant_family_match: bool,
        archetype_attr_delta: dict[str, Any],
        archetype_composite_delta: dict[str, Any],  # noqa: ARG002
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
        if not archetype_attribution_match:
            for fam, d in (archetype_attr_delta.get("families") or {}).items():
                if not d.get("match"):
                    codes.append(f"archetype_family_delta:{fam}")
                    break
            else:
                codes.append("archetype_family_delta")
        if not archetype_composite_match:
            codes.append("archetype_integration_delta")
        if not archetype_dominant_family_match:
            codes.append("archetype_dominant_family_shift")
        for layer in missing_source_layers:
            codes.append(f"missing_source_archetype_layer:{layer}")
        for layer in missing_replay_layers:
            codes.append(f"missing_replay_archetype_layer:{layer}")
        seen: set[str] = set()
        out: list[str] = []
        for c in codes:
            if c not in seen:
                seen.add(c)
                out.append(c)
        return out

    # ── family stability rows ───────────────────────────────────────────
    def compute_family_archetype_stability(
        self, source: ArchetypeRunSnapshot, replay: ArchetypeRunSnapshot,
    ) -> list[FamilyArchetypeStabilityRow]:
        all_fams = sorted(set(source.family) | set(replay.family))
        rows: list[FamilyArchetypeStabilityRow] = []
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            s_state  = s.get("transition_state")
            r_state  = r.get("transition_state")
            s_seq    = s.get("sequence_class")
            r_seq    = r.get("sequence_class")
            s_arche  = s.get("archetype_key")
            r_arche  = r.get("archetype_key")
            s_attr   = _as_float(s.get("archetype_adjusted_contribution"))
            r_attr   = _as_float(r.get("archetype_adjusted_contribution"))
            s_int    = _as_float(s.get("archetype_integration_contribution"))
            r_int    = _as_float(r.get("archetype_integration_contribution"))
            s_rank   = s.get("archetype_family_rank")
            r_rank   = r.get("archetype_family_rank")
            s_crank  = s.get("archetype_composite_family_rank")
            r_crank  = r.get("archetype_composite_family_rank")

            codes: list[str] = []
            if not _num_match(s_attr, r_attr):
                codes.append("archetype_family_delta")
            if not _num_match(s_int, r_int):
                codes.append("archetype_integration_delta")
            if s_state != r_state:
                codes.append("transition_state_mismatch")
            if s_seq != r_seq:
                codes.append("sequence_class_mismatch")
            if s_arche != r_arche:
                codes.append("archetype_key_mismatch")
            if s_rank != r_rank:
                codes.append("archetype_family_rank_shift")
            if s_crank != r_crank:
                codes.append("archetype_composite_family_rank_shift")

            rows.append(FamilyArchetypeStabilityRow(
                dependency_family=fam,
                source_transition_state=s_state,
                replay_transition_state=r_state,
                source_sequence_class=s_seq,
                replay_sequence_class=r_seq,
                source_archetype_key=s_arche,
                replay_archetype_key=r_arche,
                source_archetype_adjusted_contribution=s_attr,
                replay_archetype_adjusted_contribution=r_attr,
                source_archetype_integration_contribution=s_int,
                replay_archetype_integration_contribution=r_int,
                archetype_adjusted_delta=_num_delta(s_attr, r_attr),
                archetype_integration_delta=_num_delta(s_int, r_int),
                transition_state_match=(s_state == r_state),
                sequence_class_match=(s_seq == r_seq),
                archetype_match=(s_arche == r_arche),
                archetype_family_rank_match=(s_rank == r_rank),
                archetype_composite_family_rank_match=(s_crank == r_crank),
                drift_reason_codes=codes,
                metadata={
                    "source_archetype_family_rank":      s_rank,
                    "replay_archetype_family_rank":      r_rank,
                    "source_archetype_composite_rank":   s_crank,
                    "replay_archetype_composite_rank":   r_crank,
                },
            ))
        return rows

    # ── main builder ────────────────────────────────────────────────────
    def build_replay_validation_for_run(
        self,
        conn,
        *,
        replay_run_id: str,
    ) -> ArchetypeReplayValidationResult | None:
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
        if source.archetype_adjusted_total is None and not source.family:
            missing_source_layers.append("attribution")
        if source.archetype_net_contribution is None and source.composite_post_archetype is None:
            missing_source_layers.append("composite")
        if replay.archetype_adjusted_total is None and not replay.family:
            missing_replay_layers.append("attribution")
        if replay.archetype_net_contribution is None and replay.composite_post_archetype is None:
            missing_replay_layers.append("composite")

        context_match          = self.compare_context_hashes(source, replay)
        regime_match           = self.compare_regime_keys(source, replay)
        timing_class_match     = self.compare_dominant_timing_classes(source, replay)
        transition_state_match = self.compare_transition_states(source, replay)
        sequence_class_match   = self.compare_sequence_classes(source, replay)
        archetype_match        = self.compare_archetype_keys(source, replay)
        attr_match, attr_delta         = self.compare_archetype_attribution(source, replay)
        comp_match, composite_delta    = self.compare_archetype_composite(source, replay)
        dominant_family_match          = self.compare_archetype_dominant_family(source, replay)

        drift_codes = self.derive_archetype_drift_reason_codes(
            context_match=context_match,
            regime_match=regime_match,
            timing_class_match=timing_class_match,
            transition_state_match=transition_state_match,
            sequence_class_match=sequence_class_match,
            archetype_match=archetype_match,
            archetype_attribution_match=attr_match,
            archetype_composite_match=comp_match,
            archetype_dominant_family_match=dominant_family_match,
            archetype_attr_delta=attr_delta,
            archetype_composite_delta=composite_delta,
            missing_source_layers=missing_source_layers,
            missing_replay_layers=missing_replay_layers,
        )

        # Validation state priority:
        # insufficient_source/replay → context_mismatch → timing_mismatch →
        # transition_mismatch → archetype_mismatch → drift_detected → validated
        if missing_source_layers == ["attribution", "composite"]:
            validation_state = "insufficient_source"
        elif missing_replay_layers == ["attribution", "composite"]:
            validation_state = "insufficient_replay"
        elif not context_match and not any(
            c for c in drift_codes
            if c.startswith((
                "archetype_family_delta", "archetype_integration_delta",
                "archetype_dominant_family_shift",
                "archetype_key_mismatch",
                "transition_state_mismatch", "sequence_class_mismatch",
                "timing_class_mismatch", "regime_key_mismatch",
            ))
        ):
            validation_state = "context_mismatch"
        elif not timing_class_match and context_match and regime_match and \
             transition_state_match and sequence_class_match and \
             archetype_match and attr_match and comp_match:
            validation_state = "timing_mismatch"
        elif (not transition_state_match or not sequence_class_match) and \
             context_match and regime_match and timing_class_match and \
             archetype_match and attr_match and comp_match:
            validation_state = "transition_mismatch"
        elif (not archetype_match) and \
             context_match and regime_match and timing_class_match and \
             transition_state_match and sequence_class_match and \
             attr_match and comp_match:
            validation_state = "archetype_mismatch"
        elif drift_codes:
            validation_state = "drift_detected"
        else:
            validation_state = "validated"

        family_rows = self.compute_family_archetype_stability(source, replay)

        metadata: dict[str, Any] = {
            "scoring_version":        _SCORING_VERSION,
            "numeric_tolerance":      _NUMERIC_TOLERANCE,
            "missing_source_layers":  missing_source_layers,
            "missing_replay_layers":  missing_replay_layers,
            "is_replay":              lineage.get("is_replay"),
        }

        return ArchetypeReplayValidationResult(
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
            context_hash_match=context_match,
            regime_match=regime_match,
            timing_class_match=timing_class_match,
            transition_state_match=transition_state_match,
            sequence_class_match=sequence_class_match,
            archetype_match=archetype_match,
            archetype_attribution_match=attr_match,
            archetype_composite_match=comp_match,
            archetype_dominant_family_match=dominant_family_match,
            archetype_delta=attr_delta,
            archetype_composite_delta=composite_delta,
            drift_reason_codes=drift_codes,
            validation_state=validation_state,
            metadata=metadata,
            family_rows=family_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_archetype_replay_validation(
        self, conn, *, result: ArchetypeReplayValidationResult,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_archetype_replay_validation_snapshot(
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
            context_hash_match=result.context_hash_match,
            regime_match=result.regime_match,
            timing_class_match=result.timing_class_match,
            transition_state_match=result.transition_state_match,
            sequence_class_match=result.sequence_class_match,
            archetype_match=result.archetype_match,
            archetype_attribution_match=result.archetype_attribution_match,
            archetype_composite_match=result.archetype_composite_match,
            archetype_dominant_family_match=result.archetype_dominant_family_match,
            archetype_delta=result.archetype_delta,
            archetype_composite_delta=result.archetype_composite_delta,
            drift_reason_codes=result.drift_reason_codes,
            validation_state=result.validation_state,
            metadata=result.metadata,
        )
        return str(row["id"])

    def persist_family_archetype_stability(
        self, conn, *, result: ArchetypeReplayValidationResult,
    ) -> int:
        if not result.family_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_archetype_replay_stability_snapshots(
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
                    "source_archetype_key":                         fr.source_archetype_key,
                    "replay_archetype_key":                         fr.replay_archetype_key,
                    "source_archetype_adjusted_contribution":       fr.source_archetype_adjusted_contribution,
                    "replay_archetype_adjusted_contribution":       fr.replay_archetype_adjusted_contribution,
                    "source_archetype_integration_contribution":    fr.source_archetype_integration_contribution,
                    "replay_archetype_integration_contribution":    fr.replay_archetype_integration_contribution,
                    "archetype_adjusted_delta":                     fr.archetype_adjusted_delta,
                    "archetype_integration_delta":                  fr.archetype_integration_delta,
                    "transition_state_match":                       fr.transition_state_match,
                    "sequence_class_match":                         fr.sequence_class_match,
                    "archetype_match":                              fr.archetype_match,
                    "archetype_family_rank_match":                  fr.archetype_family_rank_match,
                    "archetype_composite_family_rank_match":        fr.archetype_composite_family_rank_match,
                    "drift_reason_codes":                           fr.drift_reason_codes,
                    "metadata":                                     fr.metadata,
                }
                for fr in result.family_rows
            ],
        )

    def build_and_persist(
        self, conn, *, replay_run_id: str,
    ) -> ArchetypeReplayValidationResult | None:
        result = self.build_replay_validation_for_run(
            conn, replay_run_id=replay_run_id,
        )
        if result is None:
            return None
        self.persist_archetype_replay_validation(conn, result=result)
        self.persist_family_archetype_stability(conn, result=result)
        return result

    def refresh_archetype_replay_validation_for_run(
        self, conn, *, replay_run_id: str,
    ) -> ArchetypeReplayValidationResult | None:
        """Consumer entrypoint. Only acts when the run is a replay with a
        resolvable source. Commits on success."""
        try:
            result = self.build_and_persist(conn, replay_run_id=replay_run_id)
            if result is not None:
                conn.commit()
            return result
        except Exception as exc:
            logger.warning(
                "cross_asset_archetype_replay_validation: run=%s build/persist failed: %s",
                replay_run_id, exc,
            )
            conn.rollback()
            return None
