"""Phase 4.2D: Replay Validation for Timing-Aware Composite Behavior.

Compares a replay run against its source across the timing stack:
  * 4.0B context hash
  * 2.5D regime key
  * 4.2A dominant timing class (from 4.2B timing-attribution family summary)
  * 4.2B timing-adjusted attribution (run aggregate + per-family)
  * 4.2C timing-aware composite (timing_net + pre/post composite + family
    integration contribution)
  * timing dominant family from the 4.2B integration summary

Persists:
  * one cross_asset_timing_replay_validation_snapshots row per pair
  * one cross_asset_family_timing_replay_stability_snapshots row per family
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_NUMERIC_TOLERANCE = 1e-9
_SCORING_VERSION   = "4.2D.v1"


@dataclass
class TimingRunSnapshot:
    """Compact container for a single run's timing-layer state."""
    run_id: str
    context_snapshot_id: str | None
    context_hash: str | None
    regime_key: str | None
    dominant_timing_class: str | None
    timing_adjusted_total: float | None
    timing_dominant_family: str | None
    timing_net_contribution: float | None
    composite_pre_timing: float | None
    composite_post_timing: float | None
    # family_key -> {dominant_timing_class, timing_adjusted_contribution,
    #                timing_family_rank, timing_integration_contribution,
    #                family_rank (composite)}
    family: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class FamilyTimingStabilityRow:
    dependency_family: str
    source_dominant_timing_class: str | None
    replay_dominant_timing_class: str | None
    source_timing_adjusted_contribution: float | None
    replay_timing_adjusted_contribution: float | None
    source_timing_integration_contribution: float | None
    replay_timing_integration_contribution: float | None
    timing_adjusted_delta: float | None
    timing_integration_delta: float | None
    timing_class_match: bool
    timing_family_rank_match: bool
    timing_composite_family_rank_match: bool
    drift_reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TimingReplayValidationResult:
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
    context_hash_match: bool
    regime_match: bool
    timing_class_match: bool
    timing_attribution_match: bool
    timing_composite_match: bool
    timing_dominant_family_match: bool
    timing_net_delta: dict[str, Any]
    timing_composite_delta: dict[str, Any]
    drift_reason_codes: list[str]
    validation_state: str
    metadata: dict[str, Any]
    family_rows: list[FamilyTimingStabilityRow]


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


class CrossAssetTimingReplayValidationService:
    """Deterministic replay comparison across the 4.2A/4.2B/4.2C timing stack."""

    # ── lineage ─────────────────────────────────────────────────────────
    def load_source_and_replay_runs(
        self, conn, *, replay_run_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    id::text                 as replay_run_id,
                    replayed_from_run_id::text as source_run_id,
                    workspace_id::text       as workspace_id,
                    watchlist_id::text       as watchlist_id,
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
    ) -> TimingRunSnapshot:
        snap = TimingRunSnapshot(
            run_id=run_id, context_snapshot_id=None, context_hash=None,
            regime_key=None, dominant_timing_class=None,
            timing_adjusted_total=None, timing_dominant_family=None,
            timing_net_contribution=None,
            composite_pre_timing=None, composite_post_timing=None,
        )

        with conn.cursor() as cur:
            # 4.2C composite snapshot — primary source for context + regime +
            # dominant timing class + composite pre/post.
            cur.execute(
                """
                select
                    context_snapshot_id::text as context_snapshot_id,
                    timing_adjusted_cross_asset_contribution,
                    composite_pre_timing,
                    timing_net_contribution,
                    composite_post_timing,
                    dominant_timing_class
                from public.cross_asset_timing_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                d = dict(row)
                snap.context_snapshot_id      = d.get("context_snapshot_id")
                snap.timing_adjusted_total    = _as_float(d.get("timing_adjusted_cross_asset_contribution"))
                snap.composite_pre_timing     = _as_float(d.get("composite_pre_timing"))
                snap.timing_net_contribution  = _as_float(d.get("timing_net_contribution"))
                snap.composite_post_timing    = _as_float(d.get("composite_post_timing"))
                snap.dominant_timing_class    = d.get("dominant_timing_class")

            # Context hash from 4.0B (via snapshot id)
            if snap.context_snapshot_id:
                cur.execute(
                    "select context_hash from public.watchlist_context_snapshots where id = %s::uuid",
                    (snap.context_snapshot_id,),
                )
                r = cur.fetchone()
                if r:
                    snap.context_hash = dict(r)["context_hash"]

            # Regime key + timing dominant family from 4.2B integration summary
            cur.execute(
                """
                select regime_dominant_dependency_family,
                       timing_dominant_dependency_family
                from public.run_cross_asset_regime_integration_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                d = dict(r)
                # Regime key comes from the 4.1C integration view (which
                # carries the run's regime_key).
            cur.execute(
                """
                select regime_key, timing_dominant_dependency_family
                from public.run_cross_asset_regime_integration_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                d = dict(r)
                snap.regime_key = d.get("regime_key")

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
            if r:
                snap.timing_dominant_family = dict(r).get("timing_dominant_dependency_family")

            # Per-family 4.2B timing-adjusted contributions
            cur.execute(
                """
                select dependency_family,
                       dominant_timing_class,
                       timing_adjusted_family_contribution,
                       timing_family_rank
                from public.cross_asset_family_timing_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                snap.family.setdefault(fam, {})
                snap.family[fam]["dominant_timing_class"]           = d.get("dominant_timing_class")
                snap.family[fam]["timing_adjusted_contribution"]    = _as_float(d.get("timing_adjusted_family_contribution"))
                snap.family[fam]["timing_family_rank"]              = d.get("timing_family_rank")

            # Per-family 4.2C timing-integration contributions
            cur.execute(
                """
                select dependency_family,
                       dominant_timing_class,
                       timing_integration_contribution,
                       family_rank
                from public.cross_asset_family_timing_composite_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                snap.family.setdefault(fam, {})
                if snap.family[fam].get("dominant_timing_class") is None:
                    snap.family[fam]["dominant_timing_class"] = d.get("dominant_timing_class")
                snap.family[fam]["timing_integration_contribution"] = _as_float(d.get("timing_integration_contribution"))
                snap.family[fam]["timing_composite_family_rank"]    = d.get("family_rank")

        return snap

    # ── pairwise comparisons ────────────────────────────────────────────
    @staticmethod
    def compare_context_hashes(
        source: TimingRunSnapshot, replay: TimingRunSnapshot,
    ) -> bool:
        if source.context_hash is None and replay.context_hash is None:
            return True
        return source.context_hash == replay.context_hash

    @staticmethod
    def compare_regime_keys(
        source: TimingRunSnapshot, replay: TimingRunSnapshot,
    ) -> bool:
        if source.regime_key is None and replay.regime_key is None:
            return True
        return source.regime_key == replay.regime_key

    @staticmethod
    def compare_dominant_timing_classes(
        source: TimingRunSnapshot, replay: TimingRunSnapshot,
    ) -> bool:
        if source.dominant_timing_class is None and replay.dominant_timing_class is None:
            return True
        return source.dominant_timing_class == replay.dominant_timing_class

    def compare_timing_attribution(
        self, source: TimingRunSnapshot, replay: TimingRunSnapshot,
    ) -> tuple[bool, dict[str, Any]]:
        net_match = _num_match(source.timing_adjusted_total, replay.timing_adjusted_total)
        per_family: dict[str, Any] = {}
        fam_all_match = True
        all_fams = sorted(set(source.family) | set(replay.family))
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            sv = _as_float(s.get("timing_adjusted_contribution"))
            rv = _as_float(r.get("timing_adjusted_contribution"))
            m = _num_match(sv, rv)
            per_family[fam] = {
                "source":     sv,
                "replay":     rv,
                "delta":      _num_delta(sv, rv),
                "source_rank": s.get("timing_family_rank"),
                "replay_rank": r.get("timing_family_rank"),
                "rank_match": (s.get("timing_family_rank") == r.get("timing_family_rank")),
                "match":      m,
            }
            if not m:
                fam_all_match = False
        return (net_match and fam_all_match, {
            "net": {
                "source": source.timing_adjusted_total,
                "replay": replay.timing_adjusted_total,
                "delta":  _num_delta(source.timing_adjusted_total, replay.timing_adjusted_total),
                "match":  net_match,
            },
            "families": per_family,
        })

    def compare_timing_composite(
        self, source: TimingRunSnapshot, replay: TimingRunSnapshot,
    ) -> tuple[bool, dict[str, Any]]:
        net_match = _num_match(source.timing_net_contribution, replay.timing_net_contribution)
        pre_match  = _num_match(source.composite_pre_timing, replay.composite_pre_timing)
        post_match = _num_match(source.composite_post_timing, replay.composite_post_timing)
        per_family: dict[str, Any] = {}
        fam_all_match = True
        all_fams = sorted(set(source.family) | set(replay.family))
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            sv = _as_float(s.get("timing_integration_contribution"))
            rv = _as_float(r.get("timing_integration_contribution"))
            m = _num_match(sv, rv)
            per_family[fam] = {
                "source":      sv,
                "replay":      rv,
                "delta":       _num_delta(sv, rv),
                "source_rank": s.get("timing_composite_family_rank"),
                "replay_rank": r.get("timing_composite_family_rank"),
                "rank_match":  (s.get("timing_composite_family_rank") == r.get("timing_composite_family_rank")),
                "match":       m,
            }
            if not m:
                fam_all_match = False
        return (net_match and pre_match and post_match and fam_all_match, {
            "timing_net": {
                "source": source.timing_net_contribution,
                "replay": replay.timing_net_contribution,
                "delta":  _num_delta(source.timing_net_contribution, replay.timing_net_contribution),
                "match":  net_match,
            },
            "composite_pre": {
                "source": source.composite_pre_timing,
                "replay": replay.composite_pre_timing,
                "delta":  _num_delta(source.composite_pre_timing, replay.composite_pre_timing),
                "match":  pre_match,
            },
            "composite_post": {
                "source": source.composite_post_timing,
                "replay": replay.composite_post_timing,
                "delta":  _num_delta(source.composite_post_timing, replay.composite_post_timing),
                "match":  post_match,
            },
            "families": per_family,
        })

    @staticmethod
    def compare_timing_dominant_family(
        source: TimingRunSnapshot, replay: TimingRunSnapshot,
    ) -> bool:
        if source.timing_dominant_family is None and replay.timing_dominant_family is None:
            return True
        return source.timing_dominant_family == replay.timing_dominant_family

    # ── drift reason code derivation ────────────────────────────────────
    def derive_timing_drift_reason_codes(
        self,
        *,
        context_match: bool,
        regime_match: bool,
        timing_class_match: bool,
        timing_attribution_match: bool,
        timing_composite_match: bool,
        timing_dominant_family_match: bool,
        timing_attr_delta: dict[str, Any],
        timing_composite_delta: dict[str, Any],
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
        if not timing_attribution_match:
            for fam, d in (timing_attr_delta.get("families") or {}).items():
                if not d.get("match"):
                    codes.append(f"timing_family_delta:{fam}")
                    break
            else:
                codes.append("timing_family_delta")
        if not timing_composite_match:
            codes.append("timing_integration_delta")
        if not timing_dominant_family_match:
            codes.append("timing_dominant_family_shift")
        for layer in missing_source_layers:
            codes.append(f"missing_source_timing_layer:{layer}")
        for layer in missing_replay_layers:
            codes.append(f"missing_replay_timing_layer:{layer}")
        # Dedupe preserving order
        seen: set[str] = set()
        out: list[str] = []
        for c in codes:
            if c not in seen:
                seen.add(c)
                out.append(c)
        return out

    # ── family stability rows ───────────────────────────────────────────
    def compute_family_timing_stability(
        self, source: TimingRunSnapshot, replay: TimingRunSnapshot,
    ) -> list[FamilyTimingStabilityRow]:
        all_fams = sorted(set(source.family) | set(replay.family))
        rows: list[FamilyTimingStabilityRow] = []
        for fam in all_fams:
            s = source.family.get(fam) or {}
            r = replay.family.get(fam) or {}
            s_class  = s.get("dominant_timing_class")
            r_class  = r.get("dominant_timing_class")
            s_attr   = _as_float(s.get("timing_adjusted_contribution"))
            r_attr   = _as_float(r.get("timing_adjusted_contribution"))
            s_int    = _as_float(s.get("timing_integration_contribution"))
            r_int    = _as_float(r.get("timing_integration_contribution"))
            s_rank   = s.get("timing_family_rank")
            r_rank   = r.get("timing_family_rank")
            s_crank  = s.get("timing_composite_family_rank")
            r_crank  = r.get("timing_composite_family_rank")

            codes: list[str] = []
            if not _num_match(s_attr, r_attr):
                codes.append("timing_family_delta")
            if not _num_match(s_int, r_int):
                codes.append("timing_integration_delta")
            if s_class != r_class:
                codes.append("timing_class_mismatch")
            if s_rank != r_rank:
                codes.append("timing_family_rank_shift")
            if s_crank != r_crank:
                codes.append("timing_composite_family_rank_shift")

            rows.append(FamilyTimingStabilityRow(
                dependency_family=fam,
                source_dominant_timing_class=s_class,
                replay_dominant_timing_class=r_class,
                source_timing_adjusted_contribution=s_attr,
                replay_timing_adjusted_contribution=r_attr,
                source_timing_integration_contribution=s_int,
                replay_timing_integration_contribution=r_int,
                timing_adjusted_delta=_num_delta(s_attr, r_attr),
                timing_integration_delta=_num_delta(s_int, r_int),
                timing_class_match=(s_class == r_class),
                timing_family_rank_match=(s_rank == r_rank),
                timing_composite_family_rank_match=(s_crank == r_crank),
                drift_reason_codes=codes,
                metadata={
                    "source_timing_family_rank":       s_rank,
                    "replay_timing_family_rank":       r_rank,
                    "source_timing_composite_rank":    s_crank,
                    "replay_timing_composite_rank":    r_crank,
                },
            ))
        return rows

    # ── main builder ────────────────────────────────────────────────────
    def build_replay_validation_for_run(
        self,
        conn,
        *,
        replay_run_id: str,
    ) -> TimingReplayValidationResult | None:
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
        if source.timing_adjusted_total is None and not source.family:
            missing_source_layers.append("attribution")
        if source.timing_net_contribution is None and source.composite_post_timing is None:
            missing_source_layers.append("composite")
        if replay.timing_adjusted_total is None and not replay.family:
            missing_replay_layers.append("attribution")
        if replay.timing_net_contribution is None and replay.composite_post_timing is None:
            missing_replay_layers.append("composite")

        context_match      = self.compare_context_hashes(source, replay)
        regime_match       = self.compare_regime_keys(source, replay)
        timing_class_match = self.compare_dominant_timing_classes(source, replay)
        attr_match, attr_delta         = self.compare_timing_attribution(source, replay)
        comp_match, composite_delta    = self.compare_timing_composite(source, replay)
        dominant_family_match          = self.compare_timing_dominant_family(source, replay)

        drift_codes = self.derive_timing_drift_reason_codes(
            context_match=context_match,
            regime_match=regime_match,
            timing_class_match=timing_class_match,
            timing_attribution_match=attr_match,
            timing_composite_match=comp_match,
            timing_dominant_family_match=dominant_family_match,
            timing_attr_delta=attr_delta,
            timing_composite_delta=composite_delta,
            missing_source_layers=missing_source_layers,
            missing_replay_layers=missing_replay_layers,
        )

        # Validation state priority:
        # insufficient_source/replay → context_mismatch → timing_mismatch →
        # drift_detected → validated
        if missing_source_layers == ["attribution", "composite"]:
            validation_state = "insufficient_source"
        elif missing_replay_layers == ["attribution", "composite"]:
            validation_state = "insufficient_replay"
        elif not context_match and not any(
            c for c in drift_codes
            if c.startswith(("timing_family_delta", "timing_integration_delta",
                             "timing_dominant_family_shift", "timing_class_mismatch",
                             "regime_key_mismatch"))
        ):
            validation_state = "context_mismatch"
        elif not timing_class_match and context_match and regime_match and attr_match and comp_match:
            validation_state = "timing_mismatch"
        elif drift_codes:
            validation_state = "drift_detected"
        else:
            validation_state = "validated"

        family_rows = self.compute_family_timing_stability(source, replay)

        metadata: dict[str, Any] = {
            "scoring_version":        _SCORING_VERSION,
            "numeric_tolerance":      _NUMERIC_TOLERANCE,
            "missing_source_layers":  missing_source_layers,
            "missing_replay_layers":  missing_replay_layers,
            "is_replay":              lineage.get("is_replay"),
        }

        return TimingReplayValidationResult(
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
            context_hash_match=context_match,
            regime_match=regime_match,
            timing_class_match=timing_class_match,
            timing_attribution_match=attr_match,
            timing_composite_match=comp_match,
            timing_dominant_family_match=dominant_family_match,
            timing_net_delta=attr_delta,
            timing_composite_delta=composite_delta,
            drift_reason_codes=drift_codes,
            validation_state=validation_state,
            metadata=metadata,
            family_rows=family_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_timing_replay_validation(
        self, conn, *, result: TimingReplayValidationResult,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_timing_replay_validation_snapshot(
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
            context_hash_match=result.context_hash_match,
            regime_match=result.regime_match,
            timing_class_match=result.timing_class_match,
            timing_attribution_match=result.timing_attribution_match,
            timing_composite_match=result.timing_composite_match,
            timing_dominant_family_match=result.timing_dominant_family_match,
            timing_net_delta=result.timing_net_delta,
            timing_composite_delta=result.timing_composite_delta,
            drift_reason_codes=result.drift_reason_codes,
            validation_state=result.validation_state,
            metadata=result.metadata,
        )
        return str(row["id"])

    def persist_family_timing_stability(
        self, conn, *, result: TimingReplayValidationResult,
    ) -> int:
        if not result.family_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_timing_replay_stability_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            source_run_id=result.source_run_id,
            replay_run_id=result.replay_run_id,
            rows=[
                {
                    "dependency_family":                         fr.dependency_family,
                    "source_dominant_timing_class":              fr.source_dominant_timing_class,
                    "replay_dominant_timing_class":              fr.replay_dominant_timing_class,
                    "source_timing_adjusted_contribution":       fr.source_timing_adjusted_contribution,
                    "replay_timing_adjusted_contribution":       fr.replay_timing_adjusted_contribution,
                    "source_timing_integration_contribution":    fr.source_timing_integration_contribution,
                    "replay_timing_integration_contribution":    fr.replay_timing_integration_contribution,
                    "timing_adjusted_delta":                     fr.timing_adjusted_delta,
                    "timing_integration_delta":                  fr.timing_integration_delta,
                    "timing_class_match":                        fr.timing_class_match,
                    "timing_family_rank_match":                  fr.timing_family_rank_match,
                    "timing_composite_family_rank_match":        fr.timing_composite_family_rank_match,
                    "drift_reason_codes":                        fr.drift_reason_codes,
                    "metadata":                                  fr.metadata,
                }
                for fr in result.family_rows
            ],
        )

    def build_and_persist(
        self, conn, *, replay_run_id: str,
    ) -> TimingReplayValidationResult | None:
        result = self.build_replay_validation_for_run(
            conn, replay_run_id=replay_run_id,
        )
        if result is None:
            return None
        self.persist_timing_replay_validation(conn, result=result)
        self.persist_family_timing_stability(conn, result=result)
        return result

    def refresh_timing_replay_validation_for_run(
        self, conn, *, replay_run_id: str,
    ) -> TimingReplayValidationResult | None:
        """Consumer entrypoint. Only acts when the run is a replay with a
        resolvable source. Commits on success."""
        try:
            result = self.build_and_persist(conn, replay_run_id=replay_run_id)
            if result is not None:
                conn.commit()
            return result
        except Exception as exc:
            logger.warning(
                "cross_asset_timing_replay_validation: run=%s build/persist failed: %s",
                replay_run_id, exc,
            )
            conn.rollback()
            return None
