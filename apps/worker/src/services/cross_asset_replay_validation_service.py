"""Phase 4.1D: Cross-Asset Replay + Stability Validation Service.

Compares a replay run against its source run across the full cross-asset
attribution stack (raw 4.1A, weighted 4.1B, regime-aware 4.1C) plus context
(4.0B) and regime (2.5D regime_transition_events). Persists:

  * one cross_asset_replay_validation_snapshots row per comparison
  * one cross_asset_family_replay_stability_snapshots row per dependency family

Validation is deterministic. Numeric equality uses a small tolerance
(1e-9 by default). Drift reason codes are explicit and exhaustive.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_NUMERIC_TOLERANCE = 1e-9
_SCORING_VERSION   = "4.1D.v1"


@dataclass
class RunLayerSnapshot:
    """Compact container for a single run's cross-asset state."""
    run_id: str
    context_snapshot_id: str | None
    context_hash: str | None
    regime_key: str | None
    raw_net_contribution: float | None
    weighted_net_contribution: float | None
    regime_net_contribution: float | None
    raw_dominant_family: str | None
    weighted_dominant_family: str | None
    regime_dominant_family: str | None
    # Family-level maps keyed by dependency_family → {contribution, rank}
    raw_family: dict[str, dict[str, Any]] = field(default_factory=dict)
    weighted_family: dict[str, dict[str, Any]] = field(default_factory=dict)
    regime_family: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class FamilyStabilityRow:
    dependency_family: str
    source_raw_contribution: float | None
    replay_raw_contribution: float | None
    source_weighted_contribution: float | None
    replay_weighted_contribution: float | None
    source_regime_contribution: float | None
    replay_regime_contribution: float | None
    raw_delta: float | None
    weighted_delta: float | None
    regime_delta: float | None
    family_rank_match: bool
    weighted_family_rank_match: bool
    regime_family_rank_match: bool
    drift_reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReplayValidationResult:
    workspace_id: str
    watchlist_id: str
    source_run_id: str
    replay_run_id: str
    source_context_snapshot_id: str | None
    replay_context_snapshot_id: str | None
    source_regime_key: str | None
    replay_regime_key: str | None
    context_hash_match: bool
    regime_match: bool
    raw_attribution_match: bool
    weighted_attribution_match: bool
    regime_attribution_match: bool
    dominant_family_match: bool
    weighted_dominant_family_match: bool
    regime_dominant_family_match: bool
    raw_delta: dict[str, Any]
    weighted_delta: dict[str, Any]
    regime_delta: dict[str, Any]
    drift_reason_codes: list[str]
    validation_state: str
    metadata: dict[str, Any]
    family_rows: list[FamilyStabilityRow]


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


class CrossAssetReplayValidationService:
    """Deterministic replay comparison across the 4.0B/4.1A/4.1B/4.1C stack."""

    # ── lineage ─────────────────────────────────────────────────────────
    def load_source_and_replay_runs(
        self, conn, *, replay_run_id: str,
    ) -> dict[str, Any] | None:
        """Return {source_run_id, replay_run_id, workspace_id, watchlist_id}
        or None if the run is not a replay or has no valid source."""
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    id::text            as replay_run_id,
                    replayed_from_run_id::text as source_run_id,
                    workspace_id::text  as workspace_id,
                    watchlist_id::text  as watchlist_id,
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

    # ── state loading per run ───────────────────────────────────────────
    def _load_run_state(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> RunLayerSnapshot:
        snap = RunLayerSnapshot(
            run_id=run_id, context_snapshot_id=None, context_hash=None,
            regime_key=None,
            raw_net_contribution=None, weighted_net_contribution=None,
            regime_net_contribution=None,
            raw_dominant_family=None, weighted_dominant_family=None,
            regime_dominant_family=None,
        )

        with conn.cursor() as cur:
            # 4.1A raw attribution
            cur.execute(
                """
                select context_snapshot_id::text as context_snapshot_id,
                       cross_asset_net_contribution
                from public.cross_asset_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                d = dict(row)
                snap.context_snapshot_id = d.get("context_snapshot_id")
                snap.raw_net_contribution = _as_float(d.get("cross_asset_net_contribution"))

            # 4.0B context hash (via snapshot id)
            if snap.context_snapshot_id:
                cur.execute(
                    "select context_hash from public.watchlist_context_snapshots where id = %s::uuid",
                    (snap.context_snapshot_id,),
                )
                r = cur.fetchone()
                if r:
                    snap.context_hash = dict(r)["context_hash"]

            # 4.0D dominant family + regime bridge
            cur.execute(
                """
                select dominant_dependency_family
                from public.run_cross_asset_explanation_bridge
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                snap.raw_dominant_family = dict(r).get("dominant_dependency_family")

            # 4.1B weighted integration
            cur.execute(
                """
                select weighted_cross_asset_net_contribution,
                       weighted_dominant_dependency_family
                from public.run_cross_asset_weighted_integration_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            r = cur.fetchone()
            if r:
                d = dict(r)
                snap.weighted_net_contribution = _as_float(d.get("weighted_cross_asset_net_contribution"))
                snap.weighted_dominant_family = d.get("weighted_dominant_dependency_family")

            # 4.1C regime integration
            cur.execute(
                """
                select regime_key,
                       regime_adjusted_cross_asset_contribution,
                       regime_dominant_dependency_family
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
                snap.regime_net_contribution = _as_float(d.get("regime_adjusted_cross_asset_contribution"))
                snap.regime_dominant_family = d.get("regime_dominant_dependency_family")

            # Per-family: 4.1A raw
            cur.execute(
                """
                select dependency_family, family_net_contribution, family_rank
                from public.cross_asset_family_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                snap.raw_family[str(d["dependency_family"])] = {
                    "contribution": _as_float(d.get("family_net_contribution")),
                    "rank":         d.get("family_rank"),
                }

            # Per-family: 4.1B weighted
            cur.execute(
                """
                select dependency_family,
                       weighted_family_net_contribution,
                       weighted_family_rank
                from public.cross_asset_family_weighted_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                snap.weighted_family[str(d["dependency_family"])] = {
                    "contribution": _as_float(d.get("weighted_family_net_contribution")),
                    "rank":         d.get("weighted_family_rank"),
                }

            # Per-family: 4.1C regime
            cur.execute(
                """
                select dependency_family,
                       regime_adjusted_family_contribution,
                       regime_family_rank
                from public.cross_asset_family_regime_attribution_summary
                where run_id = %s::uuid
                """,
                (run_id,),
            )
            for r in cur.fetchall():
                d = dict(r)
                snap.regime_family[str(d["dependency_family"])] = {
                    "contribution": _as_float(d.get("regime_adjusted_family_contribution")),
                    "rank":         d.get("regime_family_rank"),
                }

        return snap

    # ── comparison helpers ──────────────────────────────────────────────
    @staticmethod
    def compare_context_hashes(
        source: RunLayerSnapshot, replay: RunLayerSnapshot,
    ) -> bool:
        if source.context_hash is None and replay.context_hash is None:
            return True
        return source.context_hash == replay.context_hash

    @staticmethod
    def compare_regime_keys(
        source: RunLayerSnapshot, replay: RunLayerSnapshot,
    ) -> bool:
        if source.regime_key is None and replay.regime_key is None:
            return True
        return source.regime_key == replay.regime_key

    def _compare_layer_net(
        self, a: float | None, b: float | None,
    ) -> tuple[bool, dict[str, Any]]:
        match = _num_match(a, b)
        return match, {
            "source": a,
            "replay": b,
            "delta":  _num_delta(a, b),
            "match":  match,
        }

    def _compare_family_map(
        self,
        source_map: dict[str, dict[str, Any]],
        replay_map: dict[str, dict[str, Any]],
    ) -> tuple[bool, dict[str, Any]]:
        """Compare family contribution maps. Returns (all_match, per-family delta dict)."""
        fam_keys = sorted(set(source_map) | set(replay_map))
        per_family: dict[str, Any] = {}
        all_match = True
        for fam in fam_keys:
            s = source_map.get(fam) or {}
            r = replay_map.get(fam) or {}
            sv = _as_float(s.get("contribution"))
            rv = _as_float(r.get("contribution"))
            m = _num_match(sv, rv)
            per_family[fam] = {
                "source":      sv,
                "replay":      rv,
                "delta":       _num_delta(sv, rv),
                "source_rank": s.get("rank"),
                "replay_rank": r.get("rank"),
                "rank_match":  (s.get("rank") == r.get("rank")),
                "match":       m,
            }
            if not m:
                all_match = False
        return all_match, per_family

    def compare_raw_attribution(
        self, source: RunLayerSnapshot, replay: RunLayerSnapshot,
    ) -> tuple[bool, dict[str, Any]]:
        net_match, net_delta = self._compare_layer_net(
            source.raw_net_contribution, replay.raw_net_contribution,
        )
        fam_match, fam_delta = self._compare_family_map(
            source.raw_family, replay.raw_family,
        )
        return (net_match and fam_match, {
            "net":      net_delta,
            "families": fam_delta,
        })

    def compare_weighted_attribution(
        self, source: RunLayerSnapshot, replay: RunLayerSnapshot,
    ) -> tuple[bool, dict[str, Any]]:
        net_match, net_delta = self._compare_layer_net(
            source.weighted_net_contribution, replay.weighted_net_contribution,
        )
        fam_match, fam_delta = self._compare_family_map(
            source.weighted_family, replay.weighted_family,
        )
        return (net_match and fam_match, {
            "net":      net_delta,
            "families": fam_delta,
        })

    def compare_regime_attribution(
        self, source: RunLayerSnapshot, replay: RunLayerSnapshot,
    ) -> tuple[bool, dict[str, Any]]:
        net_match, net_delta = self._compare_layer_net(
            source.regime_net_contribution, replay.regime_net_contribution,
        )
        fam_match, fam_delta = self._compare_family_map(
            source.regime_family, replay.regime_family,
        )
        return (net_match and fam_match, {
            "net":      net_delta,
            "families": fam_delta,
        })

    # ── drift reason code derivation ────────────────────────────────────
    def derive_drift_reason_codes(
        self,
        *,
        context_match: bool,
        regime_match: bool,
        raw_match: bool,
        weighted_match: bool,
        regime_attr_match: bool,
        dominant_match: bool,
        weighted_dominant_match: bool,
        regime_dominant_match: bool,
        raw_delta: dict[str, Any],
        weighted_delta: dict[str, Any],
        regime_delta: dict[str, Any],
        missing_source_layers: list[str],
        missing_replay_layers: list[str],
    ) -> list[str]:
        codes: list[str] = []
        if not context_match:
            codes.append("context_hash_mismatch")
        if not regime_match:
            codes.append("regime_key_mismatch")
        if not raw_match:
            for fam, d in (raw_delta.get("families") or {}).items():
                if not d.get("match"):
                    codes.append(f"raw_family_delta:{fam}")
                    break
            else:
                codes.append("raw_family_delta")
        if not weighted_match:
            codes.append("weighted_family_delta")
        if not regime_attr_match:
            codes.append("regime_family_delta")
        if not dominant_match:
            codes.append("dominant_family_shift")
        if not weighted_dominant_match:
            codes.append("weighted_dominant_family_shift")
        if not regime_dominant_match:
            codes.append("regime_dominant_family_shift")
        for layer in missing_source_layers:
            codes.append(f"missing_source_layer:{layer}")
        for layer in missing_replay_layers:
            codes.append(f"missing_replay_layer:{layer}")
        # Dedupe while preserving order
        seen: set[str] = set()
        deduped: list[str] = []
        for code in codes:
            if code not in seen:
                seen.add(code)
                deduped.append(code)
        return deduped

    # ── family stability derivation ─────────────────────────────────────
    def compute_family_stability(
        self,
        source: RunLayerSnapshot,
        replay: RunLayerSnapshot,
    ) -> list[FamilyStabilityRow]:
        fam_keys = sorted(
            set(source.raw_family) | set(replay.raw_family)
            | set(source.weighted_family) | set(replay.weighted_family)
            | set(source.regime_family) | set(replay.regime_family)
        )
        rows: list[FamilyStabilityRow] = []
        for fam in fam_keys:
            s_raw = source.raw_family.get(fam) or {}
            r_raw = replay.raw_family.get(fam) or {}
            s_w   = source.weighted_family.get(fam) or {}
            r_w   = replay.weighted_family.get(fam) or {}
            s_rg  = source.regime_family.get(fam) or {}
            r_rg  = replay.regime_family.get(fam) or {}

            s_raw_v   = _as_float(s_raw.get("contribution"))
            r_raw_v   = _as_float(r_raw.get("contribution"))
            s_w_v     = _as_float(s_w.get("contribution"))
            r_w_v     = _as_float(r_w.get("contribution"))
            s_rg_v    = _as_float(s_rg.get("contribution"))
            r_rg_v    = _as_float(r_rg.get("contribution"))

            codes: list[str] = []
            if not _num_match(s_raw_v, r_raw_v):
                codes.append("raw_family_delta")
            if not _num_match(s_w_v, r_w_v):
                codes.append("weighted_family_delta")
            if not _num_match(s_rg_v, r_rg_v):
                codes.append("regime_family_delta")
            if s_raw.get("rank") != r_raw.get("rank"):
                codes.append("raw_family_rank_shift")
            if s_w.get("rank") != r_w.get("rank"):
                codes.append("weighted_family_rank_shift")
            if s_rg.get("rank") != r_rg.get("rank"):
                codes.append("regime_family_rank_shift")

            rows.append(FamilyStabilityRow(
                dependency_family=fam,
                source_raw_contribution=s_raw_v,
                replay_raw_contribution=r_raw_v,
                source_weighted_contribution=s_w_v,
                replay_weighted_contribution=r_w_v,
                source_regime_contribution=s_rg_v,
                replay_regime_contribution=r_rg_v,
                raw_delta=_num_delta(s_raw_v, r_raw_v),
                weighted_delta=_num_delta(s_w_v, r_w_v),
                regime_delta=_num_delta(s_rg_v, r_rg_v),
                family_rank_match=(s_raw.get("rank") == r_raw.get("rank")),
                weighted_family_rank_match=(s_w.get("rank") == r_w.get("rank")),
                regime_family_rank_match=(s_rg.get("rank") == r_rg.get("rank")),
                drift_reason_codes=codes,
                metadata={
                    "source_raw_rank":      s_raw.get("rank"),
                    "replay_raw_rank":      r_raw.get("rank"),
                    "source_weighted_rank": s_w.get("rank"),
                    "replay_weighted_rank": r_w.get("rank"),
                    "source_regime_rank":   s_rg.get("rank"),
                    "replay_regime_rank":   r_rg.get("rank"),
                },
            ))
        return rows

    # ── main builder ────────────────────────────────────────────────────
    def build_replay_validation_for_run(
        self,
        conn,
        *,
        replay_run_id: str,
    ) -> ReplayValidationResult | None:
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
        if source.raw_net_contribution is None and not source.raw_family:
            missing_source_layers.append("raw")
        if source.weighted_net_contribution is None and not source.weighted_family:
            missing_source_layers.append("weighted")
        if source.regime_net_contribution is None and not source.regime_family:
            missing_source_layers.append("regime")
        if replay.raw_net_contribution is None and not replay.raw_family:
            missing_replay_layers.append("raw")
        if replay.weighted_net_contribution is None and not replay.weighted_family:
            missing_replay_layers.append("weighted")
        if replay.regime_net_contribution is None and not replay.regime_family:
            missing_replay_layers.append("regime")

        context_match = self.compare_context_hashes(source, replay)
        regime_match  = self.compare_regime_keys(source, replay)
        raw_match,      raw_delta      = self.compare_raw_attribution(source, replay)
        weighted_match, weighted_delta = self.compare_weighted_attribution(source, replay)
        regime_attr_match, regime_delta = self.compare_regime_attribution(source, replay)

        dominant_match          = (source.raw_dominant_family == replay.raw_dominant_family)
        weighted_dominant_match = (source.weighted_dominant_family == replay.weighted_dominant_family)
        regime_dominant_match   = (source.regime_dominant_family == replay.regime_dominant_family)

        drift_codes = self.derive_drift_reason_codes(
            context_match=context_match,
            regime_match=regime_match,
            raw_match=raw_match,
            weighted_match=weighted_match,
            regime_attr_match=regime_attr_match,
            dominant_match=dominant_match,
            weighted_dominant_match=weighted_dominant_match,
            regime_dominant_match=regime_dominant_match,
            raw_delta=raw_delta,
            weighted_delta=weighted_delta,
            regime_delta=regime_delta,
            missing_source_layers=missing_source_layers,
            missing_replay_layers=missing_replay_layers,
        )

        # Validation state
        if missing_source_layers == ["raw", "weighted", "regime"]:
            validation_state = "insufficient_source"
        elif missing_replay_layers == ["raw", "weighted", "regime"]:
            validation_state = "insufficient_replay"
        elif not context_match and not any(
            c for c in drift_codes
            if c.startswith(("raw_family_delta", "weighted_family_delta", "regime_family_delta",
                             "dominant_family_shift", "weighted_dominant_family_shift",
                             "regime_dominant_family_shift"))
        ):
            validation_state = "context_mismatch"
        elif drift_codes:
            validation_state = "drift_detected"
        else:
            validation_state = "validated"

        family_rows = self.compute_family_stability(source, replay)

        metadata: dict[str, Any] = {
            "scoring_version":        _SCORING_VERSION,
            "numeric_tolerance":      _NUMERIC_TOLERANCE,
            "missing_source_layers":  missing_source_layers,
            "missing_replay_layers":  missing_replay_layers,
            "is_replay":              lineage.get("is_replay"),
        }

        return ReplayValidationResult(
            workspace_id=lineage["workspace_id"],
            watchlist_id=lineage["watchlist_id"],
            source_run_id=lineage["source_run_id"],
            replay_run_id=lineage["replay_run_id"],
            source_context_snapshot_id=source.context_snapshot_id,
            replay_context_snapshot_id=replay.context_snapshot_id,
            source_regime_key=source.regime_key,
            replay_regime_key=replay.regime_key,
            context_hash_match=context_match,
            regime_match=regime_match,
            raw_attribution_match=raw_match,
            weighted_attribution_match=weighted_match,
            regime_attribution_match=regime_attr_match,
            dominant_family_match=dominant_match,
            weighted_dominant_family_match=weighted_dominant_match,
            regime_dominant_family_match=regime_dominant_match,
            raw_delta=raw_delta,
            weighted_delta=weighted_delta,
            regime_delta=regime_delta,
            drift_reason_codes=drift_codes,
            validation_state=validation_state,
            metadata=metadata,
            family_rows=family_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_replay_validation(
        self, conn, *, result: ReplayValidationResult,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_replay_validation_snapshot(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            source_run_id=result.source_run_id,
            replay_run_id=result.replay_run_id,
            source_context_snapshot_id=result.source_context_snapshot_id,
            replay_context_snapshot_id=result.replay_context_snapshot_id,
            source_regime_key=result.source_regime_key,
            replay_regime_key=result.replay_regime_key,
            context_hash_match=result.context_hash_match,
            regime_match=result.regime_match,
            raw_attribution_match=result.raw_attribution_match,
            weighted_attribution_match=result.weighted_attribution_match,
            regime_attribution_match=result.regime_attribution_match,
            dominant_family_match=result.dominant_family_match,
            weighted_dominant_family_match=result.weighted_dominant_family_match,
            regime_dominant_family_match=result.regime_dominant_family_match,
            raw_delta=result.raw_delta,
            weighted_delta=result.weighted_delta,
            regime_delta=result.regime_delta,
            drift_reason_codes=result.drift_reason_codes,
            validation_state=result.validation_state,
            metadata=result.metadata,
        )
        return str(row["id"])

    def persist_family_stability(
        self, conn, *, result: ReplayValidationResult,
    ) -> int:
        if not result.family_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_replay_stability_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            source_run_id=result.source_run_id,
            replay_run_id=result.replay_run_id,
            rows=[
                {
                    "dependency_family":              fr.dependency_family,
                    "source_raw_contribution":        fr.source_raw_contribution,
                    "replay_raw_contribution":        fr.replay_raw_contribution,
                    "source_weighted_contribution":   fr.source_weighted_contribution,
                    "replay_weighted_contribution":   fr.replay_weighted_contribution,
                    "source_regime_contribution":     fr.source_regime_contribution,
                    "replay_regime_contribution":     fr.replay_regime_contribution,
                    "raw_delta":                      fr.raw_delta,
                    "weighted_delta":                 fr.weighted_delta,
                    "regime_delta":                   fr.regime_delta,
                    "family_rank_match":              fr.family_rank_match,
                    "weighted_family_rank_match":     fr.weighted_family_rank_match,
                    "regime_family_rank_match":       fr.regime_family_rank_match,
                    "drift_reason_codes":             fr.drift_reason_codes,
                    "metadata":                       fr.metadata,
                }
                for fr in result.family_rows
            ],
        )

    def build_and_persist(
        self, conn, *, replay_run_id: str,
    ) -> ReplayValidationResult | None:
        result = self.build_replay_validation_for_run(
            conn, replay_run_id=replay_run_id,
        )
        if result is None:
            return None
        self.persist_replay_validation(conn, result=result)
        self.persist_family_stability(conn, result=result)
        return result

    def refresh_replay_validation_for_run(
        self, conn, *, replay_run_id: str,
    ) -> ReplayValidationResult | None:
        """Entrypoint used by the consumer. Only acts when the run is an
        actual replay with a resolvable source. Commits on success."""
        try:
            result = self.build_and_persist(conn, replay_run_id=replay_run_id)
            if result is not None:
                conn.commit()
            return result
        except Exception as exc:
            logger.warning(
                "cross_asset_replay_validation: run=%s build/persist failed: %s",
                replay_run_id, exc,
            )
            conn.rollback()
            return None
