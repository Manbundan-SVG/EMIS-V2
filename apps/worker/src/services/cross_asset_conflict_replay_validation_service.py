"""Phase 4.8D: Replay Validation for Conflict-Aware Behavior.

Compares a source run's conflict-aware attribution + composite to its
replay counterpart and persists explicit drift diagnostics. Mirrors the
4.7D decay replay-validation pattern but extends comparisons to consensus
state, agreement / conflict scores, dominant conflict source, the 4.8C
replay-readiness fields (source_contribution_layer, source_composite_layer,
scoring_version), conflict-aware attribution, conflict-aware composite,
and conflict dominant family.

Persists:
  * cross_asset_conflict_replay_validation_snapshots
  * cross_asset_family_conflict_replay_stability_snapshots

All comparison logic is deterministic, side-by-side, and metadata-stamped.
Numeric tolerances are stamped on every row for audit. No predictive
behavior in this phase.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.8D.v1"

# Explicit numeric tolerances.
_CONTRIBUTION_TOLERANCE = 1e-9   # conflict-adjusted / conflict integration / composite
_SCORE_TOLERANCE        = 1e-6   # agreement_score / conflict_score


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _eq_str(a: Any, b: Any) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return str(a) == str(b)


def _eq_num(a: Any, b: Any, tol: float) -> bool:
    fa, fb = _as_float(a), _as_float(b)
    if fa is None and fb is None:
        return True
    if fa is None or fb is None:
        return False
    return abs(fa - fb) <= tol


class CrossAssetConflictReplayValidationService:
    """Phase 4.8D — conflict replay validation service."""

    # ── source/replay loaders ───────────────────────────────────────────
    def load_source_and_replay_runs(
        self, conn, *, run_id: str, workspace_id: str,
    ) -> dict | None:
        """Returns {source_run_id, replay_run_id, workspace_id} or None.

        ``run_id`` is the replay run; if it has no ``replayed_from_run_id``,
        returns None.
        """
        import src.db.repositories_48d as repo
        row = repo.get_replay_pair_for_run(conn, run_id=run_id, workspace_id=workspace_id)
        if not row or not row.get("source_run_id"):
            return None
        return {
            "source_run_id": row["source_run_id"],
            "replay_run_id": row["run_id"],
            "workspace_id":  row["workspace_id"],
        }

    # ── comparison primitives ───────────────────────────────────────────
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
    def compare_freshness_states(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_layer_consensus_states(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_agreement_scores(src: Any, rep: Any) -> bool:
        return _eq_num(src, rep, _SCORE_TOLERANCE)

    @staticmethod
    def compare_conflict_scores(src: Any, rep: Any) -> bool:
        return _eq_num(src, rep, _SCORE_TOLERANCE)

    @staticmethod
    def compare_dominant_conflict_sources(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_source_contribution_layers(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_source_composite_layers(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_scoring_versions(src: str | None, rep: str | None) -> bool:
        return _eq_str(src, rep)

    @staticmethod
    def compare_conflict_attribution(src: Any, rep: Any) -> bool:
        return _eq_num(src, rep, _CONTRIBUTION_TOLERANCE)

    @staticmethod
    def compare_conflict_composite(src: Any, rep: Any) -> bool:
        return _eq_num(src, rep, _CONTRIBUTION_TOLERANCE)

    @staticmethod
    def compare_conflict_dominant_family(src_family: str | None, rep_family: str | None) -> bool:
        return _eq_str(src_family, rep_family)

    # ── family comparison ───────────────────────────────────────────────
    def compute_family_conflict_stability(
        self,
        *,
        source_attr_rows: list[dict],
        replay_attr_rows: list[dict],
        source_comp_rows: list[dict],
        replay_comp_rows: list[dict],
        workspace_id: str,
        watchlist_id: str,
        source_run_id: str,
        replay_run_id: str,
    ) -> list[dict]:
        """Build family-level stability rows comparing 4.8B + 4.8C surfaces."""
        def _index(rows: list[dict], by: str = "dependency_family") -> dict[str, dict]:
            return {str(r.get(by) or ""): r for r in rows if r.get(by)}

        src_attr = _index(source_attr_rows)
        rep_attr = _index(replay_attr_rows)
        src_comp = _index(source_comp_rows)
        rep_comp = _index(replay_comp_rows)

        all_families = sorted(set(src_attr) | set(rep_attr) | set(src_comp) | set(rep_comp))

        rows: list[dict] = []
        for fam in all_families:
            sa = src_attr.get(fam, {})
            ra = rep_attr.get(fam, {})
            sc = src_comp.get(fam, {})
            rc = rep_comp.get(fam, {})

            src_consensus = sc.get("family_consensus_state") or sa.get("family_consensus_state")
            rep_consensus = rc.get("family_consensus_state") or ra.get("family_consensus_state")
            src_agree = _as_float(sc.get("agreement_score") if sc else None) \
                        or _as_float(sa.get("agreement_score"))
            rep_agree = _as_float(rc.get("agreement_score") if rc else None) \
                        or _as_float(ra.get("agreement_score"))
            src_conf  = _as_float(sc.get("conflict_score") if sc else None) \
                        or _as_float(sa.get("conflict_score"))
            rep_conf  = _as_float(rc.get("conflict_score") if rc else None) \
                        or _as_float(ra.get("conflict_score"))
            src_dom   = sc.get("dominant_conflict_source") or sa.get("dominant_conflict_source")
            rep_dom   = rc.get("dominant_conflict_source") or ra.get("dominant_conflict_source")

            src_contribution_layer = sc.get("source_contribution_layer")
            rep_contribution_layer = rc.get("source_contribution_layer")
            src_scoring = sc.get("scoring_version")
            rep_scoring = rc.get("scoring_version")

            src_attr_contrib = _as_float(sa.get("conflict_adjusted_family_contribution"))
            rep_attr_contrib = _as_float(ra.get("conflict_adjusted_family_contribution"))
            src_int_contrib  = _as_float(sc.get("conflict_integration_contribution"))
            rep_int_contrib  = _as_float(rc.get("conflict_integration_contribution"))

            attr_delta = None
            if src_attr_contrib is not None and rep_attr_contrib is not None:
                attr_delta = rep_attr_contrib - src_attr_contrib
            int_delta = None
            if src_int_contrib is not None and rep_int_contrib is not None:
                int_delta = rep_int_contrib - src_int_contrib

            consensus_match = _eq_str(src_consensus, rep_consensus)
            agree_match     = _eq_num(src_agree, rep_agree, _SCORE_TOLERANCE)
            conf_match      = _eq_num(src_conf, rep_conf, _SCORE_TOLERANCE)
            dom_match       = _eq_str(src_dom, rep_dom)
            contribution_layer_match = _eq_str(src_contribution_layer, rep_contribution_layer)
            scoring_version_match    = _eq_str(src_scoring, rep_scoring)

            attr_rank_match = _eq_num(sa.get("conflict_family_rank"), ra.get("conflict_family_rank"), 0)
            comp_rank_match = _eq_num(sc.get("family_rank"), rc.get("family_rank"), 0)

            reason_codes: list[str] = []
            if not consensus_match:
                reason_codes.append("family_consensus_state_mismatch")
            if not agree_match:
                reason_codes.append("agreement_score_mismatch")
            if not conf_match:
                reason_codes.append("conflict_score_mismatch")
            if not dom_match:
                reason_codes.append("dominant_conflict_source_mismatch")
            if not contribution_layer_match:
                reason_codes.append("source_contribution_layer_mismatch")
            if not scoring_version_match:
                reason_codes.append("scoring_version_mismatch")
            if attr_delta is not None and abs(attr_delta) > _CONTRIBUTION_TOLERANCE:
                reason_codes.append("conflict_family_delta")
            if int_delta is not None and abs(int_delta) > _CONTRIBUTION_TOLERANCE:
                reason_codes.append("conflict_integration_delta")
            if not attr_rank_match:
                reason_codes.append("conflict_family_rank_mismatch")
            if not comp_rank_match:
                reason_codes.append("conflict_composite_family_rank_mismatch")
            if fam not in src_attr:
                reason_codes.append("missing_source_conflict_layer:attribution")
            if fam not in rep_attr:
                reason_codes.append("missing_replay_conflict_layer:attribution")
            if fam not in src_comp:
                reason_codes.append("missing_source_conflict_layer:composite")
            if fam not in rep_comp:
                reason_codes.append("missing_replay_conflict_layer:composite")

            rows.append({
                "workspace_id":  workspace_id,
                "watchlist_id":  watchlist_id,
                "source_run_id": source_run_id,
                "replay_run_id": replay_run_id,
                "dependency_family": fam,
                "source_family_consensus_state": src_consensus,
                "replay_family_consensus_state": rep_consensus,
                "source_agreement_score": src_agree,
                "replay_agreement_score": rep_agree,
                "source_conflict_score":  src_conf,
                "replay_conflict_score":  rep_conf,
                "source_dominant_conflict_source": src_dom,
                "replay_dominant_conflict_source": rep_dom,
                "source_contribution_layer": src_contribution_layer,
                "replay_contribution_layer": rep_contribution_layer,
                "source_scoring_version": src_scoring,
                "replay_scoring_version": rep_scoring,
                "source_conflict_adjusted_contribution":   src_attr_contrib,
                "replay_conflict_adjusted_contribution":   rep_attr_contrib,
                "source_conflict_integration_contribution": src_int_contrib,
                "replay_conflict_integration_contribution": rep_int_contrib,
                "conflict_adjusted_delta":   attr_delta,
                "conflict_integration_delta": int_delta,
                "family_consensus_state_match":     consensus_match,
                "agreement_score_match":            agree_match,
                "conflict_score_match":             conf_match,
                "dominant_conflict_source_match":   dom_match,
                "source_contribution_layer_match": contribution_layer_match,
                "scoring_version_match":            scoring_version_match,
                "conflict_family_rank_match":       attr_rank_match,
                "conflict_composite_family_rank_match": comp_rank_match,
                "drift_reason_codes": list(dict.fromkeys(reason_codes)),
                "metadata": {
                    "scoring_version": _SCORING_VERSION,
                    "tolerances": {
                        "score": _SCORE_TOLERANCE,
                        "contribution": _CONTRIBUTION_TOLERANCE,
                    },
                },
            })

        return rows

    # ── drift reason / validation state derivation ──────────────────────
    @staticmethod
    def derive_conflict_drift_reason_codes(*, flags: dict[str, bool]) -> list[str]:
        out: list[str] = []
        mapping = [
            ("context_hash_match",            "context_hash_mismatch"),
            ("regime_match",                  "regime_key_mismatch"),
            ("timing_class_match",            "timing_class_mismatch"),
            ("transition_state_match",        "transition_state_mismatch"),
            ("sequence_class_match",          "sequence_class_mismatch"),
            ("archetype_match",               "archetype_key_mismatch"),
            ("cluster_state_match",           "cluster_state_mismatch"),
            ("persistence_state_match",       "persistence_state_mismatch"),
            ("freshness_state_match",         "freshness_state_mismatch"),
            ("layer_consensus_state_match",   "layer_consensus_state_mismatch"),
            ("agreement_score_match",         "agreement_score_mismatch"),
            ("conflict_score_match",          "conflict_score_mismatch"),
            ("dominant_conflict_source_match", "dominant_conflict_source_mismatch"),
            ("source_contribution_layer_match", "source_contribution_layer_mismatch"),
            ("source_composite_layer_match",  "source_composite_layer_mismatch"),
            ("scoring_version_match",         "scoring_version_mismatch"),
            ("conflict_attribution_match",    "conflict_family_delta"),
            ("conflict_composite_match",      "conflict_integration_delta"),
            ("conflict_dominant_family_match", "conflict_dominant_family_shift"),
        ]
        for flag, code in mapping:
            if not flags.get(flag, False):
                out.append(code)
        return out

    @staticmethod
    def _derive_validation_state(
        *, flags: dict[str, bool],
        source_present: bool, replay_present: bool,
    ) -> str:
        if not source_present:
            return "insufficient_source"
        if not replay_present:
            return "insufficient_replay"
        if not flags["context_hash_match"]:
            return "context_mismatch"
        # Upstream-state mismatches in priority order
        if not flags["timing_class_match"]:
            return "timing_mismatch"
        if not (flags["transition_state_match"] and flags["sequence_class_match"]):
            return "transition_mismatch"
        if not flags["archetype_match"]:
            return "archetype_mismatch"
        if not flags["cluster_state_match"]:
            return "cluster_mismatch"
        if not flags["persistence_state_match"]:
            return "persistence_mismatch"
        if not flags["freshness_state_match"]:
            return "decay_mismatch"
        # Conflict-state / metadata mismatches
        if not (flags["layer_consensus_state_match"] and flags["agreement_score_match"]
                and flags["conflict_score_match"] and flags["dominant_conflict_source_match"]
                and flags["source_contribution_layer_match"]
                and flags["source_composite_layer_match"]
                and flags["scoring_version_match"]):
            return "conflict_mismatch"
        # Final-layer drift
        if not (flags["conflict_attribution_match"] and flags["conflict_composite_match"]
                and flags["conflict_dominant_family_match"]):
            return "drift_detected"
        return "validated"

    # ── persistence ─────────────────────────────────────────────────────
    def persist_conflict_replay_validation(self, conn, *, payload: dict) -> str:
        import src.db.repositories_48d as repo
        row = repo.insert_cross_asset_conflict_replay_validation_snapshot(conn, **payload)
        return str(row["id"])

    def persist_family_conflict_stability(
        self, conn, *, rows: list[dict],
    ) -> list[str]:
        if not rows:
            return []
        import src.db.repositories_48d as repo
        out = repo.insert_cross_asset_family_conflict_replay_stability_snapshots(
            conn, rows=rows,
        )
        return [str(r["id"]) for r in out]

    # ── orchestration ───────────────────────────────────────────────────
    def refresh_conflict_replay_validation_for_run(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> int:
        """Run the validation flow if this run is a replay; no-op otherwise.

        Returns 1 on persisted validation, 0 on skip / no replay lineage.
        """
        import src.db.repositories_48d as repo

        pair = self.load_source_and_replay_runs(
            conn, run_id=run_id, workspace_id=workspace_id,
        )
        if not pair:
            return 0
        source_run_id = pair["source_run_id"]
        replay_run_id = pair["replay_run_id"]

        # Source/replay 4.8B/C run-level rows.
        src_attr = repo.get_cross_asset_conflict_attribution_for_run(
            conn, run_id=source_run_id, workspace_id=workspace_id,
        )
        rep_attr = repo.get_cross_asset_conflict_attribution_for_run(
            conn, run_id=replay_run_id, workspace_id=workspace_id,
        )
        src_comp = repo.get_cross_asset_conflict_composite_for_run(
            conn, run_id=source_run_id, workspace_id=workspace_id,
        )
        rep_comp = repo.get_cross_asset_conflict_composite_for_run(
            conn, run_id=replay_run_id, workspace_id=workspace_id,
        )

        # Bail-out conditions (still persist the row so ops sees the
        # insufficient state explicitly).
        source_present = bool(src_attr or src_comp)
        replay_present = bool(rep_attr or rep_comp)

        # Watchlist comes from the conflict surfaces; require both sides.
        watchlist_id = (
            (src_comp or {}).get("watchlist_id")
            or (rep_comp or {}).get("watchlist_id")
            or (src_attr or {}).get("watchlist_id")
            or (rep_attr or {}).get("watchlist_id")
        )
        if not watchlist_id:
            logger.debug(
                "conflict_replay_validation: no watchlist for run=%s/%s; skipping",
                source_run_id, replay_run_id,
            )
            return 0

        # Upstream state per side.
        src_state = repo.get_context_snapshot_for_run(
            conn, run_id=source_run_id, workspace_id=workspace_id,
        ) or {}
        rep_state = repo.get_context_snapshot_for_run(
            conn, run_id=replay_run_id, workspace_id=workspace_id,
        ) or {}
        src_conflict_state = repo.get_conflict_state_for_run(
            conn, run_id=source_run_id, workspace_id=workspace_id,
        ) or {}
        rep_conflict_state = repo.get_conflict_state_for_run(
            conn, run_id=replay_run_id, workspace_id=workspace_id,
        ) or {}

        src_ctx_id = (src_comp or {}).get("context_snapshot_id")
        rep_ctx_id = (rep_comp or {}).get("context_snapshot_id")

        # Match flags.
        flags = {
            "context_hash_match":           self.compare_context_hashes(src_ctx_id, rep_ctx_id),
            "regime_match":                 self.compare_regime_keys(
                src_state.get("regime_key"), rep_state.get("regime_key")
            ),
            "timing_class_match":           self.compare_dominant_timing_classes(
                src_state.get("dominant_timing_class"), rep_state.get("dominant_timing_class")
            ),
            "transition_state_match":       self.compare_transition_states(
                src_state.get("dominant_transition_state"), rep_state.get("dominant_transition_state")
            ),
            "sequence_class_match":         self.compare_sequence_classes(
                src_state.get("dominant_sequence_class"), rep_state.get("dominant_sequence_class")
            ),
            "archetype_match":              self.compare_archetype_keys(
                src_state.get("dominant_archetype_key"), rep_state.get("dominant_archetype_key")
            ),
            "cluster_state_match":          self.compare_cluster_states(
                src_state.get("cluster_state"), rep_state.get("cluster_state")
            ),
            "persistence_state_match":      self.compare_persistence_states(
                src_state.get("persistence_state"), rep_state.get("persistence_state")
            ),
            "freshness_state_match":        self.compare_freshness_states(
                src_state.get("freshness_state"), rep_state.get("freshness_state")
            ),
            "layer_consensus_state_match":  self.compare_layer_consensus_states(
                (src_comp or src_attr or {}).get("layer_consensus_state"),
                (rep_comp or rep_attr or {}).get("layer_consensus_state"),
            ),
            "agreement_score_match":        self.compare_agreement_scores(
                (src_comp or src_attr or {}).get("agreement_score"),
                (rep_comp or rep_attr or {}).get("agreement_score"),
            ),
            "conflict_score_match":         self.compare_conflict_scores(
                (src_comp or src_attr or {}).get("conflict_score"),
                (rep_comp or rep_attr or {}).get("conflict_score"),
            ),
            "dominant_conflict_source_match": self.compare_dominant_conflict_sources(
                (src_comp or src_attr or {}).get("dominant_conflict_source"),
                (rep_comp or rep_attr or {}).get("dominant_conflict_source"),
            ),
            "source_contribution_layer_match": self.compare_source_contribution_layers(
                (src_comp or {}).get("source_contribution_layer"),
                (rep_comp or {}).get("source_contribution_layer"),
            ),
            "source_composite_layer_match":   self.compare_source_composite_layers(
                (src_comp or {}).get("source_composite_layer"),
                (rep_comp or {}).get("source_composite_layer"),
            ),
            "scoring_version_match":         self.compare_scoring_versions(
                (src_comp or {}).get("scoring_version"),
                (rep_comp or {}).get("scoring_version"),
            ),
            "conflict_attribution_match":     self.compare_conflict_attribution(
                (src_attr or {}).get("conflict_adjusted_cross_asset_contribution"),
                (rep_attr or {}).get("conflict_adjusted_cross_asset_contribution"),
            ),
            "conflict_composite_match":       self.compare_conflict_composite(
                (src_comp or {}).get("composite_post_conflict"),
                (rep_comp or {}).get("composite_post_conflict"),
            ),
            "conflict_dominant_family_match": True,  # filled below from family data
        }

        # Family rows.
        src_attr_fam = repo.get_family_conflict_attribution_for_run(conn, run_id=source_run_id)
        rep_attr_fam = repo.get_family_conflict_attribution_for_run(conn, run_id=replay_run_id)
        src_comp_fam = repo.get_family_conflict_composite_for_run(conn, run_id=source_run_id)
        rep_comp_fam = repo.get_family_conflict_composite_for_run(conn, run_id=replay_run_id)

        # Dominant family by rank=1.
        def _dominant(rows: list[dict], rank_key: str) -> str | None:
            for r in rows:
                if r.get(rank_key) == 1:
                    return r.get("dependency_family")
            return None

        src_dominant_family = _dominant(src_attr_fam, "conflict_family_rank") \
                              or _dominant(src_comp_fam, "family_rank")
        rep_dominant_family = _dominant(rep_attr_fam, "conflict_family_rank") \
                              or _dominant(rep_comp_fam, "family_rank")
        flags["conflict_dominant_family_match"] = self.compare_conflict_dominant_family(
            src_dominant_family, rep_dominant_family,
        )

        # Conflict deltas (run-level, JSON-stamped).
        src_attr_total = _as_float((src_attr or {}).get("conflict_adjusted_cross_asset_contribution"))
        rep_attr_total = _as_float((rep_attr or {}).get("conflict_adjusted_cross_asset_contribution"))
        src_comp_post  = _as_float((src_comp or {}).get("composite_post_conflict"))
        rep_comp_post  = _as_float((rep_comp or {}).get("composite_post_conflict"))
        src_comp_pre   = _as_float((src_comp or {}).get("composite_pre_conflict"))
        rep_comp_pre   = _as_float((rep_comp or {}).get("composite_pre_conflict"))
        src_comp_net   = _as_float((src_comp or {}).get("conflict_net_contribution"))
        rep_comp_net   = _as_float((rep_comp or {}).get("conflict_net_contribution"))

        def _delta(s, r):
            if s is None or r is None:
                return None
            return r - s

        conflict_delta = {
            "source_conflict_adjusted_total": src_attr_total,
            "replay_conflict_adjusted_total": rep_attr_total,
            "delta": _delta(src_attr_total, rep_attr_total),
            "source_dominant_family": src_dominant_family,
            "replay_dominant_family": rep_dominant_family,
        }
        conflict_composite_delta = {
            "source_composite_post_conflict": src_comp_post,
            "replay_composite_post_conflict": rep_comp_post,
            "composite_post_delta": _delta(src_comp_post, rep_comp_post),
            "source_composite_pre_conflict": src_comp_pre,
            "replay_composite_pre_conflict": rep_comp_pre,
            "composite_pre_delta": _delta(src_comp_pre, rep_comp_pre),
            "source_conflict_net_contribution": src_comp_net,
            "replay_conflict_net_contribution": rep_comp_net,
            "conflict_net_delta": _delta(src_comp_net, rep_comp_net),
        }

        drift_reason_codes = self.derive_conflict_drift_reason_codes(flags=flags)
        if not source_present:
            drift_reason_codes.append("missing_source_conflict_layer:attribution")
            drift_reason_codes.append("missing_source_conflict_layer:composite")
        if not replay_present:
            drift_reason_codes.append("missing_replay_conflict_layer:attribution")
            drift_reason_codes.append("missing_replay_conflict_layer:composite")
        drift_reason_codes = list(dict.fromkeys(drift_reason_codes))

        validation_state = self._derive_validation_state(
            flags=flags,
            source_present=source_present,
            replay_present=replay_present,
        )

        payload = {
            "workspace_id": workspace_id,
            "watchlist_id": watchlist_id,
            "source_run_id": source_run_id,
            "replay_run_id": replay_run_id,
            "source_context_snapshot_id": src_ctx_id,
            "replay_context_snapshot_id": rep_ctx_id,
            "source_regime_key": src_state.get("regime_key"),
            "replay_regime_key": rep_state.get("regime_key"),
            "source_dominant_timing_class": src_state.get("dominant_timing_class"),
            "replay_dominant_timing_class": rep_state.get("dominant_timing_class"),
            "source_dominant_transition_state": src_state.get("dominant_transition_state"),
            "replay_dominant_transition_state": rep_state.get("dominant_transition_state"),
            "source_dominant_sequence_class": src_state.get("dominant_sequence_class"),
            "replay_dominant_sequence_class": rep_state.get("dominant_sequence_class"),
            "source_dominant_archetype_key": src_state.get("dominant_archetype_key"),
            "replay_dominant_archetype_key": rep_state.get("dominant_archetype_key"),
            "source_cluster_state": src_state.get("cluster_state"),
            "replay_cluster_state": rep_state.get("cluster_state"),
            "source_persistence_state": src_state.get("persistence_state"),
            "replay_persistence_state": rep_state.get("persistence_state"),
            "source_freshness_state": src_state.get("freshness_state"),
            "replay_freshness_state": rep_state.get("freshness_state"),
            "source_layer_consensus_state": (src_comp or src_attr or {}).get("layer_consensus_state"),
            "replay_layer_consensus_state": (rep_comp or rep_attr or {}).get("layer_consensus_state"),
            "source_agreement_score": (src_comp or src_attr or {}).get("agreement_score"),
            "replay_agreement_score": (rep_comp or rep_attr or {}).get("agreement_score"),
            "source_conflict_score":  (src_comp or src_attr or {}).get("conflict_score"),
            "replay_conflict_score":  (rep_comp or rep_attr or {}).get("conflict_score"),
            "source_dominant_conflict_source": (src_comp or src_attr or {}).get("dominant_conflict_source"),
            "replay_dominant_conflict_source": (rep_comp or rep_attr or {}).get("dominant_conflict_source"),
            "source_contribution_layer": (src_comp or {}).get("source_contribution_layer"),
            "replay_contribution_layer": (rep_comp or {}).get("source_contribution_layer"),
            "source_composite_layer":    (src_comp or {}).get("source_composite_layer"),
            "replay_composite_layer":    (rep_comp or {}).get("source_composite_layer"),
            "source_scoring_version":    (src_comp or {}).get("scoring_version"),
            "replay_scoring_version":    (rep_comp or {}).get("scoring_version"),
            **flags,
            "conflict_delta": conflict_delta,
            "conflict_composite_delta": conflict_composite_delta,
            "drift_reason_codes": drift_reason_codes,
            "validation_state": validation_state,
            "metadata": {
                "scoring_version": _SCORING_VERSION,
                "tolerances": {
                    "score": _SCORE_TOLERANCE,
                    "contribution": _CONTRIBUTION_TOLERANCE,
                },
            },
        }

        try:
            self.persist_conflict_replay_validation(conn, payload=payload)
            family_rows = self.compute_family_conflict_stability(
                source_attr_rows=src_attr_fam,
                replay_attr_rows=rep_attr_fam,
                source_comp_rows=src_comp_fam,
                replay_comp_rows=rep_comp_fam,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                source_run_id=source_run_id,
                replay_run_id=replay_run_id,
            )
            self.persist_family_conflict_stability(conn, rows=family_rows)
            conn.commit()
            return 1
        except Exception as exc:
            logger.warning(
                "conflict_replay_validation: persist failed for source=%s replay=%s: %s",
                source_run_id, replay_run_id, exc,
            )
            conn.rollback()
            return 0
