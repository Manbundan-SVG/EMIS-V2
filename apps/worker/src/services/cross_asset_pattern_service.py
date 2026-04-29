"""Phase 4.4A: Sequencing Pattern Registry and Regime-Conditioned Transition
Archetypes.

Classifies per-family and per-run transition/sequence state into a small,
explicit controlled vocabulary of archetypes from
`cross_asset_transition_archetype_registry`. Persists:

  * cross_asset_family_archetype_snapshots (one per family)
  * cross_asset_run_archetype_snapshots (one per run)

Classification is deterministic. Each decision carries explicit reason codes
so the downstream analytics and operators can audit why a given family/run
was mapped to a given archetype.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.4A.v1"

# Built-in fallback registry, used only if the DB registry is unavailable.
_FALLBACK_REGISTRY: tuple[dict[str, Any], ...] = (
    {"archetype_key": "rotation_handoff",          "archetype_family": "rotation"},
    {"archetype_key": "reinforcing_continuation",  "archetype_family": "reinforcement"},
    {"archetype_key": "recovering_reentry",        "archetype_family": "recovery"},
    {"archetype_key": "deteriorating_breakdown",   "archetype_family": "degradation"},
    {"archetype_key": "mixed_transition_noise",    "archetype_family": "mixed"},
    {"archetype_key": "insufficient_history",      "archetype_family": "none"},
)

# Run-level dominant-archetype priority for tie-break when two families tie.
_ARCHETYPE_PRIORITY: dict[str, int] = {
    "reinforcing_continuation": 5,
    "rotation_handoff":         4,
    "recovering_reentry":       4,
    "deteriorating_breakdown":  3,
    "mixed_transition_noise":   2,
    "insufficient_history":     1,
}

# Structural family priority (matches prior 4.x layers).
_FAMILY_PRIORITY: dict[str, int] = {
    "macro":        100,
    "rates":         95,
    "fx":            90,
    "equity_index":  85,
    "risk":          85,
    "crypto_cross":  75,
    "commodity":     70,
}


@dataclass
class FamilyArchetype:
    dependency_family: str
    regime_key: str | None
    archetype_key: str
    transition_state: str
    dominant_sequence_class: str
    dominant_timing_class: str | None
    family_rank: int | None
    family_contribution: float | None
    archetype_confidence: float
    classification_reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunArchetypeResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    regime_key: str | None
    dominant_archetype_key: str
    dominant_dependency_family: str | None
    dominant_transition_state: str | None
    dominant_sequence_class: str | None
    archetype_confidence: float
    rotation_event_count: int
    recovery_event_count: int
    degradation_event_count: int
    mixed_event_count: int
    classification_reason_codes: list[str]
    metadata: dict[str, Any]
    family_rows: list[FamilyArchetype]


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


class CrossAssetPatternService:
    """Deterministic pattern-registry-driven archetype classification."""

    # ── registry loading ────────────────────────────────────────────────
    def load_active_archetype_registry(
        self, conn,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, archetype_key, archetype_label,
                       archetype_family, description, classification_rules,
                       is_active, metadata, created_at
                from public.cross_asset_transition_archetype_registry
                where is_active = true
                order by archetype_key
                """,
            )
            rows = [dict(r) for r in cur.fetchall()]
        if rows:
            return rows
        logger.warning("cross_asset_pattern: no active archetype registry rows; using fallback")
        return list(_FALLBACK_REGISTRY)

    # ── input loading ───────────────────────────────────────────────────
    def _load_run_context(
        self, conn, *, run_id: str,
    ) -> dict[str, Any]:
        ctx: dict[str, Any] = {
            "context_snapshot_id": None,
            "regime_key":          None,
            "dominant_timing_class": None,
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
        return ctx

    def _load_family_transition_state(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, dict[str, Any]]:
        """Keyed by dependency_family; carries transition_state, rank,
        sequence_class, timing class, contribution, and whether event types
        are present for the family."""
        out: dict[str, dict[str, Any]] = {}
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family, transition_state,
                       dominant_timing_class, family_rank,
                       family_contribution
                from public.cross_asset_family_transition_state_summary
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
                out[fam]["transition_state"]      = d.get("transition_state")
                out[fam]["dominant_timing_class"] = d.get("dominant_timing_class")
                out[fam]["family_rank"]           = d.get("family_rank")
                out[fam]["family_contribution"]   = _as_float(d.get("family_contribution"))

            cur.execute(
                """
                select dependency_family, dominant_sequence_class,
                       sequence_confidence
                from public.cross_asset_family_sequence_summary
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
                out[fam]["dominant_sequence_class"] = d.get("dominant_sequence_class")
                out[fam]["sequence_confidence"]     = _as_float(d.get("sequence_confidence"))

            # Event types per family (target run): presence is used as a
            # classification signal (dominance_gain/loss, rotation, recovery,
            # degradation).
            cur.execute(
                """
                select dependency_family, event_type
                from public.cross_asset_family_transition_event_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and target_run_id = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            for r in cur.fetchall():
                d = dict(r)
                fam = str(d["dependency_family"])
                out.setdefault(fam, {})
                events = out[fam].setdefault("event_types", set())
                if d.get("event_type"):
                    events.add(str(d["event_type"]))

            # Pull transition-aware attribution rank/contribution when it's
            # available — 4.3B is the authoritative ranking for run dominance.
            cur.execute(
                """
                select dependency_family, transition_state, dominant_sequence_class,
                       transition_adjusted_family_contribution, transition_family_rank
                from public.cross_asset_family_transition_attribution_summary
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
                # 4.3B values override when present so we stay aligned with
                # downstream ranking logic.
                if out[fam].get("transition_state") is None:
                    out[fam]["transition_state"] = d.get("transition_state")
                if out[fam].get("dominant_sequence_class") is None:
                    out[fam]["dominant_sequence_class"] = d.get("dominant_sequence_class")
                trans_contrib = _as_float(d.get("transition_adjusted_family_contribution"))
                if trans_contrib is not None:
                    out[fam]["transition_adjusted_family_contribution"] = trans_contrib
                rank = d.get("transition_family_rank")
                if rank is not None:
                    out[fam]["transition_family_rank"] = rank
        return out

    # ── classification primitives ───────────────────────────────────────
    @staticmethod
    def compute_archetype_confidence(
        *,
        transition_state: str,
        sequence_class: str,
        event_types: set[str] | None,
        sequence_confidence: float | None,
    ) -> float:
        """Bounded [0.0, 1.0] confidence combining state/class coherence +
        supporting events + history fullness."""
        score = 0.0

        # State vs class alignment
        coherent = {
            ("reinforcing",   "reinforcing_path"):   0.45,
            ("recovering",    "recovery_path"):      0.40,
            ("rotating_in",   "rotation_path"):      0.40,
            ("rotating_out",  "rotation_path"):      0.40,
            ("deteriorating", "deteriorating_path"): 0.45,
            ("stable",        "reinforcing_path"):   0.30,
            ("stable",        "mixed_path"):         0.25,
        }
        score += coherent.get((transition_state, sequence_class), 0.10)

        # Supporting event types
        evts = event_types or set()
        if "dominance_gain" in evts or "rotation" in evts:
            score += 0.15
        if "recovery" in evts:
            score += 0.15
        if "degradation" in evts or "dominance_loss" in evts:
            score += 0.15

        # Sequence confidence bucket (sequence rows carry their own
        # confidence; fold it in so sparse windows don't claim high
        # archetype confidence).
        if sequence_confidence is not None:
            score += 0.25 * _clip(sequence_confidence, 0.0, 1.0)

        # Insufficient history caps out low.
        if transition_state == "insufficient_history" or sequence_class == "insufficient_history":
            return _clip(min(score, 0.10), 0.0, 1.0)
        return _clip(score, 0.0, 1.0)

    @staticmethod
    def derive_classification_reason_codes(
        *,
        archetype_key: str,
        transition_state: str,
        sequence_class: str,
        event_types: set[str] | None,
    ) -> list[str]:
        codes: list[str] = []
        evts = event_types or set()
        if archetype_key == "rotation_handoff":
            codes.append("rotation_path_detected")
            if "dominance_gain" in evts:
                codes.append("dominance_gain_present")
            if "dominance_loss" in evts:
                codes.append("dominance_loss_present")
            if "rotation" in evts:
                codes.append("rotation_event_present")
        elif archetype_key == "reinforcing_continuation":
            codes.append("reinforcing_sequence_match")
        elif archetype_key == "recovering_reentry":
            codes.append("recovery_sequence_match")
            if "recovery" in evts:
                codes.append("recovery_event_present")
        elif archetype_key == "deteriorating_breakdown":
            codes.append("deteriorating_sequence_match")
            if "degradation" in evts:
                codes.append("degradation_event_present")
        elif archetype_key == "mixed_transition_noise":
            codes.append("mixed_signals")
        elif archetype_key == "insufficient_history":
            codes.append("insufficient_history")
        # Always record raw state + class for replay-audit purposes
        codes.append(f"transition_state:{transition_state}")
        codes.append(f"sequence_class:{sequence_class}")
        # Dedupe preserving order
        seen: set[str] = set()
        out: list[str] = []
        for c in codes:
            if c not in seen:
                seen.add(c)
                out.append(c)
        return out

    @staticmethod
    def _classify_key(
        *, transition_state: str, sequence_class: str,
    ) -> str:
        """Deterministic rule-based mapping from (state, class) to archetype
        key. Falls back to mixed/insufficient when evidence is ambiguous."""
        if transition_state == "insufficient_history" or sequence_class == "insufficient_history":
            return "insufficient_history"
        if sequence_class == "rotation_path" or transition_state in ("rotating_in", "rotating_out"):
            return "rotation_handoff"
        if transition_state == "reinforcing" and sequence_class == "reinforcing_path":
            return "reinforcing_continuation"
        if transition_state == "recovering" and sequence_class == "recovery_path":
            return "recovering_reentry"
        if transition_state == "deteriorating" and sequence_class == "deteriorating_path":
            return "deteriorating_breakdown"
        if sequence_class == "mixed_path":
            return "mixed_transition_noise"
        # State/class disagreement → mixed
        return "mixed_transition_noise"

    def classify_family_archetype(
        self,
        *,
        dependency_family: str,
        family_info: dict[str, Any],
        regime_key: str | None,
        valid_archetype_keys: set[str],
    ) -> FamilyArchetype:
        state = str(family_info.get("transition_state") or "insufficient_history")
        seq_class = str(family_info.get("dominant_sequence_class") or "insufficient_history")
        timing_class = family_info.get("dominant_timing_class")
        events = family_info.get("event_types") or set()
        seq_conf = _as_float(family_info.get("sequence_confidence"))

        archetype_key = self._classify_key(
            transition_state=state, sequence_class=seq_class,
        )
        # If the DB registry has been pruned, fall back to insufficient_history
        # rather than write an unknown key.
        if archetype_key not in valid_archetype_keys:
            archetype_key = "insufficient_history"

        confidence = self.compute_archetype_confidence(
            transition_state=state, sequence_class=seq_class,
            event_types=events if isinstance(events, set) else set(events or []),
            sequence_confidence=seq_conf,
        )
        reason_codes = self.derive_classification_reason_codes(
            archetype_key=archetype_key,
            transition_state=state, sequence_class=seq_class,
            event_types=events if isinstance(events, set) else set(events or []),
        )

        rank = family_info.get("transition_family_rank") or family_info.get("family_rank")
        contribution = (
            _as_float(family_info.get("transition_adjusted_family_contribution"))
            if family_info.get("transition_adjusted_family_contribution") is not None
            else _as_float(family_info.get("family_contribution"))
        )

        return FamilyArchetype(
            dependency_family=dependency_family,
            regime_key=regime_key,
            archetype_key=archetype_key,
            transition_state=state,
            dominant_sequence_class=seq_class,
            dominant_timing_class=timing_class,
            family_rank=rank,
            family_contribution=contribution,
            archetype_confidence=confidence,
            classification_reason_codes=reason_codes,
            metadata={
                "scoring_version":    _SCORING_VERSION,
                "sequence_confidence": seq_conf,
                "event_types":         sorted(list(events)) if events else [],
            },
        )

    @staticmethod
    def _archetype_family_bucket_counts(
        family_rows: list[FamilyArchetype],
    ) -> dict[str, int]:
        counts = {
            "rotation":      0,
            "recovery":      0,
            "degradation":   0,
            "reinforcement": 0,
            "mixed":         0,
            "none":          0,
        }
        key_to_family = {
            "rotation_handoff":         "rotation",
            "reinforcing_continuation": "reinforcement",
            "recovering_reentry":       "recovery",
            "deteriorating_breakdown":  "degradation",
            "mixed_transition_noise":   "mixed",
            "insufficient_history":     "none",
        }
        for fa in family_rows:
            bucket = key_to_family.get(fa.archetype_key, "mixed")
            counts[bucket] = counts.get(bucket, 0) + 1
        return counts

    def classify_run_archetype(
        self,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
        context_snapshot_id: str | None,
        regime_key: str | None,
        family_rows: list[FamilyArchetype],
    ) -> RunArchetypeResult:
        if not family_rows:
            return RunArchetypeResult(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                run_id=run_id,
                context_snapshot_id=context_snapshot_id,
                regime_key=regime_key,
                dominant_archetype_key="insufficient_history",
                dominant_dependency_family=None,
                dominant_transition_state=None,
                dominant_sequence_class=None,
                archetype_confidence=0.0,
                rotation_event_count=0,
                recovery_event_count=0,
                degradation_event_count=0,
                mixed_event_count=0,
                classification_reason_codes=["insufficient_history"],
                metadata={"scoring_version": _SCORING_VERSION},
                family_rows=[],
            )

        # Sort family_rows by rank asc → confidence desc → contribution mag
        # desc → archetype priority desc → family priority desc → alpha
        def _key(fa: FamilyArchetype) -> tuple[Any, ...]:
            return (
                fa.family_rank if fa.family_rank is not None else 10**6,
                -(fa.archetype_confidence or 0.0),
                -abs(fa.family_contribution or 0.0),
                -_ARCHETYPE_PRIORITY.get(fa.archetype_key, 0),
                -_FAMILY_PRIORITY.get(fa.dependency_family, 0),
                fa.dependency_family,
            )

        ranked = sorted(family_rows, key=_key)
        top = ranked[0]

        counts = self._archetype_family_bucket_counts(family_rows)

        reason_codes = list(top.classification_reason_codes)
        reason_codes.append(f"top_family:{top.dependency_family}")
        if regime_key:
            reason_codes.append(f"regime:{regime_key}")

        return RunArchetypeResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snapshot_id,
            regime_key=regime_key,
            dominant_archetype_key=top.archetype_key,
            dominant_dependency_family=top.dependency_family,
            dominant_transition_state=top.transition_state,
            dominant_sequence_class=top.dominant_sequence_class,
            archetype_confidence=top.archetype_confidence,
            rotation_event_count=counts.get("rotation", 0),
            recovery_event_count=counts.get("recovery", 0),
            degradation_event_count=counts.get("degradation", 0),
            mixed_event_count=counts.get("mixed", 0),
            classification_reason_codes=reason_codes,
            metadata={
                "scoring_version":       _SCORING_VERSION,
                "archetype_bucket_counts": counts,
                "family_count":           len(family_rows),
            },
            family_rows=ranked,
        )

    # ── orchestration ───────────────────────────────────────────────────
    def build_archetypes_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> RunArchetypeResult | None:
        registry = self.load_active_archetype_registry(conn)
        valid_keys = {str(r["archetype_key"]) for r in registry}

        ctx = self._load_run_context(conn, run_id=run_id)
        fam_info = self._load_family_transition_state(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not fam_info:
            return None

        family_rows: list[FamilyArchetype] = []
        for fam, info in sorted(fam_info.items()):
            family_rows.append(self.classify_family_archetype(
                dependency_family=fam,
                family_info=info,
                regime_key=ctx.get("regime_key"),
                valid_archetype_keys=valid_keys,
            ))
            # Inherit run-level timing class as a fallback for families that
            # never saw a 4.3A state row.
            if family_rows[-1].dominant_timing_class is None:
                family_rows[-1].dominant_timing_class = ctx.get("dominant_timing_class")

        return self.classify_run_archetype(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=ctx.get("context_snapshot_id"),
            regime_key=ctx.get("regime_key"),
            family_rows=family_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_family_archetypes(
        self, conn, *, result: RunArchetypeResult,
    ) -> int:
        if not result.family_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_archetype_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            rows=[
                {
                    "dependency_family":           fa.dependency_family,
                    "regime_key":                  fa.regime_key,
                    "archetype_key":               fa.archetype_key,
                    "transition_state":            fa.transition_state,
                    "dominant_sequence_class":     fa.dominant_sequence_class,
                    "dominant_timing_class":       fa.dominant_timing_class,
                    "family_rank":                 fa.family_rank,
                    "family_contribution":         fa.family_contribution,
                    "archetype_confidence":        fa.archetype_confidence,
                    "classification_reason_codes": fa.classification_reason_codes,
                    "metadata":                    fa.metadata,
                }
                for fa in result.family_rows
            ],
        )

    def persist_run_archetype(
        self, conn, *, result: RunArchetypeResult,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_run_archetype_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            regime_key=result.regime_key,
            dominant_archetype_key=result.dominant_archetype_key,
            dominant_dependency_family=result.dominant_dependency_family,
            dominant_transition_state=result.dominant_transition_state,
            dominant_sequence_class=result.dominant_sequence_class,
            archetype_confidence=result.archetype_confidence,
            rotation_event_count=result.rotation_event_count,
            recovery_event_count=result.recovery_event_count,
            degradation_event_count=result.degradation_event_count,
            mixed_event_count=result.mixed_event_count,
            classification_reason_codes=result.classification_reason_codes,
            metadata=result.metadata,
        )
        return str(row["id"])

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> RunArchetypeResult | None:
        result = self.build_archetypes_for_run(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if result is None:
            return None
        self.persist_family_archetypes(conn, result=result)
        self.persist_run_archetype(conn, result=result)
        return result

    def refresh_workspace_archetypes(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[RunArchetypeResult]:
        """Emit archetype classifications for every watchlist. Commits
        per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[RunArchetypeResult] = []
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
                    "cross_asset_pattern: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
