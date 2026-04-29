"""Phase 4.4C: Archetype-Aware Composite Refinement Service.

Reads 4.4B archetype-aware attribution and the most mature upstream composite
(4.3C composite_post_transition → 4.2C composite_post_timing → 4.1C regime
equivalent → 4.1B weighted equivalent → 4.1A raw composite post) and emits a
bounded archetype-aware delta that refines the final integrated score.
Persists:

  * one cross_asset_archetype_composite_snapshots row per run
  * one cross_asset_family_archetype_composite_snapshots row per family

All adjustments are deterministic; the archetype-aware net contribution is
clipped to a conservative band and the integration weight is small and
explicit. Archetype-aware integration never dominates upstream cross-asset
integration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_SCORING_VERSION = "4.4C.v1"

_VALID_MODES = frozenset({
    "archetype_additive_guardrailed",
    "reinforcing_confirmation_only",
    "breakdown_suppression_only",
    "rotation_sensitive",
})

_DEFAULT_PROFILE: dict[str, Any] = {
    "profile_name":                    "default_archetype_integration",
    "integration_mode":                "archetype_additive_guardrailed",
    "integration_weight":              0.10,
    "reinforcing_continuation_scale":  1.08,
    "recovering_reentry_scale":        1.03,
    "rotation_handoff_scale":          1.01,
    "mixed_transition_noise_scale":    0.92,
    "deteriorating_breakdown_scale":   0.82,
    "insufficient_history_scale":      0.85,
    "max_positive_contribution":       0.20,
    "max_negative_contribution":       0.20,
}

# Conservative first-pass clamp per spec §11.
_NET_BOUND_DEFAULT = 0.15


@dataclass
class FamilyArchetypeComposite:
    dependency_family: str
    archetype_key: str
    transition_state: str
    dominant_sequence_class: str
    archetype_adjusted_family_contribution: float | None
    integration_weight_applied: float
    archetype_integration_contribution: float | None
    family_rank: int | None = None
    top_symbols: list[str] = field(default_factory=list)
    classification_reason_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ArchetypeCompositeResult:
    workspace_id: str
    watchlist_id: str
    run_id: str
    context_snapshot_id: str | None
    archetype_integration_profile_id: str | None
    profile_name: str
    default_profile_used: bool
    integration_mode: str
    base_signal_score: float | None
    cross_asset_net_contribution: float | None
    weighted_cross_asset_net_contribution: float | None
    regime_adjusted_cross_asset_contribution: float | None
    timing_adjusted_cross_asset_contribution: float | None
    transition_adjusted_cross_asset_contribution: float | None
    archetype_adjusted_cross_asset_contribution: float | None
    composite_pre_archetype: float | None
    archetype_net_contribution: float | None
    composite_post_archetype: float | None
    dominant_archetype_key: str
    metadata: dict[str, Any]
    family_rows: list[FamilyArchetypeComposite]


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


# Archetypes that each integration-mode admits.
_MODE_ADMITS: dict[str, frozenset[str]] = {
    "archetype_additive_guardrailed": frozenset({
        "reinforcing_continuation", "recovering_reentry", "rotation_handoff",
        "mixed_transition_noise", "deteriorating_breakdown", "insufficient_history",
    }),
    "reinforcing_confirmation_only": frozenset({
        "reinforcing_continuation", "recovering_reentry",
    }),
    "breakdown_suppression_only": frozenset({
        "deteriorating_breakdown", "mixed_transition_noise",
    }),
    "rotation_sensitive": frozenset({
        "rotation_handoff", "reinforcing_continuation", "recovering_reentry",
    }),
}


class CrossAssetArchetypeCompositeService:
    """Deterministic archetype-aware final composite integration."""

    # ── profile loading ─────────────────────────────────────────────────
    def get_active_archetype_integration_profile(
        self, conn, *, workspace_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, profile_name, is_active, integration_mode,
                       integration_weight,
                       reinforcing_continuation_scale, recovering_reentry_scale,
                       rotation_handoff_scale, mixed_transition_noise_scale,
                       deteriorating_breakdown_scale, insufficient_history_scale,
                       max_positive_contribution, max_negative_contribution,
                       metadata, created_at
                from public.cross_asset_archetype_integration_profiles
                where workspace_id = %s::uuid and is_active = true
                order by created_at desc
                limit 1
                """,
                (workspace_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # ── scale primitive ─────────────────────────────────────────────────
    def compute_archetype_integration_scale(
        self, *, archetype_key: str, profile: dict[str, Any],
    ) -> float:
        key_map = {
            "reinforcing_continuation": "reinforcing_continuation_scale",
            "recovering_reentry":       "recovering_reentry_scale",
            "rotation_handoff":         "rotation_handoff_scale",
            "mixed_transition_noise":   "mixed_transition_noise_scale",
            "deteriorating_breakdown":  "deteriorating_breakdown_scale",
            "insufficient_history":     "insufficient_history_scale",
        }
        scale = _as_float(profile.get(key_map.get(archetype_key, "insufficient_history_scale")))
        return scale if scale is not None else 1.0

    @staticmethod
    def _mode_gate_archetype(mode: str, archetype_key: str) -> bool:
        admits = _MODE_ADMITS.get(mode)
        if admits is None:
            return True
        return archetype_key in admits

    # ── input loading ───────────────────────────────────────────────────
    def _load_run_context(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> dict[str, Any]:
        """Load all upstream contributions + the composite_pre_archetype
        fallback chain."""
        ctx: dict[str, Any] = {
            "context_snapshot_id":        None,
            "base_signal_score":          None,
            "raw_net":                    None,
            "weighted_net":               None,
            "regime_net":                 None,
            "timing_net":                 None,
            "transition_net":             None,
            "archetype_net":              None,
            "timing_composite_post":      None,
            "transition_composite_post":  None,
            "composite_pre_archetype":    None,
            "composite_pre_source":       None,
        }
        with conn.cursor() as cur:
            cur.execute(
                """
                select context_snapshot_id::text as context_snapshot_id,
                       base_signal_score,
                       cross_asset_net_contribution,
                       composite_post_cross_asset
                from public.cross_asset_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            raw_composite_post: float | None = None
            if row:
                d = dict(row)
                ctx["context_snapshot_id"] = d.get("context_snapshot_id")
                ctx["base_signal_score"]   = _as_float(d.get("base_signal_score"))
                ctx["raw_net"]             = _as_float(d.get("cross_asset_net_contribution"))
                raw_composite_post         = _as_float(d.get("composite_post_cross_asset"))

            cur.execute(
                """
                select weighted_cross_asset_net_contribution
                from public.run_cross_asset_weighted_integration_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["weighted_net"] = _as_float(dict(row).get("weighted_cross_asset_net_contribution"))

            cur.execute(
                """
                select regime_adjusted_cross_asset_contribution
                from public.run_cross_asset_regime_integration_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["regime_net"] = _as_float(dict(row).get("regime_adjusted_cross_asset_contribution"))

            cur.execute(
                """
                select timing_adjusted_cross_asset_contribution
                from public.run_cross_asset_timing_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["timing_net"] = _as_float(dict(row).get("timing_adjusted_cross_asset_contribution"))

            cur.execute(
                """
                select transition_adjusted_cross_asset_contribution
                from public.run_cross_asset_transition_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["transition_net"] = _as_float(dict(row).get("transition_adjusted_cross_asset_contribution"))

            cur.execute(
                """
                select archetype_adjusted_cross_asset_contribution
                from public.run_cross_asset_archetype_attribution_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["archetype_net"] = _as_float(dict(row).get("archetype_adjusted_cross_asset_contribution"))

            cur.execute(
                """
                select composite_post_timing
                from public.cross_asset_timing_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["timing_composite_post"] = _as_float(dict(row).get("composite_post_timing"))

            cur.execute(
                """
                select composite_post_transition
                from public.cross_asset_transition_composite_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                ctx["transition_composite_post"] = _as_float(dict(row).get("composite_post_transition"))

        # composite_pre_archetype fallback chain
        INTEGRATION_WEIGHT = 0.10
        base = ctx["base_signal_score"]
        if ctx["transition_composite_post"] is not None:
            ctx["composite_pre_archetype"] = ctx["transition_composite_post"]
            ctx["composite_pre_source"] = "transition_composite_post"
        elif ctx["timing_composite_post"] is not None:
            ctx["composite_pre_archetype"] = ctx["timing_composite_post"]
            ctx["composite_pre_source"] = "timing_composite_post"
        elif base is not None and ctx["regime_net"] is not None:
            ctx["composite_pre_archetype"] = base + ctx["regime_net"] * INTEGRATION_WEIGHT
            ctx["composite_pre_source"] = "regime_post_integration_equivalent"
        elif base is not None and ctx["weighted_net"] is not None:
            ctx["composite_pre_archetype"] = base + ctx["weighted_net"] * INTEGRATION_WEIGHT
            ctx["composite_pre_source"] = "weighted_post_integration_equivalent"
        elif raw_composite_post is not None:
            ctx["composite_pre_archetype"] = raw_composite_post
            ctx["composite_pre_source"] = "raw_composite_post_cross_asset"
        elif base is not None:
            ctx["composite_pre_archetype"] = base
            ctx["composite_pre_source"] = "base_signal_score"
        return ctx

    def _load_archetype_family_rows(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dependency_family,
                       archetype_key,
                       transition_state,
                       dominant_sequence_class,
                       archetype_adjusted_family_contribution,
                       archetype_family_rank,
                       top_symbols,
                       classification_reason_codes
                from public.cross_asset_family_archetype_attribution_summary
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and run_id       = %s::uuid
                """,
                (workspace_id, watchlist_id, run_id),
            )
            return [dict(r) for r in cur.fetchall()]

    def _load_run_archetype(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,  # noqa: ARG002
    ) -> str | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select dominant_archetype_key
                from public.cross_asset_run_archetype_summary
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if row:
                return dict(row).get("dominant_archetype_key")
        return None

    # ── core builders ───────────────────────────────────────────────────
    def compute_archetype_net_contribution(
        self,
        *,
        archetype_adjusted_total: float | None,
        dominant_archetype_key: str,
        profile: dict[str, Any],
    ) -> float | None:
        if archetype_adjusted_total is None:
            return None
        mode = profile.get("integration_mode", "archetype_additive_guardrailed")

        if not self._mode_gate_archetype(mode, dominant_archetype_key):
            return 0.0
        base = archetype_adjusted_total
        if mode == "breakdown_suppression_only" and dominant_archetype_key in (
            "deteriorating_breakdown", "mixed_transition_noise",
        ):
            # Force downward contribution under suppression-only mode.
            base = -abs(base)

        scale = self.compute_archetype_integration_scale(
            archetype_key=dominant_archetype_key, profile=profile,
        )
        integration_weight = _as_float(profile.get("integration_weight")) or 0.10
        raw = base * integration_weight * scale

        max_pos = _as_float(profile.get("max_positive_contribution")) or _NET_BOUND_DEFAULT
        max_neg = _as_float(profile.get("max_negative_contribution")) or _NET_BOUND_DEFAULT
        lower = -min(max_neg, _NET_BOUND_DEFAULT)
        upper =  min(max_pos, _NET_BOUND_DEFAULT)
        return _clip(raw, lower, upper)

    def integrate_archetype_with_composite(
        self,
        *,
        composite_pre_archetype: float | None,
        archetype_net_contribution: float | None,
    ) -> float | None:
        if composite_pre_archetype is None:
            return None
        if archetype_net_contribution is None:
            return composite_pre_archetype
        return composite_pre_archetype + archetype_net_contribution

    def compute_family_archetype_integration(
        self,
        family_rows: Sequence[dict[str, Any]],
        *,
        profile: dict[str, Any],
    ) -> list[FamilyArchetypeComposite]:
        integration_weight = _as_float(profile.get("integration_weight")) or 0.10
        mode = profile.get("integration_mode", "archetype_additive_guardrailed")
        max_pos = _as_float(profile.get("max_positive_contribution")) or _NET_BOUND_DEFAULT
        max_neg = _as_float(profile.get("max_negative_contribution")) or _NET_BOUND_DEFAULT
        lower = -min(max_neg, _NET_BOUND_DEFAULT)
        upper =  min(max_pos, _NET_BOUND_DEFAULT)

        out: list[FamilyArchetypeComposite] = []
        for r in family_rows:
            fam            = str(r["dependency_family"])
            archetype_key  = str(r.get("archetype_key") or "insufficient_history")
            state          = str(r.get("transition_state") or "insufficient_history")
            seq_class      = str(r.get("dominant_sequence_class") or "insufficient_history")
            archetype_adj  = _as_float(r.get("archetype_adjusted_family_contribution"))

            scale = self.compute_archetype_integration_scale(
                archetype_key=archetype_key, profile=profile,
            )

            top_syms_raw = r.get("top_symbols") or []
            if isinstance(top_syms_raw, str):
                import json
                try:
                    top_syms_raw = json.loads(top_syms_raw)
                except json.JSONDecodeError:
                    top_syms_raw = []
            top_syms = [str(s) for s in top_syms_raw]

            reason_raw = r.get("classification_reason_codes") or []
            if isinstance(reason_raw, str):
                import json
                try:
                    reason_raw = json.loads(reason_raw)
                except json.JSONDecodeError:
                    reason_raw = []
            reason_codes = [str(c) for c in reason_raw]

            contribution: float | None
            if archetype_adj is None:
                contribution = None
            else:
                gated = archetype_adj
                if not self._mode_gate_archetype(mode, archetype_key):
                    gated = 0.0
                elif mode == "breakdown_suppression_only" and archetype_key in (
                    "deteriorating_breakdown", "mixed_transition_noise",
                ):
                    gated = -abs(archetype_adj)
                raw = gated * integration_weight * scale
                contribution = _clip(raw, lower, upper)

            out.append(FamilyArchetypeComposite(
                dependency_family=fam,
                archetype_key=archetype_key,
                transition_state=state,
                dominant_sequence_class=seq_class,
                archetype_adjusted_family_contribution=archetype_adj,
                integration_weight_applied=integration_weight * scale,
                archetype_integration_contribution=contribution,
                top_symbols=top_syms,
                classification_reason_codes=reason_codes,
                metadata={
                    "scoring_version":      _SCORING_VERSION,
                    "archetype_scale":      scale,
                    "integration_mode":     mode,
                    "integration_weight":   integration_weight,
                    "source_family_rank":   r.get("archetype_family_rank"),
                },
            ))
        return out

    def rank_family_archetype_integration(
        self, items: list[FamilyArchetypeComposite],
    ) -> list[FamilyArchetypeComposite]:
        ranked = sorted(
            items,
            key=lambda fa: (
                -(fa.archetype_integration_contribution or 0.0),
                -abs(fa.archetype_integration_contribution or 0.0),
                fa.dependency_family,
            ),
        )
        for i, item in enumerate(ranked, start=1):
            item.family_rank = i
        return ranked

    @staticmethod
    def _determine_dominant_archetype_from_families(
        family_rows: Sequence[dict[str, Any]],
    ) -> str:
        if not family_rows:
            return "insufficient_history"
        ranked = sorted(
            family_rows,
            key=lambda r: r.get("archetype_family_rank") or 10**6,
        )
        top = ranked[0]
        return str(top.get("archetype_key") or "insufficient_history")

    # ── orchestration ───────────────────────────────────────────────────
    def build_archetype_composite_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str,
    ) -> ArchetypeCompositeResult | None:
        family_rows_raw = self._load_archetype_family_rows(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if not family_rows_raw:
            return None

        profile_row = self.get_active_archetype_integration_profile(
            conn, workspace_id=workspace_id,
        )
        default_profile_used = profile_row is None
        profile = profile_row if profile_row is not None else dict(_DEFAULT_PROFILE)
        profile_id = profile_row["id"] if profile_row is not None else None
        profile_name = profile.get("profile_name", _DEFAULT_PROFILE["profile_name"])
        mode = str(profile.get("integration_mode", "archetype_additive_guardrailed"))
        if mode not in _VALID_MODES:
            raise ValueError(f"invalid integration_mode: {mode!r}")

        ctx = self._load_run_context(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        run_archetype = self._load_run_archetype(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        dominant_archetype_key = (
            run_archetype if run_archetype
            else self._determine_dominant_archetype_from_families(family_rows_raw)
        )

        archetype_net = self.compute_archetype_net_contribution(
            archetype_adjusted_total=ctx["archetype_net"],
            dominant_archetype_key=dominant_archetype_key,
            profile=profile,
        )
        composite_post = self.integrate_archetype_with_composite(
            composite_pre_archetype=ctx["composite_pre_archetype"],
            archetype_net_contribution=archetype_net,
        )

        family_rows = self.compute_family_archetype_integration(family_rows_raw, profile=profile)
        family_rows = self.rank_family_archetype_integration(family_rows)

        metadata: dict[str, Any] = {
            "scoring_version":                            _SCORING_VERSION,
            "default_archetype_integration_profile_used": default_profile_used,
            "profile_name":                               profile_name,
            "composite_pre_source":                       ctx.get("composite_pre_source"),
            "integration_weight":                         _as_float(profile.get("integration_weight")),
            "max_positive_contribution":                  _as_float(profile.get("max_positive_contribution")),
            "max_negative_contribution":                  _as_float(profile.get("max_negative_contribution")),
            "net_bound_default":                          _NET_BOUND_DEFAULT,
            "dominant_archetype_source":                  "run_archetype_summary" if run_archetype else "family_top_rank",
        }
        for fa in family_rows:
            fa.metadata.setdefault("profile_name", profile_name)
            fa.metadata.setdefault("default_archetype_integration_profile_used", default_profile_used)

        return ArchetypeCompositeResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=ctx.get("context_snapshot_id"),
            archetype_integration_profile_id=profile_id,
            profile_name=profile_name,
            default_profile_used=default_profile_used,
            integration_mode=mode,
            base_signal_score=ctx.get("base_signal_score"),
            cross_asset_net_contribution=ctx.get("raw_net"),
            weighted_cross_asset_net_contribution=ctx.get("weighted_net"),
            regime_adjusted_cross_asset_contribution=ctx.get("regime_net"),
            timing_adjusted_cross_asset_contribution=ctx.get("timing_net"),
            transition_adjusted_cross_asset_contribution=ctx.get("transition_net"),
            archetype_adjusted_cross_asset_contribution=ctx.get("archetype_net"),
            composite_pre_archetype=ctx.get("composite_pre_archetype"),
            archetype_net_contribution=archetype_net,
            composite_post_archetype=composite_post,
            dominant_archetype_key=dominant_archetype_key,
            metadata=metadata,
            family_rows=family_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_archetype_composite(
        self, conn, *, result: ArchetypeCompositeResult,
    ) -> str:
        import src.db.repositories as repo
        row = repo.insert_cross_asset_archetype_composite_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            archetype_integration_profile_id=result.archetype_integration_profile_id,
            base_signal_score=result.base_signal_score,
            cross_asset_net_contribution=result.cross_asset_net_contribution,
            weighted_cross_asset_net_contribution=result.weighted_cross_asset_net_contribution,
            regime_adjusted_cross_asset_contribution=result.regime_adjusted_cross_asset_contribution,
            timing_adjusted_cross_asset_contribution=result.timing_adjusted_cross_asset_contribution,
            transition_adjusted_cross_asset_contribution=result.transition_adjusted_cross_asset_contribution,
            archetype_adjusted_cross_asset_contribution=result.archetype_adjusted_cross_asset_contribution,
            composite_pre_archetype=result.composite_pre_archetype,
            archetype_net_contribution=result.archetype_net_contribution,
            composite_post_archetype=result.composite_post_archetype,
            dominant_archetype_key=result.dominant_archetype_key,
            integration_mode=result.integration_mode,
            metadata=result.metadata,
        )
        return str(row["id"])

    def persist_family_archetype_composite(
        self, conn, *, result: ArchetypeCompositeResult,
    ) -> int:
        if not result.family_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_archetype_composite_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            rows=[
                {
                    "dependency_family":                      fa.dependency_family,
                    "archetype_key":                          fa.archetype_key,
                    "transition_state":                       fa.transition_state,
                    "dominant_sequence_class":                fa.dominant_sequence_class,
                    "archetype_adjusted_family_contribution": fa.archetype_adjusted_family_contribution,
                    "integration_weight_applied":             fa.integration_weight_applied,
                    "archetype_integration_contribution":     fa.archetype_integration_contribution,
                    "family_rank":                            fa.family_rank,
                    "top_symbols":                            fa.top_symbols,
                    "classification_reason_codes":            fa.classification_reason_codes,
                    "metadata":                               fa.metadata,
                }
                for fa in result.family_rows
            ],
        )

    def build_and_persist(
        self, conn, *, workspace_id: str, watchlist_id: str, run_id: str,
    ) -> ArchetypeCompositeResult | None:
        result = self.build_archetype_composite_for_run(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
        )
        if result is None:
            return None
        self.persist_archetype_composite(conn, result=result)
        self.persist_family_archetype_composite(conn, result=result)
        return result

    def refresh_workspace_archetype_composite(
        self, conn, *, workspace_id: str, run_id: str,
    ) -> list[ArchetypeCompositeResult]:
        """Emit archetype-aware composite for every watchlist. Commits
        per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[ArchetypeCompositeResult] = []
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
                    "cross_asset_archetype_composite: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
