"""
Phase 3.7A: Governance Policy Optimization Service

Advisory-only: builds feature effectiveness summaries, context-fit summaries,
and policy opportunity recommendations from existing live analytics.

Does NOT modify live threshold or routing policies.
Scoring is deterministic, transparent, and auditable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import logging

logger = logging.getLogger(__name__)

# ── Scoring constants ─────────────────────────────────────────────────────────
_MIN_SAMPLE_SIZE = 10
_HIGH_CONFIDENCE_SAMPLE = 50
_MEDIUM_CONFIDENCE_SAMPLE = 20

# Opportunity reason codes
REASON_TIGHTEN_THRESHOLD = "tighten_threshold_for_regime"
REASON_LOOSEN_THRESHOLD = "loosen_threshold_for_regime"
REASON_PREFER_ROUTING_TEAM = "prefer_routing_team_for_context"
REASON_AVOID_ROUTING_UNDER_LOAD = "avoid_routing_scope_under_load"
REASON_RAISE_AUTOPROMOTION_CONFIDENCE = "raise_autopromotion_confidence_requirement"
REASON_INCREASE_ROLLBACK_STRICTNESS = "increase_rollback_review_strictness"
REASON_SEPARATE_BY_VERSION = "separate_policy_by_version_tuple"
REASON_REDUCE_SENSITIVITY_LOW_SIGNAL = "reduce_policy_sensitivity_for_low_signal_context"
REASON_SPLIT_THRESHOLD_BY_WATCHLIST = "split_threshold_by_watchlist"


@dataclass(frozen=True)
class PolicyFeatureEffectivenessRow:
    policy_family: str
    feature_type: str
    feature_key: str
    sample_size: int
    recurrence_rate: float | None
    reopen_rate: float | None
    escalation_rate: float | None
    reassignment_rate: float | None
    rollback_rate: float | None
    mute_rate: float | None
    approved_review_rate: float | None
    application_rate: float | None
    avg_ack_latency_seconds: float | None
    avg_resolve_latency_seconds: float | None
    effectiveness_score: float
    risk_score: float
    net_policy_fit_score: float


@dataclass(frozen=True)
class PolicyContextFitRow:
    context_key: str
    best_policy_family: str
    best_policy_variant: str
    fit_score: float
    sample_size: int
    confidence: str
    supporting_metrics: dict[str, Any]


@dataclass(frozen=True)
class PolicyOpportunityRow:
    recommendation_key: str
    policy_family: str
    scope_type: str
    scope_value: str
    current_policy: dict[str, Any]
    recommended_policy: dict[str, Any]
    reason_code: str
    confidence: str
    sample_size: int
    expected_benefit_score: float
    risk_score: float
    supporting_metrics: dict[str, Any]


@dataclass(frozen=True)
class GovernancePolicyOptimizationResult:
    feature_effectiveness: list[PolicyFeatureEffectivenessRow]
    context_fit: list[PolicyContextFitRow]
    opportunities: list[PolicyOpportunityRow]
    recommendation_count: int
    metadata: dict[str, Any] = field(default_factory=dict)


class GovernancePolicyOptimizationService:
    """
    Advisory governance policy optimization.

    Derives optimization signals from existing live analytics surfaces.
    Scoring is entirely deterministic — no opaque ML.
    """

    # ── Scoring helpers ───────────────────────────────────────────────────────

    def score_expected_benefit(
        self,
        *,
        recurrence_rate: float | None = None,
        reopen_rate: float | None = None,
        escalation_rate: float | None = None,
        reassignment_rate: float | None = None,
        rollback_rate: float | None = None,
        mute_rate: float | None = None,
        approved_review_rate: float | None = None,
        application_rate: float | None = None,
        avg_resolve_latency_seconds: float | None = None,
        baseline_resolve_latency: float | None = None,
    ) -> float:
        score = 0.0
        # Negative outcome contributors lower benefit
        score -= (recurrence_rate or 0.0) * 0.25
        score -= (reopen_rate or 0.0) * 0.15
        score -= (escalation_rate or 0.0) * 0.20
        score -= (reassignment_rate or 0.0) * 0.15
        score -= (rollback_rate or 0.0) * 0.10
        score -= (mute_rate or 0.0) * 0.05
        # Positive outcome contributors raise benefit
        score += (approved_review_rate or 0.0) * 0.05
        score += (application_rate or 0.0) * 0.05
        # Latency improvement
        if avg_resolve_latency_seconds is not None and baseline_resolve_latency is not None and baseline_resolve_latency > 0:
            latency_improvement = (baseline_resolve_latency - avg_resolve_latency_seconds) / baseline_resolve_latency
            score += latency_improvement * 0.05
        return max(0.0, min(1.0, score + 0.5))

    def score_risk(
        self,
        *,
        sample_size: int,
        rollback_rate: float | None = None,
        reassignment_rate: float | None = None,
        escalation_rate: float | None = None,
        outcome_variance: float | None = None,
        already_promoted: bool = False,
    ) -> float:
        risk = 0.0
        if sample_size < _MIN_SAMPLE_SIZE:
            risk += 0.30
        elif sample_size < _MEDIUM_CONFIDENCE_SAMPLE:
            risk += 0.10
        risk += (rollback_rate or 0.0) * 0.30
        risk += (reassignment_rate or 0.0) * 0.20
        risk += (escalation_rate or 0.0) * 0.15
        risk += (outcome_variance or 0.0) * 0.15
        if already_promoted:
            risk += 0.10
        return max(0.0, min(1.0, risk))

    def derive_confidence(self, *, sample_size: int, signal_consistency: float = 1.0) -> str:
        if sample_size >= _HIGH_CONFIDENCE_SAMPLE and signal_consistency >= 0.8:
            return "high"
        if sample_size >= _MEDIUM_CONFIDENCE_SAMPLE and signal_consistency >= 0.5:
            return "medium"
        return "low"

    # ── Feature effectiveness ─────────────────────────────────────────────────

    def build_policy_feature_effectiveness_summary(
        self,
        conn: Any,
        *,
        workspace_id: str,
    ) -> list[PolicyFeatureEffectivenessRow]:
        """
        Reads from the live DB view. Returns typed rows.
        Kept as a thin pass-through so callers get typed objects.
        """
        import src.db.repositories as repo  # avoid circular at module level
        try:
            raw = repo.get_governance_policy_feature_effectiveness_summary(
                conn, workspace_id=workspace_id
            )
        except Exception as exc:
            logger.warning("feature_effectiveness query failed: %s", exc)
            return []

        rows: list[PolicyFeatureEffectivenessRow] = []
        for r in raw:
            rows.append(PolicyFeatureEffectivenessRow(
                policy_family=r.get("policy_family", ""),
                feature_type=r.get("feature_type", ""),
                feature_key=r.get("feature_key", ""),
                sample_size=int(r.get("sample_size") or 0),
                recurrence_rate=_maybe_float(r.get("recurrence_rate")),
                reopen_rate=_maybe_float(r.get("reopen_rate")),
                escalation_rate=_maybe_float(r.get("escalation_rate")),
                reassignment_rate=_maybe_float(r.get("reassignment_rate")),
                rollback_rate=_maybe_float(r.get("rollback_rate")),
                mute_rate=_maybe_float(r.get("mute_rate")),
                approved_review_rate=_maybe_float(r.get("approved_review_rate")),
                application_rate=_maybe_float(r.get("application_rate")),
                avg_ack_latency_seconds=_maybe_float(r.get("avg_ack_latency_seconds")),
                avg_resolve_latency_seconds=_maybe_float(r.get("avg_resolve_latency_seconds")),
                effectiveness_score=float(r.get("effectiveness_score") or 0.0),
                risk_score=float(r.get("risk_score") or 0.0),
                net_policy_fit_score=float(r.get("net_policy_fit_score") or 0.0),
            ))
        return rows

    # ── Context fit ───────────────────────────────────────────────────────────

    def build_policy_context_fit_summary(
        self,
        conn: Any,
        *,
        workspace_id: str,
    ) -> list[PolicyContextFitRow]:
        import src.db.repositories as repo
        try:
            raw = repo.get_governance_policy_context_fit_summary(
                conn, workspace_id=workspace_id
            )
        except Exception as exc:
            logger.warning("context_fit query failed: %s", exc)
            return []

        rows: list[PolicyContextFitRow] = []
        for r in raw:
            rows.append(PolicyContextFitRow(
                context_key=r.get("context_key", ""),
                best_policy_family=r.get("best_policy_family", ""),
                best_policy_variant=r.get("best_policy_variant", ""),
                fit_score=float(r.get("fit_score") or 0.0),
                sample_size=int(r.get("sample_size") or 0),
                confidence=r.get("confidence") or "low",
                supporting_metrics=r.get("supporting_metrics") or {},
            ))
        return rows

    # ── Opportunity generation ─────────────────────────────────────────────────

    def build_policy_opportunities(
        self,
        conn: Any,
        *,
        workspace_id: str,
        feature_effectiveness: list[PolicyFeatureEffectivenessRow],
    ) -> list[PolicyOpportunityRow]:
        """
        Generates advisory recommendations from effectiveness signals.
        All scoring is deterministic and rule-based.
        """
        opportunities: list[PolicyOpportunityRow] = []
        seen_keys: set[str] = set()

        for row in feature_effectiveness:
            if row.sample_size < _MIN_SAMPLE_SIZE:
                continue

            recurrence = row.recurrence_rate or 0.0
            escalation = row.escalation_rate or 0.0
            reassignment = row.reassignment_rate or 0.0
            rollback = row.rollback_rate or 0.0
            net_fit = row.net_policy_fit_score

            confidence = self.derive_confidence(sample_size=row.sample_size)

            # High recurrence + threshold policy → tighten threshold
            if row.policy_family == "threshold" and recurrence > 0.30:
                key = f"tighten_threshold:{row.feature_key}"
                if key not in seen_keys:
                    seen_keys.add(key)
                    benefit = self.score_expected_benefit(
                        recurrence_rate=recurrence,
                        escalation_rate=escalation,
                        reassignment_rate=reassignment,
                    )
                    risk = self.score_risk(
                        sample_size=row.sample_size,
                        rollback_rate=rollback,
                        reassignment_rate=reassignment,
                        escalation_rate=escalation,
                    )
                    opportunities.append(PolicyOpportunityRow(
                        recommendation_key=key,
                        policy_family="threshold",
                        scope_type=row.feature_type,
                        scope_value=row.feature_key,
                        current_policy={"threshold_policy": "current"},
                        recommended_policy={"action": "tighten", "target_recurrence_reduction": 0.10},
                        reason_code=REASON_TIGHTEN_THRESHOLD,
                        confidence=confidence,
                        sample_size=row.sample_size,
                        expected_benefit_score=round(benefit, 4),
                        risk_score=round(risk, 4),
                        supporting_metrics={
                            "recurrence_rate": recurrence,
                            "escalation_rate": escalation,
                            "net_policy_fit_score": net_fit,
                        },
                    ))

            # Low recurrence but high reassignment → threshold too tight
            if row.policy_family == "threshold" and recurrence < 0.05 and reassignment > 0.25:
                key = f"loosen_threshold:{row.feature_key}"
                if key not in seen_keys:
                    seen_keys.add(key)
                    benefit = self.score_expected_benefit(
                        reassignment_rate=reassignment,
                        application_rate=row.application_rate,
                    )
                    risk = self.score_risk(
                        sample_size=row.sample_size,
                        reassignment_rate=reassignment,
                    )
                    opportunities.append(PolicyOpportunityRow(
                        recommendation_key=key,
                        policy_family="threshold",
                        scope_type=row.feature_type,
                        scope_value=row.feature_key,
                        current_policy={"threshold_policy": "current"},
                        recommended_policy={"action": "loosen", "target_reassignment_reduction": 0.10},
                        reason_code=REASON_LOOSEN_THRESHOLD,
                        confidence=confidence,
                        sample_size=row.sample_size,
                        expected_benefit_score=round(benefit, 4),
                        risk_score=round(risk, 4),
                        supporting_metrics={
                            "reassignment_rate": reassignment,
                            "recurrence_rate": recurrence,
                            "net_policy_fit_score": net_fit,
                        },
                    ))

            # Routing policy with high rollback rate → stricter review
            if row.policy_family in ("routing", "routing_autopromotion") and rollback > 0.15:
                key = f"increase_rollback_strictness:{row.feature_key}"
                if key not in seen_keys:
                    seen_keys.add(key)
                    benefit = self.score_expected_benefit(rollback_rate=rollback)
                    risk = self.score_risk(
                        sample_size=row.sample_size,
                        rollback_rate=rollback,
                    )
                    opportunities.append(PolicyOpportunityRow(
                        recommendation_key=key,
                        policy_family=row.policy_family,
                        scope_type=row.feature_type,
                        scope_value=row.feature_key,
                        current_policy={"review_strictness": "standard"},
                        recommended_policy={"review_strictness": "strict", "min_approved_reviews": 2},
                        reason_code=REASON_INCREASE_ROLLBACK_STRICTNESS,
                        confidence=confidence,
                        sample_size=row.sample_size,
                        expected_benefit_score=round(benefit, 4),
                        risk_score=round(risk, 4),
                        supporting_metrics={"rollback_rate": rollback, "net_policy_fit_score": net_fit},
                    ))

            # Autopromotion with high escalation → raise confidence requirement
            if row.policy_family == "routing_autopromotion" and escalation > 0.20:
                key = f"raise_autopromotion_confidence:{row.feature_key}"
                if key not in seen_keys:
                    seen_keys.add(key)
                    benefit = self.score_expected_benefit(escalation_rate=escalation)
                    risk = self.score_risk(
                        sample_size=row.sample_size,
                        escalation_rate=escalation,
                    )
                    opportunities.append(PolicyOpportunityRow(
                        recommendation_key=key,
                        policy_family="routing_autopromotion",
                        scope_type=row.feature_type,
                        scope_value=row.feature_key,
                        current_policy={"min_confidence": "medium"},
                        recommended_policy={"min_confidence": "high"},
                        reason_code=REASON_RAISE_AUTOPROMOTION_CONFIDENCE,
                        confidence=confidence,
                        sample_size=row.sample_size,
                        expected_benefit_score=round(benefit, 4),
                        risk_score=round(risk, 4),
                        supporting_metrics={"escalation_rate": escalation, "net_policy_fit_score": net_fit},
                    ))

            # Very negative net_policy_fit → low-signal context sensitivity reduction
            if net_fit < -0.20 and row.sample_size < _MEDIUM_CONFIDENCE_SAMPLE:
                key = f"reduce_sensitivity_low_signal:{row.policy_family}:{row.feature_key}"
                if key not in seen_keys:
                    seen_keys.add(key)
                    benefit = 0.3
                    risk = self.score_risk(sample_size=row.sample_size)
                    opportunities.append(PolicyOpportunityRow(
                        recommendation_key=key,
                        policy_family=row.policy_family,
                        scope_type=row.feature_type,
                        scope_value=row.feature_key,
                        current_policy={"sensitivity": "current"},
                        recommended_policy={"sensitivity": "reduced", "reason": "insufficient_signal"},
                        reason_code=REASON_REDUCE_SENSITIVITY_LOW_SIGNAL,
                        confidence="low",
                        sample_size=row.sample_size,
                        expected_benefit_score=round(benefit, 4),
                        risk_score=round(risk, 4),
                        supporting_metrics={"net_policy_fit_score": net_fit, "sample_size": row.sample_size},
                    ))

        # Sort by expected benefit descending, risk ascending
        opportunities.sort(key=lambda o: (-o.expected_benefit_score, o.risk_score))
        return opportunities

    # ── Snapshot + persistence ────────────────────────────────────────────────

    def persist_optimization_snapshot(
        self,
        conn: Any,
        *,
        workspace_id: str,
        recommendation_count: int,
        window_label: str = "30d",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        import src.db.repositories as repo
        return repo.insert_governance_policy_optimization_snapshot(
            conn,
            workspace_id=workspace_id,
            window_label=window_label,
            recommendation_count=recommendation_count,
            metadata=metadata or {},
        )

    def upsert_opportunities(
        self,
        conn: Any,
        *,
        workspace_id: str,
        opportunities: list[PolicyOpportunityRow],
    ) -> int:
        import src.db.repositories as repo
        count = 0
        for opp in opportunities:
            try:
                repo.upsert_governance_policy_recommendation(
                    conn,
                    workspace_id=workspace_id,
                    recommendation_key=opp.recommendation_key,
                    policy_family=opp.policy_family,
                    scope_type=opp.scope_type,
                    scope_value=opp.scope_value,
                    current_policy=opp.current_policy,
                    recommended_policy=opp.recommended_policy,
                    reason_code=opp.reason_code,
                    confidence=opp.confidence,
                    sample_size=opp.sample_size,
                    expected_benefit_score=opp.expected_benefit_score,
                    risk_score=opp.risk_score,
                    supporting_metrics=opp.supporting_metrics,
                )
                count += 1
            except Exception as exc:
                logger.warning("upsert_recommendation failed key=%s: %s", opp.recommendation_key, exc)
        return count

    # ── Main entry point ──────────────────────────────────────────────────────

    def refresh_workspace_optimization(
        self,
        conn: Any,
        *,
        workspace_id: str,
        window_label: str = "30d",
    ) -> GovernancePolicyOptimizationResult:
        feature_effectiveness = self.build_policy_feature_effectiveness_summary(
            conn, workspace_id=workspace_id
        )
        context_fit = self.build_policy_context_fit_summary(
            conn, workspace_id=workspace_id
        )
        opportunities = self.build_policy_opportunities(
            conn,
            workspace_id=workspace_id,
            feature_effectiveness=feature_effectiveness,
        )

        upserted = self.upsert_opportunities(
            conn, workspace_id=workspace_id, opportunities=opportunities
        )
        self.persist_optimization_snapshot(
            conn,
            workspace_id=workspace_id,
            recommendation_count=upserted,
            window_label=window_label,
            metadata={
                "feature_effectiveness_count": len(feature_effectiveness),
                "context_fit_count": len(context_fit),
                "opportunities_evaluated": len(opportunities),
            },
        )
        conn.commit()

        logger.info(
            "governance_policy_optimization workspace=%s feature_eff=%d context_fit=%d opps=%d",
            workspace_id, len(feature_effectiveness), len(context_fit), upserted,
        )
        return GovernancePolicyOptimizationResult(
            feature_effectiveness=feature_effectiveness,
            context_fit=context_fit,
            opportunities=opportunities,
            recommendation_count=upserted,
        )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _maybe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if f != f else f  # NaN guard
    except (TypeError, ValueError):
        return None
