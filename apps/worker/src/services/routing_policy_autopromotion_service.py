from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

CONFIDENCE_RANK = {"low": 0, "medium": 1, "high": 2}

VALID_OUTCOMES = frozenset({"promoted", "skipped", "blocked"})


@dataclass(frozen=True)
class AutopromotionPolicy:
    id: str
    workspace_id: str
    enabled: bool
    scope_type: str
    scope_value: str
    promotion_target: str
    min_confidence: str
    min_approved_review_count: int
    min_application_count: int
    min_sample_size: int
    max_recent_override_rate: float
    max_recent_reassignment_rate: float
    cooldown_hours: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AutopromotionEligibility:
    eligible: bool
    blocked_reason: str | None
    approved_review_count: int
    application_count: int
    in_cooldown: bool


@dataclass(frozen=True)
class AutopromotionDecision:
    outcome: str          # promoted | skipped | blocked
    blocked_reason: str | None
    skipped_reason: str | None
    prior_policy: dict[str, Any]
    applied_policy: dict[str, Any]
    proposal_metadata: dict[str, Any]


class RoutingPolicyAutopromotionService:
    """Pure-logic service for policy-level routing autopromotion guardrails."""

    def select_policy_for_recommendation(
        self,
        *,
        policies: list[dict[str, Any]],
        scope_type: str,
        scope_value: str,
    ) -> dict[str, Any] | None:
        for p in policies:
            if p["scope_type"] == scope_type and p["scope_value"] == scope_value and p.get("enabled", True):
                return p
        return None

    def evaluate_autopromotion_eligibility(
        self,
        *,
        policy: dict[str, Any],
        recommendation: dict[str, Any],
        approved_review_count: int,
        application_count: int,
        last_promoted_at_hours_ago: float | None,
        recent_override_rate: float = 0.0,
        recent_reassignment_rate: float = 0.0,
    ) -> AutopromotionEligibility:
        confidence = recommendation.get("confidence", "low")
        sample_size = recommendation.get("sample_size", 0) or 0

        min_conf_rank = CONFIDENCE_RANK.get(policy.get("min_confidence", "high"), 2)
        rec_conf_rank = CONFIDENCE_RANK.get(confidence, 0)

        if rec_conf_rank < min_conf_rank:
            return AutopromotionEligibility(
                eligible=False,
                blocked_reason="confidence_below_threshold",
                approved_review_count=approved_review_count,
                application_count=application_count,
                in_cooldown=False,
            )

        if sample_size < (policy.get("min_sample_size") or 50):
            return AutopromotionEligibility(
                eligible=False,
                blocked_reason="insufficient_sample_size",
                approved_review_count=approved_review_count,
                application_count=application_count,
                in_cooldown=False,
            )

        if approved_review_count < (policy.get("min_approved_review_count") or 1):
            return AutopromotionEligibility(
                eligible=False,
                blocked_reason="insufficient_approved_reviews",
                approved_review_count=approved_review_count,
                application_count=application_count,
                in_cooldown=False,
            )

        if application_count < (policy.get("min_application_count") or 1):
            return AutopromotionEligibility(
                eligible=False,
                blocked_reason="insufficient_manual_applications",
                approved_review_count=approved_review_count,
                application_count=application_count,
                in_cooldown=False,
            )

        max_override_rate = policy.get("max_recent_override_rate") or 0.20
        if recent_override_rate > max_override_rate:
            return AutopromotionEligibility(
                eligible=False,
                blocked_reason="override_rate_too_high",
                approved_review_count=approved_review_count,
                application_count=application_count,
                in_cooldown=False,
            )

        max_reassignment_rate = policy.get("max_recent_reassignment_rate") or 0.15
        if recent_reassignment_rate > max_reassignment_rate:
            return AutopromotionEligibility(
                eligible=False,
                blocked_reason="reassignment_rate_too_high",
                approved_review_count=approved_review_count,
                application_count=application_count,
                in_cooldown=False,
            )

        cooldown_hours = policy.get("cooldown_hours") or 168
        in_cooldown = (
            last_promoted_at_hours_ago is not None
            and last_promoted_at_hours_ago < cooldown_hours
        )
        if in_cooldown:
            return AutopromotionEligibility(
                eligible=False,
                blocked_reason="in_cooldown",
                approved_review_count=approved_review_count,
                application_count=application_count,
                in_cooldown=True,
            )

        return AutopromotionEligibility(
            eligible=True,
            blocked_reason=None,
            approved_review_count=approved_review_count,
            application_count=application_count,
            in_cooldown=False,
        )

    def is_in_cooldown(
        self,
        *,
        last_promoted_at_hours_ago: float | None,
        cooldown_hours: int,
    ) -> bool:
        if last_promoted_at_hours_ago is None:
            return False
        return last_promoted_at_hours_ago < cooldown_hours

    def build_autopromotion_decision(
        self,
        *,
        eligibility: AutopromotionEligibility,
        prior_policy: dict[str, Any],
        recommended_policy: dict[str, Any],
        policy_id: str,
        recommendation_key: str,
    ) -> AutopromotionDecision:
        if not eligibility.eligible:
            skipped = eligibility.blocked_reason in (
                "in_cooldown",
                "no_policy_coverage",
            )
            return AutopromotionDecision(
                outcome="skipped" if skipped else "blocked",
                blocked_reason=None if skipped else eligibility.blocked_reason,
                skipped_reason=eligibility.blocked_reason if skipped else None,
                prior_policy=prior_policy,
                applied_policy={},
                proposal_metadata={},
            )

        return AutopromotionDecision(
            outcome="promoted",
            blocked_reason=None,
            skipped_reason=None,
            prior_policy=prior_policy,
            applied_policy=recommended_policy,
            proposal_metadata={
                "source": "routing_policy_autopromotion",
                "policy_id": policy_id,
                "recommendation_key": recommendation_key,
            },
        )

    def create_rollback_candidate(
        self,
        *,
        workspace_id: str,
        execution_id: str,
        recommendation_key: str,
        scope_type: str,
        scope_value: str,
        prior_policy: dict[str, Any],
        applied_policy: dict[str, Any],
        routing_row_id: str | None,
        routing_table: str | None,
    ) -> dict[str, Any]:
        return {
            "workspace_id": workspace_id,
            "execution_id": execution_id,
            "recommendation_key": recommendation_key,
            "scope_type": scope_type,
            "scope_value": scope_value,
            "prior_policy": prior_policy,
            "applied_policy": applied_policy,
            "routing_row_id": routing_row_id,
            "routing_table": routing_table,
        }
