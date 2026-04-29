from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RecommendationReviewDecision:
    recommendation_id: str
    reviewer: str
    decision: str
    rationale: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class PromotionProposalDraft:
    recommendation_id: str
    profile_id: str
    event_type: str
    dimension_type: str
    dimension_value: str | None
    current_value: float
    proposed_value: float
    source_metrics: dict[str, Any]
    metadata: dict[str, Any]


class ThresholdReviewService:
    """Advisory-first review helper for threshold promotion flow."""

    def build_review_decision(
        self,
        *,
        recommendation_id: str,
        reviewer: str,
        decision: str,
        rationale: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RecommendationReviewDecision:
        normalized = decision.strip().lower()
        if normalized not in {"approved", "rejected", "deferred"}:
            raise ValueError(f"unsupported threshold review decision: {decision}")
        return RecommendationReviewDecision(
            recommendation_id=recommendation_id,
            reviewer=reviewer,
            decision=normalized,
            rationale=rationale.strip() if rationale else None,
            metadata=metadata or {},
        )

    def build_promotion_proposal(
        self,
        recommendation: dict[str, Any],
    ) -> PromotionProposalDraft | None:
        profile_id = recommendation.get("threshold_profile_id")
        current_value = recommendation.get("current_value")
        proposed_value = recommendation.get("recommended_value")
        direction = str(recommendation.get("direction") or "keep")
        if (
            not profile_id
            or current_value is None
            or proposed_value is None
            or direction == "keep"
        ):
            return None

        return PromotionProposalDraft(
            recommendation_id=str(recommendation["recommendation_id"]),
            profile_id=str(profile_id),
            event_type=str(recommendation["event_type"]),
            dimension_type=str(recommendation["dimension_type"]),
            dimension_value=(
                str(recommendation["dimension_value"])
                if recommendation.get("dimension_value") is not None
                else None
            ),
            current_value=float(current_value),
            proposed_value=float(proposed_value),
            source_metrics=dict(recommendation.get("supporting_metrics") or {}),
            metadata={
                "reason_code": recommendation.get("reason_code"),
                "confidence": recommendation.get("confidence"),
                "direction": direction,
            },
        )
