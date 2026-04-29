from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RoutingRecommendationReview:
    workspace_id: str
    recommendation_id: str
    case_id: str | None
    review_status: str
    review_reason: str | None
    notes: str | None
    reviewed_by: str | None
    apply_immediately: bool = False


class RoutingRecommendationReviewService:
    VALID_STATUSES = {"approved", "rejected", "deferred"}

    def validate(self, review: RoutingRecommendationReview) -> None:
        if review.review_status not in self.VALID_STATUSES:
            raise ValueError(f"invalid review status: {review.review_status}")
        if not review.workspace_id or not review.recommendation_id:
            raise ValueError("workspace_id and recommendation_id are required")
        if review.apply_immediately and review.review_status != "approved":
            raise ValueError("apply_immediately requires approved review_status")

    def build_review_row(self, review: RoutingRecommendationReview) -> dict[str, object]:
        self.validate(review)
        return {
            "workspace_id": review.workspace_id,
            "recommendation_id": review.recommendation_id,
            "case_id": review.case_id,
            "review_status": review.review_status,
            "review_reason": review.review_reason,
            "notes": review.notes,
            "reviewed_by": review.reviewed_by,
            "applied_immediately": review.apply_immediately,
        }
