from __future__ import annotations

from dataclasses import dataclass
from typing import Any


VALID_REVIEW_STATUSES = frozenset({"approved", "rejected", "deferred"})


@dataclass(frozen=True)
class ReviewDecision:
    workspace_id: str
    recommendation_key: str
    review_status: str
    review_reason: str | None
    reviewed_by: str
    notes: str | None
    metadata: dict[str, Any]


class RoutingPolicyReviewService:
    """
    Pure-logic service for routing policy recommendation reviews.
    No DB calls — all persistence is the caller's responsibility.
    """

    def validate_review(
        self,
        *,
        recommendation_key: str,
        review_status: str,
        reviewed_by: str,
    ) -> None:
        if not recommendation_key:
            raise ValueError("recommendation_key is required")
        if review_status not in VALID_REVIEW_STATUSES:
            raise ValueError(f"review_status must be one of {sorted(VALID_REVIEW_STATUSES)}")
        if not reviewed_by:
            raise ValueError("reviewed_by is required")

    def build_review_decision(
        self,
        *,
        workspace_id: str,
        recommendation_key: str,
        review_status: str,
        review_reason: str | None = None,
        reviewed_by: str,
        notes: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ReviewDecision:
        self.validate_review(
            recommendation_key=recommendation_key,
            review_status=review_status,
            reviewed_by=reviewed_by,
        )
        return ReviewDecision(
            workspace_id=workspace_id,
            recommendation_key=recommendation_key,
            review_status=review_status,
            review_reason=review_reason,
            reviewed_by=reviewed_by,
            notes=notes,
            metadata={**(metadata or {}), "source": "ops_api"},
        )

    def is_approved(self, latest_review_status: str | None) -> bool:
        return latest_review_status == "approved"

    def is_eligible_for_proposal(
        self,
        *,
        latest_review_status: str | None,
        existing_proposal_status: str | None,
    ) -> bool:
        """A recommendation is eligible for promotion proposal if it has been approved
        and does not already have a pending/approved/applied proposal."""
        if latest_review_status != "approved":
            return False
        if existing_proposal_status in ("pending", "approved", "applied"):
            return False
        return True
