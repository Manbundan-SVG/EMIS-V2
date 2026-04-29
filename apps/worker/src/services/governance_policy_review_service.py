"""Phase 3.7B: Governance policy review service."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

VALID_REVIEW_STATUS = frozenset({"approved", "rejected", "deferred"})


@dataclass(frozen=True)
class GovernancePolicyReviewDecision:
    workspace_id: str
    recommendation_key: str
    policy_family: str
    review_status: str
    reviewed_by: str
    review_reason: str | None
    notes: str | None
    metadata: dict[str, Any] = field(default_factory=dict)


class GovernancePolicyReviewService:
    def validate(
        self,
        *,
        workspace_id: str,
        recommendation_key: str,
        policy_family: str,
        review_status: str,
        reviewed_by: str,
    ) -> None:
        if review_status not in VALID_REVIEW_STATUS:
            raise ValueError(
                f"invalid review_status: {review_status!r}; "
                f"must be one of {sorted(VALID_REVIEW_STATUS)}"
            )
        if not workspace_id:
            raise ValueError("workspace_id is required")
        if not recommendation_key:
            raise ValueError("recommendation_key is required")
        if not policy_family:
            raise ValueError("policy_family is required")
        if not reviewed_by:
            raise ValueError("reviewed_by is required")

    def build_decision(
        self,
        *,
        workspace_id: str,
        recommendation_key: str,
        policy_family: str,
        review_status: str,
        reviewed_by: str,
        review_reason: str | None = None,
        notes: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> GovernancePolicyReviewDecision:
        self.validate(
            workspace_id=workspace_id,
            recommendation_key=recommendation_key,
            policy_family=policy_family,
            review_status=review_status,
            reviewed_by=reviewed_by,
        )
        return GovernancePolicyReviewDecision(
            workspace_id=workspace_id,
            recommendation_key=recommendation_key,
            policy_family=policy_family,
            review_status=review_status,
            reviewed_by=reviewed_by,
            review_reason=review_reason,
            notes=notes,
            metadata=metadata or {},
        )
