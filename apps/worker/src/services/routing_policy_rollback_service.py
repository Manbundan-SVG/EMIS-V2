from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RollbackReviewDecision:
    workspace_id: str
    rollback_candidate_id: str
    review_status: str  # approved | rejected | deferred
    review_reason: str | None
    reviewed_by: str
    notes: str | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RollbackExecutionResult:
    rollback_candidate_id: str
    execution_target: str   # override | rule
    scope_type: str
    scope_value: str
    restored_policy: dict[str, Any]
    replaced_policy: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


class RoutingPolicyRollbackService:
    """Pure-logic service for rollback review validation and execution preparation."""

    VALID_REVIEW_STATUSES = frozenset({"approved", "rejected", "deferred"})
    VALID_ROUTING_TABLES = frozenset({"governance_routing_rules", "governance_routing_overrides"})

    def validate_rollback_review(
        self,
        *,
        rollback_candidate_id: str,
        review_status: str,
        reviewed_by: str,
    ) -> None:
        if not rollback_candidate_id:
            raise ValueError("rollback_candidate_id is required")
        if review_status not in self.VALID_REVIEW_STATUSES:
            raise ValueError(
                f"invalid review_status {review_status!r}; "
                f"must be one of {sorted(self.VALID_REVIEW_STATUSES)}"
            )
        if not reviewed_by:
            raise ValueError("reviewed_by is required")

    def build_review_decision(
        self,
        *,
        workspace_id: str,
        rollback_candidate_id: str,
        review_status: str,
        review_reason: str | None = None,
        reviewed_by: str,
        notes: str | None = None,
    ) -> RollbackReviewDecision:
        self.validate_rollback_review(
            rollback_candidate_id=rollback_candidate_id,
            review_status=review_status,
            reviewed_by=reviewed_by,
        )
        return RollbackReviewDecision(
            workspace_id=workspace_id,
            rollback_candidate_id=rollback_candidate_id,
            review_status=review_status,
            review_reason=review_reason,
            reviewed_by=reviewed_by,
            notes=notes,
            metadata={"source": "rollback_review_service"},
        )

    def get_latest_rollback_review_status(
        self,
        *,
        review_summary: dict[str, Any] | None,
    ) -> str | None:
        if review_summary is None:
            return None
        return review_summary.get("latest_review_status")

    def is_candidate_approved_for_rollback(
        self,
        *,
        review_summary: dict[str, Any] | None,
    ) -> bool:
        return self.get_latest_rollback_review_status(review_summary=review_summary) == "approved"

    def validate_rollback_execution(
        self,
        *,
        candidate: dict[str, Any],
        review_summary: dict[str, Any] | None,
        executed_by: str,
    ) -> None:
        """Raise ValueError with a clear reason if execution cannot proceed."""
        if not executed_by:
            raise ValueError("executed_by is required")

        if candidate.get("resolved"):
            raise ValueError(
                f"rollback candidate {candidate['id']} has already been rolled back "
                f"at {candidate.get('resolved_at')}"
            )

        if not self.is_candidate_approved_for_rollback(review_summary=review_summary):
            status = self.get_latest_rollback_review_status(review_summary=review_summary)
            raise ValueError(
                f"rollback candidate requires an approved review before execution; "
                f"current review status: {status!r}"
            )

        routing_table = candidate.get("routing_table")
        if routing_table and routing_table not in self.VALID_ROUTING_TABLES:
            raise ValueError(
                f"unknown routing_table {routing_table!r} on rollback candidate"
            )

    def validate_optimistic_policy_consistency(
        self,
        *,
        candidate_applied_policy: dict[str, Any],
        live_policy_snapshot: dict[str, Any] | None,
    ) -> None:
        """Optionally verify live policy still matches what was promoted.

        Raises ValueError if significant drift is detected. Pass live_policy_snapshot=None
        to skip this check (best-effort mode).
        """
        if live_policy_snapshot is None:
            return
        # compare top-level keys that matter for routing
        routing_keys = {"preferred_team", "preferred_operator", "assign_team", "assign_user"}
        for key in routing_keys:
            promoted_val = candidate_applied_policy.get(key)
            live_val = live_policy_snapshot.get(key)
            if promoted_val is not None and live_val is not None and promoted_val != live_val:
                raise ValueError(
                    f"live policy has drifted from promoted state: "
                    f"key={key!r} promoted={promoted_val!r} live={live_val!r}; "
                    "manual inspection required before rollback"
                )

    def build_execution_target(self, *, routing_table: str | None) -> str:
        if routing_table == "governance_routing_overrides":
            return "override"
        return "rule"

    def execute_routing_policy_rollback(
        self,
        *,
        candidate: dict[str, Any],
        review_summary: dict[str, Any] | None,
        executed_by: str,
        replaced_policy: dict[str, Any] | None = None,
        live_policy_snapshot: dict[str, Any] | None = None,
    ) -> RollbackExecutionResult:
        self.validate_rollback_execution(
            candidate=candidate,
            review_summary=review_summary,
            executed_by=executed_by,
        )
        # optionally check for drift
        if live_policy_snapshot is not None:
            self.validate_optimistic_policy_consistency(
                candidate_applied_policy=candidate.get("applied_policy") or {},
                live_policy_snapshot=live_policy_snapshot,
            )

        prior_policy = candidate.get("prior_policy") or {}
        execution_target = self.build_execution_target(routing_table=candidate.get("routing_table"))

        return RollbackExecutionResult(
            rollback_candidate_id=str(candidate["id"]),
            execution_target=execution_target,
            scope_type=candidate.get("scope_type", ""),
            scope_value=candidate.get("scope_value", ""),
            restored_policy=prior_policy,
            replaced_policy=replaced_policy or (candidate.get("applied_policy") or {}),
            metadata={
                "source": "routing_policy_rollback",
                "executed_by": executed_by,
                "routing_table": candidate.get("routing_table"),
                "routing_row_id": str(candidate.get("routing_row_id") or ""),
            },
        )

    def build_rollback_metadata(
        self,
        *,
        candidate: dict[str, Any],
        executed_by: str,
        review_summary: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "source": "routing_policy_rollback_service",
            "executed_by": executed_by,
            "rollback_candidate_id": str(candidate.get("id", "")),
            "recommendation_key": candidate.get("recommendation_key", ""),
            "scope_type": candidate.get("scope_type", ""),
            "scope_value": candidate.get("scope_value", ""),
            "routing_table": candidate.get("routing_table"),
            "approved_by": (review_summary or {}).get("latest_reviewed_by"),
        }
