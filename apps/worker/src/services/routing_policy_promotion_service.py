from __future__ import annotations

from dataclasses import dataclass
from typing import Any


VALID_PROPOSAL_STATUSES = frozenset({"pending", "approved", "rejected", "applied", "deferred"})
VALID_PROMOTION_TARGETS = frozenset({"override", "rule"})

# Maps 3.5A scope_type → promotion_target (default classification).
# Operators are most naturally expressed as overrides; structural patterns as rules.
SCOPE_TO_DEFAULT_TARGET: dict[str, str] = {
    "operator":    "override",
    "team":        "rule",
    "root_cause":  "rule",
    "regime":      "rule",
    "severity":    "rule",
    "chronicity":  "rule",
}


@dataclass(frozen=True)
class PromotionProposal:
    workspace_id: str
    recommendation_key: str
    promotion_target: str
    scope_type: str
    scope_value: str
    current_policy: dict[str, Any]
    recommended_policy: dict[str, Any]
    proposed_by: str
    proposal_reason: str | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class RoutingFieldMapping:
    """Translated form of scope_type/scope_value ready for the routing tables."""
    target_table: str               # "governance_routing_overrides" or "governance_routing_rules"
    assign_user: str | None         # → governance_routing_overrides.assigned_user
    assign_team: str | None         # → governance_routing_overrides.assigned_team / rules.assign_team
    root_cause_code: str | None     # → rules.root_cause_code
    regime: str | None              # → rules.regime
    severity: str | None            # → rules.severity
    chronic_only: bool              # → rules.chronic_only


class RoutingPolicyPromotionService:
    """
    Pure-logic service for routing policy promotion proposals and applications.
    No DB calls — all persistence is the caller's responsibility.
    """

    def default_target_for_scope(self, scope_type: str) -> str:
        return SCOPE_TO_DEFAULT_TARGET.get(scope_type, "rule")

    def validate_proposal(
        self,
        *,
        recommendation_key: str,
        promotion_target: str,
        scope_type: str,
        scope_value: str,
        proposed_by: str,
    ) -> None:
        if not recommendation_key:
            raise ValueError("recommendation_key is required")
        if promotion_target not in VALID_PROMOTION_TARGETS:
            raise ValueError(f"promotion_target must be one of {sorted(VALID_PROMOTION_TARGETS)}")
        if not scope_type:
            raise ValueError("scope_type is required")
        if not scope_value:
            raise ValueError("scope_value is required")
        if not proposed_by:
            raise ValueError("proposed_by is required")

    def build_promotion_proposal(
        self,
        *,
        workspace_id: str,
        recommendation_key: str,
        promotion_target: str,
        scope_type: str,
        scope_value: str,
        current_policy: dict[str, Any],
        recommended_policy: dict[str, Any],
        proposed_by: str,
        proposal_reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PromotionProposal:
        self.validate_proposal(
            recommendation_key=recommendation_key,
            promotion_target=promotion_target,
            scope_type=scope_type,
            scope_value=scope_value,
            proposed_by=proposed_by,
        )
        return PromotionProposal(
            workspace_id=workspace_id,
            recommendation_key=recommendation_key,
            promotion_target=promotion_target,
            scope_type=scope_type,
            scope_value=scope_value,
            current_policy=current_policy,
            recommended_policy=recommended_policy,
            proposed_by=proposed_by,
            proposal_reason=proposal_reason,
            metadata={**(metadata or {}), "source": "ops_api"},
        )

    def validate_apply(
        self,
        *,
        proposal: dict[str, Any],
        applied_by: str,
    ) -> None:
        if proposal.get("proposal_status") != "approved":
            raise ValueError(
                f"proposal must be approved before application; "
                f"current status: {proposal.get('proposal_status')!r}"
            )
        if not applied_by:
            raise ValueError("applied_by is required")

    def validate_optimistic_consistency(
        self,
        *,
        proposal_current_policy: dict[str, Any],
        live_current_policy: dict[str, Any],
    ) -> None:
        """Reject apply if the live policy has changed since the proposal was created.
        Compares the subset of keys present in proposal_current_policy."""
        for key, expected_val in proposal_current_policy.items():
            live_val = live_current_policy.get(key)
            if live_val != expected_val:
                raise ValueError(
                    f"optimistic consistency check failed: live policy key {key!r} "
                    f"has value {live_val!r}, expected {expected_val!r}. "
                    "Re-create the proposal with the current live policy."
                )

    def map_scope_to_routing_fields(
        self,
        *,
        scope_type: str,
        scope_value: str,
        promotion_target: str,
        recommended_policy: dict[str, Any],
    ) -> RoutingFieldMapping:
        """Translate a 3.5A scope_type/scope_value pair into concrete routing table column values."""
        assign_user: str | None = None
        assign_team: str | None = None
        root_cause_code: str | None = None
        regime: str | None = None
        severity: str | None = None
        chronic_only: bool = False
        target_table: str

        if promotion_target == "override":
            target_table = "governance_routing_overrides"
            if scope_type == "operator":
                assign_user = scope_value
            elif scope_type == "team":
                assign_team = scope_value
        else:
            target_table = "governance_routing_rules"
            if scope_type == "team":
                assign_team = (
                    recommended_policy.get("preferred_team")
                    or recommended_policy.get("preferred_team_for_reopens")
                    or scope_value
                )
            elif scope_type == "root_cause":
                root_cause_code = scope_value
                assign_team = (
                    recommended_policy.get("preferred_team")
                    or recommended_policy.get("preferred_team_for_reopens")
                )
            elif scope_type == "regime":
                regime = scope_value
                assign_team = recommended_policy.get("preferred_team")
            elif scope_type == "severity":
                severity = scope_value
                assign_team = recommended_policy.get("preferred_team")
            elif scope_type == "chronicity":
                chronic_only = True
                assign_team = recommended_policy.get("preferred_team")
            else:
                # fallback: treat scope_value as a team name
                assign_team = scope_value

        return RoutingFieldMapping(
            target_table=target_table,
            assign_user=assign_user,
            assign_team=assign_team,
            root_cause_code=root_cause_code,
            regime=regime,
            severity=severity,
            chronic_only=chronic_only,
        )
