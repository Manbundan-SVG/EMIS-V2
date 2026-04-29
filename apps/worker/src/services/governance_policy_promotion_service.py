"""Phase 3.7B: Governance policy promotion service."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

VALID_PROPOSAL_STATUS = frozenset({"pending", "approved", "rejected", "applied", "deferred"})
VALID_PROMOTION_TARGET = frozenset({
    "threshold_profile",
    "routing_rule",
    "routing_override",
    "autopromotion_policy",
})

# Scope-type → column mapping for routing targets (mirrors Phase 3.6A restore pattern)
_ROUTING_RULE_SCOPE_COLUMNS: dict[str, str] = {
    "team":       "assign_team",
    "root_cause": "root_cause_code",
    "regime":     "regime",
    "severity":   "severity",
    "chronicity": "chronic_only",
}
_ROUTING_OVERRIDE_SCOPE_COLUMNS: dict[str, str] = {
    "operator": "assigned_user",
    "team":     "assign_team",
}


@dataclass(frozen=True)
class GovernancePolicyProposalRequest:
    workspace_id: str
    recommendation_key: str
    policy_family: str
    promotion_target: str
    scope_type: str
    scope_value: str
    current_policy: dict[str, Any]
    recommended_policy: dict[str, Any]
    proposed_by: str
    proposal_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GovernancePolicyApplicationResult:
    workspace_id: str
    proposal_id: str
    recommendation_key: str
    policy_family: str
    applied_target: str
    applied_scope_type: str
    applied_scope_value: str
    prior_policy: dict[str, Any]
    applied_policy: dict[str, Any]
    applied_by: str
    rollback_candidate: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


class GovernancePolicyPromotionService:

    # ── Validation ────────────────────────────────────────────────────────────

    def validate_proposal(self, req: GovernancePolicyProposalRequest) -> None:
        if req.promotion_target not in VALID_PROMOTION_TARGET:
            raise ValueError(
                f"invalid promotion_target: {req.promotion_target!r}; "
                f"must be one of {sorted(VALID_PROMOTION_TARGET)}"
            )
        if not req.workspace_id:
            raise ValueError("workspace_id is required")
        if not req.recommendation_key:
            raise ValueError("recommendation_key is required")
        if not req.policy_family:
            raise ValueError("policy_family is required")
        if not req.scope_type or not req.scope_value:
            raise ValueError("scope_type and scope_value are required")
        if not req.proposed_by:
            raise ValueError("proposed_by is required")

    def validate_apply_allowed(self, proposal: dict[str, Any]) -> None:
        status = proposal.get("proposal_status")
        if status != "approved":
            raise ValueError(
                f"proposal must be in 'approved' state before apply; current: {status!r}"
            )

    def validate_optimistic_consistency(
        self,
        proposal: dict[str, Any],
        live_current_policy: dict[str, Any] | None,
    ) -> None:
        """
        Soft check: warn if the live policy has drifted from what the proposal
        captured as current_policy. Does not block application but logs a warning.
        """
        if live_current_policy is None:
            return
        captured = proposal.get("current_policy") or {}
        for key, captured_val in captured.items():
            live_val = live_current_policy.get(key)
            if live_val is not None and live_val != captured_val:
                logger.warning(
                    "optimistic_consistency_drift key=%s captured=%r live=%r proposal_id=%s",
                    key, captured_val, live_val, proposal.get("id"),
                )

    # ── Proposal builder ──────────────────────────────────────────────────────

    def build_proposal_row(self, req: GovernancePolicyProposalRequest) -> dict[str, Any]:
        self.validate_proposal(req)
        return {
            "workspace_id":     req.workspace_id,
            "recommendation_key": req.recommendation_key,
            "policy_family":    req.policy_family,
            "proposal_status":  "pending",
            "promotion_target": req.promotion_target,
            "scope_type":       req.scope_type,
            "scope_value":      req.scope_value,
            "current_policy":   req.current_policy,
            "recommended_policy": req.recommended_policy,
            "proposed_by":      req.proposed_by,
            "proposal_reason":  req.proposal_reason,
            "metadata":         req.metadata,
        }

    # ── Application dispatch ──────────────────────────────────────────────────

    def apply_proposal(
        self,
        conn: Any,
        *,
        workspace_id: str,
        proposal: dict[str, Any],
        applied_by: str,
    ) -> GovernancePolicyApplicationResult:
        """
        Approval-gated policy application.
        Captures prior_policy before mutation, applies recommended_policy additively,
        and returns an auditable result object.
        """
        import src.db.repositories as repo  # avoid circular at module level

        self.validate_apply_allowed(proposal)

        target = proposal["promotion_target"]
        scope_type = proposal["scope_type"]
        scope_value = proposal["scope_value"]
        recommended_policy = proposal["recommended_policy"]
        current_policy = proposal["current_policy"]

        prior_policy: dict[str, Any] = dict(current_policy)

        if target == "routing_rule":
            prior_policy = _capture_routing_rule_state(conn, workspace_id, scope_type, scope_value)
            self.validate_optimistic_consistency(proposal, prior_policy)
            _apply_routing_rule(conn, workspace_id, scope_type, scope_value, recommended_policy, applied_by, proposal)

        elif target == "routing_override":
            prior_policy = _capture_routing_override_state(conn, workspace_id, scope_type, scope_value)
            self.validate_optimistic_consistency(proposal, prior_policy)
            _apply_routing_override(conn, workspace_id, scope_type, scope_value, recommended_policy, applied_by, proposal)

        elif target == "threshold_profile":
            prior_policy = _capture_threshold_profile_state(conn, workspace_id, scope_type, scope_value)
            self.validate_optimistic_consistency(proposal, prior_policy)
            _apply_threshold_profile(conn, workspace_id, scope_type, scope_value, recommended_policy, applied_by, proposal)

        elif target == "autopromotion_policy":
            prior_policy = _capture_autopromotion_policy_state(conn, workspace_id, scope_type, scope_value)
            self.validate_optimistic_consistency(proposal, prior_policy)
            _apply_autopromotion_policy(conn, workspace_id, scope_type, scope_value, recommended_policy, applied_by, proposal)

        else:
            raise ValueError(f"unsupported promotion_target: {target!r}")

        return GovernancePolicyApplicationResult(
            workspace_id=workspace_id,
            proposal_id=str(proposal["id"]),
            recommendation_key=proposal["recommendation_key"],
            policy_family=proposal["policy_family"],
            applied_target=target,
            applied_scope_type=scope_type,
            applied_scope_value=scope_value,
            prior_policy=prior_policy,
            applied_policy=recommended_policy,
            applied_by=applied_by,
            rollback_candidate=True,
            metadata={
                "source": "governance_policy_promotion",
                "proposal_id": str(proposal["id"]),
            },
        )


# ── Target-specific apply helpers ─────────────────────────────────────────────

def _capture_routing_rule_state(conn: Any, workspace_id: str, scope_type: str, scope_value: str) -> dict[str, Any]:
    col = _ROUTING_RULE_SCOPE_COLUMNS.get(scope_type)
    if not col:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            f"select * from public.governance_routing_rules"
            f" where workspace_id = %s::uuid and {col} = %s and is_enabled = true"
            f" order by priority asc, id desc limit 1",
            (workspace_id, scope_value),
        )
        row = cur.fetchone()
        return dict(row) if row else {}


def _apply_routing_rule(
    conn: Any,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
    recommended_policy: dict[str, Any],
    applied_by: str,
    proposal: dict[str, Any],
) -> None:
    """Additive INSERT into governance_routing_rules — preserves history."""
    assign_team = recommended_policy.get("assign_team") or (scope_value if scope_type == "team" else None)
    root_cause_code = recommended_policy.get("root_cause_code") or (scope_value if scope_type == "root_cause" else None)
    regime = recommended_policy.get("regime") or (scope_value if scope_type == "regime" else None)
    severity = recommended_policy.get("severity") or (scope_value if scope_type == "severity" else None)
    chronic_only = recommended_policy.get("chronic_only", scope_type == "chronicity")

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_rules
                (workspace_id, is_enabled, priority, root_cause_code, severity,
                 regime, chronic_only, assign_team, routing_reason_template, metadata)
            values (%s::uuid, true, 4, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                workspace_id,
                root_cause_code,
                severity,
                regime,
                chronic_only,
                assign_team,
                f"governance_policy_promotion:{applied_by}",
                _jsonb({
                    "source": "governance_policy_promotion",
                    "applied_by": applied_by,
                    "proposal_id": str(proposal.get("id", "")),
                }),
            ),
        )


def _capture_routing_override_state(conn: Any, workspace_id: str, scope_type: str, scope_value: str) -> dict[str, Any]:
    col = _ROUTING_OVERRIDE_SCOPE_COLUMNS.get(scope_type)
    if not col:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            f"select * from public.governance_routing_overrides"
            f" where workspace_id = %s::uuid and {col} = %s and is_enabled = true"
            f" order by id desc limit 1",
            (workspace_id, scope_value),
        )
        row = cur.fetchone()
        return dict(row) if row else {}


def _apply_routing_override(
    conn: Any,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
    recommended_policy: dict[str, Any],
    applied_by: str,
    proposal: dict[str, Any],
) -> None:
    """Additive INSERT into governance_routing_overrides."""
    assigned_user = recommended_policy.get("assigned_user") or (scope_value if scope_type == "operator" else None)
    assigned_team = recommended_policy.get("assign_team") or (scope_value if scope_type == "team" else None)

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_overrides
                (workspace_id, assigned_user, assign_team, reason, is_enabled, metadata)
            values (%s::uuid, %s, %s, %s, true, %s::jsonb)
            """,
            (
                workspace_id,
                assigned_user,
                assigned_team,
                f"governance_policy_promotion:{applied_by}",
                _jsonb({
                    "source": "governance_policy_promotion",
                    "applied_by": applied_by,
                    "proposal_id": str(proposal.get("id", "")),
                }),
            ),
        )


def _capture_threshold_profile_state(conn: Any, workspace_id: str, scope_type: str, scope_value: str) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.regime_threshold_profiles"
            " where workspace_id = %s::uuid and regime = %s"
            " order by id desc limit 1",
            (workspace_id, scope_value),
        )
        row = cur.fetchone()
        return dict(row) if row else {}


def _apply_threshold_profile(
    conn: Any,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
    recommended_policy: dict[str, Any],
    applied_by: str,
    proposal: dict[str, Any],
) -> None:
    """
    Updates threshold profile for the given regime.
    Only touches columns explicitly in recommended_policy; unknown keys are logged and skipped.
    """
    allowed_columns = {
        "alert_threshold", "warning_threshold", "critical_threshold",
        "min_sample_size", "cooldown_minutes", "is_active",
    }
    updates = {k: v for k, v in recommended_policy.items() if k in allowed_columns}
    if not updates:
        logger.warning(
            "apply_threshold_profile: no recognized columns in recommended_policy for scope=%s/%s",
            scope_type, scope_value,
        )
        return

    set_clauses = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [workspace_id, scope_value]
    with conn.cursor() as cur:
        cur.execute(
            f"update public.regime_threshold_profiles"
            f" set {set_clauses}"
            f" where workspace_id = %s::uuid and regime = %s",
            values,
        )


def _capture_autopromotion_policy_state(conn: Any, workspace_id: str, scope_type: str, scope_value: str) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_autopromotion_policies"
            " where workspace_id = %s::uuid and scope_type = %s and scope_value = %s"
            " limit 1",
            (workspace_id, scope_type, scope_value),
        )
        row = cur.fetchone()
        return dict(row) if row else {}


def _apply_autopromotion_policy(
    conn: Any,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
    recommended_policy: dict[str, Any],
    applied_by: str,
    proposal: dict[str, Any],
) -> None:
    """Upserts autopromotion policy settings for the given scope."""
    allowed_columns = {
        "min_confidence", "min_sample_size", "max_override_rate",
        "max_reassignment_rate", "cooldown_hours", "promotion_target", "enabled",
    }
    updates = {k: v for k, v in recommended_policy.items() if k in allowed_columns}
    if not updates:
        logger.warning(
            "apply_autopromotion_policy: no recognized columns for scope=%s/%s",
            scope_type, scope_value,
        )
        return

    set_clauses = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [workspace_id, scope_type, scope_value]
    with conn.cursor() as cur:
        cur.execute(
            f"update public.governance_routing_policy_autopromotion_policies"
            f" set {set_clauses}"
            f" where workspace_id = %s::uuid and scope_type = %s and scope_value = %s",
            values,
        )


def _jsonb(d: dict[str, Any]) -> str:
    import json
    return json.dumps(d, default=str)
