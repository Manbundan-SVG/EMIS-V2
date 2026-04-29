"""Phase 3.7C: Governance Policy Autopromotion Service."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

VALID_PROMOTION_TARGETS = frozenset({
    "threshold_profile", "routing_rule", "routing_override", "autopromotion_policy",
})


@dataclass(frozen=True)
class GovernancePolicyAutopromotionDecision:
    eligible: bool
    blocked_reason_code: str | None
    recommendation_key: str
    policy_id: str | None
    policy_family: str
    scope_type: str
    scope_value: str
    promotion_target: str | None
    confidence: str | None
    sample_size: int
    approved_review_count: int
    application_count: int
    recent_override_rate: float
    recent_reassignment_rate: float
    last_execution_at: Any
    cooldown_ends_at: Any


@dataclass(frozen=True)
class GovernancePolicyAutopromotionResult:
    outcome: str  # promoted | blocked | skipped
    blocked_reason_code: str | None
    recommendation_key: str
    policy_id: str | None
    execution_id: str | None
    rollback_candidate_id: str | None
    applied_policy: dict[str, Any] = field(default_factory=dict)
    prior_policy: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class GovernancePolicyAutopromotionService:

    def parse_decision(self, row: dict[str, Any]) -> GovernancePolicyAutopromotionDecision:
        return GovernancePolicyAutopromotionDecision(
            eligible=bool(row.get("eligible")),
            blocked_reason_code=row.get("blocked_reason_code"),
            recommendation_key=str(row["recommendation_key"]),
            policy_id=str(row["policy_id"]) if row.get("policy_id") else None,
            policy_family=str(row["policy_family"]),
            scope_type=str(row["scope_type"]),
            scope_value=str(row["scope_value"]),
            promotion_target=str(row["promotion_target"]) if row.get("promotion_target") else None,
            confidence=row.get("confidence"),
            sample_size=int(row.get("sample_size") or 0),
            approved_review_count=int(row.get("approved_review_count") or 0),
            application_count=int(row.get("application_count") or 0),
            recent_override_rate=float(row.get("recent_override_rate") or 0.0),
            recent_reassignment_rate=float(row.get("recent_reassignment_rate") or 0.0),
            last_execution_at=row.get("last_execution_at"),
            cooldown_ends_at=row.get("cooldown_ends_at"),
        )

    def validate_policy_row(self, row: dict[str, Any]) -> None:
        for field_name in ("workspace_id", "policy_family", "scope_type", "scope_value", "promotion_target", "created_by"):
            if not row.get(field_name):
                raise ValueError(f"{field_name} is required")
        if row["promotion_target"] not in VALID_PROMOTION_TARGETS:
            raise ValueError(f"invalid promotion_target: {row['promotion_target']!r}")
        if row.get("min_confidence", "high") not in ("low", "medium", "high"):
            raise ValueError(f"invalid min_confidence: {row.get('min_confidence')!r}")

    def evaluate_and_autopromote(
        self,
        conn: Any,
        *,
        workspace_id: str,
    ) -> list[GovernancePolicyAutopromotionResult]:
        """
        Main entry point. Reads eligibility view, evaluates each eligible
        recommendation, applies policy additively, records execution + rollback candidate.
        """
        import src.db.repositories as repo

        results: list[GovernancePolicyAutopromotionResult] = []

        try:
            eligibility_rows = repo.list_governance_policy_autopromotion_eligibility(
                conn, workspace_id=workspace_id
            )
        except Exception as exc:
            logger.warning("governance_policy_autopromotion: eligibility query failed: %s", exc)
            return results

        for row in eligibility_rows:
            decision = self.parse_decision(row)
            result = self._process_one(conn, workspace_id=workspace_id, decision=decision, row=row)
            results.append(result)
            if result.outcome == "promoted":
                conn.commit()

        return results

    def _process_one(
        self,
        conn: Any,
        *,
        workspace_id: str,
        decision: GovernancePolicyAutopromotionDecision,
        row: dict[str, Any],
    ) -> GovernancePolicyAutopromotionResult:
        import src.db.repositories as repo

        if not decision.eligible:
            return GovernancePolicyAutopromotionResult(
                outcome="blocked",
                blocked_reason_code=decision.blocked_reason_code,
                recommendation_key=decision.recommendation_key,
                policy_id=decision.policy_id,
                execution_id=None,
                rollback_candidate_id=None,
            )

        # load recommendation for current/recommended policy
        try:
            rec_rows = repo.get_governance_policy_opportunity_summary(
                conn, workspace_id=workspace_id, limit=1
            )
            rec = next((r for r in rec_rows if r["recommendation_key"] == decision.recommendation_key), None)
        except Exception:
            rec = None

        current_policy: dict[str, Any] = dict(rec.get("current_policy") or {}) if rec else {}
        recommended_policy: dict[str, Any] = dict(rec.get("recommended_policy") or {}) if rec else {}

        # Apply to live policy table using promotion service helpers
        try:
            from src.services.governance_policy_promotion_service import (
                GovernancePolicyPromotionService,
                _apply_routing_rule, _apply_routing_override,
                _apply_threshold_profile, _apply_autopromotion_policy,
                _capture_routing_rule_state, _capture_routing_override_state,
                _capture_threshold_profile_state, _capture_autopromotion_policy_state,
            )
            target = decision.promotion_target or ""
            scope_type = decision.scope_type
            scope_value = decision.scope_value
            prior_policy: dict[str, Any] = {}
            fake_proposal: dict[str, Any] = {"id": "", "recommendation_key": decision.recommendation_key}

            if target == "routing_rule":
                prior_policy = _capture_routing_rule_state(conn, workspace_id, scope_type, scope_value)
                _apply_routing_rule(conn, workspace_id, scope_type, scope_value, recommended_policy, "system", fake_proposal)
            elif target == "routing_override":
                prior_policy = _capture_routing_override_state(conn, workspace_id, scope_type, scope_value)
                _apply_routing_override(conn, workspace_id, scope_type, scope_value, recommended_policy, "system", fake_proposal)
            elif target == "threshold_profile":
                prior_policy = _capture_threshold_profile_state(conn, workspace_id, scope_type, scope_value)
                _apply_threshold_profile(conn, workspace_id, scope_type, scope_value, recommended_policy, "system", fake_proposal)
            elif target == "autopromotion_policy":
                prior_policy = _capture_autopromotion_policy_state(conn, workspace_id, scope_type, scope_value)
                _apply_autopromotion_policy(conn, workspace_id, scope_type, scope_value, recommended_policy, "system", fake_proposal)
            else:
                logger.warning("governance_policy_autopromotion: unknown target %s", target)
                return GovernancePolicyAutopromotionResult(
                    outcome="blocked",
                    blocked_reason_code="unknown_promotion_target",
                    recommendation_key=decision.recommendation_key,
                    policy_id=decision.policy_id,
                    execution_id=None,
                    rollback_candidate_id=None,
                )
        except Exception as exc:
            logger.error(
                "governance_policy_autopromotion: apply failed key=%s: %s",
                decision.recommendation_key, exc,
            )
            return GovernancePolicyAutopromotionResult(
                outcome="blocked",
                blocked_reason_code="apply_failed",
                recommendation_key=decision.recommendation_key,
                policy_id=decision.policy_id,
                execution_id=None,
                rollback_candidate_id=None,
                metadata={"error": str(exc)},
            )

        # Persist execution
        try:
            exec_row = repo.insert_governance_policy_autopromotion_execution(
                conn,
                workspace_id=workspace_id,
                recommendation_key=decision.recommendation_key,
                policy_id=str(decision.policy_id or ""),
                policy_family=decision.policy_family,
                promotion_target=decision.promotion_target or "",
                scope_type=decision.scope_type,
                scope_value=decision.scope_value,
                current_policy=current_policy,
                applied_policy=recommended_policy,
                executed_by="system",
                cooldown_applied=False,
                metadata={"source": "governance_policy_autopromotion"},
            )
            execution_id = str(exec_row["id"])
        except Exception as exc:
            logger.error("governance_policy_autopromotion: insert_execution failed: %s", exc)
            return GovernancePolicyAutopromotionResult(
                outcome="blocked",
                blocked_reason_code="execution_insert_failed",
                recommendation_key=decision.recommendation_key,
                policy_id=decision.policy_id,
                execution_id=None,
                rollback_candidate_id=None,
            )

        # Persist rollback candidate
        rollback_candidate_id: str | None = None
        try:
            rc_row = repo.insert_governance_policy_autopromotion_rollback_candidate(
                conn,
                workspace_id=workspace_id,
                execution_id=execution_id,
                recommendation_key=decision.recommendation_key,
                policy_family=decision.policy_family,
                scope_type=decision.scope_type,
                scope_value=decision.scope_value,
                target_type=decision.promotion_target or "",
                prior_policy=prior_policy,
                applied_policy=recommended_policy,
                rollback_reason_code=None,
                rollback_risk_score=0.0,
                metadata={},
            )
            rollback_candidate_id = str(rc_row["id"])
        except Exception as exc:
            logger.warning("governance_policy_autopromotion: rollback_candidate insert failed: %s", exc)

        logger.info(
            "governance_policy_autopromotion: promoted key=%s target=%s execution=%s",
            decision.recommendation_key, decision.promotion_target, execution_id,
        )
        return GovernancePolicyAutopromotionResult(
            outcome="promoted",
            blocked_reason_code=None,
            recommendation_key=decision.recommendation_key,
            policy_id=decision.policy_id,
            execution_id=execution_id,
            rollback_candidate_id=rollback_candidate_id,
            applied_policy=recommended_policy,
            prior_policy=prior_policy,
        )
