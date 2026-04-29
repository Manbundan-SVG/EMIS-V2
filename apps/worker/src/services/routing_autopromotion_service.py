from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RoutingAutopromotionDecision:
    should_execute: bool
    execution_reason: str
    target_type: str
    target_key: str
    prior_state: dict[str, Any]
    new_state: dict[str, Any]
    rollback_reason: str


class RoutingAutopromotionService:
    """Guardrailed evaluator for routing recommendation autopromotion."""

    CONFIDENCE_ORDER = {"low": 1, "medium": 2, "high": 3}

    def evaluate(
        self,
        recommendation: dict[str, Any],
        policy: dict[str, Any],
        cooldown_ok: bool,
    ) -> RoutingAutopromotionDecision:
        disabled = not policy or not bool(policy.get("enabled", True))
        if disabled:
            return RoutingAutopromotionDecision(
                False,
                "policy_disabled",
                str((policy or {}).get("promotion_target") or "override"),
                "",
                {},
                {},
                "policy_disabled",
            )
        if not cooldown_ok:
            return RoutingAutopromotionDecision(
                False,
                "cooldown_active",
                str(policy.get("promotion_target") or "override"),
                "",
                {},
                {},
                "cooldown_active",
            )
        if recommendation.get("review_status") != "approved":
            return RoutingAutopromotionDecision(
                False,
                "recommendation_not_approved",
                str(policy.get("promotion_target") or "override"),
                "",
                {},
                {},
                "recommendation_not_approved",
            )

        recommendation_confidence = str(recommendation.get("confidence", "low")).lower()
        policy_confidence = str(policy.get("min_confidence", "high")).lower()
        confidence_ok = self.CONFIDENCE_ORDER.get(recommendation_confidence, 0) >= self.CONFIDENCE_ORDER.get(
            policy_confidence,
            0,
        )
        acceptance_rate = float(recommendation.get("acceptance_rate", 0.0) or 0.0)
        sample_size = int(recommendation.get("sample_size", 0) or 0)
        override_rate_raw = recommendation.get("override_rate")
        override_rate = float(override_rate_raw) if override_rate_raw is not None else 1.0

        if not confidence_ok:
            return RoutingAutopromotionDecision(
                False,
                "confidence_below_policy",
                str(policy.get("promotion_target") or "override"),
                "",
                {},
                {},
                "confidence_below_policy",
            )
        if acceptance_rate < float(policy.get("min_acceptance_rate", 0.80)):
            return RoutingAutopromotionDecision(
                False,
                "acceptance_rate_below_policy",
                str(policy.get("promotion_target") or "override"),
                "",
                {},
                {},
                "acceptance_rate_below_policy",
            )
        if sample_size < int(policy.get("min_sample_size", 5)):
            return RoutingAutopromotionDecision(
                False,
                "sample_size_below_policy",
                str(policy.get("promotion_target") or "override"),
                "",
                {},
                {},
                "sample_size_below_policy",
            )
        if override_rate > float(policy.get("max_recent_override_rate", 0.20)):
            return RoutingAutopromotionDecision(
                False,
                "override_rate_above_policy",
                str(policy.get("promotion_target") or "override"),
                "",
                {},
                {},
                "override_rate_above_policy",
            )

        target_type = str(policy.get("promotion_target", "override"))
        target_key = self._build_target_key(recommendation, policy)
        prior_state = {
            "assigned_user": recommendation.get("current_rule_user"),
            "assigned_team": recommendation.get("current_rule_team"),
        }
        new_state = {
            "assigned_user": recommendation.get("recommended_user"),
            "assigned_team": recommendation.get("recommended_team"),
        }
        return RoutingAutopromotionDecision(
            True,
            "policy_conditions_met",
            target_type,
            target_key,
            prior_state,
            new_state,
            "autopromotion_regression_or_override_spike",
        )

    def _build_target_key(self, recommendation: dict[str, Any], policy: dict[str, Any]) -> str:
        model_inputs = recommendation.get("model_inputs") or {}
        scope_type = str(policy.get("scope_type", "global"))
        fallback_map = {
            "team": recommendation.get("recommended_team"),
            "watchlist": model_inputs.get("watchlist_id"),
            "root_cause": model_inputs.get("root_cause_code"),
            "version_tuple": model_inputs.get("version_tuple"),
            "regime": model_inputs.get("regime"),
        }
        scope_value = policy.get("scope_value") or fallback_map.get(scope_type) or "default"
        return f"{scope_type}:{scope_value}"
