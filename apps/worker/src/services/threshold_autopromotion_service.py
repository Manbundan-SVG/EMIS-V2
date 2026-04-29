from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AutoPromotionEligibility:
    eligible: bool
    reason: str
    previous_value: float | None = None
    new_value: float | None = None
    metadata: dict[str, Any] | None = None


class ThresholdAutoPromotionService:
    """Guardrailed evaluator for semi-automatic threshold promotion."""

    def evaluate(
        self,
        *,
        proposal: dict[str, Any],
        policy: dict[str, Any],
    ) -> AutoPromotionEligibility:
        confidence = float((proposal.get("source_metrics") or {}).get("confidence", 0.0))
        support = int((proposal.get("source_metrics") or {}).get("support_count", 0))
        current_value = float(proposal["current_value"])
        proposed_value = float(proposal["proposed_value"])
        max_step_pct = float(policy.get("max_step_pct", 0.20))
        min_confidence = float(policy.get("min_confidence", 0.85))
        min_support = int(policy.get("min_support", 20))

        delta_pct = abs(proposed_value - current_value) / max(abs(current_value), 1e-9)

        if not bool(policy.get("enabled", False)):
            return AutoPromotionEligibility(False, "policy_disabled")
        if confidence < min_confidence:
            return AutoPromotionEligibility(
                False,
                "confidence_too_low",
                metadata={"confidence": confidence, "min_confidence": min_confidence},
            )
        if support < min_support:
            return AutoPromotionEligibility(
                False,
                "support_too_low",
                metadata={"support_count": support, "min_support": min_support},
            )
        if delta_pct > max_step_pct:
            return AutoPromotionEligibility(
                False,
                "step_too_large",
                metadata={"delta_pct": delta_pct, "max_step_pct": max_step_pct},
            )

        return AutoPromotionEligibility(
            eligible=True,
            reason="eligible",
            previous_value=current_value,
            new_value=proposed_value,
            metadata={
                "confidence": confidence,
                "support_count": support,
                "delta_pct": delta_pct,
            },
        )
