from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class ResolutionAction:
    workspace_id: str
    degradation_state_id: str
    action_type: str
    performed_by: str
    note: str | None
    metadata: dict[str, Any]
    created_at: datetime


class GovernanceRecoveryService:
    def build_auto_resolution_action(
        self,
        *,
        workspace_id: str,
        degradation_state_id: str,
        recovery_reason: str,
        trailing_metrics: dict[str, Any],
    ) -> ResolutionAction:
        return ResolutionAction(
            workspace_id=workspace_id,
            degradation_state_id=degradation_state_id,
            action_type="auto_recovered",
            performed_by="worker",
            note=recovery_reason.replace("_", " "),
            metadata={
                "recovery_reason": recovery_reason,
                "trailing_metrics": trailing_metrics,
            },
            created_at=datetime.now(timezone.utc),
        )
