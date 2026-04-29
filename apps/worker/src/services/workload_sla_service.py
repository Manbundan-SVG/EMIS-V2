from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class SlaPolicy:
    id: str | None
    severity: str
    chronicity_class: str | None
    ack_within_minutes: int
    resolve_within_minutes: int


@dataclass(frozen=True)
class SlaEvaluation:
    case_id: str
    policy_id: str | None
    chronicity_class: str | None
    ack_due_at: datetime | None
    resolve_due_at: datetime | None
    ack_breached: bool
    resolve_breached: bool
    breach_severity: str | None
    metadata: dict[str, Any]


class WorkloadSlaService:
    def derive_chronicity_class(self, case_row: dict[str, Any]) -> str | None:
        repeat_count = int(case_row.get("repeat_count") or 1)
        if repeat_count >= 3:
            return "chronic"
        if repeat_count > 1:
            return "recurring"
        return None

    def choose_policy(
        self,
        *,
        severity: str,
        chronicity_class: str | None,
        policies: list[SlaPolicy],
    ) -> SlaPolicy | None:
        exact_match = next(
            (
                policy
                for policy in policies
                if policy.severity == severity and policy.chronicity_class == chronicity_class
            ),
            None,
        )
        if exact_match:
            return exact_match
        return next(
            (
                policy
                for policy in policies
                if policy.severity == severity and policy.chronicity_class is None
            ),
            None,
        )

    def evaluate_case(
        self,
        *,
        case_row: dict[str, Any],
        policy: SlaPolicy | None,
        now: datetime | None = None,
    ) -> SlaEvaluation:
        now = now or datetime.now(timezone.utc)
        opened_at = case_row.get("opened_at")
        if not opened_at:
            raise RuntimeError("case_row missing opened_at for SLA evaluation")

        chronicity_class = self.derive_chronicity_class(case_row)
        acknowledged_at = case_row.get("acknowledged_at")
        resolved_marker = case_row.get("closed_at") or case_row.get("resolved_at")

        if policy is None:
            return SlaEvaluation(
                case_id=str(case_row["id"]),
                policy_id=None,
                chronicity_class=chronicity_class,
                ack_due_at=None,
                resolve_due_at=None,
                ack_breached=False,
                resolve_breached=False,
                breach_severity=None,
                metadata={"reason": "no_matching_policy"},
            )

        ack_due_at = opened_at + timedelta(minutes=policy.ack_within_minutes)
        resolve_due_at = opened_at + timedelta(minutes=policy.resolve_within_minutes)
        ack_breached = (
            acknowledged_at is None and now > ack_due_at
        ) or (
            acknowledged_at is not None and acknowledged_at > ack_due_at
        )
        resolve_breached = (
            resolved_marker is None and now > resolve_due_at
        ) or (
            resolved_marker is not None and resolved_marker > resolve_due_at
        )

        return SlaEvaluation(
            case_id=str(case_row["id"]),
            policy_id=policy.id,
            chronicity_class=chronicity_class,
            ack_due_at=ack_due_at,
            resolve_due_at=resolve_due_at,
            ack_breached=ack_breached,
            resolve_breached=resolve_breached,
            breach_severity=str(case_row.get("severity")) if (ack_breached or resolve_breached) else None,
            metadata={
                "opened_at": opened_at.isoformat() if hasattr(opened_at, "isoformat") else str(opened_at),
                "acknowledged_at": acknowledged_at.isoformat() if hasattr(acknowledged_at, "isoformat") else (str(acknowledged_at) if acknowledged_at else None),
                "resolved_at": resolved_marker.isoformat() if hasattr(resolved_marker, "isoformat") else (str(resolved_marker) if resolved_marker else None),
            },
        )
