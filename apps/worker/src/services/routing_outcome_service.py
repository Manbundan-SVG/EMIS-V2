from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class RoutingOutcomeEvent:
    workspace_id: str
    case_id: str
    outcome_type: str
    occurred_at: datetime
    routing_decision_id: str | None = None
    assignment_id: str | None = None
    assigned_to: str | None = None
    assigned_team: str | None = None
    root_cause_code: str | None = None
    severity: str | None = None
    watchlist_id: str | None = None
    compute_version: str | None = None
    signal_registry_version: str | None = None
    model_version: str | None = None
    recurrence_group_id: str | None = None
    repeat_count: int = 1
    outcome_value: float | None = None
    outcome_context: dict[str, Any] | None = None


class RoutingOutcomeService:
    def __init__(self, repositories: Any) -> None:
        self.repositories = repositories

    def _to_datetime(self, value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None

    def _hours_between(self, start: Any, end: Any) -> float | None:
        start_dt = self._to_datetime(start)
        end_dt = self._to_datetime(end)
        if start_dt is None or end_dt is None:
            return None
        return max((end_dt - start_dt).total_seconds() / 3600.0, 0.0)

    def _version_fields(self, case_snapshot: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
        compute_version = case_snapshot.get("compute_version")
        signal_registry_version = case_snapshot.get("signal_registry_version")
        model_version = case_snapshot.get("model_version")
        if compute_version or signal_registry_version or model_version:
            return (
                str(compute_version) if compute_version else None,
                str(signal_registry_version) if signal_registry_version else None,
                str(model_version) if model_version else None,
            )

        version_tuple = case_snapshot.get("version_tuple")
        if not version_tuple:
            return None, None, None

        parts = str(version_tuple).split("|")
        return (
            parts[0] if len(parts) > 0 else None,
            parts[1] if len(parts) > 1 else None,
            parts[2] if len(parts) > 2 else None,
        )

    def _build_event(
        self,
        *,
        case_snapshot: dict[str, Any],
        outcome_type: str,
        occurred_at: Any,
        outcome_value: float | None = None,
        outcome_context: dict[str, Any] | None = None,
    ) -> RoutingOutcomeEvent:
        compute_version, signal_registry_version, model_version = self._version_fields(case_snapshot)
        return RoutingOutcomeEvent(
            workspace_id=str(case_snapshot["workspace_id"]),
            case_id=str(case_snapshot["id"]),
            routing_decision_id=(
                str(case_snapshot["routing_decision_id"])
                if case_snapshot.get("routing_decision_id")
                else None
            ),
            assignment_id=(
                str(case_snapshot["assignment_id"])
                if case_snapshot.get("assignment_id")
                else None
            ),
            assigned_to=str(case_snapshot["assigned_to"]) if case_snapshot.get("assigned_to") else None,
            assigned_team=str(case_snapshot["assigned_team"]) if case_snapshot.get("assigned_team") else None,
            root_cause_code=(
                str(case_snapshot["root_cause_code"])
                if case_snapshot.get("root_cause_code")
                else None
            ),
            severity=str(case_snapshot["severity"]) if case_snapshot.get("severity") else None,
            watchlist_id=str(case_snapshot["watchlist_id"]) if case_snapshot.get("watchlist_id") else None,
            compute_version=compute_version,
            signal_registry_version=signal_registry_version,
            model_version=model_version,
            recurrence_group_id=(
                str(case_snapshot["recurrence_group_id"])
                if case_snapshot.get("recurrence_group_id")
                else None
            ),
            repeat_count=int(case_snapshot.get("repeat_count") or 1),
            outcome_type=outcome_type,
            outcome_value=outcome_value,
            occurred_at=self._to_datetime(occurred_at) or datetime.now(timezone.utc),
            outcome_context=outcome_context or {},
        )

    def _insert(self, conn, event: RoutingOutcomeEvent) -> dict[str, Any]:
        return self.repositories.insert_governance_routing_outcome(conn, event=event)

    def record_assignment(self, conn, case_snapshot: dict[str, Any]) -> dict[str, Any]:
        return self._insert(
            conn,
            self._build_event(
                case_snapshot=case_snapshot,
                outcome_type="assigned",
                occurred_at=datetime.now(timezone.utc),
                outcome_context={"source": "routing_decision"},
            )
        )

    def record_acknowledgment(self, conn, case_snapshot: dict[str, Any], acknowledged_at: Any) -> list[dict[str, Any]]:
        ack_hours = self._hours_between(
            case_snapshot.get("opened_at") or case_snapshot.get("created_at"),
            acknowledged_at,
        )
        rows = [
            self._insert(
                conn,
                self._build_event(
                    case_snapshot=case_snapshot,
                    outcome_type="acknowledged",
                    occurred_at=acknowledged_at,
                    outcome_value=ack_hours,
                    outcome_context={"metric": "time_to_ack_hours"},
                )
            )
        ]
        if ack_hours is not None:
            rows.append(
                self._insert(
                    conn,
                    self._build_event(
                        case_snapshot=case_snapshot,
                        outcome_type="time_to_ack_hours",
                        occurred_at=acknowledged_at,
                        outcome_value=ack_hours,
                        outcome_context={"metric": "time_to_ack_hours"},
                    )
                )
            )
        return rows

    def record_resolution(self, conn, case_snapshot: dict[str, Any], resolved_at: Any) -> list[dict[str, Any]]:
        resolve_hours = self._hours_between(
            case_snapshot.get("opened_at") or case_snapshot.get("created_at"),
            resolved_at,
        )
        rows = [
            self._insert(
                conn,
                self._build_event(
                    case_snapshot=case_snapshot,
                    outcome_type="resolved",
                    occurred_at=resolved_at,
                    outcome_value=resolve_hours,
                    outcome_context={"metric": "time_to_resolve_hours"},
                )
            )
        ]
        if resolve_hours is not None:
            rows.append(
                self._insert(
                    conn,
                    self._build_event(
                        case_snapshot=case_snapshot,
                        outcome_type="time_to_resolve_hours",
                        occurred_at=resolved_at,
                        outcome_value=resolve_hours,
                        outcome_context={"metric": "time_to_resolve_hours"},
                    )
                )
            )
        return rows

    def record_reassignment(self, conn, case_snapshot: dict[str, Any], reason: str | None) -> dict[str, Any]:
        return self._insert(
            conn,
            self._build_event(
                case_snapshot=case_snapshot,
                outcome_type="reassigned",
                occurred_at=datetime.now(timezone.utc),
                outcome_context={"reason": reason or "reassignment"},
            )
        )

    def record_escalation(
        self,
        conn,
        case_snapshot: dict[str, Any],
        *,
        level: str | None,
        reason: str | None,
    ) -> dict[str, Any]:
        return self._insert(
            conn,
            self._build_event(
                case_snapshot=case_snapshot,
                outcome_type="escalated",
                occurred_at=datetime.now(timezone.utc),
                outcome_context={
                    "escalation_level": level,
                    "reason": reason or "escalation",
                },
            )
        )

    def record_reopen(self, conn, case_snapshot: dict[str, Any], reason: str | None) -> dict[str, Any]:
        return self._insert(
            conn,
            self._build_event(
                case_snapshot=case_snapshot,
                outcome_type="reopened",
                occurred_at=datetime.now(timezone.utc),
                outcome_context={"reason": reason or "reopened"},
            )
        )
