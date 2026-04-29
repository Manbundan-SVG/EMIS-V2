from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class IncidentAnalyticsRefreshResult:
    workspace_id: str
    snapshot_date: str
    open_case_count: int
    resolved_case_count: int
    recurring_case_count: int
    escalated_case_count: int


class IncidentAnalyticsService:
    """Refresh durable incident analytics snapshots from existing governance facts."""

    def __init__(self, repo_module: Any) -> None:
        self.repo = repo_module

    def refresh_workspace_snapshot(
        self,
        conn,
        *,
        workspace_id: str,
        snapshot_date: date | None = None,
    ) -> IncidentAnalyticsRefreshResult:
        as_of = snapshot_date or date.today()
        summary = self.repo.get_governance_incident_analytics_summary(conn, workspace_id=workspace_id) or {}
        escalation = self.repo.get_governance_escalation_effectiveness_summary(conn, workspace_id=workspace_id) or {}
        stale_rows = self.repo.list_governance_stale_case_summary(conn, workspace_id=workspace_id)

        payload = {
            "workspace_id": workspace_id,
            "snapshot_date": as_of.isoformat(),
            "open_case_count": int(summary.get("open_case_count", 0) or 0),
            "acknowledged_case_count": int(summary.get("acknowledged_case_count", 0) or 0),
            "resolved_case_count": int(summary.get("resolved_case_count", 0) or 0),
            "reopened_case_count": int(summary.get("reopened_case_count", 0) or 0),
            "recurring_case_count": int(summary.get("recurring_case_count", 0) or 0),
            "escalated_case_count": int(escalation.get("escalated_case_count", 0) or 0),
            "high_severity_open_count": int(summary.get("high_severity_open_count", 0) or 0),
            "stale_case_count": len(stale_rows),
            "mean_ack_hours": summary.get("mean_ack_hours"),
            "mean_resolve_hours": summary.get("mean_resolve_hours"),
        }
        self.repo.upsert_governance_incident_analytics_snapshot(conn, payload=payload)

        return IncidentAnalyticsRefreshResult(
            workspace_id=workspace_id,
            snapshot_date=payload["snapshot_date"],
            open_case_count=payload["open_case_count"],
            resolved_case_count=payload["resolved_case_count"],
            recurring_case_count=payload["recurring_case_count"],
            escalated_case_count=payload["escalated_case_count"],
        )

