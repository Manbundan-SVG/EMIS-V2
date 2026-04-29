from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ManagerAnalyticsRefreshResult:
    workspace_id: str
    snapshot_id: str
    open_case_count: int
    recurring_case_count: int
    escalated_case_count: int
    chronic_watchlist_count: int
    degraded_promotion_count: int
    rollback_risk_count: int


class ManagerAnalyticsService:
    """Build manager-facing rollups from the existing analytics stack."""

    def __init__(self, repo_module: Any, default_window_days: int = 30) -> None:
        self.repo = repo_module
        self.default_window_days = default_window_days

    def refresh_workspace_snapshot(
        self,
        conn,
        *,
        workspace_id: str,
        window_days: int | None = None,
    ) -> ManagerAnalyticsRefreshResult:
        manager_window = int(window_days or self.default_window_days)
        incident_summary = self.repo.get_governance_incident_analytics_summary(conn, workspace_id=workspace_id) or {}
        recurrence_burden = self.repo.list_governance_recurrence_burden_summary(conn, workspace_id=workspace_id)
        root_cause_trends = self.repo.list_governance_root_cause_trend_summary(conn, workspace_id=workspace_id)
        operator_summary = self.repo.list_governance_operator_performance_summary(conn, workspace_id=workspace_id)
        team_summary = self.repo.list_governance_team_performance_summary(conn, workspace_id=workspace_id)
        threshold_impact = self.repo.list_threshold_promotion_impact_summary(conn, workspace_id=workspace_id)
        routing_impact = self.repo.list_routing_promotion_impact_summary(conn, workspace_id=workspace_id)
        rollback_risk = self.repo.list_promotion_rollback_risk_summary(conn, workspace_id=workspace_id)
        stale_cases = self.repo.list_governance_stale_case_summary(conn, workspace_id=workspace_id)
        operator_pressure = self.repo.list_operator_workload_pressure(conn, workspace_id=workspace_id)
        team_pressure = self.repo.list_team_workload_pressure(conn, workspace_id=workspace_id)

        degraded_promotion_count = sum(
            1
            for row in [*threshold_impact, *routing_impact]
            if str(row.get("impact_classification") or "") in {"degraded", "rollback_candidate"}
        )

        metadata = {
            "source": "phase3_4D",
            "window_days": manager_window,
            "top_root_causes": [
                {
                    "root_cause_code": row.get("root_cause_code"),
                    "case_count": row.get("case_count"),
                    "reopened_count": row.get("reopened_count"),
                }
                for row in root_cause_trends[:5]
            ],
            "top_watchlists": [
                {
                    "watchlist_id": row.get("watchlist_id"),
                    "recurring_case_count": row.get("recurring_case_count"),
                    "reopened_case_count": row.get("reopened_case_count"),
                }
                for row in recurrence_burden[:5]
                if int(row.get("recurring_case_count") or 0) > 0
            ],
            "top_operators": [
                {
                    "operator_name": row.get("operator_name"),
                    "resolution_quality_proxy": row.get("resolution_quality_proxy"),
                    "active_open_case_count": row.get("active_open_case_count"),
                }
                for row in operator_summary[:5]
            ],
            "top_teams": [
                {
                    "assigned_team": row.get("assigned_team"),
                    "resolution_quality_proxy": row.get("resolution_quality_proxy"),
                    "active_open_case_count": row.get("active_open_case_count"),
                }
                for row in team_summary[:5]
            ],
            "stale_case_count": len(stale_cases),
            "operator_pressure_count": sum(
                1
                for row in operator_pressure
                if int(row.get("open_case_count") or 0) > 0
                and (
                    int(row.get("recurring_case_count") or 0) > 0
                    or int(row.get("severe_open_case_count") or 0) > 0
                    or int(row.get("ack_breached_case_count") or 0) > 0
                    or int(row.get("resolve_breached_case_count") or 0) > 0
                    or float(row.get("severity_weighted_load") or 0) >= 6
                )
            ),
            "team_pressure_count": sum(
                1
                for row in team_pressure
                if int(row.get("open_case_count") or 0) > 0
                and (
                    int(row.get("recurring_case_count") or 0) > 0
                    or int(row.get("severe_open_case_count") or 0) > 0
                    or int(row.get("ack_breached_case_count") or 0) > 0
                    or int(row.get("resolve_breached_case_count") or 0) > 0
                    or float(row.get("severity_weighted_load") or 0) >= 8
                )
            ),
        }

        snapshot = self.repo.insert_governance_manager_analytics_snapshot(
            conn,
            workspace_id=workspace_id,
            window_days=manager_window,
            open_case_count=int(incident_summary.get("open_case_count") or 0),
            recurring_case_count=int(incident_summary.get("recurring_case_count") or 0),
            escalated_case_count=int(
                (self.repo.get_governance_escalation_effectiveness_summary(conn, workspace_id=workspace_id) or {}).get(
                    "escalated_case_count"
                )
                or 0
            ),
            chronic_watchlist_count=sum(
                1 for row in recurrence_burden if int(row.get("recurring_case_count") or 0) > 0
            ),
            degraded_promotion_count=degraded_promotion_count,
            rollback_risk_count=len(rollback_risk),
            metadata=metadata,
        )

        return ManagerAnalyticsRefreshResult(
            workspace_id=workspace_id,
            snapshot_id=str(snapshot["id"]),
            open_case_count=int(snapshot["open_case_count"]),
            recurring_case_count=int(snapshot["recurring_case_count"]),
            escalated_case_count=int(snapshot["escalated_case_count"]),
            chronic_watchlist_count=int(snapshot["chronic_watchlist_count"]),
            degraded_promotion_count=int(snapshot["degraded_promotion_count"]),
            rollback_risk_count=int(snapshot["rollback_risk_count"]),
        )
