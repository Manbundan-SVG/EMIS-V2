from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class IncidentPerformanceRefreshResult:
    workspace_id: str
    operator_count: int
    team_count: int
    operator_case_mix_count: int
    team_case_mix_count: int
    snapshot_id: str


class IncidentPerformanceService:
    """Refresh durable operator/team performance snapshots from existing incident facts."""

    def __init__(self, repo_module: Any) -> None:
        self.repo = repo_module

    def refresh_workspace_snapshot(
        self,
        conn,
        *,
        workspace_id: str,
    ) -> IncidentPerformanceRefreshResult:
        operator_count = self.repo.count_governance_operator_performance_rows(conn, workspace_id=workspace_id)
        team_count = self.repo.count_governance_team_performance_rows(conn, workspace_id=workspace_id)
        operator_case_mix_count = self.repo.count_governance_operator_case_mix_rows(conn, workspace_id=workspace_id)
        team_case_mix_count = self.repo.count_governance_team_case_mix_rows(conn, workspace_id=workspace_id)
        snapshot = self.repo.insert_governance_performance_snapshot(
            conn,
            workspace_id=workspace_id,
            operator_count=operator_count,
            team_count=team_count,
            operator_case_mix_count=operator_case_mix_count,
            team_case_mix_count=team_case_mix_count,
            metadata={
                "source": "phase3_4B",
                "refresh_mode": "worker",
            },
        )
        return IncidentPerformanceRefreshResult(
            workspace_id=workspace_id,
            operator_count=operator_count,
            team_count=team_count,
            operator_case_mix_count=operator_case_mix_count,
            team_case_mix_count=team_case_mix_count,
            snapshot_id=str(snapshot["id"]),
        )
