from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RoutingRecommendationApplication:
    workspace_id: str
    recommendation_id: str
    case_id: str
    previous_assigned_user: str | None
    previous_assigned_team: str | None
    applied_user: str | None
    applied_team: str | None
    applied_by: str | None
    application_reason: str | None
    application_mode: str = "manual_reviewed"
    review_status: str | None = None
    review_id: str | None = None


class RoutingRecommendationApplicationService:
    def validate(self, app: RoutingRecommendationApplication) -> None:
        if not app.workspace_id or not app.recommendation_id or not app.case_id:
            raise ValueError("workspace_id, recommendation_id, and case_id are required")
        if app.review_status != "approved":
            raise ValueError("recommendation must be approved before application")
        if not app.applied_user and not app.applied_team:
            raise ValueError("application must target a user or team")

    def build_application_row(self, app: RoutingRecommendationApplication) -> dict[str, object]:
        self.validate(app)
        return {
            "workspace_id": app.workspace_id,
            "recommendation_id": app.recommendation_id,
            "review_id": app.review_id,
            "case_id": app.case_id,
            "previous_assigned_user": app.previous_assigned_user,
            "previous_assigned_team": app.previous_assigned_team,
            "applied_user": app.applied_user,
            "applied_team": app.applied_team,
            "applied_by": app.applied_by,
            "application_reason": app.application_reason,
            "application_mode": app.application_mode,
        }
