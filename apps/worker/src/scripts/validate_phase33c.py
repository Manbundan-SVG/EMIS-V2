from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db.client import get_connection
from src.db import repositories as repo
from src.services.case_management_service import build_case_seed
from src.services.routing_outcome_service import RoutingOutcomeService
from src.services.routing_recommendation_service import RoutingRecommendationService
from src.services.routing_recommendation_review_service import (
    RoutingRecommendationReview,
    RoutingRecommendationReviewService,
)
from src.services.routing_recommendation_application_service import (
    RoutingRecommendationApplication,
    RoutingRecommendationApplicationService,
)


def _create_workspace_and_watchlist(workspace_slug: str, watchlist_slug: str) -> tuple[str, str]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.workspaces (slug, name)
                values (%s, %s)
                returning id
                """,
                (workspace_slug, workspace_slug),
            )
            workspace_id = str(cur.fetchone()["id"])
            cur.execute(
                """
                insert into public.watchlists (workspace_id, slug, name)
                values (%s::uuid, %s, %s)
                returning id
                """,
                (workspace_id, watchlist_slug, watchlist_slug),
            )
            watchlist_id = str(cur.fetchone()["id"])
        conn.commit()
    return workspace_id, watchlist_id


def _cleanup_workspace(workspace_id: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("delete from public.workspaces where id = %s::uuid", (workspace_id,))
        conn.commit()


def _seed_case(
    conn,
    *,
    workspace_id: str,
    watchlist_id: str,
    degradation_type: str,
    severity: str,
    version_tuple: str,
    title_suffix: str,
    root_cause_code: str,
    opened_at: datetime,
) -> dict:
    case_row = repo.upsert_governance_case(
        conn,
        build_case_seed(
            workspace_id=workspace_id,
            degradation_state_id=None,
            watchlist_id=watchlist_id,
            version_tuple=version_tuple,
            degradation_type=degradation_type,
            severity=severity,
            source_summary={"message": f"phase33c {title_suffix}"},
        ).__dict__,
    )
    repo.upsert_governance_case_summary(
        conn,
        workspace_id=workspace_id,
        case_id=str(case_row["id"]),
        summary_version="v1",
        status_summary=f"{title_suffix} status",
        root_cause_code=root_cause_code,
        root_cause_confidence=0.95,
        root_cause_summary=f"{root_cause_code} detected",
        evidence_summary=None,
        recurrence_summary=None,
        operator_summary=None,
        closure_summary=None,
        recommended_next_action="review routing recommendation governance",
        source_note_ids=[],
        source_evidence_ids=[],
        metadata={"validator": "phase33c"},
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_cases
            set opened_at = %s::timestamptz,
                updated_at = now()
            where id = %s::uuid
            returning *
            """,
            (opened_at, str(case_row["id"])),
        )
        return dict(cur.fetchone())


def _build_snapshot(
    *,
    case_row: dict,
    routing_decision_id: str | None,
    assignment_id: str | None,
    assigned_to: str | None,
    assigned_team: str | None,
    root_cause_code: str,
) -> dict:
    snapshot = dict(case_row)
    snapshot["routing_decision_id"] = routing_decision_id
    snapshot["assignment_id"] = assignment_id
    snapshot["assigned_to"] = assigned_to
    snapshot["assigned_team"] = assigned_team
    snapshot["root_cause_code"] = root_cause_code
    return snapshot


def _seed_recommendation(conn, *, workspace_id: str, watchlist_id: str, now: datetime) -> dict:
    outcome_service = RoutingOutcomeService(repo)

    history_case = _seed_case(
        conn,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        degradation_type="version_regression",
        severity="high",
        version_tuple="compute-g1|signals-g1|model-g1",
        title_suffix="history",
        root_cause_code="version_regression",
        opened_at=now - timedelta(hours=8),
    )
    history_decision = repo.create_governance_routing_decision(
        conn,
        workspace_id=workspace_id,
        case_id=str(history_case["id"]),
        routing_rule_id=None,
        override_id=None,
        assigned_team="research",
        assigned_user="alice",
        routing_reason="validator_phase33c_history",
        workload_snapshot={"validator": "phase33c"},
        metadata={"validator": "phase33c"},
    )
    history_assignment = repo.create_governance_assignment(
        conn,
        case_id=str(history_case["id"]),
        workspace_id=workspace_id,
        assigned_to="alice",
        assigned_team="research",
        assigned_by="worker",
        reason="validator_phase33c_history",
        metadata={"validator": "phase33c"},
    )
    history_snapshot = _build_snapshot(
        case_row=history_case,
        routing_decision_id=str(history_decision["id"]),
        assignment_id=str(history_assignment["id"]),
        assigned_to="alice",
        assigned_team="research",
        root_cause_code="version_regression",
    )
    outcome_service.record_assignment(conn, history_snapshot)
    outcome_service.record_acknowledgment(conn, history_snapshot, now - timedelta(hours=6))
    outcome_service.record_resolution(conn, history_snapshot, now - timedelta(hours=2))

    target_case = _seed_case(
        conn,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        degradation_type="version_regression",
        severity="high",
        version_tuple="compute-g1|signals-g1|model-g1",
        title_suffix="target",
        root_cause_code="version_regression",
        opened_at=now - timedelta(hours=1),
    )
    case_row = repo.get_case_for_routing_recommendation(
        conn,
        workspace_id=workspace_id,
        case_id=str(target_case["id"]),
    )
    assert case_row is not None
    candidates = repo.list_routing_recommendation_candidates(
        conn,
        workspace_id=workspace_id,
        case_row=case_row,
    )
    recommendation = RoutingRecommendationService().recommend(case_row=case_row, candidate_rows=candidates)
    recommendation_row = repo.upsert_governance_routing_recommendation(
        conn,
        workspace_id=workspace_id,
        case_id=str(target_case["id"]),
        recommendation={
            "recommendation_key": recommendation.recommendation_key,
            "recommended_user": recommendation.recommended_user,
            "recommended_team": recommendation.recommended_team,
            "fallback_user": recommendation.fallback_user,
            "fallback_team": recommendation.fallback_team,
            "reason_code": recommendation.reason_code,
            "confidence": recommendation.confidence,
            "score": recommendation.score,
            "supporting_metrics": recommendation.supporting_metrics,
            "model_inputs": recommendation.model_inputs,
            "alternatives": recommendation.alternatives,
        },
    )
    return {
        "case": target_case,
        "recommendation": recommendation_row,
    }


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase33c-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "routing-review-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            seeded = _seed_recommendation(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                now=now,
            )
            case_row = seeded["case"]
            recommendation_row = seeded["recommendation"]

            review_service = RoutingRecommendationReviewService()
            review_row = repo.insert_governance_routing_recommendation_review(
                conn,
                metadata={"validator": "phase33c"},
                **review_service.build_review_row(
                    RoutingRecommendationReview(
                        workspace_id=workspace_id,
                        recommendation_id=str(recommendation_row["id"]),
                        case_id=str(case_row["id"]),
                        review_status="approved",
                        review_reason="validator_approved",
                        notes="reviewed in validator",
                        reviewed_by="validator",
                        apply_immediately=False,
                    )
                ),
            )

            app_service = RoutingRecommendationApplicationService()
            assignment_row = repo.create_governance_assignment(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                assigned_to=str(recommendation_row["recommended_user"]) if recommendation_row.get("recommended_user") else None,
                assigned_team=str(recommendation_row["recommended_team"]) if recommendation_row.get("recommended_team") else None,
                assigned_by="validator",
                reason="approved_routing_recommendation",
                metadata={"validator": "phase33c"},
            )
            application_row = repo.insert_governance_routing_application(
                conn,
                metadata={"validator": "phase33c"},
                **app_service.build_application_row(
                    RoutingRecommendationApplication(
                        workspace_id=workspace_id,
                        recommendation_id=str(recommendation_row["id"]),
                        review_id=str(review_row["id"]),
                        case_id=str(case_row["id"]),
                        previous_assigned_user=None,
                        previous_assigned_team=None,
                        applied_user=str(recommendation_row["recommended_user"]) if recommendation_row.get("recommended_user") else None,
                        applied_team=str(recommendation_row["recommended_team"]) if recommendation_row.get("recommended_team") else None,
                        applied_by="validator",
                        application_reason="approved_routing_recommendation",
                        application_mode="manual_reviewed",
                        review_status="approved",
                    )
                ),
            )
            recommendation_feedback = repo.record_governance_routing_recommendation_feedback(
                conn,
                recommendation_id=str(recommendation_row["id"]),
                accepted=True,
                accepted_by="validator",
                override_reason=None,
                applied=True,
            )
            review_summary = repo.get_governance_routing_review_summary(
                conn,
                workspace_id=workspace_id,
                recommendation_id=str(recommendation_row["id"]),
            )
            application_summary = repo.get_governance_routing_application_summary(
                conn,
                workspace_id=workspace_id,
                recommendation_id=str(recommendation_row["id"]),
                case_id=str(case_row["id"]),
            )
            conn.commit()

        detail_contract_ok = bool(
            review_summary
            and review_summary.get("latest_review_status") == "approved"
            and application_summary
            and int(application_summary.get("application_count") or 0) == 1
        )
        print(f"review_persisted={str(bool(review_row.get('id'))).lower()}")
        print(f"application_persisted={str(bool(application_row.get('id'))).lower()}")
        print(f"latest_review_status={review_summary.get('latest_review_status') if review_summary else 'missing'}")
        print(f"application_count={application_summary.get('application_count') if application_summary else 0}")
        print(f"applied_user={assignment_row.get('assigned_to')}")
        print(f"recommendation_applied={str(bool(recommendation_feedback.get('applied'))).lower()}")
        print(f"detail_contract_ok={str(detail_contract_ok).lower()}")
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
