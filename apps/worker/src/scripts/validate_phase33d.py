from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db import repositories as repo
from src.db.client import get_connection
from src.services.case_management_service import build_case_seed
from src.services.routing_autopromotion_service import RoutingAutopromotionService
from src.services.routing_outcome_service import RoutingOutcomeService
from src.services.routing_recommendation_review_service import (
    RoutingRecommendationReview,
    RoutingRecommendationReviewService,
)
from src.services.routing_recommendation_service import RoutingRecommendationService


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
    opened_at: datetime,
    title_suffix: str,
) -> dict:
    case_row = repo.upsert_governance_case(
        conn,
        build_case_seed(
            workspace_id=workspace_id,
            degradation_state_id=None,
            watchlist_id=watchlist_id,
            version_tuple="compute-g1|signals-g1|model-g1",
            degradation_type="version_regression",
            severity="high",
            source_summary={"message": f"phase33d {title_suffix}"},
        ).__dict__,
    )
    repo.upsert_governance_case_summary(
        conn,
        workspace_id=workspace_id,
        case_id=str(case_row["id"]),
        summary_version="v1",
        status_summary=f"{title_suffix} status",
        root_cause_code="version_regression",
        root_cause_confidence=0.95,
        root_cause_summary="version_regression detected",
        evidence_summary=None,
        recurrence_summary=None,
        operator_summary=None,
        closure_summary=None,
        recommended_next_action="review routing autopromotion",
        source_note_ids=[],
        source_evidence_ids=[],
        metadata={"validator": "phase33d"},
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
) -> dict:
    snapshot = dict(case_row)
    snapshot["routing_decision_id"] = routing_decision_id
    snapshot["assignment_id"] = assignment_id
    snapshot["assigned_to"] = assigned_to
    snapshot["assigned_team"] = assigned_team
    snapshot["root_cause_code"] = "version_regression"
    return snapshot


def _seed_recommendation(conn, *, workspace_id: str, watchlist_id: str, now: datetime) -> dict[str, dict]:
    outcome_service = RoutingOutcomeService(repo)

    for offset_hours in (12, 9, 6):
        history_case = _seed_case(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            opened_at=now - timedelta(hours=offset_hours),
            title_suffix=f"history-{offset_hours}",
        )
        decision_row = repo.create_governance_routing_decision(
            conn,
            workspace_id=workspace_id,
            case_id=str(history_case["id"]),
            routing_rule_id=None,
            override_id=None,
            assigned_team="research",
            assigned_user="alice",
            routing_reason="validator_phase33d_history",
            workload_snapshot={"validator": "phase33d"},
            metadata={"validator": "phase33d"},
        )
        assignment_row = repo.create_governance_assignment(
            conn,
            case_id=str(history_case["id"]),
            workspace_id=workspace_id,
            assigned_to="alice",
            assigned_team="research",
            assigned_by="worker",
            reason="validator_phase33d_history",
            metadata={"validator": "phase33d"},
        )
        snapshot = _build_snapshot(
            case_row=history_case,
            routing_decision_id=str(decision_row["id"]),
            assignment_id=str(assignment_row["id"]),
            assigned_to="alice",
            assigned_team="research",
        )
        outcome_service.record_assignment(conn, snapshot)
        outcome_service.record_acknowledgment(conn, snapshot, now - timedelta(hours=offset_hours - 1))
        outcome_service.record_resolution(conn, snapshot, now - timedelta(hours=offset_hours - 0.5))
        repo.create_governance_routing_feedback(
            conn,
            workspace_id=workspace_id,
            case_id=str(history_case["id"]),
            routing_decision_id=str(decision_row["id"]),
            feedback_type="accepted",
            assigned_to="alice",
            assigned_team="research",
            prior_assigned_to=None,
            prior_assigned_team=None,
            root_cause_code="version_regression",
            severity="high",
            recurrence_group_id=None,
            repeat_count=1,
            reason="validator_phase33d_history",
            metadata={"validator": "phase33d"},
        )

    target_case = _seed_case(
        conn,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        opened_at=now - timedelta(hours=1),
        title_suffix="target",
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
    review_service = RoutingRecommendationReviewService()
    review_row = repo.insert_governance_routing_recommendation_review(
        conn,
        metadata={"validator": "phase33d"},
        **review_service.build_review_row(
            RoutingRecommendationReview(
                workspace_id=workspace_id,
                recommendation_id=str(recommendation_row["id"]),
                case_id=str(target_case["id"]),
                review_status="approved",
                review_reason="validator_approved",
                notes="reviewed in validator",
                reviewed_by="validator",
                apply_immediately=False,
            )
        ),
    )
    return {
        "case": target_case,
        "recommendation": recommendation_row,
        "review": review_row,
    }


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase33d-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "routing-autopromotion-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            seeded = _seed_recommendation(conn, workspace_id=workspace_id, watchlist_id=watchlist_id, now=now)
            recommendation_row = seeded["recommendation"]
            candidate = repo.get_governance_routing_autopromotion_candidate(
                conn,
                recommendation_id=str(recommendation_row["id"]),
            )
            assert candidate is not None
            policy_acceptance_rate = max(0.0, float(candidate.get("acceptance_rate") or 0.0) - 0.05)
            policy_override_rate = min(1.0, float(candidate.get("override_rate") or 0.0) + 0.05)
            policy_sample_size = max(1, int(candidate.get("sample_size") or 1))

            policy = repo.upsert_governance_routing_autopromotion_policy(
                conn,
                workspace_id=workspace_id,
                scope_type="root_cause",
                scope_value="version_regression",
                enabled=True,
                promotion_target="override",
                min_confidence=str(candidate["confidence"]),
                min_acceptance_rate=policy_acceptance_rate,
                min_sample_size=policy_sample_size,
                max_recent_override_rate=policy_override_rate,
                cooldown_hours=24,
                created_by="validator",
                metadata={"validator": "phase33d"},
            )
            selected_policy = repo.get_governance_routing_autopromotion_policy(
                conn,
                workspace_id=workspace_id,
                recommendation=candidate,
            )
            assert selected_policy is not None

            service = RoutingAutopromotionService()
            preview = service.evaluate(candidate, selected_policy, True)
            assert preview.should_execute

            apply_result = repo.apply_governance_routing_autopromotion(
                conn,
                workspace_id=workspace_id,
                policy=selected_policy,
                recommendation=candidate,
                target_type=preview.target_type,
                target_key=preview.target_key,
                executed_by="validator_autopromotion",
            )
            execution_row = repo.create_governance_routing_autopromotion_execution(
                conn,
                workspace_id=workspace_id,
                policy_id=str(selected_policy["id"]),
                recommendation_id=str(candidate["id"]),
                target_type=preview.target_type,
                target_key=preview.target_key,
                recommended_user=candidate.get("recommended_user"),
                recommended_team=candidate.get("recommended_team"),
                confidence=str(candidate.get("confidence") or "low"),
                acceptance_rate=float(candidate.get("acceptance_rate") or 0.0),
                sample_size=int(candidate.get("sample_size") or 0),
                override_rate=float(candidate.get("override_rate") or 0.0),
                execution_status="executed",
                execution_reason=preview.execution_reason,
                cooldown_bucket=f"{preview.target_key}:{now.strftime('%Y%m%d%H')}",
                prior_state=apply_result["prior_state"],
                new_state=apply_result["new_state"],
                metadata={"validator": "phase33d", "target_row_id": apply_result["target_row"].get("id")},
            )
            rollback_row = repo.create_governance_routing_autopromotion_rollback_candidate(
                conn,
                workspace_id=workspace_id,
                execution_id=str(execution_row["id"]),
                target_type=preview.target_type,
                target_key=preview.target_key,
                prior_state=apply_result["prior_state"],
                rollback_reason=preview.rollback_reason,
            )
            summary_rows = repo.list_governance_routing_autopromotion_summary(conn, workspace_id=workspace_id)
            recent_execution = repo.get_recent_governance_routing_autopromotion_execution(
                conn,
                workspace_id=workspace_id,
                target_type=preview.target_type,
                target_key=preview.target_key,
                since_hours=int(selected_policy["cooldown_hours"]),
            )
            conn.commit()

        print("policy_selected=true")
        print(f"execution_persisted={1 if execution_row else 0}")
        print(f"rollback_candidate_persisted={1 if rollback_row else 0}")
        print(f"autopromotion_summary_rows={len(summary_rows)}")
        print(f"detail_contract_ok={str(bool(recent_execution)).lower()}")
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
