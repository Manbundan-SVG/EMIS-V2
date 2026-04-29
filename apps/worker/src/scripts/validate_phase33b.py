from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db.client import get_connection
from src.db import repositories as repo
from src.services.case_management_service import build_case_seed
from src.services.routing_outcome_service import RoutingOutcomeService
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
            source_summary={"message": f"phase33b {title_suffix}"},
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
        recommended_next_action="review routing recommendation",
        source_note_ids=[],
        source_evidence_ids=[],
        metadata={"validator": "phase33b"},
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


def _seed_outcome_history(
    conn,
    *,
    workspace_id: str,
    watchlist_id: str,
    now: datetime,
) -> None:
    outcome_service = RoutingOutcomeService(repo)

    alice_case = _seed_case(
        conn,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        degradation_type="version_regression",
        severity="high",
        version_tuple="compute-r1|signals-r1|model-r1",
        title_suffix="alice-history",
        root_cause_code="version_regression",
        opened_at=now - timedelta(hours=10),
    )
    alice_decision = repo.create_governance_routing_decision(
        conn,
        workspace_id=workspace_id,
        case_id=str(alice_case["id"]),
        routing_rule_id=None,
        override_id=None,
        assigned_team="research",
        assigned_user="alice",
        routing_reason="validator_phase33b_alice",
        workload_snapshot={"validator": "phase33b"},
        metadata={"validator": "phase33b"},
    )
    alice_assignment = repo.create_governance_assignment(
        conn,
        case_id=str(alice_case["id"]),
        workspace_id=workspace_id,
        assigned_to="alice",
        assigned_team="research",
        assigned_by="worker",
        reason="validator_phase33b_alice",
        metadata={"validator": "phase33b"},
    )
    alice_snapshot = _build_snapshot(
        case_row=alice_case,
        routing_decision_id=str(alice_decision["id"]),
        assignment_id=str(alice_assignment["id"]),
        assigned_to="alice",
        assigned_team="research",
        root_cause_code="version_regression",
    )
    outcome_service.record_assignment(conn, alice_snapshot)
    outcome_service.record_acknowledgment(conn, alice_snapshot, now - timedelta(hours=7))
    outcome_service.record_resolution(conn, alice_snapshot, now - timedelta(hours=2))

    bob_case = _seed_case(
        conn,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        degradation_type="provider_failure",
        severity="high",
        version_tuple="compute-r1|signals-r1|model-r1",
        title_suffix="bob-history",
        root_cause_code="provider_failure",
        opened_at=now - timedelta(hours=9),
    )
    bob_decision = repo.create_governance_routing_decision(
        conn,
        workspace_id=workspace_id,
        case_id=str(bob_case["id"]),
        routing_rule_id=None,
        override_id=None,
        assigned_team="platform",
        assigned_user="bob",
        routing_reason="validator_phase33b_bob",
        workload_snapshot={"validator": "phase33b"},
        metadata={"validator": "phase33b"},
    )
    bob_assignment = repo.create_governance_assignment(
        conn,
        case_id=str(bob_case["id"]),
        workspace_id=workspace_id,
        assigned_to="bob",
        assigned_team="platform",
        assigned_by="worker",
        reason="validator_phase33b_bob",
        metadata={"validator": "phase33b"},
    )
    bob_snapshot = _build_snapshot(
        case_row=bob_case,
        routing_decision_id=str(bob_decision["id"]),
        assignment_id=str(bob_assignment["id"]),
        assigned_to="bob",
        assigned_team="platform",
        root_cause_code="provider_failure",
    )
    outcome_service.record_assignment(conn, bob_snapshot)
    outcome_service.record_escalation(conn, bob_snapshot, level="lead", reason="stale_high_severity")
    outcome_service.record_reassignment(conn, bob_snapshot, reason="escalation_reassign")


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase33b-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "routing-recommendation-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            _seed_outcome_history(conn, workspace_id=workspace_id, watchlist_id=watchlist_id, now=now)

            target_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="version_regression",
                severity="high",
                version_tuple="compute-r1|signals-r1|model-r1",
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
            service = RoutingRecommendationService()
            recommendation = service.recommend(case_row=case_row, candidate_rows=candidates)
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
            feedback_row = repo.record_governance_routing_recommendation_feedback(
                conn,
                recommendation_id=str(recommendation_row["id"]),
                accepted=True,
                accepted_by="validator",
                override_reason=None,
                applied=True,
            )
            summary_rows = repo.list_recent_governance_routing_recommendations(
                conn,
                workspace_id=workspace_id,
                case_id=str(target_case["id"]),
                limit=10,
            )
            conn.commit()

        detail_contract_ok = bool(
            summary_rows
            and "recommended_user" in summary_rows[0]
            and "confidence" in summary_rows[0]
            and "accepted" in summary_rows[0]
        )
        print(f"candidate_rows={len(candidates)}")
        print(f"recommended_user={recommendation.recommended_user}")
        print(f"recommended_team={recommendation.recommended_team}")
        print(f"confidence={recommendation.confidence}")
        print(f"summary_rows={len(summary_rows)}")
        print(f"accepted={str(bool(feedback_row.get('accepted'))).lower()}")
        print(f"detail_contract_ok={str(detail_contract_ok).lower()}")
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
