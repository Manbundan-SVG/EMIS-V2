from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db.client import get_connection
from src.db import repositories as repo
from src.services.case_management_service import build_case_seed
from src.services.routing_outcome_service import RoutingOutcomeService


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
            source_summary={"message": f"phase33a {title_suffix}"},
        ).__dict__,
    )
    repo.upsert_governance_case_summary(
        conn,
        workspace_id=workspace_id,
        case_id=str(case_row["id"]),
        summary_version="v1",
        status_summary=f"{title_suffix} status",
        root_cause_code=root_cause_code,
        root_cause_confidence=0.9,
        root_cause_summary=f"{root_cause_code} detected",
        evidence_summary=None,
        recurrence_summary=None,
        operator_summary=None,
        closure_summary=None,
        recommended_next_action="review routing effectiveness",
        source_note_ids=[],
        source_evidence_ids=[],
        metadata={"validator": "phase33a"},
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


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase33a-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "routing-outcomes-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            outcome_service = RoutingOutcomeService(repo)

            bob_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="version_regression",
                severity="high",
                version_tuple="compute-o1|signals-o1|model-o1",
                title_suffix="operator-bob",
                root_cause_code="version_regression",
                opened_at=now - timedelta(hours=6),
            )
            bob_decision = repo.create_governance_routing_decision(
                conn,
                workspace_id=workspace_id,
                case_id=str(bob_case["id"]),
                routing_rule_id=None,
                override_id=None,
                assigned_team="research",
                assigned_user="bob",
                routing_reason="validator_phase33a_bob",
                workload_snapshot={"validator": "phase33a"},
                metadata={"validator": "phase33a"},
            )
            bob_assignment = repo.create_governance_assignment(
                conn,
                case_id=str(bob_case["id"]),
                workspace_id=workspace_id,
                assigned_to="bob",
                assigned_team="research",
                assigned_by="worker",
                reason="validator_phase33a_bob",
                metadata={"validator": "phase33a"},
            )
            bob_snapshot = _build_snapshot(
                case_row=bob_case,
                routing_decision_id=str(bob_decision["id"]),
                assignment_id=str(bob_assignment["id"]),
                assigned_to="bob",
                assigned_team="research",
                root_cause_code="version_regression",
            )
            outcome_service.record_assignment(conn, bob_snapshot)
            outcome_service.record_acknowledgment(conn, bob_snapshot, now - timedelta(hours=4))
            outcome_service.record_resolution(conn, bob_snapshot, now - timedelta(hours=1))

            carol_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="provider_failure",
                severity="high",
                version_tuple="compute-o1|signals-o1|model-o1",
                title_suffix="operator-carol",
                root_cause_code="provider_failure",
                opened_at=now - timedelta(hours=8),
            )
            carol_decision = repo.create_governance_routing_decision(
                conn,
                workspace_id=workspace_id,
                case_id=str(carol_case["id"]),
                routing_rule_id=None,
                override_id=None,
                assigned_team="platform",
                assigned_user="carol",
                routing_reason="validator_phase33a_carol",
                workload_snapshot={"validator": "phase33a"},
                metadata={"validator": "phase33a"},
            )
            carol_assignment = repo.create_governance_assignment(
                conn,
                case_id=str(carol_case["id"]),
                workspace_id=workspace_id,
                assigned_to="carol",
                assigned_team="platform",
                assigned_by="worker",
                reason="validator_phase33a_carol",
                metadata={"validator": "phase33a"},
            )
            carol_snapshot = _build_snapshot(
                case_row=carol_case,
                routing_decision_id=str(carol_decision["id"]),
                assignment_id=str(carol_assignment["id"]),
                assigned_to="carol",
                assigned_team="platform",
                root_cause_code="provider_failure",
            )
            outcome_service.record_assignment(conn, carol_snapshot)
            outcome_service.record_escalation(
                conn,
                carol_snapshot,
                level="lead",
                reason="stale_high_severity",
            )
            outcome_service.record_reassignment(
                conn,
                carol_snapshot,
                reason="escalation_reassign",
            )
            outcome_service.record_reopen(
                conn,
                carol_snapshot,
                reason="repeat_provider_failure",
            )

            research_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="regime_conflict",
                severity="medium",
                version_tuple="compute-o2|signals-o2|model-o2",
                title_suffix="team-research",
                root_cause_code="regime_conflict",
                opened_at=now - timedelta(hours=5),
            )
            research_decision = repo.create_governance_routing_decision(
                conn,
                workspace_id=workspace_id,
                case_id=str(research_case["id"]),
                routing_rule_id=None,
                override_id=None,
                assigned_team="research",
                assigned_user="alice",
                routing_reason="validator_phase33a_research",
                workload_snapshot={"validator": "phase33a"},
                metadata={"validator": "phase33a"},
            )
            research_assignment = repo.create_governance_assignment(
                conn,
                case_id=str(research_case["id"]),
                workspace_id=workspace_id,
                assigned_to="alice",
                assigned_team="research",
                assigned_by="worker",
                reason="validator_phase33a_research",
                metadata={"validator": "phase33a"},
            )
            research_snapshot = _build_snapshot(
                case_row=research_case,
                routing_decision_id=str(research_decision["id"]),
                assignment_id=str(research_assignment["id"]),
                assigned_to="alice",
                assigned_team="research",
                root_cause_code="regime_conflict",
            )
            outcome_service.record_assignment(conn, research_snapshot)
            outcome_service.record_acknowledgment(conn, research_snapshot, now - timedelta(hours=3))
            outcome_service.record_resolution(conn, research_snapshot, now - timedelta(minutes=90))

            operator_rows = repo.list_governance_operator_effectiveness_summary(conn, workspace_id)
            team_rows = repo.list_governance_team_effectiveness_summary(conn, workspace_id)
            recommendation_rows = repo.list_governance_routing_recommendation_inputs(conn, workspace_id)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select count(*) as count
                    from public.governance_routing_outcomes
                    where workspace_id = %s::uuid
                    """,
                    (workspace_id,),
                )
                outcome_rows = int(cur.fetchone()["count"])

            conn.commit()

        top_operator = operator_rows[0]["assigned_to"] if operator_rows else "none"
        detail_contract_ok = bool(
            operator_rows
            and "resolution_rate" in operator_rows[0]
            and team_rows
            and "escalation_rate" in team_rows[0]
            and recommendation_rows
            and "routing_target" in recommendation_rows[0]
        )
        print(f"routing_outcome_rows={outcome_rows}")
        print(f"operator_summary_rows={len(operator_rows)}")
        print(f"team_summary_rows={len(team_rows)}")
        print(f"recommendation_input_rows={len(recommendation_rows)}")
        print(f"top_operator={top_operator}")
        print(f"detail_contract_ok={str(detail_contract_ok).lower()}")
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
