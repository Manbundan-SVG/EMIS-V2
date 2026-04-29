from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db.client import get_connection
from src.db.repositories import (
    create_governance_assignment,
    create_governance_reassignment_event,
    create_governance_routing_decision,
    create_governance_routing_feedback,
    upsert_governance_case,
    upsert_governance_case_summary,
)
from src.services.assignment_routing_feedback_service import classify_routing_feedback
from src.services.case_management_service import build_case_seed


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
    case_row = upsert_governance_case(
        conn,
        build_case_seed(
            workspace_id=workspace_id,
            degradation_state_id=None,
            watchlist_id=watchlist_id,
            version_tuple=version_tuple,
            degradation_type=degradation_type,
            severity=severity,
            source_summary={"message": f"phase31d {title_suffix}"},
        ).__dict__,
    )
    upsert_governance_case_summary(
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
        recommended_next_action="review routing outcome",
        source_note_ids=[],
        source_evidence_ids=[],
        metadata={"validator": "phase31d"},
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_cases
            set opened_at = %s,
                updated_at = now()
            where id = %s::uuid
            returning *
            """,
            (opened_at, str(case_row["id"])),
        )
        return dict(cur.fetchone())


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase31d-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "routing-feedback-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            accepted_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="version_regression",
                severity="high",
                version_tuple="compute-f1|signals-f1|model-f1",
                title_suffix="accepted",
                root_cause_code="version_regression",
                opened_at=now - timedelta(hours=2),
            )
            accepted_decision = create_governance_routing_decision(
                conn,
                workspace_id=workspace_id,
                case_id=str(accepted_case["id"]),
                routing_rule_id=None,
                override_id=None,
                assigned_team="research",
                assigned_user="bob",
                routing_reason="validator_acceptance_route",
                workload_snapshot={"validator": "phase31d"},
                metadata={"validator": "phase31d"},
            )
            accepted_assignment = create_governance_assignment(
                conn,
                case_id=str(accepted_case["id"]),
                workspace_id=workspace_id,
                assigned_to="bob",
                assigned_team="research",
                assigned_by="worker",
                reason="validator_acceptance_route",
                metadata={"validator": "phase31d"},
            )
            accepted_feedback = classify_routing_feedback(
                previous_assigned_to=None,
                previous_assigned_team=None,
                new_assigned_to="bob",
                new_assigned_team="research",
                escalation_active=False,
                reason="validator acceptance",
            )
            create_governance_routing_feedback(
                conn,
                workspace_id=workspace_id,
                case_id=str(accepted_case["id"]),
                routing_decision_id=str(accepted_decision["id"]),
                feedback_type=accepted_feedback.feedback_type,
                assigned_to=accepted_feedback.assigned_to,
                assigned_team=accepted_feedback.assigned_team,
                prior_assigned_to=accepted_feedback.prior_assigned_to,
                prior_assigned_team=accepted_feedback.prior_assigned_team,
                root_cause_code="version_regression",
                severity="high",
                recurrence_group_id=None,
                repeat_count=1,
                reason=accepted_feedback.reason,
                metadata={"assignment_id": str(accepted_assignment["id"]), "validator": "phase31d"},
            )

            manual_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="provider_failure",
                severity="high",
                version_tuple="compute-f1|signals-f1|model-f1",
                title_suffix="manual",
                root_cause_code="provider_failure",
                opened_at=now - timedelta(hours=4),
            )
            baseline_manual_assignment = create_governance_assignment(
                conn,
                case_id=str(manual_case["id"]),
                workspace_id=workspace_id,
                assigned_to="alice",
                assigned_team="platform",
                assigned_by="validator",
                reason="baseline_assignment",
                metadata={"validator": "phase31d"},
            )
            manual_decision = create_governance_routing_decision(
                conn,
                workspace_id=workspace_id,
                case_id=str(manual_case["id"]),
                routing_rule_id=None,
                override_id=None,
                assigned_team="research",
                assigned_user="carol",
                routing_reason="manual_override_to_research",
                workload_snapshot={"validator": "phase31d"},
                metadata={"validator": "phase31d"},
            )
            manual_assignment = create_governance_assignment(
                conn,
                case_id=str(manual_case["id"]),
                workspace_id=workspace_id,
                assigned_to="carol",
                assigned_team="research",
                assigned_by="ops",
                reason="manual_override_to_research",
                metadata={"validator": "phase31d"},
            )
            manual_feedback = classify_routing_feedback(
                previous_assigned_to="alice",
                previous_assigned_team="platform",
                new_assigned_to="carol",
                new_assigned_team="research",
                escalation_active=False,
                policy_changed=True,
                reason="manual override by ops",
            )
            create_governance_routing_feedback(
                conn,
                workspace_id=workspace_id,
                case_id=str(manual_case["id"]),
                routing_decision_id=str(manual_decision["id"]),
                feedback_type=manual_feedback.feedback_type,
                assigned_to=manual_feedback.assigned_to,
                assigned_team=manual_feedback.assigned_team,
                prior_assigned_to=manual_feedback.prior_assigned_to,
                prior_assigned_team=manual_feedback.prior_assigned_team,
                root_cause_code="provider_failure",
                severity="high",
                recurrence_group_id=None,
                repeat_count=1,
                reason=manual_feedback.reason,
                metadata={"assignment_id": str(manual_assignment["id"]), "validator": "phase31d"},
            )
            create_governance_reassignment_event(
                conn,
                workspace_id=workspace_id,
                case_id=str(manual_case["id"]),
                routing_decision_id=str(manual_decision["id"]),
                previous_assigned_to="alice",
                previous_assigned_team="platform",
                new_assigned_to="carol",
                new_assigned_team="research",
                reassignment_type=manual_feedback.reassignment_type or "manual_override",
                reassignment_reason=manual_feedback.reason,
                minutes_since_open=240,
                minutes_since_last_assignment=15,
                metadata={
                    "baseline_assignment_id": str(baseline_manual_assignment["id"]),
                    "assignment_id": str(manual_assignment["id"]),
                    "validator": "phase31d",
                },
            )

            escalation_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="provider_failure",
                severity="critical",
                version_tuple="compute-f1|signals-f1|model-f1",
                title_suffix="escalation",
                root_cause_code="provider_failure",
                opened_at=now - timedelta(hours=6),
            )
            baseline_escalation_assignment = create_governance_assignment(
                conn,
                case_id=str(escalation_case["id"]),
                workspace_id=workspace_id,
                assigned_to="dave",
                assigned_team="triage",
                assigned_by="validator",
                reason="baseline_assignment",
                metadata={"validator": "phase31d"},
            )
            escalation_assignment = create_governance_assignment(
                conn,
                case_id=str(escalation_case["id"]),
                workspace_id=workspace_id,
                assigned_to="erin",
                assigned_team="platform",
                assigned_by="worker",
                reason="escalation_reroute",
                metadata={"validator": "phase31d"},
            )
            escalation_feedback = classify_routing_feedback(
                previous_assigned_to="dave",
                previous_assigned_team="triage",
                new_assigned_to="erin",
                new_assigned_team="platform",
                escalation_active=True,
                reason="escalated to platform lead",
            )
            create_governance_routing_feedback(
                conn,
                workspace_id=workspace_id,
                case_id=str(escalation_case["id"]),
                routing_decision_id=None,
                feedback_type=escalation_feedback.feedback_type,
                assigned_to=escalation_feedback.assigned_to,
                assigned_team=escalation_feedback.assigned_team,
                prior_assigned_to=escalation_feedback.prior_assigned_to,
                prior_assigned_team=escalation_feedback.prior_assigned_team,
                root_cause_code="provider_failure",
                severity="critical",
                recurrence_group_id=None,
                repeat_count=2,
                reason=escalation_feedback.reason,
                metadata={"assignment_id": str(escalation_assignment["id"]), "validator": "phase31d"},
            )
            create_governance_reassignment_event(
                conn,
                workspace_id=workspace_id,
                case_id=str(escalation_case["id"]),
                routing_decision_id=None,
                previous_assigned_to="dave",
                previous_assigned_team="triage",
                new_assigned_to="erin",
                new_assigned_team="platform",
                reassignment_type=escalation_feedback.reassignment_type or "escalation",
                reassignment_reason=escalation_feedback.reason,
                minutes_since_open=360,
                minutes_since_last_assignment=30,
                metadata={
                    "baseline_assignment_id": str(baseline_escalation_assignment["id"]),
                    "assignment_id": str(escalation_assignment["id"]),
                    "validator": "phase31d",
                },
            )

            with conn.cursor() as cur:
                cur.execute(
                    "select count(*) as count from public.governance_routing_feedback where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                routing_feedback_rows = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_reassignment_events where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                reassignment_event_rows = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_routing_quality_summary where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                quality_summary_rows = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_reassignment_pressure_summary where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                pressure_summary_rows = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_routing_feedback where workspace_id = %s::uuid and feedback_type = 'accepted'",
                    (workspace_id,),
                )
                accepted_feedback_count = int(cur.fetchone()["count"])
                cur.execute(
                    """
                    select count(*) as count
                    from public.governance_routing_feedback
                    where workspace_id = %s::uuid
                      and feedback_type in ('manual_reassign', 'escalation_reroute', 'workload_rebalance')
                    """,
                    (workspace_id,),
                )
                rerouted_feedback_count = int(cur.fetchone()["count"])

            conn.commit()

        if routing_feedback_rows != 3:
            raise RuntimeError(f"expected 3 routing feedback rows, got {routing_feedback_rows}")
        if reassignment_event_rows != 2:
            raise RuntimeError(f"expected 2 reassignment events, got {reassignment_event_rows}")
        if quality_summary_rows < 3:
            raise RuntimeError(f"expected at least 3 routing quality rows, got {quality_summary_rows}")
        if pressure_summary_rows != 2:
            raise RuntimeError(f"expected 2 reassignment pressure rows, got {pressure_summary_rows}")
        if accepted_feedback_count != 1:
            raise RuntimeError(f"expected 1 accepted feedback row, got {accepted_feedback_count}")
        if rerouted_feedback_count != 2:
            raise RuntimeError(f"expected 2 rerouted feedback rows, got {rerouted_feedback_count}")

        print(
            "phase31d smoke ok "
            f"workspace_slug={workspace_slug} "
            f"routing_feedback_rows={routing_feedback_rows} "
            f"reassignment_event_rows={reassignment_event_rows} "
            f"quality_summary_rows={quality_summary_rows} "
            f"pressure_summary_rows={pressure_summary_rows} "
            "detail_contract_ok=true"
        )
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
