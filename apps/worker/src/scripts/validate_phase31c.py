from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db.client import get_connection
from src.db.repositories import (
    create_governance_assignment,
    ensure_default_governance_escalation_policies,
    ensure_default_governance_sla_policies,
    get_governance_case_sla_summary_row,
    get_governance_case_summary_latest,
    get_governance_case_summary_row,
    get_governance_escalation_state,
    insert_governance_escalation_event,
    list_governance_escalation_policies,
    list_governance_sla_policies,
    list_operator_workload_pressure,
    list_team_workload_pressure,
    upsert_governance_case,
    upsert_governance_case_summary,
    upsert_governance_escalation_state,
    upsert_governance_sla_evaluation,
)
from src.services.case_management_service import build_case_seed
from src.services.escalation_service import EscalationPolicy, EscalationService
from src.services.workload_sla_service import SlaPolicy, WorkloadSlaService


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
    severity: str,
    version_tuple: str,
    degradation_type: str,
    root_cause_code: str,
    title_suffix: str,
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
            source_summary={"message": f"phase31c {title_suffix}"},
        ).__dict__,
    )
    upsert_governance_case_summary(
        conn,
        workspace_id=workspace_id,
        case_id=str(case_row["id"]),
        summary_version="v1",
        status_summary=f"{title_suffix} status",
        root_cause_code=root_cause_code,
        root_cause_confidence=0.96,
        root_cause_summary=f"{root_cause_code} detected",
        evidence_summary="validator evidence",
        recurrence_summary=None,
        operator_summary=None,
        closure_summary=None,
        recommended_next_action="evaluate escalation",
        source_note_ids=[],
        source_evidence_ids=[],
        metadata={"validator": "phase31c"},
    )
    return case_row


def _evaluate_sla(conn, *, case_id: str, sla_service: WorkloadSlaService) -> None:
    case_row = get_governance_case_summary_row(conn, case_id=case_id)
    if not case_row:
        raise RuntimeError("case_row missing for SLA evaluation")
    workspace_id = str(case_row["workspace_id"])
    ensure_default_governance_sla_policies(conn, workspace_id=workspace_id)
    policies = [
        SlaPolicy(
            id=str(row["id"]),
            severity=str(row["severity"]),
            chronicity_class=str(row["chronicity_class"]) if row.get("chronicity_class") else None,
            ack_within_minutes=int(row["ack_within_minutes"]),
            resolve_within_minutes=int(row["resolve_within_minutes"]),
        )
        for row in list_governance_sla_policies(conn, workspace_id=workspace_id)
    ]
    policy = sla_service.choose_policy(
        severity=str(case_row["severity"]),
        chronicity_class=sla_service.derive_chronicity_class(case_row),
        policies=policies,
    )
    evaluation = sla_service.evaluate_case(case_row=case_row, policy=policy)
    upsert_governance_sla_evaluation(
        conn,
        workspace_id=workspace_id,
        case_id=case_id,
        policy_id=evaluation.policy_id,
        chronicity_class=evaluation.chronicity_class,
        ack_due_at=evaluation.ack_due_at,
        resolve_due_at=evaluation.resolve_due_at,
        ack_breached=evaluation.ack_breached,
        resolve_breached=evaluation.resolve_breached,
        breach_severity=evaluation.breach_severity,
        metadata=evaluation.metadata,
    )


def _evaluate_escalation(conn, *, case_id: str, escalation_service: EscalationService):
    case_row = get_governance_case_summary_row(conn, case_id=case_id)
    if not case_row:
        raise RuntimeError("case_row missing for escalation evaluation")

    workspace_id = str(case_row["workspace_id"])
    ensure_default_governance_escalation_policies(conn, workspace_id=workspace_id)
    policies = [
        EscalationPolicy(
            id=str(row["id"]),
            severity=str(row["severity"]) if row.get("severity") else None,
            chronicity_class=str(row["chronicity_class"]) if row.get("chronicity_class") else None,
            root_cause_code=str(row["root_cause_code"]) if row.get("root_cause_code") else None,
            min_case_age_minutes=int(row["min_case_age_minutes"]) if row.get("min_case_age_minutes") is not None else None,
            min_ack_age_minutes=int(row["min_ack_age_minutes"]) if row.get("min_ack_age_minutes") is not None else None,
            min_repeat_count=int(row["min_repeat_count"]) if row.get("min_repeat_count") is not None else None,
            min_operator_pressure=float(row["min_operator_pressure"]) if row.get("min_operator_pressure") is not None else None,
            escalation_level=str(row["escalation_level"]),
            escalate_to_team=str(row["escalate_to_team"]) if row.get("escalate_to_team") else None,
            escalate_to_user=str(row["escalate_to_user"]) if row.get("escalate_to_user") else None,
            cooldown_minutes=int(row.get("cooldown_minutes") or 240),
            metadata=dict(row.get("metadata") or {}),
        )
        for row in list_governance_escalation_policies(conn, workspace_id=workspace_id)
    ]
    summary_latest = get_governance_case_summary_latest(conn, case_id=case_id)
    sla_row = get_governance_case_sla_summary_row(conn, case_id=case_id)
    operator_pressure_row = next(
        (
            row
            for row in list_operator_workload_pressure(conn, workspace_id)
            if row.get("assigned_to") == case_row.get("current_assignee")
        ),
        None,
    )
    team_pressure_row = next(
        (
            row
            for row in list_team_workload_pressure(conn, workspace_id)
            if row.get("assigned_team") == case_row.get("current_team")
        ),
        None,
    )
    current_state = get_governance_escalation_state(conn, case_id=case_id)
    context = escalation_service.build_context(
        case_row=case_row,
        case_summary_latest=summary_latest,
        sla_row=sla_row,
        operator_pressure_row=operator_pressure_row,
        team_pressure_row=team_pressure_row,
    )
    decision = escalation_service.evaluate(
        context=context,
        policies=policies,
        current_state=current_state,
    )
    if not decision.should_escalate and not decision.clear_existing:
        return None

    repeated_count = (
        decision.repeated_count
        or (int(current_state.get("repeated_count") or 1) if current_state else 1)
    )
    state_row = upsert_governance_escalation_state(
        conn,
        workspace_id=workspace_id,
        case_id=case_id,
        escalation_level=decision.escalation_level or str(current_state.get("escalation_level") or "cleared"),
        status=decision.status or ("active" if decision.should_escalate else "cleared"),
        escalated_to_team=decision.escalated_to_team,
        escalated_to_user=decision.escalated_to_user,
        reason=decision.reason,
        source_policy_id=decision.policy_id,
        escalated_at=current_state.get("escalated_at") if current_state and decision.event_type == "escalation_repeated" else context.now,
        last_evaluated_at=context.now,
        repeated_count=repeated_count,
        cleared_at=context.now if decision.clear_existing else None,
        metadata=decision.metadata,
    )
    event_row = insert_governance_escalation_event(
        conn,
        workspace_id=workspace_id,
        case_id=case_id,
        escalation_state_id=str(state_row["id"]),
        event_type=decision.event_type or ("case_escalated" if decision.should_escalate else "escalation_cleared"),
        escalation_level=decision.escalation_level,
        escalated_to_team=decision.escalated_to_team,
        escalated_to_user=decision.escalated_to_user,
        reason=decision.reason,
        source_policy_id=decision.policy_id,
        metadata=decision.metadata,
    )
    return {"decision": decision, "state": state_row, "event": event_row}


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase31c-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "escalation-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)
    sla_service = WorkloadSlaService()
    escalation_service = EscalationService()

    try:
        with get_connection() as conn:
            ensure_default_governance_escalation_policies(conn, workspace_id=workspace_id)
            ensure_default_governance_sla_policies(conn, workspace_id=workspace_id)

            healthy_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                severity="medium",
                version_tuple="compute-e1|signals-e1|model-e1",
                degradation_type="provider_failure",
                root_cause_code="provider_failure",
                title_suffix="healthy",
            )
            create_governance_assignment(
                conn,
                case_id=str(healthy_case["id"]),
                workspace_id=workspace_id,
                assigned_to="alice",
                assigned_team="platform",
                assigned_by="validator",
                reason="seed_healthy",
                metadata={"validator": "phase31c"},
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update public.governance_cases
                    set opened_at = %s,
                        acknowledged_at = %s,
                        status = 'acknowledged',
                        updated_at = now()
                    where id = %s::uuid
                    """,
                    (now - timedelta(minutes=30), now - timedelta(minutes=10), str(healthy_case["id"])),
                )

            recurring_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                severity="high",
                version_tuple="compute-e1|signals-e1|model-e1",
                degradation_type="version_regression",
                root_cause_code="version_regression",
                title_suffix="recurring",
            )
            create_governance_assignment(
                conn,
                case_id=str(recurring_case["id"]),
                workspace_id=workspace_id,
                assigned_to="bob",
                assigned_team="research",
                assigned_by="validator",
                reason="seed_recurring",
                metadata={"validator": "phase31c"},
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update public.governance_cases
                    set opened_at = %s,
                        acknowledged_at = %s,
                        status = 'acknowledged',
                        repeat_count = 2,
                        updated_at = now()
                    where id = %s::uuid
                    """,
                    (now - timedelta(hours=5), now - timedelta(hours=4), str(recurring_case["id"])),
                )

            _evaluate_sla(conn, case_id=str(healthy_case["id"]), sla_service=sla_service)
            _evaluate_sla(conn, case_id=str(recurring_case["id"]), sla_service=sla_service)

            healthy_decision = _evaluate_escalation(
                conn,
                case_id=str(healthy_case["id"]),
                escalation_service=escalation_service,
            )
            first_escalation = _evaluate_escalation(
                conn,
                case_id=str(recurring_case["id"]),
                escalation_service=escalation_service,
            )
            second_escalation = _evaluate_escalation(
                conn,
                case_id=str(recurring_case["id"]),
                escalation_service=escalation_service,
            )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    update public.governance_cases
                    set status = 'resolved',
                        resolved_at = now(),
                        closed_at = now(),
                        updated_at = now()
                    where id = %s::uuid
                    """,
                    (str(recurring_case["id"]),),
                )
            _evaluate_sla(conn, case_id=str(recurring_case["id"]), sla_service=sla_service)
            cleared_escalation = _evaluate_escalation(
                conn,
                case_id=str(recurring_case["id"]),
                escalation_service=escalation_service,
            )

            with conn.cursor() as cur:
                cur.execute(
                    "select count(*) as count from public.governance_escalation_state where workspace_id = %s::uuid and status = 'active'",
                    (workspace_id,),
                )
                active_escalations = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_escalation_events where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                event_count = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_escalation_events where workspace_id = %s::uuid and event_type = 'escalation_repeated'",
                    (workspace_id,),
                )
                repeated_event_count = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_escalation_events where workspace_id = %s::uuid and event_type = 'escalation_cleared'",
                    (workspace_id,),
                )
                cleared_count = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_escalation_state where case_id = %s::uuid",
                    (str(healthy_case["id"]),),
                )
                healthy_state_count = int(cur.fetchone()["count"])

            conn.commit()

        if healthy_decision is not None or healthy_state_count != 0:
            raise RuntimeError("healthy case should not escalate")
        if not first_escalation or first_escalation["decision"].event_type != "case_escalated":
            raise RuntimeError("expected first escalation event")
        if second_escalation is not None:
            raise RuntimeError("expected cooldown to suppress duplicate escalation event")
        if not cleared_escalation or cleared_escalation["decision"].event_type != "escalation_cleared":
            raise RuntimeError("expected escalation clear after resolution")
        if active_escalations != 0:
            raise RuntimeError(f"expected 0 active escalations after clear, got {active_escalations}")
        if event_count != 2:
            raise RuntimeError(f"expected 2 escalation events, got {event_count}")
        if repeated_event_count != 0:
            raise RuntimeError(f"expected 0 repeated escalation events, got {repeated_event_count}")
        if cleared_count != 1:
            raise RuntimeError(f"expected 1 cleared escalation event, got {cleared_count}")

        print(
            "phase31c smoke ok "
            f"workspace_slug={workspace_slug} "
            f"active_escalations={active_escalations} "
            f"event_count={event_count} "
            f"repeated_event_count={repeated_event_count} "
            f"cleared_count={cleared_count} "
            "healthy_case_escalated=false"
        )
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
