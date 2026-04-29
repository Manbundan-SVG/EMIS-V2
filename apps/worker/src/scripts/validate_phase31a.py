from __future__ import annotations

from datetime import datetime, timezone

from src.db.client import get_connection
from src.db.repositories import (
    create_governance_assignment,
    create_governance_routing_decision,
    get_governance_case_summary_row,
    list_operator_case_metrics,
    list_team_case_metrics,
    upsert_governance_case,
    upsert_governance_case_summary,
)
from src.services.assignment_routing_service import AssignmentRoutingService, RoutingInput
from src.services.case_management_service import build_case_seed
from src.db import repositories as repo


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
            source_summary={"message": f"phase31a {title_suffix}"},
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
        recommended_next_action="route to owner",
        source_note_ids=[],
        source_evidence_ids=[],
        metadata={"validator": "phase31a"},
    )
    return case_row


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase31a-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "routing-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)
    routing_service = AssignmentRoutingService(repo)

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into public.governance_routing_rules (
                      workspace_id,
                      priority,
                      root_cause_code,
                      severity,
                      watchlist_id,
                      version_tuple,
                      assign_team,
                      routing_reason_template,
                      metadata
                    ) values (
                      %s::uuid,
                      10,
                      'version_regression',
                      'high',
                      %s::uuid,
                      'compute-r1|signals-r1|model-r1',
                      'research',
                      'rule_{root_cause_code}_{severity}',
                      %s::jsonb
                    )
                    returning id
                    """,
                    (workspace_id, watchlist_id, '{"validator":"phase31a"}'),
                )
                rule_id = str(cur.fetchone()["id"])

            alice_one = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="version_regression",
                severity="high",
                version_tuple="compute-r1|signals-r1|model-r1",
                title_suffix="alice-one",
                root_cause_code="version_regression",
            )
            create_governance_assignment(
                conn,
                case_id=str(alice_one["id"]),
                workspace_id=workspace_id,
                assigned_to="alice",
                assigned_team="research",
                assigned_by="validator",
                reason="seed_workload",
                metadata={"validator": "phase31a"},
            )

            alice_two = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="version_regression",
                severity="medium",
                version_tuple="compute-r1|signals-r1|model-r1",
                title_suffix="alice-two",
                root_cause_code="version_regression",
            )
            create_governance_assignment(
                conn,
                case_id=str(alice_two["id"]),
                workspace_id=workspace_id,
                assigned_to="alice",
                assigned_team="research",
                assigned_by="validator",
                reason="seed_workload",
                metadata={"validator": "phase31a"},
            )

            bob_one = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="version_regression",
                severity="medium",
                version_tuple="compute-r1|signals-r1|model-r1",
                title_suffix="bob-one",
                root_cause_code="version_regression",
            )
            create_governance_assignment(
                conn,
                case_id=str(bob_one["id"]),
                workspace_id=workspace_id,
                assigned_to="bob",
                assigned_team="research",
                assigned_by="validator",
                reason="seed_workload",
                metadata={"validator": "phase31a"},
            )

            target_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="version_regression",
                severity="high",
                version_tuple="compute-r1|signals-r1|model-r1",
                title_suffix="rule-target",
                root_cause_code="version_regression",
            )
            rule_decision = routing_service.route_case(
                conn,
                RoutingInput(
                    workspace_id=workspace_id,
                    case_id=str(target_case["id"]),
                    watchlist_id=watchlist_id,
                    severity="high",
                    root_cause_code="version_regression",
                    version_tuple="compute-r1|signals-r1|model-r1",
                    repeat_count=1,
                    chronic=False,
                ),
            )
            if rule_decision.assigned_user != "bob":
                raise RuntimeError(f"expected bob from workload routing, got {rule_decision.assigned_user!r}")
            if rule_decision.assigned_team != "research":
                raise RuntimeError(f"expected research team, got {rule_decision.assigned_team!r}")

            create_governance_routing_decision(
                conn,
                workspace_id=workspace_id,
                case_id=str(target_case["id"]),
                routing_rule_id=rule_decision.routing_rule_id,
                override_id=rule_decision.override_id,
                assigned_team=rule_decision.assigned_team,
                assigned_user=rule_decision.assigned_user,
                routing_reason=rule_decision.routing_reason,
                workload_snapshot=rule_decision.workload_snapshot,
                metadata={"validator": "phase31a", "path": "rule"},
            )
            create_governance_assignment(
                conn,
                case_id=str(target_case["id"]),
                workspace_id=workspace_id,
                assigned_to=rule_decision.assigned_user,
                assigned_team=rule_decision.assigned_team,
                assigned_by="worker",
                reason=rule_decision.routing_reason,
                metadata={"validator": "phase31a"},
            )

            override_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="version_regression",
                severity="high",
                version_tuple="compute-r1|signals-r1|model-r1",
                title_suffix="override-target",
                root_cause_code="version_regression",
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into public.governance_routing_overrides (
                      workspace_id,
                      case_id,
                      assigned_team,
                      assigned_user,
                      reason,
                      metadata
                    ) values (
                      %s::uuid,
                      %s::uuid,
                      'platform',
                      'carol',
                      'case_specific_override',
                      %s::jsonb
                    )
                    returning id
                    """,
                    (workspace_id, str(override_case["id"]), '{"validator":"phase31a"}'),
                )
                override_id = str(cur.fetchone()["id"])

            override_decision = routing_service.route_case(
                conn,
                RoutingInput(
                    workspace_id=workspace_id,
                    case_id=str(override_case["id"]),
                    watchlist_id=watchlist_id,
                    severity="high",
                    root_cause_code="version_regression",
                    version_tuple="compute-r1|signals-r1|model-r1",
                    repeat_count=2,
                    chronic=True,
                ),
            )
            if override_decision.override_id != override_id:
                raise RuntimeError("expected override to win for override case")
            if override_decision.assigned_user != "carol":
                raise RuntimeError(f"expected carol from override, got {override_decision.assigned_user!r}")

            create_governance_routing_decision(
                conn,
                workspace_id=workspace_id,
                case_id=str(override_case["id"]),
                routing_rule_id=override_decision.routing_rule_id,
                override_id=override_decision.override_id,
                assigned_team=override_decision.assigned_team,
                assigned_user=override_decision.assigned_user,
                routing_reason=override_decision.routing_reason,
                workload_snapshot=override_decision.workload_snapshot,
                metadata={"validator": "phase31a", "path": "override"},
            )
            create_governance_assignment(
                conn,
                case_id=str(override_case["id"]),
                workspace_id=workspace_id,
                assigned_to=override_decision.assigned_user,
                assigned_team=override_decision.assigned_team,
                assigned_by="worker",
                reason=override_decision.routing_reason,
                metadata={"validator": "phase31a"},
            )

            target_case_after = get_governance_case_summary_row(conn, case_id=str(target_case["id"]))
            override_case_after = get_governance_case_summary_row(conn, case_id=str(override_case["id"]))
            operator_metrics = list_operator_case_metrics(conn, workspace_id)
            team_metrics = list_team_case_metrics(conn, workspace_id)

            if not target_case_after or target_case_after.get("current_assignee") != "bob":
                raise RuntimeError("expected target case to be assigned to bob")
            if not override_case_after or override_case_after.get("current_assignee") != "carol":
                raise RuntimeError("expected override case to be assigned to carol")
            if not any(row["operator_id"] == "bob" for row in operator_metrics):
                raise RuntimeError("expected bob in operator metrics")
            if not any(row["assigned_team"] == "research" for row in team_metrics):
                raise RuntimeError("expected research team in team metrics")

            conn.commit()

        print(
            "phase31a smoke ok "
            f"workspace_slug={workspace_slug} "
            f"routing_rule_id={rule_id} "
            f"rule_assigned_user={rule_decision.assigned_user} "
            f"override_assigned_user={override_decision.assigned_user} "
            f"operator_metric_rows={len(operator_metrics)} "
            f"team_metric_rows={len(team_metrics)}"
        )
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
