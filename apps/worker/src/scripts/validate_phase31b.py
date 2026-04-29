from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db.client import get_connection
from src.db.repositories import (
    create_governance_assignment,
    ensure_default_governance_sla_policies,
    get_governance_case_summary_row,
    list_governance_sla_policies,
    upsert_governance_case,
    upsert_governance_sla_evaluation,
)
from src.services.case_management_service import build_case_seed
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
    title_suffix: str,
) -> dict:
    return upsert_governance_case(
        conn,
        build_case_seed(
            workspace_id=workspace_id,
            degradation_state_id=None,
            watchlist_id=watchlist_id,
            version_tuple=version_tuple,
            degradation_type=degradation_type,
            severity=severity,
            source_summary={"message": f"phase31b {title_suffix}"},
        ).__dict__,
    )


def _evaluate_case(conn, *, case_id: str, service: WorkloadSlaService) -> None:
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
    chronicity_class = service.derive_chronicity_class(case_row)
    policy = service.choose_policy(
        severity=str(case_row["severity"]),
        chronicity_class=chronicity_class,
        policies=policies,
    )
    evaluation = service.evaluate_case(case_row=case_row, policy=policy)
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


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase31b-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "workload-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)
    service = WorkloadSlaService()

    try:
        with get_connection() as conn:
            ensure_default_governance_sla_policies(conn, workspace_id=workspace_id)

            case_one = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                severity="critical",
                version_tuple="compute-w1|signals-w1|model-w1",
                degradation_type="provider_failure",
                title_suffix="case-one",
            )
            create_governance_assignment(
                conn,
                case_id=str(case_one["id"]),
                workspace_id=workspace_id,
                assigned_to="alice",
                assigned_team="platform",
                assigned_by="validator",
                reason="seed_case_one",
                metadata={"validator": "phase31b"},
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update public.governance_cases
                    set opened_at = %s,
                        updated_at = now()
                    where id = %s::uuid
                    """,
                    (now - timedelta(hours=7), str(case_one["id"])),
                )

            case_two = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                severity="high",
                version_tuple="compute-w1|signals-w1|model-w1",
                degradation_type="version_regression",
                title_suffix="case-two",
            )
            create_governance_assignment(
                conn,
                case_id=str(case_two["id"]),
                workspace_id=workspace_id,
                assigned_to="bob",
                assigned_team="research",
                assigned_by="validator",
                reason="seed_case_two",
                metadata={"validator": "phase31b"},
            )
            with conn.cursor() as cur:
                opened_at = now - timedelta(hours=2)
                acknowledged_at = opened_at + timedelta(minutes=10)
                cur.execute(
                    """
                    update public.governance_cases
                    set opened_at = %s,
                        acknowledged_at = %s,
                        status = 'acknowledged',
                        updated_at = now()
                    where id = %s::uuid
                    """,
                    (opened_at, acknowledged_at, str(case_two["id"])),
                )

            case_three = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                severity="medium",
                version_tuple="compute-w1|signals-w1|model-w1",
                degradation_type="regime_conflict_persistence",
                title_suffix="case-three",
            )
            create_governance_assignment(
                conn,
                case_id=str(case_three["id"]),
                workspace_id=workspace_id,
                assigned_to="alice",
                assigned_team="platform",
                assigned_by="validator",
                reason="seed_case_three",
                metadata={"validator": "phase31b"},
            )
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update public.governance_cases
                    set opened_at = %s,
                        repeat_count = 2,
                        updated_at = now()
                    where id = %s::uuid
                    """,
                    (now - timedelta(minutes=90), str(case_three["id"])),
                )

            _evaluate_case(conn, case_id=str(case_one["id"]), service=service)
            _evaluate_case(conn, case_id=str(case_two["id"]), service=service)
            _evaluate_case(conn, case_id=str(case_three["id"]), service=service)

            with conn.cursor() as cur:
                cur.execute(
                    "select count(*) as count from public.governance_sla_policies where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                sla_policy_rows = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_sla_evaluations where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                evaluated_case_rows = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_stale_case_summary where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                stale_case_rows = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_operator_workload_pressure where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                operator_pressure_rows = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*) as count from public.governance_team_workload_pressure where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                team_pressure_rows = int(cur.fetchone()["count"])
                cur.execute(
                    """
                    select count(*) as count
                    from public.governance_case_sla_summary
                    where workspace_id = %s::uuid
                      and ack_due_at is not null
                    """,
                    (workspace_id,),
                )
                detail_contract_count = int(cur.fetchone()["count"])

            conn.commit()

        if sla_policy_rows < 4:
            raise RuntimeError(f"expected at least 4 SLA policies, got {sla_policy_rows}")
        if evaluated_case_rows != 3:
            raise RuntimeError(f"expected 3 evaluated cases, got {evaluated_case_rows}")
        if stale_case_rows < 1:
            raise RuntimeError("expected at least one stale case")
        if operator_pressure_rows < 2:
            raise RuntimeError("expected at least two operator pressure rows")
        if team_pressure_rows < 2:
            raise RuntimeError("expected at least two team pressure rows")
        if detail_contract_count != 3:
            raise RuntimeError(f"expected 3 SLA summary rows, got {detail_contract_count}")

        print(
            "phase31b smoke ok "
            f"workspace_slug={workspace_slug} "
            f"sla_policy_rows={sla_policy_rows} "
            f"evaluated_case_rows={evaluated_case_rows} "
            f"stale_case_rows={stale_case_rows} "
            f"operator_pressure_rows={operator_pressure_rows} "
            f"team_pressure_rows={team_pressure_rows} "
            "detail_contract_ok=true"
        )
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
