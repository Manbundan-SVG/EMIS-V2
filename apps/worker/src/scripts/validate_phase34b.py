from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db import repositories as repo
from src.db.client import get_connection
from src.services.case_management_service import build_case_seed
from src.services.incident_performance_service import IncidentPerformanceService


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
    title_suffix: str,
    severity: str,
    root_cause_code: str,
    repeat_count: int,
    reopened_from_case_id: str | None,
    status: str,
    opened_at: datetime,
    acknowledged_at: datetime | None = None,
    resolved_at: datetime | None = None,
    assignments: list[tuple[str | None, str | None, str | None]] | None = None,
    escalated: bool = False,
) -> dict:
    case_row = repo.upsert_governance_case(
        conn,
        build_case_seed(
            workspace_id=workspace_id,
            degradation_state_id=None,
            watchlist_id=watchlist_id,
            version_tuple="compute-g2|signals-g2|model-g2",
            degradation_type="version_regression",
            severity=severity,
            source_summary={"message": f"phase34b {title_suffix}"},
        ).__dict__,
    )

    latest_assignment = (assignments or [(None, None, None)])[-1]
    if assignments:
        for assignee, team, changed_by in assignments:
            repo.create_governance_assignment(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                assigned_to=assignee,
                assigned_team=team,
                assigned_by=changed_by,
                reason=f"validator_{title_suffix}",
                metadata={"validator": "phase34b"},
            )

    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_cases
            set status = %s,
                opened_at = %s::timestamptz,
                acknowledged_at = %s::timestamptz,
                resolved_at = %s::timestamptz,
                closed_at = %s::timestamptz,
                repeat_count = %s,
                reopened_from_case_id = %s::uuid,
                current_assignee = %s,
                current_team = %s,
                updated_at = now()
            where id = %s::uuid
            returning *
            """,
            (
                status,
                opened_at,
                acknowledged_at,
                resolved_at,
                resolved_at if status in {"resolved", "closed"} else None,
                repeat_count,
                reopened_from_case_id,
                latest_assignment[0],
                latest_assignment[1],
                str(case_row["id"]),
            ),
        )
        updated = dict(cur.fetchone())

    repo.upsert_governance_case_summary(
        conn,
        workspace_id=workspace_id,
        case_id=str(updated["id"]),
        summary_version="v1",
        status_summary=f"{title_suffix} status",
        root_cause_code=root_cause_code,
        root_cause_confidence=0.9,
        root_cause_summary=f"{root_cause_code} detected",
        evidence_summary=None,
        recurrence_summary=None,
        operator_summary=None,
        closure_summary=None,
        recommended_next_action="review performance",
        source_note_ids=[],
        source_evidence_ids=[],
        metadata={"validator": "phase34b"},
    )

    if escalated:
        repo.upsert_governance_escalation_state(
            conn,
            workspace_id=workspace_id,
            case_id=str(updated["id"]),
            escalation_level="lead",
            status="active",
            escalated_to_team=latest_assignment[1],
            escalated_to_user=f"lead-{latest_assignment[1]}" if latest_assignment[1] else None,
            reason=f"validator_{title_suffix}_escalated",
            source_policy_id=None,
            escalated_at=opened_at + timedelta(hours=2),
            last_evaluated_at=opened_at + timedelta(hours=3),
            repeated_count=0,
            cleared_at=None,
            metadata={"validator": "phase34b"},
        )

    return updated


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase34b-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "performance-intel-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            resolved_alice = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="resolved-alice",
                severity="medium",
                root_cause_code="provider_failure",
                repeat_count=1,
                reopened_from_case_id=None,
                status="resolved",
                opened_at=now - timedelta(hours=24),
                acknowledged_at=now - timedelta(hours=23),
                resolved_at=now - timedelta(hours=10),
                assignments=[("alice", "research", "validator")],
                escalated=False,
            )
            reopened_bob = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="reopened-bob",
                severity="high",
                root_cause_code="version_regression",
                repeat_count=3,
                reopened_from_case_id=str(resolved_alice["id"]),
                status="acknowledged",
                opened_at=now - timedelta(hours=18),
                acknowledged_at=now - timedelta(hours=16),
                resolved_at=None,
                assignments=[("alice", "research", "validator"), ("bob", "platform", "validator")],
                escalated=True,
            )
            open_bob = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="open-bob",
                severity="critical",
                root_cause_code="version_regression",
                repeat_count=2,
                reopened_from_case_id=None,
                status="in_progress",
                opened_at=now - timedelta(hours=12),
                acknowledged_at=now - timedelta(hours=11),
                resolved_at=None,
                assignments=[("bob", "platform", "validator")],
                escalated=True,
            )
            closed_alice = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="closed-alice",
                severity="high",
                root_cause_code="regime_conflict",
                repeat_count=1,
                reopened_from_case_id=None,
                status="closed",
                opened_at=now - timedelta(hours=36),
                acknowledged_at=now - timedelta(hours=35),
                resolved_at=now - timedelta(hours=4),
                assignments=[("alice", "research", "validator")],
                escalated=False,
            )
            _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="open-carol",
                severity="medium",
                root_cause_code="provider_failure",
                repeat_count=1,
                reopened_from_case_id=None,
                status="open",
                opened_at=now - timedelta(hours=8),
                acknowledged_at=None,
                resolved_at=None,
                assignments=[("carol", "platform", "validator")],
                escalated=False,
            )

            service = IncidentPerformanceService(repo)
            result = service.refresh_workspace_snapshot(conn, workspace_id=workspace_id)
            operator_summary = repo.list_governance_operator_performance_summary(conn, workspace_id=workspace_id)
            team_summary = repo.list_governance_team_performance_summary(conn, workspace_id=workspace_id)
            operator_case_mix = repo.list_governance_operator_case_mix_summary(conn, workspace_id=workspace_id)
            team_case_mix = repo.list_governance_team_case_mix_summary(conn, workspace_id=workspace_id)
            conn.commit()

        print(f"workspace_id={result.workspace_id}")
        print(f"operator_summary_rows={len(operator_summary)}")
        print(f"team_summary_rows={len(team_summary)}")
        print(f"operator_case_mix_rows={len(operator_case_mix)}")
        print(f"team_case_mix_rows={len(team_case_mix)}")
        print(f"top_operator={operator_summary[0]['operator_name'] if operator_summary else None}")
        print(f"top_team={team_summary[0]['assigned_team'] if team_summary else None}")
        print(f"detail_contract_ok={str(bool(operator_summary and team_summary and operator_case_mix and team_case_mix)).lower()}")
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
