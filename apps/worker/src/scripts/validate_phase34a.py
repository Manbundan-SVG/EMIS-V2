from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db import repositories as repo
from src.db.client import get_connection
from src.services.case_management_service import build_case_seed
from src.services.incident_analytics_service import IncidentAnalyticsService


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
    current_assignee: str | None = None,
    current_team: str | None = None,
) -> dict:
    case_row = repo.upsert_governance_case(
        conn,
        build_case_seed(
            workspace_id=workspace_id,
            degradation_state_id=None,
            watchlist_id=watchlist_id,
            version_tuple="compute-g1|signals-g1|model-g1",
            degradation_type="version_regression",
            severity=severity,
            source_summary={"message": f"phase34a {title_suffix}"},
        ).__dict__,
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
                resolved_at,
                repeat_count,
                reopened_from_case_id,
                current_assignee,
                current_team,
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
        root_cause_confidence=0.95,
        root_cause_summary=f"{root_cause_code} detected",
        evidence_summary=None,
        recurrence_summary=None,
        operator_summary=None,
        closure_summary=None,
        recommended_next_action="review analytics",
        source_note_ids=[],
        source_evidence_ids=[],
        metadata={"validator": "phase34a"},
    )
    return updated


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase34a-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "incident-analytics-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            resolved_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="resolved",
                severity="medium",
                root_cause_code="provider_failure",
                repeat_count=1,
                reopened_from_case_id=None,
                status="resolved",
                opened_at=now - timedelta(hours=18),
                acknowledged_at=now - timedelta(hours=17),
                resolved_at=now - timedelta(hours=5),
                current_assignee="alice",
                current_team="research",
            )
            reopened_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="reopened",
                severity="high",
                root_cause_code="version_regression",
                repeat_count=3,
                reopened_from_case_id=str(resolved_case["id"]),
                status="acknowledged",
                opened_at=now - timedelta(hours=10),
                acknowledged_at=now - timedelta(hours=8),
                resolved_at=None,
                current_assignee="bob",
                current_team="platform",
            )
            stale_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="stale",
                severity="critical",
                root_cause_code="version_regression",
                repeat_count=2,
                reopened_from_case_id=None,
                status="open",
                opened_at=now - timedelta(hours=30),
                acknowledged_at=None,
                resolved_at=None,
                current_assignee="carol",
                current_team="platform",
            )
            repo.upsert_governance_escalation_state(
                conn,
                workspace_id=workspace_id,
                case_id=str(reopened_case["id"]),
                escalation_level="lead",
                status="active",
                escalated_to_team="platform",
                escalated_to_user="lead-platform",
                reason="validator_escalated",
                source_policy_id=None,
                escalated_at=now - timedelta(hours=7),
                last_evaluated_at=now - timedelta(hours=1),
                repeated_count=0,
                cleared_at=None,
                metadata={"validator": "phase34a"},
            )
            repo.upsert_governance_escalation_state(
                conn,
                workspace_id=workspace_id,
                case_id=str(resolved_case["id"]),
                escalation_level="lead",
                status="active",
                escalated_to_team="research",
                escalated_to_user="lead-research",
                reason="validator_escalated_resolved",
                source_policy_id=None,
                escalated_at=now - timedelta(hours=16),
                last_evaluated_at=now - timedelta(hours=6),
                repeated_count=0,
                cleared_at=None,
                metadata={"validator": "phase34a"},
            )

            service = IncidentAnalyticsService(repo)
            result = service.refresh_workspace_snapshot(conn, workspace_id=workspace_id)
            summary = repo.get_governance_incident_analytics_summary(conn, workspace_id=workspace_id)
            root_causes = repo.list_governance_root_cause_trend_summary(conn, workspace_id=workspace_id)
            recurrence = repo.list_governance_recurrence_burden_summary(conn, workspace_id=workspace_id)
            escalation = repo.get_governance_escalation_effectiveness_summary(conn, workspace_id=workspace_id)
            conn.commit()

        print(f"workspace_id={result.workspace_id}")
        print(f"snapshot_date={result.snapshot_date}")
        print(f"open_case_count={result.open_case_count}")
        print(f"resolved_case_count={result.resolved_case_count}")
        print(f"recurring_case_count={result.recurring_case_count}")
        print(f"escalated_case_count={result.escalated_case_count}")
        print(f"root_cause_rows={len(root_causes)}")
        print(f"recurrence_rows={len(recurrence)}")
        print(f"escalation_resolution_rate={escalation.get('escalation_resolution_rate') if escalation else None}")
        print(f"detail_contract_ok={str(bool(summary and escalation)).lower()}")
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()

