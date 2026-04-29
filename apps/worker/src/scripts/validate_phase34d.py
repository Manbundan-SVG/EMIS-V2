from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db import repositories as repo
from src.db.client import get_connection
from src.services.case_management_service import build_case_seed
from src.services.manager_analytics_service import ManagerAnalyticsService


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
    status: str,
    opened_at: datetime,
    acknowledged_at: datetime | None = None,
    resolved_at: datetime | None = None,
    repeat_count: int = 1,
    reopened_from_case_id: str | None = None,
    assignments: list[tuple[str | None, str | None, str | None]] | None = None,
    escalated: bool = False,
) -> dict:
    case_row = repo.upsert_governance_case(
        conn,
        build_case_seed(
            workspace_id=workspace_id,
            degradation_state_id=None,
            watchlist_id=watchlist_id,
            version_tuple="compute-m1|signals-m1|model-m1",
            degradation_type=root_cause_code,
            severity=severity,
            source_summary={"message": f"phase34d {title_suffix}"},
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
                metadata={"validator": "phase34d"},
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
        root_cause_confidence=0.92,
        root_cause_summary=f"{root_cause_code} detected",
        evidence_summary=None,
        recurrence_summary=None,
        operator_summary=None,
        closure_summary=None,
        recommended_next_action="review manager overview",
        source_note_ids=[],
        source_evidence_ids=[],
        metadata={"validator": "phase34d"},
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
            metadata={"validator": "phase34d"},
        )

    return updated


def _seed_promotion_impact(conn, *, workspace_id: str, now: datetime) -> None:
    repo.upsert_promotion_impact_snapshot(
        conn,
        payload={
            "workspace_id": workspace_id,
            "promotion_type": "threshold",
            "execution_id": "11111111-1111-4111-8111-111111111111",
            "scope_type": "regime",
            "scope_value": "risk_off",
            "impact_classification": "improved",
            "pre_window_start": (now - timedelta(days=14)).isoformat(),
            "pre_window_end": (now - timedelta(days=7)).isoformat(),
            "post_window_start": (now - timedelta(days=7)).isoformat(),
            "post_window_end": now.isoformat(),
            "recurrence_rate_before": 0.40,
            "recurrence_rate_after": 0.10,
            "escalation_rate_before": 0.30,
            "escalation_rate_after": 0.10,
            "resolution_latency_before_ms": 8 * 3600000,
            "resolution_latency_after_ms": 3 * 3600000,
            "reassignment_rate_before": None,
            "reassignment_rate_after": None,
            "rollback_risk_score": 0.05,
            "supporting_metrics": {"validator": "phase34d", "promotion": "threshold"},
        },
    )
    repo.upsert_promotion_impact_snapshot(
        conn,
        payload={
            "workspace_id": workspace_id,
            "promotion_type": "routing",
            "execution_id": "22222222-2222-4222-8222-222222222222",
            "scope_type": "root_cause",
            "scope_value": "version_regression",
            "impact_classification": "rollback_candidate",
            "pre_window_start": (now - timedelta(days=14)).isoformat(),
            "pre_window_end": (now - timedelta(days=7)).isoformat(),
            "post_window_start": (now - timedelta(days=7)).isoformat(),
            "post_window_end": now.isoformat(),
            "recurrence_rate_before": 0.10,
            "recurrence_rate_after": 0.35,
            "escalation_rate_before": 0.15,
            "escalation_rate_after": 0.40,
            "resolution_latency_before_ms": 2 * 3600000,
            "resolution_latency_after_ms": 7 * 3600000,
            "reassignment_rate_before": 0.05,
            "reassignment_rate_after": 0.25,
            "rollback_risk_score": 0.92,
            "supporting_metrics": {"validator": "phase34d", "promotion": "routing"},
        },
    )


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase34d-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "manager-overview-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            resolved_case = _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="resolved-alice",
                severity="medium",
                root_cause_code="provider_failure",
                status="resolved",
                opened_at=now - timedelta(hours=28),
                acknowledged_at=now - timedelta(hours=27),
                resolved_at=now - timedelta(hours=10),
                assignments=[("alice", "research", "validator")],
                escalated=False,
            )
            _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="reopened-bob",
                severity="high",
                root_cause_code="version_regression",
                status="acknowledged",
                opened_at=now - timedelta(hours=18),
                acknowledged_at=now - timedelta(hours=16),
                resolved_at=None,
                repeat_count=3,
                reopened_from_case_id=str(resolved_case["id"]),
                assignments=[("bob", "platform", "validator")],
                escalated=True,
            )
            _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="stale-carol",
                severity="critical",
                root_cause_code="regime_conflict",
                status="open",
                opened_at=now - timedelta(hours=36),
                acknowledged_at=None,
                resolved_at=None,
                repeat_count=2,
                assignments=[("carol", "platform", "validator")],
                escalated=True,
            )
            _seed_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                title_suffix="active-alice",
                severity="medium",
                root_cause_code="provider_failure",
                status="in_progress",
                opened_at=now - timedelta(hours=8),
                acknowledged_at=now - timedelta(hours=7),
                resolved_at=None,
                assignments=[("alice", "research", "validator")],
                escalated=False,
            )

            _seed_promotion_impact(conn, workspace_id=workspace_id, now=now)

            service = ManagerAnalyticsService(repo)
            refresh = service.refresh_workspace_snapshot(conn, workspace_id=workspace_id)
            manager_overview = repo.list_governance_manager_overview_summary(conn, workspace_id=workspace_id)
            chronic_watchlists = repo.list_governance_chronic_watchlist_summary(conn, workspace_id=workspace_id)
            operator_team_comparison = repo.list_governance_operator_team_comparison_summary(conn, workspace_id=workspace_id)
            promotion_health = repo.list_governance_promotion_health_overview(conn, workspace_id=workspace_id)
            operating_risk = repo.list_governance_operating_risk_summary(conn, workspace_id=workspace_id)
            conn.commit()

        print(f"workspace_id={refresh.workspace_id}")
        print(f"manager_overview_rows={len(manager_overview)}")
        print(f"chronic_watchlist_rows={len(chronic_watchlists)}")
        print(f"operator_team_comparison_rows={len(operator_team_comparison)}")
        print(f"promotion_health_rows={len(promotion_health)}")
        print(f"operating_risk_rows={len(operating_risk)}")
        print(f"detail_contract_ok={str(bool(manager_overview and chronic_watchlists and operator_team_comparison and promotion_health and operating_risk)).lower()}")
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
