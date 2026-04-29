from __future__ import annotations

from datetime import datetime, timezone

from src.db.client import get_connection
from src.db.repositories import (
    append_governance_case_event,
    create_governance_assignment,
    persist_governance_degradation_state,
    resolve_governance_case_for_state,
    upsert_governance_case,
)
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


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase29-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "case-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            state_row = persist_governance_degradation_state(
                conn,
                {
                    "workspace_id": workspace_id,
                    "watchlist_id": watchlist_id,
                    "degradation_type": "version_regression",
                    "version_tuple": "compute-b|signals-b|model-b",
                    "regime": "trend_persistence",
                    "state_status": "escalated",
                    "severity": "critical",
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "event_count": 5,
                    "cluster_count": 2,
                    "source_summary": {"message": "phase29 validation state"},
                    "metadata": {"validator": "phase29"},
                },
            )
            case_row = upsert_governance_case(
                conn,
                build_case_seed(
                    workspace_id=workspace_id,
                    degradation_state_id=str(state_row["id"]),
                    watchlist_id=watchlist_id,
                    version_tuple=str(state_row["version_tuple"]),
                    degradation_type=str(state_row["degradation_type"]),
                    severity=str(state_row["severity"]),
                    source_summary=dict(state_row.get("source_summary") or {}),
                ).__dict__,
            )
            append_governance_case_event(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                event_type="case_opened",
                actor="validator",
                payload={"validator": "phase29"},
            )
            assignment_row = create_governance_assignment(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                assigned_to=None,
                assigned_team="platform",
                assigned_by="validator",
                reason="initial routing",
                metadata={"validator": "phase29"},
            )
            append_governance_case_event(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                event_type="assignment_changed",
                actor="validator",
                payload={"assigned_team": "platform"},
            )
            resolved_case = resolve_governance_case_for_state(
                conn,
                degradation_state_id=str(state_row["id"]),
                resolution_note="validator recovered",
                metadata={"validator": "phase29"},
            )
            append_governance_case_event(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                event_type="case_resolved",
                actor="validator",
                payload={"resolution_note": "validator recovered"},
            )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    select *
                    from public.governance_case_summary
                    where id = %s::uuid
                    """,
                    (str(case_row["id"]),),
                )
                summary = dict(cur.fetchone())
            conn.commit()

        if summary["status"] != "resolved":
            raise RuntimeError(f"expected resolved case status, got {summary['status']!r}")
        if summary["event_count"] < 3:
            raise RuntimeError("expected at least three case events")
        if summary["current_team"] != "platform":
            raise RuntimeError("assignment did not persist to case summary")

        print(
            "phase29 smoke ok "
            f"workspace_slug={workspace_slug} "
            f"case_id={case_row['id']} "
            f"assignment_id={assignment_row['id']} "
            f"resolved_case_id={resolved_case['id'] if resolved_case else 'none'}"
        )
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
