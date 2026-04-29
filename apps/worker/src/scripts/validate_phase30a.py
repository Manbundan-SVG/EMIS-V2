from __future__ import annotations

from datetime import datetime, timezone

from src.db.client import get_connection
from src.db.repositories import (
    append_governance_case_evidence,
    append_governance_case_event,
    append_governance_incident_timeline_event,
    create_governance_assignment,
    persist_governance_degradation_state,
    resolve_governance_case_for_state,
    upsert_governance_case,
)
from src.services.case_management_service import build_case_seed
from src.services.incident_timeline_service import (
    build_assignment_event,
    build_case_opened_event,
    build_case_resolved_event,
)


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
    workspace_slug = f"phase30a-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "incident-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            state_row = persist_governance_degradation_state(
                conn,
                {
                    "workspace_id": workspace_id,
                    "watchlist_id": watchlist_id,
                    "degradation_type": "version_regression",
                    "version_tuple": "compute-c|signals-c|model-c",
                    "regime": "macro_dominant",
                    "state_status": "escalated",
                    "severity": "critical",
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "event_count": 6,
                    "cluster_count": 2,
                    "source_summary": {"message": "phase30a validation state"},
                    "metadata": {"validator": "phase30a"},
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

            append_governance_case_evidence(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                evidence_type="run",
                reference_id="validator-run-1",
                payload={"validator": "phase30a"},
            )
            append_governance_case_evidence(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                evidence_type="anomaly_cluster",
                reference_id="validator-cluster-1",
                payload={"validator": "phase30a"},
            )

            opened = build_case_opened_event(
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                severity=str(case_row["severity"]),
                summary=str(case_row["summary"]),
                degradation_state_id=str(state_row["id"]),
            )
            append_governance_case_event(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                event_type="case_opened",
                actor="validator",
                payload={"validator": "phase30a"},
            )
            append_governance_incident_timeline_event(
                conn,
                case_id=opened.case_id,
                workspace_id=opened.workspace_id,
                event_type=opened.event_type,
                event_source=opened.event_source,
                title=opened.title,
                detail=opened.detail,
                actor="validator",
                event_at=opened.event_at,
                metadata=opened.metadata,
                source_table=opened.source_table,
                source_id=opened.source_id,
            )

            assignment_row = create_governance_assignment(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                assigned_to="ops.user",
                assigned_team="platform",
                assigned_by="validator",
                reason="phase30a routing",
                metadata={"validator": "phase30a"},
            )
            assignment_event = build_assignment_event(
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                assigned_to="ops.user",
                assigned_team="platform",
                reason="phase30a routing",
                actor="validator",
                assignment_id=str(assignment_row["id"]),
            )
            append_governance_case_event(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                event_type="assignment_changed",
                actor="validator",
                payload={"assigned_to": "ops.user", "assigned_team": "platform"},
            )
            append_governance_incident_timeline_event(
                conn,
                case_id=assignment_event.case_id,
                workspace_id=assignment_event.workspace_id,
                event_type=assignment_event.event_type,
                event_source=assignment_event.event_source,
                title=assignment_event.title,
                detail=assignment_event.detail,
                actor=assignment_event.actor,
                event_at=assignment_event.event_at,
                metadata=assignment_event.metadata,
                source_table=assignment_event.source_table,
                source_id=assignment_event.source_id,
            )

            resolved_case = resolve_governance_case_for_state(
                conn,
                degradation_state_id=str(state_row["id"]),
                resolution_note="validator quiet-window recovery",
                metadata={"validator": "phase30a"},
            )
            resolved = build_case_resolved_event(
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                actor="validator",
                resolution_note="validator quiet-window recovery",
                recovery_reason="quiet_window_normalized",
                action_id=None,
                recovery_event_id=None,
            )
            append_governance_case_event(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                event_type="case_resolved",
                actor="validator",
                payload={"resolution_note": "validator quiet-window recovery"},
            )
            append_governance_incident_timeline_event(
                conn,
                case_id=resolved.case_id,
                workspace_id=resolved.workspace_id,
                event_type=resolved.event_type,
                event_source=resolved.event_source,
                title=resolved.title,
                detail=resolved.detail,
                actor=resolved.actor,
                event_at=resolved.event_at,
                metadata=resolved.metadata,
                source_table=resolved.source_table,
                source_id=resolved.source_id,
            )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    select *
                    from public.governance_incident_detail
                    where case_id = %s::uuid
                    """,
                    (str(case_row["id"]),),
                )
                detail = dict(cur.fetchone())
                cur.execute(
                    """
                    select *
                    from public.governance_incident_timeline_events
                    where case_id = %s::uuid
                    order by event_at asc, id asc
                    """,
                    (str(case_row["id"]),),
                )
                timeline_rows = [dict(row) for row in cur.fetchall()]
            conn.commit()

        if detail["timeline_event_count"] < 3:
            raise RuntimeError("expected at least three incident timeline events")
        if detail["evidence_count"] < 2:
            raise RuntimeError("expected linked evidence to be summarized")
        if timeline_rows[-1]["event_type"] != "case_resolved":
            raise RuntimeError("expected deterministic final timeline event ordering")
        if not resolved_case or resolved_case["status"] != "resolved":
            raise RuntimeError("expected resolved governance case")

        print(
            "phase30a smoke ok "
            f"workspace_slug={workspace_slug} "
            f"case_id={case_row['id']} "
            f"timeline_events={len(timeline_rows)} "
            f"evidence_count={detail['evidence_count']}"
        )
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
