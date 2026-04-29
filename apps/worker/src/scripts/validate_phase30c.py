from __future__ import annotations

from datetime import datetime, timezone

from src.db.client import get_connection
from src.db.repositories import (
    append_governance_case_evidence,
    append_governance_case_note,
    create_governance_assignment,
    get_governance_case_evidence_summary,
    list_governance_case_evidence,
    list_governance_case_notes,
    persist_governance_degradation_state,
    resolve_governance_case_for_state,
    resolve_governance_degradation_state,
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
    workspace_slug = f"phase30c-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "collab-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            state_row = persist_governance_degradation_state(
                conn,
                {
                    "workspace_id": workspace_id,
                    "watchlist_id": watchlist_id,
                    "degradation_type": "family_instability_spike",
                    "version_tuple": "compute-c|signals-c|model-c",
                    "regime": "macro_dominant",
                    "state_status": "escalated",
                    "severity": "high",
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "event_count": 4,
                    "cluster_count": 1,
                    "source_summary": {"message": "phase30c validation state"},
                    "metadata": {"validator": "phase30c"},
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

            assignment_row = create_governance_assignment(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                assigned_to="analyst@emis.local",
                assigned_team="platform",
                assigned_by="validator",
                reason="phase30c assignment",
                metadata={"validator": "phase30c"},
            )

            append_governance_case_note(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                note="Initial investigation confirms instability is version-scoped.",
                author="validator",
                note_type="investigation",
                visibility="internal",
                metadata={"validator": "phase30c"},
            )
            append_governance_case_note(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                note="Handing off to platform due to repeated family churn after assignment.",
                author="validator",
                note_type="handoff",
                visibility="internal",
                metadata={"validator": "phase30c", "assignment_id": str(assignment_row["id"])},
            )
            append_governance_case_note(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                note="Root cause points to unstable canary signal-registry tuple.",
                author="validator",
                note_type="root_cause",
                visibility="internal",
                metadata={"validator": "phase30c"},
            )

            append_governance_case_evidence(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                evidence_type="run",
                reference_id="run-phase30c-001",
                title="Latest failing run",
                summary="Run showing the sharpest family instability spike for this case.",
                payload={"validator": "phase30c", "family": "momentum"},
            )
            append_governance_case_evidence(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                evidence_type="threshold_application",
                reference_id="threshold-phase30c-001",
                title="Active threshold application",
                summary="Regime-aware threshold profile applied during the incident window.",
                payload={"validator": "phase30c", "regime": "macro_dominant"},
            )

            resolve_governance_degradation_state(
                conn,
                state_id=str(state_row["id"]),
                resolution_summary={"validator": "phase30c", "reason": "metrics normalized"},
                resolved_at=now,
            )
            resolved_case = resolve_governance_case_for_state(
                conn,
                degradation_state_id=str(state_row["id"]),
                resolution_note="Metrics normalized after canary rollback.",
                metadata={"validator": "phase30c"},
            )
            if not resolved_case:
                raise RuntimeError("expected resolved case row")

            append_governance_case_note(
                conn,
                case_id=str(resolved_case["id"]),
                workspace_id=workspace_id,
                note="Metrics normalized after canary rollback.",
                author="validator",
                note_type="closure",
                visibility="internal",
                metadata={"validator": "phase30c"},
            )

            notes = list_governance_case_notes(conn, case_id=str(case_row["id"]))
            evidence = list_governance_case_evidence(conn, case_id=str(case_row["id"]))
            evidence_summary = get_governance_case_evidence_summary(conn, case_id=str(case_row["id"]))
            conn.commit()

        if len(notes) != 4:
            raise RuntimeError(f"expected 4 notes, got {len(notes)}")
        if len(evidence) != 2:
            raise RuntimeError(f"expected 2 evidence rows, got {len(evidence)}")
        note_types = {str(note["note_type"]) for note in notes}
        if {"investigation", "handoff", "root_cause", "closure"} - note_types:
            raise RuntimeError(f"missing expected note types: {note_types}")
        if not evidence_summary:
            raise RuntimeError("expected governance_case_evidence_summary row")
        if int(evidence_summary["evidence_count"]) != 2:
            raise RuntimeError(f"expected evidence_count=2, got {evidence_summary['evidence_count']}")
        if str(evidence_summary["latest_run_id"]) != "run-phase30c-001":
            raise RuntimeError(f"unexpected latest_run_id={evidence_summary['latest_run_id']!r}")
        if not any(str(note["note_type"]) == "handoff" and str(note["author"]) == "validator" for note in notes):
            raise RuntimeError("handoff note missing or author did not persist")
        if not any(str(note["note_type"]) == "closure" for note in notes):
            raise RuntimeError("closure note did not persist after resolution")

        print(
            "phase30c smoke ok "
            f"workspace_slug={workspace_slug} "
            f"case_id={case_row['id']} "
            f"notes_persisted={len(notes)} "
            f"evidence_persisted={len(evidence)} "
            f"latest_run_id={evidence_summary['latest_run_id']} "
            "detail_contract_ok=true"
        )
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
