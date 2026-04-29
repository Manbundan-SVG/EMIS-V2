from __future__ import annotations

from datetime import datetime, timezone

from src.db.client import get_connection
from src.db.repositories import (
    append_governance_case_evidence,
    append_governance_case_note,
    get_case_recurrence_summary,
    get_governance_case_lifecycle_row,
    get_governance_case_summary_latest,
    get_governance_case_summary_row,
    list_governance_case_evidence,
    list_governance_case_notes,
    persist_governance_degradation_state,
    resolve_governance_case_for_state,
    resolve_governance_degradation_state,
    upsert_governance_case,
    upsert_governance_case_summary,
    update_case_recurrence,
)
from src.services.case_management_service import build_case_seed
from src.services.case_summary_service import CaseSummaryService


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


def _refresh_case_summary(conn, *, case_id: str, service: CaseSummaryService) -> dict:
    case_row = get_governance_case_summary_row(conn, case_id=case_id)
    if not case_row:
        raise RuntimeError("case row missing for summary refresh")
    notes = list_governance_case_notes(conn, case_id=case_id)
    evidence = list_governance_case_evidence(conn, case_id=case_id)
    recurrence = get_case_recurrence_summary(conn, case_id=case_id)
    lifecycle = get_governance_case_lifecycle_row(
        conn,
        degradation_state_id=str(case_row["degradation_state_id"]) if case_row.get("degradation_state_id") else None,
    )
    summary = service.build_summary(
        case_row=case_row,
        notes=notes,
        evidence=evidence,
        recurrence=recurrence,
        lifecycle=lifecycle,
    )
    return upsert_governance_case_summary(
        conn,
        workspace_id=str(case_row["workspace_id"]),
        case_id=case_id,
        summary_version="v1",
        status_summary=summary.status_summary,
        root_cause_code=summary.root_cause_code,
        root_cause_confidence=summary.root_cause_confidence,
        root_cause_summary=summary.root_cause_summary,
        evidence_summary=summary.evidence_summary,
        recurrence_summary=summary.recurrence_summary,
        operator_summary=summary.operator_summary,
        closure_summary=summary.closure_summary,
        recommended_next_action=summary.recommended_next_action,
        source_note_ids=summary.source_note_ids,
        source_evidence_ids=summary.source_evidence_ids,
        metadata=summary.metadata,
    )


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase30d-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "summary-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)
    summary_service = CaseSummaryService()

    try:
        with get_connection() as conn:
            state_row = persist_governance_degradation_state(
                conn,
                {
                    "workspace_id": workspace_id,
                    "watchlist_id": watchlist_id,
                    "degradation_type": "version_regression",
                    "version_tuple": "compute-d|signals-d|model-d",
                    "regime": "macro_dominant",
                    "state_status": "escalated",
                    "severity": "high",
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "event_count": 6,
                    "cluster_count": 2,
                    "source_summary": {"message": "phase30d validation state"},
                    "metadata": {"validator": "phase30d"},
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

            update_case_recurrence(
                conn,
                case_id=str(case_row["id"]),
                recurrence_group_id=str(case_row["id"]),
                reopened_from_case_id=None,
                repeat_count=2,
                reopened_at=now,
                reopen_reason="matched_recent_case_within_window",
                recurrence_match_basis={"window_days": 7, "version_tuple": str(state_row["version_tuple"])},
            )

            append_governance_case_note(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                note="Investigation points to a canary version rollout that increased family instability.",
                author="validator",
                note_type="investigation",
                visibility="internal",
                metadata={"validator": "phase30d"},
            )
            append_governance_case_note(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                note="Handoff to platform with request to compare against last stable version tuple.",
                author="validator",
                note_type="handoff",
                visibility="internal",
                metadata={"validator": "phase30d"},
            )
            append_governance_case_note(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                note="Root cause is version regression in the canary signal registry.",
                author="validator",
                note_type="root_cause",
                visibility="internal",
                metadata={"validator": "phase30d"},
            )

            append_governance_case_evidence(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                evidence_type="version_tuple",
                reference_id=str(state_row["version_tuple"]),
                title="Canary version tuple",
                summary="Version tuple tied to the degraded case and suspected rollout regression.",
                payload={"validator": "phase30d"},
            )
            append_governance_case_evidence(
                conn,
                case_id=str(case_row["id"]),
                workspace_id=workspace_id,
                evidence_type="run",
                reference_id="run-phase30d-001",
                title="Latest unstable run",
                summary="Run exhibiting the version-driven instability spike.",
                payload={"validator": "phase30d"},
            )

            _refresh_case_summary(conn, case_id=str(case_row["id"]), service=summary_service)

            resolve_governance_degradation_state(
                conn,
                state_id=str(state_row["id"]),
                resolution_summary={"validator": "phase30d", "reason": "rolled back canary tuple"},
                resolved_at=now,
            )
            resolved_case = resolve_governance_case_for_state(
                conn,
                degradation_state_id=str(state_row["id"]),
                resolution_note="Rolled back canary tuple and stability normalized.",
                metadata={"validator": "phase30d"},
            )
            if not resolved_case:
                raise RuntimeError("expected resolved case row")

            append_governance_case_note(
                conn,
                case_id=str(resolved_case["id"]),
                workspace_id=workspace_id,
                note="Rolled back canary tuple and stability normalized.",
                author="validator",
                note_type="closure",
                visibility="internal",
                metadata={"validator": "phase30d"},
            )

            final_summary = _refresh_case_summary(conn, case_id=str(resolved_case["id"]), service=summary_service)
            latest_summary = get_governance_case_summary_latest(conn, case_id=str(resolved_case["id"]))
            conn.commit()

        if not latest_summary:
            raise RuntimeError("expected persisted summary row")
        if latest_summary["root_cause_code"] != "version_regression":
            raise RuntimeError(f"unexpected root_cause_code={latest_summary['root_cause_code']!r}")
        if not latest_summary["closure_summary"]:
            raise RuntimeError("expected closure_summary to be populated")
        if str(latest_summary["operator_summary"] or "").lower().find("handoff") == -1:
            raise RuntimeError("expected operator_summary to reflect latest operator note")
        if len(list(latest_summary["source_note_ids"])) < 4:
            raise RuntimeError("expected source_note_ids to include all note rows")
        if len(list(latest_summary["source_evidence_ids"])) != 2:
            raise RuntimeError("expected source_evidence_ids to include both evidence rows")

        print(
            "phase30d smoke ok "
            f"workspace_slug={workspace_slug} "
            f"case_id={case_row['id']} "
            "summary_persisted=true "
            f"root_cause_code={latest_summary['root_cause_code']} "
            "detail_contract_ok=true"
        )
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
