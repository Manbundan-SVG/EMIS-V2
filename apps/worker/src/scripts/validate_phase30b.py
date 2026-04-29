from __future__ import annotations

from datetime import datetime, timezone

from src.db.client import get_connection
from src.db.repositories import (
    create_case_recurrence_link,
    find_recent_related_case,
    list_related_cases,
    persist_governance_degradation_state,
    resolve_governance_degradation_state,
    resolve_governance_case_for_state,
    update_case_recurrence,
    upsert_governance_case,
)
from src.services.case_management_service import build_case_seed
from src.services.case_recurrence_service import CaseRecurrenceService


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


def _create_case(
    conn,
    *,
    workspace_id: str,
    watchlist_id: str,
    degradation_type: str,
    version_tuple: str,
    regime: str,
    severity: str,
    summary: str,
) -> tuple[dict, dict]:
    now = datetime.now(timezone.utc)
    state = persist_governance_degradation_state(
        conn,
        {
            "workspace_id": workspace_id,
            "watchlist_id": watchlist_id,
            "degradation_type": degradation_type,
            "version_tuple": version_tuple,
            "regime": regime,
            "state_status": "escalated",
            "severity": severity,
            "first_seen_at": now,
            "last_seen_at": now,
            "event_count": 5,
            "cluster_count": 2,
            "source_summary": {"message": summary},
            "metadata": {"regime": regime, "validator": "phase30b"},
        },
    )
    case = upsert_governance_case(
        conn,
        build_case_seed(
            workspace_id=workspace_id,
            degradation_state_id=str(state["id"]),
            watchlist_id=watchlist_id,
            version_tuple=version_tuple,
            degradation_type=degradation_type,
            severity=severity,
            source_summary=dict(state.get("source_summary") or {}),
        ).__dict__,
    )
    return state, case


def main() -> None:
    service = CaseRecurrenceService(reopen_window_days=7)
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase30b-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "recurrence-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            first_state, first_case = _create_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="version_regression",
                version_tuple="compute-d|signals-d|model-d",
                regime="macro_dominant",
                severity="high",
                summary="phase30b first case",
            )
            resolved_first = resolve_governance_case_for_state(
                conn,
                degradation_state_id=str(first_state["id"]),
                resolution_note="phase30b quiet-window recovery",
                metadata={"validator": "phase30b"},
            )
            resolve_governance_degradation_state(
                conn,
                state_id=str(first_state["id"]),
                resolution_summary={
                    "recovery_reason": "phase30b quiet-window recovery",
                    "trailing_metrics": {"validator": "phase30b"},
                },
                resolved_at=now,
            )

            second_state, second_case = _create_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="version_regression",
                version_tuple="compute-d|signals-d|model-d",
                regime="macro_dominant",
                severity="critical",
                summary="phase30b reopened case",
            )

            match_basis = service.compute_match_basis(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_family="version_regression",
                version_tuple="compute-d|signals-d|model-d",
                regime="macro_dominant",
                cluster_count=2,
            )
            prior_case = find_recent_related_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_family="version_regression",
                version_tuple="compute-d|signals-d|model-d",
                regime="macro_dominant",
            )
            recurrence = service.build_result(prior_case=prior_case, match_basis=match_basis)
            if not recurrence.matched_case_id or not recurrence.reopen:
                raise RuntimeError("expected resolved first case to match as reopen candidate")

            if prior_case and not prior_case.get("recurrence_group_id"):
                update_case_recurrence(
                    conn,
                    case_id=str(prior_case["id"]),
                    recurrence_group_id=recurrence.recurrence_group_id,
                    reopened_from_case_id=prior_case.get("reopened_from_case_id"),
                    repeat_count=int(prior_case.get("repeat_count") or 1),
                    reopened_at=prior_case.get("reopened_at"),
                    reopen_reason=prior_case.get("reopen_reason"),
                    recurrence_match_basis=dict(prior_case.get("recurrence_match_basis") or {}),
                )

            reopened_case = update_case_recurrence(
                conn,
                case_id=str(second_case["id"]),
                recurrence_group_id=recurrence.recurrence_group_id,
                reopened_from_case_id=recurrence.matched_case_id,
                repeat_count=recurrence.repeat_count,
                reopened_at=second_state["last_seen_at"],
                reopen_reason=recurrence.reopen_reason,
                recurrence_match_basis=recurrence.match_basis,
            )
            create_case_recurrence_link(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                recurrence_group_id=recurrence.recurrence_group_id,
                source_case_id=str(reopened_case["id"]),
                matched_case_id=recurrence.matched_case_id,
                match_type="reopen",
                match_score=1.0,
                matched_within_window=True,
                match_basis=recurrence.match_basis,
            )

            _, unrelated_case = _create_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_type="family_instability_spike",
                version_tuple="compute-z|signals-z|model-z",
                regime="trend_persistence",
                severity="medium",
                summary="phase30b unrelated case",
            )
            false_link = find_recent_related_case(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                degradation_family="family_instability_spike",
                version_tuple="compute-z|signals-z|model-z",
                regime="trend_persistence",
            )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    select *
                    from public.governance_case_recurrence_summary
                    where case_id = %s::uuid
                    """,
                    (str(reopened_case["id"]),),
                )
                summary = dict(cur.fetchone())
            related = list_related_cases(
                conn,
                recurrence_group_id=str(summary["recurrence_group_id"]),
                exclude_case_id=str(reopened_case["id"]),
            )
            conn.commit()

        if reopened_case["reopened_from_case_id"] != resolved_first["id"]:
            raise RuntimeError("expected reopened_from_case_id to link to first case")
        if int(reopened_case["repeat_count"]) != 2:
            raise RuntimeError("expected repeat_count to increment to 2")
        if summary["prior_related_case_count"] < 1:
            raise RuntimeError("expected related case summary count")
        if len(related) < 1:
            raise RuntimeError("expected related cases to list the prior case")
        false_link_count = 0
        if false_link and str(false_link["id"]) == str(unrelated_case["id"]):
            false_link_count = 0
        elif false_link:
            false_link_count = 1
        if false_link_count != 0:
            raise RuntimeError("unexpected false recurrence linkage detected")

        print(
            "phase30b smoke ok "
            f"first_case_id={first_case['id']} "
            f"reopened_case_id={reopened_case['id']} "
            f"recurrence_group_id={summary['recurrence_group_id']} "
            f"repeat_count={reopened_case['repeat_count']} "
            f"related_case_count={len(related)} "
            f"false_link_count={false_link_count}"
        )
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
