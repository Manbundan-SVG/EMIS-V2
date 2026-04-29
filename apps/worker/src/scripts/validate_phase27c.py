from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db.client import get_connection
from src.db.repositories import (
    get_active_governance_degradation_state,
    get_governance_degradation_states,
    insert_governance_alert_events,
    insert_governance_degradation_state_members,
    insert_governance_recovery_event,
    persist_governance_degradation_state,
    resolve_governance_degradation_state,
    upsert_governance_anomaly_clusters,
)
from src.services.anomaly_clustering_service import build_cluster_candidates
from src.services.governance_degradation_service import (
    GovernanceDegradationService,
    build_degradation_signals,
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
    workspace_slug = f"phase27c-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "chronic-core"
    service = GovernanceDegradationService()
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            state_id: str | None = None
            member_total = 0

            def apply_iteration(iteration: int) -> dict:
                nonlocal state_id
                inserted_events = insert_governance_alert_events(
                    conn,
                    [
                        {
                            "workspace_id": workspace_id,
                            "watchlist_id": watchlist_id,
                            "run_id": None,
                            "rule_name": "validate_family_instability_spike",
                            "event_type": "family_instability_spike",
                            "severity": "high",
                            "dedupe_key": f"phase27c:{iteration}",
                            "metric_source": "latest_stability_summary",
                            "metric_name": "family_instability_score",
                            "metric_value_numeric": 0.91,
                            "metric_value_text": None,
                            "threshold_numeric": 0.62,
                            "threshold_text": None,
                            "compute_version": "compute-stable",
                            "signal_registry_version": "signals-canary",
                            "model_version": "model-alpha",
                            "metadata": {
                                "message": "family instability remained elevated",
                                "source_row": {
                                    "watchlist_id": watchlist_id,
                                    "dominant_regime": "macro_dominant",
                                },
                            },
                        }
                    ],
                )
                cluster_rows = upsert_governance_anomaly_clusters(
                    conn,
                    [candidate.__dict__ for candidate in build_cluster_candidates(inserted_events)],
                )
                signals = build_degradation_signals(inserted_events, cluster_rows)
                if len(signals) != 1:
                    raise RuntimeError(f"expected one degradation signal, got {len(signals)}")
                signal = signals[0]
                active_state = get_active_governance_degradation_state(
                    conn,
                    workspace_id=signal.workspace_id,
                    watchlist_id=signal.watchlist_id,
                    degradation_type=signal.degradation_type,
                    version_tuple=signal.version_tuple,
                    regime=signal.regime,
                )
                decision = service.evaluate_signal(signal, active_state)
                if decision is None:
                    return {}
                state_row = persist_governance_degradation_state(conn, decision)
                state_id = str(state_row["id"])
                insert_governance_degradation_state_members(
                    conn,
                    [member.to_row(state_id=state_id, workspace_id=workspace_id) for member in signal.members],
                )
                return state_row

            state_row = {}
            for iteration in range(1, 6):
                state_row = apply_iteration(iteration)

            if not state_row:
                raise RuntimeError("expected degradation state to be created by iteration 3")
            if state_row["state_status"] != "escalated":
                raise RuntimeError(f"expected escalated state after five signals, got {state_row['state_status']!r}")
            if int(state_row["event_count"]) < 5:
                raise RuntimeError(f"expected event_count >= 5, got {state_row['event_count']}")

            with conn.cursor() as cur:
                cur.execute(
                    """
                    select count(*)::integer as count
                    from public.governance_degradation_state_members
                    where state_id = %s::uuid
                    """,
                    (state_id,),
                )
                member_total = int(cur.fetchone()["count"])

            if member_total < 2:
                raise RuntimeError(f"expected at least 2 state members, got {member_total}")

            duplicate_attempt = insert_governance_degradation_state_members(
                conn,
                [
                    {
                        "state_id": state_id,
                        "workspace_id": workspace_id,
                        "governance_alert_event_id": None,
                        "anomaly_cluster_id": None,
                        "job_run_id": None,
                        "member_type": "manual_probe",
                        "member_key": "manual_probe",
                        "observed_at": datetime.now(timezone.utc),
                        "metadata": {"probe": True},
                    },
                    {
                        "state_id": state_id,
                        "workspace_id": workspace_id,
                        "governance_alert_event_id": None,
                        "anomaly_cluster_id": None,
                        "job_run_id": None,
                        "member_type": "manual_probe",
                        "member_key": "manual_probe",
                        "observed_at": datetime.now(timezone.utc),
                        "metadata": {"probe": True},
                    },
                ],
            )
            if len(duplicate_attempt) != 1:
                raise RuntimeError("expected duplicate member insert to be ignored")

            quiet_seen_at = datetime.now(timezone.utc) - timedelta(hours=13)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update public.governance_degradation_states
                    set last_seen_at = %s::timestamptz
                    where id = %s::uuid
                    """,
                    (quiet_seen_at, state_id),
                )

            active_states = get_governance_degradation_states(
                conn,
                workspace_id,
                watchlist_id=watchlist_id,
                statuses=("active", "escalated"),
                limit=10,
            )
            if len(active_states) != 1:
                raise RuntimeError(f"expected one active degradation state, got {len(active_states)}")

            recovery = service.evaluate_recovery(
                active_states[0],
                trailing_metrics={
                    "latest_stability": {
                        "family_instability_score": 0.24,
                        "stability_classification": "watch",
                    }
                },
            )
            if recovery is None:
                raise RuntimeError("expected recovery decision after quiet window")

            resolved_state = resolve_governance_degradation_state(
                conn,
                state_id=str(active_states[0]["id"]),
                resolution_summary={
                    "recovery_reason": recovery["recovery_reason"],
                    "trailing_metrics": recovery["trailing_metrics"],
                },
                resolved_at=recovery["resolved_at"],
            )
            recovery_row = insert_governance_recovery_event(
                conn,
                {
                    "workspace_id": workspace_id,
                    "state_id": str(active_states[0]["id"]),
                    "watchlist_id": watchlist_id,
                    "degradation_type": active_states[0]["degradation_type"],
                    "version_tuple": active_states[0]["version_tuple"],
                    "regime": active_states[0]["regime"],
                    "recovered_at": recovery["resolved_at"],
                    "recovery_reason": recovery["recovery_reason"],
                    "prior_severity": active_states[0]["severity"],
                    "trailing_metrics": recovery["trailing_metrics"],
                    "metadata": {"validator": "phase27c"},
                },
            )
            conn.commit()

            if resolved_state["state_status"] != "resolved":
                raise RuntimeError(f"expected resolved state, got {resolved_state['state_status']!r}")
            if recovery_row["recovery_reason"] != "quiet_window_normalized":
                raise RuntimeError("unexpected recovery reason")

            with conn.cursor() as cur:
                cur.execute(
                    """
                    select
                      count(*) filter (where state_status = 'resolved')::integer as resolved_count,
                      count(*) filter (where state_status in ('active', 'escalated'))::integer as open_count
                    from public.governance_degradation_summary
                    where workspace_id = %s::uuid
                    """,
                    (workspace_id,),
                )
                summary_row = dict(cur.fetchone())
                cur.execute(
                    """
                    select count(*)::integer as count
                    from public.governance_recovery_event_summary
                    where workspace_id = %s::uuid
                    """,
                    (workspace_id,),
                )
                recovery_count = int(cur.fetchone()["count"])

        if summary_row["resolved_count"] != 1 or summary_row["open_count"] != 0:
            raise RuntimeError(f"unexpected degradation summary counts: {summary_row}")
        if recovery_count != 1:
            raise RuntimeError(f"expected one recovery event, got {recovery_count}")

        print(
            "phase27c smoke ok "
            f"workspace_slug={workspace_slug} "
            f"state_status={state_row['state_status']} "
            f"event_count={state_row['event_count']} "
            f"member_total={member_total} "
            f"recovery_reason={recovery_row['recovery_reason']}"
        )
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
