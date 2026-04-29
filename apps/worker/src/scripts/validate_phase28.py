from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db.client import get_connection
from src.db.repositories import (
    insert_governance_acknowledgment,
    insert_governance_resolution_action,
    persist_governance_degradation_state,
    upsert_governance_muting_rule,
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
    workspace_slug = f"phase28-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "lifecycle-core"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            state_row = persist_governance_degradation_state(
                conn,
                {
                    "workspace_id": workspace_id,
                    "watchlist_id": watchlist_id,
                    "degradation_type": "family_instability_spike",
                    "version_tuple": "compute-a|signals-a|model-a",
                    "regime": "macro_dominant",
                    "state_status": "active",
                    "severity": "high",
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "event_count": 3,
                    "cluster_count": 1,
                    "source_summary": {"message": "phase28 validation state"},
                    "metadata": {"validator": "phase28"},
                },
            )
            ack_row = insert_governance_acknowledgment(
                conn,
                {
                    "workspace_id": workspace_id,
                    "degradation_state_id": str(state_row["id"]),
                    "acknowledged_by": "validator",
                    "note": "acknowledged for lifecycle smoke",
                    "metadata": {"validator": "phase28"},
                },
            )
            mute_row = upsert_governance_muting_rule(
                conn,
                {
                    "workspace_id": workspace_id,
                    "target_type": "degradation_state",
                    "target_key": str(state_row["id"]),
                    "reason": "temporary suppression",
                    "muted_until": now + timedelta(hours=2),
                    "created_by": "validator",
                    "is_active": True,
                    "metadata": {"validator": "phase28"},
                },
            )
            resolution_row = insert_governance_resolution_action(
                conn,
                {
                    "workspace_id": workspace_id,
                    "degradation_state_id": str(state_row["id"]),
                    "action_type": "acknowledged",
                    "performed_by": "validator",
                    "note": "manual lifecycle test",
                    "metadata": {"validator": "phase28"},
                },
            )

            with conn.cursor() as cur:
                cur.execute(
                    """
                    select *
                    from public.governance_lifecycle_summary
                    where degradation_state_id = %s::uuid
                    """,
                    (str(state_row["id"]),),
                )
                summary = dict(cur.fetchone())
            conn.commit()

        if summary["acknowledged_by"] != "validator":
            raise RuntimeError("acknowledgment did not persist into lifecycle summary")
        if summary["mute_target_type"] != "degradation_state":
            raise RuntimeError("muting rule did not persist into lifecycle summary")
        if summary["last_resolution_action"] != "acknowledged":
            raise RuntimeError("resolution action did not persist into lifecycle summary")

        print(
            "phase28 smoke ok "
            f"workspace_slug={workspace_slug} "
            f"ack_id={ack_row['id']} "
            f"mute_id={mute_row['id']} "
            f"resolution_id={resolution_row['id']}"
        )
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
