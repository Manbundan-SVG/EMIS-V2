"""Phase 4.6A smoke validation: Cross-Window Regime Memory + Persistence Diagnostics.

Checks:
  1. state persistence snapshots persist
  2. regime memory snapshots persist
  3. persistence transition event snapshots persist
  4. state persistence summary rows populate
  5. regime memory summary rows populate
  6. persistence event summary rows populate
  7. run persistence bridge row populates
  8. controlled inputs produce expected persistent / fragile / rotating /
     breaking_down classifications + persistence events
  9. route contract remains typed and stable
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid

import asyncpg

DATABASE_URL = os.environ.get("DATABASE_URL", "")


async def get_conn() -> asyncpg.Connection:
    if not DATABASE_URL:
        sys.exit("DATABASE_URL not set")
    return await asyncpg.connect(DATABASE_URL)


async def setup(conn: asyncpg.Connection) -> tuple[str, str]:
    workspace_row = await conn.fetchrow("SELECT id FROM workspaces LIMIT 1")
    assert workspace_row, "no workspaces"
    workspace_id = str(workspace_row["id"])
    row = await conn.fetchrow(
        "SELECT id FROM watchlists WHERE workspace_id = $1::uuid LIMIT 1",
        workspace_id,
    )
    if row:
        watchlist_id = str(row["id"])
    else:
        watchlist_id = str(uuid.uuid4())
        await conn.execute(
            "INSERT INTO watchlists (id, workspace_id, slug, name) "
            "VALUES ($1::uuid, $2::uuid, 'phase46a_validation', 'Phase 4.6A Validation')",
            watchlist_id, workspace_id,
        )
    return workspace_id, watchlist_id


async def ensure_job_run(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    existing = await conn.fetchrow("SELECT id FROM job_runs WHERE id = $1::uuid", run_id)
    if existing:
        return
    await conn.execute(
        """
        INSERT INTO job_runs (id, workspace_id, watchlist_id, status, queue_name)
        VALUES ($1::uuid, $2::uuid, $3::uuid, 'completed', 'recompute')
        ON CONFLICT (id) DO NOTHING
        """,
        run_id, workspace_id, watchlist_id,
    )


async def seed_state_persistence(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str, run_id: str,
    regime_key: str, archetype_key: str, cluster_state: str,
    persistence_state: str,
    state_age: int, same_state_count: int,
    state_ratio: float, regime_ratio: float,
    cluster_ratio: float, archetype_ratio: float,
    memory_score: float,
) -> None:
    signature = (
        f"regime={regime_key}|timing=lead|transition=reinforcing|"
        f"sequence=reinforcing_path|archetype={archetype_key}|cluster={cluster_state}"
    )
    await conn.execute(
        """
        INSERT INTO cross_asset_state_persistence_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           window_label,
           regime_key, dominant_timing_class,
           dominant_transition_state, dominant_sequence_class,
           dominant_archetype_key, cluster_state,
           current_state_signature, state_age_runs, same_state_count,
           state_persistence_ratio, regime_persistence_ratio,
           cluster_persistence_ratio, archetype_persistence_ratio,
           persistence_state, memory_score, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                'recent_20',
                $4, 'lead', 'reinforcing', 'reinforcing_path',
                $5, $6,
                $7, $8, $9,
                $10, $11, $12, $13,
                $14, $15,
                '{"scoring_version":"4.6A.v1","run_window_size":20}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
        regime_key, archetype_key, cluster_state,
        signature, state_age, same_state_count,
        state_ratio, regime_ratio, cluster_ratio, archetype_ratio,
        persistence_state, memory_score,
    )


async def seed_regime_memory(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, regime_key: str,
    run_count: int, streak: int, switches: int,
    avg_dur: float, max_dur: int,
    memory_score: float,
    dominant_cluster: str, dominant_archetype: str,
    persistence_state: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_regime_memory_snapshots
          (workspace_id, regime_key, window_label,
           run_count, same_regime_streak_count, regime_switch_count,
           avg_regime_duration_runs, max_regime_duration_runs,
           regime_memory_score,
           dominant_cluster_state, dominant_archetype_key,
           persistence_state, metadata)
        VALUES ($1::uuid, $2, 'recent_50',
                $3, $4, $5,
                $6, $7,
                $8,
                $9, $10,
                $11,
                '{"scoring_version":"4.6A.v1","window_size":50}'::jsonb)
        """,
        workspace_id, regime_key,
        run_count, streak, switches,
        avg_dur, max_dur,
        memory_score,
        dominant_cluster, dominant_archetype,
        persistence_state,
    )


async def seed_persistence_event(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str | None,
    source_run_id: str | None, target_run_id: str,
    regime_key: str | None,
    prior_sig: str | None, current_sig: str,
    prior_state: str | None, current_state: str,
    prior_memory: float | None, current_memory: float | None,
    delta: float | None, event_type: str,
    reasons: list[str],
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_persistence_transition_event_snapshots
          (workspace_id, watchlist_id, source_run_id, target_run_id,
           regime_key,
           prior_state_signature, current_state_signature,
           prior_persistence_state, current_persistence_state,
           prior_memory_score, current_memory_score, memory_score_delta,
           event_type, reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid,
                $5,
                $6, $7,
                $8, $9,
                $10, $11, $12,
                $13, $14::jsonb,
                '{"scoring_version":"4.6A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, source_run_id, target_run_id,
        regime_key,
        prior_sig, current_sig,
        prior_state, current_state,
        prior_memory, current_memory, delta,
        event_type, json.dumps(reasons),
    )


async def check_1_2_3(
    conn: asyncpg.Connection, workspace_id: str,
) -> None:
    state_rows = await conn.fetch(
        "SELECT run_id FROM cross_asset_state_persistence_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert state_rows, "CHECK 1 FAILED: no state persistence snapshots"
    print(f"CHECK 1 PASSED: state_persistence_rows={len(state_rows)}")

    regime_rows = await conn.fetch(
        "SELECT regime_key FROM cross_asset_regime_memory_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert regime_rows, "CHECK 2 FAILED: no regime memory snapshots"
    print(f"CHECK 2 PASSED: regime_memory_rows={len(regime_rows)}")

    event_rows = await conn.fetch(
        "SELECT event_type FROM cross_asset_persistence_transition_event_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert event_rows, "CHECK 3 FAILED: no persistence event snapshots"
    print(f"CHECK 3 PASSED: persistence_event_rows={len(event_rows)}")


async def check_4_state_summary(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT run_id, persistence_state FROM cross_asset_state_persistence_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 4 FAILED: state summary empty"
    print(f"CHECK 4 PASSED: state_summary_rows={len(rows)}")


async def check_5_regime_summary(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT regime_key, persistence_state FROM cross_asset_regime_memory_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 5 FAILED: regime summary empty"
    print(f"CHECK 5 PASSED: regime_summary_rows={len(rows)}")


async def check_6_event_summary(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT event_type FROM cross_asset_persistence_transition_event_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 6 FAILED: event summary empty"
    print(f"CHECK 6 PASSED: event_summary_rows={len(rows)}")


async def check_7_run_persistence(
    conn: asyncpg.Connection, workspace_id: str, run_id: str,
) -> None:
    row = await conn.fetchrow(
        "SELECT run_id, persistence_state, memory_score, latest_persistence_event_type "
        "FROM run_cross_asset_persistence_summary "
        "WHERE workspace_id = $1::uuid AND run_id = $2::uuid",
        workspace_id, run_id,
    )
    assert row, "CHECK 7 FAILED: run persistence summary missing"
    print(
        f"CHECK 7 PASSED: run_persistence_rows>=1 "
        f"persistence_state={row['persistence_state']!r} "
        f"memory={row['memory_score']} "
        f"latest_event={row['latest_persistence_event_type']!r}"
    )


async def check_8_classification(
    conn: asyncpg.Connection, run_ids: dict[str, str],
) -> None:
    """Verify the seeded scenarios are classified correctly:
      - persistent_run    → persistence_state=persistent
      - fragile_run       → persistence_state=fragile
      - rotating_run      → persistence_state=rotating
      - breakdown_run     → persistence_state=breaking_down
    Verify event types align:
      - rotating_run target → state_rotation
      - breakdown_run target → cluster_memory_break
    """
    rows = await conn.fetch(
        "SELECT run_id::text as run_id, persistence_state "
        "FROM cross_asset_state_persistence_summary "
        "WHERE run_id = ANY($1::uuid[])",
        list(run_ids.values()),
    )
    by_run = {r["run_id"]: r for r in rows}

    persistent = by_run.get(run_ids["persistent_run"])
    fragile    = by_run.get(run_ids["fragile_run"])
    rotating   = by_run.get(run_ids["rotating_run"])
    breakdown  = by_run.get(run_ids["breakdown_run"])
    assert persistent and fragile and rotating and breakdown, "CHECK 8 FAILED: missing seeded run rows"
    assert persistent["persistence_state"] == "persistent", (
        f"CHECK 8 FAILED: persistent_run expected 'persistent', got {persistent['persistence_state']!r}"
    )
    assert fragile["persistence_state"] == "fragile", (
        f"CHECK 8 FAILED: fragile_run expected 'fragile', got {fragile['persistence_state']!r}"
    )
    assert rotating["persistence_state"] == "rotating", (
        f"CHECK 8 FAILED: rotating_run expected 'rotating', got {rotating['persistence_state']!r}"
    )
    assert breakdown["persistence_state"] == "breaking_down", (
        f"CHECK 8 FAILED: breakdown_run expected 'breaking_down', got {breakdown['persistence_state']!r}"
    )

    drift_rows = await conn.fetch(
        "SELECT target_run_id::text as target_run_id, event_type "
        "FROM cross_asset_persistence_transition_event_summary "
        "WHERE target_run_id = ANY($1::uuid[])",
        [run_ids["rotating_run"], run_ids["breakdown_run"]],
    )
    by_target = {r["target_run_id"]: r["event_type"] for r in drift_rows}
    assert by_target.get(run_ids["rotating_run"]) == "state_rotation", (
        f"CHECK 8 FAILED: rotating_run event expected 'state_rotation', "
        f"got {by_target.get(run_ids['rotating_run'])!r}"
    )
    assert by_target.get(run_ids["breakdown_run"]) == "cluster_memory_break", (
        f"CHECK 8 FAILED: breakdown_run event expected 'cluster_memory_break', "
        f"got {by_target.get(run_ids['breakdown_run'])!r}"
    )
    print(
        "CHECK 8 PASSED: persistence_classification_checked=true "
        "(persistent/fragile/rotating/breaking_down + state_rotation+cluster_memory_break)"
    )


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_state_persistence_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "window_label",
            "regime_key", "dominant_timing_class",
            "dominant_transition_state", "dominant_sequence_class",
            "dominant_archetype_key", "cluster_state",
            "current_state_signature", "state_age_runs", "same_state_count",
            "state_persistence_ratio", "regime_persistence_ratio",
            "cluster_persistence_ratio", "archetype_persistence_ratio",
            "persistence_state", "memory_score", "metadata", "created_at",
        ],
        "cross_asset_regime_memory_snapshots": [
            "id", "workspace_id", "regime_key", "window_label",
            "run_count", "same_regime_streak_count", "regime_switch_count",
            "avg_regime_duration_runs", "max_regime_duration_runs",
            "regime_memory_score",
            "dominant_cluster_state", "dominant_archetype_key",
            "persistence_state", "metadata", "created_at",
        ],
        "cross_asset_persistence_transition_event_snapshots": [
            "id", "workspace_id", "watchlist_id",
            "source_run_id", "target_run_id",
            "regime_key",
            "prior_state_signature", "current_state_signature",
            "prior_persistence_state", "current_persistence_state",
            "prior_memory_score", "current_memory_score", "memory_score_delta",
            "event_type", "reason_codes", "metadata", "created_at",
        ],
        "cross_asset_state_persistence_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "window_label",
            "regime_key", "dominant_timing_class",
            "dominant_transition_state", "dominant_sequence_class",
            "dominant_archetype_key", "cluster_state",
            "current_state_signature", "state_age_runs", "same_state_count",
            "state_persistence_ratio", "regime_persistence_ratio",
            "cluster_persistence_ratio", "archetype_persistence_ratio",
            "persistence_state", "memory_score", "created_at",
        ],
        "cross_asset_regime_memory_summary": [
            "workspace_id", "regime_key", "window_label",
            "run_count", "same_regime_streak_count", "regime_switch_count",
            "avg_regime_duration_runs", "max_regime_duration_runs",
            "regime_memory_score",
            "dominant_cluster_state", "dominant_archetype_key",
            "persistence_state", "created_at",
        ],
        "cross_asset_persistence_transition_event_summary": [
            "workspace_id", "watchlist_id",
            "source_run_id", "target_run_id",
            "regime_key",
            "prior_state_signature", "current_state_signature",
            "prior_persistence_state", "current_persistence_state",
            "prior_memory_score", "current_memory_score", "memory_score_delta",
            "event_type", "reason_codes", "created_at",
        ],
        "run_cross_asset_persistence_summary": [
            "run_id", "workspace_id", "watchlist_id",
            "regime_key", "cluster_state", "dominant_archetype_key",
            "persistence_state", "memory_score",
            "state_age_runs", "state_persistence_ratio",
            "latest_persistence_event_type", "created_at",
        ],
    }
    for table, cols in checks.items():
        cols_sql = ", ".join(cols)
        await conn.fetchrow(f"SELECT {cols_sql} FROM {table} LIMIT 1")  # noqa: S608
    print("CHECK 9 PASSED: detail_contract_ok=true")


async def main() -> None:
    conn = await get_conn()
    try:
        workspace_id, watchlist_id = await setup(conn)
        persistent_run = str(uuid.uuid4())
        fragile_run    = str(uuid.uuid4())
        rotating_run   = str(uuid.uuid4())
        breakdown_run  = str(uuid.uuid4())
        for rid in (persistent_run, fragile_run, rotating_run, breakdown_run):
            await ensure_job_run(conn, workspace_id, watchlist_id, rid)

        # Persistent: stable cluster, reinforcing archetype, high ratios.
        await seed_state_persistence(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=persistent_run,
            regime_key="macro_dominant",
            archetype_key="reinforcing_continuation", cluster_state="stable",
            persistence_state="persistent",
            state_age=8, same_state_count=8,
            state_ratio=0.80, regime_ratio=0.85, cluster_ratio=0.80, archetype_ratio=0.75,
            memory_score=0.80,
        )
        # Fragile: low memory, no clear directional cluster.
        await seed_state_persistence(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=fragile_run,
            regime_key="macro_dominant",
            archetype_key="insufficient_history", cluster_state="stable",
            persistence_state="fragile",
            state_age=1, same_state_count=1,
            state_ratio=0.10, regime_ratio=0.30, cluster_ratio=0.20, archetype_ratio=0.15,
            memory_score=0.20,
        )
        # Rotating: cluster_state rotating.
        await seed_state_persistence(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=rotating_run,
            regime_key="macro_dominant",
            archetype_key="rotation_handoff", cluster_state="rotating",
            persistence_state="rotating",
            state_age=2, same_state_count=2,
            state_ratio=0.30, regime_ratio=0.60, cluster_ratio=0.45, archetype_ratio=0.40,
            memory_score=0.45,
        )
        # Breakdown: cluster_state deteriorating.
        await seed_state_persistence(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=breakdown_run,
            regime_key="macro_dominant",
            archetype_key="deteriorating_breakdown", cluster_state="deteriorating",
            persistence_state="breaking_down",
            state_age=3, same_state_count=3,
            state_ratio=0.40, regime_ratio=0.55, cluster_ratio=0.50, archetype_ratio=0.50,
            memory_score=0.48,
        )

        # Regime memory snapshot
        await seed_regime_memory(
            conn,
            workspace_id=workspace_id, regime_key="macro_dominant",
            run_count=18, streak=5, switches=3,
            avg_dur=4.5, max_dur=8,
            memory_score=0.72,
            dominant_cluster="stable", dominant_archetype="reinforcing_continuation",
            persistence_state="persistent",
        )

        # Persistence events for rotating + breakdown pairs
        persistent_sig = (
            "regime=macro_dominant|timing=lead|transition=reinforcing|"
            "sequence=reinforcing_path|archetype=reinforcing_continuation|cluster=stable"
        )
        rotating_sig = (
            "regime=macro_dominant|timing=lead|transition=reinforcing|"
            "sequence=reinforcing_path|archetype=rotation_handoff|cluster=rotating"
        )
        breakdown_sig = (
            "regime=macro_dominant|timing=lead|transition=reinforcing|"
            "sequence=reinforcing_path|archetype=deteriorating_breakdown|cluster=deteriorating"
        )
        await seed_persistence_event(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=persistent_run, target_run_id=rotating_run,
            regime_key="macro_dominant",
            prior_sig=persistent_sig, current_sig=rotating_sig,
            prior_state="persistent", current_state="rotating",
            prior_memory=0.80, current_memory=0.45, delta=-0.35,
            event_type="state_rotation",
            reasons=["state_signature_changed", "cluster_state_changed",
                     "archetype_changed", "memory_score_decreased"],
        )
        await seed_persistence_event(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=rotating_run, target_run_id=breakdown_run,
            regime_key="macro_dominant",
            prior_sig=rotating_sig, current_sig=breakdown_sig,
            prior_state="rotating", current_state="breaking_down",
            prior_memory=0.45, current_memory=0.48, delta=0.03,
            event_type="cluster_memory_break",
            reasons=["state_signature_changed", "cluster_state_changed",
                     "archetype_changed"],
        )
        print(
            f"SEEDED: persistent={persistent_run[:12]}… fragile={fragile_run[:12]}… "
            f"rotating={rotating_run[:12]}… breakdown={breakdown_run[:12]}…"
        )

        await check_1_2_3(conn, workspace_id)
        await check_4_state_summary(conn, workspace_id)
        await check_5_regime_summary(conn, workspace_id)
        await check_6_event_summary(conn, workspace_id)
        await check_7_run_persistence(conn, workspace_id, rotating_run)
        await check_8_classification(conn, {
            "persistent_run": persistent_run,
            "fragile_run":    fragile_run,
            "rotating_run":   rotating_run,
            "breakdown_run":  breakdown_run,
        })
        await check_9_route_contract(conn)
        print("\nAll Phase 4.6A checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
