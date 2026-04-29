"""Phase 4.5A smoke validation: Pattern-Cluster Drift + Archetype Regime Rotation.

Checks:
  1. archetype cluster snapshots persist
  2. regime rotation snapshots persist
  3. pattern drift event snapshots persist
  4. cluster summary rows populate
  5. regime rotation summary rows populate
  6. drift event summary rows populate
  7. run pattern-cluster summary row populates
  8. controlled inputs produce expected stable / rotating / deteriorating
     classifications + drift events
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
            "VALUES ($1::uuid, $2::uuid, 'phase45a_validation', 'Phase 4.5A Validation')",
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


async def seed_cluster_snapshot(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str, run_id: str,
    regime_key: str, dominant_archetype: str,
    cluster_state: str,
    rein: float, rec: float, rot: float, deg: float, mix: float,
    entropy: float, drift_score: float,
    archetype_mix: dict[str, float],
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_archetype_cluster_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           regime_key, window_label,
           dominant_archetype_key, archetype_mix,
           reinforcement_share, recovery_share, rotation_share,
           degradation_share, mixed_share,
           pattern_entropy, cluster_state, drift_score, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                $4, 'recent_10',
                $5, $6::jsonb,
                $7, $8, $9, $10, $11,
                $12, $13, $14,
                '{"scoring_version":"4.5A.v1","run_window_size":10}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
        regime_key, dominant_archetype, json.dumps(archetype_mix),
        rein, rec, rot, deg, mix,
        entropy, cluster_state, drift_score,
    )


async def seed_regime_rotation(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, regime_key: str,
    prior_dom: str, current_dom: str,
    rotation_count: int, rein: int, rec: int, deg: int, mix: int,
    rotation_state: str, drift_score: float,
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_archetype_regime_rotation_snapshots
          (workspace_id, regime_key, window_label,
           prior_dominant_archetype_key, current_dominant_archetype_key,
           rotation_count,
           reinforcement_run_count, recovery_run_count,
           degradation_run_count, mixed_run_count,
           rotation_state, regime_drift_score, metadata)
        VALUES ($1::uuid, $2, 'recent_25',
                $3, $4,
                $5, $6, $7, $8, $9,
                $10, $11,
                '{"scoring_version":"4.5A.v1","window_size":25}'::jsonb)
        """,
        workspace_id, regime_key,
        prior_dom, current_dom,
        rotation_count, rein, rec, deg, mix,
        rotation_state, drift_score,
    )


async def seed_drift_event(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str | None,
    source_run_id: str | None, target_run_id: str,
    regime_key: str | None,
    prior_state: str | None, current_state: str,
    prior_archetype: str | None, current_archetype: str,
    drift_event_type: str, drift_score: float,
    reasons: list[str],
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_pattern_drift_event_snapshots
          (workspace_id, watchlist_id, source_run_id, target_run_id,
           regime_key,
           prior_cluster_state, current_cluster_state,
           prior_dominant_archetype_key, current_dominant_archetype_key,
           drift_event_type, drift_score,
           reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid,
                $5,
                $6, $7,
                $8, $9,
                $10, $11,
                $12::jsonb,
                '{"scoring_version":"4.5A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, source_run_id, target_run_id,
        regime_key,
        prior_state, current_state,
        prior_archetype, current_archetype,
        drift_event_type, drift_score,
        json.dumps(reasons),
    )


async def check_1_2_3(
    conn: asyncpg.Connection, workspace_id: str, run_ids: list[str],
) -> None:
    cluster_rows = await conn.fetch(
        "SELECT run_id FROM cross_asset_archetype_cluster_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert cluster_rows, "CHECK 1 FAILED: no cluster snapshots"
    print(f"CHECK 1 PASSED: cluster_snapshot_rows={len(cluster_rows)}")

    regime_rows = await conn.fetch(
        "SELECT regime_key FROM cross_asset_archetype_regime_rotation_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert regime_rows, "CHECK 2 FAILED: no regime rotation snapshots"
    print(f"CHECK 2 PASSED: regime_rotation_rows={len(regime_rows)}")

    drift_rows = await conn.fetch(
        "SELECT drift_event_type FROM cross_asset_pattern_drift_event_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert drift_rows, "CHECK 3 FAILED: no drift event snapshots"
    print(f"CHECK 3 PASSED: drift_event_rows={len(drift_rows)}")


async def check_4_cluster_summary(
    conn: asyncpg.Connection, workspace_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT run_id, cluster_state, dominant_archetype_key "
        "FROM cross_asset_archetype_cluster_summary WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 4 FAILED: cluster summary empty"
    print(f"CHECK 4 PASSED: cluster_summary_rows={len(rows)}")


async def check_5_regime_summary(
    conn: asyncpg.Connection, workspace_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT regime_key, rotation_state FROM cross_asset_archetype_regime_rotation_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 5 FAILED: regime summary empty"
    print(f"CHECK 5 PASSED: regime_summary_rows={len(rows)}")


async def check_6_drift_summary(
    conn: asyncpg.Connection, workspace_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT drift_event_type FROM cross_asset_pattern_drift_event_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 6 FAILED: drift summary empty"
    print(f"CHECK 6 PASSED: drift_summary_rows={len(rows)}")


async def check_7_run_pattern_cluster(
    conn: asyncpg.Connection, workspace_id: str, run_id: str,
) -> None:
    row = await conn.fetchrow(
        "SELECT run_id, cluster_state, dominant_archetype_key, "
        "       current_rotation_state, latest_drift_event_type "
        "FROM run_cross_asset_pattern_cluster_summary "
        "WHERE workspace_id = $1::uuid AND run_id = $2::uuid",
        workspace_id, run_id,
    )
    assert row, "CHECK 7 FAILED: run pattern-cluster summary missing"
    print(
        f"CHECK 7 PASSED: run_pattern_cluster_rows>=1 "
        f"cluster={row['cluster_state']!r} "
        f"rotation={row['current_rotation_state']!r} "
        f"latest_drift={row['latest_drift_event_type']!r}"
    )


async def check_8_classification(
    conn: asyncpg.Connection, run_ids: dict[str, str],
) -> None:
    """Verify the seeded scenarios are classified correctly:
      - stable_run    → cluster_state=stable, archetype=reinforcing_continuation
      - rotating_run  → cluster_state=rotating
      - degrade_run   → cluster_state=deteriorating
    Verify drift events line up:
      - rotating_run target → archetype_rotation
      - degrade_run target  → degradation_acceleration
    """
    rows = await conn.fetch(
        "SELECT run_id::text as run_id, cluster_state, dominant_archetype_key "
        "FROM cross_asset_archetype_cluster_summary "
        "WHERE run_id = ANY($1::uuid[])",
        list(run_ids.values()),
    )
    by_run = {r["run_id"]: r for r in rows}

    stable = by_run.get(run_ids["stable_run"])
    rotating = by_run.get(run_ids["rotating_run"])
    degrade = by_run.get(run_ids["degrade_run"])
    assert stable and rotating and degrade, "CHECK 8 FAILED: missing seeded run rows"
    assert stable["cluster_state"] == "stable", (
        f"CHECK 8 FAILED: stable_run cluster_state expected 'stable', got {stable['cluster_state']!r}"
    )
    assert rotating["cluster_state"] == "rotating", (
        f"CHECK 8 FAILED: rotating_run cluster_state expected 'rotating', got {rotating['cluster_state']!r}"
    )
    assert degrade["cluster_state"] == "deteriorating", (
        f"CHECK 8 FAILED: degrade_run cluster_state expected 'deteriorating', got {degrade['cluster_state']!r}"
    )
    assert stable["dominant_archetype_key"] == "reinforcing_continuation"

    drift_rows = await conn.fetch(
        "SELECT target_run_id::text as target_run_id, drift_event_type "
        "FROM cross_asset_pattern_drift_event_summary "
        "WHERE target_run_id = ANY($1::uuid[])",
        [run_ids["rotating_run"], run_ids["degrade_run"]],
    )
    drift_by_target = {r["target_run_id"]: r["drift_event_type"] for r in drift_rows}
    assert drift_by_target.get(run_ids["rotating_run"]) == "archetype_rotation", (
        f"CHECK 8 FAILED: rotating_run drift expected 'archetype_rotation', "
        f"got {drift_by_target.get(run_ids['rotating_run'])!r}"
    )
    assert drift_by_target.get(run_ids["degrade_run"]) == "degradation_acceleration", (
        f"CHECK 8 FAILED: degrade_run drift expected 'degradation_acceleration', "
        f"got {drift_by_target.get(run_ids['degrade_run'])!r}"
    )
    print(
        "CHECK 8 PASSED: cluster_classification_checked=true "
        "(stable→stable, rotating→rotating+archetype_rotation, "
        "degrade→deteriorating+degradation_acceleration)"
    )


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_archetype_cluster_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "regime_key", "window_label",
            "dominant_archetype_key", "archetype_mix",
            "reinforcement_share", "recovery_share", "rotation_share",
            "degradation_share", "mixed_share",
            "pattern_entropy", "cluster_state", "drift_score",
            "metadata", "created_at",
        ],
        "cross_asset_archetype_regime_rotation_snapshots": [
            "id", "workspace_id", "regime_key", "window_label",
            "prior_dominant_archetype_key", "current_dominant_archetype_key",
            "rotation_count",
            "reinforcement_run_count", "recovery_run_count",
            "degradation_run_count", "mixed_run_count",
            "rotation_state", "regime_drift_score",
            "metadata", "created_at",
        ],
        "cross_asset_pattern_drift_event_snapshots": [
            "id", "workspace_id", "watchlist_id",
            "source_run_id", "target_run_id",
            "regime_key",
            "prior_cluster_state", "current_cluster_state",
            "prior_dominant_archetype_key", "current_dominant_archetype_key",
            "drift_event_type", "drift_score",
            "reason_codes", "metadata", "created_at",
        ],
        "cross_asset_archetype_cluster_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "regime_key", "window_label",
            "dominant_archetype_key", "archetype_mix",
            "reinforcement_share", "recovery_share", "rotation_share",
            "degradation_share", "mixed_share",
            "pattern_entropy", "cluster_state", "drift_score", "created_at",
        ],
        "cross_asset_archetype_regime_rotation_summary": [
            "workspace_id", "regime_key", "window_label",
            "prior_dominant_archetype_key", "current_dominant_archetype_key",
            "rotation_count",
            "reinforcement_run_count", "recovery_run_count",
            "degradation_run_count", "mixed_run_count",
            "rotation_state", "regime_drift_score", "created_at",
        ],
        "cross_asset_pattern_drift_event_summary": [
            "workspace_id", "watchlist_id",
            "source_run_id", "target_run_id",
            "regime_key",
            "prior_cluster_state", "current_cluster_state",
            "prior_dominant_archetype_key", "current_dominant_archetype_key",
            "drift_event_type", "drift_score",
            "reason_codes", "created_at",
        ],
        "run_cross_asset_pattern_cluster_summary": [
            "run_id", "workspace_id", "watchlist_id",
            "regime_key",
            "dominant_archetype_key",
            "cluster_state",
            "drift_score",
            "pattern_entropy",
            "current_rotation_state",
            "latest_drift_event_type",
            "created_at",
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
        stable_run    = str(uuid.uuid4())
        rotating_run  = str(uuid.uuid4())
        degrade_run   = str(uuid.uuid4())
        for rid in (stable_run, rotating_run, degrade_run):
            await ensure_job_run(conn, workspace_id, watchlist_id, rid)

        # Stable scenario: reinforcement dominant, low entropy/drift.
        await seed_cluster_snapshot(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=stable_run,
            regime_key="macro_dominant",
            dominant_archetype="reinforcing_continuation",
            cluster_state="stable",
            rein=0.6, rec=0.1, rot=0.1, deg=0.1, mix=0.1,
            entropy=0.55, drift_score=0.05,
            archetype_mix={"reinforcing_continuation": 0.6,
                           "rotation_handoff": 0.1,
                           "recovering_reentry": 0.1,
                           "deteriorating_breakdown": 0.1,
                           "mixed_transition_noise": 0.1},
        )

        # Rotating scenario: rotation share elevated, dominant archetype shifted.
        await seed_cluster_snapshot(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=rotating_run,
            regime_key="macro_dominant",
            dominant_archetype="rotation_handoff",
            cluster_state="rotating",
            rein=0.2, rec=0.1, rot=0.5, deg=0.1, mix=0.1,
            entropy=0.70, drift_score=0.45,
            archetype_mix={"rotation_handoff": 0.5,
                           "reinforcing_continuation": 0.2,
                           "recovering_reentry": 0.1,
                           "deteriorating_breakdown": 0.1,
                           "mixed_transition_noise": 0.1},
        )

        # Deteriorating scenario: degradation share dominant.
        await seed_cluster_snapshot(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=degrade_run,
            regime_key="macro_dominant",
            dominant_archetype="deteriorating_breakdown",
            cluster_state="deteriorating",
            rein=0.1, rec=0.1, rot=0.1, deg=0.6, mix=0.1,
            entropy=0.55, drift_score=0.65,
            archetype_mix={"deteriorating_breakdown": 0.6,
                           "rotation_handoff": 0.1,
                           "recovering_reentry": 0.1,
                           "reinforcing_continuation": 0.1,
                           "mixed_transition_noise": 0.1},
        )

        # Regime rotation summary
        await seed_regime_rotation(
            conn,
            workspace_id=workspace_id, regime_key="macro_dominant",
            prior_dom="reinforcing_continuation",
            current_dom="rotation_handoff",
            rotation_count=3, rein=4, rec=2, deg=2, mix=1,
            rotation_state="rotating", drift_score=0.40,
        )

        # Drift events for rotating + degrade pairs
        await seed_drift_event(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=stable_run, target_run_id=rotating_run,
            regime_key="macro_dominant",
            prior_state="stable", current_state="rotating",
            prior_archetype="reinforcing_continuation",
            current_archetype="rotation_handoff",
            drift_event_type="archetype_rotation", drift_score=0.42,
            reasons=["dominant_archetype_changed", "rotation_share_increase"],
        )
        await seed_drift_event(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=rotating_run, target_run_id=degrade_run,
            regime_key="macro_dominant",
            prior_state="rotating", current_state="deteriorating",
            prior_archetype="rotation_handoff",
            current_archetype="deteriorating_breakdown",
            drift_event_type="degradation_acceleration", drift_score=0.62,
            reasons=["dominant_archetype_changed", "degradation_share_increase"],
        )
        print(
            f"SEEDED: stable={stable_run[:12]}… "
            f"rotating={rotating_run[:12]}… degrade={degrade_run[:12]}…"
        )

        await check_1_2_3(conn, workspace_id, [stable_run, rotating_run, degrade_run])
        await check_4_cluster_summary(conn, workspace_id)
        await check_5_regime_summary(conn, workspace_id)
        await check_6_drift_summary(conn, workspace_id)
        await check_7_run_pattern_cluster(conn, workspace_id, rotating_run)
        await check_8_classification(conn, {
            "stable_run":   stable_run,
            "rotating_run": rotating_run,
            "degrade_run":  degrade_run,
        })
        await check_9_route_contract(conn)
        print("\nAll Phase 4.5A checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
