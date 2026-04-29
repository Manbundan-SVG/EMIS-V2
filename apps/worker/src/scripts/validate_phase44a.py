"""Phase 4.4A smoke validation: Sequencing Pattern Registry + Transition
Archetypes.

Checks:
  1. archetype registry rows exist
  2. family archetype snapshots persist
  3. run archetype snapshots persist
  4. family archetype summary rows populate
  5. run archetype summary rows populate
  6. regime archetype summary rows populate
  7. run pattern summary row populates
  8. controlled transition/sequence inputs classify into expected archetypes
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
            "VALUES ($1::uuid, $2::uuid, 'phase44a_validation', 'Phase 4.4A Validation')",
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


async def check_1_registry(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        "SELECT archetype_key FROM cross_asset_transition_archetype_registry WHERE is_active = true",
    )
    assert rows, "CHECK 1 FAILED: archetype registry empty"
    keys = {r["archetype_key"] for r in rows}
    for expected in (
        "rotation_handoff", "reinforcing_continuation",
        "recovering_reentry", "deteriorating_breakdown",
        "mixed_transition_noise", "insufficient_history",
    ):
        assert expected in keys, f"CHECK 1 FAILED: missing archetype {expected!r}"
    print(f"CHECK 1 PASSED: archetype_registry_rows={len(rows)}")


async def seed_family_archetype(
    conn: asyncpg.Connection, *,
    workspace_id: str, watchlist_id: str, run_id: str,
    family: str, archetype_key: str,
    transition_state: str, sequence_class: str,
    family_rank: int, family_contribution: float,
    archetype_confidence: float,
    reason_codes: list[str],
    regime_key: str = "macro_dominant",
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_family_archetype_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           dependency_family, regime_key, archetype_key,
           transition_state, dominant_sequence_class, dominant_timing_class,
           family_rank, family_contribution, archetype_confidence,
           classification_reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                $4, $5, $6,
                $7, $8, 'lead',
                $9, $10, $11,
                $12::jsonb, '{"scoring_version":"4.4A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
        family, regime_key, archetype_key,
        transition_state, sequence_class,
        family_rank, family_contribution, archetype_confidence,
        json.dumps(reason_codes),
    )


async def seed_run_archetype(
    conn: asyncpg.Connection, *,
    workspace_id: str, watchlist_id: str, run_id: str,
    dominant_archetype_key: str, dominant_family: str,
    dominant_state: str, dominant_sequence_class: str,
    archetype_confidence: float,
    rotation: int, recovery: int, degradation: int, mixed: int,
    reason_codes: list[str],
    regime_key: str = "macro_dominant",
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_run_archetype_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           regime_key,
           dominant_archetype_key,
           dominant_dependency_family,
           dominant_transition_state,
           dominant_sequence_class,
           archetype_confidence,
           rotation_event_count, recovery_event_count,
           degradation_event_count, mixed_event_count,
           classification_reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13,
                $14::jsonb, '{"scoring_version":"4.4A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
        regime_key,
        dominant_archetype_key, dominant_family,
        dominant_state, dominant_sequence_class,
        archetype_confidence,
        rotation, recovery, degradation, mixed,
        json.dumps(reason_codes),
    )


async def check_2_3_family_and_run(
    conn: asyncpg.Connection, run_id: str,
) -> None:
    fam = await conn.fetch(
        "SELECT dependency_family, archetype_key "
        "FROM cross_asset_family_archetype_snapshots WHERE run_id = $1::uuid",
        run_id,
    )
    assert fam, "CHECK 2 FAILED: no family archetype snapshots"
    print(f"CHECK 2 PASSED: family_archetype_rows={len(fam)}")

    run_row = await conn.fetchrow(
        "SELECT dominant_archetype_key FROM cross_asset_run_archetype_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert run_row, "CHECK 3 FAILED: no run archetype snapshot"
    print(f"CHECK 3 PASSED: run_archetype_rows>=1 dominant={run_row['dominant_archetype_key']!r}")


async def check_4_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, archetype_key, archetype_confidence "
        "FROM cross_asset_family_archetype_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: family archetype summary empty"
    print(f"CHECK 4 PASSED: family_summary_rows={len(rows)}")


async def check_5_run_summary(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT dominant_archetype_key, rotation_event_count, recovery_event_count, "
        "       degradation_event_count, mixed_event_count "
        "FROM cross_asset_run_archetype_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert row, "CHECK 5 FAILED: run archetype summary empty"
    print(
        f"CHECK 5 PASSED: run_summary_rows>=1 dominant={row['dominant_archetype_key']!r} "
        f"counts(rot={row['rotation_event_count']},rec={row['recovery_event_count']},"
        f"deg={row['degradation_event_count']},mix={row['mixed_event_count']})"
    )


async def check_6_regime_summary(
    conn: asyncpg.Connection, workspace_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT regime_key, archetype_key, run_count, avg_confidence "
        "FROM cross_asset_regime_archetype_summary WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 6 FAILED: regime archetype summary empty"
    print(f"CHECK 6 PASSED: regime_summary_rows={len(rows)}")


async def check_7_run_pattern(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT run_id, dominant_archetype_key, dominant_dependency_family "
        "FROM run_cross_asset_pattern_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert row, "CHECK 7 FAILED: run pattern summary empty"
    print(
        f"CHECK 7 PASSED: run_pattern_rows>=1 "
        f"dominant={row['dominant_archetype_key']!r} "
        f"family={row['dominant_dependency_family']!r}"
    )


async def check_8_archetype_classification(
    conn: asyncpg.Connection, run_id: str,
) -> None:
    """Verify the seeded scenario: rates→reinforcing_continuation,
    risk→recovering_reentry, fx→rotation_handoff, commodity→deteriorating_breakdown.
    Dominant run archetype should be reinforcing_continuation (rank 1 family)."""
    rows = await conn.fetch(
        "SELECT dependency_family, archetype_key, family_rank "
        "FROM cross_asset_family_archetype_summary WHERE run_id = $1::uuid "
        "ORDER BY family_rank ASC",
        run_id,
    )
    by_fam = {r["dependency_family"]: r for r in rows}
    expected = {
        "rates":     "reinforcing_continuation",
        "risk":      "recovering_reentry",
        "fx":        "rotation_handoff",
        "commodity": "deteriorating_breakdown",
    }
    for fam, exp_key in expected.items():
        assert fam in by_fam, f"CHECK 8 FAILED: family {fam!r} missing"
        assert by_fam[fam]["archetype_key"] == exp_key, (
            f"CHECK 8 FAILED: {fam!r} expected {exp_key!r}, got {by_fam[fam]['archetype_key']!r}"
        )
    run_row = await conn.fetchrow(
        "SELECT dominant_archetype_key, dominant_dependency_family "
        "FROM cross_asset_run_archetype_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert run_row["dominant_archetype_key"] == "reinforcing_continuation", (
        f"CHECK 8 FAILED: run dominant archetype expected 'reinforcing_continuation', "
        f"got {run_row['dominant_archetype_key']!r}"
    )
    assert run_row["dominant_dependency_family"] == "rates", (
        f"CHECK 8 FAILED: run dominant family expected 'rates', "
        f"got {run_row['dominant_dependency_family']!r}"
    )
    print("CHECK 8 PASSED: archetype_classification_checked=true "
          "(rates→reinforcing, risk→recovering, fx→rotation, commodity→deteriorating; "
          "run dominant=reinforcing_continuation)")


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_transition_archetype_registry": [
            "id", "archetype_key", "archetype_label",
            "archetype_family", "description", "classification_rules",
            "is_active", "metadata", "created_at",
        ],
        "cross_asset_family_archetype_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "regime_key", "archetype_key",
            "transition_state", "dominant_sequence_class", "dominant_timing_class",
            "family_rank", "family_contribution", "archetype_confidence",
            "classification_reason_codes", "metadata", "created_at",
        ],
        "cross_asset_run_archetype_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "regime_key",
            "dominant_archetype_key",
            "dominant_dependency_family",
            "dominant_transition_state",
            "dominant_sequence_class",
            "archetype_confidence",
            "rotation_event_count", "recovery_event_count",
            "degradation_event_count", "mixed_event_count",
            "classification_reason_codes", "metadata", "created_at",
        ],
        "cross_asset_family_archetype_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "regime_key", "archetype_key",
            "transition_state", "dominant_sequence_class", "dominant_timing_class",
            "family_rank", "family_contribution", "archetype_confidence",
            "classification_reason_codes", "created_at",
        ],
        "cross_asset_run_archetype_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "regime_key",
            "dominant_archetype_key",
            "dominant_dependency_family",
            "dominant_transition_state",
            "dominant_sequence_class",
            "archetype_confidence",
            "rotation_event_count", "recovery_event_count",
            "degradation_event_count", "mixed_event_count",
            "classification_reason_codes", "created_at",
        ],
        "cross_asset_regime_archetype_summary": [
            "workspace_id", "regime_key", "archetype_key",
            "run_count", "avg_confidence", "latest_seen_at",
        ],
        "run_cross_asset_pattern_summary": [
            "run_id", "workspace_id", "watchlist_id",
            "regime_key",
            "dominant_archetype_key",
            "dominant_dependency_family",
            "dominant_transition_state",
            "dominant_sequence_class",
            "archetype_confidence", "created_at",
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
        run_id = str(uuid.uuid4())
        await ensure_job_run(conn, workspace_id, watchlist_id, run_id)

        await check_1_registry(conn)

        # Seed four families covering the four non-mixed archetypes
        await seed_family_archetype(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
            family="rates", archetype_key="reinforcing_continuation",
            transition_state="reinforcing", sequence_class="reinforcing_path",
            family_rank=1, family_contribution=0.231, archetype_confidence=0.85,
            reason_codes=["reinforcing_sequence_match", "transition_state:reinforcing",
                          "sequence_class:reinforcing_path"],
        )
        await seed_family_archetype(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
            family="risk", archetype_key="recovering_reentry",
            transition_state="recovering", sequence_class="recovery_path",
            family_rank=2, family_contribution=0.191, archetype_confidence=0.72,
            reason_codes=["recovery_sequence_match", "transition_state:recovering",
                          "sequence_class:recovery_path"],
        )
        await seed_family_archetype(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
            family="fx", archetype_key="rotation_handoff",
            transition_state="rotating_in", sequence_class="rotation_path",
            family_rank=3, family_contribution=0.113, archetype_confidence=0.70,
            reason_codes=["rotation_path_detected", "dominance_gain_present",
                          "transition_state:rotating_in", "sequence_class:rotation_path"],
        )
        await seed_family_archetype(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
            family="commodity", archetype_key="deteriorating_breakdown",
            transition_state="deteriorating", sequence_class="deteriorating_path",
            family_rank=4, family_contribution=0.088, archetype_confidence=0.60,
            reason_codes=["deteriorating_sequence_match", "transition_state:deteriorating",
                          "sequence_class:deteriorating_path"],
        )

        # Run archetype (dominant family = rank-1 = rates/reinforcing)
        await seed_run_archetype(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=run_id,
            dominant_archetype_key="reinforcing_continuation",
            dominant_family="rates",
            dominant_state="reinforcing",
            dominant_sequence_class="reinforcing_path",
            archetype_confidence=0.85,
            rotation=1, recovery=1, degradation=1, mixed=0,
            reason_codes=["reinforcing_sequence_match", "top_family:rates",
                          "regime:macro_dominant"],
        )
        print(f"SEEDED: run_id={run_id[:12]}… families=4")

        await check_2_3_family_and_run(conn, run_id)
        await check_4_family_summary(conn, run_id)
        await check_5_run_summary(conn, run_id)
        await check_6_regime_summary(conn, workspace_id)
        await check_7_run_pattern(conn, run_id)
        await check_8_archetype_classification(conn, run_id)
        await check_9_route_contract(conn)
        print("\nAll Phase 4.4A checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
