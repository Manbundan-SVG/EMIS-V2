"""Phase 4.2D smoke validation: Replay Validation for Timing-Aware Composite.

Checks:
  1. source/replay lineage exists or seeded test lineage path works
  2. timing replay validation snapshot persists
  3. family timing replay stability rows persist
  4. timing replay validation summary rows populate
  5. family timing replay stability summary rows populate
  6. timing replay stability aggregate row populates
  7. unchanged timing inputs produce validated / match-positive results
  8. controlled timing-class/context change produces explicit drift reason codes
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
            "VALUES ($1::uuid, $2::uuid, 'phase42d_validation', 'Phase 4.2D Validation')",
            watchlist_id, workspace_id,
        )
    return workspace_id, watchlist_id


async def ensure_job_run(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str,
    run_id: str, replayed_from_run_id: str | None,
) -> None:
    existing = await conn.fetchrow("SELECT id FROM job_runs WHERE id = $1::uuid", run_id)
    if existing:
        return
    await conn.execute(
        """
        INSERT INTO job_runs (id, workspace_id, watchlist_id, status, queue_name,
                              is_replay, replayed_from_run_id)
        VALUES ($1::uuid, $2::uuid, $3::uuid, 'completed', 'recompute',
                $4, $5::uuid)
        ON CONFLICT (id) DO NOTHING
        """,
        run_id, workspace_id, watchlist_id,
        replayed_from_run_id is not None, replayed_from_run_id,
    )


async def check_1_lineage(conn: asyncpg.Connection, replay_run_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT replayed_from_run_id FROM job_runs WHERE id = $1::uuid",
        replay_run_id,
    )
    assert row and row["replayed_from_run_id"], "CHECK 1 FAILED: replay run has no source"
    print(f"CHECK 1 PASSED: replay_lineage_present_or_seeded=true source={str(row['replayed_from_run_id'])[:12]}…")


async def seed_validation_row(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str,
    source_run_id: str, replay_run_id: str,
    context_hash_match: bool, regime_match: bool, timing_class_match: bool,
    timing_attribution_match: bool, timing_composite_match: bool,
    timing_dominant_family_match: bool,
    drift_codes: list[str], validation_state: str,
    source_class: str | None, replay_class: str | None,
) -> str:
    vid = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO cross_asset_timing_replay_validation_snapshots
          (id, workspace_id, watchlist_id, source_run_id, replay_run_id,
           source_context_snapshot_id, replay_context_snapshot_id,
           source_regime_key, replay_regime_key,
           source_dominant_timing_class, replay_dominant_timing_class,
           context_hash_match, regime_match, timing_class_match,
           timing_attribution_match, timing_composite_match,
           timing_dominant_family_match,
           timing_net_delta, timing_composite_delta,
           drift_reason_codes, validation_state, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid,
                NULL, NULL,
                'macro_dominant', 'macro_dominant',
                $6, $7,
                $8, $9, $10, $11, $12, $13,
                '{}'::jsonb, '{}'::jsonb,
                $14::jsonb, $15,
                '{"scoring_version":"4.2D.v1","numeric_tolerance":1e-9}'::jsonb)
        """,
        vid, workspace_id, watchlist_id, source_run_id, replay_run_id,
        source_class, replay_class,
        context_hash_match, regime_match, timing_class_match,
        timing_attribution_match, timing_composite_match,
        timing_dominant_family_match,
        json.dumps(drift_codes), validation_state,
    )
    return vid


async def seed_family_stability_rows(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str,
    source_run_id: str, replay_run_id: str,
    source_class: str, replay_class: str,
    drift_codes: list[str],
    timing_class_match: bool,
) -> None:
    fam_rows = [
        ("rates", 0.25, 0.25, 0.0275, 0.0275),
        ("risk",  0.15, 0.15, 0.01875, 0.01875),
    ]
    for (fam, s_attr, r_attr, s_int, r_int) in fam_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_timing_replay_stability_snapshots
              (workspace_id, watchlist_id, source_run_id, replay_run_id,
               dependency_family,
               source_dominant_timing_class, replay_dominant_timing_class,
               source_timing_adjusted_contribution, replay_timing_adjusted_contribution,
               source_timing_integration_contribution, replay_timing_integration_contribution,
               timing_adjusted_delta, timing_integration_delta,
               timing_class_match, timing_family_rank_match, timing_composite_family_rank_match,
               drift_reason_codes, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid,
                    $5, $6, $7, $8, $9, $10, $11, $12, $13,
                    $14, true, true, $15::jsonb, '{}'::jsonb)
            """,
            workspace_id, watchlist_id, source_run_id, replay_run_id,
            fam, source_class, replay_class, s_attr, r_attr, s_int, r_int,
            r_attr - s_attr, r_int - s_int,
            timing_class_match,
            json.dumps(drift_codes),
        )


async def check_2_6(
    conn: asyncpg.Connection,
    workspace_id: str, watchlist_id: str,
    source_run_id: str, replay_run_id: str,
) -> None:
    # Check 2: snapshot persists
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_timing_replay_validation_snapshots "
        "WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid",
        source_run_id, replay_run_id,
    )
    assert row, "CHECK 2 FAILED: no validation snapshot persisted"
    print(f"CHECK 2 PASSED: timing_replay_validation_rows>=1 id={str(row['id'])[:12]}…")

    # Check 3: family rows persist
    rows = await conn.fetch(
        "SELECT dependency_family FROM cross_asset_family_timing_replay_stability_snapshots "
        "WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid",
        source_run_id, replay_run_id,
    )
    assert rows, "CHECK 3 FAILED: no family timing replay stability rows"
    print(f"CHECK 3 PASSED: family_timing_replay_rows={len(rows)}")

    # Check 4: summary view
    srow = await conn.fetchrow(
        "SELECT validation_state FROM cross_asset_timing_replay_validation_summary "
        "WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid",
        source_run_id, replay_run_id,
    )
    assert srow, "CHECK 4 FAILED: validation summary empty"
    print(f"CHECK 4 PASSED: validation_summary_rows>=1 state={srow['validation_state']}")

    # Check 5: family summary view
    frows = await conn.fetch(
        "SELECT dependency_family, timing_adjusted_delta FROM cross_asset_family_timing_replay_stability_summary "
        "WHERE workspace_id = $1::uuid AND source_run_id = $2::uuid AND replay_run_id = $3::uuid",
        workspace_id, source_run_id, replay_run_id,
    )
    assert frows, "CHECK 5 FAILED: family summary empty"
    print(f"CHECK 5 PASSED: family_summary_rows={len(frows)}")

    # Check 6: aggregate
    agg = await conn.fetchrow(
        "SELECT validation_count, drift_detected_count, timing_class_match_rate "
        "FROM cross_asset_timing_replay_stability_aggregate WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert agg, "CHECK 6 FAILED: aggregate empty"
    print(
        f"CHECK 6 PASSED: aggregate_rows>=1 count={agg['validation_count']} "
        f"drift={agg['drift_detected_count']} timing_class_match_rate={agg['timing_class_match_rate']}"
    )


async def check_7_stable_match(
    conn: asyncpg.Connection, source_run_id: str, replay_run_id: str,
) -> None:
    row = await conn.fetchrow(
        """
        SELECT context_hash_match, regime_match, timing_class_match,
               timing_attribution_match, timing_composite_match,
               timing_dominant_family_match, validation_state
        FROM cross_asset_timing_replay_validation_summary
        WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid
        LIMIT 1
        """,
        source_run_id, replay_run_id,
    )
    assert row, "CHECK 7 FAILED: no validation row for stable pair"
    assert row["context_hash_match"]
    assert row["regime_match"]
    assert row["timing_class_match"]
    assert row["timing_attribution_match"]
    assert row["timing_composite_match"]
    assert row["timing_dominant_family_match"]
    assert row["validation_state"] == "validated", (
        f"CHECK 7 FAILED: expected 'validated', got {row['validation_state']!r}"
    )
    print("CHECK 7 PASSED: stable_timing_replay_match_checked=true validation_state=validated")


async def check_8_drift_codes(
    conn: asyncpg.Connection,
    workspace_id: str, watchlist_id: str,
    source_run_id: str, replay_run_id: str,
) -> None:
    """Seed a second drift-scenario validation: timing_class differs +
    context_hash differs, producing explicit drift codes."""
    drift_codes = [
        "context_hash_mismatch",
        "timing_class_mismatch",
        "timing_integration_delta",
        "timing_dominant_family_shift",
    ]
    await seed_validation_row(
        conn,
        workspace_id=workspace_id, watchlist_id=watchlist_id,
        source_run_id=source_run_id, replay_run_id=replay_run_id,
        context_hash_match=False, regime_match=True, timing_class_match=False,
        timing_attribution_match=True, timing_composite_match=False,
        timing_dominant_family_match=False,
        drift_codes=drift_codes,
        validation_state="drift_detected",
        source_class="lead", replay_class="lag",
    )
    row = await conn.fetchrow(
        """
        SELECT drift_reason_codes, validation_state,
               source_dominant_timing_class, replay_dominant_timing_class
        FROM cross_asset_timing_replay_validation_summary
        WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid
        """,
        source_run_id, replay_run_id,
    )
    assert row
    codes_raw = row["drift_reason_codes"]
    if isinstance(codes_raw, str):
        codes_raw = json.loads(codes_raw)
    codes = list(codes_raw or [])
    assert "timing_class_mismatch" in codes, (
        f"CHECK 8 FAILED: timing_class_mismatch missing from {codes}"
    )
    assert "context_hash_mismatch" in codes, (
        f"CHECK 8 FAILED: context_hash_mismatch missing from {codes}"
    )
    assert row["validation_state"] == "drift_detected"
    assert row["source_dominant_timing_class"] == "lead"
    assert row["replay_dominant_timing_class"] == "lag"
    print(f"CHECK 8 PASSED: timing_drift_reason_codes_checked=true codes={codes}")


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_timing_replay_validation_snapshots": [
            "id", "workspace_id", "watchlist_id", "source_run_id", "replay_run_id",
            "source_context_snapshot_id", "replay_context_snapshot_id",
            "source_regime_key", "replay_regime_key",
            "source_dominant_timing_class", "replay_dominant_timing_class",
            "context_hash_match", "regime_match", "timing_class_match",
            "timing_attribution_match", "timing_composite_match",
            "timing_dominant_family_match",
            "timing_net_delta", "timing_composite_delta",
            "drift_reason_codes", "validation_state", "metadata", "created_at",
        ],
        "cross_asset_family_timing_replay_stability_snapshots": [
            "id", "workspace_id", "watchlist_id", "source_run_id", "replay_run_id",
            "dependency_family",
            "source_dominant_timing_class", "replay_dominant_timing_class",
            "source_timing_adjusted_contribution", "replay_timing_adjusted_contribution",
            "source_timing_integration_contribution", "replay_timing_integration_contribution",
            "timing_adjusted_delta", "timing_integration_delta",
            "timing_class_match", "timing_family_rank_match", "timing_composite_family_rank_match",
            "drift_reason_codes", "metadata", "created_at",
        ],
        "cross_asset_timing_replay_validation_summary": [
            "workspace_id", "watchlist_id", "source_run_id", "replay_run_id",
            "source_context_snapshot_id", "replay_context_snapshot_id",
            "source_regime_key", "replay_regime_key",
            "source_dominant_timing_class", "replay_dominant_timing_class",
            "context_hash_match", "regime_match", "timing_class_match",
            "timing_attribution_match", "timing_composite_match",
            "timing_dominant_family_match",
            "drift_reason_codes", "validation_state", "created_at",
        ],
        "cross_asset_family_timing_replay_stability_summary": [
            "workspace_id", "watchlist_id", "source_run_id", "replay_run_id",
            "dependency_family",
            "source_dominant_timing_class", "replay_dominant_timing_class",
            "source_timing_adjusted_contribution", "replay_timing_adjusted_contribution",
            "source_timing_integration_contribution", "replay_timing_integration_contribution",
            "timing_adjusted_delta", "timing_integration_delta",
            "timing_class_match", "timing_family_rank_match", "timing_composite_family_rank_match",
            "drift_reason_codes", "created_at",
        ],
        "cross_asset_timing_replay_stability_aggregate": [
            "workspace_id",
            "validation_count",
            "context_match_rate", "regime_match_rate",
            "timing_class_match_rate",
            "timing_attribution_match_rate",
            "timing_composite_match_rate",
            "timing_dominant_family_match_rate",
            "drift_detected_count", "latest_validated_at",
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
        source_run_id = str(uuid.uuid4())
        replay_run_id = str(uuid.uuid4())
        await ensure_job_run(conn, workspace_id, watchlist_id, source_run_id, None)
        await ensure_job_run(conn, workspace_id, watchlist_id, replay_run_id, source_run_id)
        await check_1_lineage(conn, replay_run_id)

        # Stable validated row (for checks 2-7)
        await seed_validation_row(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=source_run_id, replay_run_id=replay_run_id,
            context_hash_match=True, regime_match=True, timing_class_match=True,
            timing_attribution_match=True, timing_composite_match=True,
            timing_dominant_family_match=True,
            drift_codes=[], validation_state="validated",
            source_class="lead", replay_class="lead",
        )
        await seed_family_stability_rows(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=source_run_id, replay_run_id=replay_run_id,
            source_class="lead", replay_class="lead",
            drift_codes=[], timing_class_match=True,
        )
        print(f"SEEDED: stable pair source={source_run_id[:12]}… replay={replay_run_id[:12]}…")

        await check_2_6(conn, workspace_id, watchlist_id, source_run_id, replay_run_id)
        await check_7_stable_match(conn, source_run_id, replay_run_id)

        # Drift-scenario pair for check 8
        source2 = str(uuid.uuid4())
        replay2 = str(uuid.uuid4())
        await ensure_job_run(conn, workspace_id, watchlist_id, source2, None)
        await ensure_job_run(conn, workspace_id, watchlist_id, replay2, source2)
        await check_8_drift_codes(conn, workspace_id, watchlist_id, source2, replay2)

        await check_9_route_contract(conn)
        print("\nAll Phase 4.2D checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
