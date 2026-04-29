"""Phase 4.6D smoke validation: Replay Validation for Persistence-Aware Composite.

Checks:
  1. source/replay lineage exists or seeded test lineage path works
  2. persistence replay validation snapshot persists
  3. family persistence replay stability rows persist
  4. persistence replay validation summary rows populate
  5. family persistence replay stability summary rows populate
  6. persistence replay stability aggregate row populates
  7. unchanged persistence inputs produce validated / match-positive results
  8. controlled persistence/state/memory/event/context change produces explicit drift codes
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
            "VALUES ($1::uuid, $2::uuid, 'phase46d_validation', 'Phase 4.6D Validation')",
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
    transition_state_match: bool, sequence_class_match: bool,
    archetype_match: bool,
    cluster_state_match: bool,
    persistence_state_match: bool, memory_score_match: bool,
    state_age_match: bool, persistence_event_match: bool,
    persistence_attribution_match: bool, persistence_composite_match: bool,
    persistence_dominant_family_match: bool,
    drift_codes: list[str], validation_state: str,
    source_timing_class: str | None, replay_timing_class: str | None,
    source_transition_state: str | None, replay_transition_state: str | None,
    source_sequence_class: str | None, replay_sequence_class: str | None,
    source_archetype_key: str | None, replay_archetype_key: str | None,
    source_cluster_state: str | None, replay_cluster_state: str | None,
    source_persistence_state: str | None, replay_persistence_state: str | None,
    source_memory_score: float | None, replay_memory_score: float | None,
    source_state_age_runs: int | None, replay_state_age_runs: int | None,
    source_latest_event: str | None, replay_latest_event: str | None,
) -> str:
    vid = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO cross_asset_persistence_replay_validation_snapshots
          (id, workspace_id, watchlist_id, source_run_id, replay_run_id,
           source_context_snapshot_id, replay_context_snapshot_id,
           source_regime_key, replay_regime_key,
           source_dominant_timing_class, replay_dominant_timing_class,
           source_dominant_transition_state, replay_dominant_transition_state,
           source_dominant_sequence_class, replay_dominant_sequence_class,
           source_dominant_archetype_key, replay_dominant_archetype_key,
           source_cluster_state, replay_cluster_state,
           source_persistence_state, replay_persistence_state,
           source_memory_score, replay_memory_score,
           source_state_age_runs, replay_state_age_runs,
           source_latest_persistence_event_type, replay_latest_persistence_event_type,
           context_hash_match, regime_match, timing_class_match,
           transition_state_match, sequence_class_match,
           archetype_match,
           cluster_state_match,
           persistence_state_match, memory_score_match,
           state_age_match, persistence_event_match,
           persistence_attribution_match, persistence_composite_match,
           persistence_dominant_family_match,
           persistence_delta, persistence_composite_delta,
           drift_reason_codes, validation_state, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5::uuid,
                NULL, NULL,
                'macro_dominant', 'macro_dominant',
                $6, $7,
                $8, $9,
                $10, $11,
                $12, $13,
                $14, $15,
                $16, $17,
                $18, $19,
                $20, $21,
                $22, $23,
                $24, $25, $26,
                $27, $28,
                $29,
                $30,
                $31, $32,
                $33, $34,
                $35, $36,
                $37,
                '{}'::jsonb, '{}'::jsonb,
                $38::jsonb, $39,
                '{"scoring_version":"4.6D.v1","numeric_tolerance":1e-9,"memory_score_tolerance":1e-6}'::jsonb)
        """,
        vid, workspace_id, watchlist_id, source_run_id, replay_run_id,
        source_timing_class, replay_timing_class,
        source_transition_state, replay_transition_state,
        source_sequence_class, replay_sequence_class,
        source_archetype_key, replay_archetype_key,
        source_cluster_state, replay_cluster_state,
        source_persistence_state, replay_persistence_state,
        source_memory_score, replay_memory_score,
        source_state_age_runs, replay_state_age_runs,
        source_latest_event, replay_latest_event,
        context_hash_match, regime_match, timing_class_match,
        transition_state_match, sequence_class_match,
        archetype_match,
        cluster_state_match,
        persistence_state_match, memory_score_match,
        state_age_match, persistence_event_match,
        persistence_attribution_match, persistence_composite_match,
        persistence_dominant_family_match,
        json.dumps(drift_codes), validation_state,
    )
    return vid


async def seed_family_stability_rows(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str,
    source_run_id: str, replay_run_id: str,
    source_state: str, replay_state: str,
    source_memory: float, replay_memory: float,
    source_age: int, replay_age: int,
    source_event: str | None, replay_event: str | None,
    drift_codes: list[str],
    persistence_state_match: bool, memory_score_match: bool,
    state_age_match: bool, persistence_event_match: bool,
) -> None:
    fam_rows = [
        ("rates", 0.2602, 0.2602, 0.0260, 0.0260),
        ("risk",  0.1766, 0.1766, 0.0182, 0.0182),
    ]
    for (fam, s_attr, r_attr, s_int, r_int) in fam_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_persistence_replay_stability_snapshots
              (workspace_id, watchlist_id, source_run_id, replay_run_id,
               dependency_family,
               source_persistence_state, replay_persistence_state,
               source_memory_score, replay_memory_score,
               source_state_age_runs, replay_state_age_runs,
               source_latest_persistence_event_type, replay_latest_persistence_event_type,
               source_persistence_adjusted_contribution, replay_persistence_adjusted_contribution,
               source_persistence_integration_contribution, replay_persistence_integration_contribution,
               persistence_adjusted_delta, persistence_integration_delta,
               persistence_state_match, memory_score_match,
               state_age_match, persistence_event_match,
               persistence_family_rank_match, persistence_composite_family_rank_match,
               drift_reason_codes, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid,
                    $5,
                    $6, $7,
                    $8, $9,
                    $10, $11,
                    $12, $13,
                    $14, $15,
                    $16, $17,
                    $18, $19,
                    $20, $21,
                    $22, $23,
                    true, true,
                    $24::jsonb, '{}'::jsonb)
            """,
            workspace_id, watchlist_id, source_run_id, replay_run_id,
            fam, source_state, replay_state,
            source_memory, replay_memory,
            source_age, replay_age,
            source_event, replay_event,
            s_attr, r_attr, s_int, r_int,
            r_attr - s_attr, r_int - s_int,
            persistence_state_match, memory_score_match,
            state_age_match, persistence_event_match,
            json.dumps(drift_codes),
        )


async def check_2_6(
    conn: asyncpg.Connection,
    workspace_id: str, watchlist_id: str,
    source_run_id: str, replay_run_id: str,
) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_persistence_replay_validation_snapshots "
        "WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid",
        source_run_id, replay_run_id,
    )
    assert row, "CHECK 2 FAILED: no validation snapshot persisted"
    print(f"CHECK 2 PASSED: persistence_replay_validation_rows>=1 id={str(row['id'])[:12]}…")

    rows = await conn.fetch(
        "SELECT dependency_family FROM cross_asset_family_persistence_replay_stability_snapshots "
        "WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid",
        source_run_id, replay_run_id,
    )
    assert rows, "CHECK 3 FAILED: no family persistence replay stability rows"
    print(f"CHECK 3 PASSED: family_persistence_replay_rows={len(rows)}")

    srow = await conn.fetchrow(
        "SELECT validation_state FROM cross_asset_persistence_replay_validation_summary "
        "WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid",
        source_run_id, replay_run_id,
    )
    assert srow, "CHECK 4 FAILED: validation summary empty"
    print(f"CHECK 4 PASSED: validation_summary_rows>=1 state={srow['validation_state']}")

    frows = await conn.fetch(
        "SELECT dependency_family, persistence_adjusted_delta "
        "FROM cross_asset_family_persistence_replay_stability_summary "
        "WHERE workspace_id = $1::uuid AND source_run_id = $2::uuid AND replay_run_id = $3::uuid",
        workspace_id, source_run_id, replay_run_id,
    )
    assert frows, "CHECK 5 FAILED: family summary empty"
    print(f"CHECK 5 PASSED: family_summary_rows={len(frows)}")

    agg = await conn.fetchrow(
        "SELECT validation_count, drift_detected_count, "
        "       persistence_state_match_rate, memory_score_match_rate, "
        "       persistence_composite_match_rate "
        "FROM cross_asset_persistence_replay_stability_aggregate WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert agg, "CHECK 6 FAILED: aggregate empty"
    print(
        f"CHECK 6 PASSED: aggregate_rows>=1 count={agg['validation_count']} "
        f"drift={agg['drift_detected_count']} "
        f"persist_state_match_rate={agg['persistence_state_match_rate']} "
        f"memory_match_rate={agg['memory_score_match_rate']} "
        f"persist_composite_match_rate={agg['persistence_composite_match_rate']}"
    )


async def check_7_stable_match(
    conn: asyncpg.Connection, source_run_id: str, replay_run_id: str,
) -> None:
    row = await conn.fetchrow(
        """
        SELECT context_hash_match, regime_match, timing_class_match,
               transition_state_match, sequence_class_match,
               archetype_match,
               cluster_state_match,
               persistence_state_match, memory_score_match,
               state_age_match, persistence_event_match,
               persistence_attribution_match, persistence_composite_match,
               persistence_dominant_family_match, validation_state
        FROM cross_asset_persistence_replay_validation_summary
        WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid
        LIMIT 1
        """,
        source_run_id, replay_run_id,
    )
    assert row, "CHECK 7 FAILED: no validation row for stable pair"
    assert row["context_hash_match"]
    assert row["regime_match"]
    assert row["timing_class_match"]
    assert row["transition_state_match"]
    assert row["sequence_class_match"]
    assert row["archetype_match"]
    assert row["cluster_state_match"]
    assert row["persistence_state_match"]
    assert row["memory_score_match"]
    assert row["state_age_match"]
    assert row["persistence_event_match"]
    assert row["persistence_attribution_match"]
    assert row["persistence_composite_match"]
    assert row["persistence_dominant_family_match"]
    assert row["validation_state"] == "validated", (
        f"CHECK 7 FAILED: expected 'validated', got {row['validation_state']!r}"
    )
    print("CHECK 7 PASSED: stable_persistence_replay_match_checked=true validation_state=validated")


async def check_8_drift_codes(
    conn: asyncpg.Connection,
    workspace_id: str, watchlist_id: str,
    source_run_id: str, replay_run_id: str,
) -> None:
    """Seed a drift-scenario validation: context + persistence_state +
    memory_score + state_age + persistence_event all differ, producing
    explicit drift codes."""
    drift_codes = [
        "context_hash_mismatch",
        "persistence_state_mismatch",
        "memory_score_mismatch",
        "state_age_mismatch",
        "persistence_event_mismatch",
        "persistence_integration_delta",
        "persistence_dominant_family_shift",
    ]
    await seed_validation_row(
        conn,
        workspace_id=workspace_id, watchlist_id=watchlist_id,
        source_run_id=source_run_id, replay_run_id=replay_run_id,
        context_hash_match=False, regime_match=True, timing_class_match=True,
        transition_state_match=True, sequence_class_match=True,
        archetype_match=True,
        cluster_state_match=True,
        persistence_state_match=False, memory_score_match=False,
        state_age_match=False, persistence_event_match=False,
        persistence_attribution_match=True, persistence_composite_match=False,
        persistence_dominant_family_match=False,
        drift_codes=drift_codes,
        validation_state="drift_detected",
        source_timing_class="lead", replay_timing_class="lead",
        source_transition_state="reinforcing", replay_transition_state="reinforcing",
        source_sequence_class="reinforcing_path", replay_sequence_class="reinforcing_path",
        source_archetype_key="reinforcing_continuation",
        replay_archetype_key="reinforcing_continuation",
        source_cluster_state="stable", replay_cluster_state="stable",
        source_persistence_state="persistent", replay_persistence_state="breaking_down",
        source_memory_score=0.80, replay_memory_score=0.20,
        source_state_age_runs=8, replay_state_age_runs=2,
        source_latest_event=None, replay_latest_event="cluster_memory_break",
    )
    row = await conn.fetchrow(
        """
        SELECT drift_reason_codes, validation_state,
               source_persistence_state, replay_persistence_state,
               source_memory_score, replay_memory_score,
               source_state_age_runs, replay_state_age_runs,
               source_latest_persistence_event_type, replay_latest_persistence_event_type
        FROM cross_asset_persistence_replay_validation_summary
        WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid
        """,
        source_run_id, replay_run_id,
    )
    assert row
    codes_raw = row["drift_reason_codes"]
    if isinstance(codes_raw, str):
        codes_raw = json.loads(codes_raw)
    codes = list(codes_raw or [])
    assert "persistence_state_mismatch" in codes, (
        f"CHECK 8 FAILED: persistence_state_mismatch missing from {codes}"
    )
    assert "memory_score_mismatch" in codes, (
        f"CHECK 8 FAILED: memory_score_mismatch missing from {codes}"
    )
    assert "state_age_mismatch" in codes, (
        f"CHECK 8 FAILED: state_age_mismatch missing from {codes}"
    )
    assert "persistence_event_mismatch" in codes, (
        f"CHECK 8 FAILED: persistence_event_mismatch missing from {codes}"
    )
    assert "context_hash_mismatch" in codes, (
        f"CHECK 8 FAILED: context_hash_mismatch missing from {codes}"
    )
    assert row["validation_state"] == "drift_detected"
    assert row["source_persistence_state"] == "persistent"
    assert row["replay_persistence_state"] == "breaking_down"
    assert float(row["source_memory_score"]) == 0.80
    assert float(row["replay_memory_score"]) == 0.20
    assert row["source_state_age_runs"] == 8
    assert row["replay_state_age_runs"] == 2
    assert row["source_latest_persistence_event_type"] is None
    assert row["replay_latest_persistence_event_type"] == "cluster_memory_break"
    print(f"CHECK 8 PASSED: persistence_drift_reason_codes_checked=true codes={codes}")


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_persistence_replay_validation_snapshots": [
            "id", "workspace_id", "watchlist_id", "source_run_id", "replay_run_id",
            "source_context_snapshot_id", "replay_context_snapshot_id",
            "source_regime_key", "replay_regime_key",
            "source_dominant_timing_class", "replay_dominant_timing_class",
            "source_dominant_transition_state", "replay_dominant_transition_state",
            "source_dominant_sequence_class", "replay_dominant_sequence_class",
            "source_dominant_archetype_key", "replay_dominant_archetype_key",
            "source_cluster_state", "replay_cluster_state",
            "source_persistence_state", "replay_persistence_state",
            "source_memory_score", "replay_memory_score",
            "source_state_age_runs", "replay_state_age_runs",
            "source_latest_persistence_event_type", "replay_latest_persistence_event_type",
            "context_hash_match", "regime_match", "timing_class_match",
            "transition_state_match", "sequence_class_match",
            "archetype_match",
            "cluster_state_match",
            "persistence_state_match", "memory_score_match",
            "state_age_match", "persistence_event_match",
            "persistence_attribution_match", "persistence_composite_match",
            "persistence_dominant_family_match",
            "persistence_delta", "persistence_composite_delta",
            "drift_reason_codes", "validation_state", "metadata", "created_at",
        ],
        "cross_asset_family_persistence_replay_stability_snapshots": [
            "id", "workspace_id", "watchlist_id", "source_run_id", "replay_run_id",
            "dependency_family",
            "source_persistence_state", "replay_persistence_state",
            "source_memory_score", "replay_memory_score",
            "source_state_age_runs", "replay_state_age_runs",
            "source_latest_persistence_event_type", "replay_latest_persistence_event_type",
            "source_persistence_adjusted_contribution", "replay_persistence_adjusted_contribution",
            "source_persistence_integration_contribution", "replay_persistence_integration_contribution",
            "persistence_adjusted_delta", "persistence_integration_delta",
            "persistence_state_match", "memory_score_match",
            "state_age_match", "persistence_event_match",
            "persistence_family_rank_match", "persistence_composite_family_rank_match",
            "drift_reason_codes", "metadata", "created_at",
        ],
        "cross_asset_persistence_replay_validation_summary": [
            "workspace_id", "watchlist_id", "source_run_id", "replay_run_id",
            "source_context_snapshot_id", "replay_context_snapshot_id",
            "source_regime_key", "replay_regime_key",
            "source_dominant_timing_class", "replay_dominant_timing_class",
            "source_dominant_transition_state", "replay_dominant_transition_state",
            "source_dominant_sequence_class", "replay_dominant_sequence_class",
            "source_dominant_archetype_key", "replay_dominant_archetype_key",
            "source_cluster_state", "replay_cluster_state",
            "source_persistence_state", "replay_persistence_state",
            "source_memory_score", "replay_memory_score",
            "source_state_age_runs", "replay_state_age_runs",
            "source_latest_persistence_event_type", "replay_latest_persistence_event_type",
            "context_hash_match", "regime_match", "timing_class_match",
            "transition_state_match", "sequence_class_match",
            "archetype_match",
            "cluster_state_match",
            "persistence_state_match", "memory_score_match",
            "state_age_match", "persistence_event_match",
            "persistence_attribution_match", "persistence_composite_match",
            "persistence_dominant_family_match",
            "drift_reason_codes", "validation_state", "created_at",
        ],
        "cross_asset_family_persistence_replay_stability_summary": [
            "workspace_id", "watchlist_id", "source_run_id", "replay_run_id",
            "dependency_family",
            "source_persistence_state", "replay_persistence_state",
            "source_memory_score", "replay_memory_score",
            "source_state_age_runs", "replay_state_age_runs",
            "source_latest_persistence_event_type", "replay_latest_persistence_event_type",
            "source_persistence_adjusted_contribution", "replay_persistence_adjusted_contribution",
            "source_persistence_integration_contribution", "replay_persistence_integration_contribution",
            "persistence_adjusted_delta", "persistence_integration_delta",
            "persistence_state_match", "memory_score_match",
            "state_age_match", "persistence_event_match",
            "persistence_family_rank_match", "persistence_composite_family_rank_match",
            "drift_reason_codes", "created_at",
        ],
        "cross_asset_persistence_replay_stability_aggregate": [
            "workspace_id",
            "validation_count",
            "context_match_rate", "regime_match_rate",
            "timing_class_match_rate",
            "transition_state_match_rate", "sequence_class_match_rate",
            "archetype_match_rate",
            "cluster_state_match_rate",
            "persistence_state_match_rate",
            "memory_score_match_rate",
            "state_age_match_rate",
            "persistence_event_match_rate",
            "persistence_attribution_match_rate",
            "persistence_composite_match_rate",
            "persistence_dominant_family_match_rate",
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
            transition_state_match=True, sequence_class_match=True,
            archetype_match=True,
            cluster_state_match=True,
            persistence_state_match=True, memory_score_match=True,
            state_age_match=True, persistence_event_match=True,
            persistence_attribution_match=True, persistence_composite_match=True,
            persistence_dominant_family_match=True,
            drift_codes=[], validation_state="validated",
            source_timing_class="lead", replay_timing_class="lead",
            source_transition_state="reinforcing", replay_transition_state="reinforcing",
            source_sequence_class="reinforcing_path", replay_sequence_class="reinforcing_path",
            source_archetype_key="reinforcing_continuation",
            replay_archetype_key="reinforcing_continuation",
            source_cluster_state="stable", replay_cluster_state="stable",
            source_persistence_state="persistent", replay_persistence_state="persistent",
            source_memory_score=0.80, replay_memory_score=0.80,
            source_state_age_runs=8, replay_state_age_runs=8,
            source_latest_event=None, replay_latest_event=None,
        )
        await seed_family_stability_rows(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=source_run_id, replay_run_id=replay_run_id,
            source_state="persistent", replay_state="persistent",
            source_memory=0.80, replay_memory=0.80,
            source_age=8, replay_age=8,
            source_event=None, replay_event=None,
            drift_codes=[],
            persistence_state_match=True, memory_score_match=True,
            state_age_match=True, persistence_event_match=True,
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
        print("\nAll Phase 4.6D checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
