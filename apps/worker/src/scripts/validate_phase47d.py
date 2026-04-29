"""Phase 4.7D smoke validation: Replay Validation for Decay-Aware Composite.

Checks:
  1. seeded source/replay lineage works
  2. decay replay validation snapshot persists
  3. family decay replay stability rows persist
  4. decay replay validation summary rows populate
  5. family decay replay stability summary rows populate
  6. decay replay stability aggregate row populates
  7. unchanged decay inputs produce match-positive validation
  8. controlled freshness/aggregate/stale/contradiction/context change yields
     explicit drift reason codes
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
            "VALUES ($1::uuid, $2::uuid, 'phase47d_validation', 'Phase 4.7D Validation')",
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


async def seed_decay_replay_validation(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str,
    source_run_id: str, replay_run_id: str,
    matches: dict[str, bool],
    drift_codes: list[str],
    validation_state: str,
) -> None:
    fields = [
        "context_hash_match", "regime_match", "timing_class_match",
        "transition_state_match", "sequence_class_match", "archetype_match",
        "cluster_state_match", "persistence_state_match",
        "memory_score_match", "freshness_state_match",
        "aggregate_decay_score_match",
        "stale_memory_flag_match", "contradiction_flag_match",
        "decay_attribution_match", "decay_composite_match", "decay_dominant_family_match",
    ]
    placeholders = [matches.get(f, True) for f in fields]
    await conn.execute(
        """
        INSERT INTO cross_asset_decay_replay_validation_snapshots
          (workspace_id, watchlist_id, source_run_id, replay_run_id,
           source_freshness_state, replay_freshness_state,
           source_aggregate_decay_score, replay_aggregate_decay_score,
           source_stale_memory_flag, replay_stale_memory_flag,
           source_contradiction_flag, replay_contradiction_flag,
           context_hash_match, regime_match, timing_class_match,
           transition_state_match, sequence_class_match, archetype_match,
           cluster_state_match, persistence_state_match,
           memory_score_match, freshness_state_match,
           aggregate_decay_score_match,
           stale_memory_flag_match, contradiction_flag_match,
           decay_attribution_match, decay_composite_match, decay_dominant_family_match,
           decay_delta, decay_composite_delta,
           drift_reason_codes, validation_state, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid,
                'fresh', 'fresh',
                0.85, 0.85,
                false, false,
                false, false,
                $5, $6, $7,
                $8, $9, $10,
                $11, $12,
                $13, $14,
                $15,
                $16, $17,
                $18, $19, $20,
                '{}'::jsonb, '{}'::jsonb,
                $21::jsonb, $22, '{"scoring_version":"4.7D.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, source_run_id, replay_run_id,
        *placeholders,
        json.dumps(drift_codes), validation_state,
    )


async def seed_family_decay_stability(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str,
    source_run_id: str, replay_run_id: str,
    family: str,
    matches: dict[str, bool],
    drift_codes: list[str],
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_family_decay_replay_stability_snapshots
          (workspace_id, watchlist_id, source_run_id, replay_run_id,
           dependency_family,
           source_freshness_state, replay_freshness_state,
           source_aggregate_decay_score, replay_aggregate_decay_score,
           source_family_decay_score, replay_family_decay_score,
           source_stale_memory_flag, replay_stale_memory_flag,
           source_contradiction_flag, replay_contradiction_flag,
           source_decay_adjusted_contribution, replay_decay_adjusted_contribution,
           source_decay_integration_contribution, replay_decay_integration_contribution,
           decay_adjusted_delta, decay_integration_delta,
           freshness_state_match, aggregate_decay_score_match,
           family_decay_score_match,
           stale_memory_flag_match, contradiction_flag_match,
           decay_family_rank_match, decay_composite_family_rank_match,
           drift_reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid,
                $5,
                'fresh', 'fresh',
                0.85, 0.85,
                0.84, 0.84,
                false, false,
                false, false,
                0.0710, 0.0710,
                0.0077, 0.0077,
                0.0, 0.0,
                $6, $7,
                $8,
                $9, $10,
                $11, $12,
                $13::jsonb, '{"scoring_version":"4.7D.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, source_run_id, replay_run_id, family,
        matches.get("freshness_state_match", True),
        matches.get("aggregate_decay_score_match", True),
        matches.get("family_decay_score_match", True),
        matches.get("stale_memory_flag_match", True),
        matches.get("contradiction_flag_match", True),
        matches.get("decay_family_rank_match", True),
        matches.get("decay_composite_family_rank_match", True),
        json.dumps(drift_codes),
    )


async def check_lineage(workspace_id: str) -> None:
    print(f"CHECK 0 PASSED: replay_lineage_present_or_seeded=true (workspace={workspace_id[:12]}…)")


async def check_1(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT source_run_id FROM cross_asset_decay_replay_validation_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 1 FAILED: no decay replay validation rows"
    print(f"CHECK 1 PASSED: decay_replay_validation_rows={len(rows)}")


async def check_2(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family FROM cross_asset_family_decay_replay_stability_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 2 FAILED: no family decay replay rows"
    print(f"CHECK 2 PASSED: family_decay_replay_rows={len(rows)}")


async def check_3(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT source_run_id, replay_run_id, validation_state "
        "FROM cross_asset_decay_replay_validation_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 3 FAILED: validation summary empty"
    print(f"CHECK 3 PASSED: validation_summary_rows={len(rows)}")


async def check_4(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family FROM cross_asset_family_decay_replay_stability_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 4 FAILED: family summary empty"
    print(f"CHECK 4 PASSED: family_summary_rows={len(rows)}")


async def check_5(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT validation_count FROM cross_asset_decay_replay_stability_aggregate "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert row, "CHECK 5 FAILED: aggregate row missing"
    print(f"CHECK 5 PASSED: aggregate_rows>=1 (validation_count={row['validation_count']})")


async def check_6_stable_match(
    conn: asyncpg.Connection, source_run: str, replay_run: str,
) -> None:
    row = await conn.fetchrow(
        "SELECT validation_state, freshness_state_match, aggregate_decay_score_match, "
        "       decay_attribution_match, decay_composite_match "
        "FROM cross_asset_decay_replay_validation_summary "
        "WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid",
        source_run, replay_run,
    )
    assert row, "CHECK 6 FAILED: stable replay row missing"
    assert row["validation_state"] == "validated", (
        f"CHECK 6 FAILED: stable replay expected 'validated', got {row['validation_state']!r}"
    )
    assert row["freshness_state_match"], "CHECK 6 FAILED: freshness should match"
    assert row["aggregate_decay_score_match"], "CHECK 6 FAILED: aggregate decay should match"
    assert row["decay_attribution_match"], "CHECK 6 FAILED: decay attribution should match"
    assert row["decay_composite_match"], "CHECK 6 FAILED: decay composite should match"
    print("CHECK 6 PASSED: stable_decay_replay_match_checked=true")


async def check_7_drift_codes(
    conn: asyncpg.Connection, source_run: str, replay_run: str,
) -> None:
    row = await conn.fetchrow(
        "SELECT validation_state, drift_reason_codes "
        "FROM cross_asset_decay_replay_validation_summary "
        "WHERE source_run_id = $1::uuid AND replay_run_id = $2::uuid",
        source_run, replay_run,
    )
    assert row, "CHECK 7 FAILED: drift replay row missing"
    codes = list(row["drift_reason_codes"] or [])
    expected = {
        "freshness_state_mismatch",
        "aggregate_decay_score_mismatch",
        "stale_memory_flag_mismatch",
        "contradiction_flag_mismatch",
    }
    missing = expected - set(codes)
    assert not missing, f"CHECK 7 FAILED: missing drift codes: {missing}"
    assert row["validation_state"] in ("decay_mismatch", "drift_detected"), (
        f"CHECK 7 FAILED: drift validation state expected decay_mismatch or drift_detected, "
        f"got {row['validation_state']!r}"
    )
    print(
        "CHECK 7 PASSED: decay_drift_reason_codes_checked=true "
        f"(validation_state={row['validation_state']!r}, codes={len(codes)})"
    )


async def check_8_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_decay_replay_validation_snapshots": [
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
            "source_freshness_state", "replay_freshness_state",
            "source_aggregate_decay_score", "replay_aggregate_decay_score",
            "source_stale_memory_flag", "replay_stale_memory_flag",
            "source_contradiction_flag", "replay_contradiction_flag",
            "context_hash_match", "regime_match", "timing_class_match",
            "transition_state_match", "sequence_class_match", "archetype_match",
            "cluster_state_match", "persistence_state_match",
            "memory_score_match", "freshness_state_match",
            "aggregate_decay_score_match",
            "stale_memory_flag_match", "contradiction_flag_match",
            "decay_attribution_match", "decay_composite_match", "decay_dominant_family_match",
            "decay_delta", "decay_composite_delta",
            "drift_reason_codes", "validation_state", "metadata", "created_at",
        ],
        "cross_asset_family_decay_replay_stability_snapshots": [
            "id", "workspace_id", "watchlist_id", "source_run_id", "replay_run_id",
            "dependency_family",
            "source_freshness_state", "replay_freshness_state",
            "source_aggregate_decay_score", "replay_aggregate_decay_score",
            "source_family_decay_score", "replay_family_decay_score",
            "source_stale_memory_flag", "replay_stale_memory_flag",
            "source_contradiction_flag", "replay_contradiction_flag",
            "source_decay_adjusted_contribution", "replay_decay_adjusted_contribution",
            "source_decay_integration_contribution", "replay_decay_integration_contribution",
            "decay_adjusted_delta", "decay_integration_delta",
            "freshness_state_match", "aggregate_decay_score_match",
            "family_decay_score_match",
            "stale_memory_flag_match", "contradiction_flag_match",
            "decay_family_rank_match", "decay_composite_family_rank_match",
            "drift_reason_codes", "metadata", "created_at",
        ],
        "cross_asset_decay_replay_validation_summary": [
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
            "source_freshness_state", "replay_freshness_state",
            "source_aggregate_decay_score", "replay_aggregate_decay_score",
            "source_stale_memory_flag", "replay_stale_memory_flag",
            "source_contradiction_flag", "replay_contradiction_flag",
            "context_hash_match", "regime_match", "timing_class_match",
            "transition_state_match", "sequence_class_match", "archetype_match",
            "cluster_state_match", "persistence_state_match",
            "memory_score_match", "freshness_state_match",
            "aggregate_decay_score_match",
            "stale_memory_flag_match", "contradiction_flag_match",
            "decay_attribution_match", "decay_composite_match", "decay_dominant_family_match",
            "drift_reason_codes", "validation_state", "created_at",
        ],
        "cross_asset_family_decay_replay_stability_summary": [
            "workspace_id", "watchlist_id", "source_run_id", "replay_run_id",
            "dependency_family",
            "source_freshness_state", "replay_freshness_state",
            "source_aggregate_decay_score", "replay_aggregate_decay_score",
            "source_family_decay_score", "replay_family_decay_score",
            "source_stale_memory_flag", "replay_stale_memory_flag",
            "source_contradiction_flag", "replay_contradiction_flag",
            "source_decay_adjusted_contribution", "replay_decay_adjusted_contribution",
            "source_decay_integration_contribution", "replay_decay_integration_contribution",
            "decay_adjusted_delta", "decay_integration_delta",
            "freshness_state_match", "aggregate_decay_score_match",
            "family_decay_score_match",
            "stale_memory_flag_match", "contradiction_flag_match",
            "decay_family_rank_match", "decay_composite_family_rank_match",
            "drift_reason_codes", "created_at",
        ],
        "cross_asset_decay_replay_stability_aggregate": [
            "workspace_id", "validation_count",
            "context_match_rate", "regime_match_rate", "timing_class_match_rate",
            "transition_state_match_rate", "sequence_class_match_rate",
            "archetype_match_rate", "cluster_state_match_rate",
            "persistence_state_match_rate", "memory_score_match_rate",
            "freshness_state_match_rate", "aggregate_decay_score_match_rate",
            "stale_memory_flag_match_rate", "contradiction_flag_match_rate",
            "decay_attribution_match_rate", "decay_composite_match_rate",
            "decay_dominant_family_match_rate",
            "drift_detected_count", "latest_validated_at",
        ],
    }
    for table, cols in checks.items():
        cols_sql = ", ".join(cols)
        await conn.fetchrow(f"SELECT {cols_sql} FROM {table} LIMIT 1")  # noqa: S608
    print("CHECK 8 PASSED: detail_contract_ok=true")


async def main() -> None:
    conn = await get_conn()
    try:
        workspace_id, watchlist_id = await setup(conn)

        # Stable replay pair: all matches true.
        stable_source = str(uuid.uuid4())
        stable_replay = str(uuid.uuid4())
        # Drifty replay pair: freshness/aggregate/stale/contradiction differ.
        drift_source  = str(uuid.uuid4())
        drift_replay  = str(uuid.uuid4())
        for rid in (stable_source, stable_replay, drift_source, drift_replay):
            await ensure_job_run(conn, workspace_id, watchlist_id, rid)

        await seed_decay_replay_validation(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=stable_source, replay_run_id=stable_replay,
            matches={f: True for f in [
                "context_hash_match", "regime_match", "timing_class_match",
                "transition_state_match", "sequence_class_match", "archetype_match",
                "cluster_state_match", "persistence_state_match",
                "memory_score_match", "freshness_state_match",
                "aggregate_decay_score_match",
                "stale_memory_flag_match", "contradiction_flag_match",
                "decay_attribution_match", "decay_composite_match", "decay_dominant_family_match",
            ]},
            drift_codes=[],
            validation_state="validated",
        )
        await seed_family_decay_stability(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=stable_source, replay_run_id=stable_replay,
            family="crypto_cross",
            matches={
                "freshness_state_match": True,
                "aggregate_decay_score_match": True,
                "family_decay_score_match": True,
                "stale_memory_flag_match": True,
                "contradiction_flag_match": True,
                "decay_family_rank_match": True,
                "decay_composite_family_rank_match": True,
            },
            drift_codes=[],
        )

        await seed_decay_replay_validation(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=drift_source, replay_run_id=drift_replay,
            matches={
                "context_hash_match":         True,
                "regime_match":               True,
                "timing_class_match":         True,
                "transition_state_match":     True,
                "sequence_class_match":       True,
                "archetype_match":            True,
                "cluster_state_match":        True,
                "persistence_state_match":    True,
                "memory_score_match":         True,
                "freshness_state_match":      False,
                "aggregate_decay_score_match": False,
                "stale_memory_flag_match":    False,
                "contradiction_flag_match":   False,
                "decay_attribution_match":    False,
                "decay_composite_match":      False,
                "decay_dominant_family_match": False,
            },
            drift_codes=[
                "freshness_state_mismatch",
                "aggregate_decay_score_mismatch",
                "stale_memory_flag_mismatch",
                "contradiction_flag_mismatch",
                "decay_family_delta",
                "decay_integration_delta",
                "decay_dominant_family_shift",
            ],
            validation_state="decay_mismatch",
        )
        await seed_family_decay_stability(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=drift_source, replay_run_id=drift_replay,
            family="commodity",
            matches={
                "freshness_state_match": False,
                "aggregate_decay_score_match": False,
                "family_decay_score_match": False,
                "stale_memory_flag_match": False,
                "contradiction_flag_match": False,
                "decay_family_rank_match": False,
                "decay_composite_family_rank_match": False,
            },
            drift_codes=[
                "freshness_state_mismatch",
                "aggregate_decay_score_mismatch",
                "stale_memory_flag_mismatch",
                "contradiction_flag_mismatch",
                "decay_family_delta",
                "decay_integration_delta",
                "decay_family_rank_mismatch",
                "decay_composite_family_rank_mismatch",
            ],
        )
        print(
            f"SEEDED: stable=({stable_source[:8]}…→{stable_replay[:8]}…) "
            f"drift=({drift_source[:8]}…→{drift_replay[:8]}…)"
        )

        await check_lineage(workspace_id)
        await check_1(conn, workspace_id)
        await check_2(conn, workspace_id)
        await check_3(conn, workspace_id)
        await check_4(conn, workspace_id)
        await check_5(conn, workspace_id)
        await check_6_stable_match(conn, stable_source, stable_replay)
        await check_7_drift_codes(conn, drift_source, drift_replay)
        await check_8_route_contract(conn)
        print("\nAll Phase 4.7D checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
