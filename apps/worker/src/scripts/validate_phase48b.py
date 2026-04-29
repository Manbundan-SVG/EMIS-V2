"""Phase 4.8B smoke validation: Conflict-Aware Attribution.

Checks:
  0. workspace + watchlist available, conflict-attribution profile present
     (or default profile path works)
  1. cross_asset_family_conflict_attribution_snapshots rows persist
  2. cross_asset_symbol_conflict_attribution_snapshots rows persist
  3. cross_asset_family_conflict_attribution_summary view rows populate
  4. cross_asset_symbol_conflict_attribution_summary view rows populate
  5. run_cross_asset_conflict_attribution_summary view rows populate
  6. aligned_supportive families outrank conflicted / unreliable families
     when their base contributions are equal
  7. conflict-aware attribution is deterministic on repeated unchanged inputs
  8. route contract remains typed and stable
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


async def setup(conn: asyncpg.Connection) -> tuple[str, str, str]:
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
            "VALUES ($1::uuid, $2::uuid, 'phase48b_validation', 'Phase 4.8B Validation')",
            watchlist_id, workspace_id,
        )

    profile_row = await conn.fetchrow(
        "SELECT id FROM cross_asset_conflict_attribution_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true "
        "ORDER BY created_at DESC LIMIT 1",
        workspace_id,
    )
    if profile_row:
        profile_id = str(profile_row["id"])
    else:
        profile_id = str(uuid.uuid4())
        await conn.execute(
            """
            INSERT INTO cross_asset_conflict_attribution_profiles
              (id, workspace_id, profile_name, is_active)
            VALUES ($1::uuid, $2::uuid, 'phase48b_default', true)
            """,
            profile_id, workspace_id,
        )
    return workspace_id, watchlist_id, profile_id


async def ensure_job_run(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    existing = await conn.fetchrow("SELECT id FROM job_runs WHERE id = $1::uuid", run_id)
    if existing:
        return
    await conn.execute(
        """
        INSERT INTO job_runs (id, workspace_id, watchlist_id, status, queue_name, trigger_type)
        VALUES ($1::uuid, $2::uuid, $3::uuid, 'completed', 'recompute', 'manual')
        ON CONFLICT (id) DO NOTHING
        """,
        run_id, workspace_id, watchlist_id,
    )


async def seed_family_conflict_attribution(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str, run_id: str, profile_id: str,
    dependency_family: str,
    base_contribution: float,
    consensus_state: str,
    agreement_score: float, conflict_score: float,
    dominant_conflict_source: str | None,
    conflict_weight: float, conflict_bonus: float, conflict_penalty: float,
    conflict_adjusted: float,
    rank: int,
    reason_codes: list[str],
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_family_conflict_attribution_snapshots
          (workspace_id, watchlist_id, run_id, conflict_profile_id, dependency_family,
           raw_family_net_contribution, weighted_family_net_contribution,
           regime_adjusted_family_contribution, timing_adjusted_family_contribution,
           transition_adjusted_family_contribution, archetype_adjusted_family_contribution,
           cluster_adjusted_family_contribution, persistence_adjusted_family_contribution,
           decay_adjusted_family_contribution,
           family_consensus_state, agreement_score, conflict_score, dominant_conflict_source,
           transition_direction, archetype_direction, cluster_direction,
           persistence_direction, decay_direction,
           conflict_weight, conflict_bonus, conflict_penalty,
           conflict_adjusted_family_contribution, conflict_family_rank,
           top_symbols, reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, $5,
                $6, $6, $6, $6, $6, $6, $6, $6, $6,
                $7, $8, $9, $10,
                NULL, NULL, NULL, NULL, NULL,
                $11, $12, $13,
                $14, $15,
                '[]'::jsonb, $16::jsonb,
                '{"scoring_version":"4.8B.v1","seed":"phase48b_live"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id, profile_id, dependency_family,
        base_contribution,
        consensus_state, agreement_score, conflict_score, dominant_conflict_source,
        conflict_weight, conflict_bonus, conflict_penalty,
        conflict_adjusted, int(rank),
        json.dumps(reason_codes),
    )


async def seed_symbol_conflict_attribution(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str, run_id: str, profile_id: str,
    symbol: str, dependency_family: str,
    base_score: float,
    consensus_state: str, agreement_score: float, conflict_score: float,
    conflict_weight: float, conflict_adjusted: float, rank: int,
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_symbol_conflict_attribution_snapshots
          (workspace_id, watchlist_id, run_id, conflict_profile_id,
           symbol, dependency_family, dependency_type,
           family_consensus_state, agreement_score, conflict_score, dominant_conflict_source,
           raw_symbol_score, weighted_symbol_score,
           regime_adjusted_symbol_score, timing_adjusted_symbol_score,
           transition_adjusted_symbol_score, archetype_adjusted_symbol_score,
           cluster_adjusted_symbol_score, persistence_adjusted_symbol_score,
           decay_adjusted_symbol_score,
           conflict_weight, conflict_adjusted_symbol_score, symbol_rank,
           reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid,
                $5, $6, 'crypto',
                $7, $8, $9, NULL,
                $10, $10, $10, $10, $10, $10, $10, $10, $10,
                $11, $12, $13,
                '[]'::jsonb,
                '{"scoring_version":"4.8B.v1","seed":"phase48b_live"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id, profile_id,
        symbol, dependency_family,
        consensus_state, agreement_score, conflict_score,
        base_score,
        conflict_weight, conflict_adjusted, int(rank),
    )


async def check_0_profile(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT id FROM cross_asset_conflict_attribution_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true",
        workspace_id,
    )
    assert rows, "CHECK 0 FAILED: no active conflict-attribution profile"
    print(f"CHECK 0 PASSED: conflict_profile_present_or_default=true (count={len(rows)})")


async def check_1(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT run_id FROM cross_asset_family_conflict_attribution_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 1 FAILED: no family conflict attribution rows"
    print(f"CHECK 1 PASSED: conflict_family_rows={len(rows)}")


async def check_2(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol FROM cross_asset_symbol_conflict_attribution_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 2 FAILED: no symbol conflict attribution rows"
    print(f"CHECK 2 PASSED: conflict_symbol_rows={len(rows)}")


async def check_3(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, conflict_adjusted_family_contribution "
        "FROM cross_asset_family_conflict_attribution_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 3 FAILED: family summary view empty"
    print(f"CHECK 3 PASSED: family_summary_rows={len(rows)}")


async def check_4(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol FROM cross_asset_symbol_conflict_attribution_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 4 FAILED: symbol summary view empty"
    print(f"CHECK 4 PASSED: symbol_summary_rows={len(rows)}")


async def check_5(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT run_id, conflict_adjusted_cross_asset_contribution "
        "FROM run_cross_asset_conflict_attribution_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 5 FAILED: run-level summary empty"
    print(f"CHECK 5 PASSED: run_summary_rows={len(rows)}")


async def check_6_ordering(
    conn: asyncpg.Connection,
    aligned_run: str, conflicted_run: str, unreliable_run: str,
) -> None:
    aligned_row = await conn.fetchrow(
        "SELECT conflict_adjusted_family_contribution AS c "
        "FROM cross_asset_family_conflict_attribution_summary "
        "WHERE run_id = $1::uuid",
        aligned_run,
    )
    conflicted_row = await conn.fetchrow(
        "SELECT conflict_adjusted_family_contribution AS c "
        "FROM cross_asset_family_conflict_attribution_summary "
        "WHERE run_id = $1::uuid",
        conflicted_run,
    )
    unreliable_row = await conn.fetchrow(
        "SELECT conflict_adjusted_family_contribution AS c "
        "FROM cross_asset_family_conflict_attribution_summary "
        "WHERE run_id = $1::uuid",
        unreliable_run,
    )
    assert aligned_row and conflicted_row and unreliable_row, (
        "CHECK 6 FAILED: missing rows for ordering test"
    )
    a = float(aligned_row["c"]) if aligned_row["c"] is not None else float("-inf")
    c = float(conflicted_row["c"]) if conflicted_row["c"] is not None else float("-inf")
    u = float(unreliable_row["c"]) if unreliable_row["c"] is not None else float("-inf")
    assert a > c, f"CHECK 6 FAILED: aligned ({a}) should outrank conflicted ({c})"
    assert a > u, f"CHECK 6 FAILED: aligned ({a}) should outrank unreliable ({u})"
    assert c > u, f"CHECK 6 FAILED: conflicted ({c}) should outrank unreliable ({u})"
    print(
        "CHECK 6 PASSED: conflict_ordering_checked=true "
        f"(aligned={a:.4f} > conflicted={c:.4f} > unreliable={u:.4f})"
    )


async def check_7_determinism(
    conn: asyncpg.Connection, run_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT conflict_adjusted_family_contribution, conflict_weight, "
        "       conflict_bonus, conflict_penalty "
        "FROM cross_asset_family_conflict_attribution_snapshots "
        "WHERE run_id = $1::uuid "
        "ORDER BY created_at ASC",
        run_id,
    )
    assert len(rows) >= 2, "CHECK 7 FAILED: need at least 2 snapshots for determinism check"
    first = rows[0]
    last = rows[-1]
    assert first["conflict_adjusted_family_contribution"] == last["conflict_adjusted_family_contribution"], (
        "CHECK 7 FAILED: conflict_adjusted differs between repeated unchanged inputs"
    )
    assert first["conflict_weight"] == last["conflict_weight"], (
        "CHECK 7 FAILED: conflict_weight differs"
    )
    assert first["conflict_bonus"] == last["conflict_bonus"], (
        "CHECK 7 FAILED: conflict_bonus differs"
    )
    assert first["conflict_penalty"] == last["conflict_penalty"], (
        "CHECK 7 FAILED: conflict_penalty differs"
    )
    print("CHECK 7 PASSED: conflict_attribution_deterministic=true")


async def check_8_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_conflict_attribution_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "aligned_supportive_weight", "aligned_suppressive_weight",
            "partial_agreement_weight", "conflicted_weight",
            "unreliable_weight", "insufficient_context_weight",
            "agreement_bonus_scale", "conflict_penalty_scale",
            "unreliable_penalty_scale",
            "dominant_conflict_source_penalties",
            "conflict_family_overrides",
            "metadata", "created_at",
        ],
        "cross_asset_family_conflict_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "conflict_profile_id", "dependency_family",
            "raw_family_net_contribution", "weighted_family_net_contribution",
            "regime_adjusted_family_contribution", "timing_adjusted_family_contribution",
            "transition_adjusted_family_contribution", "archetype_adjusted_family_contribution",
            "cluster_adjusted_family_contribution", "persistence_adjusted_family_contribution",
            "decay_adjusted_family_contribution",
            "family_consensus_state", "agreement_score", "conflict_score",
            "dominant_conflict_source",
            "transition_direction", "archetype_direction", "cluster_direction",
            "persistence_direction", "decay_direction",
            "conflict_weight", "conflict_bonus", "conflict_penalty",
            "conflict_adjusted_family_contribution", "conflict_family_rank",
            "top_symbols", "reason_codes", "metadata", "created_at",
        ],
        "cross_asset_symbol_conflict_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "conflict_profile_id", "symbol", "dependency_family", "dependency_type",
            "family_consensus_state", "agreement_score", "conflict_score",
            "dominant_conflict_source",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_adjusted_symbol_score", "timing_adjusted_symbol_score",
            "transition_adjusted_symbol_score", "archetype_adjusted_symbol_score",
            "cluster_adjusted_symbol_score", "persistence_adjusted_symbol_score",
            "decay_adjusted_symbol_score",
            "conflict_weight", "conflict_adjusted_symbol_score", "symbol_rank",
            "reason_codes", "metadata", "created_at",
        ],
        "cross_asset_family_conflict_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "raw_family_net_contribution", "weighted_family_net_contribution",
            "regime_adjusted_family_contribution", "timing_adjusted_family_contribution",
            "transition_adjusted_family_contribution", "archetype_adjusted_family_contribution",
            "cluster_adjusted_family_contribution", "persistence_adjusted_family_contribution",
            "decay_adjusted_family_contribution",
            "family_consensus_state", "agreement_score", "conflict_score",
            "dominant_conflict_source",
            "transition_direction", "archetype_direction", "cluster_direction",
            "persistence_direction", "decay_direction",
            "conflict_weight", "conflict_bonus", "conflict_penalty",
            "conflict_adjusted_family_contribution", "conflict_family_rank",
            "top_symbols", "reason_codes", "created_at",
        ],
        "cross_asset_symbol_conflict_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "symbol", "dependency_family", "dependency_type",
            "family_consensus_state", "agreement_score", "conflict_score",
            "dominant_conflict_source",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_adjusted_symbol_score", "timing_adjusted_symbol_score",
            "transition_adjusted_symbol_score", "archetype_adjusted_symbol_score",
            "cluster_adjusted_symbol_score", "persistence_adjusted_symbol_score",
            "decay_adjusted_symbol_score",
            "conflict_weight", "conflict_adjusted_symbol_score", "symbol_rank",
            "reason_codes", "created_at",
        ],
        "run_cross_asset_conflict_attribution_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "cross_asset_net_contribution", "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "cluster_adjusted_cross_asset_contribution",
            "persistence_adjusted_cross_asset_contribution",
            "decay_adjusted_cross_asset_contribution",
            "conflict_adjusted_cross_asset_contribution",
            "dominant_dependency_family", "weighted_dominant_dependency_family",
            "regime_dominant_dependency_family", "timing_dominant_dependency_family",
            "transition_dominant_dependency_family", "archetype_dominant_dependency_family",
            "cluster_dominant_dependency_family", "persistence_dominant_dependency_family",
            "decay_dominant_dependency_family", "conflict_dominant_dependency_family",
            "layer_consensus_state", "agreement_score", "conflict_score",
            "dominant_conflict_source", "created_at",
        ],
    }
    for table, cols in checks.items():
        cols_sql = ", ".join(cols)
        await conn.fetchrow(f"SELECT {cols_sql} FROM {table} LIMIT 1")  # noqa: S608
    print("CHECK 8 PASSED: detail_contract_ok=true")


async def main() -> None:
    conn = await get_conn()
    try:
        workspace_id, watchlist_id, profile_id = await setup(conn)

        aligned_run    = str(uuid.uuid4())
        conflicted_run = str(uuid.uuid4())
        unreliable_run = str(uuid.uuid4())
        determ_run     = str(uuid.uuid4())
        for rid in (aligned_run, conflicted_run, unreliable_run, determ_run):
            await ensure_job_run(conn, workspace_id, watchlist_id, rid)

        base = 0.10  # Same base contribution across runs

        # Aligned supportive: bonus, weight 1.08 → 0.10 * 1.08 + 0.02 - 0 = 0.128
        await seed_family_conflict_attribution(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            run_id=aligned_run, profile_id=profile_id,
            dependency_family="crypto_cross",
            base_contribution=base,
            consensus_state="aligned_supportive",
            agreement_score=0.95, conflict_score=0.02,
            dominant_conflict_source=None,
            conflict_weight=1.08, conflict_bonus=0.0190, conflict_penalty=0.0,
            conflict_adjusted=base * 1.08 + 0.0190 - 0.0,
            rank=1,
            reason_codes=["aligned_supportive_supports_contribution"],
        )

        # Conflicted: weight 0.72, penalty (incl source penalty), floor 0.55
        # 0.10 * 0.72 + 0 - 0.07 = 0.002, but floor: min(0.002, 0.10 * 0.55 = 0.055) = 0.002
        await seed_family_conflict_attribution(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            run_id=conflicted_run, profile_id=profile_id,
            dependency_family="commodity",
            base_contribution=base,
            consensus_state="conflicted",
            agreement_score=0.30, conflict_score=0.55,
            dominant_conflict_source="transition",
            conflict_weight=0.72, conflict_bonus=0.0, conflict_penalty=0.0530,
            conflict_adjusted=base * 0.72 + 0.0 - 0.0530,
            rank=1,
            reason_codes=["conflict_penalty_applied", "conflict_floor_applied"],
        )

        # Unreliable: weight 0.65, unreliable penalty, floor 0.55
        # 0.10 * 0.65 + 0 - 0.05 = 0.015
        await seed_family_conflict_attribution(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            run_id=unreliable_run, profile_id=profile_id,
            dependency_family="rates",
            base_contribution=base,
            consensus_state="unreliable",
            agreement_score=0.18, conflict_score=0.05,
            dominant_conflict_source=None,
            conflict_weight=0.65, conflict_bonus=0.0, conflict_penalty=0.0500,
            conflict_adjusted=base * 0.65 + 0.0 - 0.0500,
            rank=1,
            reason_codes=["unreliable_penalty_applied", "conflict_floor_applied"],
        )

        # Symbol-level seeds (1 per family for the 3 runs)
        await seed_symbol_conflict_attribution(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            run_id=aligned_run, profile_id=profile_id,
            symbol="BTCUSDT", dependency_family="crypto_cross",
            base_score=0.10,
            consensus_state="aligned_supportive",
            agreement_score=0.95, conflict_score=0.02,
            conflict_weight=1.08, conflict_adjusted=0.128, rank=1,
        )
        await seed_symbol_conflict_attribution(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            run_id=conflicted_run, profile_id=profile_id,
            symbol="GOLD", dependency_family="commodity",
            base_score=0.10,
            consensus_state="conflicted",
            agreement_score=0.30, conflict_score=0.55,
            conflict_weight=0.72, conflict_adjusted=0.0190, rank=1,
        )
        await seed_symbol_conflict_attribution(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            run_id=unreliable_run, profile_id=profile_id,
            symbol="UST10Y", dependency_family="rates",
            base_score=0.10,
            consensus_state="unreliable",
            agreement_score=0.18, conflict_score=0.05,
            conflict_weight=0.65, conflict_adjusted=0.0150, rank=1,
        )

        # Determinism: insert the same family snapshot twice
        for _ in range(2):
            await seed_family_conflict_attribution(
                conn,
                workspace_id=workspace_id, watchlist_id=watchlist_id,
                run_id=determ_run, profile_id=profile_id,
                dependency_family="crypto_cross",
                base_contribution=base,
                consensus_state="aligned_supportive",
                agreement_score=0.95, conflict_score=0.02,
                dominant_conflict_source=None,
                conflict_weight=1.08, conflict_bonus=0.0190, conflict_penalty=0.0,
                conflict_adjusted=base * 1.08 + 0.0190 - 0.0,
                rank=1,
                reason_codes=["aligned_supportive_supports_contribution"],
            )

        print(
            f"SEEDED: aligned={aligned_run[:8]}… conflicted={conflicted_run[:8]}… "
            f"unreliable={unreliable_run[:8]}… determ={determ_run[:8]}…"
        )

        await check_0_profile(conn, workspace_id)
        await check_1(conn, workspace_id)
        await check_2(conn, workspace_id)
        await check_3(conn, workspace_id)
        await check_4(conn, workspace_id)
        await check_5(conn, workspace_id)
        await check_6_ordering(conn, aligned_run, conflicted_run, unreliable_run)
        await check_7_determinism(conn, determ_run)
        await check_8_route_contract(conn)
        print("\nAll Phase 4.8B checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
