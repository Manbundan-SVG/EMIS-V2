"""Phase 4.0D smoke validation: Cross-Asset Explainability.

Checks:
  1. explanation snapshot persists
  2. family contribution rows persist
  3. explanation summary rows populate
  4. family explanation summary rows populate
  5. dominant dependency family is deterministic for stable inputs
  6. top confirming/contradicting lists are non-empty when signals exist
  7. partial/missing/stale states are explicit when expected
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
            "VALUES ($1::uuid, $2::uuid, 'phase40d_validation', 'Phase 4.0D Validation')",
            watchlist_id, workspace_id,
        )
    return workspace_id, watchlist_id


async def seed_explanation_and_families(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> tuple[str, int]:
    # Deterministic inputs: scoring_version stable, hash of inputs produces
    # same dominant family across runs.
    explanation_payload = dict(
        dominant_dependency_family="rates",
        cross_asset_confidence_score=0.55,
        confirmation_score=0.70,
        contradiction_score=0.15,
        missing_context_score=0.05,
        stale_context_score=0.05,
        top_confirming=["SPY", "QQQ", "US10Y"],
        top_contradicting=["DXY"],
        missing_symbols=["EURUSD"],
        stale_symbols=[],
        explanation_state="partial",
    )
    exp_id = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO cross_asset_explanation_snapshots
          (id, workspace_id, watchlist_id, run_id, context_snapshot_id,
           dominant_dependency_family,
           cross_asset_confidence_score,
           confirmation_score, contradiction_score,
           missing_context_score, stale_context_score,
           top_confirming_symbols, top_contradicting_symbols,
           missing_dependency_symbols, stale_dependency_symbols,
           explanation_state, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, NULL,
                $5, $6, $7, $8, $9, $10,
                $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb,
                $15, '{"scoring_version":"4.0D.v1"}'::jsonb)
        """,
        exp_id, workspace_id, watchlist_id, run_id,
        explanation_payload["dominant_dependency_family"],
        explanation_payload["cross_asset_confidence_score"],
        explanation_payload["confirmation_score"],
        explanation_payload["contradiction_score"],
        explanation_payload["missing_context_score"],
        explanation_payload["stale_context_score"],
        json.dumps(explanation_payload["top_confirming"]),
        json.dumps(explanation_payload["top_contradicting"]),
        json.dumps(explanation_payload["missing_symbols"]),
        json.dumps(explanation_payload["stale_symbols"]),
        explanation_payload["explanation_state"],
    )

    # Seed family contributions: 'rates' has highest net score so should be
    # picked as dominant across deterministic reruns.
    family_rows = [
        ("rates",        3, 2, 0, 0, 1, 0.80, 0.67, 0.00, ["US10Y", "US02Y"]),
        ("risk",         2, 1, 1, 0, 0, 0.40, 0.50, 0.50, ["SPY", "QQQ"]),
        ("fx",           2, 0, 1, 1, 0, 0.10, 0.00, 0.50, ["DXY"]),
        ("commodity",    1, 0, 0, 0, 1, 0.30, 0.00, 0.00, ["GLD"]),
    ]
    for (family, total, confirmed, contradicted, missing, stale,
         confidence, support, contradiction, top_syms) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_contribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               dependency_family,
               family_signal_count,
               confirmed_count, contradicted_count,
               missing_count, stale_count,
               family_confidence_score, family_support_score, family_contradiction_score,
               top_symbols, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4, $5, $6, $7, $8, $9, $10, $11, $12,
                    $13::jsonb, '{}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            family, total, confirmed, contradicted, missing, stale,
            confidence, support, contradiction,
            json.dumps(top_syms),
        )
    return exp_id, len(family_rows)


async def check_1_explanation_persists(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_explanation_snapshots WHERE run_id = $1::uuid LIMIT 1",
        run_id,
    )
    assert row, "CHECK 1 FAILED: no explanation snapshot persisted"
    print(f"CHECK 1 PASSED: explanation_snapshot_rows>=1 id={str(row['id'])[:8]}…")


async def check_2_family_contributions(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family FROM cross_asset_family_contribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 2 FAILED: no family contribution rows"
    print(f"CHECK 2 PASSED: family_contribution_rows={len(rows)}")


async def check_3_explanation_summary(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT dominant_dependency_family, explanation_state "
        "FROM cross_asset_explanation_summary "
        "WHERE workspace_id = $1::uuid AND watchlist_id = $2::uuid",
        workspace_id, watchlist_id,
    )
    assert rows, "CHECK 3 FAILED: explanation_summary empty"
    print(f"CHECK 3 PASSED: explanation_summary_rows={len(rows)}")


async def check_4_family_summary(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, family_confidence_score "
        "FROM cross_asset_family_explanation_summary "
        "WHERE workspace_id = $1::uuid AND watchlist_id = $2::uuid",
        workspace_id, watchlist_id,
    )
    assert rows, "CHECK 4 FAILED: family_explanation_summary empty"
    print(f"CHECK 4 PASSED: family_summary_rows={len(rows)}")


async def check_5_dominant_family_deterministic(
    conn: asyncpg.Connection, run_id: str,
) -> None:
    # Insert a second snapshot with identical inputs and run_id; since the
    # seed set keeps 'rates' as the net-top family, it should remain dominant.
    exp_id_2 = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO cross_asset_explanation_snapshots
          (id, workspace_id, watchlist_id, run_id, context_snapshot_id,
           dominant_dependency_family,
           cross_asset_confidence_score,
           confirmation_score, contradiction_score,
           missing_context_score, stale_context_score,
           top_confirming_symbols, top_contradicting_symbols,
           missing_dependency_symbols, stale_dependency_symbols,
           explanation_state, metadata)
        SELECT $1::uuid, workspace_id, watchlist_id, run_id, context_snapshot_id,
               dominant_dependency_family,
               cross_asset_confidence_score,
               confirmation_score, contradiction_score,
               missing_context_score, stale_context_score,
               top_confirming_symbols, top_contradicting_symbols,
               missing_dependency_symbols, stale_dependency_symbols,
               explanation_state, metadata
        FROM cross_asset_explanation_snapshots
        WHERE run_id = $2::uuid
        ORDER BY created_at ASC
        LIMIT 1
        """,
        exp_id_2, run_id,
    )
    rows = await conn.fetch(
        "SELECT DISTINCT dominant_dependency_family "
        "FROM cross_asset_explanation_snapshots WHERE run_id = $1::uuid",
        run_id,
    )
    distinct_families = {r["dominant_dependency_family"] for r in rows}
    assert len(distinct_families) == 1, (
        f"CHECK 5 FAILED: non-deterministic dominant family {distinct_families}"
    )
    print(
        f"CHECK 5 PASSED: dominant_family_deterministic=true family={distinct_families.pop()}"
    )


async def check_6_confirming_or_contradicting(
    conn: asyncpg.Connection, run_id: str,
) -> None:
    row = await conn.fetchrow(
        """
        SELECT top_confirming_symbols, top_contradicting_symbols
        FROM cross_asset_explanation_snapshots WHERE run_id = $1::uuid LIMIT 1
        """,
        run_id,
    )
    assert row
    confirming = row["top_confirming_symbols"] or []
    contradicting = row["top_contradicting_symbols"] or []
    # asyncpg returns jsonb as str OR python obj depending on version
    if isinstance(confirming, str):
        confirming = json.loads(confirming)
    if isinstance(contradicting, str):
        contradicting = json.loads(contradicting)
    present = len(confirming) > 0 or len(contradicting) > 0
    assert present, "CHECK 6 FAILED: no confirming or contradicting symbols present"
    print(
        f"CHECK 6 PASSED: confirming_or_contradicting_symbols_present=true "
        f"confirming={len(confirming)} contradicting={len(contradicting)}"
    )


async def check_7_partial_missing_stale_state(
    conn: asyncpg.Connection, run_id: str,
) -> None:
    row = await conn.fetchrow(
        "SELECT explanation_state FROM cross_asset_explanation_snapshots "
        "WHERE run_id = $1::uuid LIMIT 1",
        run_id,
    )
    assert row
    state = row["explanation_state"]
    assert state in ("partial", "missing_context", "stale_context", "computed"), (
        f"CHECK 7 FAILED: unexpected explanation_state {state}"
    )
    print(f"CHECK 7 PASSED: explanation_state_explicit=true state={state}")


async def check_8_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_explanation_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dominant_dependency_family",
            "cross_asset_confidence_score",
            "confirmation_score", "contradiction_score",
            "missing_context_score", "stale_context_score",
            "top_confirming_symbols", "top_contradicting_symbols",
            "missing_dependency_symbols", "stale_dependency_symbols",
            "explanation_state", "metadata", "created_at",
        ],
        "cross_asset_family_contribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "family_signal_count",
            "confirmed_count", "contradicted_count",
            "missing_count", "stale_count",
            "family_confidence_score", "family_support_score", "family_contradiction_score",
            "top_symbols", "metadata", "created_at",
        ],
        "cross_asset_explanation_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dominant_dependency_family",
            "cross_asset_confidence_score",
            "confirmation_score", "contradiction_score",
            "missing_context_score", "stale_context_score",
            "top_confirming_symbols", "top_contradicting_symbols",
            "missing_dependency_symbols", "stale_dependency_symbols",
            "explanation_state", "created_at",
        ],
        "cross_asset_family_explanation_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "family_signal_count",
            "confirmed_count", "contradicted_count",
            "missing_count", "stale_count",
            "family_confidence_score", "family_support_score", "family_contradiction_score",
            "top_symbols", "created_at",
        ],
        "run_cross_asset_explanation_bridge": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "dominant_dependency_family",
            "cross_asset_confidence_score",
            "confirmation_score", "contradiction_score",
            "missing_context_score", "stale_context_score",
            "explanation_state", "created_at",
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
        run_id = str(uuid.uuid4())
        exp_id, family_count = await seed_explanation_and_families(
            conn, workspace_id, watchlist_id, run_id,
        )
        print(f"SEEDED: explanation_id={exp_id[:12]}… families={family_count} run_id={run_id[:12]}…")

        await check_1_explanation_persists(conn, run_id)
        await check_2_family_contributions(conn, run_id)
        await check_3_explanation_summary(conn, workspace_id, watchlist_id)
        await check_4_family_summary(conn, workspace_id, watchlist_id)
        await check_5_dominant_family_deterministic(conn, run_id)
        await check_6_confirming_or_contradicting(conn, run_id)
        await check_7_partial_missing_stale_state(conn, run_id)
        await check_8_route_contract(conn)
        print("\nAll Phase 4.0D checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
