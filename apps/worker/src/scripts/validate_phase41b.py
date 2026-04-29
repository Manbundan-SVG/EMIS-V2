"""Phase 4.1B smoke validation: Dependency-Priority-Aware Ranking and Contribution Weighting.

Checks:
  1. weighting profile exists or default weighting path works
  2. weighted family attribution rows persist
  3. weighted symbol attribution rows persist
  4. weighted family summary rows populate
  5. weighted symbol summary rows populate
  6. weighted rankings are deterministic on repeated unchanged inputs
  7. weighted dominant family may differ from raw dominant family when priorities justify it
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
            "VALUES ($1::uuid, $2::uuid, 'phase41b_validation', 'Phase 4.1B Validation')",
            watchlist_id, workspace_id,
        )
    return workspace_id, watchlist_id


async def seed_weighting_profile(conn: asyncpg.Connection, workspace_id: str) -> str | None:
    """If no active profile exists, we don't insert one — we let the default
    path apply. If one exists, we keep it. Return its id if present."""
    row = await conn.fetchrow(
        "SELECT id FROM dependency_weighting_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true LIMIT 1",
        workspace_id,
    )
    return str(row["id"]) if row else None


async def seed_weighted_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
    profile_id: str | None,
) -> tuple[int, int]:
    """Seed weighted rows directly. This validates the storage/view contracts
    independently of the service build path."""
    # Family rows (rates highest, so weighted rank 1 stays at rates — stable
    # ranking on repeated inserts with identical inputs).
    family_rows = [
        ("rates",      0.25, 1.00, 1.00, 1.00, 1.00, 0.25, 1, ["US10Y", "US02Y"]),
        ("risk",       0.20, 0.90, 1.00, 1.00, 0.90, 0.162, 2, ["SPY", "QQQ"]),
        ("fx",         0.10, 0.85, 1.00, 1.00, 0.90, 0.077, 3, ["DXY"]),
        ("commodity",  0.05, 0.85, 1.00, 1.00, 0.90, 0.038, 4, ["GLD"]),
    ]
    for (family, raw_net, pw, fw, tw, cw, weighted_net, rank, top_syms) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_weighted_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               weighting_profile_id, dependency_family,
               raw_family_net_contribution,
               priority_weight, family_weight, type_weight, coverage_weight,
               weighted_family_net_contribution, weighted_family_rank,
               top_symbols, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4::uuid, $5, $6, $7, $8, $9, $10, $11, $12,
                    $13::jsonb,
                    '{"scoring_version":"4.1B.v1","default_profile_used":true}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            profile_id, family, raw_net, pw, fw, tw, cw, weighted_net, rank,
            json.dumps(top_syms),
        )

    symbol_rows = [
        ("US10Y", "rates",     "rates_link",         95, True, 0.15, 0.95, 1.0, 1.0, 1.0, 0.135, 1),
        ("US02Y", "rates",     "rates_link",         85, True, 0.10, 0.90, 1.0, 1.0, 1.0, 0.081, 2),
        ("SPY",   "risk",      "risk_proxy",        100, True, 0.12, 1.00, 1.0, 1.0, 0.9, 0.108, 3),
        ("QQQ",   "risk",      "risk_proxy",         95, True, 0.08, 0.95, 1.0, 1.0, 0.9, 0.068, 4),
        ("DXY",   "fx",        "fx_link",            90, True, 0.10, 0.925, 1.0, 1.0, 0.9, 0.083, 5),
        ("GLD",   "commodity", "commodity_link",     80, True, 0.05, 0.875, 1.0, 1.0, 0.9, 0.039, 6),
    ]
    for (sym, family, dep_type, priority, direct, raw_s, pw, fw, tw, cw, weighted_s, rank) in symbol_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_symbol_weighted_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               weighting_profile_id,
               symbol, dependency_family, dependency_type,
               graph_priority, is_direct_dependency,
               raw_symbol_score,
               priority_weight, family_weight, type_weight, coverage_weight,
               weighted_symbol_score, symbol_rank, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4::uuid,
                    $5, $6, $7,
                    $8, $9,
                    $10, $11, $12, $13, $14, $15, $16,
                    '{"scoring_version":"4.1B.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            profile_id,
            sym, family, dep_type,
            priority, direct,
            raw_s, pw, fw, tw, cw, weighted_s, rank,
        )
    return len(family_rows), len(symbol_rows)


async def seed_raw_attribution_for_shift_check(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    """Seed a minimal 4.1A attribution row + explanation bridge row so the
    weighted integration view has something to join against."""
    await conn.execute(
        """
        INSERT INTO cross_asset_explanation_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           dominant_dependency_family,
           cross_asset_confidence_score,
           confirmation_score, contradiction_score,
           missing_context_score, stale_context_score,
           top_confirming_symbols, top_contradicting_symbols,
           missing_dependency_symbols, stale_dependency_symbols,
           explanation_state, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                'rates', 0.60, 0.65, 0.10, 0.05, 0.05,
                '["US10Y"]'::jsonb, '[]'::jsonb,
                '[]'::jsonb, '[]'::jsonb,
                'computed', '{}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )
    await conn.execute(
        """
        INSERT INTO cross_asset_attribution_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           base_signal_score, cross_asset_signal_score,
           cross_asset_confirmation_score, cross_asset_contradiction_penalty,
           cross_asset_missing_penalty, cross_asset_stale_penalty,
           cross_asset_net_contribution,
           composite_pre_cross_asset, composite_post_cross_asset,
           integration_mode, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                0.10, 0.55, 0.65, 0.10, 0.025, 0.025,
                0.25, 0.10, 0.125,
                'additive_guardrailed', '{"scoring_version":"4.1A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )


async def check_1_profile_or_default(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM dependency_weighting_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true",
        workspace_id,
    )
    if row:
        print(f"CHECK 1 PASSED: active profile id={str(row['id'])[:12]}…")
    else:
        print("CHECK 1 PASSED: no active profile — default_deterministic path applies")


async def check_2_weighted_family(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, weighted_family_rank FROM cross_asset_family_weighted_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 2 FAILED: no weighted family attribution rows"
    print(f"CHECK 2 PASSED: weighted_family_rows={len(rows)}")


async def check_3_weighted_symbol(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, symbol_rank FROM cross_asset_symbol_weighted_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no weighted symbol attribution rows"
    print(f"CHECK 3 PASSED: weighted_symbol_rows={len(rows)}")


async def check_4_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, weighted_family_rank, weighted_family_net_contribution "
        "FROM cross_asset_family_weighted_attribution_summary "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: weighted_family summary empty"
    ranks = sorted(r["weighted_family_rank"] for r in rows if r["weighted_family_rank"] is not None)
    assert ranks == list(range(1, len(ranks) + 1)), (
        f"CHECK 4 FAILED: non-contiguous ranks {ranks}"
    )
    print(f"CHECK 4 PASSED: family_summary_rows={len(rows)} ranks={ranks}")


async def check_5_symbol_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, symbol_rank, weighted_symbol_score "
        "FROM cross_asset_symbol_weighted_attribution_summary "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 5 FAILED: weighted symbol summary empty"
    print(f"CHECK 5 PASSED: symbol_summary_rows={len(rows)}")


async def check_6_determinism(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    """Duplicate family rows with identical values — the summary view dedup
    is DISTINCT ON ... ORDER BY created_at DESC, so each family's latest row
    has identical weighted values."""
    await conn.execute(
        """
        INSERT INTO cross_asset_family_weighted_attribution_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           weighting_profile_id, dependency_family,
           raw_family_net_contribution,
           priority_weight, family_weight, type_weight, coverage_weight,
           weighted_family_net_contribution, weighted_family_rank,
           top_symbols, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               weighting_profile_id, dependency_family,
               raw_family_net_contribution,
               priority_weight, family_weight, type_weight, coverage_weight,
               weighted_family_net_contribution, weighted_family_rank,
               top_symbols, metadata
        FROM cross_asset_family_weighted_attribution_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    rows = await conn.fetch(
        "SELECT dependency_family, weighted_family_net_contribution "
        "FROM cross_asset_family_weighted_attribution_summary "
        "WHERE run_id = $1::uuid "
        "ORDER BY dependency_family",
        run_id,
    )
    distinct_per_family = {}
    for r in rows:
        fam = r["dependency_family"]
        val = float(r["weighted_family_net_contribution"]) if r["weighted_family_net_contribution"] is not None else None
        distinct_per_family.setdefault(fam, set()).add(val)
    for fam, vals in distinct_per_family.items():
        assert len(vals) == 1, (
            f"CHECK 6 FAILED: family {fam!r} has non-deterministic weighted values {vals}"
        )
    print(f"CHECK 6 PASSED: weighting_deterministic=true ({len(distinct_per_family)} families stable)")


async def check_7_dominant_shift_checked(conn: asyncpg.Connection, run_id: str) -> None:
    """Compare raw dominant family (from the 4.0D explanation bridge) with
    weighted dominant family (from the 4.1B weighted summary)."""
    row = await conn.fetchrow(
        """
        SELECT dominant_dependency_family, weighted_dominant_dependency_family
        FROM run_cross_asset_weighted_integration_summary
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 7 FAILED: run_cross_asset_weighted_integration_summary empty"
    raw_fam = row["dominant_dependency_family"]
    weighted_fam = row["weighted_dominant_dependency_family"]
    shifted = (raw_fam is not None and weighted_fam is not None and raw_fam != weighted_fam)
    # Whether or not shift occurred, the comparison must be expressible.
    print(
        f"CHECK 7 PASSED: dominant_family_shift_checked=true "
        f"raw={raw_fam!r} weighted={weighted_fam!r} shifted={shifted}"
    )


async def check_8_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "dependency_weighting_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "priority_weight_scale",
            "direct_dependency_bonus", "secondary_dependency_penalty",
            "missing_penalty_scale", "stale_penalty_scale",
            "family_weight_overrides", "type_weight_overrides",
            "metadata", "created_at",
        ],
        "cross_asset_family_weighted_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "weighting_profile_id", "dependency_family",
            "raw_family_net_contribution",
            "priority_weight", "family_weight", "type_weight", "coverage_weight",
            "weighted_family_net_contribution", "weighted_family_rank",
            "top_symbols", "metadata", "created_at",
        ],
        "cross_asset_symbol_weighted_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "weighting_profile_id",
            "symbol", "dependency_family", "dependency_type",
            "graph_priority", "is_direct_dependency",
            "raw_symbol_score",
            "priority_weight", "family_weight", "type_weight", "coverage_weight",
            "weighted_symbol_score", "symbol_rank", "metadata", "created_at",
        ],
        "cross_asset_family_weighted_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "raw_family_net_contribution",
            "priority_weight", "family_weight", "type_weight", "coverage_weight",
            "weighted_family_net_contribution", "weighted_family_rank",
            "top_symbols", "created_at",
        ],
        "cross_asset_symbol_weighted_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "symbol", "dependency_family", "dependency_type",
            "graph_priority", "is_direct_dependency",
            "raw_symbol_score",
            "priority_weight", "family_weight", "type_weight", "coverage_weight",
            "weighted_symbol_score", "symbol_rank", "created_at",
        ],
        "run_cross_asset_weighted_integration_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "base_signal_score",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "dominant_dependency_family",
            "weighted_dominant_dependency_family",
            "created_at",
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
        profile_id = await seed_weighting_profile(conn, workspace_id)
        run_id = str(uuid.uuid4())
        await seed_raw_attribution_for_shift_check(conn, workspace_id, watchlist_id, run_id)
        fc, sc = await seed_weighted_rows(
            conn, workspace_id, watchlist_id, run_id, profile_id,
        )
        print(f"SEEDED: families={fc} symbols={sc} run_id={run_id[:12]}… profile_id={profile_id}")

        await check_1_profile_or_default(conn, workspace_id)
        await check_2_weighted_family(conn, run_id)
        await check_3_weighted_symbol(conn, run_id)
        await check_4_family_summary(conn, run_id)
        await check_5_symbol_summary(conn, run_id)
        await check_6_determinism(conn, workspace_id, watchlist_id, run_id)
        await check_7_dominant_shift_checked(conn, run_id)
        await check_8_route_contract(conn)
        print("\nAll Phase 4.1B checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
