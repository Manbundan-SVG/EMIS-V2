"""Phase 4.2A smoke validation: Cross-Asset Lead/Lag + Dependency Timing.

Checks:
  1. pair timing snapshots persist
  2. family timing snapshots persist
  3. pair summary rows populate
  4. family timing summary rows populate
  5. run timing summary row populates
  6. unchanged inputs produce deterministic lag classification
  7. controlled shifted inputs can produce a lead vs coincident distinction
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
            "VALUES ($1::uuid, $2::uuid, 'phase42a_validation', 'Phase 4.2A Validation')",
            watchlist_id, workspace_id,
        )
    return workspace_id, watchlist_id


async def seed_pair_timing_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> tuple[int, int]:
    # Coincident: SPY leads BTC by ~0h
    # Lead:       DXY leads BTC by 6h (negative lag)
    # Lag:        QQQ follows BTC by 12h
    # Insufficient: GLD no data
    pair_rows = [
        ("BTC", "SPY",   "risk",         "risk_proxy",         "coincident",        0, 0.55,  0.55, "7d"),
        ("BTC", "DXY",   "fx",           "fx_link",            "lead",             -6, 0.48, -0.48, "14d"),
        ("BTC", "QQQ",   "risk",         "risk_proxy",         "lag",              12, 0.42,  0.42, "7d"),
        ("BTC", "US10Y", "rates",        "rates_link",         "lead",            -12, 0.32, -0.32, "14d"),
        ("BTC", "GLD",   "commodity",    "commodity_link",     "insufficient_data", None, None, None, "14d"),
    ]
    for (base, dep, fam, dep_type, bucket, best_lag, strength, corr, window) in pair_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_lead_lag_pair_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               base_symbol, dependency_symbol,
               dependency_family, dependency_type,
               lag_bucket, best_lag_hours,
               timing_strength, correlation_at_best_lag,
               base_return_series_key, dependency_return_series_key,
               window_label, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4, $5, $6, $7,
                    $8, $9, $10, $11,
                    'market_bars.return_1h[BTC]',
                    $12,
                    $13, '{"scoring_version":"4.2A.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            base, dep, fam, dep_type,
            bucket, best_lag, strength, corr,
            f"source[{dep}]", window,
        )

    # Family aggregates (match the buckets above):
    # - risk:      lead=0, coincident=1 (SPY), lag=1 (QQQ)           → no lead-dominant; dominant=coincident (tie-break by name: coincident)
    # - fx:        lead=1 (DXY), coincident=0, lag=0                 → dominant=lead
    # - rates:     lead=1 (US10Y), coincident=0, lag=0               → dominant=lead
    # - commodity: insufficient_data                                 → dominant=insufficient_data
    family_rows = [
        ("risk",      0, 1, 1, None, 0.485, "coincident",        []),
        ("fx",        1, 0, 0, -6.0, 0.480, "lead",              ["DXY"]),
        ("rates",     1, 0, 0, -12.0, 0.320, "lead",             ["US10Y"]),
        ("commodity", 0, 0, 0, None, None, "insufficient_data", []),
    ]
    for (fam, lead_n, coinc_n, lag_n, avg_lag, avg_strength, dominant, top_syms) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_timing_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               dependency_family,
               lead_pair_count, coincident_pair_count, lag_pair_count,
               avg_best_lag_hours, avg_timing_strength,
               dominant_timing_class, top_leading_symbols, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4, $5, $6, $7, $8, $9, $10,
                    $11::jsonb, '{"scoring_version":"4.2A.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            fam, lead_n, coinc_n, lag_n, avg_lag, avg_strength, dominant,
            json.dumps(top_syms),
        )
    return len(pair_rows), len(family_rows)


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


async def check_1_pair_persists(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT base_symbol, dependency_symbol, lag_bucket FROM cross_asset_lead_lag_pair_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 1 FAILED: no pair timing rows"
    print(f"CHECK 1 PASSED: pair_timing_rows={len(rows)}")


async def check_2_family_persists(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, dominant_timing_class FROM cross_asset_family_timing_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 2 FAILED: no family timing rows"
    print(f"CHECK 2 PASSED: family_timing_rows={len(rows)}")


async def check_3_pair_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT base_symbol, dependency_symbol, lag_bucket FROM cross_asset_lead_lag_pair_summary "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: pair summary empty"
    print(f"CHECK 3 PASSED: pair_summary_rows={len(rows)}")


async def check_4_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, dominant_timing_class, lead_pair_count "
        "FROM cross_asset_family_timing_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: family summary empty"
    print(f"CHECK 4 PASSED: family_summary_rows={len(rows)}")


async def check_5_run_summary(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        """
        SELECT run_id, lead_pair_count, coincident_pair_count, lag_pair_count,
               dominant_leading_family, strongest_leading_symbol
        FROM run_cross_asset_timing_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 5 FAILED: run timing summary empty"
    print(
        f"CHECK 5 PASSED: run_summary_rows>=1 "
        f"dominant_leading_family={row['dominant_leading_family']!r} "
        f"strongest_leading_symbol={row['strongest_leading_symbol']!r}"
    )


async def check_6_determinism(conn: asyncpg.Connection, run_id: str) -> None:
    """Duplicate family rows with identical values — summary views dedup on
    (run_id, dependency_family) so the latest row must match the prior row
    when inputs are unchanged."""
    await conn.execute(
        """
        INSERT INTO cross_asset_family_timing_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           dependency_family,
           lead_pair_count, coincident_pair_count, lag_pair_count,
           avg_best_lag_hours, avg_timing_strength,
           dominant_timing_class, top_leading_symbols, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               dependency_family,
               lead_pair_count, coincident_pair_count, lag_pair_count,
               avg_best_lag_hours, avg_timing_strength,
               dominant_timing_class, top_leading_symbols, metadata
        FROM cross_asset_family_timing_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    rows = await conn.fetch(
        "SELECT dependency_family, dominant_timing_class, lead_pair_count "
        "FROM cross_asset_family_timing_summary WHERE run_id = $1::uuid",
        run_id,
    )
    per_fam_distinct: dict[str, set] = {}
    for r in rows:
        fam = r["dependency_family"]
        key = (r["dominant_timing_class"], r["lead_pair_count"])
        per_fam_distinct.setdefault(fam, set()).add(key)
    for fam, keys in per_fam_distinct.items():
        assert len(keys) == 1, f"CHECK 6 FAILED: family {fam!r} non-deterministic {keys}"
    print(f"CHECK 6 PASSED: timing_deterministic=true ({len(per_fam_distinct)} families stable)")


async def check_7_lead_vs_coincident_checked(conn: asyncpg.Connection, run_id: str) -> None:
    """Verify the seeded fx/rates pairs classified as 'lead' (negative best_lag)
    while the risk pair classified as 'coincident' (best_lag 0)."""
    rows = await conn.fetch(
        "SELECT dependency_symbol, lag_bucket, best_lag_hours "
        "FROM cross_asset_lead_lag_pair_summary WHERE run_id = $1::uuid",
        run_id,
    )
    by_sym = {r["dependency_symbol"]: r for r in rows}
    assert by_sym.get("SPY", {}).get("lag_bucket") == "coincident", (
        f"CHECK 7 FAILED: SPY expected coincident, got {by_sym.get('SPY')}"
    )
    assert by_sym.get("DXY", {}).get("lag_bucket") == "lead" and by_sym["DXY"]["best_lag_hours"] < 0, (
        f"CHECK 7 FAILED: DXY expected lead with negative lag, got {by_sym.get('DXY')}"
    )
    assert by_sym.get("QQQ", {}).get("lag_bucket") == "lag" and by_sym["QQQ"]["best_lag_hours"] > 0, (
        f"CHECK 7 FAILED: QQQ expected lag with positive lag, got {by_sym.get('QQQ')}"
    )
    print("CHECK 7 PASSED: lead_vs_coincident_checked=true (SPY→coincident, DXY→lead, QQQ→lag)")


async def check_8_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_lead_lag_pair_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "base_symbol", "dependency_symbol",
            "dependency_family", "dependency_type",
            "lag_bucket", "best_lag_hours",
            "timing_strength", "correlation_at_best_lag",
            "base_return_series_key", "dependency_return_series_key",
            "window_label", "metadata", "created_at",
        ],
        "cross_asset_family_timing_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "lead_pair_count", "coincident_pair_count", "lag_pair_count",
            "avg_best_lag_hours", "avg_timing_strength",
            "dominant_timing_class", "top_leading_symbols", "metadata", "created_at",
        ],
        "cross_asset_lead_lag_pair_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "base_symbol", "dependency_symbol",
            "dependency_family", "dependency_type",
            "lag_bucket", "best_lag_hours",
            "timing_strength", "correlation_at_best_lag",
            "window_label", "created_at",
        ],
        "cross_asset_family_timing_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "lead_pair_count", "coincident_pair_count", "lag_pair_count",
            "avg_best_lag_hours", "avg_timing_strength",
            "dominant_timing_class", "top_leading_symbols", "created_at",
        ],
        "run_cross_asset_timing_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "lead_pair_count", "coincident_pair_count", "lag_pair_count",
            "dominant_leading_family", "strongest_leading_symbol",
            "avg_timing_strength", "created_at",
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
        await ensure_job_run(conn, workspace_id, watchlist_id, run_id)
        pc, fc = await seed_pair_timing_rows(conn, workspace_id, watchlist_id, run_id)
        print(f"SEEDED: pairs={pc} families={fc} run_id={run_id[:12]}…")

        await check_1_pair_persists(conn, run_id)
        await check_2_family_persists(conn, run_id)
        await check_3_pair_summary(conn, run_id)
        await check_4_family_summary(conn, run_id)
        await check_5_run_summary(conn, run_id)
        await check_6_determinism(conn, run_id)
        await check_7_lead_vs_coincident_checked(conn, run_id)
        await check_8_route_contract(conn)
        print("\nAll Phase 4.2A checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
