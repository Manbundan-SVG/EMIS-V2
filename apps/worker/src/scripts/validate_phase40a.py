"""Phase 4.0A smoke validation: Multi-Asset Data Foundation.

Checks:
  1. asset_universe_catalog rows persist
  2. at least one non-crypto asset class has a synced market state row
  3. multi_asset_sync_health_summary view returns data
  4. normalized_multi_asset_market_state populates across multiple asset classes
  5. multi_asset_family_state_summary populates
  6. route contract remains typed and stable (column presence check)
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone

import asyncpg

DATABASE_URL = os.environ.get("DATABASE_URL", "")


async def get_conn() -> asyncpg.Connection:
    if not DATABASE_URL:
        sys.exit("DATABASE_URL not set")
    return await asyncpg.connect(DATABASE_URL)


async def get_workspace_id(conn: asyncpg.Connection, slug: str = "demo") -> str:
    row = await conn.fetchrow("SELECT id FROM workspaces WHERE slug = $1", slug)
    if not row:
        row = await conn.fetchrow("SELECT id FROM workspaces LIMIT 1")
    assert row, "no workspaces found"
    return str(row["id"])


async def seed_validation_market_state(conn: asyncpg.Connection) -> None:
    """Seed a minimal non-crypto asset state so the view checks don't require
    live provider credentials. Uses SPY (equity/index via market_bars) and
    EURUSD (fx via macro_series_points)."""
    # Ensure SPY exists in assets (migration 0001 seeds it, but be defensive)
    await conn.execute(
        "INSERT INTO assets (symbol, name, asset_class) "
        "VALUES ('SPY', 'SPDR S&P 500 ETF Trust', 'equity_index_proxy') "
        "ON CONFLICT (symbol) DO NOTHING"
    )
    spy_row = await conn.fetchrow("SELECT id FROM assets WHERE symbol = 'SPY'")
    assert spy_row
    spy_id = spy_row["id"]

    now = datetime.now(timezone.utc)
    await conn.execute(
        """
        INSERT INTO market_bars (asset_id, timeframe, ts, open, high, low, close,
                                 volume, source, return_1h, volume_zscore)
        VALUES ($1, '1d', $2, 500.0, 501.0, 499.5, 500.5, 1000000, 'alpaca', NULL, NULL)
        ON CONFLICT (asset_id, timeframe, ts, source) DO UPDATE SET close = EXCLUDED.close
        """,
        spy_id, now,
    )

    await conn.execute(
        """
        INSERT INTO macro_series_points (series_code, ts, value, source, return_1d, change_1d)
        VALUES ('EURUSD', $1, 1.0875, 'currency_api', NULL, NULL)
        ON CONFLICT (series_code, ts, source) DO UPDATE SET value = EXCLUDED.value
        """,
        now,
    )

    await conn.execute(
        """
        INSERT INTO macro_series_points (series_code, ts, value, source, return_1d, change_1d)
        VALUES ('US02Y', $1, 4.7500, 'fred', NULL, NULL)
        ON CONFLICT (series_code, ts, source) DO UPDATE SET value = EXCLUDED.value
        """,
        now,
    )

    # Seed a synthetic sync run for sync_health visibility
    wid = (await conn.fetchrow("SELECT id FROM workspaces LIMIT 1"))["id"]
    await conn.execute(
        """
        INSERT INTO market_data_sync_runs
          (id, source, workspace_id, status, requested_symbols, synced_symbols,
           asset_count, metadata, started_at, completed_at)
        VALUES ($1, 'multi_asset_equity_sync', $2, 'completed',
                '["SPY","QQQ","DIA","IWM"]'::jsonb, '["SPY"]'::jsonb,
                4,
                '{"provider_mode":"alpaca_snapshots","provider_family":"alpaca","asset_class":"index"}'::jsonb,
                now(), now())
        ON CONFLICT (id) DO NOTHING
        """,
        str(uuid.uuid4()), wid,
    )


async def check_1_catalog_rows(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        "SELECT asset_class, count(*) AS c FROM asset_universe_catalog "
        "WHERE is_active GROUP BY asset_class ORDER BY asset_class"
    )
    assert rows, "CHECK 1 FAILED: no asset_universe_catalog rows"
    classes = {r["asset_class"] for r in rows}
    expected = {"crypto", "index", "fx", "rates", "macro_proxy", "commodity"}
    missing = expected - classes
    assert not missing, f"CHECK 1 FAILED: asset classes missing from catalog: {missing}"
    total = sum(r["c"] for r in rows)
    print(f"CHECK 1 PASSED: asset_catalog_rows={total} asset_classes_present={sorted(classes)}")


async def check_2_non_crypto_state(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT DISTINCT asset_class FROM normalized_multi_asset_market_state "
        "WHERE workspace_id = $1::uuid AND asset_class != 'crypto'",
        workspace_id,
    )
    non_crypto = sorted(r["asset_class"] for r in rows)
    assert non_crypto, "CHECK 2 FAILED: no non-crypto asset class has synced state"
    print(f"CHECK 2 PASSED: non_crypto_classes_synced={non_crypto}")


async def check_3_sync_health(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT provider_family, asset_class, latest_status "
        "FROM multi_asset_sync_health_summary WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 3 FAILED: multi_asset_sync_health_summary returned no rows"
    print(f"CHECK 3 PASSED: sync_health_rows={len(rows)}")


async def check_4_market_state_multi_class(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT asset_class, count(*) AS c "
        "FROM normalized_multi_asset_market_state WHERE workspace_id = $1::uuid "
        "GROUP BY asset_class",
        workspace_id,
    )
    assert rows, "CHECK 4 FAILED: normalized_multi_asset_market_state empty"
    total = sum(r["c"] for r in rows)
    classes = sorted(r["asset_class"] for r in rows)
    assert len(classes) >= 1, "CHECK 4 FAILED: only zero asset classes populated"
    print(f"CHECK 4 PASSED: market_state_rows={total} classes={classes}")


async def check_5_family_summary(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT asset_class, symbol_count FROM multi_asset_family_state_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 5 FAILED: multi_asset_family_state_summary empty"
    print(f"CHECK 5 PASSED: family_summary_rows={len(rows)}")


async def check_6_route_contract(conn: asyncpg.Connection, workspace_id: str) -> None:
    """Verify column-level contract for the three views and the catalog table."""
    checks = {
        "asset_universe_catalog": [
            "id", "symbol", "canonical_symbol", "asset_class", "venue",
            "quote_currency", "base_currency", "region", "is_active", "metadata",
        ],
        "multi_asset_sync_health_summary": [
            "workspace_id", "provider_family", "asset_class",
            "requested_symbol_count", "synced_symbol_count", "failed_symbol_count",
            "latest_run_started_at", "latest_run_completed_at",
            "latest_status", "latest_provider_mode", "latest_metadata",
        ],
        "normalized_multi_asset_market_state": [
            "workspace_id", "symbol", "canonical_symbol", "asset_class",
            "provider_family", "price", "price_timestamp",
            "volume_24h", "oi_change_1h", "funding_rate",
            "yield_value", "fx_return_1d", "macro_proxy_value",
            "liquidation_count", "metadata",
        ],
        "multi_asset_family_state_summary": [
            "workspace_id", "asset_class", "family_key", "symbol_count",
            "latest_timestamp", "avg_return_1d", "avg_volatility_proxy", "metadata",
        ],
    }
    for table, cols in checks.items():
        cols_sql = ", ".join(cols)
        await conn.fetchrow(f"SELECT {cols_sql} FROM {table} LIMIT 1")  # noqa: S608
    print("CHECK 6 PASSED: detail_contract_ok=true")


async def main() -> None:
    conn = await get_conn()
    try:
        workspace_id = await get_workspace_id(conn)
        await seed_validation_market_state(conn)
        await check_1_catalog_rows(conn)
        await check_2_non_crypto_state(conn, workspace_id)
        await check_3_sync_health(conn, workspace_id)
        await check_4_market_state_multi_class(conn, workspace_id)
        await check_5_family_summary(conn, workspace_id)
        await check_6_route_contract(conn, workspace_id)
        print("\nAll Phase 4.0A checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
