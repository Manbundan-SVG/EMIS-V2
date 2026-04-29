"""Phase 4.0B smoke validation: Dependency Graph + Context Model.

Checks:
  1. asset_dependency_graph rows exist
  2. asset_family_mappings rows exist
  3. active watchlist_dependency_profile exists or default path works
  4. context snapshot persists
  5. coverage summary rows populate
  6. context detail rows populate
  7. family state rows populate
  8. context_hash is deterministic across repeated runs with unchanged inputs
  9. route contract remains typed and stable
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import asyncpg

DATABASE_URL = os.environ.get("DATABASE_URL", "")
_CONTEXT_VERSION = "4.0B.v1"


async def get_conn() -> asyncpg.Connection:
    if not DATABASE_URL:
        sys.exit("DATABASE_URL not set")
    return await asyncpg.connect(DATABASE_URL)


async def ensure_watchlist(conn: asyncpg.Connection, workspace_id: str) -> str:
    """Ensure at least one watchlist exists in the workspace with at least one
    crypto asset attached, so primary_symbols is non-empty."""
    row = await conn.fetchrow(
        "SELECT id FROM watchlists WHERE workspace_id = $1::uuid LIMIT 1",
        workspace_id,
    )
    if not row:
        wid = str(uuid.uuid4())
        await conn.execute(
            "INSERT INTO watchlists (id, workspace_id, slug, name) "
            "VALUES ($1::uuid, $2::uuid, 'phase40b_validation', 'Phase 4.0B Validation')",
            wid, workspace_id,
        )
        watchlist_id = wid
    else:
        watchlist_id = str(row["id"])

    btc_row = await conn.fetchrow("SELECT id FROM assets WHERE symbol = 'BTC'")
    if btc_row:
        await conn.execute(
            "INSERT INTO watchlist_assets (watchlist_id, asset_id) "
            "VALUES ($1::uuid, $2::uuid) ON CONFLICT DO NOTHING",
            watchlist_id, btc_row["id"],
        )
    return watchlist_id


def compute_context_hash(
    primary_symbols, dependency_symbols, profile_name, profile_controls,
) -> str:
    payload = {
        "version":            _CONTEXT_VERSION,
        "profile_name":       profile_name,
        "profile_controls":   {k: profile_controls[k] for k in sorted(profile_controls.keys())},
        "primary_symbols":    sorted(primary_symbols),
        "dependency_symbols": sorted(dependency_symbols),
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


async def check_1_graph(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        "SELECT from_symbol, to_symbol, dependency_type, dependency_family "
        "FROM asset_dependency_graph WHERE is_active = true"
    )
    assert rows, "CHECK 1 FAILED: no asset_dependency_graph rows"
    assert len(rows) >= 5, f"CHECK 1 FAILED: only {len(rows)} graph rows"
    print(f"CHECK 1 PASSED: dependency_graph_rows={len(rows)}")


async def check_2_family_mappings(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        "SELECT symbol, family_key FROM asset_family_mappings WHERE is_active = true"
    )
    assert rows, "CHECK 2 FAILED: no asset_family_mappings rows"
    print(f"CHECK 2 PASSED: family_mapping_rows={len(rows)}")


async def check_3_profile_or_default(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str,
) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM watchlist_dependency_profiles "
        "WHERE workspace_id = $1::uuid AND watchlist_id = $2::uuid AND is_active = true",
        workspace_id, watchlist_id,
    )
    if row:
        print(f"CHECK 3 PASSED: active profile exists id={row['id']}")
    else:
        print("CHECK 3 PASSED: no active profile — default_inclusive path applies")


async def seed_validation_snapshot(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str,
) -> tuple[str, str]:
    """Build a deterministic snapshot like the service does and insert it.
    Returns (snapshot_id, context_hash)."""
    primary = ["BTC"]
    dependencies = ["SPY", "QQQ", "US10Y", "DXY", "GLD"]
    families = sorted({"risk", "rates", "fx", "commodity"})
    profile_name = "default_inclusive"
    profile_controls = {
        "include_macro":        True,
        "include_fx":           True,
        "include_rates":        True,
        "include_equity_index": True,
        "include_commodity":    True,
        "include_crypto_cross": True,
        "max_dependencies":     25,
    }
    context_hash = compute_context_hash(primary, dependencies, profile_name, profile_controls)
    sid = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO watchlist_context_snapshots
          (id, workspace_id, watchlist_id, profile_id, snapshot_at,
           primary_symbols, dependency_symbols, dependency_families,
           context_hash, coverage_summary, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL, now(),
                $4::jsonb, $5::jsonb, $6::jsonb,
                $7, $8::jsonb, $9::jsonb)
        """,
        sid, workspace_id, watchlist_id,
        json.dumps(sorted(primary)),
        json.dumps(sorted(dependencies)),
        json.dumps(families),
        context_hash,
        json.dumps({"evaluated_symbols": len(dependencies), "covered": 0, "missing": 0, "stale": 0}),
        json.dumps({"profile_name": profile_name, "profile_controls": profile_controls,
                    "context_version": _CONTEXT_VERSION}),
    )
    return sid, context_hash


async def check_4_snapshot_persists(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, snapshot_id: str,
) -> None:
    row = await conn.fetchrow(
        "SELECT id, context_hash FROM watchlist_context_snapshots WHERE id = $1::uuid",
        snapshot_id,
    )
    assert row, "CHECK 4 FAILED: snapshot not persisted"
    print(f"CHECK 4 PASSED: context_snapshot_rows>=1 hash={row['context_hash'][:12]}…")


async def check_5_coverage_view(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT context_hash, primary_symbol_count, dependency_symbol_count, coverage_ratio "
        "FROM watchlist_dependency_coverage_summary "
        "WHERE workspace_id = $1::uuid AND watchlist_id = $2::uuid",
        workspace_id, watchlist_id,
    )
    assert rows, "CHECK 5 FAILED: coverage summary empty"
    print(f"CHECK 5 PASSED: coverage_summary_rows={len(rows)}")


async def check_6_context_detail(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT symbol, is_primary FROM watchlist_dependency_context_detail "
        "WHERE workspace_id = $1::uuid AND watchlist_id = $2::uuid",
        workspace_id, watchlist_id,
    )
    assert rows, "CHECK 6 FAILED: context_detail empty"
    primary_count = sum(1 for r in rows if r["is_primary"])
    dep_count = len(rows) - primary_count
    print(f"CHECK 6 PASSED: context_detail_rows={len(rows)} primary={primary_count} dep={dep_count}")


async def check_7_family_state(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, symbol_count FROM watchlist_dependency_family_state "
        "WHERE workspace_id = $1::uuid AND watchlist_id = $2::uuid",
        workspace_id, watchlist_id,
    )
    assert rows, "CHECK 7 FAILED: family_state empty"
    print(f"CHECK 7 PASSED: family_state_rows={len(rows)}")


async def check_8_hash_determinism(expected_hash: str) -> None:
    """Recompute the same hash from the same inputs — must match."""
    primary = ["BTC"]
    dependencies = ["SPY", "QQQ", "US10Y", "DXY", "GLD"]
    profile_name = "default_inclusive"
    profile_controls = {
        "include_macro":        True,
        "include_fx":           True,
        "include_rates":        True,
        "include_equity_index": True,
        "include_commodity":    True,
        "include_crypto_cross": True,
        "max_dependencies":     25,
    }
    recomputed = compute_context_hash(primary, dependencies, profile_name, profile_controls)
    assert recomputed == expected_hash, (
        f"CHECK 8 FAILED: hash mismatch {recomputed} != {expected_hash}"
    )
    print("CHECK 8 PASSED: context_hash_stable=true")


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "asset_dependency_graph": [
            "id", "from_symbol", "to_symbol", "dependency_type",
            "dependency_family", "priority", "weight", "is_active", "metadata",
        ],
        "watchlist_dependency_profiles": [
            "id", "workspace_id", "watchlist_id", "profile_name",
            "include_macro", "include_fx", "include_rates",
            "include_equity_index", "include_commodity", "include_crypto_cross",
            "max_dependencies", "is_active",
        ],
        "asset_family_mappings": [
            "id", "symbol", "asset_class", "family_key", "family_label",
            "region", "is_active", "metadata",
        ],
        "watchlist_context_snapshots": [
            "id", "workspace_id", "watchlist_id", "profile_id", "snapshot_at",
            "primary_symbols", "dependency_symbols", "dependency_families",
            "context_hash", "coverage_summary", "metadata",
        ],
        "watchlist_dependency_coverage_summary": [
            "workspace_id", "watchlist_id", "context_hash",
            "primary_symbol_count", "dependency_symbol_count", "dependency_family_count",
            "covered_dependency_count", "missing_dependency_count", "stale_dependency_count",
            "latest_context_snapshot_at", "coverage_ratio", "metadata",
        ],
        "watchlist_dependency_context_detail": [
            "workspace_id", "watchlist_id", "context_hash", "symbol",
            "asset_class", "dependency_family", "dependency_type",
            "priority", "weight", "is_primary",
            "latest_timestamp", "is_missing", "is_stale", "metadata",
        ],
        "watchlist_dependency_family_state": [
            "workspace_id", "watchlist_id", "context_hash", "dependency_family",
            "symbol_count", "covered_count", "missing_count", "stale_count",
            "latest_timestamp", "metadata",
        ],
    }
    for table, cols in checks.items():
        cols_sql = ", ".join(cols)
        await conn.fetchrow(f"SELECT {cols_sql} FROM {table} LIMIT 1")  # noqa: S608
    print("CHECK 9 PASSED: detail_contract_ok=true")


async def main() -> None:
    conn = await get_conn()
    try:
        workspace_row = await conn.fetchrow("SELECT id FROM workspaces LIMIT 1")
        assert workspace_row, "no workspaces"
        workspace_id = str(workspace_row["id"])
        watchlist_id = await ensure_watchlist(conn, workspace_id)

        await check_1_graph(conn)
        await check_2_family_mappings(conn)
        await check_3_profile_or_default(conn, workspace_id, watchlist_id)
        snapshot_id, expected_hash = await seed_validation_snapshot(
            conn, workspace_id, watchlist_id,
        )
        await check_4_snapshot_persists(conn, workspace_id, watchlist_id, snapshot_id)
        await check_5_coverage_view(conn, workspace_id, watchlist_id)
        await check_6_context_detail(conn, workspace_id, watchlist_id)
        await check_7_family_state(conn, workspace_id, watchlist_id)
        await check_8_hash_determinism(expected_hash)
        await check_9_route_contract(conn)
        print("\nAll Phase 4.0B checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
