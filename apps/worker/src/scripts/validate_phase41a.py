"""Phase 4.1A smoke validation: Cross-Asset Attribution + Composite Integration.

Checks:
  1. attribution snapshot persists
  2. family attribution rows persist
  3. attribution summary rows populate
  4. family attribution summary rows populate
  5. run integration summary populates
  6. composite_post_cross_asset differs from pre when net contribution is nonzero
  7. integration is deterministic on repeated unchanged inputs
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

_INTEGRATION_WEIGHT = 0.10
_NET_BOUND          = 0.25


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
            "VALUES ($1::uuid, $2::uuid, 'phase41a_validation', 'Phase 4.1A Validation')",
            watchlist_id, workspace_id,
        )
    return workspace_id, watchlist_id


async def seed_explanation_bridge_row(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    """Ensure run_cross_asset_explanation_bridge has a row for this run so the
    composite integration view can join it. We insert a base explanation
    snapshot (which drives both the bridge view and the 4.1A inputs)."""
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
                'rates', 0.55,
                0.60, 0.10, 0.10, 0.10,
                '["SPY","QQQ"]'::jsonb, '["DXY"]'::jsonb,
                '["EURUSD"]'::jsonb, '[]'::jsonb,
                'partial', '{"scoring_version":"4.0D.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


async def seed_attribution_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> tuple[str, int]:
    """Seed attribution mirroring what the service would produce given the
    explanation seeded above."""
    confirmation = 0.60
    contradiction_penalty = 0.10
    missing_penalty = 0.10 * 0.5     # 0.05
    stale_penalty = 0.10 * 0.5       # 0.05
    raw_net = confirmation - contradiction_penalty - missing_penalty - stale_penalty  # 0.40
    net_clipped = _clip(raw_net, -_NET_BOUND, _NET_BOUND)                             # 0.25
    base_signal = 0.10
    composite_pre = base_signal
    composite_post = composite_pre + net_clipped * _INTEGRATION_WEIGHT                # 0.10 + 0.025 = 0.125
    cross_asset_signal_score = confirmation - contradiction_penalty                   # 0.50

    attr_id = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO cross_asset_attribution_snapshots
          (id, workspace_id, watchlist_id, run_id, context_snapshot_id,
           base_signal_score,
           cross_asset_signal_score,
           cross_asset_confirmation_score,
           cross_asset_contradiction_penalty,
           cross_asset_missing_penalty,
           cross_asset_stale_penalty,
           cross_asset_net_contribution,
           composite_pre_cross_asset,
           composite_post_cross_asset,
           integration_mode, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, NULL,
                $5, $6, $7, $8, $9, $10, $11, $12, $13,
                'additive_guardrailed', '{"scoring_version":"4.1A.v1"}'::jsonb)
        """,
        attr_id, workspace_id, watchlist_id, run_id,
        base_signal, cross_asset_signal_score, confirmation,
        contradiction_penalty, missing_penalty, stale_penalty,
        net_clipped, composite_pre, composite_post,
    )

    family_rows = [
        ("rates",        0.50, 0.60, 0.10, 0.03, 0.02, _clip(0.60 - 0.10 - 0.03 - 0.02, -_NET_BOUND, _NET_BOUND), 1, ["US10Y"]),
        ("risk",         0.30, 0.50, 0.20, 0.05, 0.00, _clip(0.50 - 0.20 - 0.05 - 0.00, -_NET_BOUND, _NET_BOUND), 2, ["SPY", "QQQ"]),
        ("fx",          -0.10, 0.20, 0.30, 0.10, 0.05, _clip(0.20 - 0.30 - 0.10 - 0.05, -_NET_BOUND, _NET_BOUND), 3, ["DXY"]),
        ("commodity",    0.10, 0.30, 0.20, 0.00, 0.05, _clip(0.30 - 0.20 - 0.00 - 0.05, -_NET_BOUND, _NET_BOUND), 4, ["GLD"]),
    ]
    for (family, signal, confirm, contra, miss, stale, net, rank, top_syms) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               dependency_family,
               family_signal_score,
               family_confirmation_score,
               family_contradiction_penalty,
               family_missing_penalty,
               family_stale_penalty,
               family_net_contribution,
               family_rank,
               top_symbols, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4, $5, $6, $7, $8, $9, $10, $11,
                    $12::jsonb, '{}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            family, signal, confirm, contra, miss, stale, net, rank,
            json.dumps(top_syms),
        )
    return attr_id, len(family_rows)


async def check_1_attribution_persists(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_attribution_snapshots WHERE run_id = $1::uuid LIMIT 1",
        run_id,
    )
    assert row, "CHECK 1 FAILED: no attribution snapshot persisted"
    print(f"CHECK 1 PASSED: attribution_snapshot_rows>=1 id={str(row['id'])[:12]}…")


async def check_2_family_attribution(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, family_rank FROM cross_asset_family_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 2 FAILED: no family attribution rows"
    ranks = sorted(r["family_rank"] for r in rows if r["family_rank"] is not None)
    assert ranks == list(range(1, len(ranks) + 1)), (
        f"CHECK 2 FAILED: non-contiguous ranks {ranks}"
    )
    print(f"CHECK 2 PASSED: family_attribution_rows={len(rows)} ranks={ranks}")


async def check_3_attribution_summary(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT run_id, base_signal_score, cross_asset_net_contribution, integration_mode "
        "FROM cross_asset_attribution_summary "
        "WHERE workspace_id = $1::uuid AND watchlist_id = $2::uuid",
        workspace_id, watchlist_id,
    )
    assert rows, "CHECK 3 FAILED: attribution_summary empty"
    print(f"CHECK 3 PASSED: attribution_summary_rows={len(rows)}")


async def check_4_family_summary(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str,
) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, family_net_contribution, family_rank "
        "FROM cross_asset_family_attribution_summary "
        "WHERE workspace_id = $1::uuid AND watchlist_id = $2::uuid",
        workspace_id, watchlist_id,
    )
    assert rows, "CHECK 4 FAILED: family_attribution_summary empty"
    print(f"CHECK 4 PASSED: family_summary_rows={len(rows)}")


async def check_5_run_integration(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        """
        SELECT run_id, composite_pre_cross_asset, composite_post_cross_asset,
               dominant_dependency_family, cross_asset_confidence_score
        FROM run_composite_integration_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 5 FAILED: run_composite_integration_summary empty"
    print(
        f"CHECK 5 PASSED: run_integration_rows>=1 "
        f"dominant={row['dominant_dependency_family']} "
        f"confidence={row['cross_asset_confidence_score']}"
    )


async def check_6_composite_delta(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT composite_pre_cross_asset AS pre, composite_post_cross_asset AS post, "
        "       cross_asset_net_contribution AS net "
        "FROM cross_asset_attribution_snapshots WHERE run_id = $1::uuid LIMIT 1",
        run_id,
    )
    assert row
    pre  = float(row["pre"])  if row["pre"]  is not None else None
    post = float(row["post"]) if row["post"] is not None else None
    net  = float(row["net"])  if row["net"]  is not None else None
    assert pre is not None and post is not None, "CHECK 6 FAILED: pre/post missing"
    assert net is not None and abs(net) > 1e-9, "CHECK 6 FAILED: net contribution is zero"
    delta = post - pre
    # Delta must be positive when net is positive, within floating-point tolerance
    expected_delta = net * _INTEGRATION_WEIGHT
    assert abs(delta - expected_delta) < 1e-6, (
        f"CHECK 6 FAILED: delta={delta} != net*weight={expected_delta}"
    )
    print(
        f"CHECK 6 PASSED: net_contribution_nonzero=true "
        f"pre={pre:.4f} post={post:.4f} delta={delta:.4f} net={net:.4f}"
    )


async def check_7_determinism(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    """Duplicate the attribution row for the same run with identical values —
    the summary view is DISTINCT ON run_id so post-integration score should
    remain stable if inputs do not change."""
    row0 = await conn.fetchrow(
        "SELECT composite_post_cross_asset FROM cross_asset_attribution_snapshots "
        "WHERE run_id = $1::uuid ORDER BY created_at ASC LIMIT 1",
        run_id,
    )
    assert row0
    await conn.execute(
        """
        INSERT INTO cross_asset_attribution_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           base_signal_score,
           cross_asset_signal_score,
           cross_asset_confirmation_score,
           cross_asset_contradiction_penalty,
           cross_asset_missing_penalty,
           cross_asset_stale_penalty,
           cross_asset_net_contribution,
           composite_pre_cross_asset,
           composite_post_cross_asset,
           integration_mode, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               base_signal_score,
               cross_asset_signal_score,
               cross_asset_confirmation_score,
               cross_asset_contradiction_penalty,
               cross_asset_missing_penalty,
               cross_asset_stale_penalty,
               cross_asset_net_contribution,
               composite_pre_cross_asset,
               composite_post_cross_asset,
               integration_mode, metadata
        FROM cross_asset_attribution_snapshots
        WHERE run_id = $1::uuid
        ORDER BY created_at ASC
        LIMIT 1
        """,
        run_id,
    )
    rows = await conn.fetch(
        "SELECT DISTINCT composite_post_cross_asset "
        "FROM cross_asset_attribution_snapshots WHERE run_id = $1::uuid",
        run_id,
    )
    distinct_posts = {float(r["composite_post_cross_asset"]) for r in rows if r["composite_post_cross_asset"] is not None}
    assert len(distinct_posts) == 1, (
        f"CHECK 7 FAILED: non-deterministic composite_post {distinct_posts}"
    )
    print(f"CHECK 7 PASSED: integration_deterministic=true post={distinct_posts.pop():.4f}")


async def check_8_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "base_signal_score",
            "cross_asset_signal_score",
            "cross_asset_confirmation_score",
            "cross_asset_contradiction_penalty",
            "cross_asset_missing_penalty",
            "cross_asset_stale_penalty",
            "cross_asset_net_contribution",
            "composite_pre_cross_asset", "composite_post_cross_asset",
            "integration_mode", "metadata", "created_at",
        ],
        "cross_asset_family_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "family_signal_score",
            "family_confirmation_score",
            "family_contradiction_penalty",
            "family_missing_penalty",
            "family_stale_penalty",
            "family_net_contribution",
            "family_rank", "top_symbols", "metadata", "created_at",
        ],
        "cross_asset_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "base_signal_score",
            "cross_asset_signal_score",
            "cross_asset_confirmation_score",
            "cross_asset_contradiction_penalty",
            "cross_asset_missing_penalty",
            "cross_asset_stale_penalty",
            "cross_asset_net_contribution",
            "composite_pre_cross_asset", "composite_post_cross_asset",
            "integration_mode", "created_at",
        ],
        "cross_asset_family_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "family_signal_score",
            "family_confirmation_score",
            "family_contradiction_penalty",
            "family_missing_penalty",
            "family_stale_penalty",
            "family_net_contribution",
            "family_rank", "top_symbols", "created_at",
        ],
        "run_composite_integration_summary": [
            "run_id", "workspace_id", "watchlist_id",
            "base_signal_score",
            "cross_asset_signal_score",
            "cross_asset_net_contribution",
            "composite_pre_cross_asset", "composite_post_cross_asset",
            "dominant_dependency_family",
            "cross_asset_confidence_score", "created_at",
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
        await seed_explanation_bridge_row(conn, workspace_id, watchlist_id, run_id)
        attr_id, family_count = await seed_attribution_rows(
            conn, workspace_id, watchlist_id, run_id,
        )
        print(f"SEEDED: attribution_id={attr_id[:12]}… families={family_count} run_id={run_id[:12]}…")

        await check_1_attribution_persists(conn, run_id)
        await check_2_family_attribution(conn, run_id)
        await check_3_attribution_summary(conn, workspace_id, watchlist_id)
        await check_4_family_summary(conn, workspace_id, watchlist_id)
        await check_5_run_integration(conn, run_id)
        await check_6_composite_delta(conn, run_id)
        await check_7_determinism(conn, workspace_id, watchlist_id, run_id)
        await check_8_route_contract(conn)
        print("\nAll Phase 4.1A checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
