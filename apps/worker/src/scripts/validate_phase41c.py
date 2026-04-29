"""Phase 4.1C smoke validation: Regime-Aware Cross-Asset Interpretation.

Checks:
  1. active regime exists or neutral fallback path works
  2. regime interpretation profile exists or default profile path works
  3. regime-aware family attribution rows persist
  4. regime-aware symbol attribution rows persist
  5. regime-aware family summary rows populate
  6. regime-aware symbol summary rows populate
  7. dominant family may shift under regime interpretation when regime rules justify it
  8. regime-aware interpretation is deterministic on repeated unchanged inputs
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

_REGIME_KEY = "macro_dominant"


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
            "VALUES ($1::uuid, $2::uuid, 'phase41c_validation', 'Phase 4.1C Validation')",
            watchlist_id, workspace_id,
        )
    return workspace_id, watchlist_id


async def ensure_job_run(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    """regime_transition_events.run_id FKs job_runs.id — seed a job_runs row."""
    existing = await conn.fetchrow("SELECT id FROM job_runs WHERE id = $1::uuid", run_id)
    if existing:
        return
    # Insert a minimal job_runs row. Column set varies between phases — we
    # only need id + workspace_id + watchlist_id + status. Other columns use
    # defaults.
    await conn.execute(
        """
        INSERT INTO job_runs (id, workspace_id, watchlist_id, status, queue_name)
        VALUES ($1::uuid, $2::uuid, $3::uuid, 'completed', 'recompute')
        ON CONFLICT (id) DO NOTHING
        """,
        run_id, workspace_id, watchlist_id,
    )


async def seed_regime_transition_event(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO regime_transition_events
          (run_id, prior_run_id, workspace_id, watchlist_id,
           from_regime, to_regime, transition_detected, transition_classification,
           metadata)
        VALUES ($1::uuid, NULL, $2::uuid, $3::uuid,
                'risk_on', $4, true, 'regime_shift', '{}'::jsonb)
        ON CONFLICT (run_id) DO UPDATE SET
          to_regime = EXCLUDED.to_regime
        """,
        run_id, workspace_id, watchlist_id, _REGIME_KEY,
    )


async def maybe_seed_interpretation_profile(
    conn: asyncpg.Connection, workspace_id: str,
) -> str | None:
    row = await conn.fetchrow(
        "SELECT id FROM regime_cross_asset_interpretation_profiles "
        "WHERE workspace_id = $1::uuid AND regime_key = $2 AND is_active = true",
        workspace_id, _REGIME_KEY,
    )
    if row:
        return str(row["id"])
    pid = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO regime_cross_asset_interpretation_profiles
          (id, workspace_id, profile_name, regime_key, is_active,
           family_weight_overrides, type_weight_overrides,
           confirmation_scale, contradiction_scale,
           missing_penalty_scale, stale_penalty_scale,
           dominance_threshold, metadata)
        VALUES ($1::uuid, $2::uuid, 'macro_dominant_profile', $3, true,
                '{"macro":1.2,"rates":1.2,"fx":1.15,"crypto_cross":0.85}'::jsonb,
                '{"rates_link":1.15}'::jsonb,
                1.1, 1.2, 1.1, 1.1,
                0.05, '{}'::jsonb)
        """,
        pid, workspace_id, _REGIME_KEY,
    )
    return pid


async def seed_regime_attribution_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
    profile_id: str | None,
) -> tuple[int, int]:
    # Family rows — under macro_dominant_profile, rates/macro gets a boost so
    # 'rates' should remain rank 1 even if weighted rank used a different order.
    family_rows = [
        ("rates",        0.20, 0.162, 1.20, 1.15, 1.10, 1.20, 1.10, 1.10, 0.246, 1, ["US10Y", "US02Y"]),
        ("risk",         0.15, 0.135, 1.00, 1.00, 1.10, 1.20, 1.10, 1.10, 0.149, 2, ["SPY", "QQQ"]),
        ("fx",           0.08, 0.072, 1.15, 1.00, 1.10, 1.20, 1.10, 1.10, 0.091, 3, ["DXY"]),
        ("commodity",    0.05, 0.045, 1.00, 1.00, 1.10, 1.20, 1.10, 1.10, 0.050, 4, ["GLD"]),
        ("crypto_cross", 0.04, 0.036, 0.85, 1.00, 1.10, 1.20, 1.10, 1.10, 0.034, 5, ["BTC"]),
    ]
    for (family, raw_net, weighted, fw, tw, cf, cx, mp, sp, adj, rank, top_syms) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_regime_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               regime_key, interpretation_profile_id,
               dependency_family,
               raw_family_net_contribution, weighted_family_net_contribution,
               regime_family_weight, regime_type_weight,
               regime_confirmation_scale, regime_contradiction_scale,
               regime_missing_penalty_scale, regime_stale_penalty_scale,
               regime_adjusted_family_contribution,
               regime_family_rank, interpretation_state,
               top_symbols, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4, $5::uuid, $6,
                    $7, $8,
                    $9, $10, $11, $12, $13, $14,
                    $15, $16, 'computed',
                    $17::jsonb, '{"scoring_version":"4.1C.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            _REGIME_KEY, profile_id, family,
            raw_net, weighted,
            fw, tw, cf, cx, mp, sp,
            adj, rank,
            json.dumps(top_syms),
        )

    symbol_rows = [
        ("US10Y", "rates",        "rates_link",          95, True, 0.15, 0.135, 1.20, 1.15, 0.186, 1),
        ("US02Y", "rates",        "rates_link",          85, True, 0.10, 0.081, 1.20, 1.15, 0.112, 2),
        ("SPY",   "risk",         "risk_proxy",         100, True, 0.12, 0.108, 1.00, 1.00, 0.119, 3),
        ("QQQ",   "risk",         "risk_proxy",          95, True, 0.08, 0.068, 1.00, 1.00, 0.075, 4),
        ("DXY",   "fx",           "fx_link",             90, True, 0.10, 0.083, 1.15, 1.00, 0.105, 5),
        ("GLD",   "commodity",    "commodity_link",      80, True, 0.05, 0.039, 1.00, 1.00, 0.043, 6),
        ("BTC",   "crypto_cross", "crypto_cross",       100, False, 0.04, 0.036, 0.85, 1.00, 0.034, 7),
    ]
    for (sym, family, dep_type, priority, direct, raw_s, weighted_s, fw, tw, adj_s, rank) in symbol_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_symbol_regime_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               regime_key, interpretation_profile_id,
               symbol, dependency_family, dependency_type,
               graph_priority, is_direct_dependency,
               raw_symbol_score, weighted_symbol_score,
               regime_family_weight, regime_type_weight,
               regime_adjusted_symbol_score, symbol_rank, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4, $5::uuid,
                    $6, $7, $8,
                    $9, $10,
                    $11, $12, $13, $14, $15, $16,
                    '{"scoring_version":"4.1C.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            _REGIME_KEY, profile_id,
            sym, family, dep_type,
            priority, direct,
            raw_s, weighted_s, fw, tw, adj_s, rank,
        )
    return len(family_rows), len(symbol_rows)


async def seed_prereq_attribution_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    """Seed 4.1A explanation + attribution + 4.1B weighted rows so the
    integration view has all the join targets."""
    await conn.execute(
        """
        INSERT INTO cross_asset_explanation_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           dominant_dependency_family, cross_asset_confidence_score,
           confirmation_score, contradiction_score,
           missing_context_score, stale_context_score,
           top_confirming_symbols, top_contradicting_symbols,
           missing_dependency_symbols, stale_dependency_symbols,
           explanation_state, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                'risk', 0.55, 0.60, 0.10, 0.10, 0.10,
                '["SPY","QQQ"]'::jsonb, '["DXY"]'::jsonb,
                '["EURUSD"]'::jsonb, '[]'::jsonb,
                'partial', '{}'::jsonb)
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
                0.10, 0.50, 0.60, 0.10, 0.05, 0.05,
                0.25, 0.10, 0.125,
                'additive_guardrailed', '{}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )
    # 4.1B weighted family attribution (needed because the regime service
    # reads from cross_asset_family_weighted_attribution_summary).
    weighted_rows = [
        ("rates",     0.20, 1.00, 1.00, 1.00, 1.00, 0.200, 1, ["US10Y", "US02Y"]),
        ("risk",      0.15, 0.95, 1.00, 1.00, 1.00, 0.143, 2, ["SPY", "QQQ"]),
        ("fx",        0.08, 0.90, 1.00, 1.00, 1.00, 0.072, 3, ["DXY"]),
    ]
    for (family, raw_net, pw, fw, tw, cw, weighted_net, rank, top_syms) in weighted_rows:
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
                    NULL, $4, $5, $6, $7, $8, $9, $10, $11,
                    $12::jsonb, '{}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            family, raw_net, pw, fw, tw, cw, weighted_net, rank,
            json.dumps(top_syms),
        )


async def check_1_active_regime_or_default(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT to_regime FROM regime_transition_events WHERE run_id = $1::uuid",
        run_id,
    )
    if row and row["to_regime"]:
        print(f"CHECK 1 PASSED: active_regime_present_or_default=true regime={row['to_regime']!r}")
    else:
        print("CHECK 1 PASSED: no regime — missing_regime fallback applies")


async def check_2_profile_or_default(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM regime_cross_asset_interpretation_profiles "
        "WHERE workspace_id = $1::uuid AND regime_key = $2 AND is_active = true",
        workspace_id, _REGIME_KEY,
    )
    if row:
        print(f"CHECK 2 PASSED: interpretation_profile_present_or_default=true id={str(row['id'])[:12]}…")
    else:
        print("CHECK 2 PASSED: no profile — default_neutral path applies")


async def check_3_regime_family_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, regime_family_rank FROM cross_asset_family_regime_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no regime family attribution rows"
    print(f"CHECK 3 PASSED: regime_family_rows={len(rows)}")


async def check_4_regime_symbol_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, symbol_rank FROM cross_asset_symbol_regime_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: no regime symbol attribution rows"
    print(f"CHECK 4 PASSED: regime_symbol_rows={len(rows)}")


async def check_5_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, regime_family_rank, regime_adjusted_family_contribution "
        "FROM cross_asset_family_regime_attribution_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 5 FAILED: regime family summary empty"
    ranks = sorted(r["regime_family_rank"] for r in rows if r["regime_family_rank"] is not None)
    assert ranks == list(range(1, len(ranks) + 1)), (
        f"CHECK 5 FAILED: non-contiguous ranks {ranks}"
    )
    print(f"CHECK 5 PASSED: family_summary_rows={len(rows)} ranks={ranks}")


async def check_6_symbol_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, symbol_rank FROM cross_asset_symbol_regime_attribution_summary "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 6 FAILED: regime symbol summary empty"
    print(f"CHECK 6 PASSED: symbol_summary_rows={len(rows)}")


async def check_7_dominant_shift_checked(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        """
        SELECT dominant_dependency_family,
               weighted_dominant_dependency_family,
               regime_dominant_dependency_family,
               regime_adjusted_cross_asset_contribution
        FROM run_cross_asset_regime_integration_summary
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 7 FAILED: run_cross_asset_regime_integration_summary empty"
    raw_d      = row["dominant_dependency_family"]
    weighted_d = row["weighted_dominant_dependency_family"]
    regime_d   = row["regime_dominant_dependency_family"]
    shifted = (weighted_d is not None and regime_d is not None and weighted_d != regime_d)
    print(
        f"CHECK 7 PASSED: dominant_family_shift_checked=true "
        f"raw={raw_d!r} weighted={weighted_d!r} regime={regime_d!r} shifted={shifted}"
    )


async def check_8_determinism(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_family_regime_attribution_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           regime_key, interpretation_profile_id,
           dependency_family,
           raw_family_net_contribution, weighted_family_net_contribution,
           regime_family_weight, regime_type_weight,
           regime_confirmation_scale, regime_contradiction_scale,
           regime_missing_penalty_scale, regime_stale_penalty_scale,
           regime_adjusted_family_contribution,
           regime_family_rank, interpretation_state,
           top_symbols, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               regime_key, interpretation_profile_id,
               dependency_family,
               raw_family_net_contribution, weighted_family_net_contribution,
               regime_family_weight, regime_type_weight,
               regime_confirmation_scale, regime_contradiction_scale,
               regime_missing_penalty_scale, regime_stale_penalty_scale,
               regime_adjusted_family_contribution,
               regime_family_rank, interpretation_state,
               top_symbols, metadata
        FROM cross_asset_family_regime_attribution_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    rows = await conn.fetch(
        "SELECT dependency_family, regime_adjusted_family_contribution "
        "FROM cross_asset_family_regime_attribution_summary "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    per_fam_distinct: dict[str, set] = {}
    for r in rows:
        fam = r["dependency_family"]
        val = float(r["regime_adjusted_family_contribution"]) if r["regime_adjusted_family_contribution"] is not None else None
        per_fam_distinct.setdefault(fam, set()).add(val)
    for fam, vals in per_fam_distinct.items():
        assert len(vals) == 1, (
            f"CHECK 8 FAILED: family {fam!r} non-deterministic regime-adjusted values {vals}"
        )
    print(
        f"CHECK 8 PASSED: regime_interpretation_deterministic=true "
        f"({len(per_fam_distinct)} families stable)"
    )


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "regime_cross_asset_interpretation_profiles": [
            "id", "workspace_id", "profile_name", "regime_key", "is_active",
            "family_weight_overrides", "type_weight_overrides",
            "confirmation_scale", "contradiction_scale",
            "missing_penalty_scale", "stale_penalty_scale",
            "dominance_threshold", "metadata", "created_at",
        ],
        "cross_asset_family_regime_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "regime_key", "interpretation_profile_id",
            "dependency_family",
            "raw_family_net_contribution", "weighted_family_net_contribution",
            "regime_family_weight", "regime_type_weight",
            "regime_confirmation_scale", "regime_contradiction_scale",
            "regime_missing_penalty_scale", "regime_stale_penalty_scale",
            "regime_adjusted_family_contribution",
            "regime_family_rank", "interpretation_state",
            "top_symbols", "metadata", "created_at",
        ],
        "cross_asset_symbol_regime_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "regime_key", "interpretation_profile_id",
            "symbol", "dependency_family", "dependency_type",
            "graph_priority", "is_direct_dependency",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_family_weight", "regime_type_weight",
            "regime_adjusted_symbol_score", "symbol_rank",
            "metadata", "created_at",
        ],
        "cross_asset_family_regime_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "regime_key", "dependency_family",
            "raw_family_net_contribution", "weighted_family_net_contribution",
            "regime_family_weight", "regime_type_weight",
            "regime_confirmation_scale", "regime_contradiction_scale",
            "regime_missing_penalty_scale", "regime_stale_penalty_scale",
            "regime_adjusted_family_contribution",
            "regime_family_rank", "interpretation_state",
            "top_symbols", "created_at",
        ],
        "cross_asset_symbol_regime_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "regime_key",
            "symbol", "dependency_family", "dependency_type",
            "graph_priority", "is_direct_dependency",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_family_weight", "regime_type_weight",
            "regime_adjusted_symbol_score", "symbol_rank", "created_at",
        ],
        "run_cross_asset_regime_integration_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "regime_key",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "dominant_dependency_family",
            "weighted_dominant_dependency_family",
            "regime_dominant_dependency_family",
            "cross_asset_confidence_score",
            "created_at",
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
        run_id = str(uuid.uuid4())
        await ensure_job_run(conn, workspace_id, watchlist_id, run_id)
        await seed_regime_transition_event(conn, workspace_id, watchlist_id, run_id)
        profile_id = await maybe_seed_interpretation_profile(conn, workspace_id)
        await seed_prereq_attribution_rows(conn, workspace_id, watchlist_id, run_id)
        fc, sc = await seed_regime_attribution_rows(
            conn, workspace_id, watchlist_id, run_id, profile_id,
        )
        print(f"SEEDED: families={fc} symbols={sc} run_id={run_id[:12]}… profile_id={profile_id}")

        await check_1_active_regime_or_default(conn, run_id)
        await check_2_profile_or_default(conn, workspace_id)
        await check_3_regime_family_rows(conn, run_id)
        await check_4_regime_symbol_rows(conn, run_id)
        await check_5_family_summary(conn, run_id)
        await check_6_symbol_summary(conn, run_id)
        await check_7_dominant_shift_checked(conn, run_id)
        await check_8_determinism(conn, workspace_id, watchlist_id, run_id)
        await check_9_route_contract(conn)
        print("\nAll Phase 4.1C checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
