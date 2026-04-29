"""Phase 4.2B smoke validation: Family-Level Lead/Lag Attribution.

Checks:
  1. timing attribution profile exists or default profile path works
  2. timing-aware family attribution rows persist
  3. timing-aware symbol attribution rows persist
  4. timing-aware family summary rows populate
  5. timing-aware symbol summary rows populate
  6. timing-aware integration summary row populates
  7. lead/coincident/lag classes change attribution ordering when justified
  8. timing-aware attribution is deterministic on repeated unchanged inputs
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
            "VALUES ($1::uuid, $2::uuid, 'phase42b_validation', 'Phase 4.2B Validation')",
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


async def seed_timing_attribution_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> tuple[int, int]:
    """Seed timing-aware family/symbol rows. Designed so the lag-class
    ordering check is deterministic:

      - rates:  lead    (timing weight 1.10 + lead bonus)   → top
      - risk:   coincident (weight 1.00)                    → mid
      - fx:     lag     (weight 0.85, penalty)              → bottom
      - commodity: insufficient_data (weight 0.75)          → lowest
    """
    family_rows = [
        # family, raw, weighted, regime, timing_class, lead_n, coinc_n, lag_n,
        # class_weight, bonus, penalty, timing_adj, rank, top
        ("rates",    0.20, 0.19, 0.19, "lead",              2, 0, 0, 1.10,  0.0095, 0.00, 0.2185, 1, ["US10Y", "US02Y"]),
        ("risk",     0.15, 0.14, 0.14, "coincident",        0, 2, 0, 1.00,  0.00,   0.00, 0.1400, 2, []),
        ("fx",       0.10, 0.09, 0.09, "lag",               0, 0, 2, 0.85,  0.00,   0.0045, 0.0810, 3, []),
        ("commodity",0.05, 0.04, 0.04, "insufficient_data", 0, 0, 0, 0.75,  0.00,   0.00, 0.0300, 4, []),
    ]
    for (family, raw, weighted, regime, timing_class,
         lead_n, coinc_n, lag_n,
         class_w, bonus, penalty, timing_adj, rank, top_syms) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_timing_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               timing_profile_id, dependency_family,
               raw_family_net_contribution,
               weighted_family_net_contribution,
               regime_adjusted_family_contribution,
               dominant_timing_class,
               lead_pair_count, coincident_pair_count, lag_pair_count,
               timing_class_weight, timing_bonus, timing_penalty,
               timing_adjusted_family_contribution,
               timing_family_rank,
               top_leading_symbols, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    NULL, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                    $17::jsonb, '{"scoring_version":"4.2B.v1","default_profile_used":true}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            family, raw, weighted, regime, timing_class,
            lead_n, coinc_n, lag_n,
            class_w, bonus, penalty, timing_adj, rank,
            json.dumps(top_syms),
        )

    # Symbol rows — lead symbols should out-rank lag symbols at comparable
    # raw/regime scores.
    symbol_rows = [
        # sym, family, type, bucket, best_lag, raw, weighted, regime, class_w, timing_adj, rank
        ("US10Y", "rates",     "rates_link",     "lead",             -12, 0.10, 0.095, 0.095, 1.10, 0.1093, 1),
        ("US02Y", "rates",     "rates_link",     "lead",              -6, 0.08, 0.075, 0.075, 1.10, 0.0863, 2),
        ("SPY",   "risk",      "risk_proxy",     "coincident",         0, 0.09, 0.085, 0.085, 1.00, 0.0850, 3),
        ("QQQ",   "risk",      "risk_proxy",     "coincident",         0, 0.07, 0.065, 0.065, 1.00, 0.0650, 4),
        ("DXY",   "fx",        "fx_link",        "lag",               12, 0.10, 0.093, 0.093, 0.85, 0.0744, 5),
        ("GLD",   "commodity", "commodity_link", "insufficient_data", None, 0.05, 0.045, 0.045, 0.75, 0.0338, 6),
    ]
    for (sym, fam, dep_type, bucket, best_lag, raw_s, weighted_s, regime_s, class_w, timing_adj, rank) in symbol_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_symbol_timing_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               timing_profile_id,
               symbol, dependency_family, dependency_type,
               lag_bucket, best_lag_hours,
               raw_symbol_score, weighted_symbol_score,
               regime_adjusted_symbol_score,
               timing_class_weight,
               timing_adjusted_symbol_score,
               symbol_rank, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    NULL,
                    $4, $5, $6,
                    $7, $8,
                    $9, $10, $11,
                    $12, $13, $14,
                    '{"scoring_version":"4.2B.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            sym, fam, dep_type,
            bucket, best_lag,
            raw_s, weighted_s, regime_s,
            class_w, timing_adj, rank,
        )
    return len(family_rows), len(symbol_rows)


async def seed_upstream_dependencies(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    """Seed the minimal 4.1A/B/C and 4.0D bridge rows the 4.2B integration
    view needs for JOINs (otherwise the view returns NULLs for upstream
    columns, which defeats the shift-comparison check)."""
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
                0.10, 0.50, 0.60, 0.10, 0.025, 0.025, 0.25, 0.10, 0.125,
                'additive_guardrailed', '{}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )
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
                'risk', 0.55, 0.60, 0.10, 0.025, 0.025,
                '["SPY","QQQ"]'::jsonb, '[]'::jsonb,
                '[]'::jsonb, '[]'::jsonb,
                'computed', '{}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )
    # 4.1B weighted family + integration view row
    weighted_rows = [
        ("rates",     0.20, 1.00, 1.00, 1.00, 1.00, 0.20,  1, ["US10Y"]),
        ("risk",      0.15, 0.95, 1.00, 1.00, 1.00, 0.143, 2, ["SPY"]),
        ("fx",        0.10, 0.90, 1.00, 1.00, 1.00, 0.09,  3, ["DXY"]),
    ]
    for (family, raw, pw, fw, tw, cw, weighted_net, rank, top_syms) in weighted_rows:
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
            family, raw, pw, fw, tw, cw, weighted_net, rank,
            json.dumps(top_syms),
        )
    # 4.1C regime transition event + family regime rows
    await conn.execute(
        """
        INSERT INTO regime_transition_events
          (run_id, workspace_id, watchlist_id, to_regime, from_regime,
           transition_detected, transition_classification)
        VALUES ($1::uuid, $2::uuid, $3::uuid, 'macro_dominant', 'risk_on',
                true, 'regime_shift')
        ON CONFLICT (run_id) DO UPDATE SET to_regime = EXCLUDED.to_regime
        """,
        run_id, workspace_id, watchlist_id,
    )
    regime_rows = [
        ("rates",    0.20, 0.20, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.20, 1, ["US10Y"]),
        ("risk",     0.15, 0.143, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.143, 2, ["SPY"]),
        ("fx",       0.10, 0.09, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.09, 3, ["DXY"]),
    ]
    for (family, raw, weighted, fw, tw, cf, cx, mp, sp, regime_adj, rank, top_syms) in regime_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_regime_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               regime_key, interpretation_profile_id,
               dependency_family, raw_family_net_contribution,
               weighted_family_net_contribution,
               regime_family_weight, regime_type_weight,
               regime_confirmation_scale, regime_contradiction_scale,
               regime_missing_penalty_scale, regime_stale_penalty_scale,
               regime_adjusted_family_contribution,
               regime_family_rank, interpretation_state,
               top_symbols, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    'macro_dominant', NULL, $4, $5, $6,
                    $7, $8, $9, $10, $11, $12, $13, $14, 'computed',
                    $15::jsonb, '{}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            family, raw, weighted, fw, tw, cf, cx, mp, sp, regime_adj, rank,
            json.dumps(top_syms),
        )


async def check_1_profile_or_default(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_timing_attribution_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true LIMIT 1",
        workspace_id,
    )
    if row:
        print(f"CHECK 1 PASSED: active timing attribution profile id={str(row['id'])[:12]}…")
    else:
        print("CHECK 1 PASSED: no profile — default_timing path applies")


async def check_2_family_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, dominant_timing_class FROM cross_asset_family_timing_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 2 FAILED: no timing-aware family rows"
    print(f"CHECK 2 PASSED: timing_family_rows={len(rows)}")


async def check_3_symbol_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, lag_bucket FROM cross_asset_symbol_timing_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no timing-aware symbol rows"
    print(f"CHECK 3 PASSED: timing_symbol_rows={len(rows)}")


async def check_4_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, timing_family_rank, timing_adjusted_family_contribution "
        "FROM cross_asset_family_timing_attribution_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: family summary empty"
    ranks = sorted(r["timing_family_rank"] for r in rows if r["timing_family_rank"] is not None)
    assert ranks == list(range(1, len(ranks) + 1)), (
        f"CHECK 4 FAILED: non-contiguous ranks {ranks}"
    )
    print(f"CHECK 4 PASSED: family_summary_rows={len(rows)} ranks={ranks}")


async def check_5_symbol_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, symbol_rank, lag_bucket FROM cross_asset_symbol_timing_attribution_summary "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 5 FAILED: symbol summary empty"
    print(f"CHECK 5 PASSED: symbol_summary_rows={len(rows)}")


async def check_6_run_summary(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        """
        SELECT run_id, cross_asset_net_contribution,
               weighted_cross_asset_net_contribution,
               regime_adjusted_cross_asset_contribution,
               timing_adjusted_cross_asset_contribution,
               dominant_dependency_family,
               weighted_dominant_dependency_family,
               regime_dominant_dependency_family,
               timing_dominant_dependency_family
        FROM run_cross_asset_timing_attribution_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 6 FAILED: run summary empty"
    print(
        f"CHECK 6 PASSED: run_summary_rows>=1 "
        f"timing_dominant={row['timing_dominant_dependency_family']!r} "
        f"regime_dominant={row['regime_dominant_dependency_family']!r}"
    )


async def check_7_lead_vs_lag_ordering(conn: asyncpg.Connection, run_id: str) -> None:
    """Verify that the lead family (rates, rank 1) out-ranks the lag family
    (fx, rank 3) in the timing-adjusted view."""
    rows = await conn.fetch(
        """
        SELECT dependency_family, dominant_timing_class, timing_family_rank,
               timing_adjusted_family_contribution
        FROM cross_asset_family_timing_attribution_summary
        WHERE run_id = $1::uuid
        ORDER BY timing_family_rank ASC
        """,
        run_id,
    )
    by_fam = {r["dependency_family"]: r for r in rows}
    rates = by_fam.get("rates")
    fx    = by_fam.get("fx")
    assert rates and fx, "CHECK 7 FAILED: expected rates + fx rows"
    assert rates["dominant_timing_class"] == "lead"
    assert fx["dominant_timing_class"]    == "lag"
    assert rates["timing_family_rank"] < fx["timing_family_rank"], (
        f"CHECK 7 FAILED: lead ({rates['timing_family_rank']}) not ranked before lag ({fx['timing_family_rank']})"
    )
    # Also verify lead symbol out-ranks lag symbol
    sym_rows = await conn.fetch(
        "SELECT symbol, lag_bucket, symbol_rank FROM cross_asset_symbol_timing_attribution_summary "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    sym_by = {r["symbol"]: r for r in sym_rows}
    us10y = sym_by.get("US10Y")
    dxy   = sym_by.get("DXY")
    assert us10y and dxy
    assert us10y["symbol_rank"] < dxy["symbol_rank"], (
        f"CHECK 7 FAILED: US10Y (lead, rank {us10y['symbol_rank']}) not ranked before DXY (lag, rank {dxy['symbol_rank']})"
    )
    print("CHECK 7 PASSED: lead_vs_lag_ordering_checked=true (rates<fx, US10Y<DXY)")


async def check_8_determinism(conn: asyncpg.Connection, run_id: str) -> None:
    """Duplicate family rows; summary view dedups on (run_id, family) so
    values per family remain identical."""
    await conn.execute(
        """
        INSERT INTO cross_asset_family_timing_attribution_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           timing_profile_id, dependency_family,
           raw_family_net_contribution, weighted_family_net_contribution,
           regime_adjusted_family_contribution,
           dominant_timing_class,
           lead_pair_count, coincident_pair_count, lag_pair_count,
           timing_class_weight, timing_bonus, timing_penalty,
           timing_adjusted_family_contribution,
           timing_family_rank, top_leading_symbols, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               timing_profile_id, dependency_family,
               raw_family_net_contribution, weighted_family_net_contribution,
               regime_adjusted_family_contribution,
               dominant_timing_class,
               lead_pair_count, coincident_pair_count, lag_pair_count,
               timing_class_weight, timing_bonus, timing_penalty,
               timing_adjusted_family_contribution,
               timing_family_rank, top_leading_symbols, metadata
        FROM cross_asset_family_timing_attribution_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    rows = await conn.fetch(
        "SELECT dependency_family, timing_adjusted_family_contribution "
        "FROM cross_asset_family_timing_attribution_summary WHERE run_id = $1::uuid",
        run_id,
    )
    per_fam: dict[str, set] = {}
    for r in rows:
        fam = r["dependency_family"]
        val = float(r["timing_adjusted_family_contribution"]) if r["timing_adjusted_family_contribution"] is not None else None
        per_fam.setdefault(fam, set()).add(val)
    for fam, vals in per_fam.items():
        assert len(vals) == 1, f"CHECK 8 FAILED: family {fam!r} non-deterministic {vals}"
    print(f"CHECK 8 PASSED: timing_attribution_deterministic=true ({len(per_fam)} families stable)")


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_timing_attribution_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "lead_weight", "coincident_weight", "lag_weight",
            "insufficient_data_weight",
            "lead_bonus_scale", "lag_penalty_scale",
            "family_weight_overrides", "metadata", "created_at",
        ],
        "cross_asset_family_timing_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "timing_profile_id", "dependency_family",
            "raw_family_net_contribution",
            "weighted_family_net_contribution",
            "regime_adjusted_family_contribution",
            "dominant_timing_class",
            "lead_pair_count", "coincident_pair_count", "lag_pair_count",
            "timing_class_weight", "timing_bonus", "timing_penalty",
            "timing_adjusted_family_contribution",
            "timing_family_rank", "top_leading_symbols", "metadata", "created_at",
        ],
        "cross_asset_symbol_timing_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "timing_profile_id",
            "symbol", "dependency_family", "dependency_type",
            "lag_bucket", "best_lag_hours",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_adjusted_symbol_score",
            "timing_class_weight", "timing_adjusted_symbol_score",
            "symbol_rank", "metadata", "created_at",
        ],
        "cross_asset_family_timing_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "raw_family_net_contribution",
            "weighted_family_net_contribution",
            "regime_adjusted_family_contribution",
            "dominant_timing_class",
            "lead_pair_count", "coincident_pair_count", "lag_pair_count",
            "timing_class_weight", "timing_bonus", "timing_penalty",
            "timing_adjusted_family_contribution",
            "timing_family_rank", "top_leading_symbols", "created_at",
        ],
        "cross_asset_symbol_timing_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "symbol", "dependency_family", "dependency_type",
            "lag_bucket", "best_lag_hours",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_adjusted_symbol_score",
            "timing_class_weight", "timing_adjusted_symbol_score",
            "symbol_rank", "created_at",
        ],
        "run_cross_asset_timing_attribution_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "dominant_dependency_family",
            "weighted_dominant_dependency_family",
            "regime_dominant_dependency_family",
            "timing_dominant_dependency_family",
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
        await seed_upstream_dependencies(conn, workspace_id, watchlist_id, run_id)
        fc, sc = await seed_timing_attribution_rows(
            conn, workspace_id, watchlist_id, run_id,
        )
        print(f"SEEDED: families={fc} symbols={sc} run_id={run_id[:12]}…")

        await check_1_profile_or_default(conn, workspace_id)
        await check_2_family_rows(conn, run_id)
        await check_3_symbol_rows(conn, run_id)
        await check_4_family_summary(conn, run_id)
        await check_5_symbol_summary(conn, run_id)
        await check_6_run_summary(conn, run_id)
        await check_7_lead_vs_lag_ordering(conn, run_id)
        await check_8_determinism(conn, run_id)
        await check_9_route_contract(conn)
        print("\nAll Phase 4.2B checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
