"""Phase 4.3B smoke validation: Transition-Aware Attribution.

Checks:
  1. transition attribution profile exists or default profile path works
  2. transition-aware family attribution rows persist
  3. transition-aware symbol attribution rows persist
  4. transition-aware family summary rows populate
  5. transition-aware symbol summary rows populate
  6. transition-aware integration summary row populates
  7. transition ordering: reinforcing/recovering out-rank deteriorating/rotating_out
  8. transition-aware attribution is deterministic on repeated unchanged inputs
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
            "VALUES ($1::uuid, $2::uuid, 'phase43b_validation', 'Phase 4.3B Validation')",
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


async def seed_upstream_attribution(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    """Seed the minimal 4.1A snapshot row so the 4.3B integration bridge view
    has a base row to LEFT JOIN onto."""
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


async def seed_transition_attribution_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> tuple[int, int]:
    """Seed transition-aware family/symbol rows. Designed so the transition
    ordering check is deterministic:

      - rates: reinforcing + reinforcing_path    → top  (rank 1)
      - risk:  recovering  + recovery_path       → mid  (rank 2)
      - fx:    deteriorating + deteriorating_path → bottom (rank 3)
      - commodity: rotating_out + mixed_path      → lowest (rank 4)
    """
    family_rows = [
        # family, raw, weighted, regime, timing, state, seq_class,
        # state_w, seq_w, bonus, penalty, adjusted, rank, top_syms
        ("rates",     0.20, 0.20, 0.20, 0.20, "reinforcing",    "reinforcing_path",
         1.10, 1.05, 0.0000, 0.0000, 0.2310, 1, ["US10Y", "US02Y"]),
        ("risk",      0.18, 0.18, 0.18, 0.18, "recovering",     "recovery_path",
         1.03, 1.02, 0.0018, 0.0000, 0.1908, 2, ["SPY"]),
        ("fx",        0.15, 0.15, 0.15, 0.15, "deteriorating",  "deteriorating_path",
         0.85, 0.90, 0.0000, 0.0023, 0.1125, 3, ["DXY"]),
        ("commodity", 0.10, 0.10, 0.10, 0.10, "rotating_out",   "mixed_path",
         0.90, 1.00, 0.0000, 0.0015, 0.0885, 4, ["GLD"]),
    ]
    for (family, raw, weighted, regime, timing, state, seq_class,
         state_w, seq_w, bonus, penalty, adjusted, rank, top_syms) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_transition_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               transition_profile_id, dependency_family,
               raw_family_net_contribution,
               weighted_family_net_contribution,
               regime_adjusted_family_contribution,
               timing_adjusted_family_contribution,
               transition_state, dominant_sequence_class,
               transition_state_weight, sequence_class_weight,
               transition_bonus, transition_penalty,
               transition_adjusted_family_contribution,
               transition_family_rank, top_symbols, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    NULL, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                    $17::jsonb, '{"scoring_version":"4.3B.v1","default_transition_profile_used":true}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            family, raw, weighted, regime, timing, state, seq_class,
            state_w, seq_w, bonus, penalty, adjusted, rank,
            json.dumps(top_syms),
        )

    symbol_rows = [
        # sym, family, type, state, seq_class, raw, weighted, regime, timing,
        # state_w, seq_w, adjusted, rank
        ("US10Y", "rates",     "rates_link",     "reinforcing",   "reinforcing_path",
         0.10, 0.10, 0.10, 0.10, 1.10, 1.05, 0.1155, 1),
        ("US02Y", "rates",     "rates_link",     "reinforcing",   "reinforcing_path",
         0.08, 0.08, 0.08, 0.08, 1.10, 1.05, 0.0924, 2),
        ("SPY",   "risk",      "risk_proxy",     "recovering",    "recovery_path",
         0.09, 0.09, 0.09, 0.09, 1.03, 1.02, 0.0954, 3),
        ("DXY",   "fx",        "fx_link",        "deteriorating", "deteriorating_path",
         0.10, 0.10, 0.10, 0.10, 0.85, 0.90, 0.0750, 4),
        ("GLD",   "commodity", "commodity_link", "rotating_out",  "mixed_path",
         0.08, 0.08, 0.08, 0.08, 0.90, 1.00, 0.0708, 5),
    ]
    for (sym, fam, dep_type, state, seq_class,
         raw_s, weighted_s, regime_s, timing_s,
         state_w, seq_w, adjusted, rank) in symbol_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_symbol_transition_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               transition_profile_id,
               symbol, dependency_family, dependency_type,
               transition_state, dominant_sequence_class,
               raw_symbol_score, weighted_symbol_score,
               regime_adjusted_symbol_score, timing_adjusted_symbol_score,
               transition_state_weight, sequence_class_weight,
               transition_adjusted_symbol_score,
               symbol_rank, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    NULL,
                    $4, $5, $6, $7, $8,
                    $9, $10, $11, $12, $13, $14, $15, $16,
                    '{"scoring_version":"4.3B.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            sym, fam, dep_type, state, seq_class,
            raw_s, weighted_s, regime_s, timing_s,
            state_w, seq_w, adjusted, rank,
        )
    return len(family_rows), len(symbol_rows)


async def check_1_profile_or_default(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_transition_attribution_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true LIMIT 1",
        workspace_id,
    )
    if row:
        print(f"CHECK 1 PASSED: active transition attribution profile id={str(row['id'])[:12]}…")
    else:
        print("CHECK 1 PASSED: no profile — default_transition path applies")


async def check_2_family_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, transition_state FROM cross_asset_family_transition_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 2 FAILED: no transition-aware family rows"
    print(f"CHECK 2 PASSED: transition_family_rows={len(rows)}")


async def check_3_symbol_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, transition_state FROM cross_asset_symbol_transition_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no transition-aware symbol rows"
    print(f"CHECK 3 PASSED: transition_symbol_rows={len(rows)}")


async def check_4_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, transition_family_rank, transition_adjusted_family_contribution "
        "FROM cross_asset_family_transition_attribution_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: family summary empty"
    ranks = sorted(r["transition_family_rank"] for r in rows if r["transition_family_rank"] is not None)
    assert ranks == list(range(1, len(ranks) + 1)), (
        f"CHECK 4 FAILED: non-contiguous ranks {ranks}"
    )
    print(f"CHECK 4 PASSED: family_summary_rows={len(rows)} ranks={ranks}")


async def check_5_symbol_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, symbol_rank, transition_state FROM cross_asset_symbol_transition_attribution_summary "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 5 FAILED: symbol summary empty"
    print(f"CHECK 5 PASSED: symbol_summary_rows={len(rows)}")


async def check_6_run_summary(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        """
        SELECT run_id,
               cross_asset_net_contribution,
               weighted_cross_asset_net_contribution,
               regime_adjusted_cross_asset_contribution,
               timing_adjusted_cross_asset_contribution,
               transition_adjusted_cross_asset_contribution,
               dominant_dependency_family,
               weighted_dominant_dependency_family,
               regime_dominant_dependency_family,
               timing_dominant_dependency_family,
               transition_dominant_dependency_family,
               dominant_transition_state,
               dominant_sequence_class
        FROM run_cross_asset_transition_attribution_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 6 FAILED: run integration summary empty"
    print(
        f"CHECK 6 PASSED: run_summary_rows>=1 "
        f"transition_dominant={row['transition_dominant_dependency_family']!r} "
        f"transition_state={row['dominant_transition_state']!r} "
        f"seq={row['dominant_sequence_class']!r}"
    )


async def check_7_transition_ordering(conn: asyncpg.Connection, run_id: str) -> None:
    """Verify that the reinforcing family (rates) out-ranks the deteriorating
    family (fx), and the recovering family (risk) out-ranks rotating_out
    (commodity) in the transition-adjusted view."""
    rows = await conn.fetch(
        """
        SELECT dependency_family, transition_state, transition_family_rank,
               transition_adjusted_family_contribution
        FROM cross_asset_family_transition_attribution_summary
        WHERE run_id = $1::uuid
        ORDER BY transition_family_rank ASC
        """,
        run_id,
    )
    by_fam = {r["dependency_family"]: r for r in rows}
    rates = by_fam.get("rates")
    fx    = by_fam.get("fx")
    risk  = by_fam.get("risk")
    commodity = by_fam.get("commodity")
    assert rates and fx and risk and commodity, (
        "CHECK 7 FAILED: expected rates + fx + risk + commodity rows"
    )
    assert rates["transition_state"] == "reinforcing"
    assert fx["transition_state"]    == "deteriorating"
    assert risk["transition_state"]  == "recovering"
    assert commodity["transition_state"] == "rotating_out"
    assert rates["transition_family_rank"] < fx["transition_family_rank"], (
        f"CHECK 7 FAILED: reinforcing (rank {rates['transition_family_rank']}) "
        f"not ranked before deteriorating (rank {fx['transition_family_rank']})"
    )
    assert risk["transition_family_rank"] < commodity["transition_family_rank"], (
        f"CHECK 7 FAILED: recovering (rank {risk['transition_family_rank']}) "
        f"not ranked before rotating_out (rank {commodity['transition_family_rank']})"
    )
    # Symbol ordering mirror: US10Y (reinforcing) < DXY (deteriorating)
    sym_rows = await conn.fetch(
        "SELECT symbol, transition_state, symbol_rank FROM cross_asset_symbol_transition_attribution_summary "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    sym_by = {r["symbol"]: r for r in sym_rows}
    us10y = sym_by.get("US10Y")
    dxy   = sym_by.get("DXY")
    assert us10y and dxy
    assert us10y["symbol_rank"] < dxy["symbol_rank"], (
        f"CHECK 7 FAILED: US10Y (reinforcing, rank {us10y['symbol_rank']}) "
        f"not ranked before DXY (deteriorating, rank {dxy['symbol_rank']})"
    )
    print(
        "CHECK 7 PASSED: transition_ordering_checked=true "
        "(rates<fx, risk<commodity, US10Y<DXY)"
    )


async def check_8_determinism(conn: asyncpg.Connection, run_id: str) -> None:
    """Duplicate family rows; summary view dedups on (run_id, family) so
    values per family remain identical."""
    await conn.execute(
        """
        INSERT INTO cross_asset_family_transition_attribution_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           transition_profile_id, dependency_family,
           raw_family_net_contribution,
           weighted_family_net_contribution,
           regime_adjusted_family_contribution,
           timing_adjusted_family_contribution,
           transition_state, dominant_sequence_class,
           transition_state_weight, sequence_class_weight,
           transition_bonus, transition_penalty,
           transition_adjusted_family_contribution,
           transition_family_rank, top_symbols, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               transition_profile_id, dependency_family,
               raw_family_net_contribution,
               weighted_family_net_contribution,
               regime_adjusted_family_contribution,
               timing_adjusted_family_contribution,
               transition_state, dominant_sequence_class,
               transition_state_weight, sequence_class_weight,
               transition_bonus, transition_penalty,
               transition_adjusted_family_contribution,
               transition_family_rank, top_symbols, metadata
        FROM cross_asset_family_transition_attribution_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    rows = await conn.fetch(
        "SELECT dependency_family, transition_adjusted_family_contribution "
        "FROM cross_asset_family_transition_attribution_summary WHERE run_id = $1::uuid",
        run_id,
    )
    per_fam: dict[str, set] = {}
    for r in rows:
        fam = r["dependency_family"]
        val = float(r["transition_adjusted_family_contribution"]) if r["transition_adjusted_family_contribution"] is not None else None
        per_fam.setdefault(fam, set()).add(val)
    for fam, vals in per_fam.items():
        assert len(vals) == 1, f"CHECK 8 FAILED: family {fam!r} non-deterministic {vals}"
    print(f"CHECK 8 PASSED: transition_attribution_deterministic=true ({len(per_fam)} families stable)")


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_transition_attribution_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "reinforcing_weight", "stable_weight", "recovering_weight",
            "rotating_in_weight", "rotating_out_weight",
            "deteriorating_weight", "insufficient_history_weight",
            "recovery_bonus_scale", "degradation_penalty_scale", "rotation_bonus_scale",
            "sequence_class_overrides", "family_weight_overrides",
            "metadata", "created_at",
        ],
        "cross_asset_family_transition_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "transition_profile_id", "dependency_family",
            "raw_family_net_contribution",
            "weighted_family_net_contribution",
            "regime_adjusted_family_contribution",
            "timing_adjusted_family_contribution",
            "transition_state", "dominant_sequence_class",
            "transition_state_weight", "sequence_class_weight",
            "transition_bonus", "transition_penalty",
            "transition_adjusted_family_contribution",
            "transition_family_rank", "top_symbols", "metadata", "created_at",
        ],
        "cross_asset_symbol_transition_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "transition_profile_id",
            "symbol", "dependency_family", "dependency_type",
            "transition_state", "dominant_sequence_class",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_adjusted_symbol_score", "timing_adjusted_symbol_score",
            "transition_state_weight", "sequence_class_weight",
            "transition_adjusted_symbol_score",
            "symbol_rank", "metadata", "created_at",
        ],
        "cross_asset_family_transition_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "raw_family_net_contribution",
            "weighted_family_net_contribution",
            "regime_adjusted_family_contribution",
            "timing_adjusted_family_contribution",
            "transition_state", "dominant_sequence_class",
            "transition_state_weight", "sequence_class_weight",
            "transition_bonus", "transition_penalty",
            "transition_adjusted_family_contribution",
            "transition_family_rank", "top_symbols", "created_at",
        ],
        "cross_asset_symbol_transition_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "symbol", "dependency_family", "dependency_type",
            "transition_state", "dominant_sequence_class",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_adjusted_symbol_score", "timing_adjusted_symbol_score",
            "transition_state_weight", "sequence_class_weight",
            "transition_adjusted_symbol_score",
            "symbol_rank", "created_at",
        ],
        "run_cross_asset_transition_attribution_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "dominant_dependency_family",
            "weighted_dominant_dependency_family",
            "regime_dominant_dependency_family",
            "timing_dominant_dependency_family",
            "transition_dominant_dependency_family",
            "dominant_transition_state",
            "dominant_sequence_class",
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
        await seed_upstream_attribution(conn, workspace_id, watchlist_id, run_id)
        fc, sc = await seed_transition_attribution_rows(
            conn, workspace_id, watchlist_id, run_id,
        )
        print(f"SEEDED: families={fc} symbols={sc} run_id={run_id[:12]}…")

        await check_1_profile_or_default(conn, workspace_id)
        await check_2_family_rows(conn, run_id)
        await check_3_symbol_rows(conn, run_id)
        await check_4_family_summary(conn, run_id)
        await check_5_symbol_summary(conn, run_id)
        await check_6_run_summary(conn, run_id)
        await check_7_transition_ordering(conn, run_id)
        await check_8_determinism(conn, run_id)
        await check_9_route_contract(conn)
        print("\nAll Phase 4.3B checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
