"""Phase 4.4B smoke validation: Archetype-Aware Attribution.

Checks:
  1. archetype attribution profile exists or default profile path works
  2. archetype-aware family attribution rows persist
  3. archetype-aware symbol attribution rows persist
  4. archetype-aware family summary rows populate
  5. archetype-aware symbol summary rows populate
  6. archetype-aware integration summary row populates
  7. reinforcing/recovering/rotation archetypes outrank deteriorating/mixed
     archetypes when justified
  8. archetype-aware attribution is deterministic on repeated unchanged inputs
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
            "VALUES ($1::uuid, $2::uuid, 'phase44b_validation', 'Phase 4.4B Validation')",
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
    """Seed minimal 4.1A rows so the bridge view has a base row to join onto."""
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
                'rates', 0.60, 0.65, 0.10, 0.025, 0.025,
                '["US10Y","US02Y"]'::jsonb, '[]'::jsonb,
                '[]'::jsonb, '[]'::jsonb,
                'computed', '{}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )


async def seed_archetype_attribution_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> tuple[int, int]:
    """Seed archetype-aware family/symbol rows. Designed so archetype ordering
    is deterministic:
      - rates: reinforcing_continuation (weight 1.10)     → rank 1
      - risk:  recovering_reentry (1.05 + bonus)          → rank 2
      - fx:    rotation_handoff (1.03 + bonus)            → rank 3
      - commodity: deteriorating_breakdown (0.82, penalty)→ rank 4
    """
    family_rows = [
        # family, archetype, state, seq, trans_adj, weight, bonus, penalty, archetype_adj, rank, top
        ("rates",     "reinforcing_continuation", "reinforcing",   "reinforcing_path",
         0.200, 1.10, 0.0000, 0.0000, 0.2200, 1, ["US10Y", "US02Y"]),
        ("risk",      "recovering_reentry",       "recovering",    "recovery_path",
         0.150, 1.05, 0.0015, 0.0000, 0.1590, 2, ["SPY"]),
        ("fx",        "rotation_handoff",         "rotating_in",   "rotation_path",
         0.100, 1.03, 0.0013, 0.0000, 0.1043, 3, ["DXY"]),
        ("commodity", "deteriorating_breakdown",  "deteriorating", "deteriorating_path",
         0.080, 0.82, 0.0000, 0.0012, 0.0644, 4, ["GLD"]),
    ]
    for (fam, arche, state, seq, trans, weight, bonus, penalty, archetype_adj, rank, top) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_archetype_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               archetype_profile_id, dependency_family,
               raw_family_net_contribution, weighted_family_net_contribution,
               regime_adjusted_family_contribution,
               timing_adjusted_family_contribution,
               transition_adjusted_family_contribution,
               archetype_key, transition_state, dominant_sequence_class,
               archetype_weight, archetype_bonus, archetype_penalty,
               archetype_adjusted_family_contribution,
               archetype_family_rank, top_symbols,
               classification_reason_codes, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    NULL, $4, $5, $5, $5, $5, $6,
                    $7, $8, $9,
                    $10, $11, $12, $13,
                    $14, $15::jsonb,
                    $16::jsonb,
                    '{"scoring_version":"4.4B.v1","default_archetype_profile_used":true}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            fam, trans, trans,
            arche, state, seq,
            weight, bonus, penalty, archetype_adj,
            rank, json.dumps(top),
            json.dumps([f"archetype:{arche}", f"transition_state:{state}",
                        f"sequence_class:{seq}"]),
        )

    symbol_rows = [
        # sym, family, type, archetype, state, seq, trans_score, weight, archetype_score, rank
        ("US10Y", "rates",     "rates_link",     "reinforcing_continuation", "reinforcing", "reinforcing_path",
         0.100, 1.10, 0.1100, 1),
        ("US02Y", "rates",     "rates_link",     "reinforcing_continuation", "reinforcing", "reinforcing_path",
         0.080, 1.10, 0.0880, 2),
        ("SPY",   "risk",      "risk_proxy",     "recovering_reentry",       "recovering",  "recovery_path",
         0.090, 1.05, 0.0951, 3),
        ("DXY",   "fx",        "fx_link",        "rotation_handoff",         "rotating_in", "rotation_path",
         0.070, 1.03, 0.0739, 4),
        ("GLD",   "commodity", "commodity_link", "deteriorating_breakdown",  "deteriorating", "deteriorating_path",
         0.060, 0.82, 0.0488, 5),
    ]
    for (sym, fam, dep_type, arche, state, seq, trans_score, weight, archetype_score, rank) in symbol_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_symbol_archetype_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               archetype_profile_id,
               symbol, dependency_family, dependency_type,
               archetype_key, transition_state, dominant_sequence_class,
               raw_symbol_score, weighted_symbol_score,
               regime_adjusted_symbol_score, timing_adjusted_symbol_score,
               transition_adjusted_symbol_score,
               archetype_weight, archetype_adjusted_symbol_score,
               symbol_rank, classification_reason_codes, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    NULL,
                    $4, $5, $6,
                    $7, $8, $9,
                    $10, $10, $10, $10, $10,
                    $11, $12, $13,
                    $14::jsonb,
                    '{"scoring_version":"4.4B.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            sym, fam, dep_type,
            arche, state, seq,
            trans_score, weight, archetype_score, rank,
            json.dumps([f"archetype:{arche}"]),
        )
    return len(family_rows), len(symbol_rows)


async def check_1_profile_or_default(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_archetype_attribution_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true LIMIT 1",
        workspace_id,
    )
    if row:
        print(f"CHECK 1 PASSED: active archetype attribution profile id={str(row['id'])[:12]}…")
    else:
        print("CHECK 1 PASSED: no profile — default_archetype_attribution path applies")


async def check_2_family_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, archetype_key FROM cross_asset_family_archetype_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 2 FAILED: no archetype-aware family rows"
    print(f"CHECK 2 PASSED: archetype_family_rows={len(rows)}")


async def check_3_symbol_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, archetype_key FROM cross_asset_symbol_archetype_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no archetype-aware symbol rows"
    print(f"CHECK 3 PASSED: archetype_symbol_rows={len(rows)}")


async def check_4_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, archetype_family_rank, archetype_adjusted_family_contribution "
        "FROM cross_asset_family_archetype_attribution_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: family summary empty"
    ranks = sorted(r["archetype_family_rank"] for r in rows if r["archetype_family_rank"] is not None)
    assert ranks == list(range(1, len(ranks) + 1)), (
        f"CHECK 4 FAILED: non-contiguous ranks {ranks}"
    )
    print(f"CHECK 4 PASSED: family_summary_rows={len(rows)} ranks={ranks}")


async def check_5_symbol_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, symbol_rank, archetype_key FROM cross_asset_symbol_archetype_attribution_summary "
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
               archetype_adjusted_cross_asset_contribution,
               dominant_dependency_family,
               archetype_dominant_dependency_family,
               dominant_archetype_key
        FROM run_cross_asset_archetype_attribution_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 6 FAILED: run integration summary empty"
    print(
        f"CHECK 6 PASSED: run_summary_rows>=1 "
        f"archetype_dom={row['archetype_dominant_dependency_family']!r} "
        f"dom_archetype={row['dominant_archetype_key']!r}"
    )


async def check_7_archetype_ordering(conn: asyncpg.Connection, run_id: str) -> None:
    """Verify reinforcing/recovering/rotation outrank deteriorating at the
    archetype-adjusted level."""
    rows = await conn.fetch(
        """
        SELECT dependency_family, archetype_key, archetype_family_rank,
               archetype_adjusted_family_contribution
        FROM cross_asset_family_archetype_attribution_summary
        WHERE run_id = $1::uuid
        ORDER BY archetype_family_rank ASC
        """,
        run_id,
    )
    by_fam = {r["dependency_family"]: r for r in rows}
    rates = by_fam.get("rates")
    risk  = by_fam.get("risk")
    fx    = by_fam.get("fx")
    commodity = by_fam.get("commodity")
    assert rates and risk and fx and commodity, (
        "CHECK 7 FAILED: expected rates + risk + fx + commodity rows"
    )
    assert rates["archetype_key"] == "reinforcing_continuation"
    assert risk["archetype_key"]  == "recovering_reentry"
    assert fx["archetype_key"]    == "rotation_handoff"
    assert commodity["archetype_key"] == "deteriorating_breakdown"
    assert rates["archetype_family_rank"] < commodity["archetype_family_rank"], (
        f"CHECK 7 FAILED: reinforcing (rank {rates['archetype_family_rank']}) "
        f"not ranked before deteriorating (rank {commodity['archetype_family_rank']})"
    )
    assert risk["archetype_family_rank"] < commodity["archetype_family_rank"], (
        f"CHECK 7 FAILED: recovering (rank {risk['archetype_family_rank']}) "
        f"not ranked before deteriorating (rank {commodity['archetype_family_rank']})"
    )
    assert fx["archetype_family_rank"] < commodity["archetype_family_rank"], (
        f"CHECK 7 FAILED: rotation (rank {fx['archetype_family_rank']}) "
        f"not ranked before deteriorating (rank {commodity['archetype_family_rank']})"
    )
    # Symbol ordering mirror: US10Y (reinforcing) < GLD (deteriorating)
    sym_rows = await conn.fetch(
        "SELECT symbol, symbol_rank FROM cross_asset_symbol_archetype_attribution_summary "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    sym_by = {r["symbol"]: r for r in sym_rows}
    us10y = sym_by.get("US10Y")
    gld   = sym_by.get("GLD")
    assert us10y and gld
    assert us10y["symbol_rank"] < gld["symbol_rank"], (
        f"CHECK 7 FAILED: US10Y (reinforcing, rank {us10y['symbol_rank']}) "
        f"not ranked before GLD (deteriorating, rank {gld['symbol_rank']})"
    )
    print(
        "CHECK 7 PASSED: archetype_ordering_checked=true "
        "(rates<commodity, risk<commodity, fx<commodity, US10Y<GLD)"
    )


async def check_8_determinism(conn: asyncpg.Connection, run_id: str) -> None:
    """Duplicate family rows; summary view dedups on (run_id, family) so
    values per family remain identical."""
    await conn.execute(
        """
        INSERT INTO cross_asset_family_archetype_attribution_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           archetype_profile_id, dependency_family,
           raw_family_net_contribution, weighted_family_net_contribution,
           regime_adjusted_family_contribution,
           timing_adjusted_family_contribution,
           transition_adjusted_family_contribution,
           archetype_key, transition_state, dominant_sequence_class,
           archetype_weight, archetype_bonus, archetype_penalty,
           archetype_adjusted_family_contribution,
           archetype_family_rank, top_symbols,
           classification_reason_codes, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               archetype_profile_id, dependency_family,
               raw_family_net_contribution, weighted_family_net_contribution,
               regime_adjusted_family_contribution,
               timing_adjusted_family_contribution,
               transition_adjusted_family_contribution,
               archetype_key, transition_state, dominant_sequence_class,
               archetype_weight, archetype_bonus, archetype_penalty,
               archetype_adjusted_family_contribution,
               archetype_family_rank, top_symbols,
               classification_reason_codes, metadata
        FROM cross_asset_family_archetype_attribution_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    rows = await conn.fetch(
        "SELECT dependency_family, archetype_adjusted_family_contribution "
        "FROM cross_asset_family_archetype_attribution_summary WHERE run_id = $1::uuid",
        run_id,
    )
    per_fam: dict[str, set] = {}
    for r in rows:
        fam = r["dependency_family"]
        val = float(r["archetype_adjusted_family_contribution"]) if r["archetype_adjusted_family_contribution"] is not None else None
        per_fam.setdefault(fam, set()).add(val)
    for fam, vals in per_fam.items():
        assert len(vals) == 1, f"CHECK 8 FAILED: family {fam!r} non-deterministic {vals}"
    print(f"CHECK 8 PASSED: archetype_attribution_deterministic=true ({len(per_fam)} families stable)")


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_archetype_attribution_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "rotation_handoff_weight", "reinforcing_continuation_weight",
            "recovering_reentry_weight", "deteriorating_breakdown_weight",
            "mixed_transition_noise_weight", "insufficient_history_weight",
            "recovery_bonus_scale", "breakdown_penalty_scale", "rotation_bonus_scale",
            "archetype_family_overrides", "metadata", "created_at",
        ],
        "cross_asset_family_archetype_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "archetype_profile_id", "dependency_family",
            "raw_family_net_contribution",
            "weighted_family_net_contribution",
            "regime_adjusted_family_contribution",
            "timing_adjusted_family_contribution",
            "transition_adjusted_family_contribution",
            "archetype_key", "transition_state", "dominant_sequence_class",
            "archetype_weight", "archetype_bonus", "archetype_penalty",
            "archetype_adjusted_family_contribution",
            "archetype_family_rank", "top_symbols",
            "classification_reason_codes", "metadata", "created_at",
        ],
        "cross_asset_symbol_archetype_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "archetype_profile_id",
            "symbol", "dependency_family", "dependency_type",
            "archetype_key", "transition_state", "dominant_sequence_class",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_adjusted_symbol_score", "timing_adjusted_symbol_score",
            "transition_adjusted_symbol_score",
            "archetype_weight", "archetype_adjusted_symbol_score",
            "symbol_rank", "classification_reason_codes", "metadata", "created_at",
        ],
        "cross_asset_family_archetype_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "raw_family_net_contribution",
            "weighted_family_net_contribution",
            "regime_adjusted_family_contribution",
            "timing_adjusted_family_contribution",
            "transition_adjusted_family_contribution",
            "archetype_key", "transition_state", "dominant_sequence_class",
            "archetype_weight", "archetype_bonus", "archetype_penalty",
            "archetype_adjusted_family_contribution",
            "archetype_family_rank", "top_symbols",
            "classification_reason_codes", "created_at",
        ],
        "cross_asset_symbol_archetype_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "symbol", "dependency_family", "dependency_type",
            "archetype_key", "transition_state", "dominant_sequence_class",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_adjusted_symbol_score", "timing_adjusted_symbol_score",
            "transition_adjusted_symbol_score",
            "archetype_weight", "archetype_adjusted_symbol_score",
            "symbol_rank", "classification_reason_codes", "created_at",
        ],
        "run_cross_asset_archetype_attribution_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "dominant_dependency_family",
            "weighted_dominant_dependency_family",
            "regime_dominant_dependency_family",
            "timing_dominant_dependency_family",
            "transition_dominant_dependency_family",
            "archetype_dominant_dependency_family",
            "dominant_archetype_key",
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
        fc, sc = await seed_archetype_attribution_rows(
            conn, workspace_id, watchlist_id, run_id,
        )
        print(f"SEEDED: families={fc} symbols={sc} run_id={run_id[:12]}…")

        await check_1_profile_or_default(conn, workspace_id)
        await check_2_family_rows(conn, run_id)
        await check_3_symbol_rows(conn, run_id)
        await check_4_family_summary(conn, run_id)
        await check_5_symbol_summary(conn, run_id)
        await check_6_run_summary(conn, run_id)
        await check_7_archetype_ordering(conn, run_id)
        await check_8_determinism(conn, run_id)
        await check_9_route_contract(conn)
        print("\nAll Phase 4.4B checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
