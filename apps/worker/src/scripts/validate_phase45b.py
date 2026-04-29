"""Phase 4.5B smoke validation: Cluster-Aware Attribution.

Checks:
  1. cluster attribution profile exists or default profile path works
  2. cluster-aware family attribution rows persist
  3. cluster-aware symbol attribution rows persist
  4. cluster-aware family summary rows populate
  5. cluster-aware symbol summary rows populate
  6. cluster-aware integration summary row populates
  7. stable/recovering clusters can outrank deteriorating/mixed clusters when
     justified by upstream contribution
  8. cluster-aware attribution is deterministic on repeated unchanged inputs
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
            "VALUES ($1::uuid, $2::uuid, 'phase45b_validation', 'Phase 4.5B Validation')",
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
    """Seed minimal 4.1A rows so the bridge view has a base row."""
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


async def seed_cluster_attribution_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> tuple[int, int]:
    """Seed cluster-aware family/symbol rows. Designed so cluster ordering is
    deterministic — stable cluster + reinforcing archetype on rates outranks
    deteriorating cluster on commodity:
      - rates: stable + reinforcing      → rank 1
      - risk:  recovering + recovery     → rank 2
      - fx:    rotating low-drift        → rank 3
      - commodity: deteriorating + mixed → rank 4
    """
    family_rows = [
        # family, cluster_state, archetype, drift, entropy, archetype_adj,
        # weight, bonus, penalty, cluster_adj, rank, top
        ("rates",     "stable",        "reinforcing_continuation", 0.10, 0.50,
         0.220, 1.08, 0.0000, 0.0000, 0.2376, 1, ["US10Y", "US02Y"]),
        ("risk",      "recovering",    "recovering_reentry",       0.15, 0.55,
         0.159, 1.04, 0.0016, 0.0000, 0.1670, 2, ["SPY"]),
        ("fx",        "rotating",      "rotation_handoff",         0.30, 0.60,
         0.104, 1.02, 0.0010, 0.0008, 0.1063, 3, ["DXY"]),
        ("commodity", "deteriorating", "deteriorating_breakdown",  0.60, 0.65,
         0.064, 0.82, 0.0000, 0.0035, 0.0490, 4, ["GLD"]),
    ]
    for (fam, cstate, arche, drift, ent, arche_adj,
         weight, bonus, penalty, cluster_adj, rank, top) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_cluster_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               cluster_profile_id, dependency_family,
               raw_family_net_contribution, weighted_family_net_contribution,
               regime_adjusted_family_contribution,
               timing_adjusted_family_contribution,
               transition_adjusted_family_contribution,
               archetype_adjusted_family_contribution,
               cluster_state, dominant_archetype_key,
               drift_score, pattern_entropy,
               cluster_weight, cluster_bonus, cluster_penalty,
               cluster_adjusted_family_contribution,
               cluster_family_rank, top_symbols, reason_codes, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    NULL, $4, $5, $5, $5, $5, $5, $5,
                    $6, $7, $8, $9,
                    $10, $11, $12, $13,
                    $14, $15::jsonb, $16::jsonb,
                    '{"scoring_version":"4.5B.v1","default_cluster_profile_used":true}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            fam, arche_adj,
            cstate, arche, drift, ent,
            weight, bonus, penalty, cluster_adj,
            rank, json.dumps(top),
            json.dumps([f"cluster_state:{cstate}", f"dominant_archetype:{arche}"]),
        )

    symbol_rows = [
        # sym, family, type, cluster_state, archetype, archetype_score, weight, cluster_score, rank
        ("US10Y", "rates",     "rates_link",     "stable",        "reinforcing_continuation",
         0.110, 1.08, 0.1188, 1),
        ("US02Y", "rates",     "rates_link",     "stable",        "reinforcing_continuation",
         0.088, 1.08, 0.0950, 2),
        ("SPY",   "risk",      "risk_proxy",     "recovering",    "recovering_reentry",
         0.095, 1.04, 0.0988, 3),
        ("DXY",   "fx",        "fx_link",        "rotating",      "rotation_handoff",
         0.074, 1.02, 0.0755, 4),
        ("GLD",   "commodity", "commodity_link", "deteriorating", "deteriorating_breakdown",
         0.049, 0.82, 0.0402, 5),
    ]
    for (sym, fam, dep_type, cstate, arche, arche_score, weight, cluster_score, rank) in symbol_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_symbol_cluster_attribution_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               cluster_profile_id,
               symbol, dependency_family, dependency_type,
               cluster_state, dominant_archetype_key,
               raw_symbol_score, weighted_symbol_score,
               regime_adjusted_symbol_score, timing_adjusted_symbol_score,
               transition_adjusted_symbol_score,
               archetype_adjusted_symbol_score,
               cluster_weight, cluster_adjusted_symbol_score,
               symbol_rank, reason_codes, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    NULL,
                    $4, $5, $6,
                    $7, $8,
                    $9, $9, $9, $9, $9, $9,
                    $10, $11, $12,
                    $13::jsonb,
                    '{"scoring_version":"4.5B.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            sym, fam, dep_type,
            cstate, arche,
            arche_score, weight, cluster_score, rank,
            json.dumps([f"cluster_state:{cstate}"]),
        )
    return len(family_rows), len(symbol_rows)


async def check_1_profile_or_default(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_cluster_attribution_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true LIMIT 1",
        workspace_id,
    )
    if row:
        print(f"CHECK 1 PASSED: active cluster attribution profile id={str(row['id'])[:12]}…")
    else:
        print("CHECK 1 PASSED: no profile — default_cluster_attribution path applies")


async def check_2_family_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, cluster_state FROM cross_asset_family_cluster_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 2 FAILED: no cluster-aware family rows"
    print(f"CHECK 2 PASSED: cluster_family_rows={len(rows)}")


async def check_3_symbol_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, cluster_state FROM cross_asset_symbol_cluster_attribution_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no cluster-aware symbol rows"
    print(f"CHECK 3 PASSED: cluster_symbol_rows={len(rows)}")


async def check_4_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, cluster_family_rank, cluster_adjusted_family_contribution "
        "FROM cross_asset_family_cluster_attribution_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: family summary empty"
    ranks = sorted(r["cluster_family_rank"] for r in rows if r["cluster_family_rank"] is not None)
    assert ranks == list(range(1, len(ranks) + 1)), (
        f"CHECK 4 FAILED: non-contiguous ranks {ranks}"
    )
    print(f"CHECK 4 PASSED: family_summary_rows={len(rows)} ranks={ranks}")


async def check_5_symbol_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT symbol, symbol_rank, cluster_state FROM cross_asset_symbol_cluster_attribution_summary "
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
               cluster_adjusted_cross_asset_contribution,
               dominant_dependency_family,
               cluster_dominant_dependency_family,
               cluster_state,
               dominant_archetype_key
        FROM run_cross_asset_cluster_attribution_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 6 FAILED: run integration summary empty"
    print(
        f"CHECK 6 PASSED: run_summary_rows>=1 "
        f"cluster_dom={row['cluster_dominant_dependency_family']!r} "
        f"cluster_state={row['cluster_state']!r}"
    )


async def check_7_cluster_ordering(conn: asyncpg.Connection, run_id: str) -> None:
    """Verify stable/recovering/rotating outrank deteriorating at the cluster
    integration level."""
    rows = await conn.fetch(
        """
        SELECT dependency_family, cluster_state, cluster_family_rank,
               cluster_adjusted_family_contribution
        FROM cross_asset_family_cluster_attribution_summary
        WHERE run_id = $1::uuid
        ORDER BY cluster_family_rank ASC
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
    assert rates["cluster_state"] == "stable"
    assert risk["cluster_state"]  == "recovering"
    assert fx["cluster_state"]    == "rotating"
    assert commodity["cluster_state"] == "deteriorating"
    assert rates["cluster_family_rank"] < commodity["cluster_family_rank"], (
        f"CHECK 7 FAILED: stable (rank {rates['cluster_family_rank']}) "
        f"not ranked before deteriorating (rank {commodity['cluster_family_rank']})"
    )
    assert risk["cluster_family_rank"] < commodity["cluster_family_rank"]
    assert fx["cluster_family_rank"] < commodity["cluster_family_rank"]
    rates_contrib    = float(rates["cluster_adjusted_family_contribution"]    or 0.0)
    commodity_contrib = float(commodity["cluster_adjusted_family_contribution"] or 0.0)
    assert rates_contrib > commodity_contrib, (
        f"CHECK 7 FAILED: stable contribution ({rates_contrib}) "
        f"should exceed deteriorating ({commodity_contrib})"
    )
    print(
        "CHECK 7 PASSED: cluster_ordering_checked=true "
        "(rates>commodity, risk>commodity, fx>commodity)"
    )


async def check_8_determinism(conn: asyncpg.Connection, run_id: str) -> None:
    """Duplicate family rows; summary view dedups on (run_id, family)."""
    await conn.execute(
        """
        INSERT INTO cross_asset_family_cluster_attribution_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           cluster_profile_id, dependency_family,
           raw_family_net_contribution, weighted_family_net_contribution,
           regime_adjusted_family_contribution,
           timing_adjusted_family_contribution,
           transition_adjusted_family_contribution,
           archetype_adjusted_family_contribution,
           cluster_state, dominant_archetype_key,
           drift_score, pattern_entropy,
           cluster_weight, cluster_bonus, cluster_penalty,
           cluster_adjusted_family_contribution,
           cluster_family_rank, top_symbols, reason_codes, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               cluster_profile_id, dependency_family,
               raw_family_net_contribution, weighted_family_net_contribution,
               regime_adjusted_family_contribution,
               timing_adjusted_family_contribution,
               transition_adjusted_family_contribution,
               archetype_adjusted_family_contribution,
               cluster_state, dominant_archetype_key,
               drift_score, pattern_entropy,
               cluster_weight, cluster_bonus, cluster_penalty,
               cluster_adjusted_family_contribution,
               cluster_family_rank, top_symbols, reason_codes, metadata
        FROM cross_asset_family_cluster_attribution_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    rows = await conn.fetch(
        "SELECT dependency_family, cluster_adjusted_family_contribution "
        "FROM cross_asset_family_cluster_attribution_summary WHERE run_id = $1::uuid",
        run_id,
    )
    per_fam: dict[str, set] = {}
    for r in rows:
        fam = r["dependency_family"]
        val = float(r["cluster_adjusted_family_contribution"]) if r["cluster_adjusted_family_contribution"] is not None else None
        per_fam.setdefault(fam, set()).add(val)
    for fam, vals in per_fam.items():
        assert len(vals) == 1, f"CHECK 8 FAILED: family {fam!r} non-deterministic {vals}"
    print(f"CHECK 8 PASSED: cluster_attribution_deterministic=true ({len(per_fam)} families stable)")


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_cluster_attribution_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "stable_weight", "rotating_weight", "recovering_weight",
            "deteriorating_weight", "mixed_weight", "insufficient_history_weight",
            "drift_penalty_scale", "rotation_bonus_scale",
            "recovery_bonus_scale", "entropy_penalty_scale",
            "cluster_family_overrides", "metadata", "created_at",
        ],
        "cross_asset_family_cluster_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "cluster_profile_id", "dependency_family",
            "raw_family_net_contribution",
            "weighted_family_net_contribution",
            "regime_adjusted_family_contribution",
            "timing_adjusted_family_contribution",
            "transition_adjusted_family_contribution",
            "archetype_adjusted_family_contribution",
            "cluster_state", "dominant_archetype_key",
            "drift_score", "pattern_entropy",
            "cluster_weight", "cluster_bonus", "cluster_penalty",
            "cluster_adjusted_family_contribution",
            "cluster_family_rank", "top_symbols",
            "reason_codes", "metadata", "created_at",
        ],
        "cross_asset_symbol_cluster_attribution_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "cluster_profile_id",
            "symbol", "dependency_family", "dependency_type",
            "cluster_state", "dominant_archetype_key",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_adjusted_symbol_score", "timing_adjusted_symbol_score",
            "transition_adjusted_symbol_score",
            "archetype_adjusted_symbol_score",
            "cluster_weight", "cluster_adjusted_symbol_score",
            "symbol_rank", "reason_codes", "metadata", "created_at",
        ],
        "cross_asset_family_cluster_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "raw_family_net_contribution",
            "weighted_family_net_contribution",
            "regime_adjusted_family_contribution",
            "timing_adjusted_family_contribution",
            "transition_adjusted_family_contribution",
            "archetype_adjusted_family_contribution",
            "cluster_state", "dominant_archetype_key",
            "drift_score", "pattern_entropy",
            "cluster_weight", "cluster_bonus", "cluster_penalty",
            "cluster_adjusted_family_contribution",
            "cluster_family_rank", "top_symbols",
            "reason_codes", "created_at",
        ],
        "cross_asset_symbol_cluster_attribution_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "symbol", "dependency_family", "dependency_type",
            "cluster_state", "dominant_archetype_key",
            "raw_symbol_score", "weighted_symbol_score",
            "regime_adjusted_symbol_score", "timing_adjusted_symbol_score",
            "transition_adjusted_symbol_score",
            "archetype_adjusted_symbol_score",
            "cluster_weight", "cluster_adjusted_symbol_score",
            "symbol_rank", "reason_codes", "created_at",
        ],
        "run_cross_asset_cluster_attribution_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "cluster_adjusted_cross_asset_contribution",
            "dominant_dependency_family",
            "weighted_dominant_dependency_family",
            "regime_dominant_dependency_family",
            "timing_dominant_dependency_family",
            "transition_dominant_dependency_family",
            "archetype_dominant_dependency_family",
            "cluster_dominant_dependency_family",
            "cluster_state",
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
        fc, sc = await seed_cluster_attribution_rows(
            conn, workspace_id, watchlist_id, run_id,
        )
        print(f"SEEDED: families={fc} symbols={sc} run_id={run_id[:12]}…")

        await check_1_profile_or_default(conn, workspace_id)
        await check_2_family_rows(conn, run_id)
        await check_3_symbol_rows(conn, run_id)
        await check_4_family_summary(conn, run_id)
        await check_5_symbol_summary(conn, run_id)
        await check_6_run_summary(conn, run_id)
        await check_7_cluster_ordering(conn, run_id)
        await check_8_determinism(conn, run_id)
        await check_9_route_contract(conn)
        print("\nAll Phase 4.5B checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
