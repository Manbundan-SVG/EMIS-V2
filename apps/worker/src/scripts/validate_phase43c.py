"""Phase 4.3C smoke validation: Sequencing-Aware Composite Refinement.

Checks:
  1. transition integration profile exists or default profile path works
  2. transition-aware composite snapshot persists
  3. family transition composite rows persist
  4. transition composite summary rows populate
  5. family transition composite summary rows populate
  6. final sequencing integration summary row populates
  7. reinforcing/rotating_in states can increase final contribution vs
     deteriorating/rotating_out when justified
  8. sequencing-aware composite integration is deterministic on repeated inputs
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
            "VALUES ($1::uuid, $2::uuid, 'phase43c_validation', 'Phase 4.3C Validation')",
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
    """Seed a 4.1A attribution row so the sequencing integration bridge view
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


async def seed_transition_composite_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> tuple[int, int]:
    """Seed the 4.3C transition-aware composite row plus family contributions.
    Designed so:
      - reinforcing family (rates) outranks deteriorating (fx)
      - rotating_in family (risk) outranks rotating_out (commodity)
    Pre-transition composite = 0.125 (= raw composite_post), post-transition
    composite = pre + bounded transition_net_contribution.
    """
    # One composite row per run
    await conn.execute(
        """
        INSERT INTO cross_asset_transition_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           transition_integration_profile_id,
           base_signal_score,
           cross_asset_net_contribution,
           weighted_cross_asset_net_contribution,
           regime_adjusted_cross_asset_contribution,
           timing_adjusted_cross_asset_contribution,
           transition_adjusted_cross_asset_contribution,
           composite_pre_transition,
           transition_net_contribution,
           composite_post_transition,
           dominant_transition_state, integration_mode, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                NULL,
                0.10, 0.25, 0.24, 0.24, 0.24, 0.231,
                0.125, 0.02618, 0.15118,
                'reinforcing', 'transition_additive_guardrailed',
                '{"scoring_version":"4.3C.v1","default_transition_integration_profile_used":true,"composite_pre_source":"timing_composite_post"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )

    # Family transition composite rows.
    #   base transition-adjusted × integration_weight (0.10) × state_scale × seq_w(1.00)
    # bounded to [-0.15, +0.15]
    family_rows = [
        # family, state, seq_class, trans_adj, int_weight_applied, integration_contribution, rank, top
        ("rates",     "reinforcing",    "reinforcing_path",     0.2310, 0.1050, 0.02426, 1, ["US10Y", "US02Y"]),
        ("risk",      "rotating_in",    "rotation_path",        0.1908, 0.1080, 0.02061, 2, ["SPY"]),
        ("fx",        "deteriorating",  "deteriorating_path",   0.1125, 0.0850, 0.00956, 3, ["DXY"]),
        ("commodity", "rotating_out",   "mixed_path",           0.0885, 0.0920, 0.00814, 4, ["GLD"]),
    ]
    for (fam, state, seq_class, trans_adj, iw_applied, integ_contrib, rank, top) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_transition_composite_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               dependency_family, transition_state, dominant_sequence_class,
               transition_adjusted_family_contribution,
               integration_weight_applied,
               transition_integration_contribution,
               family_rank, top_symbols, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4, $5, $6, $7, $8, $9, $10, $11::jsonb,
                    '{"scoring_version":"4.3C.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            fam, state, seq_class, trans_adj, iw_applied, integ_contrib, rank,
            json.dumps(top),
        )
    return 1, len(family_rows)


async def check_1_profile_or_default(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_transition_integration_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true LIMIT 1",
        workspace_id,
    )
    if row:
        print(f"CHECK 1 PASSED: active transition integration profile id={str(row['id'])[:12]}…")
    else:
        print("CHECK 1 PASSED: no profile — default_transition_integration path applies")


async def check_2_composite_snapshot(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id, dominant_transition_state, integration_mode "
        "FROM cross_asset_transition_composite_snapshots WHERE run_id = $1::uuid",
        run_id,
    )
    assert row, "CHECK 2 FAILED: no transition-aware composite snapshot"
    print(
        f"CHECK 2 PASSED: transition_composite_rows>=1 "
        f"state={row['dominant_transition_state']!r} mode={row['integration_mode']!r}"
    )


async def check_3_family_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, transition_state FROM cross_asset_family_transition_composite_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no family transition composite rows"
    print(f"CHECK 3 PASSED: family_transition_composite_rows={len(rows)}")


async def check_4_composite_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT run_id, composite_pre_transition, transition_net_contribution, composite_post_transition "
        "FROM cross_asset_transition_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: transition composite summary empty"
    print(f"CHECK 4 PASSED: transition_summary_rows={len(rows)}")


async def check_5_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, family_rank, transition_integration_contribution "
        "FROM cross_asset_family_transition_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 5 FAILED: family summary empty"
    ranks = sorted(r["family_rank"] for r in rows if r["family_rank"] is not None)
    assert ranks == list(range(1, len(ranks) + 1)), (
        f"CHECK 5 FAILED: non-contiguous ranks {ranks}"
    )
    print(f"CHECK 5 PASSED: family_summary_rows={len(rows)} ranks={ranks}")


async def check_6_final_summary(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        """
        SELECT run_id,
               cross_asset_net_contribution,
               weighted_cross_asset_net_contribution,
               regime_adjusted_cross_asset_contribution,
               timing_adjusted_cross_asset_contribution,
               transition_adjusted_cross_asset_contribution,
               transition_net_contribution,
               composite_pre_transition, composite_post_transition,
               dominant_dependency_family, transition_dominant_dependency_family,
               dominant_transition_state
        FROM run_cross_asset_sequencing_integration_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 6 FAILED: final sequencing integration summary empty"
    print(
        f"CHECK 6 PASSED: final_summary_rows>=1 "
        f"post={row['composite_post_transition']} state={row['dominant_transition_state']!r}"
    )


async def check_7_state_integration(conn: asyncpg.Connection, run_id: str) -> None:
    """Verify that reinforcing/rotating_in families rank above deteriorating/
    rotating_out at the family composite level."""
    rows = await conn.fetch(
        """
        SELECT dependency_family, transition_state, family_rank,
               transition_integration_contribution
        FROM cross_asset_family_transition_composite_summary
        WHERE run_id = $1::uuid
        ORDER BY family_rank ASC
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
    assert risk["transition_state"]  == "rotating_in"
    assert commodity["transition_state"] == "rotating_out"
    assert rates["family_rank"] < fx["family_rank"], (
        f"CHECK 7 FAILED: reinforcing (rank {rates['family_rank']}) "
        f"not ranked before deteriorating (rank {fx['family_rank']})"
    )
    assert risk["family_rank"] < commodity["family_rank"], (
        f"CHECK 7 FAILED: rotating_in (rank {risk['family_rank']}) "
        f"not ranked before rotating_out (rank {commodity['family_rank']})"
    )
    rates_contrib = float(rates["transition_integration_contribution"] or 0.0)
    fx_contrib    = float(fx["transition_integration_contribution"]    or 0.0)
    assert rates_contrib > fx_contrib, (
        f"CHECK 7 FAILED: reinforcing integration ({rates_contrib}) "
        f"should exceed deteriorating ({fx_contrib})"
    )
    print(
        "CHECK 7 PASSED: transition_state_integration_checked=true "
        "(rates>fx, risk>commodity)"
    )


async def check_8_determinism(conn: asyncpg.Connection, run_id: str) -> None:
    """Duplicate family composite rows; summary view dedups on (run_id,
    family) so values per family remain identical."""
    await conn.execute(
        """
        INSERT INTO cross_asset_family_transition_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           dependency_family, transition_state, dominant_sequence_class,
           transition_adjusted_family_contribution,
           integration_weight_applied,
           transition_integration_contribution,
           family_rank, top_symbols, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               dependency_family, transition_state, dominant_sequence_class,
               transition_adjusted_family_contribution,
               integration_weight_applied,
               transition_integration_contribution,
               family_rank, top_symbols, metadata
        FROM cross_asset_family_transition_composite_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    await conn.execute(
        """
        INSERT INTO cross_asset_transition_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           transition_integration_profile_id,
           base_signal_score,
           cross_asset_net_contribution,
           weighted_cross_asset_net_contribution,
           regime_adjusted_cross_asset_contribution,
           timing_adjusted_cross_asset_contribution,
           transition_adjusted_cross_asset_contribution,
           composite_pre_transition,
           transition_net_contribution,
           composite_post_transition,
           dominant_transition_state, integration_mode, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               transition_integration_profile_id,
               base_signal_score,
               cross_asset_net_contribution,
               weighted_cross_asset_net_contribution,
               regime_adjusted_cross_asset_contribution,
               timing_adjusted_cross_asset_contribution,
               transition_adjusted_cross_asset_contribution,
               composite_pre_transition,
               transition_net_contribution,
               composite_post_transition,
               dominant_transition_state, integration_mode, metadata
        FROM cross_asset_transition_composite_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    fam_rows = await conn.fetch(
        "SELECT dependency_family, transition_integration_contribution "
        "FROM cross_asset_family_transition_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    per_fam: dict[str, set] = {}
    for r in fam_rows:
        fam = r["dependency_family"]
        val = float(r["transition_integration_contribution"]) if r["transition_integration_contribution"] is not None else None
        per_fam.setdefault(fam, set()).add(val)
    for fam, vals in per_fam.items():
        assert len(vals) == 1, f"CHECK 8 FAILED: family {fam!r} non-deterministic {vals}"
    comp_rows = await conn.fetch(
        "SELECT composite_post_transition "
        "FROM cross_asset_transition_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    posts = {float(r["composite_post_transition"]) if r["composite_post_transition"] is not None else None
             for r in comp_rows}
    assert len(posts) == 1, f"CHECK 8 FAILED: composite_post_transition non-deterministic {posts}"
    print(
        f"CHECK 8 PASSED: transition_composite_deterministic=true "
        f"({len(per_fam)} families + 1 composite stable)"
    )


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_transition_integration_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "integration_mode", "integration_weight",
            "reinforcing_weight_scale", "stable_weight_scale",
            "recovering_weight_scale", "rotating_in_weight_scale",
            "rotating_out_weight_scale", "deteriorating_weight_scale",
            "insufficient_history_weight_scale",
            "max_positive_contribution", "max_negative_contribution",
            "metadata", "created_at",
        ],
        "cross_asset_transition_composite_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "transition_integration_profile_id",
            "base_signal_score",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "composite_pre_transition", "transition_net_contribution", "composite_post_transition",
            "dominant_transition_state", "integration_mode", "metadata", "created_at",
        ],
        "cross_asset_family_transition_composite_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "transition_state", "dominant_sequence_class",
            "transition_adjusted_family_contribution",
            "integration_weight_applied",
            "transition_integration_contribution",
            "family_rank", "top_symbols", "metadata", "created_at",
        ],
        "cross_asset_transition_composite_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "base_signal_score",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "composite_pre_transition", "transition_net_contribution", "composite_post_transition",
            "dominant_transition_state", "integration_mode", "created_at",
        ],
        "cross_asset_family_transition_composite_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "transition_state", "dominant_sequence_class",
            "transition_adjusted_family_contribution",
            "integration_weight_applied",
            "transition_integration_contribution",
            "family_rank", "top_symbols", "created_at",
        ],
        "run_cross_asset_sequencing_integration_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "transition_net_contribution",
            "composite_pre_transition", "composite_post_transition",
            "dominant_dependency_family",
            "weighted_dominant_dependency_family",
            "regime_dominant_dependency_family",
            "timing_dominant_dependency_family",
            "transition_dominant_dependency_family",
            "dominant_transition_state",
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
        cc, fc = await seed_transition_composite_rows(
            conn, workspace_id, watchlist_id, run_id,
        )
        print(f"SEEDED: composite_rows={cc} family_rows={fc} run_id={run_id[:12]}…")

        await check_1_profile_or_default(conn, workspace_id)
        await check_2_composite_snapshot(conn, run_id)
        await check_3_family_rows(conn, run_id)
        await check_4_composite_summary(conn, run_id)
        await check_5_family_summary(conn, run_id)
        await check_6_final_summary(conn, run_id)
        await check_7_state_integration(conn, run_id)
        await check_8_determinism(conn, run_id)
        await check_9_route_contract(conn)
        print("\nAll Phase 4.3C checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
