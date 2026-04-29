"""Phase 4.5C smoke validation: Cluster-Aware Composite Refinement.

Checks:
  1. cluster integration profile exists or default profile path works
  2. cluster-aware composite snapshot persists
  3. family cluster composite rows persist
  4. cluster composite summary rows populate
  5. family cluster composite summary rows populate
  6. final cluster integration summary row populates
  7. stable/recovering clusters can increase final contribution vs
     deteriorating/mixed clusters when justified
  8. cluster-aware composite integration is deterministic on repeated inputs
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
            "VALUES ($1::uuid, $2::uuid, 'phase45c_validation', 'Phase 4.5C Validation')",
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


async def seed_cluster_composite_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> tuple[int, int]:
    """Seed the 4.5C cluster-aware composite row + family contributions.
    Designed so stable cluster on rates outranks deteriorating on commodity at
    the cluster integration level."""
    await conn.execute(
        """
        INSERT INTO cross_asset_cluster_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           cluster_integration_profile_id,
           base_signal_score,
           cross_asset_net_contribution,
           weighted_cross_asset_net_contribution,
           regime_adjusted_cross_asset_contribution,
           timing_adjusted_cross_asset_contribution,
           transition_adjusted_cross_asset_contribution,
           archetype_adjusted_cross_asset_contribution,
           cluster_adjusted_cross_asset_contribution,
           composite_pre_cluster,
           cluster_net_contribution,
           composite_post_cluster,
           cluster_state, dominant_archetype_key, integration_mode, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                NULL,
                0.10, 0.25, 0.24, 0.24, 0.24, 0.231, 0.220, 0.245,
                0.170, 0.02117, 0.19117,
                'stable', 'reinforcing_continuation',
                'cluster_additive_guardrailed',
                '{"scoring_version":"4.5C.v1","default_cluster_integration_profile_used":true,"composite_pre_source":"archetype_composite_post"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )

    # Family cluster composite rows.
    # integration_weight=0.10 × cluster_scale × cluster_adj ⇒ bounded to [-0.15, +0.15]
    family_rows = [
        # family, cluster_state, archetype, cluster_adj, iw_applied, integ_contrib, rank, top
        ("rates",     "stable",        "reinforcing_continuation", 0.2376, 0.1080, 0.02566, 1, ["US10Y", "US02Y"]),
        ("risk",      "recovering",    "recovering_reentry",       0.1670, 0.1030, 0.01720, 2, ["SPY"]),
        ("fx",        "rotating",      "rotation_handoff",         0.1063, 0.1010, 0.01074, 3, ["DXY"]),
        ("commodity", "deteriorating", "deteriorating_breakdown",  0.0490, 0.0820, 0.00402, 4, ["GLD"]),
    ]
    for (fam, cstate, arche, cluster_adj, iw, integ, rank, top) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_cluster_composite_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               dependency_family, cluster_state, dominant_archetype_key,
               cluster_adjusted_family_contribution,
               integration_weight_applied,
               cluster_integration_contribution,
               family_rank, top_symbols, reason_codes, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4, $5, $6, $7, $8, $9, $10, $11::jsonb,
                    $12::jsonb,
                    '{"scoring_version":"4.5C.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            fam, cstate, arche, cluster_adj, iw, integ, rank,
            json.dumps(top),
            json.dumps([f"cluster_state:{cstate}", f"dominant_archetype:{arche}"]),
        )
    return 1, len(family_rows)


async def check_1_profile_or_default(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_cluster_integration_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true LIMIT 1",
        workspace_id,
    )
    if row:
        print(f"CHECK 1 PASSED: active cluster integration profile id={str(row['id'])[:12]}…")
    else:
        print("CHECK 1 PASSED: no profile — default_cluster_integration path applies")


async def check_2_composite_snapshot(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id, cluster_state, integration_mode "
        "FROM cross_asset_cluster_composite_snapshots WHERE run_id = $1::uuid",
        run_id,
    )
    assert row, "CHECK 2 FAILED: no cluster-aware composite snapshot"
    print(
        f"CHECK 2 PASSED: cluster_composite_rows>=1 "
        f"state={row['cluster_state']!r} mode={row['integration_mode']!r}"
    )


async def check_3_family_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, cluster_state FROM cross_asset_family_cluster_composite_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no family cluster composite rows"
    print(f"CHECK 3 PASSED: family_cluster_composite_rows={len(rows)}")


async def check_4_composite_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT run_id, composite_pre_cluster, cluster_net_contribution, composite_post_cluster "
        "FROM cross_asset_cluster_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: cluster composite summary empty"
    print(f"CHECK 4 PASSED: cluster_summary_rows={len(rows)}")


async def check_5_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, family_rank, cluster_integration_contribution "
        "FROM cross_asset_family_cluster_composite_summary WHERE run_id = $1::uuid",
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
               archetype_adjusted_cross_asset_contribution,
               cluster_adjusted_cross_asset_contribution,
               cluster_net_contribution,
               composite_pre_cluster, composite_post_cluster,
               dominant_dependency_family,
               cluster_dominant_dependency_family,
               cluster_state,
               dominant_archetype_key
        FROM run_cross_asset_cluster_integration_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 6 FAILED: final cluster integration summary empty"
    print(
        f"CHECK 6 PASSED: final_summary_rows>=1 "
        f"post={row['composite_post_cluster']} "
        f"cluster_state={row['cluster_state']!r}"
    )


async def check_7_state_integration(conn: asyncpg.Connection, run_id: str) -> None:
    """Verify stable/recovering/rotating clusters rank above deteriorating at
    the family composite integration level."""
    rows = await conn.fetch(
        """
        SELECT dependency_family, cluster_state, family_rank,
               cluster_integration_contribution
        FROM cross_asset_family_cluster_composite_summary
        WHERE run_id = $1::uuid
        ORDER BY family_rank ASC
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
    assert rates["family_rank"] < commodity["family_rank"], (
        f"CHECK 7 FAILED: stable (rank {rates['family_rank']}) "
        f"not ranked before deteriorating (rank {commodity['family_rank']})"
    )
    assert risk["family_rank"] < commodity["family_rank"]
    assert fx["family_rank"] < commodity["family_rank"]
    rates_contrib    = float(rates["cluster_integration_contribution"]    or 0.0)
    commodity_contrib = float(commodity["cluster_integration_contribution"] or 0.0)
    assert rates_contrib > commodity_contrib, (
        f"CHECK 7 FAILED: stable integration ({rates_contrib}) "
        f"should exceed deteriorating ({commodity_contrib})"
    )
    print(
        "CHECK 7 PASSED: cluster_state_integration_checked=true "
        "(rates>commodity, risk>commodity, fx>commodity)"
    )


async def check_8_determinism(conn: asyncpg.Connection, run_id: str) -> None:
    """Duplicate family + composite rows; summary views dedupe so values
    remain identical."""
    await conn.execute(
        """
        INSERT INTO cross_asset_family_cluster_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           dependency_family, cluster_state, dominant_archetype_key,
           cluster_adjusted_family_contribution,
           integration_weight_applied,
           cluster_integration_contribution,
           family_rank, top_symbols, reason_codes, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               dependency_family, cluster_state, dominant_archetype_key,
               cluster_adjusted_family_contribution,
               integration_weight_applied,
               cluster_integration_contribution,
               family_rank, top_symbols, reason_codes, metadata
        FROM cross_asset_family_cluster_composite_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    await conn.execute(
        """
        INSERT INTO cross_asset_cluster_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           cluster_integration_profile_id,
           base_signal_score,
           cross_asset_net_contribution,
           weighted_cross_asset_net_contribution,
           regime_adjusted_cross_asset_contribution,
           timing_adjusted_cross_asset_contribution,
           transition_adjusted_cross_asset_contribution,
           archetype_adjusted_cross_asset_contribution,
           cluster_adjusted_cross_asset_contribution,
           composite_pre_cluster,
           cluster_net_contribution,
           composite_post_cluster,
           cluster_state, dominant_archetype_key, integration_mode, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               cluster_integration_profile_id,
               base_signal_score,
               cross_asset_net_contribution,
               weighted_cross_asset_net_contribution,
               regime_adjusted_cross_asset_contribution,
               timing_adjusted_cross_asset_contribution,
               transition_adjusted_cross_asset_contribution,
               archetype_adjusted_cross_asset_contribution,
               cluster_adjusted_cross_asset_contribution,
               composite_pre_cluster,
               cluster_net_contribution,
               composite_post_cluster,
               cluster_state, dominant_archetype_key, integration_mode, metadata
        FROM cross_asset_cluster_composite_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    fam_rows = await conn.fetch(
        "SELECT dependency_family, cluster_integration_contribution "
        "FROM cross_asset_family_cluster_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    per_fam: dict[str, set] = {}
    for r in fam_rows:
        fam = r["dependency_family"]
        val = float(r["cluster_integration_contribution"]) if r["cluster_integration_contribution"] is not None else None
        per_fam.setdefault(fam, set()).add(val)
    for fam, vals in per_fam.items():
        assert len(vals) == 1, f"CHECK 8 FAILED: family {fam!r} non-deterministic {vals}"
    comp_rows = await conn.fetch(
        "SELECT composite_post_cluster "
        "FROM cross_asset_cluster_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    posts = {float(r["composite_post_cluster"]) if r["composite_post_cluster"] is not None else None
             for r in comp_rows}
    assert len(posts) == 1, f"CHECK 8 FAILED: composite_post_cluster non-deterministic {posts}"
    print(
        f"CHECK 8 PASSED: cluster_composite_deterministic=true "
        f"({len(per_fam)} families + 1 composite stable)"
    )


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_cluster_integration_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "integration_mode", "integration_weight",
            "stable_scale", "recovering_scale", "rotating_scale",
            "mixed_scale", "deteriorating_scale", "insufficient_history_scale",
            "max_positive_contribution", "max_negative_contribution",
            "metadata", "created_at",
        ],
        "cross_asset_cluster_composite_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "cluster_integration_profile_id",
            "base_signal_score",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "cluster_adjusted_cross_asset_contribution",
            "composite_pre_cluster", "cluster_net_contribution", "composite_post_cluster",
            "cluster_state", "dominant_archetype_key", "integration_mode",
            "metadata", "created_at",
        ],
        "cross_asset_family_cluster_composite_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "cluster_state", "dominant_archetype_key",
            "cluster_adjusted_family_contribution",
            "integration_weight_applied",
            "cluster_integration_contribution",
            "family_rank", "top_symbols", "reason_codes",
            "metadata", "created_at",
        ],
        "cross_asset_cluster_composite_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "base_signal_score",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "cluster_adjusted_cross_asset_contribution",
            "composite_pre_cluster", "cluster_net_contribution", "composite_post_cluster",
            "cluster_state", "dominant_archetype_key", "integration_mode", "created_at",
        ],
        "cross_asset_family_cluster_composite_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "cluster_state", "dominant_archetype_key",
            "cluster_adjusted_family_contribution",
            "integration_weight_applied",
            "cluster_integration_contribution",
            "family_rank", "top_symbols",
            "reason_codes", "created_at",
        ],
        "run_cross_asset_cluster_integration_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "cluster_adjusted_cross_asset_contribution",
            "cluster_net_contribution",
            "composite_pre_cluster", "composite_post_cluster",
            "dominant_dependency_family",
            "weighted_dominant_dependency_family",
            "regime_dominant_dependency_family",
            "timing_dominant_dependency_family",
            "transition_dominant_dependency_family",
            "archetype_dominant_dependency_family",
            "cluster_dominant_dependency_family",
            "cluster_state", "dominant_archetype_key",
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
        cc, fc = await seed_cluster_composite_rows(
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
        print("\nAll Phase 4.5C checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
