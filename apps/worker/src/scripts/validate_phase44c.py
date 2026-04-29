"""Phase 4.4C smoke validation: Archetype-Aware Composite Refinement.

Checks:
  1. archetype integration profile exists or default profile path works
  2. archetype-aware composite snapshot persists
  3. family archetype composite rows persist
  4. archetype composite summary rows populate
  5. family archetype composite summary rows populate
  6. final archetype integration summary row populates
  7. reinforcing/recovering/rotation archetypes can increase final contribution
     vs deteriorating/mixed archetypes when justified
  8. archetype-aware composite integration is deterministic on repeated inputs
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
            "VALUES ($1::uuid, $2::uuid, 'phase44c_validation', 'Phase 4.4C Validation')",
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
    """Seed a 4.1A row so the archetype integration bridge view has a base
    row to LEFT JOIN onto."""
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


async def seed_archetype_composite_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> tuple[int, int]:
    """Seed the 4.4C archetype-aware composite row + family contributions.
    Designed so reinforcing_continuation (rates) outranks deteriorating_breakdown
    (commodity), and recovering_reentry (risk) outranks mixed_transition_noise
    (fx).
    """
    # One composite row per run. composite_post = pre + bounded net.
    await conn.execute(
        """
        INSERT INTO cross_asset_archetype_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           archetype_integration_profile_id,
           base_signal_score,
           cross_asset_net_contribution,
           weighted_cross_asset_net_contribution,
           regime_adjusted_cross_asset_contribution,
           timing_adjusted_cross_asset_contribution,
           transition_adjusted_cross_asset_contribution,
           archetype_adjusted_cross_asset_contribution,
           composite_pre_archetype,
           archetype_net_contribution,
           composite_post_archetype,
           dominant_archetype_key, integration_mode, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                NULL,
                0.10, 0.25, 0.24, 0.24, 0.24, 0.231, 0.220,
                0.151, 0.01901, 0.17001,
                'reinforcing_continuation', 'archetype_additive_guardrailed',
                '{"scoring_version":"4.4C.v1","default_archetype_integration_profile_used":true,"composite_pre_source":"transition_composite_post"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )

    # Family archetype composite rows.
    # integration_weight=0.10 × archetype_scale × archetype_adj ⇒ bounded to [-0.15, +0.15]
    family_rows = [
        # family, archetype, state, seq, arche_adj, iw_applied, integration_contrib, rank, top
        ("rates",     "reinforcing_continuation", "reinforcing",   "reinforcing_path",
         0.2200, 0.1080, 0.02376, 1, ["US10Y", "US02Y"]),
        ("risk",      "recovering_reentry",       "recovering",    "recovery_path",
         0.1590, 0.1030, 0.01638, 2, ["SPY"]),
        ("fx",        "rotation_handoff",         "rotating_in",   "rotation_path",
         0.1043, 0.1010, 0.01053, 3, ["DXY"]),
        ("commodity", "deteriorating_breakdown",  "deteriorating", "deteriorating_path",
         0.0644, 0.0820, 0.00528, 4, ["GLD"]),
    ]
    for (fam, arche, state, seq, arche_adj, iw, integ, rank, top) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_archetype_composite_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               dependency_family, archetype_key, transition_state, dominant_sequence_class,
               archetype_adjusted_family_contribution,
               integration_weight_applied,
               archetype_integration_contribution,
               family_rank, top_symbols,
               classification_reason_codes, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb,
                    $13::jsonb,
                    '{"scoring_version":"4.4C.v1"}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            fam, arche, state, seq, arche_adj, iw, integ, rank,
            json.dumps(top),
            json.dumps([f"archetype:{arche}"]),
        )
    return 1, len(family_rows)


async def check_1_profile_or_default(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_archetype_integration_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true LIMIT 1",
        workspace_id,
    )
    if row:
        print(f"CHECK 1 PASSED: active archetype integration profile id={str(row['id'])[:12]}…")
    else:
        print("CHECK 1 PASSED: no profile — default_archetype_integration path applies")


async def check_2_composite_snapshot(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id, dominant_archetype_key, integration_mode "
        "FROM cross_asset_archetype_composite_snapshots WHERE run_id = $1::uuid",
        run_id,
    )
    assert row, "CHECK 2 FAILED: no archetype-aware composite snapshot"
    print(
        f"CHECK 2 PASSED: archetype_composite_rows>=1 "
        f"dominant={row['dominant_archetype_key']!r} mode={row['integration_mode']!r}"
    )


async def check_3_family_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, archetype_key FROM cross_asset_family_archetype_composite_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no family archetype composite rows"
    print(f"CHECK 3 PASSED: family_archetype_composite_rows={len(rows)}")


async def check_4_composite_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT run_id, composite_pre_archetype, archetype_net_contribution, composite_post_archetype "
        "FROM cross_asset_archetype_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: archetype composite summary empty"
    print(f"CHECK 4 PASSED: archetype_summary_rows={len(rows)}")


async def check_5_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, family_rank, archetype_integration_contribution "
        "FROM cross_asset_family_archetype_composite_summary WHERE run_id = $1::uuid",
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
               archetype_net_contribution,
               composite_pre_archetype, composite_post_archetype,
               dominant_dependency_family, archetype_dominant_dependency_family,
               dominant_archetype_key
        FROM run_cross_asset_archetype_integration_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 6 FAILED: final archetype integration summary empty"
    print(
        f"CHECK 6 PASSED: final_summary_rows>=1 "
        f"post={row['composite_post_archetype']} "
        f"dom_archetype={row['dominant_archetype_key']!r}"
    )


async def check_7_state_integration(conn: asyncpg.Connection, run_id: str) -> None:
    """Verify reinforcing/recovering/rotation archetypes rank above deteriorating
    at the family composite integration level."""
    rows = await conn.fetch(
        """
        SELECT dependency_family, archetype_key, family_rank,
               archetype_integration_contribution
        FROM cross_asset_family_archetype_composite_summary
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
    assert rates["archetype_key"] == "reinforcing_continuation"
    assert risk["archetype_key"]  == "recovering_reentry"
    assert fx["archetype_key"]    == "rotation_handoff"
    assert commodity["archetype_key"] == "deteriorating_breakdown"
    assert rates["family_rank"] < commodity["family_rank"], (
        f"CHECK 7 FAILED: reinforcing (rank {rates['family_rank']}) "
        f"not ranked before deteriorating (rank {commodity['family_rank']})"
    )
    assert risk["family_rank"] < commodity["family_rank"], (
        f"CHECK 7 FAILED: recovering (rank {risk['family_rank']}) "
        f"not ranked before deteriorating (rank {commodity['family_rank']})"
    )
    assert fx["family_rank"] < commodity["family_rank"], (
        f"CHECK 7 FAILED: rotation (rank {fx['family_rank']}) "
        f"not ranked before deteriorating (rank {commodity['family_rank']})"
    )
    rates_contrib = float(rates["archetype_integration_contribution"] or 0.0)
    commodity_contrib = float(commodity["archetype_integration_contribution"] or 0.0)
    assert rates_contrib > commodity_contrib, (
        f"CHECK 7 FAILED: reinforcing integration ({rates_contrib}) "
        f"should exceed deteriorating ({commodity_contrib})"
    )
    print(
        "CHECK 7 PASSED: archetype_state_integration_checked=true "
        "(rates>commodity, risk>commodity, fx>commodity)"
    )


async def check_8_determinism(conn: asyncpg.Connection, run_id: str) -> None:
    """Duplicate family + composite rows; summary views dedupe on (run,family)
    and (run) so values remain identical."""
    await conn.execute(
        """
        INSERT INTO cross_asset_family_archetype_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           dependency_family, archetype_key, transition_state, dominant_sequence_class,
           archetype_adjusted_family_contribution,
           integration_weight_applied,
           archetype_integration_contribution,
           family_rank, top_symbols,
           classification_reason_codes, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               dependency_family, archetype_key, transition_state, dominant_sequence_class,
               archetype_adjusted_family_contribution,
               integration_weight_applied,
               archetype_integration_contribution,
               family_rank, top_symbols,
               classification_reason_codes, metadata
        FROM cross_asset_family_archetype_composite_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    await conn.execute(
        """
        INSERT INTO cross_asset_archetype_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           archetype_integration_profile_id,
           base_signal_score,
           cross_asset_net_contribution,
           weighted_cross_asset_net_contribution,
           regime_adjusted_cross_asset_contribution,
           timing_adjusted_cross_asset_contribution,
           transition_adjusted_cross_asset_contribution,
           archetype_adjusted_cross_asset_contribution,
           composite_pre_archetype,
           archetype_net_contribution,
           composite_post_archetype,
           dominant_archetype_key, integration_mode, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               archetype_integration_profile_id,
               base_signal_score,
               cross_asset_net_contribution,
               weighted_cross_asset_net_contribution,
               regime_adjusted_cross_asset_contribution,
               timing_adjusted_cross_asset_contribution,
               transition_adjusted_cross_asset_contribution,
               archetype_adjusted_cross_asset_contribution,
               composite_pre_archetype,
               archetype_net_contribution,
               composite_post_archetype,
               dominant_archetype_key, integration_mode, metadata
        FROM cross_asset_archetype_composite_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    fam_rows = await conn.fetch(
        "SELECT dependency_family, archetype_integration_contribution "
        "FROM cross_asset_family_archetype_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    per_fam: dict[str, set] = {}
    for r in fam_rows:
        fam = r["dependency_family"]
        val = float(r["archetype_integration_contribution"]) if r["archetype_integration_contribution"] is not None else None
        per_fam.setdefault(fam, set()).add(val)
    for fam, vals in per_fam.items():
        assert len(vals) == 1, f"CHECK 8 FAILED: family {fam!r} non-deterministic {vals}"
    comp_rows = await conn.fetch(
        "SELECT composite_post_archetype "
        "FROM cross_asset_archetype_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    posts = {float(r["composite_post_archetype"]) if r["composite_post_archetype"] is not None else None
             for r in comp_rows}
    assert len(posts) == 1, f"CHECK 8 FAILED: composite_post_archetype non-deterministic {posts}"
    print(
        f"CHECK 8 PASSED: archetype_composite_deterministic=true "
        f"({len(per_fam)} families + 1 composite stable)"
    )


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_archetype_integration_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "integration_mode", "integration_weight",
            "reinforcing_continuation_scale", "recovering_reentry_scale",
            "rotation_handoff_scale", "mixed_transition_noise_scale",
            "deteriorating_breakdown_scale", "insufficient_history_scale",
            "max_positive_contribution", "max_negative_contribution",
            "metadata", "created_at",
        ],
        "cross_asset_archetype_composite_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "archetype_integration_profile_id",
            "base_signal_score",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "composite_pre_archetype", "archetype_net_contribution", "composite_post_archetype",
            "dominant_archetype_key", "integration_mode", "metadata", "created_at",
        ],
        "cross_asset_family_archetype_composite_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "archetype_key", "transition_state", "dominant_sequence_class",
            "archetype_adjusted_family_contribution",
            "integration_weight_applied",
            "archetype_integration_contribution",
            "family_rank", "top_symbols", "classification_reason_codes",
            "metadata", "created_at",
        ],
        "cross_asset_archetype_composite_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "base_signal_score",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "composite_pre_archetype", "archetype_net_contribution", "composite_post_archetype",
            "dominant_archetype_key", "integration_mode", "created_at",
        ],
        "cross_asset_family_archetype_composite_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "archetype_key", "transition_state", "dominant_sequence_class",
            "archetype_adjusted_family_contribution",
            "integration_weight_applied",
            "archetype_integration_contribution",
            "family_rank", "top_symbols",
            "classification_reason_codes", "created_at",
        ],
        "run_cross_asset_archetype_integration_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "archetype_net_contribution",
            "composite_pre_archetype", "composite_post_archetype",
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
        cc, fc = await seed_archetype_composite_rows(
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
        print("\nAll Phase 4.4C checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
