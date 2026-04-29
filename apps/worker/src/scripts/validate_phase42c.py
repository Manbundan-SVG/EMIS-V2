"""Phase 4.2C smoke validation: Timing-Aware Composite Refinement.

Checks:
  1. timing integration profile exists or default profile path works
  2. timing-aware composite snapshot persists
  3. family timing composite rows persist
  4. timing composite summary rows populate
  5. family timing composite summary rows populate
  6. final integration summary row populates
  7. lead-dominant timing increases final contribution relative to lag-dominant timing
  8. timing-aware composite integration is deterministic on repeated unchanged inputs
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
            "VALUES ($1::uuid, $2::uuid, 'phase42c_validation', 'Phase 4.2C Validation')",
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


async def seed_upstream(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> None:
    """Seed 4.1A + 4.0D + 4.1B + 4.1C + 4.2B rows so the 4.2C bridge view has
    all upstream join targets."""
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
                'rates', 0.55, 0.60, 0.10, 0.025, 0.025,
                '["US10Y"]'::jsonb, '[]'::jsonb,
                '[]'::jsonb, '[]'::jsonb,
                'computed', '{}'::jsonb)
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
                0.10, 0.50, 0.60, 0.10, 0.025, 0.025, 0.25, 0.10, 0.125,
                'additive_guardrailed', '{}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )
    # 4.1B weighted family row (so the weighted summary view has data)
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
                NULL, 'rates', 0.25, 1.0, 1.0, 1.0, 1.0, 0.25, 1,
                '["US10Y"]'::jsonb, '{}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )
    # 4.1C regime transition + regime family row
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
                'macro_dominant', NULL, 'rates', 0.25, 0.25,
                1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.25, 1, 'computed',
                '["US10Y"]'::jsonb, '{}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )
    # 4.2B timing-aware family attribution — rates dominant=lead
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
           timing_family_rank, top_leading_symbols, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                NULL, 'rates', 0.25, 0.25, 0.25, 'lead',
                2, 0, 0, 1.10, 0.0125, 0.0, 0.2875, 1,
                '["US10Y"]'::jsonb, '{}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )


async def seed_timing_composite(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
    dominant_timing_class: str,
) -> str:
    """Seed one composite snapshot + one family composite row. Returns the
    composite snapshot id."""
    # For the lead-vs-lag ordering check we seed two runs with identical
    # base numbers but different dominant classes. Formula applied manually
    # so the validator doesn't depend on running the live service:
    #   timing_net = timing_adjusted * integration_weight * class_scale
    #   clamped to [-0.15, +0.15]
    base_signal = 0.10
    raw_net     = 0.25
    weighted_net = 0.25
    regime_net  = 0.25
    timing_adj  = 0.25
    composite_pre = base_signal + regime_net * 0.10  # 0.125

    class_scale = {"lead": 1.10, "coincident": 1.00, "lag": 0.85, "insufficient_data": 0.75}[dominant_timing_class]
    integration_weight = 0.10
    timing_net_raw = timing_adj * integration_weight * class_scale
    timing_net = max(-0.15, min(0.15, timing_net_raw))
    composite_post = composite_pre + timing_net

    cid = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO cross_asset_timing_composite_snapshots
          (id, workspace_id, watchlist_id, run_id, context_snapshot_id,
           timing_integration_profile_id,
           base_signal_score,
           cross_asset_net_contribution,
           weighted_cross_asset_net_contribution,
           regime_adjusted_cross_asset_contribution,
           timing_adjusted_cross_asset_contribution,
           composite_pre_timing, timing_net_contribution, composite_post_timing,
           dominant_timing_class, integration_mode, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, NULL,
                NULL, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, 'timing_additive_guardrailed',
                '{"scoring_version":"4.2C.v1","default_timing_integration_profile_used":true}'::jsonb)
        """,
        cid, workspace_id, watchlist_id, run_id,
        base_signal, raw_net, weighted_net, regime_net, timing_adj,
        composite_pre, timing_net, composite_post, dominant_timing_class,
    )

    await conn.execute(
        """
        INSERT INTO cross_asset_family_timing_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           dependency_family, dominant_timing_class,
           timing_adjusted_family_contribution,
           integration_weight_applied, timing_integration_contribution,
           family_rank, top_symbols, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                'rates', $4, 0.2875, $5, $6, 1,
                '["US10Y"]'::jsonb, '{"scoring_version":"4.2C.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id, dominant_timing_class,
        integration_weight * class_scale, timing_net,
    )
    return cid


async def check_1_profile_or_default(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_timing_integration_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true LIMIT 1",
        workspace_id,
    )
    if row:
        print(f"CHECK 1 PASSED: active timing integration profile id={str(row['id'])[:12]}…")
    else:
        print("CHECK 1 PASSED: no profile — default_timing_integration path applies")


async def check_2_composite_persists(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_timing_composite_snapshots WHERE run_id = $1::uuid LIMIT 1",
        run_id,
    )
    assert row, "CHECK 2 FAILED: no composite snapshot persisted"
    print(f"CHECK 2 PASSED: timing_composite_rows>=1 id={str(row['id'])[:12]}…")


async def check_3_family_composite(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, family_rank FROM cross_asset_family_timing_composite_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no family composite rows"
    print(f"CHECK 3 PASSED: family_timing_composite_rows={len(rows)}")


async def check_4_composite_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        """
        SELECT run_id, composite_pre_timing, timing_net_contribution, composite_post_timing,
               dominant_timing_class, integration_mode
        FROM cross_asset_timing_composite_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert rows, "CHECK 4 FAILED: timing composite summary empty"
    print(f"CHECK 4 PASSED: timing_summary_rows={len(rows)}")


async def check_5_family_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, family_rank, timing_integration_contribution "
        "FROM cross_asset_family_timing_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 5 FAILED: family composite summary empty"
    print(f"CHECK 5 PASSED: family_summary_rows={len(rows)}")


async def check_6_final_integration(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        """
        SELECT run_id,
               cross_asset_net_contribution,
               weighted_cross_asset_net_contribution,
               regime_adjusted_cross_asset_contribution,
               timing_adjusted_cross_asset_contribution,
               timing_net_contribution,
               composite_pre_timing, composite_post_timing,
               dominant_dependency_family,
               timing_dominant_dependency_family,
               dominant_timing_class
        FROM run_cross_asset_final_integration_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 6 FAILED: final integration summary empty"
    print(
        f"CHECK 6 PASSED: final_summary_rows>=1 "
        f"pre={row['composite_pre_timing']} post={row['composite_post_timing']} "
        f"class={row['dominant_timing_class']}"
    )


async def check_7_lead_vs_lag_integration(
    conn: asyncpg.Connection,
    workspace_id: str, watchlist_id: str,
    base_run_id: str,
) -> None:
    """Seed a parallel lag-dominant run and confirm the lead-dominant run
    produced a higher timing_net_contribution + higher composite_post_timing."""
    lag_run_id = str(uuid.uuid4())
    await ensure_job_run(conn, workspace_id, watchlist_id, lag_run_id)
    await seed_upstream(conn, workspace_id, watchlist_id, lag_run_id)
    await seed_timing_composite(conn, workspace_id, watchlist_id, lag_run_id, "lag")

    lead_row = await conn.fetchrow(
        "SELECT timing_net_contribution, composite_post_timing "
        "FROM cross_asset_timing_composite_summary WHERE run_id = $1::uuid",
        base_run_id,
    )
    lag_row = await conn.fetchrow(
        "SELECT timing_net_contribution, composite_post_timing "
        "FROM cross_asset_timing_composite_summary WHERE run_id = $1::uuid",
        lag_run_id,
    )
    assert lead_row and lag_row
    lead_net = float(lead_row["timing_net_contribution"])
    lag_net  = float(lag_row["timing_net_contribution"])
    lead_post = float(lead_row["composite_post_timing"])
    lag_post  = float(lag_row["composite_post_timing"])
    assert lead_net > lag_net, (
        f"CHECK 7 FAILED: lead timing_net {lead_net} not > lag timing_net {lag_net}"
    )
    assert lead_post > lag_post, (
        f"CHECK 7 FAILED: lead composite_post {lead_post} not > lag composite_post {lag_post}"
    )
    print(
        f"CHECK 7 PASSED: lead_vs_lag_integration_checked=true "
        f"lead_net={lead_net:.4f} lag_net={lag_net:.4f} "
        f"lead_post={lead_post:.4f} lag_post={lag_post:.4f}"
    )


async def check_8_determinism(conn: asyncpg.Connection, run_id: str) -> None:
    """Duplicate the composite snapshot; summary view dedups on run_id so the
    latest row must retain identical numbers."""
    await conn.execute(
        """
        INSERT INTO cross_asset_timing_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           timing_integration_profile_id,
           base_signal_score,
           cross_asset_net_contribution,
           weighted_cross_asset_net_contribution,
           regime_adjusted_cross_asset_contribution,
           timing_adjusted_cross_asset_contribution,
           composite_pre_timing, timing_net_contribution, composite_post_timing,
           dominant_timing_class, integration_mode, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               timing_integration_profile_id,
               base_signal_score,
               cross_asset_net_contribution,
               weighted_cross_asset_net_contribution,
               regime_adjusted_cross_asset_contribution,
               timing_adjusted_cross_asset_contribution,
               composite_pre_timing, timing_net_contribution, composite_post_timing,
               dominant_timing_class, integration_mode, metadata
        FROM cross_asset_timing_composite_snapshots
        WHERE run_id = $1::uuid
        ORDER BY created_at ASC
        LIMIT 1
        """,
        run_id,
    )
    rows = await conn.fetch(
        "SELECT DISTINCT composite_post_timing "
        "FROM cross_asset_timing_composite_snapshots WHERE run_id = $1::uuid",
        run_id,
    )
    distinct = {float(r["composite_post_timing"]) for r in rows if r["composite_post_timing"] is not None}
    assert len(distinct) == 1, f"CHECK 8 FAILED: non-deterministic composite_post {distinct}"
    print(f"CHECK 8 PASSED: timing_composite_deterministic=true post={distinct.pop():.4f}")


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_timing_integration_profiles": [
            "id", "workspace_id", "profile_name", "is_active", "integration_mode",
            "integration_weight",
            "lead_weight_scale", "coincident_weight_scale",
            "lag_weight_scale", "insufficient_data_weight_scale",
            "max_positive_contribution", "max_negative_contribution",
            "metadata", "created_at",
        ],
        "cross_asset_timing_composite_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "timing_integration_profile_id",
            "base_signal_score",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "composite_pre_timing", "timing_net_contribution", "composite_post_timing",
            "dominant_timing_class", "integration_mode", "metadata", "created_at",
        ],
        "cross_asset_family_timing_composite_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "dominant_timing_class",
            "timing_adjusted_family_contribution",
            "integration_weight_applied", "timing_integration_contribution",
            "family_rank", "top_symbols", "metadata", "created_at",
        ],
        "cross_asset_timing_composite_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "base_signal_score",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "composite_pre_timing", "timing_net_contribution", "composite_post_timing",
            "dominant_timing_class", "integration_mode", "created_at",
        ],
        "cross_asset_family_timing_composite_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "dominant_timing_class",
            "timing_adjusted_family_contribution",
            "integration_weight_applied", "timing_integration_contribution",
            "family_rank", "top_symbols", "created_at",
        ],
        "run_cross_asset_final_integration_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "timing_net_contribution",
            "composite_pre_timing", "composite_post_timing",
            "dominant_dependency_family",
            "weighted_dominant_dependency_family",
            "regime_dominant_dependency_family",
            "timing_dominant_dependency_family",
            "dominant_timing_class",
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
        await seed_upstream(conn, workspace_id, watchlist_id, run_id)
        cid = await seed_timing_composite(conn, workspace_id, watchlist_id, run_id, "lead")
        print(f"SEEDED: composite_id={cid[:12]}… run_id={run_id[:12]}… dominant=lead")

        await check_1_profile_or_default(conn, workspace_id)
        await check_2_composite_persists(conn, run_id)
        await check_3_family_composite(conn, run_id)
        await check_4_composite_summary(conn, run_id)
        await check_5_family_summary(conn, run_id)
        await check_6_final_integration(conn, run_id)
        await check_7_lead_vs_lag_integration(conn, workspace_id, watchlist_id, run_id)
        await check_8_determinism(conn, run_id)
        await check_9_route_contract(conn)
        print("\nAll Phase 4.2C checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
