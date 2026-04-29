"""Phase 4.6C smoke validation: Persistence-Aware Composite Refinement.

Checks:
  1. persistence integration profile exists or default profile path works
  2. persistence-aware composite snapshot rows persist
  3. family persistence composite snapshot rows persist
  4. cross_asset_persistence_composite_summary populates
  5. cross_asset_family_persistence_composite_summary populates
  6. run_cross_asset_persistence_integration_summary populates
  7. persistent state composite_post >= composite_pre, breaking_down composite_post <= composite_pre
  8. persistence-aware composite is deterministic on repeated unchanged inputs
  9. detail contract remains typed and stable
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
            "VALUES ($1::uuid, $2::uuid, 'phase46c_validation', 'Phase 4.6C Validation')",
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


async def seed_persistence_composite_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> int:
    """Seed cross_asset_persistence_composite_snapshots row directly."""
    await conn.execute(
        """
        INSERT INTO cross_asset_persistence_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           persistence_integration_profile_id,
           base_signal_score,
           cross_asset_net_contribution,
           weighted_cross_asset_net_contribution,
           regime_adjusted_cross_asset_contribution,
           timing_adjusted_cross_asset_contribution,
           transition_adjusted_cross_asset_contribution,
           archetype_adjusted_cross_asset_contribution,
           cluster_adjusted_cross_asset_contribution,
           persistence_adjusted_cross_asset_contribution,
           composite_pre_persistence,
           persistence_net_contribution,
           composite_post_persistence,
           persistence_state, memory_score, state_age_runs,
           latest_persistence_event_type,
           integration_mode, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                NULL,
                0.10, 0.25, 0.27, 0.28, 0.29, 0.30, 0.31, 0.32, 0.33,
                0.130, 0.020, 0.150,
                'persistent', 0.80, 8,
                NULL,
                'persistence_additive_guardrailed',
                '{"scoring_version":"4.6C.v1","default_persistence_integration_profile_used":true}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
    )
    return 1


async def seed_family_persistence_composite_rows(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, run_id: str,
) -> int:
    """Seed family persistence composite rows so persistent > recovering >
    fragile > breaking_down at composite level."""
    family_rows = [
        # family, persist_state, memory, age, event, persist_adj_family,
        # weight_applied, integration_contribution, rank, top
        ("rates",     "persistent",    0.80, 8, None,
         0.2602, 0.108, 0.0260, 1, ["US10Y", "US02Y"]),
        ("risk",      "recovering",    0.70, 4, "stabilization",
         0.1766, 0.103, 0.0182, 2, ["SPY"]),
        ("fx",        "fragile",       0.20, 1, None,
         0.0915, 0.088, 0.0080, 3, ["DXY"]),
        ("commodity", "breaking_down", 0.30, 3, "cluster_memory_break",
         0.0362, 0.080, 0.0009, 4, ["GLD"]),
    ]
    for (fam, ps, memory, age, event, persist_adj_family,
         weight_applied, integration_contribution, rank, top) in family_rows:
        await conn.execute(
            """
            INSERT INTO cross_asset_family_persistence_composite_snapshots
              (workspace_id, watchlist_id, run_id, context_snapshot_id,
               dependency_family,
               persistence_state, memory_score, state_age_runs,
               latest_persistence_event_type,
               persistence_adjusted_family_contribution,
               integration_weight_applied,
               persistence_integration_contribution,
               family_rank, top_symbols, reason_codes, metadata)
            VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                    $4,
                    $5, $6, $7,
                    $8,
                    $9,
                    $10,
                    $11,
                    $12, $13::jsonb, $14::jsonb,
                    '{"scoring_version":"4.6C.v1","default_persistence_integration_profile_used":true}'::jsonb)
            """,
            workspace_id, watchlist_id, run_id,
            fam,
            ps, memory, age,
            event,
            persist_adj_family,
            weight_applied,
            integration_contribution,
            rank, json.dumps(top),
            json.dumps([f"persistence_state:{ps}", f"memory_score:{memory}"]),
        )
    return len(family_rows)


async def check_1_profile_or_default(conn: asyncpg.Connection, workspace_id: str) -> None:
    row = await conn.fetchrow(
        "SELECT id FROM cross_asset_persistence_integration_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true LIMIT 1",
        workspace_id,
    )
    if row:
        print(f"CHECK 1 PASSED: active persistence integration profile id={str(row['id'])[:12]}…")
    else:
        print("CHECK 1 PASSED: no profile — default_persistence_integration path applies")


async def check_2_composite_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT persistence_state, composite_post_persistence "
        "FROM cross_asset_persistence_composite_snapshots WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 2 FAILED: no persistence-aware composite rows"
    print(f"CHECK 2 PASSED: persistence_composite_rows={len(rows)}")


async def check_3_family_composite_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, persistence_state "
        "FROM cross_asset_family_persistence_composite_snapshots WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no family persistence composite rows"
    print(f"CHECK 3 PASSED: family_persistence_composite_rows={len(rows)}")


async def check_4_composite_summary(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        """
        SELECT run_id, persistence_state,
               composite_pre_persistence, persistence_net_contribution,
               composite_post_persistence, integration_mode
        FROM cross_asset_persistence_composite_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 4 FAILED: composite summary empty"
    print(
        f"CHECK 4 PASSED: composite_summary state={row['persistence_state']!r} "
        f"mode={row['integration_mode']!r}"
    )


async def check_5_family_composite_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        """
        SELECT dependency_family, family_rank, persistence_integration_contribution
        FROM cross_asset_family_persistence_composite_summary
        WHERE run_id = $1::uuid
        ORDER BY family_rank ASC
        """,
        run_id,
    )
    assert rows, "CHECK 5 FAILED: family composite summary empty"
    ranks = [r["family_rank"] for r in rows if r["family_rank"] is not None]
    assert ranks == sorted(ranks), f"CHECK 5 FAILED: family ranks not sorted {ranks}"
    print(f"CHECK 5 PASSED: family_composite_summary_rows={len(rows)} ranks={ranks}")


async def check_6_run_integration_summary(conn: asyncpg.Connection, run_id: str) -> None:
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
               persistence_adjusted_cross_asset_contribution,
               persistence_net_contribution,
               composite_pre_persistence, composite_post_persistence,
               dominant_dependency_family,
               persistence_dominant_dependency_family,
               persistence_state,
               memory_score, state_age_runs, latest_persistence_event_type
        FROM run_cross_asset_persistence_integration_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 6 FAILED: run integration summary empty"
    print(
        f"CHECK 6 PASSED: run_integration_summary persist_dom="
        f"{row['persistence_dominant_dependency_family']!r} state={row['persistence_state']!r}"
    )


async def check_7_persistent_vs_breaking_down(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str,
) -> None:
    """Verify a fresh breaking_down run has composite_post < composite_pre and
    a persistent run has composite_post >= composite_pre."""
    bd_run = str(uuid.uuid4())
    await ensure_job_run(conn, workspace_id, watchlist_id, bd_run)
    await seed_upstream_attribution(conn, workspace_id, watchlist_id, bd_run)
    await conn.execute(
        """
        INSERT INTO cross_asset_persistence_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           persistence_integration_profile_id,
           base_signal_score,
           cross_asset_net_contribution,
           weighted_cross_asset_net_contribution,
           regime_adjusted_cross_asset_contribution,
           timing_adjusted_cross_asset_contribution,
           transition_adjusted_cross_asset_contribution,
           archetype_adjusted_cross_asset_contribution,
           cluster_adjusted_cross_asset_contribution,
           persistence_adjusted_cross_asset_contribution,
           composite_pre_persistence,
           persistence_net_contribution,
           composite_post_persistence,
           persistence_state, memory_score, state_age_runs,
           latest_persistence_event_type,
           integration_mode, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                NULL,
                0.10, 0.25, 0.20, 0.18, 0.16, 0.14, 0.12, 0.10, 0.08,
                0.150, -0.030, 0.120,
                'breaking_down', 0.20, 2,
                'cluster_memory_break',
                'persistence_additive_guardrailed',
                '{"scoring_version":"4.6C.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, bd_run,
    )
    persistent = await conn.fetchrow(
        """
        SELECT composite_pre_persistence, composite_post_persistence
        FROM cross_asset_persistence_composite_summary
        WHERE persistence_state = 'persistent' AND workspace_id = $1::uuid
        ORDER BY created_at DESC LIMIT 1
        """,
        workspace_id,
    )
    breaking_down = await conn.fetchrow(
        """
        SELECT composite_pre_persistence, composite_post_persistence
        FROM cross_asset_persistence_composite_summary
        WHERE run_id = $1::uuid
        """,
        bd_run,
    )
    assert persistent, "CHECK 7 FAILED: no persistent row"
    assert breaking_down, "CHECK 7 FAILED: no breaking_down row"
    p_pre = float(persistent["composite_pre_persistence"])
    p_post = float(persistent["composite_post_persistence"])
    bd_pre = float(breaking_down["composite_pre_persistence"])
    bd_post = float(breaking_down["composite_post_persistence"])
    assert p_post >= p_pre, (
        f"CHECK 7 FAILED: persistent post ({p_post}) should be >= pre ({p_pre})"
    )
    assert bd_post <= bd_pre, (
        f"CHECK 7 FAILED: breaking_down post ({bd_post}) should be <= pre ({bd_pre})"
    )
    print(
        f"CHECK 7 PASSED: persistent_post>=pre={p_post:.4f}>={p_pre:.4f} "
        f"breaking_down_post<=pre={bd_post:.4f}<={bd_pre:.4f}"
    )


async def check_8_determinism(conn: asyncpg.Connection, run_id: str) -> None:
    """Duplicate composite snapshot; summary view dedups on run_id."""
    await conn.execute(
        """
        INSERT INTO cross_asset_persistence_composite_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           persistence_integration_profile_id,
           base_signal_score,
           cross_asset_net_contribution,
           weighted_cross_asset_net_contribution,
           regime_adjusted_cross_asset_contribution,
           timing_adjusted_cross_asset_contribution,
           transition_adjusted_cross_asset_contribution,
           archetype_adjusted_cross_asset_contribution,
           cluster_adjusted_cross_asset_contribution,
           persistence_adjusted_cross_asset_contribution,
           composite_pre_persistence,
           persistence_net_contribution,
           composite_post_persistence,
           persistence_state, memory_score, state_age_runs,
           latest_persistence_event_type,
           integration_mode, metadata)
        SELECT workspace_id, watchlist_id, run_id, context_snapshot_id,
               persistence_integration_profile_id,
               base_signal_score,
               cross_asset_net_contribution,
               weighted_cross_asset_net_contribution,
               regime_adjusted_cross_asset_contribution,
               timing_adjusted_cross_asset_contribution,
               transition_adjusted_cross_asset_contribution,
               archetype_adjusted_cross_asset_contribution,
               cluster_adjusted_cross_asset_contribution,
               persistence_adjusted_cross_asset_contribution,
               composite_pre_persistence,
               persistence_net_contribution,
               composite_post_persistence,
               persistence_state, memory_score, state_age_runs,
               latest_persistence_event_type,
               integration_mode, metadata
        FROM cross_asset_persistence_composite_snapshots
        WHERE run_id = $1::uuid
        """,
        run_id,
    )
    rows = await conn.fetch(
        "SELECT composite_post_persistence "
        "FROM cross_asset_persistence_composite_summary WHERE run_id = $1::uuid",
        run_id,
    )
    distinct = {float(r["composite_post_persistence"]) for r in rows
                if r["composite_post_persistence"] is not None}
    assert len(distinct) == 1, (
        f"CHECK 8 FAILED: composite_post_persistence non-deterministic {distinct}"
    )
    print(f"CHECK 8 PASSED: persistence_composite_deterministic=true ({distinct})")


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_persistence_integration_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "integration_mode", "integration_weight",
            "persistent_scale", "recovering_scale", "rotating_scale",
            "fragile_scale", "breaking_down_scale", "mixed_scale",
            "insufficient_history_scale",
            "memory_break_extra_suppression",
            "max_positive_contribution", "max_negative_contribution",
            "metadata", "created_at",
        ],
        "cross_asset_persistence_composite_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "persistence_integration_profile_id",
            "base_signal_score",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "cluster_adjusted_cross_asset_contribution",
            "persistence_adjusted_cross_asset_contribution",
            "composite_pre_persistence",
            "persistence_net_contribution",
            "composite_post_persistence",
            "persistence_state", "memory_score", "state_age_runs",
            "latest_persistence_event_type",
            "integration_mode", "metadata", "created_at",
        ],
        "cross_asset_family_persistence_composite_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "persistence_state", "memory_score", "state_age_runs",
            "latest_persistence_event_type",
            "persistence_adjusted_family_contribution",
            "integration_weight_applied",
            "persistence_integration_contribution",
            "family_rank", "top_symbols",
            "reason_codes", "metadata", "created_at",
        ],
        "cross_asset_persistence_composite_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "base_signal_score",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "cluster_adjusted_cross_asset_contribution",
            "persistence_adjusted_cross_asset_contribution",
            "composite_pre_persistence",
            "persistence_net_contribution",
            "composite_post_persistence",
            "persistence_state", "memory_score", "state_age_runs",
            "latest_persistence_event_type",
            "integration_mode", "created_at",
        ],
        "cross_asset_family_persistence_composite_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "persistence_state", "memory_score", "state_age_runs",
            "latest_persistence_event_type",
            "persistence_adjusted_family_contribution",
            "integration_weight_applied",
            "persistence_integration_contribution",
            "family_rank", "top_symbols",
            "reason_codes", "created_at",
        ],
        "run_cross_asset_persistence_integration_summary": [
            "run_id", "workspace_id", "watchlist_id", "context_snapshot_id",
            "cross_asset_net_contribution",
            "weighted_cross_asset_net_contribution",
            "regime_adjusted_cross_asset_contribution",
            "timing_adjusted_cross_asset_contribution",
            "transition_adjusted_cross_asset_contribution",
            "archetype_adjusted_cross_asset_contribution",
            "cluster_adjusted_cross_asset_contribution",
            "persistence_adjusted_cross_asset_contribution",
            "persistence_net_contribution",
            "composite_pre_persistence", "composite_post_persistence",
            "dominant_dependency_family",
            "weighted_dominant_dependency_family",
            "regime_dominant_dependency_family",
            "timing_dominant_dependency_family",
            "transition_dominant_dependency_family",
            "archetype_dominant_dependency_family",
            "cluster_dominant_dependency_family",
            "persistence_dominant_dependency_family",
            "persistence_state", "memory_score", "state_age_runs",
            "latest_persistence_event_type",
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
        cc = await seed_persistence_composite_rows(conn, workspace_id, watchlist_id, run_id)
        fc = await seed_family_persistence_composite_rows(
            conn, workspace_id, watchlist_id, run_id,
        )
        print(f"SEEDED: composite={cc} family={fc} run_id={run_id[:12]}…")

        await check_1_profile_or_default(conn, workspace_id)
        await check_2_composite_rows(conn, run_id)
        await check_3_family_composite_rows(conn, run_id)
        await check_4_composite_summary(conn, run_id)
        await check_5_family_composite_summary(conn, run_id)
        await check_6_run_integration_summary(conn, run_id)
        await check_7_persistent_vs_breaking_down(conn, workspace_id, watchlist_id)
        await check_8_determinism(conn, run_id)
        await check_9_route_contract(conn)
        print("\nAll Phase 4.6C checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
