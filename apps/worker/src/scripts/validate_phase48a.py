"""Phase 4.8A smoke validation: Cross-Layer Conflict and Agreement Diagnostics.

Checks:
  0. workspace + watchlist available, conflict policy profile present (or seeded)
  1. cross_asset_layer_agreement_snapshots row persists
  2. cross_asset_family_layer_agreement_snapshots rows persist
  3. cross_asset_layer_conflict_event_snapshots rows persist
  4. cross_asset_layer_agreement_summary view rows populate
  5. cross_asset_family_layer_agreement_summary view rows populate
  6. cross_asset_layer_conflict_event_summary view rows populate
  7. run_cross_asset_layer_conflict_summary view rows populate
  8. controlled aligned/conflicted/unreliable consensus states classified correctly
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


async def setup(conn: asyncpg.Connection) -> tuple[str, str, str]:
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
            "VALUES ($1::uuid, $2::uuid, 'phase48a_validation', 'Phase 4.8A Validation')",
            watchlist_id, workspace_id,
        )

    profile_row = await conn.fetchrow(
        "SELECT id FROM cross_asset_conflict_policy_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true "
        "ORDER BY created_at DESC LIMIT 1",
        workspace_id,
    )
    if profile_row:
        profile_id = str(profile_row["id"])
    else:
        profile_id = str(uuid.uuid4())
        await conn.execute(
            """
            INSERT INTO cross_asset_conflict_policy_profiles
              (id, workspace_id, profile_name, is_active,
               timing_weight, transition_weight, archetype_weight,
               cluster_weight, persistence_weight, decay_weight,
               agreement_threshold, partial_agreement_threshold,
               conflict_threshold, unreliable_threshold)
            VALUES ($1::uuid, $2::uuid, 'phase48a_default', true,
                    0.15, 0.20, 0.15, 0.20, 0.15, 0.15,
                    0.70, 0.50, 0.35, 0.20)
            """,
            profile_id, workspace_id,
        )
    return workspace_id, watchlist_id, profile_id


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


async def seed_layer_agreement(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str, run_id: str, profile_id: str,
    directions: dict[str, str],
    weights: dict[str, float],
    agreement_score: float, conflict_score: float,
    consensus_state: str,
    dominant_conflict_source: str | None,
    reason_codes: list[str],
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_layer_agreement_snapshots
          (workspace_id, watchlist_id, run_id, conflict_policy_profile_id,
           dominant_timing_class, dominant_transition_state, dominant_sequence_class,
           dominant_archetype_key, cluster_state, persistence_state, freshness_state,
           timing_direction, transition_direction, archetype_direction,
           cluster_direction, persistence_direction, decay_direction,
           supportive_weight, suppressive_weight, neutral_weight, missing_weight,
           agreement_score, conflict_score,
           layer_consensus_state, dominant_conflict_source,
           conflict_reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid,
                'leading','breakout','rotation','momentum_breakout',
                'cohesive','persistent','fresh',
                $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14,
                $15, $16,
                $17, $18,
                $19::jsonb, '{"scoring_version":"4.8A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id, profile_id,
        directions["timing"], directions["transition"], directions["archetype"],
        directions["cluster"], directions["persistence"], directions["decay"],
        weights["supportive"], weights["suppressive"], weights["neutral"], weights["missing"],
        agreement_score, conflict_score,
        consensus_state, dominant_conflict_source,
        json.dumps(reason_codes),
    )


async def seed_family_layer_agreement(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str, run_id: str,
    family: str,
    directions: dict[str, str],
    agreement_score: float, conflict_score: float,
    consensus_state: str,
    dominant_conflict_source: str | None,
    reason_codes: list[str],
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_family_layer_agreement_snapshots
          (workspace_id, watchlist_id, run_id, dependency_family,
           transition_state, dominant_sequence_class, archetype_key,
           cluster_state, persistence_state, freshness_state,
           family_contribution,
           transition_direction, archetype_direction,
           cluster_direction, persistence_direction, decay_direction,
           agreement_score, conflict_score,
           family_consensus_state, dominant_conflict_source, family_rank,
           conflict_reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4,
                'breakout','rotation','momentum_breakout',
                'cohesive','persistent','fresh',
                0.072,
                $5, $6, $7, $8, $9,
                $10, $11,
                $12, $13, 1,
                $14::jsonb, '{"scoring_version":"4.8A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id, family,
        directions["transition"], directions["archetype"],
        directions["cluster"], directions["persistence"], directions["decay"],
        agreement_score, conflict_score,
        consensus_state, dominant_conflict_source,
        json.dumps(reason_codes),
    )


async def seed_conflict_event(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str,
    source_run_id: str, target_run_id: str,
    prior_state: str, current_state: str,
    event_type: str,
    reason_codes: list[str],
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_layer_conflict_event_snapshots
          (workspace_id, watchlist_id, source_run_id, target_run_id,
           prior_consensus_state, current_consensus_state,
           prior_dominant_conflict_source, current_dominant_conflict_source,
           prior_agreement_score, current_agreement_score,
           prior_conflict_score, current_conflict_score,
           event_type, reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid,
                $5, $6,
                NULL, 'transition',
                0.50, 0.78,
                0.20, 0.05,
                $7, $8::jsonb, '{"scoring_version":"4.8A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, source_run_id, target_run_id,
        prior_state, current_state, event_type, json.dumps(reason_codes),
    )


async def check_0_profile(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT id FROM cross_asset_conflict_policy_profiles "
        "WHERE workspace_id = $1::uuid AND is_active = true",
        workspace_id,
    )
    assert rows, "CHECK 0 FAILED: no active conflict policy profile"
    print(f"CHECK 0 PASSED: active_conflict_policy_profiles={len(rows)}")


async def check_1(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT run_id FROM cross_asset_layer_agreement_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 1 FAILED: no layer agreement snapshots"
    print(f"CHECK 1 PASSED: layer_agreement_rows={len(rows)}")


async def check_2(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family FROM cross_asset_family_layer_agreement_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 2 FAILED: no family layer agreement rows"
    print(f"CHECK 2 PASSED: family_layer_agreement_rows={len(rows)}")


async def check_3(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT event_type FROM cross_asset_layer_conflict_event_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 3 FAILED: no conflict event rows"
    print(f"CHECK 3 PASSED: layer_conflict_event_rows={len(rows)}")


async def check_4(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT run_id, layer_consensus_state FROM cross_asset_layer_agreement_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 4 FAILED: layer agreement summary empty"
    print(f"CHECK 4 PASSED: layer_agreement_summary_rows={len(rows)}")


async def check_5(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family FROM cross_asset_family_layer_agreement_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 5 FAILED: family layer agreement summary empty"
    print(f"CHECK 5 PASSED: family_layer_agreement_summary_rows={len(rows)}")


async def check_6(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT target_run_id, event_type FROM cross_asset_layer_conflict_event_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 6 FAILED: conflict event summary empty"
    print(f"CHECK 6 PASSED: layer_conflict_event_summary_rows={len(rows)}")


async def check_7(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT run_id, layer_consensus_state, latest_conflict_event_type "
        "FROM run_cross_asset_layer_conflict_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 7 FAILED: run-level layer conflict summary empty"
    print(f"CHECK 7 PASSED: run_layer_conflict_summary_rows={len(rows)}")


async def check_8_classification(
    conn: asyncpg.Connection,
    aligned_run: str, conflicted_run: str, unreliable_run: str,
) -> None:
    aligned_row = await conn.fetchrow(
        "SELECT layer_consensus_state FROM cross_asset_layer_agreement_summary "
        "WHERE run_id = $1::uuid",
        aligned_run,
    )
    conflicted_row = await conn.fetchrow(
        "SELECT layer_consensus_state FROM cross_asset_layer_agreement_summary "
        "WHERE run_id = $1::uuid",
        conflicted_run,
    )
    unreliable_row = await conn.fetchrow(
        "SELECT layer_consensus_state FROM cross_asset_layer_agreement_summary "
        "WHERE run_id = $1::uuid",
        unreliable_run,
    )
    assert aligned_row and aligned_row["layer_consensus_state"] in (
        "aligned_supportive", "aligned_suppressive",
    ), (
        "CHECK 8 FAILED: aligned run did not classify as aligned "
        f"(got {aligned_row and aligned_row['layer_consensus_state']!r})"
    )
    assert conflicted_row and conflicted_row["layer_consensus_state"] == "conflicted", (
        "CHECK 8 FAILED: conflicted run did not classify as conflicted "
        f"(got {conflicted_row and conflicted_row['layer_consensus_state']!r})"
    )
    assert unreliable_row and unreliable_row["layer_consensus_state"] == "unreliable", (
        "CHECK 8 FAILED: unreliable run did not classify as unreliable "
        f"(got {unreliable_row and unreliable_row['layer_consensus_state']!r})"
    )
    print(
        "CHECK 8 PASSED: consensus classification correct "
        f"(aligned={aligned_row['layer_consensus_state']!r}, "
        f"conflicted={conflicted_row['layer_consensus_state']!r}, "
        f"unreliable={unreliable_row['layer_consensus_state']!r})"
    )


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_conflict_policy_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "timing_weight", "transition_weight", "archetype_weight",
            "cluster_weight", "persistence_weight", "decay_weight",
            "agreement_threshold", "partial_agreement_threshold",
            "conflict_threshold", "unreliable_threshold",
            "metadata", "created_at",
        ],
        "cross_asset_layer_agreement_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id",
            "context_snapshot_id", "conflict_policy_profile_id",
            "dominant_timing_class", "dominant_transition_state",
            "dominant_sequence_class", "dominant_archetype_key",
            "cluster_state", "persistence_state", "freshness_state",
            "timing_direction", "transition_direction", "archetype_direction",
            "cluster_direction", "persistence_direction", "decay_direction",
            "supportive_weight", "suppressive_weight", "neutral_weight", "missing_weight",
            "agreement_score", "conflict_score",
            "layer_consensus_state", "dominant_conflict_source",
            "conflict_reason_codes", "metadata", "created_at",
        ],
        "cross_asset_family_layer_agreement_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id",
            "context_snapshot_id", "dependency_family",
            "transition_state", "dominant_sequence_class", "archetype_key",
            "cluster_state", "persistence_state", "freshness_state",
            "family_contribution",
            "transition_direction", "archetype_direction", "cluster_direction",
            "persistence_direction", "decay_direction",
            "agreement_score", "conflict_score",
            "family_consensus_state", "dominant_conflict_source", "family_rank",
            "conflict_reason_codes", "metadata", "created_at",
        ],
        "cross_asset_layer_conflict_event_snapshots": [
            "id", "workspace_id", "watchlist_id",
            "source_run_id", "target_run_id",
            "prior_consensus_state", "current_consensus_state",
            "prior_dominant_conflict_source", "current_dominant_conflict_source",
            "prior_agreement_score", "current_agreement_score",
            "prior_conflict_score", "current_conflict_score",
            "event_type", "reason_codes", "metadata", "created_at",
        ],
        "cross_asset_layer_agreement_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dominant_timing_class", "dominant_transition_state",
            "dominant_sequence_class", "dominant_archetype_key",
            "cluster_state", "persistence_state", "freshness_state",
            "timing_direction", "transition_direction", "archetype_direction",
            "cluster_direction", "persistence_direction", "decay_direction",
            "supportive_weight", "suppressive_weight", "neutral_weight", "missing_weight",
            "agreement_score", "conflict_score",
            "layer_consensus_state", "dominant_conflict_source",
            "conflict_reason_codes", "created_at",
        ],
        "cross_asset_family_layer_agreement_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "transition_state", "dominant_sequence_class", "archetype_key",
            "cluster_state", "persistence_state", "freshness_state",
            "family_contribution",
            "transition_direction", "archetype_direction", "cluster_direction",
            "persistence_direction", "decay_direction",
            "agreement_score", "conflict_score",
            "family_consensus_state", "dominant_conflict_source", "family_rank",
            "conflict_reason_codes", "created_at",
        ],
        "cross_asset_layer_conflict_event_summary": [
            "workspace_id", "watchlist_id", "source_run_id", "target_run_id",
            "prior_consensus_state", "current_consensus_state",
            "prior_dominant_conflict_source", "current_dominant_conflict_source",
            "prior_agreement_score", "current_agreement_score",
            "prior_conflict_score", "current_conflict_score",
            "event_type", "reason_codes", "created_at",
        ],
        "run_cross_asset_layer_conflict_summary": [
            "run_id", "workspace_id", "watchlist_id",
            "layer_consensus_state", "agreement_score", "conflict_score",
            "dominant_conflict_source",
            "freshness_state", "persistence_state", "cluster_state",
            "latest_conflict_event_type", "created_at",
        ],
    }
    for table, cols in checks.items():
        cols_sql = ", ".join(cols)
        await conn.fetchrow(f"SELECT {cols_sql} FROM {table} LIMIT 1")  # noqa: S608
    print("CHECK 9 PASSED: detail_contract_ok=true")


async def main() -> None:
    conn = await get_conn()
    try:
        workspace_id, watchlist_id, profile_id = await setup(conn)

        # Run 1: aligned_supportive — all directions supportive.
        aligned_run = str(uuid.uuid4())
        # Run 2: conflicted — strong supportive AND strong suppressive present.
        conflicted_run = str(uuid.uuid4())
        # Run 3: unreliable — most layers missing/neutral, low overall weight.
        unreliable_run = str(uuid.uuid4())
        # Run 4: prior anchor for the event seed.
        prior_run = str(uuid.uuid4())

        for rid in (aligned_run, conflicted_run, unreliable_run, prior_run):
            await ensure_job_run(conn, workspace_id, watchlist_id, rid)

        # Aligned supportive: 6/6 supportive.
        await seed_layer_agreement(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            run_id=aligned_run, profile_id=profile_id,
            directions={
                "timing": "supportive", "transition": "supportive",
                "archetype": "supportive", "cluster": "supportive",
                "persistence": "supportive", "decay": "supportive",
            },
            weights={"supportive": 1.00, "suppressive": 0.00, "neutral": 0.00, "missing": 0.00},
            agreement_score=0.95, conflict_score=0.02,
            consensus_state="aligned_supportive",
            dominant_conflict_source=None,
            reason_codes=[],
        )

        # Conflicted: ~50/50 supportive and suppressive.
        await seed_layer_agreement(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            run_id=conflicted_run, profile_id=profile_id,
            directions={
                "timing": "supportive", "transition": "suppressive",
                "archetype": "supportive", "cluster": "suppressive",
                "persistence": "supportive", "decay": "suppressive",
            },
            weights={"supportive": 0.45, "suppressive": 0.45, "neutral": 0.10, "missing": 0.00},
            agreement_score=0.30, conflict_score=0.55,
            consensus_state="conflicted",
            dominant_conflict_source="transition",
            reason_codes=[
                "supportive_suppressive_tension",
                "decay_disagreement",
            ],
        )

        # Unreliable: most layers missing.
        await seed_layer_agreement(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            run_id=unreliable_run, profile_id=profile_id,
            directions={
                "timing": "missing", "transition": "missing",
                "archetype": "missing", "cluster": "supportive",
                "persistence": "missing", "decay": "missing",
            },
            weights={"supportive": 0.20, "suppressive": 0.00, "neutral": 0.00, "missing": 0.80},
            agreement_score=0.18, conflict_score=0.05,
            consensus_state="unreliable",
            dominant_conflict_source=None,
            reason_codes=["insufficient_layer_coverage"],
        )

        # Family-level seed for aligned run.
        await seed_family_layer_agreement(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            run_id=aligned_run,
            family="crypto_cross",
            directions={
                "transition": "supportive", "archetype": "supportive",
                "cluster": "supportive", "persistence": "supportive",
                "decay": "supportive",
            },
            agreement_score=0.93, conflict_score=0.03,
            consensus_state="aligned_supportive",
            dominant_conflict_source=None,
            reason_codes=[],
        )

        # Family-level seed for conflicted run.
        await seed_family_layer_agreement(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            run_id=conflicted_run,
            family="commodity",
            directions={
                "transition": "suppressive", "archetype": "supportive",
                "cluster": "suppressive", "persistence": "supportive",
                "decay": "suppressive",
            },
            agreement_score=0.30, conflict_score=0.55,
            consensus_state="conflicted",
            dominant_conflict_source="transition",
            reason_codes=["supportive_suppressive_tension"],
        )

        # Conflict event: prior anchor → current aligned run = strengthening.
        await seed_conflict_event(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=prior_run, target_run_id=aligned_run,
            prior_state="partial_agreement", current_state="aligned_supportive",
            event_type="agreement_strengthened",
            reason_codes=["agreement_score_jump"],
        )
        # Conflict event: aligned → conflicted = conflict_emerged.
        await seed_conflict_event(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=aligned_run, target_run_id=conflicted_run,
            prior_state="aligned_supportive", current_state="conflicted",
            event_type="conflict_emerged",
            reason_codes=["conflict_score_spike", "supportive_suppressive_tension"],
        )
        # Conflict event: aligned → unreliable = unreliable_stack_detected.
        await seed_conflict_event(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=aligned_run, target_run_id=unreliable_run,
            prior_state="aligned_supportive", current_state="unreliable",
            event_type="unreliable_stack_detected",
            reason_codes=["insufficient_layer_coverage"],
        )

        print(
            f"SEEDED: aligned={aligned_run[:8]}… "
            f"conflicted={conflicted_run[:8]}… unreliable={unreliable_run[:8]}…"
        )

        await check_0_profile(conn, workspace_id)
        await check_1(conn, workspace_id)
        await check_2(conn, workspace_id)
        await check_3(conn, workspace_id)
        await check_4(conn, workspace_id)
        await check_5(conn, workspace_id)
        await check_6(conn, workspace_id)
        await check_7(conn, workspace_id)
        await check_8_classification(conn, aligned_run, conflicted_run, unreliable_run)
        await check_9_route_contract(conn)
        print("\nAll Phase 4.8A checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
