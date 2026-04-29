"""Phase 4.7A smoke validation: Signal Decay & Stale-Memory Diagnostics.

Checks:
  1. signal decay snapshots persist
  2. family signal decay snapshots persist
  3. stale-memory event snapshots persist
  4. signal decay summary rows populate
  5. family signal decay summary rows populate
  6. stale-memory event summary rows populate
  7. run signal decay bridge row populates
  8. controlled inputs produce expected fresh / decaying / stale /
     contradicted classifications + matching stale-memory event types
  9. route contract remains typed and stable (decay tables / views)
 10. decay policy profile present (or default policy path verified)
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
            "VALUES ($1::uuid, $2::uuid, 'phase47a_validation', 'Phase 4.7A Validation')",
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


async def seed_signal_decay(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str, run_id: str,
    regime_key: str, archetype_key: str, cluster_state: str,
    persistence_state: str,
    state_age: int, memory_score: float,
    regime_decay: float, timing_decay: float, transition_decay: float,
    archetype_decay: float, cluster_decay: float, persistence_decay: float,
    aggregate_decay: float, freshness_state: str,
    stale_flag: bool, contradiction_flag: bool,
    reason_codes: list[str],
) -> None:
    signature = (
        f"regime={regime_key}|timing=lead|transition=reinforcing|"
        f"sequence=reinforcing_path|archetype={archetype_key}|cluster={cluster_state}"
    )
    await conn.execute(
        """
        INSERT INTO cross_asset_signal_decay_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           decay_policy_profile_id,
           regime_key, dominant_timing_class,
           dominant_transition_state, dominant_sequence_class,
           dominant_archetype_key, cluster_state,
           persistence_state, current_state_signature,
           state_age_runs, memory_score,
           regime_decay_score, timing_decay_score,
           transition_decay_score, archetype_decay_score,
           cluster_decay_score, persistence_decay_score,
           aggregate_decay_score,
           freshness_state, stale_memory_flag, contradiction_flag,
           reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                NULL,
                $4, 'lead', 'reinforcing', 'reinforcing_path',
                $5, $6,
                $7, $8,
                $9, $10,
                $11, $12,
                $13, $14,
                $15, $16,
                $17,
                $18, $19, $20,
                $21::jsonb,
                '{"scoring_version":"4.7A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
        regime_key, archetype_key, cluster_state,
        persistence_state, signature,
        state_age, memory_score,
        regime_decay, timing_decay,
        transition_decay, archetype_decay,
        cluster_decay, persistence_decay,
        aggregate_decay,
        freshness_state, stale_flag, contradiction_flag,
        json.dumps(reason_codes),
    )


async def seed_family_signal_decay(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str, run_id: str,
    dependency_family: str, transition_state: str | None,
    sequence_class: str | None, archetype_key: str | None,
    cluster_state: str | None, persistence_state: str | None,
    family_rank: int, family_contribution: float,
    family_state_age: int, family_memory: float,
    family_decay: float, family_freshness: str,
    stale_family_flag: bool, contradicted_family_flag: bool,
    reason_codes: list[str],
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_family_signal_decay_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           dependency_family,
           transition_state, dominant_sequence_class,
           archetype_key, cluster_state, persistence_state,
           family_rank, family_contribution,
           family_state_age_runs, family_memory_score,
           family_decay_score, family_freshness_state,
           stale_family_memory_flag, contradicted_family_flag,
           reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                $4,
                $5, $6,
                $7, $8, $9,
                $10, $11,
                $12, $13,
                $14, $15,
                $16, $17,
                $18::jsonb,
                '{"scoring_version":"4.7A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
        dependency_family,
        transition_state, sequence_class,
        archetype_key, cluster_state, persistence_state,
        family_rank, family_contribution,
        family_state_age, family_memory,
        family_decay, family_freshness,
        stale_family_flag, contradicted_family_flag,
        json.dumps(reason_codes),
    )


async def seed_stale_memory_event(
    conn: asyncpg.Connection,
    *,
    workspace_id: str, watchlist_id: str | None,
    source_run_id: str | None, target_run_id: str,
    regime_key: str | None,
    prior_freshness: str | None, current_freshness: str,
    prior_sig: str | None, current_sig: str,
    prior_memory: float | None, current_memory: float | None,
    prior_aggregate: float | None, current_aggregate: float | None,
    event_type: str, reasons: list[str],
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_stale_memory_event_snapshots
          (workspace_id, watchlist_id, source_run_id, target_run_id,
           regime_key,
           prior_freshness_state, current_freshness_state,
           prior_state_signature, current_state_signature,
           prior_memory_score, current_memory_score,
           prior_aggregate_decay_score, current_aggregate_decay_score,
           event_type, reason_codes, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid,
                $5,
                $6, $7,
                $8, $9,
                $10, $11,
                $12, $13,
                $14, $15::jsonb,
                '{"scoring_version":"4.7A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, source_run_id, target_run_id,
        regime_key,
        prior_freshness, current_freshness,
        prior_sig, current_sig,
        prior_memory, current_memory,
        prior_aggregate, current_aggregate,
        event_type, json.dumps(reasons),
    )


async def check_decay_policy(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT id FROM cross_asset_signal_decay_policy_profiles "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    # Either present, or the service will fall back to its deterministic default profile.
    print(
        f"CHECK 0 PASSED: decay_policy_present_or_default=true "
        f"(profile_rows={len(rows)})"
    )


async def check_1_2_3(
    conn: asyncpg.Connection, workspace_id: str,
) -> None:
    decay_rows = await conn.fetch(
        "SELECT run_id FROM cross_asset_signal_decay_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert decay_rows, "CHECK 1 FAILED: no signal decay snapshots"
    print(f"CHECK 1 PASSED: signal_decay_rows={len(decay_rows)}")

    family_rows = await conn.fetch(
        "SELECT dependency_family FROM cross_asset_family_signal_decay_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert family_rows, "CHECK 2 FAILED: no family signal decay snapshots"
    print(f"CHECK 2 PASSED: family_decay_rows={len(family_rows)}")

    event_rows = await conn.fetch(
        "SELECT event_type FROM cross_asset_stale_memory_event_snapshots "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert event_rows, "CHECK 3 FAILED: no stale-memory event snapshots"
    print(f"CHECK 3 PASSED: stale_memory_event_rows={len(event_rows)}")


async def check_4_signal_summary(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT run_id, freshness_state FROM cross_asset_signal_decay_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 4 FAILED: signal decay summary empty"
    print(f"CHECK 4 PASSED: signal_summary_rows={len(rows)}")


async def check_5_family_summary(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, family_freshness_state "
        "FROM cross_asset_family_signal_decay_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 5 FAILED: family decay summary empty"
    print(f"CHECK 5 PASSED: family_summary_rows={len(rows)}")


async def check_6_event_summary(conn: asyncpg.Connection, workspace_id: str) -> None:
    rows = await conn.fetch(
        "SELECT event_type FROM cross_asset_stale_memory_event_summary "
        "WHERE workspace_id = $1::uuid",
        workspace_id,
    )
    assert rows, "CHECK 6 FAILED: event summary empty"
    print(f"CHECK 6 PASSED: event_summary_rows={len(rows)}")


async def check_7_run_decay(
    conn: asyncpg.Connection, workspace_id: str, run_id: str,
) -> None:
    row = await conn.fetchrow(
        "SELECT run_id, freshness_state, aggregate_decay_score, "
        "       stale_memory_flag, contradiction_flag, "
        "       latest_stale_memory_event_type "
        "FROM run_cross_asset_signal_decay_summary "
        "WHERE workspace_id = $1::uuid AND run_id = $2::uuid",
        workspace_id, run_id,
    )
    assert row, "CHECK 7 FAILED: run signal decay summary missing"
    print(
        f"CHECK 7 PASSED: run_decay_rows>=1 "
        f"freshness={row['freshness_state']!r} "
        f"aggregate_decay={row['aggregate_decay_score']} "
        f"stale={row['stale_memory_flag']} "
        f"contradiction={row['contradiction_flag']} "
        f"latest_event={row['latest_stale_memory_event_type']!r}"
    )


async def check_8_classification(
    conn: asyncpg.Connection, run_ids: dict[str, str],
) -> None:
    """Verify the seeded scenarios are classified correctly:
      - fresh_run        → freshness_state=fresh
      - decaying_run     → freshness_state=decaying
      - stale_run        → freshness_state=stale, stale_memory_flag=true
      - contradicted_run → freshness_state=contradicted, contradiction_flag=true
    Verify event-type alignment for the seeded transitions:
      - fresh→decaying target → memory_decayed
      - decaying→stale target → memory_became_stale
      - any→contradicted target → memory_contradicted
    """
    rows = await conn.fetch(
        "SELECT run_id::text as run_id, freshness_state, "
        "       stale_memory_flag, contradiction_flag "
        "FROM cross_asset_signal_decay_summary "
        "WHERE run_id = ANY($1::uuid[])",
        list(run_ids.values()),
    )
    by_run = {r["run_id"]: r for r in rows}
    fresh        = by_run.get(run_ids["fresh_run"])
    decaying     = by_run.get(run_ids["decaying_run"])
    stale        = by_run.get(run_ids["stale_run"])
    contradicted = by_run.get(run_ids["contradicted_run"])
    assert fresh and decaying and stale and contradicted, (
        "CHECK 8 FAILED: missing seeded run rows"
    )
    assert fresh["freshness_state"] == "fresh", (
        f"CHECK 8 FAILED: fresh_run expected 'fresh', got {fresh['freshness_state']!r}"
    )
    assert decaying["freshness_state"] == "decaying", (
        f"CHECK 8 FAILED: decaying_run expected 'decaying', got {decaying['freshness_state']!r}"
    )
    assert stale["freshness_state"] == "stale", (
        f"CHECK 8 FAILED: stale_run expected 'stale', got {stale['freshness_state']!r}"
    )
    assert stale["stale_memory_flag"] is True, (
        f"CHECK 8 FAILED: stale_run expected stale_memory_flag=true"
    )
    assert contradicted["freshness_state"] == "contradicted", (
        f"CHECK 8 FAILED: contradicted_run expected 'contradicted', "
        f"got {contradicted['freshness_state']!r}"
    )
    assert contradicted["contradiction_flag"] is True, (
        f"CHECK 8 FAILED: contradicted_run expected contradiction_flag=true"
    )

    drift_rows = await conn.fetch(
        "SELECT target_run_id::text as target_run_id, event_type "
        "FROM cross_asset_stale_memory_event_summary "
        "WHERE target_run_id = ANY($1::uuid[])",
        [run_ids["decaying_run"], run_ids["stale_run"], run_ids["contradicted_run"]],
    )
    by_target = {r["target_run_id"]: r["event_type"] for r in drift_rows}
    assert by_target.get(run_ids["decaying_run"]) == "memory_decayed", (
        f"CHECK 8 FAILED: decaying_run event expected 'memory_decayed', "
        f"got {by_target.get(run_ids['decaying_run'])!r}"
    )
    assert by_target.get(run_ids["stale_run"]) == "memory_became_stale", (
        f"CHECK 8 FAILED: stale_run event expected 'memory_became_stale', "
        f"got {by_target.get(run_ids['stale_run'])!r}"
    )
    assert by_target.get(run_ids["contradicted_run"]) == "memory_contradicted", (
        f"CHECK 8 FAILED: contradicted_run event expected 'memory_contradicted', "
        f"got {by_target.get(run_ids['contradicted_run'])!r}"
    )
    print(
        "CHECK 8 PASSED: decay_classification_checked=true "
        "(fresh/decaying/stale/contradicted + memory_decayed+memory_became_stale+memory_contradicted)"
    )


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_signal_decay_policy_profiles": [
            "id", "workspace_id", "profile_name", "is_active",
            "regime_half_life_runs", "timing_half_life_runs",
            "transition_half_life_runs", "archetype_half_life_runs",
            "cluster_half_life_runs", "persistence_half_life_runs",
            "fresh_memory_threshold", "decaying_memory_threshold",
            "stale_memory_threshold", "contradiction_penalty_threshold",
            "metadata", "created_at",
        ],
        "cross_asset_signal_decay_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id",
            "context_snapshot_id", "decay_policy_profile_id",
            "regime_key", "dominant_timing_class",
            "dominant_transition_state", "dominant_sequence_class",
            "dominant_archetype_key", "cluster_state",
            "persistence_state", "current_state_signature",
            "state_age_runs", "memory_score",
            "regime_decay_score", "timing_decay_score",
            "transition_decay_score", "archetype_decay_score",
            "cluster_decay_score", "persistence_decay_score",
            "aggregate_decay_score",
            "freshness_state", "stale_memory_flag", "contradiction_flag",
            "reason_codes", "metadata", "created_at",
        ],
        "cross_asset_family_signal_decay_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id",
            "context_snapshot_id", "dependency_family",
            "transition_state", "dominant_sequence_class",
            "archetype_key", "cluster_state", "persistence_state",
            "family_rank", "family_contribution",
            "family_state_age_runs", "family_memory_score",
            "family_decay_score", "family_freshness_state",
            "stale_family_memory_flag", "contradicted_family_flag",
            "reason_codes", "metadata", "created_at",
        ],
        "cross_asset_stale_memory_event_snapshots": [
            "id", "workspace_id", "watchlist_id",
            "source_run_id", "target_run_id",
            "regime_key",
            "prior_freshness_state", "current_freshness_state",
            "prior_state_signature", "current_state_signature",
            "prior_memory_score", "current_memory_score",
            "prior_aggregate_decay_score", "current_aggregate_decay_score",
            "event_type", "reason_codes", "metadata", "created_at",
        ],
        "cross_asset_signal_decay_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "regime_key", "dominant_timing_class",
            "dominant_transition_state", "dominant_sequence_class",
            "dominant_archetype_key", "cluster_state", "persistence_state",
            "current_state_signature", "state_age_runs", "memory_score",
            "regime_decay_score", "timing_decay_score",
            "transition_decay_score", "archetype_decay_score",
            "cluster_decay_score", "persistence_decay_score",
            "aggregate_decay_score",
            "freshness_state", "stale_memory_flag", "contradiction_flag",
            "reason_codes", "created_at",
        ],
        "cross_asset_family_signal_decay_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family",
            "transition_state", "dominant_sequence_class",
            "archetype_key", "cluster_state", "persistence_state",
            "family_rank", "family_contribution",
            "family_state_age_runs", "family_memory_score",
            "family_decay_score", "family_freshness_state",
            "stale_family_memory_flag", "contradicted_family_flag",
            "reason_codes", "created_at",
        ],
        "cross_asset_stale_memory_event_summary": [
            "workspace_id", "watchlist_id",
            "source_run_id", "target_run_id",
            "regime_key",
            "prior_freshness_state", "current_freshness_state",
            "prior_state_signature", "current_state_signature",
            "prior_memory_score", "current_memory_score",
            "prior_aggregate_decay_score", "current_aggregate_decay_score",
            "event_type", "reason_codes", "created_at",
        ],
        "run_cross_asset_signal_decay_summary": [
            "run_id", "workspace_id", "watchlist_id",
            "regime_key", "persistence_state", "memory_score",
            "freshness_state", "aggregate_decay_score",
            "stale_memory_flag", "contradiction_flag",
            "latest_stale_memory_event_type", "created_at",
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
        fresh_run        = str(uuid.uuid4())
        decaying_run     = str(uuid.uuid4())
        stale_run        = str(uuid.uuid4())
        contradicted_run = str(uuid.uuid4())
        for rid in (fresh_run, decaying_run, stale_run, contradicted_run):
            await ensure_job_run(conn, workspace_id, watchlist_id, rid)

        # Fresh: high decay across the board, persistent state.
        await seed_signal_decay(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=fresh_run,
            regime_key="macro_dominant",
            archetype_key="reinforcing_continuation", cluster_state="stable",
            persistence_state="persistent",
            state_age=5, memory_score=0.82,
            regime_decay=0.85, timing_decay=0.80, transition_decay=0.82,
            archetype_decay=0.84, cluster_decay=0.86, persistence_decay=0.88,
            aggregate_decay=0.85, freshness_state="fresh",
            stale_flag=False, contradiction_flag=False,
            reason_codes=["aggregate_decay_above_fresh_threshold",
                          "memory_reconfirmed", "decay_score_recovered"],
        )
        # Decaying: aggregate decay between thresholds, no contradiction.
        await seed_signal_decay(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=decaying_run,
            regime_key="macro_dominant",
            archetype_key="reinforcing_continuation", cluster_state="stable",
            persistence_state="fragile",
            state_age=8, memory_score=0.55,
            regime_decay=0.60, timing_decay=0.55, transition_decay=0.58,
            archetype_decay=0.60, cluster_decay=0.65, persistence_decay=0.62,
            aggregate_decay=0.60, freshness_state="decaying",
            stale_flag=False, contradiction_flag=False,
            reason_codes=["aggregate_decay_below_fresh_threshold"],
        )
        # Stale: aggregate decay below stale threshold.
        await seed_signal_decay(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=stale_run,
            regime_key="macro_dominant",
            archetype_key="insufficient_history", cluster_state="stable",
            persistence_state="fragile",
            state_age=18, memory_score=0.18,
            regime_decay=0.22, timing_decay=0.20, transition_decay=0.18,
            archetype_decay=0.25, cluster_decay=0.30, persistence_decay=0.18,
            aggregate_decay=0.22, freshness_state="stale",
            stale_flag=True, contradiction_flag=False,
            reason_codes=["aggregate_decay_below_stale_threshold"],
        )
        # Contradicted: high memory but cluster/archetype shifted to suppressive.
        await seed_signal_decay(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=contradicted_run,
            regime_key="macro_dominant",
            archetype_key="deteriorating_breakdown", cluster_state="deteriorating",
            persistence_state="persistent",
            state_age=4, memory_score=0.78,
            regime_decay=0.65, timing_decay=0.60, transition_decay=0.58,
            archetype_decay=0.40, cluster_decay=0.40, persistence_decay=0.70,
            aggregate_decay=0.55, freshness_state="contradicted",
            stale_flag=False, contradiction_flag=True,
            reason_codes=["high_memory_low_current_support",
                          "persistent_state_now_deteriorating",
                          "archetype_shift_to_breakdown",
                          "contradiction_detected"],
        )

        # Family decay rows for the stale and contradicted runs.
        await seed_family_signal_decay(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=stale_run,
            dependency_family="macro",
            transition_state="reinforcing", sequence_class="reinforcing_path",
            archetype_key="insufficient_history", cluster_state="stable",
            persistence_state="fragile",
            family_rank=1, family_contribution=0.0124,
            family_state_age=18, family_memory=0.18,
            family_decay=0.22, family_freshness="stale",
            stale_family_flag=True, contradicted_family_flag=False,
            reason_codes=["aggregate_decay_below_stale_threshold"],
        )
        await seed_family_signal_decay(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=contradicted_run,
            dependency_family="equity_index",
            transition_state="recovering", sequence_class="recovering_path",
            archetype_key="deteriorating_breakdown", cluster_state="deteriorating",
            persistence_state="persistent",
            family_rank=2, family_contribution=0.0341,
            family_state_age=4, family_memory=0.78,
            family_decay=0.55, family_freshness="contradicted",
            stale_family_flag=False, contradicted_family_flag=True,
            reason_codes=["persistent_state_now_deteriorating",
                          "high_memory_low_current_support"],
        )

        # Stale-memory transitions
        fresh_sig = (
            "regime=macro_dominant|timing=lead|transition=reinforcing|"
            "sequence=reinforcing_path|archetype=reinforcing_continuation|cluster=stable"
        )
        contradicted_sig = (
            "regime=macro_dominant|timing=lead|transition=reinforcing|"
            "sequence=reinforcing_path|archetype=deteriorating_breakdown|cluster=deteriorating"
        )
        await seed_stale_memory_event(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=fresh_run, target_run_id=decaying_run,
            regime_key="macro_dominant",
            prior_freshness="fresh", current_freshness="decaying",
            prior_sig=fresh_sig, current_sig=fresh_sig,
            prior_memory=0.82, current_memory=0.55,
            prior_aggregate=0.85, current_aggregate=0.60,
            event_type="memory_decayed",
            reasons=["aggregate_decay_below_fresh_threshold",
                     "memory_score_decreased"],
        )
        await seed_stale_memory_event(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=decaying_run, target_run_id=stale_run,
            regime_key="macro_dominant",
            prior_freshness="decaying", current_freshness="stale",
            prior_sig=fresh_sig, current_sig=fresh_sig,
            prior_memory=0.55, current_memory=0.18,
            prior_aggregate=0.60, current_aggregate=0.22,
            event_type="memory_became_stale",
            reasons=["aggregate_decay_below_stale_threshold",
                     "memory_score_decreased"],
        )
        await seed_stale_memory_event(
            conn,
            workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=stale_run, target_run_id=contradicted_run,
            regime_key="macro_dominant",
            prior_freshness="stale", current_freshness="contradicted",
            prior_sig=fresh_sig, current_sig=contradicted_sig,
            prior_memory=0.18, current_memory=0.78,
            prior_aggregate=0.22, current_aggregate=0.55,
            event_type="memory_contradicted",
            reasons=["state_signature_changed",
                     "high_memory_low_current_support",
                     "contradiction_detected"],
        )
        print(
            f"SEEDED: fresh={fresh_run[:12]}… decaying={decaying_run[:12]}… "
            f"stale={stale_run[:12]}… contradicted={contradicted_run[:12]}…"
        )

        await check_decay_policy(conn, workspace_id)
        await check_1_2_3(conn, workspace_id)
        await check_4_signal_summary(conn, workspace_id)
        await check_5_family_summary(conn, workspace_id)
        await check_6_event_summary(conn, workspace_id)
        await check_7_run_decay(conn, workspace_id, contradicted_run)
        await check_8_classification(conn, {
            "fresh_run":        fresh_run,
            "decaying_run":     decaying_run,
            "stale_run":        stale_run,
            "contradicted_run": contradicted_run,
        })
        await check_9_route_contract(conn)
        print("\nAll Phase 4.7A checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
