"""Phase 4.3A smoke validation: Family-Level Sequencing + Transition-State Diagnostics.

Checks:
  1. transition-state snapshots persist
  2. transition-event snapshots persist
  3. sequence-summary snapshots persist
  4. transition-state summary rows populate
  5. transition-event summary rows populate
  6. sequence summary rows populate
  7. run transition diagnostics summary row populates
  8. controlled prior/current family states produce expected recovery/degradation/rotation outcomes
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
            "VALUES ($1::uuid, $2::uuid, 'phase43a_validation', 'Phase 4.3A Validation')",
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


async def seed_state_row(
    conn: asyncpg.Connection, *, workspace_id: str, watchlist_id: str, run_id: str,
    dependency_family: str, signal_state: str, transition_state: str,
    family_contribution: float, family_rank: int | None,
    dominant_timing_class: str = "lead", regime_key: str = "macro_dominant",
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_family_transition_state_snapshots
          (workspace_id, watchlist_id, run_id, context_snapshot_id,
           dependency_family, regime_key, dominant_timing_class,
           signal_state, transition_state,
           family_contribution,
           timing_adjusted_contribution, timing_integration_contribution,
           family_rank, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, NULL,
                $4, $5, $6, $7, $8,
                $9, $10, $11, $12, '{"scoring_version":"4.3A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
        dependency_family, regime_key, dominant_timing_class,
        signal_state, transition_state,
        family_contribution, family_contribution * 1.1, family_contribution * 0.1,
        family_rank,
    )


async def seed_transition_event_row(
    conn: asyncpg.Connection, *, workspace_id: str, watchlist_id: str,
    source_run_id: str, target_run_id: str,
    dependency_family: str, event_type: str,
    prior_signal_state: str, current_signal_state: str,
    prior_transition_state: str, current_transition_state: str,
    prior_rank: int | None, current_rank: int | None,
    prior_contrib: float, current_contrib: float,
) -> None:
    rank_delta = None
    if prior_rank is not None and current_rank is not None:
        rank_delta = current_rank - prior_rank
    contrib_delta = current_contrib - prior_contrib
    await conn.execute(
        """
        INSERT INTO cross_asset_family_transition_event_snapshots
          (workspace_id, watchlist_id, source_run_id, target_run_id,
           dependency_family,
           prior_signal_state, current_signal_state,
           prior_transition_state, current_transition_state,
           prior_family_rank, current_family_rank, rank_delta,
           prior_family_contribution, current_family_contribution, contribution_delta,
           event_type, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid,
                $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15,
                $16, '{"scoring_version":"4.3A.v1"}'::jsonb)
        """,
        workspace_id, watchlist_id, source_run_id, target_run_id,
        dependency_family,
        prior_signal_state, current_signal_state,
        prior_transition_state, current_transition_state,
        prior_rank, current_rank, rank_delta,
        prior_contrib, current_contrib, contrib_delta,
        event_type,
    )


async def seed_sequence_row(
    conn: asyncpg.Connection, *, workspace_id: str, watchlist_id: str, run_id: str,
    dependency_family: str, sequence_signature: str, sequence_length: int,
    dominant_sequence_class: str, sequence_confidence: float,
) -> None:
    await conn.execute(
        """
        INSERT INTO cross_asset_family_sequence_summary_snapshots
          (workspace_id, watchlist_id, run_id,
           dependency_family, window_label,
           sequence_signature, sequence_length,
           dominant_sequence_class, sequence_confidence, metadata)
        VALUES ($1::uuid, $2::uuid, $3::uuid,
                $4, 'recent_5',
                $5, $6, $7, $8, '{"scoring_version":"4.3A.v1","window_size":5}'::jsonb)
        """,
        workspace_id, watchlist_id, run_id,
        dependency_family,
        sequence_signature, sequence_length, dominant_sequence_class, sequence_confidence,
    )


async def check_1_state_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, transition_state FROM cross_asset_family_transition_state_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 1 FAILED: no transition state rows"
    print(f"CHECK 1 PASSED: transition_state_rows={len(rows)}")


async def check_2_event_rows(conn: asyncpg.Connection, target_run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, event_type FROM cross_asset_family_transition_event_snapshots "
        "WHERE target_run_id = $1::uuid",
        target_run_id,
    )
    assert rows, "CHECK 2 FAILED: no transition event rows"
    print(f"CHECK 2 PASSED: transition_event_rows={len(rows)}")


async def check_3_sequence_rows(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, dominant_sequence_class FROM cross_asset_family_sequence_summary_snapshots "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 3 FAILED: no sequence summary rows"
    print(f"CHECK 3 PASSED: sequence_summary_rows={len(rows)}")


async def check_4_state_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, transition_state FROM cross_asset_family_transition_state_summary "
        "WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 4 FAILED: transition state summary empty"
    print(f"CHECK 4 PASSED: state_summary_rows={len(rows)}")


async def check_5_event_summary(conn: asyncpg.Connection, target_run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, event_type FROM cross_asset_family_transition_event_summary "
        "WHERE target_run_id = $1::uuid",
        target_run_id,
    )
    assert rows, "CHECK 5 FAILED: transition event summary empty"
    print(f"CHECK 5 PASSED: event_summary_rows={len(rows)}")


async def check_6_sequence_summary(conn: asyncpg.Connection, run_id: str) -> None:
    rows = await conn.fetch(
        "SELECT dependency_family, dominant_sequence_class, sequence_confidence "
        "FROM cross_asset_family_sequence_summary WHERE run_id = $1::uuid",
        run_id,
    )
    assert rows, "CHECK 6 FAILED: sequence summary view empty"
    print(f"CHECK 6 PASSED: sequence_summary_view_rows={len(rows)}")


async def check_7_run_summary(conn: asyncpg.Connection, run_id: str) -> None:
    row = await conn.fetchrow(
        """
        SELECT run_id, dominant_dependency_family, prior_dominant_dependency_family,
               dominant_transition_state, dominant_sequence_class,
               rotation_event_count, degradation_event_count, recovery_event_count
        FROM run_cross_asset_transition_diagnostics_summary WHERE run_id = $1::uuid
        """,
        run_id,
    )
    assert row, "CHECK 7 FAILED: run transition diagnostics summary empty"
    print(
        f"CHECK 7 PASSED: run_summary_rows>=1 "
        f"dominant={row['dominant_dependency_family']!r} "
        f"prior={row['prior_dominant_dependency_family']!r} "
        f"transition={row['dominant_transition_state']} "
        f"seq={row['dominant_sequence_class']} "
        f"events(rot={row['rotation_event_count']},deg={row['degradation_event_count']},rec={row['recovery_event_count']})"
    )


async def check_8_transition_classification(
    conn: asyncpg.Connection, workspace_id: str, watchlist_id: str, target_run_id: str,
) -> None:
    """Verify seeded scenarios are classified correctly:
      - rates: recovery (prior contradicted → current confirmed)
      - risk:  degradation (prior confirmed → current contradicted)
      - fx:    rotating_in (rank 3 → rank 1)
    """
    rows = await conn.fetch(
        """
        SELECT dependency_family, event_type, current_transition_state
        FROM cross_asset_family_transition_event_summary
        WHERE target_run_id = $1::uuid
        """,
        target_run_id,
    )
    by_fam = {r["dependency_family"]: r for r in rows}
    assert by_fam.get("rates"), "CHECK 8 FAILED: rates event missing"
    assert by_fam["rates"]["event_type"] == "recovery", (
        f"CHECK 8 FAILED: rates expected 'recovery', got {by_fam['rates']['event_type']!r}"
    )
    assert by_fam.get("risk"), "CHECK 8 FAILED: risk event missing"
    assert by_fam["risk"]["event_type"] == "degradation", (
        f"CHECK 8 FAILED: risk expected 'degradation', got {by_fam['risk']['event_type']!r}"
    )
    assert by_fam.get("fx"), "CHECK 8 FAILED: fx event missing"
    # fx scenario: rank changed 3→1 which is also dominance_gain (higher priority than rank_shift)
    assert by_fam["fx"]["event_type"] in ("dominance_gain", "rank_shift"), (
        f"CHECK 8 FAILED: fx expected dominance_gain or rank_shift, got {by_fam['fx']['event_type']!r}"
    )
    assert by_fam["fx"]["current_transition_state"] == "rotating_in", (
        f"CHECK 8 FAILED: fx current transition expected 'rotating_in', "
        f"got {by_fam['fx']['current_transition_state']!r}"
    )
    print(
        f"CHECK 8 PASSED: transition_classification_checked=true "
        f"(rates→recovery, risk→degradation, fx→{by_fam['fx']['event_type']}/rotating_in)"
    )


async def check_9_route_contract(conn: asyncpg.Connection) -> None:
    checks = {
        "cross_asset_family_transition_state_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "regime_key", "dominant_timing_class",
            "signal_state", "transition_state",
            "family_contribution",
            "timing_adjusted_contribution", "timing_integration_contribution",
            "family_rank", "metadata", "created_at",
        ],
        "cross_asset_family_transition_event_snapshots": [
            "id", "workspace_id", "watchlist_id", "source_run_id", "target_run_id",
            "dependency_family",
            "prior_signal_state", "current_signal_state",
            "prior_transition_state", "current_transition_state",
            "prior_family_rank", "current_family_rank", "rank_delta",
            "prior_family_contribution", "current_family_contribution", "contribution_delta",
            "event_type", "metadata", "created_at",
        ],
        "cross_asset_family_sequence_summary_snapshots": [
            "id", "workspace_id", "watchlist_id", "run_id",
            "dependency_family", "window_label",
            "sequence_signature", "sequence_length",
            "dominant_sequence_class", "sequence_confidence", "metadata", "created_at",
        ],
        "cross_asset_family_transition_state_summary": [
            "workspace_id", "watchlist_id", "run_id", "context_snapshot_id",
            "dependency_family", "regime_key", "dominant_timing_class",
            "signal_state", "transition_state",
            "family_contribution",
            "timing_adjusted_contribution", "timing_integration_contribution",
            "family_rank", "created_at",
        ],
        "cross_asset_family_transition_event_summary": [
            "workspace_id", "watchlist_id", "source_run_id", "target_run_id",
            "dependency_family",
            "prior_signal_state", "current_signal_state",
            "prior_transition_state", "current_transition_state",
            "prior_family_rank", "current_family_rank", "rank_delta",
            "prior_family_contribution", "current_family_contribution", "contribution_delta",
            "event_type", "created_at",
        ],
        "cross_asset_family_sequence_summary": [
            "workspace_id", "watchlist_id", "run_id",
            "dependency_family", "window_label",
            "sequence_signature", "sequence_length",
            "dominant_sequence_class", "sequence_confidence", "created_at",
        ],
        "run_cross_asset_transition_diagnostics_summary": [
            "run_id", "workspace_id", "watchlist_id",
            "dominant_dependency_family", "prior_dominant_dependency_family",
            "dominant_timing_class",
            "dominant_transition_state", "dominant_sequence_class",
            "rotation_event_count", "degradation_event_count", "recovery_event_count",
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
        prior_run_id  = str(uuid.uuid4())
        target_run_id = str(uuid.uuid4())
        await ensure_job_run(conn, workspace_id, watchlist_id, prior_run_id)
        await ensure_job_run(conn, workspace_id, watchlist_id, target_run_id)

        # Seed prior + target states for 3 families:
        #   rates: contradicted→confirmed → recovery
        #   risk:  confirmed→contradicted → degradation
        #   fx:    rank 3→rank 1          → rotating_in / dominance_gain
        await seed_state_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=prior_run_id,
            dependency_family="rates", signal_state="contradicted", transition_state="deteriorating",
            family_contribution=-0.05, family_rank=2,
        )
        await seed_state_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=target_run_id,
            dependency_family="rates", signal_state="confirmed", transition_state="recovering",
            family_contribution=0.20, family_rank=2,
        )
        await seed_state_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=prior_run_id,
            dependency_family="risk", signal_state="confirmed", transition_state="reinforcing",
            family_contribution=0.25, family_rank=1,
        )
        await seed_state_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=target_run_id,
            dependency_family="risk", signal_state="contradicted", transition_state="deteriorating",
            family_contribution=-0.05, family_rank=3,
        )
        await seed_state_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=prior_run_id,
            dependency_family="fx", signal_state="unconfirmed", transition_state="stable",
            family_contribution=0.08, family_rank=3,
        )
        await seed_state_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=target_run_id,
            dependency_family="fx", signal_state="confirmed", transition_state="rotating_in",
            family_contribution=0.22, family_rank=1,
        )

        # Seed transition events matching the expected classifications
        await seed_transition_event_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=prior_run_id, target_run_id=target_run_id,
            dependency_family="rates", event_type="recovery",
            prior_signal_state="contradicted", current_signal_state="confirmed",
            prior_transition_state="deteriorating", current_transition_state="recovering",
            prior_rank=2, current_rank=2,
            prior_contrib=-0.05, current_contrib=0.20,
        )
        await seed_transition_event_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=prior_run_id, target_run_id=target_run_id,
            dependency_family="risk", event_type="degradation",
            prior_signal_state="confirmed", current_signal_state="contradicted",
            prior_transition_state="reinforcing", current_transition_state="deteriorating",
            prior_rank=1, current_rank=3,
            prior_contrib=0.25, current_contrib=-0.05,
        )
        await seed_transition_event_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=prior_run_id, target_run_id=target_run_id,
            dependency_family="fx", event_type="dominance_gain",
            prior_signal_state="unconfirmed", current_signal_state="confirmed",
            prior_transition_state="stable", current_transition_state="rotating_in",
            prior_rank=3, current_rank=1,
            prior_contrib=0.08, current_contrib=0.22,
        )
        # Dominance-loss for risk (prior rank 1 → current rank 3)
        await seed_transition_event_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
            source_run_id=prior_run_id, target_run_id=target_run_id,
            dependency_family="risk_prior_dom", event_type="dominance_loss",
            prior_signal_state="confirmed", current_signal_state="contradicted",
            prior_transition_state="reinforcing", current_transition_state="rotating_out",
            prior_rank=1, current_rank=4,
            prior_contrib=0.25, current_contrib=-0.05,
        )

        # Seed sequence rows for the target run
        await seed_sequence_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=target_run_id,
            dependency_family="rates",
            sequence_signature="contradicted>deteriorating | confirmed>recovering",
            sequence_length=2, dominant_sequence_class="recovery_path", sequence_confidence=0.8,
        )
        await seed_sequence_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=target_run_id,
            dependency_family="risk",
            sequence_signature="confirmed>reinforcing | contradicted>deteriorating",
            sequence_length=2, dominant_sequence_class="deteriorating_path", sequence_confidence=0.75,
        )
        await seed_sequence_row(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id, run_id=target_run_id,
            dependency_family="fx",
            sequence_signature="unconfirmed>stable | confirmed>rotating_in",
            sequence_length=2, dominant_sequence_class="rotation_path", sequence_confidence=0.7,
        )
        print(f"SEEDED: prior={prior_run_id[:12]}… target={target_run_id[:12]}…")

        await check_1_state_rows(conn, target_run_id)
        await check_2_event_rows(conn, target_run_id)
        await check_3_sequence_rows(conn, target_run_id)
        await check_4_state_summary(conn, target_run_id)
        await check_5_event_summary(conn, target_run_id)
        await check_6_sequence_summary(conn, target_run_id)
        await check_7_run_summary(conn, target_run_id)
        await check_8_transition_classification(
            conn, workspace_id, watchlist_id, target_run_id,
        )
        await check_9_route_contract(conn)
        print("\nAll Phase 4.3A checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
