"""
Phase 3.6B validation: Rollback Impact Analysis

Checks:
  1. rollback_execution_present   — at least one rollback execution exists
  2. impact_snapshot_rows         — at least one impact snapshot was created
  3. impact_summary_rows          — the impact summary view returns rows
  4. effectiveness_summary_present — effectiveness aggregate view is populated
  5. impact_classification_valid  — all snapshots have a valid classification
  6. detail_contract_ok           — snapshot fields match expected schema
"""

import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

import asyncpg

VALID_CLASSIFICATIONS = {"improved", "neutral", "degraded", "insufficient_data"}

WORKSPACE = "demo"


async def main() -> None:
    dsn = os.environ["DATABASE_URL"]
    conn = await asyncpg.connect(dsn)

    results: dict[str, bool | str] = {}

    try:
        # 1. rollback_execution_present
        exec_count = await conn.fetchval(
            "SELECT COUNT(*) FROM governance_routing_policy_rollback_executions WHERE workspace_id=$1",
            WORKSPACE,
        )
        results["rollback_execution_present"] = int(exec_count) >= 1

        # 2. impact_snapshot_rows
        snap_count = await conn.fetchval(
            "SELECT COUNT(*) FROM governance_routing_policy_rollback_impact_snapshots WHERE workspace_id=$1",
            WORKSPACE,
        )
        results["impact_snapshot_rows"] = int(snap_count)
        results["impact_snapshot_rows_ok"] = int(snap_count) >= 1

        # 3. impact_summary_rows
        summary_count = await conn.fetchval(
            "SELECT COUNT(*) FROM governance_routing_policy_rollback_impact_summary WHERE workspace_id=$1",
            WORKSPACE,
        )
        results["impact_summary_rows"] = int(summary_count)
        results["impact_summary_rows_ok"] = int(summary_count) >= 1

        # 4. effectiveness_summary_present
        eff_row = await conn.fetchrow(
            "SELECT rollback_count FROM governance_routing_policy_rollback_effectiveness_summary WHERE workspace_id=$1",
            WORKSPACE,
        )
        results["effectiveness_summary_present"] = eff_row is not None and int(eff_row["rollback_count"]) >= 1

        # 5. impact_classification_valid
        bad_class = await conn.fetchval(
            """
            SELECT COUNT(*) FROM governance_routing_policy_rollback_impact_snapshots
            WHERE workspace_id=$1
              AND impact_classification NOT IN ('improved','neutral','degraded','insufficient_data')
            """,
            WORKSPACE,
        )
        results["impact_classification_valid"] = int(bad_class) == 0

        # 6. detail_contract_ok — check required columns exist and have non-null values in at least one row
        snap_row = await conn.fetchrow(
            """
            SELECT rollback_execution_id, rollback_candidate_id, recommendation_key,
                   scope_type, scope_value, target_type, impact_classification,
                   before_metrics, after_metrics, delta_metrics, evaluation_window_label
            FROM governance_routing_policy_rollback_impact_snapshots
            WHERE workspace_id=$1
            LIMIT 1
            """,
            WORKSPACE,
        )
        if snap_row is None:
            results["detail_contract_ok"] = "no snapshot rows — skipped"
        else:
            required = [
                "rollback_execution_id", "rollback_candidate_id", "recommendation_key",
                "scope_type", "scope_value", "target_type", "impact_classification",
                "before_metrics", "after_metrics", "delta_metrics", "evaluation_window_label",
            ]
            missing = [k for k in required if snap_row[k] is None]
            results["detail_contract_ok"] = len(missing) == 0
            if missing:
                results["detail_contract_missing_fields"] = missing

    finally:
        await conn.close()

    # ── Print results ──────────────────────────────────────────────────────────
    all_ok = True
    for key, val in results.items():
        if isinstance(val, bool):
            status = "PASS" if val else "FAIL"
            if not val:
                all_ok = False
        else:
            status = "INFO"
        print(f"  [{status}] {key}: {val}")

    print()
    if all_ok:
        print("Phase 3.6B validation PASSED")
    else:
        print("Phase 3.6B validation FAILED — see FAIL rows above")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
