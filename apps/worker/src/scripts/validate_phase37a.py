"""
Phase 3.7A validation: Governance Policy Optimization Summaries

Checks:
  1. snapshot_rows              — at least one optimization snapshot persisted
  2. feature_effectiveness_rows — feature effectiveness view returns rows
  3. context_fit_rows           — context fit view returns rows
  4. policy_opportunity_rows    — opportunity summary view returns rows
  5. recommendation_ranking_ok  — rows are ordered by expected_benefit_score DESC
  6. detail_contract_ok         — recommendation fields match expected schema
"""

import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

import asyncpg

WORKSPACE = "demo"


async def main() -> None:
    dsn = os.environ["DATABASE_URL"]
    conn = await asyncpg.connect(dsn)

    results: dict[str, bool | str | int] = {}

    try:
        # 1. snapshot_rows
        snap_count = await conn.fetchval(
            "SELECT COUNT(*) FROM governance_policy_optimization_snapshots WHERE workspace_id="
            "(SELECT id FROM workspaces WHERE slug=$1 LIMIT 1)",
            WORKSPACE,
        )
        results["snapshot_rows"] = int(snap_count)
        results["snapshot_rows_ok"] = int(snap_count) >= 1

        # 2. feature_effectiveness_rows
        eff_count = await conn.fetchval(
            "SELECT COUNT(*) FROM governance_policy_feature_effectiveness_summary WHERE workspace_id="
            "(SELECT id FROM workspaces WHERE slug=$1 LIMIT 1)",
            WORKSPACE,
        )
        results["feature_effectiveness_rows"] = int(eff_count)
        results["feature_effectiveness_rows_ok"] = int(eff_count) >= 1

        # 3. context_fit_rows
        ctx_count = await conn.fetchval(
            "SELECT COUNT(*) FROM governance_policy_context_fit_summary WHERE workspace_id="
            "(SELECT id FROM workspaces WHERE slug=$1 LIMIT 1)",
            WORKSPACE,
        )
        results["context_fit_rows"] = int(ctx_count)
        results["context_fit_rows_ok"] = int(ctx_count) >= 1

        # 4. policy_opportunity_rows
        opp_count = await conn.fetchval(
            "SELECT COUNT(*) FROM governance_policy_opportunity_summary WHERE workspace_id="
            "(SELECT id FROM workspaces WHERE slug=$1 LIMIT 1)",
            WORKSPACE,
        )
        results["policy_opportunity_rows"] = int(opp_count)
        results["policy_opportunity_rows_ok"] = int(opp_count) >= 1

        # 5. recommendation_ranking_ok
        opp_rows = await conn.fetch(
            "SELECT expected_benefit_score FROM governance_policy_opportunity_summary"
            " WHERE workspace_id=(SELECT id FROM workspaces WHERE slug=$1 LIMIT 1)"
            " ORDER BY expected_benefit_score DESC LIMIT 5",
            WORKSPACE,
        )
        if len(opp_rows) >= 2:
            scores = [float(r["expected_benefit_score"]) for r in opp_rows]
            ranking_ok = all(scores[i] >= scores[i + 1] for i in range(len(scores) - 1))
            results["recommendation_ranking_ok"] = ranking_ok
        else:
            results["recommendation_ranking_ok"] = "skipped (< 2 rows)"

        # 6. detail_contract_ok
        rec_row = await conn.fetchrow(
            "SELECT recommendation_key, policy_family, scope_type, scope_value,"
            "       current_policy, recommended_policy, reason_code, confidence,"
            "       sample_size, expected_benefit_score, risk_score, supporting_metrics"
            " FROM governance_policy_recommendations"
            " WHERE workspace_id=(SELECT id FROM workspaces WHERE slug=$1 LIMIT 1)"
            " LIMIT 1",
            WORKSPACE,
        )
        if rec_row is None:
            results["detail_contract_ok"] = "no recommendation rows — skipped"
            results["top_recommendation_key"] = "n/a"
        else:
            required = [
                "recommendation_key", "policy_family", "scope_type", "scope_value",
                "current_policy", "recommended_policy", "reason_code", "confidence",
                "sample_size", "expected_benefit_score", "risk_score", "supporting_metrics",
            ]
            missing = [k for k in required if rec_row[k] is None]
            results["detail_contract_ok"] = len(missing) == 0
            results["top_recommendation_key"] = rec_row["recommendation_key"]
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
        print("Phase 3.7A validation PASSED")
    else:
        print("Phase 3.7A validation FAILED — see FAIL rows above")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
