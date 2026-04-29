"""Phase 3.7C smoke validation: Governance Policy Autopromotion."""

from __future__ import annotations

import asyncio
import os
import sys
import uuid

import asyncpg

DATABASE_URL = os.environ.get("DATABASE_URL", "")


async def get_conn() -> asyncpg.Connection:
    if not DATABASE_URL:
        sys.exit("DATABASE_URL not set")
    return await asyncpg.connect(DATABASE_URL)


async def get_workspace_id(conn: asyncpg.Connection, slug: str = "demo") -> str:
    row = await conn.fetchrow("SELECT id FROM workspaces WHERE slug = $1", slug)
    assert row, f"workspace '{slug}' not found"
    return str(row["id"])


async def check_1_policy_persists(conn: asyncpg.Connection, workspace_id: str) -> None:
    """Autopromotion policy upsert persists correctly."""
    pid = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO governance_policy_autopromotion_policies
          (id, workspace_id, policy_family, scope_type, scope_value, promotion_target,
           min_confidence, min_approved_review_count, min_application_count,
           min_sample_size, max_recent_override_rate, max_recent_reassignment_rate,
           cooldown_hours, enabled, created_by)
        VALUES ($1,$2,'threshold','team','validate_team','threshold_profile',
                'high',1,1,5,0.25,0.25,72,true,'validate_37c')
        ON CONFLICT (workspace_id, policy_family, scope_type, scope_value, promotion_target)
        DO UPDATE SET enabled = true, created_by = EXCLUDED.created_by
        """,
        pid, workspace_id,
    )
    row = await conn.fetchrow(
        "SELECT id FROM governance_policy_autopromotion_policies "
        "WHERE workspace_id=$1 AND scope_value='validate_team' AND policy_family='threshold'",
        workspace_id,
    )
    assert row, "CHECK 1 FAILED: policy not persisted"
    print("CHECK 1 PASSED: autopromotion policy persists")


async def check_2_summary_view(conn: asyncpg.Connection, workspace_id: str) -> None:
    """Summary view returns the policy row."""
    rows = await conn.fetch(
        "SELECT policy_id, execution_count FROM governance_policy_autopromotion_summary "
        "WHERE workspace_id=$1 AND scope_value='validate_team'",
        workspace_id,
    )
    assert rows, "CHECK 2 FAILED: policy not visible in summary view"
    print(f"CHECK 2 PASSED: summary view returns {len(rows)} row(s)")


async def check_3_eligibility_view_exists(conn: asyncpg.Connection, workspace_id: str) -> None:
    """Eligibility view is queryable without error."""
    rows = await conn.fetch(
        "SELECT eligible, blocked_reason_code FROM governance_policy_autopromotion_eligibility "
        "WHERE workspace_id=$1 LIMIT 20",
        workspace_id,
    )
    print(f"CHECK 3 PASSED: eligibility view queryable, {len(rows)} row(s) returned")


async def check_4_execution_persists(conn: asyncpg.Connection, workspace_id: str) -> None:
    """Execution row can be inserted and read back."""
    policy_row = await conn.fetchrow(
        "SELECT id FROM governance_policy_autopromotion_policies "
        "WHERE workspace_id=$1 AND scope_value='validate_team'",
        workspace_id,
    )
    assert policy_row
    eid = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO governance_policy_autopromotion_executions
          (id, workspace_id, policy_id, recommendation_key, policy_family,
           promotion_target, scope_type, scope_value, current_policy,
           applied_policy, executed_by, cooldown_applied, metadata)
        VALUES ($1,$2,$3,'validate_key_37c','threshold',
                'threshold_profile','team','validate_team','{}','{}',
                'validate_37c',false,'{}')
        """,
        eid, workspace_id, str(policy_row["id"]),
    )
    row = await conn.fetchrow(
        "SELECT id FROM governance_policy_autopromotion_executions WHERE id=$1", eid
    )
    assert row, "CHECK 4 FAILED: execution row not persisted"
    print("CHECK 4 PASSED: execution row persists")


async def check_5_rollback_candidate_persists(
    conn: asyncpg.Connection, workspace_id: str
) -> None:
    """Rollback candidate row can be inserted and read back."""
    exec_row = await conn.fetchrow(
        "SELECT id FROM governance_policy_autopromotion_executions "
        "WHERE workspace_id=$1 AND recommendation_key='validate_key_37c'",
        workspace_id,
    )
    assert exec_row, "prerequisite execution missing"
    rc_id = str(uuid.uuid4())
    await conn.execute(
        """
        INSERT INTO governance_policy_autopromotion_rollback_candidates
          (id, workspace_id, execution_id, recommendation_key, policy_family,
           scope_type, scope_value, target_type, prior_policy, applied_policy,
           rollback_risk_score, rolled_back, metadata)
        VALUES ($1,$2,$3,'validate_key_37c','threshold',
                'team','validate_team','threshold_profile','{}','{}',
                0.0,false,'{}')
        """,
        rc_id, workspace_id, str(exec_row["id"]),
    )
    row = await conn.fetchrow(
        "SELECT id FROM governance_policy_autopromotion_rollback_candidates WHERE id=$1", rc_id
    )
    assert row, "CHECK 5 FAILED: rollback candidate not persisted"
    print("CHECK 5 PASSED: rollback candidate persists")


async def check_6_disable_policy(conn: asyncpg.Connection, workspace_id: str) -> None:
    """Disable (enabled=false) prevents policy from appearing in enabled filter."""
    await conn.execute(
        "UPDATE governance_policy_autopromotion_policies SET enabled=false "
        "WHERE workspace_id=$1 AND scope_value='validate_team'",
        workspace_id,
    )
    row = await conn.fetchrow(
        "SELECT enabled FROM governance_policy_autopromotion_policies "
        "WHERE workspace_id=$1 AND scope_value='validate_team'",
        workspace_id,
    )
    assert row and not row["enabled"], "CHECK 6 FAILED: policy still enabled after disable"
    print("CHECK 6 PASSED: policy disabled correctly")


async def check_7_route_contract(conn: asyncpg.Connection, workspace_id: str) -> None:
    """All expected columns present in key tables/views."""
    checks = {
        "governance_policy_autopromotion_policies": [
            "id", "workspace_id", "policy_family", "scope_type", "scope_value",
            "promotion_target", "min_confidence", "cooldown_hours", "enabled",
        ],
        "governance_policy_autopromotion_executions": [
            "id", "workspace_id", "policy_id", "recommendation_key",
            "promotion_target", "applied_policy", "executed_by",
        ],
        "governance_policy_autopromotion_rollback_candidates": [
            "id", "workspace_id", "execution_id", "recommendation_key",
            "target_type", "rollback_risk_score", "rolled_back",
        ],
    }
    for table, cols in checks.items():
        row = await conn.fetchrow(f"SELECT {', '.join(cols)} FROM {table} LIMIT 1")  # noqa: S608
        _ = row  # presence of columns without error is the check
    print("CHECK 7 PASSED: route contract columns present in all Phase 3.7C tables")


async def main() -> None:
    conn = await get_conn()
    try:
        workspace_id = await get_workspace_id(conn)
        await check_1_policy_persists(conn, workspace_id)
        await check_2_summary_view(conn, workspace_id)
        await check_3_eligibility_view_exists(conn, workspace_id)
        await check_4_execution_persists(conn, workspace_id)
        await check_5_rollback_candidate_persists(conn, workspace_id)
        await check_6_disable_policy(conn, workspace_id)
        await check_7_route_contract(conn, workspace_id)
        print("\nAll Phase 3.7C checks passed.")
    except AssertionError as exc:
        print(f"\nFAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
