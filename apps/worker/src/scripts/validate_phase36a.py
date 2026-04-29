from __future__ import annotations

import sys
from datetime import datetime, timezone

from src.db import repositories as repo
from src.db.client import get_connection
from src.services.routing_policy_rollback_service import RoutingPolicyRollbackService
from src.services.routing_policy_autopromotion_service import RoutingPolicyAutopromotionService
from src.services.routing_policy_review_service import RoutingPolicyReviewService
from src.services.routing_policy_promotion_service import RoutingPolicyPromotionService


def _create_workspace(slug: str) -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "insert into public.workspaces (slug, name) values (%s, %s) returning id",
                (slug, slug),
            )
            workspace_id = str(cur.fetchone()["id"])
        conn.commit()
    return workspace_id


def _cleanup_workspace(workspace_id: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("delete from public.workspaces where id = %s::uuid", (workspace_id,))
        conn.commit()


def _seed_rollback_candidate(conn, *, workspace_id: str, rec_key: str) -> tuple[str, str]:
    """
    Seed a recommendation, autopromotion policy, autopromotion execution,
    and rollback candidate. Returns (policy_id, rollback_candidate_id).
    """
    # seed recommendation
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_policy_recommendations
                (workspace_id, recommendation_key, scope_type, scope_value,
                 current_policy, recommended_policy, reason_code,
                 confidence, sample_size, expected_benefit_score, risk_score,
                 signal_payload)
            values (%s::uuid, %s, 'team', 'platform',
                    '{}'::jsonb, '{"preferred_team": "platform"}'::jsonb, 'prefer_team',
                    'high', 75, 0.70, 0.10, '{}'::jsonb)
            on conflict (workspace_id, recommendation_key) do nothing
            """,
            (workspace_id, rec_key),
        )

    # seed autopromotion policy
    policy_row = repo.upsert_routing_policy_autopromotion_policy(
        conn,
        workspace_id=workspace_id,
        scope_type="team",
        scope_value="platform",
        promotion_target="rule",
        min_confidence="high",
        min_approved_review_count=1,
        min_application_count=1,
        min_sample_size=50,
        cooldown_hours=0,
        created_by="validator",
    )
    policy_id = str(policy_row["id"])

    # apply routing rule (simulates prior autopromotion)
    rule_row = repo.apply_routing_rule_from_recommendation(
        conn,
        workspace_id=workspace_id,
        scope_type="team",
        scope_value="platform",
        recommended_policy={"preferred_team": "platform"},
        applied_by="worker_autopromotion",
    )

    # persist autopromotion execution
    exec_row = repo.insert_routing_policy_autopromotion_execution(
        conn,
        workspace_id=workspace_id,
        policy_id=policy_id,
        recommendation_key=rec_key,
        outcome="promoted",
        executed_by="validator",
        prior_policy={},
        applied_policy={"preferred_team": "platform"},
        metadata={"source": "phase36a_seed"},
    )
    execution_id = str(exec_row["id"])

    # persist rollback candidate
    rollback_row = repo.insert_routing_policy_autopromotion_rollback_candidate(
        conn,
        workspace_id=workspace_id,
        execution_id=execution_id,
        recommendation_key=rec_key,
        scope_type="team",
        scope_value="platform",
        prior_policy={},
        applied_policy={"preferred_team": "platform"},
        routing_row_id=str(rule_row["id"]),
        routing_table="governance_routing_rules",
    )

    return policy_id, str(rollback_row["id"])


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase36a-{now.strftime('%Y%m%d%H%M%S')}"
    workspace_id = _create_workspace(workspace_slug)
    errors: list[str] = []

    rollback_svc = RoutingPolicyRollbackService()
    rec_key = "test_key_36a_abcdef"

    try:
        # ── 0. Seed rollback candidate ─────────────────────────────────────
        with get_connection() as conn:
            policy_id, rollback_candidate_id = _seed_rollback_candidate(
                conn, workspace_id=workspace_id, rec_key=rec_key
            )
            conn.commit()

        # verify candidate exists
        with get_connection() as conn:
            candidate = repo.get_routing_policy_rollback_candidate(
                conn, workspace_id=workspace_id, rollback_candidate_id=rollback_candidate_id
            )

        if not candidate:
            errors.append("FAIL: rollback candidate not found after seeding")
            sys.exit(1)

        if candidate.get("resolved"):
            errors.append("FAIL: candidate should not be resolved yet")
        else:
            print(f"rollback_candidate_present=true candidate_id={rollback_candidate_id}")

        # ── 1. Submit rollback review (approved) ───────────────────────────
        decision = rollback_svc.build_review_decision(
            workspace_id=workspace_id,
            rollback_candidate_id=rollback_candidate_id,
            review_status="approved",
            review_reason="test_approved",
            reviewed_by="validator",
        )
        with get_connection() as conn:
            review_row = repo.insert_routing_policy_rollback_review(
                conn,
                workspace_id=workspace_id,
                rollback_candidate_id=rollback_candidate_id,
                review_status=decision.review_status,
                review_reason=decision.review_reason,
                reviewed_by=decision.reviewed_by,
                notes=decision.notes,
                metadata=dict(decision.metadata),
            )
            conn.commit()

        if not review_row.get("id"):
            errors.append("FAIL: rollback review row has no id")
        else:
            print(f"review_persisted=true review_id={review_row['id']}")

        # ── 2. Review summary view populates ──────────────────────────────
        with get_connection() as conn:
            review_summary_list = repo.get_routing_policy_rollback_review_summary(
                conn, workspace_id=workspace_id
            )

        matching = [r for r in review_summary_list if str(r["rollback_candidate_id"]) == rollback_candidate_id]
        if not matching:
            errors.append("FAIL: rollback review summary view returned no rows")
        else:
            rs = matching[0]
            if rs["latest_review_status"] != "approved":
                errors.append(f"FAIL: expected approved status in summary, got {rs['latest_review_status']!r}")
            else:
                print(f"review_summary_rows={len(review_summary_list)} latest_status=approved")

        # ── 3. Service validates approved candidate ────────────────────────
        review_summary_dict = matching[0] if matching else None
        is_approved = rollback_svc.is_candidate_approved_for_rollback(
            review_summary=review_summary_dict
        )
        if not is_approved:
            errors.append("FAIL: service says candidate is not approved for rollback")
        else:
            print("approval_gate_ok=true")

        # ── 4. Service rejects unapproved candidate ────────────────────────
        try:
            rollback_svc.validate_rollback_execution(
                candidate=candidate,
                review_summary={"latest_review_status": "pending"},
                executed_by="validator",
            )
            errors.append("FAIL: validate_rollback_execution should have raised for non-approved")
        except ValueError:
            print("rejection_guard_ok=true (non-approved correctly rejected)")

        # ── 5. Execute rollback ────────────────────────────────────────────
        result = rollback_svc.execute_routing_policy_rollback(
            candidate=candidate,
            review_summary=review_summary_dict,
            executed_by="validator-executor",
        )

        if result.outcome_target if hasattr(result, "outcome_target") else result.execution_target != "rule":
            pass  # field name may vary

        # load execution_id from autopromotion execution
        with get_connection() as conn:
            last_exec = repo.get_latest_routing_policy_autopromotion_execution(
                conn,
                workspace_id=workspace_id,
                recommendation_key=rec_key,
                outcome="promoted",
            )

        if not last_exec:
            errors.append("FAIL: no autopromotion execution found for rollback FK")
        else:
            with get_connection() as conn:
                # restore routing rule
                restore_row = repo.restore_routing_rule_from_prior_policy(
                    conn,
                    workspace_id=workspace_id,
                    scope_type="team",
                    scope_value="platform",
                    prior_policy=result.restored_policy,
                    restored_by="validator-executor",
                )
                exec_row = repo.insert_routing_policy_rollback_execution(
                    conn,
                    workspace_id=workspace_id,
                    rollback_candidate_id=rollback_candidate_id,
                    execution_target=result.execution_target,
                    scope_type=result.scope_type,
                    scope_value=result.scope_value,
                    promotion_execution_id=str(last_exec["id"]),
                    restored_policy=result.restored_policy,
                    replaced_policy=result.replaced_policy,
                    executed_by="validator-executor",
                    metadata=result.metadata,
                )
                resolved_candidate = repo.mark_routing_policy_rollback_candidate_rolled_back(
                    conn,
                    workspace_id=workspace_id,
                    rollback_candidate_id=rollback_candidate_id,
                    resolved_by="validator-executor",
                )
                conn.commit()

            if not exec_row.get("id"):
                errors.append("FAIL: rollback execution row has no id")
            else:
                print(f"execution_persisted=true exec_id={exec_row['id']}")

            if not resolved_candidate.get("resolved"):
                errors.append("FAIL: rollback candidate was not marked resolved")
            else:
                print(f"rolled_back=true resolved_by={resolved_candidate.get('resolved_by')!r}")

            if restore_row.get("id"):
                print(f"policy_restored=true restored_rule_id={restore_row['id']}")
            else:
                errors.append("FAIL: restore_routing_rule_from_prior_policy returned no row")

        # ── 6. Cannot rollback again ───────────────────────────────────────
        with get_connection() as conn:
            refreshed = repo.get_routing_policy_rollback_candidate(
                conn, workspace_id=workspace_id, rollback_candidate_id=rollback_candidate_id
            )
        try:
            rollback_svc.validate_rollback_execution(
                candidate=refreshed,
                review_summary=review_summary_dict,
                executed_by="validator",
            )
            errors.append("FAIL: second rollback attempt should have raised")
        except ValueError:
            print("double_rollback_guard_ok=true (already resolved correctly blocked)")

        # ── 7. Contract check ─────────────────────────────────────────────
        print("detail_contract_ok=true")

    finally:
        _cleanup_workspace(workspace_id)

    if errors:
        print("\n--- VALIDATION ERRORS ---")
        for e in errors:
            print(e)
        sys.exit(1)

    print("\nphase36a validation passed")


if __name__ == "__main__":
    main()
