from __future__ import annotations

import sys
from datetime import datetime, timezone

from src.db import repositories as repo
from src.db.client import get_connection
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


def _seed_recommendation(conn, *, workspace_id: str, key: str) -> None:
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
            (workspace_id, key),
        )


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase35c-{now.strftime('%Y%m%d%H%M%S')}"
    workspace_id = _create_workspace(workspace_slug)
    errors: list[str] = []

    autopromo_svc = RoutingPolicyAutopromotionService()
    review_svc = RoutingPolicyReviewService()
    promo_svc = RoutingPolicyPromotionService()
    rec_key = "test_key_35c_abcdef"

    try:
        # ── 0. Seed recommendation + manual review + manual application ────
        with get_connection() as conn:
            _seed_recommendation(conn, workspace_id=workspace_id, key=rec_key)
            conn.commit()

        # insert approved review
        decision = review_svc.build_review_decision(
            workspace_id=workspace_id,
            recommendation_key=rec_key,
            review_status="approved",
            review_reason="test_approved",
            reviewed_by="validator",
        )
        with get_connection() as conn:
            repo.insert_routing_policy_review(
                conn,
                workspace_id=workspace_id,
                recommendation_key=rec_key,
                review_status=decision.review_status,
                review_reason=decision.review_reason,
                reviewed_by=decision.reviewed_by,
                notes=decision.notes,
                metadata=dict(decision.metadata),
            )
            conn.commit()

        # insert manual proposal + application to satisfy min_application_count guardrail
        proposal_obj = promo_svc.build_promotion_proposal(
            workspace_id=workspace_id,
            recommendation_key=rec_key,
            promotion_target="rule",
            scope_type="team",
            scope_value="platform",
            current_policy={},
            recommended_policy={"preferred_team": "platform"},
            proposed_by="validator",
            proposal_reason="seed_for_35c",
        )
        with get_connection() as conn:
            proposal_row = repo.insert_routing_policy_promotion_proposal(
                conn,
                workspace_id=proposal_obj.workspace_id,
                recommendation_key=proposal_obj.recommendation_key,
                promotion_target=proposal_obj.promotion_target,
                scope_type=proposal_obj.scope_type,
                scope_value=proposal_obj.scope_value,
                current_policy=dict(proposal_obj.current_policy),
                recommended_policy=dict(proposal_obj.recommended_policy),
                proposed_by=proposal_obj.proposed_by,
                proposal_reason=proposal_obj.proposal_reason,
                metadata=dict(proposal_obj.metadata),
            )
            conn.commit()

        proposal_id = str(proposal_row["id"])

        with get_connection() as conn:
            repo.update_routing_policy_promotion_proposal(conn, proposal_id=proposal_id, status="approved", approved_by="validator")
            rule_row = repo.apply_routing_rule_from_recommendation(
                conn,
                workspace_id=workspace_id,
                scope_type="team",
                scope_value="platform",
                recommended_policy={"preferred_team": "platform"},
                applied_by="validator",
            )
            repo.insert_routing_policy_application(
                conn,
                workspace_id=workspace_id,
                proposal_id=proposal_id,
                recommendation_key=rec_key,
                applied_target="rule",
                applied_scope_type="team",
                applied_scope_value="platform",
                prior_policy={},
                applied_policy={"preferred_team": "platform"},
                applied_by="validator",
                metadata={"source": "phase35c_seed"},
            )
            repo.update_routing_policy_promotion_proposal(conn, proposal_id=proposal_id, status="applied")
            conn.commit()

        print(f"seed_complete=true rule_id={rule_row['id']}")

        # ── 1. Create autopromotion policy ─────────────────────────────────
        with get_connection() as conn:
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
                cooldown_hours=0,  # no cooldown for test
                created_by="validator",
            )
            conn.commit()

        policy_id = str(policy_row["id"])
        if not policy_id:
            errors.append("FAIL: autopromotion policy row has no id")
        else:
            print(f"autopromotion_policy_created=true policy_id={policy_id}")

        # ── 2. Eligibility evaluation — service logic ──────────────────────
        rec_stub = {"confidence": "high", "sample_size": 75}
        eligibility = autopromo_svc.evaluate_autopromotion_eligibility(
            policy=policy_row,
            recommendation=rec_stub,
            approved_review_count=1,
            application_count=1,
            last_promoted_at_hours_ago=None,
        )
        if not eligibility.eligible:
            errors.append(f"FAIL: expected eligible, got blocked: {eligibility.blocked_reason}")
        else:
            print("eligibility_ok=true")

        # ── 3. Cooldown guard ──────────────────────────────────────────────
        blocked_eligibility = autopromo_svc.evaluate_autopromotion_eligibility(
            policy={**policy_row, "cooldown_hours": 168},
            recommendation=rec_stub,
            approved_review_count=1,
            application_count=1,
            last_promoted_at_hours_ago=12.0,  # 12 hrs ago, within 168hr cooldown
        )
        if blocked_eligibility.eligible or blocked_eligibility.blocked_reason != "in_cooldown":
            errors.append(f"FAIL: cooldown guard did not block: {blocked_eligibility}")
        else:
            print("cooldown_guard_ok=true")

        # ── 4. Insufficient review guard ──────────────────────────────────
        blocked_review = autopromo_svc.evaluate_autopromotion_eligibility(
            policy={**policy_row, "min_approved_review_count": 3},
            recommendation=rec_stub,
            approved_review_count=1,
            application_count=1,
            last_promoted_at_hours_ago=None,
        )
        if blocked_review.eligible or blocked_review.blocked_reason != "insufficient_approved_reviews":
            errors.append(f"FAIL: review count guard did not block: {blocked_review}")
        else:
            print("review_count_guard_ok=true")

        # ── 5. Build and persist autopromotion execution (promoted) ───────
        decision_obj = autopromo_svc.build_autopromotion_decision(
            eligibility=eligibility,
            prior_policy={},
            recommended_policy={"preferred_team": "platform"},
            policy_id=policy_id,
            recommendation_key=rec_key,
        )

        with get_connection() as conn:
            new_rule = repo.apply_routing_rule_from_recommendation(
                conn,
                workspace_id=workspace_id,
                scope_type="team",
                scope_value="platform",
                recommended_policy={"preferred_team": "platform"},
                applied_by="worker_autopromotion",
            )
            exec_row = repo.insert_routing_policy_autopromotion_execution(
                conn,
                workspace_id=workspace_id,
                policy_id=policy_id,
                recommendation_key=rec_key,
                outcome=decision_obj.outcome,
                blocked_reason=decision_obj.blocked_reason,
                skipped_reason=decision_obj.skipped_reason,
                executed_by="validator",
                prior_policy=decision_obj.prior_policy,
                applied_policy=decision_obj.applied_policy,
                metadata=decision_obj.proposal_metadata,
            )
            rollback_row = repo.insert_routing_policy_autopromotion_rollback_candidate(
                conn,
                workspace_id=workspace_id,
                execution_id=str(exec_row["id"]),
                recommendation_key=rec_key,
                scope_type="team",
                scope_value="platform",
                prior_policy={},
                applied_policy={"preferred_team": "platform"},
                routing_row_id=str(new_rule["id"]),
                routing_table="governance_routing_rules",
            )
            conn.commit()

        if exec_row["outcome"] != "promoted":
            errors.append(f"FAIL: expected outcome=promoted, got {exec_row['outcome']!r}")
        else:
            print(f"execution_persisted=true exec_id={exec_row['id']}")

        if not rollback_row.get("id"):
            errors.append("FAIL: rollback candidate row has no id")
        else:
            print(f"rollback_candidate_ok=true rollback_id={rollback_row['id']}")

        # ── 6. Summary view populates ──────────────────────────────────────
        with get_connection() as conn:
            summary = repo.get_routing_policy_autopromotion_summary(conn, workspace_id=workspace_id)

        matching = [r for r in summary if r["recommendation_key"] == rec_key]
        if not matching:
            errors.append("FAIL: autopromotion summary view returned no rows")
        else:
            s = matching[0]
            if s["latest_outcome"] != "promoted":
                errors.append(f"FAIL: summary latest_outcome={s['latest_outcome']!r}, expected promoted")
            elif s["promoted_count"] < 1:
                errors.append(f"FAIL: promoted_count={s['promoted_count']}, expected >= 1")
            else:
                print(f"summary_ok=true promoted_count={s['promoted_count']}")

        # ── 7. Eligibility view populates ─────────────────────────────────
        with get_connection() as conn:
            eligibility_rows = repo.get_routing_policy_autopromotion_eligibility(conn, workspace_id=workspace_id)

        elig_matching = [r for r in eligibility_rows if r["recommendation_key"] == rec_key]
        if not elig_matching:
            errors.append("FAIL: eligibility view returned no rows for rec_key")
        else:
            print(f"eligibility_view_ok=true is_eligible={elig_matching[0]['is_eligible']}")

    finally:
        _cleanup_workspace(workspace_id)

    if errors:
        print("\n--- VALIDATION ERRORS ---")
        for e in errors:
            print(e)
        sys.exit(1)

    print("\nphase35c validation passed")


if __name__ == "__main__":
    main()
