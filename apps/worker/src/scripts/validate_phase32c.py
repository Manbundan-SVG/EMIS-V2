from __future__ import annotations

from datetime import datetime, timezone

from src.db.client import get_connection
from src.db.repositories import (
    apply_threshold_promotion,
    create_threshold_promotion_execution,
    create_threshold_rollback_candidate,
    get_threshold_autopromotion_policy,
    get_recent_threshold_promotion_execution,
    list_governance_threshold_learning_summary,
    list_threshold_autopromotion_summary,
    replace_governance_threshold_recommendations,
    upsert_threshold_autopromotion_policy,
    upsert_threshold_promotion_proposal,
)
from src.services.threshold_autopromotion_service import ThresholdAutoPromotionService
from src.services.threshold_review_service import ThresholdReviewService


def _create_workspace(workspace_slug: str) -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.workspaces (slug, name)
                values (%s, %s)
                returning id
                """,
                (workspace_slug, workspace_slug),
            )
            workspace_id = str(cur.fetchone()["id"])
            cur.execute(
                """
                insert into public.governance_threshold_profiles (
                  workspace_id,
                  profile_name,
                  is_default,
                  enabled,
                  family_instability_ceiling,
                  metadata
                ) values (
                  %s::uuid,
                  'validator_profile',
                  true,
                  true,
                  0.50,
                  jsonb_build_object('validator', 'phase32c')
                )
                """,
                (workspace_id,),
            )
        conn.commit()
    return workspace_id


def _cleanup_workspace(workspace_id: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("delete from public.workspaces where id = %s::uuid", (workspace_id,))
        conn.commit()


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase32c-{now.strftime('%Y%m%d%H%M%S')}"
    workspace_id = _create_workspace(workspace_slug)
    review_service = ThresholdReviewService()
    autopromotion_service = ThresholdAutoPromotionService()

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select id
                    from public.governance_threshold_profiles
                    where workspace_id = %s::uuid
                    limit 1
                    """,
                    (workspace_id,),
                )
                profile_id = str(cur.fetchone()["id"])

            replace_governance_threshold_recommendations(
                conn,
                workspace_id=workspace_id,
                rows=[
                    {
                        "workspace_id": workspace_id,
                        "threshold_profile_id": profile_id,
                        "dimension_type": "regime",
                        "dimension_value": "risk_off",
                        "event_type": "family_instability_spike",
                        "current_value": 0.50,
                        "recommended_value": 0.55,
                        "direction": "loosen",
                        "reason_code": "high_noise",
                        "confidence": 0.92,
                        "supporting_metrics": {"support_count": 8, "feedback_rows": 8, "confidence": 0.92},
                    }
                ],
            )
            recommendation = list_governance_threshold_learning_summary(conn, workspace_id=workspace_id)[0]
            proposal = review_service.build_promotion_proposal(recommendation)
            if proposal is None:
                raise RuntimeError("expected autopromotion proposal draft")
            proposal_row = upsert_threshold_promotion_proposal(
                conn,
                workspace_id=workspace_id,
                recommendation_id=proposal.recommendation_id,
                profile_id=proposal.profile_id,
                event_type=proposal.event_type,
                dimension_type=proposal.dimension_type,
                dimension_value=proposal.dimension_value,
                current_value=proposal.current_value,
                proposed_value=proposal.proposed_value,
                source_metrics=proposal.source_metrics,
                metadata=proposal.metadata,
            )

            upsert_threshold_autopromotion_policy(
                conn,
                workspace_id=workspace_id,
                profile_id=profile_id,
                event_type="family_instability_spike",
                dimension_type="regime",
                dimension_value="risk_off",
                enabled=True,
                min_confidence=0.80,
                min_support=5,
                max_step_pct=0.20,
                cooldown_hours=24,
                metadata={"validator": "phase32c"},
            )
            policy = get_threshold_autopromotion_policy(
                conn,
                workspace_id=workspace_id,
                profile_id=profile_id,
                event_type="family_instability_spike",
                dimension_type="regime",
                dimension_value="risk_off",
            )
            if not policy:
                raise RuntimeError("expected autopromotion policy")

            eligibility = autopromotion_service.evaluate(
                proposal={
                    **proposal_row,
                    "source_metrics": recommendation["supporting_metrics"],
                },
                policy=policy,
            )
            if not eligibility.eligible:
                raise RuntimeError(f"expected eligible proposal, got {eligibility.reason}")

            apply_threshold_promotion(
                conn,
                workspace_id=workspace_id,
                profile_id=profile_id,
                event_type="family_instability_spike",
                dimension_type="regime",
                dimension_value="risk_off",
                new_value=float(eligibility.new_value or proposal_row["proposed_value"]),
            )
            execution = create_threshold_promotion_execution(
                conn,
                workspace_id=workspace_id,
                proposal_id=str(proposal_row["id"]),
                profile_id=profile_id,
                event_type="family_instability_spike",
                dimension_type="regime",
                dimension_value="risk_off",
                previous_value=float(eligibility.previous_value or proposal_row["current_value"]),
                new_value=float(eligibility.new_value or proposal_row["proposed_value"]),
                executed_by="validator_auto",
                execution_mode="automatic",
                rationale="validator_autopromotion",
                metadata={"policy_id": str(policy["id"])},
            )
            create_threshold_rollback_candidate(
                conn,
                workspace_id=workspace_id,
                execution_id=str(execution["id"]),
                profile_id=profile_id,
                rollback_to_value=float(eligibility.previous_value or proposal_row["current_value"]),
                reason="validator_monitoring_window",
                metadata={"validator": "phase32c"},
            )
            summary = list_threshold_autopromotion_summary(conn, workspace_id=workspace_id)
            recent_execution = get_recent_threshold_promotion_execution(
                conn,
                workspace_id=workspace_id,
                profile_id=profile_id,
                event_type="family_instability_spike",
                dimension_type="regime",
                dimension_value="risk_off",
                since_hours=24,
            )
            conn.commit()

        if not recent_execution:
          raise RuntimeError("expected recent autopromotion execution")

        print("policy_selected=true")
        print("execution_persisted=1")
        print("rollback_candidate_persisted=1")
        print(f"autopromotion_summary_rows={len(summary)}")
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
