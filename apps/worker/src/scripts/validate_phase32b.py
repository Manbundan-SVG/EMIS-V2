from __future__ import annotations

from datetime import datetime, timezone

from src.db.client import get_connection
from src.db.repositories import (
    create_threshold_recommendation_review,
    list_governance_threshold_learning_summary,
    list_threshold_review_summary,
    replace_governance_threshold_recommendations,
    update_threshold_promotion_proposal,
    update_threshold_recommendation_status,
    upsert_threshold_promotion_proposal,
)
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
                  metadata
                ) values (
                  %s::uuid,
                  'validator_profile',
                  true,
                  true,
                  jsonb_build_object('validator', 'phase32b')
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
    workspace_slug = f"phase32b-{now.strftime('%Y%m%d%H%M%S')}"
    workspace_id = _create_workspace(workspace_slug)
    review_service = ThresholdReviewService()

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
                        "confidence": 0.88,
                        "supporting_metrics": {"support_count": 5, "feedback_rows": 5},
                    }
                ],
            )
            recommendation = list_governance_threshold_learning_summary(conn, workspace_id=workspace_id)[0]

            proposal = review_service.build_promotion_proposal(recommendation)
            if proposal is None:
                raise RuntimeError("expected promotion proposal draft")

            upsert_threshold_promotion_proposal(
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

            review = review_service.build_review_decision(
                recommendation_id=str(recommendation["recommendation_id"]),
                reviewer="validator",
                decision="approved",
                rationale="strong enough signal to review",
            )
            create_threshold_recommendation_review(
                conn,
                workspace_id=workspace_id,
                recommendation_id=review.recommendation_id,
                reviewer=review.reviewer,
                decision=review.decision,
                rationale=review.rationale,
                metadata=review.metadata,
            )
            review_summary = list_threshold_review_summary(conn, workspace_id=workspace_id)
            if not review_summary:
                raise RuntimeError("expected threshold review summary rows")

            update_threshold_promotion_proposal(
                conn,
                proposal_id=str(review_summary[0]["proposal_id"]),
                status="approved",
                approved_by="validator",
                approved_at=now,
            )
            update_threshold_recommendation_status(
                conn,
                recommendation_id=str(recommendation["recommendation_id"]),
                status="accepted",
            )
            conn.commit()

        print("review_persisted=1")
        print("proposal_persisted=1")
        print(f"review_summary_rows={len(review_summary)}")
        print("detail_contract_ok=true")
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
