from __future__ import annotations

from datetime import datetime, timezone

from src.db.client import get_connection
from src.db.repositories import (
    insert_governance_threshold_feedback,
    list_governance_threshold_learning_summary,
    list_governance_threshold_performance_summary,
    replace_governance_threshold_recommendations,
)
from src.services.threshold_learning_service import (
    ThresholdLearningService,
    ThresholdOutcomeContext,
)


def _create_workspace_and_watchlist(workspace_slug: str, watchlist_slug: str) -> tuple[str, str]:
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
                insert into public.watchlists (workspace_id, slug, name)
                values (%s::uuid, %s, %s)
                returning id
                """,
                (workspace_id, watchlist_slug, watchlist_slug),
            )
            watchlist_id = str(cur.fetchone()["id"])
        conn.commit()
    return workspace_id, watchlist_id


def _cleanup_workspace(workspace_id: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("delete from public.workspaces where id = %s::uuid", (workspace_id,))
        conn.commit()


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase32a-{now.strftime('%Y%m%d%H%M%S')}"
    workspace_id, watchlist_id = _create_workspace_and_watchlist(workspace_slug, "threshold-learning-core")
    service = ThresholdLearningService()

    try:
        noisy_contexts = [
            ThresholdOutcomeContext(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                threshold_profile_id=None,
                event_type="family_instability_spike",
                regime="risk_off",
                compute_version="compute-v1",
                signal_registry_version="signals-v1",
                model_version="model-v1",
                case_id=None,
                degradation_state_id=None,
                threshold_applied_value=0.85,
                acknowledged=False,
                muted=False,
                escalated=False,
                resolved=False,
                reopened=False,
                evidence={"validator": "phase32a", "kind": "noisy"},
            ),
            ThresholdOutcomeContext(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                threshold_profile_id=None,
                event_type="family_instability_spike",
                regime="risk_off",
                compute_version="compute-v1",
                signal_registry_version="signals-v1",
                model_version="model-v1",
                case_id=None,
                degradation_state_id=None,
                threshold_applied_value=0.85,
                acknowledged=False,
                muted=True,
                escalated=False,
                resolved=False,
                reopened=False,
                evidence={"validator": "phase32a", "kind": "muted"},
            ),
            ThresholdOutcomeContext(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                threshold_profile_id=None,
                event_type="family_instability_spike",
                regime="risk_off",
                compute_version="compute-v1",
                signal_registry_version="signals-v1",
                model_version="model-v1",
                case_id=None,
                degradation_state_id=None,
                threshold_applied_value=0.85,
                acknowledged=False,
                muted=False,
                escalated=False,
                resolved=False,
                reopened=False,
                evidence={"validator": "phase32a", "kind": "repeat"},
            ),
        ]

        with get_connection() as conn:
            for ctx in noisy_contexts:
                insert_governance_threshold_feedback(conn, service.build_feedback_row(ctx))

            performance = list_governance_threshold_performance_summary(conn, workspace_id=workspace_id)
            recommendations = service.build_recommendations(performance)
            replace_governance_threshold_recommendations(
                conn,
                workspace_id=workspace_id,
                rows=recommendations,
            )
            learning_summary = list_governance_threshold_learning_summary(conn, workspace_id=workspace_id)
            conn.commit()

        if not performance:
            raise RuntimeError("expected threshold performance rows")
        if not learning_summary:
            raise RuntimeError("expected threshold learning recommendations")

        print(f"feedback_rows={sum(int(row['feedback_rows']) for row in performance)}")
        print(f"performance_rows={len(performance)}")
        print(f"recommendation_rows={len(learning_summary)}")
        print(f"top_reason_code={learning_summary[0]['reason_code']}")
        print("detail_contract_ok=true")
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
