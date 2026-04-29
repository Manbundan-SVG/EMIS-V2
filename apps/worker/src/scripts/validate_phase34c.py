from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.db import repositories as repo
from src.db.client import get_connection
from src.services.case_management_service import build_case_seed
from src.services.promotion_impact_service import PromotionImpactService
from src.services.routing_outcome_service import RoutingOutcomeEvent
from src.services.routing_recommendation_review_service import (
    RoutingRecommendationReview,
    RoutingRecommendationReviewService,
)
from src.services.routing_recommendation_service import RoutingRecommendationService
from src.services.threshold_review_service import ThresholdReviewService


def _create_workspace_and_watchlist(workspace_slug: str, watchlist_slug: str) -> tuple[str, str, str]:
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
                  jsonb_build_object('validator', 'phase34c')
                )
                returning id
                """,
                (workspace_id,),
            )
            profile_id = str(cur.fetchone()["id"])
        conn.commit()
    return workspace_id, watchlist_id, profile_id


def _cleanup_workspace(workspace_id: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("delete from public.workspaces where id = %s::uuid", (workspace_id,))
        conn.commit()


def _seed_case(
    conn,
    *,
    workspace_id: str,
    watchlist_id: str,
    title_suffix: str,
    root_cause_code: str,
    severity: str,
    version_tuple: str,
    opened_at: datetime,
    assigned_to: str | None = None,
    assigned_team: str | None = None,
) -> dict:
    case_row = repo.upsert_governance_case(
        conn,
        build_case_seed(
            workspace_id=workspace_id,
            degradation_state_id=None,
            watchlist_id=watchlist_id,
            version_tuple=version_tuple,
            degradation_type=root_cause_code,
            severity=severity,
            source_summary={"message": f"phase34c {title_suffix}"},
        ).__dict__,
    )
    repo.upsert_governance_case_summary(
        conn,
        workspace_id=workspace_id,
        case_id=str(case_row["id"]),
        summary_version="v1",
        status_summary=f"{title_suffix} status",
        root_cause_code=root_cause_code,
        root_cause_confidence=0.95,
        root_cause_summary=f"{root_cause_code} detected",
        evidence_summary=None,
        recurrence_summary=None,
        operator_summary=None,
        closure_summary=None,
        recommended_next_action="review promotion impact",
        source_note_ids=[],
        source_evidence_ids=[],
        metadata={"validator": "phase34c"},
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_cases
            set opened_at = %s::timestamptz,
                current_assignee = %s,
                current_team = %s,
                updated_at = now()
            where id = %s::uuid
            returning *
            """,
            (opened_at, assigned_to, assigned_team, str(case_row["id"])),
        )
        return dict(cur.fetchone())


def _set_created_at(conn, *, table: str, row_id: str, value: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            update public.{table}
            set created_at = %s::timestamptz
            where id = %s::uuid
            """,
            (value, row_id),
        )


def _insert_incident_snapshot(
    conn,
    *,
    workspace_id: str,
    snapshot_date: datetime,
    mean_resolve_hours: float,
    recurring_case_count: int,
    escalated_case_count: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_incident_analytics_snapshots (
              workspace_id,
              snapshot_date,
              open_case_count,
              acknowledged_case_count,
              resolved_case_count,
              reopened_case_count,
              recurring_case_count,
              high_severity_open_count,
              mean_ack_hours,
              mean_resolve_hours,
              escalated_case_count,
              stale_case_count,
              created_at
            ) values (
              %s::uuid,
              %s::date,
              4,
              2,
              2,
              %s,
              %s,
              1,
              2.0,
              %s,
              %s,
              1,
              %s::timestamptz
            )
            """,
            (
                workspace_id,
                snapshot_date,
                recurring_case_count,
                recurring_case_count,
                mean_resolve_hours,
                escalated_case_count,
                snapshot_date,
            ),
        )


def _seed_threshold_impact(
    conn,
    *,
    workspace_id: str,
    watchlist_id: str,
    profile_id: str,
    executed_at: datetime,
) -> None:
    review_service = ThresholdReviewService()

    repo.replace_governance_threshold_recommendations(
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
                "supporting_metrics": {"support_count": 12, "feedback_rows": 12, "confidence": 0.92},
            }
        ],
    )
    recommendation = repo.list_governance_threshold_learning_summary(conn, workspace_id=workspace_id)[0]
    proposal = review_service.build_promotion_proposal(recommendation)
    if proposal is None:
        raise RuntimeError("expected threshold promotion proposal")
    proposal_row = repo.upsert_threshold_promotion_proposal(
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
    repo.upsert_threshold_autopromotion_policy(
        conn,
        workspace_id=workspace_id,
        profile_id=profile_id,
        event_type="family_instability_spike",
        dimension_type="regime",
        dimension_value="risk_off",
        enabled=True,
        min_confidence=0.80,
        min_support=5,
        max_step_pct=0.25,
        cooldown_hours=24,
        metadata={"validator": "phase34c"},
    )
    repo.apply_threshold_promotion(
        conn,
        workspace_id=workspace_id,
        profile_id=profile_id,
        event_type="family_instability_spike",
        dimension_type="regime",
        dimension_value="risk_off",
        new_value=float(proposal_row["proposed_value"]),
    )
    execution = repo.create_threshold_promotion_execution(
        conn,
        workspace_id=workspace_id,
        proposal_id=str(proposal_row["id"]),
        profile_id=profile_id,
        event_type="family_instability_spike",
        dimension_type="regime",
        dimension_value="risk_off",
        previous_value=float(proposal_row["current_value"]),
        new_value=float(proposal_row["proposed_value"]),
        executed_by="validator_auto",
        execution_mode="automatic",
        rationale="validator_threshold_impact",
        metadata={"validator": "phase34c"},
    )
    repo.create_threshold_rollback_candidate(
        conn,
        workspace_id=workspace_id,
        execution_id=str(execution["id"]),
        profile_id=profile_id,
        rollback_to_value=float(proposal_row["current_value"]),
        reason="validator_monitoring_window",
        metadata={"validator": "phase34c"},
    )
    _set_created_at(
        conn,
        table="governance_threshold_promotion_executions",
        row_id=str(execution["id"]),
        value=executed_at,
    )

    before_case = _seed_case(
        conn,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        title_suffix="threshold-before",
        root_cause_code="family_instability_spike",
        severity="high",
        version_tuple="compute-t1|signals-t1|model-t1",
        opened_at=executed_at - timedelta(days=2),
    )
    after_case = _seed_case(
        conn,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        title_suffix="threshold-after",
        root_cause_code="family_instability_spike",
        severity="medium",
        version_tuple="compute-t1|signals-t1|model-t1",
        opened_at=executed_at + timedelta(days=2),
    )

    before_feedback = repo.insert_governance_threshold_feedback(
        conn,
        {
            "workspace_id": workspace_id,
            "watchlist_id": watchlist_id,
            "threshold_profile_id": profile_id,
            "event_type": "family_instability_spike",
            "regime": "risk_off",
            "compute_version": "compute-t1",
            "signal_registry_version": "signals-t1",
            "model_version": "model-t1",
            "case_id": str(before_case["id"]),
            "degradation_state_id": None,
            "threshold_applied_value": 0.50,
            "trigger_count": 10,
            "ack_count": 4,
            "mute_count": 2,
            "escalation_count": 3,
            "resolution_count": 4,
            "reopen_count": 4,
            "precision_proxy": 0.42,
            "noise_score": 0.61,
            "evidence": {"validator": "phase34c", "window": "before"},
        },
    )
    after_feedback = repo.insert_governance_threshold_feedback(
        conn,
        {
            "workspace_id": workspace_id,
            "watchlist_id": watchlist_id,
            "threshold_profile_id": profile_id,
            "event_type": "family_instability_spike",
            "regime": "risk_off",
            "compute_version": "compute-t1",
            "signal_registry_version": "signals-t1",
            "model_version": "model-t1",
            "case_id": str(after_case["id"]),
            "degradation_state_id": None,
            "threshold_applied_value": 0.55,
            "trigger_count": 10,
            "ack_count": 7,
            "mute_count": 0,
            "escalation_count": 1,
            "resolution_count": 8,
            "reopen_count": 1,
            "precision_proxy": 0.81,
            "noise_score": 0.18,
            "evidence": {"validator": "phase34c", "window": "after"},
        },
    )
    _set_created_at(
        conn,
        table="governance_threshold_feedback",
        row_id=str(before_feedback["id"]),
        value=executed_at - timedelta(days=1),
    )
    _set_created_at(
        conn,
        table="governance_threshold_feedback",
        row_id=str(after_feedback["id"]),
        value=executed_at + timedelta(days=1),
    )

    _insert_incident_snapshot(
        conn,
        workspace_id=workspace_id,
        snapshot_date=executed_at - timedelta(days=1),
        mean_resolve_hours=8.0,
        recurring_case_count=3,
        escalated_case_count=3,
    )
    _insert_incident_snapshot(
        conn,
        workspace_id=workspace_id,
        snapshot_date=executed_at + timedelta(days=1),
        mean_resolve_hours=3.0,
        recurring_case_count=1,
        escalated_case_count=1,
    )


def _seed_routing_recommendation(
    conn,
    *,
    workspace_id: str,
    watchlist_id: str,
    now: datetime,
) -> dict[str, dict]:
    review_service = RoutingRecommendationReviewService()

    for offset_hours in (12, 9, 6):
        history_case = _seed_case(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            title_suffix=f"routing-history-{offset_hours}",
            root_cause_code="version_regression",
            severity="high",
            version_tuple="compute-r1|signals-r1|model-r1",
            opened_at=now - timedelta(hours=offset_hours),
            assigned_to="alice",
            assigned_team="research",
        )
        decision_row = repo.create_governance_routing_decision(
            conn,
            workspace_id=workspace_id,
            case_id=str(history_case["id"]),
            routing_rule_id=None,
            override_id=None,
            assigned_team="research",
            assigned_user="alice",
            routing_reason="validator_phase34c_history",
            workload_snapshot={"validator": "phase34c"},
            metadata={"validator": "phase34c"},
        )
        repo.create_governance_assignment(
            conn,
            case_id=str(history_case["id"]),
            workspace_id=workspace_id,
            assigned_to="alice",
            assigned_team="research",
            assigned_by="worker",
            reason="validator_phase34c_history",
            metadata={"validator": "phase34c"},
        )
        repo.create_governance_routing_feedback(
            conn,
            workspace_id=workspace_id,
            case_id=str(history_case["id"]),
            routing_decision_id=str(decision_row["id"]),
            feedback_type="accepted",
            assigned_to="alice",
            assigned_team="research",
            prior_assigned_to=None,
            prior_assigned_team=None,
            root_cause_code="version_regression",
            severity="high",
            recurrence_group_id=None,
            repeat_count=1,
            reason="validator_phase34c_history",
            metadata={"validator": "phase34c"},
        )

    target_case = _seed_case(
        conn,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        title_suffix="routing-target",
        root_cause_code="version_regression",
        severity="high",
        version_tuple="compute-r1|signals-r1|model-r1",
        opened_at=now - timedelta(hours=1),
    )
    case_row = repo.get_case_for_routing_recommendation(
        conn,
        workspace_id=workspace_id,
        case_id=str(target_case["id"]),
    )
    if case_row is None:
        raise RuntimeError("expected routing recommendation case row")
    candidates = repo.list_routing_recommendation_candidates(
        conn,
        workspace_id=workspace_id,
        case_row=case_row,
    )
    recommendation = RoutingRecommendationService().recommend(case_row=case_row, candidate_rows=candidates)
    recommendation_row = repo.upsert_governance_routing_recommendation(
        conn,
        workspace_id=workspace_id,
        case_id=str(target_case["id"]),
        recommendation={
            "recommendation_key": recommendation.recommendation_key,
            "recommended_user": recommendation.recommended_user,
            "recommended_team": recommendation.recommended_team,
            "fallback_user": recommendation.fallback_user,
            "fallback_team": recommendation.fallback_team,
            "reason_code": recommendation.reason_code,
            "confidence": recommendation.confidence,
            "score": recommendation.score,
            "supporting_metrics": recommendation.supporting_metrics,
            "model_inputs": recommendation.model_inputs,
            "alternatives": recommendation.alternatives,
        },
    )
    repo.insert_governance_routing_recommendation_review(
        conn,
        metadata={"validator": "phase34c"},
        **review_service.build_review_row(
            RoutingRecommendationReview(
                workspace_id=workspace_id,
                recommendation_id=str(recommendation_row["id"]),
                case_id=str(target_case["id"]),
                review_status="approved",
                review_reason="validator_approved",
                notes="approved in validator",
                reviewed_by="validator",
                apply_immediately=False,
            )
        ),
    )
    return {
        "case": target_case,
        "recommendation": recommendation_row,
    }


def _seed_routing_impact(
    conn,
    *,
    workspace_id: str,
    watchlist_id: str,
    executed_at: datetime,
) -> None:
    seeded = _seed_routing_recommendation(
        conn,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        now=executed_at + timedelta(hours=2),
    )
    recommendation_row = seeded["recommendation"]
    candidate = repo.get_governance_routing_autopromotion_candidate(
        conn,
        recommendation_id=str(recommendation_row["id"]),
    )
    if candidate is None:
        raise RuntimeError("expected routing autopromotion candidate")

    policy = repo.upsert_governance_routing_autopromotion_policy(
        conn,
        workspace_id=workspace_id,
        scope_type="root_cause",
        scope_value="version_regression",
        enabled=True,
        promotion_target="override",
        min_confidence=str(candidate["confidence"]),
        min_acceptance_rate=max(0.0, float(candidate.get("acceptance_rate") or 0.0) - 0.05),
        min_sample_size=max(1, int(candidate.get("sample_size") or 1)),
        max_recent_override_rate=min(1.0, float(candidate.get("override_rate") or 0.0) + 0.05),
        cooldown_hours=24,
        created_by="validator",
        metadata={"validator": "phase34c"},
    )
    apply_result = repo.apply_governance_routing_autopromotion(
        conn,
        workspace_id=workspace_id,
        policy=policy,
        recommendation=candidate,
        target_type="override",
        target_key=f"root_cause|version_regression|{candidate.get('recommended_team') or 'research'}",
        executed_by="validator_autopromotion",
    )
    execution = repo.create_governance_routing_autopromotion_execution(
        conn,
        workspace_id=workspace_id,
        policy_id=str(policy["id"]),
        recommendation_id=str(candidate["id"]),
        target_type="override",
        target_key=f"root_cause|version_regression|{candidate.get('recommended_team') or 'research'}",
        recommended_user=candidate.get("recommended_user"),
        recommended_team=candidate.get("recommended_team"),
        confidence=str(candidate.get("confidence") or "high"),
        acceptance_rate=float(candidate.get("acceptance_rate") or 0.0),
        sample_size=int(candidate.get("sample_size") or 0),
        override_rate=float(candidate.get("override_rate") or 0.0),
        execution_status="executed",
        execution_reason="validator_routing_impact",
        cooldown_bucket=f"phase34c:{executed_at.strftime('%Y%m%d%H')}",
        prior_state=apply_result["prior_state"],
        new_state=apply_result["new_state"],
        metadata={"validator": "phase34c"},
    )
    repo.create_governance_routing_autopromotion_rollback_candidate(
        conn,
        workspace_id=workspace_id,
        execution_id=str(execution["id"]),
        target_type="override",
        target_key=f"root_cause|version_regression|{candidate.get('recommended_team') or 'research'}",
        prior_state=apply_result["prior_state"],
        rollback_reason="validator_monitoring_window",
    )
    _set_created_at(
        conn,
        table="governance_routing_autopromotion_executions",
        row_id=str(execution["id"]),
        value=executed_at,
    )

    before_case = _seed_case(
        conn,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        title_suffix="routing-before",
        root_cause_code="version_regression",
        severity="high",
        version_tuple="compute-r1|signals-r1|model-r1",
        opened_at=executed_at - timedelta(days=2),
        assigned_to="alice",
        assigned_team="research",
    )
    after_case = _seed_case(
        conn,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        title_suffix="routing-after",
        root_cause_code="version_regression",
        severity="high",
        version_tuple="compute-r1|signals-r1|model-r1",
        opened_at=executed_at + timedelta(days=2),
        assigned_to="alice",
        assigned_team="research",
    )

    before_events = [
        RoutingOutcomeEvent(
            workspace_id=workspace_id,
            case_id=str(before_case["id"]),
            outcome_type="assigned",
            occurred_at=executed_at - timedelta(days=1, hours=2),
            assigned_to="alice",
            assigned_team="research",
            root_cause_code="version_regression",
            severity="high",
            watchlist_id=watchlist_id,
            compute_version="compute-r1",
            signal_registry_version="signals-r1",
            model_version="model-r1",
            outcome_context={"validator": "phase34c", "window": "before"},
        ),
        RoutingOutcomeEvent(
            workspace_id=workspace_id,
            case_id=str(before_case["id"]),
            outcome_type="time_to_resolve_hours",
            occurred_at=executed_at - timedelta(days=1, hours=1),
            assigned_to="alice",
            assigned_team="research",
            root_cause_code="version_regression",
            severity="high",
            watchlist_id=watchlist_id,
            compute_version="compute-r1",
            signal_registry_version="signals-r1",
            model_version="model-r1",
            outcome_value=2.0,
            outcome_context={"validator": "phase34c", "window": "before"},
        ),
    ]
    after_events = [
        RoutingOutcomeEvent(
            workspace_id=workspace_id,
            case_id=str(after_case["id"]),
            outcome_type="assigned",
            occurred_at=executed_at + timedelta(days=1, hours=1),
            assigned_to="alice",
            assigned_team="research",
            root_cause_code="version_regression",
            severity="high",
            watchlist_id=watchlist_id,
            compute_version="compute-r1",
            signal_registry_version="signals-r1",
            model_version="model-r1",
            outcome_context={"validator": "phase34c", "window": "after"},
        ),
        RoutingOutcomeEvent(
            workspace_id=workspace_id,
            case_id=str(after_case["id"]),
            outcome_type="reopened",
            occurred_at=executed_at + timedelta(days=1, hours=2),
            assigned_to="alice",
            assigned_team="research",
            root_cause_code="version_regression",
            severity="high",
            watchlist_id=watchlist_id,
            compute_version="compute-r1",
            signal_registry_version="signals-r1",
            model_version="model-r1",
            outcome_context={"validator": "phase34c", "window": "after"},
        ),
        RoutingOutcomeEvent(
            workspace_id=workspace_id,
            case_id=str(after_case["id"]),
            outcome_type="escalated",
            occurred_at=executed_at + timedelta(days=1, hours=3),
            assigned_to="alice",
            assigned_team="research",
            root_cause_code="version_regression",
            severity="high",
            watchlist_id=watchlist_id,
            compute_version="compute-r1",
            signal_registry_version="signals-r1",
            model_version="model-r1",
            outcome_context={"validator": "phase34c", "window": "after"},
        ),
        RoutingOutcomeEvent(
            workspace_id=workspace_id,
            case_id=str(after_case["id"]),
            outcome_type="reassigned",
            occurred_at=executed_at + timedelta(days=1, hours=4),
            assigned_to="alice",
            assigned_team="research",
            root_cause_code="version_regression",
            severity="high",
            watchlist_id=watchlist_id,
            compute_version="compute-r1",
            signal_registry_version="signals-r1",
            model_version="model-r1",
            outcome_context={"validator": "phase34c", "window": "after"},
        ),
        RoutingOutcomeEvent(
            workspace_id=workspace_id,
            case_id=str(after_case["id"]),
            outcome_type="time_to_resolve_hours",
            occurred_at=executed_at + timedelta(days=1, hours=5),
            assigned_to="alice",
            assigned_team="research",
            root_cause_code="version_regression",
            severity="high",
            watchlist_id=watchlist_id,
            compute_version="compute-r1",
            signal_registry_version="signals-r1",
            model_version="model-r1",
            outcome_value=8.0,
            outcome_context={"validator": "phase34c", "window": "after"},
        ),
    ]

    for event in before_events + after_events:
        repo.insert_governance_routing_outcome(conn, event=event)


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase34c-{now.strftime('%Y%m%d%H%M%S')}"
    watchlist_slug = "promotion-impact-core"
    workspace_id, watchlist_id, profile_id = _create_workspace_and_watchlist(workspace_slug, watchlist_slug)

    try:
        with get_connection() as conn:
            threshold_executed_at = now - timedelta(days=3)
            routing_executed_at = now - timedelta(days=2)

            _seed_threshold_impact(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                profile_id=profile_id,
                executed_at=threshold_executed_at,
            )
            _seed_routing_impact(
                conn,
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                executed_at=routing_executed_at,
            )

            service = PromotionImpactService(repo)
            refresh = service.refresh_recent_impacts(conn, workspace_id=workspace_id, limit=25)
            threshold_rows = repo.list_threshold_promotion_impact_summary(conn, workspace_id=workspace_id)
            routing_rows = repo.list_routing_promotion_impact_summary(conn, workspace_id=workspace_id)
            rollback_rows = repo.list_promotion_rollback_risk_summary(conn, workspace_id=workspace_id)
            conn.commit()

        print(f"workspace_id={refresh.workspace_id}")
        print(f"impact_snapshot_rows={refresh.refreshed_count}")
        print(f"threshold_impact_rows={len(threshold_rows)}")
        print(f"routing_impact_rows={len(routing_rows)}")
        print(f"rollback_risk_rows={len(rollback_rows)}")
        print(f"threshold_impact={threshold_rows[0]['impact_classification'] if threshold_rows else None}")
        print(f"routing_impact={routing_rows[0]['impact_classification'] if routing_rows else None}")
        print(f"detail_contract_ok={str(bool(threshold_rows and routing_rows and rollback_rows)).lower()}")
    finally:
        _cleanup_workspace(workspace_id)


if __name__ == "__main__":
    main()
