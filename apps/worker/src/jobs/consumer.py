import logging
import time
from dataclasses import asdict
from datetime import datetime, timezone

from src.alerts.fanout import emit_alert
from src.alerts.policy_engine import build_job_event_payload
from src.config import get_settings
from src.db import repositories as repo
from src.db.client import get_connection
from src.db.repositories import (
    append_governance_case_event,
    append_governance_case_evidence,
    append_governance_case_note,
    append_governance_incident_timeline_event,
    claim_next_job,
    complete_job,
    create_case_recurrence_link,
    create_governance_assignment,
    create_governance_routing_autopromotion_execution,
    create_governance_routing_autopromotion_rollback_candidate,
    create_governance_reassignment_event,
    create_governance_routing_feedback,
    create_governance_routing_decision,
    create_threshold_promotion_execution,
    create_threshold_rollback_candidate,
    get_recent_threshold_promotion_execution,
    get_threshold_autopromotion_policy,
    list_threshold_autopromotion_summary,
    list_threshold_review_summary,
    ensure_default_governance_escalation_policies,
    ensure_default_governance_sla_policies,
    evaluate_alert_policies_db,
    get_active_governance_degradation_state,
    get_active_governance_muting_rules,
    get_governance_alert_rules,
    get_case_recurrence_summary,
    get_governance_case_sla_summary_row,
    get_governance_case_lifecycle_row,
    get_governance_case_summary_latest,
    get_governance_case_summary_row,
    get_governance_degradation_states,
    get_governance_escalation_state,
    get_latest_governance_assignment,
    get_governance_routing_autopromotion_candidate,
    get_governance_routing_autopromotion_policy,
    get_active_regime_threshold_row,
    get_governance_stability_row,
    get_governance_version_health_row,
    get_governance_version_regime_row,
    get_governance_version_replay_row,
    get_family_history_for_runs,
    get_prior_successful_run_drift_context,
    get_recent_regime_stability_metrics,
    get_recent_replay_consistency_metrics,
    get_recent_successful_run_contexts,
    get_run_compute_scope,
    get_run_drift_context,
    get_run_family_attribution_rows,
    get_run_input_snapshot_payload,
    get_run_replay_delta_context,
    get_run_signal_attribution_rows,
    has_matching_alert_policy_db,
    heartbeat_worker,
    insert_governance_degradation_state_members,
    insert_governance_alert_events,
    insert_governance_recovery_event,
    insert_governance_resolution_action,
    insert_governance_escalation_event,
    insert_governance_threshold_application,
    insert_governance_threshold_feedback,
    list_governance_routing_autopromotion_summary,
    list_governance_case_evidence,
    list_governance_case_notes,
    list_governance_escalation_policies,
    list_governance_threshold_learning_summary,
    list_governance_threshold_performance_summary,
    list_operator_workload_pressure,
    list_governance_sla_policies,
    list_team_workload_pressure,
    load_latest_market_state,
    load_watchlist_asset_symbols,
    mark_job_running,
    persist_governance_degradation_state,
    persist_compute_scope,
    replace_run_drift_metrics,
    replace_regime_transition_family_shifts,
    replace_run_stage_timings,
    replace_run_attributions,
    replace_governance_threshold_recommendations,
    resolve_governance_degradation_state,
    resolve_governance_case_for_state,
    persist_stability_metrics,
    schedule_job_retry_db,
    apply_threshold_promotion,
    create_threshold_recommendation_review,
    update_threshold_recommendation_status,
    get_threshold_promotion_proposal,
    update_threshold_promotion_proposal,
    upsert_threshold_promotion_proposal,
    update_run_forensics,
    update_run_drift_summary,
    update_run_lineage,
    upsert_governance_sla_evaluation,
    upsert_governance_anomaly_clusters,
    upsert_threshold_autopromotion_policy,
    upsert_governance_case,
    upsert_governance_case_summary,
    upsert_governance_routing_autopromotion_policy,
    upsert_governance_escalation_state,
    update_case_recurrence,
    find_recent_related_case,
    upsert_regime_transition_event,
    upsert_replay_delta,
    upsert_composite_scores,
    upsert_feature_values,
    upsert_run_explanation,
    upsert_run_input_snapshot,
    upsert_signal_values,
)
from src.features.feature_service import compute_feature_rows
from src.jobs.retry_policy import (
    classify_retry_outcome,
    retry_scheduled_message,
    terminal_failure_message,
)
from src.services.lineage import ComputeLineage
from src.services.attribution_service import (
    build_run_attributions,
    serialize_family_attributions,
    serialize_signal_attributions,
)
from src.services.anomaly_clustering_service import build_cluster_candidates
from src.services.drift_service import build_run_drift, serialize_drift_metrics
from src.services.governance_alert_service import evaluate_governance_alerts
from src.services.governance_ack_service import find_matching_mute_rule
from src.services.assignment_routing_service import AssignmentRoutingService, RoutingInput
from src.services.assignment_routing_feedback_service import classify_routing_feedback
from src.services.case_management_service import build_case_seed
from src.services.case_recurrence_service import CaseRecurrenceService
from src.services.case_summary_service import CaseSummaryService
from src.services.escalation_service import EscalationPolicy, EscalationService
from src.services.governance_degradation_service import (
    GovernanceDegradationService,
    build_degradation_signals,
)
from src.services.governance_recovery_service import GovernanceRecoveryService
from src.services.incident_analytics_service import IncidentAnalyticsService
from src.services.incident_performance_service import IncidentPerformanceService
from src.services.incident_timeline_service import (
    build_assignment_event,
    build_case_recurring_detected_event,
    build_case_escalated_event,
    build_case_reopened_event,
    build_escalation_cleared_event,
    build_escalation_repeated_event,
    build_case_opened_event,
    build_case_resolved_event,
    build_state_status_changed_event,
)
from src.services.manager_analytics_service import ManagerAnalyticsService
from src.services.promotion_impact_service import PromotionImpactService
from src.services.routing_optimization_service import RoutingOptimizationService
from src.services.routing_autopromotion_service import RoutingAutopromotionService
from src.services.routing_policy_autopromotion_service import RoutingPolicyAutopromotionService
from src.services.routing_policy_rollback_impact_service import RoutingPolicyRollbackImpactService
from src.services.governance_policy_optimization_service import GovernancePolicyOptimizationService
from src.services.governance_policy_autopromotion_service import GovernancePolicyAutopromotionService
from src.services.dependency_context_service import DependencyContextService
from src.services.cross_asset_signal_service import CrossAssetSignalService
from src.services.cross_asset_explanation_service import CrossAssetExplanationService
from src.services.cross_asset_attribution_service import CrossAssetAttributionService
from src.services.dependency_priority_weighting_service import DependencyPriorityWeightingService
from src.services.regime_aware_cross_asset_service import RegimeAwareCrossAssetService
from src.services.cross_asset_replay_validation_service import CrossAssetReplayValidationService
from src.services.cross_asset_timing_service import CrossAssetTimingService
from src.services.cross_asset_timing_attribution_service import CrossAssetTimingAttributionService
from src.services.cross_asset_timing_composite_service import CrossAssetTimingCompositeService
from src.services.cross_asset_timing_replay_validation_service import CrossAssetTimingReplayValidationService
from src.services.cross_asset_transition_diagnostics_service import CrossAssetTransitionDiagnosticsService
from src.services.cross_asset_transition_attribution_service import CrossAssetTransitionAttributionService
from src.services.cross_asset_transition_composite_service import CrossAssetTransitionCompositeService
from src.services.cross_asset_transition_replay_validation_service import CrossAssetTransitionReplayValidationService
from src.services.cross_asset_pattern_service import CrossAssetPatternService
from src.services.cross_asset_archetype_attribution_service import CrossAssetArchetypeAttributionService
from src.services.cross_asset_archetype_composite_service import CrossAssetArchetypeCompositeService
from src.services.cross_asset_archetype_replay_validation_service import CrossAssetArchetypeReplayValidationService
from src.services.cross_asset_pattern_cluster_service import CrossAssetPatternClusterService
from src.services.cross_asset_cluster_attribution_service import CrossAssetClusterAttributionService
from src.services.cross_asset_cluster_composite_service import CrossAssetClusterCompositeService
from src.services.cross_asset_cluster_replay_validation_service import CrossAssetClusterReplayValidationService
from src.services.cross_asset_persistence_service import CrossAssetPersistenceService
from src.services.cross_asset_persistence_attribution_service import CrossAssetPersistenceAttributionService
from src.services.cross_asset_persistence_composite_service import CrossAssetPersistenceCompositeService
from src.services.cross_asset_persistence_replay_validation_service import CrossAssetPersistenceReplayValidationService
from src.services.cross_asset_signal_decay_service import CrossAssetSignalDecayService
from src.services.cross_asset_decay_attribution_service import CrossAssetDecayAttributionService
from src.services.cross_asset_decay_composite_service import CrossAssetDecayCompositeService
from src.services.cross_asset_decay_replay_validation_service import CrossAssetDecayReplayValidationService
from src.services.cross_asset_layer_conflict_service import CrossAssetLayerConflictService
from src.services.cross_asset_conflict_attribution_service import CrossAssetConflictAttributionService
from src.services.cross_asset_conflict_composite_service import CrossAssetConflictCompositeService
from src.services.regime_threshold_service import RegimeThresholdService
from src.services.regime_transition_service import analyze_regime_transition
from src.services.replay_delta_service import build_replay_delta
from src.services.routing_recommendation_service import RoutingRecommendationService
from src.services.routing_outcome_service import RoutingOutcomeService
from src.services.scope_service import resolve_compute_scope
from src.services.stability_service import StabilityInputs, build_stability_payload
from src.services.threshold_learning_service import (
    ThresholdLearningService,
    ThresholdOutcomeContext,
)
from src.services.threshold_review_service import ThresholdReviewService
from src.services.threshold_autopromotion_service import ThresholdAutoPromotionService
from src.services.workload_sla_service import SlaPolicy, WorkloadSlaService
from src.services.run_intelligence import (
    DEFAULT_COMPUTE_VERSION,
    DEFAULT_MODEL_VERSION,
    DEFAULT_SIGNAL_REGISTRY_VERSION,
    EXPLANATION_VERSION,
    RunTelemetry,
    build_input_snapshot,
    build_run_explanation,
    build_version_pins,
    classify_failure_code,
    extract_replay_context,
)
from src.signals.composite_service import compute_composite_rows
from src.signals.signal_service import compute_signal_rows

logger = logging.getLogger(__name__)
RETRY_ALERT_THRESHOLD = 2


def _stage_rows(telemetry: RunTelemetry) -> list[dict]:
    return [asdict(row) for row in telemetry.stage_timings]


def _coerce_utc_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _minutes_since(value) -> int | None:
    ts = _coerce_utc_datetime(value)
    if ts is None:
        return None
    delta = datetime.now(timezone.utc) - ts
    return max(int(delta.total_seconds() // 60), 0)


def _persist_assignment_routing_feedback(
    conn,
    *,
    case_row: dict,
    latest_case_summary: dict | None,
    routing_decision_id: str | None,
    previous_assigned_to: str | None,
    previous_assigned_team: str | None,
    new_assigned_to: str | None,
    new_assigned_team: str | None,
    reason: str | None,
    escalation_active: bool = False,
    policy_changed: bool = False,
    workload_rebalanced: bool = False,
    resolved_without_change: bool = False,
    latest_assignment_at=None,
    metadata: dict | None = None,
) -> dict[str, dict] | None:
    decision = classify_routing_feedback(
        previous_assigned_to=previous_assigned_to,
        previous_assigned_team=previous_assigned_team,
        new_assigned_to=new_assigned_to,
        new_assigned_team=new_assigned_team,
        escalation_active=escalation_active,
        policy_changed=policy_changed,
        workload_rebalanced=workload_rebalanced,
        resolved_without_change=resolved_without_change,
        reason=reason,
    )
    feedback_row = create_governance_routing_feedback(
        conn,
        workspace_id=str(case_row["workspace_id"]),
        case_id=str(case_row["id"]),
        routing_decision_id=routing_decision_id,
        feedback_type=decision.feedback_type,
        assigned_to=decision.assigned_to,
        assigned_team=decision.assigned_team,
        prior_assigned_to=decision.prior_assigned_to,
        prior_assigned_team=decision.prior_assigned_team,
        root_cause_code=(
            str(latest_case_summary["root_cause_code"])
            if latest_case_summary and latest_case_summary.get("root_cause_code")
            else None
        ),
        severity=str(case_row.get("severity")) if case_row.get("severity") else None,
        recurrence_group_id=(
            str(case_row["recurrence_group_id"])
            if case_row.get("recurrence_group_id")
            else None
        ),
        repeat_count=int(case_row.get("repeat_count") or 1),
        reason=decision.reason,
        metadata={
            **decision.metadata,
            **(metadata or {}),
        },
    )

    reassignment_row = None
    if decision.reassignment_type:
        reassignment_row = create_governance_reassignment_event(
            conn,
            workspace_id=str(case_row["workspace_id"]),
            case_id=str(case_row["id"]),
            routing_decision_id=routing_decision_id,
            previous_assigned_to=previous_assigned_to,
            previous_assigned_team=previous_assigned_team,
            new_assigned_to=new_assigned_to,
            new_assigned_team=new_assigned_team,
            reassignment_type=decision.reassignment_type,
            reassignment_reason=decision.reason,
            minutes_since_open=_minutes_since(case_row.get("opened_at")),
            minutes_since_last_assignment=_minutes_since(latest_assignment_at),
            metadata={
                **decision.metadata,
                **(metadata or {}),
            },
        )

    return {
        "feedback": feedback_row,
        "reassignment": reassignment_row,
    }


def _build_degradation_trailing_metrics(
    *,
    state: dict,
    latest_stability_row: dict | None,
    version_health_row: dict | None,
    version_replay_row: dict | None,
    version_regime_row: dict | None,
) -> dict:
    degradation_type = str(state.get("degradation_type") or "unknown")
    metrics: dict[str, object] = {
        "captured_at": time.time(),
        "degradation_type": degradation_type,
    }
    if degradation_type in {"family_instability_spike", "stability_classification_downgrade", "regime_instability_spike"}:
        metrics["latest_stability"] = latest_stability_row or {}
    if degradation_type == "version_regression":
        metrics["version_health"] = version_health_row or {}
    if degradation_type == "replay_degradation":
        metrics["version_replay"] = version_replay_row or {}
    if degradation_type == "regime_conflict_persistence":
        metrics["version_regime"] = version_regime_row or {}
    return metrics


def _filter_muted_governance_candidates(candidates, muting_rules):
    kept = []
    for candidate in candidates:
        muted_by = find_matching_mute_rule(candidate.__dict__, muting_rules)
        if muted_by:
            logger.info(
                "muted governance candidate event_type=%s target_type=%s target_key=%s",
                candidate.event_type,
                muted_by.get("target_type"),
                muted_by.get("target_key"),
            )
            continue
        kept.append(candidate)
    return kept


def _attach_case_evidence(
    conn,
    *,
    case_id: str,
    workspace_id: str,
    degradation_state_id: str,
    run_id: str,
    version_tuple: str | None,
    members: tuple | list,
) -> None:
    append_governance_case_evidence(
        conn,
        case_id=case_id,
        workspace_id=workspace_id,
        evidence_type="degradation_state",
        reference_id=degradation_state_id,
        title="Degradation state",
        summary="Persistent degradation state driving the case lifecycle.",
        payload={"source": "worker"},
    )
    append_governance_case_evidence(
        conn,
        case_id=case_id,
        workspace_id=workspace_id,
        evidence_type="run",
        reference_id=run_id,
        title="Triggering run",
        summary="Latest run contributing to the active governance case.",
        payload={"source": "worker"},
    )
    if version_tuple:
        append_governance_case_evidence(
            conn,
            case_id=case_id,
            workspace_id=workspace_id,
            evidence_type="version_tuple",
            reference_id=version_tuple,
            title="Version tuple",
            summary="Compute, signal registry, and model versions linked to the case.",
            payload={"source": "worker"},
        )
    for member in members:
        if member.governance_alert_event_id:
            append_governance_case_evidence(
                conn,
                case_id=case_id,
                workspace_id=workspace_id,
                evidence_type="governance_alert_event",
                reference_id=str(member.governance_alert_event_id),
                title="Governance alert event",
                summary="Alert event contributing evidence to the degradation case.",
                payload=member.metadata or {},
            )
        if member.anomaly_cluster_id:
            append_governance_case_evidence(
                conn,
                case_id=case_id,
                workspace_id=workspace_id,
                evidence_type="anomaly_cluster",
                reference_id=str(member.anomaly_cluster_id),
                title="Anomaly cluster",
                summary="Cluster linked to the case as durable anomaly evidence.",
                payload=member.metadata or {},
            )


def _build_routing_outcome_snapshot(
    *,
    case_row: dict,
    latest_case_summary: dict | None = None,
    routing_decision_id: str | None = None,
    assignment_id: str | None = None,
    assigned_to: str | None = None,
    assigned_team: str | None = None,
) -> dict:
    snapshot = dict(case_row)
    if latest_case_summary and latest_case_summary.get("root_cause_code"):
        snapshot["root_cause_code"] = latest_case_summary.get("root_cause_code")
    if routing_decision_id is not None:
        snapshot["routing_decision_id"] = routing_decision_id
    if assignment_id is not None:
        snapshot["assignment_id"] = assignment_id
    if assigned_to is not None or "assigned_to" not in snapshot:
        snapshot["assigned_to"] = assigned_to
    if assigned_team is not None or "assigned_team" not in snapshot:
        snapshot["assigned_team"] = assigned_team
    return snapshot


def _refresh_case_summary(
    conn,
    *,
    case_id: str,
    summary_service: CaseSummaryService,
) -> dict | None:
    case_row = get_governance_case_summary_row(conn, case_id=case_id)
    if not case_row:
        return None

    notes = list_governance_case_notes(conn, case_id=case_id)
    evidence = list_governance_case_evidence(conn, case_id=case_id)
    recurrence = get_case_recurrence_summary(conn, case_id=case_id)
    lifecycle = get_governance_case_lifecycle_row(
        conn,
        degradation_state_id=str(case_row["degradation_state_id"]) if case_row.get("degradation_state_id") else None,
    )
    summary = summary_service.build_summary(
        case_row=case_row,
        notes=notes,
        evidence=evidence,
        recurrence=recurrence,
        lifecycle=lifecycle,
    )
    return upsert_governance_case_summary(
        conn,
        workspace_id=str(case_row["workspace_id"]),
        case_id=case_id,
        summary_version="v1",
        status_summary=summary.status_summary,
        root_cause_code=summary.root_cause_code,
        root_cause_confidence=summary.root_cause_confidence,
        root_cause_summary=summary.root_cause_summary,
        evidence_summary=summary.evidence_summary,
        recurrence_summary=summary.recurrence_summary,
        operator_summary=summary.operator_summary,
        closure_summary=summary.closure_summary,
        recommended_next_action=summary.recommended_next_action,
        source_note_ids=summary.source_note_ids,
        source_evidence_ids=summary.source_evidence_ids,
        metadata=summary.metadata,
    )


def _refresh_case_sla_evaluation(
    conn,
    *,
    case_id: str,
    sla_service: WorkloadSlaService,
) -> dict | None:
    case_row = get_governance_case_summary_row(conn, case_id=case_id)
    if not case_row:
        return None

    workspace_id = str(case_row["workspace_id"])
    ensure_default_governance_sla_policies(conn, workspace_id=workspace_id)
    policy_rows = list_governance_sla_policies(conn, workspace_id=workspace_id)
    policies = [
        SlaPolicy(
            id=str(row["id"]),
            severity=str(row["severity"]),
            chronicity_class=str(row["chronicity_class"]) if row.get("chronicity_class") else None,
            ack_within_minutes=int(row["ack_within_minutes"]),
            resolve_within_minutes=int(row["resolve_within_minutes"]),
        )
        for row in policy_rows
    ]
    chronicity_class = sla_service.derive_chronicity_class(case_row)
    policy = sla_service.choose_policy(
        severity=str(case_row["severity"]),
        chronicity_class=chronicity_class,
        policies=policies,
    )
    evaluation = sla_service.evaluate_case(case_row=case_row, policy=policy)
    return upsert_governance_sla_evaluation(
        conn,
        workspace_id=workspace_id,
        case_id=case_id,
        policy_id=evaluation.policy_id,
        chronicity_class=evaluation.chronicity_class,
        ack_due_at=evaluation.ack_due_at,
        resolve_due_at=evaluation.resolve_due_at,
        ack_breached=evaluation.ack_breached,
        resolve_breached=evaluation.resolve_breached,
        breach_severity=evaluation.breach_severity,
        metadata=evaluation.metadata,
    )


def _refresh_case_routing_recommendation(
    conn,
    *,
    case_id: str,
    recommendation_service: RoutingRecommendationService,
) -> dict | None:
    base_case_row = get_governance_case_summary_row(conn, case_id=case_id)
    if not base_case_row:
        return None

    case_row = repo.get_case_for_routing_recommendation(
        conn,
        workspace_id=str(base_case_row["workspace_id"]),
        case_id=case_id,
    )
    if not case_row:
        return None

    candidate_rows = repo.list_routing_recommendation_candidates(
        conn,
        workspace_id=str(case_row["workspace_id"]),
        case_row=case_row,
    )
    if not candidate_rows:
        return None

    recommendation = recommendation_service.recommend(
        case_row=case_row,
        candidate_rows=candidate_rows,
    )
    return repo.upsert_governance_routing_recommendation(
        conn,
        workspace_id=str(case_row["workspace_id"]),
        case_id=case_id,
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


def _evaluate_routing_autopromotions(
    conn,
    *,
    workspace_id: str,
    recommendation_id: str,
    routing_autopromotion_service: RoutingAutopromotionService,
) -> dict | None:
    candidate = get_governance_routing_autopromotion_candidate(
        conn,
        recommendation_id=recommendation_id,
    )
    if not candidate:
        return None

    policy = get_governance_routing_autopromotion_policy(
        conn,
        workspace_id=workspace_id,
        recommendation=candidate,
    )
    if not policy:
        return None

    preview = routing_autopromotion_service.evaluate(candidate, policy, True)
    if not preview.should_execute:
        return None

    recent_execution = repo.get_recent_governance_routing_autopromotion_execution(
        conn,
        workspace_id=workspace_id,
        target_type=preview.target_type,
        target_key=preview.target_key,
        since_hours=int(policy.get("cooldown_hours") or 0),
    )
    decision = routing_autopromotion_service.evaluate(
        candidate,
        policy,
        cooldown_ok=recent_execution is None,
    )
    if not decision.should_execute:
        return None

    apply_result = repo.apply_governance_routing_autopromotion(
        conn,
        workspace_id=workspace_id,
        policy=policy,
        recommendation=candidate,
        target_type=decision.target_type,
        target_key=decision.target_key,
        executed_by="worker_autopromotion",
    )
    execution_row = create_governance_routing_autopromotion_execution(
        conn,
        workspace_id=workspace_id,
        policy_id=str(policy["id"]),
        recommendation_id=recommendation_id,
        target_type=decision.target_type,
        target_key=decision.target_key,
        recommended_user=candidate.get("recommended_user"),
        recommended_team=candidate.get("recommended_team"),
        confidence=str(candidate.get("confidence") or "low"),
        acceptance_rate=float(candidate.get("acceptance_rate") or 0.0),
        sample_size=int(candidate.get("sample_size") or 0),
        override_rate=float(candidate.get("override_rate") or 0.0),
        execution_status="executed",
        execution_reason=decision.execution_reason,
        cooldown_bucket=f"{decision.target_key}:{datetime.now(timezone.utc).strftime('%Y%m%d%H')}",
        prior_state=apply_result["prior_state"],
        new_state=apply_result["new_state"],
        metadata={
            "target_row_id": str(apply_result["target_row"].get("id")),
            "scope_type": policy.get("scope_type"),
        },
    )
    rollback_row = create_governance_routing_autopromotion_rollback_candidate(
        conn,
        workspace_id=workspace_id,
        execution_id=str(execution_row["id"]),
        target_type=decision.target_type,
        target_key=decision.target_key,
        prior_state=apply_result["prior_state"],
        rollback_reason=decision.rollback_reason,
    )

    case_id = candidate.get("case_id")
    if case_id:
        append_governance_case_event(
            conn,
            case_id=str(case_id),
            workspace_id=workspace_id,
            event_type="routing_autopromotion_executed",
            actor="worker_autopromotion",
            payload={
                "execution_id": str(execution_row["id"]),
                "rollback_candidate_id": str(rollback_row["id"]),
                "policy_id": str(policy["id"]),
                "target_type": decision.target_type,
                "target_key": decision.target_key,
                "recommended_user": candidate.get("recommended_user"),
                "recommended_team": candidate.get("recommended_team"),
            },
        )
        append_governance_incident_timeline_event(
            conn,
            case_id=str(case_id),
            workspace_id=workspace_id,
            event_type="routing_autopromotion_executed",
            event_source="routing_autopromotion",
            title=f"Routing autopromotion executed for {decision.target_key}",
            detail=decision.execution_reason,
            actor="worker_autopromotion",
            event_at=datetime.now(timezone.utc),
            metadata={
                "execution_id": str(execution_row["id"]),
                "rollback_candidate_id": str(rollback_row["id"]),
                "policy_id": str(policy["id"]),
                "target_type": decision.target_type,
                "target_key": decision.target_key,
            },
            source_table="governance_routing_autopromotion_executions",
            source_id=str(execution_row["id"]),
        )

    return execution_row


def _evaluate_routing_policy_autopromotions(
    conn,
    *,
    workspace_id: str,
    routing_policy_autopromotion_service: "RoutingPolicyAutopromotionService",
) -> None:
    """Evaluate all active autopromotion policies for workspace, promote eligible recommendations."""
    from src.db import repositories as repo

    try:
        policies = repo.list_routing_policy_autopromotion_policies(conn, workspace_id=workspace_id)
        if not policies:
            return

        eligibility_rows = repo.get_routing_policy_autopromotion_eligibility(conn, workspace_id=workspace_id)

        for row in eligibility_rows:
            recommendation_key = row["recommendation_key"]
            scope_type = row["scope_type"]
            scope_value = row["scope_value"]
            policy_id = str(row["policy_id"])

            policy = routing_policy_autopromotion_service.select_policy_for_recommendation(
                policies=policies,
                scope_type=scope_type,
                scope_value=scope_value,
            )
            if not policy:
                continue

            last_exec = repo.get_latest_routing_policy_autopromotion_execution(
                conn, workspace_id=workspace_id, recommendation_key=recommendation_key, outcome="promoted"
            )
            last_promoted_hours_ago: float | None = None
            if last_exec:
                import datetime
                delta = datetime.datetime.now(datetime.timezone.utc) - last_exec["executed_at"].replace(
                    tzinfo=datetime.timezone.utc
                )
                last_promoted_hours_ago = delta.total_seconds() / 3600.0

            eligibility = routing_policy_autopromotion_service.evaluate_autopromotion_eligibility(
                policy=policy,
                recommendation=row,
                approved_review_count=int(row.get("approved_review_count") or 0),
                application_count=int(row.get("application_count") or 0),
                last_promoted_at_hours_ago=last_promoted_hours_ago,
            )

            prior_policy = {}
            recommended_policy = {}
            routing_row_id = None
            routing_table = None
            proposal_id = None
            application_id = None

            if eligibility.eligible:
                try:
                    rec_rows = [
                        r for r in repo.get_routing_policy_optimization_snapshot(conn, workspace_id=workspace_id)
                        if r.get("recommendation_key") == recommendation_key
                    ]
                    rec = rec_rows[0] if rec_rows else {}
                    recommended_policy = rec.get("recommended_policy") or {}
                    prior_policy = rec.get("current_policy") or {}
                except Exception:
                    recommended_policy = {}
                    prior_policy = {}

                promotion_target = policy.get("promotion_target", "rule")
                if promotion_target == "override":
                    routing_row = repo.apply_routing_override_from_recommendation(
                        conn,
                        workspace_id=workspace_id,
                        scope_type=scope_type,
                        scope_value=scope_value,
                        recommended_policy=recommended_policy,
                        applied_by="worker_autopromotion",
                    )
                    routing_row_id = str(routing_row["id"])
                    routing_table = "governance_routing_overrides"
                else:
                    routing_row = repo.apply_routing_rule_from_recommendation(
                        conn,
                        workspace_id=workspace_id,
                        scope_type=scope_type,
                        scope_value=scope_value,
                        recommended_policy=recommended_policy,
                        applied_by="worker_autopromotion",
                    )
                    routing_row_id = str(routing_row["id"])
                    routing_table = "governance_routing_rules"

            decision = routing_policy_autopromotion_service.build_autopromotion_decision(
                eligibility=eligibility,
                prior_policy=prior_policy,
                recommended_policy=recommended_policy,
                policy_id=policy_id,
                recommendation_key=recommendation_key,
            )

            execution_row = repo.insert_routing_policy_autopromotion_execution(
                conn,
                workspace_id=workspace_id,
                policy_id=policy_id,
                recommendation_key=recommendation_key,
                outcome=decision.outcome,
                proposal_id=proposal_id,
                application_id=application_id,
                blocked_reason=decision.blocked_reason,
                skipped_reason=decision.skipped_reason,
                executed_by="worker_autopromotion",
                prior_policy=decision.prior_policy,
                applied_policy=decision.applied_policy,
                metadata=decision.proposal_metadata,
            )

            if decision.outcome == "promoted":
                repo.insert_routing_policy_autopromotion_rollback_candidate(
                    conn,
                    workspace_id=workspace_id,
                    execution_id=str(execution_row["id"]),
                    recommendation_key=recommendation_key,
                    scope_type=scope_type,
                    scope_value=scope_value,
                    prior_policy=decision.prior_policy,
                    applied_policy=decision.applied_policy,
                    routing_row_id=routing_row_id,
                    routing_table=routing_table,
                )

        conn.commit()

    except Exception:
        logger.exception("routing_policy_autopromotion evaluation failed workspace=%s", workspace_id)


def _evaluate_routing_policy_rollback_impact(
    conn,
    *,
    workspace_id: str,
    routing_policy_rollback_impact_service: "RoutingPolicyRollbackImpactService",
) -> None:
    """Compute and persist impact snapshots for resolved rollback candidates lacking one."""
    from src.db import repositories as repo
    import datetime

    try:
        executions = repo.list_recent_routing_policy_rollback_executions(
            conn, workspace_id=workspace_id, limit=20
        )
        if not executions:
            return

        pending_evals = repo.get_pending_routing_policy_rollback_evaluation_summary(
            conn, workspace_id=workspace_id
        )
        # index pending by execution id — only evaluate executions that need a snapshot
        needs_snapshot = {
            str(p["rollback_execution_id"])
            for p in pending_evals
            if not p.get("has_impact_snapshot") and p.get("sufficient_post_data")
        }

        for execution in executions:
            exec_id = str(execution["id"])
            if exec_id not in needs_snapshot:
                continue

            candidate_id = str(execution["rollback_candidate_id"])
            candidate = repo.get_routing_policy_rollback_candidate(
                conn, workspace_id=workspace_id, rollback_candidate_id=candidate_id
            )
            if not candidate:
                continue

            executed_at = execution.get("executed_at")
            days_since = None
            if executed_at:
                delta = datetime.datetime.now(datetime.timezone.utc) - executed_at.replace(
                    tzinfo=datetime.timezone.utc
                )
                days_since = delta.total_seconds() / 86400.0

            # Collect before/after metrics from routing feedback
            ba = repo.get_before_after_metrics_for_rollback(
                conn,
                workspace_id=workspace_id,
                scope_type=execution.get("scope_type", ""),
                scope_value=execution.get("scope_value", ""),
                rollback_executed_at=executed_at,
                window_days=30,
            )
            before_metrics = routing_policy_rollback_impact_service.collect_before_metrics(
                rollback_candidate=candidate,
                analytics_snapshot=ba.get("before"),
            )
            after_metrics = routing_policy_rollback_impact_service.collect_after_metrics(
                analytics_snapshot=ba.get("after"),
            )

            snapshot = routing_policy_rollback_impact_service.build_impact_snapshot(
                rollback_execution=execution,
                rollback_candidate=candidate,
                before_metrics=before_metrics,
                after_metrics=after_metrics,
                days_since_rollback=days_since,
            )

            repo.insert_routing_policy_rollback_impact_snapshot(
                conn,
                workspace_id=workspace_id,
                rollback_execution_id=exec_id,
                rollback_candidate_id=candidate_id,
                recommendation_key=snapshot.recommendation_key,
                scope_type=snapshot.scope_type,
                scope_value=snapshot.scope_value,
                target_type=snapshot.target_type,
                evaluation_window_label=snapshot.evaluation_window_label,
                impact_classification=snapshot.impact_classification,
                before_metrics=snapshot.before_metrics,
                after_metrics=snapshot.after_metrics,
                delta_metrics=snapshot.delta_metrics,
                metadata=snapshot.metadata,
            )

        conn.commit()

    except Exception:
        logger.exception("rollback impact evaluation failed workspace=%s", workspace_id)


def _refresh_case_escalation(
    conn,
    *,
    case_id: str,
    escalation_service: EscalationService,
    routing_outcome_service: RoutingOutcomeService,
) -> dict | None:
    case_row = get_governance_case_summary_row(conn, case_id=case_id)
    if not case_row:
        return None

    workspace_id = str(case_row["workspace_id"])
    ensure_default_governance_escalation_policies(conn, workspace_id=workspace_id)
    policy_rows = list_governance_escalation_policies(conn, workspace_id=workspace_id)
    policies = [
        EscalationPolicy(
            id=str(row["id"]),
            severity=str(row["severity"]) if row.get("severity") else None,
            chronicity_class=str(row["chronicity_class"]) if row.get("chronicity_class") else None,
            root_cause_code=str(row["root_cause_code"]) if row.get("root_cause_code") else None,
            min_case_age_minutes=int(row["min_case_age_minutes"]) if row.get("min_case_age_minutes") is not None else None,
            min_ack_age_minutes=int(row["min_ack_age_minutes"]) if row.get("min_ack_age_minutes") is not None else None,
            min_repeat_count=int(row["min_repeat_count"]) if row.get("min_repeat_count") is not None else None,
            min_operator_pressure=float(row["min_operator_pressure"]) if row.get("min_operator_pressure") is not None else None,
            escalation_level=str(row["escalation_level"]),
            escalate_to_team=str(row["escalate_to_team"]) if row.get("escalate_to_team") else None,
            escalate_to_user=str(row["escalate_to_user"]) if row.get("escalate_to_user") else None,
            cooldown_minutes=int(row.get("cooldown_minutes") or 240),
            metadata=dict(row.get("metadata") or {}),
        )
        for row in policy_rows
    ]
    case_summary_latest = get_governance_case_summary_latest(conn, case_id=case_id)
    sla_row = get_governance_case_sla_summary_row(conn, case_id=case_id)
    operator_pressure_row = next(
        (
            row
            for row in list_operator_workload_pressure(conn, workspace_id)
            if row.get("assigned_to") == case_row.get("current_assignee")
        ),
        None,
    )
    team_pressure_row = next(
        (
            row
            for row in list_team_workload_pressure(conn, workspace_id)
            if row.get("assigned_team") == case_row.get("current_team")
        ),
        None,
    )
    context = escalation_service.build_context(
        case_row=case_row,
        case_summary_latest=case_summary_latest,
        sla_row=sla_row,
        operator_pressure_row=operator_pressure_row,
        team_pressure_row=team_pressure_row,
    )
    current_state = get_governance_escalation_state(conn, case_id=case_id)
    decision = escalation_service.evaluate(
        context=context,
        policies=policies,
        current_state=current_state,
    )
    if not decision.should_escalate and not decision.clear_existing:
        return None

    if decision.clear_existing:
        state_row = upsert_governance_escalation_state(
            conn,
            workspace_id=workspace_id,
            case_id=case_id,
            escalation_level=decision.escalation_level or str(current_state.get("escalation_level") or "cleared"),
            status=decision.status or "cleared",
            escalated_to_team=decision.escalated_to_team,
            escalated_to_user=decision.escalated_to_user,
            reason=decision.reason,
            source_policy_id=decision.policy_id,
            escalated_at=current_state.get("escalated_at") if current_state else context.now,
            last_evaluated_at=context.now,
            repeated_count=decision.repeated_count or int(current_state.get("repeated_count") or 1),
            cleared_at=context.now,
            metadata=decision.metadata,
        )
        event_row = insert_governance_escalation_event(
            conn,
            workspace_id=workspace_id,
            case_id=case_id,
            escalation_state_id=str(state_row["id"]),
            event_type=decision.event_type or "escalation_cleared",
            escalation_level=decision.escalation_level,
            escalated_to_team=decision.escalated_to_team,
            escalated_to_user=decision.escalated_to_user,
            reason=decision.reason,
            source_policy_id=decision.policy_id,
            metadata=decision.metadata,
        )
        append_governance_case_event(
            conn,
            case_id=case_id,
            workspace_id=workspace_id,
            event_type="escalation_cleared",
            actor="worker",
            payload={
                "escalation_level": decision.escalation_level,
                "reason": decision.reason,
                "escalation_event_id": str(event_row["id"]),
            },
        )
        timeline_event = build_escalation_cleared_event(
            case_id=case_id,
            workspace_id=workspace_id,
            escalation_level=decision.escalation_level,
            reason=decision.reason,
            escalation_event_id=str(event_row["id"]),
        )
        append_governance_incident_timeline_event(
            conn,
            case_id=timeline_event.case_id,
            workspace_id=timeline_event.workspace_id,
            event_type=timeline_event.event_type,
            event_source=timeline_event.event_source,
            title=timeline_event.title,
            detail=timeline_event.detail,
            actor=timeline_event.actor,
            event_at=timeline_event.event_at,
            metadata=timeline_event.metadata,
            source_table=timeline_event.source_table,
            source_id=timeline_event.source_id,
        )
        return {"state": state_row, "event": event_row, "decision": decision}

    escalated_at = (
        current_state.get("escalated_at")
        if current_state and decision.event_type == "escalation_repeated"
        else context.now
    )
    state_row = upsert_governance_escalation_state(
        conn,
        workspace_id=workspace_id,
        case_id=case_id,
        escalation_level=decision.escalation_level or "active",
        status=decision.status or "active",
        escalated_to_team=decision.escalated_to_team,
        escalated_to_user=decision.escalated_to_user,
        reason=decision.reason,
        source_policy_id=decision.policy_id,
        escalated_at=escalated_at,
        last_evaluated_at=context.now,
        repeated_count=decision.repeated_count or 1,
        cleared_at=None,
        metadata=decision.metadata,
    )
    event_row = insert_governance_escalation_event(
        conn,
        workspace_id=workspace_id,
        case_id=case_id,
        escalation_state_id=str(state_row["id"]),
        event_type=decision.event_type or "case_escalated",
        escalation_level=decision.escalation_level,
        escalated_to_team=decision.escalated_to_team,
        escalated_to_user=decision.escalated_to_user,
        reason=decision.reason,
        source_policy_id=decision.policy_id,
        metadata=decision.metadata,
    )
    append_governance_case_event(
        conn,
        case_id=case_id,
        workspace_id=workspace_id,
        event_type=decision.event_type or "case_escalated",
        actor="worker",
        payload={
            "escalation_level": decision.escalation_level,
            "escalated_to_team": decision.escalated_to_team,
            "escalated_to_user": decision.escalated_to_user,
            "reason": decision.reason,
            "source_policy_id": decision.policy_id,
            "escalation_event_id": str(event_row["id"]),
            "repeated_count": decision.repeated_count,
        },
    )
    timeline_event = (
        build_escalation_repeated_event(
            case_id=case_id,
            workspace_id=workspace_id,
            escalation_level=decision.escalation_level or "active",
            repeated_count=decision.repeated_count or 1,
            reason=decision.reason,
            escalation_event_id=str(event_row["id"]),
        )
        if decision.event_type == "escalation_repeated"
        else build_case_escalated_event(
            case_id=case_id,
            workspace_id=workspace_id,
            escalation_level=decision.escalation_level or "active",
            escalated_to_team=decision.escalated_to_team,
            escalated_to_user=decision.escalated_to_user,
            reason=decision.reason,
            escalation_event_id=str(event_row["id"]),
        )
    )
    append_governance_incident_timeline_event(
        conn,
        case_id=timeline_event.case_id,
        workspace_id=timeline_event.workspace_id,
        event_type=timeline_event.event_type,
        event_source=timeline_event.event_source,
        title=timeline_event.title,
        detail=timeline_event.detail,
        actor=timeline_event.actor,
        event_at=timeline_event.event_at,
        metadata=timeline_event.metadata,
        source_table=timeline_event.source_table,
        source_id=timeline_event.source_id,
    )
    routing_outcome_service.record_escalation(
        conn,
        _build_routing_outcome_snapshot(
            case_row=case_row,
            latest_case_summary=case_summary_latest,
        ),
        level=decision.escalation_level,
        reason=decision.reason,
    )

    target_user = decision.escalated_to_user or None
    target_team = decision.escalated_to_team or None
    if target_user or target_team:
        if target_user != case_row.get("current_assignee") or target_team != case_row.get("current_team"):
            latest_assignment = get_latest_governance_assignment(conn, case_id=case_id)
            assignment_row = create_governance_assignment(
                conn,
                case_id=case_id,
                workspace_id=workspace_id,
                assigned_to=target_user,
                assigned_team=target_team,
                assigned_by="worker",
                reason=decision.reason,
                metadata={
                    "escalation_event_id": str(event_row["id"]),
                    "source_policy_id": decision.policy_id,
                    "escalation_level": decision.escalation_level,
                },
            )
            routing_outcome_service.record_reassignment(
                conn,
                _build_routing_outcome_snapshot(
                    case_row=case_row,
                    latest_case_summary=case_summary_latest,
                    assignment_id=str(assignment_row["id"]),
                    assigned_to=target_user,
                    assigned_team=target_team,
                ),
                decision.reason,
            )
            _persist_assignment_routing_feedback(
                conn,
                case_row=case_row,
                latest_case_summary=case_summary_latest,
                routing_decision_id=None,
                previous_assigned_to=str(case_row.get("current_assignee")) if case_row.get("current_assignee") else None,
                previous_assigned_team=str(case_row.get("current_team")) if case_row.get("current_team") else None,
                new_assigned_to=target_user,
                new_assigned_team=target_team,
                reason=decision.reason,
                escalation_active=True,
                latest_assignment_at=latest_assignment.get("assigned_at") if latest_assignment else None,
                metadata={
                    "source_policy_id": decision.policy_id,
                    "escalation_event_id": str(event_row["id"]),
                    "assignment_id": str(assignment_row["id"]),
                },
            )
            assignment_event = build_assignment_event(
                case_id=case_id,
                workspace_id=workspace_id,
                assigned_to=target_user,
                assigned_team=target_team,
                reason=decision.reason,
                actor="worker",
                assignment_id=str(assignment_row["id"]),
            )
            append_governance_incident_timeline_event(
                conn,
                case_id=assignment_event.case_id,
                workspace_id=assignment_event.workspace_id,
                event_type=assignment_event.event_type,
                event_source=assignment_event.event_source,
                title=assignment_event.title,
                detail=assignment_event.detail,
                actor=assignment_event.actor,
                event_at=assignment_event.event_at,
                metadata=assignment_event.metadata,
                source_table=assignment_event.source_table,
                source_id=assignment_event.source_id,
            )

    return {"state": state_row, "event": event_row, "decision": decision}


def _threshold_value_for_event(
    *,
    active_threshold_row: dict | None,
    event_type: str,
) -> float | None:
    if not active_threshold_row:
        return None

    if event_type == "version_regression":
        value = active_threshold_row.get("version_health_floor")
    elif event_type in {"family_instability_spike", "stability_classification_downgrade"}:
        value = active_threshold_row.get("family_instability_ceiling")
    elif event_type == "replay_degradation":
        value = active_threshold_row.get("replay_consistency_floor")
    elif event_type == "regime_conflict_persistence":
        value = active_threshold_row.get("conflicting_transition_ceiling")
    elif event_type == "regime_instability_spike":
        value = active_threshold_row.get("regime_instability_ceiling")
    else:
        value = None

    return float(value) if value is not None else None


def _enrich_threshold_performance_with_current_values(
    conn,
    *,
    workspace_id: str,
    performance: list[dict],
) -> list[dict]:
    enriched: list[dict] = []
    for row in performance:
        active_row = get_active_regime_threshold_row(
            conn,
            workspace_id,
            row.get("regime"),
        )
        enriched.append(
            {
                **row,
                "current_value": _threshold_value_for_event(
                    active_threshold_row=active_row,
                    event_type=str(row["event_type"]),
                ),
            }
        )
    return enriched


def _sync_threshold_review_state(
    conn,
    *,
    workspace_id: str,
    recommendations: list[dict],
    threshold_review_service: ThresholdReviewService,
) -> list[dict]:
    proposals: list[dict] = []
    for recommendation in recommendations:
        proposal_draft = threshold_review_service.build_promotion_proposal(recommendation)
        if proposal_draft is None:
            continue
        proposal_row = upsert_threshold_promotion_proposal(
            conn,
            workspace_id=workspace_id,
            recommendation_id=proposal_draft.recommendation_id,
            profile_id=proposal_draft.profile_id,
            event_type=proposal_draft.event_type,
            dimension_type=proposal_draft.dimension_type,
            dimension_value=proposal_draft.dimension_value,
            current_value=proposal_draft.current_value,
            proposed_value=proposal_draft.proposed_value,
            source_metrics=proposal_draft.source_metrics,
            metadata=proposal_draft.metadata,
        )
        proposals.append(proposal_row)
    return proposals


def _evaluate_threshold_autopromotions(
    conn,
    *,
    workspace_id: str,
    threshold_autopromotion_service: ThresholdAutoPromotionService,
) -> list[dict]:
    executed: list[dict] = []
    proposals = [
        row
        for row in list_threshold_review_summary(conn, workspace_id=workspace_id)
        if row.get("status") == "approved"
    ]
    for proposal in proposals:
        policy = get_threshold_autopromotion_policy(
            conn,
            workspace_id=workspace_id,
            profile_id=str(proposal["profile_id"]) if proposal.get("profile_id") else None,
            event_type=str(proposal["event_type"]),
            dimension_type=str(proposal["dimension_type"]),
            dimension_value=(
                str(proposal["dimension_value"])
                if proposal.get("dimension_value") is not None
                else None
            ),
        )
        if not policy:
            continue

        recent_execution = get_recent_threshold_promotion_execution(
            conn,
            workspace_id=workspace_id,
            profile_id=str(proposal["profile_id"]),
            event_type=str(proposal["event_type"]),
            dimension_type=str(proposal["dimension_type"]),
            dimension_value=(
                str(proposal["dimension_value"])
                if proposal.get("dimension_value") is not None
                else None
            ),
            since_hours=int(policy.get("cooldown_hours") or 0),
        )
        if recent_execution:
            continue

        eligibility = threshold_autopromotion_service.evaluate(
            proposal=proposal,
            policy=policy,
        )
        if not eligibility.eligible:
            continue

        apply_threshold_promotion(
            conn,
            workspace_id=workspace_id,
            profile_id=str(proposal["profile_id"]),
            event_type=str(proposal["event_type"]),
            dimension_type=str(proposal["dimension_type"]),
            dimension_value=(
                str(proposal["dimension_value"])
                if proposal.get("dimension_value") is not None
                else None
            ),
            new_value=float(eligibility.new_value or proposal["proposed_value"]),
        )
        execution_row = create_threshold_promotion_execution(
            conn,
            workspace_id=workspace_id,
            proposal_id=str(proposal["proposal_id"]),
            profile_id=str(proposal["profile_id"]),
            event_type=str(proposal["event_type"]),
            dimension_type=str(proposal["dimension_type"]),
            dimension_value=(
                str(proposal["dimension_value"])
                if proposal.get("dimension_value") is not None
                else None
            ),
            previous_value=float(eligibility.previous_value or proposal["current_value"]),
            new_value=float(eligibility.new_value or proposal["proposed_value"]),
            executed_by="worker_autopromotion",
            execution_mode="automatic",
            rationale="policy_guardrailed_autopromotion",
            metadata={
                "policy_id": str(policy["id"]),
                "eligibility": eligibility.metadata or {},
            },
        )
        create_threshold_rollback_candidate(
            conn,
            workspace_id=workspace_id,
            execution_id=str(execution_row["id"]),
            profile_id=str(proposal["profile_id"]),
            rollback_to_value=float(eligibility.previous_value or proposal["current_value"]),
            reason="monitor_post_autopromotion_outcome",
            metadata={"proposal_id": str(proposal["proposal_id"])},
        )
        update_threshold_promotion_proposal(
            conn,
            proposal_id=str(proposal["proposal_id"]),
            status="executed",
            metadata={
                **(proposal.get("metadata") or {}),
                "auto_execution_id": str(execution_row["id"]),
                "policy_id": str(policy["id"]),
            },
        )
        update_threshold_recommendation_status(
            conn,
            recommendation_id=str(proposal["recommendation_id"]),
            status="accepted",
        )
        executed.append(execution_row)
    return executed


def _refresh_case_threshold_learning(
    conn,
    *,
    case_id: str,
    workspace_id: str,
    event_type: str,
    regime: str | None,
    threshold_learning_service: ThresholdLearningService,
    threshold_review_service: ThresholdReviewService,
    threshold_autopromotion_service: ThresholdAutoPromotionService,
    muted: bool = False,
) -> dict | None:
    case_row = get_governance_case_summary_row(conn, case_id=case_id)
    if not case_row:
        return None

    active_threshold_row = get_active_regime_threshold_row(conn, workspace_id, regime)
    escalation_state = get_governance_escalation_state(conn, case_id=case_id)
    latest_case_summary = get_governance_case_summary_latest(conn, case_id=case_id)
    version_tuple = str(case_row["version_tuple"]) if case_row.get("version_tuple") else None
    version_parts = version_tuple.split("|") if version_tuple else []
    threshold_context = ThresholdOutcomeContext(
        workspace_id=workspace_id,
        watchlist_id=str(case_row["watchlist_id"]) if case_row.get("watchlist_id") else None,
        threshold_profile_id=(
            str(active_threshold_row["profile_id"])
            if active_threshold_row and active_threshold_row.get("profile_id")
            else None
        ),
        event_type=event_type,
        regime=regime,
        compute_version=version_parts[0] if len(version_parts) > 0 else None,
        signal_registry_version=version_parts[1] if len(version_parts) > 1 else None,
        model_version=version_parts[2] if len(version_parts) > 2 else None,
        case_id=case_id,
        degradation_state_id=(
            str(case_row["degradation_state_id"])
            if case_row.get("degradation_state_id")
            else None
        ),
        threshold_applied_value=_threshold_value_for_event(
            active_threshold_row=active_threshold_row,
            event_type=event_type,
        ),
        acknowledged=bool(case_row.get("acknowledged_at")),
        muted=muted,
        escalated=bool(escalation_state and escalation_state.get("status") == "active"),
        resolved=bool(case_row.get("resolved_at") or case_row.get("closed_at")),
        reopened=bool(case_row.get("reopened_from_case_id")) or int(case_row.get("repeat_count") or 1) > 1,
        evidence={
            "case_status": case_row.get("status"),
            "severity": case_row.get("severity"),
            "repeat_count": int(case_row.get("repeat_count") or 1),
            "root_cause_code": (
                latest_case_summary.get("root_cause_code")
                if latest_case_summary
                else None
            ),
            "recommended_next_action": (
                latest_case_summary.get("recommended_next_action")
                if latest_case_summary
                else None
            ),
        },
    )
    feedback_row = insert_governance_threshold_feedback(
        conn,
        threshold_learning_service.build_feedback_row(threshold_context),
    )
    performance = list_governance_threshold_performance_summary(
        conn,
        workspace_id=workspace_id,
    )
    enriched_performance = _enrich_threshold_performance_with_current_values(
        conn,
        workspace_id=workspace_id,
        performance=performance,
    )
    recommendations = threshold_learning_service.build_recommendations(enriched_performance)
    replace_governance_threshold_recommendations(
        conn,
        workspace_id=workspace_id,
        rows=recommendations,
    )
    live_recommendations = list_governance_threshold_learning_summary(
        conn,
        workspace_id=workspace_id,
    )
    _sync_threshold_review_state(
        conn,
        workspace_id=workspace_id,
        recommendations=live_recommendations,
        threshold_review_service=threshold_review_service,
    )
    _evaluate_threshold_autopromotions(
        conn,
        workspace_id=workspace_id,
        threshold_autopromotion_service=threshold_autopromotion_service,
    )
    list_threshold_review_summary(conn, workspace_id=workspace_id)
    list_threshold_autopromotion_summary(conn, workspace_id=workspace_id)
    return feedback_row


def run_forever() -> None:
    settings = get_settings()
    threshold_service = RegimeThresholdService()
    degradation_service = GovernanceDegradationService()
    recovery_action_service = GovernanceRecoveryService()
    recurrence_service = CaseRecurrenceService()
    case_summary_service = CaseSummaryService()
    workload_sla_service = WorkloadSlaService()
    escalation_service = EscalationService()
    threshold_learning_service = ThresholdLearningService()
    threshold_review_service = ThresholdReviewService()
    threshold_autopromotion_service = ThresholdAutoPromotionService()
    routing_recommendation_service = RoutingRecommendationService()
    routing_autopromotion_service = RoutingAutopromotionService()
    routing_outcome_service = RoutingOutcomeService(repo)
    assignment_routing_service = AssignmentRoutingService(repo)
    incident_analytics_service = IncidentAnalyticsService(repo)
    incident_performance_service = IncidentPerformanceService(repo)
    promotion_impact_service = PromotionImpactService(repo)
    manager_analytics_service = ManagerAnalyticsService(repo)
    routing_optimization_service = RoutingOptimizationService(repo)
    routing_policy_autopromotion_service = RoutingPolicyAutopromotionService()
    routing_policy_rollback_impact_service = RoutingPolicyRollbackImpactService()
    governance_policy_optimization_service = GovernancePolicyOptimizationService()
    governance_policy_autopromotion_service = GovernancePolicyAutopromotionService()
    dependency_context_service = DependencyContextService()
    cross_asset_signal_service = CrossAssetSignalService()
    cross_asset_explanation_service = CrossAssetExplanationService()
    cross_asset_attribution_service = CrossAssetAttributionService()
    dependency_priority_weighting_service = DependencyPriorityWeightingService()
    regime_aware_cross_asset_service = RegimeAwareCrossAssetService()
    cross_asset_replay_validation_service = CrossAssetReplayValidationService()
    cross_asset_timing_service = CrossAssetTimingService()
    cross_asset_timing_attribution_service = CrossAssetTimingAttributionService()
    cross_asset_timing_composite_service = CrossAssetTimingCompositeService()
    cross_asset_timing_replay_validation_service = CrossAssetTimingReplayValidationService()
    cross_asset_transition_diagnostics_service = CrossAssetTransitionDiagnosticsService()
    cross_asset_transition_attribution_service = CrossAssetTransitionAttributionService()
    cross_asset_transition_composite_service = CrossAssetTransitionCompositeService()
    cross_asset_transition_replay_validation_service = CrossAssetTransitionReplayValidationService()
    cross_asset_pattern_service = CrossAssetPatternService()
    cross_asset_archetype_attribution_service = CrossAssetArchetypeAttributionService()
    cross_asset_archetype_composite_service = CrossAssetArchetypeCompositeService()
    cross_asset_archetype_replay_validation_service = CrossAssetArchetypeReplayValidationService()
    cross_asset_pattern_cluster_service = CrossAssetPatternClusterService()
    cross_asset_cluster_attribution_service = CrossAssetClusterAttributionService()
    cross_asset_cluster_composite_service = CrossAssetClusterCompositeService()
    cross_asset_cluster_replay_validation_service = CrossAssetClusterReplayValidationService()
    cross_asset_persistence_service = CrossAssetPersistenceService()
    cross_asset_persistence_attribution_service = CrossAssetPersistenceAttributionService()
    cross_asset_persistence_composite_service = CrossAssetPersistenceCompositeService()
    cross_asset_persistence_replay_validation_service = CrossAssetPersistenceReplayValidationService()
    cross_asset_signal_decay_service = CrossAssetSignalDecayService()
    cross_asset_decay_attribution_service = CrossAssetDecayAttributionService()
    cross_asset_decay_composite_service = CrossAssetDecayCompositeService()
    cross_asset_decay_replay_validation_service = CrossAssetDecayReplayValidationService()
    cross_asset_layer_conflict_service = CrossAssetLayerConflictService()
    cross_asset_conflict_attribution_service = CrossAssetConflictAttributionService()
    cross_asset_conflict_composite_service = CrossAssetConflictCompositeService()
    logger.info("starting worker loop worker_id=%s", settings.worker_id)

    while True:
        try:
            with get_connection() as hb_conn:
                heartbeat_worker(hb_conn, settings.worker_id)
                hb_conn.commit()
        except Exception:
            logger.warning("heartbeat failed, continuing")

        with get_connection() as conn:
            job = None
            lineage: ComputeLineage | None = None
            telemetry = RunTelemetry()
            input_snapshot: dict | None = None
            compute_scope_row: dict | None = None
            explanation_payload: dict | None = None
            input_snapshot_id: int | None = None
            signal_attribution_rows: list[dict] = []
            family_attribution_rows: list[dict] = []
            attribution_reconciliation: dict | None = None
            drift_metrics_rows: list[dict] = []
            drift_envelope: dict | None = None
            replay_delta_payload: dict | None = None
            regime_transition_payload: dict | None = None
            stability_payload: dict | None = None
            governance_alert_rows: list[dict] = []
            anomaly_cluster_rows: list[dict] = []
            threshold_application_rows: list[dict] = []
            governance_degradation_rows: list[dict] = []
            governance_recovery_rows: list[dict] = []
            governance_case_rows: list[dict] = []
            try:
                job = claim_next_job(conn, settings.worker_id)
                if not job:
                    conn.rollback()
                    time.sleep(settings.worker_poll_seconds)
                    continue

                job_id = str(job["job_id"])
                queue_id = int(job["queue_id"])
                workspace_id = str(job["workspace_id"])
                watchlist_id = str(job["watchlist_id"]) if job.get("watchlist_id") else None
                payload = job.get("payload") or {}
                replay_context = extract_replay_context(payload if isinstance(payload, dict) else {})
                version_pins = build_version_pins(
                    replay_context.compute_version or DEFAULT_COMPUTE_VERSION,
                    replay_context.signal_registry_version or DEFAULT_SIGNAL_REGISTRY_VERSION,
                    replay_context.model_version or DEFAULT_MODEL_VERSION,
                )

                lineage = ComputeLineage.start(
                    compute_version=version_pins["compute_version"],
                    signal_registry_version=version_pins["signal_registry_version"],
                    model_version=version_pins["model_version"],
                    pipeline_name="recompute",
                    source_window=replay_context.replay_as_of_ts or "latest",
                )

                mark_job_running(conn, job_id)
                update_run_lineage(
                    conn,
                    job_id,
                    queue_id,
                    lineage.as_payload(),
                    status="running",
                )

                with telemetry.track_stage("load_inputs") as stage_meta:
                    source_scope = (
                        get_run_compute_scope(conn, replay_context.source_run_id)
                        if replay_context.source_run_id
                        else None
                    )
                    watchlist_assets = (
                        load_watchlist_asset_symbols(conn, watchlist_id)
                        if watchlist_id and not source_scope
                        else []
                    )
                    resolved_scope = resolve_compute_scope(
                        watchlist_assets,
                        source_scope=source_scope,
                    )
                    compute_scope_row = persist_compute_scope(
                        conn,
                        run_id=job_id,
                        workspace_id=workspace_id,
                        watchlist_id=watchlist_id,
                        queue_name=str(job["queue_name"]),
                        scope_version=resolved_scope.scope_version,
                        primary_assets=resolved_scope.primary_assets,
                        dependency_assets=resolved_scope.dependency_assets,
                        asset_universe=resolved_scope.asset_universe,
                        dependency_policy=resolved_scope.dependency_policy,
                        scope_hash=resolved_scope.scope_hash,
                        metadata={
                            "reused_source_scope": replay_context.source_run_id is not None and source_scope is not None,
                            "source_scope_run_id": replay_context.source_run_id,
                            "watchlist_asset_count": len(watchlist_assets),
                        },
                    )
                    market_states = load_latest_market_state(
                        conn,
                        workspace_id,
                        replay_context.replay_as_of_ts,
                        asset_symbols=resolved_scope.asset_universe,
                    )
                    if not market_states:
                        raise RuntimeError("No normalized market state available for recompute")
                    input_snapshot = build_input_snapshot(
                        market_states,
                        version_pins,
                        replay_context=replay_context,
                        compute_scope=compute_scope_row,
                    )
                    stage_meta["asset_count"] = len(market_states)
                    stage_meta["replay_as_of_ts"] = replay_context.replay_as_of_ts
                    stage_meta["scope_hash"] = compute_scope_row["scope_hash"] if compute_scope_row else None
                    stage_meta["primary_assets"] = compute_scope_row["primary_assets"] if compute_scope_row else []
                    stage_meta["dependency_assets"] = compute_scope_row["dependency_assets"] if compute_scope_row else []
                    stage_meta["asset_universe_count"] = compute_scope_row["asset_universe_count"] if compute_scope_row else 0

                with telemetry.track_stage("build_features") as stage_meta:
                    feature_rows = compute_feature_rows(workspace_id, market_states)
                    stage_meta["feature_count"] = len(feature_rows)

                with telemetry.track_stage("build_signals") as stage_meta:
                    signal_rows = compute_signal_rows(workspace_id, market_states)
                    stage_meta["signal_count"] = len(signal_rows)

                with telemetry.track_stage("build_composite") as stage_meta:
                    composite_rows = compute_composite_rows(workspace_id, market_states, signal_rows)
                    explanation_payload = build_run_explanation(market_states, signal_rows, composite_rows)
                    signal_attributions, family_attributions, attribution_reconciliation = build_run_attributions(
                        market_states,
                        signal_rows,
                        composite_rows,
                    )
                    signal_attribution_rows = serialize_signal_attributions(signal_attributions)
                    family_attribution_rows = serialize_family_attributions(family_attributions)
                    stage_meta["composite_count"] = len(composite_rows)
                    stage_meta["dominant_regime"] = (
                        explanation_payload.get("regime_summary", {}).get("regime_counts", {})
                    )
                    stage_meta["attribution_reconciliation_delta"] = (
                        attribution_reconciliation["reconciliation_delta"] if attribution_reconciliation else None
                    )
                    stage_meta["attribution_reconciled"] = (
                        attribution_reconciliation["reconciled"] if attribution_reconciliation else None
                    )

                with telemetry.track_stage("persist_outputs") as stage_meta:
                    latest_stability_row = None
                    version_health_row = None
                    version_replay_row = None
                    version_regime_row = None
                    upsert_feature_values(conn, workspace_id, feature_rows)
                    upsert_signal_values(conn, workspace_id, signal_rows)
                    upsert_composite_scores(conn, workspace_id, composite_rows)
                    if attribution_reconciliation is not None:
                        replace_run_attributions(
                            conn,
                            job_id,
                            workspace_id,
                            watchlist_id,
                            signal_attribution_rows,
                            family_attribution_rows,
                            attribution_version=attribution_reconciliation["attribution_version"],
                            attribution_total=attribution_reconciliation["attribution_total"],
                            attribution_target_total=attribution_reconciliation["composite_target_total"],
                            attribution_reconciliation_delta=attribution_reconciliation["reconciliation_delta"],
                            attribution_reconciled=attribution_reconciliation["reconciled"],
                        )
                    if input_snapshot:
                        input_snapshot_id = upsert_run_input_snapshot(
                            conn,
                            job_id,
                            workspace_id,
                            watchlist_id,
                            input_snapshot,
                            compute_scope_id=str(compute_scope_row["id"]) if compute_scope_row else None,
                        )
                    if explanation_payload:
                        upsert_run_explanation(
                            conn,
                            job_id,
                            workspace_id,
                            watchlist_id,
                            explanation_payload,
                            EXPLANATION_VERSION,
                        )
                    current_run_context = get_run_drift_context(conn, job_id)
                    comparison_run_context = get_prior_successful_run_drift_context(
                        conn,
                        workspace_id,
                        watchlist_id,
                        str(job["queue_name"]),
                        job_id,
                    )
                    if current_run_context:
                        current_family_rows = get_run_family_attribution_rows(conn, job_id)
                        comparison_family_rows = (
                            get_run_family_attribution_rows(conn, str(comparison_run_context["id"]))
                            if comparison_run_context
                            else []
                        )
                        current_signal_rows = get_run_signal_attribution_rows(conn, job_id)
                        comparison_signal_rows = (
                            get_run_signal_attribution_rows(conn, str(comparison_run_context["id"]))
                            if comparison_run_context
                            else []
                        )
                        drift_metrics, drift_envelope = build_run_drift(
                            current_run_context,
                            comparison_run_context,
                            current_family_rows,
                            comparison_family_rows,
                            current_signal_rows,
                            comparison_signal_rows,
                        )
                        drift_metrics_rows = serialize_drift_metrics(drift_metrics)
                        replace_run_drift_metrics(
                            conn,
                            job_id,
                            workspace_id,
                            watchlist_id,
                            str(comparison_run_context["id"]) if comparison_run_context else None,
                            drift_metrics_rows,
                        )
                        update_run_drift_summary(
                            conn,
                            job_id,
                            str(comparison_run_context["id"]) if comparison_run_context else None,
                            drift_envelope["severity"],
                            drift_envelope["summary"],
                        )
                        regime_transition = analyze_regime_transition(
                            current_run=current_run_context,
                            prior_run=comparison_run_context,
                            current_family_rows=current_family_rows,
                            prior_family_rows=comparison_family_rows,
                        )
                        regime_transition_payload = regime_transition.__dict__
                        transition_event_id = upsert_regime_transition_event(conn, regime_transition_payload)
                        replace_regime_transition_family_shifts(
                            conn,
                            transition_event_id,
                            regime_transition_payload,
                        )
                        if not replay_context.source_run_id:
                            recent_runs = get_recent_successful_run_contexts(
                                conn,
                                workspace_id,
                                watchlist_id,
                                str(job["queue_name"]),
                                job_id,
                            )
                            family_history = get_family_history_for_runs(
                                conn,
                                [str(row["id"]) for row in recent_runs],
                            )
                            replay_metrics = get_recent_replay_consistency_metrics(
                                conn,
                                workspace_id,
                                watchlist_id,
                                str(job["queue_name"]),
                                job_id,
                            )
                            regime_metrics = get_recent_regime_stability_metrics(
                                conn,
                                workspace_id,
                                watchlist_id,
                                str(job["queue_name"]),
                                job_id,
                            )
                            stability_payload = build_stability_payload(
                                StabilityInputs(
                                    run_id=job_id,
                                    workspace_id=workspace_id,
                                    watchlist_id=watchlist_id,
                                    queue_name=str(job["queue_name"]),
                                    composite_current=(
                                        float(current_run_context["composite_score"])
                                        if current_run_context.get("composite_score") is not None
                                        else None
                                    ),
                                    composite_history=[
                                        float(row["composite_score"])
                                        for row in recent_runs
                                        if row.get("composite_score") is not None
                                    ],
                                    family_current={
                                        str(row["signal_family"]): float(row["family_score"])
                                        for row in current_family_rows
                                        if row.get("family_score") is not None
                                    },
                                    family_history=family_history,
                                    replay_runs_considered=int(replay_metrics.get("runs_considered") or 0),
                                    replay_mismatch_rate=(
                                        float(replay_metrics["mismatch_rate"])
                                        if replay_metrics.get("mismatch_rate") is not None
                                        else None
                                    ),
                                    replay_avg_input_match_score=(
                                        float(replay_metrics["avg_input_match_score"])
                                        if replay_metrics.get("avg_input_match_score") is not None
                                        else None
                                    ),
                                    replay_avg_composite_delta_abs=(
                                        float(replay_metrics["avg_composite_delta_abs"])
                                        if replay_metrics.get("avg_composite_delta_abs") is not None
                                        else None
                                    ),
                                    regime_transitions_considered=int(regime_metrics.get("transitions_considered") or 0),
                                    regime_conflicting_transition_count=int(regime_metrics.get("conflicting_transition_count") or 0),
                                    regime_abrupt_transition_count=int(regime_metrics.get("abrupt_transition_count") or 0),
                                    regime_changed=bool(regime_transition_payload.get("transition_detected")),
                                    dominant_regime=regime_transition_payload.get("to_regime"),
                                )
                            )
                            persist_stability_metrics(conn, stability_payload)
                            latest_stability_row = get_governance_stability_row(conn, job_id)
                            version_health_row = get_governance_version_health_row(
                                conn,
                                workspace_id,
                                version_pins["compute_version"],
                                version_pins["signal_registry_version"],
                                version_pins["model_version"],
                            )
                            version_regime_row = get_governance_version_regime_row(
                                conn,
                                workspace_id,
                                version_pins["compute_version"],
                                version_pins["signal_registry_version"],
                                version_pins["model_version"],
                            )
                            active_regime = (
                                (latest_stability_row or {}).get("dominant_regime")
                                or regime_transition_payload.get("to_regime")
                                or "default"
                            )
                            threshold_row = get_active_regime_threshold_row(conn, workspace_id, active_regime)
                            threshold_selection = threshold_service.select_thresholds(active_regime, threshold_row)
                            threshold_application_rows.append(
                                insert_governance_threshold_application(
                                    conn,
                                    threshold_selection.to_application_payload(
                                        run_id=job_id,
                                        workspace_id=workspace_id,
                                        watchlist_id=watchlist_id,
                                        evaluation_stage="stability",
                                        metadata={"queue_name": str(job["queue_name"])},
                                    ),
                                )
                            )
                            governance_rules = threshold_service.apply_thresholds_to_rules(
                                get_governance_alert_rules(conn, workspace_id),
                                threshold_selection,
                            )
                            governance_candidates = evaluate_governance_alerts(
                                workspace_id=workspace_id,
                                rules=governance_rules,
                                latest_stability_row=latest_stability_row,
                                version_health_row=version_health_row,
                                version_regime_row=version_regime_row,
                            )
                            governance_candidates = _filter_muted_governance_candidates(
                                governance_candidates,
                                get_active_governance_muting_rules(conn, workspace_id),
                            )
                            inserted_governance_rows = insert_governance_alert_events(
                                conn,
                                [candidate.__dict__ for candidate in governance_candidates],
                            )
                            governance_alert_rows.extend(inserted_governance_rows)
                            anomaly_cluster_rows.extend(
                                upsert_governance_anomaly_clusters(
                                    conn,
                                    [candidate.__dict__ for candidate in build_cluster_candidates(inserted_governance_rows)],
                                )
                            )
                    if replay_context.source_run_id:
                        replay_run_context = get_run_replay_delta_context(conn, job_id)
                        source_run_context = get_run_replay_delta_context(conn, replay_context.source_run_id)
                        if replay_run_context and source_run_context:
                            replay_delta = build_replay_delta(
                                replay_run=replay_run_context,
                                source_run=source_run_context,
                                replay_snapshot=get_run_input_snapshot_payload(conn, job_id),
                                source_snapshot=get_run_input_snapshot_payload(conn, replay_context.source_run_id),
                                replay_signal_attributions=get_run_signal_attribution_rows(conn, job_id, limit=None),
                                source_signal_attributions=get_run_signal_attribution_rows(conn, replay_context.source_run_id, limit=None),
                                replay_family_attributions=get_run_family_attribution_rows(conn, job_id),
                                source_family_attributions=get_run_family_attribution_rows(conn, replay_context.source_run_id),
                            )
                            replay_delta_payload = replay_delta.__dict__
                            upsert_replay_delta(conn, replay_delta_payload)
                            version_replay_row = get_governance_version_replay_row(
                                conn,
                                workspace_id,
                                version_pins["compute_version"],
                                version_pins["signal_registry_version"],
                                version_pins["model_version"],
                            )
                            replay_threshold_selection = threshold_service.select_thresholds(
                                (stability_payload or {}).get("baseline", {}).get("dominant_regime"),
                                get_active_regime_threshold_row(
                                    conn,
                                    workspace_id,
                                    (stability_payload or {}).get("baseline", {}).get("dominant_regime"),
                                ),
                            )
                            threshold_application_rows.append(
                                insert_governance_threshold_application(
                                    conn,
                                    replay_threshold_selection.to_application_payload(
                                        run_id=job_id,
                                        workspace_id=workspace_id,
                                        watchlist_id=watchlist_id,
                                        evaluation_stage="replay",
                                        metadata={"queue_name": str(job["queue_name"])},
                                    ),
                                )
                            )
                            governance_rules = threshold_service.apply_thresholds_to_rules(
                                get_governance_alert_rules(conn, workspace_id),
                                replay_threshold_selection,
                            )
                            governance_candidates = evaluate_governance_alerts(
                                workspace_id=workspace_id,
                                rules=governance_rules,
                                version_replay_row=version_replay_row,
                            )
                            governance_candidates = _filter_muted_governance_candidates(
                                governance_candidates,
                                get_active_governance_muting_rules(conn, workspace_id),
                            )
                            inserted_governance_rows = insert_governance_alert_events(
                                conn,
                                [candidate.__dict__ for candidate in governance_candidates],
                            )
                            governance_alert_rows.extend(inserted_governance_rows)
                            anomaly_cluster_rows.extend(
                                upsert_governance_anomaly_clusters(
                                    conn,
                                    [candidate.__dict__ for candidate in build_cluster_candidates(inserted_governance_rows)],
                                )
                            )
                    degradation_signals = build_degradation_signals(governance_alert_rows, anomaly_cluster_rows)
                    touched_state_ids: set[str] = set()
                    for signal in degradation_signals:
                        active_state = get_active_governance_degradation_state(
                            conn,
                            workspace_id=signal.workspace_id,
                            watchlist_id=signal.watchlist_id,
                            degradation_type=signal.degradation_type,
                            version_tuple=signal.version_tuple,
                            regime=signal.regime,
                        )
                        degradation_decision = degradation_service.evaluate_signal(signal, active_state)
                        if not degradation_decision:
                            continue
                        state_row = persist_governance_degradation_state(conn, degradation_decision)
                        touched_state_ids.add(str(state_row["id"]))
                        governance_degradation_rows.append(state_row)
                        insert_governance_degradation_state_members(
                            conn,
                            [
                                member.to_row(
                                    state_id=str(state_row["id"]),
                                    workspace_id=signal.workspace_id,
                                )
                                for member in signal.members
                            ],
                        )
                        case_row = upsert_governance_case(
                            conn,
                            build_case_seed(
                                workspace_id=workspace_id,
                                degradation_state_id=str(state_row["id"]),
                                watchlist_id=signal.watchlist_id,
                                version_tuple=signal.version_tuple,
                                degradation_type=signal.degradation_type,
                                severity=str(state_row["severity"]),
                                source_summary=dict(state_row.get("source_summary") or {}),
                            ).__dict__,
                        )
                        _attach_case_evidence(
                            conn,
                            case_id=str(case_row["id"]),
                            workspace_id=workspace_id,
                            degradation_state_id=str(state_row["id"]),
                            run_id=str(job["id"]),
                            version_tuple=signal.version_tuple,
                            members=signal.members,
                        )
                        if active_state is None:
                            match_basis = recurrence_service.compute_match_basis(
                                workspace_id=workspace_id,
                                watchlist_id=signal.watchlist_id,
                                degradation_family=signal.degradation_type,
                                version_tuple=signal.version_tuple,
                                regime=signal.regime,
                                cluster_count=signal.cluster_count,
                            )
                            prior_case = find_recent_related_case(
                                conn,
                                workspace_id=workspace_id,
                                watchlist_id=signal.watchlist_id,
                                degradation_family=signal.degradation_type,
                                version_tuple=signal.version_tuple,
                                regime=signal.regime,
                            )
                            recurrence = recurrence_service.build_result(
                                prior_case=prior_case,
                                match_basis=match_basis,
                            )
                            if recurrence.matched_case_id and recurrence.recurrence_group_id:
                                if prior_case and not prior_case.get("recurrence_group_id"):
                                    update_case_recurrence(
                                        conn,
                                        case_id=str(prior_case["id"]),
                                        recurrence_group_id=recurrence.recurrence_group_id,
                                        reopened_from_case_id=prior_case.get("reopened_from_case_id"),
                                        repeat_count=int(prior_case.get("repeat_count") or 1),
                                        reopened_at=prior_case.get("reopened_at"),
                                        reopen_reason=prior_case.get("reopen_reason"),
                                        recurrence_match_basis=dict(prior_case.get("recurrence_match_basis") or {}),
                                    )
                                case_row = update_case_recurrence(
                                    conn,
                                    case_id=str(case_row["id"]),
                                    recurrence_group_id=recurrence.recurrence_group_id,
                                    reopened_from_case_id=recurrence.matched_case_id if recurrence.reopen else None,
                                    repeat_count=recurrence.repeat_count,
                                    reopened_at=state_row["last_seen_at"] if recurrence.reopen else None,
                                    reopen_reason=recurrence.reopen_reason,
                                    recurrence_match_basis=recurrence.match_basis,
                                )
                                create_case_recurrence_link(
                                    conn,
                                    workspace_id=workspace_id,
                                    watchlist_id=signal.watchlist_id,
                                    recurrence_group_id=recurrence.recurrence_group_id,
                                    source_case_id=str(case_row["id"]),
                                    matched_case_id=recurrence.matched_case_id,
                                    match_type="reopen" if recurrence.reopen else "recurrence",
                                    match_score=1.0,
                                    matched_within_window=recurrence.reopen,
                                    match_basis=recurrence.match_basis,
                                )
                                append_governance_case_evidence(
                                    conn,
                                    case_id=str(case_row["id"]),
                                    workspace_id=workspace_id,
                                    evidence_type="prior_case",
                                    reference_id=recurrence.matched_case_id,
                                    title="Prior related case",
                                    summary="Most recent related case linked through recurrence matching.",
                                    payload=recurrence.match_basis,
                                )
                                if recurrence.reopen:
                                    routing_outcome_service.record_reopen(
                                        conn,
                                        _build_routing_outcome_snapshot(
                                            case_row=case_row,
                                        ),
                                        recurrence.reopen_reason,
                                    )
                            append_governance_case_event(
                                conn,
                                case_id=str(case_row["id"]),
                                workspace_id=workspace_id,
                                event_type="case_opened",
                                actor="worker",
                                payload={
                                    "degradation_state_id": str(state_row["id"]),
                                    "degradation_type": signal.degradation_type,
                                    "severity": state_row["severity"],
                                },
                            )
                            opened_event = build_case_opened_event(
                                case_id=str(case_row["id"]),
                                workspace_id=workspace_id,
                                severity=str(state_row["severity"]),
                                summary=str(case_row.get("summary") or ""),
                                degradation_state_id=str(state_row["id"]),
                            )
                            append_governance_incident_timeline_event(
                                conn,
                                case_id=opened_event.case_id,
                                workspace_id=opened_event.workspace_id,
                                event_type=opened_event.event_type,
                                event_source=opened_event.event_source,
                                title=opened_event.title,
                                detail=opened_event.detail,
                                actor=opened_event.actor,
                                event_at=opened_event.event_at,
                                metadata=opened_event.metadata,
                                source_table=opened_event.source_table,
                                source_id=opened_event.source_id,
                            )
                            if recurrence.matched_case_id and recurrence.recurrence_group_id:
                                recurrence_event = (
                                    build_case_reopened_event(
                                        case_id=str(case_row["id"]),
                                        workspace_id=workspace_id,
                                        prior_case_id=recurrence.matched_case_id,
                                        recurrence_group_id=recurrence.recurrence_group_id,
                                        repeat_count=recurrence.repeat_count,
                                        reopen_reason=recurrence.reopen_reason,
                                    )
                                    if recurrence.reopen
                                    else build_case_recurring_detected_event(
                                        case_id=str(case_row["id"]),
                                        workspace_id=workspace_id,
                                        prior_case_id=recurrence.matched_case_id,
                                        recurrence_group_id=recurrence.recurrence_group_id,
                                        repeat_count=recurrence.repeat_count,
                                        reopen_reason=recurrence.reopen_reason,
                                        match_basis=recurrence.match_basis,
                                    )
                                )
                                append_governance_case_event(
                                    conn,
                                    case_id=str(case_row["id"]),
                                    workspace_id=workspace_id,
                                    event_type="case_reopened" if recurrence.reopen else "case_related_prior_found",
                                    actor="worker",
                                    payload={
                                        "prior_case_id": recurrence.matched_case_id,
                                        "recurrence_group_id": recurrence.recurrence_group_id,
                                        "repeat_count": recurrence.repeat_count,
                                        "reopen_reason": recurrence.reopen_reason,
                                        "match_basis": recurrence.match_basis,
                                    },
                                )
                                append_governance_incident_timeline_event(
                                    conn,
                                    case_id=recurrence_event.case_id,
                                    workspace_id=recurrence_event.workspace_id,
                                    event_type=recurrence_event.event_type,
                                    event_source=recurrence_event.event_source,
                                    title=recurrence_event.title,
                                    detail=recurrence_event.detail,
                                    actor=recurrence_event.actor,
                                    event_at=recurrence_event.event_at,
                                    metadata=recurrence_event.metadata,
                                    source_table=recurrence_event.source_table,
                                    source_id=recurrence_event.source_id,
                                )
                        elif active_state.get("state_status") != state_row.get("state_status"):
                            append_governance_case_event(
                                conn,
                                case_id=str(case_row["id"]),
                                workspace_id=workspace_id,
                                event_type="state_status_changed",
                                actor="worker",
                                payload={
                                    "from_status": active_state.get("state_status"),
                                    "to_status": state_row.get("state_status"),
                                    "degradation_state_id": str(state_row["id"]),
                                },
                            )
                            status_event = build_state_status_changed_event(
                                case_id=str(case_row["id"]),
                                workspace_id=workspace_id,
                                from_status=active_state.get("state_status"),
                                to_status=state_row.get("state_status"),
                                degradation_state_id=str(state_row["id"]),
                            )
                            append_governance_incident_timeline_event(
                                conn,
                                case_id=status_event.case_id,
                                workspace_id=status_event.workspace_id,
                                event_type=status_event.event_type,
                                event_source=status_event.event_source,
                                title=status_event.title,
                                detail=status_event.detail,
                                actor=status_event.actor,
                                event_at=status_event.event_at,
                                metadata=status_event.metadata,
                                source_table=status_event.source_table,
                                source_id=status_event.source_id,
                            )
                        governance_case_rows.append(case_row)
                        latest_case_summary = _refresh_case_summary(
                            conn,
                            case_id=str(case_row["id"]),
                            summary_service=case_summary_service,
                        )
                        _refresh_case_sla_evaluation(
                            conn,
                            case_id=str(case_row["id"]),
                            sla_service=workload_sla_service,
                        )
                        routing_recommendation_row = _refresh_case_routing_recommendation(
                            conn,
                            case_id=str(case_row["id"]),
                            recommendation_service=routing_recommendation_service,
                        )
                        if routing_recommendation_row:
                            _evaluate_routing_autopromotions(
                                conn,
                                workspace_id=workspace_id,
                                recommendation_id=str(routing_recommendation_row["id"]),
                                routing_autopromotion_service=routing_autopromotion_service,
                            )
                        _refresh_case_escalation(
                            conn,
                            case_id=str(case_row["id"]),
                            escalation_service=escalation_service,
                            routing_outcome_service=routing_outcome_service,
                        )
                        _refresh_case_threshold_learning(
                            conn,
                            case_id=str(case_row["id"]),
                            workspace_id=workspace_id,
                            event_type=signal.degradation_type,
                            regime=signal.regime,
                            threshold_learning_service=threshold_learning_service,
                            threshold_review_service=threshold_review_service,
                            threshold_autopromotion_service=threshold_autopromotion_service,
                        )
                        case_row_for_routing = get_governance_case_summary_row(
                            conn,
                            case_id=str(case_row["id"]),
                        ) or case_row
                        if not case_row_for_routing.get("current_assignee") and not case_row_for_routing.get("current_team"):
                            routing_decision = assignment_routing_service.route_case(
                                conn,
                                RoutingInput(
                                    workspace_id=workspace_id,
                                    case_id=str(case_row["id"]),
                                    watchlist_id=str(case_row_for_routing["watchlist_id"]) if case_row_for_routing.get("watchlist_id") else None,
                                    severity=str(case_row_for_routing["severity"]),
                                    root_cause_code=(
                                        str(latest_case_summary["root_cause_code"])
                                        if latest_case_summary and latest_case_summary.get("root_cause_code")
                                        else None
                                    ),
                                    version_tuple=str(case_row_for_routing["version_tuple"]) if case_row_for_routing.get("version_tuple") else None,
                                    regime=str(case_row_for_routing["regime"]) if case_row_for_routing.get("regime") else None,
                                    repeat_count=int(case_row_for_routing.get("repeat_count") or 1),
                                    chronic=bool(int(case_row_for_routing.get("repeat_count") or 1) > 1 or state_row.get("state_status") == "escalated"),
                                ),
                            )
                            if routing_decision.assigned_user or routing_decision.assigned_team:
                                decision_row = create_governance_routing_decision(
                                    conn,
                                    workspace_id=workspace_id,
                                    case_id=str(case_row["id"]),
                                    routing_rule_id=routing_decision.routing_rule_id,
                                    override_id=routing_decision.override_id,
                                    assigned_team=routing_decision.assigned_team,
                                    assigned_user=routing_decision.assigned_user,
                                    routing_reason=routing_decision.routing_reason,
                                    workload_snapshot=routing_decision.workload_snapshot,
                                    metadata={
                                        "degradation_state_id": str(state_row["id"]),
                                        **routing_decision.metadata,
                                    },
                                )
                                assignment_row = create_governance_assignment(
                                    conn,
                                    case_id=str(case_row["id"]),
                                    workspace_id=workspace_id,
                                    assigned_to=routing_decision.assigned_user,
                                    assigned_team=routing_decision.assigned_team,
                                    assigned_by="worker",
                                    reason=routing_decision.routing_reason,
                                    metadata={
                                        "degradation_state_id": str(state_row["id"]),
                                        "routing_decision_id": str(decision_row["id"]),
                                        "routing_rule_id": routing_decision.routing_rule_id,
                                        "routing_override_id": routing_decision.override_id,
                                    },
                                )
                                routing_outcome_service.record_assignment(
                                    conn,
                                    _build_routing_outcome_snapshot(
                                        case_row=case_row_for_routing,
                                        latest_case_summary=latest_case_summary,
                                        routing_decision_id=str(decision_row["id"]),
                                        assignment_id=str(assignment_row["id"]),
                                        assigned_to=routing_decision.assigned_user,
                                        assigned_team=routing_decision.assigned_team,
                                    ),
                                )
                                if routing_recommendation_row:
                                    recommendation_match = (
                                        routing_recommendation_row.get("recommended_user") == routing_decision.assigned_user
                                        and routing_recommendation_row.get("recommended_team") == routing_decision.assigned_team
                                    )
                                    repo.record_governance_routing_recommendation_feedback(
                                        conn,
                                        recommendation_id=str(routing_recommendation_row["id"]),
                                        accepted=recommendation_match,
                                        accepted_by="worker",
                                        override_reason=(
                                            None
                                            if recommendation_match
                                            else "worker_assignment_differs_from_advisory_recommendation"
                                        ),
                                        applied=recommendation_match,
                                    )
                                _persist_assignment_routing_feedback(
                                    conn,
                                    case_row=case_row_for_routing,
                                    latest_case_summary=latest_case_summary,
                                    routing_decision_id=str(decision_row["id"]),
                                    previous_assigned_to=None,
                                    previous_assigned_team=None,
                                    new_assigned_to=routing_decision.assigned_user,
                                    new_assigned_team=routing_decision.assigned_team,
                                    reason=routing_decision.routing_reason,
                                    metadata={
                                        "degradation_state_id": str(state_row["id"]),
                                        "assignment_id": str(assignment_row["id"]),
                                        "routing_rule_id": routing_decision.routing_rule_id,
                                        "routing_override_id": routing_decision.override_id,
                                    },
                                )
                                append_governance_case_event(
                                    conn,
                                    case_id=str(case_row["id"]),
                                    workspace_id=workspace_id,
                                    event_type="auto_routed",
                                    actor="worker",
                                    payload={
                                        "assigned_to": routing_decision.assigned_user,
                                        "assigned_team": routing_decision.assigned_team,
                                        "routing_reason": routing_decision.routing_reason,
                                        "routing_rule_id": routing_decision.routing_rule_id,
                                        "routing_override_id": routing_decision.override_id,
                                        "routing_decision_id": str(decision_row["id"]),
                                    },
                                )
                                assignment_event = build_assignment_event(
                                    case_id=str(case_row["id"]),
                                    workspace_id=workspace_id,
                                    assigned_to=routing_decision.assigned_user,
                                    assigned_team=routing_decision.assigned_team,
                                    reason=routing_decision.routing_reason,
                                    actor="worker",
                                    assignment_id=str(assignment_row["id"]),
                                )
                                append_governance_incident_timeline_event(
                                    conn,
                                    case_id=assignment_event.case_id,
                                    workspace_id=assignment_event.workspace_id,
                                    event_type=assignment_event.event_type,
                                    event_source=assignment_event.event_source,
                                    title=assignment_event.title,
                                    detail=assignment_event.detail,
                                    actor=assignment_event.actor,
                                    event_at=assignment_event.event_at,
                                    metadata=assignment_event.metadata,
                                    source_table=assignment_event.source_table,
                                    source_id=assignment_event.source_id,
                                )
                        _refresh_case_summary(
                            conn,
                            case_id=str(case_row["id"]),
                            summary_service=case_summary_service,
                        )
                        _refresh_case_sla_evaluation(
                            conn,
                            case_id=str(case_row["id"]),
                            sla_service=workload_sla_service,
                        )
                        _refresh_case_escalation(
                            conn,
                            case_id=str(case_row["id"]),
                            escalation_service=escalation_service,
                            routing_outcome_service=routing_outcome_service,
                        )

                    for active_state in get_governance_degradation_states(
                        conn,
                        workspace_id,
                        watchlist_id=watchlist_id,
                        statuses=("active", "escalated"),
                        limit=100,
                    ):
                        if str(active_state["id"]) in touched_state_ids:
                            continue
                        recovery_decision = degradation_service.evaluate_recovery(
                            active_state,
                            trailing_metrics=_build_degradation_trailing_metrics(
                                state=active_state,
                                latest_stability_row=latest_stability_row,
                                version_health_row=version_health_row,
                                version_replay_row=version_replay_row,
                                version_regime_row=version_regime_row,
                            ),
                        )
                        if not recovery_decision:
                            continue
                        resolved_state = resolve_governance_degradation_state(
                            conn,
                            state_id=str(active_state["id"]),
                            resolution_summary={
                                "recovery_reason": recovery_decision["recovery_reason"],
                                "trailing_metrics": recovery_decision["trailing_metrics"],
                            },
                            resolved_at=recovery_decision["resolved_at"],
                        )
                        governance_degradation_rows.append(resolved_state)
                        resolution_action = recovery_action_service.build_auto_resolution_action(
                            workspace_id=workspace_id,
                            degradation_state_id=str(active_state["id"]),
                            recovery_reason=recovery_decision["recovery_reason"],
                            trailing_metrics=recovery_decision["trailing_metrics"],
                        )
                        resolution_action_row = insert_governance_resolution_action(conn, resolution_action.__dict__)
                        recovery_row = insert_governance_recovery_event(
                            conn,
                            {
                                "workspace_id": workspace_id,
                                "state_id": active_state["id"],
                                "watchlist_id": active_state.get("watchlist_id"),
                                "degradation_type": active_state["degradation_type"],
                                "version_tuple": active_state["version_tuple"],
                                "regime": active_state.get("regime"),
                                "recovered_at": recovery_decision["resolved_at"],
                                "recovery_reason": recovery_decision["recovery_reason"],
                                "prior_severity": active_state["severity"],
                                "trailing_metrics": recovery_decision["trailing_metrics"],
                                "metadata": {
                                    "resolved_state_status": resolved_state["state_status"],
                                    "queue_name": str(job["queue_name"]),
                                },
                            },
                        )
                        governance_recovery_rows.append(recovery_row)
                        resolved_case = resolve_governance_case_for_state(
                            conn,
                            degradation_state_id=str(active_state["id"]),
                            resolution_note=str(recovery_decision["recovery_reason"]),
                            metadata={"resolved_by": "worker"},
                        )
                        if resolved_case:
                            governance_case_rows.append(resolved_case)
                            resolved_case_summary = get_governance_case_summary_latest(
                                conn,
                                case_id=str(resolved_case["id"]),
                            )
                            routing_outcome_service.record_resolution(
                                conn,
                                _build_routing_outcome_snapshot(
                                    case_row=resolved_case,
                                    latest_case_summary=resolved_case_summary,
                                ),
                                recovery_decision["resolved_at"],
                            )
                            append_governance_case_evidence(
                                conn,
                                case_id=str(resolved_case["id"]),
                                workspace_id=workspace_id,
                                evidence_type="recovery_event",
                                reference_id=str(recovery_row["id"]),
                                title="Recovery event",
                                summary="Recovery event that resolved the chronic degradation case.",
                                payload={"recovery_reason": recovery_decision["recovery_reason"]},
                            )
                            append_governance_case_note(
                                conn,
                                case_id=str(resolved_case["id"]),
                                workspace_id=workspace_id,
                                note=str(recovery_decision["recovery_reason"]),
                                author="worker",
                                note_type="closure",
                                visibility="internal",
                                metadata={
                                    "source": "worker",
                                    "recovery_event_id": str(recovery_row["id"]),
                                    "resolution_action_id": str(resolution_action_row["id"]),
                                },
                            )
                            append_governance_case_event(
                                conn,
                                case_id=str(resolved_case["id"]),
                                workspace_id=workspace_id,
                                event_type="case_resolved",
                                actor="worker",
                                payload={
                                    "degradation_state_id": str(active_state["id"]),
                                    "recovery_reason": recovery_decision["recovery_reason"],
                                },
                            )
                            resolved_event = build_case_resolved_event(
                                case_id=str(resolved_case["id"]),
                                workspace_id=workspace_id,
                                actor="worker",
                                resolution_note=str(recovery_decision["recovery_reason"]),
                                recovery_reason=str(recovery_decision["recovery_reason"]),
                                action_id=str(resolution_action_row["id"]),
                                recovery_event_id=str(recovery_row["id"]),
                            )
                            append_governance_incident_timeline_event(
                                conn,
                                case_id=resolved_event.case_id,
                                workspace_id=resolved_event.workspace_id,
                                event_type=resolved_event.event_type,
                                event_source=resolved_event.event_source,
                                title=resolved_event.title,
                                detail=resolved_event.detail,
                                actor=resolved_event.actor,
                                event_at=resolved_event.event_at,
                                metadata=resolved_event.metadata,
                                source_table=resolved_event.source_table,
                                source_id=resolved_event.source_id,
                            )
                            _refresh_case_summary(
                                conn,
                                case_id=str(resolved_case["id"]),
                                summary_service=case_summary_service,
                            )
                            _refresh_case_sla_evaluation(
                                conn,
                                case_id=str(resolved_case["id"]),
                                sla_service=workload_sla_service,
                            )
                            _refresh_case_escalation(
                                conn,
                                case_id=str(resolved_case["id"]),
                                escalation_service=escalation_service,
                                routing_outcome_service=routing_outcome_service,
                            )
                            _refresh_case_threshold_learning(
                                conn,
                                case_id=str(resolved_case["id"]),
                                workspace_id=workspace_id,
                                event_type=str(active_state["degradation_type"]),
                                regime=(
                                    str(active_state["regime"])
                                    if active_state.get("regime")
                                    else None
                                ),
                                threshold_learning_service=threshold_learning_service,
                                threshold_review_service=threshold_review_service,
                                threshold_autopromotion_service=threshold_autopromotion_service,
                            )
                    stage_meta["feature_count"] = len(feature_rows)
                    stage_meta["signal_count"] = len(signal_rows)
                    stage_meta["composite_count"] = len(composite_rows)
                    stage_meta["attribution_rows"] = len(signal_attribution_rows)
                    stage_meta["family_attribution_rows"] = len(family_attribution_rows)
                    stage_meta["drift_metric_count"] = len(drift_metrics_rows)
                    stage_meta["drift_severity"] = drift_envelope["severity"] if drift_envelope else None
                    stage_meta["comparison_run_id"] = (
                        drift_envelope["summary"].get("comparison_run_id")
                        if drift_envelope
                        else None
                    )
                    stage_meta["replay_delta_severity"] = (
                        replay_delta_payload.get("severity")
                        if replay_delta_payload
                        else None
                    )
                    stage_meta["replay_input_match_score"] = (
                        replay_delta_payload.get("input_match_score")
                        if replay_delta_payload
                        else None
                    )
                    stage_meta["regime_transition_detected"] = (
                        regime_transition_payload.get("transition_detected")
                        if regime_transition_payload
                        else None
                    )
                    stage_meta["regime_transition_classification"] = (
                        regime_transition_payload.get("transition_classification")
                        if regime_transition_payload
                        else None
                    )
                    stage_meta["regime_transition_stability_score"] = (
                        regime_transition_payload.get("stability_score")
                        if regime_transition_payload
                        else None
                    )
                    stage_meta["regime_transition_anomaly_likelihood"] = (
                        regime_transition_payload.get("anomaly_likelihood")
                        if regime_transition_payload
                        else None
                    )
                    stage_meta["stability_classification"] = (
                        stability_payload.get("baseline", {}).get("stability_classification")
                        if stability_payload
                        else None
                    )
                    stage_meta["family_instability_score"] = (
                        stability_payload.get("baseline", {}).get("family_instability_score")
                        if stability_payload
                        else None
                    )
                    stage_meta["replay_consistency_risk_score"] = (
                        stability_payload.get("baseline", {}).get("replay_consistency_risk_score")
                        if stability_payload
                        else None
                    )
                    stage_meta["regime_instability_score"] = (
                        stability_payload.get("baseline", {}).get("regime_instability_score")
                        if stability_payload
                        else None
                    )
                    stage_meta["governance_alert_count"] = len(governance_alert_rows)
                    stage_meta["governance_alert_types"] = [row["event_type"] for row in governance_alert_rows]
                    stage_meta["threshold_application_count"] = len(threshold_application_rows)
                    stage_meta["threshold_profile_names"] = list({row.get("metadata", {}).get("profile_name") for row in threshold_application_rows if row.get("metadata", {}).get("profile_name")})
                    stage_meta["anomaly_cluster_count"] = len({str(row["id"]) for row in anomaly_cluster_rows})
                    stage_meta["anomaly_cluster_keys"] = list({str(row["cluster_key"]) for row in anomaly_cluster_rows})
                    stage_meta["governance_degradation_count"] = len({str(row["id"]) for row in governance_degradation_rows})
                    stage_meta["governance_recovery_count"] = len(governance_recovery_rows)
                    stage_meta["governance_case_count"] = len({str(row["id"]) for row in governance_case_rows})

                with telemetry.track_stage("emit_alerts") as stage_meta:
                    final_lineage = lineage.finish(
                        {
                            "feature_count": len(feature_rows),
                            "signal_count": len(signal_rows),
                            "composite_count": len(composite_rows),
                            "attribution_count": len(signal_attribution_rows),
                            "family_attribution_count": len(family_attribution_rows),
                            "attribution_reconciliation_delta": (
                                attribution_reconciliation["reconciliation_delta"] if attribution_reconciliation else None
                            ),
                            "attribution_reconciled": (
                                attribution_reconciliation["reconciled"] if attribution_reconciliation else None
                            ),
                            "drift_metric_count": len(drift_metrics_rows),
                            "drift_severity": drift_envelope["severity"] if drift_envelope else None,
                            "comparison_run_id": (
                                drift_envelope["summary"].get("comparison_run_id")
                                if drift_envelope
                                else None
                            ),
                            "replay_delta_severity": (
                                replay_delta_payload.get("severity")
                                if replay_delta_payload
                                else None
                            ),
                            "replay_input_match_score": (
                                replay_delta_payload.get("input_match_score")
                                if replay_delta_payload
                                else None
                            ),
                            "replay_diagnosis": (
                                replay_delta_payload.get("summary", {}).get("diagnosis")
                                if replay_delta_payload
                                else None
                            ),
                            "regime_transition_detected": (
                                regime_transition_payload.get("transition_detected")
                                if regime_transition_payload
                                else None
                            ),
                            "regime_transition_classification": (
                                regime_transition_payload.get("transition_classification")
                                if regime_transition_payload
                                else None
                            ),
                            "regime_transition_stability_score": (
                                regime_transition_payload.get("stability_score")
                                if regime_transition_payload
                                else None
                            ),
                            "regime_transition_anomaly_likelihood": (
                                regime_transition_payload.get("anomaly_likelihood")
                                if regime_transition_payload
                                else None
                            ),
                            "stability_classification": (
                                stability_payload.get("baseline", {}).get("stability_classification")
                                if stability_payload
                                else None
                            ),
                            "family_instability_score": (
                                stability_payload.get("baseline", {}).get("family_instability_score")
                                if stability_payload
                                else None
                            ),
                            "replay_consistency_risk_score": (
                                stability_payload.get("baseline", {}).get("replay_consistency_risk_score")
                                if stability_payload
                                else None
                            ),
                            "regime_instability_score": (
                                stability_payload.get("baseline", {}).get("regime_instability_score")
                                if stability_payload
                                else None
                            ),
                            "governance_alert_count": len(governance_alert_rows),
                            "governance_alert_types": [row["event_type"] for row in governance_alert_rows],
                            "governance_degradation_count": len({str(row["id"]) for row in governance_degradation_rows}),
                            "governance_recovery_count": len(governance_recovery_rows),
                            "scope_hash": (
                                compute_scope_row.get("scope_hash")
                                if compute_scope_row
                                else None
                            ),
                            "scope_version": (
                                compute_scope_row.get("scope_version")
                                if compute_scope_row
                                else None
                            ),
                            "primary_assets": (
                                compute_scope_row.get("primary_assets")
                                if compute_scope_row
                                else []
                            ),
                            "dependency_assets": (
                                compute_scope_row.get("dependency_assets")
                                if compute_scope_row
                                else []
                            ),
                            "asset_universe_count": (
                                compute_scope_row.get("asset_universe_count")
                                if compute_scope_row
                                else 0
                            ),
                            "is_replay": replay_context.source_run_id is not None,
                            "replayed_from_run_id": replay_context.source_run_id,
                        }
                    )
                    update_run_lineage(
                        conn,
                        job_id,
                        queue_id,
                        final_lineage,
                        status="completed",
                        runtime_ms=int(final_lineage["runtime_ms"]),
                    )

                    has_completed_policy = has_matching_alert_policy_db(
                        conn,
                        workspace_id,
                        watchlist_id,
                        "job_completed",
                    )
                    inserted_alerts = evaluate_alert_policies_db(
                        conn,
                        workspace_id,
                        watchlist_id,
                        "job_completed",
                        "info",
                        job_id,
                        build_job_event_payload(
                            job_id,
                            "completed",
                            f"Job {job_id} wrote {len(composite_rows)} composites.",
                            final_lineage,
                            {
                                "features": len(feature_rows),
                                "signals": len(signal_rows),
                                "composites": len(composite_rows),
                                "is_replay": replay_context.source_run_id is not None,
                                "replayed_from_run_id": replay_context.source_run_id,
                            },
                        ),
                    )

                    incident_analytics_service.refresh_workspace_snapshot(
                        conn,
                        workspace_id=workspace_id,
                    )
                    incident_performance_service.refresh_workspace_snapshot(
                        conn,
                        workspace_id=workspace_id,
                    )
                    promotion_impact_service.refresh_recent_impacts(
                        conn,
                        workspace_id=workspace_id,
                    )
                    manager_analytics_service.refresh_workspace_snapshot(
                        conn,
                        workspace_id=workspace_id,
                    )
                    routing_optimization_service.refresh_workspace_snapshot(
                        conn,
                        workspace_id=workspace_id,
                    )
                    _evaluate_routing_policy_autopromotions(
                        conn,
                        workspace_id=workspace_id,
                        routing_policy_autopromotion_service=routing_policy_autopromotion_service,
                    )
                    _evaluate_routing_policy_rollback_impact(
                        conn,
                        workspace_id=workspace_id,
                        routing_policy_rollback_impact_service=routing_policy_rollback_impact_service,
                    )
                    try:
                        governance_policy_optimization_service.refresh_workspace_optimization(
                            conn,
                            workspace_id=workspace_id,
                        )
                    except Exception:
                        logger.warning(
                            "governance_policy_optimization failed for workspace=%s, continuing",
                            workspace_id,
                            exc_info=True,
                        )
                    try:
                        governance_policy_autopromotion_service.evaluate_and_autopromote(
                            conn,
                            workspace_id=workspace_id,
                        )
                    except Exception:
                        logger.warning(
                            "governance_policy_autopromotion failed for workspace=%s, continuing",
                            workspace_id,
                            exc_info=True,
                        )
                    try:
                        dependency_context_service.refresh_workspace_contexts(
                            conn,
                            workspace_id=workspace_id,
                        )
                    except Exception:
                        logger.warning(
                            "dependency_context refresh failed for workspace=%s, continuing",
                            workspace_id,
                            exc_info=True,
                        )
                    try:
                        cross_asset_signal_service.refresh_workspace_cross_asset_signals(
                            conn,
                            workspace_id=workspace_id,
                            run_id=str(job_id) if job_id else None,
                        )
                    except Exception:
                        logger.warning(
                            "cross_asset_signal refresh failed for workspace=%s, continuing",
                            workspace_id,
                            exc_info=True,
                        )
                    try:
                        cross_asset_explanation_service.refresh_workspace_explanations(
                            conn,
                            workspace_id=workspace_id,
                            run_id=str(job_id) if job_id else None,
                        )
                    except Exception:
                        logger.warning(
                            "cross_asset_explanation refresh failed for workspace=%s, continuing",
                            workspace_id,
                            exc_info=True,
                        )
                    if job_id:
                        try:
                            cross_asset_attribution_service.refresh_workspace_attribution(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_attribution refresh failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            dependency_priority_weighting_service.refresh_workspace_weighting(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "dependency_priority_weighting refresh failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            regime_aware_cross_asset_service.refresh_workspace_regime_interpretation(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "regime_aware_cross_asset refresh failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        # 4.1D replay validation — fires only when this run is
                        # a replay with a resolvable source. Service internally
                        # short-circuits when lineage is not a replay.
                        try:
                            cross_asset_replay_validation_service.refresh_replay_validation_for_run(
                                conn,
                                replay_run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_replay_validation failed for run=%s, continuing",
                                job_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_timing_service.refresh_workspace_timing(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_timing refresh failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_timing_attribution_service.refresh_workspace_timing_attribution(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_timing_attribution refresh failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_timing_composite_service.refresh_workspace_timing_composite(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_timing_composite refresh failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        # 4.2D timing replay validation — fires only when the
                        # run is a replay with a resolvable source. Service
                        # internally short-circuits for non-replay runs.
                        try:
                            cross_asset_timing_replay_validation_service.refresh_timing_replay_validation_for_run(
                                conn,
                                replay_run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_timing_replay_validation failed for run=%s, continuing",
                                job_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_transition_diagnostics_service.refresh_workspace_transition_diagnostics(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_transition_diagnostics failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_transition_attribution_service.refresh_workspace_transition_attribution(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_transition_attribution failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_transition_composite_service.refresh_workspace_transition_composite(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_transition_composite failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        # 4.3D sequencing replay validation — fires only when
                        # the run is a replay with a resolvable source. Service
                        # internally short-circuits for non-replay runs.
                        try:
                            cross_asset_transition_replay_validation_service.refresh_transition_replay_validation_for_run(
                                conn,
                                replay_run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_transition_replay_validation failed for run=%s, continuing",
                                job_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_pattern_service.refresh_workspace_archetypes(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_pattern archetype classification failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_archetype_attribution_service.refresh_workspace_archetype_attribution(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_archetype_attribution failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_archetype_composite_service.refresh_workspace_archetype_composite(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_archetype_composite failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        # 4.4D archetype replay validation — fires only when the
                        # run is a replay with a resolvable source. Service
                        # internally short-circuits for non-replay runs.
                        try:
                            cross_asset_archetype_replay_validation_service.refresh_archetype_replay_validation_for_run(
                                conn,
                                replay_run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_archetype_replay_validation failed for run=%s, continuing",
                                job_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_pattern_cluster_service.refresh_workspace_pattern_clusters(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_pattern_cluster failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_cluster_attribution_service.refresh_workspace_cluster_attribution(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_cluster_attribution failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_cluster_composite_service.refresh_workspace_cluster_composite(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_cluster_composite failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        # 4.5D cluster replay validation — fires only when the
                        # run is a replay with a resolvable source. Service
                        # internally short-circuits for non-replay runs.
                        try:
                            cross_asset_cluster_replay_validation_service.refresh_cluster_replay_validation_for_run(
                                conn,
                                replay_run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_cluster_replay_validation failed for run=%s, continuing",
                                job_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_persistence_service.refresh_workspace_persistence(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_persistence failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_persistence_attribution_service.refresh_workspace_persistence_attribution(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_persistence_attribution failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_persistence_composite_service.refresh_workspace_persistence_composite(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_persistence_composite failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_persistence_replay_validation_service.refresh_persistence_replay_validation_for_run(
                                conn,
                                replay_run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_persistence_replay_validation failed for run=%s, continuing",
                                job_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_signal_decay_service.refresh_workspace_signal_decay(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_signal_decay failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_decay_attribution_service.refresh_workspace_decay_attribution(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_decay_attribution failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_decay_composite_service.refresh_workspace_decay_composite(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_decay_composite failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_decay_replay_validation_service.refresh_decay_replay_validation_for_run(
                                conn,
                                replay_run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_decay_replay_validation failed for run=%s, continuing",
                                job_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_layer_conflict_service.refresh_workspace_layer_conflict(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_layer_conflict failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_conflict_attribution_service.refresh_workspace_conflict_attribution(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_conflict_attribution failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )
                        try:
                            cross_asset_conflict_composite_service.refresh_workspace_conflict_composite(
                                conn,
                                workspace_id=workspace_id,
                                run_id=str(job_id),
                            )
                        except Exception:
                            logger.warning(
                                "cross_asset_conflict_composite failed for workspace=%s, continuing",
                                workspace_id,
                                exc_info=True,
                            )

                    complete_job(conn, job_id)
                    stage_meta["inserted_alerts"] = inserted_alerts
                    stage_meta["completed_policy_present"] = has_completed_policy

                replace_run_stage_timings(
                    conn,
                    job_id,
                    workspace_id,
                    watchlist_id,
                    _stage_rows(telemetry),
                )
                update_run_forensics(
                    conn,
                    job_id,
                    failure_stage=None,
                    failure_code=None,
                    input_snapshot_id=input_snapshot_id,
                    explanation_version=EXPLANATION_VERSION,
                )
                conn.commit()

                logger.info(
                    "completed job_id=%s features=%s signals=%s composites=%s replay=%s",
                    job_id,
                    len(feature_rows),
                    len(signal_rows),
                    len(composite_rows),
                    replay_context.source_run_id is not None,
                )
                if inserted_alerts == 0 and not has_completed_policy:
                    emit_alert(
                        job_id,
                        workspace_id,
                        "Recompute completed",
                        f"Job {job_id} wrote {len(composite_rows)} composites.",
                        "info",
                        {
                            "features": len(feature_rows),
                            "signals": len(signal_rows),
                            "composites": len(composite_rows),
                            "is_replay": replay_context.source_run_id is not None,
                            "replayed_from_run_id": replay_context.source_run_id,
                        },
                    )

            except Exception as exc:
                conn.rollback()
                logger.exception("job execution failed: %s", exc)

                if job is not None:
                    job_id = str(job["job_id"])
                    queue_id = int(job["queue_id"])
                    workspace_id = str(job["workspace_id"])
                    watchlist_id = str(job["watchlist_id"]) if job.get("watchlist_id") else None
                    payload = job.get("payload") or {}
                    replay_context = extract_replay_context(payload if isinstance(payload, dict) else {})
                    failure_stage = telemetry.failure_stage or "worker"
                    failure_code = telemetry.failure_code or classify_failure_code(failure_stage, exc)
                    try:
                        result = schedule_job_retry_db(conn, queue_id, str(exc), failure_stage)
                        outcome = classify_retry_outcome(result)
                        job_type = str(job.get("queue_name", "recompute"))
                        retry_count = int(result.get("retry_count") or 0)

                        if input_snapshot:
                            input_snapshot_id = upsert_run_input_snapshot(
                                conn,
                                job_id,
                                workspace_id,
                                watchlist_id,
                                input_snapshot,
                                compute_scope_id=str(compute_scope_row["id"]) if compute_scope_row else None,
                            )

                        replace_run_stage_timings(
                            conn,
                            job_id,
                            workspace_id,
                            watchlist_id,
                            _stage_rows(telemetry),
                        )
                        update_run_forensics(
                            conn,
                            job_id,
                            failure_stage=failure_stage,
                            failure_code=failure_code,
                            input_snapshot_id=input_snapshot_id,
                            explanation_version=EXPLANATION_VERSION,
                        )

                        failure_lineage = (
                            lineage.finish(
                                {
                                    "outcome": outcome,
                                    "error": str(exc)[:500],
                                    "failure_stage": failure_stage,
                                    "failure_code": failure_code,
                                    "scope_hash": compute_scope_row.get("scope_hash") if compute_scope_row else None,
                                    "scope_version": compute_scope_row.get("scope_version") if compute_scope_row else None,
                                    "is_replay": replay_context.source_run_id is not None,
                                    "replayed_from_run_id": replay_context.source_run_id,
                                }
                            )
                            if lineage
                            else {
                                "outcome": outcome,
                                "error": str(exc)[:500],
                                "failure_stage": failure_stage,
                                "failure_code": failure_code,
                                "scope_hash": compute_scope_row.get("scope_hash") if compute_scope_row else None,
                                "scope_version": compute_scope_row.get("scope_version") if compute_scope_row else None,
                                "is_replay": replay_context.source_run_id is not None,
                                "replayed_from_run_id": replay_context.source_run_id,
                            }
                        )
                        failure_status = "dead_lettered" if outcome == "dead_lettered" else "queued"
                        update_run_lineage(
                            conn,
                            job_id,
                            queue_id,
                            failure_lineage,
                            status=failure_status,
                            runtime_ms=int(failure_lineage.get("runtime_ms", 0)) or None,
                        )

                        inserted_alerts = 0
                        has_failed_policy = False
                        has_dead_letter_policy = False
                        has_retry_threshold_policy = False
                        if outcome == "dead_lettered":
                            has_dead_letter_policy = has_matching_alert_policy_db(
                                conn,
                                workspace_id,
                                watchlist_id,
                                "job_dead_letter",
                            )
                            inserted_alerts = evaluate_alert_policies_db(
                                conn,
                                workspace_id,
                                watchlist_id,
                                "job_dead_letter",
                                "high",
                                job_id,
                                build_job_event_payload(
                                    job_id,
                                    "dead_lettered",
                                    str(exc),
                                    failure_lineage,
                                    {
                                        "retry_count": result.get("retry_count"),
                                        "action": result.get("action"),
                                        "failure_stage": failure_stage,
                                        "failure_code": failure_code,
                                    },
                                ),
                            )
                        else:
                            has_failed_policy = has_matching_alert_policy_db(
                                conn,
                                workspace_id,
                                watchlist_id,
                                "job_failed",
                            )
                            inserted_alerts += evaluate_alert_policies_db(
                                conn,
                                workspace_id,
                                watchlist_id,
                                "job_failed",
                                "high",
                                job_id,
                                build_job_event_payload(
                                    job_id,
                                    "retry_scheduled",
                                    str(exc),
                                    failure_lineage,
                                    {
                                        "retry_count": retry_count,
                                        "action": result.get("action"),
                                        "next_retry_at": result.get("next_retry_at"),
                                        "failure_stage": failure_stage,
                                        "failure_code": failure_code,
                                    },
                                ),
                            )

                        if outcome != "dead_lettered" and retry_count >= RETRY_ALERT_THRESHOLD:
                            has_retry_threshold_policy = has_matching_alert_policy_db(
                                conn,
                                workspace_id,
                                watchlist_id,
                                "job_retry_threshold",
                            )
                            inserted_alerts += evaluate_alert_policies_db(
                                conn,
                                workspace_id,
                                watchlist_id,
                                "job_retry_threshold",
                                "high" if retry_count >= RETRY_ALERT_THRESHOLD + 1 else "medium",
                                job_id,
                                build_job_event_payload(
                                    job_id,
                                    "retry_threshold",
                                    str(exc),
                                    failure_lineage,
                                    {
                                        "retry_count": retry_count,
                                        "action": result.get("action"),
                                        "next_retry_at": result.get("next_retry_at"),
                                        "failure_stage": failure_stage,
                                        "failure_code": failure_code,
                                    },
                                ),
                            )

                        conn.commit()

                        if outcome == "retry_scheduled":
                            if inserted_alerts == 0 and not has_failed_policy and not has_retry_threshold_policy:
                                _, title, body = retry_scheduled_message(
                                    job_type,
                                    result.get("next_retry_at"),
                                )
                                emit_alert(
                                    job_id,
                                    workspace_id,
                                    title,
                                    body,
                                    "warning",
                                    {
                                        "retry_count": retry_count,
                                        "failure_stage": failure_stage,
                                        "failure_code": failure_code,
                                    },
                                )
                        else:
                            _, title, body = terminal_failure_message(job_type, str(exc))
                            if inserted_alerts == 0 and not has_dead_letter_policy:
                                emit_alert(
                                    job_id,
                                    workspace_id,
                                    title,
                                    body,
                                    "high",
                                    {
                                        "retry_count": result.get("retry_count"),
                                        "action": result.get("action"),
                                        "failure_stage": failure_stage,
                                        "failure_code": failure_code,
                                    },
                                )
                    except Exception:
                        conn.rollback()
                        logger.exception("failed to persist job failure state")

                time.sleep(settings.worker_poll_seconds)
