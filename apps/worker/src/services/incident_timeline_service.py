from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class IncidentTimelineEvent:
    case_id: str
    workspace_id: str
    event_type: str
    event_source: str
    title: str
    detail: str | None = None
    actor: str | None = None
    event_at: datetime | None = None
    metadata: dict[str, Any] | None = None
    source_table: str | None = None
    source_id: str | None = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_case_opened_event(
    *,
    case_id: str,
    workspace_id: str,
    severity: str,
    summary: str | None,
    degradation_state_id: str | None,
) -> IncidentTimelineEvent:
    return IncidentTimelineEvent(
        case_id=case_id,
        workspace_id=workspace_id,
        event_type="case_opened",
        event_source="governance_case",
        title="Case opened",
        detail=summary,
        metadata={
            "severity": severity,
            "degradation_state_id": degradation_state_id,
        },
        source_table="governance_cases",
        source_id=case_id,
        event_at=utc_now(),
    )


def build_state_status_changed_event(
    *,
    case_id: str,
    workspace_id: str,
    from_status: str | None,
    to_status: str | None,
    degradation_state_id: str | None,
) -> IncidentTimelineEvent:
    return IncidentTimelineEvent(
        case_id=case_id,
        workspace_id=workspace_id,
        event_type="state_status_changed",
        event_source="governance_degradation",
        title="Degradation state updated",
        detail=f"{from_status or 'unknown'} -> {to_status or 'unknown'}",
        metadata={
            "from_status": from_status,
            "to_status": to_status,
            "degradation_state_id": degradation_state_id,
        },
        source_table="governance_degradation_states",
        source_id=degradation_state_id,
        event_at=utc_now(),
    )


def build_assignment_event(
    *,
    case_id: str,
    workspace_id: str,
    assigned_to: str | None,
    assigned_team: str | None,
    reason: str | None,
    actor: str | None,
    assignment_id: str | None = None,
) -> IncidentTimelineEvent:
    target = assigned_to or assigned_team or "unassigned"
    return IncidentTimelineEvent(
        case_id=case_id,
        workspace_id=workspace_id,
        event_type="assignment_changed",
        event_source="governance_assignment",
        title=f"Assigned to {target}",
        detail=reason,
        actor=actor,
        metadata={
            "assigned_to": assigned_to,
            "assigned_team": assigned_team,
            "reason": reason,
        },
        source_table="governance_assignments",
        source_id=assignment_id,
        event_at=utc_now(),
    )


def build_ack_event(
    *,
    case_id: str,
    workspace_id: str,
    actor: str | None,
    note: str | None,
    acknowledgment_id: str | None = None,
) -> IncidentTimelineEvent:
    return IncidentTimelineEvent(
        case_id=case_id,
        workspace_id=workspace_id,
        event_type="case_acknowledged",
        event_source="governance_acknowledgment",
        title="Case acknowledged",
        detail=note,
        actor=actor,
        source_table="governance_acknowledgments",
        source_id=acknowledgment_id,
        event_at=utc_now(),
    )


def build_mute_event(
    *,
    case_id: str,
    workspace_id: str,
    actor: str | None,
    reason: str | None,
    target_type: str,
    target_key: str,
    muted_until: str | None,
    mute_rule_id: str | None = None,
) -> IncidentTimelineEvent:
    return IncidentTimelineEvent(
        case_id=case_id,
        workspace_id=workspace_id,
        event_type="case_muted",
        event_source="governance_muting_rule",
        title="Case muted",
        detail=reason,
        actor=actor,
        metadata={
            "target_type": target_type,
            "target_key": target_key,
            "muted_until": muted_until,
        },
        source_table="governance_muting_rules",
        source_id=mute_rule_id,
        event_at=utc_now(),
    )


def build_case_resolved_event(
    *,
    case_id: str,
    workspace_id: str,
    actor: str | None,
    resolution_note: str | None,
    recovery_reason: str | None,
    action_id: str | None = None,
    recovery_event_id: str | None = None,
) -> IncidentTimelineEvent:
    return IncidentTimelineEvent(
        case_id=case_id,
        workspace_id=workspace_id,
        event_type="case_resolved",
        event_source="governance_resolution",
        title="Case resolved",
        detail=resolution_note or recovery_reason,
        actor=actor,
        metadata={
            "recovery_reason": recovery_reason,
            "recovery_event_id": recovery_event_id,
        },
        source_table="governance_resolution_actions",
        source_id=action_id,
        event_at=utc_now(),
    )


def build_case_reopened_event(
    *,
    case_id: str,
    workspace_id: str,
    prior_case_id: str,
    recurrence_group_id: str,
    repeat_count: int,
    reopen_reason: str | None,
) -> IncidentTimelineEvent:
    return IncidentTimelineEvent(
        case_id=case_id,
        workspace_id=workspace_id,
        event_type="case_reopened",
        event_source="governance_case_recurrence",
        title="Case reopened from prior incident",
        detail=reopen_reason,
        metadata={
            "prior_case_id": prior_case_id,
            "recurrence_group_id": recurrence_group_id,
            "repeat_count": repeat_count,
        },
        source_table="governance_case_recurrence",
        source_id=prior_case_id,
        event_at=utc_now(),
    )


def build_case_recurring_detected_event(
    *,
    case_id: str,
    workspace_id: str,
    prior_case_id: str,
    recurrence_group_id: str,
    repeat_count: int,
    reopen_reason: str | None,
    match_basis: dict[str, Any] | None = None,
) -> IncidentTimelineEvent:
    return IncidentTimelineEvent(
        case_id=case_id,
        workspace_id=workspace_id,
        event_type="case_recurring_detected",
        event_source="governance_case_recurrence",
        title="Recurring incident linked to prior case",
        detail=reopen_reason,
        metadata={
            "prior_case_id": prior_case_id,
            "recurrence_group_id": recurrence_group_id,
            "repeat_count": repeat_count,
            "match_basis": match_basis or {},
        },
        source_table="governance_case_recurrence",
        source_id=prior_case_id,
        event_at=utc_now(),
    )


def build_case_escalated_event(
    *,
    case_id: str,
    workspace_id: str,
    escalation_level: str,
    escalated_to_team: str | None,
    escalated_to_user: str | None,
    reason: str | None,
    escalation_event_id: str | None = None,
) -> IncidentTimelineEvent:
    target = escalated_to_user or escalated_to_team or "escalation queue"
    return IncidentTimelineEvent(
        case_id=case_id,
        workspace_id=workspace_id,
        event_type="case_escalated",
        event_source="governance_escalation",
        title=f"Escalated to {target}",
        detail=reason,
        metadata={
            "escalation_level": escalation_level,
            "escalated_to_team": escalated_to_team,
            "escalated_to_user": escalated_to_user,
        },
        source_table="governance_escalation_events",
        source_id=escalation_event_id,
        event_at=utc_now(),
    )


def build_escalation_repeated_event(
    *,
    case_id: str,
    workspace_id: str,
    escalation_level: str,
    repeated_count: int,
    reason: str | None,
    escalation_event_id: str | None = None,
) -> IncidentTimelineEvent:
    return IncidentTimelineEvent(
        case_id=case_id,
        workspace_id=workspace_id,
        event_type="escalation_repeated",
        event_source="governance_escalation",
        title="Escalation repeated",
        detail=reason,
        metadata={
            "escalation_level": escalation_level,
            "repeated_count": repeated_count,
        },
        source_table="governance_escalation_events",
        source_id=escalation_event_id,
        event_at=utc_now(),
    )


def build_escalation_cleared_event(
    *,
    case_id: str,
    workspace_id: str,
    escalation_level: str | None,
    reason: str | None,
    escalation_event_id: str | None = None,
) -> IncidentTimelineEvent:
    return IncidentTimelineEvent(
        case_id=case_id,
        workspace_id=workspace_id,
        event_type="escalation_cleared",
        event_source="governance_escalation",
        title="Escalation cleared",
        detail=reason,
        metadata={
            "escalation_level": escalation_level,
        },
        source_table="governance_escalation_events",
        source_id=escalation_event_id,
        event_at=utc_now(),
    )
