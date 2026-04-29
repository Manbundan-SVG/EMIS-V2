from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class EscalationPolicy:
    id: str
    severity: str | None
    chronicity_class: str | None
    root_cause_code: str | None
    min_case_age_minutes: int | None
    min_ack_age_minutes: int | None
    min_repeat_count: int | None
    min_operator_pressure: float | None
    escalation_level: str
    escalate_to_team: str | None
    escalate_to_user: str | None
    cooldown_minutes: int
    metadata: dict[str, Any]


@dataclass(frozen=True)
class EscalationContext:
    workspace_id: str
    case_id: str
    severity: str
    status: str
    root_cause_code: str | None
    repeat_count: int
    chronicity_class: str | None
    opened_at: datetime
    acknowledged_at: datetime | None
    current_assignee: str | None
    current_team: str | None
    operator_pressure: float | None
    team_pressure: float | None
    ack_breached: bool
    resolve_breached: bool
    now: datetime
    metadata: dict[str, Any]


@dataclass(frozen=True)
class EscalationDecision:
    should_escalate: bool
    clear_existing: bool
    event_type: str | None
    escalation_level: str | None
    escalated_to_team: str | None
    escalated_to_user: str | None
    reason: str | None
    policy_id: str | None
    status: str | None
    repeated_count: int | None
    metadata: dict[str, Any]


class EscalationService:
    def derive_chronicity_class(self, repeat_count: int) -> str | None:
        if repeat_count >= 3:
            return "chronic"
        if repeat_count > 1:
            return "recurring"
        return None

    def build_context(
        self,
        *,
        case_row: dict[str, Any],
        case_summary_latest: dict[str, Any] | None,
        sla_row: dict[str, Any] | None,
        operator_pressure_row: dict[str, Any] | None,
        team_pressure_row: dict[str, Any] | None,
        now: datetime | None = None,
    ) -> EscalationContext:
        now = now or datetime.now(timezone.utc)
        opened_at = case_row.get("opened_at")
        if not isinstance(opened_at, datetime):
            raise RuntimeError("case_row missing opened_at for escalation evaluation")

        acknowledged_at = case_row.get("acknowledged_at")
        repeat_count = int(case_row.get("repeat_count") or 1)
        root_cause_code = (
            str(case_summary_latest["root_cause_code"])
            if case_summary_latest and case_summary_latest.get("root_cause_code")
            else None
        )

        operator_pressure = (
            float(operator_pressure_row["severity_weighted_load"])
            if operator_pressure_row and operator_pressure_row.get("severity_weighted_load") is not None
            else None
        )
        team_pressure = (
            float(team_pressure_row["severity_weighted_load"])
            if team_pressure_row and team_pressure_row.get("severity_weighted_load") is not None
            else None
        )

        return EscalationContext(
            workspace_id=str(case_row["workspace_id"]),
            case_id=str(case_row["id"]),
            severity=str(case_row["severity"]),
            status=str(case_row["status"]),
            root_cause_code=root_cause_code,
            repeat_count=repeat_count,
            chronicity_class=self.derive_chronicity_class(repeat_count),
            opened_at=opened_at,
            acknowledged_at=acknowledged_at if isinstance(acknowledged_at, datetime) else None,
            current_assignee=str(case_row["current_assignee"]) if case_row.get("current_assignee") else None,
            current_team=str(case_row["current_team"]) if case_row.get("current_team") else None,
            operator_pressure=operator_pressure,
            team_pressure=team_pressure,
            ack_breached=bool(sla_row.get("ack_breached")) if sla_row else False,
            resolve_breached=bool(sla_row.get("resolve_breached")) if sla_row else False,
            now=now,
            metadata={
                "version_tuple": str(case_row["version_tuple"]) if case_row.get("version_tuple") else None,
                "sla_policy_id": str(sla_row["policy_id"]) if sla_row and sla_row.get("policy_id") else None,
                "breach_severity": str(sla_row["breach_severity"]) if sla_row and sla_row.get("breach_severity") else None,
            },
        )

    def choose_policy(
        self,
        *,
        context: EscalationContext,
        policies: list[EscalationPolicy],
    ) -> EscalationPolicy | None:
        case_age_minutes = self._age_minutes(context.now, context.opened_at)
        ack_age_minutes = self._age_minutes(context.now, context.acknowledged_at) if context.acknowledged_at else None
        effective_pressure = context.operator_pressure if context.operator_pressure is not None else context.team_pressure

        for policy in policies:
            if policy.severity and policy.severity != context.severity:
                continue
            if policy.chronicity_class and policy.chronicity_class != context.chronicity_class:
                continue
            if policy.root_cause_code and policy.root_cause_code != context.root_cause_code:
                continue
            if policy.min_repeat_count is not None and context.repeat_count < policy.min_repeat_count:
                continue
            if policy.min_case_age_minutes is not None and case_age_minutes < float(policy.min_case_age_minutes):
                continue
            if policy.min_ack_age_minutes is not None:
                if ack_age_minutes is None or ack_age_minutes < float(policy.min_ack_age_minutes):
                    continue
            if policy.min_operator_pressure is not None:
                if effective_pressure is None or effective_pressure < policy.min_operator_pressure:
                    continue
            return policy
        return None

    def evaluate(
        self,
        *,
        context: EscalationContext,
        policies: list[EscalationPolicy],
        current_state: dict[str, Any] | None,
    ) -> EscalationDecision:
        if context.status in {"resolved", "closed"}:
            if current_state and str(current_state.get("status") or "active") == "active":
                return EscalationDecision(
                    should_escalate=False,
                    clear_existing=True,
                    event_type="escalation_cleared",
                    escalation_level=str(current_state.get("escalation_level") or ""),
                    escalated_to_team=str(current_state["escalated_to_team"]) if current_state.get("escalated_to_team") else None,
                    escalated_to_user=str(current_state["escalated_to_user"]) if current_state.get("escalated_to_user") else None,
                    reason="case_resolved",
                    policy_id=str(current_state["source_policy_id"]) if current_state.get("source_policy_id") else None,
                    status="cleared",
                    repeated_count=int(current_state.get("repeated_count") or 1),
                    metadata={"reason": "case_resolved", **context.metadata},
                )
            return EscalationDecision(
                should_escalate=False,
                clear_existing=False,
                event_type=None,
                escalation_level=None,
                escalated_to_team=None,
                escalated_to_user=None,
                reason=None,
                policy_id=None,
                status=None,
                repeated_count=None,
                metadata={"reason": "case_resolved_without_active_escalation", **context.metadata},
            )

        policy = self.choose_policy(context=context, policies=policies)
        if policy is None:
            return EscalationDecision(
                should_escalate=False,
                clear_existing=False,
                event_type=None,
                escalation_level=None,
                escalated_to_team=None,
                escalated_to_user=None,
                reason=None,
                policy_id=None,
                status=None,
                repeated_count=None,
                metadata={"reason": "no_matching_policy", **context.metadata},
            )

        reason = self._build_reason(context=context, policy=policy)
        base_metadata = {
            **context.metadata,
            "root_cause_code": context.root_cause_code,
            "repeat_count": context.repeat_count,
            "chronicity_class": context.chronicity_class,
            "operator_pressure": context.operator_pressure,
            "team_pressure": context.team_pressure,
            "ack_breached": context.ack_breached,
            "resolve_breached": context.resolve_breached,
            "policy_metadata": policy.metadata,
        }
        if not current_state or str(current_state.get("status") or "cleared") != "active":
            return EscalationDecision(
                should_escalate=True,
                clear_existing=False,
                event_type="case_escalated",
                escalation_level=policy.escalation_level,
                escalated_to_team=policy.escalate_to_team,
                escalated_to_user=policy.escalate_to_user,
                reason=reason,
                policy_id=policy.id,
                status="active",
                repeated_count=1,
                metadata=base_metadata,
            )

        last_evaluated_at = current_state.get("last_evaluated_at") or current_state.get("escalated_at")
        cooldown_deadline = (
            last_evaluated_at + timedelta(minutes=policy.cooldown_minutes)
            if isinstance(last_evaluated_at, datetime)
            else None
        )
        state_same = (
            str(current_state.get("escalation_level") or "") == policy.escalation_level
            and (current_state.get("escalated_to_team") or None) == policy.escalate_to_team
            and (current_state.get("escalated_to_user") or None) == policy.escalate_to_user
        )
        if state_same and cooldown_deadline and context.now < cooldown_deadline:
            return EscalationDecision(
                should_escalate=False,
                clear_existing=False,
                event_type=None,
                escalation_level=policy.escalation_level,
                escalated_to_team=policy.escalate_to_team,
                escalated_to_user=policy.escalate_to_user,
                reason=reason,
                policy_id=policy.id,
                status="active",
                repeated_count=int(current_state.get("repeated_count") or 1),
                metadata={"reason": "cooldown_active", "cooldown_deadline": cooldown_deadline.isoformat(), **base_metadata},
            )

        return EscalationDecision(
            should_escalate=True,
            clear_existing=False,
            event_type="escalation_repeated",
            escalation_level=policy.escalation_level,
            escalated_to_team=policy.escalate_to_team,
            escalated_to_user=policy.escalate_to_user,
            reason=reason,
            policy_id=policy.id,
            status="active",
            repeated_count=int(current_state.get("repeated_count") or 1) + 1,
            metadata=base_metadata,
        )

    def _build_reason(self, *, context: EscalationContext, policy: EscalationPolicy) -> str:
        qualifiers: list[str] = []
        if context.resolve_breached:
            qualifiers.append("resolve_sla_breached")
        elif context.ack_breached:
            qualifiers.append("ack_sla_breached")
        if context.chronicity_class:
            qualifiers.append(context.chronicity_class)
        if policy.min_operator_pressure is not None:
            qualifiers.append("workload_pressure")
        if context.root_cause_code:
            qualifiers.append(context.root_cause_code)
        qualifier_text = ", ".join(qualifiers) if qualifiers else "policy_match"
        return f"{policy.escalation_level}: {qualifier_text}"

    def _age_minutes(self, now: datetime, started_at: datetime | None) -> float:
        if started_at is None:
            return 0.0
        return max(0.0, (now - started_at).total_seconds() / 60.0)
