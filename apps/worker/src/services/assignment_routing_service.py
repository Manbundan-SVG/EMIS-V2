from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RoutingInput:
    workspace_id: str
    case_id: str
    watchlist_id: str | None
    severity: str | None
    root_cause_code: str | None
    version_tuple: str | None
    regime: str | None = None
    repeat_count: int = 1
    chronic: bool = False


@dataclass(frozen=True)
class RoutingDecision:
    assigned_team: str | None
    assigned_user: str | None
    routing_rule_id: str | None
    override_id: str | None
    routing_reason: str
    workload_snapshot: dict[str, Any]
    metadata: dict[str, Any]


class AssignmentRoutingService:
    def __init__(self, repo: Any) -> None:
        self.repo = repo

    def route_case(self, conn, routing_input: RoutingInput) -> RoutingDecision:
        snapshot = self._build_workload_snapshot(conn, routing_input.workspace_id)

        override = self.repo.get_active_routing_override(
            conn,
            workspace_id=routing_input.workspace_id,
            case_id=routing_input.case_id,
            watchlist_id=routing_input.watchlist_id,
            root_cause_code=routing_input.root_cause_code,
            severity=routing_input.severity,
            version_tuple=routing_input.version_tuple,
            regime=routing_input.regime,
        )
        if override:
            return RoutingDecision(
                assigned_team=override.get("assigned_team"),
                assigned_user=override.get("assigned_user"),
                routing_rule_id=None,
                override_id=str(override["id"]),
                routing_reason=str(override.get("reason") or "routing_override"),
                workload_snapshot=snapshot,
                metadata={
                    "strategy": "override",
                    "reason": override.get("reason"),
                },
            )

        rules = self.repo.list_matching_routing_rules(
            conn,
            workspace_id=routing_input.workspace_id,
            watchlist_id=routing_input.watchlist_id,
            root_cause_code=routing_input.root_cause_code,
            severity=routing_input.severity,
            version_tuple=routing_input.version_tuple,
            regime=routing_input.regime,
            repeat_count=routing_input.repeat_count,
            chronic=routing_input.chronic,
        )
        if rules:
            best_rule = rules[0]
            assigned_team = best_rule.get("assign_team") or best_rule.get("fallback_team")
            assigned_user = best_rule.get("assign_user") or self._pick_least_loaded_user(
                snapshot,
                assigned_team=assigned_team,
            )
            if assigned_team is None and assigned_user is None:
                assigned_team, assigned_user = self._fallback_assignment(snapshot, routing_input.severity)
            return RoutingDecision(
                assigned_team=assigned_team,
                assigned_user=assigned_user,
                routing_rule_id=str(best_rule["id"]),
                override_id=None,
                routing_reason=self._render_reason(best_rule, routing_input),
                workload_snapshot=snapshot,
                metadata={
                    "strategy": "rule",
                    "priority": best_rule.get("priority"),
                    "rule_name": best_rule.get("routing_reason_template"),
                },
            )

        fallback_team, fallback_user = self._fallback_assignment(snapshot, routing_input.severity)
        return RoutingDecision(
            assigned_team=fallback_team,
            assigned_user=fallback_user,
            routing_rule_id=None,
            override_id=None,
            routing_reason="fallback_workload_routing" if fallback_team or fallback_user else "unassigned_fallback",
            workload_snapshot=snapshot,
            metadata={"strategy": "fallback"},
        )

    def _render_reason(self, rule: dict[str, Any], routing_input: RoutingInput) -> str:
        template = str(rule.get("routing_reason_template") or "routing_rule_match")
        return template.format_map(
            {
                "root_cause_code": routing_input.root_cause_code or "unknown",
                "severity": routing_input.severity or "unknown",
                "version_tuple": routing_input.version_tuple or "unknown",
                "regime": routing_input.regime or "unknown",
                "repeat_count": routing_input.repeat_count,
            }
        )

    def _build_workload_snapshot(self, conn, workspace_id: str) -> dict[str, Any]:
        return {
            "team_metrics": self.repo.list_team_case_metrics(conn, workspace_id),
            "operator_metrics": self.repo.list_operator_case_metrics(conn, workspace_id),
        }

    def _fallback_assignment(
        self,
        snapshot: dict[str, Any],
        severity: str | None,
    ) -> tuple[str | None, str | None]:
        fallback_team = self._pick_least_loaded_team(snapshot)
        fallback_user = self._pick_least_loaded_user(snapshot, assigned_team=fallback_team)

        if fallback_team or fallback_user:
            return fallback_team, fallback_user

        if severity == "critical":
            return "platform", None
        if severity == "high":
            return "research", None
        if severity == "medium":
            return "triage", None
        return "ops", None

    def _pick_least_loaded_team(self, snapshot: dict[str, Any]) -> str | None:
        teams = snapshot.get("team_metrics", [])
        if not teams:
            return None
        ranked = sorted(
            teams,
            key=lambda row: (
                row.get("severe_open_case_count", 0),
                row.get("open_case_count", 0),
                row.get("stale_open_case_count", 0),
                row.get("avg_open_age_hours") or 0.0,
            ),
        )
        return ranked[0].get("assigned_team")

    def _pick_least_loaded_user(
        self,
        snapshot: dict[str, Any],
        *,
        assigned_team: str | None,
    ) -> str | None:
        users = snapshot.get("operator_metrics", [])
        if assigned_team:
            users = [row for row in users if row.get("assigned_team") == assigned_team]
        if not users:
            return None
        ranked = sorted(
            users,
            key=lambda row: (
                row.get("severe_open_case_count", 0),
                row.get("open_case_count", 0),
                row.get("stale_open_case_count", 0),
                row.get("avg_open_age_hours") or 0.0,
            ),
        )
        return ranked[0].get("operator_id")
