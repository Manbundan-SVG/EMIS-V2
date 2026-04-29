from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class PromotionExecution:
    workspace_id: str
    promotion_type: str
    execution_id: str
    scope_type: str
    scope_value: str | None
    executed_at: datetime
    metadata: dict[str, Any]


@dataclass(frozen=True)
class PromotionImpactRefreshResult:
    workspace_id: str
    refreshed_count: int


class PromotionImpactService:
    def __init__(self, repo_module: Any, window_days: int = 7) -> None:
        self.repo = repo_module
        self.window_days = window_days

    def refresh_recent_impacts(
        self,
        conn,
        *,
        workspace_id: str,
        limit: int = 50,
    ) -> PromotionImpactRefreshResult:
        executions = self.repo.list_recent_promotion_executions(
            conn,
            workspace_id=workspace_id,
            limit=limit,
        )
        refreshed_count = 0
        for execution_row in executions:
            execution = PromotionExecution(
                workspace_id=str(execution_row["workspace_id"]),
                promotion_type=str(execution_row["promotion_type"]),
                execution_id=str(execution_row["execution_id"]),
                scope_type=str(execution_row["scope_type"]),
                scope_value=(
                    str(execution_row["scope_value"])
                    if execution_row.get("scope_value") is not None
                    else None
                ),
                executed_at=self._to_datetime(execution_row["executed_at"]),
                metadata=dict(execution_row.get("metadata") or {}),
            )
            snapshot = self.build_snapshot(conn, execution=execution)
            if snapshot is None:
                continue
            self.repo.upsert_promotion_impact_snapshot(conn, payload=snapshot)
            refreshed_count += 1
        return PromotionImpactRefreshResult(workspace_id=workspace_id, refreshed_count=refreshed_count)

    def build_snapshot(
        self,
        conn,
        *,
        execution: PromotionExecution,
    ) -> dict[str, Any] | None:
        window = timedelta(days=self.window_days)
        pre_window_start = execution.executed_at - window
        pre_window_end = execution.executed_at
        post_window_start = execution.executed_at
        post_window_end = execution.executed_at + window

        if execution.promotion_type == "threshold":
            metrics = self.repo.get_threshold_promotion_window_metrics(
                conn,
                workspace_id=execution.workspace_id,
                scope_type=execution.scope_type,
                scope_value=execution.scope_value,
                execution_metadata=execution.metadata,
                pre_window_start=pre_window_start,
                pre_window_end=pre_window_end,
                post_window_start=post_window_start,
                post_window_end=post_window_end,
            )
        else:
            metrics = self.repo.get_routing_promotion_window_metrics(
                conn,
                workspace_id=execution.workspace_id,
                scope_type=execution.scope_type,
                scope_value=execution.scope_value,
                execution_metadata=execution.metadata,
                pre_window_start=pre_window_start,
                pre_window_end=pre_window_end,
                post_window_start=post_window_start,
                post_window_end=post_window_end,
            )

        if metrics is None:
            return None

        impact_classification = self.classify(metrics)
        rollback_risk_score = self.rollback_risk(metrics, impact_classification)

        return {
            "workspace_id": execution.workspace_id,
            "promotion_type": execution.promotion_type,
            "execution_id": execution.execution_id,
            "scope_type": execution.scope_type,
            "scope_value": execution.scope_value,
            "impact_classification": impact_classification,
            "pre_window_start": pre_window_start.isoformat(),
            "pre_window_end": pre_window_end.isoformat(),
            "post_window_start": post_window_start.isoformat(),
            "post_window_end": post_window_end.isoformat(),
            "recurrence_rate_before": metrics.get("recurrence_rate_before"),
            "recurrence_rate_after": metrics.get("recurrence_rate_after"),
            "escalation_rate_before": metrics.get("escalation_rate_before"),
            "escalation_rate_after": metrics.get("escalation_rate_after"),
            "resolution_latency_before_ms": metrics.get("resolution_latency_before_ms"),
            "resolution_latency_after_ms": metrics.get("resolution_latency_after_ms"),
            "reassignment_rate_before": metrics.get("reassignment_rate_before"),
            "reassignment_rate_after": metrics.get("reassignment_rate_after"),
            "rollback_risk_score": rollback_risk_score,
            "supporting_metrics": metrics,
        }

    def classify(self, metrics: dict[str, Any]) -> str:
        improvement_signals = 0
        degradation_signals = 0

        comparisons = [
            ("recurrence_rate_before", "recurrence_rate_after", "lower"),
            ("escalation_rate_before", "escalation_rate_after", "lower"),
            ("resolution_latency_before_ms", "resolution_latency_after_ms", "lower"),
            ("reassignment_rate_before", "reassignment_rate_after", "lower"),
        ]

        seen = 0
        for before_key, after_key, direction in comparisons:
            before_value = metrics.get(before_key)
            after_value = metrics.get(after_key)
            if before_value is None or after_value is None:
                continue
            seen += 1
            if direction == "lower":
                if after_value < before_value:
                    improvement_signals += 1
                elif after_value > before_value:
                    degradation_signals += 1

        if seen == 0:
            return "insufficient_data"
        if degradation_signals >= 2:
            return "rollback_candidate"
        if degradation_signals > improvement_signals:
            return "degraded"
        if improvement_signals > degradation_signals:
            return "improved"
        return "neutral"

    def rollback_risk(self, metrics: dict[str, Any], classification: str) -> float | None:
        if classification == "insufficient_data":
            return None

        base = {
            "improved": 0.05,
            "neutral": 0.20,
            "degraded": 0.70,
            "rollback_candidate": 0.90,
        }.get(classification, 0.20)

        recurrence_before = metrics.get("recurrence_rate_before")
        recurrence_after = metrics.get("recurrence_rate_after")
        escalation_before = metrics.get("escalation_rate_before")
        escalation_after = metrics.get("escalation_rate_after")

        if recurrence_before is not None and recurrence_after is not None and recurrence_after > recurrence_before:
            base = min(base + 0.05, 1.0)
        if escalation_before is not None and escalation_after is not None and escalation_after > escalation_before:
            base = min(base + 0.05, 1.0)

        return float(base)

    def _to_datetime(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        raise TypeError(f"unsupported datetime value: {value!r}")
