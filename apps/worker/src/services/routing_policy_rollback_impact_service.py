from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Thresholds for classification
_IMPROVEMENT_THRESHOLD = -0.04   # delta must be at least this negative to count as improved
_DEGRADATION_THRESHOLD = 0.04    # delta must be at least this positive to count as degraded
_MIN_SAMPLE_SIZE = 10            # fewer cases = insufficient_data
_MIN_DAYS_POST_ROLLBACK = 3.0    # rollback too recent = insufficient_data


@dataclass(frozen=True)
class RollbackImpactSnapshot:
    rollback_execution_id: str
    rollback_candidate_id: str
    recommendation_key: str
    scope_type: str
    scope_value: str
    target_type: str
    evaluation_window_label: str
    impact_classification: str   # improved | neutral | degraded | insufficient_data
    before_metrics: dict[str, Any]
    after_metrics: dict[str, Any]
    delta_metrics: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


class RoutingPolicyRollbackImpactService:
    """Pure-logic service for post-rollback outcome measurement and classification."""

    METRIC_KEYS = (
        "recurrence_rate",
        "reassignment_rate",
        "escalation_rate",
        "avg_resolve_latency_seconds",
        "reopen_rate",
        "workload_pressure",
    )

    # Lower is better for all metrics above (negative delta = improvement)
    LOWER_IS_BETTER = frozenset(METRIC_KEYS)

    def collect_before_metrics(
        self,
        *,
        rollback_candidate: dict[str, Any],
        analytics_snapshot: dict[str, Any] | None,
        window_label: str = "30d",
    ) -> dict[str, Any]:
        """Collect pre-rollback (during autopromotion) metrics.

        Priority order:
        1. Explicitly stored before_metrics in rollback candidate metadata
        2. analytics_snapshot passed in (caller queries live analytics for before window)
        3. Empty dict (insufficient_data will result)
        """
        candidate_meta = rollback_candidate.get("metadata") or {}
        if isinstance(candidate_meta, dict) and candidate_meta.get("before_metrics"):
            return dict(candidate_meta["before_metrics"])

        if analytics_snapshot is not None:
            return self._extract_metrics_from_snapshot(analytics_snapshot)

        return {}

    def collect_after_metrics(
        self,
        *,
        analytics_snapshot: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Collect post-rollback (current state) metrics from live analytics snapshot."""
        if analytics_snapshot is None:
            return {}
        return self._extract_metrics_from_snapshot(analytics_snapshot)

    def _extract_metrics_from_snapshot(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        metrics: dict[str, Any] = {}
        for key in self.METRIC_KEYS:
            val = snapshot.get(key)
            if val is not None:
                try:
                    metrics[key] = float(val)
                except (TypeError, ValueError):
                    pass
        # sample size for data sufficiency check
        sample = snapshot.get("case_count") or snapshot.get("sample_size")
        if sample is not None:
            try:
                metrics["sample_size"] = int(sample)
            except (TypeError, ValueError):
                pass
        return metrics

    def compute_delta_metrics(
        self,
        *,
        before_metrics: dict[str, Any],
        after_metrics: dict[str, Any],
    ) -> dict[str, Any]:
        deltas: dict[str, Any] = {}
        for key in self.METRIC_KEYS:
            before_val = before_metrics.get(key)
            after_val = after_metrics.get(key)
            if before_val is not None and after_val is not None:
                try:
                    deltas[key] = round(float(after_val) - float(before_val), 6)
                except (TypeError, ValueError):
                    pass
        return deltas

    def classify_rollback_impact(
        self,
        *,
        before_metrics: dict[str, Any],
        after_metrics: dict[str, Any],
        delta_metrics: dict[str, Any],
        days_since_rollback: float | None = None,
    ) -> str:
        # Insufficient data checks first
        after_sample = after_metrics.get("sample_size")
        if after_sample is not None and int(after_sample) < _MIN_SAMPLE_SIZE:
            return "insufficient_data"

        if days_since_rollback is not None and days_since_rollback < _MIN_DAYS_POST_ROLLBACK:
            return "insufficient_data"

        if not delta_metrics:
            return "insufficient_data"

        # Count directional signals
        improvement_signals = 0
        degradation_signals = 0
        primary_keys = (
            "recurrence_rate",
            "reassignment_rate",
            "escalation_rate",
            "avg_resolve_latency_seconds",
        )

        for key in primary_keys:
            delta = delta_metrics.get(key)
            if delta is None:
                continue
            delta = float(delta)
            if key in self.LOWER_IS_BETTER:
                if delta <= _IMPROVEMENT_THRESHOLD:
                    improvement_signals += 1
                elif delta >= _DEGRADATION_THRESHOLD:
                    degradation_signals += 1

        total_signals = improvement_signals + degradation_signals
        if total_signals == 0:
            # No meaningful deltas available — check if we have enough data
            if len(delta_metrics) < 2:
                return "insufficient_data"
            return "neutral"

        # Classify based on dominant signal
        if improvement_signals >= 2 and improvement_signals > degradation_signals:
            return "improved"
        if degradation_signals >= 2 and degradation_signals > improvement_signals:
            return "degraded"
        if improvement_signals > degradation_signals:
            return "improved"
        if degradation_signals > improvement_signals:
            return "degraded"
        return "neutral"

    def build_impact_snapshot(
        self,
        *,
        rollback_execution: dict[str, Any],
        rollback_candidate: dict[str, Any],
        before_metrics: dict[str, Any],
        after_metrics: dict[str, Any],
        days_since_rollback: float | None = None,
        evaluation_window_label: str = "30d",
    ) -> RollbackImpactSnapshot:
        delta_metrics = self.compute_delta_metrics(
            before_metrics=before_metrics,
            after_metrics=after_metrics,
        )
        classification = self.classify_rollback_impact(
            before_metrics=before_metrics,
            after_metrics=after_metrics,
            delta_metrics=delta_metrics,
            days_since_rollback=days_since_rollback,
        )

        return RollbackImpactSnapshot(
            rollback_execution_id=str(rollback_execution["id"]),
            rollback_candidate_id=str(rollback_candidate["id"]),
            recommendation_key=rollback_candidate.get("recommendation_key", ""),
            scope_type=rollback_candidate.get("scope_type", ""),
            scope_value=rollback_candidate.get("scope_value", ""),
            target_type=rollback_execution.get("execution_target", "rule"),
            evaluation_window_label=evaluation_window_label,
            impact_classification=classification,
            before_metrics=before_metrics,
            after_metrics=after_metrics,
            delta_metrics=delta_metrics,
            metadata={
                "days_since_rollback": days_since_rollback,
                "source": "routing_policy_rollback_impact_service",
                "before_sample_size": before_metrics.get("sample_size"),
                "after_sample_size": after_metrics.get("sample_size"),
            },
        )
