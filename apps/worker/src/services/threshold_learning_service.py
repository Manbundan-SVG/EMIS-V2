from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ThresholdOutcomeContext:
    workspace_id: str
    watchlist_id: str | None
    threshold_profile_id: str | None
    event_type: str
    regime: str | None
    compute_version: str | None
    signal_registry_version: str | None
    model_version: str | None
    case_id: str | None
    degradation_state_id: str | None
    threshold_applied_value: float | None
    acknowledged: bool
    muted: bool
    escalated: bool
    resolved: bool
    reopened: bool
    evidence: dict[str, Any]


class ThresholdLearningService:
    """Advisory threshold-learning helper.

    This service is intentionally non-destructive. It records downstream case
    outcomes and emits recommendations for operator review instead of mutating
    live threshold profiles directly.
    """

    def build_feedback_row(self, ctx: ThresholdOutcomeContext) -> dict[str, Any]:
        precision_proxy = self._compute_precision_proxy(ctx)
        noise_score = self._compute_noise_score(ctx)
        return {
            "workspace_id": ctx.workspace_id,
            "watchlist_id": ctx.watchlist_id,
            "threshold_profile_id": ctx.threshold_profile_id,
            "event_type": ctx.event_type,
            "regime": ctx.regime,
            "compute_version": ctx.compute_version,
            "signal_registry_version": ctx.signal_registry_version,
            "model_version": ctx.model_version,
            "case_id": ctx.case_id,
            "degradation_state_id": ctx.degradation_state_id,
            "threshold_applied_value": ctx.threshold_applied_value,
            "trigger_count": 1,
            "ack_count": 1 if ctx.acknowledged else 0,
            "mute_count": 1 if ctx.muted else 0,
            "escalation_count": 1 if ctx.escalated else 0,
            "resolution_count": 1 if ctx.resolved else 0,
            "reopen_count": 1 if ctx.reopened else 0,
            "precision_proxy": precision_proxy,
            "noise_score": noise_score,
            "evidence": ctx.evidence or {},
        }

    def build_recommendations(
        self,
        summaries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for summary in summaries:
            feedback_rows = int(summary.get("feedback_rows") or 0)
            if feedback_rows < 3:
                continue

            avg_noise = float(summary.get("avg_noise_score") or 0)
            avg_precision = float(summary.get("avg_precision_proxy") or 0)
            escalation_count = int(summary.get("escalation_count") or 0)
            reopen_count = int(summary.get("reopen_count") or 0)
            metrics = {
                "feedback_rows": feedback_rows,
                "support_count": feedback_rows,
                "avg_noise_score": avg_noise,
                "avg_precision_proxy": avg_precision,
                "trigger_count": int(summary.get("trigger_count") or 0),
                "ack_count": int(summary.get("ack_count") or 0),
                "mute_count": int(summary.get("mute_count") or 0),
                "escalation_count": escalation_count,
                "resolution_count": int(summary.get("resolution_count") or 0),
                "reopen_count": reopen_count,
            }

            threshold_profile_id = None
            threshold_profile_key = summary.get("threshold_profile_key")
            if threshold_profile_key and threshold_profile_key != "unscoped":
                threshold_profile_id = str(threshold_profile_key)
            current_value = self._coerce_numeric(summary.get("current_value"))

            recommendation: dict[str, Any]
            if avg_noise >= 0.7:
                recommendation = self._recommendation(
                    workspace_id=str(summary["workspace_id"]),
                    threshold_profile_id=threshold_profile_id,
                    dimension_type="regime",
                    dimension_value=str(summary.get("regime") or "any"),
                    event_type=str(summary["event_type"]),
                    current_value=current_value,
                    recommended_value=self._recommend_threshold_value(
                        event_type=str(summary["event_type"]),
                        direction="loosen",
                        current_value=current_value,
                        step=0.05,
                    ),
                    direction="loosen",
                    reason_code="high_noise",
                    confidence=min(0.95, 0.55 + feedback_rows * 0.05),
                    supporting_metrics=metrics,
                )
            elif avg_precision <= 0.2 and escalation_count == 0 and reopen_count == 0:
                recommendation = self._recommendation(
                    workspace_id=str(summary["workspace_id"]),
                    threshold_profile_id=threshold_profile_id,
                    dimension_type="regime",
                    dimension_value=str(summary.get("regime") or "any"),
                    event_type=str(summary["event_type"]),
                    current_value=current_value,
                    recommended_value=self._recommend_threshold_value(
                        event_type=str(summary["event_type"]),
                        direction="loosen",
                        current_value=current_value,
                        step=0.03,
                    ),
                    direction="loosen",
                    reason_code="under_supported",
                    confidence=min(0.85, 0.45 + feedback_rows * 0.04),
                    supporting_metrics=metrics,
                )
            elif avg_precision >= 0.75 and reopen_count > 0:
                recommendation = self._recommendation(
                    workspace_id=str(summary["workspace_id"]),
                    threshold_profile_id=threshold_profile_id,
                    dimension_type="regime",
                    dimension_value=str(summary.get("regime") or "any"),
                    event_type=str(summary["event_type"]),
                    current_value=current_value,
                    recommended_value=self._recommend_threshold_value(
                        event_type=str(summary["event_type"]),
                        direction="tighten",
                        current_value=current_value,
                        step=0.03,
                    ),
                    direction="tighten",
                    reason_code="high_reopen_precision",
                    confidence=min(0.9, 0.5 + feedback_rows * 0.04),
                    supporting_metrics=metrics,
                )
            else:
                recommendation = self._recommendation(
                    workspace_id=str(summary["workspace_id"]),
                    threshold_profile_id=threshold_profile_id,
                    dimension_type="regime",
                    dimension_value=str(summary.get("regime") or "any"),
                    event_type=str(summary["event_type"]),
                    current_value=current_value,
                    recommended_value=current_value,
                    direction="keep",
                    reason_code="keep_current",
                    confidence=min(0.9, 0.5 + feedback_rows * 0.03),
                    supporting_metrics=metrics,
                )

            rows.append(recommendation)

        return rows

    def _recommendation(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "workspace_id": kwargs["workspace_id"],
            "threshold_profile_id": kwargs["threshold_profile_id"],
            "dimension_type": kwargs["dimension_type"],
            "dimension_value": kwargs["dimension_value"],
            "event_type": kwargs["event_type"],
            "current_value": kwargs["current_value"],
            "recommended_value": kwargs["recommended_value"],
            "direction": kwargs["direction"],
            "reason_code": kwargs["reason_code"],
            "confidence": kwargs["confidence"],
            "supporting_metrics": kwargs["supporting_metrics"],
        }

    def _compute_precision_proxy(self, ctx: ThresholdOutcomeContext) -> float:
        score = 0.0
        if ctx.acknowledged:
            score += 0.2
        if ctx.escalated:
            score += 0.2
        if ctx.resolved:
            score += 0.3
        if ctx.reopened:
            score += 0.3
        return min(1.0, score)

    def _compute_noise_score(self, ctx: ThresholdOutcomeContext) -> float:
        score = 0.0
        if ctx.muted:
            score += 0.5
        if not ctx.acknowledged:
            score += 0.2
        if not ctx.escalated and not ctx.resolved:
            score += 0.3
        return min(1.0, score)

    def _coerce_numeric(self, value: Any) -> float | None:
        if value is None:
            return None
        return float(value)

    def _recommend_threshold_value(
        self,
        *,
        event_type: str,
        direction: str,
        current_value: float | None,
        step: float,
    ) -> float | None:
        if current_value is None or direction == "keep":
            return current_value

        metric_mode = self._threshold_metric_mode(event_type)
        if metric_mode is None:
            return current_value

        if metric_mode == "floor":
            next_value = current_value - step if direction == "loosen" else current_value + step
        else:
            next_value = current_value + step if direction == "loosen" else current_value - step

        return round(min(max(next_value, 0.0), 1.0), 4)

    def _threshold_metric_mode(self, event_type: str) -> str | None:
        if event_type in {"version_regression", "replay_degradation"}:
            return "floor"
        if event_type in {
            "family_instability_spike",
            "stability_classification_downgrade",
            "regime_conflict_persistence",
            "regime_instability_spike",
        }:
            return "ceiling"
        return None
