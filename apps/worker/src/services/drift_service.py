from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

DRIFT_VERSION = "phase2.5B/v1"


@dataclass(frozen=True)
class DriftMetricRow:
    metric_type: str
    entity_name: str
    current_value: float | None
    baseline_value: float | None
    delta_abs: float | None
    delta_pct: float | None
    z_score: float | None
    drift_flag: bool
    severity: str
    metadata: dict[str, Any]


def _severity_for_delta(delta_pct: float | None, drift_flag: bool) -> str:
    if not drift_flag or delta_pct is None:
        return "low"
    abs_pct = abs(delta_pct)
    if abs_pct >= 50:
        return "high"
    if abs_pct >= 20:
        return "medium"
    return "low"


def _calculate_delta(current_value: float | None, baseline_value: float | None) -> tuple[float | None, float | None, bool]:
    if current_value is None and baseline_value is None:
        return None, None, False
    if current_value is None or baseline_value is None:
        return None, None, True

    delta_abs = float(current_value - baseline_value)
    if baseline_value == 0:
        if current_value == 0:
            return delta_abs, 0.0, False
        return delta_abs, 100.0, True

    delta_pct = float((delta_abs / abs(baseline_value)) * 100)
    return delta_abs, delta_pct, abs(delta_pct) >= 20


def _extract_dominant_regime(run_context: dict[str, Any]) -> str | None:
    regime_summary = run_context.get("regime_summary") or {}
    regime_counts = regime_summary.get("regime_counts") if isinstance(regime_summary, dict) else None
    if not isinstance(regime_counts, dict) or not regime_counts:
        return None
    return str(max(regime_counts.items(), key=lambda item: item[1])[0])


def _top_flagged(metrics: list[DriftMetricRow]) -> list[dict[str, Any]]:
    flagged = [metric for metric in metrics if metric.drift_flag]
    flagged.sort(
        key=lambda metric: (
            0 if metric.severity == "high" else 1 if metric.severity == "medium" else 2,
            -(abs(metric.delta_pct) if metric.delta_pct is not None else 0.0),
        )
    )
    return [
        {
            "metric_type": metric.metric_type,
            "entity_name": metric.entity_name,
            "severity": metric.severity,
            "delta_pct": metric.delta_pct,
        }
        for metric in flagged[:5]
    ]


def build_run_drift(
    current_run: dict[str, Any],
    comparison_run: dict[str, Any] | None,
    current_family_rows: list[dict[str, Any]],
    comparison_family_rows: list[dict[str, Any]],
    current_signal_rows: list[dict[str, Any]],
    comparison_signal_rows: list[dict[str, Any]],
) -> tuple[list[DriftMetricRow], dict[str, Any]]:
    metrics: list[DriftMetricRow] = []
    has_comparison = comparison_run is not None

    current_composite = float(current_run["composite_score"]) if current_run.get("composite_score") is not None else None
    baseline_composite = (
        float(comparison_run["composite_score"])
        if comparison_run and comparison_run.get("composite_score") is not None
        else None
    )
    composite_delta_abs, composite_delta_pct, composite_flag = _calculate_delta(current_composite, baseline_composite)
    if not has_comparison:
        composite_flag = False
    metrics.append(
        DriftMetricRow(
            metric_type="composite",
            entity_name="composite_score",
            current_value=current_composite,
            baseline_value=baseline_composite,
            delta_abs=composite_delta_abs,
            delta_pct=composite_delta_pct,
            z_score=None,
            drift_flag=composite_flag,
            severity=_severity_for_delta(composite_delta_pct, composite_flag),
            metadata={
                "current_regime": _extract_dominant_regime(current_run),
                "baseline_regime": _extract_dominant_regime(comparison_run or {}),
            },
        )
    )

    current_regime = _extract_dominant_regime(current_run)
    baseline_regime = _extract_dominant_regime(comparison_run or {})
    regime_changed = has_comparison and current_regime != baseline_regime
    metrics.append(
        DriftMetricRow(
            metric_type="regime",
            entity_name="dominant_regime",
            current_value=None,
            baseline_value=None,
            delta_abs=None,
            delta_pct=None,
            z_score=None,
            drift_flag=regime_changed,
            severity="medium" if regime_changed else "low",
            metadata={
                "current_regime": current_regime,
                "baseline_regime": baseline_regime,
                "current_regime_counts": current_run.get("regime_summary", {}).get("regime_counts", {}),
                "baseline_regime_counts": (comparison_run or {}).get("regime_summary", {}).get("regime_counts", {}),
            },
        )
    )

    current_families = {str(row["signal_family"]): row for row in current_family_rows}
    baseline_families = {str(row["signal_family"]): row for row in comparison_family_rows}
    for family_name in sorted(set(current_families) | set(baseline_families)):
        current_row = current_families.get(family_name, {})
        baseline_row = baseline_families.get(family_name, {})
        current_value = float(current_row["family_score"]) if current_row.get("family_score") is not None else None
        baseline_value = float(baseline_row["family_score"]) if baseline_row.get("family_score") is not None else None
        delta_abs, delta_pct, drift_flag = _calculate_delta(current_value, baseline_value)
        if not has_comparison:
            drift_flag = False
        metrics.append(
            DriftMetricRow(
                metric_type="family",
                entity_name=family_name,
                current_value=current_value,
                baseline_value=baseline_value,
                delta_abs=delta_abs,
                delta_pct=delta_pct,
                z_score=None,
                drift_flag=drift_flag,
                severity=_severity_for_delta(delta_pct, drift_flag),
                metadata={
                    "current_rank": current_row.get("family_rank"),
                    "baseline_rank": baseline_row.get("family_rank"),
                    "current_invalidators": current_row.get("active_invalidators", []),
                    "baseline_invalidators": baseline_row.get("active_invalidators", []),
                },
            )
        )

    current_signals = {
        f"{row.get('asset_symbol') or 'unknown'}:{row['signal_name']}": row for row in current_signal_rows
    }
    baseline_signals = {
        f"{row.get('asset_symbol') or 'unknown'}:{row['signal_name']}": row for row in comparison_signal_rows
    }
    for entity_name in sorted(set(current_signals) | set(baseline_signals)):
        current_row = current_signals.get(entity_name, {})
        baseline_row = baseline_signals.get(entity_name, {})
        current_value = (
            float(current_row["contribution_value"])
            if current_row.get("contribution_value") is not None
            else None
        )
        baseline_value = (
            float(baseline_row["contribution_value"])
            if baseline_row.get("contribution_value") is not None
            else None
        )
        delta_abs, delta_pct, drift_flag = _calculate_delta(current_value, baseline_value)
        if not has_comparison:
            drift_flag = False
        metrics.append(
            DriftMetricRow(
                metric_type="signal",
                entity_name=entity_name,
                current_value=current_value,
                baseline_value=baseline_value,
                delta_abs=delta_abs,
                delta_pct=delta_pct,
                z_score=None,
                drift_flag=drift_flag,
                severity=_severity_for_delta(delta_pct, drift_flag),
                metadata={
                    "signal_name": current_row.get("signal_name") or baseline_row.get("signal_name"),
                    "signal_family": current_row.get("signal_family") or baseline_row.get("signal_family"),
                    "regime": current_row.get("regime") or baseline_row.get("regime"),
                    "current_invalidators": current_row.get("active_invalidators", []),
                    "baseline_invalidators": baseline_row.get("active_invalidators", []),
                },
            )
        )

    overall_severity = "low"
    if any(metric.severity == "high" and metric.drift_flag for metric in metrics):
        overall_severity = "high"
    elif any(metric.severity == "medium" and metric.drift_flag for metric in metrics):
        overall_severity = "medium"

    summary = {
        "drift_version": DRIFT_VERSION,
        "comparison_run_id": comparison_run.get("id") if comparison_run else None,
        "metric_count": len(metrics),
        "flagged_metric_count": sum(1 for metric in metrics if metric.drift_flag),
        "composite_score": current_composite,
        "baseline_composite_score": baseline_composite,
        "composite_delta_pct": composite_delta_pct,
        "regime": current_regime,
        "baseline_regime": baseline_regime,
        "regime_changed": regime_changed,
        "current_versions": {
            "compute_version": current_run.get("compute_version"),
            "signal_registry_version": current_run.get("signal_registry_version"),
            "model_version": current_run.get("model_version"),
        },
        "baseline_versions": {
            "compute_version": comparison_run.get("compute_version") if comparison_run else None,
            "signal_registry_version": comparison_run.get("signal_registry_version") if comparison_run else None,
            "model_version": comparison_run.get("model_version") if comparison_run else None,
        },
        "top_flagged": _top_flagged(metrics),
    }
    return metrics, {"severity": overall_severity, "summary": summary}


def serialize_drift_metrics(rows: list[DriftMetricRow]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]
