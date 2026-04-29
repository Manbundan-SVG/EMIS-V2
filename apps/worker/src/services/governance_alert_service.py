from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


CLASSIFICATION_ORDER = {
    "stable": 0,
    "watch": 1,
    "unstable": 2,
    "critical": 3,
}


@dataclass(frozen=True)
class GovernanceAlertCandidate:
    workspace_id: str
    watchlist_id: str | None
    run_id: str | None
    rule_name: str
    event_type: str
    severity: str
    metric_source: str
    metric_name: str
    metric_value_numeric: float | None
    metric_value_text: str | None
    threshold_numeric: float | None
    threshold_text: str | None
    compute_version: str | None
    signal_registry_version: str | None
    model_version: str | None
    dedupe_key: str
    metadata: dict[str, Any]


def _numeric(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _compare_numeric(value: float | None, comparator: str, threshold: float | None) -> bool:
    if value is None or threshold is None:
        return False
    if comparator == "gt":
        return value > threshold
    if comparator == "gte":
        return value >= threshold
    if comparator == "lt":
        return value < threshold
    if comparator == "lte":
        return value <= threshold
    if comparator == "eq":
        return value == threshold
    if comparator == "neq":
        return value != threshold
    return False


def _compare_text(value: str | None, comparator: str, threshold: str | None) -> bool:
    if value is None or threshold is None:
        return False

    if value in CLASSIFICATION_ORDER and threshold in CLASSIFICATION_ORDER and comparator in {"gt", "gte", "lt", "lte"}:
        value_rank = CLASSIFICATION_ORDER[value]
        threshold_rank = CLASSIFICATION_ORDER[threshold]
        if comparator == "gt":
            return value_rank > threshold_rank
        if comparator == "gte":
            return value_rank >= threshold_rank
        if comparator == "lt":
            return value_rank < threshold_rank
        if comparator == "lte":
            return value_rank <= threshold_rank

    if comparator == "eq":
        return value == threshold
    if comparator == "neq":
        return value != threshold
    return False


def _bucket_index(evaluated_at: datetime, cooldown_seconds: int) -> int:
    if cooldown_seconds <= 0:
        return 0
    return int(evaluated_at.timestamp()) // cooldown_seconds


def _dedupe_key(
    *,
    rule_name: str,
    event_type: str,
    workspace_id: str,
    watchlist_id: str | None,
    compute_version: str | None,
    signal_registry_version: str | None,
    model_version: str | None,
    bucket: int,
) -> str:
    return ":".join(
        [
            "governance",
            rule_name,
            event_type,
            workspace_id,
            watchlist_id or "all",
            compute_version or "none",
            signal_registry_version or "none",
            model_version or "none",
            str(bucket),
        ]
    )


def _build_message(event_type: str, row: dict[str, Any], metric_name: str) -> str:
    compute_version = row.get("compute_version") or "none"
    signal_registry_version = row.get("signal_registry_version") or "none"
    model_version = row.get("model_version") or "none"
    tuple_label = f"{compute_version} / {signal_registry_version} / {model_version}"
    value = row.get(metric_name)

    if event_type == "version_regression":
        return f"Version tuple {tuple_label} governance health regressed to {value}."
    if event_type == "replay_degradation":
        return f"Replay consistency degraded for {tuple_label}; {metric_name}={value}."
    if event_type == "family_instability_spike":
        return f"Family instability spiked on the latest run for {tuple_label}; {metric_name}={value}."
    if event_type == "regime_instability_spike":
        return f"Regime instability spiked on the latest run for {tuple_label}; {metric_name}={value}."
    if event_type == "regime_conflict_persistence":
        return f"Conflicting regime transitions persisted for {tuple_label}; {metric_name}={value}."
    if event_type == "stability_classification_downgrade":
        return f"Latest stability classification downgraded for {tuple_label}; {metric_name}={value}."
    return f"Governance alert triggered for {tuple_label}; {metric_name}={value}."


def evaluate_governance_alerts(
    *,
    workspace_id: str,
    rules: list[dict[str, Any]],
    latest_stability_row: dict[str, Any] | None = None,
    version_health_row: dict[str, Any] | None = None,
    version_replay_row: dict[str, Any] | None = None,
    version_regime_row: dict[str, Any] | None = None,
    evaluated_at: datetime | None = None,
) -> list[GovernanceAlertCandidate]:
    evaluated_at = evaluated_at or datetime.now(timezone.utc)
    source_rows = {
        "latest_stability_summary": [latest_stability_row] if latest_stability_row else [],
        "version_health_rankings": [version_health_row] if version_health_row else [],
        "version_replay_consistency_summary": [version_replay_row] if version_replay_row else [],
        "version_regime_behavior_summary": [version_regime_row] if version_regime_row else [],
    }

    candidates: list[GovernanceAlertCandidate] = []

    for rule in rules:
        metric_source = str(rule["metric_source"])
        metric_name = str(rule["metric_name"])
        comparator = str(rule["comparator"])
        threshold_numeric = _numeric(rule.get("threshold_numeric"))
        threshold_text = str(rule["threshold_text"]) if rule.get("threshold_text") is not None else None

        for row in source_rows.get(metric_source, []):
            raw_value = row.get(metric_name)
            numeric_value = _numeric(raw_value)
            text_value = raw_value if isinstance(raw_value, str) else None

            matched = False
            if threshold_numeric is not None:
                matched = _compare_numeric(numeric_value, comparator, threshold_numeric)
            elif threshold_text is not None:
                matched = _compare_text(text_value, comparator, threshold_text)

            if not matched:
                continue

            cooldown_seconds = int(rule.get("cooldown_seconds") or 0)
            bucket = _bucket_index(evaluated_at, cooldown_seconds)
            metadata = {
                "rule_name": rule["rule_name"],
                "message": _build_message(str(rule["event_type"]), row, metric_name),
                "rule_metadata": rule.get("metadata") or {},
                "source_row": row,
                "evaluated_at": evaluated_at.isoformat(),
            }
            candidates.append(
                GovernanceAlertCandidate(
                    workspace_id=workspace_id,
                    watchlist_id=str(row["watchlist_id"]) if row.get("watchlist_id") else None,
                    run_id=str(row["run_id"]) if row.get("run_id") else None,
                    rule_name=str(rule["rule_name"]),
                    event_type=str(rule["event_type"]),
                    severity=str(rule["severity"]),
                    metric_source=metric_source,
                    metric_name=metric_name,
                    metric_value_numeric=numeric_value,
                    metric_value_text=text_value,
                    threshold_numeric=threshold_numeric,
                    threshold_text=threshold_text,
                    compute_version=str(row["compute_version"]) if row.get("compute_version") is not None else None,
                    signal_registry_version=str(row["signal_registry_version"]) if row.get("signal_registry_version") is not None else None,
                    model_version=str(row["model_version"]) if row.get("model_version") is not None else None,
                    dedupe_key=_dedupe_key(
                        rule_name=str(rule["rule_name"]),
                        event_type=str(rule["event_type"]),
                        workspace_id=workspace_id,
                        watchlist_id=str(row["watchlist_id"]) if row.get("watchlist_id") else None,
                        compute_version=str(row["compute_version"]) if row.get("compute_version") is not None else None,
                        signal_registry_version=str(row["signal_registry_version"]) if row.get("signal_registry_version") is not None else None,
                        model_version=str(row["model_version"]) if row.get("model_version") is not None else None,
                        bucket=bucket,
                    ),
                    metadata=metadata,
                )
            )

    return candidates
