from __future__ import annotations

from dataclasses import dataclass
from typing import Any


SEVERITY_RANK = {
    "info": 0,
    "low": 0,
    "medium": 1,
    "high": 2,
    "critical": 2,
}


@dataclass(frozen=True)
class ClusterCandidate:
    workspace_id: str
    watchlist_id: str | None
    version_tuple: str
    cluster_key: str
    alert_type: str
    regime: str | None
    severity: str
    governance_alert_event_id: str | None
    run_id: str | None
    metadata: dict[str, Any]


def _cluster_severity(severity: str | None) -> str:
    rank = SEVERITY_RANK.get((severity or "").lower(), 0)
    if rank >= 2:
        return "high"
    if rank == 1:
        return "medium"
    return "low"


def _version_tuple(event: dict[str, Any]) -> str:
    return " / ".join(
        [
            str(event.get("compute_version") or "none"),
            str(event.get("signal_registry_version") or "none"),
            str(event.get("model_version") or "none"),
        ]
    )


def _extract_regime(event: dict[str, Any]) -> str | None:
    metadata = event.get("metadata") or {}
    if not isinstance(metadata, dict):
        return None
    source_row = metadata.get("source_row") or {}
    if not isinstance(source_row, dict):
        source_row = {}
    for key in ("dominant_regime", "to_regime", "regime", "source_regime", "replay_regime"):
        value = source_row.get(key)
        if value:
            return str(value)
    return None


def _cluster_key(event: dict[str, Any], regime: str | None) -> str:
    return "::".join(
        [
            _version_tuple(event),
            str(event.get("event_type") or "unknown"),
            regime or "none",
            str(event.get("watchlist_id") or "all"),
        ]
    )


def build_cluster_candidates(events: list[dict[str, Any]]) -> list[ClusterCandidate]:
    candidates: list[ClusterCandidate] = []
    for event in events:
        regime = _extract_regime(event)
        metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
        candidates.append(
            ClusterCandidate(
                workspace_id=str(event["workspace_id"]),
                watchlist_id=str(event["watchlist_id"]) if event.get("watchlist_id") else None,
                version_tuple=_version_tuple(event),
                cluster_key=_cluster_key(event, regime),
                alert_type=str(event["event_type"]),
                regime=regime,
                severity=_cluster_severity(str(event.get("severity") or "info")),
                governance_alert_event_id=str(event["id"]) if event.get("id") else None,
                run_id=str(event["run_id"]) if event.get("run_id") else None,
                metadata={
                    "rule_name": event.get("rule_name"),
                    "metric_source": event.get("metric_source"),
                    "metric_name": event.get("metric_name"),
                    "metric_value_numeric": event.get("metric_value_numeric"),
                    "metric_value_text": event.get("metric_value_text"),
                    "threshold_numeric": event.get("threshold_numeric"),
                    "threshold_text": event.get("threshold_text"),
                    "message": metadata.get("message"),
                    "source_row": metadata.get("source_row"),
                },
            )
        )
    return candidates
