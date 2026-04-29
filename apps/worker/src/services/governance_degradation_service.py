from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


SEVERITY_ORDER = {
    "info": 0,
    "low": 0,
    "medium": 1,
    "high": 2,
    "critical": 3,
}


@dataclass(frozen=True)
class DegradationMemberRef:
    member_type: str
    member_key: str
    governance_alert_event_id: str | None = None
    anomaly_cluster_id: str | None = None
    job_run_id: str | None = None
    observed_at: datetime | None = None
    metadata: dict[str, Any] | None = None

    def to_row(self, *, state_id: str, workspace_id: str) -> dict[str, Any]:
        return {
            "state_id": state_id,
            "workspace_id": workspace_id,
            "governance_alert_event_id": self.governance_alert_event_id,
            "anomaly_cluster_id": self.anomaly_cluster_id,
            "job_run_id": self.job_run_id,
            "member_type": self.member_type,
            "member_key": self.member_key,
            "observed_at": self.observed_at or datetime.now(timezone.utc),
            "metadata": self.metadata or {},
        }


@dataclass(frozen=True)
class DegradationSignal:
    workspace_id: str
    watchlist_id: str | None
    degradation_type: str
    version_tuple: str
    regime: str | None
    severity: str
    first_seen_at: datetime
    last_seen_at: datetime
    recent_event_count: int
    cluster_count: int
    source_summary: dict[str, Any]
    metadata: dict[str, Any]
    members: tuple[DegradationMemberRef, ...]


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def _severity_rank(value: str | None) -> int:
    return SEVERITY_ORDER.get((value or "").lower(), 0)


def _max_severity(left: str | None, right: str | None) -> str:
    return (left or "low") if _severity_rank(left) >= _severity_rank(right) else (right or "low")


def _version_tuple_from_event(event: dict[str, Any]) -> str:
    return " / ".join(
        [
            str(event.get("compute_version") or "none"),
            str(event.get("signal_registry_version") or "none"),
            str(event.get("model_version") or "none"),
        ]
    )


def _extract_regime_from_event(event: dict[str, Any]) -> str | None:
    metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    source_row = metadata.get("source_row") if isinstance(metadata.get("source_row"), dict) else {}
    for key in ("dominant_regime", "to_regime", "regime", "source_regime", "replay_regime"):
        value = source_row.get(key)
        if value:
            return str(value)
    return None


def build_degradation_signals(
    governance_events: list[dict[str, Any]],
    anomaly_clusters: list[dict[str, Any]],
) -> list[DegradationSignal]:
    event_map = {
        str(event["id"]): event
        for event in governance_events
        if event.get("id")
    }
    signals: dict[tuple[str, str | None, str, str, str | None], dict[str, Any]] = {}

    def ensure_slot(
        *,
        workspace_id: str,
        watchlist_id: str | None,
        degradation_type: str,
        version_tuple: str,
        regime: str | None,
        severity: str,
        first_seen_at: datetime,
        last_seen_at: datetime,
        recent_event_count: int,
        cluster_count: int,
        source_summary: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        key = (workspace_id, watchlist_id, degradation_type, version_tuple, regime)
        slot = signals.get(key)
        if slot is None:
            slot = {
                "workspace_id": workspace_id,
                "watchlist_id": watchlist_id,
                "degradation_type": degradation_type,
                "version_tuple": version_tuple,
                "regime": regime,
                "severity": severity,
                "first_seen_at": first_seen_at,
                "last_seen_at": last_seen_at,
                "recent_event_count": max(1, recent_event_count),
                "cluster_count": max(0, cluster_count),
                "source_summary": dict(source_summary),
                "metadata": dict(metadata),
                "members": [],
                "_member_keys": set(),
            }
            signals[key] = slot
            return slot

        slot["severity"] = _max_severity(str(slot["severity"]), severity)
        slot["first_seen_at"] = min(slot["first_seen_at"], first_seen_at)
        slot["last_seen_at"] = max(slot["last_seen_at"], last_seen_at)
        slot["recent_event_count"] = max(int(slot["recent_event_count"]), max(1, recent_event_count))
        slot["cluster_count"] = max(int(slot["cluster_count"]), max(0, cluster_count))
        slot["source_summary"] = {**slot["source_summary"], **source_summary}
        slot["metadata"] = {**slot["metadata"], **metadata}
        return slot

    for cluster in anomaly_clusters:
        workspace_id = str(cluster["workspace_id"])
        watchlist_id = str(cluster["watchlist_id"]) if cluster.get("watchlist_id") else None
        degradation_type = str(cluster.get("alert_type") or "unknown")
        version_tuple = str(cluster.get("version_tuple") or "none / none / none")
        regime = str(cluster["regime"]) if cluster.get("regime") else None
        severity = str(cluster.get("severity") or "low")
        first_seen_at = _parse_dt(cluster.get("first_seen_at"))
        last_seen_at = _parse_dt(cluster.get("last_seen_at"))
        event_count = int(cluster.get("event_count") or 0)
        cluster_id = str(cluster["id"])

        slot = ensure_slot(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            degradation_type=degradation_type,
            version_tuple=version_tuple,
            regime=regime,
            severity=severity,
            first_seen_at=first_seen_at,
            last_seen_at=last_seen_at,
            recent_event_count=event_count,
            cluster_count=1,
            source_summary={
                "cluster_key": cluster.get("cluster_key"),
                "cluster_event_count": event_count,
                "latest_cluster_id": cluster_id,
            },
            metadata={
                "cluster_status": cluster.get("status"),
                "cluster_metadata": cluster.get("metadata") if isinstance(cluster.get("metadata"), dict) else {},
            },
        )

        cluster_member_key = f"cluster:{cluster_id}"
        if cluster_member_key not in slot["_member_keys"]:
            slot["_member_keys"].add(cluster_member_key)
            slot["members"].append(
                DegradationMemberRef(
                    member_type="anomaly_cluster",
                    member_key=cluster_member_key,
                    anomaly_cluster_id=cluster_id,
                    job_run_id=str(cluster["latest_run_id"]) if cluster.get("latest_run_id") else None,
                    observed_at=last_seen_at,
                    metadata={
                        "event_count": event_count,
                        "cluster_key": cluster.get("cluster_key"),
                    },
                )
            )

        latest_event_id = str(cluster["latest_event_id"]) if cluster.get("latest_event_id") else None
        latest_event = event_map.get(latest_event_id or "")
        if latest_event:
            event_member_key = f"event:{latest_event_id}"
            if event_member_key not in slot["_member_keys"]:
                slot["_member_keys"].add(event_member_key)
                slot["members"].append(
                    DegradationMemberRef(
                        member_type="governance_alert_event",
                        member_key=event_member_key,
                        governance_alert_event_id=latest_event_id,
                        job_run_id=str(latest_event["run_id"]) if latest_event.get("run_id") else None,
                        observed_at=_parse_dt(latest_event.get("created_at")),
                        metadata={
                            "rule_name": latest_event.get("rule_name"),
                            "message": (latest_event.get("metadata") or {}).get("message")
                            if isinstance(latest_event.get("metadata"), dict)
                            else None,
                        },
                    )
                )

    for event in governance_events:
        workspace_id = str(event["workspace_id"])
        watchlist_id = str(event["watchlist_id"]) if event.get("watchlist_id") else None
        degradation_type = str(event.get("event_type") or "unknown")
        version_tuple = _version_tuple_from_event(event)
        regime = _extract_regime_from_event(event)
        severity = str(event.get("severity") or "info")
        created_at = _parse_dt(event.get("created_at"))
        event_id = str(event["id"]) if event.get("id") else None
        slot = ensure_slot(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            degradation_type=degradation_type,
            version_tuple=version_tuple,
            regime=regime,
            severity=severity,
            first_seen_at=created_at,
            last_seen_at=created_at,
            recent_event_count=1,
            cluster_count=0,
            source_summary={
                "last_rule_name": event.get("rule_name"),
                "metric_source": event.get("metric_source"),
                "metric_name": event.get("metric_name"),
            },
            metadata={
                "event_message": (event.get("metadata") or {}).get("message")
                if isinstance(event.get("metadata"), dict)
                else None,
            },
        )
        if event_id:
            event_member_key = f"event:{event_id}"
            if event_member_key not in slot["_member_keys"]:
                slot["_member_keys"].add(event_member_key)
                slot["members"].append(
                    DegradationMemberRef(
                        member_type="governance_alert_event",
                        member_key=event_member_key,
                        governance_alert_event_id=event_id,
                        job_run_id=str(event["run_id"]) if event.get("run_id") else None,
                        observed_at=created_at,
                        metadata={
                            "rule_name": event.get("rule_name"),
                            "message": (event.get("metadata") or {}).get("message")
                            if isinstance(event.get("metadata"), dict)
                            else None,
                        },
                    )
                )

    signals_out: list[DegradationSignal] = []
    for slot in signals.values():
        signals_out.append(
            DegradationSignal(
                workspace_id=slot["workspace_id"],
                watchlist_id=slot["watchlist_id"],
                degradation_type=slot["degradation_type"],
                version_tuple=slot["version_tuple"],
                regime=slot["regime"],
                severity=slot["severity"],
                first_seen_at=slot["first_seen_at"],
                last_seen_at=slot["last_seen_at"],
                recent_event_count=int(slot["recent_event_count"]),
                cluster_count=int(slot["cluster_count"]),
                source_summary=slot["source_summary"],
                metadata=slot["metadata"],
                members=tuple(slot["members"]),
            )
        )
    return signals_out


class GovernanceDegradationService:
    """Stateful chronic degradation logic layered on top of governance events."""

    def __init__(
        self,
        open_event_threshold: int = 3,
        escalate_event_threshold: int = 5,
        escalation_age_hours: int = 24,
        recovery_quiet_hours: int = 12,
    ) -> None:
        self.open_event_threshold = open_event_threshold
        self.escalate_event_threshold = escalate_event_threshold
        self.escalation_age_hours = escalation_age_hours
        self.recovery_quiet_hours = recovery_quiet_hours

    def evaluate_signal(
        self,
        signal: DegradationSignal,
        active_state: dict[str, Any] | None,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        now = now or datetime.now(timezone.utc)
        recent_event_count = signal.recent_event_count
        cluster_count = signal.cluster_count

        if active_state:
            recent_event_count = max(recent_event_count, int(active_state.get("event_count") or 0))
            cluster_count = max(cluster_count, int(active_state.get("cluster_count") or 0))

        if recent_event_count < self.open_event_threshold and not active_state:
            return None

        if active_state:
            action = "touch"
            state_status = str(active_state.get("state_status") or "active")
            first_seen_at = _parse_dt(active_state.get("first_seen_at"))
            severity = _max_severity(str(active_state.get("severity") or "low"), signal.severity)
        else:
            action = "open"
            state_status = "active"
            first_seen_at = signal.first_seen_at
            severity = signal.severity

        escalated_at = active_state.get("escalated_at") if active_state else None
        state_age = now - first_seen_at
        if recent_event_count >= self.escalate_event_threshold or state_age >= timedelta(hours=self.escalation_age_hours):
            state_status = "escalated"
            severity = _max_severity(severity, "high")
            if not active_state or not active_state.get("escalated_at"):
                escalated_at = now
            if action != "open":
                action = "escalate"

        return {
            "action": action,
            "workspace_id": signal.workspace_id,
            "watchlist_id": signal.watchlist_id,
            "degradation_type": signal.degradation_type,
            "version_tuple": signal.version_tuple,
            "regime": signal.regime,
            "state_status": state_status,
            "severity": severity,
            "first_seen_at": first_seen_at,
            "last_seen_at": signal.last_seen_at,
            "escalated_at": escalated_at,
            "event_count": recent_event_count,
            "cluster_count": cluster_count,
            "source_summary": {
                **signal.source_summary,
                "recent_event_count": recent_event_count,
                "cluster_count": cluster_count,
                "open_event_threshold": self.open_event_threshold,
                "escalate_event_threshold": self.escalate_event_threshold,
            },
            "metadata": {
                **signal.metadata,
                "degradation_service": {
                    "action": action,
                    "recovery_quiet_hours": self.recovery_quiet_hours,
                    "escalation_age_hours": self.escalation_age_hours,
                },
            },
        }

    def evaluate_recovery(
        self,
        active_state: dict[str, Any],
        *,
        trailing_metrics: dict[str, Any],
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        now = now or datetime.now(timezone.utc)
        last_seen_at = _parse_dt(active_state.get("last_seen_at"))
        quiet_cutoff = now - timedelta(hours=self.recovery_quiet_hours)
        if last_seen_at > quiet_cutoff:
            return None

        return {
            "recovery_reason": "quiet_window_normalized",
            "resolved_at": now,
            "trailing_metrics": {
                **trailing_metrics,
                "quiet_cutoff": quiet_cutoff.isoformat(),
                "recovery_quiet_hours": self.recovery_quiet_hours,
            },
        }
