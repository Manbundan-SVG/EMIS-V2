from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence


@dataclass(frozen=True)
class AcknowledgeRequest:
    workspace_id: str
    degradation_state_id: str
    acknowledged_by: str
    note: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class MuteRequest:
    workspace_id: str
    target_type: str
    target_key: str
    created_by: str
    muted_until: datetime | None = None
    reason: str | None = None
    metadata: dict[str, Any] | None = None


def build_version_tuple(
    compute_version: str | None,
    signal_registry_version: str | None,
    model_version: str | None,
) -> str:
    return "|".join(
        [
            compute_version or "none",
            signal_registry_version or "none",
            model_version or "none",
        ]
    )


def _rule_is_active(rule: dict[str, Any], now: datetime) -> bool:
    if not bool(rule.get("is_active", True)):
        return False
    muted_until = rule.get("muted_until")
    if muted_until is None:
        return True
    if isinstance(muted_until, datetime):
        return muted_until > now
    return True


def find_matching_mute_rule(
    candidate: dict[str, Any],
    rules: Sequence[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    now = now or datetime.now(timezone.utc)
    watchlist_id = candidate.get("watchlist_id")
    event_type = candidate.get("event_type")
    version_tuple = build_version_tuple(
        candidate.get("compute_version"),
        candidate.get("signal_registry_version"),
        candidate.get("model_version"),
    )

    for rule in rules:
        if not _rule_is_active(rule, now):
            continue

        target_type = str(rule.get("target_type") or "")
        target_key = str(rule.get("target_key") or "")

        if target_type == "event_type" and target_key == str(event_type):
            return rule
        if target_type == "watchlist_id" and watchlist_id and target_key == str(watchlist_id):
            return rule
        if target_type == "version_tuple" and target_key == version_tuple:
            return rule
        if target_type == "all" and target_key in {"*", "all"}:
            return rule

    return None
