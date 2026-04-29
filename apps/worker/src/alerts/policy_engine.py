from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def build_job_event_payload(
    job_run_id: str,
    outcome: str,
    message: str,
    lineage: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "job_run_id": job_run_id,
        "outcome": outcome,
        "message": message,
        "lineage": _json_safe(lineage or {}),
    }
    if extra:
        payload.update(_json_safe(extra))
    return payload


def build_terminal_alert_payload(
    job_run_id: str,
    outcome: str,
    message: str,
    lineage: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return build_job_event_payload(job_run_id, outcome, message, lineage, extra)
