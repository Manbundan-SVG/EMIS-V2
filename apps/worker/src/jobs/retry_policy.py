from __future__ import annotations

from typing import Any


def classify_retry_outcome(result: dict[str, Any]) -> str:
    action = result.get("action", "unknown")
    if action == "retry":
        return "retry_scheduled"
    if action == "dead_letter":
        return "dead_lettered"
    return "unknown"


def retry_scheduled_message(job_type: str, next_retry_at: str | None) -> tuple[str, str, str]:
    when = f"Next retry at {next_retry_at}" if next_retry_at else "Retry scheduled with backoff."
    return (
        "job.retry",
        f"{job_type} retry scheduled",
        when,
    )


def terminal_failure_message(job_type: str, error: str) -> tuple[str, str, str]:
    return (
        "job.dead_letter",
        f"{job_type} dead-lettered",
        error[:500],
    )
