from __future__ import annotations


def choose_default_assignee(
    *,
    severity: str,
    current_assignee: str | None = None,
    current_team: str | None = None,
) -> tuple[str | None, str | None]:
    if current_assignee or current_team:
        return current_assignee, current_team
    if severity == "critical":
        return None, "platform"
    if severity == "high":
        return None, "research"
    if severity == "medium":
        return None, "triage"
    return None, "ops"
