from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GovernanceCaseSeed:
    workspace_id: str
    degradation_state_id: str | None
    watchlist_id: str | None
    version_tuple: str | None
    severity: str
    title: str
    summary: str
    metadata: dict[str, Any]


def build_case_seed(
    *,
    workspace_id: str,
    degradation_state_id: str | None,
    watchlist_id: str | None,
    version_tuple: str | None,
    degradation_type: str,
    severity: str,
    source_summary: dict[str, Any] | None,
) -> GovernanceCaseSeed:
    pretty_type = degradation_type.replace("_", " ").title()
    summary = (
        (source_summary or {}).get("message")
        or f"{pretty_type} requires operator review."
    )
    return GovernanceCaseSeed(
        workspace_id=workspace_id,
        degradation_state_id=degradation_state_id,
        watchlist_id=watchlist_id,
        version_tuple=version_tuple,
        severity=severity,
        title=f"{pretty_type} case",
        summary=str(summary),
        metadata={
            "degradation_type": degradation_type,
            "source_summary": source_summary or {},
        },
    )
