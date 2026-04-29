from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class CaseRecurrenceResult:
    matched_case_id: str | None
    reopen: bool
    recurrence_group_id: str | None
    repeat_count: int
    reopen_reason: str | None
    match_basis: dict[str, Any]


class CaseRecurrenceService:
    def __init__(self, reopen_window_days: int = 7) -> None:
        self.reopen_window_days = reopen_window_days

    def compute_match_basis(
        self,
        *,
        workspace_id: str,
        watchlist_id: str | None,
        degradation_family: str,
        version_tuple: str | None,
        regime: str | None,
        cluster_count: int | None = None,
    ) -> dict[str, Any]:
        return {
            "workspace_id": workspace_id,
            "watchlist_id": watchlist_id,
            "degradation_family": degradation_family,
            "version_tuple": version_tuple,
            "regime": regime,
            "cluster_count": cluster_count,
            "reopen_window_days": self.reopen_window_days,
            "matching_policy": "strict_workspace_watchlist_family_version",
        }

    def should_reopen(self, prior_closed_at: datetime | None, now: datetime | None = None) -> bool:
        if prior_closed_at is None:
            return False
        now = now or datetime.now(timezone.utc)
        return prior_closed_at >= now - timedelta(days=self.reopen_window_days)

    def build_result(
        self,
        *,
        prior_case: dict[str, Any] | None,
        match_basis: dict[str, Any],
        now: datetime | None = None,
    ) -> CaseRecurrenceResult:
        if not prior_case:
            return CaseRecurrenceResult(
                matched_case_id=None,
                reopen=False,
                recurrence_group_id=None,
                repeat_count=1,
                reopen_reason=None,
                match_basis=match_basis,
            )

        reopen = self.should_reopen(prior_case.get("prior_closed_at"), now=now)
        return CaseRecurrenceResult(
            matched_case_id=str(prior_case["id"]),
            reopen=reopen,
            recurrence_group_id=str(prior_case.get("recurrence_group_id") or prior_case["id"]),
            repeat_count=int(prior_case.get("repeat_count") or 1) + 1,
            reopen_reason="matched_recent_closed_case" if reopen else "matched_related_historical_case",
            match_basis=match_basis,
        )
