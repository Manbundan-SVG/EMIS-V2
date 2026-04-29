from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class FredSeriesPoint:
    series_code: str
    ts: datetime
    value: float
    return_1d: float | None
    change_1d: float | None
    source: str


class FredClient:
    def __init__(
        self,
        *,
        api_key: str | None,
        base_url: str,
        timeout_seconds: int,
        dxy_series_code: str,
        us10y_series_code: str,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.dxy_series_code = dxy_series_code
        self.us10y_series_code = us10y_series_code

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def fetch_macro_points(self) -> list[FredSeriesPoint]:
        if not self.enabled:
            return []
        return [
            self._fetch_dxy_point(),
            self._fetch_us10y_point(),
        ]

    def _fetch_dxy_point(self) -> FredSeriesPoint:
        observations = self._fetch_series_observations(self.dxy_series_code)
        latest, previous = self._latest_pair(observations, self.dxy_series_code)
        latest_value = float(latest["value"])
        previous_value = float(previous["value"])
        return FredSeriesPoint(
            series_code="DXY",
            ts=self._observation_timestamp(latest["date"]),
            value=latest_value,
            return_1d=((latest_value - previous_value) / previous_value) if previous_value else 0.0,
            change_1d=None,
            source="fred_api",
        )

    def _fetch_us10y_point(self) -> FredSeriesPoint:
        observations = self._fetch_series_observations(self.us10y_series_code)
        latest, previous = self._latest_pair(observations, self.us10y_series_code)
        latest_value = float(latest["value"])
        previous_value = float(previous["value"])
        return FredSeriesPoint(
            series_code="US10Y",
            ts=self._observation_timestamp(latest["date"]),
            value=latest_value,
            return_1d=None,
            change_1d=latest_value - previous_value,
            source="fred_api",
        )

    def _fetch_series_observations(self, series_id: str) -> list[dict[str, Any]]:
        if not self.api_key:
            raise RuntimeError("FRED_API_KEY is required for macro sync")
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 5,
        }
        url = f"{self.base_url}/series/observations?{urlencode(params)}"
        request = Request(url, headers={"Accept": "application/json"})
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(f"FRED request failed series={series_id} status={exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"FRED request failed series={series_id}: {exc.reason}") from exc

        observations = payload.get("observations")
        if not isinstance(observations, list):
            raise RuntimeError(f"FRED response missing observations for series={series_id}")
        valid = [row for row in observations if row.get("value") not in (None, ".")]
        if len(valid) < 2:
            raise RuntimeError(f"FRED series {series_id} returned insufficient observations")
        return valid

    @staticmethod
    def _latest_pair(observations: list[dict[str, Any]], series_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        if len(observations) < 2:
            raise RuntimeError(f"FRED series {series_id} returned insufficient observations")
        return observations[0], observations[1]

    @staticmethod
    def _observation_timestamp(date_string: str) -> datetime:
        return datetime.fromisoformat(date_string).replace(tzinfo=timezone.utc)
