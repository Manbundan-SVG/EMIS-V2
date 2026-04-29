from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class CurrencyApiMacroPoint:
    series_code: str
    ts: datetime
    value: float
    return_1d: float | None
    change_1d: float | None
    source: str


class CurrencyApiClient:
    _DXY_WEIGHTS = {
        "EUR": 0.576,
        "JPY": 0.136,
        "GBP": 0.119,
        "CAD": 0.091,
        "SEK": 0.042,
        "CHF": 0.036,
    }
    _DXY_SCALE = 50.14348112

    def __init__(self, *, api_key: str | None, base_url: str, timeout_seconds: int) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def fetch_macro_points(self) -> list[CurrencyApiMacroPoint]:
        if not self.enabled:
            return []
        latest = self._fetch_rates()
        previous = self._fetch_rates(date=(latest["meta"]["last_updated_at"].date() - timedelta(days=1)))
        latest_ts = latest["meta"]["last_updated_at"]
        latest_dxy = self._dxy_proxy(latest["data"])
        previous_dxy = self._dxy_proxy(previous["data"])
        return [
            CurrencyApiMacroPoint(
                series_code="DXY",
                ts=latest_ts,
                value=latest_dxy,
                return_1d=((latest_dxy - previous_dxy) / previous_dxy) if previous_dxy else 0.0,
                change_1d=None,
                source="currencyapi_fx_proxy",
            )
        ]

    def _fetch_rates(self, *, date=None) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("CURRENCY_API_KEY is required for currency API macro sync")
        params = {
            "apikey": self.api_key,
            "base_currency": "USD",
            "currencies": ",".join(sorted(self._DXY_WEIGHTS.keys())),
        }
        path = "/v3/latest"
        if date is not None:
            path = "/v3/historical"
            params["date"] = date.isoformat()
        url = f"{self.base_url}{path}?{urlencode(params)}"
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "EMIS/1.0"})
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(f"CurrencyAPI request failed path={path} status={exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"CurrencyAPI request failed path={path}: {exc.reason}") from exc

        data = payload.get("data")
        if not isinstance(data, dict) or not data:
            raise RuntimeError(f"CurrencyAPI response missing rates path={path}")

        meta = payload.get("meta") or {}
        raw_updated = meta.get("last_updated_at")
        if not raw_updated:
            raise RuntimeError(f"CurrencyAPI response missing last_updated_at path={path}")
        updated_at = datetime.fromisoformat(str(raw_updated).replace("Z", "+00:00"))
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        return {"data": data, "meta": {"last_updated_at": updated_at}}

    @classmethod
    def _dxy_proxy(cls, rates_payload: dict[str, Any]) -> float:
        product = cls._DXY_SCALE
        for currency, weight in cls._DXY_WEIGHTS.items():
            row = rates_payload.get(currency)
            if not isinstance(row, dict):
                raise RuntimeError(f"CurrencyAPI rates missing {currency}")
            value = float(row["value"])
            if value <= 0:
                raise RuntimeError(f"CurrencyAPI rate for {currency} must be positive")
            product *= math.pow(value, weight)
        return product
