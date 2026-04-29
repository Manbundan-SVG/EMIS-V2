from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class CoinGlassMarketSnapshot:
    symbol: str
    ts: datetime
    open_interest: float
    oi_change_1h: float
    funding_rate: float
    liquidation_notional_1h: float
    source: str


class CoinGlassClient:
    def __init__(
        self,
        *,
        api_key: str | None,
        base_url: str,
        timeout_seconds: int,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def fetch_coin_markets(
        self,
        symbols: list[str],
    ) -> dict[str, CoinGlassMarketSnapshot]:
        if not self.enabled or not symbols:
            return {}

        payload = self._request_json("/api/futures/coins-markets", {})
        rows = payload.get("data")
        if not isinstance(rows, list):
            return {}

        requested = {symbol.upper() for symbol in symbols}
        snapshots: dict[str, CoinGlassMarketSnapshot] = {}
        observed_at = datetime.now(tz=timezone.utc)
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = self._coerce_symbol(row)
            if symbol is None or symbol not in requested:
                continue
            snapshots[symbol] = CoinGlassMarketSnapshot(
                symbol=symbol,
                ts=observed_at,
                open_interest=self._coerce_float(
                    row,
                    "open_interest_usd",
                    "openInterestUsd",
                    "open_interest",
                    "openInterest",
                    default=0.0,
                ),
                oi_change_1h=self._coerce_percent_decimal(
                    row,
                    "open_interest_change_percent_1h",
                    "openInterestChangePercent1h",
                    "oiChangePercent1h",
                    default=0.0,
                ),
                funding_rate=self._coerce_float(
                    row,
                    "avg_funding_rate_by_oi",
                    "avgFundingRateByOi",
                    "funding_rate",
                    "fundingRate",
                    default=0.0,
                ),
                liquidation_notional_1h=self._coerce_float(
                    row,
                    "liquidation_usd_1h",
                    "liquidationUsd1h",
                    "liquidation_1h",
                    "liquidation1h",
                    default=0.0,
                ),
                source="coinglass_futures",
            )
        return snapshots

    def _request_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("CoinGlass API key is required")
        query = urlencode(params)
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "CG-API-KEY": self.api_key,
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(f"CoinGlass request failed path={path} status={exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"CoinGlass request failed path={path}: {exc.reason}") from exc

        code = str(payload.get("code", "0"))
        success = payload.get("success")
        if code not in ("0", "200", "") and success is not True:
            raise RuntimeError(
                f"CoinGlass request returned code={payload.get('code')} msg={payload.get('msg') or payload.get('message')}"
            )
        return payload

    @staticmethod
    def _coerce_symbol(row: dict[str, Any]) -> str | None:
        raw = row.get("symbol") or row.get("coin") or row.get("baseCoin") or row.get("base_symbol")
        if raw is None:
            return None
        return str(raw).upper()

    @staticmethod
    def _coerce_float(row: dict[str, Any], *keys: str, default: float) -> float:
        for key in keys:
            value = row.get(key)
            if value is None or value == "":
                continue
            return float(value)
        return default

    @classmethod
    def _coerce_percent_decimal(cls, row: dict[str, Any], *keys: str, default: float) -> float:
        value = cls._coerce_float(row, *keys, default=default)
        return value / 100.0
