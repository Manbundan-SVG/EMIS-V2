from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class CoinMarketCapQuote:
    symbol: str
    price_usd: float
    market_cap_usd: float
    volume_24h_usd: float
    percent_change_24h: float
    cmc_rank: int | None
    last_updated: datetime | None


class CoinMarketCapClient:
    def __init__(self, *, api_key: str | None, base_url: str, timeout_seconds: int) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def fetch_latest_quotes(self, symbols: list[str]) -> dict[str, CoinMarketCapQuote]:
        if not self.enabled or not symbols:
            return {}
        payload = self._request_json(
            "/v2/cryptocurrency/quotes/latest",
            {"symbol": ",".join(sorted({symbol.upper() for symbol in symbols})), "convert": "USD"},
        )
        rows = payload.get("data")
        if not isinstance(rows, dict):
            return {}
        quotes: dict[str, CoinMarketCapQuote] = {}
        for symbol, raw in rows.items():
            row = raw[0] if isinstance(raw, list) and raw else raw
            if not isinstance(row, dict):
                continue
            usd_quote = row.get("quote", {}).get("USD", {})
            if not isinstance(usd_quote, dict):
                continue
            last_updated_raw = usd_quote.get("last_updated")
            last_updated = None
            if last_updated_raw:
                last_updated = datetime.fromisoformat(str(last_updated_raw).replace("Z", "+00:00"))
                if last_updated.tzinfo is None:
                    last_updated = last_updated.replace(tzinfo=timezone.utc)
            quotes[str(symbol).upper()] = CoinMarketCapQuote(
                symbol=str(symbol).upper(),
                price_usd=float(usd_quote.get("price") or 0.0),
                market_cap_usd=float(usd_quote.get("market_cap") or 0.0),
                volume_24h_usd=float(usd_quote.get("volume_24h") or 0.0),
                percent_change_24h=float(usd_quote.get("percent_change_24h") or 0.0),
                cmc_rank=int(row["cmc_rank"]) if row.get("cmc_rank") is not None else None,
                last_updated=last_updated,
            )
        return quotes

    def fetch_global_metrics(self) -> dict[str, float | str | None]:
        if not self.enabled:
            return {}
        payload = self._request_json("/v1/global-metrics/quotes/latest", {"convert": "USD"})
        row = payload.get("data")
        if not isinstance(row, dict):
            return {}
        usd_quote = row.get("quote", {}).get("USD", {})
        if not isinstance(usd_quote, dict):
            usd_quote = {}
        return {
            "total_market_cap_usd": float(usd_quote.get("total_market_cap") or 0.0),
            "total_volume_24h_usd": float(usd_quote.get("total_volume_24h") or 0.0),
            "btc_dominance": float(row.get("btc_dominance") or 0.0),
            "eth_dominance": float(row.get("eth_dominance") or 0.0),
            "altcoin_market_cap_usd": float(usd_quote.get("altcoin_market_cap") or 0.0),
            "last_updated": str(row.get("last_updated") or usd_quote.get("last_updated") or ""),
        }

    def _request_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("COINMARKETCAP_API_KEY is required")
        url = f"{self.base_url}{path}?{urlencode(params)}"
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "EMIS/1.0",
                "X-CMC_PRO_API_KEY": self.api_key,
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(f"CoinMarketCap request failed path={path} status={exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"CoinMarketCap request failed path={path}: {exc.reason}") from exc
        status = payload.get("status")
        if isinstance(status, dict) and int(status.get("error_code") or 0) != 0:
            raise RuntimeError(
                f"CoinMarketCap request returned error_code={status.get('error_code')} error_message={status.get('error_message')}"
            )
        return payload
