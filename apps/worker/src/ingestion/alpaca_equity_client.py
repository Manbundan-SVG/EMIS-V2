"""Phase 4.0A: Lightweight Alpaca equity/ETF snapshot client.

Used by `multi_asset_sync_service` for SPY/QQQ/DIA/IWM/GLD/USO etc.
Kept separate from BinanceClient (which carries Alpaca keys for a different
fallback purpose) so the equity path is auditable on its own terms.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class AlpacaEquitySnapshot:
    symbol: str
    price: float | None
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    ts: datetime | None
    raw: dict[str, Any]


class AlpacaEquityClient:
    def __init__(
        self,
        *,
        api_key: str | None,
        secret_key: str | None,
        base_url: str,
        timeout_seconds: int,
        feed: str = "iex",
    ) -> None:
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.feed = feed

    @property
    def enabled(self) -> bool:
        return bool(self.api_key and self.secret_key)

    def fetch_snapshots(self, symbols: list[str]) -> dict[str, AlpacaEquitySnapshot]:
        if not self.enabled or not symbols:
            return {}
        url = (
            f"{self.base_url}/v2/stocks/snapshots"
            f"?symbols={','.join(symbols)}&feed={self.feed}"
        )
        req = Request(url)
        req.add_header("APCA-API-KEY-ID", self.api_key or "")
        req.add_header("APCA-API-SECRET-KEY", self.secret_key or "")
        try:
            with urlopen(req, timeout=self.timeout_seconds) as resp:
                payload: dict[str, Any] = json.loads(resp.read())
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
            raise

        snapshots: dict[str, AlpacaEquitySnapshot] = {}
        for sym, snap in payload.items():
            if not isinstance(snap, dict):
                continue
            trade = snap.get("latestTrade") or {}
            daily = snap.get("dailyBar") or snap.get("minuteBar") or {}
            price = trade.get("p") if trade.get("p") is not None else daily.get("c")
            ts_raw = trade.get("t") or daily.get("t")
            snapshots[sym] = AlpacaEquitySnapshot(
                symbol=sym,
                price=float(price) if price is not None else None,
                open=float(daily["o"]) if daily.get("o") is not None else None,
                high=float(daily["h"]) if daily.get("h") is not None else None,
                low=float(daily["l"]) if daily.get("l") is not None else None,
                close=float(daily["c"]) if daily.get("c") is not None else None,
                volume=float(daily["v"]) if daily.get("v") is not None else None,
                ts=_parse_ts(ts_raw),
                raw=snap,
            )
        return snapshots


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        text = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None
