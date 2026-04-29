from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import pstdev
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class BinanceBarSnapshot:
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    return_1h: float
    volume_zscore: float
    source: str


@dataclass(frozen=True)
class BinanceDerivativeSnapshot:
    symbol: str
    ts: datetime
    open_interest: float
    oi_change_1h: float
    funding_rate: float
    source: str


class BinanceClient:
    def __init__(
        self,
        *,
        alpaca_api_key: str | None,
        alpaca_secret_key: str | None,
        alpaca_data_base_url: str,
        massive_api_key: str | None,
        massive_base_url: str,
        okx_base_url: str,
        futures_base_url: str,
        spot_base_url: str,
        bybit_base_url: str,
        timeout_seconds: int,
        quote_asset: str = "USDT",
        symbol_overrides: dict[str, str] | None = None,
    ) -> None:
        self.alpaca_api_key = alpaca_api_key
        self.alpaca_secret_key = alpaca_secret_key
        self.alpaca_data_base_url = alpaca_data_base_url.rstrip("/")
        self.massive_api_key = massive_api_key
        self.massive_base_url = massive_base_url.rstrip("/")
        self.okx_base_url = okx_base_url.rstrip("/")
        self.futures_base_url = futures_base_url.rstrip("/")
        self.spot_base_url = spot_base_url.rstrip("/")
        self.bybit_base_url = bybit_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.quote_asset = quote_asset
        self.symbol_overrides = symbol_overrides or {}

    def fetch_hourly_bar(self, asset_symbol: str, lookback_hours: int = 25) -> BinanceBarSnapshot | None:
        try:
            okx_bar = self._fetch_okx_hourly_bar(asset_symbol, lookback_hours)
        except RuntimeError:
            okx_bar = None
        if okx_bar is not None:
            return okx_bar

        market_symbol = self._market_symbol(asset_symbol)
        try:
            klines = self._request_json(
                self.futures_base_url,
                "/fapi/v1/klines",
                {"symbol": market_symbol, "interval": "1h", "limit": lookback_hours},
            )
        except RuntimeError as exc:
            klines = []
        source = "binance_futures"
        if not klines:
            try:
                klines = self._request_json(
                    self.spot_base_url,
                    "/api/v3/klines",
                    {"symbol": market_symbol, "interval": "1h", "limit": lookback_hours},
                )
            except RuntimeError as exc:
                klines = []
            source = "binance_spot"
        if not klines:
            try:
                bybit_bar = self._fetch_bybit_hourly_bar(asset_symbol, market_symbol, lookback_hours)
            except RuntimeError:
                bybit_bar = None
            if bybit_bar is not None:
                return bybit_bar
            try:
                alpaca_bar = self._fetch_alpaca_hourly_bar(asset_symbol, lookback_hours)
            except RuntimeError:
                alpaca_bar = None
            if alpaca_bar is not None:
                return alpaca_bar
            try:
                alpaca_snapshot_bar = self._fetch_alpaca_snapshot_bar(asset_symbol)
            except RuntimeError:
                alpaca_snapshot_bar = None
            if alpaca_snapshot_bar is not None:
                return alpaca_snapshot_bar
            return self._fetch_massive_delayed_bar(asset_symbol)

        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        closed = [row for row in klines if len(row) >= 8 and int(row[6]) <= now_ms]
        if len(closed) < 2:
            return None

        latest = closed[-1]
        previous = closed[-2]
        close_price = float(latest[4])
        previous_close = float(previous[4])
        quote_volumes = [float(row[7]) for row in closed[-24:] if len(row) >= 8]
        volume = float(latest[7]) if len(latest) >= 8 else float(latest[5])
        volume_zscore = self._zscore(quote_volumes, volume)

        return BinanceBarSnapshot(
            symbol=asset_symbol,
            ts=datetime.fromtimestamp(int(latest[6]) / 1000, tz=timezone.utc),
            open=float(latest[1]),
            high=float(latest[2]),
            low=float(latest[3]),
            close=close_price,
            volume=volume,
            return_1h=((close_price - previous_close) / previous_close) if previous_close else 0.0,
            volume_zscore=volume_zscore,
            source=source,
        )

    def fetch_derivative_snapshot(self, asset_symbol: str) -> BinanceDerivativeSnapshot | None:
        try:
            okx_snapshot = self._fetch_okx_derivative_snapshot(asset_symbol)
        except RuntimeError:
            okx_snapshot = None
        if okx_snapshot is not None:
            return okx_snapshot

        market_symbol = self._market_symbol(asset_symbol)
        try:
            oi_history = self._request_json(
                self.futures_base_url,
                "/futures/data/openInterestHist",
                {"symbol": market_symbol, "period": "1h", "limit": 2},
            )
        except RuntimeError as exc:
            oi_history = []
        if not oi_history:
            try:
                return self._fetch_bybit_derivative_snapshot(asset_symbol, market_symbol)
            except RuntimeError:
                return None

        latest = oi_history[-1]
        previous = oi_history[-2] if len(oi_history) > 1 else None
        latest_oi = self._coerce_float(latest, "sumOpenInterestValue", "sumOpenInterest", "openInterest")
        previous_oi = self._coerce_float(previous, "sumOpenInterestValue", "sumOpenInterest", "openInterest") if previous else None
        oi_change_1h = 0.0
        if previous_oi not in (None, 0.0):
            oi_change_1h = (latest_oi - previous_oi) / previous_oi

        try:
            funding_rows = self._request_json(
                self.futures_base_url,
                "/fapi/v1/fundingRate",
                {"symbol": market_symbol, "limit": 1},
            )
        except RuntimeError:
            funding_rows = []
        funding_row = funding_rows[-1] if funding_rows else {}
        funding_rate = self._coerce_float(funding_row, "fundingRate", default=0.0)
        funding_time = funding_row.get("fundingTime")
        snapshot_ts_ms = int(
            funding_time
            or latest.get("timestamp")
            or latest.get("time")
            or 0
        )
        if snapshot_ts_ms <= 0:
            snapshot_ts = datetime.now(tz=timezone.utc)
        else:
            snapshot_ts = datetime.fromtimestamp(snapshot_ts_ms / 1000, tz=timezone.utc)

        return BinanceDerivativeSnapshot(
            symbol=asset_symbol,
            ts=snapshot_ts,
            open_interest=latest_oi,
            oi_change_1h=oi_change_1h,
            funding_rate=funding_rate,
            source="binance_futures",
        )

    def _fetch_okx_hourly_bar(self, asset_symbol: str, lookback_hours: int) -> BinanceBarSnapshot | None:
        payload = self._request_json(
            self.okx_base_url,
            "/api/v5/market/candles",
            {"instId": self._okx_swap_inst_id(asset_symbol), "bar": "1H", "limit": lookback_hours},
        )
        bar = self._okx_bar_from_payload(asset_symbol, payload, source="okx_swap")
        if bar is not None:
            return bar

        spot_payload = self._request_json(
            self.okx_base_url,
            "/api/v5/market/candles",
            {"instId": self._okx_spot_inst_id(asset_symbol), "bar": "1H", "limit": lookback_hours},
        )
        return self._okx_bar_from_payload(asset_symbol, spot_payload, source="okx_spot")

    def _fetch_okx_derivative_snapshot(self, asset_symbol: str) -> BinanceDerivativeSnapshot | None:
        oi_payload = self._request_json(
            self.okx_base_url,
            "/api/v5/public/open-interest",
            {"instType": "SWAP", "instId": self._okx_swap_inst_id(asset_symbol)},
        )
        if not isinstance(oi_payload, dict):
            return None
        oi_rows = oi_payload.get("data", [])
        if not oi_rows:
            return None
        latest = oi_rows[0]
        latest_oi = self._coerce_float(latest, "oiUsd", "oiCcy", "oi")

        funding_payload = self._request_json(
            self.okx_base_url,
            "/api/v5/public/funding-rate",
            {"instId": self._okx_swap_inst_id(asset_symbol)},
        )
        if not isinstance(funding_payload, dict):
            funding_payload = {}
        funding_rows = funding_payload.get("data", [])
        funding_row = funding_rows[0] if funding_rows else {}
        funding_rate = self._coerce_float(funding_row, "fundingRate", default=0.0)
        funding_time = int(funding_row.get("fundingTime") or latest.get("ts") or 0)
        snapshot_ts = (
            datetime.fromtimestamp(funding_time / 1000, tz=timezone.utc)
            if funding_time > 0
            else datetime.now(tz=timezone.utc)
        )
        return BinanceDerivativeSnapshot(
            symbol=asset_symbol,
            ts=snapshot_ts,
            open_interest=latest_oi,
            oi_change_1h=0.0,
            funding_rate=funding_rate,
            source="okx_swap",
        )

    def _fetch_bybit_hourly_bar(
        self,
        asset_symbol: str,
        market_symbol: str,
        lookback_hours: int,
    ) -> BinanceBarSnapshot | None:
        payload = self._request_json(
            self.bybit_base_url,
            "/v5/market/kline",
            {"category": "linear", "symbol": market_symbol, "interval": "60", "limit": lookback_hours},
        )
        if not isinstance(payload, dict):
            return None
        rows = payload.get("result", {}).get("list", [])
        if not rows:
            return None
        ordered = sorted(rows, key=lambda row: int(row[0]))
        if len(ordered) < 2:
            return None
        latest = ordered[-1]
        previous = ordered[-2]
        close_price = float(latest[4])
        previous_close = float(previous[4])
        quote_volumes = [float(row[6]) for row in ordered[-24:] if len(row) >= 7]
        volume = float(latest[6]) if len(latest) >= 7 else float(latest[5])
        return BinanceBarSnapshot(
            symbol=asset_symbol,
            ts=datetime.fromtimestamp(int(latest[0]) / 1000, tz=timezone.utc),
            open=float(latest[1]),
            high=float(latest[2]),
            low=float(latest[3]),
            close=close_price,
            volume=volume,
            return_1h=((close_price - previous_close) / previous_close) if previous_close else 0.0,
            volume_zscore=self._zscore(quote_volumes, volume),
            source="bybit_linear",
        )

    def _fetch_bybit_derivative_snapshot(
        self,
        asset_symbol: str,
        market_symbol: str,
    ) -> BinanceDerivativeSnapshot | None:
        oi_payload = self._request_json(
            self.bybit_base_url,
            "/v5/market/open-interest",
            {"category": "linear", "symbol": market_symbol, "intervalTime": "1h", "limit": 2},
        )
        if not isinstance(oi_payload, dict):
            return None
        oi_rows = oi_payload.get("result", {}).get("list", [])
        if not oi_rows:
            return None
        ordered = sorted(oi_rows, key=lambda row: int(row["timestamp"]))
        latest = ordered[-1]
        previous = ordered[-2] if len(ordered) > 1 else None
        latest_oi = float(latest["openInterest"])
        previous_oi = float(previous["openInterest"]) if previous else None
        oi_change_1h = 0.0
        if previous_oi not in (None, 0.0):
            oi_change_1h = (latest_oi - previous_oi) / previous_oi

        funding_payload = self._request_json(
            self.bybit_base_url,
            "/v5/market/funding/history",
            {"category": "linear", "symbol": market_symbol, "limit": 1},
        )
        if not isinstance(funding_payload, dict):
            funding_payload = {}
        funding_rows = funding_payload.get("result", {}).get("list", [])
        funding_row = funding_rows[0] if funding_rows else {}
        funding_rate = self._coerce_float(funding_row, "fundingRate", default=0.0)
        funding_time = int(funding_row.get("fundingRateTimestamp") or latest["timestamp"])
        return BinanceDerivativeSnapshot(
            symbol=asset_symbol,
            ts=datetime.fromtimestamp(funding_time / 1000, tz=timezone.utc),
            open_interest=latest_oi,
            oi_change_1h=oi_change_1h,
            funding_rate=funding_rate,
            source="bybit_linear",
        )

    def _fetch_massive_delayed_bar(self, asset_symbol: str) -> BinanceBarSnapshot | None:
        if not self.massive_api_key:
            return None
        payload = self._request_json(
            self.massive_base_url,
            f"/v2/aggs/ticker/{self._massive_ticker(asset_symbol)}/prev",
            {"adjusted": "true", "apiKey": self.massive_api_key},
        )
        if not isinstance(payload, dict):
            return None
        rows = payload.get("results", [])
        if not rows:
            return None
        latest = rows[0]
        ts_ms = int(latest.get("t") or 0)
        if ts_ms <= 0:
            return None
        return BinanceBarSnapshot(
            symbol=asset_symbol,
            ts=datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
            open=float(latest["o"]),
            high=float(latest["h"]),
            low=float(latest["l"]),
            close=float(latest["c"]),
            volume=float(latest.get("v") or 0.0),
            # Massive is only being used here as a delayed daily fallback, not a 1h signal-native bar source.
            return_1h=0.0,
            volume_zscore=0.0,
            source="massive_delayed_daily",
        )

    def _fetch_alpaca_hourly_bar(self, asset_symbol: str, lookback_hours: int) -> BinanceBarSnapshot | None:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=max(lookback_hours + 2, 30))
        payload = self._request_alpaca_json(
            "/v1beta3/crypto/us/bars",
            {
                "symbols": self._alpaca_symbol(asset_symbol),
                "timeframe": "1Hour",
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
                "limit": str(max(lookback_hours, 25)),
                "sort": "desc",
            },
        )
        if not isinstance(payload, dict):
            return None
        rows = payload.get("bars", {}).get(self._alpaca_symbol(asset_symbol), [])
        if not rows:
            return None
        ordered = sorted(rows, key=lambda row: row.get("t", ""))
        if len(ordered) < 2:
            return None
        latest = ordered[-1]
        previous = ordered[-2]
        close_price = float(latest["c"])
        previous_close = float(previous["c"])
        volumes = [float(row.get("v") or 0.0) for row in ordered[-24:]]
        volume = float(latest.get("v") or 0.0)
        return BinanceBarSnapshot(
            symbol=asset_symbol,
            ts=datetime.fromisoformat(str(latest["t"]).replace("Z", "+00:00")),
            open=float(latest["o"]),
            high=float(latest["h"]),
            low=float(latest["l"]),
            close=close_price,
            volume=volume,
            return_1h=((close_price - previous_close) / previous_close) if previous_close else 0.0,
            volume_zscore=self._zscore(volumes, volume),
            source="alpaca_crypto",
        )

    def _fetch_alpaca_snapshot_bar(self, asset_symbol: str) -> BinanceBarSnapshot | None:
        payload = self._request_alpaca_json(
            "/v1beta3/crypto/us/snapshots",
            {"symbols": self._alpaca_symbol(asset_symbol)},
        )
        if not isinstance(payload, dict):
            return None
        snapshot = payload.get("snapshots", {}).get(self._alpaca_symbol(asset_symbol), {})
        if not isinstance(snapshot, dict):
            return None
        minute_bar = snapshot.get("minuteBar")
        if not isinstance(minute_bar, dict):
            return None
        ts_raw = minute_bar.get("t")
        if not ts_raw:
            return None
        volume = float(minute_bar.get("v") or 0.0)
        return BinanceBarSnapshot(
            symbol=asset_symbol,
            ts=datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00")),
            open=float(minute_bar.get("o") or 0.0),
            high=float(minute_bar.get("h") or 0.0),
            low=float(minute_bar.get("l") or 0.0),
            close=float(minute_bar.get("c") or 0.0),
            volume=volume,
            # Alpaca snapshots are minute-level reference data, not a closed 1h candle.
            return_1h=0.0,
            volume_zscore=0.0,
            source="alpaca_crypto_snapshot",
        )

    def _market_symbol(self, asset_symbol: str) -> str:
        return self.symbol_overrides.get(asset_symbol, f"{asset_symbol}{self.quote_asset}")

    def _massive_ticker(self, asset_symbol: str) -> str:
        return f"X:{asset_symbol}USD"

    def _alpaca_symbol(self, asset_symbol: str) -> str:
        return f"{asset_symbol}/USD"

    def _okx_swap_inst_id(self, asset_symbol: str) -> str:
        override = self.symbol_overrides.get(asset_symbol)
        if override and "-" in override:
            return override if override.endswith("-SWAP") else f"{override}-SWAP"
        return f"{asset_symbol}-{self.quote_asset}-SWAP"

    def _okx_spot_inst_id(self, asset_symbol: str) -> str:
        override = self.symbol_overrides.get(asset_symbol)
        if override and "-" in override:
            return override.replace("-SWAP", "")
        return f"{asset_symbol}-{self.quote_asset}"

    def _okx_bar_from_payload(
        self,
        asset_symbol: str,
        payload: Any,
        *,
        source: str,
    ) -> BinanceBarSnapshot | None:
        if not isinstance(payload, dict):
            return None
        rows = payload.get("data", [])
        if not rows:
            return None
        closed_rows = [row for row in rows if len(row) >= 9 and row[8] == "1"]
        ordered = sorted(closed_rows or rows, key=lambda row: int(row[0]))
        if len(ordered) < 2:
            return None
        latest = ordered[-1]
        previous = ordered[-2]
        close_price = float(latest[4])
        previous_close = float(previous[4])
        quote_volumes = [float(row[7] or row[6] or row[5]) for row in ordered[-24:] if len(row) >= 8]
        volume = float(latest[7] or latest[6] or latest[5])
        return BinanceBarSnapshot(
            symbol=asset_symbol,
            ts=datetime.fromtimestamp(int(latest[0]) / 1000, tz=timezone.utc),
            open=float(latest[1]),
            high=float(latest[2]),
            low=float(latest[3]),
            close=close_price,
            volume=volume,
            return_1h=((close_price - previous_close) / previous_close) if previous_close else 0.0,
            volume_zscore=self._zscore(quote_volumes, volume),
            source=source,
        )

    def _request_json(self, base_url: str, path: str, params: dict[str, Any]) -> Any:
        url = f"{base_url}{path}?{urlencode(params)}"
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "EMIS/1.0"})
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code in (400, 403, 404, 451):
                return []
            raise RuntimeError(f"market data request failed url={base_url}{path} status={exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"market data request failed url={base_url}{path}: {exc.reason}") from exc
        if isinstance(payload, dict) and payload.get("retCode") not in (None, 0):
            return []
        if isinstance(payload, dict) and payload.get("code") not in (None, 0, "0"):
            return []
        return payload

    def _request_alpaca_json(self, path: str, params: dict[str, Any]) -> Any:
        headers = {"Accept": "application/json", "User-Agent": "EMIS/1.0"}
        if self.alpaca_api_key and self.alpaca_secret_key:
            headers["APCA-API-KEY-ID"] = self.alpaca_api_key
            headers["APCA-API-SECRET-KEY"] = self.alpaca_secret_key
        url = f"{self.alpaca_data_base_url}{path}?{urlencode(params)}"
        request = Request(url, headers=headers)
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code in (400, 401, 403, 404, 429):
                return []
            raise RuntimeError(f"alpaca market data request failed path={path} status={exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"alpaca market data request failed path={path}: {exc.reason}") from exc
        return payload

    @staticmethod
    def _coerce_float(payload: dict[str, Any] | None, *keys: str, default: float | None = None) -> float:
        if payload is None:
            if default is None:
                raise RuntimeError("missing payload for float coercion")
            return default
        for key in keys:
            value = payload.get(key)
            if value is None or value == "":
                continue
            return float(value)
        if default is None:
            raise RuntimeError(f"missing numeric fields {keys!r}")
        return default

    @staticmethod
    def _zscore(values: list[float], current: float) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        deviation = pstdev(values)
        if deviation == 0 or math.isnan(deviation):
            return 0.0
        return (current - mean) / deviation
