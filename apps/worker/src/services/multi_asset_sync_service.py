"""Phase 4.0A: Multi-Asset Sync Service.

Orchestrates non-crypto asset-class ingestion (equities/indices, FX,
rates/macro proxies) on top of the existing market_data_sync_runs ledger
and market_bars / macro_series_points storage. Does not replace the
crypto or macro sync paths — it sits alongside them.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.db.repositories import (
    complete_market_data_sync_run,
    create_market_data_sync_run,
    get_asset_id_by_symbol,
    resolve_workspace_watchlist_scope,
    upsert_macro_series_points_rows,
    upsert_market_bars_rows,
)
from src.ingestion.alpaca_equity_client import AlpacaEquityClient

logger = logging.getLogger(__name__)

_EQUITY_INDEX_SYMBOLS = ("SPY", "QQQ", "DIA", "IWM")
_COMMODITY_ETF_SYMBOLS = ("GLD", "USO")
_FX_PAIRS: dict[str, tuple[str, str]] = {
    "EURUSD": ("EUR", "USD"),
    "USDJPY": ("USD", "JPY"),
    "GBPUSD": ("GBP", "USD"),
    "USDCHF": ("USD", "CHF"),
    "USDCAD": ("USD", "CAD"),
    "AUDUSD": ("AUD", "USD"),
}
# Additional FRED series covered by this service. US10Y / DXY are owned by
# the existing macro_sync_service and are not duplicated here.
_ADDITIONAL_FRED_SERIES: dict[str, tuple[str, str]] = {
    # canonical_symbol : (fred_series_id, asset_class)
    "US02Y":  ("DGS2",   "rates"),
    "2S10S":  ("T10Y2Y", "macro_proxy"),
}


@dataclass(frozen=True)
class MultiAssetSymbolResult:
    asset_class: str
    symbol: str
    canonical_symbol: str
    provider_family: str
    price: float | None
    volume: float | None
    ts: datetime | None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MultiAssetSyncRunResult:
    asset_class: str
    source: str
    provider_family: str
    sync_run_id: str | None
    requested_symbols: list[str]
    synced_symbols: list[str]
    skipped: bool
    provider_mode: str
    metadata: dict[str, Any]


class MultiAssetSyncService:
    """Coordinates equities, FX, and supplemental-rates ingestion."""

    # ── equities / indices / commodity ETFs ──────────────────────────────
    def sync_equities_and_indices(
        self,
        conn,
        *,
        workspace_slug: str,
        watchlist_slug: str | None = None,
        alpaca_client: AlpacaEquityClient,
        equity_symbols: Sequence[str] = _EQUITY_INDEX_SYMBOLS,
        commodity_symbols: Sequence[str] = _COMMODITY_ETF_SYMBOLS,
    ) -> MultiAssetSyncRunResult:
        symbols = list(equity_symbols) + list(commodity_symbols)
        workspace, watchlist = resolve_workspace_watchlist_scope(
            conn, workspace_slug, watchlist_slug
        )
        if not alpaca_client.enabled:
            return MultiAssetSyncRunResult(
                asset_class="index",
                source="multi_asset_equity_sync",
                provider_family="alpaca",
                sync_run_id=None,
                requested_symbols=symbols,
                synced_symbols=[],
                skipped=True,
                provider_mode="disabled",
                metadata={"reason": "ALPACA_API_KEY/ALPACA_SECRET_KEY not configured"},
            )

        sync_run = create_market_data_sync_run(
            conn,
            source="multi_asset_equity_sync",
            workspace_id=str(workspace["id"]),
            watchlist_id=str(watchlist["id"]) if watchlist else None,
            requested_symbols=symbols,
            metadata={
                "provider_mode":   "alpaca_snapshots",
                "provider_family": "alpaca",
                "asset_class":     "index",
            },
        )

        per_symbol: list[MultiAssetSymbolResult] = []
        try:
            snapshots = alpaca_client.fetch_snapshots(symbols)
        except (HTTPError, URLError, TimeoutError) as exc:
            logger.warning("multi_asset_sync: alpaca snapshot fetch failed: %s", exc)
            complete_market_data_sync_run(
                conn,
                str(sync_run["id"]),
                status="failed",
                synced_symbols=[],
                metadata={
                    "provider_mode":   "alpaca_snapshots",
                    "provider_family": "alpaca",
                    "asset_class":     "index",
                    "fetch_error":     str(exc),
                },
                error=str(exc),
            )
            return MultiAssetSyncRunResult(
                asset_class="index",
                source="multi_asset_equity_sync",
                provider_family="alpaca",
                sync_run_id=str(sync_run["id"]),
                requested_symbols=symbols,
                synced_symbols=[],
                skipped=False,
                provider_mode="alpaca_snapshots",
                metadata={"fetch_error": str(exc)},
            )

        bar_rows: list[dict[str, Any]] = []
        synced: list[str] = []
        for sym in symbols:
            asset_class = "commodity" if sym in commodity_symbols else "index"
            snap = snapshots.get(sym)
            if snap is None or snap.price is None:
                per_symbol.append(MultiAssetSymbolResult(
                    asset_class=asset_class, symbol=sym, canonical_symbol=sym,
                    provider_family="alpaca", price=None, volume=None, ts=None,
                    error="snapshot_missing_or_empty",
                ))
                continue
            asset_id = get_asset_id_by_symbol(conn, sym)
            if asset_id is None:
                per_symbol.append(MultiAssetSymbolResult(
                    asset_class=asset_class, symbol=sym, canonical_symbol=sym,
                    provider_family="alpaca", price=snap.price, volume=snap.volume,
                    ts=snap.ts, error="asset_row_missing",
                ))
                continue

            ts_value = snap.ts or datetime.now(timezone.utc)
            open_  = snap.open if snap.open is not None else snap.price
            high   = snap.high if snap.high is not None else snap.price
            low    = snap.low  if snap.low  is not None else snap.price
            close  = snap.close if snap.close is not None else snap.price
            volume = snap.volume if snap.volume is not None else 0.0

            bar_rows.append({
                "asset_id":      asset_id,
                "timeframe":     "1d",
                "ts":            ts_value,
                "open":          open_,
                "high":          high,
                "low":           low,
                "close":         close,
                "volume":        volume,
                "source":        "alpaca",
                "return_1h":     None,
                "volume_zscore": None,
            })
            synced.append(sym)
            per_symbol.append(MultiAssetSymbolResult(
                asset_class=asset_class, symbol=sym, canonical_symbol=sym,
                provider_family="alpaca", price=close, volume=volume,
                ts=ts_value,
            ))

        upsert_market_bars_rows(conn, bar_rows)

        final_meta: dict[str, Any] = {
            "provider_mode":   "alpaca_snapshots",
            "provider_family": "alpaca",
            "asset_class":     "index",
            "per_symbol":      [
                {
                    "symbol":         r.symbol,
                    "asset_class":    r.asset_class,
                    "price":          r.price,
                    "error":          r.error,
                }
                for r in per_symbol
            ],
        }
        complete_market_data_sync_run(
            conn,
            str(sync_run["id"]),
            status="completed" if synced else "failed",
            synced_symbols=synced,
            bar_count=len(bar_rows),
            metadata=final_meta,
        )
        return MultiAssetSyncRunResult(
            asset_class="index",
            source="multi_asset_equity_sync",
            provider_family="alpaca",
            sync_run_id=str(sync_run["id"]),
            requested_symbols=symbols,
            synced_symbols=synced,
            skipped=False,
            provider_mode="alpaca_snapshots",
            metadata=final_meta,
        )

    # ── FX (via CurrencyAPI) ─────────────────────────────────────────────
    def sync_fx(
        self,
        conn,
        *,
        workspace_slug: str,
        watchlist_slug: str | None = None,
        currency_api_key: str | None,
        currency_api_base_url: str,
        timeout_seconds: int,
        fx_pairs: dict[str, tuple[str, str]] = _FX_PAIRS,
    ) -> MultiAssetSyncRunResult:
        symbols = list(fx_pairs.keys())
        workspace, watchlist = resolve_workspace_watchlist_scope(
            conn, workspace_slug, watchlist_slug
        )
        if not currency_api_key:
            return MultiAssetSyncRunResult(
                asset_class="fx",
                source="multi_asset_fx_sync",
                provider_family="currency_api",
                sync_run_id=None,
                requested_symbols=symbols,
                synced_symbols=[],
                skipped=True,
                provider_mode="disabled",
                metadata={"reason": "CURRENCY_API_KEY not configured"},
            )

        sync_run = create_market_data_sync_run(
            conn,
            source="multi_asset_fx_sync",
            workspace_id=str(workspace["id"]),
            watchlist_id=str(watchlist["id"]) if watchlist else None,
            requested_symbols=symbols,
            metadata={
                "provider_mode":   "currency_api_latest",
                "provider_family": "currency_api",
                "asset_class":     "fx",
            },
        )

        currencies = sorted({c for pair in fx_pairs.values() for c in pair})
        url = (
            f"{currency_api_base_url.rstrip('/')}/v3/latest"
            f"?apikey={currency_api_key}"
            f"&currencies={','.join(currencies)}"
            f"&base_currency=USD"
        )
        try:
            req = Request(url)
            with urlopen(req, timeout=timeout_seconds) as resp:
                payload: dict[str, Any] = json.loads(resp.read())
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            logger.warning("multi_asset_sync: currency_api fetch failed: %s", exc)
            complete_market_data_sync_run(
                conn,
                str(sync_run["id"]),
                status="failed",
                synced_symbols=[],
                metadata={
                    "provider_mode":   "currency_api_latest",
                    "provider_family": "currency_api",
                    "asset_class":     "fx",
                    "fetch_error":     str(exc),
                },
                error=str(exc),
            )
            return MultiAssetSyncRunResult(
                asset_class="fx",
                source="multi_asset_fx_sync",
                provider_family="currency_api",
                sync_run_id=str(sync_run["id"]),
                requested_symbols=symbols,
                synced_symbols=[],
                skipped=False,
                provider_mode="currency_api_latest",
                metadata={"fetch_error": str(exc)},
            )

        rates: dict[str, float] = {}
        for code, entry in (payload.get("data") or {}).items():
            try:
                rates[code] = float(entry.get("value"))
            except (TypeError, ValueError):
                continue

        ts_value = _parse_currency_api_ts(
            (payload.get("meta") or {}).get("last_updated_at")
        ) or datetime.now(timezone.utc)

        macro_rows: list[dict[str, Any]] = []
        synced: list[str] = []
        per_symbol: list[dict[str, Any]] = []
        for pair, (base, quote) in fx_pairs.items():
            rate = _derive_fx_rate(base, quote, rates)
            if rate is None:
                per_symbol.append({
                    "symbol": pair, "error": "rate_unavailable",
                })
                continue
            macro_rows.append({
                "series_code": pair,
                "ts":          ts_value,
                "value":       rate,
                "source":      "currency_api",
                "return_1d":   None,
                "change_1d":   None,
            })
            synced.append(pair)
            per_symbol.append({
                "symbol": pair, "value": rate, "base": base, "quote": quote,
            })

        upsert_macro_series_points_rows(conn, macro_rows)

        final_meta: dict[str, Any] = {
            "provider_mode":   "currency_api_latest",
            "provider_family": "currency_api",
            "asset_class":     "fx",
            "per_symbol":      per_symbol,
        }
        complete_market_data_sync_run(
            conn,
            str(sync_run["id"]),
            status="completed" if synced else "failed",
            synced_symbols=synced,
            macro_point_count=len(macro_rows),
            metadata=final_meta,
        )
        return MultiAssetSyncRunResult(
            asset_class="fx",
            source="multi_asset_fx_sync",
            provider_family="currency_api",
            sync_run_id=str(sync_run["id"]),
            requested_symbols=symbols,
            synced_symbols=synced,
            skipped=False,
            provider_mode="currency_api_latest",
            metadata=final_meta,
        )

    # ── rates / macro proxies (via FRED) ─────────────────────────────────
    def sync_rates_and_macro_proxies(
        self,
        conn,
        *,
        workspace_slug: str,
        watchlist_slug: str | None = None,
        fred_api_key: str | None,
        fred_base_url: str,
        timeout_seconds: int,
        fred_us2y_series_code: str,
        fred_yield_spread_series_code: str,
    ) -> MultiAssetSyncRunResult:
        # Override the default FRED series ids with config-supplied values.
        local_series = dict(_ADDITIONAL_FRED_SERIES)
        local_series["US02Y"] = (fred_us2y_series_code, "rates")
        local_series["2S10S"] = (fred_yield_spread_series_code, "macro_proxy")

        symbols = list(local_series.keys())
        workspace, watchlist = resolve_workspace_watchlist_scope(
            conn, workspace_slug, watchlist_slug
        )
        if not fred_api_key:
            return MultiAssetSyncRunResult(
                asset_class="rates",
                source="multi_asset_rates_sync",
                provider_family="fred",
                sync_run_id=None,
                requested_symbols=symbols,
                synced_symbols=[],
                skipped=True,
                provider_mode="disabled",
                metadata={"reason": "FRED_API_KEY not configured"},
            )

        sync_run = create_market_data_sync_run(
            conn,
            source="multi_asset_rates_sync",
            workspace_id=str(workspace["id"]),
            watchlist_id=str(watchlist["id"]) if watchlist else None,
            requested_symbols=symbols,
            metadata={
                "provider_mode":   "fred_api",
                "provider_family": "fred",
                "asset_class":     "rates",
            },
        )

        macro_rows: list[dict[str, Any]] = []
        synced: list[str] = []
        per_symbol: list[dict[str, Any]] = []
        for canonical, (series_id, sub_class) in local_series.items():
            url = (
                f"{fred_base_url.rstrip('/')}/series/observations"
                f"?series_id={series_id}"
                f"&api_key={fred_api_key}"
                f"&file_type=json&limit=1&sort_order=desc"
            )
            try:
                req = Request(url)
                with urlopen(req, timeout=timeout_seconds) as resp:
                    payload = json.loads(resp.read())
                obs = (payload.get("observations") or [{}])[0]
                raw_val = obs.get("value")
                if raw_val in (None, "", "."):
                    per_symbol.append({
                        "symbol": canonical, "series_id": series_id,
                        "error": "observation_unavailable",
                    })
                    continue
                value = float(raw_val)
                ts_value = _parse_fred_date(obs.get("date")) or datetime.now(timezone.utc)
                macro_rows.append({
                    "series_code": canonical,
                    "ts":          ts_value,
                    "value":       value,
                    "source":      "fred",
                    "return_1d":   None,
                    "change_1d":   None,
                })
                synced.append(canonical)
                per_symbol.append({
                    "symbol":     canonical,
                    "series_id":  series_id,
                    "sub_class":  sub_class,
                    "value":      value,
                })
            except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
                logger.warning(
                    "multi_asset_sync: FRED %s (%s) fetch failed: %s",
                    canonical, series_id, exc,
                )
                per_symbol.append({
                    "symbol":    canonical,
                    "series_id": series_id,
                    "error":     str(exc),
                })

        upsert_macro_series_points_rows(conn, macro_rows)

        final_meta: dict[str, Any] = {
            "provider_mode":   "fred_api",
            "provider_family": "fred",
            "asset_class":     "rates",
            "per_symbol":      per_symbol,
        }
        complete_market_data_sync_run(
            conn,
            str(sync_run["id"]),
            status="completed" if synced else "failed",
            synced_symbols=synced,
            macro_point_count=len(macro_rows),
            metadata=final_meta,
        )
        return MultiAssetSyncRunResult(
            asset_class="rates",
            source="multi_asset_rates_sync",
            provider_family="fred",
            sync_run_id=str(sync_run["id"]),
            requested_symbols=symbols,
            synced_symbols=synced,
            skipped=False,
            provider_mode="fred_api",
            metadata=final_meta,
        )

    # ── normalization / persistence helpers (public API surface) ─────────
    def normalize_multi_asset_payloads(
        self, results: Sequence[MultiAssetSyncRunResult]
    ) -> list[dict[str, Any]]:
        """Flatten per-class run results into a common diagnostic dict list."""
        rows: list[dict[str, Any]] = []
        for res in results:
            rows.append({
                "asset_class":         res.asset_class,
                "source":              res.source,
                "provider_family":     res.provider_family,
                "sync_run_id":         res.sync_run_id,
                "requested_symbols":   res.requested_symbols,
                "synced_symbol_count": len(res.synced_symbols),
                "failed_symbol_count": max(
                    0, len(res.requested_symbols) - len(res.synced_symbols)
                ),
                "skipped":             res.skipped,
                "provider_mode":       res.provider_mode,
            })
        return rows

    def persist_multi_asset_sync_results(
        self,
        conn,
        *,
        workspace_id: str,  # noqa: ARG002 (reserved for future per-workspace state rollups)
        results: Sequence[MultiAssetSyncRunResult],
    ) -> int:
        """Row-level persistence already happened inside each sync_* call
        (bars → market_bars, macro → macro_series_points). This entrypoint
        exists for the spec contract and returns a synced-symbol count."""
        return sum(len(r.synced_symbols) for r in results if not r.skipped)

    # ── top-level entrypoint ─────────────────────────────────────────────
    def sync_all(
        self,
        conn,
        *,
        workspace_slug: str,
        watchlist_slug: str | None = None,
        alpaca_client: AlpacaEquityClient,
        currency_api_key: str | None,
        currency_api_base_url: str,
        fred_api_key: str | None,
        fred_base_url: str,
        timeout_seconds: int,
        fred_us2y_series_code: str,
        fred_yield_spread_series_code: str,
    ) -> list[MultiAssetSyncRunResult]:
        results: list[MultiAssetSyncRunResult] = []
        # 1. equities / indices / commodity ETFs
        try:
            results.append(self.sync_equities_and_indices(
                conn,
                workspace_slug=workspace_slug,
                watchlist_slug=watchlist_slug,
                alpaca_client=alpaca_client,
            ))
        except Exception as exc:
            logger.warning("multi_asset_sync: equities step failed: %s", exc)
        # 2. FX
        try:
            results.append(self.sync_fx(
                conn,
                workspace_slug=workspace_slug,
                watchlist_slug=watchlist_slug,
                currency_api_key=currency_api_key,
                currency_api_base_url=currency_api_base_url,
                timeout_seconds=timeout_seconds,
            ))
        except Exception as exc:
            logger.warning("multi_asset_sync: fx step failed: %s", exc)
        # 3. rates / macro proxies
        try:
            results.append(self.sync_rates_and_macro_proxies(
                conn,
                workspace_slug=workspace_slug,
                watchlist_slug=watchlist_slug,
                fred_api_key=fred_api_key,
                fred_base_url=fred_base_url,
                timeout_seconds=timeout_seconds,
                fred_us2y_series_code=fred_us2y_series_code,
                fred_yield_spread_series_code=fred_yield_spread_series_code,
            ))
        except Exception as exc:
            logger.warning("multi_asset_sync: rates step failed: %s", exc)
        return results


# ── local helpers ────────────────────────────────────────────────────────
def _derive_fx_rate(
    base: str, quote: str, usd_rates: dict[str, float]
) -> float | None:
    """Convert CurrencyAPI USD-base rates into arbitrary base/quote rates."""
    try:
        if base == "USD":
            return usd_rates.get(quote)
        if quote == "USD":
            base_rate = usd_rates.get(base)
            return (1.0 / base_rate) if base_rate else None
        base_rate = usd_rates.get(base)
        quote_rate = usd_rates.get(quote)
        if not base_rate or not quote_rate:
            return None
        return quote_rate / base_rate
    except ZeroDivisionError:
        return None


def _parse_currency_api_ts(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_fred_date(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    except ValueError:
        return None
