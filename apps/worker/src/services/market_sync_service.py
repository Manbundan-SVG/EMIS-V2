from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from src.db.repositories import (
    complete_market_data_sync_run,
    create_market_data_sync_run,
    enqueue_governed_recompute,
    get_latest_open_interest_by_asset_ids,
    get_syncable_crypto_assets,
    resolve_workspace_watchlist_scope,
    upsert_market_bars_rows,
    upsert_market_funding_rows,
    upsert_market_liquidation_rows,
    upsert_market_open_interest_rows,
)
from src.ingestion.binance_client import BinanceBarSnapshot, BinanceClient, BinanceDerivativeSnapshot
from src.ingestion.coinmarketcap_client import CoinMarketCapClient
from src.ingestion.coinglass_client import CoinGlassClient, CoinGlassMarketSnapshot

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketSyncResult:
    sync_run_id: str
    workspace_slug: str
    watchlist_slug: str | None
    requested_symbols: list[str]
    synced_symbols: list[str]
    bar_count: int
    open_interest_count: int
    funding_count: int
    liquidation_count: int
    enqueue_result: dict[str, Any] | None
    metadata: dict[str, Any]


def sync_crypto_market_data(
    conn,
    *,
    workspace_slug: str,
    watchlist_slug: str | None = None,
    enqueue_recompute: bool = True,
    requested_by: str = "market-sync",
    binance_client: BinanceClient,
    coinglass_client: CoinGlassClient | None = None,
    coinmarketcap_client: CoinMarketCapClient | None = None,
) -> MarketSyncResult:
    workspace, watchlist = resolve_workspace_watchlist_scope(conn, workspace_slug, watchlist_slug)
    assets = get_syncable_crypto_assets(
        conn,
        workspace_id=str(workspace["id"]),
        watchlist_id=str(watchlist["id"]) if watchlist else None,
    )
    if not assets:
        raise RuntimeError(
            f"no syncable crypto assets found for workspace={workspace_slug!r} watchlist={watchlist_slug!r}"
        )

    requested_symbols = [asset.symbol for asset in assets]
    coinglass_expected = bool(coinglass_client and coinglass_client.enabled)
    provider_stack: dict[str, Any] = {
        "bars": "okx_primary_public",
        "derivatives": "coinglass_enriched" if coinglass_expected else "exchange_fallback",
        "reference": "coinmarketcap_quotes" if coinmarketcap_client and coinmarketcap_client.enabled else "disabled",
    }
    metadata: dict[str, Any] = {
        "provider_mode": "coinglass_enriched_okx_primary" if coinglass_expected else "okx_primary_public",
        "provider_stack": provider_stack,
        "watchlist_scoped": watchlist is not None,
    }
    sync_run = create_market_data_sync_run(
        conn,
        source="crypto_market_sync",
        workspace_id=str(workspace["id"]),
        watchlist_id=str(watchlist["id"]) if watchlist else None,
        requested_symbols=requested_symbols,
        metadata=metadata,
    )

    bar_rows: list[dict[str, Any]] = []
    oi_rows: list[dict[str, Any]] = []
    funding_rows: list[dict[str, Any]] = []
    liquidation_rows: list[dict[str, Any]] = []
    synced_symbols: list[str] = []
    missing_bar_symbols: list[str] = []
    provider_coverage: dict[str, dict[str, str]] = {}
    symbol_errors: dict[str, str] = {}
    prior_open_interest = get_latest_open_interest_by_asset_ids(conn, [asset.asset_id for asset in assets])

    try:
        coinglass_snapshots: dict[str, CoinGlassMarketSnapshot] = {}
        if coinglass_expected:
            try:
                coinglass_snapshots = coinglass_client.fetch_coin_markets(requested_symbols) if coinglass_client else {}
            except Exception as exc:
                metadata["derivatives_provider_error"] = str(exc)
                metadata["provider_mode"] = "okx_primary_public"
                metadata["provider_stack"] = {
                    **provider_stack,
                    "derivatives": "exchange_fallback",
                }
        reference_quotes: dict[str, Any] = {}
        global_market_metrics: dict[str, Any] = {}
        if coinmarketcap_client and coinmarketcap_client.enabled:
            try:
                reference_quotes = coinmarketcap_client.fetch_latest_quotes(requested_symbols)
                global_market_metrics = coinmarketcap_client.fetch_global_metrics()
            except Exception as exc:
                metadata["reference_provider_error"] = str(exc)

        for asset in assets:
            try:
                bar = binance_client.fetch_hourly_bar(asset.symbol)
            except Exception as exc:
                symbol_errors[asset.symbol] = str(exc)
                provider_coverage[asset.symbol] = {"bars": "error", "derivatives": "error"}
                missing_bar_symbols.append(asset.symbol)
                continue
            if bar is None:
                missing_bar_symbols.append(asset.symbol)
                provider_coverage[asset.symbol] = {"bars": "missing", "derivatives": "missing"}
                continue

            synced_symbols.append(asset.symbol)
            bar_rows.append(_bar_row(asset.asset_id, bar))

            derivatives_provider = "binance_futures"
            derivative_snapshot = coinglass_snapshots.get(asset.symbol)
            if derivative_snapshot is None:
                try:
                    fallback_snapshot = binance_client.fetch_derivative_snapshot(asset.symbol)
                except Exception as exc:
                    symbol_errors[asset.symbol] = str(exc)
                    fallback_snapshot = None
                if fallback_snapshot is not None:
                    derivatives_provider = fallback_snapshot.source
                    derivative_snapshot = _promote_binance_derivative_to_coinglass_shape(fallback_snapshot)
                else:
                    derivatives_provider = "missing"
            else:
                derivatives_provider = derivative_snapshot.source

            provider_coverage[asset.symbol] = {
                "bars": bar.source,
                "derivatives": derivatives_provider,
            }

            if derivative_snapshot is None:
                continue

            oi_change_1h = derivative_snapshot.oi_change_1h
            if derivative_snapshot.source.startswith("okx"):
                previous_oi = prior_open_interest.get(asset.asset_id)
                if previous_oi not in (None, 0.0):
                    oi_change_1h = (derivative_snapshot.open_interest - previous_oi) / previous_oi

            oi_rows.append(
                {
                    "asset_id": asset.asset_id,
                    "ts": bar.ts,
                    "open_interest": derivative_snapshot.open_interest,
                    "source": derivative_snapshot.source,
                    "oi_change_1h": oi_change_1h,
                }
            )
            funding_rows.append(
                {
                    "asset_id": asset.asset_id,
                    "ts": bar.ts,
                    "funding_rate": derivative_snapshot.funding_rate,
                    "source": derivative_snapshot.source,
                }
            )
            if derivative_snapshot.liquidation_notional_1h > 0:
                liquidation_rows.append(
                    {
                        "asset_id": asset.asset_id,
                        "ts": bar.ts,
                        "side": "all",
                        "notional_usd": None,
                        "reference_price": bar.close,
                        "source": derivative_snapshot.source,
                        "liquidation_notional_1h": derivative_snapshot.liquidation_notional_1h,
                    }
                )

        if not bar_rows:
            raise RuntimeError("no market bars were synced for the requested scope")

        upsert_market_bars_rows(conn, bar_rows)
        upsert_market_open_interest_rows(conn, oi_rows)
        upsert_market_funding_rows(conn, funding_rows)
        upsert_market_liquidation_rows(conn, liquidation_rows)

        enqueue_result = None
        if enqueue_recompute and bar_rows:
            enqueue_result = enqueue_governed_recompute(
                conn,
                workspace_slug=workspace_slug,
                watchlist_slug=watchlist_slug,
                trigger_type="manual",
                requested_by=requested_by,
                payload={
                    "source": "crypto_market_sync",
                    "sync_run_id": str(sync_run["id"]),
                    "requested_symbols": requested_symbols,
                    "synced_symbols": synced_symbols,
                    "provider_mode": metadata["provider_mode"],
                },
            )

        metadata = {
            **metadata,
            "missing_bar_symbols": missing_bar_symbols,
            "provider_coverage": provider_coverage,
            "symbol_errors": symbol_errors,
            "reference_quotes": {
                symbol: {
                    "price_usd": quote.price_usd,
                    "market_cap_usd": quote.market_cap_usd,
                    "volume_24h_usd": quote.volume_24h_usd,
                    "percent_change_24h": quote.percent_change_24h,
                    "cmc_rank": quote.cmc_rank,
                    "last_updated": quote.last_updated.isoformat() if quote.last_updated else None,
                }
                for symbol, quote in reference_quotes.items()
            },
            "global_market_metrics": global_market_metrics,
        }
        complete_market_data_sync_run(
            conn,
            str(sync_run["id"]),
            status="completed",
            synced_symbols=synced_symbols,
            bar_count=len(bar_rows),
            open_interest_count=len(oi_rows),
            funding_count=len(funding_rows),
            liquidation_count=len(liquidation_rows),
            metadata=metadata,
        )

        return MarketSyncResult(
            sync_run_id=str(sync_run["id"]),
            workspace_slug=workspace_slug,
            watchlist_slug=watchlist_slug,
            requested_symbols=requested_symbols,
            synced_symbols=synced_symbols,
            bar_count=len(bar_rows),
            open_interest_count=len(oi_rows),
            funding_count=len(funding_rows),
            liquidation_count=len(liquidation_rows),
            enqueue_result=enqueue_result,
            metadata=metadata,
        )
    except Exception as exc:
        logger.exception("crypto market sync failed")
        complete_market_data_sync_run(
            conn,
            str(sync_run["id"]),
            status="failed",
            synced_symbols=synced_symbols,
            bar_count=len(bar_rows),
            open_interest_count=len(oi_rows),
            funding_count=len(funding_rows),
            liquidation_count=len(liquidation_rows),
            metadata={
                **metadata,
                "provider_coverage": provider_coverage,
                "missing_bar_symbols": missing_bar_symbols,
                "symbol_errors": symbol_errors,
            },
            error=str(exc),
        )
        raise


def _bar_row(asset_id: str, bar: BinanceBarSnapshot) -> dict[str, Any]:
    return {
        "asset_id": asset_id,
        "timeframe": "1h",
        "ts": bar.ts,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "source": bar.source,
        "return_1h": bar.return_1h,
        "volume_zscore": bar.volume_zscore,
    }


def _promote_binance_derivative_to_coinglass_shape(
    snapshot: BinanceDerivativeSnapshot,
) -> CoinGlassMarketSnapshot:
    return CoinGlassMarketSnapshot(
        symbol=snapshot.symbol,
        ts=snapshot.ts,
        open_interest=snapshot.open_interest,
        oi_change_1h=snapshot.oi_change_1h,
        funding_rate=snapshot.funding_rate,
        liquidation_notional_1h=0.0,
        source=snapshot.source,
    )
