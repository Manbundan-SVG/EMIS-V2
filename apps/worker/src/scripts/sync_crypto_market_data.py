from __future__ import annotations

import argparse
import json

from src.config import get_settings
from src.db.client import get_connection
from src.ingestion.alpaca_equity_client import AlpacaEquityClient
from src.ingestion.binance_client import BinanceClient
from src.ingestion.coinmarketcap_client import CoinMarketCapClient
from src.ingestion.coinglass_client import CoinGlassClient
from src.ingestion.currency_api_client import CurrencyApiClient
from src.ingestion.fred_client import FredClient
from src.services.macro_sync_service import sync_macro_market_data
from src.services.market_sync_service import sync_crypto_market_data
from src.services.multi_asset_sync_service import MultiAssetSyncService


def main() -> None:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Sync EMIS market data into crypto and macro source tables.")
    parser.add_argument("--workspace", default=settings.default_workspace_slug, help="Workspace slug to sync")
    parser.add_argument("--watchlist", default=None, help="Optional watchlist slug to scope the sync")
    parser.add_argument("--no-enqueue", action="store_true", help="Do not enqueue a governed recompute after sync")
    parser.add_argument("--no-macro", action="store_true", help="Skip the FRED macro sync path")
    parser.add_argument("--macro-only", action="store_true", help="Sync only macro data via FRED and skip crypto providers")
    parser.add_argument("--no-multi-asset", action="store_true", help="Skip the Phase 4.0A multi-asset (equities/FX/rates) sync step")
    parser.add_argument("--multi-asset-only", action="store_true", help="Run only the Phase 4.0A multi-asset sync step")
    args = parser.parse_args()

    binance_client = BinanceClient(
        alpaca_api_key=settings.alpaca_api_key,
        alpaca_secret_key=settings.alpaca_secret_key,
        alpaca_data_base_url=settings.alpaca_data_base_url,
        massive_api_key=settings.massive_api_key,
        massive_base_url=settings.massive_base_url,
        okx_base_url=settings.okx_base_url,
        futures_base_url=settings.binance_futures_base_url,
        spot_base_url=settings.binance_spot_base_url,
        bybit_base_url=settings.bybit_base_url,
        timeout_seconds=settings.market_sync_timeout_seconds,
        quote_asset=settings.market_sync_quote_asset,
        symbol_overrides=settings.market_sync_symbol_overrides,
    )
    coinglass_client = CoinGlassClient(
        api_key=settings.coinglass_api_key,
        base_url=settings.coinglass_base_url,
        timeout_seconds=settings.market_sync_timeout_seconds,
    )
    coinmarketcap_client = CoinMarketCapClient(
        api_key=settings.coinmarketcap_api_key,
        base_url=settings.coinmarketcap_base_url,
        timeout_seconds=settings.market_sync_timeout_seconds,
    )
    fred_client = FredClient(
        api_key=settings.fred_api_key,
        base_url=settings.fred_base_url,
        timeout_seconds=settings.market_sync_timeout_seconds,
        dxy_series_code=settings.fred_dxy_series_code,
        us10y_series_code=settings.fred_us10y_series_code,
    )
    currency_api_client = CurrencyApiClient(
        api_key=settings.currency_api_key,
        base_url=settings.currency_api_base_url,
        timeout_seconds=settings.market_sync_timeout_seconds,
    )
    alpaca_equity_client = AlpacaEquityClient(
        api_key=settings.alpaca_api_key,
        secret_key=settings.alpaca_secret_key,
        base_url=settings.alpaca_data_base_url,
        timeout_seconds=settings.market_sync_timeout_seconds,
    )

    with get_connection() as conn:
        macro_result = None
        crypto_result = None
        multi_asset_results = None
        try:
            if not args.macro_only and not args.multi_asset_only:
                # Default path also runs macro + crypto. multi-asset-only skips both.
                pass
            if not args.no_macro and not args.multi_asset_only:
                macro_result = sync_macro_market_data(
                    conn,
                    workspace_slug=args.workspace,
                    watchlist_slug=args.watchlist,
                    fred_client=fred_client,
                    currency_api_client=currency_api_client,
                )
            if not args.macro_only and not args.multi_asset_only:
                crypto_result = sync_crypto_market_data(
                    conn,
                    workspace_slug=args.workspace,
                    watchlist_slug=args.watchlist,
                    enqueue_recompute=not args.no_enqueue,
                    requested_by="sync_crypto_market_data.py",
                    binance_client=binance_client,
                    coinglass_client=coinglass_client,
                    coinmarketcap_client=coinmarketcap_client,
                )
            if not args.no_multi_asset:
                multi_asset_service = MultiAssetSyncService()
                multi_asset_results = multi_asset_service.sync_all(
                    conn,
                    workspace_slug=args.workspace,
                    watchlist_slug=args.watchlist,
                    alpaca_client=alpaca_equity_client,
                    currency_api_key=settings.currency_api_key,
                    currency_api_base_url=settings.currency_api_base_url,
                    fred_api_key=settings.fred_api_key,
                    fred_base_url=settings.fred_base_url,
                    timeout_seconds=settings.market_sync_timeout_seconds,
                    fred_us2y_series_code=settings.fred_us2y_series_code,
                    fred_yield_spread_series_code=settings.fred_yield_spread_series_code,
                )
            if macro_result is None and crypto_result is None and multi_asset_results is None:
                raise RuntimeError("nothing to sync: all sync steps were disabled")
        except Exception:
            conn.commit()
            raise
        else:
            conn.commit()

    print(
        json.dumps(
            {
                "sync_run_id": crypto_result.sync_run_id if crypto_result else None,
                "workspace_slug": (crypto_result.workspace_slug if crypto_result else args.workspace),
                "watchlist_slug": (crypto_result.watchlist_slug if crypto_result else args.watchlist),
                "requested_symbols": crypto_result.requested_symbols if crypto_result else [],
                "synced_symbols": crypto_result.synced_symbols if crypto_result else [],
                "bar_count": crypto_result.bar_count if crypto_result else 0,
                "open_interest_count": crypto_result.open_interest_count if crypto_result else 0,
                "funding_count": crypto_result.funding_count if crypto_result else 0,
                "liquidation_count": crypto_result.liquidation_count if crypto_result else 0,
                "provider_mode": crypto_result.metadata.get("provider_mode") if crypto_result else None,
                "macro_result": (
                    {
                        "sync_run_id": macro_result.sync_run_id,
                        "macro_point_count": macro_result.macro_point_count,
                        "provider_mode": macro_result.provider_mode,
                        "skipped": macro_result.skipped,
                        "metadata": macro_result.metadata,
                    }
                    if macro_result is not None
                    else None
                ),
                "enqueue_result": crypto_result.enqueue_result if crypto_result else None,
                "multi_asset_results": (
                    [
                        {
                            "asset_class":         r.asset_class,
                            "source":              r.source,
                            "provider_family":     r.provider_family,
                            "sync_run_id":         r.sync_run_id,
                            "requested_symbols":   r.requested_symbols,
                            "synced_symbols":      r.synced_symbols,
                            "skipped":             r.skipped,
                            "provider_mode":       r.provider_mode,
                        }
                        for r in multi_asset_results
                    ]
                    if multi_asset_results is not None
                    else None
                ),
            },
            default=str,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
