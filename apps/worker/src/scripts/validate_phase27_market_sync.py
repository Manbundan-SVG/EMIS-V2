from __future__ import annotations

from datetime import datetime, timezone

from psycopg import connect
from psycopg.rows import dict_row

from src.config import get_settings
from src.db.repositories import load_latest_market_state
from src.ingestion.binance_client import BinanceBarSnapshot, BinanceDerivativeSnapshot
from src.ingestion.coinglass_client import CoinGlassMarketSnapshot
from src.ingestion.fred_client import FredSeriesPoint
from src.services.macro_sync_service import sync_macro_market_data
from src.services.market_sync_service import sync_crypto_market_data


class FakeBinanceClient:
    def __init__(self, ts: datetime) -> None:
        self.ts = ts

    def fetch_hourly_bar(self, asset_symbol: str, lookback_hours: int = 25) -> BinanceBarSnapshot | None:
        values = {
            "BTC": (64000.0, 0.012, 1.25),
            "ETH": (3200.0, -0.006, 0.85),
        }.get(asset_symbol)
        if values is None:
            return None
        close_price, return_1h, volume_zscore = values
        previous_close = close_price / (1 + return_1h)
        return BinanceBarSnapshot(
            symbol=asset_symbol,
            ts=self.ts,
            open=previous_close,
            high=close_price * 1.01,
            low=close_price * 0.99,
            close=close_price,
            volume=125_000_000.0,
            return_1h=return_1h,
            volume_zscore=volume_zscore,
            source="binance_futures",
        )

    def fetch_derivative_snapshot(self, asset_symbol: str) -> BinanceDerivativeSnapshot | None:
        return None


class FakeCoinGlassClient:
    enabled = True

    def __init__(self, ts: datetime) -> None:
        self.ts = ts

    def fetch_coin_markets(self, symbols: list[str]) -> dict[str, CoinGlassMarketSnapshot]:
        rows = {
            "BTC": CoinGlassMarketSnapshot(
                symbol="BTC",
                ts=self.ts,
                open_interest=18_500_000_000.0,
                oi_change_1h=0.042,
                funding_rate=0.0012,
                liquidation_notional_1h=2_400_000.0,
                source="coinglass_futures",
            ),
            "ETH": CoinGlassMarketSnapshot(
                symbol="ETH",
                ts=self.ts,
                open_interest=9_750_000_000.0,
                oi_change_1h=-0.018,
                funding_rate=0.0008,
                liquidation_notional_1h=1_100_000.0,
                source="coinglass_futures",
            ),
        }
        return {symbol: rows[symbol] for symbol in symbols if symbol in rows}


class FakeFredClient:
    enabled = True

    def __init__(self, ts: datetime) -> None:
        self.ts = ts

    def fetch_macro_points(self) -> list[FredSeriesPoint]:
        return [
            FredSeriesPoint(
                series_code="DXY",
                ts=self.ts,
                value=104.6,
                return_1d=0.0032,
                change_1d=None,
                source="fred_api",
            ),
            FredSeriesPoint(
                series_code="US10Y",
                ts=self.ts,
                value=4.31,
                return_1d=None,
                change_1d=0.06,
                source="fred_api",
            ),
        ]


def main() -> None:
    settings = get_settings()
    ts = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    workspace_slug = f"phase27-sync-{int(ts.timestamp())}"

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "insert into public.workspaces (slug, name) values (%s, %s) returning id",
                (workspace_slug, "Phase 27 Sync Validation"),
            )
            workspace_id = str(cur.fetchone()["id"])
            cur.execute(
                "insert into public.watchlists (workspace_id, slug, name) values (%s::uuid, %s, %s) returning id",
                (workspace_id, "core", "Core"),
            )
            watchlist_id = str(cur.fetchone()["id"])

            assets: dict[str, str] = {}
            for idx, symbol in enumerate(("BTC", "ETH", "SPY"), start=1):
                cur.execute(
                    """
                    insert into public.assets (symbol, name, asset_class)
                    values (%s, %s, %s)
                    on conflict (symbol) do update
                    set name = excluded.name,
                        asset_class = excluded.asset_class
                    returning id
                    """,
                    (symbol, f"{symbol} Validation", "crypto" if symbol != "SPY" else "equity_index_proxy"),
                )
                assets[symbol] = str(cur.fetchone()["id"])
                if symbol != "SPY":
                    cur.execute(
                        """
                        insert into public.watchlist_assets (watchlist_id, asset_id, sort_order)
                        values (%s::uuid, %s::uuid, %s)
                        on conflict (watchlist_id, asset_id) do update
                        set sort_order = excluded.sort_order
                        """,
                        (watchlist_id, assets[symbol], idx),
                    )

        try:
            macro = sync_macro_market_data(
                conn,
                workspace_slug=workspace_slug,
                watchlist_slug="core",
                fred_client=FakeFredClient(ts),
            )
            first = sync_crypto_market_data(
                conn,
                workspace_slug=workspace_slug,
                watchlist_slug="core",
                enqueue_recompute=False,
                requested_by="validate_phase27_market_sync",
                binance_client=FakeBinanceClient(ts),
                coinglass_client=FakeCoinGlassClient(ts),
            )
            second = sync_crypto_market_data(
                conn,
                workspace_slug=workspace_slug,
                watchlist_slug="core",
                enqueue_recompute=False,
                requested_by="validate_phase27_market_sync",
                binance_client=FakeBinanceClient(ts),
                coinglass_client=FakeCoinGlassClient(ts),
            )

            if first.bar_count != 2 or first.open_interest_count != 2 or first.funding_count != 2 or first.liquidation_count != 2:
                raise RuntimeError("first sync did not persist the expected row counts")
            if second.bar_count != 2 or second.open_interest_count != 2 or second.funding_count != 2 or second.liquidation_count != 2:
                raise RuntimeError("second sync did not preserve the expected row counts")

            market_states = load_latest_market_state(conn, workspace_id, asset_symbols=["BTC", "ETH"])
            if len(market_states) != 2:
                raise RuntimeError("expected two market states after sync")
            for state in market_states:
                if not state.has_open_interest or not state.has_funding or not state.has_liquidations:
                    raise RuntimeError("synced state did not expose full derivatives coverage")
                if not state.has_macro_dxy or not state.has_macro_us10y:
                    raise RuntimeError("macro sync did not expose macro coverage")

            with conn.cursor() as cur:
                cur.execute(
                    "select count(*)::int as count from public.macro_series_points where series_code in ('DXY', 'US10Y') and ts = %s::timestamptz and source = 'fred_api'",
                    (ts,),
                )
                macro_count = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*)::int as count from public.market_bars where asset_id = any(%s::uuid[]) and ts = %s::timestamptz and source = 'binance_futures'",
                    ([assets["BTC"], assets["ETH"]], ts),
                )
                bar_count = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*)::int as count from public.market_open_interest where asset_id = any(%s::uuid[]) and ts = %s::timestamptz and source = 'coinglass_futures'",
                    ([assets["BTC"], assets["ETH"]], ts),
                )
                oi_count = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*)::int as count from public.market_funding where asset_id = any(%s::uuid[]) and ts = %s::timestamptz and source = 'coinglass_futures'",
                    ([assets["BTC"], assets["ETH"]], ts),
                )
                funding_count = int(cur.fetchone()["count"])
                cur.execute(
                    "select count(*)::int as count from public.market_liquidations where asset_id = any(%s::uuid[]) and ts = %s::timestamptz and source = 'coinglass_futures' and side = 'all'",
                    ([assets["BTC"], assets["ETH"]], ts),
                )
                liquidation_count = int(cur.fetchone()["count"])

            if macro.macro_point_count != 2 or macro_count != 2:
                raise RuntimeError("macro sync did not persist the expected row counts")
            if bar_count != 2 or oi_count != 2 or funding_count != 2 or liquidation_count != 2:
                raise RuntimeError("idempotent upsert expectations failed")

            conn.rollback()
            print(
                "phase27 market sync smoke ok "
                f"workspace_slug={workspace_slug} bars={bar_count} oi={oi_count} funding={funding_count} liquidations={liquidation_count} macro={macro_count}"
            )
        finally:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(
                    "delete from public.macro_series_points where series_code in ('DXY', 'US10Y') and ts = %s::timestamptz and source = 'fred_api'",
                    (ts,),
                )
                cur.execute("delete from public.workspaces where slug = %s", (workspace_slug,))
            conn.commit()


if __name__ == "__main__":
    main()
