from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.db.repositories import (
    complete_market_data_sync_run,
    create_market_data_sync_run,
    resolve_workspace_watchlist_scope,
    upsert_macro_series_points_rows,
)
from src.ingestion.currency_api_client import CurrencyApiClient
from src.ingestion.fred_client import FredClient


@dataclass(frozen=True)
class MacroSyncResult:
    sync_run_id: str | None
    workspace_slug: str
    watchlist_slug: str | None
    macro_point_count: int
    provider_mode: str
    skipped: bool
    metadata: dict[str, Any]


def sync_macro_market_data(
    conn,
    *,
    workspace_slug: str,
    watchlist_slug: str | None = None,
    fred_client: FredClient,
    currency_api_client: CurrencyApiClient | None = None,
) -> MacroSyncResult:
    workspace, watchlist = resolve_workspace_watchlist_scope(conn, workspace_slug, watchlist_slug)
    provider_mode = "fred_api" if fred_client.enabled else "disabled"
    requested_symbols = ["DXY", "US10Y"]
    missing_series: list[str] = []
    if not fred_client.enabled and currency_api_client and currency_api_client.enabled:
        provider_mode = "currencyapi_fx_proxy"
        requested_symbols = ["DXY"]
        missing_series = ["US10Y"]
    if provider_mode == "disabled":
        return MacroSyncResult(
            sync_run_id=None,
            workspace_slug=workspace_slug,
            watchlist_slug=watchlist_slug,
            macro_point_count=0,
            provider_mode="disabled",
            skipped=True,
            metadata={"reason": "FRED_API_KEY and CURRENCY_API_KEY not configured"},
        )

    sync_run = create_market_data_sync_run(
        conn,
        source="macro_market_sync",
        workspace_id=str(workspace["id"]),
        watchlist_id=str(watchlist["id"]) if watchlist else None,
        requested_symbols=requested_symbols,
        metadata={"provider_mode": provider_mode, "missing_series": missing_series},
    )

    try:
        try:
            if fred_client.enabled:
                points = fred_client.fetch_macro_points()
                provider_mode = "fred_api"
                missing_series = []
            elif currency_api_client and currency_api_client.enabled:
                points = currency_api_client.fetch_macro_points()
                provider_mode = "currencyapi_fx_proxy"
                missing_series = ["US10Y"]
            else:
                points = []
        except Exception:
            if currency_api_client and currency_api_client.enabled:
                points = currency_api_client.fetch_macro_points()
                provider_mode = "currencyapi_fx_proxy"
                missing_series = ["US10Y"]
            else:
                raise
        rows = [
            {
                "series_code": point.series_code,
                "ts": point.ts,
                "value": point.value,
                "source": point.source,
                "return_1d": point.return_1d,
                "change_1d": point.change_1d,
            }
            for point in points
        ]
        upsert_macro_series_points_rows(conn, rows)
        complete_market_data_sync_run(
            conn,
            str(sync_run["id"]),
            status="completed",
            synced_symbols=requested_symbols,
            macro_point_count=len(rows),
            metadata={"provider_mode": provider_mode, "missing_series": missing_series},
        )
        return MacroSyncResult(
            sync_run_id=str(sync_run["id"]),
            workspace_slug=workspace_slug,
            watchlist_slug=watchlist_slug,
            macro_point_count=len(rows),
            provider_mode=provider_mode,
            skipped=False,
            metadata={"provider_mode": provider_mode, "missing_series": missing_series},
        )
    except Exception as exc:
        complete_market_data_sync_run(
            conn,
            str(sync_run["id"]),
            status="failed",
            synced_symbols=[],
            macro_point_count=0,
            metadata={"provider_mode": provider_mode, "missing_series": missing_series},
            error=str(exc),
        )
        raise
