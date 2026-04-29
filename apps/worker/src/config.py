import json
from dataclasses import dataclass
import os
from pathlib import Path
from dotenv import load_dotenv

_CONFIG_PATH = Path(__file__).resolve()
_WORKER_ROOT = _CONFIG_PATH.parents[1]
_REPO_ROOT = _CONFIG_PATH.parents[3]

# Prefer the worker-local secret file, with repo-root fallback for older setups.
load_dotenv(_WORKER_ROOT / ".env")
load_dotenv(_REPO_ROOT / ".env")


def _env(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value not in (None, ""):
            return value
    return default

@dataclass(frozen=True)
class Settings:
    database_url: str
    worker_id: str
    worker_poll_seconds: int
    default_workspace_slug: str
    coinglass_api_key: str | None
    coinglass_base_url: str
    alpaca_api_key: str | None
    alpaca_secret_key: str | None
    alpaca_data_base_url: str
    coinmarketcap_api_key: str | None
    coinmarketcap_base_url: str
    currency_api_key: str | None
    currency_api_base_url: str
    massive_api_key: str | None
    massive_base_url: str
    okx_base_url: str
    binance_futures_base_url: str
    binance_spot_base_url: str
    bybit_base_url: str
    fred_api_key: str | None
    fred_base_url: str
    fred_dxy_series_code: str
    fred_us10y_series_code: str
    fred_us2y_series_code: str
    fred_yield_spread_series_code: str
    market_sync_timeout_seconds: int
    market_sync_quote_asset: str
    market_sync_symbol_overrides: dict[str, str]
    multi_asset_equity_symbols: list[str]
    multi_asset_commodity_symbols: list[str]

def get_settings() -> Settings:
    symbol_overrides_raw = _env("MARKET_SYNC_SYMBOL_OVERRIDES_JSON", default="{}")
    try:
        symbol_overrides = json.loads(symbol_overrides_raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("MARKET_SYNC_SYMBOL_OVERRIDES_JSON must be valid JSON") from exc
    if not isinstance(symbol_overrides, dict):
        raise RuntimeError("MARKET_SYNC_SYMBOL_OVERRIDES_JSON must decode to an object")

    def _parse_symbol_list(env_name: str, default: list[str]) -> list[str]:
        raw = _env(env_name, default="")
        if not raw:
            return default
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = [tok.strip() for tok in raw.split(",") if tok.strip()]
        if isinstance(parsed, list):
            return [str(s) for s in parsed if s]
        return default

    equity_symbols = _parse_symbol_list(
        "MULTI_ASSET_EQUITY_SYMBOLS_JSON", ["SPY", "QQQ", "DIA", "IWM"]
    )
    commodity_symbols = _parse_symbol_list(
        "MULTI_ASSET_COMMODITY_SYMBOLS_JSON", ["GLD", "USO"]
    )

    return Settings(
        database_url=os.environ["DATABASE_URL"],
        worker_id=_env("WORKER_ID", default="worker-local-1") or "worker-local-1",
        worker_poll_seconds=int(_env("WORKER_POLL_SECONDS", default="2") or "2"),
        default_workspace_slug=_env("DEFAULT_WORKSPACE_SLUG", "EMIS_DEFAULT_WORKSPACE_SLUG", default="default") or "default",
        coinglass_api_key=_env("COINGLASS_API_KEY"),
        coinglass_base_url=_env("COINGLASS_BASE_URL", default="https://open-api-v4.coinglass.com") or "https://open-api-v4.coinglass.com",
        alpaca_api_key=_env("ALPACA_API_KEY", "Alpaca_API_KEY"),
        alpaca_secret_key=_env("ALPACA_SECRET_KEY", "Alpaca_SECRET_KEY"),
        alpaca_data_base_url=_env("ALPACA_DATA_BASE_URL", default="https://data.alpaca.markets") or "https://data.alpaca.markets",
        coinmarketcap_api_key=_env("COINMARKETCAP_API_KEY", "Coinmarketcap_API_KEY"),
        coinmarketcap_base_url=_env("COINMARKETCAP_BASE_URL", default="https://pro-api.coinmarketcap.com") or "https://pro-api.coinmarketcap.com",
        currency_api_key=_env("CURRENCY_API_KEY", "Currency_API_KEY"),
        currency_api_base_url=_env("CURRENCY_API_BASE_URL", default="https://api.currencyapi.com") or "https://api.currencyapi.com",
        massive_api_key=_env("MASSIVE_API_KEY"),
        massive_base_url=_env("MASSIVE_BASE_URL", default="https://api.massive.com") or "https://api.massive.com",
        okx_base_url=_env("OKX_BASE_URL", default="https://www.okx.com") or "https://www.okx.com",
        binance_futures_base_url=_env("BINANCE_FUTURES_BASE_URL", default="https://fapi.binance.com") or "https://fapi.binance.com",
        binance_spot_base_url=_env("BINANCE_SPOT_BASE_URL", default="https://api.binance.com") or "https://api.binance.com",
        bybit_base_url=_env("BYBIT_BASE_URL", default="https://api.bybit.com") or "https://api.bybit.com",
        fred_api_key=_env("FRED_API_KEY"),
        fred_base_url=_env("FRED_BASE_URL", default="https://api.stlouisfed.org/fred") or "https://api.stlouisfed.org/fred",
        fred_dxy_series_code=_env("FRED_DXY_SERIES_CODE", default="DTWEXBGS") or "DTWEXBGS",
        fred_us10y_series_code=_env("FRED_US10Y_SERIES_CODE", default="DGS10") or "DGS10",
        fred_us2y_series_code=_env("FRED_US2Y_SERIES_CODE", "FRED_2Y_SERIES_CODE", default="DGS2") or "DGS2",
        fred_yield_spread_series_code=_env("FRED_YIELD_SPREAD_SERIES_CODE", default="T10Y2Y") or "T10Y2Y",
        market_sync_timeout_seconds=int(_env("MARKET_SYNC_TIMEOUT_SECONDS", default="15") or "15"),
        market_sync_quote_asset=_env("MARKET_SYNC_QUOTE_ASSET", default="USDT") or "USDT",
        market_sync_symbol_overrides={str(key): str(value) for key, value in symbol_overrides.items()},
        multi_asset_equity_symbols=equity_symbols,
        multi_asset_commodity_symbols=commodity_symbols,
    )
