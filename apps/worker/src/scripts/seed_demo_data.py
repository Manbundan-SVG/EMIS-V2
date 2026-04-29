import json
from datetime import datetime, timedelta, timezone
from psycopg import connect
from psycopg.rows import dict_row
from src.config import get_settings

ASSETS = [
    ("BTC", "Bitcoin", "crypto"),
    ("ETH", "Ethereum", "crypto"),
    ("SOL", "Solana", "crypto"),
    ("XRP", "XRP", "crypto"),
    ("SPY", "SPDR S&P 500 ETF", "equity"),
]

def main() -> None:
    settings = get_settings()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("insert into public.workspaces (slug, name) values ('demo', 'Demo Workspace') on conflict (slug) do update set name = excluded.name returning id")
            workspace_id = cur.fetchone()["id"]
            cur.execute("insert into public.watchlists (workspace_id, name, slug) values (%s, 'Core', 'core') on conflict (workspace_id, slug) do update set name = excluded.name returning id", (workspace_id,))
            watchlist_id = cur.fetchone()["id"]
            asset_ids = {}
            for symbol, name, asset_class in ASSETS:
                cur.execute("insert into public.assets (symbol, name, asset_class) values (%s, %s, %s) on conflict (symbol) do update set name = excluded.name, asset_class = excluded.asset_class returning id", (symbol, name, asset_class))
                asset_ids[symbol] = cur.fetchone()["id"]
            for idx, (symbol, _, _) in enumerate(ASSETS, start=1):
                cur.execute("insert into public.watchlist_assets (watchlist_id, asset_id, sort_order) values (%s, %s, %s) on conflict (watchlist_id, asset_id) do update set sort_order = excluded.sort_order", (watchlist_id, asset_ids[symbol], idx))
            cur.execute("delete from public.market_bars where ts >= %s", (now - timedelta(days=2),))
            cur.execute("delete from public.market_open_interest where ts >= %s", (now - timedelta(days=2),))
            cur.execute("delete from public.market_funding where ts >= %s", (now - timedelta(days=2),))
            cur.execute("delete from public.market_liquidations where ts >= %s", (now - timedelta(days=2),))
            cur.execute("delete from public.macro_series_points where ts >= %s", (now - timedelta(days=2),))
            demo = {
                "BTC": {"close": 84250, "ret": 0.008, "volz": 1.2, "oi": 0.035, "fund": 0.0018, "liq": 1800000},
                "ETH": {"close": 3975, "ret": 0.011, "volz": 1.4, "oi": 0.042, "fund": 0.0023, "liq": 2600000},
                "SOL": {"close": 196, "ret": 0.014, "volz": 1.7, "oi": 0.051, "fund": 0.0028, "liq": 3400000},
                "XRP": {"close": 1.88, "ret": 0.006, "volz": 0.8, "oi": 0.022, "fund": 0.0012, "liq": 700000},
                "SPY": {"close": 512, "ret": -0.002, "volz": 0.5, "oi": 0.0, "fund": 0.0, "liq": 0},
            }
            for symbol, values in demo.items():
                asset_id = asset_ids[symbol]
                cur.execute("insert into public.market_bars (asset_id, ts, open, high, low, close, volume, return_1h, volume_zscore) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (asset_id, now, values["close"] * (1 - values["ret"]), values["close"] * 1.01, values["close"] * 0.99, values["close"], 1000000, values["ret"], values["volz"]))
                cur.execute("insert into public.market_open_interest (asset_id, ts, open_interest, oi_change_1h) values (%s, %s, %s, %s)", (asset_id, now, 100000000, values["oi"]))
                cur.execute("insert into public.market_funding (asset_id, ts, funding_rate) values (%s, %s, %s)", (asset_id, now, values["fund"]))
                cur.execute("insert into public.market_liquidations (asset_id, ts, liquidation_notional_1h) values (%s, %s, %s)", (asset_id, now, values["liq"]))
            cur.execute(
                """
                insert into public.macro_series_points (series_code, ts, value, source, return_1d, change_1d)
                values
                  ('DXY', %s, 104.2, 'demo_seed', 0.0028, 0),
                  ('US10Y', %s, 4.18, 'demo_seed', 0, 0.045)
                """,
                (now, now),
            )
            cur.execute("select * from public.enqueue_recompute_job(%s, %s, %s, %s::jsonb)", ("demo", "seed", "seed-script", json.dumps({"source": "seed_demo_data.py"})))
            job = cur.fetchone()
            conn.commit()
            print(f"Seed complete. workspace_id={workspace_id} job_id={job['job_id']}")

if __name__ == "__main__":
    main()
