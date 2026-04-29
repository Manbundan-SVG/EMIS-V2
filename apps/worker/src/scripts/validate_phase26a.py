from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from psycopg import connect
from psycopg.rows import dict_row

from src.config import get_settings
from src.scripts.validate_phase25a import (
    attach_watchlist_asset,
    ensure_asset,
    ensure_workspace,
    seed_macro,
    seed_market_rows,
)
from src.scripts.validate_phase25c import enqueue_governed, enqueue_replay, wait_for_run


WORKER_ROOT = Path(__file__).resolve().parents[2]


def fetch_latest_stability(cur, workspace_id: str, watchlist_id: str) -> dict:
    cur.execute(
        """
        select *
        from public.latest_stability_summary
        where workspace_id = %s::uuid
          and (watchlist_id is not distinct from %s::uuid)
          and queue_name = 'recompute'
        order by created_at desc
        limit 1
        """,
        (workspace_id, watchlist_id),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("missing latest stability summary")
    return dict(row)


def cleanup(cur, workspace_id: str, temp_symbols: list[str], macro_timestamps: list[datetime]) -> None:
    cur.execute("delete from public.signal_family_stability_metrics where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.run_stability_baselines where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.replay_consistency_metrics where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.regime_stability_metrics where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.regime_transition_family_shifts where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.regime_transition_events where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.job_run_replay_deltas where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.job_run_drift_metrics where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.job_run_attributions where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.job_run_signal_family_attributions where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.job_run_stage_timings where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.job_run_explanations where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.job_run_input_snapshots where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.alert_events where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.job_dead_letters where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.job_queue where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.job_runs where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.watchlist_assets where watchlist_id in (select id from public.watchlists where workspace_id = %s::uuid)", (workspace_id,))
    cur.execute("delete from public.watchlists where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.queue_governance_rules where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.workspaces where id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.market_bars where asset_id in (select id from public.assets where symbol = any(%s))", (temp_symbols,))
    cur.execute("delete from public.market_open_interest where asset_id in (select id from public.assets where symbol = any(%s))", (temp_symbols,))
    cur.execute("delete from public.market_funding where asset_id in (select id from public.assets where symbol = any(%s))", (temp_symbols,))
    cur.execute("delete from public.market_liquidations where asset_id in (select id from public.assets where symbol = any(%s))", (temp_symbols,))
    cur.execute("delete from public.macro_series_points where series_code in ('DXY', 'US10Y') and ts = any(%s)", (macro_timestamps,))
    cur.execute("delete from public.assets where symbol = any(%s)", (temp_symbols,))


def main() -> None:
    settings = get_settings()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    suffix = now.strftime("%Y%m%d%H%M%S")
    workspace_slug = f"phase26a-{suffix}"
    temp_symbols = [f"P26A{suffix[-4:]}", f"P26B{suffix[-4:]}"]
    worker_env = os.environ.copy()
    worker_env["WORKER_ID"] = f"phase26a-validator-{suffix}"
    worker = None
    workspace_id = None
    base_ts = now + timedelta(days=366)
    macro_timestamps = [base_ts + timedelta(minutes=5 * idx) for idx in range(5)]

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                workspace_id, watchlist_id = ensure_workspace(cur, workspace_slug)
                asset_a = ensure_asset(cur, temp_symbols[0], "Phase26A Asset A")
                asset_b = ensure_asset(cur, temp_symbols[1], "Phase26A Asset B")
                attach_watchlist_asset(cur, watchlist_id, asset_a, 1)
                attach_watchlist_asset(cur, watchlist_id, asset_b, 2)
                conn.commit()

                worker = subprocess.Popen([sys.executable, "-m", "src.main"], cwd=str(WORKER_ROOT), env=worker_env)
                time.sleep(3)

                stable_runs: list[str] = []
                stable_configs = [
                    (111, 84, 0.017, -0.009, 1.2, 0.8, 0.022, 0.014, 0.0011, 0.0008, 900000, 1100000, 0.0019, 0.03),
                    (112, 84.5, 0.015, -0.008, 1.1, 0.75, 0.020, 0.013, 0.0010, 0.0007, 880000, 1050000, 0.0020, 0.028),
                    (112.5, 85, 0.014, -0.007, 1.05, 0.7, 0.019, 0.012, 0.0010, 0.0007, 860000, 1000000, 0.0021, 0.027),
                ]

                for index, config in enumerate(stable_configs):
                    ts = macro_timestamps[index]
                    seed_market_rows(cur, asset_a, ts, close=config[0], return_1h=config[2], volume_zscore=config[4], oi_change_1h=config[6], funding_rate=config[8], liquidation_notional_1h=config[10])
                    seed_market_rows(cur, asset_b, ts, close=config[1], return_1h=config[3], volume_zscore=config[5], oi_change_1h=config[7], funding_rate=config[9], liquidation_notional_1h=config[11])
                    seed_macro(cur, ts, dxy_return_1d=config[12], us10y_change_1d=config[13])
                    conn.commit()

                    run_id = enqueue_governed(cur, workspace_slug)
                    conn.commit()
                    run = wait_for_run(cur, run_id)
                    if run["status"] != "completed":
                        raise RuntimeError(f"expected stable completed run, got {run['status']}")
                    stable_runs.append(run_id)

                stable_summary = fetch_latest_stability(cur, workspace_id, watchlist_id)
                if str(stable_summary["run_id"]) != stable_runs[-1]:
                    raise RuntimeError("stable summary should point at latest stable run")
                if stable_summary["baseline_run_count"] < 2:
                    raise RuntimeError("stable summary should include prior baseline runs")
                if float(stable_summary["family_instability_score"]) >= 0.25:
                    raise RuntimeError("stable sequence produced unexpectedly high family instability")
                if stable_summary["stability_classification"] not in {"stable", "watch"}:
                    raise RuntimeError(f"stable sequence classification unexpected: {stable_summary['stability_classification']}")

                replay_run_id = enqueue_replay(cur, stable_runs[-1])
                conn.commit()
                replay_run = wait_for_run(cur, replay_run_id)
                if replay_run["status"] != "completed":
                    raise RuntimeError(f"expected completed replay run, got {replay_run['status']}")

                ts_four = macro_timestamps[3]
                seed_market_rows(cur, asset_a, ts_four, close=114, return_1h=0.010, volume_zscore=1.1, oi_change_1h=0.018, funding_rate=0.0012, liquidation_notional_1h=980000)
                seed_market_rows(cur, asset_b, ts_four, close=80, return_1h=-0.016, volume_zscore=1.0, oi_change_1h=0.016, funding_rate=0.0011, liquidation_notional_1h=1350000)
                seed_macro(cur, ts_four, dxy_return_1d=0.0058, us10y_change_1d=0.09)
                conn.commit()
                run_four_id = enqueue_governed(cur, workspace_slug)
                conn.commit()
                run_four = wait_for_run(cur, run_four_id)
                if run_four["status"] != "completed":
                    raise RuntimeError(f"expected completed transition run four, got {run_four['status']}")

                ts_five = macro_timestamps[4]
                seed_market_rows(cur, asset_a, ts_five, close=109, return_1h=-0.021, volume_zscore=1.6, oi_change_1h=-0.040, funding_rate=0.0032, liquidation_notional_1h=3200000)
                seed_market_rows(cur, asset_b, ts_five, close=77, return_1h=-0.018, volume_zscore=1.4, oi_change_1h=-0.028, funding_rate=0.0030, liquidation_notional_1h=2950000)
                seed_macro(cur, ts_five, dxy_return_1d=0.0018, us10y_change_1d=0.025)
                conn.commit()
                run_five_id = enqueue_governed(cur, workspace_slug)
                conn.commit()
                run_five = wait_for_run(cur, run_five_id)
                if run_five["status"] != "completed":
                    raise RuntimeError(f"expected completed transition run five, got {run_five['status']}")

                unstable_summary = fetch_latest_stability(cur, workspace_id, watchlist_id)
                if str(unstable_summary["run_id"]) != run_five_id:
                    raise RuntimeError("latest stability summary should point at latest primary run")
                if float(unstable_summary["family_instability_score"]) <= float(stable_summary["family_instability_score"]):
                    raise RuntimeError("family instability did not increase after churn")
                if float(unstable_summary["regime_instability_score"]) <= float(stable_summary["regime_instability_score"]):
                    raise RuntimeError("regime instability did not increase after repeated transitions")
                if (unstable_summary.get("replay_runs_considered") or 0) < 1:
                    raise RuntimeError("replay consistency metrics did not include prior replay runs")
                if unstable_summary.get("avg_input_match_score") is None:
                    raise RuntimeError("replay consistency metrics missing avg_input_match_score")
                if not unstable_summary.get("family_rows"):
                    raise RuntimeError("latest stability summary missing family rows")

                print("phase26a smoke ok")
                print(f"workspace_slug={workspace_slug}")
                print(f"stable_run_id={stable_runs[-1]}")
                print(f"replay_run_id={replay_run_id}")
                print(f"latest_run_id={run_five_id}")
                print(f"stable_classification={stable_summary['stability_classification']}")
                print(f"stable_family_instability={stable_summary['family_instability_score']}")
                print(f"latest_classification={unstable_summary['stability_classification']}")
                print(f"latest_family_instability={unstable_summary['family_instability_score']}")
                print(f"latest_regime_instability={unstable_summary['regime_instability_score']}")
                print(f"replay_runs_considered={unstable_summary['replay_runs_considered']}")
            finally:
                if worker is not None:
                    worker.terminate()
                    try:
                        worker.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        worker.kill()
                        worker.wait(timeout=10)
                if workspace_id is not None:
                    cleanup(cur, workspace_id, temp_symbols, macro_timestamps)
                    conn.commit()


if __name__ == "__main__":
    main()
