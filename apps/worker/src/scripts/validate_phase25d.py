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


def cleanup(cur, workspace_id: str, temp_symbols: list[str], macro_timestamps: list[datetime]) -> None:
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


def fetch_transition_summary(cur, run_id: str) -> dict:
    cur.execute("select * from public.run_regime_stability_summary where run_id = %s::uuid", (run_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"missing regime transition summary for run {run_id}")
    return dict(row)


def fetch_transition_family_shifts(cur, run_id: str) -> list[dict]:
    cur.execute(
        """
        select *
        from public.regime_transition_family_shifts
        where run_id = %s::uuid
        order by family_delta_abs desc, signal_family asc
        """,
        (run_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def main() -> None:
    settings = get_settings()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    suffix = now.strftime("%Y%m%d%H%M%S")
    workspace_slug = f"phase25d-{suffix}"
    temp_symbols = [f"P25J{suffix[-4:]}", f"P25K{suffix[-4:]}"]
    worker_env = os.environ.copy()
    worker_env["WORKER_ID"] = f"phase25d-validator-{suffix}"
    worker = None
    workspace_id = None
    ts_one = now + timedelta(days=365)
    ts_two = ts_one + timedelta(minutes=5)

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                workspace_id, watchlist_id = ensure_workspace(cur, workspace_slug)
                asset_a = ensure_asset(cur, temp_symbols[0], "Phase25D Asset A")
                asset_b = ensure_asset(cur, temp_symbols[1], "Phase25D Asset B")
                attach_watchlist_asset(cur, watchlist_id, asset_a, 1)
                attach_watchlist_asset(cur, watchlist_id, asset_b, 2)

                seed_market_rows(cur, asset_a, ts_one, close=111, return_1h=0.017, volume_zscore=1.2, oi_change_1h=0.022, funding_rate=0.0011, liquidation_notional_1h=900000)
                seed_market_rows(cur, asset_b, ts_one, close=84, return_1h=-0.009, volume_zscore=0.8, oi_change_1h=0.014, funding_rate=0.0008, liquidation_notional_1h=1100000)
                seed_macro(cur, ts_one, dxy_return_1d=0.0019, us10y_change_1d=0.03)

                conn.commit()

                worker = subprocess.Popen([sys.executable, "-m", "src.main"], cwd=str(WORKER_ROOT), env=worker_env)
                time.sleep(3)

                run_one_id = enqueue_governed(cur, workspace_slug)
                conn.commit()
                run_one = wait_for_run(cur, run_one_id)
                if run_one["status"] != "completed":
                    raise RuntimeError(f"expected completed run one, got {run_one['status']}")

                run_one_summary = fetch_transition_summary(cur, run_one_id)
                if bool(run_one_summary["transition_detected"]):
                    raise RuntimeError("first run should not be marked as a transition")

                seed_market_rows(cur, asset_a, ts_two, close=113, return_1h=0.012, volume_zscore=1.1, oi_change_1h=0.018, funding_rate=0.0012, liquidation_notional_1h=950000)
                seed_market_rows(cur, asset_b, ts_two, close=81, return_1h=-0.015, volume_zscore=1.0, oi_change_1h=0.016, funding_rate=0.0010, liquidation_notional_1h=1300000)
                seed_macro(cur, ts_two, dxy_return_1d=0.0058, us10y_change_1d=0.09)
                conn.commit()

                run_two_id = enqueue_governed(cur, workspace_slug)
                conn.commit()
                run_two = wait_for_run(cur, run_two_id)
                if run_two["status"] != "completed":
                    raise RuntimeError(f"expected completed run two, got {run_two['status']}")

                run_two_summary = fetch_transition_summary(cur, run_two_id)
                run_two_shifts = fetch_transition_family_shifts(cur, run_two_id)
                if str(run_two_summary["prior_run_id"]) != run_one_id:
                    raise RuntimeError("regime transition prior_run_id mismatch")
                if not bool(run_two_summary["transition_detected"]):
                    raise RuntimeError("second run should have a detected transition")
                if run_two_summary["transition_classification"] == "none":
                    raise RuntimeError("second run transition classification should not be none")
                if not run_two_shifts:
                    raise RuntimeError("second run should have family shift rows")
                if run_two_summary["from_regime"] == run_two_summary["to_regime"]:
                    raise RuntimeError("second run should have changed dominant regime")

                replay_run_id = enqueue_replay(cur, run_two_id)
                conn.commit()
                replay_run = wait_for_run(cur, replay_run_id)
                if replay_run["status"] != "completed":
                    raise RuntimeError(f"expected completed replay run, got {replay_run['status']}")

                replay_summary = fetch_transition_summary(cur, replay_run_id)
                replay_shifts = fetch_transition_family_shifts(cur, replay_run_id)
                if replay_summary["transition_classification"] != "replay_suppressed":
                    raise RuntimeError("replay run should be replay_suppressed")
                if bool(replay_summary["transition_detected"]):
                    raise RuntimeError("replay run should not mark transition_detected=true")
                if replay_shifts:
                    raise RuntimeError("replay-suppressed run should not persist family shift rows")

                print("phase25d smoke ok")
                print(f"workspace_slug={workspace_slug}")
                print(f"baseline_run_id={run_one_id}")
                print(f"transition_run_id={run_two_id}")
                print(f"replay_run_id={replay_run_id}")
                print(f"from_regime={run_two_summary['from_regime']}")
                print(f"to_regime={run_two_summary['to_regime']}")
                print(f"transition_classification={run_two_summary['transition_classification']}")
                print(f"stability_score={run_two_summary['stability_score']}")
                print(f"anomaly_likelihood={run_two_summary['anomaly_likelihood']}")
                print(f"family_shift_rows={len(run_two_shifts)}")
            finally:
                if worker is not None:
                    worker.terminate()
                    try:
                        worker.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        worker.kill()
                        worker.wait(timeout=10)
                if workspace_id is not None:
                    cleanup(cur, workspace_id, temp_symbols, [ts_one, ts_two])
                    conn.commit()


if __name__ == "__main__":
    main()
