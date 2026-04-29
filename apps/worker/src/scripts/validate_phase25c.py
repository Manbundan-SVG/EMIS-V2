from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timezone
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


WORKER_ROOT = Path(__file__).resolve().parents[2]


def enqueue_governed(cur, workspace_slug: str, watchlist_slug: str = "core") -> str:
    cur.execute(
        "select * from public.enqueue_governed_recompute(%s, %s, 'manual', 'phase25c-validator', %s::jsonb)",
        (workspace_slug, watchlist_slug, "{}"),
    )
    row = cur.fetchone()
    if not row or not row["job_id"]:
        raise RuntimeError("enqueue_governed_recompute returned no job_id")
    return str(row["job_id"])


def enqueue_replay(cur, run_id: str) -> str:
    cur.execute("select * from public.enqueue_replay_run(%s::uuid, %s)", (run_id, "phase25c-validator"))
    row = cur.fetchone()
    if not row or not row["job_id"]:
        raise RuntimeError("enqueue_replay_run returned no job_id")
    return str(row["job_id"])


def wait_for_run(cur, run_id: str, timeout_seconds: int = 120) -> dict:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        cur.execute("select * from public.run_inspection where run_id = %s::uuid", (run_id,))
        row = cur.fetchone()
        if row and row["status"] in {"completed", "dead_lettered", "failed"}:
            return dict(row)
        time.sleep(1.0)
    raise TimeoutError(f"timed out waiting for run {run_id}")


def cleanup(cur, workspace_id: str, temp_symbols: list[str]) -> None:
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
    cur.execute("delete from public.assets where symbol = any(%s)", (temp_symbols,))


def main() -> None:
    settings = get_settings()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    suffix = now.strftime("%Y%m%d%H%M%S")
    workspace_slug = f"phase25c-{suffix}"
    temp_symbols = [f"P25G{suffix[-4:]}", f"P25H{suffix[-4:]}"]
    worker_env = os.environ.copy()
    worker_env["WORKER_ID"] = f"phase25c-validator-{suffix}"
    worker = None
    workspace_id = None

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                workspace_id, watchlist_id = ensure_workspace(cur, workspace_slug)
                asset_a = ensure_asset(cur, temp_symbols[0], "Phase25C Asset A")
                asset_b = ensure_asset(cur, temp_symbols[1], "Phase25C Asset B")
                attach_watchlist_asset(cur, watchlist_id, asset_a, 1)
                attach_watchlist_asset(cur, watchlist_id, asset_b, 2)

                ts = now
                seed_market_rows(cur, asset_a, ts, close=122, return_1h=0.019, volume_zscore=1.5, oi_change_1h=0.04, funding_rate=0.0015, liquidation_notional_1h=850000)
                seed_market_rows(cur, asset_b, ts, close=78, return_1h=-0.011, volume_zscore=0.9, oi_change_1h=0.015, funding_rate=0.0009, liquidation_notional_1h=1400000)
                seed_macro(cur, ts, dxy_return_1d=0.0021, us10y_change_1d=0.03)
                conn.commit()

                worker = subprocess.Popen([sys.executable, "-m", "src.main"], cwd=str(WORKER_ROOT), env=worker_env)
                time.sleep(3)

                source_run_id = enqueue_governed(cur, workspace_slug)
                conn.commit()
                source_run = wait_for_run(cur, source_run_id)
                if source_run["status"] != "completed":
                    raise RuntimeError(f"expected completed source run, got {source_run['status']}")

                replay_run_id = enqueue_replay(cur, source_run_id)
                conn.commit()
                replay_run = wait_for_run(cur, replay_run_id)
                if replay_run["status"] != "completed":
                    raise RuntimeError(f"expected completed replay run, got {replay_run['status']}")

                cur.execute("select * from public.job_run_replay_deltas where replay_run_id = %s::uuid", (replay_run_id,))
                replay_delta = cur.fetchone()
                if not replay_delta:
                    raise RuntimeError("replay delta row missing")

                cur.execute(
                    "select * from public.job_run_version_behavior_comparison where workspace_id = %s::uuid and watchlist_id = %s::uuid",
                    (workspace_id, watchlist_id),
                )
                version_rows = cur.fetchall()
                if not version_rows:
                    raise RuntimeError("version behavior comparison view returned no rows")

                replay_delta = dict(replay_delta)
                if str(replay_delta["source_run_id"]) != source_run_id:
                    raise RuntimeError("replay delta source_run_id mismatch")
                if float(replay_delta["input_match_score"]) < 0.999:
                    raise RuntimeError(f"expected near-perfect input match, got {replay_delta['input_match_score']}")
                if bool(replay_delta["version_match"]) is not True:
                    raise RuntimeError("expected version_match=true for replay validator")
                if replay_delta["severity"] != "low":
                    raise RuntimeError(f"expected low replay severity, got {replay_delta['severity']}")

                print("phase25c smoke ok")
                print(f"workspace_slug={workspace_slug}")
                print(f"source_run_id={source_run_id}")
                print(f"replay_run_id={replay_run_id}")
                print(f"input_match_score={replay_delta['input_match_score']}")
                print(f"version_match={replay_delta['version_match']}")
                print(f"composite_delta_abs={replay_delta['composite_delta_abs']}")
                print(f"severity={replay_delta['severity']}")
                print(f"version_behavior_rows={len(version_rows)}")
            finally:
                if worker is not None:
                    worker.terminate()
                    try:
                        worker.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        worker.kill()
                        worker.wait(timeout=10)
                if workspace_id is not None:
                    cleanup(cur, workspace_id, temp_symbols)
                    conn.commit()


if __name__ == "__main__":
    main()
