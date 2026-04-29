from __future__ import annotations

import json
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
from src.scripts.validate_phase25c import enqueue_replay, wait_for_run


WORKER_ROOT = Path(__file__).resolve().parents[2]


def enqueue_governed(cur, workspace_slug: str, payload: dict | None = None) -> str:
    cur.execute(
        "select * from public.enqueue_governed_recompute(%s, %s, 'manual', 'phase26c-validator', %s::jsonb)",
        (workspace_slug, "core", json.dumps(payload or {})),
    )
    row = cur.fetchone()
    if not row or not row["job_id"]:
        raise RuntimeError("enqueue_governed_recompute returned no job_id")
    return str(row["job_id"])


def fetch_version_rankings(cur, workspace_id: str) -> list[dict]:
    cur.execute(
        """
        select *
        from public.version_health_rankings
        where workspace_id = %s::uuid
        order by health_rank asc, governance_health_score desc
        """,
        (workspace_id,),
    )
    return [dict(row) for row in cur.fetchall()]


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
    cur.execute("delete from public.job_run_compute_scopes where workspace_id = %s::uuid", (workspace_id,))
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
    workspace_slug = f"phase26c-{suffix}"
    temp_symbols = [f"P26C{suffix[-4:]}", f"P26D{suffix[-4:]}"]
    worker_env = os.environ.copy()
    worker_env["WORKER_ID"] = f"phase26c-validator-{suffix}"
    worker = None
    workspace_id = None
    base_ts = now + timedelta(days=367)
    macro_timestamps = [base_ts + timedelta(minutes=5 * idx) for idx in range(7)]

    stable_runs: list[str] = []
    canary_runs: list[str] = []

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                workspace_id, watchlist_id = ensure_workspace(cur, workspace_slug)
                asset_a = ensure_asset(cur, temp_symbols[0], "Phase26C Asset A")
                asset_b = ensure_asset(cur, temp_symbols[1], "Phase26C Asset B")
                attach_watchlist_asset(cur, watchlist_id, asset_a, 1)
                attach_watchlist_asset(cur, watchlist_id, asset_b, 2)
                conn.commit()

                worker = subprocess.Popen([sys.executable, "-m", "src.main"], cwd=str(WORKER_ROOT), env=worker_env)
                time.sleep(3)

                stable_configs = [
                    (118, 76, 0.018, -0.007, 1.1, 0.8, 0.022, 0.013, 0.0010, 0.0008, 880000, 1100000, 0.0019, 0.028),
                    (118.5, 76.2, 0.017, -0.006, 1.0, 0.75, 0.021, 0.012, 0.0010, 0.0008, 860000, 1040000, 0.0020, 0.027),
                    (119, 76.4, 0.016, -0.006, 1.05, 0.78, 0.020, 0.012, 0.0011, 0.0009, 870000, 1080000, 0.0021, 0.026),
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
                        raise RuntimeError(f"expected completed stable run, got {run['status']}")
                    stable_runs.append(run_id)

                replay_run_id = enqueue_replay(cur, stable_runs[-1])
                conn.commit()
                replay_run = wait_for_run(cur, replay_run_id)
                if replay_run["status"] != "completed":
                    raise RuntimeError(f"expected completed replay run, got {replay_run['status']}")

                canary_payload = {
                    "replay": {
                        "pinned_versions": {
                            "compute_version": "phase2.6C-canary",
                            "signal_registry_version": "v2-canary",
                            "model_version": "v2-canary",
                        }
                    }
                }
                canary_configs = [
                    (121, 74, 0.042, -0.025, 2.7, 1.9, 0.090, -0.050, 0.0035, 0.0028, 3600000, 2900000, 0.0065, 0.095),
                    (112, 82, -0.031, 0.020, 0.6, 2.5, -0.080, 0.072, 0.0040, 0.0032, 4100000, 3300000, 0.0015, 0.018),
                    (126, 71, 0.057, -0.033, 3.1, 0.7, 0.120, -0.064, 0.0048, 0.0037, 4700000, 3500000, 0.0072, 0.108),
                ]

                for offset, config in enumerate(canary_configs, start=len(stable_configs)):
                    ts = macro_timestamps[offset]
                    seed_market_rows(cur, asset_a, ts, close=config[0], return_1h=config[2], volume_zscore=config[4], oi_change_1h=config[6], funding_rate=config[8], liquidation_notional_1h=config[10])
                    seed_market_rows(cur, asset_b, ts, close=config[1], return_1h=config[3], volume_zscore=config[5], oi_change_1h=config[7], funding_rate=config[9], liquidation_notional_1h=config[11])
                    seed_macro(cur, ts, dxy_return_1d=config[12], us10y_change_1d=config[13])
                    conn.commit()

                    run_id = enqueue_governed(cur, workspace_slug, canary_payload)
                    conn.commit()
                    run = wait_for_run(cur, run_id)
                    if run["status"] != "completed":
                        raise RuntimeError(f"expected completed canary run, got {run['status']}")
                    canary_runs.append(run_id)

                version_rows = fetch_version_rankings(cur, workspace_id)
                if len(version_rows) < 2:
                    raise RuntimeError("expected at least two version-governance rows")

                stable_row = next((row for row in version_rows if row["compute_version"] == "phase2.4"), None)
                canary_row = next((row for row in version_rows if row["compute_version"] == "phase2.6C-canary"), None)
                if not stable_row:
                    raise RuntimeError("missing stable version governance row")
                if not canary_row:
                    raise RuntimeError("missing canary version governance row")

                if int(stable_row["replay_count"]) < 1:
                    raise RuntimeError("stable version governance row should include replay_count >= 1")
                if float(stable_row["avg_input_match_score"]) < 0.999:
                    raise RuntimeError("stable version replay consistency should be near-perfect")
                if float(stable_row["governance_health_score"]) <= float(canary_row["governance_health_score"]):
                    raise RuntimeError("stable version should rank healthier than canary version")
                if int(stable_row["health_rank"]) >= int(canary_row["health_rank"]):
                    raise RuntimeError("stable version should rank ahead of canary version")
                if float(canary_row["avg_family_instability"]) <= float(stable_row["avg_family_instability"]):
                    raise RuntimeError("canary version should show higher family instability than stable version")

                print("phase26c smoke ok")
                print(f"workspace_slug={workspace_slug}")
                print(f"stable_version_health={stable_row['governance_health_score']}")
                print(f"stable_version_rank={stable_row['health_rank']}")
                print(f"stable_replay_count={stable_row['replay_count']}")
                print(f"stable_avg_input_match_score={stable_row['avg_input_match_score']}")
                print(f"canary_version_health={canary_row['governance_health_score']}")
                print(f"canary_version_rank={canary_row['health_rank']}")
                print(f"canary_avg_family_instability={canary_row['avg_family_instability']}")
                print(f"version_row_count={len(version_rows)}")
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
