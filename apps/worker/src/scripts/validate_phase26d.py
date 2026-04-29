from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections import Counter
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
        "select * from public.enqueue_governed_recompute(%s, %s, 'manual', 'phase26d-validator', %s::jsonb)",
        (workspace_slug, "core", json.dumps(payload or {})),
    )
    row = cur.fetchone()
    if not row or not row["job_id"]:
        raise RuntimeError("enqueue_governed_recompute returned no job_id")
    return str(row["job_id"])


def fetch_governance_rules(cur) -> list[dict]:
    cur.execute(
        """
        select *
        from public.governance_alert_rules
        where workspace_id is null
        order by rule_name asc
        """
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_governance_events(cur, workspace_id: str) -> list[dict]:
    cur.execute(
        """
        select *
        from public.governance_alert_events
        where workspace_id = %s::uuid
        order by created_at asc
        """,
        (workspace_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_governance_state(cur, workspace_id: str) -> list[dict]:
    cur.execute(
        """
        select *
        from public.governance_alert_state
        where workspace_id = %s::uuid
        order by latest_triggered_at desc
        """,
        (workspace_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def cleanup(cur, workspace_id: str, temp_symbols: list[str], macro_timestamps: list[datetime]) -> None:
    cur.execute("delete from public.governance_alert_events where workspace_id = %s::uuid", (workspace_id,))
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
    workspace_slug = f"phase26d-{suffix}"
    temp_symbols = [f"P26E{suffix[-4:]}", f"P26F{suffix[-4:]}"]
    worker_env = os.environ.copy()
    worker_env["WORKER_ID"] = f"phase26d-validator-{suffix}"
    worker = None
    workspace_id = None
    base_ts = now + timedelta(days=368)
    macro_timestamps = [base_ts + timedelta(minutes=5 * idx) for idx in range(7)]

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                workspace_id, watchlist_id = ensure_workspace(cur, workspace_slug)
                asset_a = ensure_asset(cur, temp_symbols[0], "Phase26D Asset A")
                asset_b = ensure_asset(cur, temp_symbols[1], "Phase26D Asset B")
                attach_watchlist_asset(cur, watchlist_id, asset_a, 1)
                attach_watchlist_asset(cur, watchlist_id, asset_b, 2)
                conn.commit()

                rules = fetch_governance_rules(cur)
                if len(rules) < 5:
                    raise RuntimeError("expected seeded global governance alert rules")

                worker = subprocess.Popen([sys.executable, "-m", "src.main"], cwd=str(WORKER_ROOT), env=worker_env)
                time.sleep(3)

                stable_configs = [
                    (117, 75, 0.017, -0.007, 1.1, 0.8, 0.022, 0.013, 0.0010, 0.0008, 870000, 1080000, 0.0019, 0.028),
                    (117.6, 75.4, 0.016, -0.006, 1.0, 0.75, 0.020, 0.012, 0.0010, 0.0008, 860000, 1040000, 0.0020, 0.027),
                    (118.1, 75.8, 0.016, -0.006, 1.05, 0.78, 0.019, 0.012, 0.0011, 0.0009, 880000, 1060000, 0.0021, 0.026),
                ]

                stable_runs: list[str] = []
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
                            "compute_version": "phase2.6D-canary",
                            "signal_registry_version": "v2-canary",
                            "model_version": "v2-canary",
                        }
                    }
                }
                canary_configs = [
                    (121, 73, 0.045, -0.027, 2.9, 1.8, 0.095, -0.053, 0.0038, 0.0028, 3700000, 3000000, 0.0068, 0.099),
                    (111, 83, -0.034, 0.022, 0.6, 2.6, -0.082, 0.074, 0.0042, 0.0033, 4200000, 3400000, 0.0014, 0.017),
                    (126, 70, 0.058, -0.034, 3.2, 0.7, 0.123, -0.066, 0.0049, 0.0038, 4800000, 3550000, 0.0074, 0.111),
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

                events = fetch_governance_events(cur, workspace_id)
                state_rows = fetch_governance_state(cur, workspace_id)
                if not events:
                    raise RuntimeError("expected governance alert events for the canary tuple")
                if not state_rows:
                    raise RuntimeError("expected governance alert state rows for the canary tuple")

                stable_events = [row for row in events if row.get("compute_version") == "phase2.4"]
                if stable_events:
                    raise RuntimeError("stable tuple should not have emitted governance alerts")

                canary_events = [row for row in events if row.get("compute_version") == "phase2.6D-canary"]
                if not canary_events:
                    raise RuntimeError("canary tuple governance alerts are missing")

                event_counter = Counter(row["event_type"] for row in canary_events)
                expected = {"version_regression", "family_instability_spike", "stability_classification_downgrade"}
                missing = expected.difference(event_counter.keys())
                if missing:
                    raise RuntimeError(f"missing expected governance alert types: {sorted(missing)}")
                if any(count != 1 for count in event_counter.values()):
                    raise RuntimeError(f"cooldown dedupe failed, repeated governance events observed: {event_counter}")
                if "replay_degradation" in event_counter:
                    raise RuntimeError("stable replay should not have emitted replay_degradation")

                state_counter = Counter(row["event_type"] for row in state_rows if row.get("compute_version") == "phase2.6D-canary")
                for event_type in expected:
                    if state_counter.get(event_type, 0) != 1:
                        raise RuntimeError(f"state view missing expected canary event: {event_type}")

                print("phase26d smoke ok")
                print(f"workspace_slug={workspace_slug}")
                print(f"governance_event_types={sorted(event_counter.keys())}")
                print(f"governance_event_count={len(events)}")
                print(f"canary_event_count={len(canary_events)}")
                print(f"state_row_count={len(state_rows)}")
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
