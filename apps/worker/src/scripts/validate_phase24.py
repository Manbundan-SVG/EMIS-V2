from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from psycopg import connect
from psycopg.rows import dict_row

from src.config import get_settings


WORKER_ROOT = Path(__file__).resolve().parents[2]
STAGES = {
    "load_inputs",
    "build_features",
    "build_signals",
    "build_composite",
    "persist_outputs",
    "emit_alerts",
}


def ensure_workspace(cur, slug: str) -> tuple[str, str]:
    cur.execute(
        """
        insert into public.workspaces (slug, name)
        values (%s, %s)
        on conflict (slug) do update
        set name = excluded.name
        returning id
        """,
        (slug, f"Phase 2.4 Smoke {slug}"),
    )
    workspace_id = str(cur.fetchone()["id"])
    cur.execute(
        """
        insert into public.watchlists (workspace_id, slug, name)
        values (%s::uuid, 'core', 'Core')
        on conflict (workspace_id, slug) do update
        set name = excluded.name
        returning id
        """,
        (workspace_id,),
    )
    watchlist_id = str(cur.fetchone()["id"])

    cur.execute(
        """
        insert into public.queue_governance_rules (
          workspace_id,
          watchlist_id,
          job_type,
          enabled,
          max_concurrent,
          dedupe_window_seconds,
          suppress_if_queued,
          suppress_if_claimed,
          manual_priority,
          scheduled_priority
        ) values (
          %s::uuid,
          %s::uuid,
          'recompute',
          true,
          1,
          5,
          true,
          true,
          1,
          10
        )
        on conflict (workspace_id, watchlist_id, job_type) do update
        set enabled = excluded.enabled,
            max_concurrent = excluded.max_concurrent,
            dedupe_window_seconds = excluded.dedupe_window_seconds,
            suppress_if_queued = excluded.suppress_if_queued,
            suppress_if_claimed = excluded.suppress_if_claimed,
            manual_priority = excluded.manual_priority,
            scheduled_priority = excluded.scheduled_priority,
            updated_at = now()
        """,
        (workspace_id, watchlist_id),
    )
    return workspace_id, watchlist_id


def ensure_asset(cur, symbol: str, name: str) -> str:
    cur.execute(
        """
        insert into public.assets (symbol, name, asset_class)
        values (%s, %s, 'crypto')
        on conflict (symbol) do update
        set name = excluded.name
        returning id
        """,
        (symbol, name),
    )
    return str(cur.fetchone()["id"])


def attach_watchlist_asset(cur, watchlist_id: str, asset_id: str, sort_order: int) -> None:
    cur.execute(
        """
        insert into public.watchlist_assets (watchlist_id, asset_id, sort_order)
        values (%s::uuid, %s::uuid, %s)
        on conflict (watchlist_id, asset_id) do update
        set sort_order = excluded.sort_order
        """,
        (watchlist_id, asset_id, sort_order),
    )


def seed_market_rows(
    cur,
    asset_id: str,
    ts: datetime,
    *,
    close: float,
    return_1h: float | None,
    volume_zscore: float,
    oi_change_1h: float,
    funding_rate: float,
    liquidation_notional_1h: float,
) -> None:
    cur.execute(
        """
        insert into public.market_bars (asset_id, ts, open, high, low, close, volume, return_1h, volume_zscore)
        values (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            asset_id,
            ts,
            close * 0.99,
            close * 1.01,
            close * 0.98,
            close,
            1000000,
            return_1h,
            volume_zscore,
        ),
    )
    cur.execute(
        """
        insert into public.market_open_interest (asset_id, ts, open_interest, oi_change_1h)
        values (%s::uuid, %s, %s, %s)
        """,
        (asset_id, ts, 100000000, oi_change_1h),
    )
    cur.execute(
        """
        insert into public.market_funding (asset_id, ts, funding_rate)
        values (%s::uuid, %s, %s)
        """,
        (asset_id, ts, funding_rate),
    )
    cur.execute(
        """
        insert into public.market_liquidations (asset_id, ts, liquidation_notional_1h)
        values (%s::uuid, %s, %s)
        """,
        (asset_id, ts, liquidation_notional_1h),
    )


def seed_macro(cur, ts: datetime, dxy_return_1d: float, us10y_change_1d: float) -> None:
    cur.execute(
        """
        insert into public.macro_series_points (series_code, ts, value, source, return_1d, change_1d)
        values
          ('DXY', %s, 104.0, 'phase24_validator', %s, 0),
          ('US10Y', %s, 4.2, 'phase24_validator', 0, %s)
        """,
        (ts, dxy_return_1d, ts, us10y_change_1d),
    )


def enqueue_governed(cur, workspace_slug: str, watchlist_slug: str = "core") -> dict[str, Any]:
    cur.execute(
        "select * from public.enqueue_governed_recompute(%s, %s, 'manual', 'phase24-validator', %s::jsonb)",
        (workspace_slug, watchlist_slug, "{}"),
    )
    return dict(cur.fetchone())


def enqueue_replay(cur, source_run_id: str) -> dict[str, Any]:
    cur.execute(
        "select * from public.enqueue_replay_run(%s::uuid, %s)",
        (source_run_id, "phase24-validator"),
    )
    return dict(cur.fetchone())


def fetch_run(cur, run_id: str) -> dict[str, Any]:
    cur.execute("select * from public.run_inspection where run_id = %s::uuid", (run_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"run {run_id} not found in run_inspection")
    return dict(row)


def fetch_stage_timings(cur, run_id: str) -> list[dict[str, Any]]:
    cur.execute(
        "select * from public.job_run_stage_timings where job_run_id = %s::uuid order by started_at asc",
        (run_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_explanation(cur, run_id: str) -> dict[str, Any] | None:
    cur.execute("select * from public.job_run_explanations where job_run_id = %s::uuid", (run_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_input_snapshot(cur, run_id: str) -> dict[str, Any] | None:
    cur.execute("select * from public.job_run_input_snapshots where job_run_id = %s::uuid", (run_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_comparison(cur, run_id: str) -> dict[str, Any] | None:
    cur.execute("select * from public.job_run_prior_comparison where run_id = %s::uuid", (run_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def wait_for_run(cur, run_id: str, predicate, timeout_seconds: int = 90) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        run = fetch_run(cur, run_id)
        if predicate(run):
            return run
        time.sleep(1.0)
    raise TimeoutError(f"timed out waiting for run {run_id}")


def cleanup(cur, workspace_id: str, temp_symbols: list[str]) -> None:
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
    cur.execute("delete from public.alert_policy_rules where workspace_id = %s::uuid", (workspace_id,))
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
    workspace_slug = f"phase24-{suffix}"
    temp_symbols = [f"P24A{suffix[-4:]}", f"P24B{suffix[-4:]}", f"P24F{suffix[-4:]}"]
    worker_env = os.environ.copy()
    worker_env.setdefault("WORKER_ID", f"phase24-validator-{suffix}")
    worker: subprocess.Popen[str] | None = None
    workspace_id: str | None = None

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                workspace_id, watchlist_id = ensure_workspace(cur, workspace_slug)
                asset_a = ensure_asset(cur, temp_symbols[0], "Phase24 Asset A")
                asset_b = ensure_asset(cur, temp_symbols[1], "Phase24 Asset B")
                attach_watchlist_asset(cur, watchlist_id, asset_a, 1)
                attach_watchlist_asset(cur, watchlist_id, asset_b, 2)

                t1 = now
                seed_market_rows(cur, asset_a, t1, close=110, return_1h=0.012, volume_zscore=1.1, oi_change_1h=0.02, funding_rate=0.001, liquidation_notional_1h=300000)
                seed_market_rows(cur, asset_b, t1, close=88, return_1h=-0.004, volume_zscore=0.7, oi_change_1h=0.01, funding_rate=0.0005, liquidation_notional_1h=150000)
                seed_macro(cur, t1, dxy_return_1d=0.001, us10y_change_1d=0.02)
                conn.commit()

                worker = subprocess.Popen(
                    [sys.executable, "-m", "src.main"],
                    cwd=str(WORKER_ROOT),
                    env=worker_env,
                )
                time.sleep(3)

                success_one = enqueue_governed(cur, workspace_slug)
                conn.commit()
                success_one_run = str(success_one["job_id"])
                run_one = wait_for_run(cur, success_one_run, lambda row: row["status"] == "completed")
                explanation_one = fetch_explanation(cur, success_one_run)
                input_snapshot_one = fetch_input_snapshot(cur, success_one_run)
                stages_one = fetch_stage_timings(cur, success_one_run)
                if explanation_one is None or input_snapshot_one is None:
                    raise RuntimeError("phase2.4 success run did not persist explanation and input snapshot")
                if {row["stage_name"] for row in stages_one} != STAGES:
                    raise RuntimeError(f"unexpected stage set for success run: {stages_one}")

                original_evidence = {
                    "completed_at": run_one["completed_at"],
                    "input_snapshot_id": run_one["input_snapshot_id"],
                    "explanation_version": run_one["explanation_version"],
                }

                t2 = now + timedelta(minutes=5)
                seed_market_rows(cur, asset_a, t2, close=118, return_1h=0.026, volume_zscore=1.9, oi_change_1h=0.045, funding_rate=0.0029, liquidation_notional_1h=3200000)
                seed_market_rows(cur, asset_b, t2, close=81, return_1h=-0.018, volume_zscore=1.4, oi_change_1h=0.03, funding_rate=0.0027, liquidation_notional_1h=2500000)
                seed_macro(cur, t2, dxy_return_1d=0.0062, us10y_change_1d=0.08)
                success_two = enqueue_governed(cur, workspace_slug)
                conn.commit()
                success_two_run = str(success_two["job_id"])
                wait_for_run(cur, success_two_run, lambda row: row["status"] == "completed")
                comparison = fetch_comparison(cur, success_two_run)
                if not comparison or str(comparison["prior_run_id"]) != success_one_run:
                    raise RuntimeError("prior-run comparison did not link to the first successful run")
                if not any(
                    comparison[key]
                    for key in (
                        "regime_changes",
                        "signal_changes",
                        "composite_changes",
                        "input_coverage_changes",
                    )
                ):
                    raise RuntimeError("prior-run comparison returned no meaningful changes")

                replay = enqueue_replay(cur, success_one_run)
                conn.commit()
                replay_run_id = str(replay["job_id"])
                replay_run = wait_for_run(cur, replay_run_id, lambda row: row["status"] == "completed")
                if not replay_run["is_replay"] or str(replay_run["replayed_from_run_id"]) != success_one_run:
                    raise RuntimeError("replay run linkage was not preserved")
                original_after_replay = fetch_run(cur, success_one_run)
                for key, expected in original_evidence.items():
                    if original_after_replay[key] != expected:
                        raise RuntimeError(f"original run evidence mutated on replay for {key}")

                failure_asset = ensure_asset(cur, temp_symbols[2], "Phase24 Failure Asset")
                attach_watchlist_asset(cur, watchlist_id, failure_asset, 3)
                t3 = now + timedelta(minutes=10)
                seed_market_rows(cur, failure_asset, t3, close=42, return_1h=None, volume_zscore=0.8, oi_change_1h=0.01, funding_rate=0.001, liquidation_notional_1h=120000)
                failure = enqueue_governed(cur, workspace_slug)
                conn.commit()
                failure_run_id = str(failure["job_id"])
                failure_run = wait_for_run(
                    cur,
                    failure_run_id,
                    lambda row: row["failure_stage"] is not None and row["failure_code"] is not None,
                )
                failure_stages = fetch_stage_timings(cur, failure_run_id)
                failed_stage_rows = [row for row in failure_stages if row["stage_status"] == "failed"]
                if failure_run["failure_stage"] != "build_signals":
                    raise RuntimeError(f"expected build_signals failure stage, got {failure_run['failure_stage']}")
                if not failed_stage_rows or failed_stage_rows[-1]["stage_name"] != "build_signals":
                    raise RuntimeError(f"expected build_signals failed stage row, got {failed_stage_rows}")

                print("phase24 smoke ok")
                print(f"workspace_slug={workspace_slug}")
                print(f"success_run_1={success_one_run}")
                print(f"success_run_2={success_two_run}")
                print(f"replay_run={replay_run_id}")
                print(f"failed_run={failure_run_id}")
                print(f"comparison_prior_run_id={comparison['prior_run_id']}")
                print(f"failure_stage={failure_run['failure_stage']} failure_code={failure_run['failure_code']}")
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
