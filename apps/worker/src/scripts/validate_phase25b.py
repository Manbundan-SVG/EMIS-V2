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


def ensure_workspace(cur, slug: str) -> tuple[str, str]:
    cur.execute(
        """
        insert into public.workspaces (slug, name)
        values (%s, %s)
        on conflict (slug) do update
        set name = excluded.name
        returning id
        """,
        (slug, f"Phase 2.5B Smoke {slug}"),
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
          1,
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
    return_1h: float,
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
            1500000,
            return_1h,
            volume_zscore,
        ),
    )
    cur.execute(
        """
        insert into public.market_open_interest (asset_id, ts, open_interest, oi_change_1h)
        values (%s::uuid, %s, %s, %s)
        """,
        (asset_id, ts, 125000000, oi_change_1h),
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
          ('DXY', %s, 104.0, 'phase25b_validator', %s, 0),
          ('US10Y', %s, 4.2, 'phase25b_validator', 0, %s)
        """,
        (ts, dxy_return_1d, ts, us10y_change_1d),
    )


def enqueue_governed(cur, workspace_slug: str, watchlist_slug: str = "core") -> dict[str, Any]:
    cur.execute(
        "select * from public.enqueue_governed_recompute(%s, %s, 'manual', 'phase25b-validator', %s::jsonb)",
        (workspace_slug, watchlist_slug, "{}"),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("enqueue_governed_recompute returned no rows")
    return dict(row)


def wait_for_run(cur, run_id: str, timeout_seconds: int = 90) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        cur.execute("select * from public.run_inspection where run_id = %s::uuid", (run_id,))
        row = cur.fetchone()
        if row and row["status"] in {"completed", "dead_lettered", "failed"}:
            return dict(row)
        time.sleep(1.0)
    raise TimeoutError(f"timed out waiting for run {run_id}")


def fetch_drift_summary(cur, run_id: str) -> dict[str, Any]:
    cur.execute("select * from public.job_run_drift_summary where run_id = %s::uuid", (run_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"drift summary missing for run {run_id}")
    return dict(row)


def fetch_drift_metrics(cur, run_id: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        select *
        from public.job_run_drift_metrics
        where run_id = %s::uuid
        order by drift_flag desc, abs(coalesce(delta_pct, 0)) desc, metric_type asc, entity_name asc
        """,
        (run_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def assert_true(label: str, condition: bool) -> None:
    if not condition:
        raise RuntimeError(label)


def cleanup(cur, workspace_id: str, temp_symbols: list[str]) -> None:
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
    workspace_slug = f"phase25b-{suffix}"
    temp_symbols = [f"P25C{suffix[-4:]}", f"P25D{suffix[-4:]}"]
    worker_env = os.environ.copy()
    worker_env.setdefault("WORKER_ID", f"phase25b-validator-{suffix}")
    worker: subprocess.Popen[str] | None = None
    workspace_id: str | None = None

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                workspace_id, watchlist_id = ensure_workspace(cur, workspace_slug)
                asset_a = ensure_asset(cur, temp_symbols[0], "Phase25B Asset A")
                asset_b = ensure_asset(cur, temp_symbols[1], "Phase25B Asset B")
                attach_watchlist_asset(cur, watchlist_id, asset_a, 1)
                attach_watchlist_asset(cur, watchlist_id, asset_b, 2)

                ts1 = now
                ts2 = now + timedelta(minutes=5)

                seed_market_rows(cur, asset_a, ts1, close=122, return_1h=0.019, volume_zscore=1.5, oi_change_1h=0.04, funding_rate=0.0015, liquidation_notional_1h=850000)
                seed_market_rows(cur, asset_b, ts1, close=78, return_1h=-0.011, volume_zscore=0.9, oi_change_1h=0.015, funding_rate=0.0009, liquidation_notional_1h=1400000)
                seed_macro(cur, ts1, dxy_return_1d=0.0021, us10y_change_1d=0.03)
                conn.commit()

                worker = subprocess.Popen(
                    [sys.executable, "-m", "src.main"],
                    cwd=str(WORKER_ROOT),
                    env=worker_env,
                )
                time.sleep(3)

                run1_enqueue = enqueue_governed(cur, workspace_slug)
                conn.commit()
                run1_id = str(run1_enqueue["job_id"])
                run1 = wait_for_run(cur, run1_id)
                assert_true("expected first run completed", run1["status"] == "completed")

                run1_summary = fetch_drift_summary(cur, run1_id)
                run1_metrics = fetch_drift_metrics(cur, run1_id)
                assert_true("expected first run drift metrics", len(run1_metrics) > 0)
                assert_true("expected first run to have no flagged drift without comparator", int(run1_summary["flagged_metric_count"]) == 0)
                assert_true("expected first run comparator to be null", run1_summary["comparison_run_id"] is None)

                seed_market_rows(cur, asset_a, ts2, close=136, return_1h=0.061, volume_zscore=3.2, oi_change_1h=0.11, funding_rate=0.0038, liquidation_notional_1h=2400000)
                seed_market_rows(cur, asset_b, ts2, close=69, return_1h=-0.039, volume_zscore=2.6, oi_change_1h=-0.052, funding_rate=-0.0015, liquidation_notional_1h=3200000)
                seed_macro(cur, ts2, dxy_return_1d=-0.0042, us10y_change_1d=-0.07)
                conn.commit()

                run2_enqueue = enqueue_governed(cur, workspace_slug)
                conn.commit()
                run2_id = str(run2_enqueue["job_id"])
                run2 = wait_for_run(cur, run2_id)
                assert_true("expected second run completed", run2["status"] == "completed")

                run2_summary = fetch_drift_summary(cur, run2_id)
                run2_metrics = fetch_drift_metrics(cur, run2_id)
                flagged = [row for row in run2_metrics if row["drift_flag"]]

                assert_true("expected second run comparator", str(run2_summary["comparison_run_id"]) == run1_id)
                assert_true("expected second run drift metrics", len(run2_metrics) > 0)
                assert_true("expected materially changed second run to flag drift", len(flagged) > 0)
                assert_true("expected summary flagged count to match metric rows", int(run2_summary["flagged_metric_count"]) == len(flagged))
                assert_true(
                    "expected at least one flagged family or signal drift row",
                    any(row["metric_type"] in {"family", "signal", "composite", "regime"} for row in flagged),
                )

                print("phase25b smoke ok")
                print(f"workspace_slug={workspace_slug}")
                print(f"run1_id={run1_id}")
                print(f"run2_id={run2_id}")
                print(f"comparison_run_id={run2_summary['comparison_run_id']}")
                print(f"metric_count={len(run2_metrics)}")
                print(f"flagged_metric_count={len(flagged)}")
                print(f"drift_severity={run2_summary['drift_severity']}")
                if flagged:
                    top = flagged[0]
                    print(f"top_flagged={top['metric_type']}:{top['entity_name']}")
                    print(f"top_flagged_delta_pct={top['delta_pct']}")
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
