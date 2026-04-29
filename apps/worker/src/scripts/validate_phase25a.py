from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timezone
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
        (slug, f"Phase 2.5A Smoke {slug}"),
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
          ('DXY', %s, 104.0, 'phase25a_validator', %s, 0),
          ('US10Y', %s, 4.2, 'phase25a_validator', 0, %s)
        """,
        (ts, dxy_return_1d, ts, us10y_change_1d),
    )


def enqueue_governed(cur, workspace_slug: str, watchlist_slug: str = "core") -> dict[str, Any]:
    cur.execute(
        "select * from public.enqueue_governed_recompute(%s, %s, 'manual', 'phase25a-validator', %s::jsonb)",
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


def fetch_signal_attributions(cur, run_id: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        select *
        from public.job_run_attributions
        where run_id = %s::uuid
        order by abs(contribution_value) desc, asset_symbol asc nulls last, signal_name asc
        """,
        (run_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_family_attributions(cur, run_id: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        select *
        from public.job_run_signal_family_attributions
        where run_id = %s::uuid
        order by family_rank asc
        """,
        (run_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def fetch_run_summary(cur, run_id: str) -> dict[str, Any]:
    cur.execute("select * from public.run_attribution_summary where run_id = %s::uuid", (run_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"run attribution summary missing for run {run_id}")
    return dict(row)


def fetch_job_run(cur, run_id: str) -> dict[str, Any]:
    cur.execute(
        """
        select attribution_version,
               attribution_reconciled,
               attribution_total,
               attribution_target_total,
               attribution_reconciliation_delta
        from public.job_runs
        where id = %s::uuid
        """,
        (run_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"job_run not found for run {run_id}")
    return dict(row)


def assert_close(label: str, left: float, right: float, tolerance: float = 1e-6) -> None:
    if abs(left - right) > tolerance:
        raise RuntimeError(f"{label} mismatch: left={left} right={right} tolerance={tolerance}")


def cleanup(cur, workspace_id: str, temp_symbols: list[str]) -> None:
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
    workspace_slug = f"phase25a-{suffix}"
    temp_symbols = [f"P25A{suffix[-4:]}", f"P25B{suffix[-4:]}"]
    worker_env = os.environ.copy()
    worker_env.setdefault("WORKER_ID", f"phase25a-validator-{suffix}")
    worker: subprocess.Popen[str] | None = None
    workspace_id: str | None = None

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                workspace_id, watchlist_id = ensure_workspace(cur, workspace_slug)
                asset_a = ensure_asset(cur, temp_symbols[0], "Phase25A Asset A")
                asset_b = ensure_asset(cur, temp_symbols[1], "Phase25A Asset B")
                attach_watchlist_asset(cur, watchlist_id, asset_a, 1)
                attach_watchlist_asset(cur, watchlist_id, asset_b, 2)

                ts = now
                seed_market_rows(cur, asset_a, ts, close=122, return_1h=0.019, volume_zscore=1.5, oi_change_1h=0.04, funding_rate=0.0015, liquidation_notional_1h=850000)
                seed_market_rows(cur, asset_b, ts, close=78, return_1h=-0.011, volume_zscore=0.9, oi_change_1h=0.015, funding_rate=0.0009, liquidation_notional_1h=1400000)
                seed_macro(cur, ts, dxy_return_1d=0.0021, us10y_change_1d=0.03)
                conn.commit()

                worker = subprocess.Popen(
                    [sys.executable, "-m", "src.main"],
                    cwd=str(WORKER_ROOT),
                    env=worker_env,
                )
                time.sleep(3)

                enqueue_result = enqueue_governed(cur, workspace_slug)
                conn.commit()
                run_id = str(enqueue_result["job_id"])
                run = wait_for_run(cur, run_id)
                if run["status"] != "completed":
                    raise RuntimeError(f"expected completed run, got {run['status']}")

                signal_rows = fetch_signal_attributions(cur, run_id)
                family_rows = fetch_family_attributions(cur, run_id)
                summary = fetch_run_summary(cur, run_id)
                job_run = fetch_job_run(cur, run_id)

                if not signal_rows:
                    raise RuntimeError("no signal attribution rows persisted")
                if not family_rows:
                    raise RuntimeError("no signal-family attribution rows persisted")

                family_totals: dict[str, dict[str, float]] = {}
                for row in signal_rows:
                    bucket = family_totals.setdefault(
                        str(row["signal_family"]),
                        {"score": 0.0, "positive": 0.0, "negative": 0.0, "invalidator": 0.0},
                    )
                    contribution = float(row["contribution_value"])
                    bucket["score"] += contribution
                    if contribution > 0:
                        bucket["positive"] += contribution
                    elif contribution < 0:
                        bucket["negative"] += contribution
                    if row["is_invalidator"]:
                        bucket["invalidator"] += contribution

                for family_row in family_rows:
                    family_name = str(family_row["signal_family"])
                    bucket = family_totals.get(family_name)
                    if bucket is None:
                        raise RuntimeError(f"family row exists without signal rows: {family_name}")
                    assert_close(f"{family_name} family_score", float(family_row["family_score"]), bucket["score"])
                    assert_close(f"{family_name} positive_contribution", float(family_row["positive_contribution"]), bucket["positive"])
                    assert_close(f"{family_name} negative_contribution", float(family_row["negative_contribution"]), bucket["negative"])
                    assert_close(f"{family_name} invalidator_contribution", float(family_row["invalidator_contribution"]), bucket["invalidator"])

                signal_total = sum(float(row["contribution_value"]) for row in signal_rows)
                assert_close("job_runs attribution_total", float(job_run["attribution_total"]), signal_total)
                assert_close("summary attribution_total", float(summary["attribution_total"]), signal_total)

                delta = float(job_run["attribution_reconciliation_delta"])
                if abs(delta) > 1e-6:
                    raise RuntimeError(f"reconciliation delta too large: {delta}")
                if not bool(job_run["attribution_reconciled"]):
                    raise RuntimeError("job_runs attribution_reconciled was false")
                if len(summary["family_attributions"]) != len(family_rows):
                    raise RuntimeError("run_attribution_summary family row count mismatch")
                if len(summary["signal_attributions"]) != len(signal_rows):
                    raise RuntimeError("run_attribution_summary signal row count mismatch")

                print("phase25a smoke ok")
                print(f"workspace_slug={workspace_slug}")
                print(f"run_id={run_id}")
                print(f"signal_attribution_rows={len(signal_rows)}")
                print(f"family_attribution_rows={len(family_rows)}")
                print(f"attribution_total={job_run['attribution_total']}")
                print(f"attribution_target_total={job_run['attribution_target_total']}")
                print(f"attribution_reconciliation_delta={job_run['attribution_reconciliation_delta']}")
                print(f"top_family={family_rows[0]['signal_family'] if family_rows else 'none'}")
                print(f"top_signal={signal_rows[0]['signal_name'] if signal_rows else 'none'}")
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
