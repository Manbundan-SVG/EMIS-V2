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


def ensure_watchlist(cur, workspace_id: str, slug: str, name: str) -> str:
    cur.execute(
        """
        insert into public.watchlists (workspace_id, slug, name)
        values (%s::uuid, %s, %s)
        on conflict (workspace_id, slug) do update
        set name = excluded.name
        returning id
        """,
        (workspace_id, slug, name),
    )
    return str(cur.fetchone()["id"])


def ensure_governance(cur, workspace_id: str, watchlist_id: str) -> None:
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


def fetch_scope(cur, run_id: str) -> dict:
    cur.execute("select * from public.run_scope_inspection where run_id = %s::uuid", (run_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"missing run scope inspection for run {run_id}")
    return dict(row)


def fetch_snapshot(cur, run_id: str) -> dict:
    cur.execute("select * from public.job_run_input_snapshots where job_run_id = %s::uuid", (run_id,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"missing input snapshot for run {run_id}")
    return dict(row)


def fetch_attribution_symbols(cur, run_id: str) -> list[str]:
    cur.execute(
        """
        select distinct asset_symbol
        from public.job_run_attributions
        where run_id = %s::uuid
          and asset_symbol is not null
        order by asset_symbol asc
        """,
        (run_id,),
    )
    return [str(row["asset_symbol"]) for row in cur.fetchall()]


def cleanup(cur, workspace_id: str, temp_symbols: list[str], macro_timestamps: list[datetime]) -> None:
    cur.execute("delete from public.job_run_compute_scopes where workspace_id = %s::uuid", (workspace_id,))
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
    workspace_slug = f"phase26b-{suffix}"
    temp_symbols = [f"P26BA{suffix[-4:]}", f"P26BB{suffix[-4:]}", f"P26BC{suffix[-4:]}"]
    worker_env = os.environ.copy()
    worker_env["WORKER_ID"] = f"phase26b-validator-{suffix}"
    worker = None
    workspace_id = None
    base_ts = now + timedelta(days=366)
    macro_timestamps = [base_ts + timedelta(minutes=offset) for offset in (0, 5, 10)]

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                workspace_id, core_watchlist_id = ensure_workspace(cur, workspace_slug)
                alt_watchlist_id = ensure_watchlist(cur, workspace_id, "alt", "Alt")
                ensure_governance(cur, workspace_id, alt_watchlist_id)

                asset_a = ensure_asset(cur, temp_symbols[0], "Phase26B Asset A")
                asset_b = ensure_asset(cur, temp_symbols[1], "Phase26B Asset B")
                asset_c = ensure_asset(cur, temp_symbols[2], "Phase26B Asset C")

                attach_watchlist_asset(cur, core_watchlist_id, asset_a, 1)
                attach_watchlist_asset(cur, alt_watchlist_id, asset_a, 1)
                attach_watchlist_asset(cur, alt_watchlist_id, asset_b, 2)

                seed_market_rows(cur, asset_a, macro_timestamps[0], close=101, return_1h=0.012, volume_zscore=1.0, oi_change_1h=0.018, funding_rate=0.0010, liquidation_notional_1h=750000)
                seed_market_rows(cur, asset_b, macro_timestamps[0], close=88, return_1h=-0.009, volume_zscore=0.7, oi_change_1h=0.012, funding_rate=0.0008, liquidation_notional_1h=680000)
                seed_market_rows(cur, asset_c, macro_timestamps[0], close=55, return_1h=0.095, volume_zscore=2.8, oi_change_1h=0.160, funding_rate=0.0055, liquidation_notional_1h=4200000)
                seed_macro(cur, macro_timestamps[0], dxy_return_1d=0.0018, us10y_change_1d=0.025)
                conn.commit()

                worker = subprocess.Popen([sys.executable, "-m", "src.main"], cwd=str(WORKER_ROOT), env=worker_env)
                time.sleep(3)

                core_run_1 = enqueue_governed(cur, workspace_slug, "core")
                alt_run_1 = enqueue_governed(cur, workspace_slug, "alt")
                conn.commit()

                if wait_for_run(cur, core_run_1)["status"] != "completed":
                    raise RuntimeError("core run 1 did not complete")
                if wait_for_run(cur, alt_run_1)["status"] != "completed":
                    raise RuntimeError("alt run 1 did not complete")

                core_scope_1 = fetch_scope(cur, core_run_1)
                alt_scope_1 = fetch_scope(cur, alt_run_1)
                core_snapshot_1 = fetch_snapshot(cur, core_run_1)
                alt_snapshot_1 = fetch_snapshot(cur, alt_run_1)

                if list(core_scope_1["primary_assets"] or []) != [temp_symbols[0]]:
                    raise RuntimeError(f"unexpected core primary assets: {core_scope_1['primary_assets']}")
                if list(alt_scope_1["primary_assets"] or []) != [temp_symbols[0], temp_symbols[1]]:
                    raise RuntimeError(f"unexpected alt primary assets: {alt_scope_1['primary_assets']}")
                if temp_symbols[2] in list(core_scope_1["asset_universe"] or []):
                    raise RuntimeError("unrelated asset leaked into core asset universe")
                if temp_symbols[2] in list(alt_scope_1["asset_universe"] or []):
                    raise RuntimeError("unrelated asset leaked into alt asset universe")
                if int(core_snapshot_1["asset_count"]) != 1:
                    raise RuntimeError("core snapshot asset_count should be 1")
                if int(alt_snapshot_1["asset_count"]) != 2:
                    raise RuntimeError("alt snapshot asset_count should be 2")
                if core_scope_1["scope_hash"] == alt_scope_1["scope_hash"]:
                    raise RuntimeError("core and alt watchlists should not share the same scope hash")
                if fetch_attribution_symbols(cur, core_run_1) != [temp_symbols[0]]:
                    raise RuntimeError("core attribution symbols were not watchlist-scoped")
                if fetch_attribution_symbols(cur, alt_run_1) != [temp_symbols[0], temp_symbols[1]]:
                    raise RuntimeError("alt attribution symbols were not watchlist-scoped")

                seed_market_rows(cur, asset_a, macro_timestamps[1], close=102, return_1h=0.010, volume_zscore=1.1, oi_change_1h=0.016, funding_rate=0.0011, liquidation_notional_1h=790000)
                seed_market_rows(cur, asset_b, macro_timestamps[1], close=87, return_1h=-0.008, volume_zscore=0.8, oi_change_1h=0.010, funding_rate=0.0009, liquidation_notional_1h=710000)
                seed_market_rows(cur, asset_c, macro_timestamps[1], close=41, return_1h=-0.120, volume_zscore=3.4, oi_change_1h=-0.240, funding_rate=0.0062, liquidation_notional_1h=5100000)
                seed_macro(cur, macro_timestamps[1], dxy_return_1d=0.0020, us10y_change_1d=0.028)
                conn.commit()

                core_run_2 = enqueue_governed(cur, workspace_slug, "core")
                conn.commit()
                if wait_for_run(cur, core_run_2)["status"] != "completed":
                    raise RuntimeError("core run 2 did not complete")

                core_scope_2 = fetch_scope(cur, core_run_2)
                if core_scope_2["scope_hash"] != core_scope_1["scope_hash"]:
                    raise RuntimeError("scope hash was not deterministic for repeated identical watchlist runs")

                attach_watchlist_asset(cur, core_watchlist_id, asset_b, 2)
                seed_market_rows(cur, asset_a, macro_timestamps[2], close=103, return_1h=0.006, volume_zscore=0.9, oi_change_1h=0.014, funding_rate=0.0010, liquidation_notional_1h=720000)
                seed_market_rows(cur, asset_b, macro_timestamps[2], close=86, return_1h=-0.014, volume_zscore=0.95, oi_change_1h=0.020, funding_rate=0.0013, liquidation_notional_1h=880000)
                seed_market_rows(cur, asset_c, macro_timestamps[2], close=32, return_1h=0.180, volume_zscore=4.2, oi_change_1h=0.320, funding_rate=0.0075, liquidation_notional_1h=6300000)
                seed_macro(cur, macro_timestamps[2], dxy_return_1d=0.0019, us10y_change_1d=0.026)
                conn.commit()

                replay_run = enqueue_replay(cur, core_run_1)
                conn.commit()

                if wait_for_run(cur, replay_run)["status"] != "completed":
                    raise RuntimeError("replay run did not complete")

                replay_scope = fetch_scope(cur, replay_run)
                replay_snapshot = fetch_snapshot(cur, replay_run)

                if replay_scope["scope_hash"] != core_scope_1["scope_hash"]:
                    raise RuntimeError("replay run did not reuse the original source scope hash")
                if list(replay_scope["asset_universe"] or []) != [temp_symbols[0]]:
                    raise RuntimeError(f"replay run asset universe drifted: {replay_scope['asset_universe']}")
                if int(replay_snapshot["asset_count"]) != 1:
                    raise RuntimeError("replay snapshot asset_count should preserve the original scope")
                if fetch_attribution_symbols(cur, replay_run) != [temp_symbols[0]]:
                    raise RuntimeError("replay attribution symbols should match the original scoped universe")

                cur.execute("select * from public.job_run_replay_deltas where replay_run_id = %s::uuid", (replay_run,))
                replay_delta = cur.fetchone()
                if not replay_delta:
                    raise RuntimeError("replay delta row missing for phase26b replay")
                replay_delta = dict(replay_delta)
                if float(replay_delta["input_match_score"]) < 0.999:
                    raise RuntimeError("replay input match score unexpectedly degraded under preserved source scope")

                fresh_core_run = enqueue_governed(cur, workspace_slug, "core")
                conn.commit()
                if wait_for_run(cur, fresh_core_run)["status"] != "completed":
                    raise RuntimeError("fresh core run after watchlist membership change did not complete")

                fresh_scope = fetch_scope(cur, fresh_core_run)
                if fresh_scope["scope_hash"] == core_scope_1["scope_hash"]:
                    raise RuntimeError("fresh core run after watchlist membership change should have a new scope hash")
                if list(fresh_scope["asset_universe"] or []) != [temp_symbols[0], temp_symbols[1]]:
                    raise RuntimeError(f"fresh core run did not include updated watchlist scope: {fresh_scope['asset_universe']}")
                if temp_symbols[2] in list(fresh_scope["asset_universe"] or []):
                    raise RuntimeError("unrelated global asset leaked into fresh core scope")

                print("phase26b smoke ok")
                print(f"workspace_slug={workspace_slug}")
                print(f"core_scope_hash={core_scope_1['scope_hash']}")
                print(f"alt_scope_hash={alt_scope_1['scope_hash']}")
                print(f"deterministic_scope_hash={core_scope_2['scope_hash']}")
                print(f"replay_scope_hash={replay_scope['scope_hash']}")
                print(f"fresh_core_scope_hash={fresh_scope['scope_hash']}")
                print(f"replay_input_match_score={replay_delta['input_match_score']}")
                print(f"core_asset_universe={core_scope_1['asset_universe']}")
                print(f"alt_asset_universe={alt_scope_1['asset_universe']}")
                print(f"fresh_core_asset_universe={fresh_scope['asset_universe']}")
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
