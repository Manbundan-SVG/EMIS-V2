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
from src.db.repositories import get_active_regime_threshold_row, get_governance_alert_rules
from src.ingestion.currency_api_client import CurrencyApiClient
from src.ingestion.fred_client import FredClient
from src.scripts.validate_phase25a import attach_watchlist_asset, ensure_asset, ensure_workspace, seed_macro, seed_market_rows
from src.scripts.validate_phase25c import wait_for_run
from src.services.macro_sync_service import sync_macro_market_data
from src.services.regime_threshold_service import RegimeThresholdService


WORKER_ROOT = Path(__file__).resolve().parents[2]


def enqueue_governed(cur, workspace_slug: str) -> str:
    cur.execute(
        "select * from public.enqueue_governed_recompute(%s, %s, 'manual', 'phase27b-validator', %s::jsonb)",
        (workspace_slug, "core", "{}"),
    )
    row = cur.fetchone()
    if not row or not row["job_id"]:
        raise RuntimeError("enqueue_governed_recompute returned no job_id")
    return str(row["job_id"])


def cleanup(cur, workspace_id: str, temp_symbols: list[str], macro_timestamps: list[datetime]) -> None:
    cur.execute("delete from public.governance_threshold_applications where workspace_id = %s::uuid", (workspace_id,))
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
    cur.execute("delete from public.market_data_sync_runs where workspace_id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.workspaces where id = %s::uuid", (workspace_id,))
    cur.execute("delete from public.market_bars where asset_id in (select id from public.assets where symbol = any(%s))", (temp_symbols,))
    cur.execute("delete from public.market_open_interest where asset_id in (select id from public.assets where symbol = any(%s))", (temp_symbols,))
    cur.execute("delete from public.market_funding where asset_id in (select id from public.assets where symbol = any(%s))", (temp_symbols,))
    cur.execute("delete from public.market_liquidations where asset_id in (select id from public.assets where symbol = any(%s))", (temp_symbols,))
    cur.execute("delete from public.macro_series_points where series_code in ('DXY', 'US10Y') and ts = any(%s)", (macro_timestamps,))
    cur.execute("delete from public.assets where symbol = any(%s)", (temp_symbols,))


def main() -> None:
    settings = get_settings()
    threshold_service = RegimeThresholdService()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    suffix = now.strftime("%Y%m%d%H%M%S")
    workspace_slug = f"phase27b-{suffix}"
    temp_symbols = [f"P27B{suffix[-4:]}", f"P27C{suffix[-4:]}"]
    worker_env = os.environ.copy()
    worker_env["WORKER_ID"] = f"phase27b-validator-{suffix}"
    worker = None
    workspace_id = None
    base_ts = now + timedelta(days=400)
    macro_timestamps = [base_ts + timedelta(minutes=5 * idx) for idx in range(3)]

    fred_client = FredClient(
        api_key=settings.fred_api_key,
        base_url=settings.fred_base_url,
        timeout_seconds=settings.market_sync_timeout_seconds,
        dxy_series_code=settings.fred_dxy_series_code,
        us10y_series_code=settings.fred_us10y_series_code,
    )
    currency_api_client = CurrencyApiClient(
        api_key=settings.currency_api_key,
        base_url=settings.currency_api_base_url,
        timeout_seconds=settings.market_sync_timeout_seconds,
    )

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            try:
                workspace_id, watchlist_id = ensure_workspace(cur, workspace_slug)
                asset_a = ensure_asset(cur, temp_symbols[0], "Phase27B Asset A")
                asset_b = ensure_asset(cur, temp_symbols[1], "Phase27B Asset B")
                attach_watchlist_asset(cur, watchlist_id, asset_a, 1)
                attach_watchlist_asset(cur, watchlist_id, asset_b, 2)
                conn.commit()

                macro_result = sync_macro_market_data(
                    conn,
                    workspace_slug=workspace_slug,
                    fred_client=fred_client,
                    currency_api_client=currency_api_client,
                )
                conn.commit()
                if macro_result.provider_mode != "fred_api":
                    raise RuntimeError(f"expected fred_api macro activation, got {macro_result.provider_mode}")
                if macro_result.macro_point_count < 2:
                    raise RuntimeError("expected at least two macro points from macro sync")

                macro_row = get_active_regime_threshold_row(conn, workspace_id, "macro_dominant")
                trend_row = get_active_regime_threshold_row(conn, workspace_id, "trend_persistence")
                if not macro_row or not trend_row:
                    raise RuntimeError("expected both macro_dominant and trend_persistence active threshold rows")

                macro_selection = threshold_service.select_thresholds("macro_dominant", macro_row)
                trend_selection = threshold_service.select_thresholds("trend_persistence", trend_row)
                if macro_selection.override_id is None:
                    raise RuntimeError("expected macro_dominant regime override to be selected")
                if trend_selection.override_id is None:
                    raise RuntimeError("expected trend_persistence regime override to be selected")
                if macro_selection.thresholds["family_instability_ceiling"] <= trend_selection.thresholds["family_instability_ceiling"]:
                    raise RuntimeError("macro_dominant should allow more family instability than trend_persistence")

                rules = get_governance_alert_rules(conn, workspace_id)
                tuned_rules = threshold_service.apply_thresholds_to_rules(rules, macro_selection)
                regime_rule = next((row for row in tuned_rules if row["event_type"] == "regime_instability_spike"), None)
                if not regime_rule:
                    raise RuntimeError("expected tuned governance rules to include regime_instability_spike")
                if float(regime_rule["threshold_numeric"]) != macro_selection.thresholds["regime_instability_ceiling"]:
                    raise RuntimeError("regime_instability_spike threshold did not match macro selection")

                worker = subprocess.Popen([sys.executable, "-m", "src.main"], cwd=str(WORKER_ROOT), env=worker_env)
                time.sleep(3)

                config = (121.5, 74.1, 0.048, -0.029, 2.5, 1.7, 0.108, -0.041, 0.0031, 0.0024, 3200000, 2800000, 0.0061, 0.097)
                ts = macro_timestamps[-1]
                seed_market_rows(cur, asset_a, ts, close=config[0], return_1h=config[2], volume_zscore=config[4], oi_change_1h=config[6], funding_rate=config[8], liquidation_notional_1h=config[10])
                seed_market_rows(cur, asset_b, ts, close=config[1], return_1h=config[3], volume_zscore=config[5], oi_change_1h=config[7], funding_rate=config[9], liquidation_notional_1h=config[11])
                seed_macro(cur, ts, dxy_return_1d=config[12], us10y_change_1d=config[13])
                conn.commit()

                run_id = enqueue_governed(cur, workspace_slug)
                conn.commit()
                run = wait_for_run(cur, run_id)
                if run["status"] != "completed":
                    raise RuntimeError(f"expected completed run, got {run['status']}")

                cur.execute(
                    """
                    select *
                    from public.governance_threshold_application_summary
                    where workspace_id = %s::uuid
                      and run_id = %s::uuid
                    order by created_at asc
                    """,
                    (workspace_id, run_id),
                )
                application_rows = [dict(row) for row in cur.fetchall()]
                if not application_rows:
                    raise RuntimeError("expected threshold application rows for the completed run")
                applied = application_rows[0]
                if not applied.get("profile_name"):
                    raise RuntimeError("expected threshold application to record profile name")

                cur.execute(
                    """
                    select *
                    from public.macro_sync_health
                    where workspace_id = %s::uuid
                      and provider_mode = 'fred_api'
                    """,
                    (workspace_id,),
                )
                health_row = cur.fetchone()
                if not health_row:
                    raise RuntimeError("expected macro_sync_health row for fred_api")

                print("phase27b smoke ok")
                print(f"workspace_slug={workspace_slug}")
                print(f"macro_provider_mode={macro_result.provider_mode}")
                print(f"macro_point_count={macro_result.macro_point_count}")
                print(f"macro_family_ceiling={macro_selection.thresholds['family_instability_ceiling']}")
                print(f"trend_replay_floor={trend_selection.thresholds['replay_consistency_floor']}")
                print(f"threshold_application_count={len(application_rows)}")
                print(f"application_regime={applied['regime']}")
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
