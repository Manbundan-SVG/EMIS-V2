from __future__ import annotations
import json
import os
import socket
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Sequence
from psycopg.types.json import Jsonb


def _json_compatible(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))

@dataclass(frozen=True)
class MarketState:
    asset_id: str
    symbol: str
    bar_close: float
    bar_return_1h: float
    volume_zscore: float
    oi_change_1h: float
    funding_rate: float
    liquidation_notional_1h: float
    macro_dxy_return_1d: float
    macro_us10y_change_1d: float
    as_of_ts: datetime
    bar_ts: datetime
    oi_ts: datetime | None
    funding_ts: datetime | None
    liquidation_ts: datetime | None
    macro_dxy_ts: datetime | None
    macro_us10y_ts: datetime | None
    has_open_interest: bool
    has_funding: bool
    has_liquidations: bool
    has_macro_dxy: bool
    has_macro_us10y: bool


@dataclass(frozen=True)
class SyncableAsset:
    asset_id: str
    symbol: str
    asset_class: str


def load_watchlist_asset_symbols(conn, watchlist_id: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select a.symbol
            from public.watchlist_assets wa
            join public.assets a
              on a.id = wa.asset_id
            where wa.watchlist_id = %s::uuid
            order by wa.sort_order asc, a.symbol asc
            """,
            (watchlist_id,),
        )
        return [str(row["symbol"]) for row in cur.fetchall()]


def resolve_workspace_watchlist_scope(
    conn,
    workspace_slug: str,
    watchlist_slug: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select id, slug, name
            from public.workspaces
            where slug = %s
            """,
            (workspace_slug,),
        )
        workspace = cur.fetchone()
        if not workspace:
            raise RuntimeError(f"workspace not found for slug={workspace_slug!r}")

        watchlist = None
        if watchlist_slug is not None:
            cur.execute(
                """
                select id, slug, name
                from public.watchlists
                where workspace_id = %s::uuid
                  and slug = %s
                """,
                (workspace["id"], watchlist_slug),
            )
            watchlist = cur.fetchone()
            if not watchlist:
                raise RuntimeError(
                    f"watchlist not found for workspace={workspace_slug!r} slug={watchlist_slug!r}"
                )

        return dict(workspace), dict(watchlist) if watchlist else None


def get_syncable_crypto_assets(
    conn,
    workspace_id: str,
    watchlist_id: str | None = None,
) -> list[SyncableAsset]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct
              a.id::text as asset_id,
              a.symbol,
              a.asset_class
            from public.watchlists w
            join public.watchlist_assets wa
              on wa.watchlist_id = w.id
            join public.assets a
              on a.id = wa.asset_id
            where w.workspace_id = %s::uuid
              and a.asset_class = 'crypto'
              and a.is_active = true
              and (%s::uuid is null or w.id = %s::uuid)
            order by a.symbol asc
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [SyncableAsset(**dict(row)) for row in cur.fetchall()]


def create_market_data_sync_run(
    conn,
    *,
    source: str,
    workspace_id: str,
    watchlist_id: str | None,
    requested_symbols: Sequence[str],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.market_data_sync_runs (
              source,
              workspace_id,
              watchlist_id,
              status,
              requested_symbols,
              asset_count,
              metadata
            ) values (
              %s,
              %s::uuid,
              %s::uuid,
              'running',
              %s::jsonb,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                source,
                workspace_id,
                watchlist_id,
                json.dumps(list(requested_symbols)),
                len(requested_symbols),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("market_data_sync_runs insert returned no row")
        return dict(row)


def complete_market_data_sync_run(
    conn,
    sync_run_id: str,
    *,
    status: str,
    synced_symbols: Sequence[str],
    bar_count: int = 0,
    open_interest_count: int = 0,
    funding_count: int = 0,
    liquidation_count: int = 0,
    macro_point_count: int = 0,
    metadata: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.market_data_sync_runs
            set status = %s,
                synced_symbols = %s::jsonb,
                bar_count = %s,
                open_interest_count = %s,
                funding_count = %s,
                liquidation_count = %s,
                macro_point_count = %s,
                metadata = %s::jsonb,
                error = %s,
                completed_at = now()
            where id = %s::uuid
            """,
            (
                status,
                json.dumps(list(synced_symbols)),
                bar_count,
                open_interest_count,
                funding_count,
                liquidation_count,
                macro_point_count,
                json.dumps(_json_compatible(metadata or {})),
                error[:1000] if error else None,
                sync_run_id,
            ),
        )


def upsert_market_bars_rows(conn, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.market_bars (
              asset_id,
              timeframe,
              ts,
              open,
              high,
              low,
              close,
              volume,
              source,
              return_1h,
              volume_zscore
            ) values (
              %s::uuid,
              %s,
              %s::timestamptz,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            on conflict (asset_id, timeframe, ts, source)
            do update set open = excluded.open,
                          high = excluded.high,
                          low = excluded.low,
                          close = excluded.close,
                          volume = excluded.volume,
                          return_1h = excluded.return_1h,
                          volume_zscore = excluded.volume_zscore
            """,
            [
                (
                    row["asset_id"],
                    row["timeframe"],
                    row["ts"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    row["source"],
                    row["return_1h"],
                    row["volume_zscore"],
                )
                for row in rows
            ],
        )


def upsert_market_open_interest_rows(conn, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.market_open_interest (
              asset_id,
              ts,
              open_interest,
              source,
              oi_change_1h
            ) values (
              %s::uuid,
              %s::timestamptz,
              %s,
              %s,
              %s
            )
            on conflict (asset_id, ts, source)
            do update set open_interest = excluded.open_interest,
                          oi_change_1h = excluded.oi_change_1h
            """,
            [
                (
                    row["asset_id"],
                    row["ts"],
                    row["open_interest"],
                    row["source"],
                    row["oi_change_1h"],
                )
                for row in rows
            ],
        )


def get_latest_open_interest_by_asset_ids(conn, asset_ids: Sequence[str]) -> dict[str, float]:
    if not asset_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct on (asset_id)
              asset_id::text as asset_id,
              open_interest::float8 as open_interest
            from public.market_open_interest
            where asset_id = any(%s::uuid[])
            order by asset_id, ts desc
            """,
            (list(asset_ids),),
        )
        return {str(row["asset_id"]): float(row["open_interest"]) for row in cur.fetchall()}


def upsert_market_funding_rows(conn, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.market_funding (
              asset_id,
              ts,
              funding_rate,
              source
            ) values (
              %s::uuid,
              %s::timestamptz,
              %s,
              %s
            )
            on conflict (asset_id, ts, source)
            do update set funding_rate = excluded.funding_rate
            """,
            [
                (
                    row["asset_id"],
                    row["ts"],
                    row["funding_rate"],
                    row["source"],
                )
                for row in rows
            ],
        )


def upsert_market_liquidation_rows(conn, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.market_liquidations (
              asset_id,
              ts,
              side,
              notional_usd,
              reference_price,
              source,
              liquidation_notional_1h
            ) values (
              %s::uuid,
              %s::timestamptz,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            on conflict (asset_id, ts, side, source) where liquidation_notional_1h is not null
            do update set notional_usd = excluded.notional_usd,
                          reference_price = excluded.reference_price,
                          liquidation_notional_1h = excluded.liquidation_notional_1h
            """,
            [
                (
                    row["asset_id"],
                    row["ts"],
                    row["side"],
                    row.get("notional_usd"),
                    row.get("reference_price"),
                    row["source"],
                    row["liquidation_notional_1h"],
                )
                for row in rows
            ],
        )


def upsert_macro_series_points_rows(conn, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.macro_series_points (
              series_code,
              ts,
              value,
              source,
              return_1d,
              change_1d
            ) values (
              %s,
              %s::timestamptz,
              %s,
              %s,
              %s,
              %s
            )
            on conflict (series_code, ts, source)
            do update set value = excluded.value,
                          return_1d = excluded.return_1d,
                          change_1d = excluded.change_1d
            """,
            [
                (
                    row["series_code"],
                    row["ts"],
                    row["value"],
                    row["source"],
                    row.get("return_1d"),
                    row.get("change_1d"),
                )
                for row in rows
            ],
        )


def persist_compute_scope(
    conn,
    *,
    run_id: str,
    workspace_id: str,
    watchlist_id: str | None,
    queue_name: str,
    scope_version: str,
    primary_assets: Sequence[str],
    dependency_assets: Sequence[str],
    asset_universe: Sequence[str],
    dependency_policy: dict[str, Any],
    scope_hash: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.job_run_compute_scopes (
              run_id,
              workspace_id,
              watchlist_id,
              queue_name,
              scope_version,
              primary_assets,
              dependency_assets,
              asset_universe,
              primary_asset_count,
              dependency_asset_count,
              asset_universe_count,
              dependency_policy,
              scope_hash,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb,
              %s::jsonb,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s,
              %s::jsonb
            )
            on conflict (run_id) do update
            set watchlist_id = excluded.watchlist_id,
                queue_name = excluded.queue_name,
                scope_version = excluded.scope_version,
                primary_assets = excluded.primary_assets,
                dependency_assets = excluded.dependency_assets,
                asset_universe = excluded.asset_universe,
                primary_asset_count = excluded.primary_asset_count,
                dependency_asset_count = excluded.dependency_asset_count,
                asset_universe_count = excluded.asset_universe_count,
                dependency_policy = excluded.dependency_policy,
                scope_hash = excluded.scope_hash,
                metadata = excluded.metadata
            returning *
            """,
            (
                run_id,
                workspace_id,
                watchlist_id,
                queue_name,
                scope_version,
                json.dumps(list(primary_assets)),
                json.dumps(list(dependency_assets)),
                json.dumps(list(asset_universe)),
                len(primary_assets),
                len(dependency_assets),
                len(asset_universe),
                json.dumps(_json_compatible(dependency_policy)),
                scope_hash,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("job_run_compute_scopes upsert returned no row")
        return dict(row)


def get_run_compute_scope(conn, run_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.job_run_compute_scopes
            where run_id = %s::uuid
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def load_latest_market_state(
    conn,
    workspace_id: str,
    as_of_ts: str | None = None,
    *,
    asset_symbols: Sequence[str] | None = None,
) -> list[MarketState]:
    query = '''
    with scoped_assets as (
      select id, symbol
      from public.assets
      where (%s::text[] is null or symbol = any(%s::text[]))
    ), latest_bars as (
      select distinct on (asset_id) asset_id, ts, close, return_1h, volume_zscore
      from public.market_bars
      where asset_id in (select id from scoped_assets)
        and (%s::timestamptz is null or ts <= %s::timestamptz)
      order by asset_id, ts desc
    ), effective_as_of as (
      select coalesce(%s::timestamptz, (select max(ts) from latest_bars)) as ts
    ), latest_oi as (
      select distinct on (asset_id) asset_id, ts, oi_change_1h
      from public.market_open_interest
      where asset_id in (select id from scoped_assets)
        and ts <= (select ts from effective_as_of)
      order by asset_id, ts desc
    ), latest_funding as (
      select distinct on (asset_id) asset_id, ts, funding_rate
      from public.market_funding
      where asset_id in (select id from scoped_assets)
        and ts <= (select ts from effective_as_of)
      order by asset_id, ts desc
    ), latest_liqs as (
      select distinct on (asset_id) asset_id, ts, liquidation_notional_1h
      from public.market_liquidations
      where asset_id in (select id from scoped_assets)
        and ts <= (select ts from effective_as_of)
      order by asset_id, ts desc
    ), latest_dxy as (
      select ts, return_1d
      from public.macro_series_points
      where series_code = 'DXY'
        and ts <= (select ts from effective_as_of)
      order by ts desc
      limit 1
    ), latest_us10y as (
      select ts, change_1d
      from public.macro_series_points
      where series_code = 'US10Y'
        and ts <= (select ts from effective_as_of)
      order by ts desc
      limit 1
    )
    select a.id::text as asset_id, a.symbol,
           b.close::float8              as bar_close,
           b.return_1h::float8          as bar_return_1h,
           b.volume_zscore::float8      as volume_zscore,
           coalesce(oi.oi_change_1h, 0)::float8             as oi_change_1h,
           coalesce(f.funding_rate, 0)::float8              as funding_rate,
           coalesce(l.liquidation_notional_1h, 0)::float8   as liquidation_notional_1h,
           coalesce((select return_1d from latest_dxy), 0)::float8   as macro_dxy_return_1d,
           coalesce((select change_1d from latest_us10y), 0)::float8 as macro_us10y_change_1d,
           b.ts as as_of_ts,
           b.ts as bar_ts,
           oi.ts as oi_ts,
           f.ts as funding_ts,
           l.ts as liquidation_ts,
           (select ts from latest_dxy) as macro_dxy_ts,
           (select ts from latest_us10y) as macro_us10y_ts,
           (oi.asset_id is not null) as has_open_interest,
           (f.asset_id is not null) as has_funding,
           (l.asset_id is not null) as has_liquidations,
           ((select ts from latest_dxy) is not null) as has_macro_dxy,
           ((select ts from latest_us10y) is not null) as has_macro_us10y
    from scoped_assets a
    join latest_bars b on b.asset_id = a.id
    left join latest_oi oi on oi.asset_id = a.id
    left join latest_funding f on f.asset_id = a.id
    left join latest_liqs l on l.asset_id = a.id
    order by a.symbol asc
    '''
    with conn.cursor() as cur:
        scoped_symbols = list(asset_symbols) if asset_symbols is not None else None
        params = (
            scoped_symbols, scoped_symbols,
            as_of_ts, as_of_ts,
            as_of_ts,
        )
        cur.execute(query, params)
        rows = cur.fetchall()
    return [MarketState(**row) for row in rows]

def claim_next_job(conn, worker_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute("select * from public.claim_recompute_job(%s)", (worker_id,))
        rows = cur.fetchall()
        return rows[0] if rows else None

def mark_job_running(conn, job_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.job_runs
            set status = 'running',
                started_at = now(),
                failure_stage = null,
                failure_code = null,
                updated_at = now()
            where id = %s
            """,
            (job_id,),
        )

def complete_job(conn, job_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute("select public.complete_recompute_job(%s)", (job_id,))

def fail_job(conn, job_id: str, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute("select public.fail_recompute_job(%s, %s)", (job_id, error_message[:500]))

def upsert_feature_values(conn, workspace_id: str, rows: list[dict[str, Any]]) -> None:
    payload = [(r["workspace_id"], r["asset_id"], r["feature_name"], r["timestamp"], r["value"], Jsonb(r.get("meta", {}))) for r in rows]
    with conn.cursor() as cur:
        cur.executemany('''
            insert into public.feature_values (workspace_id, asset_id, feature_name, ts, value, meta)
            values (%s, %s, %s, %s, %s, %s)
            on conflict (workspace_id, asset_id, feature_name, ts) where workspace_id is not null
            do update set value = excluded.value, meta = excluded.meta, updated_at = now()
        ''', payload)

def upsert_signal_values(conn, workspace_id: str, rows: list[dict[str, Any]]) -> None:
    payload = [(r["workspace_id"], r["asset_id"], r["signal_name"], r["timestamp"], r["score"], Jsonb(r.get("explanation", {}))) for r in rows]
    with conn.cursor() as cur:
        cur.executemany('''
            insert into public.signal_values (workspace_id, asset_id, signal_name, ts, score, explanation)
            values (%s, %s, %s, %s, %s, %s)
            on conflict (workspace_id, asset_id, signal_name, ts) where workspace_id is not null
            do update set score = excluded.score, explanation = excluded.explanation, updated_at = now()
        ''', payload)

def upsert_composite_scores(conn, workspace_id: str, rows: list[dict[str, Any]]) -> None:
    payload = [(r["workspace_id"], r["asset_id"], r["timestamp"], r["regime"], r["long_score"], r["short_score"], r["confidence"], Jsonb(r.get("invalidators", {}))) for r in rows]
    with conn.cursor() as cur:
        cur.executemany('''
            insert into public.composite_scores (workspace_id, asset_id, timestamp, regime, long_score, short_score, confidence, invalidators)
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (workspace_id, asset_id, timestamp)
            do update set regime = excluded.regime, long_score = excluded.long_score, short_score = excluded.short_score,
                          confidence = excluded.confidence, invalidators = excluded.invalidators, created_at = now()
        ''', payload)

# ── Phase 2.1 additions ────────────────────────────────────────────────────────

def reap_stale_jobs(conn, stale_minutes: int = 10, requeue_delay_seconds: int = 15) -> int:
    with conn.cursor() as cur:
        cur.execute("select public.reap_stale_jobs(%s, %s)", (stale_minutes, requeue_delay_seconds))
        row = cur.fetchone()
        return int(next(iter(row.values()))) if row else 0

def create_alert_event(conn, workspace_id: str, title: str, message: str,
                       severity: str = "info", payload: dict[str, Any] | None = None,
                       job_id: str | None = None, alert_type: str = "worker_notice",
                       metadata: dict[str, Any] | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            '''
            insert into public.alert_events (workspace_id, job_id, alert_type, severity, title, message, payload, metadata)
            values (%s, %s::uuid, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
            ''',
            (
                workspace_id,
                job_id,
                alert_type,
                severity,
                title,
                message,
                json.dumps(payload or {}),
                json.dumps(metadata or {}),
            ),
        )

def enqueue_recompute_by_slug(conn, workspace_slug: str, requested_by: str = "scheduler",
                               reason: str = "scheduled") -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.enqueue_recompute_job(%s, %s, %s, %s::jsonb)",
            (workspace_slug, "cron", requested_by, json.dumps({"reason": reason})),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"enqueue_recompute_job returned no rows for slug={workspace_slug!r}")
        return dict(row)

def enqueue_scheduled(conn, workspace_slug: str, watchlist_slug: str | None = None) -> str:
    """Idempotent scheduled enqueue — skips if a queued/claimed job already exists."""
    with conn.cursor() as cur:
        cur.execute(
            "select public.enqueue_scheduled_recompute(%s, %s)",
            (workspace_slug, watchlist_slug),
        )
        row = cur.fetchone()
        return str(next(iter(row.values()))) if row else ""

# ── Phase 2.2 additions ────────────────────────────────────────────────────────

def heartbeat_worker(conn, worker_id: str, workspace_id: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "select public.heartbeat_worker(%s, %s::uuid, %s, %s)",
            (worker_id, workspace_id, socket.gethostname(), os.getpid()),
        )

def schedule_job_retry_db(conn, queue_id: int, error_message: str,
                          failure_stage: str = "worker") -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.schedule_job_retry(%s, %s, %s)",
            (queue_id, error_message[:500], failure_stage),
        )
        row = cur.fetchone()
        return dict(row) if row else {"action": "unknown"}


def enqueue_governed_recompute(
    conn,
    workspace_slug: str,
    watchlist_slug: str | None = None,
    trigger_type: str = "api",
    requested_by: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.enqueue_governed_recompute(%s, %s, %s, %s, %s::jsonb)",
            (
                workspace_slug,
                watchlist_slug,
                trigger_type,
                requested_by,
                json.dumps(payload or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("enqueue_governed_recompute returned no rows")
        return dict(row)


def should_enqueue_recompute_db(
    conn,
    workspace_id: str,
    watchlist_id: str | None,
    job_type: str = "recompute",
    requested_by: str = "manual",
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.should_enqueue_recompute(%s::uuid, %s::uuid, %s, %s)",
            (workspace_id, watchlist_id, job_type, requested_by),
        )
        row = cur.fetchone()
        return dict(row) if row else {"allowed": False, "reason": "missing_decision"}


def evaluate_alert_policies_db(
    conn,
    workspace_id: str,
    watchlist_id: str | None,
    event_type: str,
    severity: str,
    job_run_id: str,
    payload: dict[str, Any] | None = None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "select public.evaluate_alert_policies(%s::uuid, %s::uuid, %s, %s, %s::uuid, %s::jsonb)",
            (workspace_id, watchlist_id, event_type, severity, job_run_id, json.dumps(_json_compatible(payload or {}))),
        )
        row = cur.fetchone()
        return int(next(iter(row.values()))) if row else 0


def has_matching_alert_policy_db(
    conn,
    workspace_id: str,
    watchlist_id: str | None,
    event_type: str,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select exists(
              select 1
              from public.alert_policy_rules
              where workspace_id = %s::uuid
                and enabled = true
                and event_type = %s
                and (watchlist_id = %s::uuid or watchlist_id is null)
            )
            """,
            (workspace_id, event_type, watchlist_id),
        )
        row = cur.fetchone()
        return bool(next(iter(row.values()))) if row else False


def update_run_lineage(
    conn,
    run_id: str,
    queue_id: int | None,
    lineage: dict[str, Any],
    *,
    status: str | None = None,
    runtime_ms: int | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.job_runs
            set queue_id = coalesce(%s, queue_id),
                lineage = %s::jsonb,
                compute_version = coalesce(%s, compute_version),
                signal_registry_version = coalesce(%s, signal_registry_version),
                model_version = coalesce(%s, model_version),
                runtime_ms = coalesce(%s, runtime_ms),
                status = coalesce(%s, status),
                completed_at = case when %s in ('completed', 'dead_lettered') then now() else completed_at end,
                updated_at = now()
            where id = %s::uuid
            """,
              (
                  queue_id,
                  json.dumps(_json_compatible(lineage)),
                  lineage.get("compute_version"),
                  lineage.get("signal_registry_version"),
                  lineage.get("model_version"),
                runtime_ms,
                status,
                status,
                run_id,
            ),
        )


def update_run_forensics(
    conn,
    run_id: str,
    *,
    failure_stage: str | None = None,
    failure_code: str | None = None,
    input_snapshot_id: int | None = None,
    explanation_version: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.job_runs
            set failure_stage = %s,
                failure_code = %s,
                input_snapshot_id = coalesce(%s, input_snapshot_id),
                explanation_version = coalesce(%s, explanation_version),
                updated_at = now()
            where id = %s::uuid
            """,
            (
                failure_stage,
                failure_code,
                input_snapshot_id,
                explanation_version,
                run_id,
            ),
        )


def replace_run_stage_timings(
    conn,
    run_id: str,
    workspace_id: str,
    watchlist_id: str | None,
    rows: list[dict[str, Any]],
) -> None:
    with conn.cursor() as cur:
        cur.execute("delete from public.job_run_stage_timings where job_run_id = %s::uuid", (run_id,))
        if not rows:
            return
        cur.executemany(
            """
            insert into public.job_run_stage_timings (
              job_run_id,
              workspace_id,
              watchlist_id,
              stage_name,
              stage_status,
              started_at,
              completed_at,
              runtime_ms,
              error_summary,
              failure_code,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            """,
            [
                (
                    run_id,
                    workspace_id,
                    watchlist_id,
                    row["stage_name"],
                    row["stage_status"],
                    row["started_at"],
                    row["completed_at"],
                      row["runtime_ms"],
                      row.get("error_summary"),
                      row.get("failure_code"),
                      Jsonb(_json_compatible(row.get("metadata", {}))),
                  )
                  for row in rows
              ],
        )


def upsert_run_input_snapshot(
    conn,
    run_id: str,
    workspace_id: str,
    watchlist_id: str | None,
    snapshot: dict[str, Any],
    *,
    compute_scope_id: str | None = None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.job_run_input_snapshots (
              job_run_id,
              workspace_id,
              watchlist_id,
              source_window_start,
              source_window_end,
              asset_count,
              source_coverage,
              input_values,
              version_pins,
              metadata,
              compute_scope_id,
              scope_hash,
              scope_version,
              primary_asset_count,
              dependency_asset_count,
              asset_universe_count
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::timestamptz,
              %s::timestamptz,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            on conflict (job_run_id) do update
            set source_window_start = excluded.source_window_start,
                source_window_end = excluded.source_window_end,
                asset_count = excluded.asset_count,
                source_coverage = excluded.source_coverage,
                input_values = excluded.input_values,
                version_pins = excluded.version_pins,
                metadata = excluded.metadata,
                compute_scope_id = excluded.compute_scope_id,
                scope_hash = excluded.scope_hash,
                scope_version = excluded.scope_version,
                primary_asset_count = excluded.primary_asset_count,
                dependency_asset_count = excluded.dependency_asset_count,
                asset_universe_count = excluded.asset_universe_count,
                updated_at = now()
            returning id
            """,
            (
                run_id,
                workspace_id,
                watchlist_id,
                snapshot.get("source_window_start"),
                snapshot.get("source_window_end"),
                snapshot.get("asset_count", 0),
                Jsonb(snapshot.get("source_coverage", {})),
                Jsonb(snapshot.get("input_values", {})),
                Jsonb(snapshot.get("version_pins", {})),
                Jsonb(snapshot.get("metadata", {})),
                compute_scope_id,
                snapshot.get("metadata", {}).get("scope_hash"),
                snapshot.get("metadata", {}).get("scope_version"),
                snapshot.get("metadata", {}).get("primary_asset_count"),
                snapshot.get("metadata", {}).get("dependency_asset_count"),
                snapshot.get("metadata", {}).get("asset_universe_count"),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("job_run_input_snapshots upsert returned no id")
        return int(row["id"])


def upsert_run_explanation(
    conn,
    run_id: str,
    workspace_id: str,
    watchlist_id: str | None,
    explanation: dict[str, Any],
    explanation_version: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.job_run_explanations (
              job_run_id,
              workspace_id,
              watchlist_id,
              explanation_version,
              summary,
              regime_summary,
              signal_summary,
              composite_summary,
              invalidator_summary,
              top_positive_contributors,
              top_negative_contributors,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            on conflict (job_run_id) do update
            set explanation_version = excluded.explanation_version,
                summary = excluded.summary,
                regime_summary = excluded.regime_summary,
                signal_summary = excluded.signal_summary,
                composite_summary = excluded.composite_summary,
                invalidator_summary = excluded.invalidator_summary,
                top_positive_contributors = excluded.top_positive_contributors,
                top_negative_contributors = excluded.top_negative_contributors,
                metadata = excluded.metadata,
                updated_at = now()
            """,
            (
                run_id,
                workspace_id,
                watchlist_id,
                explanation_version,
                explanation.get("summary"),
                Jsonb(explanation.get("regime_summary", {})),
                Jsonb(explanation.get("signal_summary", {})),
                Jsonb(explanation.get("composite_summary", {})),
                Jsonb(explanation.get("invalidator_summary", {})),
                Jsonb(explanation.get("top_positive_contributors", [])),
                Jsonb(explanation.get("top_negative_contributors", [])),
                Jsonb(explanation.get("metadata", {})),
            ),
        )


def replace_run_attributions(
    conn,
    run_id: str,
    workspace_id: str,
    watchlist_id: str | None,
    signal_rows: list[dict[str, Any]],
    family_rows: list[dict[str, Any]],
    *,
    attribution_version: str,
    attribution_total: float,
    attribution_target_total: float,
    attribution_reconciliation_delta: float,
    attribution_reconciled: bool,
) -> None:
    with conn.cursor() as cur:
        cur.execute("delete from public.job_run_attributions where run_id = %s::uuid", (run_id,))
        cur.execute("delete from public.job_run_signal_family_attributions where run_id = %s::uuid", (run_id,))

        if signal_rows:
            cur.executemany(
                """
                insert into public.job_run_attributions (
                  run_id,
                  workspace_id,
                  watchlist_id,
                  asset_id,
                  asset_symbol,
                  regime,
                  signal_name,
                  signal_family,
                  raw_value,
                  normalized_value,
                  weight_applied,
                  contribution_value,
                  contribution_direction,
                  is_invalidator,
                  active_invalidators,
                  metadata
                ) values (
                  %s::uuid,
                  %s::uuid,
                  %s::uuid,
                  %s::uuid,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s
                )
                """,
                [
                    (
                        run_id,
                        workspace_id,
                        watchlist_id,
                        row.get("asset_id"),
                        row.get("asset_symbol"),
                        row.get("regime"),
                        row.get("signal_name"),
                        row.get("signal_family"),
                        row.get("raw_value"),
                        row.get("normalized_value"),
                        row.get("weight_applied"),
                        row.get("contribution_value"),
                        row.get("contribution_direction"),
                        row.get("is_invalidator"),
                        Jsonb(row.get("active_invalidators", [])),
                        Jsonb(row.get("metadata", {})),
                    )
                    for row in signal_rows
                ],
            )

        if family_rows:
            cur.executemany(
                """
                insert into public.job_run_signal_family_attributions (
                  run_id,
                  workspace_id,
                  watchlist_id,
                  signal_family,
                  family_rank,
                  family_weight,
                  family_score,
                  family_pct_of_total,
                  positive_contribution,
                  negative_contribution,
                  invalidator_contribution,
                  active_invalidators,
                  metadata
                ) values (
                  %s::uuid,
                  %s::uuid,
                  %s::uuid,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s
                )
                """,
                [
                    (
                        run_id,
                        workspace_id,
                        watchlist_id,
                        row.get("signal_family"),
                        row.get("family_rank"),
                        row.get("family_weight"),
                        row.get("family_score"),
                        row.get("family_pct_of_total"),
                        row.get("positive_contribution"),
                        row.get("negative_contribution"),
                        row.get("invalidator_contribution"),
                        Jsonb(row.get("active_invalidators", [])),
                        Jsonb(row.get("metadata", {})),
                    )
                    for row in family_rows
                ],
            )

        cur.execute(
            """
            update public.job_runs
            set attribution_version = %s,
                attribution_reconciled = %s,
                attribution_total = %s,
                attribution_target_total = %s,
                attribution_reconciliation_delta = %s,
                updated_at = now()
            where id = %s::uuid
            """,
            (
                attribution_version,
                attribution_reconciled,
                attribution_total,
                attribution_target_total,
                attribution_reconciliation_delta,
                run_id,
            ),
        )


def get_run_drift_context(conn, run_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              jr.id,
              jr.workspace_id,
              jr.watchlist_id,
              jr.queue_name,
              jr.is_replay,
              jr.replayed_from_run_id,
              jr.compute_version,
              jr.signal_registry_version,
              jr.model_version,
              jr.attribution_target_total as composite_score,
              coalesce(exp.regime_summary, '{}'::jsonb) as regime_summary
            from public.job_runs jr
            left join public.job_run_explanations exp
              on exp.job_run_id = jr.id
            where jr.id = %s::uuid
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_prior_successful_run_drift_context(
    conn,
    workspace_id: str,
    watchlist_id: str | None,
    queue_name: str,
    current_run_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              jr.id,
              jr.workspace_id,
              jr.watchlist_id,
              jr.queue_name,
              jr.is_replay,
              jr.replayed_from_run_id,
              jr.compute_version,
              jr.signal_registry_version,
              jr.model_version,
              jr.attribution_target_total as composite_score,
              coalesce(exp.regime_summary, '{}'::jsonb) as regime_summary
            from public.job_runs jr
            left join public.job_run_explanations exp
              on exp.job_run_id = jr.id
            where jr.workspace_id = %s::uuid
              and jr.queue_name = %s
              and jr.id <> %s::uuid
              and jr.status = 'completed'
              and (jr.watchlist_id is not distinct from %s::uuid)
            order by jr.completed_at desc nulls last, jr.created_at desc
            limit 1
            """,
            (workspace_id, queue_name, current_run_id, watchlist_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_run_family_attribution_rows(conn, run_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              signal_family,
              family_rank,
              family_weight,
              family_score,
              family_pct_of_total,
              positive_contribution,
              negative_contribution,
              invalidator_contribution,
              active_invalidators,
              metadata
            from public.job_run_signal_family_attributions
            where run_id = %s::uuid
            order by family_rank asc, signal_family asc
            """,
            (run_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_run_signal_attribution_rows(conn, run_id: str, limit: int | None = 12) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        query = """
            select
              asset_symbol,
              signal_name,
              signal_family,
              regime,
              contribution_value,
              weight_applied,
              active_invalidators,
              is_invalidator
            from public.job_run_attributions
            where run_id = %s::uuid
            order by abs(contribution_value) desc, asset_symbol asc nulls last, signal_name asc
        """
        params: tuple[Any, ...]
        if limit is None:
            params = (run_id,)
        else:
            query += "\n            limit %s"
            params = (run_id, limit)
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def replace_run_drift_metrics(
    conn,
    run_id: str,
    workspace_id: str,
    watchlist_id: str | None,
    comparison_run_id: str | None,
    rows: list[dict[str, Any]],
) -> None:
    with conn.cursor() as cur:
        cur.execute("delete from public.job_run_drift_metrics where run_id = %s::uuid", (run_id,))
        if not rows:
            return
        cur.executemany(
            """
            insert into public.job_run_drift_metrics (
              run_id,
              comparison_run_id,
              workspace_id,
              watchlist_id,
              metric_type,
              entity_name,
              current_value,
              baseline_value,
              delta_abs,
              delta_pct,
              z_score,
              drift_flag,
              severity,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            """,
            [
                (
                    run_id,
                    comparison_run_id,
                    workspace_id,
                    watchlist_id,
                    row["metric_type"],
                    row["entity_name"],
                    row.get("current_value"),
                    row.get("baseline_value"),
                    row.get("delta_abs"),
                    row.get("delta_pct"),
                    row.get("z_score"),
                    row["drift_flag"],
                    row["severity"],
                    Jsonb(row.get("metadata", {})),
                )
                for row in rows
            ],
        )


def update_run_drift_summary(
    conn,
    run_id: str,
    comparison_run_id: str | None,
    drift_severity: str,
    drift_summary: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.job_runs
            set comparison_run_id = %s::uuid,
                drift_severity = %s,
                drift_summary = %s::jsonb,
                updated_at = now()
            where id = %s::uuid
            """,
              (
                  comparison_run_id,
                  drift_severity,
                  json.dumps(_json_compatible(drift_summary)),
                  run_id,
                ),
            )


def get_run_replay_delta_context(conn, run_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              jr.id,
              jr.workspace_id,
              jr.watchlist_id,
              jr.queue_name,
              jr.compute_version,
              jr.signal_registry_version,
              jr.model_version,
              jr.is_replay,
              jr.replayed_from_run_id,
              jr.input_snapshot_id,
              jr.attribution_target_total as composite_score,
              coalesce(exp.regime_summary, '{}'::jsonb) as regime_summary
            from public.job_runs jr
            left join public.job_run_explanations exp
              on exp.job_run_id = jr.id
            where jr.id = %s::uuid
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_run_input_snapshot_payload(conn, run_id: str) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              source_window_start,
              source_window_end,
              asset_count,
              source_coverage,
              input_values,
              version_pins,
              metadata,
              compute_scope_id,
              scope_hash,
              scope_version,
              primary_asset_count,
              dependency_asset_count,
              asset_universe_count
            from public.job_run_input_snapshots
            where job_run_id = %s::uuid
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else {}


def upsert_replay_delta(conn, payload: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.job_run_replay_deltas (
              replay_run_id,
              source_run_id,
              workspace_id,
              watchlist_id,
              input_match_score,
              input_match_details,
              version_match,
              compute_version_changed,
              signal_registry_version_changed,
              model_version_changed,
              regime_changed,
              source_regime,
              replay_regime,
              source_composite,
              replay_composite,
              composite_delta,
              composite_delta_abs,
              largest_signal_deltas,
              largest_family_deltas,
              summary,
              severity
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            on conflict (replay_run_id) do update
            set source_run_id = excluded.source_run_id,
                workspace_id = excluded.workspace_id,
                watchlist_id = excluded.watchlist_id,
                input_match_score = excluded.input_match_score,
                input_match_details = excluded.input_match_details,
                version_match = excluded.version_match,
                compute_version_changed = excluded.compute_version_changed,
                signal_registry_version_changed = excluded.signal_registry_version_changed,
                model_version_changed = excluded.model_version_changed,
                regime_changed = excluded.regime_changed,
                source_regime = excluded.source_regime,
                replay_regime = excluded.replay_regime,
                source_composite = excluded.source_composite,
                replay_composite = excluded.replay_composite,
                composite_delta = excluded.composite_delta,
                composite_delta_abs = excluded.composite_delta_abs,
                largest_signal_deltas = excluded.largest_signal_deltas,
                largest_family_deltas = excluded.largest_family_deltas,
                summary = excluded.summary,
                severity = excluded.severity
            """,
            (
                payload["replay_run_id"],
                payload["source_run_id"],
                payload["workspace_id"],
                payload.get("watchlist_id"),
                payload["input_match_score"],
                Jsonb(_json_compatible(payload.get("input_match_details", {}))),
                payload["version_match"],
                payload["compute_version_changed"],
                payload["signal_registry_version_changed"],
                payload["model_version_changed"],
                payload["regime_changed"],
                payload.get("source_regime"),
                payload.get("replay_regime"),
                payload.get("source_composite"),
                payload.get("replay_composite"),
                payload.get("composite_delta"),
                payload.get("composite_delta_abs"),
                Jsonb(_json_compatible(payload.get("largest_signal_deltas", []))),
                Jsonb(_json_compatible(payload.get("largest_family_deltas", []))),
                Jsonb(_json_compatible(payload.get("summary", {}))),
                payload["severity"],
            ),
        )


def upsert_regime_transition_event(conn, payload: dict[str, Any]) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            select public.upsert_regime_transition_event(
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            ) as transition_event_id
            """,
            (
                payload["run_id"],
                payload.get("prior_run_id"),
                payload["workspace_id"],
                payload.get("watchlist_id"),
                payload.get("queue_name"),
                payload.get("from_regime"),
                payload.get("to_regime"),
                payload.get("transition_detected", False),
                payload.get("transition_classification", "none"),
                payload.get("stability_score"),
                payload.get("anomaly_likelihood"),
                payload.get("composite_shift"),
                payload.get("composite_shift_abs"),
                payload.get("dominant_family_gained"),
                payload.get("dominant_family_lost"),
                json.dumps(_json_compatible(payload.get("metadata", {}))),
            ),
        )
        row = cur.fetchone()
        if not row or not row.get("transition_event_id"):
            raise RuntimeError("upsert_regime_transition_event returned no id")
        return str(row["transition_event_id"])


def replace_regime_transition_family_shifts(
    conn,
    transition_event_id: str,
    payload: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select public.replace_regime_transition_family_shifts(
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::jsonb
            )
            """,
            (
                transition_event_id,
                payload["run_id"],
                payload.get("prior_run_id"),
                payload["workspace_id"],
                payload.get("watchlist_id"),
                json.dumps(_json_compatible(payload.get("family_shifts", []))),
            ),
        )


def get_recent_successful_run_contexts(
    conn,
    workspace_id: str,
    watchlist_id: str | None,
    queue_name: str,
    current_run_id: str,
    *,
    limit: int = 7,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              jr.id,
              jr.workspace_id,
              jr.watchlist_id,
              jr.queue_name,
              jr.compute_version,
              jr.signal_registry_version,
              jr.model_version,
              jr.attribution_target_total as composite_score,
              coalesce(exp.regime_summary, '{}'::jsonb) as regime_summary
            from public.job_runs jr
            left join public.job_run_explanations exp
              on exp.job_run_id = jr.id
            where jr.workspace_id = %s::uuid
              and jr.queue_name = %s
              and jr.id <> %s::uuid
              and jr.status = 'completed'
              and jr.is_replay = false
              and (jr.watchlist_id is not distinct from %s::uuid)
            order by jr.completed_at desc nulls last, jr.created_at desc
            limit %s
            """,
            (workspace_id, queue_name, current_run_id, watchlist_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_family_history_for_runs(conn, run_ids: list[str]) -> dict[str, list[float]]:
    if not run_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              signal_family,
              family_score
            from public.job_run_signal_family_attributions
            where run_id = any(%s::uuid[])
            order by created_at desc
            """,
            (run_ids,),
        )
        rows = cur.fetchall()
    history: dict[str, list[float]] = {}
    for row in rows:
        signal_family = str(row["signal_family"])
        family_score = row.get("family_score")
        if family_score is None:
            continue
        history.setdefault(signal_family, []).append(float(family_score))
    return history


def get_recent_replay_consistency_metrics(
    conn,
    workspace_id: str,
    watchlist_id: str | None,
    queue_name: str,
    current_run_id: str,
    *,
    limit: int = 7,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with recent as (
              select
                rd.input_match_score,
                rd.composite_delta_abs,
                rd.severity
              from public.job_run_replay_deltas rd
              join public.job_runs jr
                on jr.id = rd.replay_run_id
              where rd.workspace_id = %s::uuid
                and jr.queue_name = %s
                and rd.replay_run_id <> %s::uuid
                and (rd.watchlist_id is not distinct from %s::uuid)
              order by jr.completed_at desc nulls last, jr.created_at desc
              limit %s
            )
            select
              count(*)::integer as runs_considered,
              avg(
                case
                  when coalesce(input_match_score, 0) < 0.999
                    or coalesce(composite_delta_abs, 0) >= 0.01
                    or coalesce(severity, 'low') <> 'low'
                  then 1.0 else 0.0
                end
              )::double precision as mismatch_rate,
              avg(input_match_score)::double precision as avg_input_match_score,
              avg(composite_delta_abs)::double precision as avg_composite_delta_abs
            from recent
            """,
            (workspace_id, queue_name, current_run_id, watchlist_id, limit),
        )
        row = cur.fetchone()
        return dict(row) if row else {
            "runs_considered": 0,
            "mismatch_rate": None,
            "avg_input_match_score": None,
            "avg_composite_delta_abs": None,
        }


def get_recent_regime_stability_metrics(
    conn,
    workspace_id: str,
    watchlist_id: str | None,
    queue_name: str,
    current_run_id: str,
    *,
    limit: int = 7,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with recent as (
              select
                rte.transition_classification
              from public.regime_transition_events rte
              join public.job_runs jr
                on jr.id = rte.run_id
              where rte.workspace_id = %s::uuid
                and jr.queue_name = %s
                and rte.run_id <> %s::uuid
                and jr.is_replay = false
                and (rte.watchlist_id is not distinct from %s::uuid)
              order by jr.completed_at desc nulls last, jr.created_at desc
              limit %s
            )
            select
              count(*)::integer as transitions_considered,
              count(*) filter (where transition_classification = 'conflicting')::integer as conflicting_transition_count,
              count(*) filter (where transition_classification = 'abrupt')::integer as abrupt_transition_count
            from recent
            """,
            (workspace_id, queue_name, current_run_id, watchlist_id, limit),
        )
        row = cur.fetchone()
        return dict(row) if row else {
            "transitions_considered": 0,
            "conflicting_transition_count": 0,
            "abrupt_transition_count": 0,
        }


def persist_stability_metrics(conn, payload: dict[str, Any]) -> None:
    baseline = payload["baseline"]
    family_rows = payload.get("family_rows", [])
    replay_metrics = payload["replay_metrics"]
    regime_metrics = payload["regime_metrics"]

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.run_stability_baselines (
              run_id,
              workspace_id,
              watchlist_id,
              queue_name,
              window_size,
              baseline_run_count,
              composite_baseline,
              composite_current,
              composite_delta_abs,
              composite_delta_pct,
              composite_instability_score,
              family_instability_score,
              replay_consistency_risk_score,
              regime_instability_score,
              dominant_family,
              dominant_family_changed,
              dominant_regime,
              regime_changed,
              stability_classification,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            on conflict (run_id, window_size) do update
            set baseline_run_count = excluded.baseline_run_count,
                composite_baseline = excluded.composite_baseline,
                composite_current = excluded.composite_current,
                composite_delta_abs = excluded.composite_delta_abs,
                composite_delta_pct = excluded.composite_delta_pct,
                composite_instability_score = excluded.composite_instability_score,
                family_instability_score = excluded.family_instability_score,
                replay_consistency_risk_score = excluded.replay_consistency_risk_score,
                regime_instability_score = excluded.regime_instability_score,
                dominant_family = excluded.dominant_family,
                dominant_family_changed = excluded.dominant_family_changed,
                dominant_regime = excluded.dominant_regime,
                regime_changed = excluded.regime_changed,
                stability_classification = excluded.stability_classification,
                metadata = excluded.metadata
            """,
            (
                baseline["run_id"],
                baseline["workspace_id"],
                baseline.get("watchlist_id"),
                baseline["queue_name"],
                baseline["window_size"],
                baseline["baseline_run_count"],
                baseline.get("composite_baseline"),
                baseline.get("composite_current"),
                baseline.get("composite_delta_abs"),
                baseline.get("composite_delta_pct"),
                baseline["composite_instability_score"],
                baseline["family_instability_score"],
                baseline["replay_consistency_risk_score"],
                baseline["regime_instability_score"],
                baseline.get("dominant_family"),
                baseline["dominant_family_changed"],
                baseline.get("dominant_regime"),
                baseline["regime_changed"],
                baseline["stability_classification"],
                Jsonb(_json_compatible(baseline.get("metadata", {}))),
                ),
            )

        cur.execute("delete from public.signal_family_stability_metrics where run_id = %s::uuid", (baseline["run_id"],))
        if family_rows:
            cur.executemany(
                """
                insert into public.signal_family_stability_metrics (
                  run_id,
                  workspace_id,
                  watchlist_id,
                  signal_family,
                  family_score_current,
                  family_score_baseline,
                  family_delta_abs,
                  family_delta_pct,
                  instability_score,
                  family_rank,
                  metadata
                ) values (
                  %s::uuid,
                  %s::uuid,
                  %s::uuid,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s
                )
                """,
                [
                    (
                        baseline["run_id"],
                        baseline["workspace_id"],
                        baseline.get("watchlist_id"),
                        row["signal_family"],
                        row.get("family_score_current"),
                        row.get("family_score_baseline"),
                        row.get("family_delta_abs"),
                        row.get("family_delta_pct"),
                        row.get("instability_score"),
                        row["family_rank"],
                        Jsonb(_json_compatible(row.get("metadata", {}))),
                    )
                    for row in family_rows
                ],
            )

        cur.execute(
            """
            insert into public.replay_consistency_metrics (
              run_id,
              workspace_id,
              watchlist_id,
              queue_name,
              replay_runs_considered,
              mismatch_rate,
              avg_input_match_score,
              avg_composite_delta_abs,
              instability_score,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            on conflict (run_id) do update
            set replay_runs_considered = excluded.replay_runs_considered,
                mismatch_rate = excluded.mismatch_rate,
                avg_input_match_score = excluded.avg_input_match_score,
                avg_composite_delta_abs = excluded.avg_composite_delta_abs,
                instability_score = excluded.instability_score,
                metadata = excluded.metadata
            """,
            (
                replay_metrics["run_id"],
                replay_metrics["workspace_id"],
                replay_metrics.get("watchlist_id"),
                replay_metrics["queue_name"],
                replay_metrics["replay_runs_considered"],
                replay_metrics.get("mismatch_rate"),
                replay_metrics.get("avg_input_match_score"),
                replay_metrics.get("avg_composite_delta_abs"),
                replay_metrics["instability_score"],
                Jsonb(_json_compatible(replay_metrics.get("metadata", {}))),
            ),
        )

        cur.execute(
            """
            insert into public.regime_stability_metrics (
              run_id,
              workspace_id,
              watchlist_id,
              queue_name,
              transitions_considered,
              conflicting_transition_count,
              abrupt_transition_count,
              instability_score,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            on conflict (run_id) do update
            set transitions_considered = excluded.transitions_considered,
                conflicting_transition_count = excluded.conflicting_transition_count,
                abrupt_transition_count = excluded.abrupt_transition_count,
                instability_score = excluded.instability_score,
                metadata = excluded.metadata
            """,
            (
                regime_metrics["run_id"],
                regime_metrics["workspace_id"],
                regime_metrics.get("watchlist_id"),
                regime_metrics["queue_name"],
                regime_metrics["transitions_considered"],
                regime_metrics["conflicting_transition_count"],
                regime_metrics["abrupt_transition_count"],
                regime_metrics["instability_score"],
                Jsonb(_json_compatible(regime_metrics.get("metadata", {}))),
            ),
        )


def get_governance_alert_rules(conn, workspace_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct on (rule_name) *
            from public.governance_alert_rules
            where enabled = true
              and (workspace_id = %s::uuid or workspace_id is null)
            order by rule_name asc, (workspace_id is null) asc, updated_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_active_regime_threshold_row(conn, workspace_id: str, regime: str | None) -> dict[str, Any] | None:
    requested_regime = str(regime or "default")
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.active_regime_thresholds
            where (workspace_id = %s::uuid or workspace_id is null)
              and regime in (%s, 'default')
            order by
              (workspace_id is null) asc,
              (regime = %s) desc,
              profile_name asc
            limit 1
            """,
            (workspace_id, requested_regime, requested_regime),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_governance_threshold_application(conn, payload: dict[str, Any]) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_threshold_applications (
              run_id,
              workspace_id,
              watchlist_id,
              regime,
              profile_id,
              override_id,
              evaluation_stage,
              applied_thresholds,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s::uuid,
              %s::uuid,
              %s,
              %s::jsonb,
              %s::jsonb
            )
            returning *
            """,
            (
                payload["run_id"],
                payload["workspace_id"],
                payload.get("watchlist_id"),
                payload.get("regime", "default"),
                payload.get("profile_id"),
                payload.get("override_id"),
                payload.get("evaluation_stage", "stability"),
                Jsonb(_json_compatible(payload.get("applied_thresholds", {}))),
                Jsonb(_json_compatible(payload.get("metadata", {}))),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_threshold_applications insert returned no row")
        return dict(row)


def get_governance_stability_row(conn, run_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              rsb.run_id,
              rsb.workspace_id,
              rsb.watchlist_id,
              jr.compute_version,
              jr.signal_registry_version,
              jr.model_version,
              rsb.family_instability_score,
              rsb.stability_classification,
              rsb.regime_instability_score,
              rsb.replay_consistency_risk_score,
              rsb.composite_instability_score,
              rsb.dominant_regime,
              rsb.created_at
            from public.run_stability_baselines rsb
            join public.job_runs jr
              on jr.id = rsb.run_id
            where rsb.run_id = %s::uuid
              and rsb.window_size = 7
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_governance_version_health_row(
    conn,
    workspace_id: str,
    compute_version: str | None,
    signal_registry_version: str | None,
    model_version: str | None,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.version_health_rankings
            where workspace_id = %s::uuid
              and compute_version = coalesce(%s, 'unknown')
              and signal_registry_version = coalesce(%s, 'unknown')
              and model_version = coalesce(%s, 'unknown')
            """,
            (workspace_id, compute_version, signal_registry_version, model_version),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_governance_version_replay_row(
    conn,
    workspace_id: str,
    compute_version: str | None,
    signal_registry_version: str | None,
    model_version: str | None,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.version_replay_consistency_summary
            where workspace_id = %s::uuid
              and compute_version = coalesce(%s, 'unknown')
              and signal_registry_version = coalesce(%s, 'unknown')
              and model_version = coalesce(%s, 'unknown')
            """,
            (workspace_id, compute_version, signal_registry_version, model_version),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_governance_version_regime_row(
    conn,
    workspace_id: str,
    compute_version: str | None,
    signal_registry_version: str | None,
    model_version: str | None,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.version_regime_behavior_summary
            where workspace_id = %s::uuid
              and compute_version = coalesce(%s, 'unknown')
              and signal_registry_version = coalesce(%s, 'unknown')
              and model_version = coalesce(%s, 'unknown')
            """,
            (workspace_id, compute_version, signal_registry_version, model_version),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_governance_alert_events(conn, events: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    inserted: list[dict[str, Any]] = []
    if not events:
        return inserted

    with conn.cursor() as cur:
        for event in events:
            cur.execute(
                """
                insert into public.governance_alert_events (
                  workspace_id,
                  watchlist_id,
                  run_id,
                  rule_name,
                  event_type,
                  severity,
                  dedupe_key,
                  metric_source,
                  metric_name,
                  metric_value_numeric,
                  metric_value_text,
                  threshold_numeric,
                  threshold_text,
                  compute_version,
                  signal_registry_version,
                  model_version,
                  metadata
                ) values (
                  %s::uuid,
                  %s::uuid,
                  %s::uuid,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s::jsonb
                )
                on conflict (dedupe_key) do nothing
                returning *
                """,
                (
                    event["workspace_id"],
                    event.get("watchlist_id"),
                    event.get("run_id"),
                    event["rule_name"],
                    event["event_type"],
                    event["severity"],
                    event["dedupe_key"],
                    event["metric_source"],
                    event["metric_name"],
                    event.get("metric_value_numeric"),
                    event.get("metric_value_text"),
                    event.get("threshold_numeric"),
                    event.get("threshold_text"),
                    event.get("compute_version"),
                    event.get("signal_registry_version"),
                    event.get("model_version"),
                    Jsonb(_json_compatible(event.get("metadata", {}))),
                ),
            )
            row = cur.fetchone()
            if row:
                inserted.append(dict(row))
    return inserted


def upsert_governance_anomaly_clusters(conn, rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    persisted: list[dict[str, Any]] = []
    if not rows:
        return persisted

    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                insert into public.governance_anomaly_clusters (
                  workspace_id,
                  watchlist_id,
                  version_tuple,
                  cluster_key,
                  alert_type,
                  regime,
                  severity,
                  status,
                  first_seen_at,
                  last_seen_at,
                  event_count,
                  latest_event_id,
                  latest_run_id,
                  metadata
                ) values (
                  %s::uuid,
                  %s::uuid,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  'open',
                  now(),
                  now(),
                  1,
                  %s::uuid,
                  %s::uuid,
                  %s::jsonb
                )
                on conflict (workspace_id, cluster_key) where status = 'open'
                do update set
                  last_seen_at = now(),
                  event_count = public.governance_anomaly_clusters.event_count + 1,
                  latest_event_id = coalesce(excluded.latest_event_id, public.governance_anomaly_clusters.latest_event_id),
                  latest_run_id = coalesce(excluded.latest_run_id, public.governance_anomaly_clusters.latest_run_id),
                  severity = case
                    when excluded.severity = 'high' or public.governance_anomaly_clusters.severity = 'high' then 'high'
                    when excluded.severity = 'medium' or public.governance_anomaly_clusters.severity = 'medium' then 'medium'
                    else 'low'
                  end,
                  metadata = coalesce(public.governance_anomaly_clusters.metadata, '{}'::jsonb) || coalesce(excluded.metadata, '{}'::jsonb),
                  updated_at = now()
                returning *
                """,
                (
                    row["workspace_id"],
                    row.get("watchlist_id"),
                    row["version_tuple"],
                    row["cluster_key"],
                    row["alert_type"],
                    row.get("regime"),
                    row["severity"],
                    row.get("governance_alert_event_id"),
                    row.get("run_id"),
                    Jsonb(_json_compatible(row.get("metadata", {}))),
                ),
            )
            cluster = cur.fetchone()
            if not cluster:
                continue
            cluster_dict = dict(cluster)
            persisted.append(cluster_dict)

            if row.get("governance_alert_event_id"):
                cur.execute(
                    """
                    insert into public.governance_anomaly_cluster_members (
                      cluster_id,
                      governance_alert_event_id,
                      run_id
                    ) values (
                      %s::uuid,
                      %s::uuid,
                      %s::uuid
                    )
                    on conflict (cluster_id, governance_alert_event_id) do nothing
                    """,
                    (
                        cluster_dict["id"],
                        row["governance_alert_event_id"],
                        row.get("run_id"),
                    ),
                )
    return persisted


def get_governance_anomaly_clusters(
    conn,
    workspace_id: str,
    *,
    limit: int = 25,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_anomaly_cluster_state
            where workspace_id = %s::uuid
            order by
              case severity
                when 'high' then 2
                when 'medium' then 1
                else 0
              end desc,
              last_seen_at desc
            limit %s
            """,
            (workspace_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_watchlist_anomaly_summary(conn, workspace_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.watchlist_anomaly_summary
            where workspace_id = %s::uuid
            order by high_open_cluster_count desc, open_cluster_count desc, watchlist_slug asc nulls last
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_active_governance_degradation_state(
    conn,
    *,
    workspace_id: str,
    watchlist_id: str | None,
    degradation_type: str,
    version_tuple: str,
    regime: str | None,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_degradation_states
            where workspace_id = %s::uuid
              and coalesce(watchlist_id, '00000000-0000-0000-0000-000000000000'::uuid)
                = coalesce(%s::uuid, '00000000-0000-0000-0000-000000000000'::uuid)
              and degradation_type = %s
              and version_tuple = %s
              and coalesce(regime, 'all') = coalesce(%s, 'all')
              and state_status in ('active', 'escalated')
            order by
              case state_status
                when 'escalated' then 1
                else 0
              end desc,
              last_seen_at desc
            limit 1
            """,
            (workspace_id, watchlist_id, degradation_type, version_tuple, regime),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def persist_governance_degradation_state(conn, payload: dict[str, Any]) -> dict[str, Any]:
    active_state = get_active_governance_degradation_state(
        conn,
        workspace_id=str(payload["workspace_id"]),
        watchlist_id=str(payload["watchlist_id"]) if payload.get("watchlist_id") else None,
        degradation_type=str(payload["degradation_type"]),
        version_tuple=str(payload["version_tuple"]),
        regime=str(payload["regime"]) if payload.get("regime") else None,
    )
    with conn.cursor() as cur:
        if active_state:
            cur.execute(
                """
                update public.governance_degradation_states
                set state_status = %s,
                    severity = case
                      when %s = 'critical' or severity = 'critical' then 'critical'
                      when %s = 'high' or severity = 'high' then 'high'
                      when %s = 'medium' or severity = 'medium' then 'medium'
                      else 'low'
                    end,
                    last_seen_at = greatest(last_seen_at, %s::timestamptz),
                    escalated_at = case
                      when %s = 'escalated'
                        then coalesce(escalated_at, %s::timestamptz)
                      else escalated_at
                    end,
                    event_count = greatest(event_count, %s),
                    cluster_count = greatest(cluster_count, %s),
                    source_summary = coalesce(source_summary, '{}'::jsonb) || %s::jsonb,
                    metadata = coalesce(metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = now()
                where id = %s::uuid
                returning *
                """,
                (
                    payload.get("state_status", "active"),
                    payload.get("severity", "low"),
                    payload.get("severity", "low"),
                    payload.get("severity", "low"),
                    payload.get("last_seen_at"),
                    payload.get("state_status", "active"),
                    payload.get("escalated_at"),
                    int(payload.get("event_count") or 0),
                    int(payload.get("cluster_count") or 0),
                    Jsonb(_json_compatible(payload.get("source_summary", {}))),
                    Jsonb(_json_compatible(payload.get("metadata", {}))),
                    active_state["id"],
                ),
            )
        else:
            cur.execute(
                """
                insert into public.governance_degradation_states (
                  workspace_id,
                  watchlist_id,
                  degradation_type,
                  version_tuple,
                  regime,
                  state_status,
                  severity,
                  first_seen_at,
                  last_seen_at,
                  escalated_at,
                  event_count,
                  cluster_count,
                  source_summary,
                  metadata
                ) values (
                  %s::uuid,
                  %s::uuid,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s::timestamptz,
                  %s::timestamptz,
                  %s::timestamptz,
                  %s,
                  %s,
                  %s::jsonb,
                  %s::jsonb
                )
                returning *
                """,
                (
                    payload["workspace_id"],
                    payload.get("watchlist_id"),
                    payload["degradation_type"],
                    payload["version_tuple"],
                    payload.get("regime"),
                    payload.get("state_status", "active"),
                    payload.get("severity", "low"),
                    payload.get("first_seen_at"),
                    payload.get("last_seen_at"),
                    payload.get("escalated_at"),
                    int(payload.get("event_count") or 0),
                    int(payload.get("cluster_count") or 0),
                    Jsonb(_json_compatible(payload.get("source_summary", {}))),
                    Jsonb(_json_compatible(payload.get("metadata", {}))),
                ),
            )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_degradation_states persistence returned no row")
        return dict(row)


def insert_governance_degradation_state_members(
    conn,
    rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    inserted: list[dict[str, Any]] = []
    if not rows:
        return inserted

    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                insert into public.governance_degradation_state_members (
                  state_id,
                  workspace_id,
                  governance_alert_event_id,
                  anomaly_cluster_id,
                  job_run_id,
                  member_type,
                  member_key,
                  observed_at,
                  metadata
                ) values (
                  %s::uuid,
                  %s::uuid,
                  %s::uuid,
                  %s::uuid,
                  %s::uuid,
                  %s,
                  %s,
                  %s::timestamptz,
                  %s::jsonb
                )
                on conflict (state_id, member_type, member_key) do nothing
                returning *
                """,
                (
                    row["state_id"],
                    row["workspace_id"],
                    row.get("governance_alert_event_id"),
                    row.get("anomaly_cluster_id"),
                    row.get("job_run_id"),
                    row["member_type"],
                    row["member_key"],
                    row.get("observed_at"),
                    Jsonb(_json_compatible(row.get("metadata", {}))),
                ),
            )
            member = cur.fetchone()
            if member:
                inserted.append(dict(member))
    return inserted


def get_governance_degradation_states(
    conn,
    workspace_id: str,
    *,
    watchlist_id: str | None = None,
    statuses: Sequence[str] | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    statuses = tuple(statuses or ("active", "escalated"))
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_degradation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
              and state_status = any(%s::text[])
            order by
              case state_status
                when 'escalated' then 2
                when 'active' then 1
                else 0
              end desc,
              last_seen_at desc
            limit %s
            """,
            (workspace_id, watchlist_id, watchlist_id, list(statuses), limit),
        )
        return [dict(row) for row in cur.fetchall()]


def resolve_governance_degradation_state(
    conn,
    *,
    state_id: str,
    resolution_summary: dict[str, Any],
    resolved_at: datetime,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_degradation_states
            set state_status = 'resolved',
                resolved_at = %s::timestamptz,
                resolution_summary = coalesce(resolution_summary, '{}'::jsonb) || %s::jsonb,
                updated_at = now()
            where id = %s::uuid
            returning *
            """,
            (
                resolved_at,
                Jsonb(_json_compatible(resolution_summary)),
                state_id,
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_degradation_states resolve returned no row")
        return dict(row)


def insert_governance_recovery_event(conn, payload: dict[str, Any]) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_recovery_events (
              workspace_id,
              state_id,
              watchlist_id,
              degradation_type,
              version_tuple,
              regime,
              recovered_at,
              recovery_reason,
              prior_severity,
              trailing_metrics,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s::timestamptz,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb
            )
            returning *
            """,
            (
                payload["workspace_id"],
                payload["state_id"],
                payload.get("watchlist_id"),
                payload["degradation_type"],
                payload["version_tuple"],
                payload.get("regime"),
                payload.get("recovered_at"),
                payload["recovery_reason"],
                payload["prior_severity"],
                Jsonb(_json_compatible(payload.get("trailing_metrics", {}))),
                Jsonb(_json_compatible(payload.get("metadata", {}))),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_recovery_events insert returned no row")
        return dict(row)


def insert_governance_acknowledgment(conn, payload: dict[str, Any]) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_acknowledgments (
              workspace_id,
              degradation_state_id,
              acknowledged_by,
              note,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                payload["workspace_id"],
                payload["degradation_state_id"],
                payload["acknowledged_by"],
                payload.get("note"),
                Jsonb(_json_compatible(payload.get("metadata", {}))),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_acknowledgments insert returned no row")
        return dict(row)


def upsert_governance_muting_rule(conn, payload: dict[str, Any]) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_muting_rules (
              workspace_id,
              target_type,
              target_key,
              reason,
              muted_until,
              created_by,
              is_active,
              metadata
            ) values (
              %s::uuid,
              %s,
              %s,
              %s,
              %s::timestamptz,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                payload["workspace_id"],
                payload["target_type"],
                payload["target_key"],
                payload.get("reason"),
                payload.get("muted_until"),
                payload["created_by"],
                bool(payload.get("is_active", True)),
                Jsonb(_json_compatible(payload.get("metadata", {}))),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_muting_rules insert returned no row")
        return dict(row)


def get_active_governance_muting_rules(conn, workspace_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_muting_rules
            where workspace_id = %s::uuid
              and is_active = true
              and (muted_until is null or muted_until > now())
            order by created_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def insert_governance_resolution_action(conn, payload: dict[str, Any]) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_resolution_actions (
              workspace_id,
              degradation_state_id,
              action_type,
              performed_by,
              note,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                payload["workspace_id"],
                payload["degradation_state_id"],
                payload["action_type"],
                payload["performed_by"],
                payload.get("note"),
                Jsonb(_json_compatible(payload.get("metadata", {}))),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_resolution_actions insert returned no row")
        return dict(row)


def upsert_governance_case(conn, payload: dict[str, Any]) -> dict[str, Any]:
    degradation_state_id = payload.get("degradation_state_id")
    with conn.cursor() as cur:
        existing = None
        if degradation_state_id:
            cur.execute(
                """
                select *
                from public.governance_cases
                where degradation_state_id = %s::uuid
                order by
                  case status
                    when 'open' then 3
                    when 'acknowledged' then 2
                    when 'in_progress' then 1
                    else 0
                  end desc,
                  opened_at desc
                limit 1
                """,
                (degradation_state_id,),
            )
            existing = cur.fetchone()

        if existing and existing["status"] in {"open", "acknowledged", "in_progress"}:
            cur.execute(
                """
                update public.governance_cases
                set severity = %s,
                    title = %s,
                    summary = %s,
                    watchlist_id = coalesce(%s::uuid, watchlist_id),
                    version_tuple = coalesce(%s, version_tuple),
                    metadata = coalesce(metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = now()
                where id = %s::uuid
                returning *
                """,
                (
                    payload["severity"],
                    payload["title"],
                    payload["summary"],
                    payload.get("watchlist_id"),
                    payload.get("version_tuple"),
                    Jsonb(_json_compatible(payload.get("metadata", {}))),
                    existing["id"],
                ),
            )
        elif existing:
            cur.execute(
                """
                update public.governance_cases
                set status = 'open',
                    severity = %s,
                    title = %s,
                    summary = %s,
                    watchlist_id = coalesce(%s::uuid, watchlist_id),
                    version_tuple = coalesce(%s, version_tuple),
                    resolved_at = null,
                    closed_at = null,
                    reopened_count = reopened_count + 1,
                    metadata = coalesce(metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = now()
                where id = %s::uuid
                returning *
                """,
                (
                    payload["severity"],
                    payload["title"],
                    payload["summary"],
                    payload.get("watchlist_id"),
                    payload.get("version_tuple"),
                    Jsonb(_json_compatible(payload.get("metadata", {}))),
                    existing["id"],
                ),
            )
        else:
            cur.execute(
                """
                insert into public.governance_cases (
                  workspace_id,
                  degradation_state_id,
                  watchlist_id,
                  version_tuple,
                  status,
                  severity,
                  title,
                  summary,
                  metadata
                ) values (
                  %s::uuid,
                  %s::uuid,
                  %s::uuid,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s::jsonb
                )
                returning *
                """,
                (
                    payload["workspace_id"],
                    degradation_state_id,
                    payload.get("watchlist_id"),
                    payload.get("version_tuple"),
                    payload.get("status", "open"),
                    payload["severity"],
                    payload["title"],
                    payload["summary"],
                    Jsonb(_json_compatible(payload.get("metadata", {}))),
                ),
            )

        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_cases upsert returned no row")
        return dict(row)


def resolve_governance_case_for_state(
    conn,
    *,
    degradation_state_id: str,
    resolution_note: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_cases
            set status = 'resolved',
                resolved_at = now(),
                summary = case
                  when %s::text is null then summary
                  else concat_ws(E'\n', nullif(summary, ''), %s::text)
                end,
                metadata = coalesce(metadata, '{}'::jsonb) || %s::jsonb,
                updated_at = now()
            where degradation_state_id = %s::uuid
              and status in ('open', 'acknowledged', 'in_progress')
            returning *
            """,
            (
                resolution_note,
                resolution_note,
                Jsonb(_json_compatible(metadata or {})),
                degradation_state_id,
            ),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def append_governance_case_event(
    conn,
    *,
    case_id: str,
    workspace_id: str,
    event_type: str,
    actor: str | None,
    payload: dict[str, Any],
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_case_events (
              case_id,
              workspace_id,
              event_type,
              actor,
              payload
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                case_id,
                workspace_id,
                event_type,
                actor,
                Jsonb(_json_compatible(payload)),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_case_events insert returned no row")
        return dict(row)


def append_governance_case_note(
    conn,
    *,
    case_id: str,
    workspace_id: str,
    note: str,
    author: str | None,
    note_type: str = "investigation",
    visibility: str = "internal",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_case_notes (
              case_id,
              workspace_id,
              author,
              note,
              note_type,
              visibility,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                case_id,
                workspace_id,
                author,
                note,
                note_type,
                visibility,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_case_notes insert returned no row")
        return dict(row)


def create_governance_assignment(
    conn,
    *,
    case_id: str,
    workspace_id: str,
    assigned_to: str | None,
    assigned_team: str | None,
    assigned_by: str | None,
    reason: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select current_assignee, current_team
            from public.governance_cases
            where id = %s::uuid
            """,
            (case_id,),
        )
        case_before = cur.fetchone()
        previous_assignee = case_before.get("current_assignee") if case_before else None
        previous_team = case_before.get("current_team") if case_before else None
        cur.execute(
            """
            update public.governance_assignments
            set active = false
            where case_id = %s::uuid
              and active = true
            """,
            (case_id,),
        )
        cur.execute(
            """
            insert into public.governance_assignments (
              case_id,
              workspace_id,
              assigned_to,
              assigned_team,
              assigned_by,
              reason,
              active,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              true,
              %s::jsonb
            )
            returning *
            """,
            (
                case_id,
                workspace_id,
                assigned_to,
                assigned_team,
                assigned_by,
                reason,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        assignment = cur.fetchone()
        cur.execute(
            """
            update public.governance_cases
            set current_assignee = %s,
                current_team = %s,
                status = case
                  when status = 'open' then 'in_progress'
                  else status
                end,
                updated_at = now()
            where id = %s::uuid
            returning *
            """,
            (
                assigned_to,
                assigned_team,
                case_id,
            ),
        )
        cur.fetchone()
        cur.execute(
            """
            insert into public.governance_assignment_history (
              case_id,
              workspace_id,
              previous_assignee,
              previous_team,
              new_assignee,
              new_team,
              changed_by,
              reason,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            """,
            (
                case_id,
                workspace_id,
                previous_assignee,
                previous_team,
                assigned_to,
                assigned_team,
                assigned_by,
                reason,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        if not assignment:
            raise RuntimeError("governance_assignments insert returned no row")
        return dict(assignment)


def get_active_routing_override(
    conn,
    *,
    workspace_id: str,
    case_id: str | None,
    watchlist_id: str | None,
    root_cause_code: str | None,
    severity: str | None,
    version_tuple: str | None,
    regime: str | None = None,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_overrides
            where workspace_id = %s::uuid
              and is_enabled = true
              and (case_id = %s::uuid or case_id is null)
              and (watchlist_id = %s::uuid or watchlist_id is null)
              and (root_cause_code = %s or root_cause_code is null)
              and (severity = %s or severity is null)
              and (version_tuple = %s or version_tuple is null)
              and (regime = %s or regime is null)
            order by
              (case_id is not null) desc,
              (watchlist_id is not null) desc,
              (root_cause_code is not null) desc,
              (severity is not null) desc,
              (version_tuple is not null) desc,
              (regime is not null) desc,
              created_at desc
            limit 1
            """,
            (
                workspace_id,
                case_id,
                watchlist_id,
                root_cause_code,
                severity,
                version_tuple,
                regime,
            ),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def list_matching_routing_rules(
    conn,
    *,
    workspace_id: str,
    watchlist_id: str | None,
    root_cause_code: str | None,
    severity: str | None,
    version_tuple: str | None,
    regime: str | None = None,
    repeat_count: int,
    chronic: bool,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_rules
            where workspace_id = %s::uuid
              and is_enabled = true
              and (watchlist_id = %s::uuid or watchlist_id is null)
              and (root_cause_code = %s or root_cause_code is null)
              and (severity = %s or severity is null)
              and (version_tuple = %s or version_tuple is null)
              and (regime = %s or regime is null)
              and (recurrence_min is null or recurrence_min <= %s)
              and (chronic_only = false or %s = true)
            order by priority asc, created_at desc
            """,
            (
                workspace_id,
                watchlist_id,
                root_cause_code,
                severity,
                version_tuple,
                regime,
                repeat_count,
                chronic,
            ),
        )
        return [dict(row) for row in cur.fetchall()]


def create_governance_routing_decision(
    conn,
    *,
    workspace_id: str,
    case_id: str,
    routing_rule_id: str | None,
    override_id: str | None,
    assigned_team: str | None,
    assigned_user: str | None,
    routing_reason: str,
    workload_snapshot: dict[str, Any] | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_decisions (
              workspace_id,
              case_id,
              routing_rule_id,
              override_id,
              assigned_team,
              assigned_user,
              routing_reason,
              workload_snapshot,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                case_id,
                routing_rule_id,
                override_id,
                assigned_team,
                assigned_user,
                routing_reason,
                Jsonb(_json_compatible(workload_snapshot or {})),
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_routing_decisions insert returned no row")
        return dict(row)


def get_latest_governance_assignment(
    conn,
    *,
    case_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_assignments
            where case_id = %s::uuid
            order by assigned_at desc, id desc
            limit 1
            """,
            (case_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def create_governance_routing_feedback(
    conn,
    *,
    workspace_id: str,
    case_id: str,
    routing_decision_id: str | None,
    feedback_type: str,
    assigned_to: str | None,
    assigned_team: str | None,
    prior_assigned_to: str | None,
    prior_assigned_team: str | None,
    root_cause_code: str | None,
    severity: str | None,
    recurrence_group_id: str | None,
    repeat_count: int,
    reason: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_feedback (
              workspace_id,
              case_id,
              routing_decision_id,
              feedback_type,
              assigned_to,
              assigned_team,
              prior_assigned_to,
              prior_assigned_team,
              root_cause_code,
              severity,
              recurrence_group_id,
              repeat_count,
              reason,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::uuid,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                case_id,
                routing_decision_id,
                feedback_type,
                assigned_to,
                assigned_team,
                prior_assigned_to,
                prior_assigned_team,
                root_cause_code,
                severity,
                recurrence_group_id,
                repeat_count,
                reason,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_routing_feedback insert returned no row")
        return dict(row)


def create_governance_reassignment_event(
    conn,
    *,
    workspace_id: str,
    case_id: str,
    routing_decision_id: str | None,
    previous_assigned_to: str | None,
    previous_assigned_team: str | None,
    new_assigned_to: str | None,
    new_assigned_team: str | None,
    reassignment_type: str,
    reassignment_reason: str | None,
    minutes_since_open: int | None,
    minutes_since_last_assignment: int | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_reassignment_events (
              workspace_id,
              case_id,
              routing_decision_id,
              previous_assigned_to,
              previous_assigned_team,
              new_assigned_to,
              new_assigned_team,
              reassignment_type,
              reassignment_reason,
              minutes_since_open,
              minutes_since_last_assignment,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                case_id,
                routing_decision_id,
                previous_assigned_to,
                previous_assigned_team,
                new_assigned_to,
                new_assigned_team,
                reassignment_type,
                reassignment_reason,
                minutes_since_open,
                minutes_since_last_assignment,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_reassignment_events insert returned no row")
        return dict(row)


def list_governance_routing_quality_summary(
    conn,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_quality_summary
            where workspace_id = %s::uuid
            order by acceptance_rate asc, feedback_count desc, assigned_team asc, root_cause_code asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_reassignment_pressure_summary(
    conn,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_reassignment_pressure_summary
            where workspace_id = %s::uuid
            order by reassignment_count desc, assigned_team asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def insert_governance_routing_outcome(
    conn,
    *,
    event,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_outcomes (
              workspace_id,
              case_id,
              routing_decision_id,
              assignment_id,
              assigned_to,
              assigned_team,
              root_cause_code,
              severity,
              watchlist_id,
              compute_version,
              signal_registry_version,
              model_version,
              recurrence_group_id,
              repeat_count,
              outcome_type,
              outcome_value,
              outcome_context,
              occurred_at
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s::uuid,
              %s,
              %s,
              %s,
              %s::uuid,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s::timestamptz
            )
            returning *
            """,
            (
                event.workspace_id,
                event.case_id,
                event.routing_decision_id,
                event.assignment_id,
                event.assigned_to,
                event.assigned_team,
                event.root_cause_code,
                event.severity,
                event.watchlist_id,
                event.compute_version,
                event.signal_registry_version,
                event.model_version,
                event.recurrence_group_id,
                int(event.repeat_count or 1),
                event.outcome_type,
                event.outcome_value,
                Jsonb(_json_compatible(event.outcome_context or {})),
                event.occurred_at,
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_routing_outcomes insert returned no row")
        return dict(row)


def list_governance_operator_effectiveness_summary(
    conn,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_operator_effectiveness_summary
            where workspace_id = %s::uuid
            order by resolution_rate desc nulls last, assignments desc, assigned_to asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_team_effectiveness_summary(
    conn,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_team_effectiveness_summary
            where workspace_id = %s::uuid
            order by resolution_rate desc nulls last, assignments desc, assigned_team asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_routing_recommendation_inputs(
    conn,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_recommendation_inputs
            where workspace_id = %s::uuid
            order by resolved_count desc, avg_resolve_hours asc nulls last, routing_target asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_case_for_routing_recommendation(
    conn,
    *,
    workspace_id: str,
    case_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              c.*,
              s.root_cause_code,
              s.root_cause_confidence,
              ds.regime,
              case
                when coalesce(c.repeat_count, 1) > 1 then true
                else false
              end as is_chronic
            from public.governance_case_summary c
            left join public.governance_case_summary_latest s
              on s.case_id = c.id
            left join public.governance_degradation_states ds
              on ds.id = c.degradation_state_id
            where c.workspace_id = %s::uuid
              and c.id = %s::uuid
            """,
            (workspace_id, case_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def list_routing_recommendation_candidates(
    conn,
    *,
    workspace_id: str,
    case_row: dict[str, Any],
) -> list[dict[str, Any]]:
    operator_rows = list_governance_operator_effectiveness_summary(conn, workspace_id)
    team_rows = {
        row["assigned_team"]: row
        for row in list_governance_team_effectiveness_summary(conn, workspace_id)
        if row.get("assigned_team")
    }
    operator_metrics = {
        row["operator_id"]: row
        for row in list_operator_case_metrics(conn, workspace_id)
        if row.get("operator_id")
    }
    recommendation_inputs = list_governance_routing_recommendation_inputs(conn, workspace_id)

    root_cause_code = case_row.get("root_cause_code")
    severity = case_row.get("severity")
    version_tuple = case_row.get("version_tuple")
    repeat_count = int(case_row.get("repeat_count") or 1)

    candidates: list[dict[str, Any]] = []
    for row in operator_rows:
        assigned_user = row.get("assigned_to")
        if not assigned_user:
            continue

        metric_row = operator_metrics.get(assigned_user)
        assigned_team = metric_row.get("assigned_team") if metric_row else None
        team_row = team_rows.get(assigned_team) if assigned_team else None
        matching_inputs = [
            input_row
            for input_row in recommendation_inputs
            if input_row.get("routing_target") in {assigned_user, assigned_team}
        ]

        root_cause_fit = 0.5
        version_or_regime_fit = 0.5
        if matching_inputs:
            if any(input_row.get("root_cause_code") == root_cause_code for input_row in matching_inputs):
                root_cause_fit = 0.95
            if any(
                input_row.get("compute_version") == (version_tuple.split("|")[0] if version_tuple else None)
                or input_row.get("signal_registry_version") == (version_tuple.split("|")[1] if version_tuple and len(version_tuple.split("|")) > 1 else None)
                or input_row.get("model_version") == (version_tuple.split("|")[2] if version_tuple and len(version_tuple.split("|")) > 2 else None)
                for input_row in matching_inputs
            ):
                version_or_regime_fit = 0.8

        operator_resolution_rate = float(row.get("resolution_rate") or 0)
        operator_drag = (
            float(row.get("reassignment_rate") or 0) * 0.5
            + float(row.get("escalation_rate") or 0) * 0.5
        )
        operator_effectiveness_score = max(min(operator_resolution_rate - operator_drag + 0.25, 1.0), 0.0)

        team_resolution_rate = float(team_row.get("resolution_rate") or 0) if team_row else 0.5
        team_drag = (
            float(team_row.get("reassignment_rate") or 0) * 0.5
            + float(team_row.get("escalation_rate") or 0) * 0.5
        ) if team_row else 0.0
        team_effectiveness_score = max(min(team_resolution_rate - team_drag + 0.2, 1.0), 0.0)

        open_cases = int(metric_row.get("open_case_count") or 0) if metric_row else 0
        severe_open = int(metric_row.get("severe_open_case_count") or 0) if metric_row else 0
        stale_open = int(metric_row.get("stale_open_case_count") or 0) if metric_row else 0
        pressure_penalty = min((open_cases / 12.0) + (severe_open / 8.0) + (stale_open / 6.0), 1.0)
        workload_inverse = round(1.0 - pressure_penalty, 6)

        recurrence_fit = 0.8 if repeat_count > 1 and operator_resolution_rate >= 0.5 else 0.55
        if severity == "critical" and severe_open > 0:
            workload_inverse = max(workload_inverse - 0.1, 0.0)

        candidates.append(
            {
                "assigned_user": assigned_user,
                "assigned_team": assigned_team,
                "operator_effectiveness_score": round(operator_effectiveness_score, 6),
                "team_effectiveness_score": round(team_effectiveness_score, 6),
                "workload_inverse": workload_inverse,
                "root_cause_fit": root_cause_fit,
                "recurrence_fit": recurrence_fit,
                "version_or_regime_fit": version_or_regime_fit,
                "candidate_reason": "operator_effectiveness_with_workload",
            }
        )

    if candidates:
        return candidates

    if case_row.get("current_team"):
        return [
            {
                "assigned_user": case_row.get("current_assignee"),
                "assigned_team": case_row.get("current_team"),
                "operator_effectiveness_score": 0.4,
                "team_effectiveness_score": 0.5,
                "workload_inverse": 0.5,
                "root_cause_fit": 0.5,
                "recurrence_fit": 0.5,
                "version_or_regime_fit": 0.5,
                "candidate_reason": "current_owner_fallback",
            }
        ]
    return []


def upsert_governance_routing_recommendation(
    conn,
    *,
    workspace_id: str,
    case_id: str,
    recommendation: dict[str, Any],
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_recommendations (
              workspace_id,
              case_id,
              recommendation_key,
              recommended_user,
              recommended_team,
              fallback_user,
              fallback_team,
              reason_code,
              confidence,
              score,
              supporting_metrics,
              model_inputs,
              alternatives
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb,
              %s::jsonb
            )
            on conflict (workspace_id, recommendation_key) do update
            set recommended_user = excluded.recommended_user,
                recommended_team = excluded.recommended_team,
                fallback_user = excluded.fallback_user,
                fallback_team = excluded.fallback_team,
                reason_code = excluded.reason_code,
                confidence = excluded.confidence,
                score = excluded.score,
                supporting_metrics = excluded.supporting_metrics,
                model_inputs = excluded.model_inputs,
                alternatives = excluded.alternatives,
                updated_at = now()
            returning *
            """,
            (
                workspace_id,
                case_id,
                recommendation["recommendation_key"],
                recommendation.get("recommended_user"),
                recommendation.get("recommended_team"),
                recommendation.get("fallback_user"),
                recommendation.get("fallback_team"),
                recommendation["reason_code"],
                recommendation["confidence"],
                recommendation["score"],
                Jsonb(_json_compatible(recommendation.get("supporting_metrics", {}))),
                Jsonb(_json_compatible(recommendation.get("model_inputs", {}))),
                Jsonb(_json_compatible(recommendation.get("alternatives", []))),
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("governance_routing_recommendations upsert returned no row")
        return dict(row)


def get_latest_governance_routing_recommendation(
    conn,
    *,
    case_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_recommendation_summary
            where case_id = %s::uuid
            order by created_at desc, updated_at desc
            limit 1
            """,
            (case_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def record_governance_routing_recommendation_feedback(
    conn,
    *,
    recommendation_id: str,
    accepted: bool,
    accepted_by: str | None = None,
    override_reason: str | None = None,
    applied: bool = False,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_routing_recommendations
            set accepted = %s,
                accepted_at = now(),
                accepted_by = %s,
                override_reason = %s,
                applied = %s,
                applied_at = case when %s then now() else applied_at end,
                updated_at = now()
            where id = %s::uuid
            returning *
            """,
            (
                accepted,
                accepted_by,
                override_reason,
                applied,
                applied,
                recommendation_id,
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("governance_routing_recommendations feedback update returned no row")
        return dict(row)


def list_recent_governance_routing_recommendations(
    conn,
    *,
    workspace_id: str,
    case_id: str | None = None,
    limit: int = 25,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_recommendation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or case_id = %s::uuid)
            order by created_at desc, updated_at desc
            limit %s
            """,
            (workspace_id, case_id, case_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def insert_governance_routing_recommendation_review(
    conn,
    *,
    workspace_id: str,
    recommendation_id: str,
    case_id: str | None,
    review_status: str,
    review_reason: str | None,
    notes: str | None,
    reviewed_by: str | None,
    applied_immediately: bool = False,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_recommendation_reviews (
              workspace_id,
              recommendation_id,
              case_id,
              review_status,
              review_reason,
              notes,
              reviewed_by,
              applied_immediately,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                recommendation_id,
                case_id,
                review_status,
                review_reason,
                notes,
                reviewed_by,
                applied_immediately,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("governance_routing_recommendation_reviews insert returned no row")
        return dict(row)


def get_latest_governance_routing_recommendation_review(
    conn,
    *,
    recommendation_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_recommendation_reviews
            where recommendation_id = %s::uuid
            order by reviewed_at desc
            limit 1
            """,
            (recommendation_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_governance_routing_application(
    conn,
    *,
    workspace_id: str,
    recommendation_id: str,
    review_id: str | None,
    case_id: str,
    previous_assigned_user: str | None,
    previous_assigned_team: str | None,
    applied_user: str | None,
    applied_team: str | None,
    application_mode: str,
    application_reason: str | None,
    applied_by: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_applications (
              workspace_id,
              recommendation_id,
              review_id,
              case_id,
              previous_assigned_user,
              previous_assigned_team,
              applied_user,
              applied_team,
              application_mode,
              application_reason,
              applied_by,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                recommendation_id,
                review_id,
                case_id,
                previous_assigned_user,
                previous_assigned_team,
                applied_user,
                applied_team,
                application_mode,
                application_reason,
                applied_by,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("governance_routing_applications insert returned no row")
        return dict(row)


def get_governance_routing_review_summary(
    conn,
    *,
    workspace_id: str,
    recommendation_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_review_summary
            where workspace_id = %s::uuid
              and recommendation_id = %s::uuid
            """,
            (workspace_id, recommendation_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_governance_routing_application_summary(
    conn,
    *,
    workspace_id: str,
    recommendation_id: str,
    case_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_application_summary
            where workspace_id = %s::uuid
              and recommendation_id = %s::uuid
              and case_id = %s::uuid
            """,
            (workspace_id, recommendation_id, case_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def upsert_governance_routing_autopromotion_policy(
    conn,
    *,
    workspace_id: str,
    scope_type: str,
    scope_value: str | None = None,
    enabled: bool = False,
    promotion_target: str = "override",
    min_confidence: str = "high",
    min_acceptance_rate: float = 0.80,
    min_sample_size: int = 5,
    max_recent_override_rate: float = 0.20,
    cooldown_hours: int = 24,
    created_by: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_routing_autopromotion_policies
            set enabled = %s,
                min_confidence = %s,
                min_acceptance_rate = %s,
                min_sample_size = %s,
                max_recent_override_rate = %s,
                cooldown_hours = %s,
                created_by = coalesce(%s, created_by),
                metadata = %s::jsonb,
                updated_at = now()
            where workspace_id = %s::uuid
              and scope_type = %s
              and (
                (%s::text is null and scope_value is null)
                or scope_value = %s
              )
              and promotion_target = %s
            returning *
            """,
            (
                enabled,
                min_confidence,
                min_acceptance_rate,
                min_sample_size,
                max_recent_override_rate,
                cooldown_hours,
                created_by,
                Jsonb(_json_compatible(metadata or {})),
                workspace_id,
                scope_type,
                scope_value,
                scope_value,
                promotion_target,
            ),
        )
        row = cur.fetchone()
        if row:
            return dict(row)

        cur.execute(
            """
            insert into public.governance_routing_autopromotion_policies (
              workspace_id,
              enabled,
              scope_type,
              scope_value,
              promotion_target,
              min_confidence,
              min_acceptance_rate,
              min_sample_size,
              max_recent_override_rate,
              cooldown_hours,
              created_by,
              metadata
            ) values (
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                enabled,
                scope_type,
                scope_value,
                promotion_target,
                min_confidence,
                min_acceptance_rate,
                min_sample_size,
                max_recent_override_rate,
                cooldown_hours,
                created_by,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_routing_autopromotion_policies upsert returned no row")
        return dict(row)


def get_governance_routing_autopromotion_policy(
    conn,
    *,
    workspace_id: str,
    recommendation: dict[str, Any],
) -> dict[str, Any] | None:
    model_inputs = recommendation.get("model_inputs") or {}
    recommended_team = recommendation.get("recommended_team")
    watchlist_id = model_inputs.get("watchlist_id")
    root_cause_code = model_inputs.get("root_cause_code")
    version_tuple = model_inputs.get("version_tuple")
    regime = model_inputs.get("regime")

    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_autopromotion_policies
            where workspace_id = %s::uuid
              and enabled = true
              and (
                scope_type = 'global'
                or (scope_type = 'team' and scope_value = %s)
                or (scope_type = 'watchlist' and scope_value = %s)
                or (scope_type = 'root_cause' and scope_value = %s)
                or (scope_type = 'version_tuple' and scope_value = %s)
                or (scope_type = 'regime' and scope_value = %s)
              )
            order by
              case
                when scope_type = 'watchlist' and scope_value = %s then 60
                when scope_type = 'root_cause' and scope_value = %s then 50
                when scope_type = 'version_tuple' and scope_value = %s then 40
                when scope_type = 'regime' and scope_value = %s then 30
                when scope_type = 'team' and scope_value = %s then 20
                when scope_type = 'global' then 10
                else 0
              end desc,
              updated_at desc,
              created_at desc
            limit 1
            """,
            (
                workspace_id,
                recommended_team,
                watchlist_id,
                root_cause_code,
                version_tuple,
                regime,
                watchlist_id,
                root_cause_code,
                version_tuple,
                regime,
                recommended_team,
            ),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_governance_routing_autopromotion_candidate(
    conn,
    *,
    recommendation_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              r.*,
              rv.latest_review_status as review_status,
              rv.latest_reviewed_at,
              rv.review_count,
              app.application_count,
              c.current_assignee as current_rule_user,
              c.current_team as current_rule_team,
              coalesce(
                q.acceptance_rate,
                t.resolution_rate,
                o.resolution_rate,
                0
              ) as acceptance_rate,
              greatest(
                coalesce(q.feedback_count, 0),
                coalesce(t.assignments, 0),
                coalesce(o.assignments, 0)
              ) as sample_size,
              coalesce(
                case
                  when coalesce(q.feedback_count, 0) > 0
                    then q.rerouted_count::numeric / nullif(q.feedback_count, 0)
                  else null
                end,
                t.reassignment_rate,
                o.reassignment_rate,
                1
              ) as override_rate
            from public.governance_routing_recommendation_summary r
            left join public.governance_routing_review_summary rv
              on rv.workspace_id = r.workspace_id
             and rv.recommendation_id = r.id
            left join public.governance_routing_application_summary app
              on app.workspace_id = r.workspace_id
             and app.recommendation_id = r.id
             and app.case_id = r.case_id
            left join public.governance_operator_effectiveness_summary o
              on o.workspace_id = r.workspace_id
             and o.assigned_to = r.recommended_user
            left join public.governance_team_effectiveness_summary t
              on t.workspace_id = r.workspace_id
             and t.assigned_team = r.recommended_team
            left join public.governance_routing_quality_summary q
              on q.workspace_id = r.workspace_id
             and q.assigned_team = r.recommended_team
             and coalesce(q.root_cause_code, '') = coalesce(r.model_inputs ->> 'root_cause_code', '')
            left join public.governance_cases c
              on c.id = r.case_id
            where r.id = %s::uuid
            limit 1
            """,
            (recommendation_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_recent_governance_routing_autopromotion_execution(
    conn,
    *,
    workspace_id: str,
    target_type: str,
    target_key: str,
    since_hours: int,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_autopromotion_executions
            where workspace_id = %s::uuid
              and target_type = %s
              and target_key = %s
              and created_at >= now() - (%s::text || ' hours')::interval
            order by created_at desc
            limit 1
            """,
            (workspace_id, target_type, target_key, since_hours),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def _routing_autopromotion_scope_fields(
    policy: dict[str, Any],
    recommendation: dict[str, Any],
) -> dict[str, Any]:
    model_inputs = recommendation.get("model_inputs") or {}
    scope_type = str(policy.get("scope_type") or "global")
    return {
        "scope_type": scope_type,
        "scope_value": policy.get("scope_value"),
        "watchlist_id": model_inputs.get("watchlist_id") if scope_type == "watchlist" else None,
        "root_cause_code": model_inputs.get("root_cause_code") if scope_type == "root_cause" else None,
        "version_tuple": model_inputs.get("version_tuple") if scope_type == "version_tuple" else None,
        "regime": model_inputs.get("regime") if scope_type == "regime" else None,
        "severity": model_inputs.get("severity") if scope_type == "team" else None,
    }


def apply_governance_routing_autopromotion(
    conn,
    *,
    workspace_id: str,
    policy: dict[str, Any],
    recommendation: dict[str, Any],
    target_type: str,
    target_key: str,
    executed_by: str,
) -> dict[str, Any]:
    filters = _routing_autopromotion_scope_fields(policy, recommendation)
    recommended_user = recommendation.get("recommended_user")
    recommended_team = recommendation.get("recommended_team")
    root_cause_code = filters["root_cause_code"]
    watchlist_id = filters["watchlist_id"]
    version_tuple = filters["version_tuple"]
    regime = filters["regime"]
    severity = filters["severity"]

    with conn.cursor() as cur:
        if target_type == "override":
            cur.execute(
                """
                select *
                from public.governance_routing_overrides
                where workspace_id = %s::uuid
                  and case_id is null
                  and (
                    (%s::uuid is null and watchlist_id is null)
                    or watchlist_id = %s::uuid
                  )
                  and (
                    (%s::text is null and root_cause_code is null)
                    or root_cause_code = %s
                  )
                  and (
                    (%s::text is null and severity is null)
                    or severity = %s
                  )
                  and (
                    (%s::text is null and version_tuple is null)
                    or version_tuple = %s
                  )
                  and (
                    (%s::text is null and regime is null)
                    or regime = %s
                  )
                order by created_at desc
                limit 1
                """,
                (
                    workspace_id,
                    watchlist_id,
                    watchlist_id,
                    root_cause_code,
                    root_cause_code,
                    severity,
                    severity,
                    version_tuple,
                    version_tuple,
                    regime,
                    regime,
                ),
            )
            existing = cur.fetchone()
            prior_state = dict(existing) if existing else {}
            metadata = {
                **(existing.get("metadata") if existing else {}),
                "source": "routing_autopromotion",
                "autopromotion_target_key": target_key,
                "scope_type": filters["scope_type"],
                "executed_by": executed_by,
                "recommendation_id": str(recommendation["id"]),
            }
            if existing:
                cur.execute(
                    """
                    update public.governance_routing_overrides
                    set assigned_team = %s,
                        assigned_user = %s,
                        reason = %s,
                        is_enabled = true,
                        regime = %s,
                        metadata = %s::jsonb
                    where id = %s::uuid
                    returning *
                    """,
                    (
                        recommended_team,
                        recommended_user,
                        f"routing_autopromotion:{target_key}",
                        regime,
                        Jsonb(_json_compatible(metadata)),
                        existing["id"],
                    ),
                )
            else:
                cur.execute(
                    """
                    insert into public.governance_routing_overrides (
                      workspace_id,
                      case_id,
                      watchlist_id,
                      root_cause_code,
                      severity,
                      version_tuple,
                      regime,
                      assigned_team,
                      assigned_user,
                      reason,
                      is_enabled,
                      metadata
                    ) values (
                      %s::uuid,
                      null,
                      %s::uuid,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s,
                      true,
                      %s::jsonb
                    )
                    returning *
                    """,
                    (
                        workspace_id,
                        watchlist_id,
                        root_cause_code,
                        severity,
                        version_tuple,
                        regime,
                        recommended_team,
                        recommended_user,
                        f"routing_autopromotion:{target_key}",
                        Jsonb(_json_compatible(metadata)),
                    ),
                )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("routing override autopromotion returned no row")
            target_row = dict(row)
        else:
            cur.execute(
                """
                select *
                from public.governance_routing_rules
                where workspace_id = %s::uuid
                  and (
                    (%s::uuid is null and watchlist_id is null)
                    or watchlist_id = %s::uuid
                  )
                  and (
                    (%s::text is null and root_cause_code is null)
                    or root_cause_code = %s
                  )
                  and (
                    (%s::text is null and severity is null)
                    or severity = %s
                  )
                  and (
                    (%s::text is null and version_tuple is null)
                    or version_tuple = %s
                  )
                  and (
                    (%s::text is null and regime is null)
                    or regime = %s
                  )
                order by priority asc, created_at desc
                limit 1
                """,
                (
                    workspace_id,
                    watchlist_id,
                    watchlist_id,
                    root_cause_code,
                    root_cause_code,
                    severity,
                    severity,
                    version_tuple,
                    version_tuple,
                    regime,
                    regime,
                ),
            )
            existing = cur.fetchone()
            prior_state = dict(existing) if existing else {}
            metadata = {
                **(existing.get("metadata") if existing else {}),
                "source": "routing_autopromotion",
                "autopromotion_target_key": target_key,
                "scope_type": filters["scope_type"],
                "executed_by": executed_by,
                "recommendation_id": str(recommendation["id"]),
            }
            if existing:
                cur.execute(
                    """
                    update public.governance_routing_rules
                    set is_enabled = true,
                        assign_team = %s,
                        assign_user = %s,
                        fallback_team = coalesce(%s, fallback_team),
                        routing_reason_template = %s,
                        regime = %s,
                        metadata = %s::jsonb,
                        updated_at = now()
                    where id = %s::uuid
                    returning *
                    """,
                    (
                        recommended_team,
                        recommended_user,
                        recommended_team,
                        f"routing_autopromotion:{target_key}",
                        regime,
                        Jsonb(_json_compatible(metadata)),
                        existing["id"],
                    ),
                )
            else:
                cur.execute(
                    """
                    insert into public.governance_routing_rules (
                      workspace_id,
                      is_enabled,
                      priority,
                      root_cause_code,
                      severity,
                      watchlist_id,
                      version_tuple,
                      regime,
                      recurrence_min,
                      chronic_only,
                      assign_team,
                      assign_user,
                      fallback_team,
                      routing_reason_template,
                      metadata
                    ) values (
                      %s::uuid,
                      true,
                      %s,
                      %s,
                      %s,
                      %s::uuid,
                      %s,
                      %s,
                      null,
                      false,
                      %s,
                      %s,
                      %s,
                      %s,
                      %s::jsonb
                    )
                    returning *
                    """,
                    (
                        workspace_id,
                        5,
                        root_cause_code,
                        severity,
                        watchlist_id,
                        version_tuple,
                        regime,
                        recommended_team,
                        recommended_user,
                        recommended_team,
                        f"routing_autopromotion:{target_key}",
                        Jsonb(_json_compatible(metadata)),
                    ),
                )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("routing rule autopromotion returned no row")
            target_row = dict(row)

    return {
        "target_row": target_row,
        "prior_state": prior_state,
        "new_state": target_row,
    }


def create_governance_routing_autopromotion_execution(
    conn,
    *,
    workspace_id: str,
    policy_id: str,
    recommendation_id: str,
    target_type: str,
    target_key: str,
    recommended_user: str | None,
    recommended_team: str | None,
    confidence: str,
    acceptance_rate: float | None,
    sample_size: int | None,
    override_rate: float | None,
    execution_status: str,
    execution_reason: str | None,
    cooldown_bucket: str | None,
    prior_state: dict[str, Any],
    new_state: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_autopromotion_executions (
              workspace_id,
              policy_id,
              recommendation_id,
              target_type,
              target_key,
              recommended_user,
              recommended_team,
              confidence,
              acceptance_rate,
              sample_size,
              override_rate,
              execution_status,
              execution_reason,
              cooldown_bucket,
              prior_state,
              new_state,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                policy_id,
                recommendation_id,
                target_type,
                target_key,
                recommended_user,
                recommended_team,
                confidence,
                acceptance_rate,
                sample_size,
                override_rate,
                execution_status,
                execution_reason,
                cooldown_bucket,
                Jsonb(_json_compatible(prior_state)),
                Jsonb(_json_compatible(new_state)),
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_routing_autopromotion_executions insert returned no row")
        return dict(row)


def create_governance_routing_autopromotion_rollback_candidate(
    conn,
    *,
    workspace_id: str,
    execution_id: str,
    target_type: str,
    target_key: str,
    prior_state: dict[str, Any],
    rollback_reason: str | None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_autopromotion_rollback_candidates (
              workspace_id,
              execution_id,
              target_type,
              target_key,
              prior_state,
              rollback_reason
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s::jsonb,
              %s
            )
            returning *
            """,
            (
                workspace_id,
                execution_id,
                target_type,
                target_key,
                Jsonb(_json_compatible(prior_state)),
                rollback_reason,
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_routing_autopromotion_rollback_candidates insert returned no row")
        return dict(row)


def list_governance_routing_autopromotion_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_autopromotion_summary
            where workspace_id = %s::uuid
            order by created_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_operator_case_metrics(
    conn,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_operator_case_metrics
            where workspace_id = %s::uuid
            order by severe_open_case_count desc, open_case_count desc, operator_id asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_team_case_metrics(
    conn,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_team_case_metrics
            where workspace_id = %s::uuid
            order by severe_open_case_count desc, open_case_count desc, assigned_team asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def ensure_default_governance_sla_policies(
    conn,
    *,
    workspace_id: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_sla_policies (
              workspace_id,
              severity,
              chronicity_class,
              ack_within_minutes,
              resolve_within_minutes,
              enabled,
              metadata
            )
            select *
            from (
              values
                (%s::uuid, 'critical', null::text, 15, 240, true, '{"seeded_by":"worker_default"}'::jsonb),
                (%s::uuid, 'high', null::text, 30, 480, true, '{"seeded_by":"worker_default"}'::jsonb),
                (%s::uuid, 'medium', null::text, 60, 1440, true, '{"seeded_by":"worker_default"}'::jsonb),
                (%s::uuid, 'low', null::text, 240, 4320, true, '{"seeded_by":"worker_default"}'::jsonb)
            ) as defaults(workspace_id, severity, chronicity_class, ack_within_minutes, resolve_within_minutes, enabled, metadata)
            where not exists (
              select 1
              from public.governance_sla_policies p
              where p.workspace_id = defaults.workspace_id
                and p.severity = defaults.severity
                and p.chronicity_class is not distinct from defaults.chronicity_class
            )
            on conflict do nothing
            """,
            (workspace_id, workspace_id, workspace_id, workspace_id),
        )


def list_governance_sla_policies(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_sla_policies
            where workspace_id = %s::uuid
              and enabled = true
            order by
              severity asc,
              case
                when chronicity_class = 'chronic' then 0
                when chronicity_class = 'recurring' then 1
                else 2
              end asc,
              created_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def ensure_default_governance_escalation_policies(
    conn,
    *,
    workspace_id: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_escalation_policies (
              workspace_id,
              severity,
              chronicity_class,
              root_cause_code,
              min_case_age_minutes,
              min_ack_age_minutes,
              min_repeat_count,
              min_operator_pressure,
              escalation_level,
              escalate_to_team,
              escalate_to_user,
              cooldown_minutes,
              is_enabled,
              metadata
            )
            select *
            from (
              values
                (%s::uuid, 'critical', null::text, null::text, 30, 15, 1, null::numeric, 'lead_review', 'platform', null::text, 240, true, '{"seeded_by":"worker_default"}'::jsonb),
                (%s::uuid, 'high', 'recurring', 'version_regression', 60, 30, 2, null::numeric, 'senior_review', 'research', null::text, 240, true, '{"seeded_by":"worker_default"}'::jsonb),
                (%s::uuid, 'high', null::text, 'provider_failure', 45, 20, 1, null::numeric, 'platform_lead', 'platform', null::text, 180, true, '{"seeded_by":"worker_default"}'::jsonb),
                (%s::uuid, 'high', null::text, null::text, 120, 60, 1, 10::numeric, 'load_shed', 'triage', null::text, 240, true, '{"seeded_by":"worker_default"}'::jsonb)
            ) as defaults(
              workspace_id,
              severity,
              chronicity_class,
              root_cause_code,
              min_case_age_minutes,
              min_ack_age_minutes,
              min_repeat_count,
              min_operator_pressure,
              escalation_level,
              escalate_to_team,
              escalate_to_user,
              cooldown_minutes,
              is_enabled,
              metadata
            )
            where not exists (
              select 1
              from public.governance_escalation_policies p
              where p.workspace_id = defaults.workspace_id
                and p.severity is not distinct from defaults.severity
                and p.chronicity_class is not distinct from defaults.chronicity_class
                and p.root_cause_code is not distinct from defaults.root_cause_code
                and p.escalation_level = defaults.escalation_level
            )
            on conflict do nothing
            """,
            (workspace_id, workspace_id, workspace_id, workspace_id),
        )


def list_governance_escalation_policies(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_escalation_policies
            where workspace_id = %s::uuid
              and is_enabled = true
            order by
              case when root_cause_code is null then 1 else 0 end asc,
              case
                when chronicity_class = 'chronic' then 0
                when chronicity_class = 'recurring' then 1
                when chronicity_class is null then 2
                else 3
              end asc,
              case when severity is null then 1 else 0 end asc,
              coalesce(min_repeat_count, 0) desc,
              coalesce(min_operator_pressure, 0) desc,
              coalesce(min_ack_age_minutes, 0) desc,
              coalesce(min_case_age_minutes, 0) desc,
              created_at asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def upsert_governance_sla_evaluation(
    conn,
    *,
    workspace_id: str,
    case_id: str,
    policy_id: str | None,
    chronicity_class: str | None,
    ack_due_at,
    resolve_due_at,
    ack_breached: bool,
    resolve_breached: bool,
    breach_severity: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_sla_evaluations (
              workspace_id,
              case_id,
              policy_id,
              chronicity_class,
              ack_due_at,
              resolve_due_at,
              ack_breached,
              resolve_breached,
              breach_severity,
              metadata,
              evaluated_at
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb,
              now()
            )
            on conflict (case_id) do update
            set policy_id = excluded.policy_id,
                chronicity_class = excluded.chronicity_class,
                ack_due_at = excluded.ack_due_at,
                resolve_due_at = excluded.resolve_due_at,
                ack_breached = excluded.ack_breached,
                resolve_breached = excluded.resolve_breached,
                breach_severity = excluded.breach_severity,
                metadata = excluded.metadata,
                evaluated_at = excluded.evaluated_at
            returning *
            """,
            (
                workspace_id,
                case_id,
                policy_id,
                chronicity_class,
                ack_due_at,
                resolve_due_at,
                ack_breached,
                resolve_breached,
                breach_severity,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_sla_evaluations upsert returned no row")
        return dict(row)


def get_governance_case_sla_summary_row(
    conn,
    *,
    case_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_case_sla_summary
            where case_id = %s::uuid
            """,
            (case_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def list_operator_workload_pressure(
    conn,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_operator_workload_pressure
            where workspace_id = %s::uuid
            order by severity_weighted_load desc nulls last, open_case_count desc, assigned_to asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_team_workload_pressure(
    conn,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_team_workload_pressure
            where workspace_id = %s::uuid
            order by severity_weighted_load desc nulls last, open_case_count desc, assigned_team asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_stale_case_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_stale_case_summary
            where workspace_id = %s::uuid
            order by age_minutes desc nulls last, evaluated_at desc nulls last, case_id asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def upsert_governance_incident_analytics_snapshot(
    conn,
    *,
    payload: dict[str, Any],
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_incident_analytics_snapshots (
              workspace_id,
              snapshot_date,
              open_case_count,
              acknowledged_case_count,
              resolved_case_count,
              reopened_case_count,
              recurring_case_count,
              escalated_case_count,
              high_severity_open_count,
              stale_case_count,
              mean_ack_hours,
              mean_resolve_hours
            ) values (
              %s::uuid,
              %s::date,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s
            )
            on conflict (workspace_id, snapshot_date) do update
            set open_case_count = excluded.open_case_count,
                acknowledged_case_count = excluded.acknowledged_case_count,
                resolved_case_count = excluded.resolved_case_count,
                reopened_case_count = excluded.reopened_case_count,
                recurring_case_count = excluded.recurring_case_count,
                escalated_case_count = excluded.escalated_case_count,
                high_severity_open_count = excluded.high_severity_open_count,
                stale_case_count = excluded.stale_case_count,
                mean_ack_hours = excluded.mean_ack_hours,
                mean_resolve_hours = excluded.mean_resolve_hours
            returning *
            """,
            (
                payload["workspace_id"],
                payload["snapshot_date"],
                int(payload.get("open_case_count") or 0),
                int(payload.get("acknowledged_case_count") or 0),
                int(payload.get("resolved_case_count") or 0),
                int(payload.get("reopened_case_count") or 0),
                int(payload.get("recurring_case_count") or 0),
                int(payload.get("escalated_case_count") or 0),
                int(payload.get("high_severity_open_count") or 0),
                int(payload.get("stale_case_count") or 0),
                payload.get("mean_ack_hours"),
                payload.get("mean_resolve_hours"),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_incident_analytics_snapshots upsert returned no row")
        return dict(row)


def get_governance_incident_analytics_summary(
    conn,
    *,
    workspace_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_incident_analytics_summary
            where workspace_id = %s::uuid
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def list_governance_root_cause_trend_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_root_cause_trend_summary
            where workspace_id = %s::uuid
            order by case_count desc, severe_count desc, root_cause_code asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_recurrence_burden_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_recurrence_burden_summary
            where workspace_id = %s::uuid
            order by recurring_case_count desc, max_repeat_count desc nulls last
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_governance_escalation_effectiveness_summary(
    conn,
    *,
    workspace_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_escalation_effectiveness_summary
            where workspace_id = %s::uuid
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def list_governance_incident_analytics_snapshots(
    conn,
    *,
    workspace_id: str,
    limit: int = 30,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_incident_analytics_snapshots
            where workspace_id = %s::uuid
            order by snapshot_date desc, created_at desc
            limit %s
            """,
            (workspace_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_operator_performance_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_operator_performance_summary
            where workspace_id = %s::uuid
            order by resolution_quality_proxy desc nulls last, assigned_case_count desc, operator_name asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_team_performance_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_team_performance_summary
            where workspace_id = %s::uuid
            order by resolution_quality_proxy desc nulls last, assigned_case_count desc, assigned_team asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_operator_case_mix_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_operator_case_mix_summary
            where workspace_id = %s::uuid
            order by case_count desc, chronic_case_count desc, actor_name asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_team_case_mix_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_team_case_mix_summary
            where workspace_id = %s::uuid
            order by case_count desc, chronic_case_count desc, actor_name asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def count_governance_operator_performance_rows(
    conn,
    *,
    workspace_id: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            select count(*)::int as row_count
            from public.governance_operator_performance_summary
            where workspace_id = %s::uuid
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return int(row["row_count"]) if row and row["row_count"] is not None else 0


def count_governance_team_performance_rows(
    conn,
    *,
    workspace_id: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            select count(*)::int as row_count
            from public.governance_team_performance_summary
            where workspace_id = %s::uuid
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return int(row["row_count"]) if row and row["row_count"] is not None else 0


def count_governance_operator_case_mix_rows(
    conn,
    *,
    workspace_id: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            select count(*)::int as row_count
            from public.governance_operator_case_mix_summary
            where workspace_id = %s::uuid
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return int(row["row_count"]) if row and row["row_count"] is not None else 0


def count_governance_team_case_mix_rows(
    conn,
    *,
    workspace_id: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            select count(*)::int as row_count
            from public.governance_team_case_mix_summary
            where workspace_id = %s::uuid
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return int(row["row_count"]) if row and row["row_count"] is not None else 0


def insert_governance_performance_snapshot(
    conn,
    *,
    workspace_id: str,
    operator_count: int,
    team_count: int,
    operator_case_mix_count: int,
    team_case_mix_count: int,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_performance_snapshots (
              workspace_id,
              operator_count,
              team_count,
              operator_case_mix_count,
              team_case_mix_count,
              metadata
            ) values (
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                operator_count,
                team_count,
                operator_case_mix_count,
                team_case_mix_count,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_performance_snapshots insert returned no row")
        return dict(row)


def list_governance_performance_snapshots(
    conn,
    *,
    workspace_id: str,
    limit: int = 30,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_performance_snapshots
            where workspace_id = %s::uuid
            order by snapshot_at desc
            limit %s
            """,
            (workspace_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def list_recent_promotion_executions(
    conn,
    *,
    workspace_id: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with unified as (
              select
                workspace_id,
                'threshold'::text as promotion_type,
                execution_id,
                dimension_type as scope_type,
                dimension_value as scope_value,
                created_at as executed_at,
                jsonb_build_object(
                  'event_type', event_type,
                  'dimension_type', dimension_type,
                  'dimension_value', dimension_value
                ) as metadata
              from public.governance_threshold_autopromotion_summary
              where workspace_id = %s::uuid
              union all
              select
                workspace_id,
                'routing'::text as promotion_type,
                execution_id,
                scope_type,
                scope_value,
                created_at as executed_at,
                jsonb_build_object(
                  'target_type', target_type,
                  'target_key', target_key,
                  'promotion_target', promotion_target
                ) as metadata
              from public.governance_routing_autopromotion_summary
              where workspace_id = %s::uuid
            )
            select *
            from unified
            order by executed_at desc
            limit %s
            """,
            (workspace_id, workspace_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_threshold_promotion_window_metrics(
    conn,
    *,
    workspace_id: str,
    scope_type: str,
    scope_value: str | None,
    execution_metadata: dict[str, Any] | None,
    pre_window_start: Any,
    pre_window_end: Any,
    post_window_start: Any,
    post_window_end: Any,
) -> dict[str, Any]:
    event_type = None
    if execution_metadata:
        event_type = execution_metadata.get("event_type")

    def _threshold_window(window_start: Any, window_end: Any) -> dict[str, Any]:
        params: list[Any] = [workspace_id, window_start, window_end, event_type]
        filters = ["workspace_id = %s::uuid", "created_at >= %s::timestamptz", "created_at < %s::timestamptz"]
        if event_type is not None:
            filters.append("event_type = %s")
        else:
            filters.append("(%s is null or event_type = %s)")
            params.append(event_type)

        if scope_type == "regime":
            filters.append("coalesce(regime, 'any') = %s")
            params.append(scope_value or "any")
        elif scope_type == "event_type" and scope_value is not None:
            filters.append("event_type = %s")
            params.append(scope_value)

        where_clause = " and ".join(filters)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select
                  coalesce(sum(trigger_count), 0)::float as trigger_count,
                  coalesce(sum(reopen_count), 0)::float as reopen_count,
                  coalesce(sum(escalation_count), 0)::float as escalation_count,
                  coalesce(sum(mute_count), 0)::float as mute_count,
                  count(*)::int as feedback_rows
                from public.governance_threshold_feedback
                where {where_clause}
                """,
                tuple(params),
            )
            feedback = dict(cur.fetchone() or {})
            cur.execute(
                """
                select
                  avg(mean_ack_hours)::float as mean_ack_hours,
                  avg(mean_resolve_hours)::float as mean_resolve_hours,
                  count(*)::int as snapshot_rows
                from public.governance_incident_analytics_snapshots
                where workspace_id = %s::uuid
                  and snapshot_date >= (%s::timestamptz)::date
                  and snapshot_date < (%s::timestamptz)::date
                """,
                (workspace_id, window_start, window_end),
            )
            analytics = dict(cur.fetchone() or {})

        trigger_count = float(feedback.get("trigger_count") or 0.0)
        return {
          "recurrence_rate": (float(feedback.get("reopen_count") or 0.0) / trigger_count) if trigger_count > 0 else None,
          "escalation_rate": (float(feedback.get("escalation_count") or 0.0) / trigger_count) if trigger_count > 0 else None,
          "resolution_latency_ms": (
            float(analytics.get("mean_resolve_hours")) * 3600000.0
            if analytics.get("mean_resolve_hours") is not None
            else None
          ),
          "reassignment_rate": None,
          "feedback_rows": int(feedback.get("feedback_rows") or 0),
          "snapshot_rows": int(analytics.get("snapshot_rows") or 0),
          "mute_rate": (float(feedback.get("mute_count") or 0.0) / trigger_count) if trigger_count > 0 else None,
        }

    pre = _threshold_window(pre_window_start, pre_window_end)
    post = _threshold_window(post_window_start, post_window_end)
    return {
        "recurrence_rate_before": pre["recurrence_rate"],
        "recurrence_rate_after": post["recurrence_rate"],
        "escalation_rate_before": pre["escalation_rate"],
        "escalation_rate_after": post["escalation_rate"],
        "resolution_latency_before_ms": pre["resolution_latency_ms"],
        "resolution_latency_after_ms": post["resolution_latency_ms"],
        "reassignment_rate_before": None,
        "reassignment_rate_after": None,
        "pre_feedback_rows": pre["feedback_rows"],
        "post_feedback_rows": post["feedback_rows"],
        "pre_snapshot_rows": pre["snapshot_rows"],
        "post_snapshot_rows": post["snapshot_rows"],
        "pre_mute_rate": pre["mute_rate"],
        "post_mute_rate": post["mute_rate"],
    }


def get_routing_promotion_window_metrics(
    conn,
    *,
    workspace_id: str,
    scope_type: str,
    scope_value: str | None,
    execution_metadata: dict[str, Any] | None,
    pre_window_start: Any,
    pre_window_end: Any,
    post_window_start: Any,
    post_window_end: Any,
) -> dict[str, Any]:
    def _routing_window(window_start: Any, window_end: Any) -> dict[str, Any]:
        params: list[Any] = [workspace_id, window_start, window_end]
        scope_filter = ""

        if scope_type == "team" and scope_value is not None:
            scope_filter = "and gro.assigned_team = %s"
            params.append(scope_value)
        elif scope_type == "root_cause" and scope_value is not None:
            scope_filter = "and gro.root_cause_code = %s"
            params.append(scope_value)
        elif scope_type == "watchlist" and scope_value is not None:
            scope_filter = "and gro.watchlist_id::text = %s"
            params.append(scope_value)
        elif scope_type == "version_tuple" and scope_value is not None:
            scope_filter = """
              and concat_ws('|',
                coalesce(gro.compute_version, ''),
                coalesce(gro.signal_registry_version, ''),
                coalesce(gro.model_version, '')
              ) = %s
            """
            params.append(scope_value)
        elif scope_type == "regime" and scope_value is not None:
            scope_filter = "and coalesce(gds.regime, 'unknown') = %s"
            params.append(scope_value)

        with conn.cursor() as cur:
            cur.execute(
                f"""
                select
                  count(*) filter (where gro.outcome_type = 'assigned')::float as assigned_count,
                  count(*) filter (where gro.outcome_type = 'reopened')::float as reopened_count,
                  count(*) filter (where gro.outcome_type = 'escalated')::float as escalated_count,
                  count(*) filter (where gro.outcome_type = 'reassigned')::float as reassigned_count,
                  avg(gro.outcome_value) filter (where gro.outcome_type = 'time_to_resolve_hours')::float as resolve_hours,
                  count(*) filter (where gro.outcome_type = 'time_to_resolve_hours')::int as resolved_samples
                from public.governance_routing_outcomes gro
                left join public.governance_cases gc
                  on gc.id = gro.case_id
                left join public.governance_degradation_states gds
                  on gds.id = gc.degradation_state_id
                where gro.workspace_id = %s::uuid
                  and gro.occurred_at >= %s::timestamptz
                  and gro.occurred_at < %s::timestamptz
                  {scope_filter}
                """,
                tuple(params),
            )
            row = dict(cur.fetchone() or {})

        assigned_count = float(row.get("assigned_count") or 0.0)
        return {
            "recurrence_rate": (float(row.get("reopened_count") or 0.0) / assigned_count) if assigned_count > 0 else None,
            "escalation_rate": (float(row.get("escalated_count") or 0.0) / assigned_count) if assigned_count > 0 else None,
            "reassignment_rate": (float(row.get("reassigned_count") or 0.0) / assigned_count) if assigned_count > 0 else None,
            "resolution_latency_ms": (
                float(row.get("resolve_hours")) * 3600000.0
                if row.get("resolve_hours") is not None
                else None
            ),
            "assigned_count": assigned_count,
            "resolved_samples": int(row.get("resolved_samples") or 0),
        }

    pre = _routing_window(pre_window_start, pre_window_end)
    post = _routing_window(post_window_start, post_window_end)
    return {
        "recurrence_rate_before": pre["recurrence_rate"],
        "recurrence_rate_after": post["recurrence_rate"],
        "escalation_rate_before": pre["escalation_rate"],
        "escalation_rate_after": post["escalation_rate"],
        "resolution_latency_before_ms": pre["resolution_latency_ms"],
        "resolution_latency_after_ms": post["resolution_latency_ms"],
        "reassignment_rate_before": pre["reassignment_rate"],
        "reassignment_rate_after": post["reassignment_rate"],
        "pre_assigned_count": pre["assigned_count"],
        "post_assigned_count": post["assigned_count"],
        "pre_resolved_samples": pre["resolved_samples"],
        "post_resolved_samples": post["resolved_samples"],
    }


def upsert_promotion_impact_snapshot(
    conn,
    *,
    payload: dict[str, Any],
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_promotion_impact_snapshots (
              workspace_id,
              promotion_type,
              execution_id,
              scope_type,
              scope_value,
              impact_classification,
              pre_window_start,
              pre_window_end,
              post_window_start,
              post_window_end,
              recurrence_rate_before,
              recurrence_rate_after,
              escalation_rate_before,
              escalation_rate_after,
              resolution_latency_before_ms,
              resolution_latency_after_ms,
              reassignment_rate_before,
              reassignment_rate_after,
              rollback_risk_score,
              supporting_metrics,
              updated_at
            ) values (
              %s::uuid,
              %s,
              %s::uuid,
              %s,
              %s,
              %s,
              %s::timestamptz,
              %s::timestamptz,
              %s::timestamptz,
              %s::timestamptz,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb,
              now()
            )
            on conflict (workspace_id, promotion_type, execution_id)
            do update set
              scope_type = excluded.scope_type,
              scope_value = excluded.scope_value,
              impact_classification = excluded.impact_classification,
              pre_window_start = excluded.pre_window_start,
              pre_window_end = excluded.pre_window_end,
              post_window_start = excluded.post_window_start,
              post_window_end = excluded.post_window_end,
              recurrence_rate_before = excluded.recurrence_rate_before,
              recurrence_rate_after = excluded.recurrence_rate_after,
              escalation_rate_before = excluded.escalation_rate_before,
              escalation_rate_after = excluded.escalation_rate_after,
              resolution_latency_before_ms = excluded.resolution_latency_before_ms,
              resolution_latency_after_ms = excluded.resolution_latency_after_ms,
              reassignment_rate_before = excluded.reassignment_rate_before,
              reassignment_rate_after = excluded.reassignment_rate_after,
              rollback_risk_score = excluded.rollback_risk_score,
              supporting_metrics = excluded.supporting_metrics,
              updated_at = now()
            returning *
            """,
            (
                payload["workspace_id"],
                payload["promotion_type"],
                payload["execution_id"],
                payload["scope_type"],
                payload.get("scope_value"),
                payload["impact_classification"],
                payload["pre_window_start"],
                payload["pre_window_end"],
                payload["post_window_start"],
                payload["post_window_end"],
                payload.get("recurrence_rate_before"),
                payload.get("recurrence_rate_after"),
                payload.get("escalation_rate_before"),
                payload.get("escalation_rate_after"),
                payload.get("resolution_latency_before_ms"),
                payload.get("resolution_latency_after_ms"),
                payload.get("reassignment_rate_before"),
                payload.get("reassignment_rate_after"),
                payload.get("rollback_risk_score"),
                Jsonb(_json_compatible(payload.get("supporting_metrics") or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_promotion_impact_snapshots upsert returned no row")
        return dict(row)


def list_threshold_promotion_impact_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_threshold_promotion_impact_summary
            where workspace_id = %s::uuid
            order by created_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_routing_promotion_impact_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_promotion_impact_summary
            where workspace_id = %s::uuid
            order by created_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_promotion_rollback_risk_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_promotion_rollback_risk_summary
            where workspace_id = %s::uuid
            order by rollback_risk_score desc nulls last, created_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def insert_governance_manager_analytics_snapshot(
    conn,
    *,
    workspace_id: str,
    window_days: int,
    open_case_count: int,
    recurring_case_count: int,
    escalated_case_count: int,
    chronic_watchlist_count: int,
    degraded_promotion_count: int,
    rollback_risk_count: int,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_manager_analytics_snapshots (
              workspace_id,
              window_days,
              open_case_count,
              recurring_case_count,
              escalated_case_count,
              chronic_watchlist_count,
              degraded_promotion_count,
              rollback_risk_count,
              metadata
            ) values (
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                int(window_days),
                int(open_case_count),
                int(recurring_case_count),
                int(escalated_case_count),
                int(chronic_watchlist_count),
                int(degraded_promotion_count),
                int(rollback_risk_count),
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_manager_analytics_snapshots insert returned no row")
        return dict(row)


def list_governance_manager_overview_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_manager_overview_summary
            where workspace_id = %s::uuid
            order by snapshot_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_chronic_watchlist_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_chronic_watchlist_summary
            where workspace_id = %s::uuid
            order by recurring_case_count desc, reopened_case_count desc, latest_case_at desc nulls last
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_operator_team_comparison_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_operator_team_comparison_summary
            where workspace_id = %s::uuid
            order by resolution_quality_proxy desc, severity_weighted_load desc nulls last, assigned_case_count desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_promotion_health_overview(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_promotion_health_overview
            where workspace_id = %s::uuid
            order by latest_created_at desc nulls last, promotion_type asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_operating_risk_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_operating_risk_summary
            where workspace_id = %s::uuid
            order by snapshot_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_review_priority_summary(
    conn,
    *,
    workspace_id: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_review_priority_summary
            where workspace_id = %s::uuid
            order by priority_rank asc, priority_score desc, entity_type asc, entity_key asc
            limit %s
            """,
            (workspace_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def get_trend_window_summary(
    conn,
    *,
    workspace_id: str,
    window_labels: list[str] | None = None,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        if window_labels:
            cur.execute(
                """
                select *
                from public.governance_trend_window_summary
                where workspace_id = %s::uuid
                  and window_label = any(%s::text[])
                order by
                  case window_label
                    when '7d' then 1
                    when '30d' then 2
                    when '90d' then 3
                    else 4
                  end,
                  metric_name asc
                """,
                (workspace_id, window_labels),
            )
        else:
            cur.execute(
                """
                select *
                from public.governance_trend_window_summary
                where workspace_id = %s::uuid
                order by
                  case window_label
                    when '7d' then 1
                    when '30d' then 2
                    when '90d' then 3
                    else 4
                  end,
                  metric_name asc
                """,
                (workspace_id,),
            )
        return [dict(row) for row in cur.fetchall()]


def get_governance_escalation_state(
    conn,
    *,
    case_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_escalation_state
            where case_id = %s::uuid
            """,
            (case_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def upsert_governance_escalation_state(
    conn,
    *,
    workspace_id: str,
    case_id: str,
    escalation_level: str,
    status: str,
    escalated_to_team: str | None,
    escalated_to_user: str | None,
    reason: str | None,
    source_policy_id: str | None,
    escalated_at,
    last_evaluated_at,
    repeated_count: int,
    cleared_at,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_escalation_state (
              workspace_id,
              case_id,
              escalation_level,
              status,
              escalated_to_team,
              escalated_to_user,
              reason,
              source_policy_id,
              escalated_at,
              last_evaluated_at,
              repeated_count,
              cleared_at,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            on conflict (case_id) do update
            set escalation_level = excluded.escalation_level,
                status = excluded.status,
                escalated_to_team = excluded.escalated_to_team,
                escalated_to_user = excluded.escalated_to_user,
                reason = excluded.reason,
                source_policy_id = excluded.source_policy_id,
                escalated_at = excluded.escalated_at,
                last_evaluated_at = excluded.last_evaluated_at,
                repeated_count = excluded.repeated_count,
                cleared_at = excluded.cleared_at,
                metadata = excluded.metadata
            returning *
            """,
            (
                workspace_id,
                case_id,
                escalation_level,
                status,
                escalated_to_team,
                escalated_to_user,
                reason,
                source_policy_id,
                escalated_at,
                last_evaluated_at,
                repeated_count,
                cleared_at,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_escalation_state upsert returned no row")
        return dict(row)


def insert_governance_escalation_event(
    conn,
    *,
    workspace_id: str,
    case_id: str,
    escalation_state_id: str | None,
    event_type: str,
    escalation_level: str | None,
    escalated_to_team: str | None,
    escalated_to_user: str | None,
    reason: str | None,
    source_policy_id: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_escalation_events (
              workspace_id,
              case_id,
              escalation_state_id,
              event_type,
              escalation_level,
              escalated_to_team,
              escalated_to_user,
              reason,
              source_policy_id,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::uuid,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                case_id,
                escalation_state_id,
                event_type,
                escalation_level,
                escalated_to_team,
                escalated_to_user,
                reason,
                source_policy_id,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_escalation_events insert returned no row")
        return dict(row)


def find_recent_related_case(
    conn,
    *,
    workspace_id: str,
    watchlist_id: str | None,
    degradation_family: str,
    version_tuple: str | None,
    regime: str | None,
    limit: int = 1,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              c.*,
              coalesce(c.closed_at, c.resolved_at, c.updated_at) as prior_closed_at
            from public.governance_cases c
            where c.workspace_id = %s::uuid
              and (
                (%s::uuid is null and c.watchlist_id is null)
                or c.watchlist_id = %s::uuid
              )
              and c.version_tuple is not distinct from %s
              and coalesce(c.metadata ->> 'degradation_type', '') = %s
              and c.status in ('resolved', 'closed')
              and (
                %s::text is null
                or coalesce(c.metadata ->> 'regime', '') = %s::text
                or coalesce(c.metadata ->> 'regime', '') = ''
              )
            order by coalesce(c.closed_at, c.resolved_at, c.updated_at) desc nulls last, c.id desc
            limit %s
            """,
            (
                workspace_id,
                watchlist_id,
                watchlist_id,
                version_tuple,
                degradation_family,
                regime,
                regime,
                limit,
            ),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def update_case_recurrence(
    conn,
    *,
    case_id: str,
    recurrence_group_id: str | None,
    reopened_from_case_id: str | None,
    repeat_count: int,
    reopened_at: Any | None,
    reopen_reason: str | None,
    recurrence_match_basis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_cases
            set recurrence_group_id = coalesce(%s::uuid, recurrence_group_id),
                reopened_from_case_id = %s::uuid,
                repeat_count = %s,
                reopened_at = %s::timestamptz,
                reopen_reason = %s,
                recurrence_match_basis = %s::jsonb,
                updated_at = now()
            where id = %s::uuid
            returning *
            """,
            (
                recurrence_group_id,
                reopened_from_case_id,
                repeat_count,
                reopened_at,
                reopen_reason,
                Jsonb(_json_compatible(recurrence_match_basis or {})),
                case_id,
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_cases recurrence update returned no row")
        return dict(row)


def create_case_recurrence_link(
    conn,
    *,
    workspace_id: str,
    watchlist_id: str | None,
    recurrence_group_id: str,
    source_case_id: str,
    matched_case_id: str,
    match_type: str,
    match_score: float,
    matched_within_window: bool,
    match_basis: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_case_recurrence (
              workspace_id,
              watchlist_id,
              recurrence_group_id,
              source_case_id,
              matched_case_id,
              match_type,
              match_score,
              matched_within_window,
              match_basis
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                watchlist_id,
                recurrence_group_id,
                source_case_id,
                matched_case_id,
                match_type,
                match_score,
                matched_within_window,
                Jsonb(_json_compatible(match_basis or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_case_recurrence insert returned no row")
        return dict(row)


def get_case_recurrence_summary(
    conn,
    *,
    case_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_case_recurrence_summary
            where case_id = %s::uuid
            """,
            (case_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def list_related_cases(
    conn,
    *,
    recurrence_group_id: str,
    exclude_case_id: str | None = None,
    limit: int = 25,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_case_summary
            where recurrence_group_id = %s::uuid
              and (%s::uuid is null or id <> %s::uuid)
            order by coalesce(closed_at, resolved_at, opened_at) desc nulls last, id desc
            limit %s
            """,
            (
                recurrence_group_id,
                exclude_case_id,
                exclude_case_id,
                limit,
            ),
        )
        return [dict(row) for row in cur.fetchall()]


def append_governance_case_evidence(
    conn,
    *,
    case_id: str,
    workspace_id: str,
    evidence_type: str,
    reference_id: str,
    title: str | None = None,
    summary: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_case_evidence
            where case_id = %s::uuid
              and evidence_type = %s
              and reference_id = %s
            limit 1
            """,
            (case_id, evidence_type, reference_id),
        )
        existing = cur.fetchone()
        if existing:
            return dict(existing)
        cur.execute(
            """
            insert into public.governance_case_evidence (
              case_id,
              workspace_id,
              evidence_type,
              reference_id,
              title,
              summary,
              payload
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                case_id,
                workspace_id,
                evidence_type,
                reference_id,
                title,
                summary,
                Jsonb(_json_compatible(payload or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_case_evidence insert returned no row")
        return dict(row)


def list_governance_case_notes(
    conn,
    *,
    case_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_case_notes
            where case_id = %s::uuid
            order by created_at desc, id desc
            """,
            (case_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def list_governance_case_evidence(
    conn,
    *,
    case_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_case_evidence
            where case_id = %s::uuid
            order by created_at desc, id desc
            """,
            (case_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_governance_case_evidence_summary(
    conn,
    *,
    case_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_case_evidence_summary
            where case_id = %s::uuid
            """,
            (case_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_governance_case_summary_row(
    conn,
    *,
    case_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_case_summary
            where id = %s::uuid
            """,
            (case_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_governance_case_lifecycle_row(
    conn,
    *,
    degradation_state_id: str | None,
) -> dict[str, Any] | None:
    if not degradation_state_id:
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_lifecycle_summary
            where degradation_state_id = %s::uuid
            """,
            (degradation_state_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def upsert_governance_case_summary(
    conn,
    *,
    workspace_id: str,
    case_id: str,
    summary_version: str = "v1",
    status_summary: str | None = None,
    root_cause_code: str | None = None,
    root_cause_confidence: float | None = None,
    root_cause_summary: str | None = None,
    evidence_summary: str | None = None,
    recurrence_summary: str | None = None,
    operator_summary: str | None = None,
    closure_summary: str | None = None,
    recommended_next_action: str | None = None,
    source_note_ids: list[str] | None = None,
    source_evidence_ids: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_case_summaries (
              workspace_id,
              case_id,
              summary_version,
              status_summary,
              root_cause_code,
              root_cause_confidence,
              root_cause_summary,
              evidence_summary,
              recurrence_summary,
              operator_summary,
              closure_summary,
              recommended_next_action,
              source_note_ids,
              source_evidence_ids,
              metadata,
              generated_at,
              updated_at
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb,
              %s::jsonb,
              now(),
              now()
            )
            on conflict (case_id, summary_version) do update
            set
              status_summary = excluded.status_summary,
              root_cause_code = excluded.root_cause_code,
              root_cause_confidence = excluded.root_cause_confidence,
              root_cause_summary = excluded.root_cause_summary,
              evidence_summary = excluded.evidence_summary,
              recurrence_summary = excluded.recurrence_summary,
              operator_summary = excluded.operator_summary,
              closure_summary = excluded.closure_summary,
              recommended_next_action = excluded.recommended_next_action,
              source_note_ids = excluded.source_note_ids,
              source_evidence_ids = excluded.source_evidence_ids,
              metadata = excluded.metadata,
              generated_at = now(),
              updated_at = now()
            returning *
            """,
            (
                workspace_id,
                case_id,
                summary_version,
                status_summary,
                root_cause_code,
                root_cause_confidence,
                root_cause_summary,
                evidence_summary,
                recurrence_summary,
                operator_summary,
                closure_summary,
                recommended_next_action,
                Jsonb(_json_compatible(source_note_ids or [])),
                Jsonb(_json_compatible(source_evidence_ids or [])),
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_case_summaries upsert returned no row")
        return dict(row)


def get_governance_case_summary_latest(
    conn,
    *,
    case_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_case_summary_latest
            where case_id = %s::uuid
            """,
            (case_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def append_governance_incident_timeline_event(
    conn,
    *,
    case_id: str,
    workspace_id: str,
    event_type: str,
    event_source: str,
    title: str,
    detail: str | None = None,
    actor: str | None = None,
    event_at: Any | None = None,
    metadata: dict[str, Any] | None = None,
    source_table: str | None = None,
    source_id: str | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_incident_timeline_events (
              case_id,
              workspace_id,
              event_type,
              event_source,
              event_at,
              actor,
              title,
              detail,
              metadata,
              source_table,
              source_id
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              coalesce(%s::timestamptz, now()),
              %s,
              %s,
              %s,
              %s::jsonb,
              %s,
              %s
            )
            returning *
            """,
            (
                case_id,
                workspace_id,
                event_type,
                event_source,
                event_at,
                actor,
                title,
                detail,
                Jsonb(_json_compatible(metadata or {})),
                source_table,
                source_id,
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_incident_timeline_events insert returned no row")
        return dict(row)


def insert_governance_threshold_feedback(
    conn,
    payload: dict[str, Any],
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_threshold_feedback (
              workspace_id,
              watchlist_id,
              threshold_profile_id,
              event_type,
              regime,
              compute_version,
              signal_registry_version,
              model_version,
              case_id,
              degradation_state_id,
              threshold_applied_value,
              trigger_count,
              ack_count,
              mute_count,
              escalation_count,
              resolution_count,
              reopen_count,
              precision_proxy,
              noise_score,
              evidence
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                payload["workspace_id"],
                payload.get("watchlist_id"),
                payload.get("threshold_profile_id"),
                payload["event_type"],
                payload.get("regime"),
                payload.get("compute_version"),
                payload.get("signal_registry_version"),
                payload.get("model_version"),
                payload.get("case_id"),
                payload.get("degradation_state_id"),
                payload.get("threshold_applied_value"),
                int(payload.get("trigger_count") or 0),
                int(payload.get("ack_count") or 0),
                int(payload.get("mute_count") or 0),
                int(payload.get("escalation_count") or 0),
                int(payload.get("resolution_count") or 0),
                int(payload.get("reopen_count") or 0),
                float(payload.get("precision_proxy") or 0),
                float(payload.get("noise_score") or 0),
                Jsonb(_json_compatible(payload.get("evidence") or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_threshold_feedback insert returned no row")
        return dict(row)


def _threshold_recommendation_key(row: dict[str, Any]) -> str:
    profile_key = str(row.get("threshold_profile_id") or "unscoped")
    dimension_type = str(row.get("dimension_type") or "dimension")
    dimension_value = str(row.get("dimension_value") or "any")
    event_type = str(row.get("event_type") or "unknown")
    direction = str(row.get("direction") or "keep")
    reason_code = str(row.get("reason_code") or "unspecified")
    return "|".join(
        [
            profile_key,
            dimension_type,
            dimension_value,
            event_type,
            direction,
            reason_code,
        ]
    )


def list_governance_threshold_performance_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_threshold_performance_summary
            where workspace_id = %s::uuid
            order by avg_noise_score desc, feedback_rows desc, event_type asc, regime asc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def replace_governance_threshold_recommendations(
    conn,
    *,
    workspace_id: str,
    rows: list[dict[str, Any]],
) -> None:
    with conn.cursor() as cur:
        active_keys = [_threshold_recommendation_key(row) for row in rows]
        if active_keys:
            cur.execute(
                """
                update public.governance_threshold_recommendations
                set status = 'superseded',
                    updated_at = now()
                where workspace_id = %s::uuid
                  and status in ('open', 'deferred')
                  and recommendation_key <> all(%s::text[])
                """,
                (workspace_id, active_keys),
            )
        else:
            cur.execute(
                """
                update public.governance_threshold_recommendations
                set status = 'superseded',
                    updated_at = now()
                where workspace_id = %s::uuid
                  and status in ('open', 'deferred')
                """,
                (workspace_id,),
            )
        for row in rows:
            cur.execute(
                """
                insert into public.governance_threshold_recommendations (
                  workspace_id,
                  recommendation_key,
                  threshold_profile_id,
                  dimension_type,
                  dimension_value,
                  event_type,
                  current_value,
                  recommended_value,
                  direction,
                  reason_code,
                  confidence,
                  supporting_metrics,
                  status,
                  updated_at
                ) values (
                  %s::uuid,
                  %s,
                  %s::uuid,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s,
                  %s::jsonb,
                  'open',
                  now()
                )
                on conflict (workspace_id, recommendation_key)
                do update set
                  threshold_profile_id = excluded.threshold_profile_id,
                  dimension_type = excluded.dimension_type,
                  dimension_value = excluded.dimension_value,
                  event_type = excluded.event_type,
                  current_value = excluded.current_value,
                  recommended_value = excluded.recommended_value,
                  direction = excluded.direction,
                  reason_code = excluded.reason_code,
                  confidence = excluded.confidence,
                  supporting_metrics = excluded.supporting_metrics,
                  status = case
                    when public.governance_threshold_recommendations.status in ('accepted', 'dismissed')
                      then public.governance_threshold_recommendations.status
                    when public.governance_threshold_recommendations.status = 'superseded'
                      then 'open'
                    else public.governance_threshold_recommendations.status
                  end,
                  updated_at = now()
                """,
                (
                    row["workspace_id"],
                    _threshold_recommendation_key(row),
                    row.get("threshold_profile_id"),
                    row["dimension_type"],
                    row["dimension_value"],
                    row["event_type"],
                    row.get("current_value"),
                    row.get("recommended_value"),
                    row["direction"],
                    row["reason_code"],
                    float(row.get("confidence") or 0),
                    Jsonb(_json_compatible(row.get("supporting_metrics") or {})),
                ),
            )


def list_governance_threshold_learning_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_threshold_learning_summary
            where workspace_id = %s::uuid
            order by created_at desc, confidence desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def create_threshold_recommendation_review(
    conn,
    *,
    workspace_id: str,
    recommendation_id: str,
    reviewer: str,
    decision: str,
    rationale: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_threshold_recommendation_reviews (
              workspace_id,
              recommendation_id,
              reviewer,
              decision,
              rationale,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                recommendation_id,
                reviewer,
                decision,
                rationale,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_threshold_recommendation_reviews insert returned no row")
        return dict(row)


def update_threshold_recommendation_status(
    conn,
    *,
    recommendation_id: str,
    status: str,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_threshold_recommendations
            set status = %s,
                updated_at = now()
            where id = %s::uuid
            returning *
            """,
            (status, recommendation_id),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_threshold_recommendations update returned no row")
        return dict(row)


def upsert_threshold_promotion_proposal(
    conn,
    *,
    workspace_id: str,
    recommendation_id: str,
    profile_id: str,
    event_type: str,
    dimension_type: str,
    dimension_value: str | None,
    current_value: float,
    proposed_value: float,
    source_metrics: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_threshold_promotion_proposals (
              workspace_id,
              recommendation_id,
              profile_id,
              event_type,
              dimension_type,
              dimension_value,
              current_value,
              proposed_value,
              source_metrics,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb
            )
            on conflict (recommendation_id)
            do update set
              profile_id = excluded.profile_id,
              event_type = excluded.event_type,
              dimension_type = excluded.dimension_type,
              dimension_value = excluded.dimension_value,
              current_value = excluded.current_value,
              proposed_value = excluded.proposed_value,
              source_metrics = excluded.source_metrics,
              metadata = excluded.metadata,
              updated_at = now()
            returning *
            """,
            (
                workspace_id,
                recommendation_id,
                profile_id,
                event_type,
                dimension_type,
                dimension_value,
                current_value,
                proposed_value,
                Jsonb(_json_compatible(source_metrics or {})),
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_threshold_promotion_proposals upsert returned no row")
        return dict(row)


def update_threshold_promotion_proposal(
    conn,
    *,
    proposal_id: str,
    status: str | None = None,
    approved_by: str | None = None,
    approved_at=None,
    blocked_reason: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    updates: list[str] = ["updated_at = now()"]
    params: list[Any] = []
    if status is not None:
        updates.append("status = %s")
        params.append(status)
    if approved_by is not None:
        updates.append("approved_by = %s")
        params.append(approved_by)
    if approved_at is not None:
        updates.append("approved_at = %s")
        params.append(approved_at)
    if blocked_reason is not None:
        updates.append("blocked_reason = %s")
        params.append(blocked_reason)
    if metadata is not None:
        updates.append("metadata = %s::jsonb")
        params.append(Jsonb(_json_compatible(metadata)))
    params.append(proposal_id)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            update public.governance_threshold_promotion_proposals
            set {", ".join(updates)}
            where id = %s::uuid
            returning *
            """,
            tuple(params),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_threshold_promotion_proposals update returned no row")
        return dict(row)


def get_threshold_promotion_proposal(
    conn,
    *,
    proposal_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_threshold_review_summary
            where proposal_id = %s::uuid
            """,
            (proposal_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def list_threshold_review_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_threshold_review_summary
            where workspace_id = %s::uuid
            order by updated_at desc, created_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def upsert_threshold_autopromotion_policy(
    conn,
    *,
    workspace_id: str,
    profile_id: str | None = None,
    event_type: str | None = None,
    dimension_type: str | None = None,
    dimension_value: str | None = None,
    enabled: bool = False,
    min_confidence: float = 0.85,
    min_support: int = 20,
    max_step_pct: float = 0.20,
    cooldown_hours: int = 168,
    allow_regime_specific: bool = True,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_threshold_autopromotion_policies
            set enabled = %s,
                min_confidence = %s,
                min_support = %s,
                max_step_pct = %s,
                cooldown_hours = %s,
                allow_regime_specific = %s,
                metadata = %s::jsonb,
                updated_at = now()
            where workspace_id = %s::uuid
              and (
                (profile_id is null and %s::uuid is null)
                or profile_id = %s::uuid
              )
              and (
                (event_type is null and %s::text is null)
                or event_type = %s
              )
              and (
                (dimension_type is null and %s::text is null)
                or dimension_type = %s
              )
              and (
                (dimension_value is null and %s::text is null)
                or dimension_value = %s
              )
            returning *
            """,
            (
                enabled,
                min_confidence,
                min_support,
                max_step_pct,
                cooldown_hours,
                allow_regime_specific,
                Jsonb(_json_compatible(metadata or {})),
                workspace_id,
                profile_id,
                profile_id,
                event_type,
                event_type,
                dimension_type,
                dimension_type,
                dimension_value,
                dimension_value,
            ),
        )
        row = cur.fetchone()
        if row:
            return dict(row)

        cur.execute(
            """
            insert into public.governance_threshold_autopromotion_policies (
              workspace_id,
              profile_id,
              event_type,
              dimension_type,
              dimension_value,
              enabled,
              min_confidence,
              min_support,
              max_step_pct,
              cooldown_hours,
              allow_regime_specific,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                profile_id,
                event_type,
                dimension_type,
                dimension_value,
                enabled,
                min_confidence,
                min_support,
                max_step_pct,
                cooldown_hours,
                allow_regime_specific,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_threshold_autopromotion_policies upsert returned no row")
        return dict(row)


def get_threshold_autopromotion_policy(
    conn,
    *,
    workspace_id: str,
    profile_id: str | None,
    event_type: str,
    dimension_type: str,
    dimension_value: str | None,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_threshold_autopromotion_policies
            where workspace_id = %s::uuid
              and (profile_id = %s::uuid or profile_id is null)
              and (event_type = %s or event_type is null)
              and (dimension_type = %s or dimension_type is null)
              and (dimension_value = %s or dimension_value is null)
            order by
              (profile_id is null) asc,
              (event_type is null) asc,
              (dimension_type is null) asc,
              (dimension_value is null) asc,
              updated_at desc
            limit 1
            """,
            (workspace_id, profile_id, event_type, dimension_type, dimension_value),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_recent_threshold_promotion_execution(
    conn,
    *,
    workspace_id: str,
    profile_id: str,
    event_type: str,
    dimension_type: str,
    dimension_value: str | None,
    since_hours: int,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_threshold_promotion_executions
            where workspace_id = %s::uuid
              and profile_id = %s::uuid
              and event_type = %s
              and dimension_type = %s
              and (
                (%s::text is null and dimension_value is null)
                or dimension_value = %s
              )
              and created_at >= now() - (%s::text || ' hours')::interval
            order by created_at desc
            limit 1
            """,
            (
                workspace_id,
                profile_id,
                event_type,
                dimension_type,
                dimension_value,
                dimension_value,
                since_hours,
            ),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def _threshold_column_for_event(event_type: str) -> str:
    mapping = {
        "version_regression": "version_health_floor",
        "replay_degradation": "replay_consistency_floor",
        "family_instability_spike": "family_instability_ceiling",
        "stability_classification_downgrade": "family_instability_ceiling",
        "regime_conflict_persistence": "conflicting_transition_ceiling",
        "regime_instability_spike": "regime_instability_ceiling",
    }
    try:
        return mapping[event_type]
    except KeyError as exc:
        raise RuntimeError(f"unsupported threshold event_type for promotion: {event_type}") from exc


def apply_threshold_promotion(
    conn,
    *,
    workspace_id: str,
    profile_id: str,
    event_type: str,
    dimension_type: str,
    dimension_value: str | None,
    new_value: float,
) -> dict[str, Any]:
    column_name = _threshold_column_for_event(event_type)
    target_table = "public.governance_threshold_profiles"
    target_id = profile_id

    if dimension_type == "regime" and dimension_value and dimension_value not in {"any", "default"}:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id
                from public.regime_threshold_overrides
                where profile_id = %s::uuid
                  and regime = %s
                order by updated_at desc
                limit 1
                """,
                (profile_id, dimension_value),
            )
            override_row = cur.fetchone()
            if override_row:
                target_table = "public.regime_threshold_overrides"
                target_id = str(override_row["id"])

    with conn.cursor() as cur:
        cur.execute(
            f"""
            update {target_table}
            set {column_name} = %s,
                updated_at = now()
            where id = %s::uuid
            returning *
            """,
            (new_value, target_id),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("threshold promotion update returned no row")
        return dict(row)


def create_threshold_promotion_execution(
    conn,
    *,
    workspace_id: str,
    proposal_id: str,
    profile_id: str,
    event_type: str,
    dimension_type: str,
    dimension_value: str | None,
    previous_value: float,
    new_value: float,
    executed_by: str,
    execution_mode: str,
    rationale: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_threshold_promotion_executions (
              workspace_id,
              proposal_id,
              profile_id,
              event_type,
              dimension_type,
              dimension_value,
              previous_value,
              new_value,
              executed_by,
              execution_mode,
              rationale,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                proposal_id,
                profile_id,
                event_type,
                dimension_type,
                dimension_value,
                previous_value,
                new_value,
                executed_by,
                execution_mode,
                rationale,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_threshold_promotion_executions insert returned no row")
        return dict(row)


def create_threshold_rollback_candidate(
    conn,
    *,
    workspace_id: str,
    execution_id: str,
    profile_id: str,
    rollback_to_value: float,
    reason: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_threshold_rollback_candidates (
              workspace_id,
              execution_id,
              profile_id,
              rollback_to_value,
              reason,
              metadata
            ) values (
              %s::uuid,
              %s::uuid,
              %s::uuid,
              %s,
              %s,
              %s::jsonb
            )
            returning *
            """,
            (
                workspace_id,
                execution_id,
                profile_id,
                rollback_to_value,
                reason,
                Jsonb(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_threshold_rollback_candidates insert returned no row")
        return dict(row)


def list_threshold_autopromotion_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_threshold_autopromotion_summary
            where workspace_id = %s::uuid
            order by created_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


# ── Phase 3.5A — Routing Optimization ───────────────────────────────────────


def get_routing_feature_effectiveness_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_feature_effectiveness_summary
            where workspace_id = %s::uuid
            order by net_fit_score asc nulls last
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_routing_context_fit_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_context_fit_summary
            where workspace_id = %s::uuid
            order by operator_fit_score desc nulls last
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_routing_policy_opportunity_summary(
    conn,
    *,
    workspace_id: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_policy_opportunity_summary
            where workspace_id = %s::uuid
            limit %s
            """,
            (workspace_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def upsert_routing_policy_recommendation(
    conn,
    *,
    workspace_id: str,
    rec: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_policy_recommendations (
                workspace_id, recommendation_key, scope_type, scope_value,
                current_policy, recommended_policy, reason_code, confidence,
                sample_size, expected_benefit_score, risk_score, supporting_metrics,
                created_at
            ) values (
                %s::uuid, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                now()
            )
            on conflict (workspace_id, recommendation_key) do update set
                scope_type             = excluded.scope_type,
                scope_value            = excluded.scope_value,
                current_policy         = excluded.current_policy,
                recommended_policy     = excluded.recommended_policy,
                reason_code            = excluded.reason_code,
                confidence             = excluded.confidence,
                sample_size            = excluded.sample_size,
                expected_benefit_score = excluded.expected_benefit_score,
                risk_score             = excluded.risk_score,
                supporting_metrics     = excluded.supporting_metrics,
                created_at             = now()
            """,
            (
                workspace_id,
                rec["recommendation_key"],
                rec["scope_type"],
                rec["scope_value"],
                Jsonb(rec.get("current_policy") or {}),
                Jsonb(rec.get("recommended_policy") or {}),
                rec["reason_code"],
                rec["confidence"],
                rec["sample_size"],
                rec["expected_benefit_score"],
                rec["risk_score"],
                Jsonb(rec.get("supporting_metrics") or {}),
            ),
        )


def insert_routing_optimization_snapshot(
    conn,
    *,
    workspace_id: str,
    window_label: str,
    recommendation_count: int,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_optimization_snapshots
                (workspace_id, window_label, recommendation_count, metadata)
            values (%s::uuid, %s, %s, %s)
            returning *
            """,
            (workspace_id, window_label, recommendation_count, Jsonb(metadata or {})),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("governance_routing_optimization_snapshots insert returned no row")
        return dict(row)


def get_latest_routing_optimization_snapshot(
    conn,
    *,
    workspace_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.governance_routing_optimization_snapshots
            where workspace_id = %s::uuid
            order by snapshot_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


# ── Phase 3.5B — Routing Policy Review + Promotion ───────────────────────────

def insert_routing_policy_review(
    conn,
    *,
    workspace_id: str,
    recommendation_key: str,
    review_status: str,
    review_reason: str | None = None,
    reviewed_by: str,
    notes: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_policy_recommendation_reviews
                (workspace_id, recommendation_key, review_status, review_reason,
                 reviewed_by, notes, metadata)
            values (%s::uuid, %s, %s, %s, %s, %s, %s)
            returning *
            """,
            (
                workspace_id, recommendation_key, review_status, review_reason,
                reviewed_by, notes, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_routing_policy_review returned no row")
        return dict(row)


def list_routing_policy_reviews(
    conn,
    *,
    workspace_id: str,
    recommendation_key: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        if recommendation_key:
            cur.execute(
                """
                select * from public.governance_routing_policy_recommendation_reviews
                where workspace_id = %s::uuid
                  and recommendation_key = %s
                order by reviewed_at desc
                limit %s
                """,
                (workspace_id, recommendation_key, limit),
            )
        else:
            cur.execute(
                """
                select * from public.governance_routing_policy_recommendation_reviews
                where workspace_id = %s::uuid
                order by reviewed_at desc
                limit %s
                """,
                (workspace_id, limit),
            )
        return [dict(row) for row in cur.fetchall()]


def get_routing_policy_review_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select * from public.governance_routing_policy_review_summary
            where workspace_id = %s::uuid
            order by latest_reviewed_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def insert_routing_policy_promotion_proposal(
    conn,
    *,
    workspace_id: str,
    recommendation_key: str,
    promotion_target: str,
    scope_type: str,
    scope_value: str,
    current_policy: dict[str, Any],
    recommended_policy: dict[str, Any],
    proposed_by: str,
    proposal_reason: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_policy_promotion_proposals
                (workspace_id, recommendation_key, promotion_target, scope_type, scope_value,
                 current_policy, recommended_policy, proposed_by, proposal_reason, metadata)
            values (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            returning *
            """,
            (
                workspace_id, recommendation_key, promotion_target, scope_type, scope_value,
                Jsonb(current_policy), Jsonb(recommended_policy),
                proposed_by, proposal_reason, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_routing_policy_promotion_proposal returned no row")
        return dict(row)


def get_routing_policy_promotion_proposal(
    conn,
    *,
    proposal_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_promotion_proposals where id = %s::uuid",
            (proposal_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def update_routing_policy_promotion_proposal(
    conn,
    *,
    proposal_id: str,
    status: str,
    approved_by: str | None = None,
    applied_at: str | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_routing_policy_promotion_proposals
            set proposal_status = %s,
                approved_by     = coalesce(%s, approved_by),
                approved_at     = case when %s = 'approved' then now() else approved_at end,
                applied_at      = case when %s::timestamptz is not null
                                       then %s::timestamptz else applied_at end
            where id = %s::uuid
            returning *
            """,
            (status, approved_by, status, applied_at, applied_at, proposal_id),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"proposal {proposal_id!r} not found")
        return dict(row)


def get_routing_policy_promotion_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select * from public.governance_routing_policy_promotion_summary
            where workspace_id = %s::uuid
            order by latest_proposed_at desc
            """,
            (workspace_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def insert_routing_policy_application(
    conn,
    *,
    workspace_id: str,
    proposal_id: str,
    recommendation_key: str,
    applied_target: str,
    applied_scope_type: str,
    applied_scope_value: str,
    prior_policy: dict[str, Any],
    applied_policy: dict[str, Any],
    applied_by: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_policy_applications
                (workspace_id, proposal_id, recommendation_key,
                 applied_target, applied_scope_type, applied_scope_value,
                 prior_policy, applied_policy, applied_by, metadata)
            values (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
            returning *
            """,
            (
                workspace_id, proposal_id, recommendation_key,
                applied_target, applied_scope_type, applied_scope_value,
                Jsonb(prior_policy), Jsonb(applied_policy),
                applied_by, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_routing_policy_application returned no row")
        return dict(row)


def get_current_routing_policy_for_scope(
    conn,
    *,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
) -> dict[str, Any]:
    """Return a snapshot of the live routing policy for a scope (for prior_policy capture)."""
    with conn.cursor() as cur:
        if scope_type == "operator":
            cur.execute(
                """
                select id, assigned_user, assigned_team, reason, is_enabled, metadata
                from public.governance_routing_overrides
                where workspace_id = %s::uuid
                  and assigned_user = %s
                  and is_enabled = true
                order by created_at desc
                limit 5
                """,
                (workspace_id, scope_value),
            )
        elif scope_type == "team":
            cur.execute(
                """
                select id, assign_team, assign_user, root_cause_code, severity,
                       regime, chronic_only, priority, is_enabled, metadata
                from public.governance_routing_rules
                where workspace_id = %s::uuid
                  and assign_team = %s
                  and is_enabled = true
                order by priority asc, created_at desc
                limit 5
                """,
                (workspace_id, scope_value),
            )
        elif scope_type == "root_cause":
            cur.execute(
                """
                select id, assign_team, assign_user, root_cause_code, severity,
                       regime, chronic_only, priority, is_enabled, metadata
                from public.governance_routing_rules
                where workspace_id = %s::uuid
                  and root_cause_code = %s
                  and is_enabled = true
                order by priority asc, created_at desc
                limit 5
                """,
                (workspace_id, scope_value),
            )
        elif scope_type == "regime":
            cur.execute(
                """
                select id, assign_team, assign_user, root_cause_code, severity,
                       regime, chronic_only, priority, is_enabled, metadata
                from public.governance_routing_rules
                where workspace_id = %s::uuid
                  and regime = %s
                  and is_enabled = true
                order by priority asc, created_at desc
                limit 5
                """,
                (workspace_id, scope_value),
            )
        else:
            return {"scope_type": scope_type, "scope_value": scope_value, "rows": []}

        rows = [dict(r) for r in cur.fetchall()]
    return {"scope_type": scope_type, "scope_value": scope_value, "rows": rows}


def apply_routing_override_from_recommendation(
    conn,
    *,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
    recommended_policy: dict[str, Any],
    applied_by: str,
) -> dict[str, Any]:
    """Insert a new governance_routing_overrides row from a recommendation (additive)."""
    assigned_user = recommended_policy.get("preferred_operator") or (
        scope_value if scope_type == "operator" else None
    )
    assigned_team = recommended_policy.get("preferred_team") or (
        scope_value if scope_type == "team" else None
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_overrides
                (workspace_id, assigned_user, assigned_team, reason, is_enabled, metadata)
            values (%s::uuid, %s, %s, %s, true, %s)
            returning *
            """,
            (
                workspace_id,
                assigned_user,
                assigned_team,
                f"applied_from_policy_recommendation:{applied_by}",
                Jsonb({"source": "routing_policy_promotion", "applied_by": applied_by,
                       "recommended_policy": recommended_policy}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("apply_routing_override_from_recommendation returned no row")
        return dict(row)


def apply_routing_rule_from_recommendation(
    conn,
    *,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
    recommended_policy: dict[str, Any],
    applied_by: str,
) -> dict[str, Any]:
    """Insert a governance_routing_rules row from a recommendation at priority 5 (high)."""
    assign_team = (
        recommended_policy.get("preferred_team")
        or recommended_policy.get("preferred_team_for_reopens")
        or (scope_value if scope_type == "team" else None)
    )
    assign_user = recommended_policy.get("preferred_operator")
    root_cause_code = scope_value if scope_type == "root_cause" else None
    regime = scope_value if scope_type == "regime" else None
    severity = scope_value if scope_type == "severity" else None
    chronic_only = scope_type == "chronicity"

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_rules
                (workspace_id, is_enabled, priority,
                 root_cause_code, severity, regime, chronic_only,
                 assign_team, assign_user,
                 routing_reason_template, metadata)
            values (%s::uuid, true, 5, %s, %s, %s, %s, %s, %s, %s, %s)
            returning *
            """,
            (
                workspace_id,
                root_cause_code, severity, regime, chronic_only,
                assign_team, assign_user,
                f"policy_promotion:{scope_type}:{scope_value}",
                Jsonb({"source": "routing_policy_promotion", "applied_by": applied_by,
                       "recommended_policy": recommended_policy}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("apply_routing_rule_from_recommendation returned no row")
        return dict(row)


# ── Phase 3.5C: Routing Policy Autopromotion ─────────────────────────────────

def list_routing_policy_autopromotion_policies(
    conn,
    *,
    workspace_id: str,
    enabled_only: bool = True,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        if enabled_only:
            cur.execute(
                "select * from public.governance_routing_policy_autopromotion_policies"
                " where workspace_id = %s::uuid and enabled = true"
                " order by scope_type, scope_value",
                (workspace_id,),
            )
        else:
            cur.execute(
                "select * from public.governance_routing_policy_autopromotion_policies"
                " where workspace_id = %s::uuid"
                " order by scope_type, scope_value",
                (workspace_id,),
            )
        return [dict(r) for r in cur.fetchall()]


def get_routing_policy_autopromotion_policy(
    conn,
    *,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_autopromotion_policies"
            " where workspace_id = %s::uuid and scope_type = %s and scope_value = %s",
            (workspace_id, scope_type, scope_value),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def upsert_routing_policy_autopromotion_policy(
    conn,
    *,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
    promotion_target: str = "rule",
    enabled: bool = True,
    min_confidence: str = "high",
    min_approved_review_count: int = 1,
    min_application_count: int = 1,
    min_sample_size: int = 50,
    max_recent_override_rate: float = 0.20,
    max_recent_reassignment_rate: float = 0.15,
    cooldown_hours: int = 168,
    created_by: str = "ops",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_policy_autopromotion_policies
                (workspace_id, scope_type, scope_value, promotion_target, enabled,
                 min_confidence, min_approved_review_count, min_application_count,
                 min_sample_size, max_recent_override_rate, max_recent_reassignment_rate,
                 cooldown_hours, created_by, metadata)
            values (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (workspace_id, scope_type, scope_value) do update set
                promotion_target              = excluded.promotion_target,
                enabled                       = excluded.enabled,
                min_confidence                = excluded.min_confidence,
                min_approved_review_count     = excluded.min_approved_review_count,
                min_application_count         = excluded.min_application_count,
                min_sample_size               = excluded.min_sample_size,
                max_recent_override_rate      = excluded.max_recent_override_rate,
                max_recent_reassignment_rate  = excluded.max_recent_reassignment_rate,
                cooldown_hours                = excluded.cooldown_hours,
                updated_at                    = now(),
                metadata                      = excluded.metadata
            returning *
            """,
            (
                workspace_id, scope_type, scope_value, promotion_target, enabled,
                min_confidence, min_approved_review_count, min_application_count,
                min_sample_size, max_recent_override_rate, max_recent_reassignment_rate,
                cooldown_hours, created_by, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("upsert_routing_policy_autopromotion_policy returned no row")
        return dict(row)


def insert_routing_policy_autopromotion_execution(
    conn,
    *,
    workspace_id: str,
    policy_id: str,
    recommendation_key: str,
    outcome: str,
    proposal_id: str | None = None,
    application_id: str | None = None,
    blocked_reason: str | None = None,
    skipped_reason: str | None = None,
    executed_by: str = "worker_autopromotion",
    prior_policy: dict[str, Any] | None = None,
    applied_policy: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_policy_autopromotion_executions
                (workspace_id, policy_id, recommendation_key, outcome,
                 proposal_id, application_id, blocked_reason, skipped_reason,
                 executed_by, prior_policy, applied_policy, metadata)
            values (%s::uuid, %s::uuid, %s, %s,
                    %s::uuid, %s::uuid, %s, %s,
                    %s, %s, %s, %s)
            returning *
            """,
            (
                workspace_id, policy_id, recommendation_key, outcome,
                proposal_id, application_id, blocked_reason, skipped_reason,
                executed_by, Jsonb(prior_policy or {}), Jsonb(applied_policy or {}),
                Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_routing_policy_autopromotion_execution returned no row")
        return dict(row)


def insert_routing_policy_autopromotion_rollback_candidate(
    conn,
    *,
    workspace_id: str,
    execution_id: str,
    recommendation_key: str,
    scope_type: str,
    scope_value: str,
    prior_policy: dict[str, Any],
    applied_policy: dict[str, Any],
    routing_row_id: str | None = None,
    routing_table: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_policy_autopromotion_rollback_candidates
                (workspace_id, execution_id, recommendation_key,
                 scope_type, scope_value, prior_policy, applied_policy,
                 routing_row_id, routing_table, metadata)
            values (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s::uuid, %s, %s)
            returning *
            """,
            (
                workspace_id, execution_id, recommendation_key,
                scope_type, scope_value, Jsonb(prior_policy), Jsonb(applied_policy),
                routing_row_id, routing_table, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_routing_policy_autopromotion_rollback_candidate returned no row")
        return dict(row)


def get_latest_routing_policy_autopromotion_execution(
    conn,
    *,
    workspace_id: str,
    recommendation_key: str,
    outcome: str = "promoted",
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select * from public.governance_routing_policy_autopromotion_executions
            where workspace_id = %s::uuid
              and recommendation_key = %s
              and outcome = %s
            order by executed_at desc
            limit 1
            """,
            (workspace_id, recommendation_key, outcome),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_routing_policy_autopromotion_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_autopromotion_summary"
            " where workspace_id = %s::uuid"
            " order by latest_executed_at desc nulls last",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_routing_policy_autopromotion_eligibility(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_autopromotion_eligibility"
            " where workspace_id = %s::uuid"
            " order by is_eligible desc, expected_benefit_score desc nulls last",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_routing_policy_autopromotion_rollback_candidates(
    conn,
    *,
    workspace_id: str,
    resolved: bool = False,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_autopromotion_rollback_candidates"
            " where workspace_id = %s::uuid and resolved = %s"
            " order by created_at desc",
            (workspace_id, resolved),
        )
        return [dict(r) for r in cur.fetchall()]


# ── Phase 3.6A: Rollback Review + Execution ───────────────────────────────────

def get_routing_policy_rollback_candidate(
    conn,
    *,
    workspace_id: str,
    rollback_candidate_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_autopromotion_rollback_candidates"
            " where workspace_id = %s::uuid and id = %s::uuid",
            (workspace_id, rollback_candidate_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_routing_policy_rollback_review(
    conn,
    *,
    workspace_id: str,
    rollback_candidate_id: str,
    review_status: str,
    review_reason: str | None = None,
    reviewed_by: str,
    notes: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_policy_rollback_reviews
                (workspace_id, rollback_candidate_id, review_status,
                 review_reason, reviewed_by, notes, metadata)
            values (%s::uuid, %s::uuid, %s, %s, %s, %s, %s)
            returning *
            """,
            (
                workspace_id, rollback_candidate_id, review_status,
                review_reason, reviewed_by, notes, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_routing_policy_rollback_review returned no row")
        return dict(row)


def list_routing_policy_rollback_reviews(
    conn,
    *,
    workspace_id: str,
    rollback_candidate_id: str | None = None,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        if rollback_candidate_id:
            cur.execute(
                "select * from public.governance_routing_policy_rollback_reviews"
                " where workspace_id = %s::uuid and rollback_candidate_id = %s::uuid"
                " order by reviewed_at desc",
                (workspace_id, rollback_candidate_id),
            )
        else:
            cur.execute(
                "select * from public.governance_routing_policy_rollback_reviews"
                " where workspace_id = %s::uuid"
                " order by reviewed_at desc"
                " limit 100",
                (workspace_id,),
            )
        return [dict(r) for r in cur.fetchall()]


def get_routing_policy_rollback_review_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_rollback_review_summary"
            " where workspace_id = %s::uuid"
            " order by latest_reviewed_at desc nulls last",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def insert_routing_policy_rollback_execution(
    conn,
    *,
    workspace_id: str,
    rollback_candidate_id: str,
    execution_target: str,
    scope_type: str,
    scope_value: str,
    promotion_execution_id: str,
    restored_policy: dict[str, Any],
    replaced_policy: dict[str, Any],
    executed_by: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_policy_rollback_executions
                (workspace_id, rollback_candidate_id, execution_target,
                 scope_type, scope_value, promotion_execution_id,
                 restored_policy, replaced_policy, executed_by, metadata)
            values (%s::uuid, %s::uuid, %s, %s, %s, %s::uuid, %s, %s, %s, %s)
            returning *
            """,
            (
                workspace_id, rollback_candidate_id, execution_target,
                scope_type, scope_value, promotion_execution_id,
                Jsonb(restored_policy), Jsonb(replaced_policy), executed_by,
                Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_routing_policy_rollback_execution returned no row")
        return dict(row)


def get_routing_policy_rollback_execution_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_rollback_execution_summary"
            " where workspace_id = %s::uuid"
            " order by latest_executed_at desc nulls last",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_pending_routing_policy_rollback_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_pending_rollback_summary"
            " where workspace_id = %s::uuid",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def mark_routing_policy_rollback_candidate_rolled_back(
    conn,
    *,
    workspace_id: str,
    rollback_candidate_id: str,
    resolved_by: str,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_routing_policy_autopromotion_rollback_candidates
            set resolved = true,
                resolved_at = now(),
                resolved_by = %s
            where workspace_id = %s::uuid
              and id = %s::uuid
              and resolved = false
            returning *
            """,
            (resolved_by, workspace_id, rollback_candidate_id),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                f"mark_routing_policy_rollback_candidate_rolled_back: "
                f"candidate {rollback_candidate_id} not found, already resolved, or wrong workspace"
            )
        return dict(row)


def restore_routing_rule_from_prior_policy(
    conn,
    *,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
    prior_policy: dict[str, Any],
    restored_by: str,
) -> dict[str, Any]:
    """Insert a new routing rule row restoring the prior policy state. Additive; preserves history."""
    assign_team = (
        prior_policy.get("preferred_team")
        or prior_policy.get("assign_team")
        or (scope_value if scope_type == "team" else None)
    )
    assign_user = prior_policy.get("preferred_operator") or prior_policy.get("assign_user")
    root_cause_code = prior_policy.get("root_cause_code") or (
        scope_value if scope_type == "root_cause" else None
    )
    regime = prior_policy.get("regime") or (scope_value if scope_type == "regime" else None)
    severity = prior_policy.get("severity") or (scope_value if scope_type == "severity" else None)
    chronic_only = prior_policy.get("chronic_only", False) or scope_type == "chronicity"

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_rules
                (workspace_id, is_enabled, priority,
                 root_cause_code, severity, regime, chronic_only,
                 assign_team, assign_user,
                 routing_reason_template, metadata)
            values (%s::uuid, true, 5, %s, %s, %s, %s, %s, %s, %s, %s)
            returning *
            """,
            (
                workspace_id,
                root_cause_code, severity, regime, chronic_only,
                assign_team, assign_user,
                f"rollback_restore:{scope_type}:{scope_value}",
                Jsonb({
                    "source": "routing_policy_rollback",
                    "restored_by": restored_by,
                    "prior_policy": prior_policy,
                }),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("restore_routing_rule_from_prior_policy returned no row")
        return dict(row)


def restore_routing_override_from_prior_policy(
    conn,
    *,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
    prior_policy: dict[str, Any],
    restored_by: str,
) -> dict[str, Any]:
    """Insert a new routing override row restoring the prior policy state. Additive."""
    assigned_user = (
        prior_policy.get("preferred_operator")
        or prior_policy.get("assigned_user")
        or (scope_value if scope_type == "operator" else None)
    )
    assigned_team = (
        prior_policy.get("preferred_team")
        or prior_policy.get("assigned_team")
        or (scope_value if scope_type == "team" else None)
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_overrides
                (workspace_id, assigned_user, assigned_team, reason, is_enabled, metadata)
            values (%s::uuid, %s, %s, %s, true, %s)
            returning *
            """,
            (
                workspace_id,
                assigned_user,
                assigned_team,
                f"rollback_restore:{restored_by}",
                Jsonb({
                    "source": "routing_policy_rollback",
                    "restored_by": restored_by,
                    "prior_policy": prior_policy,
                }),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("restore_routing_override_from_prior_policy returned no row")
        return dict(row)


# ── Phase 3.6B: Rollback Impact Analysis ─────────────────────────────────────

def insert_routing_policy_rollback_impact_snapshot(
    conn,
    *,
    workspace_id: str,
    rollback_execution_id: str,
    rollback_candidate_id: str,
    recommendation_key: str,
    scope_type: str,
    scope_value: str,
    target_type: str,
    evaluation_window_label: str = "30d",
    impact_classification: str,
    before_metrics: dict[str, Any],
    after_metrics: dict[str, Any],
    delta_metrics: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_routing_policy_rollback_impact_snapshots
                (workspace_id, rollback_execution_id, rollback_candidate_id,
                 recommendation_key, scope_type, scope_value, target_type,
                 evaluation_window_label, impact_classification,
                 before_metrics, after_metrics, delta_metrics, metadata)
            values (%s::uuid, %s::uuid, %s::uuid,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s)
            returning *
            """,
            (
                workspace_id, rollback_execution_id, rollback_candidate_id,
                recommendation_key, scope_type, scope_value, target_type,
                evaluation_window_label, impact_classification,
                Jsonb(before_metrics), Jsonb(after_metrics),
                Jsonb(delta_metrics), Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_routing_policy_rollback_impact_snapshot returned no row")
        return dict(row)


def get_routing_policy_rollback_impact_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_rollback_impact_summary"
            " where workspace_id = %s::uuid"
            " order by created_at desc",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_routing_policy_rollback_effectiveness_summary(
    conn,
    *,
    workspace_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_rollback_effectiveness_summary"
            " where workspace_id = %s::uuid"
            " limit 1",
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_pending_routing_policy_rollback_evaluation_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_rollback_pending_evaluation_summary"
            " where workspace_id = %s::uuid",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def list_recent_routing_policy_rollback_executions(
    conn,
    *,
    workspace_id: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_routing_policy_rollback_executions"
            " where workspace_id = %s::uuid"
            " order by executed_at desc"
            " limit %s",
            (workspace_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]


def get_before_after_metrics_for_rollback(
    conn,
    *,
    workspace_id: str,
    scope_type: str,
    scope_value: str,
    rollback_executed_at: Any,
    window_days: int = 30,
) -> dict[str, Any]:
    """Query routing feedback and outcomes split by before/after rollback timestamp.

    Returns a dict with 'before' and 'after' sub-dicts, each containing:
    - reassignment_rate, recurrence_rate, reopen_rate, case_count
    """
    result: dict[str, Any] = {"before": {}, "after": {}}

    # Map scope_type to the feedback column that captures it
    scope_filter_col: str | None = None
    if scope_type == "team":
        scope_filter_col = "assigned_team"
    elif scope_type == "operator":
        scope_filter_col = "prior_assigned_to"

    if scope_filter_col is None:
        # scope_type not directly queryable via feedback; return empty
        return result

    with conn.cursor() as cur:
        # Before window: window_days prior to rollback timestamp
        cur.execute(
            f"""
            select
                count(*) as case_count,
                count(*) filter (where feedback_type = 'reassignment')::numeric
                    / nullif(count(*), 0) as reassignment_rate,
                count(*) filter (where feedback_type = 'reopen')::numeric
                    / nullif(count(*), 0) as reopen_rate,
                count(*) filter (where feedback_type = 'recurrence')::numeric
                    / nullif(count(*), 0) as recurrence_rate
            from public.governance_routing_feedback
            where workspace_id = %s::uuid
              and {scope_filter_col} = %s
              and created_at >= %s - interval '{window_days} days'
              and created_at < %s
            """,
            (workspace_id, scope_value, rollback_executed_at, rollback_executed_at),
        )
        row = cur.fetchone()
        if row:
            result["before"] = {
                "case_count": int(row["case_count"] or 0),
                "reassignment_rate": float(row["reassignment_rate"] or 0.0),
                "reopen_rate": float(row["reopen_rate"] or 0.0),
                "recurrence_rate": float(row["recurrence_rate"] or 0.0),
            }

        # After window: window_days after rollback timestamp
        cur.execute(
            f"""
            select
                count(*) as case_count,
                count(*) filter (where feedback_type = 'reassignment')::numeric
                    / nullif(count(*), 0) as reassignment_rate,
                count(*) filter (where feedback_type = 'reopen')::numeric
                    / nullif(count(*), 0) as reopen_rate,
                count(*) filter (where feedback_type = 'recurrence')::numeric
                    / nullif(count(*), 0) as recurrence_rate
            from public.governance_routing_feedback
            where workspace_id = %s::uuid
              and {scope_filter_col} = %s
              and created_at >= %s
              and created_at < %s + interval '{window_days} days'
            """,
            (workspace_id, scope_value, rollback_executed_at, rollback_executed_at),
        )
        row = cur.fetchone()
        if row:
            result["after"] = {
                "case_count": int(row["case_count"] or 0),
                "reassignment_rate": float(row["reassignment_rate"] or 0.0),
                "reopen_rate": float(row["reopen_rate"] or 0.0),
                "recurrence_rate": float(row["recurrence_rate"] or 0.0),
            }

    return result


# ── Phase 3.7A: Governance Policy Optimization ────────────────────────────────

def get_governance_policy_feature_effectiveness_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_policy_feature_effectiveness_summary"
            " where workspace_id = %s::uuid"
            " order by net_policy_fit_score desc",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_governance_policy_context_fit_summary(
    conn,
    *,
    workspace_id: str,
    limit: int = 30,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_policy_context_fit_summary"
            " where workspace_id = %s::uuid"
            " order by fit_score desc"
            " limit %s",
            (workspace_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]


def get_governance_policy_opportunity_summary(
    conn,
    *,
    workspace_id: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_policy_opportunity_summary"
            " where workspace_id = %s::uuid"
            " order by expected_benefit_score desc, risk_score asc"
            " limit %s",
            (workspace_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]


def upsert_governance_policy_recommendation(
    conn,
    *,
    workspace_id: str,
    recommendation_key: str,
    policy_family: str,
    scope_type: str,
    scope_value: str,
    current_policy: dict[str, Any],
    recommended_policy: dict[str, Any],
    reason_code: str,
    confidence: str,
    sample_size: int,
    expected_benefit_score: float,
    risk_score: float,
    supporting_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_policy_recommendations
                (workspace_id, recommendation_key, policy_family, scope_type, scope_value,
                 current_policy, recommended_policy, reason_code, confidence, sample_size,
                 expected_benefit_score, risk_score, supporting_metrics)
            values (%s::uuid, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s)
            on conflict (workspace_id, recommendation_key) do update set
                policy_family           = excluded.policy_family,
                scope_type              = excluded.scope_type,
                scope_value             = excluded.scope_value,
                current_policy          = excluded.current_policy,
                recommended_policy      = excluded.recommended_policy,
                reason_code             = excluded.reason_code,
                confidence              = excluded.confidence,
                sample_size             = excluded.sample_size,
                expected_benefit_score  = excluded.expected_benefit_score,
                risk_score              = excluded.risk_score,
                supporting_metrics      = excluded.supporting_metrics,
                created_at              = now()
            returning *
            """,
            (
                workspace_id, recommendation_key, policy_family, scope_type, scope_value,
                Jsonb(current_policy), Jsonb(recommended_policy), reason_code, confidence, sample_size,
                expected_benefit_score, risk_score, Jsonb(supporting_metrics or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("upsert_governance_policy_recommendation returned no row")
        return dict(row)


def insert_governance_policy_optimization_snapshot(
    conn,
    *,
    workspace_id: str,
    window_label: str = "30d",
    recommendation_count: int = 0,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_policy_optimization_snapshots
                (workspace_id, window_label, recommendation_count, metadata)
            values (%s::uuid, %s, %s, %s)
            returning *
            """,
            (workspace_id, window_label, recommendation_count, Jsonb(metadata or {})),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_governance_policy_optimization_snapshot returned no row")
        return dict(row)


def get_latest_governance_policy_optimization_snapshot(
    conn,
    *,
    workspace_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_policy_optimization_snapshots"
            " where workspace_id = %s::uuid"
            " order by snapshot_at desc"
            " limit 1",
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


# ── Phase 3.7B: Governance Policy Review + Promotion ─────────────────────────

def insert_governance_policy_review(
    conn,
    *,
    workspace_id: str,
    recommendation_key: str,
    policy_family: str,
    review_status: str,
    reviewed_by: str,
    review_reason: str | None = None,
    notes: str | None = None,
    metadata: dict | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_policy_recommendation_reviews
                (workspace_id, recommendation_key, policy_family, review_status,
                 review_reason, reviewed_by, notes, metadata)
            values (%s::uuid, %s, %s, %s, %s, %s, %s, %s)
            returning *
            """,
            (
                workspace_id, recommendation_key, policy_family, review_status,
                review_reason, reviewed_by, notes, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_governance_policy_review returned no row")
        return dict(row)


def list_governance_policy_review_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_policy_review_summary"
            " where workspace_id = %s::uuid"
            " order by latest_reviewed_at desc nulls last",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def insert_governance_policy_promotion_proposal(
    conn,
    *,
    workspace_id: str,
    recommendation_key: str,
    policy_family: str,
    promotion_target: str,
    scope_type: str,
    scope_value: str,
    current_policy: dict,
    recommended_policy: dict,
    proposed_by: str,
    proposal_reason: str | None = None,
    metadata: dict | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_policy_promotion_proposals
                (workspace_id, recommendation_key, policy_family, proposal_status,
                 promotion_target, scope_type, scope_value,
                 current_policy, recommended_policy,
                 proposed_by, proposal_reason, metadata)
            values (%s::uuid, %s, %s, 'pending', %s, %s, %s,
                    %s, %s,
                    %s, %s, %s)
            returning *
            """,
            (
                workspace_id, recommendation_key, policy_family,
                promotion_target, scope_type, scope_value,
                Jsonb(current_policy), Jsonb(recommended_policy),
                proposed_by, proposal_reason, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_governance_policy_promotion_proposal returned no row")
        return dict(row)


def get_governance_policy_promotion_proposal(
    conn,
    *,
    workspace_id: str,
    proposal_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_policy_promotion_proposals"
            " where workspace_id = %s::uuid and id = %s::uuid",
            (workspace_id, proposal_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def approve_governance_policy_promotion_proposal(
    conn,
    *,
    workspace_id: str,
    proposal_id: str,
    approved_by: str,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_policy_promotion_proposals
               set proposal_status = 'approved',
                   approved_by     = %s,
                   approved_at     = now()
             where workspace_id = %s::uuid
               and id            = %s::uuid
               and proposal_status = 'pending'
            returning *
            """,
            (approved_by, workspace_id, proposal_id),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("approve_governance_policy_promotion_proposal: not found or not in pending state")
        return dict(row)


def list_governance_policy_promotion_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_policy_promotion_summary"
            " where workspace_id = %s::uuid"
            " order by latest_proposed_at desc nulls last",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def list_governance_policy_pending_promotion_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_policy_pending_promotion_summary"
            " where workspace_id = %s::uuid"
            " order by latest_proposed_at desc nulls last",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def insert_governance_policy_application(
    conn,
    *,
    workspace_id: str,
    proposal_id: str,
    recommendation_key: str,
    policy_family: str,
    applied_target: str,
    applied_scope_type: str,
    applied_scope_value: str,
    prior_policy: dict,
    applied_policy: dict,
    applied_by: str,
    rollback_candidate: bool = True,
    metadata: dict | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_policy_applications
                (workspace_id, proposal_id, recommendation_key, policy_family,
                 applied_target, applied_scope_type, applied_scope_value,
                 prior_policy, applied_policy,
                 applied_by, rollback_candidate, metadata)
            values (%s::uuid, %s::uuid, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s)
            returning *
            """,
            (
                workspace_id, proposal_id, recommendation_key, policy_family,
                applied_target, applied_scope_type, applied_scope_value,
                Jsonb(prior_policy), Jsonb(applied_policy),
                applied_by, rollback_candidate, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_governance_policy_application returned no row")
        return dict(row)


def mark_governance_policy_proposal_applied(
    conn,
    *,
    workspace_id: str,
    proposal_id: str,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.governance_policy_promotion_proposals
               set proposal_status = 'applied',
                   applied_at      = now()
             where workspace_id = %s::uuid
               and id            = %s::uuid
            returning *
            """,
            (workspace_id, proposal_id),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("mark_governance_policy_proposal_applied: not found")
        return dict(row)


# ── Phase 3.7C: Governance Policy Autopromotion ───────────────────────────────

def upsert_governance_policy_autopromotion_policy(
    conn,
    *,
    workspace_id: str,
    policy_family: str,
    scope_type: str,
    scope_value: str,
    promotion_target: str,
    min_confidence: str = "high",
    min_approved_review_count: int = 1,
    min_application_count: int = 1,
    min_sample_size: int = 5,
    max_recent_override_rate: float = 0.25,
    max_recent_reassignment_rate: float = 0.25,
    cooldown_hours: int = 72,
    enabled: bool = True,
    created_by: str = "ops",
    metadata: dict | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_policy_autopromotion_policies
                (workspace_id, enabled, policy_family, scope_type, scope_value, promotion_target,
                 min_confidence, min_approved_review_count, min_application_count, min_sample_size,
                 max_recent_override_rate, max_recent_reassignment_rate,
                 cooldown_hours, created_by, metadata)
            values (%s::uuid, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s, %s, %s)
            on conflict (workspace_id, policy_family, scope_type, scope_value, promotion_target)
            do update set
                enabled                     = excluded.enabled,
                min_confidence              = excluded.min_confidence,
                min_approved_review_count   = excluded.min_approved_review_count,
                min_application_count       = excluded.min_application_count,
                min_sample_size             = excluded.min_sample_size,
                max_recent_override_rate    = excluded.max_recent_override_rate,
                max_recent_reassignment_rate = excluded.max_recent_reassignment_rate,
                cooldown_hours              = excluded.cooldown_hours,
                metadata                    = excluded.metadata
            returning *
            """,
            (
                workspace_id, enabled, policy_family, scope_type, scope_value, promotion_target,
                min_confidence, min_approved_review_count, min_application_count, min_sample_size,
                max_recent_override_rate, max_recent_reassignment_rate,
                cooldown_hours, created_by, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("upsert_governance_policy_autopromotion_policy returned no row")
        return dict(row)


def list_governance_policy_autopromotion_summary(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_policy_autopromotion_summary"
            " where workspace_id = %s::uuid"
            " order by latest_execution_at desc nulls last, policy_id",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def list_governance_policy_autopromotion_eligibility(
    conn,
    *,
    workspace_id: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_policy_autopromotion_eligibility"
            " where workspace_id = %s::uuid"
            " order by eligible desc, sample_size desc nulls last",
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def insert_governance_policy_autopromotion_execution(
    conn,
    *,
    workspace_id: str,
    recommendation_key: str,
    policy_id: str,
    policy_family: str,
    promotion_target: str,
    scope_type: str,
    scope_value: str,
    current_policy: dict,
    applied_policy: dict,
    executed_by: str = "system",
    cooldown_applied: bool = False,
    metadata: dict | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_policy_autopromotion_executions
                (workspace_id, recommendation_key, policy_id, policy_family,
                 promotion_target, scope_type, scope_value,
                 current_policy, applied_policy,
                 executed_by, cooldown_applied, metadata)
            values (%s::uuid, %s, %s::uuid, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s)
            returning *
            """,
            (
                workspace_id, recommendation_key, policy_id, policy_family,
                promotion_target, scope_type, scope_value,
                Jsonb(current_policy), Jsonb(applied_policy),
                executed_by, cooldown_applied, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_governance_policy_autopromotion_execution returned no row")
        return dict(row)


def insert_governance_policy_autopromotion_rollback_candidate(
    conn,
    *,
    workspace_id: str,
    execution_id: str,
    recommendation_key: str,
    policy_family: str,
    scope_type: str,
    scope_value: str,
    target_type: str,
    prior_policy: dict,
    applied_policy: dict,
    rollback_reason_code: str | None = None,
    rollback_risk_score: float = 0.0,
    metadata: dict | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.governance_policy_autopromotion_rollback_candidates
                (workspace_id, execution_id, recommendation_key, policy_family,
                 scope_type, scope_value, target_type,
                 prior_policy, applied_policy,
                 rollback_reason_code, rollback_risk_score, metadata)
            values (%s::uuid, %s::uuid, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s)
            returning *
            """,
            (
                workspace_id, execution_id, recommendation_key, policy_family,
                scope_type, scope_value, target_type,
                Jsonb(prior_policy), Jsonb(applied_policy),
                rollback_reason_code, rollback_risk_score, Jsonb(metadata or {}),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("insert_governance_policy_autopromotion_rollback_candidate returned no row")
        return dict(row)


def list_governance_policy_autopromotion_rollback_candidates(
    conn,
    *,
    workspace_id: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "select * from public.governance_policy_autopromotion_rollback_candidates"
            " where workspace_id = %s::uuid"
            " order by created_at desc"
            " limit %s",
            (workspace_id, limit),
        )
        return [dict(r) for r in cur.fetchall()]


# ── Phase 4.0A: Multi-Asset Data Foundation ─────────────────────────────
def get_asset_id_by_symbol(conn, symbol: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "select id::text as id from public.assets where symbol = %s limit 1",
            (symbol,),
        )
        row = cur.fetchone()
        return dict(row)["id"] if row else None


def upsert_asset_universe_catalog_rows(conn, rows: Sequence[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.asset_universe_catalog (
                symbol, canonical_symbol, asset_class,
                venue, quote_currency, base_currency, region,
                is_active, metadata
            ) values (
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s::jsonb
            )
            on conflict (canonical_symbol, asset_class, coalesce(venue, ''))
            do update set
                is_active = excluded.is_active,
                metadata  = public.asset_universe_catalog.metadata || excluded.metadata
            """,
            [
                (
                    row["symbol"],
                    row["canonical_symbol"],
                    row["asset_class"],
                    row.get("venue"),
                    row.get("quote_currency"),
                    row.get("base_currency"),
                    row.get("region"),
                    row.get("is_active", True),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
        return len(rows)


def persist_multi_asset_market_state_rows(
    conn,
    *,
    workspace_id: str,  # noqa: ARG001 - reserved for future per-workspace state tables
    bar_rows: Sequence[dict[str, Any]] | None = None,
    macro_rows: Sequence[dict[str, Any]] | None = None,
) -> dict[str, int]:
    """Dispatcher that persists bar rows to market_bars and macro rows to
    macro_series_points via the existing upsert helpers."""
    counts = {"bars": 0, "macro_points": 0}
    if bar_rows:
        upsert_market_bars_rows(conn, list(bar_rows))
        counts["bars"] = len(bar_rows)
    if macro_rows:
        upsert_macro_series_points_rows(conn, list(macro_rows))
        counts["macro_points"] = len(macro_rows)
    return counts


def get_multi_asset_sync_health_summary(conn, workspace_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text           as workspace_id,
                provider_family,
                asset_class,
                requested_symbol_count,
                synced_symbol_count,
                failed_symbol_count,
                latest_run_started_at,
                latest_run_completed_at,
                latest_status,
                latest_provider_mode,
                latest_metadata
            from public.multi_asset_sync_health_summary
            where workspace_id = %s::uuid
            order by latest_run_started_at desc nulls last, asset_class asc
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_normalized_multi_asset_market_state(
    conn,
    workspace_id: str,
    symbols: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    symbol_list = list(symbols) if symbols else None
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text    as workspace_id,
                symbol,
                canonical_symbol,
                asset_class,
                provider_family,
                price,
                price_timestamp,
                volume_24h,
                oi_change_1h,
                funding_rate,
                yield_value,
                fx_return_1d,
                macro_proxy_value,
                liquidation_count,
                metadata
            from public.normalized_multi_asset_market_state
            where workspace_id = %s::uuid
              and (%s::text[] is null or canonical_symbol = any(%s::text[]))
            order by asset_class asc, canonical_symbol asc
            """,
            (workspace_id, symbol_list, symbol_list),
        )
        return [dict(r) for r in cur.fetchall()]


def get_multi_asset_family_state_summary(conn, workspace_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                asset_class,
                family_key,
                symbol_count,
                latest_timestamp,
                avg_return_1d,
                avg_volatility_proxy,
                metadata
            from public.multi_asset_family_state_summary
            where workspace_id = %s::uuid
            order by asset_class asc
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


# ----- Phase 4.0B: Dependency Graph + Context Model -----
def list_asset_dependency_graph(
    conn, from_symbols=None,
):
    with conn.cursor() as cur:
        if from_symbols:
            cur.execute(
                """
                select id::text as id, from_symbol, to_symbol, dependency_type,
                       dependency_family, priority, weight, is_active, metadata
                from public.asset_dependency_graph
                where is_active = true
                  and from_symbol = any(%s::text[])
                order by from_symbol asc, priority desc, weight desc, to_symbol asc
                """,
                (list(from_symbols),),
            )
        else:
            cur.execute(
                """
                select id::text as id, from_symbol, to_symbol, dependency_type,
                       dependency_family, priority, weight, is_active, metadata
                from public.asset_dependency_graph
                where is_active = true
                order by from_symbol asc, priority desc, weight desc, to_symbol asc
                """
            )
        return [dict(r) for r in cur.fetchall()]


def list_watchlist_dependency_profile(
    conn, *, workspace_id, watchlist_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name,
                   include_macro, include_fx, include_rates,
                   include_equity_index, include_commodity, include_crypto_cross,
                   max_dependencies, is_active, metadata,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
                   created_at
            from public.watchlist_dependency_profiles
            where workspace_id = %s::uuid
              and watchlist_id = %s::uuid
              and is_active    = true
            order by created_at desc
            limit 1
            """,
            (workspace_id, watchlist_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def list_asset_family_mappings(
    conn, symbols=None,
):
    with conn.cursor() as cur:
        if symbols:
            cur.execute(
                """
                select id::text as id, symbol, asset_class, family_key, family_label,
                       region, is_active, metadata, created_at
                from public.asset_family_mappings
                where is_active = true
                  and symbol = any(%s::text[])
                order by symbol asc, family_key asc
                """,
                (list(symbols),),
            )
        else:
            cur.execute(
                """
                select id::text as id, symbol, asset_class, family_key, family_label,
                       region, is_active, metadata, created_at
                from public.asset_family_mappings
                where is_active = true
                order by symbol asc, family_key asc
                """
            )
        return [dict(r) for r in cur.fetchall()]


def insert_watchlist_context_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    profile_id,
    primary_symbols,
    dependency_symbols,
    dependency_families,
    context_hash,
    coverage_summary,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.watchlist_context_snapshots (
                workspace_id, watchlist_id, profile_id,
                primary_symbols, dependency_symbols, dependency_families,
                context_hash, coverage_summary, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid,
                %s::jsonb, %s::jsonb, %s::jsonb,
                %s, %s::jsonb, %s::jsonb
            )
            returning id::text as id, snapshot_at
            """,
            (
                workspace_id,
                watchlist_id,
                profile_id,
                json.dumps(list(primary_symbols)),
                json.dumps(list(dependency_symbols)),
                json.dumps(list(dependency_families)),
                context_hash,
                json.dumps(_json_compatible(coverage_summary or {})),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("watchlist_context_snapshots insert returned no row")
        return dict(row)


def get_latest_watchlist_context_snapshot(
    conn, *, workspace_id, watchlist_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
                   profile_id::text as profile_id,
                   snapshot_at, primary_symbols, dependency_symbols,
                   dependency_families, context_hash,
                   coverage_summary, metadata
            from public.watchlist_context_snapshots
            where workspace_id = %s::uuid
              and watchlist_id = %s::uuid
            order by snapshot_at desc
            limit 1
            """,
            (workspace_id, watchlist_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_watchlist_dependency_coverage_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                context_hash,
                primary_symbol_count,
                dependency_symbol_count,
                dependency_family_count,
                covered_dependency_count,
                missing_dependency_count,
                stale_dependency_count,
                latest_context_snapshot_at,
                coverage_ratio,
                metadata
            from public.watchlist_dependency_coverage_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by latest_context_snapshot_at desc nulls last
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_watchlist_dependency_context_detail(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                context_hash,
                symbol,
                asset_class,
                dependency_family,
                dependency_type,
                priority,
                weight,
                is_primary,
                latest_timestamp,
                is_missing,
                is_stale,
                metadata
            from public.watchlist_dependency_context_detail
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by is_primary desc, priority desc nulls last, symbol asc
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_watchlist_dependency_family_state(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                context_hash,
                dependency_family,
                symbol_count,
                covered_count,
                missing_count,
                stale_count,
                latest_timestamp,
                metadata
            from public.watchlist_dependency_family_state
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by dependency_family asc
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.0C: Cross-Asset Signal Expansion -----
def insert_cross_asset_feature_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_feature_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                feature_family, feature_key, feature_value, feature_state,
                dependency_symbols, dependency_families, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s, %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["feature_family"], row["feature_key"],
                    row["feature_value"], row["feature_state"],
                    json.dumps(list(row.get("dependency_symbols") or [])),
                    json.dumps(list(row.get("dependency_families") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_signal_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_signal_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                signal_family, signal_key, signal_value, signal_direction,
                signal_state, base_symbol,
                dependency_symbols, dependency_families, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s, %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["signal_family"], row["signal_key"],
                    row["signal_value"], row.get("signal_direction"),
                    row["signal_state"], row.get("base_symbol"),
                    json.dumps(list(row.get("dependency_symbols") or [])),
                    json.dumps(list(row.get("dependency_families") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_signal_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                run_id::text       as run_id,
                context_snapshot_id::text as context_snapshot_id,
                signal_family, signal_key,
                signal_value, signal_direction, signal_state,
                base_symbol,
                dependency_symbol_count, dependency_family_count,
                created_at
            from public.cross_asset_signal_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, signal_family asc, signal_key asc
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_dependency_health_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                feature_count, signal_count,
                missing_dependency_count, stale_dependency_count,
                confirmed_count, contradicted_count,
                latest_created_at
            from public.cross_asset_dependency_health_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by dependency_family asc
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_context_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text       as run_id,
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_feature_count,
                cross_asset_signal_count,
                confirmed_signal_count,
                contradicted_signal_count,
                missing_context_count,
                stale_context_count,
                dominant_dependency_family,
                created_at
            from public.run_cross_asset_context_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_normalized_multi_asset_market_state_for_symbols(
    conn, *, workspace_id, symbols,
):
    if not symbols:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                symbol, canonical_symbol, asset_class,
                provider_family, price, price_timestamp,
                volume_24h, oi_change_1h, funding_rate,
                yield_value, fx_return_1d, macro_proxy_value,
                liquidation_count, metadata
            from public.normalized_multi_asset_market_state
            where workspace_id = %s::uuid
              and canonical_symbol = any(%s::text[])
            """,
            (workspace_id, list(symbols)),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.0D: Cross-Asset Explainability -----
def insert_cross_asset_explanation_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dominant_dependency_family,
    cross_asset_confidence_score,
    confirmation_score,
    contradiction_score,
    missing_context_score,
    stale_context_score,
    top_confirming_symbols,
    top_contradicting_symbols,
    missing_dependency_symbols,
    stale_dependency_symbols,
    explanation_state,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_explanation_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dominant_dependency_family,
                cross_asset_confidence_score,
                confirmation_score, contradiction_score,
                missing_context_score, stale_context_score,
                top_confirming_symbols, top_contradicting_symbols,
                missing_dependency_symbols, stale_dependency_symbols,
                explanation_state, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s,
                %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb,
                %s::jsonb, %s::jsonb,
                %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dominant_dependency_family,
                cross_asset_confidence_score,
                confirmation_score, contradiction_score,
                missing_context_score, stale_context_score,
                json.dumps(list(top_confirming_symbols or [])),
                json.dumps(list(top_contradicting_symbols or [])),
                json.dumps(list(missing_dependency_symbols or [])),
                json.dumps(list(stale_dependency_symbols or [])),
                explanation_state,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_explanation_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_contribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_contribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                family_signal_count,
                confirmed_count, contradicted_count,
                missing_count, stale_count,
                family_confidence_score, family_support_score, family_contradiction_score,
                top_symbols, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["dependency_family"],
                    int(row.get("family_signal_count") or 0),
                    int(row.get("confirmed_count") or 0),
                    int(row.get("contradicted_count") or 0),
                    int(row.get("missing_count") or 0),
                    int(row.get("stale_count") or 0),
                    row.get("family_confidence_score"),
                    row.get("family_support_score"),
                    row.get("family_contradiction_score"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_explanation_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dominant_dependency_family,
                cross_asset_confidence_score,
                confirmation_score, contradiction_score,
                missing_context_score, stale_context_score,
                top_confirming_symbols, top_contradicting_symbols,
                missing_dependency_symbols, stale_dependency_symbols,
                explanation_state,
                created_at
            from public.cross_asset_explanation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_explanation_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                family_signal_count,
                confirmed_count, contradicted_count,
                missing_count, stale_count,
                family_confidence_score, family_support_score, family_contradiction_score,
                top_symbols,
                created_at
            from public.cross_asset_family_explanation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_explanation_bridge(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                dominant_dependency_family,
                cross_asset_confidence_score,
                confirmation_score, contradiction_score,
                missing_context_score, stale_context_score,
                explanation_state,
                created_at
            from public.run_cross_asset_explanation_bridge
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_latest_cross_asset_signal_summary(
    conn, *, workspace_id, watchlist_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                signal_family, signal_key,
                signal_value, signal_direction, signal_state,
                base_symbol,
                dependency_symbol_count, dependency_family_count,
                created_at
            from public.cross_asset_signal_summary
            where workspace_id = %s::uuid
              and watchlist_id = %s::uuid
            order by created_at desc
            limit 200
            """,
            (workspace_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_latest_cross_asset_dependency_health_summary(
    conn, *, workspace_id, watchlist_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                feature_count, signal_count,
                missing_dependency_count, stale_dependency_count,
                confirmed_count, contradicted_count,
                latest_created_at
            from public.cross_asset_dependency_health_summary
            where workspace_id = %s::uuid
              and watchlist_id = %s::uuid
            order by dependency_family asc
            """,
            (workspace_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.1A: Cross-Asset Attribution + Composite Integration -----
def insert_cross_asset_attribution_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    base_signal_score,
    cross_asset_signal_score,
    cross_asset_confirmation_score,
    cross_asset_contradiction_penalty,
    cross_asset_missing_penalty,
    cross_asset_stale_penalty,
    cross_asset_net_contribution,
    composite_pre_cross_asset,
    composite_post_cross_asset,
    integration_mode,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                base_signal_score,
                cross_asset_signal_score,
                cross_asset_confirmation_score,
                cross_asset_contradiction_penalty,
                cross_asset_missing_penalty,
                cross_asset_stale_penalty,
                cross_asset_net_contribution,
                composite_pre_cross_asset,
                composite_post_cross_asset,
                integration_mode,
                metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                base_signal_score,
                cross_asset_signal_score,
                cross_asset_confirmation_score,
                cross_asset_contradiction_penalty,
                cross_asset_missing_penalty,
                cross_asset_stale_penalty,
                cross_asset_net_contribution,
                composite_pre_cross_asset,
                composite_post_cross_asset,
                integration_mode,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_attribution_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                family_signal_score,
                family_confirmation_score,
                family_contradiction_penalty,
                family_missing_penalty,
                family_stale_penalty,
                family_net_contribution,
                family_rank,
                top_symbols,
                metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s::jsonb,
                %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["dependency_family"],
                    row.get("family_signal_score"),
                    row.get("family_confirmation_score"),
                    row.get("family_contradiction_penalty"),
                    row.get("family_missing_penalty"),
                    row.get("family_stale_penalty"),
                    row.get("family_net_contribution"),
                    row.get("family_rank"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                base_signal_score,
                cross_asset_signal_score,
                cross_asset_confirmation_score,
                cross_asset_contradiction_penalty,
                cross_asset_missing_penalty,
                cross_asset_stale_penalty,
                cross_asset_net_contribution,
                composite_pre_cross_asset,
                composite_post_cross_asset,
                integration_mode,
                created_at
            from public.cross_asset_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                family_signal_score,
                family_confirmation_score,
                family_contradiction_penalty,
                family_missing_penalty,
                family_stale_penalty,
                family_net_contribution,
                family_rank,
                top_symbols,
                created_at
            from public.cross_asset_family_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_composite_integration_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text       as run_id,
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                base_signal_score,
                cross_asset_signal_score,
                cross_asset_net_contribution,
                composite_pre_cross_asset,
                composite_post_cross_asset,
                dominant_dependency_family,
                cross_asset_confidence_score,
                created_at
            from public.run_composite_integration_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_latest_base_run_scores(
    conn, *, workspace_id, run_id,  # noqa: ARG001 - run_id reserved; composite_scores is not run-keyed
):
    with conn.cursor() as cur:
        cur.execute(
            """
            with latest_per_asset as (
                select distinct on (asset_id)
                    asset_id, long_score, short_score, as_of
                from public.composite_scores
                where workspace_id = %s::uuid
                order by asset_id, as_of desc
            )
            select
                avg(long_score - short_score)::numeric as avg_net,
                max(as_of) as latest_as_of,
                count(*) as asset_count
            from latest_per_asset
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_latest_cross_asset_explanation_for_run(
    conn, *, workspace_id, run_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dominant_dependency_family,
                cross_asset_confidence_score,
                confirmation_score, contradiction_score,
                missing_context_score, stale_context_score,
                top_confirming_symbols, top_contradicting_symbols,
                missing_dependency_symbols, stale_dependency_symbols,
                explanation_state,
                created_at
            from public.cross_asset_explanation_summary
            where workspace_id = %s::uuid
              and run_id       = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 1
            """,
            (workspace_id, run_id, watchlist_id, watchlist_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None



# ----- Phase 4.1B: Dependency-Priority-Aware Ranking + Contribution Weighting -----
def list_dependency_weighting_profiles(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   priority_weight_scale,
                   direct_dependency_bonus, secondary_dependency_penalty,
                   missing_penalty_scale, stale_penalty_scale,
                   family_weight_overrides, type_weight_overrides,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.dependency_weighting_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_dependency_weighting_profile(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   priority_weight_scale,
                   direct_dependency_bonus, secondary_dependency_penalty,
                   missing_penalty_scale, stale_penalty_scale,
                   family_weight_overrides, type_weight_overrides,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.dependency_weighting_profiles
            where workspace_id = %s::uuid and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_family_weighted_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    weighting_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_weighted_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                weighting_profile_id,
                dependency_family,
                raw_family_net_contribution,
                priority_weight, family_weight, type_weight, coverage_weight,
                weighted_family_net_contribution,
                weighted_family_rank,
                top_symbols, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s,
                %s,
                %s, %s, %s, %s,
                %s,
                %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    weighting_profile_id,
                    row["dependency_family"],
                    row.get("raw_family_net_contribution"),
                    row.get("priority_weight"),
                    row.get("family_weight"),
                    row.get("type_weight"),
                    row.get("coverage_weight"),
                    row.get("weighted_family_net_contribution"),
                    row.get("weighted_family_rank"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_symbol_weighted_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    weighting_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_symbol_weighted_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                weighting_profile_id,
                symbol, dependency_family, dependency_type,
                graph_priority, is_direct_dependency,
                raw_symbol_score,
                priority_weight, family_weight, type_weight, coverage_weight,
                weighted_symbol_score,
                symbol_rank, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s, %s, %s,
                %s, %s,
                %s,
                %s, %s, %s, %s,
                %s,
                %s, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    weighting_profile_id,
                    row["symbol"],
                    row["dependency_family"],
                    row.get("dependency_type"),
                    row.get("graph_priority"),
                    bool(row.get("is_direct_dependency", True)),
                    row.get("raw_symbol_score"),
                    row.get("priority_weight"),
                    row.get("family_weight"),
                    row.get("type_weight"),
                    row.get("coverage_weight"),
                    row.get("weighted_symbol_score"),
                    row.get("symbol_rank"),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_family_weighted_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                raw_family_net_contribution,
                priority_weight, family_weight, type_weight, coverage_weight,
                weighted_family_net_contribution,
                weighted_family_rank,
                top_symbols, created_at
            from public.cross_asset_family_weighted_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, weighted_family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_symbol_weighted_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                symbol, dependency_family, dependency_type,
                graph_priority, is_direct_dependency,
                raw_symbol_score,
                priority_weight, family_weight, type_weight, coverage_weight,
                weighted_symbol_score, symbol_rank, created_at
            from public.cross_asset_symbol_weighted_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, symbol_rank asc, symbol asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_weighted_integration_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                created_at
            from public.run_cross_asset_weighted_integration_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_dependency_edges_for_context(
    conn, *, workspace_id=None, watchlist_id=None, run_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        if watchlist_id:
            cur.execute(
                """
                with primaries as (
                    select distinct a.symbol
                    from public.watchlist_assets wa
                    join public.assets a on a.id = wa.asset_id
                    where wa.watchlist_id = %s::uuid
                )
                select adg.from_symbol, adg.to_symbol, adg.dependency_type,
                       adg.dependency_family, adg.priority, adg.weight,
                       adg.is_active, adg.metadata
                from public.asset_dependency_graph adg
                join primaries p on p.symbol = adg.from_symbol
                where adg.is_active = true
                order by adg.priority desc, adg.from_symbol asc, adg.to_symbol asc
                """,
                (watchlist_id,),
            )
        else:
            cur.execute(
                """
                select from_symbol, to_symbol, dependency_type,
                       dependency_family, priority, weight,
                       is_active, metadata
                from public.asset_dependency_graph
                where is_active = true
                order by priority desc, from_symbol asc, to_symbol asc
                """
            )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.1C: Regime-Aware Cross-Asset Interpretation -----
def list_regime_cross_asset_interpretation_profiles(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, regime_key, is_active,
                   family_weight_overrides, type_weight_overrides,
                   confirmation_scale, contradiction_scale,
                   missing_penalty_scale, stale_penalty_scale,
                   dominance_threshold, metadata, created_at,
                   workspace_id::text as workspace_id
            from public.regime_cross_asset_interpretation_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 50
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_regime_cross_asset_interpretation_profile(
    conn, *, workspace_id, regime_key,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, regime_key, is_active,
                   family_weight_overrides, type_weight_overrides,
                   confirmation_scale, contradiction_scale,
                   missing_penalty_scale, stale_penalty_scale,
                   dominance_threshold, metadata, created_at,
                   workspace_id::text as workspace_id
            from public.regime_cross_asset_interpretation_profiles
            where workspace_id = %s::uuid
              and regime_key   = %s
              and is_active    = true
            order by created_at desc
            limit 1
            """,
            (workspace_id, regime_key),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_family_regime_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    regime_key,
    interpretation_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_regime_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                regime_key, interpretation_profile_id,
                dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_family_weight, regime_type_weight,
                regime_confirmation_scale, regime_contradiction_scale,
                regime_missing_penalty_scale, regime_stale_penalty_scale,
                regime_adjusted_family_contribution,
                regime_family_rank,
                interpretation_state,
                top_symbols, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s::uuid,
                %s,
                %s,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s,
                %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    regime_key, interpretation_profile_id,
                    row["dependency_family"],
                    row.get("raw_family_net_contribution"),
                    row.get("weighted_family_net_contribution"),
                    row.get("regime_family_weight"),
                    row.get("regime_type_weight"),
                    row.get("regime_confirmation_scale"),
                    row.get("regime_contradiction_scale"),
                    row.get("regime_missing_penalty_scale"),
                    row.get("regime_stale_penalty_scale"),
                    row.get("regime_adjusted_family_contribution"),
                    row.get("regime_family_rank"),
                    row.get("interpretation_state", "computed"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_symbol_regime_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    regime_key,
    interpretation_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_symbol_regime_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                regime_key, interpretation_profile_id,
                symbol, dependency_family, dependency_type,
                graph_priority, is_direct_dependency,
                raw_symbol_score, weighted_symbol_score,
                regime_family_weight, regime_type_weight,
                regime_adjusted_symbol_score, symbol_rank, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s::uuid,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    regime_key, interpretation_profile_id,
                    row["symbol"],
                    row["dependency_family"],
                    row.get("dependency_type"),
                    row.get("graph_priority"),
                    bool(row.get("is_direct_dependency", True)),
                    row.get("raw_symbol_score"),
                    row.get("weighted_symbol_score"),
                    row.get("regime_family_weight"),
                    row.get("regime_type_weight"),
                    row.get("regime_adjusted_symbol_score"),
                    row.get("symbol_rank"),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_family_regime_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                regime_key,
                dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_family_weight, regime_type_weight,
                regime_confirmation_scale, regime_contradiction_scale,
                regime_missing_penalty_scale, regime_stale_penalty_scale,
                regime_adjusted_family_contribution,
                regime_family_rank,
                interpretation_state,
                top_symbols, created_at
            from public.cross_asset_family_regime_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, regime_family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_symbol_regime_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                regime_key,
                symbol, dependency_family, dependency_type,
                graph_priority, is_direct_dependency,
                raw_symbol_score, weighted_symbol_score,
                regime_family_weight, regime_type_weight,
                regime_adjusted_symbol_score, symbol_rank, created_at
            from public.cross_asset_symbol_regime_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, symbol_rank asc, symbol asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_regime_integration_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                regime_key,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                cross_asset_confidence_score,
                created_at
            from public.run_cross_asset_regime_integration_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_run_regime(
    conn, *, run_id=None, workspace_id=None, watchlist_id=None,
):
    with conn.cursor() as cur:
        if run_id:
            cur.execute(
                """
                select run_id::text as run_id,
                       workspace_id::text as workspace_id,
                       watchlist_id::text as watchlist_id,
                       from_regime, to_regime,
                       transition_classification,
                       stability_score, anomaly_likelihood,
                       composite_shift, composite_shift_abs,
                       dominant_family_gained, dominant_family_lost,
                       created_at
                from public.regime_transition_events
                where run_id = %s::uuid
                limit 1
                """,
                (run_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
        cur.execute(
            """
            select run_id::text as run_id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
                   from_regime, to_regime,
                   transition_classification,
                   stability_score, anomaly_likelihood,
                   composite_shift, composite_shift_abs,
                   dominant_family_gained, dominant_family_lost,
                   created_at
            from public.regime_transition_events
            where (%s::uuid is null or workspace_id = %s::uuid)
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 1
            """,
            (workspace_id, workspace_id, watchlist_id, watchlist_id),
        )
        row = cur.fetchone()
        return dict(row) if row else None



# ----- Phase 4.1D: Cross-Asset Replay + Stability Validation -----
def insert_cross_asset_replay_validation_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    source_context_snapshot_id,
    replay_context_snapshot_id,
    source_regime_key,
    replay_regime_key,
    context_hash_match,
    regime_match,
    raw_attribution_match,
    weighted_attribution_match,
    regime_attribution_match,
    dominant_family_match,
    weighted_dominant_family_match,
    regime_dominant_family_match,
    raw_delta,
    weighted_delta,
    regime_delta,
    drift_reason_codes,
    validation_state,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_replay_validation_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                context_hash_match, regime_match,
                raw_attribution_match, weighted_attribution_match, regime_attribution_match,
                dominant_family_match, weighted_dominant_family_match, regime_dominant_family_match,
                raw_delta, weighted_delta, regime_delta,
                drift_reason_codes, validation_state, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s::uuid,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb,
                %s::jsonb, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                bool(context_hash_match), bool(regime_match),
                bool(raw_attribution_match), bool(weighted_attribution_match),
                bool(regime_attribution_match),
                bool(dominant_family_match), bool(weighted_dominant_family_match),
                bool(regime_dominant_family_match),
                json.dumps(_json_compatible(raw_delta or {})),
                json.dumps(_json_compatible(weighted_delta or {})),
                json.dumps(_json_compatible(regime_delta or {})),
                json.dumps(list(drift_reason_codes or [])),
                validation_state,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_replay_validation_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_replay_stability_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_replay_stability_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                dependency_family,
                source_raw_contribution, replay_raw_contribution,
                source_weighted_contribution, replay_weighted_contribution,
                source_regime_contribution, replay_regime_contribution,
                raw_delta, weighted_delta, regime_delta,
                family_rank_match, weighted_family_rank_match, regime_family_rank_match,
                drift_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, source_run_id, replay_run_id,
                    row["dependency_family"],
                    row.get("source_raw_contribution"),
                    row.get("replay_raw_contribution"),
                    row.get("source_weighted_contribution"),
                    row.get("replay_weighted_contribution"),
                    row.get("source_regime_contribution"),
                    row.get("replay_regime_contribution"),
                    row.get("raw_delta"),
                    row.get("weighted_delta"),
                    row.get("regime_delta"),
                    bool(row.get("family_rank_match", False)),
                    bool(row.get("weighted_family_rank_match", False)),
                    bool(row.get("regime_family_rank_match", False)),
                    json.dumps(list(row.get("drift_reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_replay_validation_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text              as workspace_id,
                watchlist_id::text              as watchlist_id,
                source_run_id::text             as source_run_id,
                replay_run_id::text             as replay_run_id,
                source_context_snapshot_id::text as source_context_snapshot_id,
                replay_context_snapshot_id::text as replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                context_hash_match, regime_match,
                raw_attribution_match, weighted_attribution_match, regime_attribution_match,
                dominant_family_match, weighted_dominant_family_match, regime_dominant_family_match,
                drift_reason_codes, validation_state, created_at
            from public.cross_asset_replay_validation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_replay_stability_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text      as workspace_id,
                watchlist_id::text      as watchlist_id,
                source_run_id::text     as source_run_id,
                replay_run_id::text     as replay_run_id,
                dependency_family,
                source_raw_contribution, replay_raw_contribution,
                source_weighted_contribution, replay_weighted_contribution,
                source_regime_contribution, replay_regime_contribution,
                raw_delta, weighted_delta, regime_delta,
                family_rank_match, weighted_family_rank_match, regime_family_rank_match,
                drift_reason_codes, created_at
            from public.cross_asset_family_replay_stability_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_replay_stability_aggregate(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                validation_count,
                context_match_rate, regime_match_rate,
                raw_match_rate, weighted_match_rate,
                regime_match_rate_attribution,
                dominant_family_match_rate,
                weighted_dominant_family_match_rate,
                regime_dominant_family_match_rate,
                drift_detected_count,
                latest_validated_at
            from public.cross_asset_replay_stability_aggregate
            where workspace_id = %s::uuid
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_replay_pair_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text            as replay_run_id,
                   replayed_from_run_id::text as source_run_id,
                   workspace_id::text  as workspace_id,
                   watchlist_id::text  as watchlist_id,
                   is_replay
            from public.job_runs
            where id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_context_snapshot_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id, profile_id::text as profile_id,
                   snapshot_at, context_hash,
                   primary_symbols, dependency_symbols, dependency_families,
                   coverage_summary, metadata
            from public.watchlist_context_snapshots
            where id = (
                select context_snapshot_id
                from public.cross_asset_attribution_summary
                where run_id = %s::uuid
                limit 1
            )
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_cross_asset_attribution_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
                   context_snapshot_id::text as context_snapshot_id,
                   base_signal_score,
                   cross_asset_signal_score,
                   cross_asset_confirmation_score,
                   cross_asset_contradiction_penalty,
                   cross_asset_missing_penalty,
                   cross_asset_stale_penalty,
                   cross_asset_net_contribution,
                   composite_pre_cross_asset,
                   composite_post_cross_asset,
                   integration_mode, created_at
            from public.cross_asset_attribution_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_cross_asset_weighted_attribution_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
                   base_signal_score,
                   cross_asset_net_contribution,
                   weighted_cross_asset_net_contribution,
                   dominant_dependency_family,
                   weighted_dominant_dependency_family,
                   created_at
            from public.run_cross_asset_weighted_integration_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_cross_asset_regime_attribution_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
                   regime_key,
                   cross_asset_net_contribution,
                   weighted_cross_asset_net_contribution,
                   regime_adjusted_cross_asset_contribution,
                   dominant_dependency_family,
                   weighted_dominant_dependency_family,
                   regime_dominant_dependency_family,
                   cross_asset_confidence_score,
                   created_at
            from public.run_cross_asset_regime_integration_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None



# ----- Phase 4.2A: Cross-Asset Lead/Lag + Dependency Timing -----
def insert_cross_asset_lead_lag_pair_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_lead_lag_pair_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                base_symbol, dependency_symbol,
                dependency_family, dependency_type,
                lag_bucket, best_lag_hours,
                timing_strength, correlation_at_best_lag,
                base_return_series_key, dependency_return_series_key,
                window_label, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["base_symbol"], row["dependency_symbol"],
                    row["dependency_family"], row.get("dependency_type"),
                    row["lag_bucket"], row.get("best_lag_hours"),
                    row.get("timing_strength"), row.get("correlation_at_best_lag"),
                    row.get("base_return_series_key"),
                    row.get("dependency_return_series_key"),
                    row.get("window_label", "7d"),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_family_timing_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_timing_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                lead_pair_count, coincident_pair_count, lag_pair_count,
                avg_best_lag_hours, avg_timing_strength,
                dominant_timing_class, top_leading_symbols, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s, %s,
                %s, %s,
                %s, %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["dependency_family"],
                    int(row.get("lead_pair_count") or 0),
                    int(row.get("coincident_pair_count") or 0),
                    int(row.get("lag_pair_count") or 0),
                    row.get("avg_best_lag_hours"),
                    row.get("avg_timing_strength"),
                    row.get("dominant_timing_class", "insufficient_data"),
                    json.dumps(list(row.get("top_leading_symbols") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_lead_lag_pair_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                base_symbol, dependency_symbol,
                dependency_family, dependency_type,
                lag_bucket, best_lag_hours,
                timing_strength, correlation_at_best_lag,
                window_label, created_at
            from public.cross_asset_lead_lag_pair_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, base_symbol asc, dependency_symbol asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_timing_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                lead_pair_count, coincident_pair_count, lag_pair_count,
                avg_best_lag_hours, avg_timing_strength,
                dominant_timing_class, top_leading_symbols, created_at
            from public.cross_asset_family_timing_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 100
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_timing_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                lead_pair_count, coincident_pair_count, lag_pair_count,
                dominant_leading_family, strongest_leading_symbol,
                avg_timing_strength, created_at
            from public.run_cross_asset_timing_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_return_series_for_symbols(
    conn, *, workspace_id, symbols, window_label,  # noqa: ARG001
):
    """Diagnostic helper â€” returns the most recent return observations per
    symbol (across both market_bars and macro_series_points) for the given
    window. `window_label` is informational; the service controls the actual
    lookback via the lag-grid resolution decision."""
    if not symbols:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            with bar_rows as (
                select a.symbol as symbol, b.ts as ts, b.return_1h as ret,
                       'market_bars.return_1h' as source
                from public.market_bars b
                join public.assets a on a.id = b.asset_id
                where a.symbol = any(%s::text[])
                  and b.return_1h is not null
            ),
            macro_rows as (
                select auc.canonical_symbol as symbol, m.ts as ts,
                       coalesce(m.return_1d, m.change_1d) as ret,
                       'macro_series_points.return_1d' as source
                from public.macro_series_points m
                join public.asset_universe_catalog auc
                     on auc.metadata->>'series_code' = m.series_code
                where auc.canonical_symbol = any(%s::text[])
                  and coalesce(m.return_1d, m.change_1d) is not null
            )
            select * from (
                select symbol, ts, ret, source from bar_rows
                union all
                select symbol, ts, ret, source from macro_rows
            ) combined
            order by symbol asc, ts desc
            limit 500
            """,
            (list(symbols), list(symbols)),
        )
        return [dict(r) for r in cur.fetchall()]


def get_dependency_context_for_run(
    conn, *, run_id=None, workspace_id=None, watchlist_id=None,  # noqa: ARG001
):
    """Delegates to the 4.0B latest-context-snapshot lookup when a watchlist
    is provided. run_id is not currently used by 4.0B snapshots but kept in
    the signature for future run-keyed context extensions."""
    if workspace_id and watchlist_id:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id,
                       workspace_id::text as workspace_id,
                       watchlist_id::text as watchlist_id,
                       profile_id::text as profile_id,
                       snapshot_at, primary_symbols, dependency_symbols,
                       dependency_families, context_hash,
                       coverage_summary, metadata
                from public.watchlist_context_snapshots
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                order by snapshot_at desc
                limit 1
                """,
                (workspace_id, watchlist_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    return None



# ----- Phase 4.2B: Family-Level Lead/Lag Attribution -----
def list_cross_asset_timing_attribution_profiles(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   lead_weight, coincident_weight, lag_weight,
                   insufficient_data_weight,
                   lead_bonus_scale, lag_penalty_scale,
                   family_weight_overrides, metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_timing_attribution_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_timing_attribution_profile(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   lead_weight, coincident_weight, lag_weight,
                   insufficient_data_weight,
                   lead_bonus_scale, lag_penalty_scale,
                   family_weight_overrides, metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_timing_attribution_profiles
            where workspace_id = %s::uuid and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_family_timing_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    timing_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_timing_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                timing_profile_id,
                dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_adjusted_family_contribution,
                dominant_timing_class,
                lead_pair_count, coincident_pair_count, lag_pair_count,
                timing_class_weight, timing_bonus, timing_penalty,
                timing_adjusted_family_contribution,
                timing_family_rank,
                top_leading_symbols, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s,
                %s, %s, %s,
                %s,
                %s, %s, %s,
                %s, %s, %s,
                %s,
                %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    timing_profile_id,
                    row["dependency_family"],
                    row.get("raw_family_net_contribution"),
                    row.get("weighted_family_net_contribution"),
                    row.get("regime_adjusted_family_contribution"),
                    row.get("dominant_timing_class", "insufficient_data"),
                    int(row.get("lead_pair_count") or 0),
                    int(row.get("coincident_pair_count") or 0),
                    int(row.get("lag_pair_count") or 0),
                    row.get("timing_class_weight"),
                    row.get("timing_bonus"),
                    row.get("timing_penalty"),
                    row.get("timing_adjusted_family_contribution"),
                    row.get("timing_family_rank"),
                    json.dumps(list(row.get("top_leading_symbols") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_symbol_timing_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    timing_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_symbol_timing_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                timing_profile_id,
                symbol, dependency_family, dependency_type,
                lag_bucket, best_lag_hours,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score,
                timing_class_weight,
                timing_adjusted_symbol_score,
                symbol_rank, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s,
                %s,
                %s, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    timing_profile_id,
                    row["symbol"],
                    row["dependency_family"],
                    row.get("dependency_type"),
                    row.get("lag_bucket", "insufficient_data"),
                    row.get("best_lag_hours"),
                    row.get("raw_symbol_score"),
                    row.get("weighted_symbol_score"),
                    row.get("regime_adjusted_symbol_score"),
                    row.get("timing_class_weight"),
                    row.get("timing_adjusted_symbol_score"),
                    row.get("symbol_rank"),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_family_timing_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_adjusted_family_contribution,
                dominant_timing_class,
                lead_pair_count, coincident_pair_count, lag_pair_count,
                timing_class_weight, timing_bonus, timing_penalty,
                timing_adjusted_family_contribution,
                timing_family_rank,
                top_leading_symbols, created_at
            from public.cross_asset_family_timing_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, timing_family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_symbol_timing_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                symbol, dependency_family, dependency_type,
                lag_bucket, best_lag_hours,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score,
                timing_class_weight,
                timing_adjusted_symbol_score,
                symbol_rank, created_at
            from public.cross_asset_symbol_timing_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, symbol_rank asc, symbol asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_timing_attribution_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                created_at
            from public.run_cross_asset_timing_attribution_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_timing_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family,
                   lead_pair_count, coincident_pair_count, lag_pair_count,
                   avg_best_lag_hours, avg_timing_strength,
                   dominant_timing_class, top_leading_symbols
            from public.cross_asset_family_timing_summary
            where run_id = %s::uuid
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_pair_timing_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select base_symbol, dependency_symbol,
                   dependency_family, dependency_type,
                   lag_bucket, best_lag_hours,
                   timing_strength, correlation_at_best_lag
            from public.cross_asset_lead_lag_pair_summary
            where run_id = %s::uuid
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.2C: Timing-Aware Composite Refinement -----
def list_cross_asset_timing_integration_profiles(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active, integration_mode,
                   integration_weight,
                   lead_weight_scale, coincident_weight_scale,
                   lag_weight_scale, insufficient_data_weight_scale,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_timing_integration_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_timing_integration_profile(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active, integration_mode,
                   integration_weight,
                   lead_weight_scale, coincident_weight_scale,
                   lag_weight_scale, insufficient_data_weight_scale,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_timing_integration_profiles
            where workspace_id = %s::uuid and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_timing_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    timing_integration_profile_id,
    base_signal_score,
    cross_asset_net_contribution,
    weighted_cross_asset_net_contribution,
    regime_adjusted_cross_asset_contribution,
    timing_adjusted_cross_asset_contribution,
    composite_pre_timing,
    timing_net_contribution,
    composite_post_timing,
    dominant_timing_class,
    integration_mode,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_timing_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                timing_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                composite_pre_timing, timing_net_contribution, composite_post_timing,
                dominant_timing_class, integration_mode, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s, %s, %s,
                %s, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                timing_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                composite_pre_timing, timing_net_contribution, composite_post_timing,
                dominant_timing_class, integration_mode,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_timing_composite_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_timing_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_timing_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family, dominant_timing_class,
                timing_adjusted_family_contribution,
                integration_weight_applied,
                timing_integration_contribution,
                family_rank, top_symbols, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s,
                %s,
                %s,
                %s,
                %s, %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["dependency_family"],
                    row.get("dominant_timing_class", "insufficient_data"),
                    row.get("timing_adjusted_family_contribution"),
                    row.get("integration_weight_applied"),
                    row.get("timing_integration_contribution"),
                    row.get("family_rank"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_timing_composite_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                composite_pre_timing, timing_net_contribution, composite_post_timing,
                dominant_timing_class, integration_mode, created_at
            from public.cross_asset_timing_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_timing_composite_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family, dominant_timing_class,
                timing_adjusted_family_contribution,
                integration_weight_applied,
                timing_integration_contribution,
                family_rank, top_symbols, created_at
            from public.cross_asset_family_timing_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_final_integration_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                timing_net_contribution,
                composite_pre_timing, composite_post_timing,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                dominant_timing_class,
                created_at
            from public.run_cross_asset_final_integration_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_timing_attribution_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family, dominant_timing_class,
                   timing_adjusted_family_contribution,
                   timing_family_rank, top_leading_symbols
            from public.cross_asset_family_timing_attribution_summary
            where run_id = %s::uuid
            order by timing_family_rank asc, dependency_family asc
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.2D: Timing Replay Validation -----
def insert_cross_asset_timing_replay_validation_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    source_context_snapshot_id,
    replay_context_snapshot_id,
    source_regime_key,
    replay_regime_key,
    source_dominant_timing_class,
    replay_dominant_timing_class,
    context_hash_match,
    regime_match,
    timing_class_match,
    timing_attribution_match,
    timing_composite_match,
    timing_dominant_family_match,
    timing_net_delta,
    timing_composite_delta,
    drift_reason_codes,
    validation_state,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_timing_replay_validation_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                context_hash_match, regime_match, timing_class_match,
                timing_attribution_match, timing_composite_match,
                timing_dominant_family_match,
                timing_net_delta, timing_composite_delta,
                drift_reason_codes, validation_state, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s::uuid,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s,
                %s::jsonb, %s::jsonb,
                %s::jsonb, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                bool(context_hash_match), bool(regime_match), bool(timing_class_match),
                bool(timing_attribution_match), bool(timing_composite_match),
                bool(timing_dominant_family_match),
                json.dumps(_json_compatible(timing_net_delta or {})),
                json.dumps(_json_compatible(timing_composite_delta or {})),
                json.dumps(list(drift_reason_codes or [])),
                validation_state,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_timing_replay_validation_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_timing_replay_stability_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_timing_replay_stability_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                dependency_family,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_timing_adjusted_contribution, replay_timing_adjusted_contribution,
                source_timing_integration_contribution, replay_timing_integration_contribution,
                timing_adjusted_delta, timing_integration_delta,
                timing_class_match, timing_family_rank_match, timing_composite_family_rank_match,
                drift_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, source_run_id, replay_run_id,
                    row["dependency_family"],
                    row.get("source_dominant_timing_class"),
                    row.get("replay_dominant_timing_class"),
                    row.get("source_timing_adjusted_contribution"),
                    row.get("replay_timing_adjusted_contribution"),
                    row.get("source_timing_integration_contribution"),
                    row.get("replay_timing_integration_contribution"),
                    row.get("timing_adjusted_delta"),
                    row.get("timing_integration_delta"),
                    bool(row.get("timing_class_match", False)),
                    bool(row.get("timing_family_rank_match", False)),
                    bool(row.get("timing_composite_family_rank_match", False)),
                    json.dumps(list(row.get("drift_reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_timing_replay_validation_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text               as workspace_id,
                watchlist_id::text               as watchlist_id,
                source_run_id::text              as source_run_id,
                replay_run_id::text              as replay_run_id,
                source_context_snapshot_id::text as source_context_snapshot_id,
                replay_context_snapshot_id::text as replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                context_hash_match, regime_match, timing_class_match,
                timing_attribution_match, timing_composite_match,
                timing_dominant_family_match,
                drift_reason_codes, validation_state, created_at
            from public.cross_asset_timing_replay_validation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_timing_replay_stability_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text      as workspace_id,
                watchlist_id::text      as watchlist_id,
                source_run_id::text     as source_run_id,
                replay_run_id::text     as replay_run_id,
                dependency_family,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_timing_adjusted_contribution, replay_timing_adjusted_contribution,
                source_timing_integration_contribution, replay_timing_integration_contribution,
                timing_adjusted_delta, timing_integration_delta,
                timing_class_match, timing_family_rank_match, timing_composite_family_rank_match,
                drift_reason_codes, created_at
            from public.cross_asset_family_timing_replay_stability_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_timing_replay_stability_aggregate(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                validation_count,
                context_match_rate, regime_match_rate,
                timing_class_match_rate,
                timing_attribution_match_rate,
                timing_composite_match_rate,
                timing_dominant_family_match_rate,
                drift_detected_count,
                latest_validated_at
            from public.cross_asset_timing_replay_stability_aggregate
            where workspace_id = %s::uuid
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_cross_asset_timing_composite_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   workspace_id::text as workspace_id,
                   watchlist_id::text as watchlist_id,
                   context_snapshot_id::text as context_snapshot_id,
                   base_signal_score,
                   cross_asset_net_contribution,
                   weighted_cross_asset_net_contribution,
                   regime_adjusted_cross_asset_contribution,
                   timing_adjusted_cross_asset_contribution,
                   composite_pre_timing, timing_net_contribution, composite_post_timing,
                   dominant_timing_class, integration_mode, created_at
            from public.cross_asset_timing_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_dominant_timing_class_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dominant_timing_class
            from public.cross_asset_timing_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row).get("dominant_timing_class") if row else None



# ----- Phase 4.3A: Family Transition Diagnostics -----
def insert_cross_asset_family_transition_state_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_transition_state_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family, regime_key, dominant_timing_class,
                signal_state, transition_state,
                family_contribution,
                timing_adjusted_contribution,
                timing_integration_contribution,
                family_rank, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s, %s,
                %s, %s,
                %s,
                %s,
                %s,
                %s, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["dependency_family"],
                    row.get("regime_key"),
                    row.get("dominant_timing_class"),
                    row["signal_state"],
                    row["transition_state"],
                    row.get("family_contribution"),
                    row.get("timing_adjusted_contribution"),
                    row.get("timing_integration_contribution"),
                    row.get("family_rank"),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_family_transition_event_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    target_run_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_transition_event_snapshots (
                workspace_id, watchlist_id, source_run_id, target_run_id,
                dependency_family,
                prior_signal_state, current_signal_state,
                prior_transition_state, current_transition_state,
                prior_family_rank, current_family_rank, rank_delta,
                prior_family_contribution, current_family_contribution, contribution_delta,
                event_type, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, source_run_id, target_run_id,
                    row["dependency_family"],
                    row.get("prior_signal_state"),
                    row["current_signal_state"],
                    row.get("prior_transition_state"),
                    row["current_transition_state"],
                    row.get("prior_family_rank"),
                    row.get("current_family_rank"),
                    row.get("rank_delta"),
                    row.get("prior_family_contribution"),
                    row.get("current_family_contribution"),
                    row.get("contribution_delta"),
                    row["event_type"],
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_family_sequence_summary_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_sequence_summary_snapshots (
                workspace_id, watchlist_id, run_id,
                dependency_family, window_label,
                sequence_signature, sequence_length,
                dominant_sequence_class, sequence_confidence, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid,
                %s, %s,
                %s, %s,
                %s, %s, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id,
                    row["dependency_family"],
                    row.get("window_label", "recent_5"),
                    row["sequence_signature"],
                    int(row.get("sequence_length") or 0),
                    row["dominant_sequence_class"],
                    row.get("sequence_confidence"),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_family_transition_state_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family, regime_key, dominant_timing_class,
                signal_state, transition_state,
                family_contribution,
                timing_adjusted_contribution,
                timing_integration_contribution,
                family_rank, created_at
            from public.cross_asset_family_transition_state_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, family_rank asc nulls last, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_transition_event_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text      as workspace_id,
                watchlist_id::text      as watchlist_id,
                source_run_id::text     as source_run_id,
                target_run_id::text     as target_run_id,
                dependency_family,
                prior_signal_state, current_signal_state,
                prior_transition_state, current_transition_state,
                prior_family_rank, current_family_rank, rank_delta,
                prior_family_contribution, current_family_contribution, contribution_delta,
                event_type, created_at
            from public.cross_asset_family_transition_event_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_sequence_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                run_id::text       as run_id,
                dependency_family, window_label,
                sequence_signature, sequence_length,
                dominant_sequence_class, sequence_confidence, created_at
            from public.cross_asset_family_sequence_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_transition_diagnostics_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text       as run_id,
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                dominant_dependency_family,
                prior_dominant_dependency_family,
                dominant_timing_class,
                dominant_transition_state,
                dominant_sequence_class,
                rotation_event_count,
                degradation_event_count,
                recovery_event_count,
                created_at
            from public.run_cross_asset_transition_diagnostics_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_recent_family_state_history(
    conn, *, workspace_id, watchlist_id, dependency_family, limit=5,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text as run_id,
                   dependency_family, regime_key, dominant_timing_class,
                   signal_state, transition_state,
                   family_contribution,
                   timing_adjusted_contribution,
                   timing_integration_contribution,
                   family_rank, created_at
            from public.cross_asset_family_transition_state_snapshots
            where workspace_id = %s::uuid
              and watchlist_id = %s::uuid
              and dependency_family = %s
            order by created_at desc
            limit %s
            """,
            (workspace_id, watchlist_id, dependency_family, limit),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.3B: Transition-Aware Attribution -----
def list_cross_asset_transition_attribution_profiles(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   reinforcing_weight, stable_weight, recovering_weight,
                   rotating_in_weight, rotating_out_weight,
                   deteriorating_weight, insufficient_history_weight,
                   recovery_bonus_scale, degradation_penalty_scale, rotation_bonus_scale,
                   sequence_class_overrides, family_weight_overrides,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_transition_attribution_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_transition_attribution_profile(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   reinforcing_weight, stable_weight, recovering_weight,
                   rotating_in_weight, rotating_out_weight,
                   deteriorating_weight, insufficient_history_weight,
                   recovery_bonus_scale, degradation_penalty_scale, rotation_bonus_scale,
                   sequence_class_overrides, family_weight_overrides,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_transition_attribution_profiles
            where workspace_id = %s::uuid and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_family_transition_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    transition_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_transition_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                transition_profile_id,
                dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_adjusted_family_contribution,
                timing_adjusted_family_contribution,
                transition_state, dominant_sequence_class,
                transition_state_weight, sequence_class_weight,
                transition_bonus, transition_penalty,
                transition_adjusted_family_contribution,
                transition_family_rank,
                top_symbols, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    transition_profile_id,
                    row["dependency_family"],
                    row.get("raw_family_net_contribution"),
                    row.get("weighted_family_net_contribution"),
                    row.get("regime_adjusted_family_contribution"),
                    row.get("timing_adjusted_family_contribution"),
                    row.get("transition_state", "insufficient_history"),
                    row.get("dominant_sequence_class", "insufficient_history"),
                    row.get("transition_state_weight"),
                    row.get("sequence_class_weight"),
                    row.get("transition_bonus"),
                    row.get("transition_penalty"),
                    row.get("transition_adjusted_family_contribution"),
                    row.get("transition_family_rank"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_symbol_transition_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    transition_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_symbol_transition_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                transition_profile_id,
                symbol, dependency_family, dependency_type,
                transition_state, dominant_sequence_class,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_state_weight, sequence_class_weight,
                transition_adjusted_symbol_score, symbol_rank, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    transition_profile_id,
                    row["symbol"],
                    row["dependency_family"],
                    row.get("dependency_type"),
                    row.get("transition_state", "insufficient_history"),
                    row.get("dominant_sequence_class", "insufficient_history"),
                    row.get("raw_symbol_score"),
                    row.get("weighted_symbol_score"),
                    row.get("regime_adjusted_symbol_score"),
                    row.get("timing_adjusted_symbol_score"),
                    row.get("transition_state_weight"),
                    row.get("sequence_class_weight"),
                    row.get("transition_adjusted_symbol_score"),
                    row.get("symbol_rank"),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_family_transition_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_adjusted_family_contribution,
                timing_adjusted_family_contribution,
                transition_state, dominant_sequence_class,
                transition_state_weight, sequence_class_weight,
                transition_bonus, transition_penalty,
                transition_adjusted_family_contribution,
                transition_family_rank,
                top_symbols, created_at
            from public.cross_asset_family_transition_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, transition_family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_symbol_transition_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                symbol, dependency_family, dependency_type,
                transition_state, dominant_sequence_class,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_state_weight, sequence_class_weight,
                transition_adjusted_symbol_score, symbol_rank, created_at
            from public.cross_asset_symbol_transition_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, symbol_rank asc, symbol asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_transition_attribution_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                transition_dominant_dependency_family,
                dominant_transition_state,
                dominant_sequence_class,
                created_at
            from public.run_cross_asset_transition_attribution_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_transition_state_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family, transition_state, family_rank,
                   signal_state, dominant_timing_class, regime_key
            from public.cross_asset_family_transition_state_summary
            where run_id = %s::uuid
            order by family_rank asc nulls last, dependency_family asc
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_sequence_summary_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family, dominant_sequence_class,
                   sequence_signature, sequence_length, sequence_confidence
            from public.cross_asset_family_sequence_summary
            where run_id = %s::uuid
            order by dependency_family asc
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.3C: Sequencing-Aware Composite Refinement -----
def list_cross_asset_transition_integration_profiles(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active, integration_mode,
                   integration_weight,
                   reinforcing_weight_scale, stable_weight_scale,
                   recovering_weight_scale, rotating_in_weight_scale,
                   rotating_out_weight_scale, deteriorating_weight_scale,
                   insufficient_history_weight_scale,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_transition_integration_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_transition_integration_profile(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active, integration_mode,
                   integration_weight,
                   reinforcing_weight_scale, stable_weight_scale,
                   recovering_weight_scale, rotating_in_weight_scale,
                   rotating_out_weight_scale, deteriorating_weight_scale,
                   insufficient_history_weight_scale,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_transition_integration_profiles
            where workspace_id = %s::uuid and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_transition_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    transition_integration_profile_id,
    base_signal_score,
    cross_asset_net_contribution,
    weighted_cross_asset_net_contribution,
    regime_adjusted_cross_asset_contribution,
    timing_adjusted_cross_asset_contribution,
    transition_adjusted_cross_asset_contribution,
    composite_pre_transition,
    transition_net_contribution,
    composite_post_transition,
    dominant_transition_state,
    integration_mode,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_transition_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                transition_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                composite_pre_transition, transition_net_contribution, composite_post_transition,
                dominant_transition_state, integration_mode, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s, %s, %s,
                %s, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                transition_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                composite_pre_transition, transition_net_contribution, composite_post_transition,
                dominant_transition_state, integration_mode,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_transition_composite_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_transition_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_transition_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family, transition_state, dominant_sequence_class,
                transition_adjusted_family_contribution,
                integration_weight_applied,
                transition_integration_contribution,
                family_rank, top_symbols, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s, %s,
                %s,
                %s,
                %s,
                %s, %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["dependency_family"],
                    row.get("transition_state", "insufficient_history"),
                    row.get("dominant_sequence_class", "insufficient_history"),
                    row.get("transition_adjusted_family_contribution"),
                    row.get("integration_weight_applied"),
                    row.get("transition_integration_contribution"),
                    row.get("family_rank"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_transition_composite_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                composite_pre_transition, transition_net_contribution, composite_post_transition,
                dominant_transition_state, integration_mode, created_at
            from public.cross_asset_transition_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_transition_composite_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family, transition_state, dominant_sequence_class,
                transition_adjusted_family_contribution,
                integration_weight_applied,
                transition_integration_contribution,
                family_rank, top_symbols, created_at
            from public.cross_asset_family_transition_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_sequencing_integration_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                transition_net_contribution,
                composite_pre_transition, composite_post_transition,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                transition_dominant_dependency_family,
                dominant_transition_state,
                created_at
            from public.run_cross_asset_sequencing_integration_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_transition_attribution_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family, transition_state, dominant_sequence_class,
                   transition_adjusted_family_contribution,
                   transition_family_rank, top_symbols
            from public.cross_asset_family_transition_attribution_summary
            where run_id = %s::uuid
            order by transition_family_rank asc, dependency_family asc
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.3D: Replay Validation for Sequencing-Aware Composite -----
def insert_cross_asset_transition_replay_validation_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    source_context_snapshot_id,
    replay_context_snapshot_id,
    source_regime_key,
    replay_regime_key,
    source_dominant_timing_class,
    replay_dominant_timing_class,
    source_dominant_transition_state,
    replay_dominant_transition_state,
    source_dominant_sequence_class,
    replay_dominant_sequence_class,
    context_hash_match,
    regime_match,
    timing_class_match,
    transition_state_match,
    sequence_class_match,
    transition_attribution_match,
    transition_composite_match,
    transition_dominant_family_match,
    transition_delta,
    transition_composite_delta,
    drift_reason_codes,
    validation_state,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_transition_replay_validation_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match,
                transition_attribution_match, transition_composite_match,
                transition_dominant_family_match,
                transition_delta, transition_composite_delta,
                drift_reason_codes, validation_state, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s::uuid,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s::jsonb, %s::jsonb,
                %s::jsonb, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                bool(context_hash_match), bool(regime_match), bool(timing_class_match),
                bool(transition_state_match), bool(sequence_class_match),
                bool(transition_attribution_match), bool(transition_composite_match),
                bool(transition_dominant_family_match),
                json.dumps(_json_compatible(transition_delta or {})),
                json.dumps(_json_compatible(transition_composite_delta or {})),
                json.dumps(list(drift_reason_codes or [])),
                validation_state,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_transition_replay_validation_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_transition_replay_stability_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_transition_replay_stability_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                dependency_family,
                source_transition_state, replay_transition_state,
                source_sequence_class, replay_sequence_class,
                source_transition_adjusted_contribution, replay_transition_adjusted_contribution,
                source_transition_integration_contribution, replay_transition_integration_contribution,
                transition_adjusted_delta, transition_integration_delta,
                transition_state_match, sequence_class_match,
                transition_family_rank_match, transition_composite_family_rank_match,
                drift_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, source_run_id, replay_run_id,
                    row["dependency_family"],
                    row.get("source_transition_state"),
                    row.get("replay_transition_state"),
                    row.get("source_sequence_class"),
                    row.get("replay_sequence_class"),
                    row.get("source_transition_adjusted_contribution"),
                    row.get("replay_transition_adjusted_contribution"),
                    row.get("source_transition_integration_contribution"),
                    row.get("replay_transition_integration_contribution"),
                    row.get("transition_adjusted_delta"),
                    row.get("transition_integration_delta"),
                    bool(row.get("transition_state_match", False)),
                    bool(row.get("sequence_class_match", False)),
                    bool(row.get("transition_family_rank_match", False)),
                    bool(row.get("transition_composite_family_rank_match", False)),
                    json.dumps(list(row.get("drift_reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_transition_replay_validation_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text               as workspace_id,
                watchlist_id::text               as watchlist_id,
                source_run_id::text              as source_run_id,
                replay_run_id::text              as replay_run_id,
                source_context_snapshot_id::text as source_context_snapshot_id,
                replay_context_snapshot_id::text as replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match,
                transition_attribution_match, transition_composite_match,
                transition_dominant_family_match,
                drift_reason_codes, validation_state, created_at
            from public.cross_asset_transition_replay_validation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_transition_replay_stability_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text      as workspace_id,
                watchlist_id::text      as watchlist_id,
                source_run_id::text     as source_run_id,
                replay_run_id::text     as replay_run_id,
                dependency_family,
                source_transition_state, replay_transition_state,
                source_sequence_class, replay_sequence_class,
                source_transition_adjusted_contribution, replay_transition_adjusted_contribution,
                source_transition_integration_contribution, replay_transition_integration_contribution,
                transition_adjusted_delta, transition_integration_delta,
                transition_state_match, sequence_class_match,
                transition_family_rank_match, transition_composite_family_rank_match,
                drift_reason_codes, created_at
            from public.cross_asset_family_transition_replay_stability_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_transition_replay_stability_aggregate(
    conn, *, workspace_id,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                validation_count,
                context_match_rate, regime_match_rate,
                timing_class_match_rate,
                transition_state_match_rate, sequence_class_match_rate,
                transition_attribution_match_rate,
                transition_composite_match_rate,
                transition_dominant_family_match_rate,
                drift_detected_count,
                latest_validated_at
            from public.cross_asset_transition_replay_stability_aggregate
            where workspace_id = %s::uuid
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_cross_asset_transition_composite_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family, transition_state, dominant_sequence_class,
                   transition_integration_contribution, family_rank
            from public.cross_asset_family_transition_composite_summary
            where run_id = %s::uuid
            order by family_rank asc, dependency_family asc
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_dominant_transition_state_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dominant_transition_state
            from public.cross_asset_transition_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_dominant_sequence_class_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dominant_sequence_class
            from public.run_cross_asset_transition_attribution_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None



# ----- Phase 4.4A: Sequencing Pattern Registry and Transition Archetypes -----
def list_cross_asset_transition_archetype_registry(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, archetype_key, archetype_label,
                   archetype_family, description, classification_rules,
                   is_active, metadata, created_at
            from public.cross_asset_transition_archetype_registry
            where is_active = true
            order by archetype_key
            """,
        )
        return [dict(r) for r in cur.fetchall()]


def insert_cross_asset_family_archetype_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_archetype_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family, regime_key, archetype_key,
                transition_state, dominant_sequence_class, dominant_timing_class,
                family_rank, family_contribution, archetype_confidence,
                classification_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["dependency_family"],
                    row.get("regime_key"),
                    row["archetype_key"],
                    row.get("transition_state", "insufficient_history"),
                    row.get("dominant_sequence_class", "insufficient_history"),
                    row.get("dominant_timing_class"),
                    row.get("family_rank"),
                    row.get("family_contribution"),
                    row.get("archetype_confidence"),
                    json.dumps(list(row.get("classification_reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_run_archetype_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    regime_key,
    dominant_archetype_key,
    dominant_dependency_family,
    dominant_transition_state,
    dominant_sequence_class,
    archetype_confidence,
    rotation_event_count,
    recovery_event_count,
    degradation_event_count,
    mixed_event_count,
    classification_reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_run_archetype_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                regime_key,
                dominant_archetype_key,
                dominant_dependency_family,
                dominant_transition_state,
                dominant_sequence_class,
                archetype_confidence,
                rotation_event_count, recovery_event_count,
                degradation_event_count, mixed_event_count,
                classification_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                regime_key,
                dominant_archetype_key,
                dominant_dependency_family,
                dominant_transition_state,
                dominant_sequence_class,
                archetype_confidence,
                int(rotation_event_count or 0), int(recovery_event_count or 0),
                int(degradation_event_count or 0), int(mixed_event_count or 0),
                json.dumps(list(classification_reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_run_archetype_snapshots insert returned no row")
        return dict(row)


def get_cross_asset_family_archetype_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family, regime_key, archetype_key,
                transition_state, dominant_sequence_class, dominant_timing_class,
                family_rank, family_contribution, archetype_confidence,
                classification_reason_codes, created_at
            from public.cross_asset_family_archetype_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_run_archetype_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                regime_key,
                dominant_archetype_key,
                dominant_dependency_family,
                dominant_transition_state,
                dominant_sequence_class,
                archetype_confidence,
                rotation_event_count, recovery_event_count,
                degradation_event_count, mixed_event_count,
                classification_reason_codes, created_at
            from public.cross_asset_run_archetype_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_regime_archetype_summary(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                regime_key,
                archetype_key,
                run_count,
                avg_confidence,
                latest_seen_at
            from public.cross_asset_regime_archetype_summary
            where workspace_id = %s::uuid
            order by latest_seen_at desc nulls last, regime_key asc, archetype_key asc
            limit 100
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_pattern_summary(conn, *, run_id=None, workspace_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text        as run_id,
                workspace_id::text  as workspace_id,
                watchlist_id::text  as watchlist_id,
                regime_key,
                dominant_archetype_key,
                dominant_dependency_family,
                dominant_transition_state,
                dominant_sequence_class,
                archetype_confidence,
                created_at
            from public.run_cross_asset_pattern_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.4B: Archetype-Aware Attribution -----
def list_cross_asset_archetype_attribution_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   rotation_handoff_weight, reinforcing_continuation_weight,
                   recovering_reentry_weight, deteriorating_breakdown_weight,
                   mixed_transition_noise_weight, insufficient_history_weight,
                   recovery_bonus_scale, breakdown_penalty_scale, rotation_bonus_scale,
                   archetype_family_overrides, metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_archetype_attribution_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_archetype_attribution_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   rotation_handoff_weight, reinforcing_continuation_weight,
                   recovering_reentry_weight, deteriorating_breakdown_weight,
                   mixed_transition_noise_weight, insufficient_history_weight,
                   recovery_bonus_scale, breakdown_penalty_scale, rotation_bonus_scale,
                   archetype_family_overrides, metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_archetype_attribution_profiles
            where workspace_id = %s::uuid and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_family_archetype_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    archetype_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_archetype_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                archetype_profile_id, dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_adjusted_family_contribution,
                timing_adjusted_family_contribution,
                transition_adjusted_family_contribution,
                archetype_key, transition_state, dominant_sequence_class,
                archetype_weight, archetype_bonus, archetype_penalty,
                archetype_adjusted_family_contribution,
                archetype_family_rank, top_symbols,
                classification_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s,
                %s, %s::jsonb,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    archetype_profile_id, row["dependency_family"],
                    row.get("raw_family_net_contribution"),
                    row.get("weighted_family_net_contribution"),
                    row.get("regime_adjusted_family_contribution"),
                    row.get("timing_adjusted_family_contribution"),
                    row.get("transition_adjusted_family_contribution"),
                    row.get("archetype_key", "insufficient_history"),
                    row.get("transition_state", "insufficient_history"),
                    row.get("dominant_sequence_class", "insufficient_history"),
                    row.get("archetype_weight"),
                    row.get("archetype_bonus"),
                    row.get("archetype_penalty"),
                    row.get("archetype_adjusted_family_contribution"),
                    row.get("archetype_family_rank"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(list(row.get("classification_reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_symbol_archetype_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    archetype_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_symbol_archetype_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                archetype_profile_id,
                symbol, dependency_family, dependency_type,
                archetype_key, transition_state, dominant_sequence_class,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score,
                archetype_weight, archetype_adjusted_symbol_score,
                symbol_rank,
                classification_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    archetype_profile_id,
                    row["symbol"], row["dependency_family"], row.get("dependency_type"),
                    row.get("archetype_key", "insufficient_history"),
                    row.get("transition_state", "insufficient_history"),
                    row.get("dominant_sequence_class", "insufficient_history"),
                    row.get("raw_symbol_score"),
                    row.get("weighted_symbol_score"),
                    row.get("regime_adjusted_symbol_score"),
                    row.get("timing_adjusted_symbol_score"),
                    row.get("transition_adjusted_symbol_score"),
                    row.get("archetype_weight"),
                    row.get("archetype_adjusted_symbol_score"),
                    row.get("symbol_rank"),
                    json.dumps(list(row.get("classification_reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_family_archetype_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_adjusted_family_contribution,
                timing_adjusted_family_contribution,
                transition_adjusted_family_contribution,
                archetype_key, transition_state, dominant_sequence_class,
                archetype_weight, archetype_bonus, archetype_penalty,
                archetype_adjusted_family_contribution,
                archetype_family_rank, top_symbols,
                classification_reason_codes, created_at
            from public.cross_asset_family_archetype_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, archetype_family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_symbol_archetype_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                symbol, dependency_family, dependency_type,
                archetype_key, transition_state, dominant_sequence_class,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score,
                archetype_weight, archetype_adjusted_symbol_score,
                symbol_rank, classification_reason_codes, created_at
            from public.cross_asset_symbol_archetype_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, symbol_rank asc, symbol asc
            limit 300
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_archetype_attribution_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                transition_dominant_dependency_family,
                archetype_dominant_dependency_family,
                dominant_archetype_key,
                created_at
            from public.run_cross_asset_archetype_attribution_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_archetype_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family, archetype_key,
                   transition_state, dominant_sequence_class,
                   family_rank, family_contribution, archetype_confidence
            from public.cross_asset_family_archetype_summary
            where run_id = %s::uuid
            order by family_rank asc, dependency_family asc
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_run_archetype_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dominant_archetype_key, dominant_dependency_family,
                   dominant_transition_state, dominant_sequence_class,
                   archetype_confidence
            from public.cross_asset_run_archetype_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None



# ----- Phase 4.4C: Archetype-Aware Composite Refinement -----
def list_cross_asset_archetype_integration_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active, integration_mode,
                   integration_weight,
                   reinforcing_continuation_scale, recovering_reentry_scale,
                   rotation_handoff_scale, mixed_transition_noise_scale,
                   deteriorating_breakdown_scale, insufficient_history_scale,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_archetype_integration_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_archetype_integration_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active, integration_mode,
                   integration_weight,
                   reinforcing_continuation_scale, recovering_reentry_scale,
                   rotation_handoff_scale, mixed_transition_noise_scale,
                   deteriorating_breakdown_scale, insufficient_history_scale,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_archetype_integration_profiles
            where workspace_id = %s::uuid and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_archetype_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    archetype_integration_profile_id,
    base_signal_score,
    cross_asset_net_contribution,
    weighted_cross_asset_net_contribution,
    regime_adjusted_cross_asset_contribution,
    timing_adjusted_cross_asset_contribution,
    transition_adjusted_cross_asset_contribution,
    archetype_adjusted_cross_asset_contribution,
    composite_pre_archetype,
    archetype_net_contribution,
    composite_post_archetype,
    dominant_archetype_key,
    integration_mode,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_archetype_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                archetype_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                composite_pre_archetype, archetype_net_contribution, composite_post_archetype,
                dominant_archetype_key, integration_mode, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s, %s, %s,
                %s, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                archetype_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                composite_pre_archetype, archetype_net_contribution, composite_post_archetype,
                dominant_archetype_key, integration_mode,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_archetype_composite_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_archetype_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_archetype_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family, archetype_key, transition_state, dominant_sequence_class,
                archetype_adjusted_family_contribution,
                integration_weight_applied,
                archetype_integration_contribution,
                family_rank, top_symbols, classification_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s, %s, %s,
                %s,
                %s,
                %s,
                %s, %s::jsonb, %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["dependency_family"],
                    row.get("archetype_key", "insufficient_history"),
                    row.get("transition_state", "insufficient_history"),
                    row.get("dominant_sequence_class", "insufficient_history"),
                    row.get("archetype_adjusted_family_contribution"),
                    row.get("integration_weight_applied"),
                    row.get("archetype_integration_contribution"),
                    row.get("family_rank"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(list(row.get("classification_reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_archetype_composite_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                composite_pre_archetype, archetype_net_contribution, composite_post_archetype,
                dominant_archetype_key, integration_mode, created_at
            from public.cross_asset_archetype_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_archetype_composite_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family, archetype_key, transition_state, dominant_sequence_class,
                archetype_adjusted_family_contribution,
                integration_weight_applied,
                archetype_integration_contribution,
                family_rank, top_symbols,
                classification_reason_codes, created_at
            from public.cross_asset_family_archetype_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_archetype_integration_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                archetype_net_contribution,
                composite_pre_archetype, composite_post_archetype,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                transition_dominant_dependency_family,
                archetype_dominant_dependency_family,
                dominant_archetype_key,
                created_at
            from public.run_cross_asset_archetype_integration_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_archetype_attribution_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family, archetype_key, transition_state,
                   dominant_sequence_class,
                   archetype_adjusted_family_contribution,
                   archetype_family_rank, top_symbols
            from public.cross_asset_family_archetype_attribution_summary
            where run_id = %s::uuid
            order by archetype_family_rank asc, dependency_family asc
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.4D: Replay Validation for Archetype-Aware Composite -----
def insert_cross_asset_archetype_replay_validation_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    source_context_snapshot_id,
    replay_context_snapshot_id,
    source_regime_key,
    replay_regime_key,
    source_dominant_timing_class,
    replay_dominant_timing_class,
    source_dominant_transition_state,
    replay_dominant_transition_state,
    source_dominant_sequence_class,
    replay_dominant_sequence_class,
    source_dominant_archetype_key,
    replay_dominant_archetype_key,
    context_hash_match,
    regime_match,
    timing_class_match,
    transition_state_match,
    sequence_class_match,
    archetype_match,
    archetype_attribution_match,
    archetype_composite_match,
    archetype_dominant_family_match,
    archetype_delta,
    archetype_composite_delta,
    drift_reason_codes,
    validation_state,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_archetype_replay_validation_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match,
                archetype_match,
                archetype_attribution_match, archetype_composite_match,
                archetype_dominant_family_match,
                archetype_delta, archetype_composite_delta,
                drift_reason_codes, validation_state, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s::uuid,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s,
                %s::jsonb, %s::jsonb,
                %s::jsonb, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                bool(context_hash_match), bool(regime_match), bool(timing_class_match),
                bool(transition_state_match), bool(sequence_class_match),
                bool(archetype_match),
                bool(archetype_attribution_match), bool(archetype_composite_match),
                bool(archetype_dominant_family_match),
                json.dumps(_json_compatible(archetype_delta or {})),
                json.dumps(_json_compatible(archetype_composite_delta or {})),
                json.dumps(list(drift_reason_codes or [])),
                validation_state,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_archetype_replay_validation_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_archetype_replay_stability_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_archetype_replay_stability_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                dependency_family,
                source_transition_state, replay_transition_state,
                source_sequence_class, replay_sequence_class,
                source_archetype_key, replay_archetype_key,
                source_archetype_adjusted_contribution, replay_archetype_adjusted_contribution,
                source_archetype_integration_contribution, replay_archetype_integration_contribution,
                archetype_adjusted_delta, archetype_integration_delta,
                transition_state_match, sequence_class_match, archetype_match,
                archetype_family_rank_match, archetype_composite_family_rank_match,
                drift_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, source_run_id, replay_run_id,
                    row["dependency_family"],
                    row.get("source_transition_state"),
                    row.get("replay_transition_state"),
                    row.get("source_sequence_class"),
                    row.get("replay_sequence_class"),
                    row.get("source_archetype_key"),
                    row.get("replay_archetype_key"),
                    row.get("source_archetype_adjusted_contribution"),
                    row.get("replay_archetype_adjusted_contribution"),
                    row.get("source_archetype_integration_contribution"),
                    row.get("replay_archetype_integration_contribution"),
                    row.get("archetype_adjusted_delta"),
                    row.get("archetype_integration_delta"),
                    bool(row.get("transition_state_match", False)),
                    bool(row.get("sequence_class_match", False)),
                    bool(row.get("archetype_match", False)),
                    bool(row.get("archetype_family_rank_match", False)),
                    bool(row.get("archetype_composite_family_rank_match", False)),
                    json.dumps(list(row.get("drift_reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_archetype_replay_validation_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text               as workspace_id,
                watchlist_id::text               as watchlist_id,
                source_run_id::text              as source_run_id,
                replay_run_id::text              as replay_run_id,
                source_context_snapshot_id::text as source_context_snapshot_id,
                replay_context_snapshot_id::text as replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match,
                archetype_match,
                archetype_attribution_match, archetype_composite_match,
                archetype_dominant_family_match,
                drift_reason_codes, validation_state, created_at
            from public.cross_asset_archetype_replay_validation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_archetype_replay_stability_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text      as workspace_id,
                watchlist_id::text      as watchlist_id,
                source_run_id::text     as source_run_id,
                replay_run_id::text     as replay_run_id,
                dependency_family,
                source_transition_state, replay_transition_state,
                source_sequence_class, replay_sequence_class,
                source_archetype_key, replay_archetype_key,
                source_archetype_adjusted_contribution, replay_archetype_adjusted_contribution,
                source_archetype_integration_contribution, replay_archetype_integration_contribution,
                archetype_adjusted_delta, archetype_integration_delta,
                transition_state_match, sequence_class_match, archetype_match,
                archetype_family_rank_match, archetype_composite_family_rank_match,
                drift_reason_codes, created_at
            from public.cross_asset_family_archetype_replay_stability_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_archetype_replay_stability_aggregate(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                validation_count,
                context_match_rate, regime_match_rate,
                timing_class_match_rate,
                transition_state_match_rate, sequence_class_match_rate,
                archetype_match_rate,
                archetype_attribution_match_rate,
                archetype_composite_match_rate,
                archetype_dominant_family_match_rate,
                drift_detected_count,
                latest_validated_at
            from public.cross_asset_archetype_replay_stability_aggregate
            where workspace_id = %s::uuid
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_dominant_archetype_for_run(conn, *, run_id, workspace_id=None):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select dominant_archetype_key
            from public.cross_asset_archetype_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None



# ----- Phase 4.5A: Pattern-Cluster Drift + Archetype Regime Rotation -----
def insert_cross_asset_archetype_cluster_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    regime_key,
    window_label,
    dominant_archetype_key,
    archetype_mix,
    reinforcement_share,
    recovery_share,
    rotation_share,
    degradation_share,
    mixed_share,
    pattern_entropy,
    cluster_state,
    drift_score,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_archetype_cluster_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                regime_key, window_label,
                dominant_archetype_key, archetype_mix,
                reinforcement_share, recovery_share, rotation_share,
                degradation_share, mixed_share,
                pattern_entropy, cluster_state, drift_score, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s,
                %s, %s::jsonb,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                regime_key, window_label,
                dominant_archetype_key,
                json.dumps(_json_compatible(archetype_mix or {})),
                reinforcement_share, recovery_share, rotation_share,
                degradation_share, mixed_share,
                pattern_entropy, cluster_state, drift_score,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_archetype_cluster_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_archetype_regime_rotation_snapshots(
    conn,
    *,
    workspace_id,
    regime_key,
    window_label,
    prior_dominant_archetype_key,
    current_dominant_archetype_key,
    rotation_count,
    reinforcement_run_count,
    recovery_run_count,
    degradation_run_count,
    mixed_run_count,
    rotation_state,
    regime_drift_score,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_archetype_regime_rotation_snapshots (
                workspace_id, regime_key, window_label,
                prior_dominant_archetype_key, current_dominant_archetype_key,
                rotation_count,
                reinforcement_run_count, recovery_run_count,
                degradation_run_count, mixed_run_count,
                rotation_state, regime_drift_score, metadata
            )
            values (
                %s::uuid, %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s,
                %s, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, regime_key, window_label,
                prior_dominant_archetype_key, current_dominant_archetype_key,
                int(rotation_count or 0),
                int(reinforcement_run_count or 0), int(recovery_run_count or 0),
                int(degradation_run_count or 0), int(mixed_run_count or 0),
                rotation_state, regime_drift_score,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_archetype_regime_rotation_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_pattern_drift_event_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    target_run_id,
    regime_key,
    prior_cluster_state,
    current_cluster_state,
    prior_dominant_archetype_key,
    current_dominant_archetype_key,
    drift_event_type,
    drift_score,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_pattern_drift_event_snapshots (
                workspace_id, watchlist_id, source_run_id, target_run_id,
                regime_key,
                prior_cluster_state, current_cluster_state,
                prior_dominant_archetype_key, current_dominant_archetype_key,
                drift_event_type, drift_score,
                reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, target_run_id,
                regime_key,
                prior_cluster_state, current_cluster_state,
                prior_dominant_archetype_key, current_dominant_archetype_key,
                drift_event_type, drift_score,
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_pattern_drift_event_snapshots insert returned no row")
        return dict(row)


def get_cross_asset_archetype_cluster_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                regime_key, window_label,
                dominant_archetype_key, archetype_mix,
                reinforcement_share, recovery_share, rotation_share,
                degradation_share, mixed_share,
                pattern_entropy, cluster_state, drift_score, created_at
            from public.cross_asset_archetype_cluster_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_archetype_regime_rotation_summary(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                regime_key, window_label,
                prior_dominant_archetype_key, current_dominant_archetype_key,
                rotation_count,
                reinforcement_run_count, recovery_run_count,
                degradation_run_count, mixed_run_count,
                rotation_state, regime_drift_score, created_at
            from public.cross_asset_archetype_regime_rotation_summary
            where workspace_id = %s::uuid
            order by created_at desc, regime_key asc
            limit 50
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_pattern_drift_event_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text       as workspace_id,
                watchlist_id::text       as watchlist_id,
                source_run_id::text      as source_run_id,
                target_run_id::text      as target_run_id,
                regime_key,
                prior_cluster_state, current_cluster_state,
                prior_dominant_archetype_key, current_dominant_archetype_key,
                drift_event_type, drift_score,
                reason_codes, created_at
            from public.cross_asset_pattern_drift_event_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 100
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_pattern_cluster_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text       as run_id,
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                regime_key,
                dominant_archetype_key,
                cluster_state,
                drift_score,
                pattern_entropy,
                current_rotation_state,
                latest_drift_event_type,
                created_at
            from public.run_cross_asset_pattern_cluster_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_recent_run_archetype_history(
    conn, *, workspace_id, watchlist_id=None, limit=25,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id::text       as run_id,
                   watchlist_id::text as watchlist_id,
                   regime_key, dominant_archetype_key,
                   archetype_confidence, created_at
            from public.cross_asset_run_archetype_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit %s
            """,
            (workspace_id, watchlist_id, watchlist_id, int(limit)),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.5B: Cluster-Aware Attribution -----
def list_cross_asset_cluster_attribution_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   stable_weight, rotating_weight, recovering_weight,
                   deteriorating_weight, mixed_weight, insufficient_history_weight,
                   drift_penalty_scale, rotation_bonus_scale,
                   recovery_bonus_scale, entropy_penalty_scale,
                   cluster_family_overrides, metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_cluster_attribution_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_cluster_attribution_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   stable_weight, rotating_weight, recovering_weight,
                   deteriorating_weight, mixed_weight, insufficient_history_weight,
                   drift_penalty_scale, rotation_bonus_scale,
                   recovery_bonus_scale, entropy_penalty_scale,
                   cluster_family_overrides, metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_cluster_attribution_profiles
            where workspace_id = %s::uuid and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_family_cluster_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    cluster_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_cluster_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                cluster_profile_id, dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_adjusted_family_contribution,
                timing_adjusted_family_contribution,
                transition_adjusted_family_contribution,
                archetype_adjusted_family_contribution,
                cluster_state, dominant_archetype_key,
                drift_score, pattern_entropy,
                cluster_weight, cluster_bonus, cluster_penalty,
                cluster_adjusted_family_contribution,
                cluster_family_rank, top_symbols,
                reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s,
                %s, %s::jsonb,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    cluster_profile_id, row["dependency_family"],
                    row.get("raw_family_net_contribution"),
                    row.get("weighted_family_net_contribution"),
                    row.get("regime_adjusted_family_contribution"),
                    row.get("timing_adjusted_family_contribution"),
                    row.get("transition_adjusted_family_contribution"),
                    row.get("archetype_adjusted_family_contribution"),
                    row.get("cluster_state", "insufficient_history"),
                    row.get("dominant_archetype_key", "insufficient_history"),
                    row.get("drift_score"),
                    row.get("pattern_entropy"),
                    row.get("cluster_weight"),
                    row.get("cluster_bonus"),
                    row.get("cluster_penalty"),
                    row.get("cluster_adjusted_family_contribution"),
                    row.get("cluster_family_rank"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(list(row.get("reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_symbol_cluster_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    cluster_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_symbol_cluster_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                cluster_profile_id,
                symbol, dependency_family, dependency_type,
                cluster_state, dominant_archetype_key,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score,
                archetype_adjusted_symbol_score,
                cluster_weight, cluster_adjusted_symbol_score,
                symbol_rank, reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    cluster_profile_id,
                    row["symbol"], row["dependency_family"], row.get("dependency_type"),
                    row.get("cluster_state", "insufficient_history"),
                    row.get("dominant_archetype_key", "insufficient_history"),
                    row.get("raw_symbol_score"),
                    row.get("weighted_symbol_score"),
                    row.get("regime_adjusted_symbol_score"),
                    row.get("timing_adjusted_symbol_score"),
                    row.get("transition_adjusted_symbol_score"),
                    row.get("archetype_adjusted_symbol_score"),
                    row.get("cluster_weight"),
                    row.get("cluster_adjusted_symbol_score"),
                    row.get("symbol_rank"),
                    json.dumps(list(row.get("reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_family_cluster_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_adjusted_family_contribution,
                timing_adjusted_family_contribution,
                transition_adjusted_family_contribution,
                archetype_adjusted_family_contribution,
                cluster_state, dominant_archetype_key,
                drift_score, pattern_entropy,
                cluster_weight, cluster_bonus, cluster_penalty,
                cluster_adjusted_family_contribution,
                cluster_family_rank, top_symbols,
                reason_codes, created_at
            from public.cross_asset_family_cluster_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, cluster_family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_symbol_cluster_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                symbol, dependency_family, dependency_type,
                cluster_state, dominant_archetype_key,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score,
                archetype_adjusted_symbol_score,
                cluster_weight, cluster_adjusted_symbol_score,
                symbol_rank, reason_codes, created_at
            from public.cross_asset_symbol_cluster_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, symbol_rank asc, symbol asc
            limit 300
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_cluster_attribution_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                transition_dominant_dependency_family,
                archetype_dominant_dependency_family,
                cluster_dominant_dependency_family,
                cluster_state,
                dominant_archetype_key,
                created_at
            from public.run_cross_asset_cluster_attribution_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_cluster_context_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select cluster_state, dominant_archetype_key,
                   drift_score, pattern_entropy,
                   regime_key, current_rotation_state,
                   latest_drift_event_type
            from public.run_cross_asset_pattern_cluster_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None



# ----- Phase 4.5C: Cluster-Aware Composite Refinement -----
def list_cross_asset_cluster_integration_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active, integration_mode,
                   integration_weight,
                   stable_scale, recovering_scale, rotating_scale,
                   mixed_scale, deteriorating_scale, insufficient_history_scale,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_cluster_integration_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_cluster_integration_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active, integration_mode,
                   integration_weight,
                   stable_scale, recovering_scale, rotating_scale,
                   mixed_scale, deteriorating_scale, insufficient_history_scale,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_cluster_integration_profiles
            where workspace_id = %s::uuid and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_cluster_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    cluster_integration_profile_id,
    base_signal_score,
    cross_asset_net_contribution,
    weighted_cross_asset_net_contribution,
    regime_adjusted_cross_asset_contribution,
    timing_adjusted_cross_asset_contribution,
    transition_adjusted_cross_asset_contribution,
    archetype_adjusted_cross_asset_contribution,
    cluster_adjusted_cross_asset_contribution,
    composite_pre_cluster,
    cluster_net_contribution,
    composite_post_cluster,
    cluster_state,
    dominant_archetype_key,
    integration_mode,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_cluster_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                cluster_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                composite_pre_cluster, cluster_net_contribution, composite_post_cluster,
                cluster_state, dominant_archetype_key, integration_mode, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s, %s, %s,
                %s, %s, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                cluster_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                composite_pre_cluster, cluster_net_contribution, composite_post_cluster,
                cluster_state, dominant_archetype_key, integration_mode,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_cluster_composite_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_cluster_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_cluster_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family, cluster_state, dominant_archetype_key,
                cluster_adjusted_family_contribution,
                integration_weight_applied,
                cluster_integration_contribution,
                family_rank, top_symbols, reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s, %s, %s,
                %s,
                %s,
                %s,
                %s, %s::jsonb, %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["dependency_family"],
                    row.get("cluster_state", "insufficient_history"),
                    row.get("dominant_archetype_key", "insufficient_history"),
                    row.get("cluster_adjusted_family_contribution"),
                    row.get("integration_weight_applied"),
                    row.get("cluster_integration_contribution"),
                    row.get("family_rank"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(list(row.get("reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_cluster_composite_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                composite_pre_cluster, cluster_net_contribution, composite_post_cluster,
                cluster_state, dominant_archetype_key, integration_mode, created_at
            from public.cross_asset_cluster_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_cluster_composite_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family, cluster_state, dominant_archetype_key,
                cluster_adjusted_family_contribution,
                integration_weight_applied,
                cluster_integration_contribution,
                family_rank, top_symbols,
                reason_codes, created_at
            from public.cross_asset_family_cluster_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_cluster_integration_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                cluster_net_contribution,
                composite_pre_cluster, composite_post_cluster,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                transition_dominant_dependency_family,
                archetype_dominant_dependency_family,
                cluster_dominant_dependency_family,
                cluster_state,
                dominant_archetype_key,
                created_at
            from public.run_cross_asset_cluster_integration_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_cluster_attribution_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family, cluster_state, dominant_archetype_key,
                   cluster_adjusted_family_contribution,
                   cluster_family_rank, top_symbols
            from public.cross_asset_family_cluster_attribution_summary
            where run_id = %s::uuid
            order by cluster_family_rank asc, dependency_family asc
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.5D: Replay Validation for Cluster-Aware Composite -----
def insert_cross_asset_cluster_replay_validation_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    source_context_snapshot_id,
    replay_context_snapshot_id,
    source_regime_key,
    replay_regime_key,
    source_dominant_timing_class,
    replay_dominant_timing_class,
    source_dominant_transition_state,
    replay_dominant_transition_state,
    source_dominant_sequence_class,
    replay_dominant_sequence_class,
    source_dominant_archetype_key,
    replay_dominant_archetype_key,
    source_cluster_state,
    replay_cluster_state,
    source_drift_score,
    replay_drift_score,
    context_hash_match,
    regime_match,
    timing_class_match,
    transition_state_match,
    sequence_class_match,
    archetype_match,
    cluster_state_match,
    drift_score_match,
    cluster_attribution_match,
    cluster_composite_match,
    cluster_dominant_family_match,
    cluster_delta,
    cluster_composite_delta,
    drift_reason_codes,
    validation_state,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_cluster_replay_validation_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_state, replay_cluster_state,
                source_drift_score, replay_drift_score,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match,
                archetype_match,
                cluster_state_match, drift_score_match,
                cluster_attribution_match, cluster_composite_match,
                cluster_dominant_family_match,
                cluster_delta, cluster_composite_delta,
                drift_reason_codes, validation_state, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s::uuid,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s,
                %s,
                %s::jsonb, %s::jsonb,
                %s::jsonb, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_state, replay_cluster_state,
                source_drift_score, replay_drift_score,
                bool(context_hash_match), bool(regime_match), bool(timing_class_match),
                bool(transition_state_match), bool(sequence_class_match),
                bool(archetype_match),
                bool(cluster_state_match), bool(drift_score_match),
                bool(cluster_attribution_match), bool(cluster_composite_match),
                bool(cluster_dominant_family_match),
                json.dumps(_json_compatible(cluster_delta or {})),
                json.dumps(_json_compatible(cluster_composite_delta or {})),
                json.dumps(list(drift_reason_codes or [])),
                validation_state,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_cluster_replay_validation_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_cluster_replay_stability_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_cluster_replay_stability_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                dependency_family,
                source_cluster_state, replay_cluster_state,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_adjusted_contribution, replay_cluster_adjusted_contribution,
                source_cluster_integration_contribution, replay_cluster_integration_contribution,
                cluster_adjusted_delta, cluster_integration_delta,
                cluster_state_match, archetype_match,
                cluster_family_rank_match, cluster_composite_family_rank_match,
                drift_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, source_run_id, replay_run_id,
                    row["dependency_family"],
                    row.get("source_cluster_state"),
                    row.get("replay_cluster_state"),
                    row.get("source_dominant_archetype_key"),
                    row.get("replay_dominant_archetype_key"),
                    row.get("source_cluster_adjusted_contribution"),
                    row.get("replay_cluster_adjusted_contribution"),
                    row.get("source_cluster_integration_contribution"),
                    row.get("replay_cluster_integration_contribution"),
                    row.get("cluster_adjusted_delta"),
                    row.get("cluster_integration_delta"),
                    bool(row.get("cluster_state_match", False)),
                    bool(row.get("archetype_match", False)),
                    bool(row.get("cluster_family_rank_match", False)),
                    bool(row.get("cluster_composite_family_rank_match", False)),
                    json.dumps(list(row.get("drift_reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_cluster_replay_validation_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text               as workspace_id,
                watchlist_id::text               as watchlist_id,
                source_run_id::text              as source_run_id,
                replay_run_id::text              as replay_run_id,
                source_context_snapshot_id::text as source_context_snapshot_id,
                replay_context_snapshot_id::text as replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_state, replay_cluster_state,
                source_drift_score, replay_drift_score,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match,
                archetype_match,
                cluster_state_match, drift_score_match,
                cluster_attribution_match, cluster_composite_match,
                cluster_dominant_family_match,
                drift_reason_codes, validation_state, created_at
            from public.cross_asset_cluster_replay_validation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_cluster_replay_stability_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text      as workspace_id,
                watchlist_id::text      as watchlist_id,
                source_run_id::text     as source_run_id,
                replay_run_id::text     as replay_run_id,
                dependency_family,
                source_cluster_state, replay_cluster_state,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_adjusted_contribution, replay_cluster_adjusted_contribution,
                source_cluster_integration_contribution, replay_cluster_integration_contribution,
                cluster_adjusted_delta, cluster_integration_delta,
                cluster_state_match, archetype_match,
                cluster_family_rank_match, cluster_composite_family_rank_match,
                drift_reason_codes, created_at
            from public.cross_asset_family_cluster_replay_stability_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_cluster_replay_stability_aggregate(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                validation_count,
                context_match_rate, regime_match_rate,
                timing_class_match_rate,
                transition_state_match_rate, sequence_class_match_rate,
                archetype_match_rate,
                cluster_state_match_rate, drift_score_match_rate,
                cluster_attribution_match_rate,
                cluster_composite_match_rate,
                cluster_dominant_family_match_rate,
                drift_detected_count,
                latest_validated_at
            from public.cross_asset_cluster_replay_stability_aggregate
            where workspace_id = %s::uuid
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_cluster_state_for_run(conn, *, run_id, workspace_id=None):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select cluster_state
            from public.run_cross_asset_pattern_cluster_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_drift_score_for_run(conn, *, run_id, workspace_id=None):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select drift_score
            from public.run_cross_asset_pattern_cluster_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None



# ----- Phase 4.6A: Cross-Window Regime Memory + Persistence Diagnostics -----
def insert_cross_asset_state_persistence_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    window_label,
    regime_key,
    dominant_timing_class,
    dominant_transition_state,
    dominant_sequence_class,
    dominant_archetype_key,
    cluster_state,
    current_state_signature,
    state_age_runs,
    same_state_count,
    state_persistence_ratio,
    regime_persistence_ratio,
    cluster_persistence_ratio,
    archetype_persistence_ratio,
    persistence_state,
    memory_score,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_state_persistence_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                window_label,
                regime_key, dominant_timing_class,
                dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state,
                current_state_signature, state_age_runs, same_state_count,
                state_persistence_ratio, regime_persistence_ratio,
                cluster_persistence_ratio, archetype_persistence_ratio,
                persistence_state, memory_score, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                window_label,
                regime_key, dominant_timing_class,
                dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state,
                current_state_signature, int(state_age_runs or 1), int(same_state_count or 1),
                state_persistence_ratio, regime_persistence_ratio,
                cluster_persistence_ratio, archetype_persistence_ratio,
                persistence_state, memory_score,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_state_persistence_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_regime_memory_snapshots(
    conn,
    *,
    workspace_id,
    regime_key,
    window_label,
    run_count,
    same_regime_streak_count,
    regime_switch_count,
    avg_regime_duration_runs,
    max_regime_duration_runs,
    regime_memory_score,
    dominant_cluster_state,
    dominant_archetype_key,
    persistence_state,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_regime_memory_snapshots (
                workspace_id, regime_key, window_label,
                run_count, same_regime_streak_count, regime_switch_count,
                avg_regime_duration_runs, max_regime_duration_runs,
                regime_memory_score,
                dominant_cluster_state, dominant_archetype_key,
                persistence_state, metadata
            )
            values (
                %s::uuid, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, regime_key, window_label,
                int(run_count or 0), int(same_regime_streak_count or 0), int(regime_switch_count or 0),
                avg_regime_duration_runs,
                int(max_regime_duration_runs) if max_regime_duration_runs is not None else None,
                regime_memory_score,
                dominant_cluster_state, dominant_archetype_key,
                persistence_state,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_regime_memory_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_persistence_transition_event_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    target_run_id,
    regime_key,
    prior_state_signature,
    current_state_signature,
    prior_persistence_state,
    current_persistence_state,
    prior_memory_score,
    current_memory_score,
    memory_score_delta,
    event_type,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_persistence_transition_event_snapshots (
                workspace_id, watchlist_id, source_run_id, target_run_id,
                regime_key,
                prior_state_signature, current_state_signature,
                prior_persistence_state, current_persistence_state,
                prior_memory_score, current_memory_score, memory_score_delta,
                event_type, reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, target_run_id,
                regime_key,
                prior_state_signature, current_state_signature,
                prior_persistence_state, current_persistence_state,
                prior_memory_score, current_memory_score, memory_score_delta,
                event_type,
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_persistence_transition_event_snapshots insert returned no row")
        return dict(row)


def get_cross_asset_state_persistence_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                window_label,
                regime_key, dominant_timing_class,
                dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state,
                current_state_signature, state_age_runs, same_state_count,
                state_persistence_ratio, regime_persistence_ratio,
                cluster_persistence_ratio, archetype_persistence_ratio,
                persistence_state, memory_score, created_at
            from public.cross_asset_state_persistence_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_regime_memory_summary(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                regime_key, window_label,
                run_count, same_regime_streak_count, regime_switch_count,
                avg_regime_duration_runs, max_regime_duration_runs,
                regime_memory_score,
                dominant_cluster_state, dominant_archetype_key,
                persistence_state, created_at
            from public.cross_asset_regime_memory_summary
            where workspace_id = %s::uuid
            order by created_at desc, regime_key asc
            limit 50
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_persistence_transition_event_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text       as workspace_id,
                watchlist_id::text       as watchlist_id,
                source_run_id::text      as source_run_id,
                target_run_id::text      as target_run_id,
                regime_key,
                prior_state_signature, current_state_signature,
                prior_persistence_state, current_persistence_state,
                prior_memory_score, current_memory_score, memory_score_delta,
                event_type, reason_codes, created_at
            from public.cross_asset_persistence_transition_event_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 100
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_persistence_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text       as run_id,
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                regime_key,
                cluster_state,
                dominant_archetype_key,
                persistence_state,
                memory_score,
                state_age_runs,
                state_persistence_ratio,
                latest_persistence_event_type,
                created_at
            from public.run_cross_asset_persistence_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_recent_cross_asset_state_history(
    conn, *, workspace_id, watchlist_id=None, limit=50,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            with attribution_recent as (
                select run_id, workspace_id, watchlist_id, created_at
                from public.cross_asset_attribution_summary
                where workspace_id = %s::uuid
                  and (%s::uuid is null or watchlist_id = %s::uuid)
            )
            select
                a.run_id::text       as run_id,
                a.watchlist_id::text as watchlist_id,
                rgi.regime_key,
                diag.dominant_timing_class,
                tra.dominant_transition_state,
                tra.dominant_sequence_class,
                arch.dominant_archetype_key,
                cs.cluster_state,
                a.created_at
            from attribution_recent a
            left join public.run_cross_asset_regime_integration_summary    rgi  on rgi.run_id  = a.run_id
            left join public.run_cross_asset_transition_diagnostics_summary diag on diag.run_id = a.run_id
            left join public.run_cross_asset_transition_attribution_summary tra  on tra.run_id  = a.run_id
            left join public.cross_asset_run_archetype_summary             arch on arch.run_id = a.run_id
            left join public.cross_asset_archetype_cluster_summary         cs   on cs.run_id   = a.run_id
            order by a.created_at desc
            limit %s
            """,
            (workspace_id, watchlist_id, watchlist_id, int(limit)),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.6B: Persistence-Aware Attribution -----
def list_cross_asset_persistence_attribution_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   persistent_weight, recovering_weight, rotating_weight,
                   fragile_weight, breaking_down_weight, mixed_weight,
                   insufficient_history_weight,
                   memory_score_boost_scale, memory_break_penalty_scale,
                   stabilization_bonus_scale, state_age_bonus_scale,
                   persistence_family_overrides, metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_persistence_attribution_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_persistence_attribution_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active,
                   persistent_weight, recovering_weight, rotating_weight,
                   fragile_weight, breaking_down_weight, mixed_weight,
                   insufficient_history_weight,
                   memory_score_boost_scale, memory_break_penalty_scale,
                   stabilization_bonus_scale, state_age_bonus_scale,
                   persistence_family_overrides, metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_persistence_attribution_profiles
            where workspace_id = %s::uuid and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_family_persistence_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    persistence_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_persistence_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                persistence_profile_id, dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_adjusted_family_contribution,
                timing_adjusted_family_contribution,
                transition_adjusted_family_contribution,
                archetype_adjusted_family_contribution,
                cluster_adjusted_family_contribution,
                persistence_state, memory_score, state_age_runs,
                state_persistence_ratio, regime_persistence_ratio,
                cluster_persistence_ratio, archetype_persistence_ratio,
                latest_persistence_event_type,
                persistence_weight, persistence_bonus, persistence_penalty,
                persistence_adjusted_family_contribution,
                persistence_family_rank, top_symbols,
                reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s, %s,
                %s,
                %s, %s::jsonb,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    persistence_profile_id, row["dependency_family"],
                    row.get("raw_family_net_contribution"),
                    row.get("weighted_family_net_contribution"),
                    row.get("regime_adjusted_family_contribution"),
                    row.get("timing_adjusted_family_contribution"),
                    row.get("transition_adjusted_family_contribution"),
                    row.get("archetype_adjusted_family_contribution"),
                    row.get("cluster_adjusted_family_contribution"),
                    row.get("persistence_state", "insufficient_history"),
                    row.get("memory_score"),
                    row.get("state_age_runs"),
                    row.get("state_persistence_ratio"),
                    row.get("regime_persistence_ratio"),
                    row.get("cluster_persistence_ratio"),
                    row.get("archetype_persistence_ratio"),
                    row.get("latest_persistence_event_type"),
                    row.get("persistence_weight"),
                    row.get("persistence_bonus"),
                    row.get("persistence_penalty"),
                    row.get("persistence_adjusted_family_contribution"),
                    row.get("persistence_family_rank"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(list(row.get("reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def insert_cross_asset_symbol_persistence_attribution_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    persistence_profile_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_symbol_persistence_attribution_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                persistence_profile_id,
                symbol, dependency_family, dependency_type,
                persistence_state, memory_score, state_age_runs,
                latest_persistence_event_type,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score,
                archetype_adjusted_symbol_score,
                cluster_adjusted_symbol_score,
                persistence_weight, persistence_adjusted_symbol_score,
                symbol_rank, reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s, %s, %s,
                %s, %s, %s,
                %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    persistence_profile_id,
                    row["symbol"], row["dependency_family"], row.get("dependency_type"),
                    row.get("persistence_state", "insufficient_history"),
                    row.get("memory_score"),
                    row.get("state_age_runs"),
                    row.get("latest_persistence_event_type"),
                    row.get("raw_symbol_score"),
                    row.get("weighted_symbol_score"),
                    row.get("regime_adjusted_symbol_score"),
                    row.get("timing_adjusted_symbol_score"),
                    row.get("transition_adjusted_symbol_score"),
                    row.get("archetype_adjusted_symbol_score"),
                    row.get("cluster_adjusted_symbol_score"),
                    row.get("persistence_weight"),
                    row.get("persistence_adjusted_symbol_score"),
                    row.get("symbol_rank"),
                    json.dumps(list(row.get("reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_family_persistence_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                raw_family_net_contribution,
                weighted_family_net_contribution,
                regime_adjusted_family_contribution,
                timing_adjusted_family_contribution,
                transition_adjusted_family_contribution,
                archetype_adjusted_family_contribution,
                cluster_adjusted_family_contribution,
                persistence_state, memory_score, state_age_runs,
                state_persistence_ratio, regime_persistence_ratio,
                cluster_persistence_ratio, archetype_persistence_ratio,
                latest_persistence_event_type,
                persistence_weight, persistence_bonus, persistence_penalty,
                persistence_adjusted_family_contribution,
                persistence_family_rank, top_symbols,
                reason_codes, created_at
            from public.cross_asset_family_persistence_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, persistence_family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_symbol_persistence_attribution_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                symbol, dependency_family, dependency_type,
                persistence_state, memory_score, state_age_runs,
                latest_persistence_event_type,
                raw_symbol_score, weighted_symbol_score,
                regime_adjusted_symbol_score, timing_adjusted_symbol_score,
                transition_adjusted_symbol_score,
                archetype_adjusted_symbol_score,
                cluster_adjusted_symbol_score,
                persistence_weight, persistence_adjusted_symbol_score,
                symbol_rank, reason_codes, created_at
            from public.cross_asset_symbol_persistence_attribution_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, symbol_rank asc, symbol asc
            limit 300
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_persistence_attribution_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                persistence_adjusted_cross_asset_contribution,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                transition_dominant_dependency_family,
                archetype_dominant_dependency_family,
                cluster_dominant_dependency_family,
                persistence_dominant_dependency_family,
                persistence_state,
                memory_score,
                state_age_runs,
                latest_persistence_event_type,
                created_at
            from public.run_cross_asset_persistence_attribution_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_persistence_context_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select persistence_state, memory_score, state_age_runs,
                   state_persistence_ratio, regime_persistence_ratio,
                   cluster_persistence_ratio, archetype_persistence_ratio,
                   regime_key, cluster_state, dominant_archetype_key
            from public.cross_asset_state_persistence_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None



# ----- Phase 4.6C: Persistence-Aware Composite Refinement -----
def list_cross_asset_persistence_integration_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active, integration_mode,
                   integration_weight,
                   persistent_scale, recovering_scale, rotating_scale,
                   fragile_scale, breaking_down_scale, mixed_scale,
                   insufficient_history_scale,
                   memory_break_extra_suppression,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_persistence_integration_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_persistence_integration_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, profile_name, is_active, integration_mode,
                   integration_weight,
                   persistent_scale, recovering_scale, rotating_scale,
                   fragile_scale, breaking_down_scale, mixed_scale,
                   insufficient_history_scale,
                   memory_break_extra_suppression,
                   max_positive_contribution, max_negative_contribution,
                   metadata, created_at,
                   workspace_id::text as workspace_id
            from public.cross_asset_persistence_integration_profiles
            where workspace_id = %s::uuid and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_persistence_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    persistence_integration_profile_id,
    base_signal_score,
    cross_asset_net_contribution,
    weighted_cross_asset_net_contribution,
    regime_adjusted_cross_asset_contribution,
    timing_adjusted_cross_asset_contribution,
    transition_adjusted_cross_asset_contribution,
    archetype_adjusted_cross_asset_contribution,
    cluster_adjusted_cross_asset_contribution,
    persistence_adjusted_cross_asset_contribution,
    composite_pre_persistence,
    persistence_net_contribution,
    composite_post_persistence,
    persistence_state,
    memory_score,
    state_age_runs,
    latest_persistence_event_type,
    integration_mode,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_persistence_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                persistence_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                persistence_adjusted_cross_asset_contribution,
                composite_pre_persistence,
                persistence_net_contribution,
                composite_post_persistence,
                persistence_state, memory_score, state_age_runs,
                latest_persistence_event_type,
                integration_mode, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s, %s, %s,
                %s, %s, %s,
                %s,
                %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                persistence_integration_profile_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                persistence_adjusted_cross_asset_contribution,
                composite_pre_persistence,
                persistence_net_contribution,
                composite_post_persistence,
                persistence_state, memory_score, state_age_runs,
                latest_persistence_event_type,
                integration_mode,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_persistence_composite_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_persistence_composite_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_persistence_composite_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                persistence_state, memory_score, state_age_runs,
                latest_persistence_event_type,
                persistence_adjusted_family_contribution,
                integration_weight_applied,
                persistence_integration_contribution,
                family_rank, top_symbols, reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s, %s,
                %s,
                %s,
                %s,
                %s,
                %s, %s::jsonb, %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, run_id, context_snapshot_id,
                    row["dependency_family"],
                    row.get("persistence_state", "insufficient_history"),
                    row.get("memory_score"),
                    row.get("state_age_runs"),
                    row.get("latest_persistence_event_type"),
                    row.get("persistence_adjusted_family_contribution"),
                    row.get("integration_weight_applied"),
                    row.get("persistence_integration_contribution"),
                    row.get("family_rank"),
                    json.dumps(list(row.get("top_symbols") or [])),
                    json.dumps(list(row.get("reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_persistence_composite_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                persistence_adjusted_cross_asset_contribution,
                composite_pre_persistence,
                persistence_net_contribution,
                composite_post_persistence,
                persistence_state, memory_score, state_age_runs,
                latest_persistence_event_type,
                integration_mode, created_at
            from public.cross_asset_persistence_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_persistence_composite_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                persistence_state, memory_score, state_age_runs,
                latest_persistence_event_type,
                persistence_adjusted_family_contribution,
                integration_weight_applied,
                persistence_integration_contribution,
                family_rank, top_symbols,
                reason_codes, created_at
            from public.cross_asset_family_persistence_composite_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, family_rank asc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_persistence_integration_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                persistence_adjusted_cross_asset_contribution,
                persistence_net_contribution,
                composite_pre_persistence, composite_post_persistence,
                dominant_dependency_family,
                weighted_dominant_dependency_family,
                regime_dominant_dependency_family,
                timing_dominant_dependency_family,
                transition_dominant_dependency_family,
                archetype_dominant_dependency_family,
                cluster_dominant_dependency_family,
                persistence_dominant_dependency_family,
                persistence_state,
                memory_score,
                state_age_runs,
                latest_persistence_event_type,
                created_at
            from public.run_cross_asset_persistence_integration_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_persistence_attribution_for_run(
    conn, *, run_id, workspace_id=None,  # noqa: ARG001
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select dependency_family,
                   persistence_state, memory_score, state_age_runs,
                   latest_persistence_event_type,
                   persistence_adjusted_family_contribution,
                   persistence_family_rank, top_symbols
            from public.cross_asset_family_persistence_attribution_summary
            where run_id = %s::uuid
            order by persistence_family_rank asc, dependency_family asc
            """,
            (run_id,),
        )
        return [dict(r) for r in cur.fetchall()]



# ----- Phase 4.6D: Replay Validation for Persistence-Aware Composite -----
def insert_cross_asset_persistence_replay_validation_snapshot(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    source_context_snapshot_id,
    replay_context_snapshot_id,
    source_regime_key,
    replay_regime_key,
    source_dominant_timing_class,
    replay_dominant_timing_class,
    source_dominant_transition_state,
    replay_dominant_transition_state,
    source_dominant_sequence_class,
    replay_dominant_sequence_class,
    source_dominant_archetype_key,
    replay_dominant_archetype_key,
    source_cluster_state,
    replay_cluster_state,
    source_persistence_state,
    replay_persistence_state,
    source_memory_score,
    replay_memory_score,
    source_state_age_runs,
    replay_state_age_runs,
    source_latest_persistence_event_type,
    replay_latest_persistence_event_type,
    context_hash_match,
    regime_match,
    timing_class_match,
    transition_state_match,
    sequence_class_match,
    archetype_match,
    cluster_state_match,
    persistence_state_match,
    memory_score_match,
    state_age_match,
    persistence_event_match,
    persistence_attribution_match,
    persistence_composite_match,
    persistence_dominant_family_match,
    persistence_delta,
    persistence_composite_delta,
    drift_reason_codes,
    validation_state,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_persistence_replay_validation_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_state, replay_cluster_state,
                source_persistence_state, replay_persistence_state,
                source_memory_score, replay_memory_score,
                source_state_age_runs, replay_state_age_runs,
                source_latest_persistence_event_type, replay_latest_persistence_event_type,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match,
                archetype_match,
                cluster_state_match,
                persistence_state_match, memory_score_match,
                state_age_match, persistence_event_match,
                persistence_attribution_match, persistence_composite_match,
                persistence_dominant_family_match,
                persistence_delta, persistence_composite_delta,
                drift_reason_codes, validation_state, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid, %s::uuid,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s::jsonb, %s::jsonb,
                %s::jsonb, %s, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                source_context_snapshot_id, replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_state, replay_cluster_state,
                source_persistence_state, replay_persistence_state,
                source_memory_score, replay_memory_score,
                source_state_age_runs, replay_state_age_runs,
                source_latest_persistence_event_type, replay_latest_persistence_event_type,
                bool(context_hash_match), bool(regime_match), bool(timing_class_match),
                bool(transition_state_match), bool(sequence_class_match),
                bool(archetype_match),
                bool(cluster_state_match),
                bool(persistence_state_match), bool(memory_score_match),
                bool(state_age_match), bool(persistence_event_match),
                bool(persistence_attribution_match), bool(persistence_composite_match),
                bool(persistence_dominant_family_match),
                json.dumps(_json_compatible(persistence_delta or {})),
                json.dumps(_json_compatible(persistence_composite_delta or {})),
                json.dumps(list(drift_reason_codes or [])),
                validation_state,
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_persistence_replay_validation_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_persistence_replay_stability_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    replay_run_id,
    rows,
):
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.cross_asset_family_persistence_replay_stability_snapshots (
                workspace_id, watchlist_id, source_run_id, replay_run_id,
                dependency_family,
                source_persistence_state, replay_persistence_state,
                source_memory_score, replay_memory_score,
                source_state_age_runs, replay_state_age_runs,
                source_latest_persistence_event_type, replay_latest_persistence_event_type,
                source_persistence_adjusted_contribution, replay_persistence_adjusted_contribution,
                source_persistence_integration_contribution, replay_persistence_integration_contribution,
                persistence_adjusted_delta, persistence_integration_delta,
                persistence_state_match, memory_score_match,
                state_age_match, persistence_event_match,
                persistence_family_rank_match, persistence_composite_family_rank_match,
                drift_reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb
            )
            """,
            [
                (
                    workspace_id, watchlist_id, source_run_id, replay_run_id,
                    row["dependency_family"],
                    row.get("source_persistence_state"),
                    row.get("replay_persistence_state"),
                    row.get("source_memory_score"),
                    row.get("replay_memory_score"),
                    row.get("source_state_age_runs"),
                    row.get("replay_state_age_runs"),
                    row.get("source_latest_persistence_event_type"),
                    row.get("replay_latest_persistence_event_type"),
                    row.get("source_persistence_adjusted_contribution"),
                    row.get("replay_persistence_adjusted_contribution"),
                    row.get("source_persistence_integration_contribution"),
                    row.get("replay_persistence_integration_contribution"),
                    row.get("persistence_adjusted_delta"),
                    row.get("persistence_integration_delta"),
                    bool(row.get("persistence_state_match", False)),
                    bool(row.get("memory_score_match", False)),
                    bool(row.get("state_age_match", False)),
                    bool(row.get("persistence_event_match", False)),
                    bool(row.get("persistence_family_rank_match", False)),
                    bool(row.get("persistence_composite_family_rank_match", False)),
                    json.dumps(list(row.get("drift_reason_codes") or [])),
                    json.dumps(_json_compatible(row.get("metadata") or {})),
                )
                for row in rows
            ],
        )
    return len(rows)


def get_cross_asset_persistence_replay_validation_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text               as workspace_id,
                watchlist_id::text               as watchlist_id,
                source_run_id::text              as source_run_id,
                replay_run_id::text              as replay_run_id,
                source_context_snapshot_id::text as source_context_snapshot_id,
                replay_context_snapshot_id::text as replay_context_snapshot_id,
                source_regime_key, replay_regime_key,
                source_dominant_timing_class, replay_dominant_timing_class,
                source_dominant_transition_state, replay_dominant_transition_state,
                source_dominant_sequence_class, replay_dominant_sequence_class,
                source_dominant_archetype_key, replay_dominant_archetype_key,
                source_cluster_state, replay_cluster_state,
                source_persistence_state, replay_persistence_state,
                source_memory_score, replay_memory_score,
                source_state_age_runs, replay_state_age_runs,
                source_latest_persistence_event_type, replay_latest_persistence_event_type,
                context_hash_match, regime_match, timing_class_match,
                transition_state_match, sequence_class_match,
                archetype_match,
                cluster_state_match,
                persistence_state_match, memory_score_match,
                state_age_match, persistence_event_match,
                persistence_attribution_match, persistence_composite_match,
                persistence_dominant_family_match,
                drift_reason_codes, validation_state, created_at
            from public.cross_asset_persistence_replay_validation_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_persistence_replay_stability_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text      as workspace_id,
                watchlist_id::text      as watchlist_id,
                source_run_id::text     as source_run_id,
                replay_run_id::text     as replay_run_id,
                dependency_family,
                source_persistence_state, replay_persistence_state,
                source_memory_score, replay_memory_score,
                source_state_age_runs, replay_state_age_runs,
                source_latest_persistence_event_type, replay_latest_persistence_event_type,
                source_persistence_adjusted_contribution, replay_persistence_adjusted_contribution,
                source_persistence_integration_contribution, replay_persistence_integration_contribution,
                persistence_adjusted_delta, persistence_integration_delta,
                persistence_state_match, memory_score_match,
                state_age_match, persistence_event_match,
                persistence_family_rank_match, persistence_composite_family_rank_match,
                drift_reason_codes, created_at
            from public.cross_asset_family_persistence_replay_stability_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_persistence_replay_stability_aggregate(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text as workspace_id,
                validation_count,
                context_match_rate, regime_match_rate,
                timing_class_match_rate,
                transition_state_match_rate, sequence_class_match_rate,
                archetype_match_rate,
                cluster_state_match_rate,
                persistence_state_match_rate,
                memory_score_match_rate,
                state_age_match_rate,
                persistence_event_match_rate,
                persistence_attribution_match_rate,
                persistence_composite_match_rate,
                persistence_dominant_family_match_rate,
                drift_detected_count,
                latest_validated_at
            from public.cross_asset_persistence_replay_stability_aggregate
            where workspace_id = %s::uuid
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_cross_asset_persistence_composite_for_run(conn, *, run_id, workspace_id=None):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text              as run_id,
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                context_snapshot_id::text as context_snapshot_id,
                base_signal_score,
                cross_asset_net_contribution,
                weighted_cross_asset_net_contribution,
                regime_adjusted_cross_asset_contribution,
                timing_adjusted_cross_asset_contribution,
                transition_adjusted_cross_asset_contribution,
                archetype_adjusted_cross_asset_contribution,
                cluster_adjusted_cross_asset_contribution,
                persistence_adjusted_cross_asset_contribution,
                composite_pre_persistence,
                persistence_net_contribution,
                composite_post_persistence,
                persistence_state, memory_score, state_age_runs,
                latest_persistence_event_type,
                integration_mode, created_at
            from public.cross_asset_persistence_composite_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_persistence_state_for_run(conn, *, run_id, workspace_id=None):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select persistence_state, memory_score, state_age_runs,
                   latest_persistence_event_type
            from public.run_cross_asset_persistence_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_memory_score_for_run(conn, *, run_id, workspace_id=None):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select memory_score
            from public.run_cross_asset_persistence_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_latest_persistence_event_for_run(conn, *, run_id, workspace_id=None):  # noqa: ARG001
    with conn.cursor() as cur:
        cur.execute(
            """
            select latest_persistence_event_type
            from public.run_cross_asset_persistence_summary
            where run_id = %s::uuid
            limit 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


# ----- Phase 4.7A: Signal Decay & Stale-Memory Diagnostics -----
def list_cross_asset_signal_decay_policy_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active,
                   regime_half_life_runs, timing_half_life_runs,
                   transition_half_life_runs, archetype_half_life_runs,
                   cluster_half_life_runs, persistence_half_life_runs,
                   fresh_memory_threshold, decaying_memory_threshold,
                   stale_memory_threshold, contradiction_penalty_threshold,
                   metadata, created_at
            from public.cross_asset_signal_decay_policy_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_signal_decay_policy_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active,
                   regime_half_life_runs, timing_half_life_runs,
                   transition_half_life_runs, archetype_half_life_runs,
                   cluster_half_life_runs, persistence_half_life_runs,
                   fresh_memory_threshold, decaying_memory_threshold,
                   stale_memory_threshold, contradiction_penalty_threshold,
                   metadata, created_at
            from public.cross_asset_signal_decay_policy_profiles
            where workspace_id = %s::uuid
              and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_signal_decay_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    decay_policy_profile_id,
    regime_key,
    dominant_timing_class,
    dominant_transition_state,
    dominant_sequence_class,
    dominant_archetype_key,
    cluster_state,
    persistence_state,
    current_state_signature,
    state_age_runs,
    memory_score,
    regime_decay_score,
    timing_decay_score,
    transition_decay_score,
    archetype_decay_score,
    cluster_decay_score,
    persistence_decay_score,
    aggregate_decay_score,
    freshness_state,
    stale_memory_flag,
    contradiction_flag,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_signal_decay_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                decay_policy_profile_id,
                regime_key, dominant_timing_class,
                dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state,
                persistence_state, current_state_signature,
                state_age_runs, memory_score,
                regime_decay_score, timing_decay_score,
                transition_decay_score, archetype_decay_score,
                cluster_decay_score, persistence_decay_score,
                aggregate_decay_score,
                freshness_state, stale_memory_flag, contradiction_flag,
                reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s, %s,
                %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                decay_policy_profile_id,
                regime_key, dominant_timing_class,
                dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state,
                persistence_state, current_state_signature,
                int(state_age_runs) if state_age_runs is not None else None,
                memory_score,
                regime_decay_score, timing_decay_score,
                transition_decay_score, archetype_decay_score,
                cluster_decay_score, persistence_decay_score,
                aggregate_decay_score,
                freshness_state, bool(stale_memory_flag), bool(contradiction_flag),
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_signal_decay_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_signal_decay_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    transition_state,
    dominant_sequence_class,
    archetype_key,
    cluster_state,
    persistence_state,
    family_rank,
    family_contribution,
    family_state_age_runs,
    family_memory_score,
    family_decay_score,
    family_freshness_state,
    stale_family_memory_flag,
    contradicted_family_flag,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_family_signal_decay_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                transition_state, dominant_sequence_class,
                archetype_key, cluster_state, persistence_state,
                family_rank, family_contribution,
                family_state_age_runs, family_memory_score,
                family_decay_score, family_freshness_state,
                stale_family_memory_flag, contradicted_family_flag,
                reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                transition_state, dominant_sequence_class,
                archetype_key, cluster_state, persistence_state,
                int(family_rank) if family_rank is not None else None,
                family_contribution,
                int(family_state_age_runs) if family_state_age_runs is not None else None,
                family_memory_score,
                family_decay_score, family_freshness_state,
                bool(stale_family_memory_flag), bool(contradicted_family_flag),
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_family_signal_decay_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_stale_memory_event_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    target_run_id,
    regime_key,
    prior_freshness_state,
    current_freshness_state,
    prior_state_signature,
    current_state_signature,
    prior_memory_score,
    current_memory_score,
    prior_aggregate_decay_score,
    current_aggregate_decay_score,
    event_type,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_stale_memory_event_snapshots (
                workspace_id, watchlist_id, source_run_id, target_run_id,
                regime_key,
                prior_freshness_state, current_freshness_state,
                prior_state_signature, current_state_signature,
                prior_memory_score, current_memory_score,
                prior_aggregate_decay_score, current_aggregate_decay_score,
                event_type, reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, target_run_id,
                regime_key,
                prior_freshness_state, current_freshness_state,
                prior_state_signature, current_state_signature,
                prior_memory_score, current_memory_score,
                prior_aggregate_decay_score, current_aggregate_decay_score,
                event_type,
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_stale_memory_event_snapshots insert returned no row")
        return dict(row)


def get_cross_asset_signal_decay_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                regime_key, dominant_timing_class,
                dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state, persistence_state,
                current_state_signature, state_age_runs, memory_score,
                regime_decay_score, timing_decay_score,
                transition_decay_score, archetype_decay_score,
                cluster_decay_score, persistence_decay_score,
                aggregate_decay_score,
                freshness_state, stale_memory_flag, contradiction_flag,
                reason_codes, created_at
            from public.cross_asset_signal_decay_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_signal_decay_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                transition_state, dominant_sequence_class,
                archetype_key, cluster_state, persistence_state,
                family_rank, family_contribution,
                family_state_age_runs, family_memory_score,
                family_decay_score, family_freshness_state,
                stale_family_memory_flag, contradicted_family_flag,
                reason_codes, created_at
            from public.cross_asset_family_signal_decay_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_stale_memory_event_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text       as workspace_id,
                watchlist_id::text       as watchlist_id,
                source_run_id::text      as source_run_id,
                target_run_id::text      as target_run_id,
                regime_key,
                prior_freshness_state, current_freshness_state,
                prior_state_signature, current_state_signature,
                prior_memory_score, current_memory_score,
                prior_aggregate_decay_score, current_aggregate_decay_score,
                event_type, reason_codes, created_at
            from public.cross_asset_stale_memory_event_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 100
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_signal_decay_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text       as run_id,
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                regime_key,
                persistence_state,
                memory_score,
                freshness_state,
                aggregate_decay_score,
                stale_memory_flag,
                contradiction_flag,
                latest_stale_memory_event_type,
                created_at
            from public.run_cross_asset_signal_decay_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


# --- get_recent_cross_asset_decay_history (consolidated) ---
def get_recent_cross_asset_decay_history(
    conn, *, workspace_id, watchlist_id=None, limit=50,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text       as run_id,
                watchlist_id::text as watchlist_id,
                regime_key,
                freshness_state,
                memory_score,
                aggregate_decay_score,
                stale_memory_flag,
                contradiction_flag,
                created_at
            from public.cross_asset_signal_decay_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit %s
            """,
            (workspace_id, watchlist_id, watchlist_id, int(limit)),
        )
        return [dict(r) for r in cur.fetchall()]


# ----- Phase 4.7A: Signal Decay & Stale-Memory Diagnostics -----
def list_cross_asset_signal_decay_policy_profiles(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active,
                   regime_half_life_runs, timing_half_life_runs,
                   transition_half_life_runs, archetype_half_life_runs,
                   cluster_half_life_runs, persistence_half_life_runs,
                   fresh_memory_threshold, decaying_memory_threshold,
                   stale_memory_threshold, contradiction_penalty_threshold,
                   metadata, created_at
            from public.cross_asset_signal_decay_policy_profiles
            where workspace_id = %s::uuid
            order by is_active desc, created_at desc
            limit 20
            """,
            (workspace_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_active_cross_asset_signal_decay_policy_profile(conn, *, workspace_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            select id::text as id, workspace_id::text as workspace_id,
                   profile_name, is_active,
                   regime_half_life_runs, timing_half_life_runs,
                   transition_half_life_runs, archetype_half_life_runs,
                   cluster_half_life_runs, persistence_half_life_runs,
                   fresh_memory_threshold, decaying_memory_threshold,
                   stale_memory_threshold, contradiction_penalty_threshold,
                   metadata, created_at
            from public.cross_asset_signal_decay_policy_profiles
            where workspace_id = %s::uuid
              and is_active = true
            order by created_at desc
            limit 1
            """,
            (workspace_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def insert_cross_asset_signal_decay_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    decay_policy_profile_id,
    regime_key,
    dominant_timing_class,
    dominant_transition_state,
    dominant_sequence_class,
    dominant_archetype_key,
    cluster_state,
    persistence_state,
    current_state_signature,
    state_age_runs,
    memory_score,
    regime_decay_score,
    timing_decay_score,
    transition_decay_score,
    archetype_decay_score,
    cluster_decay_score,
    persistence_decay_score,
    aggregate_decay_score,
    freshness_state,
    stale_memory_flag,
    contradiction_flag,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_signal_decay_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                decay_policy_profile_id,
                regime_key, dominant_timing_class,
                dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state,
                persistence_state, current_state_signature,
                state_age_runs, memory_score,
                regime_decay_score, timing_decay_score,
                transition_decay_score, archetype_decay_score,
                cluster_decay_score, persistence_decay_score,
                aggregate_decay_score,
                freshness_state, stale_memory_flag, contradiction_flag,
                reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s::uuid,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s, %s,
                %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                decay_policy_profile_id,
                regime_key, dominant_timing_class,
                dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state,
                persistence_state, current_state_signature,
                int(state_age_runs) if state_age_runs is not None else None,
                memory_score,
                regime_decay_score, timing_decay_score,
                transition_decay_score, archetype_decay_score,
                cluster_decay_score, persistence_decay_score,
                aggregate_decay_score,
                freshness_state, bool(stale_memory_flag), bool(contradiction_flag),
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_signal_decay_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_family_signal_decay_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    run_id,
    context_snapshot_id,
    dependency_family,
    transition_state,
    dominant_sequence_class,
    archetype_key,
    cluster_state,
    persistence_state,
    family_rank,
    family_contribution,
    family_state_age_runs,
    family_memory_score,
    family_decay_score,
    family_freshness_state,
    stale_family_memory_flag,
    contradicted_family_flag,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_family_signal_decay_snapshots (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                transition_state, dominant_sequence_class,
                archetype_key, cluster_state, persistence_state,
                family_rank, family_contribution,
                family_state_age_runs, family_memory_score,
                family_decay_score, family_freshness_state,
                stale_family_memory_flag, contradicted_family_flag,
                reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, run_id, context_snapshot_id,
                dependency_family,
                transition_state, dominant_sequence_class,
                archetype_key, cluster_state, persistence_state,
                int(family_rank) if family_rank is not None else None,
                family_contribution,
                int(family_state_age_runs) if family_state_age_runs is not None else None,
                family_memory_score,
                family_decay_score, family_freshness_state,
                bool(stale_family_memory_flag), bool(contradicted_family_flag),
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_family_signal_decay_snapshots insert returned no row")
        return dict(row)


def insert_cross_asset_stale_memory_event_snapshots(
    conn,
    *,
    workspace_id,
    watchlist_id,
    source_run_id,
    target_run_id,
    regime_key,
    prior_freshness_state,
    current_freshness_state,
    prior_state_signature,
    current_state_signature,
    prior_memory_score,
    current_memory_score,
    prior_aggregate_decay_score,
    current_aggregate_decay_score,
    event_type,
    reason_codes,
    metadata,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.cross_asset_stale_memory_event_snapshots (
                workspace_id, watchlist_id, source_run_id, target_run_id,
                regime_key,
                prior_freshness_state, current_freshness_state,
                prior_state_signature, current_state_signature,
                prior_memory_score, current_memory_score,
                prior_aggregate_decay_score, current_aggregate_decay_score,
                event_type, reason_codes, metadata
            )
            values (
                %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s::jsonb, %s::jsonb
            )
            returning id::text as id, created_at
            """,
            (
                workspace_id, watchlist_id, source_run_id, target_run_id,
                regime_key,
                prior_freshness_state, current_freshness_state,
                prior_state_signature, current_state_signature,
                prior_memory_score, current_memory_score,
                prior_aggregate_decay_score, current_aggregate_decay_score,
                event_type,
                json.dumps(list(reason_codes or [])),
                json.dumps(_json_compatible(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("cross_asset_stale_memory_event_snapshots insert returned no row")
        return dict(row)


def get_cross_asset_signal_decay_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                regime_key, dominant_timing_class,
                dominant_transition_state, dominant_sequence_class,
                dominant_archetype_key, cluster_state, persistence_state,
                current_state_signature, state_age_runs, memory_score,
                regime_decay_score, timing_decay_score,
                transition_decay_score, archetype_decay_score,
                cluster_decay_score, persistence_decay_score,
                aggregate_decay_score,
                freshness_state, stale_memory_flag, contradiction_flag,
                reason_codes, created_at
            from public.cross_asset_signal_decay_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_family_signal_decay_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text        as workspace_id,
                watchlist_id::text        as watchlist_id,
                run_id::text              as run_id,
                context_snapshot_id::text as context_snapshot_id,
                dependency_family,
                transition_state, dominant_sequence_class,
                archetype_key, cluster_state, persistence_state,
                family_rank, family_contribution,
                family_state_age_runs, family_memory_score,
                family_decay_score, family_freshness_state,
                stale_family_memory_flag, contradicted_family_flag,
                reason_codes, created_at
            from public.cross_asset_family_signal_decay_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc, dependency_family asc
            limit 200
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_cross_asset_stale_memory_event_summary(
    conn, *, workspace_id, watchlist_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                workspace_id::text       as workspace_id,
                watchlist_id::text       as watchlist_id,
                source_run_id::text      as source_run_id,
                target_run_id::text      as target_run_id,
                regime_key,
                prior_freshness_state, current_freshness_state,
                prior_state_signature, current_state_signature,
                prior_memory_score, current_memory_score,
                prior_aggregate_decay_score, current_aggregate_decay_score,
                event_type, reason_codes, created_at
            from public.cross_asset_stale_memory_event_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit 100
            """,
            (workspace_id, watchlist_id, watchlist_id),
        )
        return [dict(r) for r in cur.fetchall()]


def get_run_cross_asset_signal_decay_summary(
    conn, *, run_id=None, workspace_id=None,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text       as run_id,
                workspace_id::text as workspace_id,
                watchlist_id::text as watchlist_id,
                regime_key,
                persistence_state,
                memory_score,
                freshness_state,
                aggregate_decay_score,
                stale_memory_flag,
                contradiction_flag,
                latest_stale_memory_event_type,
                created_at
            from public.run_cross_asset_signal_decay_summary
            where (%s::uuid is null or run_id = %s::uuid)
              and (%s::uuid is null or workspace_id = %s::uuid)
            order by created_at desc
            limit 50
            """,
            (run_id, run_id, workspace_id, workspace_id),
        )
        return [dict(r) for r in cur.fetchall()]


# --- get_recent_cross_asset_decay_history (consolidated) ---
def get_recent_cross_asset_decay_history(
    conn, *, workspace_id, watchlist_id=None, limit=50,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                run_id::text       as run_id,
                watchlist_id::text as watchlist_id,
                regime_key,
                freshness_state,
                memory_score,
                aggregate_decay_score,
                stale_memory_flag,
                contradiction_flag,
                created_at
            from public.cross_asset_signal_decay_summary
            where workspace_id = %s::uuid
              and (%s::uuid is null or watchlist_id = %s::uuid)
            order by created_at desc
            limit %s
            """,
            (workspace_id, watchlist_id, watchlist_id, int(limit)),
        )
        return [dict(r) for r in cur.fetchall()]
