from __future__ import annotations

import json
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from psycopg import Connection, connect
from psycopg.rows import dict_row

from src.config import get_settings
from src.db.repositories import create_alert_event
from src.jobs.retry_policy import (
    classify_retry_outcome,
    retry_scheduled_message,
    terminal_failure_message,
)


@dataclass
class CheckResult:
    status: str
    name: str
    detail: str


class Recorder:
    def __init__(self) -> None:
        self.results: list[CheckResult] = []

    def pass_(self, name: str, detail: str) -> None:
        self.results.append(CheckResult("PASS", name, detail))

    def warn(self, name: str, detail: str) -> None:
        self.results.append(CheckResult("WARN", name, detail))

    def fail(self, name: str, detail: str) -> None:
        self.results.append(CheckResult("FAIL", name, detail))

    def check(self, condition: bool, name: str, detail: str) -> None:
        if condition:
            self.pass_(name, detail)
        else:
            self.fail(name, detail)

    def count(self, status: str) -> int:
        return sum(1 for result in self.results if result.status == status)


def open_connection() -> Connection[Any]:
    settings = get_settings()
    return connect(settings.database_url, autocommit=False, row_factory=dict_row)


def fetch_one(conn: Connection[Any], query: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"expected row for query: {query}")
        return dict(row)


def fetch_all(conn: Connection[Any], query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def execute(conn: Connection[Any], query: str, params: tuple[Any, ...] = ()) -> None:
    with conn.cursor() as cur:
        cur.execute(query, params)


def make_fixture(conn: Connection[Any], prefix: str) -> dict[str, str]:
    suffix = uuid.uuid4().hex[:8]
    workspace_slug = f"validate-{prefix}-{suffix}"
    workspace_name = f"Validate {prefix} {suffix}"
    workspace = fetch_one(
        conn,
        """
        insert into public.workspaces (slug, name)
        values (%s, %s)
        returning id::text as id, slug
        """,
        (workspace_slug, workspace_name),
    )

    watchlists: dict[str, str] = {}
    for slug in ("hot", "cold", "manual"):
        row = fetch_one(
            conn,
            """
            insert into public.watchlists (workspace_id, slug, name)
            values (%s::uuid, %s, %s)
            returning id::text as id
            """,
            (workspace["id"], slug, f"{slug.title()} Watchlist"),
        )
        watchlists[slug] = row["id"]

    return {
        "workspace_id": workspace["id"],
        "workspace_slug": workspace["slug"],
        "hot_watchlist_id": watchlists["hot"],
        "cold_watchlist_id": watchlists["cold"],
        "manual_watchlist_id": watchlists["manual"],
    }


def enqueue_job(
    conn: Connection[Any],
    workspace_slug: str,
    *,
    trigger_type: str = "api",
    requested_by: str = "validator",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = fetch_one(
        conn,
        "select * from public.enqueue_recompute_job(%s, %s, %s, %s::jsonb)",
        (
            workspace_slug,
            trigger_type,
            requested_by,
            json.dumps(payload or {}),
        ),
    )
    queue = fetch_one(
        conn,
        """
        select id as queue_id, job_id::text as job_id, workspace_id::text as workspace_id
        from public.job_queue
        where job_id = %s::uuid
        """,
        (row["job_id"],),
    )
    return {
        "job_id": row["job_id"],
        "workspace_id": row["workspace_id"],
        "queue_id": queue["queue_id"],
    }


def set_job_shape(
    conn: Connection[Any],
    *,
    job_id: str,
    watchlist_id: str | None = None,
    priority: int = 100,
    available_at: datetime | None = None,
) -> None:
    available_at = available_at or datetime.now(timezone.utc)
    execute(
        conn,
        """
        update public.job_runs
        set watchlist_id = %s::uuid
        where id = %s::uuid
        """,
        (watchlist_id, job_id),
    )
    execute(
        conn,
        """
        update public.job_queue
        set priority = %s,
            available_at = %s,
            next_retry_at = null
        where job_id = %s::uuid
        """,
        (priority, available_at, job_id),
    )


def claim_next_job(conn: Connection[Any], worker_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute("select * from public.claim_recompute_job(%s)", (worker_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def schedule_retry(
    conn: Connection[Any],
    queue_id: int,
    error_message: str,
    *,
    failure_stage: str = "worker",
) -> dict[str, Any]:
    return fetch_one(
        conn,
        "select * from public.schedule_job_retry(%s, %s, %s)",
        (queue_id, error_message[:500], failure_stage),
    )


def complete_job(conn: Connection[Any], job_id: str) -> None:
    execute(conn, "select public.complete_recompute_job(%s::uuid)", (job_id,))


def queue_state(conn: Connection[Any], queue_id: int) -> dict[str, Any]:
    return fetch_one(
        conn,
        """
        select
          id,
          job_id::text as job_id,
          workspace_id::text as workspace_id,
          available_at,
          locked_at,
          locked_by,
          claim_expires_at,
          retry_count,
          max_retries,
          next_retry_at,
          priority,
          last_error
        from public.job_queue
        where id = %s
        """,
        (queue_id,),
    )


def job_state(conn: Connection[Any], job_id: str) -> dict[str, Any]:
    return fetch_one(
        conn,
        """
        select
          id::text as id,
          workspace_id::text as workspace_id,
          watchlist_id::text as watchlist_id,
          status,
          trigger_type,
          attempt_count,
          max_attempts,
          error_message,
          metadata,
          claimed_by,
          claimed_at,
          finished_at
        from public.job_runs
        where id = %s::uuid
        """,
        (job_id,),
    )


def dead_letters_for_queue(conn: Connection[Any], queue_id: int) -> list[dict[str, Any]]:
    return fetch_all(
        conn,
        """
        select
          id,
          queue_job_id,
          job_run_id::text as job_run_id,
          workspace_id::text as workspace_id,
          watchlist_id::text as watchlist_id,
          retry_count,
          max_retries,
          last_error,
          failure_stage,
          failed_at,
          requeued_at,
          metadata
        from public.job_dead_letters
        where queue_job_id = %s
        order by id asc
        """,
        (queue_id,),
    )


def fast_forward_retry_window(conn: Connection[Any], queue_id: int) -> None:
    execute(
        conn,
        """
        update public.job_queue
        set available_at = now() - interval '1 second',
            next_retry_at = now() - interval '1 second'
        where id = %s
        """,
        (queue_id,),
    )


def emit_worker_failure_alert(
    conn: Connection[Any],
    workspace_id: str,
    job_type: str,
    error_message: str,
    outcome: dict[str, Any],
) -> None:
    if classify_retry_outcome(outcome) == "retry_scheduled":
        _, title, body = retry_scheduled_message(job_type, outcome.get("next_retry_at"))
        create_alert_event(
            conn,
            workspace_id,
            title,
            body,
            "warning",
            {"retry_count": outcome.get("retry_count")},
        )
        return

    _, title, body = terminal_failure_message(job_type, error_message)
    create_alert_event(
        conn,
        workspace_id,
        title,
        body,
        "high",
        {
            "retry_count": outcome.get("retry_count"),
            "action": outcome.get("action"),
        },
    )


def alert_count(conn: Connection[Any], workspace_id: str) -> int:
    row = fetch_one(
        conn,
        "select count(*) as total from public.alert_events where workspace_id = %s::uuid",
        (workspace_id,),
    )
    return int(row["total"])


def make_dead_letter(conn: Connection[Any], fixture: dict[str, str], *, label: str) -> dict[str, Any]:
    job = enqueue_job(
        conn,
        fixture["workspace_slug"],
        trigger_type="manual",
        requested_by=f"validator-{label}",
        payload={"label": label},
    )
    set_job_shape(
        conn,
        job_id=job["job_id"],
        watchlist_id=fixture["manual_watchlist_id"],
        priority=1,
        available_at=datetime.now(timezone.utc) - timedelta(minutes=5),
    )

    for attempt in range(1, 4):
        claimed = claim_next_job(conn, f"worker-{label}-{attempt}")
        if claimed is None:
            raise RuntimeError(f"expected claimed job for dead-letter setup {label}")
        outcome = schedule_retry(conn, int(claimed["queue_id"]), f"{label}-failure-{attempt}")
        if outcome["action"] == "retry":
            fast_forward_retry_window(conn, int(claimed["queue_id"]))

    dead_letters = dead_letters_for_queue(conn, int(job["queue_id"]))
    if len(dead_letters) != 1:
        raise RuntimeError(f"expected 1 dead letter for {label}, found {len(dead_letters)}")

    return {
        "job_id": job["job_id"],
        "queue_id": int(job["queue_id"]),
        "dead_letter_id": int(dead_letters[0]["id"]),
        "dead_letter": dead_letters[0],
    }


def scenario_retry_correctness(recorder: Recorder) -> None:
    conn = open_connection()
    try:
        fixture = make_fixture(conn, "retry")
        job = enqueue_job(
            conn,
            fixture["workspace_slug"],
            trigger_type="api",
            requested_by="validator-transient",
            payload={"case": "transient-db-network"},
        )
        set_job_shape(
            conn,
            job_id=job["job_id"],
            watchlist_id=fixture["manual_watchlist_id"],
            priority=1,
            available_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        )

        claimed = claim_next_job(conn, "worker-retry")
        if claimed is None:
            raise RuntimeError("expected claim for transient retry test")

        outcome = schedule_retry(conn, int(claimed["queue_id"]), "transient network timeout")
        emit_worker_failure_alert(conn, fixture["workspace_id"], "recompute", "transient network timeout", outcome)

        queue = queue_state(conn, int(claimed["queue_id"]))
        delta_seconds = (outcome["next_retry_at"] - datetime.now(timezone.utc)).total_seconds()

        recorder.check(
            outcome["action"] == "retry",
            "Transient issue schedules retry",
            f"action={outcome['action']} retry_count={outcome['retry_count']}",
        )
        recorder.check(
            25 <= delta_seconds <= 35,
            "Retry backoff starts at 30 seconds",
            f"observed_delay_seconds={delta_seconds:.2f}",
        )
        recorder.check(
            queue["retry_count"] == 1 and queue["locked_at"] is None and queue["locked_by"] is None,
            "Retry releases lock and increments retry_count",
            f"retry_count={queue['retry_count']} locked_by={queue['locked_by']}",
        )
        recorder.check(
            len(dead_letters_for_queue(conn, int(claimed["queue_id"]))) == 0,
            "Transient retry does not dead-letter early",
            "job_dead_letters remained empty before retry window elapsed",
        )
        recorder.check(
            alert_count(conn, fixture["workspace_id"]) == 1,
            "Retry alert emits once per retry edge",
            f"workspace_alert_events={alert_count(conn, fixture['workspace_id'])}",
        )

        fast_forward_retry_window(conn, int(claimed["queue_id"]))
        reclaimed = claim_next_job(conn, "worker-retry-second-pass")
        if reclaimed is None:
            raise RuntimeError("expected second claim after retry window")
        complete_job(conn, reclaimed["job_id"])

        recorder.check(
            len(dead_letters_for_queue(conn, int(claimed["queue_id"]))) == 0,
            "Transient retry can recover without dead-lettering",
            "job completed after one retry",
        )
    finally:
        conn.rollback()
        conn.close()


def scenario_queue_fairness(recorder: Recorder) -> None:
    conn = open_connection()
    try:
        fixture = make_fixture(conn, "fairness")
        base_time = datetime.now(timezone.utc) - timedelta(minutes=10)

        for index in range(4):
            job = enqueue_job(
                conn,
                fixture["workspace_slug"],
                trigger_type="api",
                requested_by=f"hot-{index}",
                payload={"lane": "hot", "index": index},
            )
            set_job_shape(
                conn,
                job_id=job["job_id"],
                watchlist_id=fixture["hot_watchlist_id"],
                priority=50,
                available_at=base_time + timedelta(seconds=index),
            )

        cold_job = enqueue_job(
            conn,
            fixture["workspace_slug"],
            trigger_type="api",
            requested_by="cold",
            payload={"lane": "cold"},
        )
        set_job_shape(
            conn,
            job_id=cold_job["job_id"],
            watchlist_id=fixture["cold_watchlist_id"],
            priority=50,
            available_at=base_time + timedelta(seconds=5),
        )

        claimed_watchlists: list[str | None] = []
        for index in range(5):
            claimed = claim_next_job(conn, f"worker-fairness-{index}")
            if claimed is None:
                raise RuntimeError("expected claim during watchlist fairness test")
            state = job_state(conn, claimed["job_id"])
            claimed_watchlists.append(state["watchlist_id"])
            complete_job(conn, claimed["job_id"])

        recorder.check(
            claimed_watchlists[:4] == [fixture["hot_watchlist_id"]] * 4,
            "Hot watchlist can consume the queue head repeatedly",
            f"claim_order_watchlists={claimed_watchlists}",
        )
        recorder.fail(
            "No per-watchlist fairness guard",
            "claim_recompute_job orders globally by priority/available_at/id; a hot watchlist can starve others until its backlog drains",
        )

        execute(conn, "delete from public.job_queue")
        execute(conn, "delete from public.job_runs where workspace_id = %s::uuid", (fixture["workspace_id"],))

        scheduled = enqueue_job(
            conn,
            fixture["workspace_slug"],
            trigger_type="cron",
            requested_by="scheduler",
            payload={"lane": "scheduled"},
        )
        manual = enqueue_job(
            conn,
            fixture["workspace_slug"],
            trigger_type="manual",
            requested_by="operator",
            payload={"lane": "manual"},
        )
        set_job_shape(
            conn,
            job_id=scheduled["job_id"],
            watchlist_id=fixture["manual_watchlist_id"],
            priority=100,
            available_at=base_time,
        )
        set_job_shape(
            conn,
            job_id=manual["job_id"],
            watchlist_id=fixture["manual_watchlist_id"],
            priority=100,
            available_at=base_time + timedelta(seconds=30),
        )
        first = claim_next_job(conn, "worker-scheduled-vs-manual")
        if first is None:
            raise RuntimeError("expected claim in scheduled/manual fairness test")
        first_state = job_state(conn, first["job_id"])

        recorder.check(
            first_state["trigger_type"] == "cron",
            "Scheduled work wins over manual work when older and same priority",
            f"first_trigger_type={first_state['trigger_type']}",
        )
        recorder.fail(
            "Manual jobs are not protected from scheduled backlog",
            "trigger_type is ignored during claim; older scheduled jobs crowd out manual jobs unless operators manually lower priority values",
        )

        execute(conn, "delete from public.job_queue")
        execute(conn, "delete from public.job_runs where workspace_id = %s::uuid", (fixture["workspace_id"],))

        low = enqueue_job(
            conn,
            fixture["workspace_slug"],
            trigger_type="manual",
            requested_by="low-priority",
            payload={"lane": "low"},
        )
        high = enqueue_job(
            conn,
            fixture["workspace_slug"],
            trigger_type="manual",
            requested_by="high-priority",
            payload={"lane": "high"},
        )
        set_job_shape(
            conn,
            job_id=low["job_id"],
            watchlist_id=fixture["manual_watchlist_id"],
            priority=100,
            available_at=base_time,
        )
        set_job_shape(
            conn,
            job_id=high["job_id"],
            watchlist_id=fixture["manual_watchlist_id"],
            priority=10,
            available_at=base_time,
        )
        first = claim_next_job(conn, "worker-priority-a")
        second = claim_next_job(conn, "worker-priority-b")
        if first is None or second is None:
            raise RuntimeError("expected two claims in priority test")

        recorder.check(
            int(first["queue_id"]) == int(high["queue_id"]) and int(second["queue_id"]) == int(low["queue_id"]),
            "High-priority items are consumed predictably",
            f"first_queue_id={first['queue_id']} second_queue_id={second['queue_id']}",
        )
    finally:
        conn.rollback()
        conn.close()


def scenario_terminal_failure(recorder: Recorder) -> None:
    conn = open_connection()
    try:
        fixture = make_fixture(conn, "terminal")
        job = enqueue_job(
            conn,
            fixture["workspace_slug"],
            trigger_type="manual",
            requested_by="validator-terminal",
            payload={"case": "deterministic-compute"},
        )
        set_job_shape(
            conn,
            job_id=job["job_id"],
            watchlist_id=fixture["manual_watchlist_id"],
            priority=1,
            available_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        )

        errors = [
            "deterministic compute failure: division by zero",
            "deterministic compute failure: division by zero",
            "deterministic compute failure: division by zero",
        ]
        outcomes: list[dict[str, Any]] = []
        for attempt, error in enumerate(errors, start=1):
            claimed = claim_next_job(conn, f"worker-terminal-{attempt}")
            if claimed is None:
                raise RuntimeError(f"expected claim on terminal attempt {attempt}")
            outcome = schedule_retry(conn, int(claimed["queue_id"]), error)
            outcomes.append(outcome)
            emit_worker_failure_alert(conn, fixture["workspace_id"], "recompute", error, outcome)
            if outcome["action"] == "retry":
                fast_forward_retry_window(conn, int(claimed["queue_id"]))

        recorder.warn(
            "Deterministic compute failures are retried like transient failures",
            f"first_action={outcomes[0]['action']} retry_count={outcomes[0]['retry_count']}; there is no transient vs terminal classifier in the worker/control plane",
        )

        dead_letters = dead_letters_for_queue(conn, int(job["queue_id"]))
        recorder.check(
            len(dead_letters) == 1 and outcomes[-1]["action"] == "dead_letter",
            "Terminal failure writes one dead-letter row on first terminal edge",
            f"dead_letter_rows={len(dead_letters)} final_action={outcomes[-1]['action']}",
        )
        recorder.check(
            alert_count(conn, fixture["workspace_id"]) == 3,
            "Alerts are not duplicated before terminal promotion",
            f"workspace_alert_events={alert_count(conn, fixture['workspace_id'])}",
        )

        queue = queue_state(conn, int(job["queue_id"]))
        terminal_claim = claim_next_job(conn, "worker-after-dead-letter")
        if terminal_claim is not None:
            recorder.fail(
                "Dead-lettered queue row remains claimable",
                f"queue_retry_count={queue['retry_count']} queue_id={queue['id']}",
            )
            duplicate_outcome = schedule_retry(
                conn,
                int(terminal_claim["queue_id"]),
                "deterministic compute failure: duplicate terminal edge",
            )
            emit_worker_failure_alert(
                conn,
                fixture["workspace_id"],
                "recompute",
                "deterministic compute failure: duplicate terminal edge",
                duplicate_outcome,
            )
            duplicate_rows = dead_letters_for_queue(conn, int(job["queue_id"]))
            recorder.check(
                len(duplicate_rows) == 1,
                "Terminal failure dead-letters exactly once",
                f"dead_letter_rows_after_reclaim={len(duplicate_rows)}",
            )
        else:
            recorder.pass_(
                "Dead-lettered queue row is no longer claimable",
                f"queue_id={queue['id']}",
            )
    finally:
        conn.rollback()
        conn.close()


def scenario_requeue_correctness(recorder: Recorder) -> None:
    conn = open_connection()
    try:
        fixture = make_fixture(conn, "requeue")

        clean = make_dead_letter(conn, fixture, label="clean-reset")
        original = clean["dead_letter"]
        new_job = fetch_one(
            conn,
            "select public.requeue_dead_letter(%s, %s) as new_job_id",
            (clean["dead_letter_id"], True),
        )
        new_job_state = job_state(conn, new_job["new_job_id"])
        new_queue = fetch_one(
            conn,
            """
            select
              id,
              job_id::text as job_id,
              retry_count,
              max_retries,
              priority
            from public.job_queue
            where job_id = %s::uuid
            """,
            (new_job["new_job_id"],),
        )
        updated_dead_letter = fetch_one(
            conn,
            """
            select
              id,
              requeued_at,
              last_error,
              failure_stage,
              failed_at,
              metadata
            from public.job_dead_letters
            where id = %s
            """,
            (clean["dead_letter_id"],),
        )

        recorder.check(
            new_job_state["id"] != clean["job_id"] and new_queue["retry_count"] == 0,
            "Requeue with reset starts a clean queue lifecycle",
            f"new_job_id={new_job_state['id']} new_retry_count={new_queue['retry_count']}",
        )
        recorder.check(
            new_job_state["metadata"].get("requeued_from_dead_letter_id") == clean["dead_letter_id"],
            "Requeue preserves lineage in the new job metadata",
            f"metadata={new_job_state['metadata']}",
        )
        recorder.check(
            updated_dead_letter["last_error"] == original["last_error"]
            and updated_dead_letter["failure_stage"] == original["failure_stage"]
            and updated_dead_letter["failed_at"] == original["failed_at"],
            "Old failure context remains inspectable",
            "last_error/failure_stage/failed_at stayed intact",
        )
        if updated_dead_letter["requeued_at"] is not None:
            recorder.fail(
                "Requeue mutates historical dead-letter evidence",
                f"requeued_at={updated_dead_letter['requeued_at']} metadata={updated_dead_letter['metadata']}",
            )
        else:
            recorder.pass_(
                "Requeue leaves historical dead-letter evidence untouched",
                f"dead_letter_id={clean['dead_letter_id']}",
            )

        execute(
            conn,
            """
            update public.job_queue
            set available_at = now() + interval '1 day'
            where job_id = %s::uuid or id = %s
            """,
            (new_job["new_job_id"], clean["queue_id"]),
        )

        inherited = make_dead_letter(conn, fixture, label="inherit-retries")
        inherited_job = fetch_one(
            conn,
            "select public.requeue_dead_letter(%s, %s) as new_job_id",
            (inherited["dead_letter_id"], False),
        )
        inherited_queue = fetch_one(
            conn,
            """
            select retry_count, max_retries
            from public.job_queue
            where job_id = %s::uuid
            """,
            (inherited_job["new_job_id"],),
        )
        recorder.check(
            inherited_queue["retry_count"] == 0,
            "Default requeue path creates a clean retry budget",
            f"default_retry_count={inherited_queue['retry_count']} max_retries={inherited_queue['max_retries']}",
        )
    finally:
        conn.rollback()
        conn.close()


def scenario_worker_liveness(recorder: Recorder) -> None:
    conn = open_connection()
    try:
        fixture = make_fixture(conn, "workers")

        execute(
            conn,
            """
            select public.heartbeat_worker(
              %s,
              %s::uuid,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb
            )
            """,
            (
                "worker-idle-live",
                fixture["workspace_id"],
                "validator-host",
                4242,
                "alive",
                json.dumps({"kind": "validator"}),
                json.dumps({"note": "idle"}),
            ),
        )
        first_seen = fetch_one(
            conn,
            "select last_seen_at from public.worker_heartbeats where worker_id = %s",
            ("worker-idle-live",),
        )["last_seen_at"]
        execute(
            conn,
            """
            select public.heartbeat_worker(
              %s,
              %s::uuid,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb
            )
            """,
            (
                "worker-idle-live",
                fixture["workspace_id"],
                "validator-host",
                4242,
                "alive",
                json.dumps({"kind": "validator"}),
                json.dumps({"note": "idle-second-pass"}),
            ),
        )
        second_seen = fetch_one(
            conn,
            "select last_seen_at from public.worker_heartbeats where worker_id = %s",
            ("worker-idle-live",),
        )["last_seen_at"]
        live_stale = fetch_all(
            conn,
            "select worker_id from public.stale_workers where worker_id = %s",
            ("worker-idle-live",),
        )
        recorder.check(
            second_seen >= first_seen and len(live_stale) == 0,
            "Idle workers stay healthy with heartbeats even when no jobs exist",
            f"first_seen={first_seen.isoformat()} second_seen={second_seen.isoformat()}",
        )

        for worker_id in ("worker-killed-hard", "worker-hung-alive"):
            execute(
                conn,
                """
                insert into public.worker_heartbeats (
                  worker_id, workspace_id, hostname, pid, status, capabilities, metadata, started_at, last_seen_at
                ) values (
                  %s, %s::uuid, %s, %s, %s, %s::jsonb, %s::jsonb, now(), now()
                )
                """,
                (
                    worker_id,
                    fixture["workspace_id"],
                    "validator-host",
                    5252,
                    "alive",
                    json.dumps({}),
                    json.dumps({"simulated": worker_id}),
                ),
            )

        execute(
            conn,
            """
            update public.worker_heartbeats
            set last_seen_at = now() - interval '120 seconds'
            where worker_id in ('worker-killed-hard', 'worker-hung-alive')
            """,
        )
        stale = fetch_all(
            conn,
            """
            select worker_id, seconds_since_seen
            from public.stale_workers
            where worker_id in ('worker-killed-hard', 'worker-hung-alive')
            order by worker_id asc
            """,
        )
        stale_ids = {row["worker_id"] for row in stale}

        recorder.check(
            "worker-killed-hard" in stale_ids,
            "Hard-killed workers appear in stale_workers",
            f"stale_worker_ids={sorted(stale_ids)}",
        )
        recorder.check(
            "worker-hung-alive" in stale_ids,
            "Hung workers eventually appear in stale_workers",
            f"stale_worker_ids={sorted(stale_ids)}",
        )
        recorder.warn(
            "stale_workers is timestamp-only, not operator-semantic",
            "the view cannot distinguish hard-kill, hung-but-alive, DB partition, or heartbeat loop stall; it only reports last_seen_at older than 90 seconds",
        )
    finally:
        conn.rollback()
        conn.close()


def run_scenario(name: str, callback: Callable[[Recorder], None], recorder: Recorder) -> None:
    recorder.pass_(f"Scenario start: {name}", "running validation in rollback-only transaction")
    callback(recorder)


def main() -> int:
    recorder = Recorder()
    started_at = datetime.now(timezone.utc)

    scenarios: list[tuple[str, Callable[[Recorder], None]]] = [
        ("Retry correctness", scenario_retry_correctness),
        ("Queue fairness", scenario_queue_fairness),
        ("Terminal failure semantics", scenario_terminal_failure),
        ("Requeue correctness", scenario_requeue_correctness),
        ("Worker liveness", scenario_worker_liveness),
    ]

    for name, callback in scenarios:
        run_scenario(name, callback, recorder)

    duration = datetime.now(timezone.utc) - started_at
    print("EMIS control-plane validation")
    print(f"started_at={started_at.isoformat()} duration_seconds={duration.total_seconds():.2f}")
    print("")
    for result in recorder.results:
        print(f"[{result.status}] {result.name}: {result.detail}")

    print("")
    print(
        "summary",
        json.dumps(
            {
                "pass": recorder.count("PASS"),
                "warn": recorder.count("WARN"),
                "fail": recorder.count("FAIL"),
            },
            sort_keys=True,
        ),
    )
    return 1 if recorder.count("FAIL") > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
