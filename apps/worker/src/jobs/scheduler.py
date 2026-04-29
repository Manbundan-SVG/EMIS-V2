from __future__ import annotations

import logging
import os
import time

from src.db.client import get_connection
from src.db.repositories import enqueue_governed_recompute

logger = logging.getLogger(__name__)


def run_forever(workspace_slug: str, watchlist_slug: str | None = None,
                interval_seconds: int = 300) -> None:
    logger.info(
        "scheduler started workspace=%s watchlist=%s interval=%ss",
        workspace_slug, watchlist_slug, interval_seconds,
    )
    while True:
        try:
            with get_connection() as conn:
                result = enqueue_governed_recompute(
                    conn,
                    workspace_slug=workspace_slug,
                    watchlist_slug=watchlist_slug,
                    trigger_type="cron",
                    requested_by="scheduler",
                    payload={"source": "scheduled", "watchlist_slug": watchlist_slug},
                )
                conn.commit()
            logger.info(
                "scheduler decision allowed=%s reason=%s job_id=%s priority=%s",
                result.get("allowed"),
                result.get("reason"),
                result.get("job_id"),
                result.get("assigned_priority"),
            )
        except Exception:
            logger.exception("scheduler enqueue failed")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run_forever(
        workspace_slug=os.environ.get("DEFAULT_WORKSPACE_SLUG", "demo"),
        watchlist_slug=os.environ.get("DEFAULT_WATCHLIST_SLUG"),
        interval_seconds=int(os.environ.get("SCHEDULER_INTERVAL_SECONDS", "300")),
    )
