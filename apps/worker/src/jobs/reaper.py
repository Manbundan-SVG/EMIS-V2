from __future__ import annotations

import logging
import time

from src.db.client import get_connection
from src.db.repositories import reap_stale_jobs

logger = logging.getLogger(__name__)


def run_forever(stale_minutes: int = 10, poll_seconds: int = 15) -> None:
    logger.info("reaper started stale_minutes=%s poll_seconds=%s", stale_minutes, poll_seconds)
    while True:
        try:
            with get_connection() as conn:
                recovered = reap_stale_jobs(conn, stale_minutes=stale_minutes)
                conn.commit()
            if recovered > 0:
                logger.info("reaper recovered %s stale job(s)", recovered)
        except Exception:
            logger.exception("reaper cycle failed")
        time.sleep(poll_seconds)


if __name__ == "__main__":
    import os
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run_forever(
        stale_minutes=int(os.environ.get("REAPER_STALE_MINUTES", "10")),
        poll_seconds=int(os.environ.get("REAPER_POLL_SECONDS", "15")),
    )
