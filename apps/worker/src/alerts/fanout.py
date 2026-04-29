from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


def _send_webhook(body: dict[str, Any]) -> None:
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        return
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5):
        pass


def emit_alert(job_id: str, workspace_id: str, title: str, message: str,
               severity: str = "info", payload: dict[str, Any] | None = None) -> None:
    """Fire-and-forget: write alert_event row + optional webhook. Never raises."""
    from src.db.client import get_connection
    from src.db.repositories import create_alert_event
    try:
        with get_connection() as conn:
            create_alert_event(
                conn,
                workspace_id,
                title,
                message,
                severity,
                payload,
                job_id=job_id,
                alert_type="worker_notice",
            )
            conn.commit()
    except Exception:
        logger.exception("failed to write alert_event job_id=%s", job_id)

    try:
        _send_webhook({"job_id": job_id, "workspace_id": workspace_id,
                       "severity": severity, "title": title,
                       "message": message, "payload": payload or {}})
    except Exception:
        logger.exception("webhook delivery failed job_id=%s", job_id)
