from __future__ import annotations

from datetime import datetime, timezone

from psycopg import connect
from psycopg.rows import dict_row

from src.config import get_settings
from src.db.repositories import (
    get_governance_anomaly_clusters,
    get_watchlist_anomaly_summary,
    insert_governance_alert_events,
    upsert_governance_anomaly_clusters,
)
from src.services.anomaly_clustering_service import build_cluster_candidates


def main() -> None:
    settings = get_settings()
    ts = datetime.now(tz=timezone.utc)
    workspace_slug = f"phase27a-cluster-{int(ts.timestamp())}"

    with connect(settings.database_url, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "insert into public.workspaces (slug, name) values (%s, %s) returning id",
                (workspace_slug, "Phase 2.7A Validation"),
            )
            workspace_id = str(cur.fetchone()["id"])
            cur.execute(
                "insert into public.watchlists (workspace_id, slug, name) values (%s::uuid, %s, %s) returning id",
                (workspace_id, "core", "Core"),
            )
            watchlist_id = str(cur.fetchone()["id"])

        try:
            seed_events = [
                {
                    "workspace_id": workspace_id,
                    "watchlist_id": watchlist_id,
                    "run_id": None,
                    "rule_name": "default_family_instability_spike",
                    "event_type": "family_instability_spike",
                    "severity": "medium",
                    "dedupe_key": f"{workspace_slug}:family:1",
                    "metric_source": "latest_stability_summary",
                    "metric_name": "family_instability_score",
                    "metric_value_numeric": 0.72,
                    "metric_value_text": None,
                    "threshold_numeric": 0.50,
                    "threshold_text": None,
                    "compute_version": "compute-stable",
                    "signal_registry_version": "registry-stable",
                    "model_version": "model-stable",
                    "metadata": {
                        "message": "Family instability exceeded threshold",
                        "source_row": {"dominant_regime": "macro_dominant"},
                    },
                },
                {
                    "workspace_id": workspace_id,
                    "watchlist_id": watchlist_id,
                    "run_id": None,
                    "rule_name": "default_family_instability_spike",
                    "event_type": "family_instability_spike",
                    "severity": "medium",
                    "dedupe_key": f"{workspace_slug}:family:2",
                    "metric_source": "latest_stability_summary",
                    "metric_name": "family_instability_score",
                    "metric_value_numeric": 0.81,
                    "metric_value_text": None,
                    "threshold_numeric": 0.50,
                    "threshold_text": None,
                    "compute_version": "compute-stable",
                    "signal_registry_version": "registry-stable",
                    "model_version": "model-stable",
                    "metadata": {
                        "message": "Family instability exceeded threshold again",
                        "source_row": {"dominant_regime": "macro_dominant"},
                    },
                },
                {
                    "workspace_id": workspace_id,
                    "watchlist_id": watchlist_id,
                    "run_id": None,
                    "rule_name": "default_version_regression",
                    "event_type": "version_regression",
                    "severity": "high",
                    "dedupe_key": f"{workspace_slug}:version:1",
                    "metric_source": "version_health_rankings",
                    "metric_name": "governance_health_score",
                    "metric_value_numeric": 0.82,
                    "metric_value_text": None,
                    "threshold_numeric": 0.90,
                    "threshold_text": None,
                    "compute_version": "compute-canary",
                    "signal_registry_version": "registry-canary",
                    "model_version": "model-canary",
                    "metadata": {
                        "message": "Version tuple health regressed",
                        "source_row": {},
                    },
                },
            ]

            inserted_events = insert_governance_alert_events(conn, seed_events)
            if len(inserted_events) != 3:
                raise RuntimeError(f"expected 3 governance alert events, got {len(inserted_events)}")

            cluster_rows = upsert_governance_anomaly_clusters(
                conn,
                [candidate.__dict__ for candidate in build_cluster_candidates(inserted_events)],
            )
            if len(cluster_rows) != 3:
                raise RuntimeError("expected one returned cluster row per inserted event")

            clusters = get_governance_anomaly_clusters(conn, workspace_id, limit=10)
            summary = get_watchlist_anomaly_summary(conn, workspace_id)

            if len(clusters) != 2:
                raise RuntimeError(f"expected 2 anomaly clusters, got {len(clusters)}")

            family_cluster = next((row for row in clusters if row["alert_type"] == "family_instability_spike"), None)
            version_cluster = next((row for row in clusters if row["alert_type"] == "version_regression"), None)
            if family_cluster is None or version_cluster is None:
                raise RuntimeError("expected both family_instability_spike and version_regression clusters")
            if int(family_cluster["event_count"]) != 2:
                raise RuntimeError(f"expected family cluster event_count=2, got {family_cluster['event_count']}")
            if family_cluster["regime"] != "macro_dominant":
                raise RuntimeError(f"expected family cluster regime=macro_dominant, got {family_cluster['regime']}")
            if version_cluster["severity"] != "high":
                raise RuntimeError(f"expected version cluster severity=high, got {version_cluster['severity']}")

            if len(summary) != 1:
                raise RuntimeError(f"expected 1 anomaly summary row, got {len(summary)}")
            summary_row = summary[0]
            if int(summary_row["open_cluster_count"]) != 2:
                raise RuntimeError(f"expected open_cluster_count=2, got {summary_row['open_cluster_count']}")
            if int(summary_row["high_open_cluster_count"]) != 1:
                raise RuntimeError(f"expected high_open_cluster_count=1, got {summary_row['high_open_cluster_count']}")
            if int(summary_row["open_event_count"]) != 3:
                raise RuntimeError(f"expected open_event_count=3, got {summary_row['open_event_count']}")

            conn.rollback()
            print(
                "phase27a anomaly clustering smoke ok "
                f"workspace_slug={workspace_slug} clusters={len(clusters)} "
                f"open_cluster_count={summary_row['open_cluster_count']} "
                f"open_event_count={summary_row['open_event_count']}"
            )
        finally:
            conn.rollback()


if __name__ == "__main__":
    main()
