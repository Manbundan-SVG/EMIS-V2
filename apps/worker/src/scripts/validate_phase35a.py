from __future__ import annotations

import sys
from datetime import datetime, timezone

from src.db import repositories as repo
from src.db.client import get_connection
from src.services.routing_optimization_service import RoutingOptimizationService

EXPECTED_SNAPSHOT_COLS = {
    "id", "workspace_id", "snapshot_at", "window_label",
    "recommendation_count", "metadata", "created_at",
}

EXPECTED_RECOMMENDATION_COLS = {
    "id", "workspace_id", "recommendation_key", "reason_code",
    "scope_type", "scope_value", "recommended_policy", "confidence",
    "expected_benefit_score", "risk_score", "sample_size",
    "signal_payload", "snapshot_id", "created_at", "updated_at",
}

EXPECTED_FEATURE_EFFECTIVENESS_COLS = {
    "workspace_id", "feature_type", "feature_key", "case_count",
    "accepted_recommendation_count", "override_count", "reassignment_count",
    "reopen_count", "escalation_count", "avg_ack_latency_seconds",
    "avg_resolve_latency_seconds", "effectiveness_score",
    "workload_penalty_score", "net_fit_score",
}

EXPECTED_CONTEXT_FIT_COLS = {
    "workspace_id", "context_key", "recommended_user", "recommended_team",
    "operator_fit_score", "team_fit_score", "confidence", "sample_size",
}

EXPECTED_OPPORTUNITY_COLS = {
    "id", "workspace_id", "recommendation_key", "reason_code",
    "scope_type", "scope_value", "recommended_policy", "confidence",
    "expected_benefit_score", "risk_score", "sample_size",
    "signal_payload", "snapshot_id", "created_at", "updated_at",
}


def _create_workspace(slug: str) -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "insert into public.workspaces (slug, name) values (%s, %s) returning id",
                (slug, slug),
            )
            workspace_id = str(cur.fetchone()["id"])
        conn.commit()
    return workspace_id


def _cleanup_workspace(workspace_id: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("delete from public.workspaces where id = %s::uuid", (workspace_id,))
        conn.commit()


def _check_schema(conn, table: str, expected: set[str]) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = 'public'
              and table_name = %s
            """,
            (table,),
        )
        actual = {row["column_name"] for row in cur.fetchall()}
    missing = sorted(expected - actual)
    return missing


def _check_view(conn, view: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"select * from public.{view} limit 0")  # noqa: S608


def main() -> None:
    now = datetime.now(timezone.utc)
    workspace_slug = f"phase35a-{now.strftime('%Y%m%d%H%M%S')}"
    workspace_id = _create_workspace(workspace_slug)
    errors: list[str] = []

    try:
        # ── 1. Run service on empty workspace ─────────────────────────────
        with get_connection() as conn:
            service = RoutingOptimizationService(repo)
            result = service.refresh_workspace_snapshot(conn, workspace_id=workspace_id)
            conn.commit()

        print(f"workspace_id={result.workspace_id}")
        print(f"snapshot_id={result.snapshot_id}")
        print(f"recommendation_count={result.recommendation_count}")
        print(f"window_label={result.window_label}")

        # ── 2. Verify snapshot row was written ────────────────────────────
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select count(*) as n from public.governance_routing_optimization_snapshots where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                snap_count = cur.fetchone()["n"]

        if snap_count == 0:
            errors.append("FAIL: no snapshot row written to governance_routing_optimization_snapshots")
        else:
            print(f"snapshot_rows={snap_count}")

        # ── 3. Schema contract — tables ───────────────────────────────────
        with get_connection() as conn:
            for table, expected in [
                ("governance_routing_optimization_snapshots", EXPECTED_SNAPSHOT_COLS),
                ("governance_routing_policy_recommendations", EXPECTED_RECOMMENDATION_COLS),
            ]:
                missing = _check_schema(conn, table, expected)
                if missing:
                    errors.append(f"FAIL: {table} missing columns: {missing}")
                else:
                    print(f"schema_ok={table}")

        # ── 4. View accessibility ─────────────────────────────────────────
        with get_connection() as conn:
            for view in [
                "governance_routing_feature_effectiveness_summary",
                "governance_routing_context_fit_summary",
                "governance_routing_policy_opportunity_summary",
            ]:
                try:
                    _check_view(conn, view)
                    print(f"view_accessible={view}")
                except Exception as exc:
                    errors.append(f"FAIL: view {view} not accessible: {exc}")

        # ── 5. View column contracts ──────────────────────────────────────
        with get_connection() as conn:
            for view, expected in [
                ("governance_routing_feature_effectiveness_summary", EXPECTED_FEATURE_EFFECTIVENESS_COLS),
                ("governance_routing_context_fit_summary", EXPECTED_CONTEXT_FIT_COLS),
                ("governance_routing_policy_opportunity_summary", EXPECTED_OPPORTUNITY_COLS),
            ]:
                missing = _check_schema(conn, view, expected)
                if missing:
                    errors.append(f"FAIL: view {view} missing columns: {missing}")
                else:
                    print(f"view_schema_ok={view}")

        # ── 6. Idempotency — run twice, snapshot count should be 2 ────────
        with get_connection() as conn:
            service2 = RoutingOptimizationService(repo)
            result2 = service2.refresh_workspace_snapshot(conn, workspace_id=workspace_id)
            conn.commit()

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select count(*) as n from public.governance_routing_optimization_snapshots where workspace_id = %s::uuid",
                    (workspace_id,),
                )
                snap_count2 = cur.fetchone()["n"]

        if snap_count2 < 2:
            errors.append(f"FAIL: expected >=2 snapshots after second run, got {snap_count2}")
        else:
            print(f"idempotency_ok=true (snapshot_rows={snap_count2}, second_snapshot={result2.snapshot_id})")

    finally:
        _cleanup_workspace(workspace_id)

    if errors:
        print("\n--- VALIDATION ERRORS ---")
        for e in errors:
            print(e)
        sys.exit(1)

    print("\nphase35a validation passed")


if __name__ == "__main__":
    main()
