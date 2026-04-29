from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

_WINDOW_LABEL = "30d"
_MIN_SAMPLE = 5
_BENEFIT_THRESHOLD = 0.10


@dataclass(frozen=True)
class RoutingOptimizationResult:
    workspace_id: str
    snapshot_id: str
    recommendation_count: int
    window_label: str


class RoutingOptimizationService:
    """Derive advisory routing optimization signals from live governance metrics.

    This service is analytics-only. It does NOT modify routing rules or policies.
    All outputs are durable recommendation artifacts for human review.
    """

    def __init__(self, repo_module: Any) -> None:
        self.repo = repo_module

    # ── Public entry point ──────────────────────────────────────────────────

    def refresh_workspace_snapshot(
        self,
        conn: Any,
        *,
        workspace_id: str,
    ) -> RoutingOptimizationResult:
        quality_rows = self.repo.list_governance_routing_quality_summary(conn, workspace_id=workspace_id)
        rec_input_rows = self.repo.list_governance_routing_recommendation_inputs(conn, workspace_id=workspace_id)
        operator_rows = self.repo.list_governance_operator_effectiveness_summary(conn, workspace_id=workspace_id)
        team_rows = self.repo.list_governance_team_effectiveness_summary(conn, workspace_id=workspace_id)
        operator_pressure = self.repo.list_operator_workload_pressure(conn, workspace_id=workspace_id)
        team_pressure = self.repo.list_team_workload_pressure(conn, workspace_id=workspace_id)
        autopromotion_rows = self.repo.list_governance_routing_autopromotion_summary(conn, workspace_id=workspace_id)

        opportunities = self.build_policy_opportunities(
            quality_rows=quality_rows,
            rec_input_rows=rec_input_rows,
            operator_rows=operator_rows,
            team_rows=team_rows,
            operator_pressure=operator_pressure,
            team_pressure=team_pressure,
            autopromotion_rows=autopromotion_rows,
        )

        upserted = 0
        for opp in opportunities:
            self.repo.upsert_routing_policy_recommendation(conn, workspace_id=workspace_id, rec=opp)
            upserted += 1

        snapshot = self.repo.insert_routing_optimization_snapshot(
            conn,
            workspace_id=workspace_id,
            window_label=_WINDOW_LABEL,
            recommendation_count=upserted,
            metadata={"source": "phase3_5A", "refresh_mode": "worker"},
        )

        return RoutingOptimizationResult(
            workspace_id=workspace_id,
            snapshot_id=str(snapshot["id"]),
            recommendation_count=upserted,
            window_label=_WINDOW_LABEL,
        )

    # ── Core generators ─────────────────────────────────────────────────────

    def build_policy_opportunities(
        self,
        *,
        quality_rows: list[dict[str, Any]],
        rec_input_rows: list[dict[str, Any]],
        operator_rows: list[dict[str, Any]],
        team_rows: list[dict[str, Any]],
        operator_pressure: list[dict[str, Any]],
        team_pressure: list[dict[str, Any]],
        autopromotion_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        opportunities: list[dict[str, Any]] = []

        active_scopes = self._active_autopromotion_scopes(autopromotion_rows)
        pressure_by_operator = {r["assigned_to"]: r for r in operator_pressure if r.get("assigned_to")}
        pressure_by_team = {r["assigned_team"]: r for r in team_pressure if r.get("assigned_team")}

        opportunities.extend(
            self._gen_prefer_operator(rec_input_rows, operator_rows, pressure_by_operator, active_scopes)
        )
        opportunities.extend(
            self._gen_prefer_team(quality_rows, team_rows, pressure_by_team, active_scopes)
        )
        opportunities.extend(
            self._gen_avoid_operator_under_load(operator_rows, pressure_by_operator)
        )
        opportunities.extend(
            self._gen_split_by_root_cause(quality_rows)
        )
        opportunities.extend(
            self._gen_prefer_team_for_reopen(rec_input_rows, team_rows)
        )

        opportunities.sort(key=lambda x: (-x["expected_benefit_score"], x["risk_score"]))
        return opportunities

    # ── Individual generators ────────────────────────────────────────────────

    def _gen_prefer_operator(
        self,
        rec_input_rows: list[dict[str, Any]],
        operator_rows: list[dict[str, Any]],
        pressure_by_operator: dict[str, Any],
        active_scopes: set[str],
    ) -> list[dict[str, Any]]:
        """Surface an operator that consistently outperforms for a specific context."""
        results: list[dict[str, Any]] = []

        by_context: dict[str, list[dict[str, Any]]] = {}
        for row in rec_input_rows:
            rc = row.get("root_cause_code") or "unknown"
            sev = row.get("severity") or "unknown"
            ctx = f"{rc}|{sev}"
            by_context.setdefault(ctx, []).append(row)

        global_operator_fit = {
            r["assigned_to"]: self._operator_fit_score(r)
            for r in operator_rows if r.get("assigned_to")
        }

        for ctx, rows in by_context.items():
            if len(rows) < 2:
                continue

            best = max(rows, key=lambda r: self._context_fit_score(r))
            total = self._total_outcomes(best)
            if total < _MIN_SAMPLE:
                continue

            fit = self._context_fit_score(best)
            if fit < _BENEFIT_THRESHOLD:
                continue

            target = best.get("routing_target", "")
            if not target:
                continue

            pressure = pressure_by_operator.get(target, {})
            scope_key = f"root_cause={ctx}"
            risk = self.score_risk(
                sample_size=total,
                override_rate=None,
                workload_high=bool(pressure.get("severity_weighted_load", 0) or 0 > 5),
                chronicity_high=False,
                autopromotion_active=scope_key in active_scopes,
            )

            rec_key = f"prefer_operator:{_stable_key(target, ctx)}"
            results.append({
                "recommendation_key": rec_key,
                "scope_type": "context",
                "scope_value": ctx,
                "current_policy": {},
                "recommended_policy": {"preferred_operator": target},
                "reason_code": "prefer_operator",
                "confidence": _confidence(total),
                "sample_size": total,
                "expected_benefit_score": round(self.score_expected_benefit(fit, total), 4),
                "risk_score": round(risk, 4),
                "supporting_metrics": {
                    "fit_score": fit,
                    "global_operator_fit": global_operator_fit.get(target),
                    "workload_severity": pressure.get("severity_weighted_load"),
                },
            })

        return results

    def _gen_prefer_team(
        self,
        quality_rows: list[dict[str, Any]],
        team_rows: list[dict[str, Any]],
        pressure_by_team: dict[str, Any],
        active_scopes: set[str],
    ) -> list[dict[str, Any]]:
        """Surface a team with high acceptance rate for a root-cause context."""
        results: list[dict[str, Any]] = []

        global_team_fit = {
            r["assigned_team"]: self._team_fit_score(r)
            for r in team_rows if r.get("assigned_team")
        }

        by_rc: dict[str, list[dict[str, Any]]] = {}
        for row in quality_rows:
            rc = row.get("root_cause_code") or "unknown"
            by_rc.setdefault(rc, []).append(row)

        for rc, rows in by_rc.items():
            if not rows:
                continue
            best = max(rows, key=lambda r: float(r.get("acceptance_rate") or 0))
            acc_rate = float(best.get("acceptance_rate") or 0)
            feedback_count = int(best.get("feedback_count") or 0)

            if feedback_count < _MIN_SAMPLE or acc_rate < 0.6:
                continue

            team = best.get("assigned_team", "")
            if not team:
                continue

            fit = global_team_fit.get(team, 0.0)
            pressure = pressure_by_team.get(team, {})
            scope_key = f"root_cause={rc}"
            risk = self.score_risk(
                sample_size=feedback_count,
                override_rate=float(best.get("rerouted_count") or 0) / max(feedback_count, 1),
                workload_high=bool(pressure.get("severity_weighted_load", 0) or 0 > 5),
                chronicity_high=False,
                autopromotion_active=scope_key in active_scopes,
            )

            rec_key = f"prefer_team:{_stable_key(team, rc)}"
            results.append({
                "recommendation_key": rec_key,
                "scope_type": "root_cause_code",
                "scope_value": rc,
                "current_policy": {},
                "recommended_policy": {"preferred_team": team},
                "reason_code": "prefer_team",
                "confidence": _confidence(feedback_count),
                "sample_size": feedback_count,
                "expected_benefit_score": round(self.score_expected_benefit(acc_rate - 0.5, feedback_count), 4),
                "risk_score": round(risk, 4),
                "supporting_metrics": {
                    "acceptance_rate": acc_rate,
                    "rerouted_count": best.get("rerouted_count"),
                    "team_global_fit": fit,
                    "workload_severity": pressure.get("severity_weighted_load"),
                },
            })

        return results

    def _gen_avoid_operator_under_load(
        self,
        operator_rows: list[dict[str, Any]],
        pressure_by_operator: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Flag operators with high workload and poor outcomes as routing risks."""
        results: list[dict[str, Any]] = []

        for row in operator_rows:
            op = row.get("assigned_to")
            if not op:
                continue

            pressure = pressure_by_operator.get(op, {})
            load = float(pressure.get("severity_weighted_load") or 0)
            if load < 3.0:
                continue

            fit = self._team_fit_score(row)
            if fit >= 0.0:
                continue

            assignments = int(row.get("assignments") or 0)
            if assignments < _MIN_SAMPLE:
                continue

            risk = self.score_risk(
                sample_size=assignments,
                override_rate=None,
                workload_high=True,
                chronicity_high=False,
                autopromotion_active=False,
            )

            rec_key = f"avoid_operator_under_load:{_stable_key(op)}"
            results.append({
                "recommendation_key": rec_key,
                "scope_type": "operator",
                "scope_value": op,
                "current_policy": {},
                "recommended_policy": {"deprioritize_when_load_above": 3.0},
                "reason_code": "avoid_operator_under_load",
                "confidence": _confidence(assignments),
                "sample_size": assignments,
                "expected_benefit_score": round(self.score_expected_benefit(-fit, assignments), 4),
                "risk_score": round(risk, 4),
                "supporting_metrics": {
                    "severity_weighted_load": load,
                    "operator_fit_score": fit,
                    "open_case_count": pressure.get("open_case_count"),
                },
            })

        return results

    def _gen_split_by_root_cause(
        self,
        quality_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Surface root causes where acceptance rate is low, suggesting routing splits."""
        results: list[dict[str, Any]] = []

        by_rc: dict[str, list[dict[str, Any]]] = {}
        for row in quality_rows:
            rc = row.get("root_cause_code") or "unknown"
            by_rc.setdefault(rc, []).append(row)

        for rc, rows in by_rc.items():
            total_feedback = sum(int(r.get("feedback_count") or 0) for r in rows)
            if total_feedback < _MIN_SAMPLE:
                continue

            avg_acc = sum(float(r.get("acceptance_rate") or 0) for r in rows) / len(rows)
            if avg_acc >= 0.5:
                continue

            risk = self.score_risk(
                sample_size=total_feedback,
                override_rate=1.0 - avg_acc,
                workload_high=False,
                chronicity_high=False,
                autopromotion_active=False,
            )

            rec_key = f"split_routing_by_root_cause:{_stable_key(rc)}"
            results.append({
                "recommendation_key": rec_key,
                "scope_type": "root_cause_code",
                "scope_value": rc,
                "current_policy": {"routing_mode": "undifferentiated"},
                "recommended_policy": {"routing_mode": "split_by_context"},
                "reason_code": "split_routing_by_root_cause",
                "confidence": _confidence(total_feedback),
                "sample_size": total_feedback,
                "expected_benefit_score": round(self.score_expected_benefit(0.5 - avg_acc, total_feedback), 4),
                "risk_score": round(risk, 4),
                "supporting_metrics": {
                    "avg_acceptance_rate": round(avg_acc, 4),
                    "team_count": len(rows),
                    "total_feedback": total_feedback,
                },
            })

        return results

    def _gen_prefer_team_for_reopen(
        self,
        rec_input_rows: list[dict[str, Any]],
        team_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Identify teams with better reopen outcomes for recurring case contexts."""
        results: list[dict[str, Any]] = []

        team_reopen_rates = {
            r["assigned_team"]: (
                int(r.get("reopens") or 0) / max(int(r.get("resolutions") or 1), 1)
            )
            for r in team_rows if r.get("assigned_team")
        }

        if not team_reopen_rates:
            return results

        best_team = min(team_reopen_rates, key=lambda t: team_reopen_rates[t])
        best_rate = team_reopen_rates[best_team]
        avg_rate = sum(team_reopen_rates.values()) / len(team_reopen_rates)

        if avg_rate - best_rate < 0.10:
            return results

        total_reopened = sum(
            int(r.get("reopened_count") or 0) for r in rec_input_rows
        )
        if total_reopened < _MIN_SAMPLE:
            return results

        risk = self.score_risk(
            sample_size=total_reopened,
            override_rate=None,
            workload_high=False,
            chronicity_high=True,
            autopromotion_active=False,
        )

        rec_key = f"prefer_team_for_reopen_cases:{_stable_key(best_team)}"
        results.append({
            "recommendation_key": rec_key,
            "scope_type": "case_type",
            "scope_value": "reopened",
            "current_policy": {},
            "recommended_policy": {"preferred_team_for_reopens": best_team},
            "reason_code": "prefer_team_for_reopen_cases",
            "confidence": _confidence(total_reopened),
            "sample_size": total_reopened,
            "expected_benefit_score": round(self.score_expected_benefit(avg_rate - best_rate, total_reopened), 4),
            "risk_score": round(risk, 4),
            "supporting_metrics": {
                "best_team_reopen_rate": round(best_rate, 4),
                "avg_team_reopen_rate": round(avg_rate, 4),
                "total_reopened_cases": total_reopened,
            },
        })

        return results

    # ── Scoring ──────────────────────────────────────────────────────────────

    def score_expected_benefit(self, signal_strength: float, sample_size: int) -> float:
        """0.0–1.0. Blend of signal strength and sample credibility."""
        cred = min(1.0, sample_size / 30.0)
        raw = max(0.0, min(1.0, signal_strength))
        return raw * 0.70 + cred * 0.30

    def score_risk(
        self,
        *,
        sample_size: int,
        override_rate: float | None,
        workload_high: bool,
        chronicity_high: bool,
        autopromotion_active: bool,
    ) -> float:
        """0.0–1.0 risk score. Higher = riskier to act on this recommendation."""
        risk = 0.0
        # Small sample penalty
        if sample_size < _MIN_SAMPLE:
            risk += 0.30
        elif sample_size < 10:
            risk += 0.15
        # Override rate penalty
        if override_rate is not None:
            risk += min(0.25, override_rate * 0.25)
        # Workload pressure
        if workload_high:
            risk += 0.20
        # Chronicity increases uncertainty
        if chronicity_high:
            risk += 0.10
        # Active autopromotion may conflict
        if autopromotion_active:
            risk += 0.15
        return min(1.0, risk)

    # ── Private helpers ──────────────────────────────────────────────────────

    def _context_fit_score(self, row: dict[str, Any]) -> float:
        total = self._total_outcomes(row)
        if total == 0:
            return 0.0
        resolved = int(row.get("resolved_count") or 0)
        reassigned = int(row.get("reassigned_count") or 0)
        reopened = int(row.get("reopened_count") or 0)
        escalated = int(row.get("escalated_count") or 0)
        score = (
            resolved / total * 0.40
            - reassigned / total * 0.25
            - reopened / total * 0.25
            - escalated / total * 0.10
        )
        avg_resolve = float(row.get("avg_resolve_hours") or 0)
        if avg_resolve > 0:
            score += max(0.0, (72.0 - avg_resolve) / 72.0 * 0.10)
        return round(max(-1.0, min(1.0, score)), 4)

    def _operator_fit_score(self, row: dict[str, Any]) -> float:
        return self._effectiveness_fit(row)

    def _team_fit_score(self, row: dict[str, Any]) -> float:
        return self._effectiveness_fit(row)

    def _effectiveness_fit(self, row: dict[str, Any]) -> float:
        assignments = int(row.get("assignments") or 0)
        if assignments == 0:
            return 0.0
        resolutions = int(row.get("resolutions") or 0)
        reassignments = int(row.get("reassignments") or 0)
        reopens = int(row.get("reopens") or 0)
        escalations = int(row.get("escalations") or 0)
        score = (
            resolutions / assignments * 0.40
            - reassignments / assignments * 0.25
            - reopens / assignments * 0.25
            - escalations / assignments * 0.10
        )
        avg_resolve = float(row.get("avg_resolve_hours") or 0)
        if avg_resolve > 0:
            score += max(0.0, (72.0 - avg_resolve) / 72.0 * 0.10)
        return round(max(-1.0, min(1.0, score)), 4)

    def _total_outcomes(self, row: dict[str, Any]) -> int:
        return (
            int(row.get("resolved_count") or 0)
            + int(row.get("reassigned_count") or 0)
            + int(row.get("escalated_count") or 0)
            + int(row.get("reopened_count") or 0)
        )

    def _active_autopromotion_scopes(self, rows: list[dict[str, Any]]) -> set[str]:
        scopes: set[str] = set()
        for r in rows:
            sc = r.get("scope_type")
            sv = r.get("scope_value")
            if sc and sv and r.get("execution_status") == "executed":
                scopes.add(f"{sc}={sv}")
        return scopes


# ── Module-level helpers ─────────────────────────────────────────────────────

def _confidence(sample_size: int) -> str:
    if sample_size >= 20:
        return "high"
    if sample_size >= _MIN_SAMPLE:
        return "medium"
    return "low"


def _stable_key(*parts: str) -> str:
    joined = "|".join(str(p) for p in parts)
    return hashlib.md5(joined.encode(), usedforsecurity=False).hexdigest()[:12]
