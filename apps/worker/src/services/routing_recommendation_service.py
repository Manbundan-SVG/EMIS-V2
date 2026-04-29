from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class RoutingRecommendation:
    recommendation_key: str
    recommended_user: str | None
    recommended_team: str | None
    fallback_user: str | None
    fallback_team: str | None
    reason_code: str
    confidence: str
    score: float
    supporting_metrics: dict[str, Any]
    model_inputs: dict[str, Any]
    alternatives: list[dict[str, Any]]


class RoutingRecommendationService:
    """Advisory-first routing recommendation engine."""

    def build_recommendation_key(self, case_row: dict[str, Any]) -> str:
        parts = [
            str(case_row.get("workspace_id", "")),
            str(case_row.get("id", "")),
            str(case_row.get("root_cause_code", "")),
            str(case_row.get("watchlist_id", "")),
            str(case_row.get("version_tuple", "")),
            str(case_row.get("regime", "")),
        ]
        return "|".join(parts)

    def _score_candidate(self, candidate: dict[str, Any]) -> float:
        operator_effectiveness = float(candidate.get("operator_effectiveness_score", 0) or 0)
        team_effectiveness = float(candidate.get("team_effectiveness_score", 0) or 0)
        workload_inverse = float(candidate.get("workload_inverse", 0) or 0)
        root_cause_fit = float(candidate.get("root_cause_fit", 0) or 0)
        recurrence_fit = float(candidate.get("recurrence_fit", 0) or 0)
        version_fit = float(candidate.get("version_or_regime_fit", 0) or 0)
        return round(
            0.40 * operator_effectiveness
            + 0.20 * team_effectiveness
            + 0.15 * root_cause_fit
            + 0.10 * recurrence_fit
            + 0.10 * workload_inverse
            + 0.05 * version_fit,
            6,
        )

    def _confidence(self, scored: list[dict[str, Any]]) -> str:
        if not scored:
            return "low"
        if len(scored) == 1:
            return "medium"
        delta = float(scored[0]["score"]) - float(scored[1]["score"])
        if delta >= 0.20:
            return "high"
        if delta >= 0.08:
            return "medium"
        return "low"

    def recommend(
        self,
        *,
        case_row: dict[str, Any],
        candidate_rows: list[dict[str, Any]],
    ) -> RoutingRecommendation:
        recommendation_key = self.build_recommendation_key(case_row)
        scored = [{**candidate, "score": self._score_candidate(candidate)} for candidate in candidate_rows]
        scored.sort(key=lambda row: float(row.get("score", 0) or 0), reverse=True)

        top = scored[0] if scored else {}
        fallback = scored[1] if len(scored) > 1 else {}
        confidence = self._confidence(scored)

        reason_code = "best_effectiveness_with_workload_balance"
        if case_row.get("root_cause_code") and float(top.get("root_cause_fit", 0) or 0) >= 0.8:
            reason_code = "root_cause_specialist_recommendation"
        elif int(case_row.get("repeat_count") or 1) > 1 and float(top.get("recurrence_fit", 0) or 0) >= 0.7:
            reason_code = "recurring_case_specialist_recommendation"

        supporting_metrics = {
            "candidate_count": len(candidate_rows),
            "top_score": top.get("score"),
            "second_score": fallback.get("score"),
            "top_operator_effectiveness_score": top.get("operator_effectiveness_score"),
            "top_team_effectiveness_score": top.get("team_effectiveness_score"),
            "top_workload_inverse": top.get("workload_inverse"),
        }
        model_inputs = {
            "root_cause_code": case_row.get("root_cause_code"),
            "severity": case_row.get("severity"),
            "watchlist_id": case_row.get("watchlist_id"),
            "version_tuple": case_row.get("version_tuple"),
            "regime": case_row.get("regime"),
            "repeat_count": int(case_row.get("repeat_count") or 1),
            "is_chronic": bool(case_row.get("is_chronic") or int(case_row.get("repeat_count") or 1) > 1),
        }
        alternatives = [
            {
                "user": row.get("assigned_user"),
                "team": row.get("assigned_team"),
                "score": row.get("score"),
                "reason": row.get("candidate_reason"),
            }
            for row in scored[:5]
        ]

        return RoutingRecommendation(
            recommendation_key=recommendation_key,
            recommended_user=top.get("assigned_user"),
            recommended_team=top.get("assigned_team"),
            fallback_user=fallback.get("assigned_user"),
            fallback_team=fallback.get("assigned_team"),
            reason_code=reason_code,
            confidence=confidence,
            score=float(top.get("score", 0) or 0),
            supporting_metrics=supporting_metrics,
            model_inputs=model_inputs,
            alternatives=alternatives,
        )
