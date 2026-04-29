from __future__ import annotations

from dataclasses import dataclass
from statistics import mean
from typing import Any


def _bounded(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def _relative_delta(current: float | None, baseline: float | None, floor: float = 0.05) -> float | None:
    if current is None or baseline is None:
        return None
    denominator = max(abs(baseline), floor)
    return abs(current - baseline) / denominator


def _classification(scores: list[float]) -> str:
    worst = max(scores) if scores else 0.0
    if worst >= 0.75:
        return "critical"
    if worst >= 0.50:
        return "unstable"
    if worst >= 0.25:
        return "watch"
    return "stable"


@dataclass(frozen=True)
class StabilityInputs:
    run_id: str
    workspace_id: str
    watchlist_id: str | None
    queue_name: str
    composite_current: float | None
    composite_history: list[float]
    family_current: dict[str, float]
    family_history: dict[str, list[float]]
    replay_runs_considered: int
    replay_mismatch_rate: float | None
    replay_avg_input_match_score: float | None
    replay_avg_composite_delta_abs: float | None
    regime_transitions_considered: int
    regime_conflicting_transition_count: int
    regime_abrupt_transition_count: int
    regime_changed: bool
    dominant_regime: str | None


def build_stability_payload(inputs: StabilityInputs, *, window_size: int = 7) -> dict[str, Any]:
    composite_window = inputs.composite_history[:window_size]
    composite_baseline = mean(composite_window) if composite_window else None
    composite_delta_abs = None
    if inputs.composite_current is not None and composite_baseline is not None:
        composite_delta_abs = abs(inputs.composite_current - composite_baseline)
    composite_delta_pct = _relative_delta(inputs.composite_current, composite_baseline, floor=0.10)
    composite_instability_score = _bounded(float(composite_delta_pct or 0.0))

    family_rows: list[dict[str, Any]] = []
    weighted_family_scores: list[tuple[float, float]] = []
    baseline_family_scores: dict[str, float] = {}
    dominant_family = None
    dominant_family_value = None

    for family_name, current_score in sorted(inputs.family_current.items()):
        history = inputs.family_history.get(family_name, [])[:window_size]
        baseline_score = mean(history) if history else None
        if baseline_score is not None:
          baseline_family_scores[family_name] = baseline_score
        delta_abs = abs(current_score - baseline_score) if baseline_score is not None else None
        delta_pct = _relative_delta(current_score, baseline_score, floor=0.05)
        instability_score = _bounded(float(delta_pct or 0.0))
        weight = max(abs(current_score), abs(baseline_score or 0.0), 0.05)
        weighted_family_scores.append((instability_score, weight))
        family_rows.append(
            {
                "signal_family": family_name,
                "family_score_current": current_score,
                "family_score_baseline": baseline_score,
                "family_delta_abs": delta_abs,
                "family_delta_pct": delta_pct,
                "instability_score": instability_score,
                "metadata": {
                    "history_points": len(history),
                },
            }
        )
        if dominant_family_value is None or abs(current_score) > abs(dominant_family_value):
            dominant_family = family_name
            dominant_family_value = current_score

    family_rows.sort(key=lambda row: abs(row["family_score_current"] or 0.0), reverse=True)
    for index, row in enumerate(family_rows, start=1):
        row["family_rank"] = index

    family_instability_score = 0.0
    if weighted_family_scores:
        family_instability_score = sum(score * weight for score, weight in weighted_family_scores) / sum(weight for _, weight in weighted_family_scores)

    baseline_dominant_family = None
    if baseline_family_scores:
        baseline_dominant_family = max(
            baseline_family_scores.items(),
            key=lambda item: abs(item[1]),
        )[0]

    replay_mismatch_rate = float(inputs.replay_mismatch_rate or 0.0)
    replay_input_gap = 0.0
    if inputs.replay_avg_input_match_score is not None:
        replay_input_gap = max(0.0, 1.0 - float(inputs.replay_avg_input_match_score))
    replay_composite_gap = _bounded(float((inputs.replay_avg_composite_delta_abs or 0.0) / 0.10))
    replay_risk = 0.0
    if inputs.replay_runs_considered > 0:
        replay_risk = _bounded(
            (0.55 * replay_mismatch_rate)
            + (0.30 * replay_input_gap)
            + (0.15 * replay_composite_gap)
        )

    conflict_rate = (
        inputs.regime_conflicting_transition_count / inputs.regime_transitions_considered
        if inputs.regime_transitions_considered > 0
        else 0.0
    )
    abrupt_rate = (
        inputs.regime_abrupt_transition_count / inputs.regime_transitions_considered
        if inputs.regime_transitions_considered > 0
        else 0.0
    )
    regime_instability_score = _bounded(
        (0.60 * conflict_rate)
        + (0.30 * abrupt_rate)
        + (0.10 if inputs.regime_changed else 0.0)
    )

    stability_classification = _classification(
        [
            composite_instability_score,
            family_instability_score,
            replay_risk,
            regime_instability_score,
        ]
    )

    return {
        "baseline": {
            "run_id": inputs.run_id,
            "workspace_id": inputs.workspace_id,
            "watchlist_id": inputs.watchlist_id,
            "queue_name": inputs.queue_name,
            "window_size": window_size,
            "baseline_run_count": len(composite_window),
            "composite_baseline": composite_baseline,
            "composite_current": inputs.composite_current,
            "composite_delta_abs": composite_delta_abs,
            "composite_delta_pct": composite_delta_pct,
            "composite_instability_score": composite_instability_score,
            "family_instability_score": family_instability_score,
            "replay_consistency_risk_score": replay_risk,
            "regime_instability_score": regime_instability_score,
            "dominant_family": dominant_family,
            "dominant_family_changed": (
                baseline_dominant_family is not None and baseline_dominant_family != dominant_family
            ),
            "dominant_regime": inputs.dominant_regime,
            "regime_changed": inputs.regime_changed,
            "stability_classification": stability_classification,
            "metadata": {
                "baseline_dominant_family": baseline_dominant_family,
                "family_count": len(family_rows),
                "composite_history_points": len(composite_window),
                "replay_runs_considered": inputs.replay_runs_considered,
                "regime_transitions_considered": inputs.regime_transitions_considered,
            },
        },
        "family_rows": family_rows,
        "replay_metrics": {
            "run_id": inputs.run_id,
            "workspace_id": inputs.workspace_id,
            "watchlist_id": inputs.watchlist_id,
            "queue_name": inputs.queue_name,
            "replay_runs_considered": inputs.replay_runs_considered,
            "mismatch_rate": inputs.replay_mismatch_rate,
            "avg_input_match_score": inputs.replay_avg_input_match_score,
            "avg_composite_delta_abs": inputs.replay_avg_composite_delta_abs,
            "instability_score": replay_risk,
            "metadata": {
                "input_gap": replay_input_gap,
                "composite_gap_component": replay_composite_gap,
            },
        },
        "regime_metrics": {
            "run_id": inputs.run_id,
            "workspace_id": inputs.workspace_id,
            "watchlist_id": inputs.watchlist_id,
            "queue_name": inputs.queue_name,
            "transitions_considered": inputs.regime_transitions_considered,
            "conflicting_transition_count": inputs.regime_conflicting_transition_count,
            "abrupt_transition_count": inputs.regime_abrupt_transition_count,
            "instability_score": regime_instability_score,
            "metadata": {
                "conflict_rate": conflict_rate,
                "abrupt_rate": abrupt_rate,
                "regime_changed": inputs.regime_changed,
            },
        },
    }
