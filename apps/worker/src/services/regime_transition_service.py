from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FamilyShiftRow:
    signal_family: str
    prior_family_score: float
    current_family_score: float
    family_delta: float
    family_delta_abs: float
    prior_family_rank: int | None
    current_family_rank: int | None
    shift_direction: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class RegimeTransitionResult:
    run_id: str
    prior_run_id: str | None
    workspace_id: str
    watchlist_id: str | None
    queue_name: str
    from_regime: str | None
    to_regime: str | None
    transition_detected: bool
    transition_classification: str
    stability_score: float
    anomaly_likelihood: float
    composite_shift: float | None
    composite_shift_abs: float | None
    dominant_family_gained: str | None
    dominant_family_lost: str | None
    family_shifts: list[dict[str, Any]]
    metadata: dict[str, Any]


def _dominant_regime(run_context: dict[str, Any]) -> str | None:
    regime_summary = run_context.get("regime_summary") or {}
    regime_counts = regime_summary.get("regime_counts") if isinstance(regime_summary, dict) else None
    if not isinstance(regime_counts, dict) or not regime_counts:
        return None
    return str(max(regime_counts.items(), key=lambda item: item[1])[0])


def _bounded(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def _classification(
    *,
    transition_detected: bool,
    composite_shift_abs: float,
    top_family_delta_abs: float,
    has_family_rank_conflict: bool,
) -> str:
    if not transition_detected:
        return "none"
    if has_family_rank_conflict and (composite_shift_abs >= 0.15 or top_family_delta_abs >= 0.20):
        return "conflicting"
    if composite_shift_abs >= 0.25 or top_family_delta_abs >= 0.30:
        return "abrupt"
    if composite_shift_abs >= 0.10 or top_family_delta_abs >= 0.15:
        return "unstable"
    return "normal"


def analyze_regime_transition(
    *,
    current_run: dict[str, Any],
    prior_run: dict[str, Any] | None,
    current_family_rows: list[dict[str, Any]],
    prior_family_rows: list[dict[str, Any]],
) -> RegimeTransitionResult:
    run_id = str(current_run["id"])
    workspace_id = str(current_run["workspace_id"])
    watchlist_id = str(current_run["watchlist_id"]) if current_run.get("watchlist_id") else None
    queue_name = str(current_run.get("queue_name") or "recompute")
    prior_run_id = str(prior_run["id"]) if prior_run and prior_run.get("id") else None
    from_regime = _dominant_regime(prior_run or {})
    to_regime = _dominant_regime(current_run)

    current_composite = (
        float(current_run["composite_score"])
        if current_run.get("composite_score") is not None
        else None
    )
    prior_composite = (
        float(prior_run["composite_score"])
        if prior_run and prior_run.get("composite_score") is not None
        else None
    )
    composite_shift = None
    composite_shift_abs = None
    if current_composite is not None and prior_composite is not None:
        composite_shift = current_composite - prior_composite
        composite_shift_abs = abs(composite_shift)

    if current_run.get("is_replay"):
        return RegimeTransitionResult(
            run_id=run_id,
            prior_run_id=prior_run_id,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            queue_name=queue_name,
            from_regime=from_regime,
            to_regime=to_regime,
            transition_detected=False,
            transition_classification="replay_suppressed",
            stability_score=1.0,
            anomaly_likelihood=0.0,
            composite_shift=composite_shift,
            composite_shift_abs=composite_shift_abs,
            dominant_family_gained=None,
            dominant_family_lost=None,
            family_shifts=[],
            metadata={
                "suppressed_reason": "is_replay",
                "current_regime_counts": (current_run.get("regime_summary") or {}).get("regime_counts", {}),
                "prior_regime_counts": (prior_run or {}).get("regime_summary", {}).get("regime_counts", {}),
            },
        )

    if not prior_run_id:
        return RegimeTransitionResult(
            run_id=run_id,
            prior_run_id=None,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            queue_name=queue_name,
            from_regime=None,
            to_regime=to_regime,
            transition_detected=False,
            transition_classification="none",
            stability_score=1.0,
            anomaly_likelihood=0.0,
            composite_shift=None,
            composite_shift_abs=None,
            dominant_family_gained=None,
            dominant_family_lost=None,
            family_shifts=[],
            metadata={
                "suppressed_reason": "no_prior_run",
                "current_regime_counts": (current_run.get("regime_summary") or {}).get("regime_counts", {}),
            },
        )

    current_map = {str(row["signal_family"]): row for row in current_family_rows}
    prior_map = {str(row["signal_family"]): row for row in prior_family_rows}
    family_names = sorted(set(current_map) | set(prior_map))

    family_shift_rows: list[FamilyShiftRow] = []
    for family_name in family_names:
        current_row = current_map.get(family_name, {})
        prior_row = prior_map.get(family_name, {})
        current_score = float(current_row.get("family_score") or 0.0)
        prior_score = float(prior_row.get("family_score") or 0.0)
        delta = current_score - prior_score
        family_shift_rows.append(
            FamilyShiftRow(
                signal_family=family_name,
                prior_family_score=prior_score,
                current_family_score=current_score,
                family_delta=delta,
                family_delta_abs=abs(delta),
                prior_family_rank=(
                    int(prior_row["family_rank"])
                    if prior_row.get("family_rank") is not None
                    else None
                ),
                current_family_rank=(
                    int(current_row["family_rank"])
                    if current_row.get("family_rank") is not None
                    else None
                ),
                shift_direction="gained" if delta > 0 else "lost" if delta < 0 else "unchanged",
                metadata={
                    "prior_present": bool(prior_row),
                    "current_present": bool(current_row),
                    "prior_invalidators": prior_row.get("active_invalidators", []),
                    "current_invalidators": current_row.get("active_invalidators", []),
                },
            )
        )

    family_shift_rows.sort(key=lambda row: row.family_delta_abs, reverse=True)
    top_family_delta_abs = family_shift_rows[0].family_delta_abs if family_shift_rows else 0.0
    transition_detected = bool(from_regime and to_regime and from_regime != to_regime)
    has_rank_conflict = any(
        row.prior_family_rank is not None
        and row.current_family_rank is not None
        and abs(row.current_family_rank - row.prior_family_rank) >= 2
        and row.family_delta_abs >= 0.10
        for row in family_shift_rows[:3]
    )
    classification = _classification(
        transition_detected=transition_detected,
        composite_shift_abs=composite_shift_abs or 0.0,
        top_family_delta_abs=top_family_delta_abs,
        has_family_rank_conflict=has_rank_conflict,
    )
    stability_score = _bounded(
        1.0
        - ((composite_shift_abs or 0.0) * 1.6)
        - (top_family_delta_abs * 1.25)
        - (0.12 if classification in {"unstable", "abrupt", "conflicting"} else 0.0)
    )
    anomaly_likelihood = _bounded(
        ((composite_shift_abs or 0.0) * 1.8)
        + (top_family_delta_abs * 1.4)
        + (0.20 if classification == "conflicting" else 0.0)
        + (0.08 if not transition_detected and (composite_shift_abs or 0.0) >= 0.10 else 0.0)
    )
    if classification == "normal":
        anomaly_likelihood = min(anomaly_likelihood, 0.35)

    dominant_family_gained = next(
        (row.signal_family for row in family_shift_rows if row.family_delta > 0),
        None,
    )
    dominant_family_lost = next(
        (row.signal_family for row in family_shift_rows if row.family_delta < 0),
        None,
    )

    return RegimeTransitionResult(
        run_id=run_id,
        prior_run_id=prior_run_id,
        workspace_id=workspace_id,
        watchlist_id=watchlist_id,
        queue_name=queue_name,
        from_regime=from_regime,
        to_regime=to_regime,
        transition_detected=transition_detected,
        transition_classification=classification,
        stability_score=stability_score,
        anomaly_likelihood=anomaly_likelihood,
        composite_shift=composite_shift,
        composite_shift_abs=composite_shift_abs,
        dominant_family_gained=dominant_family_gained,
        dominant_family_lost=dominant_family_lost,
        family_shifts=[row.__dict__ for row in family_shift_rows[:12]],
        metadata={
            "current_run_id": run_id,
            "prior_run_id": prior_run_id,
            "current_regime_counts": (current_run.get("regime_summary") or {}).get("regime_counts", {}),
            "prior_regime_counts": (prior_run or {}).get("regime_summary", {}).get("regime_counts", {}),
            "top_family_delta_abs": top_family_delta_abs,
            "family_shift_count": len(family_shift_rows),
            "has_rank_conflict": has_rank_conflict,
        },
    )
