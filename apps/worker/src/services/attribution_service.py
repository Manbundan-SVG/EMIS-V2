from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from src.db.repositories import MarketState
from src.signals.composite_service import REGIME_WEIGHTS

ATTRIBUTION_VERSION = "phase2.5A/v1"

SIGNAL_FAMILY_MAP = {
    "trend_structure": "trend",
    "oi_price_divergence": "leverage",
    "funding_stress": "leverage",
    "liquidation_magnet_distance": "liquidation",
    "macro_alignment": "macro",
}

FAMILY_INVALIDATOR_MAP = {
    "trend": [],
    "leverage": ["extreme_funding"],
    "liquidation": ["forced_flow"],
    "macro": ["macro_shock"],
}


@dataclass(frozen=True)
class SignalAttributionRow:
    asset_id: str
    asset_symbol: str
    regime: str
    signal_name: str
    signal_family: str
    raw_value: float | None
    normalized_value: float | None
    weight_applied: float
    contribution_value: float
    contribution_direction: str
    is_invalidator: bool
    active_invalidators: list[str]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class SignalFamilyAttributionRow:
    signal_family: str
    family_rank: int
    family_weight: float
    family_score: float
    family_pct_of_total: float
    positive_contribution: float
    negative_contribution: float
    invalidator_contribution: float
    active_invalidators: list[str]
    metadata: dict[str, Any]


def _direction(value: float) -> str:
    if value > 0:
        return "positive"
    if value < 0:
        return "negative"
    return "neutral"


def build_run_attributions(
    market_states: list[MarketState],
    signal_rows: list[dict[str, Any]],
    composite_rows: list[dict[str, Any]],
) -> tuple[list[SignalAttributionRow], list[SignalFamilyAttributionRow], dict[str, Any]]:
    state_by_asset = {state.asset_id: state for state in market_states}
    composite_by_asset = {row["asset_id"]: row for row in composite_rows}

    signal_attributions: list[SignalAttributionRow] = []
    family_rollup: dict[str, dict[str, Any]] = {}

    for row in signal_rows:
        asset_id = str(row["asset_id"])
        state = state_by_asset.get(asset_id)
        composite = composite_by_asset.get(asset_id)
        if state is None or composite is None:
            continue

        signal_name = str(row["signal_name"])
        signal_family = SIGNAL_FAMILY_MAP.get(signal_name, "unknown")
        regime = str(composite["regime"])
        weights = REGIME_WEIGHTS.get(regime, {})
        normalized_value = float(row["score"])
        weight_applied = float(weights.get(signal_name, 0.0))
        contribution_value = normalized_value * weight_applied
        composite_invalidators = composite.get("invalidators") or {}
        active_invalidators = sorted(
            name for name, enabled in composite_invalidators.items() if enabled
        )
        relevant_invalidators = [
            name for name in FAMILY_INVALIDATOR_MAP.get(signal_family, [])
            if name in active_invalidators
        ]

        signal_attr = SignalAttributionRow(
            asset_id=asset_id,
            asset_symbol=state.symbol,
            regime=regime,
            signal_name=signal_name,
            signal_family=signal_family,
            raw_value=None,
            normalized_value=normalized_value,
            weight_applied=weight_applied,
            contribution_value=contribution_value,
            contribution_direction=_direction(contribution_value),
            is_invalidator=bool(relevant_invalidators),
            active_invalidators=relevant_invalidators,
            metadata={
                "all_active_invalidators": active_invalidators,
                "timestamp": str(row["timestamp"]),
                "composite_signed_score": float(composite["long_score"]) - float(composite["short_score"]),
            },
        )
        signal_attributions.append(signal_attr)

        bucket = family_rollup.setdefault(
            signal_family,
            {
                "weight_sum": 0.0,
                "driver_count": 0,
                "family_score": 0.0,
                "positive_contribution": 0.0,
                "negative_contribution": 0.0,
                "invalidator_contribution": 0.0,
                "asset_symbols": set(),
                "active_invalidators": set(),
            },
        )
        bucket["weight_sum"] += weight_applied
        bucket["driver_count"] += 1
        bucket["family_score"] += contribution_value
        bucket["asset_symbols"].add(state.symbol)
        bucket["active_invalidators"].update(relevant_invalidators)
        if contribution_value > 0:
            bucket["positive_contribution"] += contribution_value
        elif contribution_value < 0:
            bucket["negative_contribution"] += contribution_value
        if relevant_invalidators:
            bucket["invalidator_contribution"] += contribution_value

    attribution_total = sum(row.contribution_value for row in signal_attributions)
    composite_target_total = sum(
        float(row["long_score"]) - float(row["short_score"])
        for row in composite_rows
    )
    denominator = abs(attribution_total) if abs(attribution_total) > 1e-12 else 1.0

    ranked_families = sorted(
        family_rollup.items(),
        key=lambda item: abs(item[1]["family_score"]),
        reverse=True,
    )

    family_attributions: list[SignalFamilyAttributionRow] = []
    for rank, (signal_family, bucket) in enumerate(ranked_families, start=1):
        family_attributions.append(
            SignalFamilyAttributionRow(
                signal_family=signal_family,
                family_rank=rank,
                family_weight=float(bucket["weight_sum"] / bucket["driver_count"]) if bucket["driver_count"] else 0.0,
                family_score=float(bucket["family_score"]),
                family_pct_of_total=float(bucket["family_score"] / denominator),
                positive_contribution=float(bucket["positive_contribution"]),
                negative_contribution=float(bucket["negative_contribution"]),
                invalidator_contribution=float(bucket["invalidator_contribution"]),
                active_invalidators=sorted(bucket["active_invalidators"]),
                metadata={
                    "driver_count": bucket["driver_count"],
                    "asset_symbols": sorted(bucket["asset_symbols"]),
                },
            )
        )

    reconciliation_delta = composite_target_total - attribution_total
    reconciliation = {
        "attribution_version": ATTRIBUTION_VERSION,
        "attribution_total": float(attribution_total),
        "composite_target_total": float(composite_target_total),
        "reconciliation_delta": float(reconciliation_delta),
        "reconciled": abs(reconciliation_delta) <= 1e-6,
    }

    return signal_attributions, family_attributions, reconciliation


def serialize_signal_attributions(rows: list[SignalAttributionRow]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]


def serialize_family_attributions(rows: list[SignalFamilyAttributionRow]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]
