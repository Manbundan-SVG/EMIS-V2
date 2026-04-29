from collections import defaultdict
from src.db.repositories import MarketState

REGIME_WEIGHTS = {
    "trend_persistence": {"trend_structure": 0.40, "oi_price_divergence": 0.20, "funding_stress": 0.10, "liquidation_magnet_distance": 0.10, "macro_alignment": 0.20},
    "deleveraging": {"trend_structure": 0.20, "oi_price_divergence": 0.30, "funding_stress": 0.25, "liquidation_magnet_distance": 0.15, "macro_alignment": 0.10},
    "macro_dominant": {"trend_structure": 0.15, "oi_price_divergence": 0.10, "funding_stress": 0.10, "liquidation_magnet_distance": 0.10, "macro_alignment": 0.55}
}

def determine_regime(state: MarketState) -> str:
    if state.liquidation_notional_1h > 2000000 or abs(state.funding_rate) > 0.0025:
        return "deleveraging"
    if abs(state.macro_dxy_return_1d) > 0.004 or abs(state.macro_us10y_change_1d) > 0.06:
        return "macro_dominant"
    return "trend_persistence"

def compute_composite_rows(workspace_id: str, market_states: list[MarketState], signal_rows: list[dict]) -> list[dict]:
    by_asset = defaultdict(dict)
    ts_by_asset = {}
    states_by_asset = {state.asset_id: state for state in market_states}
    for row in signal_rows:
        by_asset[row["asset_id"]][row["signal_name"]] = float(row["score"])
        ts_by_asset[row["asset_id"]] = row["timestamp"]
    out = []
    for asset_id, signals in by_asset.items():
        state = states_by_asset[asset_id]
        regime = determine_regime(state)
        weights = REGIME_WEIGHTS[regime]
        weighted_sum = sum(signals.get(name, 0.0) * weight for name, weight in weights.items())
        confidence = min(1.0, 0.4 + abs(signals.get("trend_structure", 0.0)) * 0.2 + abs(signals.get("macro_alignment", 0.0)) * 0.2)
        out.append({
            "workspace_id": workspace_id,
            "asset_id": asset_id,
            "timestamp": ts_by_asset[asset_id],
            "regime": regime,
            "long_score": round(max(0.0, weighted_sum), 6),
            "short_score": round(max(0.0, -weighted_sum), 6),
            "confidence": round(confidence, 6),
            "invalidators": {
                "macro_shock": abs(state.macro_dxy_return_1d) > 0.006,
                "extreme_funding": abs(state.funding_rate) > 0.004,
                "forced_flow": state.liquidation_notional_1h > 3000000
            }
        })
    return out
