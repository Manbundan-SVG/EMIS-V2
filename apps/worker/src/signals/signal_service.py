from src.db.repositories import MarketState

def clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value))

def compute_signal_rows(workspace_id: str, market_states: list[MarketState]) -> list[dict]:
    rows = []
    for state in market_states:
        signals = {
            "trend_structure": clamp(state.bar_return_1h * 8 + state.volume_zscore * 0.1),
            "oi_price_divergence": clamp((-state.bar_return_1h * state.oi_change_1h) * 10),
            "funding_stress": clamp(state.funding_rate * 250),
            "liquidation_magnet_distance": clamp((state.liquidation_notional_1h / 1000000) * -0.2),
            "macro_alignment": clamp(-(state.macro_dxy_return_1d * 4 + state.macro_us10y_change_1d * 1.5)),
        }
        for signal_name, score in signals.items():
            rows.append({
                "workspace_id": workspace_id,
                "asset_id": state.asset_id,
                "signal_name": signal_name,
                "timestamp": state.as_of_ts,
                "score": round(score, 6),
                "explanation": {"symbol": state.symbol}
            })
    return rows
