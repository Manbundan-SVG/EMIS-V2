from src.db.repositories import MarketState

def compute_feature_rows(workspace_id: str, market_states: list[MarketState]) -> list[dict]:
    rows = []
    for state in market_states:
        rows.extend([
            {"workspace_id": workspace_id, "asset_id": state.asset_id, "feature_name": "trend_return_1h", "timestamp": state.as_of_ts, "value": state.bar_return_1h, "meta": {"symbol": state.symbol}},
            {"workspace_id": workspace_id, "asset_id": state.asset_id, "feature_name": "volume_zscore", "timestamp": state.as_of_ts, "value": state.volume_zscore, "meta": {"symbol": state.symbol}},
            {"workspace_id": workspace_id, "asset_id": state.asset_id, "feature_name": "oi_change_1h", "timestamp": state.as_of_ts, "value": state.oi_change_1h, "meta": {"symbol": state.symbol}},
            {"workspace_id": workspace_id, "asset_id": state.asset_id, "feature_name": "funding_rate", "timestamp": state.as_of_ts, "value": state.funding_rate, "meta": {"symbol": state.symbol}},
            {"workspace_id": workspace_id, "asset_id": state.asset_id, "feature_name": "liquidation_notional_1h", "timestamp": state.as_of_ts, "value": state.liquidation_notional_1h, "meta": {"symbol": state.symbol}},
            {"workspace_id": workspace_id, "asset_id": state.asset_id, "feature_name": "macro_dxy_return_1d", "timestamp": state.as_of_ts, "value": state.macro_dxy_return_1d, "meta": {"symbol": state.symbol}},
            {"workspace_id": workspace_id, "asset_id": state.asset_id, "feature_name": "macro_us10y_change_1d", "timestamp": state.as_of_ts, "value": state.macro_us10y_change_1d, "meta": {"symbol": state.symbol}},
        ])
    return rows
