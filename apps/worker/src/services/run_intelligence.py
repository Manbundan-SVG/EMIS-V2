from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Iterator
import re

from src.db.repositories import MarketState
from src.signals.composite_service import REGIME_WEIGHTS

EXPLANATION_VERSION = "phase2.4/v1"
DEFAULT_COMPUTE_VERSION = "phase2.4"
DEFAULT_SIGNAL_REGISTRY_VERSION = "v1"
DEFAULT_MODEL_VERSION = "v1"


def isoformat_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def classify_failure_code(stage_name: str, exc: Exception) -> str:
    message = str(exc).lower()
    if stage_name == "load_inputs" and "no normalized market state" in message:
        return "inputs_unavailable"

    exc_name = type(exc).__name__
    exc_slug = re.sub(r"[^a-z0-9]+", "_", exc_name.lower()).strip("_") or "error"
    return f"{stage_name}_{exc_slug}"


@dataclass(frozen=True)
class StageTimingRecord:
    stage_name: str
    stage_status: str
    started_at: datetime
    completed_at: datetime
    runtime_ms: int
    error_summary: str | None = None
    failure_code: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReplayContext:
    source_run_id: str | None
    source_input_snapshot_id: int | None
    replay_as_of_ts: str | None
    compute_version: str
    signal_registry_version: str
    model_version: str


class RunTelemetry:
    def __init__(self) -> None:
        self.stage_timings: list[StageTimingRecord] = []
        self.failure_stage: str | None = None
        self.failure_code: str | None = None

    @contextmanager
    def track_stage(self, stage_name: str) -> Iterator[dict[str, Any]]:
        metadata: dict[str, Any] = {}
        started_at = datetime.now(timezone.utc)
        started_perf = perf_counter()
        try:
            yield metadata
        except Exception as exc:
            completed_at = datetime.now(timezone.utc)
            failure_code = classify_failure_code(stage_name, exc)
            self.stage_timings.append(
                StageTimingRecord(
                    stage_name=stage_name,
                    stage_status="failed",
                    started_at=started_at,
                    completed_at=completed_at,
                    runtime_ms=max(0, int((perf_counter() - started_perf) * 1000)),
                    error_summary=str(exc)[:500],
                    failure_code=failure_code,
                    metadata=metadata,
                )
            )
            if self.failure_stage is None:
                self.failure_stage = stage_name
            if self.failure_code is None:
                self.failure_code = failure_code
            raise

        completed_at = datetime.now(timezone.utc)
        self.stage_timings.append(
            StageTimingRecord(
                stage_name=stage_name,
                stage_status="completed",
                started_at=started_at,
                completed_at=completed_at,
                runtime_ms=max(0, int((perf_counter() - started_perf) * 1000)),
                metadata=metadata,
            )
        )


def extract_replay_context(payload: dict[str, Any] | None) -> ReplayContext:
    replay = (payload or {}).get("replay")
    replay_obj = replay if isinstance(replay, dict) else {}
    pinned = replay_obj.get("pinned_versions")
    pinned_versions = pinned if isinstance(pinned, dict) else {}
    return ReplayContext(
        source_run_id=str(replay_obj.get("source_run_id")) if replay_obj.get("source_run_id") else None,
        source_input_snapshot_id=int(replay_obj.get("source_input_snapshot_id")) if replay_obj.get("source_input_snapshot_id") else None,
        replay_as_of_ts=isoformat_or_none(replay_obj.get("replay_as_of_ts")),
        compute_version=str(pinned_versions.get("compute_version") or DEFAULT_COMPUTE_VERSION),
        signal_registry_version=str(pinned_versions.get("signal_registry_version") or DEFAULT_SIGNAL_REGISTRY_VERSION),
        model_version=str(pinned_versions.get("model_version") or DEFAULT_MODEL_VERSION),
    )


def build_version_pins(
    compute_version: str,
    signal_registry_version: str,
    model_version: str,
) -> dict[str, str]:
    return {
        "compute_version": compute_version,
        "signal_registry_version": signal_registry_version,
        "model_version": model_version,
    }


def build_input_snapshot(
    market_states: list[MarketState],
    version_pins: dict[str, str],
    *,
    replay_context: ReplayContext,
    compute_scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    timestamp_candidates = [
        ts
        for state in market_states
        for ts in (
            state.bar_ts,
            state.oi_ts,
            state.funding_ts,
            state.liquidation_ts,
            state.macro_dxy_ts,
            state.macro_us10y_ts,
        )
        if ts is not None
    ]
    market_state_candidates = [state.as_of_ts for state in market_states if state.as_of_ts is not None]
    ordered = sorted(timestamp_candidates)
    market_state_ordered = sorted(market_state_candidates)
    source_window_start = (
        isoformat_or_none(market_state_ordered[0])
        if market_state_ordered
        else isoformat_or_none(ordered[0]) if ordered else None
    )
    source_window_end = (
        isoformat_or_none(market_state_ordered[-1])
        if market_state_ordered
        else isoformat_or_none(ordered[-1]) if ordered else None
    )

    inputs_by_asset = {
        state.symbol: {
            "asset_id": state.asset_id,
            "symbol": state.symbol,
            "as_of_ts": isoformat_or_none(state.as_of_ts),
            "bar_close": state.bar_close,
            "bar_return_1h": state.bar_return_1h,
            "volume_zscore": state.volume_zscore,
            "oi_change_1h": state.oi_change_1h,
            "funding_rate": state.funding_rate,
            "liquidation_notional_1h": state.liquidation_notional_1h,
            "macro_dxy_return_1d": state.macro_dxy_return_1d,
            "macro_us10y_change_1d": state.macro_us10y_change_1d,
            "source_timestamps": {
                "bars": isoformat_or_none(state.bar_ts),
                "open_interest": isoformat_or_none(state.oi_ts),
                "funding": isoformat_or_none(state.funding_ts),
                "liquidations": isoformat_or_none(state.liquidation_ts),
                "macro_dxy": isoformat_or_none(state.macro_dxy_ts),
                "macro_us10y": isoformat_or_none(state.macro_us10y_ts),
            },
        }
        for state in market_states
    }

    coverage = {
        "bars": {
            "covered_assets": len(market_states),
            "missing_assets": [],
        },
        "open_interest": {
            "covered_assets": sum(1 for state in market_states if state.has_open_interest),
            "missing_assets": [state.symbol for state in market_states if not state.has_open_interest],
        },
        "funding": {
            "covered_assets": sum(1 for state in market_states if state.has_funding),
            "missing_assets": [state.symbol for state in market_states if not state.has_funding],
        },
        "liquidations": {
            "covered_assets": sum(1 for state in market_states if state.has_liquidations),
            "missing_assets": [state.symbol for state in market_states if not state.has_liquidations],
        },
        "macro_dxy": {
            "covered_assets": sum(1 for state in market_states if state.has_macro_dxy),
            "missing_assets": [] if all(state.has_macro_dxy for state in market_states) else [state.symbol for state in market_states if not state.has_macro_dxy],
        },
        "macro_us10y": {
            "covered_assets": sum(1 for state in market_states if state.has_macro_us10y),
            "missing_assets": [] if all(state.has_macro_us10y for state in market_states) else [state.symbol for state in market_states if not state.has_macro_us10y],
        },
    }

    return {
        "source_window_start": source_window_start,
        "source_window_end": source_window_end,
        "asset_count": len(market_states),
        "source_coverage": coverage,
        "input_values": {
            "inputs_by_asset": inputs_by_asset,
            "asset_symbols": [state.symbol for state in market_states],
            "primary_asset_symbols": list((compute_scope or {}).get("primary_assets") or []),
            "dependency_asset_symbols": list((compute_scope or {}).get("dependency_assets") or []),
            "asset_universe": list((compute_scope or {}).get("asset_universe") or [state.symbol for state in market_states]),
        },
        "version_pins": version_pins,
        "metadata": {
            "replay_source_run_id": replay_context.source_run_id,
            "replay_source_input_snapshot_id": replay_context.source_input_snapshot_id,
            "requested_replay_as_of_ts": replay_context.replay_as_of_ts,
            "raw_input_window_start": isoformat_or_none(ordered[0]) if ordered else None,
            "raw_input_window_end": isoformat_or_none(ordered[-1]) if ordered else None,
            "compute_scope_id": str((compute_scope or {}).get("id")) if (compute_scope or {}).get("id") else None,
            "scope_hash": str((compute_scope or {}).get("scope_hash")) if (compute_scope or {}).get("scope_hash") else None,
            "scope_version": str((compute_scope or {}).get("scope_version")) if (compute_scope or {}).get("scope_version") else None,
            "primary_asset_count": len((compute_scope or {}).get("primary_assets") or []),
            "dependency_asset_count": len((compute_scope or {}).get("dependency_assets") or []),
            "asset_universe_count": len((compute_scope or {}).get("asset_universe") or [state.symbol for state in market_states]),
        },
    }


def build_run_explanation(
    market_states: list[MarketState],
    signal_rows: list[dict[str, Any]],
    composite_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    symbol_by_asset = {state.asset_id: state.symbol for state in market_states}
    signal_map: dict[str, dict[str, float]] = {}
    regime_by_asset: dict[str, str] = {}
    composite_by_asset: dict[str, dict[str, Any]] = {}
    invalidators_by_asset: dict[str, dict[str, Any]] = {}
    regime_counts: dict[str, int] = {}
    contributor_rows: list[dict[str, Any]] = []

    for row in signal_rows:
        signal_map.setdefault(row["asset_id"], {})[row["signal_name"]] = float(row["score"])

    for row in composite_rows:
        asset_id = row["asset_id"]
        symbol = symbol_by_asset.get(asset_id, asset_id)
        regime = str(row["regime"])
        weights = REGIME_WEIGHTS.get(regime, {})
        signals = signal_map.get(asset_id, {})
        regime_by_asset[symbol] = regime
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
        composite_by_asset[symbol] = {
            "long_score": row["long_score"],
            "short_score": row["short_score"],
            "confidence": row["confidence"],
            "regime": regime,
        }
        invalidators = row.get("invalidators") or {}
        invalidators_by_asset[symbol] = invalidators
        for signal_name, weight in weights.items():
            contribution = round(float(signals.get(signal_name, 0.0)) * float(weight), 6)
            contributor_rows.append(
                {
                    "asset_id": asset_id,
                    "symbol": symbol,
                    "regime": regime,
                    "signal_name": signal_name,
                    "signal_score": round(float(signals.get(signal_name, 0.0)), 6),
                    "weight": round(float(weight), 6),
                    "contribution": contribution,
                }
            )

    top_positive = sorted(
        [row for row in contributor_rows if row["contribution"] > 0],
        key=lambda row: row["contribution"],
        reverse=True,
    )[:5]
    top_negative = sorted(
        [row for row in contributor_rows if row["contribution"] < 0],
        key=lambda row: row["contribution"],
    )[:5]

    signals_by_asset = {
        symbol_by_asset.get(asset_id, asset_id): {
            signal_name: round(score, 6)
            for signal_name, score in sorted(asset_signals.items())
        }
        for asset_id, asset_signals in signal_map.items()
    }

    dominant_regime = max(regime_counts.items(), key=lambda item: item[1])[0] if regime_counts else "unknown"
    summary_parts = [f"dominant regime {dominant_regime} across {len(composite_rows)} assets"]
    if top_positive:
        top = top_positive[0]
        summary_parts.append(
            f"top positive {top['symbol']} {top['signal_name']} {top['contribution']:+.3f}"
        )
    if top_negative:
        bottom = top_negative[0]
        summary_parts.append(
            f"top negative {bottom['symbol']} {bottom['signal_name']} {bottom['contribution']:+.3f}"
        )

    return {
        "summary": "; ".join(summary_parts),
        "regime_summary": {
            "regime_by_asset": regime_by_asset,
            "regime_counts": regime_counts,
        },
        "signal_summary": {
            "signals_by_asset": signals_by_asset,
        },
        "composite_summary": {
            "scores_by_asset": composite_by_asset,
        },
        "invalidator_summary": {
            "invalidators_by_asset": invalidators_by_asset,
        },
        "top_positive_contributors": top_positive,
        "top_negative_contributors": top_negative,
        "metadata": {
            "asset_count": len(composite_rows),
            "signal_row_count": len(signal_rows),
        },
    }
