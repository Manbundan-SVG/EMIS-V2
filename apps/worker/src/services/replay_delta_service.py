from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class ReplayDeltaEntry:
    name: str
    source_value: float
    replay_value: float
    delta: float
    delta_abs: float


@dataclass(frozen=True)
class ReplayDeltaResult:
    replay_run_id: str
    source_run_id: str
    workspace_id: str
    watchlist_id: str | None
    input_match_score: float
    input_match_details: dict[str, Any]
    version_match: bool
    compute_version_changed: bool
    signal_registry_version_changed: bool
    model_version_changed: bool
    regime_changed: bool
    source_regime: str | None
    replay_regime: str | None
    source_composite: float | None
    replay_composite: float | None
    composite_delta: float | None
    composite_delta_abs: float | None
    largest_signal_deltas: list[dict[str, Any]]
    largest_family_deltas: list[dict[str, Any]]
    summary: dict[str, Any]
    severity: str


def _dominant_regime(run_context: dict[str, Any]) -> str | None:
    regime_summary = run_context.get("regime_summary") or {}
    regime_counts = regime_summary.get("regime_counts") if isinstance(regime_summary, dict) else None
    if not isinstance(regime_counts, dict) or not regime_counts:
        return None
    return str(max(regime_counts.items(), key=lambda item: item[1])[0])


def _flatten(prefix: str, value: Any) -> dict[str, str]:
    key_prefix = prefix.strip(".")
    if isinstance(value, dict):
        rows: dict[str, str] = {}
        for key in sorted(value):
            child_prefix = f"{key_prefix}.{key}" if key_prefix else str(key)
            rows.update(_flatten(child_prefix, value[key]))
        return rows
    if isinstance(value, list):
        rows: dict[str, str] = {}
        for idx, item in enumerate(value):
            child_prefix = f"{key_prefix}[{idx}]"
            rows.update(_flatten(child_prefix, item))
        return rows
    return {key_prefix or "value": str(value)}


def _score_input_match(source_snapshot: dict[str, Any], replay_snapshot: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    comparable_source = {
        "source_window_start": source_snapshot.get("source_window_start"),
        "source_window_end": source_snapshot.get("source_window_end"),
        "asset_count": source_snapshot.get("asset_count"),
        "source_coverage": source_snapshot.get("source_coverage", {}),
        "input_values": source_snapshot.get("input_values", {}),
        "version_pins": source_snapshot.get("version_pins", {}),
    }
    comparable_replay = {
        "source_window_start": replay_snapshot.get("source_window_start"),
        "source_window_end": replay_snapshot.get("source_window_end"),
        "asset_count": replay_snapshot.get("asset_count"),
        "source_coverage": replay_snapshot.get("source_coverage", {}),
        "input_values": replay_snapshot.get("input_values", {}),
        "version_pins": replay_snapshot.get("version_pins", {}),
    }
    source_flat = _flatten("", comparable_source)
    replay_flat = _flatten("", comparable_replay)
    all_keys = sorted(set(source_flat) | set(replay_flat))
    if not all_keys:
        return 1.0, {"matched_keys": 0, "total_keys": 0, "mismatches": []}

    mismatches: list[dict[str, Any]] = []
    matched_keys = 0
    for key in all_keys:
        if source_flat.get(key) == replay_flat.get(key):
            matched_keys += 1
        else:
            mismatches.append(
                {
                    "key": key,
                    "source": source_flat.get(key),
                    "replay": replay_flat.get(key),
                }
            )

    return matched_keys / len(all_keys), {
        "matched_keys": matched_keys,
        "total_keys": len(all_keys),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:20],
    }


def _collect_top_deltas(
    source_rows: list[dict[str, Any]],
    replay_rows: list[dict[str, Any]],
    name_key: str,
    value_key: str,
    *,
    row_name: str,
) -> list[dict[str, Any]]:
    source_map = {str(row.get(name_key)): row for row in source_rows}
    replay_map = {str(row.get(name_key)): row for row in replay_rows}
    deltas: list[ReplayDeltaEntry] = []

    for name in sorted(set(source_map) | set(replay_map)):
        source_row = source_map.get(name, {})
        replay_row = replay_map.get(name, {})
        source_value = float(source_row.get(value_key) or 0.0)
        replay_value = float(replay_row.get(value_key) or 0.0)
        delta = replay_value - source_value
        deltas.append(
            ReplayDeltaEntry(
                name=name,
                source_value=source_value,
                replay_value=replay_value,
                delta=delta,
                delta_abs=abs(delta),
            )
        )

    top = sorted(deltas, key=lambda row: row.delta_abs, reverse=True)[:10]
    serialized: list[dict[str, Any]] = []
    for row in top:
        payload = asdict(row)
        payload[row_name] = payload.pop("name")
        serialized.append(payload)
    return serialized


def _diagnosis(
    *,
    input_match_score: float,
    version_match: bool,
    regime_changed: bool,
    composite_delta_abs: float,
) -> str:
    if input_match_score >= 0.999 and version_match and not regime_changed and composite_delta_abs <= 0.01:
        return "replay_match"
    if input_match_score < 0.99:
        return "input_mismatch"
    if not version_match:
        return "version_behavior_change"
    if regime_changed:
        return "regime_shift"
    return "suspicious_divergence"


def _severity(
    *,
    input_match_score: float,
    version_match: bool,
    regime_changed: bool,
    composite_delta_abs: float,
) -> str:
    if input_match_score >= 0.999 and version_match and not regime_changed and composite_delta_abs <= 0.01:
        return "low"
    if input_match_score < 0.95:
        return "high"
    if version_match and not regime_changed and composite_delta_abs >= 0.10:
        return "high"
    if not version_match or regime_changed or composite_delta_abs >= 0.05:
        return "medium"
    return "low"


def build_replay_delta(
    replay_run: dict[str, Any],
    source_run: dict[str, Any],
    replay_snapshot: dict[str, Any],
    source_snapshot: dict[str, Any],
    replay_signal_attributions: list[dict[str, Any]],
    source_signal_attributions: list[dict[str, Any]],
    replay_family_attributions: list[dict[str, Any]],
    source_family_attributions: list[dict[str, Any]],
) -> ReplayDeltaResult:
    input_match_score, input_match_details = _score_input_match(source_snapshot, replay_snapshot)

    source_composite = float(source_run.get("composite_score")) if source_run.get("composite_score") is not None else None
    replay_composite = float(replay_run.get("composite_score")) if replay_run.get("composite_score") is not None else None
    composite_delta = None
    composite_delta_abs = None
    if source_composite is not None and replay_composite is not None:
        composite_delta = replay_composite - source_composite
        composite_delta_abs = abs(composite_delta)

    compute_version_changed = source_run.get("compute_version") != replay_run.get("compute_version")
    signal_registry_version_changed = source_run.get("signal_registry_version") != replay_run.get("signal_registry_version")
    model_version_changed = source_run.get("model_version") != replay_run.get("model_version")
    version_match = not any(
        [
            compute_version_changed,
            signal_registry_version_changed,
            model_version_changed,
        ]
    )

    source_regime = _dominant_regime(source_run)
    replay_regime = _dominant_regime(replay_run)
    regime_changed = source_regime != replay_regime

    source_signal_rows = [
        {
            "signal_key": f"{row.get('asset_symbol') or 'unknown'}:{row.get('signal_name')}",
            "contribution_value": row.get("contribution_value"),
        }
        for row in source_signal_attributions
    ]
    replay_signal_rows = [
        {
            "signal_key": f"{row.get('asset_symbol') or 'unknown'}:{row.get('signal_name')}",
            "contribution_value": row.get("contribution_value"),
        }
        for row in replay_signal_attributions
    ]

    largest_signal_deltas = _collect_top_deltas(
        source_signal_rows,
        replay_signal_rows,
        "signal_key",
        "contribution_value",
        row_name="signal_key",
    )
    largest_family_deltas = _collect_top_deltas(
        source_family_attributions,
        replay_family_attributions,
        "signal_family",
        "family_score",
        row_name="signal_family",
    )

    delta_abs_value = composite_delta_abs or 0.0
    diagnosis = _diagnosis(
        input_match_score=input_match_score,
        version_match=version_match,
        regime_changed=regime_changed,
        composite_delta_abs=delta_abs_value,
    )
    severity = _severity(
        input_match_score=input_match_score,
        version_match=version_match,
        regime_changed=regime_changed,
        composite_delta_abs=delta_abs_value,
    )

    summary = {
        "diagnosis": diagnosis,
        "input_match_score": input_match_score,
        "version_match": version_match,
        "regime_changed": regime_changed,
        "composite_delta": composite_delta,
        "composite_delta_abs": composite_delta_abs,
        "source_versions": {
            "compute_version": source_run.get("compute_version"),
            "signal_registry_version": source_run.get("signal_registry_version"),
            "model_version": source_run.get("model_version"),
        },
        "replay_versions": {
            "compute_version": replay_run.get("compute_version"),
            "signal_registry_version": replay_run.get("signal_registry_version"),
            "model_version": replay_run.get("model_version"),
        },
    }

    return ReplayDeltaResult(
        replay_run_id=str(replay_run["id"]),
        source_run_id=str(source_run["id"]),
        workspace_id=str(replay_run["workspace_id"]),
        watchlist_id=str(replay_run["watchlist_id"]) if replay_run.get("watchlist_id") else None,
        input_match_score=input_match_score,
        input_match_details=input_match_details,
        version_match=version_match,
        compute_version_changed=compute_version_changed,
        signal_registry_version_changed=signal_registry_version_changed,
        model_version_changed=model_version_changed,
        regime_changed=regime_changed,
        source_regime=source_regime,
        replay_regime=replay_regime,
        source_composite=source_composite,
        replay_composite=replay_composite,
        composite_delta=composite_delta,
        composite_delta_abs=composite_delta_abs,
        largest_signal_deltas=largest_signal_deltas,
        largest_family_deltas=largest_family_deltas,
        summary=summary,
        severity=severity,
    )
