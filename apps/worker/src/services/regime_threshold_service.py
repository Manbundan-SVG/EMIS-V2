from __future__ import annotations

from dataclasses import dataclass
from typing import Any


DEFAULT_THRESHOLDS = {
    "version_health_floor": 0.90,
    "family_instability_ceiling": 0.50,
    "replay_consistency_floor": 0.98,
    "regime_instability_ceiling": 0.25,
    "conflicting_transition_ceiling": 0.30,
}

RULE_EVENT_THRESHOLD_FIELDS = {
    "version_regression": ("threshold_numeric", "version_health_floor"),
    "family_instability_spike": ("threshold_numeric", "family_instability_ceiling"),
    "replay_degradation": ("threshold_numeric", "replay_consistency_floor"),
    "regime_instability_spike": ("threshold_numeric", "regime_instability_ceiling"),
    "regime_conflict_persistence": ("threshold_numeric", "conflicting_transition_ceiling"),
}


@dataclass(frozen=True)
class ThresholdSelection:
    profile_id: str | None
    override_id: str | None
    regime: str
    thresholds: dict[str, float]
    profile_name: str | None = None

    def to_application_payload(
        self,
        *,
        run_id: str,
        workspace_id: str,
        watchlist_id: str | None,
        evaluation_stage: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "workspace_id": workspace_id,
            "watchlist_id": watchlist_id,
            "regime": self.regime,
            "profile_id": self.profile_id,
            "override_id": self.override_id,
            "evaluation_stage": evaluation_stage,
            "applied_thresholds": self.thresholds,
            "metadata": {
                "profile_name": self.profile_name,
                **(metadata or {}),
            },
        }


class RegimeThresholdService:
    def select_thresholds(self, regime: str | None, active_row: dict[str, Any] | None) -> ThresholdSelection:
        active_regime = active_row.get("regime") if active_row else None
        effective_regime = str(regime or active_regime or "default") or "default"
        if not active_row:
            return ThresholdSelection(
                profile_id=None,
                override_id=None,
                regime=effective_regime,
                thresholds=dict(DEFAULT_THRESHOLDS),
                profile_name=None,
            )
        thresholds = {
            "version_health_floor": float(active_row.get("version_health_floor", DEFAULT_THRESHOLDS["version_health_floor"])),
            "family_instability_ceiling": float(
                active_row.get("family_instability_ceiling", DEFAULT_THRESHOLDS["family_instability_ceiling"])
            ),
            "replay_consistency_floor": float(
                active_row.get("replay_consistency_floor", DEFAULT_THRESHOLDS["replay_consistency_floor"])
            ),
            "regime_instability_ceiling": float(
                active_row.get("regime_instability_ceiling", DEFAULT_THRESHOLDS["regime_instability_ceiling"])
            ),
            "conflicting_transition_ceiling": float(
                active_row.get("conflicting_transition_ceiling", DEFAULT_THRESHOLDS["conflicting_transition_ceiling"])
            ),
        }
        return ThresholdSelection(
            profile_id=str(active_row["profile_id"]) if active_row.get("profile_id") else None,
            override_id=str(active_row["override_id"]) if active_row.get("override_id") else None,
            regime=effective_regime,
            thresholds=thresholds,
            profile_name=str(active_row["profile_name"]) if active_row.get("profile_name") else None,
        )

    def apply_thresholds_to_rules(
        self,
        rules: list[dict[str, Any]],
        selection: ThresholdSelection,
    ) -> list[dict[str, Any]]:
        applied: list[dict[str, Any]] = []
        for rule in rules:
            event_type = str(rule.get("event_type") or "")
            threshold_mapping = RULE_EVENT_THRESHOLD_FIELDS.get(event_type)
            if not threshold_mapping:
                applied.append(dict(rule))
                continue
            column_name, threshold_key = threshold_mapping
            if threshold_key not in selection.thresholds:
                applied.append(dict(rule))
                continue
            patched = dict(rule)
            patched[column_name] = selection.thresholds[threshold_key]
            if column_name == "threshold_numeric":
                patched["threshold_text"] = None
            metadata = dict(patched.get("metadata") or {})
            metadata["threshold_source"] = "regime_override" if selection.override_id else "profile_default"
            metadata["threshold_profile_name"] = selection.profile_name
            metadata["threshold_regime"] = selection.regime
            patched["metadata"] = metadata
            applied.append(patched)
        return applied
