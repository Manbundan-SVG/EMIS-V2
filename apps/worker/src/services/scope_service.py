from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from typing import Any, Iterable


SCOPE_VERSION = "phase2.6B/v1"

DEFAULT_DEPENDENCY_POLICY = {
    "scope_mode": "watchlist_scoped",
    "macro_series_codes": ["DXY", "US10Y"],
    "cross_asset_dependencies": [],
}


@dataclass(frozen=True)
class ComputeScope:
    primary_assets: list[str]
    dependency_assets: list[str]
    asset_universe: list[str]
    dependency_policy: dict[str, Any]
    scope_hash: str
    scope_version: str = SCOPE_VERSION


def _dedupe_keep_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        normalized = str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def build_scope_hash(
    primary_assets: list[str],
    dependency_assets: list[str],
    dependency_policy: dict[str, Any],
) -> str:
    payload = {
        "primary_assets": primary_assets,
        "dependency_assets": dependency_assets,
        "dependency_policy": dependency_policy,
    }
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def resolve_compute_scope(
    watchlist_assets: list[str],
    *,
    dependency_policy: dict[str, Any] | None = None,
    source_scope: dict[str, Any] | None = None,
) -> ComputeScope:
    if source_scope:
        primary_assets = _dedupe_keep_order(source_scope.get("primary_assets") or [])
        dependency_assets = _dedupe_keep_order(source_scope.get("dependency_assets") or [])
        asset_universe = _dedupe_keep_order(source_scope.get("asset_universe") or (primary_assets + dependency_assets))
        resolved_policy = dict(source_scope.get("dependency_policy") or DEFAULT_DEPENDENCY_POLICY)
        return ComputeScope(
            primary_assets=primary_assets,
            dependency_assets=dependency_assets,
            asset_universe=asset_universe,
            dependency_policy=resolved_policy,
            scope_hash=str(source_scope.get("scope_hash") or build_scope_hash(primary_assets, dependency_assets, resolved_policy)),
            scope_version=str(source_scope.get("scope_version") or SCOPE_VERSION),
        )

    resolved_policy = dict(DEFAULT_DEPENDENCY_POLICY)
    if dependency_policy:
        resolved_policy.update(dependency_policy)

    primary_assets = _dedupe_keep_order(watchlist_assets)
    dependency_assets = [
        symbol
        for symbol in _dedupe_keep_order(resolved_policy.get("cross_asset_dependencies") or [])
        if symbol not in set(primary_assets)
    ]
    asset_universe = _dedupe_keep_order(primary_assets + dependency_assets)
    scope_hash = build_scope_hash(primary_assets, dependency_assets, resolved_policy)
    return ComputeScope(
        primary_assets=primary_assets,
        dependency_assets=dependency_assets,
        asset_universe=asset_universe,
        dependency_policy=resolved_policy,
        scope_hash=scope_hash,
    )
