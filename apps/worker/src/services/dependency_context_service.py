"""Phase 4.0B: Dependency Context Service.

Assembles deterministic dependency context for a watchlist:
  1. Resolve primary symbols from watchlist_assets.
  2. Resolve active watchlist_dependency_profile (or default).
  3. Expand asset_dependency_graph (one hop in 4.0B).
  4. Filter by profile include_* flags and max_dependencies.
  5. Rank deterministically (priority DESC, weight DESC, symbol ASC).
  6. Compute coverage against normalized_multi_asset_market_state.
  7. Compute a stable context_hash (sha256 over sorted inputs).
  8. Persist a watchlist_context_snapshots row.

Context assembly is deterministic — repeated runs with unchanged inputs
produce the same context_hash.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_CONTEXT_VERSION = "4.0B.v1"
_MAX_DEPTH = 1  # direct dependencies only in 4.0B
_DEFAULT_PROFILE_NAME = "default_inclusive"
_DEFAULT_PROFILE_CONTROLS: dict[str, Any] = {
    "include_macro":        True,
    "include_fx":           True,
    "include_rates":        True,
    "include_equity_index": True,
    "include_commodity":    True,
    "include_crypto_cross": True,
    "max_dependencies":     25,
}

# dependency_family → profile include_* key
_FAMILY_TO_INCLUDE_KEY = {
    "macro":        "include_macro",
    "fx":           "include_fx",
    "rates":        "include_rates",
    "equity_index": "include_equity_index",
    "commodity":    "include_commodity",
    "crypto_cross": "include_crypto_cross",
    "risk":         "include_equity_index",  # risk edges point to indices; gated by equity_index
}


@dataclass(frozen=True)
class DependencyEdge:
    from_symbol: str
    to_symbol: str
    dependency_type: str
    dependency_family: str
    priority: int
    weight: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ContextSnapshot:
    workspace_id: str
    watchlist_id: str
    profile_id: str | None
    snapshot_at: datetime
    primary_symbols: list[str]
    dependency_symbols: list[str]
    dependency_families: list[str]
    context_hash: str
    coverage_summary: dict[str, Any]
    metadata: dict[str, Any]


class DependencyContextService:
    """Deterministic dependency context assembly."""

    # ── input resolution ────────────────────────────────────────────────
    def get_primary_symbols(
        self, conn, *, workspace_id: str, watchlist_id: str,
    ) -> list[str]:
        with conn.cursor() as cur:
            cur.execute(
                """
                select distinct a.symbol as symbol
                from public.watchlists w
                join public.watchlist_assets wa on wa.watchlist_id = w.id
                join public.assets a           on a.id = wa.asset_id
                where w.workspace_id = %s::uuid
                  and w.id           = %s::uuid
                  and a.is_active    = true
                order by a.symbol asc
                """,
                (workspace_id, watchlist_id),
            )
            return [dict(r)["symbol"] for r in cur.fetchall()]

    def get_active_dependency_profile(
        self, conn, *, workspace_id: str, watchlist_id: str,
    ) -> dict[str, Any] | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    id::text as id,
                    profile_name,
                    include_macro, include_fx, include_rates,
                    include_equity_index, include_commodity, include_crypto_cross,
                    max_dependencies, is_active, metadata
                from public.watchlist_dependency_profiles
                where workspace_id = %s::uuid
                  and watchlist_id = %s::uuid
                  and is_active    = true
                order by created_at desc
                limit 1
                """,
                (workspace_id, watchlist_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def expand_dependency_graph(
        self, conn, *, primary_symbols: Sequence[str], max_depth: int = _MAX_DEPTH,
    ) -> list[DependencyEdge]:
        if not primary_symbols or max_depth < 1:
            return []
        with conn.cursor() as cur:
            cur.execute(
                """
                select from_symbol, to_symbol, dependency_type, dependency_family,
                       priority, weight, metadata
                from public.asset_dependency_graph
                where is_active = true
                  and from_symbol = any(%s::text[])
                """,
                (list(primary_symbols),),
            )
            return [
                DependencyEdge(
                    from_symbol=dict(r)["from_symbol"],
                    to_symbol=dict(r)["to_symbol"],
                    dependency_type=dict(r)["dependency_type"],
                    dependency_family=dict(r)["dependency_family"],
                    priority=int(dict(r)["priority"]),
                    weight=float(dict(r)["weight"]),
                    metadata=dict(r).get("metadata") or {},
                )
                for r in cur.fetchall()
            ]

    # ── filtering / ranking ─────────────────────────────────────────────
    def apply_dependency_profile_filters(
        self,
        edges: Sequence[DependencyEdge],
        profile: dict[str, Any],
    ) -> list[DependencyEdge]:
        kept: list[DependencyEdge] = []
        for edge in edges:
            include_key = _FAMILY_TO_INCLUDE_KEY.get(edge.dependency_family)
            if include_key is None:
                # Unknown family → include; conservative default.
                kept.append(edge)
                continue
            if bool(profile.get(include_key, True)):
                kept.append(edge)
        return kept

    def rank_dependency_symbols(
        self,
        edges: Sequence[DependencyEdge],
        max_dependencies: int,
    ) -> list[DependencyEdge]:
        """Collapse to one edge per to_symbol (highest priority, then weight)
        before applying max_dependencies cap. Tie-break on (from_symbol,
        dependency_type) so order is fully deterministic."""
        by_symbol: dict[str, DependencyEdge] = {}
        for edge in sorted(
            edges,
            key=lambda e: (-e.priority, -e.weight, e.from_symbol, e.dependency_type),
        ):
            if edge.to_symbol not in by_symbol:
                by_symbol[edge.to_symbol] = edge

        ranked = sorted(
            by_symbol.values(),
            key=lambda e: (-e.priority, -e.weight, e.to_symbol),
        )
        if max_dependencies > 0:
            ranked = ranked[:max_dependencies]
        return ranked

    # ── hashing / coverage ──────────────────────────────────────────────
    def compute_context_hash(
        self,
        *,
        primary_symbols: Sequence[str],
        dependency_symbols: Sequence[str],
        profile_name: str,
        profile_controls: dict[str, Any],
        version: str = _CONTEXT_VERSION,
    ) -> str:
        # Sort everything so identical inputs produce identical hashes.
        payload = {
            "version":            version,
            "profile_name":       profile_name,
            "profile_controls":   {k: profile_controls[k] for k in sorted(profile_controls.keys())},
            "primary_symbols":    sorted(primary_symbols),
            "dependency_symbols": sorted(dependency_symbols),
        }
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def compute_coverage_summary(
        self,
        conn,
        *,
        workspace_id: str,
        dependency_symbols: Sequence[str],
    ) -> dict[str, Any]:
        if not dependency_symbols:
            return {
                "evaluated_symbols": 0,
                "covered":           0,
                "missing":           0,
                "stale":             0,
                "coverage_ratio":    None,
            }
        with conn.cursor() as cur:
            cur.execute(
                """
                with requested as (
                    select unnest(%s::text[]) as symbol
                )
                select
                    r.symbol,
                    ms.price,
                    ms.price_timestamp,
                    ms.asset_class
                from requested r
                left join public.normalized_multi_asset_market_state ms
                    on ms.workspace_id    = %s::uuid
                   and ms.canonical_symbol = r.symbol
                """,
                (list(dependency_symbols), workspace_id),
            )
            rows = [dict(r) for r in cur.fetchall()]

        now = datetime.now(timezone.utc)
        covered = missing = stale = 0
        missing_symbols: list[str] = []
        stale_symbols: list[str] = []
        for row in rows:
            if row["price"] is None:
                missing += 1
                missing_symbols.append(row["symbol"])
                continue
            ts = row["price_timestamp"]
            if ts is not None and ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            threshold_hours = 72 if row["asset_class"] in ("fx", "rates", "macro_proxy") else 48
            if ts is None:
                covered += 1
                continue
            age_hours = (now - ts).total_seconds() / 3600.0
            if age_hours > threshold_hours:
                stale += 1
                stale_symbols.append(row["symbol"])
            else:
                covered += 1

        evaluated = len(rows)
        return {
            "evaluated_symbols": evaluated,
            "covered":           covered,
            "missing":           missing,
            "stale":             stale,
            "coverage_ratio":    (covered / evaluated) if evaluated else None,
            "missing_symbols":   missing_symbols,
            "stale_symbols":     stale_symbols,
        }

    # ── persistence ─────────────────────────────────────────────────────
    def persist_context_snapshot(
        self,
        conn,
        *,
        snapshot: ContextSnapshot,
    ) -> str:
        """Insert a new snapshot row. Returns the inserted id."""
        import src.db.repositories as repo
        row = repo.insert_watchlist_context_snapshot(
            conn,
            workspace_id=snapshot.workspace_id,
            watchlist_id=snapshot.watchlist_id,
            profile_id=snapshot.profile_id,
            primary_symbols=snapshot.primary_symbols,
            dependency_symbols=snapshot.dependency_symbols,
            dependency_families=snapshot.dependency_families,
            context_hash=snapshot.context_hash,
            coverage_summary=snapshot.coverage_summary,
            metadata=snapshot.metadata,
        )
        return str(row["id"])

    # ── orchestration ───────────────────────────────────────────────────
    def build_context_snapshot(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
    ) -> ContextSnapshot:
        primary_symbols = self.get_primary_symbols(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
        )
        profile = self.get_active_dependency_profile(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
        )
        if profile is None:
            profile_id: str | None = None
            profile_name = _DEFAULT_PROFILE_NAME
            profile_controls = dict(_DEFAULT_PROFILE_CONTROLS)
        else:
            profile_id = profile["id"]
            profile_name = profile["profile_name"]
            profile_controls = {
                "include_macro":        bool(profile["include_macro"]),
                "include_fx":           bool(profile["include_fx"]),
                "include_rates":        bool(profile["include_rates"]),
                "include_equity_index": bool(profile["include_equity_index"]),
                "include_commodity":    bool(profile["include_commodity"]),
                "include_crypto_cross": bool(profile["include_crypto_cross"]),
                "max_dependencies":     int(profile["max_dependencies"]),
            }

        edges = self.expand_dependency_graph(
            conn, primary_symbols=primary_symbols, max_depth=_MAX_DEPTH,
        )
        filtered = self.apply_dependency_profile_filters(edges, profile_controls)
        ranked = self.rank_dependency_symbols(
            filtered, max_dependencies=int(profile_controls["max_dependencies"]),
        )

        dependency_symbols = [e.to_symbol for e in ranked]
        dependency_families = sorted({e.dependency_family for e in ranked})

        context_hash = self.compute_context_hash(
            primary_symbols=primary_symbols,
            dependency_symbols=dependency_symbols,
            profile_name=profile_name,
            profile_controls=profile_controls,
        )
        coverage_summary = self.compute_coverage_summary(
            conn, workspace_id=workspace_id, dependency_symbols=dependency_symbols,
        )

        metadata = {
            "profile_name":     profile_name,
            "profile_controls": profile_controls,
            "edge_count":       len(edges),
            "filtered_count":   len(filtered),
            "ranked_count":     len(ranked),
            "ranked_edges":     [
                {
                    "from":     e.from_symbol,
                    "to":       e.to_symbol,
                    "type":     e.dependency_type,
                    "family":   e.dependency_family,
                    "priority": e.priority,
                    "weight":   e.weight,
                }
                for e in ranked
            ],
            "context_version":  _CONTEXT_VERSION,
        }

        return ContextSnapshot(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            profile_id=profile_id,
            snapshot_at=datetime.now(timezone.utc),
            primary_symbols=sorted(primary_symbols),
            dependency_symbols=sorted(dependency_symbols),
            dependency_families=dependency_families,
            context_hash=context_hash,
            coverage_summary=coverage_summary,
            metadata=metadata,
        )

    def refresh_workspace_contexts(
        self, conn, *, workspace_id: str,
    ) -> list[ContextSnapshot]:
        """Build + persist a context snapshot for every active watchlist in
        the workspace. Commits per-watchlist so a single failure does not
        abort the batch."""
        import src.db.repositories as repo  # late import to avoid cycles

        persisted: list[ContextSnapshot] = []
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        for wid in watchlist_ids:
            try:
                snapshot = self.build_context_snapshot(
                    conn, workspace_id=workspace_id, watchlist_id=wid,
                )
                self.persist_context_snapshot(conn, snapshot=snapshot)
                conn.commit()
                persisted.append(snapshot)
            except Exception as exc:
                logger.warning(
                    "dependency_context: watchlist=%s build/persist failed: %s", wid, exc,
                )
                conn.rollback()
        _ = repo  # silence unused-import during late binding
        return persisted
