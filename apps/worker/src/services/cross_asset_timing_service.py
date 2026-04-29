"""Phase 4.2A: Cross-Asset Lead/Lag and Dependency Timing Service.

Measures deterministic pairwise timing between a watchlist's base symbols
and their dependency context (from 4.0B). For each (base, dep) pair, loads
short-horizon return series from market_bars (bar-backed) or
macro_series_points (macro-backed), aligns them on a common timestamp grid,
computes Pearson correlation across a small fixed lag grid, and classifies
the relationship as lead / coincident / lag / insufficient_data.

This is descriptive timing only — no forecasting, no causality claims.
All logic is deterministic; repeated runs on unchanged inputs produce
identical classifications.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Sequence

logger = logging.getLogger(__name__)

# Lag grids in hours. Hourly grid used when both sides are bar-backed;
# daily grid used whenever either side is macro-backed.
_LAG_GRID_HOURLY = (-24, -12, -6, 0, 6, 12, 24)
_LAG_GRID_DAILY  = (-48, -24, 0, 24, 48)

# History windows
_WINDOW_HOURS_HOURLY = 168   # 7 days
_WINDOW_HOURS_DAILY  = 336   # 14 days

# Classification thresholds
_MIN_OBSERVATIONS_HOURLY = 30
_MIN_OBSERVATIONS_DAILY  = 10
_MIN_STRENGTH            = 0.15  # below this, classify as insufficient_data
_COINCIDENT_LAG_HOURS    = 3     # |best_lag| <= this → coincident

_TOP_SYMBOLS_LIMIT = 5
_SCORING_VERSION   = "4.2A.v1"

_BAR_ASSET_CLASSES   = ("crypto", "equity", "index", "commodity")
_MACRO_ASSET_CLASSES = ("fx", "rates", "macro_proxy")


@dataclass
class PairTiming:
    base_symbol: str
    dependency_symbol: str
    dependency_family: str
    dependency_type: str | None
    lag_bucket: str
    best_lag_hours: int | None
    timing_strength: float | None
    correlation_at_best_lag: float | None
    base_return_series_key: str | None
    dependency_return_series_key: str | None
    window_label: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FamilyTiming:
    dependency_family: str
    lead_pair_count: int
    coincident_pair_count: int
    lag_pair_count: int
    avg_best_lag_hours: float | None
    avg_timing_strength: float | None
    dominant_timing_class: str
    top_leading_symbols: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TimingResult:
    workspace_id: str
    watchlist_id: str
    run_id: str | None
    context_snapshot_id: str | None
    pair_rows: list[PairTiming]
    family_rows: list[FamilyTiming]


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    n = len(xs)
    if n != len(ys) or n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num   = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den_x = sum((x - mx) ** 2 for x in xs)
    den_y = sum((y - my) ** 2 for y in ys)
    if den_x <= 0 or den_y <= 0:
        return None
    return num / ((den_x ** 0.5) * (den_y ** 0.5))


def _bucket_ts_hourly(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def _bucket_ts_daily(ts: datetime) -> datetime:
    return ts.replace(hour=0, minute=0, second=0, microsecond=0)


class CrossAssetTimingService:

    # ── catalog classification ──────────────────────────────────────────
    def _classify_symbols(
        self, conn, symbols: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        if not symbols:
            return {}
        with conn.cursor() as cur:
            cur.execute(
                """
                select canonical_symbol, asset_class, metadata
                from public.asset_universe_catalog
                where is_active = true
                  and canonical_symbol = any(%s::text[])
                """,
                (list(symbols),),
            )
            return {dict(r)["canonical_symbol"]: dict(r) for r in cur.fetchall()}

    # ── return-series loading ───────────────────────────────────────────
    def load_base_return_series(
        self,
        conn,
        *,
        symbol: str,
        asset_class: str | None,
        series_code: str | None,
        window_hours: int,
    ) -> tuple[list[tuple[datetime, float]], str | None]:
        """Return (list of (ts, return), series_key_used). series_key_used
        records provenance for metadata."""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=window_hours)

        if asset_class in _BAR_ASSET_CLASSES:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select b.ts as ts, b.return_1h as ret
                    from public.market_bars b
                    join public.assets a on a.id = b.asset_id
                    where a.symbol = %s
                      and b.return_1h is not null
                      and b.ts >= %s
                    order by b.ts asc
                    """,
                    (symbol, cutoff),
                )
                rows = [(r["ts"], float(r["ret"])) for r in cur.fetchall() if r["ret"] is not None]
            return rows, f"market_bars.return_1h[{symbol}]"

        if asset_class in _MACRO_ASSET_CLASSES:
            code = series_code or symbol
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select ts, coalesce(return_1d, change_1d) as ret
                    from public.macro_series_points
                    where series_code = %s
                      and coalesce(return_1d, change_1d) is not null
                      and ts >= %s
                    order by ts asc
                    """,
                    (code, cutoff),
                )
                rows = [(r["ts"], float(r["ret"])) for r in cur.fetchall() if r["ret"] is not None]
            return rows, f"macro_series_points.return_1d[{code}]"

        return [], None

    def load_dependency_return_series(
        self, conn, *, symbol: str, asset_class: str | None,
        series_code: str | None, window_hours: int,
    ) -> tuple[list[tuple[datetime, float]], str | None]:
        # Same loading strategy as base; kept as a distinct method for spec clarity.
        return self.load_base_return_series(
            conn,
            symbol=symbol,
            asset_class=asset_class,
            series_code=series_code,
            window_hours=window_hours,
        )

    # ── alignment + correlation ─────────────────────────────────────────
    @staticmethod
    def _align(
        base_series: Sequence[tuple[datetime, float]],
        dep_series:  Sequence[tuple[datetime, float]],
        *,
        daily: bool,
    ) -> tuple[dict[datetime, float], dict[datetime, float]]:
        """Bucket both series to hourly or daily granularity, taking the last
        return per bucket if multiple observations fall in one bucket."""
        bucket = _bucket_ts_daily if daily else _bucket_ts_hourly
        b_map: dict[datetime, float] = {}
        d_map: dict[datetime, float] = {}
        for ts, v in base_series:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            b_map[bucket(ts)] = v
        for ts, v in dep_series:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            d_map[bucket(ts)] = v
        return b_map, d_map

    def compute_lagged_alignment(
        self,
        base_series: Sequence[tuple[datetime, float]],
        dep_series:  Sequence[tuple[datetime, float]],
        *,
        daily: bool,
    ) -> tuple[int | None, float | None, int, dict[int, float]]:
        """Return (best_lag_hours, best_correlation, aligned_count, {lag: corr}).
        Positive lag means dep shifted forward to align with base → dep lags.
        Negative lag means dep shifted backward → dep leads."""
        b_map, d_map = self._align(base_series, dep_series, daily=daily)
        if not b_map or not d_map:
            return (None, None, 0, {})

        grid = _LAG_GRID_DAILY if daily else _LAG_GRID_HOURLY
        best_lag: int | None = None
        best_corr: float | None = None
        best_abs = -1.0
        aligned_count_best = 0
        corrs: dict[int, float] = {}

        for lag_hours in grid:
            shift = timedelta(hours=lag_hours)
            xs: list[float] = []
            ys: list[float] = []
            for ts, b_val in b_map.items():
                shifted = ts + shift
                if shifted in d_map:
                    xs.append(b_val)
                    ys.append(d_map[shifted])
            if len(xs) < 2:
                continue
            corr = _pearson(xs, ys)
            if corr is None:
                continue
            corrs[lag_hours] = corr
            if abs(corr) > best_abs:
                best_abs = abs(corr)
                best_corr = corr
                best_lag = lag_hours
                aligned_count_best = len(xs)

        return (best_lag, best_corr, aligned_count_best, corrs)

    @staticmethod
    def classify_lag_bucket(
        *,
        best_lag_hours: int | None,
        best_correlation: float | None,
        aligned_count: int,
        min_observations: int,
    ) -> str:
        if best_lag_hours is None or best_correlation is None:
            return "insufficient_data"
        if aligned_count < min_observations:
            return "insufficient_data"
        strength = abs(best_correlation)
        if strength < _MIN_STRENGTH:
            return "insufficient_data"
        if best_lag_hours < -_COINCIDENT_LAG_HOURS:
            return "lead"
        if best_lag_hours > _COINCIDENT_LAG_HOURS:
            return "lag"
        return "coincident"

    # ── pair timing snapshot ────────────────────────────────────────────
    def compute_pair_timing_snapshot(
        self,
        conn,
        *,
        base_symbol: str,
        base_classification: dict[str, Any] | None,
        dependency_symbol: str,
        dependency_classification: dict[str, Any] | None,
        dependency_family: str,
        dependency_type: str | None,
    ) -> PairTiming:
        base_class = (base_classification or {}).get("asset_class")
        dep_class  = (dependency_classification or {}).get("asset_class")
        base_series_code = (base_classification or {}).get("metadata") or {}
        if isinstance(base_series_code, str):
            import json
            try:
                base_series_code = json.loads(base_series_code)
            except json.JSONDecodeError:
                base_series_code = {}
        base_series_code = base_series_code.get("series_code") if isinstance(base_series_code, dict) else None

        dep_series_code = (dependency_classification or {}).get("metadata") or {}
        if isinstance(dep_series_code, str):
            import json
            try:
                dep_series_code = json.loads(dep_series_code)
            except json.JSONDecodeError:
                dep_series_code = {}
        dep_series_code = dep_series_code.get("series_code") if isinstance(dep_series_code, dict) else None

        # Resolution: hourly only if BOTH are bar-backed; else daily.
        daily = not (base_class in _BAR_ASSET_CLASSES and dep_class in _BAR_ASSET_CLASSES)
        window_hours = _WINDOW_HOURS_DAILY if daily else _WINDOW_HOURS_HOURLY
        min_obs = _MIN_OBSERVATIONS_DAILY if daily else _MIN_OBSERVATIONS_HOURLY
        window_label = "14d" if daily else "7d"

        base_rows, base_key = self.load_base_return_series(
            conn,
            symbol=base_symbol, asset_class=base_class,
            series_code=base_series_code, window_hours=window_hours,
        )
        dep_rows, dep_key = self.load_dependency_return_series(
            conn,
            symbol=dependency_symbol, asset_class=dep_class,
            series_code=dep_series_code, window_hours=window_hours,
        )

        best_lag, best_corr, aligned_count, corrs = self.compute_lagged_alignment(
            base_rows, dep_rows, daily=daily,
        )
        lag_bucket = self.classify_lag_bucket(
            best_lag_hours=best_lag,
            best_correlation=best_corr,
            aligned_count=aligned_count,
            min_observations=min_obs,
        )
        strength = abs(best_corr) if best_corr is not None else None

        metadata: dict[str, Any] = {
            "scoring_version":  _SCORING_VERSION,
            "effective_resolution": "daily" if daily else "hourly",
            "window_hours":     window_hours,
            "min_observations": min_obs,
            "aligned_count":    aligned_count,
            "base_sample_count": len(base_rows),
            "dep_sample_count":  len(dep_rows),
            "base_asset_class":  base_class,
            "dep_asset_class":   dep_class,
            "lag_grid":         list(_LAG_GRID_DAILY if daily else _LAG_GRID_HOURLY),
            "corrs_by_lag":     {str(k): v for k, v in corrs.items()},
        }
        return PairTiming(
            base_symbol=base_symbol,
            dependency_symbol=dependency_symbol,
            dependency_family=dependency_family,
            dependency_type=dependency_type,
            lag_bucket=lag_bucket,
            best_lag_hours=best_lag,
            timing_strength=strength,
            correlation_at_best_lag=best_corr,
            base_return_series_key=base_key,
            dependency_return_series_key=dep_key,
            window_label=window_label,
            metadata=metadata,
        )

    # ── family aggregation ──────────────────────────────────────────────
    def aggregate_family_timing(
        self, pairs: Sequence[PairTiming],
    ) -> list[FamilyTiming]:
        by_family: dict[str, list[PairTiming]] = defaultdict(list)
        for p in pairs:
            by_family[p.dependency_family].append(p)

        out: list[FamilyTiming] = []
        for family, fam_pairs in by_family.items():
            lead_n = sum(1 for p in fam_pairs if p.lag_bucket == "lead")
            coinc_n = sum(1 for p in fam_pairs if p.lag_bucket == "coincident")
            lag_n = sum(1 for p in fam_pairs if p.lag_bucket == "lag")
            counted_lags = [p.best_lag_hours for p in fam_pairs if p.best_lag_hours is not None]
            counted_strengths = [p.timing_strength for p in fam_pairs if p.timing_strength is not None]
            avg_lag = (sum(counted_lags) / len(counted_lags)) if counted_lags else None
            avg_strength = (sum(counted_strengths) / len(counted_strengths)) if counted_strengths else None

            total_classified = lead_n + coinc_n + lag_n
            if total_classified == 0:
                dominant = "insufficient_data"
            else:
                order = sorted(
                    [("lead", lead_n), ("coincident", coinc_n), ("lag", lag_n)],
                    key=lambda kv: (-kv[1], kv[0]),
                )
                dominant = order[0][0]

            leaders = sorted(
                [p for p in fam_pairs if p.lag_bucket == "lead"
                 and p.timing_strength is not None],
                key=lambda p: (-(p.timing_strength or 0.0), p.dependency_symbol),
            )
            top_leading = [p.dependency_symbol for p in leaders[:_TOP_SYMBOLS_LIMIT]]

            out.append(FamilyTiming(
                dependency_family=family,
                lead_pair_count=lead_n,
                coincident_pair_count=coinc_n,
                lag_pair_count=lag_n,
                avg_best_lag_hours=avg_lag,
                avg_timing_strength=avg_strength,
                dominant_timing_class=dominant,
                top_leading_symbols=top_leading,
                metadata={
                    "pair_count":        len(fam_pairs),
                    "scoring_version":   _SCORING_VERSION,
                    "counted_lag_n":     len(counted_lags),
                },
            ))
        return sorted(out, key=lambda f: f.dependency_family)

    def rank_leading_symbols(
        self, pairs: Sequence[PairTiming], *, limit: int = 10,
    ) -> list[str]:
        leaders = [p for p in pairs if p.lag_bucket == "lead" and p.timing_strength is not None]
        ranked = sorted(
            leaders,
            key=lambda p: (-(p.timing_strength or 0.0), p.dependency_symbol),
        )
        return [p.dependency_symbol for p in ranked[:limit]]

    # ── orchestration ───────────────────────────────────────────────────
    def build_timing_for_run(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str | None = None,
    ) -> TimingResult | None:
        # Resolve latest context snapshot for (workspace, watchlist)
        import src.db.repositories as repo
        snapshot = repo.get_latest_watchlist_context_snapshot(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
        )
        if snapshot is None:
            return None

        primary = list(snapshot.get("primary_symbols") or [])
        deps = list(snapshot.get("dependency_symbols") or [])
        if not primary:
            return None

        # Classify all symbols (base + deps)
        all_syms = sorted(set(primary) | set(deps))
        classifications = self._classify_symbols(conn, all_syms)

        # Load edge type by dependency symbol for metadata (highest-priority
        # active edge pointing to each dep, same resolution pattern as 4.1B).
        edge_type_by_symbol: dict[str, tuple[str, str | None]] = {}
        if deps:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    with ranked as (
                        select to_symbol, dependency_type, dependency_family,
                               priority, weight,
                               row_number() over (
                                   partition by to_symbol
                                   order by priority desc, weight desc, from_symbol asc
                               ) as rn
                        from public.asset_dependency_graph
                        where is_active = true
                          and to_symbol = any(%s::text[])
                    )
                    select to_symbol, dependency_type, dependency_family
                    from ranked where rn = 1
                    """,
                    (deps,),
                )
                for r in cur.fetchall():
                    d = dict(r)
                    edge_type_by_symbol[d["to_symbol"]] = (
                        d["dependency_family"],
                        d["dependency_type"],
                    )

        # Build pair rows
        pair_rows: list[PairTiming] = []
        for base_sym in primary:
            base_class = classifications.get(base_sym)
            for dep_sym in deps:
                dep_class = classifications.get(dep_sym)
                fam, dep_type = edge_type_by_symbol.get(dep_sym, ("unknown", None))
                pair = self.compute_pair_timing_snapshot(
                    conn,
                    base_symbol=base_sym,
                    base_classification=base_class,
                    dependency_symbol=dep_sym,
                    dependency_classification=dep_class,
                    dependency_family=fam,
                    dependency_type=dep_type,
                )
                pair_rows.append(pair)

        family_rows = self.aggregate_family_timing(pair_rows)

        return TimingResult(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=str(snapshot["id"]) if snapshot.get("id") else None,
            pair_rows=pair_rows,
            family_rows=family_rows,
        )

    # ── persistence ─────────────────────────────────────────────────────
    def persist_pair_timing_snapshots(
        self, conn, *, result: TimingResult,
    ) -> int:
        if not result.pair_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_lead_lag_pair_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            rows=[
                {
                    "base_symbol":                   p.base_symbol,
                    "dependency_symbol":             p.dependency_symbol,
                    "dependency_family":             p.dependency_family,
                    "dependency_type":               p.dependency_type,
                    "lag_bucket":                    p.lag_bucket,
                    "best_lag_hours":                p.best_lag_hours,
                    "timing_strength":               p.timing_strength,
                    "correlation_at_best_lag":       p.correlation_at_best_lag,
                    "base_return_series_key":        p.base_return_series_key,
                    "dependency_return_series_key":  p.dependency_return_series_key,
                    "window_label":                  p.window_label,
                    "metadata":                      p.metadata,
                }
                for p in result.pair_rows
            ],
        )

    def persist_family_timing_snapshots(
        self, conn, *, result: TimingResult,
    ) -> int:
        if not result.family_rows:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_family_timing_snapshots(
            conn,
            workspace_id=result.workspace_id,
            watchlist_id=result.watchlist_id,
            run_id=result.run_id,
            context_snapshot_id=result.context_snapshot_id,
            rows=[
                {
                    "dependency_family":     fr.dependency_family,
                    "lead_pair_count":       fr.lead_pair_count,
                    "coincident_pair_count": fr.coincident_pair_count,
                    "lag_pair_count":        fr.lag_pair_count,
                    "avg_best_lag_hours":    fr.avg_best_lag_hours,
                    "avg_timing_strength":   fr.avg_timing_strength,
                    "dominant_timing_class": fr.dominant_timing_class,
                    "top_leading_symbols":   fr.top_leading_symbols,
                    "metadata":              fr.metadata,
                }
                for fr in result.family_rows
            ],
        )

    def build_and_persist(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str | None = None,
    ) -> TimingResult | None:
        result = self.build_timing_for_run(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
        )
        if result is None:
            return None
        self.persist_pair_timing_snapshots(conn, result=result)
        self.persist_family_timing_snapshots(conn, result=result)
        return result

    def refresh_workspace_timing(
        self, conn, *, workspace_id: str, run_id: str | None = None,
    ) -> list[TimingResult]:
        """Emit timing snapshots for every watchlist in the workspace. Commits
        per-watchlist."""
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        results: list[TimingResult] = []
        for wid in watchlist_ids:
            try:
                r = self.build_and_persist(
                    conn, workspace_id=workspace_id, watchlist_id=wid, run_id=run_id,
                )
                if r is not None:
                    conn.commit()
                    results.append(r)
            except Exception as exc:
                logger.warning(
                    "cross_asset_timing: watchlist=%s build/persist failed: %s",
                    wid, exc,
                )
                conn.rollback()
        return results
