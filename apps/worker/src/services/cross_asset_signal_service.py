"""Phase 4.0C: Cross-Asset Signal Service.

Reads the latest 4.0B dependency context snapshot + 4.0A normalized market
state, builds deterministic cross-asset features, derives one signal per
(base_symbol, signal_family), and persists both. Missing/stale dependency
data is represented explicitly — values are NULL with an explanatory state,
never zero.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Sequence

logger = logging.getLogger(__name__)

# Freshness thresholds mirror 4.0B coverage semantics.
_STALE_HOURS_BAR   = 48
_STALE_HOURS_MACRO = 72

_SIGNAL_FAMILIES = (
    "risk_context",
    "macro_confirmation",
    "fx_pressure",
    "rates_pressure",
    "commodity_context",
    "cross_asset_divergence",
)

_BAR_ASSET_CLASSES   = ("crypto", "equity", "index", "commodity")
_MACRO_ASSET_CLASSES = ("fx", "rates", "macro_proxy")


@dataclass(frozen=True)
class SymbolState:
    symbol: str
    asset_class: str | None
    price: float | None
    return_value: float | None       # return_1h for bars, return_1d or change_1d for macro
    timestamp: datetime | None
    is_missing: bool
    is_stale: bool


@dataclass
class FeatureRow:
    feature_family: str
    feature_key: str
    feature_value: float | None
    feature_state: str
    dependency_symbols: list[str] = field(default_factory=list)
    dependency_families: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SignalRow:
    signal_family: str
    signal_key: str
    signal_value: float | None
    signal_direction: str | None     # bullish | bearish | neutral
    signal_state: str
    base_symbol: str | None
    dependency_symbols: list[str] = field(default_factory=list)
    dependency_families: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CrossAssetSummary:
    workspace_id: str
    watchlist_id: str
    context_snapshot_id: str | None
    feature_count: int
    signal_count: int
    confirmed_count: int
    contradicted_count: int
    missing_context_count: int
    stale_context_count: int


def _sign(x: float | None) -> int:
    if x is None:
        return 0
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _direction_from_sign(s: int) -> str:
    if s > 0:
        return "bullish"
    if s < 0:
        return "bearish"
    return "neutral"


class CrossAssetSignalService:

    # ── symbol state loading ────────────────────────────────────────────
    def load_symbol_states(
        self, conn, *, symbols: Sequence[str],
    ) -> dict[str, SymbolState]:
        """Load price + return state for a set of canonical symbols. Reads
        from market_bars (bar-backed) and macro_series_points (macro-backed);
        classification is taken from asset_universe_catalog."""
        if not symbols:
            return {}
        now = datetime.now(timezone.utc)
        out: dict[str, SymbolState] = {}

        with conn.cursor() as cur:
            # Classify symbols first
            cur.execute(
                """
                select canonical_symbol, asset_class, metadata
                from public.asset_universe_catalog
                where is_active = true
                  and canonical_symbol = any(%s::text[])
                """,
                (list(symbols),),
            )
            classifications = {
                dict(r)["canonical_symbol"]: dict(r)
                for r in cur.fetchall()
            }

        bar_symbols = [
            s for s in symbols
            if classifications.get(s, {}).get("asset_class") in _BAR_ASSET_CLASSES
        ]
        macro_lookup: dict[str, str] = {}
        for s in symbols:
            cls = classifications.get(s, {})
            if cls.get("asset_class") in _MACRO_ASSET_CLASSES:
                series_code = (cls.get("metadata") or {}).get("series_code") or s
                macro_lookup[s] = series_code

        # Bar-backed: join assets → market_bars, latest row per asset_id
        if bar_symbols:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    with latest_bar as (
                        select distinct on (b.asset_id)
                            b.asset_id, a.symbol, b.close as price,
                            b.ts as price_timestamp, b.return_1h as return_value
                        from public.market_bars b
                        join public.assets a on a.id = b.asset_id
                        where a.symbol = any(%s::text[])
                        order by b.asset_id, b.ts desc
                    )
                    select symbol, price, price_timestamp, return_value
                    from latest_bar
                    """,
                    (bar_symbols,),
                )
                for row in cur.fetchall():
                    r = dict(row)
                    ts = r["price_timestamp"]
                    if ts is not None and ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    age_h = ((now - ts).total_seconds() / 3600.0) if ts else None
                    is_stale = (age_h is not None and age_h > _STALE_HOURS_BAR)
                    out[r["symbol"]] = SymbolState(
                        symbol=r["symbol"],
                        asset_class=classifications[r["symbol"]]["asset_class"],
                        price=float(r["price"]) if r["price"] is not None else None,
                        return_value=(float(r["return_value"]) if r["return_value"] is not None else None),
                        timestamp=ts,
                        is_missing=(r["price"] is None),
                        is_stale=is_stale,
                    )

        # Macro-backed: latest point per series_code
        if macro_lookup:
            codes = list({c for c in macro_lookup.values()})
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select distinct on (series_code)
                        series_code, value as price, ts as price_timestamp,
                        return_1d, change_1d
                    from public.macro_series_points
                    where series_code = any(%s::text[])
                    order by series_code, ts desc
                    """,
                    (codes,),
                )
                code_rows = {dict(r)["series_code"]: dict(r) for r in cur.fetchall()}

            for symbol, code in macro_lookup.items():
                row = code_rows.get(code)
                if row is None:
                    out[symbol] = SymbolState(
                        symbol=symbol,
                        asset_class=classifications[symbol]["asset_class"],
                        price=None, return_value=None, timestamp=None,
                        is_missing=True, is_stale=False,
                    )
                    continue
                ts = row["price_timestamp"]
                if ts is not None and ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age_h = ((now - ts).total_seconds() / 3600.0) if ts else None
                is_stale = (age_h is not None and age_h > _STALE_HOURS_MACRO)
                ret = row.get("return_1d")
                if ret is None:
                    ret = row.get("change_1d")
                out[symbol] = SymbolState(
                    symbol=symbol,
                    asset_class=classifications[symbol]["asset_class"],
                    price=float(row["price"]) if row["price"] is not None else None,
                    return_value=float(ret) if ret is not None else None,
                    timestamp=ts,
                    is_missing=(row["price"] is None),
                    is_stale=is_stale,
                )

        # Any symbols we couldn't classify → mark missing
        for s in symbols:
            if s not in out:
                out[s] = SymbolState(
                    symbol=s, asset_class=None, price=None, return_value=None,
                    timestamp=None, is_missing=True, is_stale=False,
                )
        return out

    # ── feature builders ────────────────────────────────────────────────
    def build_risk_context_features(
        self, base: SymbolState, dep: dict[str, SymbolState],
    ) -> list[FeatureRow]:
        features: list[FeatureRow] = []
        spy = dep.get("SPY")
        qqq = dep.get("QQQ")
        used = [s for s in ("SPY", "QQQ") if dep.get(s) is not None]

        # risk_proxy_alignment: mean of sign-products between base and each index
        signs: list[int] = []
        stale_hits = 0
        if base.return_value is not None and not base.is_missing:
            for idx_state in (spy, qqq):
                if idx_state is None or idx_state.is_missing or idx_state.return_value is None:
                    continue
                if idx_state.is_stale:
                    stale_hits += 1
                signs.append(_sign(base.return_value) * _sign(idx_state.return_value))

        if base.return_value is None:
            features.append(FeatureRow(
                "risk_context", "risk_proxy_alignment",
                None, "insufficient_context",
                used, ["risk"], {"reason": "base_return_missing"},
            ))
        elif not signs:
            features.append(FeatureRow(
                "risk_context", "risk_proxy_alignment",
                None, "missing_dependency",
                used, ["risk"], {"reason": "no_equity_index_returns"},
            ))
        else:
            val = sum(signs) / len(signs)
            state = "stale_dependency" if stale_hits == len(signs) and stale_hits > 0 else "computed"
            features.append(FeatureRow(
                "risk_context", "risk_proxy_alignment",
                val, state, used, ["risk"],
                {"signs": signs, "stale_dependency_count": stale_hits},
            ))

        # equity_index_confirmation_score: confirmation ratio [0, 1]
        if signs:
            confirm = sum(1 for s in signs if s > 0) / len(signs)
            features.append(FeatureRow(
                "risk_context", "equity_index_confirmation_score",
                confirm, "computed", used, ["risk"],
                {"confirm_ratio": confirm},
            ))
        else:
            features.append(FeatureRow(
                "risk_context", "equity_index_confirmation_score",
                None,
                "missing_dependency" if (base.return_value is not None) else "insufficient_context",
                used, ["risk"],
            ))
        return features

    def build_macro_confirmation_features(
        self, base: SymbolState, dep: dict[str, SymbolState],
    ) -> list[FeatureRow]:
        features: list[FeatureRow] = []
        us10y = dep.get("US10Y")
        spread = dep.get("2S10S")
        dxy = dep.get("DXY")
        used = [s for s in ("US10Y", "2S10S", "DXY") if dep.get(s) is not None]

        # yield_pressure_confirmation: risk assets benefit when yields fall
        if base.return_value is None:
            features.append(FeatureRow(
                "macro_confirmation", "yield_pressure_confirmation",
                None, "insufficient_context", used, ["rates", "macro"],
            ))
        elif us10y is None or us10y.return_value is None or us10y.is_missing:
            features.append(FeatureRow(
                "macro_confirmation", "yield_pressure_confirmation",
                None, "missing_dependency", used, ["rates", "macro"],
            ))
        else:
            val = _sign(base.return_value) * -_sign(us10y.return_value)
            state = "stale_dependency" if us10y.is_stale else "computed"
            features.append(FeatureRow(
                "macro_confirmation", "yield_pressure_confirmation",
                float(val), state, used, ["rates", "macro"],
                {"us10y_return": us10y.return_value},
            ))

        # macro_backdrop_agreement: average of yield and dollar agreement
        components: list[float] = []
        if base.return_value is not None:
            if us10y and not us10y.is_missing and us10y.return_value is not None:
                components.append(_sign(base.return_value) * -_sign(us10y.return_value))
            if dxy and not dxy.is_missing and dxy.return_value is not None:
                components.append(_sign(base.return_value) * -_sign(dxy.return_value))
        if not components:
            features.append(FeatureRow(
                "macro_confirmation", "macro_backdrop_agreement",
                None,
                "missing_dependency" if base.return_value is not None else "insufficient_context",
                used, ["rates", "macro", "fx"],
            ))
        else:
            features.append(FeatureRow(
                "macro_confirmation", "macro_backdrop_agreement",
                sum(components) / len(components),
                "computed", used, ["rates", "macro", "fx"],
                {"components": components},
            ))

        # spread_regime_alignment: positive spread change → normalizing curve
        if spread is None or spread.return_value is None or spread.is_missing:
            features.append(FeatureRow(
                "macro_confirmation", "spread_regime_alignment",
                None, "missing_dependency", used, ["macro"],
            ))
        elif base.return_value is None:
            features.append(FeatureRow(
                "macro_confirmation", "spread_regime_alignment",
                None, "insufficient_context", used, ["macro"],
            ))
        else:
            val = _sign(base.return_value) * _sign(spread.return_value)
            state = "stale_dependency" if spread.is_stale else "computed"
            features.append(FeatureRow(
                "macro_confirmation", "spread_regime_alignment",
                float(val), state, used, ["macro"],
            ))
        return features

    def build_fx_pressure_features(
        self, base: SymbolState, dep: dict[str, SymbolState],
    ) -> list[FeatureRow]:
        features: list[FeatureRow] = []
        dxy = dep.get("DXY")
        eurusd = dep.get("EURUSD")
        used = [s for s in ("DXY", "EURUSD") if dep.get(s) is not None]

        # dollar_pressure_score: risk assets inverse to dollar strength
        if base.return_value is None:
            features.append(FeatureRow(
                "fx_pressure", "dollar_pressure_score",
                None, "insufficient_context", used, ["fx"],
            ))
        elif dxy is None or dxy.return_value is None or dxy.is_missing:
            features.append(FeatureRow(
                "fx_pressure", "dollar_pressure_score",
                None, "missing_dependency", used, ["fx"],
            ))
        else:
            val = _sign(base.return_value) * -_sign(dxy.return_value)
            state = "stale_dependency" if dxy.is_stale else "computed"
            features.append(FeatureRow(
                "fx_pressure", "dollar_pressure_score",
                float(val), state, used, ["fx"],
                {"dxy_return": dxy.return_value},
            ))

        # fx_risk_alignment: EURUSD direction should align with risk-on
        if eurusd is None or eurusd.return_value is None or eurusd.is_missing:
            features.append(FeatureRow(
                "fx_pressure", "fx_risk_alignment",
                None, "missing_dependency", used, ["fx"],
            ))
        elif base.return_value is None:
            features.append(FeatureRow(
                "fx_pressure", "fx_risk_alignment",
                None, "insufficient_context", used, ["fx"],
            ))
        else:
            val = _sign(base.return_value) * _sign(eurusd.return_value)
            state = "stale_dependency" if eurusd.is_stale else "computed"
            features.append(FeatureRow(
                "fx_pressure", "fx_risk_alignment",
                float(val), state, used, ["fx"],
            ))
        return features

    def build_rates_pressure_features(
        self, base: SymbolState, dep: dict[str, SymbolState],
    ) -> list[FeatureRow]:
        features: list[FeatureRow] = []
        us10y = dep.get("US10Y")
        us02y = dep.get("US02Y")
        spread = dep.get("2S10S")
        used = [s for s in ("US10Y", "US02Y", "2S10S") if dep.get(s) is not None]

        # rates_pressure_score: magnitude of combined yield moves
        components: list[float] = []
        if us10y and not us10y.is_missing and us10y.return_value is not None:
            components.append(us10y.return_value)
        if us02y and not us02y.is_missing and us02y.return_value is not None:
            components.append(us02y.return_value)
        if not components:
            features.append(FeatureRow(
                "rates_pressure", "rates_pressure_score",
                None, "missing_dependency", used, ["rates"],
            ))
        else:
            avg = sum(components) / len(components)
            features.append(FeatureRow(
                "rates_pressure", "rates_pressure_score",
                float(avg), "computed", used, ["rates"],
                {"components": components},
            ))

        # curve_signal_alignment: spread direction vs base direction
        if spread is None or spread.return_value is None or spread.is_missing:
            features.append(FeatureRow(
                "rates_pressure", "curve_signal_alignment",
                None, "missing_dependency", used, ["rates"],
            ))
        elif base.return_value is None:
            features.append(FeatureRow(
                "rates_pressure", "curve_signal_alignment",
                None, "insufficient_context", used, ["rates"],
            ))
        else:
            features.append(FeatureRow(
                "rates_pressure", "curve_signal_alignment",
                float(_sign(base.return_value) * _sign(spread.return_value)),
                "stale_dependency" if spread.is_stale else "computed",
                used, ["rates"],
            ))
        return features

    def build_commodity_context_features(
        self, base: SymbolState, dep: dict[str, SymbolState],
    ) -> list[FeatureRow]:
        features: list[FeatureRow] = []
        gld = dep.get("GLD")
        uso = dep.get("USO")
        used = [s for s in ("GLD", "USO") if dep.get(s) is not None]

        # gold_risk_divergence: gold up with base up = low divergence; opposite = high divergence
        if gld is None or gld.return_value is None or gld.is_missing:
            features.append(FeatureRow(
                "commodity_context", "gold_risk_divergence",
                None, "missing_dependency", used, ["commodity"],
            ))
        elif base.return_value is None:
            features.append(FeatureRow(
                "commodity_context", "gold_risk_divergence",
                None, "insufficient_context", used, ["commodity"],
            ))
        else:
            divergence = abs(_sign(base.return_value) - _sign(gld.return_value)) / 2.0
            features.append(FeatureRow(
                "commodity_context", "gold_risk_divergence",
                float(divergence),
                "stale_dependency" if gld.is_stale else "computed",
                used, ["commodity"],
            ))

        # commodity_confirmation_score: fraction of commodity peers confirming direction
        if base.return_value is None:
            features.append(FeatureRow(
                "commodity_context", "commodity_confirmation_score",
                None, "insufficient_context", used, ["commodity"],
            ))
        else:
            signs = []
            for c in (gld, uso):
                if c is None or c.is_missing or c.return_value is None:
                    continue
                signs.append(_sign(base.return_value) * _sign(c.return_value))
            if not signs:
                features.append(FeatureRow(
                    "commodity_context", "commodity_confirmation_score",
                    None, "missing_dependency", used, ["commodity"],
                ))
            else:
                confirm_ratio = sum(1 for s in signs if s > 0) / len(signs)
                features.append(FeatureRow(
                    "commodity_context", "commodity_confirmation_score",
                    float(confirm_ratio), "computed", used, ["commodity"],
                ))
        return features

    def build_cross_asset_divergence_features(
        self, base: SymbolState, dep: dict[str, SymbolState],
    ) -> list[FeatureRow]:
        features: list[FeatureRow] = []
        present = {s: st for s, st in dep.items()
                   if st is not None and not st.is_missing and st.return_value is not None}

        if base.return_value is None:
            features.append(FeatureRow(
                "cross_asset_divergence", "base_vs_dependency_divergence",
                None, "insufficient_context", list(present.keys()),
                ["equity_index", "rates", "fx", "commodity"],
            ))
            features.append(FeatureRow(
                "cross_asset_divergence", "family_dispersion_score",
                None, "insufficient_context", list(present.keys()),
                ["equity_index", "rates", "fx", "commodity"],
            ))
            return features

        if not present:
            features.append(FeatureRow(
                "cross_asset_divergence", "base_vs_dependency_divergence",
                None, "missing_dependency", [],
                ["equity_index", "rates", "fx", "commodity"],
            ))
            features.append(FeatureRow(
                "cross_asset_divergence", "family_dispersion_score",
                None, "missing_dependency", [],
                ["equity_index", "rates", "fx", "commodity"],
            ))
            return features

        base_sign = _sign(base.return_value)
        # base_vs_dependency_divergence: fraction of deps whose sign differs from base
        diff_count = sum(
            1 for st in present.values()
            if _sign(st.return_value or 0) != base_sign
        )
        divergence = diff_count / len(present)
        features.append(FeatureRow(
            "cross_asset_divergence", "base_vs_dependency_divergence",
            float(divergence), "computed", list(present.keys()),
            ["equity_index", "rates", "fx", "commodity"],
            {"dep_count": len(present), "diff_count": diff_count},
        ))

        # family_dispersion_score: stdev proxy of dep sign values
        dep_signs = [_sign(st.return_value or 0) for st in present.values()]
        if len(dep_signs) <= 1:
            dispersion = 0.0
        else:
            mean = sum(dep_signs) / len(dep_signs)
            variance = sum((s - mean) ** 2 for s in dep_signs) / len(dep_signs)
            dispersion = variance ** 0.5
        features.append(FeatureRow(
            "cross_asset_divergence", "family_dispersion_score",
            float(dispersion), "computed", list(present.keys()),
            ["equity_index", "rates", "fx", "commodity"],
        ))
        return features

    # ── signal derivation ───────────────────────────────────────────────
    def derive_cross_asset_signals(
        self,
        base_symbol: str,
        base: SymbolState,
        features: Sequence[FeatureRow],
    ) -> list[SignalRow]:
        """Aggregate features within each family into one signal per
        (base_symbol, family). Signal state captures the confirmation/
        contradiction relationship against base direction."""
        signals: list[SignalRow] = []
        base_direction_sign = _sign(base.return_value) if base.return_value is not None else 0

        for family in _SIGNAL_FAMILIES:
            fam_features = [f for f in features if f.feature_family == family]
            if not fam_features:
                continue

            # If every feature in the family is missing/insufficient → missing_context
            if all(f.feature_state in ("missing_dependency", "insufficient_context") for f in fam_features):
                combined_symbols = sorted({
                    s for f in fam_features for s in f.dependency_symbols
                })
                combined_families = sorted({
                    fam for f in fam_features for fam in f.dependency_families
                })
                signals.append(SignalRow(
                    signal_family=family,
                    signal_key=f"{family}_signal",
                    signal_value=None,
                    signal_direction=None,
                    signal_state="missing_context",
                    base_symbol=base_symbol,
                    dependency_symbols=combined_symbols,
                    dependency_families=combined_families,
                    metadata={"feature_count": len(fam_features)},
                ))
                continue

            # If every computed feature in family is stale → stale_context
            computed_or_stale = [f for f in fam_features if f.feature_value is not None]
            if computed_or_stale and all(f.feature_state == "stale_dependency" for f in computed_or_stale):
                combined_symbols = sorted({
                    s for f in computed_or_stale for s in f.dependency_symbols
                })
                combined_families = sorted({
                    fam for f in computed_or_stale for fam in f.dependency_families
                })
                signals.append(SignalRow(
                    signal_family=family,
                    signal_key=f"{family}_signal",
                    signal_value=None,
                    signal_direction=None,
                    signal_state="stale_context",
                    base_symbol=base_symbol,
                    dependency_symbols=combined_symbols,
                    dependency_families=combined_families,
                    metadata={"feature_count": len(fam_features)},
                ))
                continue

            # Aggregate numeric: average of computed features' values
            numeric_vals = [f.feature_value for f in fam_features if f.feature_value is not None]
            if not numeric_vals:
                state = "missing_context"
                value: float | None = None
                direction: str | None = None
            else:
                value = sum(numeric_vals) / len(numeric_vals)
                # Direction is the sign of aggregate value when not pure "divergence" family
                if family == "cross_asset_divergence":
                    # Higher divergence = neutral/contradicted, low = confirmed
                    if value >= 0.5:
                        state = "contradicted"
                    elif value <= 0.25:
                        state = "confirmed"
                    else:
                        state = "unconfirmed"
                    direction = None
                else:
                    if value > 0.25:
                        state = "confirmed"
                        direction = _direction_from_sign(base_direction_sign) if base_direction_sign != 0 else "neutral"
                    elif value < -0.25:
                        state = "contradicted"
                        direction = _direction_from_sign(-base_direction_sign) if base_direction_sign != 0 else "neutral"
                    else:
                        state = "unconfirmed"
                        direction = "neutral"

            combined_symbols = sorted({
                s for f in fam_features for s in f.dependency_symbols
            })
            combined_families = sorted({
                fam for f in fam_features for fam in f.dependency_families
            })
            signals.append(SignalRow(
                signal_family=family,
                signal_key=f"{family}_signal",
                signal_value=value,
                signal_direction=direction,
                signal_state=state,
                base_symbol=base_symbol,
                dependency_symbols=combined_symbols,
                dependency_families=combined_families,
                metadata={
                    "aggregated_feature_count": len(fam_features),
                    "computed_feature_count":   len(numeric_vals),
                    "base_direction_sign":      base_direction_sign,
                },
            ))
        return signals

    # ── persistence ─────────────────────────────────────────────────────
    def persist_cross_asset_features(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str | None,
        context_snapshot_id: str | None,
        features: Sequence[FeatureRow],
    ) -> int:
        if not features:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_feature_snapshots(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snapshot_id,
            rows=[
                {
                    "feature_family":      f.feature_family,
                    "feature_key":         f.feature_key,
                    "feature_value":       f.feature_value,
                    "feature_state":       f.feature_state,
                    "dependency_symbols":  f.dependency_symbols,
                    "dependency_families": f.dependency_families,
                    "metadata":            f.metadata,
                }
                for f in features
            ],
        )

    def persist_cross_asset_signals(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str | None,
        context_snapshot_id: str | None,
        signals: Sequence[SignalRow],
    ) -> int:
        if not signals:
            return 0
        import src.db.repositories as repo
        return repo.insert_cross_asset_signal_snapshots(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=context_snapshot_id,
            rows=[
                {
                    "signal_family":       s.signal_family,
                    "signal_key":          s.signal_key,
                    "signal_value":        s.signal_value,
                    "signal_direction":    s.signal_direction,
                    "signal_state":        s.signal_state,
                    "base_symbol":         s.base_symbol,
                    "dependency_symbols":  s.dependency_symbols,
                    "dependency_families": s.dependency_families,
                    "metadata":            s.metadata,
                }
                for s in signals
            ],
        )

    # ── orchestration ───────────────────────────────────────────────────
    def build_and_persist_for_watchlist(
        self,
        conn,
        *,
        workspace_id: str,
        watchlist_id: str,
        run_id: str | None = None,
    ) -> CrossAssetSummary | None:
        import src.db.repositories as repo

        snapshot = repo.get_latest_watchlist_context_snapshot(
            conn, workspace_id=workspace_id, watchlist_id=watchlist_id,
        )
        if snapshot is None:
            return None

        primary_symbols = list(snapshot.get("primary_symbols") or [])
        dependency_symbols = list(snapshot.get("dependency_symbols") or [])
        if not primary_symbols:
            return CrossAssetSummary(
                workspace_id=workspace_id,
                watchlist_id=watchlist_id,
                context_snapshot_id=snapshot["id"],
                feature_count=0, signal_count=0,
                confirmed_count=0, contradicted_count=0,
                missing_context_count=0, stale_context_count=0,
            )

        all_symbols = sorted(set(primary_symbols) | set(dependency_symbols))
        states = self.load_symbol_states(conn, symbols=all_symbols)
        dep_states = {s: states[s] for s in dependency_symbols if s in states}

        all_features: list[FeatureRow] = []
        all_signals: list[SignalRow] = []
        for base_symbol in primary_symbols:
            base = states.get(base_symbol)
            if base is None:
                base = SymbolState(
                    symbol=base_symbol, asset_class=None, price=None, return_value=None,
                    timestamp=None, is_missing=True, is_stale=False,
                )
            features = (
                self.build_risk_context_features(base, dep_states)
                + self.build_macro_confirmation_features(base, dep_states)
                + self.build_fx_pressure_features(base, dep_states)
                + self.build_rates_pressure_features(base, dep_states)
                + self.build_commodity_context_features(base, dep_states)
                + self.build_cross_asset_divergence_features(base, dep_states)
            )
            for f in features:
                f.metadata.setdefault("base_symbol", base_symbol)
            all_features.extend(features)
            all_signals.extend(self.derive_cross_asset_signals(base_symbol, base, features))

        feature_count = self.persist_cross_asset_features(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=snapshot["id"],
            features=all_features,
        )
        signal_count = self.persist_cross_asset_signals(
            conn,
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            run_id=run_id,
            context_snapshot_id=snapshot["id"],
            signals=all_signals,
        )
        return CrossAssetSummary(
            workspace_id=workspace_id,
            watchlist_id=watchlist_id,
            context_snapshot_id=snapshot["id"],
            feature_count=feature_count,
            signal_count=signal_count,
            confirmed_count=sum(1 for s in all_signals if s.signal_state == "confirmed"),
            contradicted_count=sum(1 for s in all_signals if s.signal_state == "contradicted"),
            missing_context_count=sum(1 for s in all_signals if s.signal_state == "missing_context"),
            stale_context_count=sum(1 for s in all_signals if s.signal_state == "stale_context"),
        )

    def refresh_workspace_cross_asset_signals(
        self, conn, *, workspace_id: str, run_id: str | None = None,
    ) -> list[CrossAssetSummary]:
        """Run cross-asset feature/signal build for every watchlist in the
        workspace that has a recent context snapshot. Commits per-watchlist."""
        persisted: list[CrossAssetSummary] = []
        with conn.cursor() as cur:
            cur.execute(
                "select id::text as id from public.watchlists where workspace_id = %s::uuid",
                (workspace_id,),
            )
            watchlist_ids = [dict(r)["id"] for r in cur.fetchall()]

        for wid in watchlist_ids:
            try:
                summary = self.build_and_persist_for_watchlist(
                    conn,
                    workspace_id=workspace_id,
                    watchlist_id=wid,
                    run_id=run_id,
                )
                if summary is not None:
                    conn.commit()
                    persisted.append(summary)
            except Exception as exc:
                logger.warning(
                    "cross_asset_signal: watchlist=%s build/persist failed: %s", wid, exc,
                )
                conn.rollback()
        return persisted

    def summarize_cross_asset_state(
        self, summaries: Sequence[CrossAssetSummary]
    ) -> dict[str, Any]:
        if not summaries:
            return {
                "watchlist_count": 0,
                "feature_count":   0,
                "signal_count":    0,
                "confirmed":       0,
                "contradicted":    0,
                "missing_context": 0,
                "stale_context":   0,
            }
        return {
            "watchlist_count": len(summaries),
            "feature_count":   sum(s.feature_count for s in summaries),
            "signal_count":    sum(s.signal_count for s in summaries),
            "confirmed":       sum(s.confirmed_count for s in summaries),
            "contradicted":    sum(s.contradicted_count for s in summaries),
            "missing_context": sum(s.missing_context_count for s in summaries),
            "stale_context":   sum(s.stale_context_count for s in summaries),
        }
