"""Replay-validation library: helpers + transforms.

Implements the contract documented in docs/replay-contract.md:
  § 4 — manifest schema
  § 5 — per-family fixture contracts and transforms
  § 6 — hash computation rules
  § 7 — transform registry

Pure stdlib. No external dependencies.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any, Callable, Iterable

# ---------------------------------------------------------------------------
# Versioning constants (§ 4)
# ---------------------------------------------------------------------------

VALIDATOR_VERSION = "validate_replay@1.0.0"
FIXTURE_GENERATOR_VERSION = "generate_replay_fixtures@1.0.0"
FIXTURE_SEED = "emis-replay-v1:2026-04-29"

# ---------------------------------------------------------------------------
# Decimals + timestamps (§ 6.4, § 6.5)
# ---------------------------------------------------------------------------

DECIMAL_PRECISION = Decimal("0.000000000001")  # 12 places
ISO_FMT_NO_FRAC = "%Y-%m-%dT%H:%M:%S"


def normalize_decimal(d: Decimal | float | int | str) -> Decimal:
    """Quantize to 12 decimal places, banker's rounding."""
    if not isinstance(d, Decimal):
        d = Decimal(str(d))
    return d.quantize(DECIMAL_PRECISION, rounding=ROUND_HALF_EVEN)


def iso_utc_ms(dt: datetime) -> str:
    """Serialize a tz-aware datetime as ISO-8601 UTC with ms precision and trailing Z."""
    if dt.tzinfo is None:
        raise ValueError("naive timestamp not allowed; replay requires UTC tz-aware")
    dt = dt.astimezone(timezone.utc)
    return dt.strftime(ISO_FMT_NO_FRAC) + f".{dt.microsecond // 1000:03d}Z"


def parse_iso_utc(s: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp; reject naive or non-UTC."""
    if not (s.endswith("Z") or s.endswith("+00:00")):
        raise ValueError(f"non-UTC timestamp (expected Z or +00:00): {s!r}")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        raise ValueError(f"parsed naive timestamp: {s!r}")
    return dt


# ---------------------------------------------------------------------------
# Canonical JSON + hashes (§ 6.2, § 6.3)
# ---------------------------------------------------------------------------


def _json_default(o: Any) -> Any:
    if isinstance(o, Decimal):
        return str(normalize_decimal(o))
    if isinstance(o, datetime):
        return iso_utc_ms(o)
    raise TypeError(f"unserializable type: {type(o).__name__}")


def canonical_json(obj: Any) -> bytes:
    """UTF-8, ASCII-escaped, sorted keys, zero whitespace, trailing decimals stable."""
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=_json_default,
    ).encode("utf-8")


def pretty_json(obj: Any) -> str:
    """Pretty-printed JSON for golden files. Sorted keys; readable diffs."""
    return json.dumps(
        obj,
        sort_keys=True,
        indent=2,
        ensure_ascii=True,
        default=_json_default,
    )


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_sha256_hex(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_hash(obj: Any) -> str:
    return sha256_hex(canonical_json(obj))


# ---------------------------------------------------------------------------
# Fixture readers (§ 5)
# ---------------------------------------------------------------------------


def read_csv(path: Path) -> list[dict[str, str]]:
    """Read a CSV file with headers; return list of row dicts (string values)."""
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read a JSON Lines file; each line is one object."""
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                out.append(json.loads(stripped))
            except json.JSONDecodeError as e:
                raise ValueError(f"malformed JSON in {path}:{lineno}: {e}") from e
    return out


# ---------------------------------------------------------------------------
# Per-family transforms (§ 5)
# ---------------------------------------------------------------------------


def _validate_market_data_row(r: dict[str, str], path: Path, lineno: int) -> None:
    required = {"ts", "open", "high", "low", "close", "volume"}
    missing = required - r.keys()
    if missing:
        raise ValueError(f"market_data row missing fields {sorted(missing)} at {path}:{lineno}")
    parse_iso_utc(r["ts"])  # raises on naive/non-UTC


def compute_btc_1h_sma5(rows: list[dict[str, str]], path: Path) -> list[dict[str, Any]]:
    """5-bar simple moving average of close, emitted for bars 5..N (one per bar)."""
    for i, r in enumerate(rows, 1):
        _validate_market_data_row(r, path, i)
    sorted_rows = sorted(rows, key=lambda r: parse_iso_utc(r["ts"]))
    if len(sorted_rows) < 5:
        return []
    closes = [Decimal(r["close"]) for r in sorted_rows]
    out: list[dict[str, Any]] = []
    for i in range(4, len(closes)):
        window = closes[i - 4:i + 1]
        sma = normalize_decimal(sum(window) / Decimal(5))
        out.append({"ts": sorted_rows[i]["ts"], "sma5": str(sma)})
    return out


def _validate_liquidation_event(ev: dict[str, Any], path: Path, idx: int) -> None:
    required = {"asset_id", "ts", "side", "notional_usd", "reference_price", "source"}
    missing = required - ev.keys()
    if missing:
        raise ValueError(f"liquidation event {idx} missing fields {sorted(missing)} in {path}")
    if ev["side"] not in {"long", "short"}:
        raise ValueError(f"liquidation event {idx}: invalid side {ev['side']!r} in {path}")
    parse_iso_utc(ev["ts"])


def compute_hourly_liq_side_notional(events: list[dict[str, Any]], path: Path) -> list[dict[str, Any]]:
    """Hourly aggregate notional by side."""
    for i, ev in enumerate(events, 1):
        _validate_liquidation_event(ev, path, i)
    buckets: dict[tuple[str, str], Decimal] = defaultdict(lambda: Decimal(0))
    for ev in events:
        ts = parse_iso_utc(ev["ts"])
        hour = ts.replace(minute=0, second=0, microsecond=0)
        buckets[(iso_utc_ms(hour), ev["side"])] += Decimal(str(ev["notional_usd"]))
    return [
        {"hour_start_ts": h, "side": side, "total_notional_usd": str(normalize_decimal(total))}
        for (h, side), total in sorted(buckets.items())
    ]


def _validate_sentiment_row(r: dict[str, Any], path: Path, idx: int) -> None:
    required = {"asset_symbol", "ts", "source", "score", "sample_size"}
    missing = required - r.keys()
    if missing:
        raise ValueError(f"sentiment row {idx} missing fields {sorted(missing)} in {path}")
    parse_iso_utc(r["ts"])


def compute_source_weighted_hourly_sentiment(rows: list[dict[str, Any]], path: Path) -> list[dict[str, Any]]:
    """Source-weighted hourly aggregate sentiment, weight = sqrt(sample_size)."""
    for i, r in enumerate(rows, 1):
        _validate_sentiment_row(r, path, i)
    # bucket by (hour, asset_symbol)
    weighted: dict[tuple[str, str], tuple[Decimal, Decimal]] = defaultdict(
        lambda: (Decimal(0), Decimal(0))  # (sum w*s, sum w)
    )
    for r in rows:
        ts = parse_iso_utc(r["ts"])
        hour_str = iso_utc_ms(ts.replace(minute=0, second=0, microsecond=0))
        sample_size = int(r["sample_size"])
        if sample_size <= 0:
            continue
        # sqrt of int → use Decimal-friendly approximation via Decimal.sqrt()
        weight = Decimal(sample_size).sqrt()
        score = Decimal(str(r["score"]))
        ws, w = weighted[(hour_str, r["asset_symbol"])]
        weighted[(hour_str, r["asset_symbol"])] = (ws + weight * score, w + weight)
    out: list[dict[str, Any]] = []
    for (h, sym), (ws, w) in sorted(weighted.items()):
        if w == 0:
            continue
        agg = normalize_decimal(ws / w)
        out.append({"hour_start_ts": h, "asset_symbol": sym, "weighted_score": str(agg)})
    return out


def _validate_macro_row(r: dict[str, str], path: Path, idx: int) -> None:
    required = {"series_key", "ts", "value", "source"}
    missing = required - r.keys()
    if missing:
        raise ValueError(f"macro row {idx} missing fields {sorted(missing)} in {path}")
    parse_iso_utc(r["ts"])


def compute_indicator_zscore_30(rows: list[dict[str, str]], path: Path) -> list[dict[str, Any]]:
    """Per-series z-score against a rolling 30-reading baseline. Emits one row per (series, ts) for readings 31..N per series."""
    for i, r in enumerate(rows, 1):
        _validate_macro_row(r, path, i)
    # group by series, sorted by ts
    by_series: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        by_series[r["series_key"]].append(r)
    for series in by_series.values():
        series.sort(key=lambda r: parse_iso_utc(r["ts"]))
    out: list[dict[str, Any]] = []
    for series_key in sorted(by_series.keys()):
        series = by_series[series_key]
        values = [Decimal(r["value"]) for r in series]
        for i in range(30, len(values)):
            window = values[i - 30:i]
            mean = sum(window) / Decimal(30)
            # population std (Decimal-compatible)
            var_sum = sum((v - mean) ** 2 for v in window)
            variance = var_sum / Decimal(30)
            std = variance.sqrt()
            current = values[i]
            if std == 0:
                z = Decimal(0)
            else:
                z = normalize_decimal((current - mean) / std)
            out.append({
                "series_key": series_key,
                "ts": series[i]["ts"],
                "zscore": str(z),
            })
    return out


# ---------------------------------------------------------------------------
# Transform registry (§ 7.1)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TransformSpec:
    target: str
    family: str
    feature_version: str
    model_version: str
    schema_version: str
    fixture_relpath: str            # path relative to tests/fixtures/replay/
    fixture_format: str             # "csv" | "jsonl"
    callable: Callable[..., list[dict[str, Any]]]
    config: dict[str, Any]          # frozen config object that drives the transform


REPLAY_TRANSFORMS: dict[str, TransformSpec] = {
    "market_data:btc_1h_sma5": TransformSpec(
        target="market_data:btc_1h_sma5",
        family="market_data",
        feature_version="sma5@1.0.0",
        model_version="n/a",
        schema_version="market_data_fixture@1.0.0",
        fixture_relpath="market_data/btc_1h_2024-01-01__2024-01-02.csv",
        fixture_format="csv",
        callable=compute_btc_1h_sma5,
        config={"window": 5, "field": "close"},
    ),
    "liquidation:btc_hourly_side_notional": TransformSpec(
        target="liquidation:btc_hourly_side_notional",
        family="liquidation",
        feature_version="hourly_liq_side_notional@1.0.0",
        model_version="n/a",
        schema_version="liquidation_fixture@1.0.0",
        fixture_relpath="liquidation/crypto_2024-01-01__2024-01-02.jsonl",
        fixture_format="jsonl",
        callable=compute_hourly_liq_side_notional,
        config={"bucket": "hour", "group_by": "side"},
    ),
    "sentiment:btc_source_weighted_hourly": TransformSpec(
        target="sentiment:btc_source_weighted_hourly",
        family="sentiment",
        feature_version="source_weighted_hourly@1.0.0",
        model_version="n/a",
        schema_version="sentiment_fixture@1.0.0",
        fixture_relpath="sentiment/crypto_news_2024-01-01__2024-01-02.jsonl",
        fixture_format="jsonl",
        callable=compute_source_weighted_hourly_sentiment,
        config={"weight": "sqrt(sample_size)", "bucket": "hour"},
    ),
    "macro:indicator_zscore_30": TransformSpec(
        target="macro:indicator_zscore_30",
        family="macro",
        feature_version="indicator_zscore_30@1.0.0",
        model_version="n/a",
        schema_version="macro_fixture@1.0.0",
        fixture_relpath="macro/fred_2024-01-01__2024-02-10.csv",
        fixture_format="csv",
        callable=compute_indicator_zscore_30,
        config={"window": 30, "method": "rolling_population_zscore"},
    ),
}


# ---------------------------------------------------------------------------
# Path layout (§ 3)
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES_ROOT = REPO_ROOT / "tests" / "fixtures" / "replay"
GOLDEN_ROOT = FIXTURES_ROOT / "__golden__"
MANIFEST_PATH = FIXTURES_ROOT / "manifest.json"


def fixture_path(spec: TransformSpec) -> Path:
    return FIXTURES_ROOT / spec.fixture_relpath


def golden_path(spec: TransformSpec) -> Path:
    safe_target = re.sub(r"[^A-Za-z0-9_]+", "__", spec.target)
    return GOLDEN_ROOT / f"{safe_target}.json"
