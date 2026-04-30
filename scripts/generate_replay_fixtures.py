"""Generate the four replay fixtures from a pinned seed.

Per docs/replay-contract.md § 4 and § 11: deterministic, one-shot, the
seed is a pinned constant in this file and copied into manifest
metadata so fixture lineage is inspectable.

Run from repo root:
  python scripts/generate_replay_fixtures.py
"""

from __future__ import annotations

import csv
import hashlib
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from _replay_lib import (
    FIXTURE_GENERATOR_VERSION,
    FIXTURE_SEED,
    FIXTURES_ROOT,
    iso_utc_ms,
    normalize_decimal,
)


# Synthetic asset identity (fixed; the fixtures don't reference live DB rows).
SYNTHETIC_BTC_UUID = "00000000-0000-0000-0000-000000000001"
SYNTHETIC_SOURCE = "synthetic_v1"


def _seeded_rng(suffix: str) -> random.Random:
    """Per-family RNG; combines FIXTURE_SEED with a suffix so each family is independent."""
    digest = hashlib.sha256(f"{FIXTURE_SEED}|{suffix}".encode("utf-8")).digest()
    seed_int = int.from_bytes(digest[:8], "big", signed=False)
    return random.Random(seed_int)


def _ensure(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _write_lf(path: Path, content: str) -> None:
    """Write text with explicit LF endings — never CRLF, regardless of OS."""
    _ensure(path)
    path.write_bytes(content.encode("utf-8"))


# ---------------------------------------------------------------------------
# market_data: 24 hourly OHLCV bars for BTC-USD, 2024-01-01 00:00..23:00 UTC.
# ---------------------------------------------------------------------------


def gen_market_data() -> Path:
    rng = _seeded_rng("market_data")
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows: list[list[str]] = []
    close = Decimal("42000")
    for i in range(24):
        ts = start + timedelta(hours=i)
        delta = Decimal(rng.randint(-300, 300))
        new_close = normalize_decimal(close + delta)
        open_ = close
        high = normalize_decimal(max(open_, new_close) + Decimal(rng.randint(0, 80)))
        low = normalize_decimal(min(open_, new_close) - Decimal(rng.randint(0, 80)))
        volume = normalize_decimal(Decimal(rng.randint(5000, 50000)) / Decimal(100))
        rows.append([iso_utc_ms(ts), str(open_), str(high), str(low), str(new_close), str(volume)])
        close = new_close

    out_path = FIXTURES_ROOT / "market_data" / "btc_1h_2024-01-01__2024-01-02.csv"
    lines = ["ts,open,high,low,close,volume"]
    lines.extend(",".join(r) for r in rows)
    _write_lf(out_path, "\n".join(lines) + "\n")

    meta_path = out_path.with_suffix(".meta.json")
    meta = {
        "asset_symbol": "BTC-USD",
        "asset_id": SYNTHETIC_BTC_UUID,
        "timeframe": "1h",
        "source": SYNTHETIC_SOURCE,
    }
    _write_lf(meta_path, json.dumps(meta, sort_keys=True, indent=2) + "\n")
    return out_path


# ---------------------------------------------------------------------------
# liquidation: ~50 events for BTC-USD over 24h, mixed sides.
# ---------------------------------------------------------------------------


def gen_liquidation() -> Path:
    rng = _seeded_rng("liquidation")
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    n_events = 50
    events: list[dict] = []
    for _ in range(n_events):
        offset_seconds = rng.randint(0, 24 * 3600 - 1)
        ts = start + timedelta(seconds=offset_seconds)
        side = rng.choice(["long", "short"])
        # log-uniform notional roughly 500..500000 USD
        notional = normalize_decimal(Decimal(10) ** Decimal(str(rng.uniform(2.7, 5.7))))
        price = normalize_decimal(Decimal(42000) + Decimal(rng.randint(-500, 500)))
        events.append({
            "asset_id": SYNTHETIC_BTC_UUID,
            "ts": iso_utc_ms(ts),
            "side": side,
            "notional_usd": str(notional),
            "reference_price": str(price),
            "source": SYNTHETIC_SOURCE,
        })

    # Sort by ts for deterministic byte layout.
    events.sort(key=lambda e: e["ts"])
    out_path = FIXTURES_ROOT / "liquidation" / "crypto_2024-01-01__2024-01-02.jsonl"
    body = "\n".join(json.dumps(e, sort_keys=True, ensure_ascii=True) for e in events) + "\n"
    _write_lf(out_path, body)
    return out_path


# ---------------------------------------------------------------------------
# sentiment: ~50 readings for BTC-USD across 3 sources over 24h.
# ---------------------------------------------------------------------------


def gen_sentiment() -> Path:
    rng = _seeded_rng("sentiment")
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    sources = ["twitter", "news", "reddit"]
    n_readings = 50
    rows: list[dict] = []
    for _ in range(n_readings):
        offset_seconds = rng.randint(0, 24 * 3600 - 1)
        ts = start + timedelta(seconds=offset_seconds)
        score = normalize_decimal(Decimal(str(rng.uniform(-1.0, 1.0))))
        sample_size = rng.randint(10, 500)
        rows.append({
            "asset_symbol": "BTC-USD",
            "ts": iso_utc_ms(ts),
            "source": rng.choice(sources),
            "score": str(score),
            "sample_size": sample_size,
        })
    rows.sort(key=lambda r: (r["ts"], r["source"]))
    out_path = FIXTURES_ROOT / "sentiment" / "crypto_news_2024-01-01__2024-01-02.jsonl"
    body = "\n".join(json.dumps(r, sort_keys=True, ensure_ascii=True) for r in rows) + "\n"
    _write_lf(out_path, body)
    return out_path


# ---------------------------------------------------------------------------
# macro: 4 indicators × 40 readings on 6h spacing → 160 rows.
# ---------------------------------------------------------------------------


def gen_macro() -> Path:
    rng = _seeded_rng("macro")
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    indicators = {
        # name: (initial_value, walk_step_decimal_str)
        "CPIAUCSL": (Decimal("310.000"), "0.5"),
        "UNRATE":   (Decimal("3.700"), "0.05"),
        "DGS10":    (Decimal("4.200"), "0.10"),
        "DXY":      (Decimal("104.000"), "0.30"),
    }
    n_readings = 40
    rows: list[list[str]] = []
    for series_key in sorted(indicators):
        value, step_str = indicators[series_key]
        step = Decimal(step_str)
        for i in range(n_readings):
            ts = start + timedelta(hours=6 * i)
            delta = (Decimal(rng.randint(-100, 100)) / Decimal(100)) * step
            value = normalize_decimal(value + delta)
            rows.append([series_key, iso_utc_ms(ts), str(value), SYNTHETIC_SOURCE])

    rows.sort(key=lambda r: (r[0], r[1]))
    out_path = FIXTURES_ROOT / "macro" / "fred_2024-01-01__2024-02-10.csv"
    lines = ["series_key,ts,value,source"]
    lines.extend(",".join(r) for r in rows)
    _write_lf(out_path, "\n".join(lines) + "\n")
    return out_path


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def main() -> int:
    print(f"[generate] {FIXTURE_GENERATOR_VERSION} seed={FIXTURE_SEED}")
    paths = [gen_market_data(), gen_liquidation(), gen_sentiment(), gen_macro()]
    for p in paths:
        print(f"  wrote {p.relative_to(FIXTURES_ROOT.parent.parent)}")
    print("[generate] done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
