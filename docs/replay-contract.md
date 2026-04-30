# EMIS Replay Validation — Design Contract

**Status:** Decisions locked 2026-04-29 (see § 12). Ready for implementation per the build sequence in § 11.
**Owner:** TBD.
**Skill reference:** `emis-replay`.

This document defines the replay-validation layer the pre-commit hook
enforces. It is the contract that fixtures, transforms, manifests, and
the validator script must collectively satisfy. Implementation is a
separate, scoped follow-up — this is *only* the agreed shape.

## 1. Purpose and scope

### What this layer does

Recompute deterministic outputs from frozen raw fixtures and assert that
the produced output hash matches the manifest. Any drift — caused by a
feature code change, a config change, a schema change, a library
upgrade, a non-determinism leak — fails the gate.

### What this layer does NOT do

- It is not point-in-time backfill (use `emis-backfill`).
- It is not a live-DB integration smoke (the existing
  `apps/worker/src/scripts/validate_phase*.py` are those — they hit
  Postgres via `DATABASE_URL` and are excluded from this contract).
- It is not a property-based or fuzz tester.
- It is not a regression test against historical vendor data — only
  frozen fixtures.

## 2. Invariants

1. Same fixture + same config + same schema version + same feature
   version + same model version → same `outputHash`. Bit-for-bit.
2. Replay is idempotent: running the validator twice in a row produces
   the same result and writes nothing externally observable.
3. All timestamps are UTC, ISO-8601, millisecond precision (per
   `.claude/CLAUDE.md`). No local timezones in fixtures, manifests, or
   outputs.
4. Numeric outputs are decimal-safe. No float drift across runs.
5. Source freshness is deterministic under a frozen clock. Validators
   never read system clock for output computation.
6. Empty fixtures produce a stable empty output (with a stable hash).
7. Malformed fixtures fail safely with a structured error — they never
   produce a misleading "success" hash.
8. Phase regression: phase N golden output is preserved unless the
   manifest explicitly bumps `featureVersion` or `modelVersion` and a
   migration note is added at `audit/replay-migrations.md`.

## 3. File layout

```
tests/fixtures/replay/
  manifest.json                         # array of ReplayManifest entries
  market_data/
    btc_1h_2024-01-01__2024-01-02.csv
    eth_1h_2024-01-01__2024-01-02.csv
  liquidation/
    crypto_2024-01-01__2024-01-02.jsonl
  sentiment/
    crypto_news_2024-01-01__2024-01-02.jsonl
  macro/
    fred_2024-01-01__2024-01-31.csv
scripts/
  validate_replay.py                    # detected by the pre-commit hook
audit/
  replay-migrations.md                  # append-only log of golden bumps
```

CSV vs JSONL choice per family is a fixture-design call (see § 5),
locked once and not changed without a migration note.

## 4. Manifest schema

`tests/fixtures/replay/manifest.json` is a JSON array. Each entry:

```ts
type ReplayManifest = {
  target: string                    // "market_data:btc_1h_sma5"; family:transform key
  replayWindowStart: string         // ISO-8601 UTC, ms precision
  replayWindowEnd: string           // ISO-8601 UTC, ms precision
  rawInputHash: string              // sha256 hex of the raw fixture bytes
  configHash: string                // sha256 hex of the canonical config JSON
  schemaVersion: string             // family-scoped, e.g. "market_data_fixture@1.0.0"
  featureVersion: string            // "sma5@1.0.0" — semver of the transform
  modelVersion: string              // semver of the model; "n/a" for non-model features
  outputHash: string                // sha256 hex of the canonical output bytes
  validatorVersion: string          // "validate_replay@1.0.0"
  fixtureGeneratorVersion: string   // "generate_replay_fixtures@1.0.0"
  fixtureSeed: string               // "emis-replay-v1:2026-04-29" (constant in the generator)
  createdAt: string                 // ISO-8601 UTC, ms precision
};
```

### Rules

- The manifest is written by the validator (or a sibling tool) once,
  then committed. Re-running the validator must reproduce every hash
  exactly.
- Adding a new fixture appends an entry; the validator fails on entries
  that lack a corresponding fixture.
- Changing `featureVersion` or `modelVersion` requires an explanatory
  line in `audit/replay-migrations.md`.

## 5. Per-family fixture contracts

For each family below: input schema, fixture format, sample size for
MVP, and the deterministic transform that produces the validated
output. The transforms here are illustrative starting points — final
choice should align with existing feature/signal code in
`apps/worker/src/features/` and `apps/worker/src/signals/` (audit before
locking).

### 5.1 `market_data`

**Source table:** `market_bars` (created in migration 0001).

| Column | Type | Notes |
|---|---|---|
| `asset_id` | uuid | FK to `assets` |
| `timeframe` | text | e.g. `1h` |
| `ts` | timestamptz | UTC |
| `open`, `high`, `low`, `close` | numeric | decimal-safe |
| `volume` | numeric, nullable |  |
| `source` | text |  |

**Fixture format:** CSV with headers exactly `ts,open,high,low,close,volume`,
plus a sidecar JSON file (`<name>.meta.json`) containing
`asset_symbol`, `timeframe`, `source`. Asset and source live outside
the CSV so the same OHLCV stream can be reused under different metadata
without duplicating bytes.

**Sample size:** 24 bars (one full day of 1h) per asset, 1–2 assets for
MVP.

**Output transform (illustrative):** 5-bar simple moving average of
`close`, emitted as `{ts, sma5}` rows for bars 5..N. Sufficient to catch
indexing bugs, off-by-one, lookahead leakage.

### 5.2 `liquidation`

**Source table:** `market_liquidations` (created in migration 0001).

**Locked fixture schema:**

| Column | Type | Notes |
|---|---|---|
| `asset_id` | uuid |  |
| `ts` | timestamptz | event time, UTC |
| `side` | text | `long` \| `short` |
| `notional_usd` | numeric | USD-equivalent notional |
| `reference_price` | numeric |  |
| `source` | text |  |

**Fixture-writer alias mapping** (illustrative draft names → locked
column names — generators must emit the locked names):

```
size_usd  → notional_usd
price     → reference_price
```

**Audit item, do not block:** migration 0024 creates an idempotency
index referring to `liquidation_notional_1h` while the base
`market_liquidations` table from 0001 has `notional_usd`. Confirm
against the live migration history before locking the production
schema. The replay fixture follows the base 0001 event-table shape.

**Fixture format:** JSONL (one event per line), naturally sparse.

**Sample size:** ~50 events over a 24h window for one asset.

**Output transform (target `liquidation:btc_hourly_side_notional`):**
Hourly aggregate notional by side,
`{hour_start_ts, side, total_notional_usd}`.

### 5.3 `sentiment`

**Source table:** none yet. No raw sentiment table is present in the
current schema. Sentiment replay is a **synthetic external fixture
family** for MVP — it does not correspond to a DB table, only to a
file-based fixture. Future-table candidate: `sentiment_observations`.

**Locked fixture schema** (file-only, not DB-mapped):

| Column | Type | Notes |
|---|---|---|
| `asset_symbol` | text | not asset_id, since no FK is enforced |
| `ts` | timestamptz | UTC |
| `source` | text |  |
| `score` | numeric | normalized to [-1, 1] |
| `sample_size` | int | number of underlying observations |

**Fixture format:** JSONL.

**Sample size:** ~50 readings across 2–3 sources over 24h for one
asset.

**Output transform (target `sentiment:btc_source_weighted_hourly`):**
Source-weighted hourly aggregate, weight = `sqrt(sample_size)`.

**On promotion to a real table:** when a vendor or source is onboarded
and a `sentiment_observations` table is created, this fixture's column
names should be reviewed against the table and a fixture-schema
migration noted in `audit/replay-migrations.md`.

### 5.4 `macro`

**Source table:** `macro_series_points` (created in migration 0001).

**Locked fixture schema:**

| Column | Type | Notes |
|---|---|---|
| `series_key` | text | e.g. `CPIAUCSL`, `UNRATE`, `DGS10`, `DXY` |
| `ts` | timestamptz | release/observation time, UTC |
| `value` | numeric |  |
| `source` | text |  |

**Audit item, do not block:** migration 0061 (multi-asset normalized
view) refers to `macro_series_points.series_code` through the catalog
metadata path, while the base 0001 schema uses `series_key`. Confirm
whether a later migration aliases or renames the column before locking
the production schema. The replay fixture follows the base 0001 column
name (`series_key`).

**Fixture format:** CSV.

**Sample size:** ~20 readings across 3–5 indicators over a month.

**Output transform (target `macro:indicator_zscore_30`):**
Indicator-level z-score against the rolling-30-reading baseline within
the fixture window.

## 6. Hash computation rules

These rules ensure determinism across machines, OSes, library versions,
and Python/Node interpreter changes.

### 6.1 `rawInputHash`

`sha256` of the fixture file bytes as written to disk. Line endings are
LF (enforced via `.gitattributes`; see § 11). Trailing-newline policy:
exactly one newline at end of file.

### 6.2 `configHash`

`sha256` of the canonical-JSON-serialized config object that drives the
transform. Canonical-JSON rules:

- UTF-8.
- Keys sorted ASCII-lexicographically at every level.
- No whitespace except a single newline between top-level entries when
  serializing for diff readability — but the *hashed* form is the
  zero-whitespace `JSON.stringify` / `json.dumps(separators=(",",":"))`
  output.
- Numbers serialized in their canonical decimal form (no
  scientific notation, no trailing zeros). For Python: use `Decimal` and
  `json.dumps(..., default=str)` with explicit normalization.
- `null` for absent values; never omit a key that the schema declares.

### 6.3 `outputHash`

The transform produces output rows. Output is serialized to canonical
JSON (rules above), then `sha256`-hashed. The serialized output is also
written to `tests/fixtures/replay/<family>/__golden__/<target>.json` and
committed — visible diffs catch behavior changes immediately.

### 6.4 Timestamps

Always serialized as ISO-8601 UTC with millisecond precision and a
trailing `Z`: `2024-01-01T00:00:00.000Z`. Never `+00:00`. Never naive.

### 6.5 Decimals

All numeric outputs are `Decimal` in Python / `BigNumber`-string in
Node. Never floats. Precision: 12 decimal places, banker's rounding.
Serialized as JSON strings to preserve precision (`"0.123456789012"`).

## 7. Validator behavior

`scripts/validate_replay.py` is the MVP runtime — Python only. The
worker is Python and the existing feature/signal code lives there. A TS
mirror is explicitly deferred (see § 10).

### 7.1 Transform registry

Production rows currently emit `feature_name`, `timestamp`, `value`,
`meta` (and signals emit `signal_name`, `timestamp`, `score`,
`explanation`) — neither carries an explicit `feature_version` or
`model_version` column. Adding those columns is a future migration; the
replay layer **does not** wait for it. Instead, the validator owns
versioning via a registry:

```python
# scripts/validate_replay.py (sketch)

REPLAY_TRANSFORMS = {
    "market_data:btc_1h_sma5": {
        "feature_version": "sma5@1.0.0",
        "model_version": "n/a",
        "schema_version": "market_data_fixture@1.0.0",
        "callable": compute_btc_1h_sma5,
    },
    "liquidation:btc_hourly_side_notional": {
        "feature_version": "hourly_liq_side_notional@1.0.0",
        "model_version": "n/a",
        "schema_version": "liquidation_fixture@1.0.0",
        "callable": compute_hourly_liq_side_notional,
    },
    "sentiment:btc_source_weighted_hourly": {
        "feature_version": "source_weighted_hourly@1.0.0",
        "model_version": "n/a",
        "schema_version": "sentiment_fixture@1.0.0",
        "callable": compute_source_weighted_hourly_sentiment,
    },
    "macro:indicator_zscore_30": {
        "feature_version": "indicator_zscore_30@1.0.0",
        "model_version": "n/a",
        "schema_version": "macro_fixture@1.0.0",
        "callable": compute_indicator_zscore_30,
    },
}
```

The registry is the version authority for replay. When a transform is
intentionally changed, bump `feature_version` (or `model_version`) in
the registry, regenerate the manifest's golden, and add an entry to
`audit/replay-migrations.md` (§ 9). Production-row column additions can
follow later without disturbing replay.

### 7.2 CLI

```
Usage: python scripts/validate_replay.py [--target <key>] [--update-golden]

Default: validate every entry in manifest.json. Exits non-zero on the
first failure (with --strict-all, runs all entries and reports summary).

  --target <key>      Validate only the given target (e.g., market_data:btc_1h_sma5)
  --update-golden     Recompute and rewrite hashes/golden files. CI rejects this flag.
```

For each manifest entry the validator:

1. Loads the fixture file, asserts `sha256` matches `rawInputHash`.
2. Loads the config for `target`, asserts `sha256` of canonical form
   matches `configHash`.
3. Asserts `schemaVersion` matches the current schema version
   declared in code.
4. Runs the transform identified by `target` (key → callable lookup
   in a transform registry).
5. Serializes output to canonical JSON, asserts `sha256` matches
   `outputHash`.
6. Diffs serialized output against `__golden__/<target>.json` —
   they must be byte-equal.

Exit codes:

| Code | Meaning |
|---|---|
| `0` | All entries passed |
| `1` | Hash mismatch (drift detected) |
| `2` | Fixture missing or malformed |
| `3` | Config or schema version mismatch |
| `4` | Transform not registered for target |

## 8. Required checks (from the skill spec)

These translate to concrete tests in `apps/worker/tests/replay/`:

| Check | Test |
|---|---|
| Same fixture → same output hash | Run validator twice, assert identical reports |
| Replay is idempotent | Validator writes no observable side effects (no DB, no network) |
| UTC normalization | Fixture with naive timestamp → fail with code 2 |
| Frozen-clock determinism | Mock `datetime.now()` → output unchanged |
| Malformed fixture fails safely | Fixture with extra column / wrong type → exit 2 |
| Empty fixture → stable empty output | Empty CSV (header only) → empty output array, hash of empty array, no error |
| Phase N preserves N-1 golden | CI loads previous golden file and re-validates against current code |

## 9. Phase regression rule

The pre-commit hook calls the validator on every commit. CI runs it on
every PR. A `featureVersion` or `modelVersion` bump that changes
`outputHash` requires:

1. A row in `audit/replay-migrations.md` (table, append-only):

   ```markdown
   | Date | Target | Old Version | New Version | Old Hash | New Hash | Reason | Owner |
   |---|---|---|---|---|---|---|---|
   | 2026-04-29 | market_data:btc_1h_sma5 | sma5@1.0.0 | sma5@1.1.0 | abc... | def... | Fixed off-by-one window initialization | D |
   ```

   Required columns: `Date`, `Target`, `Old Version`, `New Version`,
   `Old Hash`, `New Hash`, `Reason`, `Owner`. Hashes truncated to 12
   chars in the table; full hashes live in the manifest.

2. The corresponding `__golden__/<target>.json` updated in the same
   commit, with the diff visible in PR review.

3. The PR description links the migration row.

A bump that *doesn't* change `outputHash` is a no-op and should be
flagged in review (likely indicates a stale version bump).

## 10. Out of scope for MVP

Explicitly deferred:

- Property-based / fuzz testing of transforms.
- Cross-language replay (Python validator + TS reproduction).
- Vendor-data ingestion for fixtures (we use synthetic + audit-trail).
- Multi-asset / multi-timeframe coverage matrices — MVP is one asset
  per family.
- Performance benchmarks.
- Snapshotting full feature pipelines — MVP validates a single
  transform per family.

## 11. Build sequence

When implementation begins, in this order:

1. **Schemas are locked** in § 5 (per the appendix decisions). Two
   audit items remain — the `liquidation_notional_1h` reference in
   migration 0024 and the `series_code` vs `series_key` divergence in
   migration 0061 — to be confirmed against the live migration history
   before the production schema is locked. Neither blocks the fixture
   work.
2. **Pick the four transforms** in `apps/worker/src/features/` or
   `apps/worker/src/signals/` (or write stubs to match the registry in
   § 7.1). Production code currently lacks `feature_version` /
   `model_version` columns; the registry is the version authority for
   replay until a future migration adds them.
3. **Write `.gitattributes`** to enforce LF endings on fixture files.
4. **Write fixture-generation script**
   `scripts/generate_replay_fixtures.py` — one-shot, deterministic,
   seeded RNG via the constants `FIXTURE_GENERATOR_VERSION` and
   `FIXTURE_SEED` (see § 4). Produces the four fixture files.
5. **Write the validator** (`scripts/validate_replay.py`) with the
   transform registry in § 7.1.
6. **Generate manifest** by running the validator with `--update-golden`,
   inspecting diffs, committing.
7. **Wire the required checks** as `apps/worker/tests/replay/test_*.py`.
8. **Verify the hook is now happy** with a `git commit --allow-empty`
   smoke test.
9. **Open PR for review.** This is the moment the hard gate becomes
   real for everyone.

## 12. Locked decisions (2026-04-29)

| # | Question | Decision |
|---|---|---|
| 1 | Existing feature versioning? | None. Add at the replay layer first via the transform registry (§ 7.1). Production-row column additions come later as their own migration. |
| 2 | Liquidation table | `market_liquidations` (created in migration 0001). Fixture columns: `asset_id, ts, side, notional_usd, reference_price, source`. |
| 2 | Macro table | `macro_series_points` (created in migration 0001). Fixture columns: `series_key, ts, value, source`. |
| 2 | Sentiment table | None yet. Sentiment is a synthetic external fixture family for MVP; future-table candidate `sentiment_observations`. |
| 3 | Validator runtime | Python only for MVP. TS mirror deferred. |
| 4 | Seeded RNG | Constants in `scripts/generate_replay_fixtures.py` (`FIXTURE_GENERATOR_VERSION`, `FIXTURE_SEED`); copied into manifest metadata. |
| 5 | Audit log path | `audit/replay-migrations.md`, table format defined in § 9. |

### Audit items, do not block fixture work

These should be confirmed against the live migration history before
the **production** schema is locked, but the **replay fixture** schema
follows the base 0001 column names regardless:

- Migration 0024 references `liquidation_notional_1h` while base
  `market_liquidations` has `notional_usd`. Confirm whether 0024
  introduces a derived view/column or whether the reference is stale.
- Migration 0061 (multi-asset normalized view) refers to
  `macro_series_points.series_code` while 0001 defines `series_key`.
  Confirm whether a later migration aliases or renames the column.
