# EMIS Replay Validation — Design Contract

**Status:** Draft for review.
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
  target: string              // "market_data:btc_1h_sma5"; family:transform key
  replayWindowStart: string   // ISO-8601 UTC, ms precision
  replayWindowEnd: string     // ISO-8601 UTC, ms precision
  rawInputHash: string        // sha256 hex of the raw fixture bytes
  configHash: string          // sha256 hex of the canonical config JSON
  schemaVersion: string       // e.g., "v1"
  featureVersion: string      // "sma5@1.0.0" — semver of the transform
  modelVersion: string        // semver of the model; "n/a" for non-model features
  outputHash: string          // sha256 hex of the canonical output bytes
  validatorVersion: string    // "validate_replay@1.0.0"
  createdAt: string           // ISO-8601 UTC, ms precision
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

**Source schema** (matches `market_bars` table from migration 0001):

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

**Source schema** (look for the relevant table — likely added in a
phase-2.7 or phase-4.x migration; lock during implementation):

| Column | Type | Notes |
|---|---|---|
| `asset_id` | uuid |  |
| `ts` | timestamptz | event time, UTC |
| `side` | text | `long` \| `short` |
| `size_usd` | numeric | USD-equivalent notional |
| `price` | numeric |  |
| `source` | text |  |

**Fixture format:** JSONL (one event per line), naturally sparse.

**Sample size:** ~50 events over a 24h window for one asset.

**Output transform (illustrative):** Hourly liquidation volume by side,
`{hour_start_ts, side, total_size_usd}`.

### 5.3 `sentiment`

**Source schema** (lock during implementation — search migrations for
`sentiment` table):

| Column | Type | Notes |
|---|---|---|
| `asset_id` | uuid |  |
| `ts` | timestamptz |  |
| `score` | numeric | normalized to [-1, 1] |
| `source` | text |  |
| `sample_size` | int | number of underlying observations |

**Fixture format:** JSONL.

**Sample size:** ~50 readings across 2–3 sources over 24h for one
asset.

**Output transform (illustrative):** Source-weighted hourly sentiment
aggregate, weight = `sqrt(sample_size)`.

### 5.4 `macro`

**Source schema** (likely from `0025_phase2_7_macro_provider_path.sql`
and `0027`/`0028`; lock during implementation):

| Column | Type | Notes |
|---|---|---|
| `indicator` | text | e.g. `CPIAUCSL`, `UNRATE`, `DGS10`, `DXY` |
| `ts` | timestamptz | release/observation time, UTC |
| `value` | numeric |  |
| `source` | text |  |

**Fixture format:** CSV.

**Sample size:** ~20 readings across 3–5 indicators over a month.

**Output transform (illustrative):** Indicator-level z-score against
the rolling-30-reading baseline within the fixture window.

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

`scripts/validate_replay.py` (Python is the natural choice given the
worker stack; a TS variant can mirror it later if needed):

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

1. A line in `audit/replay-migrations.md`:
   `2026-04-29 — market_data:btc_1h_sma5 — sma5@1.0.0 → sma5@1.1.0 — fixed off-by-one in window initialization — confirmed unintended behavior in v1.0.0`
2. The corresponding `__golden__/<target>.json` updated in the same
   commit, with the diff visible in PR review.

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

1. **Lock schemas.** Audit the relevant migrations for `market_bars`,
   the liquidation table, the sentiment table, the macro indicator
   table; record the field list and version.
2. **Pick transforms.** Pick one existing feature per family from
   `apps/worker/src/features/` or `apps/worker/src/signals/` and lock
   its version. If no obvious candidate exists, write a stub feature
   per the illustrative transforms in § 5.
3. **Write `.gitattributes`** to enforce LF endings on fixture files.
4. **Write fixture-generation script** (`scripts/generate_fixtures.py`,
   one-shot, deterministic, seeded RNG) that produces the four fixture
   files.
5. **Write the validator** (`scripts/validate_replay.py`).
6. **Generate manifest** by running the validator with `--update-golden`,
   inspecting diffs, committing.
7. **Wire the required checks** as `apps/worker/tests/replay/test_*.py`.
8. **Verify the hook is now happy** with a `git commit --allow-empty`
   smoke test.
9. **Open PR for review.** This is the moment the hard gate becomes
   real for everyone.

## 12. Open questions for review

1. Do existing features in `apps/worker/src/features/` have explicit
   versioning? If not, we need to add it as part of this work.
2. Do liquidation / sentiment tables exist in the current schema, and
   under what names? (Migrations 0024–0028 and 0061–0063 are likely
   sites — confirm before locking § 5.)
3. Should the validator be Python only, or do we want a TS variant for
   parity with the deploy gate?
4. Where does the fixture-generation script's seeded RNG state live?
   (Suggest: pinned in the script as a constant; same script always
   generates same fixtures.)
5. Audit log location: `audit/replay-migrations.md` is a fresh path —
   confirm or relocate.
