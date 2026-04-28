# EMIS Project Constitution

EMIS is an asset intelligence system for cross-asset relationships, sentiment, liquidity, positioning, liquidation dynamics, market structure, and regime shifts.

## Core rule

Prefer reusable systems over one-off answers.

Every implementation should define:

- inputs
- transformations
- outputs
- persistence
- validation
- monitoring
- failure modes

## Canonical timestamps

- Store all timestamps in UTC.
- Use ISO-8601 at API boundaries.
- Use millisecond precision unless a source requires finer granularity.
- Every derived output must include:
  - observed_at
  - computed_at
  - source_freshness_ms
  - model_version

## Canonical precision

- Prices and notionals must be decimal-safe.
- Percentages are ratios internally.
- UI formatting must not leak into persisted values.

## Validation philosophy

Compilation is not validation.

Every production feature needs:

- schema validation
- repository contract validation
- service behavior validation
- deterministic replay validation
- idempotency validation
- stale-data validation
- malformed-input validation
- cross-phase regression validation

Use EMIS skills for detailed conventions, signal design, lifecycle, replay, monitoring, and review workflows.
