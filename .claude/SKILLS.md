# EMIS Claude Skills Index

## Commit 1 — Determinism Foundation

| Skill | Version | Purpose |
|---|---:|---|
| emis-conventions | 4.2.0 | Canonical EMIS vocabulary, taxonomy, runtime, secret handling, default gates, timestamp and precision rules |
| emis-signal-design | 4.2.0 | Design deployable market signals from raw ideas |
| emis-cross-asset-lead-lag | 4.2.0 | Model asset relationships, transmission paths, and lag windows |
| emis-backfill | 4.2.0 | Build historical datasets without look-ahead leakage |
| emis-replay | 4.2.0 | Recompute frozen inputs deterministically |
| emis-validation-harness | 4.2.0 | Build deterministic validators and acceptance gates |
| emis-liquidation-ingestion | 4.2.0 | Normalize liquidation, OI, leverage, and heatmap data |

## Commit 2 — Operating Layer (pending)

| Skill | Purpose |
|---|---|
| emis-signal-lifecycle | Promote, monitor, degrade, rollback, retire signals |
| emis-signal-calibration | Calibrate confidence scores against realized outcomes |
| emis-monitoring | Design alerts, routing, runbooks, staleness checks |
| emis-data-source-onboarding | Add vendors, exchanges, APIs, schemas, source health checks |
| emis-universe-management | Manage asset universe tiers and onboarding |
| emis-pr-review | Review branches and PRs for EMIS production readiness |

## Pending agents and hooks (Commit 2)

- `.claude/agents/security-reviewer.md`
- `.claude/agents/schema-reviewer.md`
- `.claude/hooks/block-unsafe-migrations.sh`

## First integration commit (after Commit 1)

Replaces bootstrap mode with real fixture gate:

1. `tests/fixtures/replay/market_data/<real_fixture>.json`
2. `tests/fixtures/replay/liquidation/<real_fixture>.json`
3. `tests/fixtures/replay/sentiment/<real_fixture>.json`
4. `tests/fixtures/replay/macro/<real_fixture>.json`
5. `scripts/validate_replay.ts`
6. `scripts/validate_fixtures.ts`
7. `scripts/validate_phase.ts`
8. ReplayManifest seed
9. Package scripts: `validate:replay`, `validate:fixtures`, `validate:phase`
