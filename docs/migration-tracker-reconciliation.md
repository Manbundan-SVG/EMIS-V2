# Migration tracker reconciliation

## What broke

`supabase db push` against the dev database fails with:

```
Remote migration versions not found in local migrations directory.
Make sure your local git repo is up-to-date. If the error persists, try repairing the migration history table:
supabase migration repair --status reverted 20260426212724 ... 20260427234641
```

The remote `supabase_migrations.schema_migrations` table holds 34 entries with **14-digit timestamp version IDs** (e.g., `20260426212724`). Our local migrations under `supabase/migrations/` use **4-digit sequential IDs** (`0001` through `NNNN`). The CLI compares the two and refuses to push because the version sets don't intersect.

This happened because earlier migrations were applied through a path (Supabase dashboard SQL editor or a one-off CLI invocation against an unrelated repo state) that wrote timestamp-format IDs into the tracker even though our actual repo uses sequential IDs.

## Fix — `scripts/reconcile_migration_tracker.py`

Aligns the remote tracker to our existing sequential naming. **Does not** rename any local files. **Does not** re-run any migration SQL. Only mutates `supabase_migrations.schema_migrations`.

What it does:

1. **DELETE** every row whose `version` matches the legacy `^[0-9]{14}$` pattern.
2. **UPSERT** one row per local migration `(version, name)`, *except* the latest one — which is held back so a subsequent `supabase db push` will apply it.

The held-back migration defaults to the highest-numbered local file. Override with `--hold-back NNNN`.

## Procedure

### Prerequisites

- `DATABASE_URL` set in your shell (Session Pooler URI from Supabase dashboard, real password substituted in).
- `asyncpg` installed (`pip install asyncpg`).
- Be on a branch where `supabase/migrations/` contains every migration you want the tracker to know about (e.g., the feature branch with the new migration).

### Step 1 — dry run

Inspect the current state and the planned changes without mutating anything:

```bash
python scripts/reconcile_migration_tracker.py
```

You should see:

```
[reconcile] local migrations: 96
[reconcile] holding back:     0096
[reconcile] will mark applied: 95
[reconcile] mode:             DRY-RUN

[before] tracker has 34 rows total:
  legacy 14-digit timestamps: 34
  sequential 4-digit (ours):  0
  other formats:              0
```

### Step 2 — apply

```bash
python scripts/reconcile_migration_tracker.py --apply
```

Runs inside a single transaction. If anything fails, the entire reconciliation rolls back. After success:

```
[after] tracker has 95 rows total:
  legacy 14-digit timestamps: 0
  sequential 4-digit (ours):  95
  other formats:              0

[reconcile] OK — tracker is reconciled.
[reconcile] Next: run `supabase db push` to apply migration 0096.
```

### Step 3 — push the held-back migration

```bash
supabase db push --db-url "$SUPABASE_DB_URL_DEV"
```

`db push` should now see only `0096` as un-applied and push it. Then it appends `0096` to the tracker on success.

### Step 4 — verify

```bash
python apps/worker/src/scripts/validate_phase48d.py
```

## Going forward

After this reconciliation, the standard flow works again:

```bash
pnpm db:migrate:apply -- --file supabase/migrations/NNNN_<name>.sql --env dev --apply
```

The wrapper invokes `supabase db push --db-url <url>`, which now finds the tracker in a clean state and applies new migrations correctly.

## Recovery

If something looks wrong after `--apply`:

- The script ran inside a transaction, so partial writes are not possible.
- To restore the previous (broken) tracker state: not advised — the legacy 14-digit IDs were already broken. If you absolutely need them back, you'd `INSERT` them by hand from the timestamps printed in the original CLI error.
- A safer "undo" is no-op: leave the reconciled state and let future migrations append normally.

## Audit

Every reconciliation run:

- Prints the tracker state before and after.
- Reports the number of legacy rows deleted and sequential rows upserted.
- Validates post-conditions (no legacy rows remaining, hold-back not in tracker, count matches).

Run output can be piped to a log file for audit:

```bash
python scripts/reconcile_migration_tracker.py --apply 2>&1 | Tee-Object reconciliation_$(date +%Y%m%d).log
```
