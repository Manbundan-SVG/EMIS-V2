# Deploying to Supabase

**Always use `scripts/safe_apply_migration.ts` to apply migrations.** Direct paths bypass validation and leave no audit trail:

- Do not call `mcp__*__apply_migration` directly against prod.
- Do not call `supabase db push` directly — it skips git/sequence checks.
- Do not call `mcp__*__execute_sql` for schema changes — no migration file means no reproducibility.

## Apply a migration

```bash
# Dry-run validation (default — safe to run anytime)
pnpm db:migrate:apply -- --file supabase/migrations/0095_foo.sql --env dev

# Apply to dev
pnpm db:migrate:apply -- --file supabase/migrations/0095_foo.sql --env dev --apply

# Apply to prod (requires --confirm-prod)
pnpm db:migrate:apply -- --file supabase/migrations/0095_foo.sql --env prod --apply --confirm-prod
```

`pnpm db:migrate:apply` resolves to `tsx scripts/safe_apply_migration.ts`. Passing `--` forwards remaining args.

## What the wrapper enforces

1. File path matches `supabase/migrations/NNNN_<name>.sql`.
2. File is tracked by git with no uncommitted changes — git is the source of truth.
3. Sequence number is exactly `max(existing) + 1` — no gaps, no duplicates.
4. On `--env prod`, fails on destructive patterns (`DROP TABLE`, `DROP SCHEMA`, `TRUNCATE`, `ALTER ROLE`, `DROP ROLE`) unless `--allow-destructive` is passed.
5. `--env prod` with `--apply` requires `--confirm-prod`.
6. Records every attempt (dry-run, applied, failed) in `audit/deployments.jsonl`.

## Required env vars

| Var | Purpose |
|---|---|
| `SUPABASE_PROJECT_REF_DEV` | Dev project ref |
| `SUPABASE_PROJECT_REF_STAGING` | Staging project ref |
| `SUPABASE_PROJECT_REF_PROD` | Prod project ref |
| `SUPABASE_ACCESS_TOKEN` | Read by the supabase CLI |

The wrapper requires only the env var matching the target.

## Audit log: `audit/deployments.jsonl`

JSON Lines. One entry per attempt. Fields:

| Field | Meaning |
|---|---|
| `ts` | When the wrapper ran (ISO-8601 UTC, ms precision) |
| `env` | Target environment |
| `artifact_kind` | `migration` |
| `artifact` | Path of the migration file |
| `git_sha` | HEAD SHA at apply time |
| `git_branch` | Current branch |
| `git_user` | `git config user.email` |
| `applied_at` | When the apply completed (null on dry-run/failure) |
| `status` | `applied` \| `dry-run` \| `failed` |
| `message` | Failure reason (only on `failed`) |

**Commit the audit log after every successful deploy.** The trail is part of the repo.

## Edge functions

`scripts/safe_deploy_function.ts` follows the same pattern. Coming next.

## Defense in depth (planned)

The wrapper is the *recommended* path; an agent can still call the MCP directly. Real enforcement requires:

- **Token scoping.** Rotate `SUPABASE_ACCESS_TOKEN` for prod so only CI / the wrapper holds it. Strip prod write capability from agent contexts.
- **Branch-based dry-run.** Use the Supabase MCP's `create_branch` to test on an isolated DB before merging.
- **Rollback.** Migration-level rollback support (down migrations).

## Troubleshooting

**`Env var SUPABASE_PROJECT_REF_PROD is not set`** — set it in your shell or `.env` before running.

**`Migration NNNN breaks sequence — expected MMMM`** — the next migration number must be `max(existing) + 1`. Either renumber your file, or check whether someone else added a migration since you branched.

**`<file> has uncommitted changes`** — commit the migration before applying. The wrapper refuses to deploy uncommitted code.

**`supabase CLI not found on PATH`** — install: https://supabase.com/docs/guides/local-development/cli/getting-started
