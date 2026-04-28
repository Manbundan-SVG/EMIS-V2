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

| Var | Purpose | Where it lives |
|---|---|---|
| `SUPABASE_DB_URL_DEV` | Dev Postgres connection string | Local `.env` and CI secret |
| `SUPABASE_DB_URL_STAGING` | Staging Postgres connection string | CI secret only |
| `SUPABASE_DB_URL_PROD` | Prod Postgres connection string | CI secret only — never local |
| `EMIS_ALLOW_PROD_FROM_LOCAL` | Override the CI-only check for prod (hotfix) | Set at the shell, deliberately |

The wrapper requires only the var matching the target. `SUPABASE_PROJECT_REF`
and `SUPABASE_ACCESS_TOKEN` are unrelated — they're for the Supabase MCP and
direct CLI commands; the migration wrapper does not use them.

Copy [`.env.example`](../.env.example) to `.env` and fill in dev. `.env*` is
gitignored (`.env.example` itself is committed as the template).

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

## Token scoping (in place)

The wrapper enforces, by default:

- **Prod applies must run from CI.** `--env=prod --apply` refuses to run unless
  `CI=true` or `GITHUB_ACTIONS=true`. Local override exists
  (`EMIS_ALLOW_PROD_FROM_LOCAL=1`) for emergency hotfixes only — setting it
  from an agent context is a security incident.
- **Per-env database URLs.** Each environment's `SUPABASE_DB_URL_*` is a
  separate secret. The prod URL only exists as a GitHub Actions secret;
  it is never put into a local `.env`.

### CI workflow

[`.github/workflows/deploy-migrations.yml`](../.github/workflows/deploy-migrations.yml)
is `workflow_dispatch`-only — auto-apply on merge is intentionally off.
Triggering the workflow is the deliberate "go" decision.

### One-time setup (repo admin)

1. **Rotate any existing Supabase access tokens** that have been used from
   agent contexts or local shells with prod access. New tokens go straight
   into GitHub secrets.

2. **Configure GitHub Environments** for `dev`, `staging`, `prod` in
   *Settings → Environments*. For `prod`:
   - Add `SUPABASE_DB_URL_PROD` as an **environment** secret (not repo
     secret), so only jobs running with `environment: prod` can read it.
   - Add a required-reviewers protection rule so a human must approve
     before a deploy job starts.

3. **Strip prod write capability from agent contexts.** Don't expose any
   prod `SUPABASE_DB_URL_PROD` or unrestricted `SUPABASE_ACCESS_TOKEN` to
   shells where an agent runs.

### Token rotation procedure

If a token leaks (committed to git, pasted in chat, agent ran wild):

1. Revoke at <https://supabase.com/dashboard/account/tokens>.
2. Rotate the database password at *Project Settings → Database* for the
   affected environment (the `SUPABASE_DB_URL_*` includes the password).
3. Update the GitHub secret with the new URL.
4. Audit `audit/deployments.jsonl` for any unexpected entries during the
   leak window.
5. Open an incident note in `docs/`.

## Defense in depth (planned)

- **Branch-based dry-run.** Use the Supabase MCP's `create_branch` to test
  on an isolated DB before merging.
- **Rollback.** Migration-level rollback support (down migrations).
- **Audit log to a Supabase table.** Mirror `audit/deployments.jsonl` to a
  `_emis_deployments` table for query-able provenance.

## Troubleshooting

**`Env var SUPABASE_PROJECT_REF_PROD is not set`** — set it in your shell or `.env` before running.

**`Migration NNNN breaks sequence — expected MMMM`** — the next migration number must be `max(existing) + 1`. Either renumber your file, or check whether someone else added a migration since you branched.

**`<file> has uncommitted changes`** — commit the migration before applying. The wrapper refuses to deploy uncommitted code.

**`supabase CLI not found on PATH`** — install: https://supabase.com/docs/guides/local-development/cli/getting-started
