import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import {
  appendAuditLog,
  envFromArg,
  getGitBranch,
  getGitSha,
  getGitUser,
  isFileTrackedAndClean,
  nowIso,
  type DeploymentEntry,
  type Env,
} from './_supabase_helpers.ts';

interface Args {
  file: string;
  env: Env;
  apply: boolean;
  confirmProd: boolean;
  allowDestructive: boolean;
}

const MIGRATIONS_DIR = 'supabase/migrations';
const MIGRATION_PATTERN = /^supabase\/migrations\/(\d{4})_[a-z0-9_]+\.sql$/i;

const DESTRUCTIVE_PATTERNS: Array<{ name: string; re: RegExp }> = [
  { name: 'DROP TABLE', re: /\bDROP\s+TABLE\b/i },
  { name: 'DROP SCHEMA', re: /\bDROP\s+SCHEMA\b/i },
  { name: 'TRUNCATE', re: /\bTRUNCATE\b/i },
  { name: 'DROP ROLE', re: /\bDROP\s+ROLE\b/i },
  { name: 'ALTER ROLE', re: /\bALTER\s+ROLE\b/i },
];

function printHelp() {
  console.log(`Usage: tsx scripts/safe_apply_migration.ts --file <path> --env <dev|staging|prod> [--apply] [--confirm-prod] [--allow-destructive]

Validates a Supabase migration file before (optionally) applying it.

Flags:
  --file <path>         Migration file (e.g., supabase/migrations/0095_foo.sql)
  --env <env>           Target environment: dev | staging | prod
  --apply               Actually apply (default: dry-run / validate-only)
  --confirm-prod        Required when --env=prod and --apply
  --allow-destructive   Required if migration contains DROP/TRUNCATE/ALTER ROLE

Env vars (per-target project ref):
  SUPABASE_PROJECT_REF_DEV, SUPABASE_PROJECT_REF_STAGING, SUPABASE_PROJECT_REF_PROD
  SUPABASE_ACCESS_TOKEN (read by the supabase CLI)`);
}

function parseArgs(argv: string[]): Args {
  let file: string | undefined;
  let envStr: string | undefined;
  let apply = false;
  let confirmProd = false;
  let allowDestructive = false;

  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    switch (a) {
      case '--file':
        file = argv[++i];
        break;
      case '--env':
        envStr = argv[++i];
        break;
      case '--apply':
        apply = true;
        break;
      case '--confirm-prod':
        confirmProd = true;
        break;
      case '--allow-destructive':
        allowDestructive = true;
        break;
      case '-h':
      case '--help':
        printHelp();
        process.exit(0);
      default:
        throw new Error(`Unknown arg: ${a}`);
    }
  }

  if (!file) throw new Error('--file is required');
  return { file, env: envFromArg(envStr), apply, confirmProd, allowDestructive };
}

function validate(args: Args): void {
  const m = args.file.replace(/\\/g, '/').match(MIGRATION_PATTERN);
  if (!m) {
    throw new Error(`File must match supabase/migrations/NNNN_<name>.sql; got: ${args.file}`);
  }
  const seq = parseInt(m[1], 10);

  if (!existsSync(args.file)) {
    throw new Error(`Migration file not found: ${args.file}`);
  }
  const body = readFileSync(args.file, 'utf8');

  const { tracked, clean } = isFileTrackedAndClean(args.file);
  if (!tracked) {
    throw new Error(
      `${args.file} is not tracked by git.\n` +
        `Commit the migration first — git is the source of truth.`,
    );
  }
  if (!clean) {
    throw new Error(
      `${args.file} has uncommitted changes.\n` +
        `Commit before applying — the wrapper deploys what's in git, not your working tree.`,
    );
  }

  if (!existsSync(MIGRATIONS_DIR)) {
    throw new Error(`Migrations directory not found: ${MIGRATIONS_DIR}`);
  }
  const allSeqs = readdirSync(MIGRATIONS_DIR)
    .filter((f) => /^\d{4}_.*\.sql$/.test(f))
    .map((f) => parseInt(f.slice(0, 4), 10))
    .sort((a, b) => a - b);
  const beforeUs = allSeqs.filter((n) => n < seq).pop() ?? 0;
  const expected = beforeUs + 1;
  if (seq !== expected) {
    throw new Error(
      `Migration ${String(seq).padStart(4, '0')} breaks sequence — expected ${String(expected).padStart(4, '0')}.\n` +
        `Renumber, or check for missing/conflicting migrations.`,
    );
  }

  if (args.env === 'prod' && !args.allowDestructive) {
    const hits = DESTRUCTIVE_PATTERNS.filter(({ re }) => re.test(body));
    if (hits.length) {
      throw new Error(
        `Migration contains destructive patterns on prod: ${hits.map((h) => h.name).join(', ')}\n` +
          `Pass --allow-destructive to proceed.`,
      );
    }
  }

  if (args.env === 'prod' && args.apply && !args.confirmProd) {
    throw new Error('--env=prod with --apply requires --confirm-prod');
  }
}

function envProjectRef(env: Env): string {
  const key = `SUPABASE_PROJECT_REF_${env.toUpperCase()}`;
  const ref = process.env[key];
  if (!ref) {
    throw new Error(
      `Env var ${key} is not set. Cannot resolve target project for ${env}.`,
    );
  }
  return ref;
}

function applyMigration(args: Args): void {
  const ref = envProjectRef(args.env);
  console.log(`[safe-apply] supabase db push -p ${ref} (env=${args.env})`);
  try {
    execFileSync('supabase', ['db', 'push', '-p', ref], { stdio: 'inherit' });
  } catch (e) {
    const code = (e as NodeJS.ErrnoException).code;
    if (code === 'ENOENT') {
      throw new Error(
        'supabase CLI not found on PATH. Install: https://supabase.com/docs/guides/local-development/cli/getting-started',
      );
    }
    throw new Error(`supabase db push failed (see output above)`);
  }
}

function main() {
  const args = parseArgs(process.argv.slice(2));

  console.log(`[safe-apply] validating ${args.file} for env=${args.env}...`);
  validate(args);

  const baseEntry: Omit<DeploymentEntry, 'applied_at' | 'status' | 'message'> = {
    ts: nowIso(),
    env: args.env,
    artifact_kind: 'migration',
    artifact: args.file.replace(/\\/g, '/'),
    git_sha: getGitSha(),
    git_branch: getGitBranch(),
    git_user: getGitUser(),
  };

  if (!args.apply) {
    console.log('[safe-apply] dry-run: validation passed. Pass --apply to deploy.');
    appendAuditLog({ ...baseEntry, applied_at: null, status: 'dry-run' });
    return;
  }

  try {
    applyMigration(args);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    appendAuditLog({ ...baseEntry, applied_at: null, status: 'failed', message: msg });
    throw e;
  }

  appendAuditLog({ ...baseEntry, applied_at: nowIso(), status: 'applied' });
  console.log('[safe-apply] applied successfully.');
  console.log('[safe-apply] commit audit/deployments.jsonl to record the deploy.');
}

try {
  main();
} catch (e) {
  console.error(`[safe-apply] BLOCKED: ${e instanceof Error ? e.message : String(e)}`);
  process.exit(1);
}
