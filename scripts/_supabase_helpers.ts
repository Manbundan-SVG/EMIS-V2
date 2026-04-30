import { execFileSync } from 'node:child_process';
import { appendFileSync, existsSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

export const AUDIT_LOG = 'audit/deployments.jsonl';

export type Env = 'dev' | 'staging' | 'prod';

export type ArtifactKind = 'migration' | 'function' | 'sql';

export type DeploymentStatus = 'applied' | 'dry-run' | 'failed';

export interface DeploymentEntry {
  ts: string;
  env: Env;
  artifact_kind: ArtifactKind;
  artifact: string;
  git_sha: string;
  git_branch: string;
  git_user: string;
  applied_at: string | null;
  status: DeploymentStatus;
  message?: string;
}

export function envFromArg(arg: string | undefined): Env {
  if (arg !== 'dev' && arg !== 'staging' && arg !== 'prod') {
    throw new Error(`--env must be one of dev, staging, prod (got: ${arg ?? 'undefined'})`);
  }
  return arg;
}

export function getGitSha(): string {
  return execFileSync('git', ['rev-parse', 'HEAD']).toString().trim();
}

export function getGitBranch(): string {
  return execFileSync('git', ['rev-parse', '--abbrev-ref', 'HEAD']).toString().trim();
}

export function getGitUser(): string {
  try {
    return execFileSync('git', ['config', 'user.email']).toString().trim();
  } catch {
    return 'unknown';
  }
}

export function isFileTrackedAndClean(file: string): { tracked: boolean; clean: boolean } {
  let tracked = true;
  try {
    execFileSync('git', ['ls-files', '--error-unmatch', file], { stdio: 'ignore' });
  } catch {
    tracked = false;
  }
  if (!tracked) {
    return { tracked: false, clean: false };
  }
  const out = execFileSync('git', ['status', '--porcelain', '--', file]).toString();
  return { tracked: true, clean: out.trim() === '' };
}

export function appendAuditLog(entry: DeploymentEntry): void {
  const dir = dirname(AUDIT_LOG);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  appendFileSync(AUDIT_LOG, JSON.stringify(entry) + '\n', 'utf8');
}

export function nowIso(): string {
  return new Date().toISOString();
}
