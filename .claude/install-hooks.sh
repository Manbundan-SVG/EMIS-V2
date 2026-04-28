#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$REPO_ROOT" ]; then
  echo "[EMIS] Not inside a git working tree."
  echo "[EMIS] Run 'git init' from the repo root, then re-run: bash .claude/install-hooks.sh"
  exit 1
fi

cd "$REPO_ROOT"

if [ ! -d ".claude/hooks" ]; then
  echo "[EMIS] .claude/hooks not found at $REPO_ROOT"
  exit 1
fi

chmod +x .claude/hooks/pre-commit 2>/dev/null || true
find .claude/hooks -maxdepth 1 -type f -name "*.sh" -exec chmod +x {} +

git config core.hooksPath .claude/hooks

echo "[EMIS] core.hooksPath -> .claude/hooks"
echo "[EMIS] Hooks discovered:"
shopt -s nullglob
for prefix in validate- block- check-; do
  for f in .claude/hooks/${prefix}*.sh; do
    echo "  - $f"
  done
done
echo "[EMIS] Verify with: git commit --allow-empty -m 'verify hooks'"
