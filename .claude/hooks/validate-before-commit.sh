#!/usr/bin/env bash
set -euo pipefail

echo "[EMIS] Running pre-commit validation..."

has_replay_fixtures="false"
has_phase_validator="false"
has_replay_validator="false"

# Real fixture detection: ignore placeholder files such as .gitkeep.
if [ -d "tests/fixtures/replay" ] && \
   find tests/fixtures/replay -type f ! -name ".gitkeep" | grep -q .; then
  has_replay_fixtures="true"
fi

# Use tracked files only. Avoid scanning node_modules, dist, .next, etc.
# Tightened: anchor base name to avoid matching helper files like validate_phase_helpers.ts.
if git ls-files | grep -E '(^|/)validate_phase[^/]*\.(ts|tsx|py)$' >/dev/null 2>&1; then
  has_phase_validator="true"
fi

if git ls-files | grep -E '(^|/)scripts/validate_replay\.(ts|py)$' >/dev/null 2>&1; then
  has_replay_validator="true"
fi

# Bootstrap mode: allows committing the Claude skills layer before
# the repo has real fixtures/validators.
if [ "$has_replay_fixtures" = "false" ] && \
   [ "$has_phase_validator" = "false" ] && \
   [ "$has_replay_validator" = "false" ]; then
  echo "[EMIS] Bootstrap mode: no real replay fixtures or validators found."
  echo "[EMIS] Skipping fixture gate for initial skills/bootstrap commit."
else
  required_families=("market_data" "liquidation" "sentiment" "macro")

  for family in "${required_families[@]}"; do
    if [ ! -d "tests/fixtures/replay/${family}" ]; then
      echo "[BLOCKED] Missing replay fixture family: ${family}"
      exit 1
    fi

    if ! find "tests/fixtures/replay/${family}" -type f ! -name ".gitkeep" | grep -q .; then
      echo "[BLOCKED] Replay fixture family has no real fixture files: ${family}"
      exit 1
    fi
  done
fi

if [ -f "package.json" ]; then
  if command -v pnpm >/dev/null 2>&1; then
    pnpm typecheck || exit 1
    pnpm test || exit 1
  elif command -v npm >/dev/null 2>&1; then
    npm run typecheck --if-present || exit 1
    npm test --if-present || exit 1
  fi
fi

if [ "$has_replay_validator" = "true" ]; then
  if [ -f "scripts/validate_replay.ts" ]; then
    if command -v pnpm >/dev/null 2>&1; then
      pnpm tsx scripts/validate_replay.ts
    else
      npx tsx scripts/validate_replay.ts
    fi
  elif [ -f "scripts/validate_replay.py" ]; then
    python scripts/validate_replay.py
  fi
else
  echo "[EMIS] No replay validator found. Skipping replay validation."
fi

echo "[EMIS] Pre-commit validation complete."
