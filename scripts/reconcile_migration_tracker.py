"""Reconcile the supabase_migrations.schema_migrations tracker.

Why this exists
---------------
Our local migrations under supabase/migrations/ use sequential 4-digit IDs
(0001_phase1_schema.sql ... 0096_phase4_8D_*.sql). At some point in this
project's history, migrations were applied to the remote DB through a path
that recorded them in schema_migrations using 14-digit timestamp version
IDs (e.g., 20260426212724) — those IDs do not correspond to any local
filename. As a result, ``supabase db push`` refuses to run because it
sees the remote tracker and the local files as having entirely different
version sets:

    Remote migration versions not found in local migrations directory.

What this script does
---------------------
1. Connects to DATABASE_URL (read from env).
2. Inspects the current state of supabase_migrations.schema_migrations,
   classifying entries as: legacy 14-digit timestamps, our 4-digit
   sequentials, or other formats.
3. Lists local migrations under supabase/migrations/.
4. In ``--apply`` mode, inside a single transaction:
     a. DELETEs all rows whose version matches the legacy 14-digit pattern.
     b. INSERTs (with ON CONFLICT DO UPDATE) one row per local migration
        EXCEPT the latest one, marking those as applied (since the tables
        they create already exist on the remote DB).
   The latest migration is intentionally excluded so a subsequent
   ``supabase db push`` will see it as un-applied and push it.
5. Re-inspects the tracker and prints a before/after summary.

Usage
-----
    # Dry-run is the default. Inspect what will change.
    python scripts/reconcile_migration_tracker.py

    # Execute. Requires explicit --apply.
    python scripts/reconcile_migration_tracker.py --apply

    # Override which migration is held back (default: highest-numbered
    # local migration).
    python scripts/reconcile_migration_tracker.py --apply --hold-back 0096

Safety
------
* Dry-run by default; --apply is required to mutate state.
* All DB mutations run inside a single transaction (BEGIN/COMMIT). If any
  step fails, the entire reconciliation rolls back.
* Uses ON CONFLICT (version) DO UPDATE so re-running is idempotent.
* Bypasses Python 3.14 urllib.parse._check_bracketed_host regression by
  hand-rolling the DSN parser (same shim as validate_phase48d.py).
* Does not touch any application table — only supabase_migrations.schema_migrations.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from pathlib import Path

import asyncpg


REPO_ROOT      = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = REPO_ROOT / "supabase" / "migrations"

LEGACY_PATTERN     = re.compile(r"^\d{14}$")
SEQUENTIAL_PATTERN = re.compile(r"^\d{4}$")
FILENAME_PATTERN   = re.compile(r"^(\d{4})_(.+)\.sql$")


def _parse_dsn(url: str) -> dict:
    """Hand-rolled DSN parser. Avoids Python 3.14's urllib.parse regression."""
    if url.startswith("postgresql://"):
        rest = url[len("postgresql://"):]
    elif url.startswith("postgres://"):
        rest = url[len("postgres://"):]
    else:
        raise ValueError("DSN must start with postgresql:// or postgres://")

    at_idx = rest.rfind("@")
    if at_idx < 0:
        raise ValueError("DSN missing '@' between userinfo and host")
    userinfo = rest[:at_idx]
    hostpart = rest[at_idx + 1:]

    if ":" in userinfo:
        user, password = userinfo.split(":", 1)
    else:
        user, password = userinfo, ""

    slash_idx = hostpart.find("/")
    if slash_idx < 0:
        raise ValueError("DSN missing '/' before database name")
    hostport     = hostpart[:slash_idx]
    rest_of_url  = hostpart[slash_idx + 1:]

    if ":" in hostport:
        host, port_str = hostport.rsplit(":", 1)
        port = int(port_str)
    else:
        host, port = hostport, 5432

    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]

    q_idx    = rest_of_url.find("?")
    database = rest_of_url[:q_idx] if q_idx >= 0 else rest_of_url

    return {
        "user":     user,
        "password": password,
        "host":     host,
        "port":     port,
        "database": database,
    }


def list_local_migrations() -> list[tuple[str, str, str]]:
    """Return a sorted list of (version, name, filename) for every local migration."""
    if not MIGRATIONS_DIR.exists():
        raise SystemExit(f"migrations directory not found: {MIGRATIONS_DIR}")

    out: list[tuple[str, str, str]] = []
    for f in sorted(MIGRATIONS_DIR.glob("*.sql")):
        m = FILENAME_PATTERN.match(f.name)
        if not m:
            continue  # skip files that don't match our convention
        version, name = m.groups()
        out.append((version, name, f.name))
    return out


async def inspect_tracker(conn: asyncpg.Connection) -> dict:
    """Read the current schema_migrations state."""
    rows = await conn.fetch(
        "SELECT version, name FROM supabase_migrations.schema_migrations ORDER BY version"
    )
    legacy:     list[asyncpg.Record] = []
    sequential: list[asyncpg.Record] = []
    other:      list[asyncpg.Record] = []
    for r in rows:
        v = r["version"]
        if LEGACY_PATTERN.match(v):
            legacy.append(r)
        elif SEQUENTIAL_PATTERN.match(v):
            sequential.append(r)
        else:
            other.append(r)
    return {
        "all":        rows,
        "legacy":     legacy,
        "sequential": sequential,
        "other":      other,
    }


def print_state(label: str, state: dict) -> None:
    print(f"\n[{label}] tracker has {len(state['all'])} rows total:")
    print(f"  legacy 14-digit timestamps: {len(state['legacy'])}")
    print(f"  sequential 4-digit (ours):  {len(state['sequential'])}")
    print(f"  other formats:              {len(state['other'])}")
    if state["other"]:
        print("  WARNING: other-format versions present:")
        for r in state["other"]:
            print(f"    {r['version']!r} ({r['name']!r})")


async def reconcile(*, apply: bool, hold_back: str | None) -> int:
    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL is not set in the environment", file=sys.stderr)
        return 2

    migrations = list_local_migrations()
    if not migrations:
        print("ERROR: no local migrations found", file=sys.stderr)
        return 2

    if hold_back is None:
        hold_back = migrations[-1][0]
    if not any(v == hold_back for v, _, _ in migrations):
        print(f"ERROR: --hold-back {hold_back} doesn't match any local migration", file=sys.stderr)
        return 2

    to_mark_applied = [m for m in migrations if m[0] != hold_back]

    print(f"[reconcile] local migrations: {len(migrations)}")
    print(f"[reconcile] holding back:     {hold_back} (intentionally NOT marked applied)")
    print(f"[reconcile] will mark applied: {len(to_mark_applied)}")
    print(f"[reconcile] mode:             {'APPLY' if apply else 'DRY-RUN'}")

    conn = await asyncpg.connect(**_parse_dsn(os.environ["DATABASE_URL"]))
    try:
        before = await inspect_tracker(conn)
        print_state("before", before)

        if not apply:
            print("\n[reconcile] DRY-RUN — no changes made.")
            print("[reconcile] Plan:")
            print(f"  DELETE FROM supabase_migrations.schema_migrations WHERE version ~ '^[0-9]{{14}}$'")
            print(f"    (would remove {len(before['legacy'])} legacy timestamp rows)")
            print(f"  UPSERT {len(to_mark_applied)} sequential rows for versions other than {hold_back}")
            print("\n[reconcile] To execute, re-run with --apply")
            return 0

        # APPLY MODE — single transaction.
        async with conn.transaction():
            del_status = await conn.execute(
                "DELETE FROM supabase_migrations.schema_migrations WHERE version ~ '^[0-9]{14}$'"
            )
            # del_status looks like 'DELETE <count>'
            print(f"\n[reconcile] {del_status}")

            for version, name, _filename in to_mark_applied:
                await conn.execute(
                    "INSERT INTO supabase_migrations.schema_migrations (version, name) "
                    "VALUES ($1, $2) "
                    "ON CONFLICT (version) DO UPDATE SET name = EXCLUDED.name",
                    version, name,
                )
            print(f"[reconcile] upserted {len(to_mark_applied)} sequential entries")

        # Verify.
        after = await inspect_tracker(conn)
        print_state("after", after)

        # Sanity checks.
        if after["legacy"]:
            print("\n[reconcile] WARNING: legacy timestamps still present after delete!")
            return 1
        if any(r["version"] == hold_back for r in after["all"]):
            print(f"\n[reconcile] WARNING: {hold_back} ended up in the tracker. supabase db push won't push it.")
            return 1
        if len(after["sequential"]) != len(to_mark_applied):
            print(
                f"\n[reconcile] WARNING: expected {len(to_mark_applied)} sequential rows, "
                f"got {len(after['sequential'])}"
            )
            return 1

        print("\n[reconcile] OK — tracker is reconciled.")
        print(f"[reconcile] Next: run `supabase db push` to apply migration {hold_back}.")
        return 0
    finally:
        await conn.close()


def main() -> int:
    p = argparse.ArgumentParser(description="Reconcile supabase_migrations.schema_migrations")
    p.add_argument("--apply", action="store_true",
                   help="Execute the reconciliation. Without this, the script is dry-run only.")
    p.add_argument("--hold-back", default=None,
                   help="Migration version to keep un-applied. Defaults to the highest local version.")
    args = p.parse_args()
    return asyncio.run(reconcile(apply=args.apply, hold_back=args.hold_back))


if __name__ == "__main__":
    sys.exit(main())
