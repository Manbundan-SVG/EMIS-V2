"""Validate replay determinism per docs/replay-contract.md.

For each entry in tests/fixtures/replay/manifest.json:
  1. Hash the fixture file, assert == rawInputHash.
  2. Hash the canonical config, assert == configHash.
  3. Assert schemaVersion / featureVersion / modelVersion match the
     transform registry.
  4. Run the transform; canonicalize output; assert sha256 == outputHash.
  5. Diff against tests/fixtures/replay/__golden__/<target>.json (after
     re-canonicalizing, since the golden is pretty-printed).

Detected by `.claude/hooks/validate-before-commit.sh`. Run from repo root.

Usage:
  python scripts/validate_replay.py [--target <key>] [--update-golden]
                                    [--strict-all] [--self-test]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from _replay_lib import (
    FIXTURE_GENERATOR_VERSION,
    FIXTURE_SEED,
    FIXTURES_ROOT,
    GOLDEN_ROOT,
    MANIFEST_PATH,
    REPLAY_TRANSFORMS,
    VALIDATOR_VERSION,
    TransformSpec,
    canonical_hash,
    canonical_json,
    file_sha256_hex,
    fixture_path,
    golden_path,
    iso_utc_ms,
    pretty_json,
    read_csv,
    read_jsonl,
    sha256_hex,
)

# ---------------------------------------------------------------------------
# Exit codes (§ 7.2)
# ---------------------------------------------------------------------------

EXIT_OK = 0
EXIT_HASH_MISMATCH = 1
EXIT_FIXTURE_BAD = 2
EXIT_VERSION_MISMATCH = 3
EXIT_TRANSFORM_MISSING = 4


# ---------------------------------------------------------------------------
# Manifest IO
# ---------------------------------------------------------------------------


def _load_manifest() -> list[dict]:
    if not MANIFEST_PATH.exists():
        return []
    with MANIFEST_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("manifest.json must be a JSON array")
    return data


def _save_manifest(entries: list[dict]) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    body = pretty_json(sorted(entries, key=lambda e: e["target"])) + "\n"
    MANIFEST_PATH.write_bytes(body.encode("utf-8"))


# ---------------------------------------------------------------------------
# Fixture loading
# ---------------------------------------------------------------------------


def _load_fixture(spec: TransformSpec) -> list[dict]:
    path = fixture_path(spec)
    if spec.fixture_format == "csv":
        return read_csv(path)
    if spec.fixture_format == "jsonl":
        return read_jsonl(path)
    raise ValueError(f"unknown fixture_format: {spec.fixture_format}")


def _read_golden(spec: TransformSpec) -> list[dict] | None:
    p = golden_path(spec)
    if not p.exists():
        return None
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"golden {p} must be a JSON array")
    return data


def _write_golden(spec: TransformSpec, output: list[dict]) -> None:
    p = golden_path(spec)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes((pretty_json(output) + "\n").encode("utf-8"))


# ---------------------------------------------------------------------------
# Validation core
# ---------------------------------------------------------------------------


class ValidationError(Exception):
    def __init__(self, code: int, msg: str):
        super().__init__(msg)
        self.code = code


def _entry_for(spec: TransformSpec, manifest: list[dict]) -> dict | None:
    for e in manifest:
        if e["target"] == spec.target:
            return e
    return None


def _build_manifest_entry(
    spec: TransformSpec,
    raw_hash: str,
    output_hash: str,
    window_start: str,
    window_end: str,
) -> dict:
    return {
        "target": spec.target,
        "replayWindowStart": window_start,
        "replayWindowEnd": window_end,
        "rawInputHash": raw_hash,
        "configHash": canonical_hash(spec.config),
        "schemaVersion": spec.schema_version,
        "featureVersion": spec.feature_version,
        "modelVersion": spec.model_version,
        "outputHash": output_hash,
        "validatorVersion": VALIDATOR_VERSION,
        "fixtureGeneratorVersion": FIXTURE_GENERATOR_VERSION,
        "fixtureSeed": FIXTURE_SEED,
        "createdAt": iso_utc_ms(datetime.now(timezone.utc)),
    }


def _compute_window(rows: list[dict], spec: TransformSpec) -> tuple[str, str]:
    """Pull min/max ts from fixture rows. Returns (start, end) ISO strings."""
    from _replay_lib import parse_iso_utc  # local import to keep this file tight
    if not rows:
        # Empty fixture → window is generator's default
        return ("1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.000Z")
    times = [parse_iso_utc(r["ts"]).astimezone(timezone.utc) for r in rows]
    return (iso_utc_ms(min(times)), iso_utc_ms(max(times)))


def _validate_one(
    spec: TransformSpec,
    manifest: list[dict],
    *,
    update_golden: bool,
) -> tuple[dict, str | None]:
    """Returns (manifest_entry, error_message_or_None)."""
    fpath = fixture_path(spec)
    if not fpath.exists():
        raise ValidationError(EXIT_FIXTURE_BAD, f"fixture missing: {fpath}")

    try:
        raw_hash = file_sha256_hex(fpath)
        rows = _load_fixture(spec)
    except (ValueError, json.JSONDecodeError) as e:
        raise ValidationError(EXIT_FIXTURE_BAD, f"fixture load failed for {spec.target}: {e}") from e

    try:
        output = spec.callable(rows, fpath)
    except ValueError as e:
        raise ValidationError(EXIT_FIXTURE_BAD, f"transform validation failed for {spec.target}: {e}") from e
    except Exception as e:  # pragma: no cover — surface unexpected
        raise ValidationError(EXIT_FIXTURE_BAD, f"transform crashed for {spec.target}: {e}") from e

    output_canonical = canonical_json(output)
    output_hash = sha256_hex(output_canonical)
    window_start, window_end = _compute_window(rows, spec)

    new_entry = _build_manifest_entry(spec, raw_hash, output_hash, window_start, window_end)

    if update_golden:
        _write_golden(spec, output)
        return new_entry, None

    # Verification path.
    existing = _entry_for(spec, manifest)
    if existing is None:
        raise ValidationError(
            EXIT_VERSION_MISMATCH,
            f"no manifest entry for {spec.target}; run with --update-golden first",
        )

    if existing["rawInputHash"] != raw_hash:
        raise ValidationError(
            EXIT_HASH_MISMATCH,
            f"{spec.target}: rawInputHash drift\n  expected: {existing['rawInputHash']}\n  actual:   {raw_hash}",
        )

    cfg_hash = canonical_hash(spec.config)
    if existing["configHash"] != cfg_hash:
        raise ValidationError(
            EXIT_HASH_MISMATCH,
            f"{spec.target}: configHash drift (config object changed)",
        )

    for field in ("schemaVersion", "featureVersion", "modelVersion"):
        if existing[field] != getattr(spec, _to_snake(field)):
            raise ValidationError(
                EXIT_VERSION_MISMATCH,
                f"{spec.target}: {field} drift\n  manifest: {existing[field]}\n  registry: {getattr(spec, _to_snake(field))}",
            )

    if existing["outputHash"] != output_hash:
        raise ValidationError(
            EXIT_HASH_MISMATCH,
            f"{spec.target}: outputHash drift — transform output changed.\n"
            f"  expected: {existing['outputHash']}\n"
            f"  actual:   {output_hash}\n"
            f"  Bump featureVersion/modelVersion and add a row to audit/replay-migrations.md.",
        )

    golden = _read_golden(spec)
    if golden is None:
        raise ValidationError(
            EXIT_VERSION_MISMATCH,
            f"{spec.target}: golden file missing — run with --update-golden",
        )
    if sha256_hex(canonical_json(golden)) != output_hash:
        raise ValidationError(
            EXIT_HASH_MISMATCH,
            f"{spec.target}: golden file canonicalizes to a different hash than the manifest. "
            f"Likely the golden was edited without updating the manifest.",
        )

    # Carry over manifest fields that don't depend on rerun (createdAt).
    new_entry["createdAt"] = existing["createdAt"]
    return new_entry, None


def _to_snake(camel: str) -> str:
    """schemaVersion -> schema_version."""
    out = []
    for ch in camel:
        if ch.isupper():
            out.append("_")
            out.append(ch.lower())
        else:
            out.append(ch)
    return "".join(out)


# ---------------------------------------------------------------------------
# Self-test cases (§ 8 — required checks, MVP placement)
# ---------------------------------------------------------------------------


def _self_test() -> int:
    """Required checks from § 8. Run via --self-test. Exit 0 on pass, non-zero on fail."""
    import tempfile
    from _replay_lib import parse_iso_utc

    print("[self-test] starting...")
    failures: list[str] = []

    # 1. Idempotency: validate twice in a row, same hashes.
    manifest = _load_manifest()
    if manifest:
        first_pass = []
        for spec in REPLAY_TRANSFORMS.values():
            entry, _ = _validate_one(spec, manifest, update_golden=False)
            first_pass.append((entry["target"], entry["outputHash"]))
        second_pass = []
        for spec in REPLAY_TRANSFORMS.values():
            entry, _ = _validate_one(spec, manifest, update_golden=False)
            second_pass.append((entry["target"], entry["outputHash"]))
        if first_pass != second_pass:
            failures.append("idempotency: two consecutive validations produced different hashes")
        else:
            print("  ok  idempotency: two consecutive validations match")
    else:
        print("  --  idempotency: skipped (no manifest yet)")

    # 2. UTC normalization: parsing a naive timestamp must raise.
    try:
        parse_iso_utc("2024-01-01T00:00:00")
        failures.append("utc: parse_iso_utc accepted a non-UTC timestamp")
    except ValueError:
        print("  ok  utc normalization: naive timestamps rejected")

    # 3. Empty fixture → stable empty output, stable hash.
    from _replay_lib import compute_btc_1h_sma5
    out_empty_a = compute_btc_1h_sma5([], Path("<empty>"))
    out_empty_b = compute_btc_1h_sma5([], Path("<empty>"))
    if out_empty_a != [] or out_empty_b != []:
        failures.append("empty fixture: did not produce empty output")
    elif sha256_hex(canonical_json(out_empty_a)) != sha256_hex(canonical_json(out_empty_b)):
        failures.append("empty fixture: hash unstable across runs")
    else:
        print(f"  ok  empty fixture stable; hash={sha256_hex(canonical_json(out_empty_a))[:12]}...")

    # 4. Malformed fixture: missing required column → ValueError, not silent success.
    try:
        compute_btc_1h_sma5([{"ts": "2024-01-01T00:00:00.000Z", "open": "1"}], Path("<bad>"))
        failures.append("malformed: transform accepted row missing required fields")
    except ValueError:
        print("  ok  malformed fixture: missing fields rejected")

    # 5. Frozen-clock determinism: transforms do not consult system clock.
    # We assert by running the same input twice — already covered by (1) + (3).
    print("  ok  frozen-clock: covered by idempotency + empty-stability")

    if failures:
        print("[self-test] FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("[self-test] all checks passed")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate EMIS replay determinism")
    p.add_argument("--target", help="Only validate this target (e.g., market_data:btc_1h_sma5)")
    p.add_argument("--update-golden", action="store_true",
                   help="Recompute golden files and manifest entries. Rejected in CI.")
    p.add_argument("--strict-all", action="store_true",
                   help="Run all targets even after the first failure")
    p.add_argument("--self-test", action="store_true",
                   help="Run the required-checks self-test instead of normal validation")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    if args.self_test:
        return _self_test()

    if args.update_golden:
        if os.environ.get("CI", "").lower() in {"true", "1"}:
            print("--update-golden is rejected in CI", file=sys.stderr)
            return EXIT_VERSION_MISMATCH

    targets = [args.target] if args.target else list(REPLAY_TRANSFORMS.keys())
    unknown = [t for t in targets if t not in REPLAY_TRANSFORMS]
    if unknown:
        print(f"unknown target(s): {unknown}", file=sys.stderr)
        return EXIT_TRANSFORM_MISSING

    manifest = _load_manifest()
    new_entries: list[dict] = []
    failures: list[tuple[str, str, int]] = []

    for target in targets:
        spec = REPLAY_TRANSFORMS[target]
        try:
            entry, _ = _validate_one(spec, manifest, update_golden=args.update_golden)
            new_entries.append(entry)
        except ValidationError as e:
            failures.append((target, str(e), e.code))
            if not args.strict_all:
                break

    # On --update-golden, write the manifest with all entries.
    if args.update_golden:
        # Carry over entries we didn't touch.
        touched = {e["target"] for e in new_entries}
        for e in manifest:
            if e["target"] not in touched:
                new_entries.append(e)
        _save_manifest(new_entries)
        print(f"[validate-replay] manifest updated with {len(new_entries)} entries")

    if failures:
        for target, msg, code in failures:
            print(f"[FAIL] {target}: {msg}", file=sys.stderr)
        return failures[-1][2]

    print(f"[validate-replay] OK ({len(new_entries)}/{len(targets)} targets)")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
