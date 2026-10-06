#!/usr/bin/env python3
"""Refresh bundled JSON Schemas from the approved immutable specification commit.

Writes the machine and persistence artifact schemas from Determa State's ``schema/``
directory, or from a local checkout via ``DETERMA_SPEC_DIR``. Schema-drift conformance
tests guard that all copies match.

Usage: ``python scripts/sync_schema.py``  (or ``make sync-schema``).
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEST = ROOT / "src" / "determa" / "state" / "data"
SCHEMAS = (
    "aggregate-state-package-v1.schema.json",
    "aggregate-state-v1.schema.json",
    "archive-export-request-v1.schema.json",
    "archive-import-request-v1.schema.json",
    "archive-participant-v1.schema.json",
    "archive-result-v1.schema.json",
    "archive-v1.schema.json",
    "compilation-manifest-v1.schema.json",
    "core-step-result-v1.schema.json",
    "effect-cancellation-request-v1.schema.json",
    "effect-cancellation-response-v1.schema.json",
    "effect-result-request-v1.schema.json",
    "effect-result-response-v1.schema.json",
    "execution-checkpoint-v1.schema.json",
    "extension-capability-report-v1.schema.json",
    "extension-capability-requirement-v1.schema.json",
    "extension-descriptor-v1.schema.json",
    "host-authority-operation-v1.schema.json",
    "host-authority-profile-report-v1.schema.json",
    "host-effect-claim-v1.schema.json",
    "host-effect-journal-v1.schema.json",
    "inspection-v1.schema.json",
    "language-source-v1.schema.json",
    "machine.schema.json",
    "migration-descriptor-v1.schema.json",
    "provider-reference-v1.schema.json",
    "public-host-request-v1.schema.json",
    "public-host-response-v1.schema.json",
    "recovery-operation-v1.schema.json",
    "recovery-record-v1.schema.json",
    "runtime-action-output-v1.schema.json",
    "runtime-provider-descriptor-v1.schema.json",
    "timer-helper-operation-v1.schema.json",
    "timer-record-v1.schema.json",
)
SPEC_COMMIT = "77c0a2e60cd0771a6d44ae170a079ddd51d7d9f0"


def _fetch(name: str) -> str:
    override = os.environ.get("DETERMA_SPEC_DIR")
    if override:
        return (Path(override) / "schema" / name).read_text(encoding="utf-8")
    url = (
        f"https://raw.githubusercontent.com/fruwehq/determa-state-spec/{SPEC_COMMIT}/schema/{name}"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310 (fixed host)
            return response.read().decode("utf-8")
    except urllib.error.URLError as exc:
        raise SystemExit(f"could not fetch schema from {SPEC_COMMIT}: {exc}") from exc


def main() -> int:
    for name in SCHEMAS:
        text = _fetch(name)
        json.loads(text)
        destination = DEST / name
        if destination.exists() and destination.read_text(encoding="utf-8") == text:
            print(f"{destination.relative_to(ROOT)} already up to date")
            continue
        destination.write_text(text, encoding="utf-8")
        print(f"updated {destination.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
