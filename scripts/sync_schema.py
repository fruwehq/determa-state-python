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
    "machine.schema.json",
    "aggregate-state.schema.json",
    "migration-descriptor.schema.json",
    "aggregate-state-package.schema.json",
)
SPEC_COMMIT = "c1635d74e6a216301a8986d37be8ce7e7111dfd7"


def _fetch(name: str) -> str:
    override = os.environ.get("DETERMA_SPEC_DIR")
    if override:
        return (Path(override) / "schema" / name).read_text(encoding="utf-8")
    url = (
        "https://raw.githubusercontent.com/fruwehq/determa-state-spec/"
        f"{SPEC_COMMIT}/schema/{name}"
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
        if destination.read_text(encoding="utf-8") == text:
            print(f"{destination.relative_to(ROOT)} already up to date")
            continue
        destination.write_text(text, encoding="utf-8")
        print(f"updated {destination.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
