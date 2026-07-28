#!/usr/bin/env python3
"""Refresh the bundled JSON Schema from the approved immutable specification commit.

Writes ``src/determa/state/data/machine.schema.json`` from Determa State's
``schema/machine.schema.json`` at the format-1 pre-release pin, or from a local checkout
via ``DETERMA_SPEC_DIR``. The schema-drift conformance test guards that they match.

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
DEST = ROOT / "src" / "determa" / "state" / "data" / "machine.schema.json"
SPEC_COMMIT = "4bd4d9588d11b75d376380b6120676a056a4bc45"


def _fetch() -> str:
    override = os.environ.get("DETERMA_SPEC_DIR")
    if override:
        return (Path(override) / "schema" / "machine.schema.json").read_text(encoding="utf-8")
    url = (
        "https://raw.githubusercontent.com/fruwehq/determa-state-spec/"
        f"{SPEC_COMMIT}/schema/machine.schema.json"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310 (fixed host)
            return response.read().decode("utf-8")
    except urllib.error.URLError as exc:
        raise SystemExit(f"could not fetch schema from {SPEC_COMMIT}: {exc}") from exc


def main() -> int:
    text = _fetch()
    json.loads(text)  # sanity check: valid JSON before overwriting
    if DEST.read_text(encoding="utf-8") == text:
        print(f"{DEST.relative_to(ROOT)} already up to date")
        return 0
    DEST.write_text(text, encoding="utf-8")
    print(f"updated {DEST.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
