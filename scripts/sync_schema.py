#!/usr/bin/env python3
"""Refresh the bundled JSON Schema from the spec repo (fruwehq/determa-state-spec).

Writes ``src/determa/state/data/machine.schema.json`` from Determa State's
``schema/machine.schema.json`` at the tag matching this package's version (falling back
to ``main``), or from a local checkout via ``DETERMA_SPEC_DIR``. This removes the manual
copy step; the schema-drift test still guards that the two stay in sync.

Usage: ``python scripts/sync_schema.py``  (or ``make sync-schema``).
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEST = ROOT / "src" / "determa" / "state" / "data" / "machine.schema.json"
ABOUT = ROOT / "src" / "determa" / "state" / "__about__.py"


def _version() -> str:
    m = re.search(r'__version__\s*=\s*"([^"]+)"', ABOUT.read_text(encoding="utf-8"))
    if m is None:
        raise SystemExit(f"could not read version from {ABOUT}")
    return m.group(1)


def _fetch() -> str:
    override = os.environ.get("DETERMA_SPEC_DIR")
    if override:
        return (Path(override) / "schema" / "machine.schema.json").read_text(encoding="utf-8")
    last: Exception | None = None
    for ref in (f"v{_version()}", "main"):
        url = f"https://raw.githubusercontent.com/fruwehq/determa-state-spec/{ref}/schema/machine.schema.json"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:  # noqa: S310 (fixed host)
                return resp.read().decode("utf-8")
        except urllib.error.URLError as exc:
            last = exc
    raise SystemExit(f"could not fetch schema from fruwehq/determa-state-spec: {last}")


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
