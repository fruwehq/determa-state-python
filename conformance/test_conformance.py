"""Conformance suite gates.

Two layers:

1. **Step-1 gate** — every machine definition in the upstream suite MUST load
   and validate (SPEC §2/§9), and the bundled schema must not drift from the
   spec repo's.
2. **Engine gate** — each supported case is run end-to-end (create root,
   ``send`` to quiescence, check ``expect``). Unsupported cases are skipped
   until their features land; see ``harness.SUPPORTED``.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

import pytest

import harel
from harel import load_definitions
from harel.validator import schema as bundled_schema

from .harness import (
    CONFORMANCE_DIR,
    SUPPORTED,
    cli_cases,
    engine_cases,
    run_cli_case,
    run_engine_case,
)


def _spec_schema() -> dict | None:
    """The normative schema from fruwehq/harel at the matching tag (or a local override).

    Returns ``None`` when offline and no ``HAREL_SPEC_DIR`` override is set, so the
    drift test can skip rather than fail.
    """
    override = os.environ.get("HAREL_SPEC_DIR")
    if override:
        p = Path(override) / "schema" / "machine.schema.json"
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None
    for ref in (f"v{harel.__version__}", "main"):
        url = f"https://raw.githubusercontent.com/fruwehq/harel/{ref}/schema/machine.schema.json"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:  # noqa: S310 (fixed host)
                return json.loads(resp.read())
        except (urllib.error.URLError, OSError, ValueError):
            continue
    return None


def _each_machine_file() -> list[pytest.Param]:
    import yaml

    params: list[pytest.Param] = []
    for case in engine_cases():
        # A `static: { valid: false }` case may hold a deliberately invalid machine
        # (it must NOT load cleanly), so exclude it from the "loads and validates" gate.
        test = yaml.safe_load(case.test_file.read_text(encoding="utf-8")) or {}
        if test.get("static", {}).get("valid") is False:
            continue
        for mf in case.machine_files:
            params.append(pytest.param(mf, id=f"{case.name}:{mf.name}"))
    for case in cli_cases():
        mf = case / "machine.yaml"
        if mf.exists():
            params.append(pytest.param(mf, id=f"cli/{case.name}"))
    return params


@pytest.mark.parametrize("path", _each_machine_file())
def test_machine_file_loads_and_validates(path: Path) -> None:
    defs = load_definitions(path.read_text(encoding="utf-8"))
    assert defs, f"{path}: no definitions loaded"
    for d in defs:
        assert d.id == d.raw["id"]


def test_bundled_schema_matches_spec() -> None:
    """The engine's bundled schema must equal the spec repo's schema (no drift)."""
    upstream = _spec_schema()
    if upstream is None:
        pytest.skip("spec schema unavailable (offline; set HAREL_SPEC_DIR to a harel checkout)")
    assert upstream == bundled_schema()


def test_suite_present() -> None:
    if not CONFORMANCE_DIR.exists():
        pytest.skip("conformance suite not fetched (offline; set HAREL_CONFORMANCE_DIR)")
    assert len(engine_cases()) == 25, "expected 25 engine cases"
    assert len(cli_cases()) == 3, "expected 3 CLI cases"


@pytest.mark.parametrize("case", engine_cases(), ids=lambda c: c.name)
def test_engine_case(case) -> None:  # type: ignore[no-untyped-def]
    if case.name not in SUPPORTED:
        pytest.skip(f"not yet supported: {case.name}")
    run_engine_case(case)


@pytest.mark.parametrize("case", cli_cases(), ids=lambda c: f"cli/{c.name}")
def test_cli_case(case) -> None:  # type: ignore[no-untyped-def]
    run_cli_case(case)
