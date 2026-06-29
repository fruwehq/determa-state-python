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
from pathlib import Path

import pytest

from harel import load_definitions
from harel.validator import schema as bundled_schema

from .harness import (
    SUITE_DIR,
    SUPPORTED,
    cli_cases,
    engine_cases,
    run_engine_case,
)


def _each_machine_file() -> list[pytest.Param]:
    params: list[pytest.Param] = []
    for case in engine_cases():
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


def test_bundled_schema_matches_submodule() -> None:
    """The engine's bundled schema must equal the spec repo's schema (no drift)."""
    upstream = SUITE_DIR / "schema" / "machine.schema.json"
    assert upstream.exists(), "harel submodule not initialized"
    assert json.loads(upstream.read_text(encoding="utf-8")) == bundled_schema()


def test_suite_present() -> None:
    assert len(engine_cases()) == 22, "expected 22 engine cases"
    assert len(cli_cases()) == 1, "expected 1 CLI case"


@pytest.mark.parametrize("case", engine_cases(), ids=lambda c: c.name)
def test_engine_case(case) -> None:  # type: ignore[no-untyped-def]
    if case.name not in SUPPORTED:
        pytest.skip(f"not yet supported: {case.name}")
    run_engine_case(case)
