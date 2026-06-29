"""Conformance suite — step 1 gate.

Every machine definition in the upstream suite MUST load and validate against
the bundled schema + reserved-name rules (SPEC §2/§9). This is the step-1
gate; the run-to-expect harness grows as engine capabilities land.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from harel import load_definitions
from harel.validator import schema as bundled_schema

from .harness import SUITE_DIR, cli_cases, engine_cases


def _each_machine_file() -> list[pytest.Param]:
    params: list[pytest.Param] = []
    for case in engine_cases():
        for mf in case.machine_files:
            label = f"{case.name}:{mf.name}"
            params.append(pytest.param(mf, id=label))
    for case in cli_cases():
        mf = case / "machine.yaml"
        if mf.exists():
            params.append(pytest.param(mf, id=f"cli/{case.name}"))
    return params


@pytest.mark.parametrize("path", _each_machine_file())
def test_machine_file_loads_and_validates(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    defs = load_definitions(text)
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
