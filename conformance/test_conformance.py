"""Full format-1 core conformance gate."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from determa.state.validator import schema as bundled_schema

from .harness import CORE_DIR, CoreCase, core_cases, run_case


def _spec_schema() -> dict | None:
    override = os.environ.get("DETERMA_SPEC_DIR")
    if not override:
        return None
    path = Path(override) / "schema" / "machine.schema.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def test_suite_present() -> None:
    assert CORE_DIR.exists(), "pinned conformance suite is unavailable"
    assert len(core_cases()) == 75


def test_bundled_schema_matches_pinned_spec() -> None:
    upstream = _spec_schema()
    assert upstream is not None, "pinned specification is unavailable"
    assert bundled_schema() == upstream


@pytest.mark.parametrize("case", core_cases(), ids=lambda case: case.name)
def test_core_case(case: CoreCase) -> None:
    run_case(case)
