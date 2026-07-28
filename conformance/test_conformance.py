"""Full format-1 core conformance gate."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from determa.state import load_bundle
from determa.state.validator import schema as bundled_schema

from .harness import CORE_DIR, CoreCase, core_cases, run_case


def _spec_schema() -> dict | None:
    override = os.environ.get("DETERMA_SPEC_DIR")
    if not override:
        return None
    path = Path(override) / "schema" / "machine.schema.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _spec_root() -> Path | None:
    override = os.environ.get("DETERMA_SPEC_DIR")
    if not override:
        return None
    root = Path(override)
    return root if root.exists() else None


def test_suite_present() -> None:
    assert CORE_DIR.exists(), "pinned conformance suite is unavailable"
    assert len(core_cases()) == 88


def test_bundled_schema_matches_pinned_spec() -> None:
    upstream = _spec_schema()
    assert upstream is not None, "pinned specification is unavailable"
    assert bundled_schema() == upstream


def test_bundled_schema_is_valid_draft_2020_12() -> None:
    Draft202012Validator.check_schema(bundled_schema())


@pytest.mark.parametrize("name", ["minimal.yaml", "full.yaml"])
def test_authoritative_spec_examples_load_semantically(name: str) -> None:
    root = _spec_root()
    assert root is not None, "pinned specification is unavailable"

    bundle = load_bundle((root / "examples" / name).read_text(encoding="utf-8"))

    assert bundle.raw["format"] == 1


@pytest.mark.parametrize("case", core_cases(), ids=lambda case: case.name)
def test_core_case(case: CoreCase) -> None:
    run_case(case)
