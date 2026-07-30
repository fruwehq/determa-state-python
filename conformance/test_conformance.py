"""Full format-1 core conformance gate."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from determa.state import load_bundle
from determa.state.validator import schema as bundled_schema
from determa.state.wire import artifact_schema

from .execution_checkpoint import (
    execution_checkpoint_cases,
    execution_checkpoint_vectors,
    run_execution_checkpoint_vector,
    validate_execution_checkpoint_artifact,
)
from .harness import CORE_DIR, CoreCase, core_cases, run_case
from .persistence import persistence_vector_cases, run_persistence_vectors
from .persistence_profiles import (
    persistence_profile_cases,
    run_persistence_profile,
)


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
    assert len(core_cases()) == 110
    assert len(execution_checkpoint_vectors()) == 83


def test_bundled_schema_matches_pinned_spec() -> None:
    upstream = _spec_schema()
    assert upstream is not None, "pinned specification is unavailable"
    assert bundled_schema() == upstream


@pytest.mark.parametrize(
    ("name", "kind"),
    [
        ("aggregate-state.schema.json", "aggregate_state"),
        ("migration-descriptor.schema.json", "migration_descriptor"),
        ("aggregate-state-package.schema.json", "aggregate_state_package"),
        ("execution-checkpoint.schema.json", "execution_checkpoint"),
    ],
)
def test_bundled_artifact_schemas_match_pinned_spec(name: str, kind: str) -> None:
    root = _spec_root()
    assert root is not None, "pinned specification is unavailable"
    upstream = json.loads((root / "schema" / name).read_text(encoding="utf-8"))
    assert artifact_schema(kind) == upstream


@pytest.mark.parametrize(
    "kind",
    [
        "aggregate_state",
        "migration_descriptor",
        "aggregate_state_package",
        "execution_checkpoint",
    ],
)
def test_bundled_artifact_schema_is_valid_draft_2020_12(kind: str) -> None:
    Draft202012Validator.check_schema(artifact_schema(kind))


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


@pytest.mark.parametrize(
    "case", persistence_vector_cases(core_cases()), ids=lambda case: case.name
)
def test_persistence_vectors(case: CoreCase) -> None:
    run_persistence_vectors(case)


@pytest.mark.parametrize(
    "case", persistence_profile_cases(), ids=lambda case: case.name
)
def test_persistence_profile(case) -> None:
    run_persistence_profile(case)


@pytest.mark.parametrize(
    "item", execution_checkpoint_vectors(), ids=lambda item: item.name
)
def test_execution_checkpoint_profile(item) -> None:
    run_execution_checkpoint_vector(item)


@pytest.mark.parametrize(
    ("case", "artifact"),
    [
        (case, artifact)
        for case in execution_checkpoint_cases()
        for artifact in case.test["artifacts"]["documents"]
        if artifact["kind"] == "execution_checkpoint"
    ],
    ids=lambda value: (
        value.name
        if hasattr(value, "name")
        else value["file"]
        if isinstance(value, dict)
        else None
    ),
)
def test_execution_checkpoint_artifact(case, artifact) -> None:
    validate_execution_checkpoint_artifact(case, artifact)
