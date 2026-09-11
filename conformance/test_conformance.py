"""Full format-1 core conformance gate."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator

from determa.state import PORTABLE_CODE_SETS, MemoryExecutionStore, load_bundle
from determa.state.validator import schema as bundled_schema
from determa.state.wire import artifact_schema

from .harness import CORE_DIR, CoreCase, conformance_root, core_cases, run_case
from .version2 import (
    _assert_checkpoint_unchanged,
    run_version2_vector,
    validate_version2_artifact,
    version2_vectors,
)

_VERSION2_ARTIFACT_KINDS = {
    "aggregate_state_v2",
    "migration_descriptor_v2",
    "aggregate_state_package_v2",
    "execution_checkpoint_v2",
    "core_step_result_v2",
}


def _version2_artifacts() -> list[tuple[Path, dict]]:
    paths = sorted({item.path for item in version2_vectors()})
    return [
        (path, artifact)
        for path in paths
        for artifact in (
            yaml.safe_load((path / "test.yaml").read_text(encoding="utf-8")) or {}
        ).get("artifacts", {}).get("documents", [])
        if artifact["kind"] in _VERSION2_ARTIFACT_KINDS
    ]


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
    assert len(core_cases()) == 98
    assert len(version2_vectors()) == 162


@pytest.mark.parametrize("stored", [b"mutated", None])
def test_v2_maintenance_failure_requires_exact_unchanged_checkpoint(
    stored: bytes | None,
) -> None:
    item = next(
        item
        for item in version2_vectors()
        if item.vector["name"] == "native_v2_maintenance_stale_writer"
    )
    before = json.loads(
        (item.path / item.vector["checkpoint_before"]).read_text(encoding="utf-8")
    )
    initial = {} if stored is None else {before["root_instance_id"]: stored}
    observation = {
        "store": MemoryExecutionStore(initial),
        "root_instance_id": before["root_instance_id"],
    }

    with pytest.raises(AssertionError):
        _assert_checkpoint_unchanged(item, observation)


def test_portable_code_sets_match_authoritative_registry() -> None:
    vector_path = (
        conformance_root()
        / "conformance"
        / "closed-code-registry"
        / "vectors.generated.json"
    )
    vectors = json.loads(vector_path.read_text(encoding="utf-8"))
    expected = {
        category["id"]: frozenset(category["codes"])
        for category in vectors["categories"]
        if category["id"] != "execution_store_failure"
    }

    missing_categories = sorted(set(expected) - set(PORTABLE_CODE_SETS))
    extra_categories = sorted(set(PORTABLE_CODE_SETS) - set(expected))
    mismatches = []
    for category in sorted(set(expected) & set(PORTABLE_CODE_SETS)):
        missing = sorted(expected[category] - PORTABLE_CODE_SETS[category])
        extra = sorted(PORTABLE_CODE_SETS[category] - expected[category])
        if missing or extra:
            mismatches.append(f"{category}: missing={missing}, extra={extra}")

    assert not (missing_categories or extra_categories or mismatches), "\n".join(
        [
            f"categories: missing={missing_categories}, extra={extra_categories}",
            *mismatches,
        ]
    )


def test_bundled_schema_matches_pinned_spec() -> None:
    upstream = _spec_schema()
    assert upstream is not None, "pinned specification is unavailable"
    assert bundled_schema() == upstream


@pytest.mark.parametrize(
    ("name", "kind"),
    [
        ("aggregate-state-v2.schema.json", "aggregate_state_v2"),
        ("migration-descriptor-v2.schema.json", "migration_descriptor_v2"),
        ("aggregate-state-package-v2.schema.json", "aggregate_state_package_v2"),
        ("execution-checkpoint-v2.schema.json", "execution_checkpoint_v2"),
        ("core-step-result-v2.schema.json", "core_step_result_v2"),
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
        "aggregate_state_v2",
        "migration_descriptor_v2",
        "aggregate_state_package_v2",
        "execution_checkpoint_v2",
        "core_step_result_v2",
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


@pytest.mark.parametrize("item", version2_vectors(), ids=lambda item: item.name)
def test_version2_vector(item) -> None:
    run_version2_vector(item)


@pytest.mark.parametrize(
    ("case", "artifact"),
    _version2_artifacts(),
)
def test_version2_artifact(case, artifact) -> None:
    path = case.path if isinstance(case, CoreCase) else case
    validate_version2_artifact(path, artifact)
