"""Full format-1 core conformance gate."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator

from determa.state import PORTABLE_CODE_SETS, load_bundle
from determa.state.validator import schema as bundled_schema
from determa.state.wire import artifact_schema

from .execution_checkpoint import (
    _actual_scope_maps,
    _assert_complete_scope_maps,
    _expected_scope_maps,
    _scope_hosts,
    _scope_state,
    execution_checkpoint_cases,
    execution_checkpoint_vectors,
    run_execution_checkpoint_vector,
    validate_execution_checkpoint_artifact,
)
from .harness import CORE_DIR, CoreCase, conformance_root, core_cases, run_case
from .persistence import persistence_vector_cases, run_persistence_vectors
from .persistence_profiles import (
    persistence_profile_cases,
    run_persistence_profile,
)
from .version2 import (
    run_version2_vector,
    validate_version2_artifact,
    version2_vectors,
)


def _load_case(case: CoreCase) -> dict:
    return yaml.safe_load(case.test_file.read_text(encoding="utf-8")) or {}


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
    assert len(core_cases()) == 114
    assert len(execution_checkpoint_vectors()) == 99
    assert len(version2_vectors()) == 103


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


def _scope_map_pair() -> tuple[dict, dict]:
    item = next(
        item
        for item in execution_checkpoint_vectors()
        if item.vector["name"] == "pending_outbox_update_in_scope_a"
    )
    before = _scope_state(item.case, item.vector["scope_state_before"])
    hosts, stores = _scope_hosts(item.case, before, [])
    selected_scope = next(
        scope
        for scope in before["scopes"]
        if scope["logical_scope_id"] == "scope-a"
    )
    root_instance_id, binding = next(iter(selected_scope["checkpoints"].items()))
    checkpoint = json.loads(
        (item.case.path / binding["file"]).read_text(encoding="utf-8")
    )
    hosts["scope-a"].update_pending_outbox(
        root_instance_id,
        item.vector["effect_id"],
        item.vector["desired_pending_state"],
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    after = _scope_state(item.case, item.vector["scope_state_after"])
    return (
        _actual_scope_maps(hosts, stores),
        _expected_scope_maps(item.case, after),
    )


def test_scope_map_rejects_omitted_unchanged_scope_records() -> None:
    actual, expected = _scope_map_pair()
    root_instance_id = next(iter(expected["scope-b"]["checkpoints"]))
    expected["scope-b"]["checkpoints"].pop(root_instance_id)
    expected["scope-b"]["outbox_records"].pop(root_instance_id)

    with pytest.raises(AssertionError):
        _assert_complete_scope_maps(actual, expected)


def test_scope_map_rejects_unexpected_checkpoint() -> None:
    actual, expected = _scope_map_pair()
    actual["scope-a"]["checkpoints"]["unexpected-root"] = b"unexpected"

    with pytest.raises(AssertionError):
        _assert_complete_scope_maps(actual, expected)


def test_scope_map_rejects_unexpected_outbox_record() -> None:
    actual, expected = _scope_map_pair()
    root_instance_id = next(iter(actual["scope-a"]["outbox_records"]))
    actual["scope-a"]["outbox_records"][root_instance_id].append(
        {
            "effect_id": "unexpected-effect",
            "record_kind": "pending",
            "source_digest": "sha256:unexpected",
        }
    )

    with pytest.raises(AssertionError):
        _assert_complete_scope_maps(actual, expected)


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
        "aggregate_state",
        "migration_descriptor",
        "aggregate_state_package",
        "execution_checkpoint",
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


@pytest.mark.parametrize("item", version2_vectors(), ids=lambda item: item.name)
def test_version2_vector(item) -> None:
    run_version2_vector(item)


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


@pytest.mark.parametrize(
    ("case", "artifact"),
    [
        (case, artifact)
        for case in core_cases()
        for artifact in (_load_case(case).get("artifacts", {}).get("documents", []))
        if artifact["kind"]
        in {
            "aggregate_state_v2",
            "migration_descriptor_v2",
            "aggregate_state_package_v2",
            "execution_checkpoint_v2",
            "core_step_result_v2",
        }
    ]
    + [
        (case.path, artifact)
        for case in execution_checkpoint_cases()
        for artifact in case.test.get("artifacts", {}).get("documents", [])
        if artifact["kind"]
        in {
            "aggregate_state_v2",
            "migration_descriptor_v2",
            "aggregate_state_package_v2",
            "execution_checkpoint_v2",
            "core_step_result_v2",
        }
    ],
)
def test_version2_artifact(case, artifact) -> None:
    path = case.path if isinstance(case, CoreCase) else case
    validate_version2_artifact(path, artifact)
