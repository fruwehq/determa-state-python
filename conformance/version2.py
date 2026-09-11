"""Driver for the pinned version-2 aggregate and checkpoint vectors."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from determa.state import (
    ArtifactError,
    ExecutionHost,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    load_bundle,
    restore_aggregate_package,
    serialize_execution_checkpoint,
)
from determa.state.checkpoint_v2 import (
    admit_checkpoint_v2,
    prune_checkpoint_v2,
    restore_execution_checkpoint_v2,
    step_checkpoint_v2,
    upgrade_checkpoint_v1_to_v2,
)
from determa.state.queueing import (
    admit_aggregate_v2,
    create_aggregate_v2,
    downgrade_aggregate_v2_to_v1,
    migrate_aggregate_v2,
    restore_aggregate_v2,
    step_aggregate_v2,
    upgrade_aggregate_v1_to_v2,
)
from determa.state.wire import load_json_artifact, migration_descriptor_digest

from .harness import conformance_root


@dataclass(frozen=True)
class Version2Vector:
    path: Path
    vector: dict[str, Any]

    @property
    def name(self) -> str:
        return f"{self.path.name}/{self.vector['name']}"


def version2_vectors() -> list[Version2Vector]:
    roots = [
        conformance_root() / "conformance" / "core",
        conformance_root() / "conformance" / "profiles" / "execution-checkpoint",
    ]
    result = []
    for root in roots:
        for path in sorted(root.iterdir()):
            test_path = path / "test.yaml"
            if not test_path.exists():
                continue
            test = yaml.safe_load(test_path.read_text(encoding="utf-8")) or {}
            result.extend(Version2Vector(path, item) for item in test.get("version2_vectors", []))
    return result


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _pointer(document: Any, pointer: str) -> Any:
    current = document
    for part in pointer.removeprefix("/").split("/"):
        current = current[part.replace("~1", "/").replace("~0", "~")]
    return current


def _resolver(path: Path) -> MemoryArtifactResolver:
    definitions = {}
    descriptors = {}
    definition_root = path.parent if path.parent.name == "execution-checkpoint" else path
    for candidate in definition_root.glob("**/*.yaml"):
        try:
            bundle = load_bundle(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        definitions[bundle.fingerprint] = bundle
    for candidate in path.glob("*descriptor*.json"):
        document = _json(candidate)
        try:
            descriptors[migration_descriptor_digest(document)] = document
        except ArtifactError:
            continue
    return MemoryArtifactResolver(definitions=definitions, migration_descriptors=descriptors)


def _invoke(item: Version2Vector, request: dict[str, Any]) -> Any:
    path = item.path
    vector = item.vector
    operation = vector["operation"]
    resolver = _resolver(path)
    before_name = vector.get("state_before") or vector.get("checkpoint_before")
    before = _json(path / before_name) if before_name else None
    bundle = (
        load_bundle((path / vector["bundle"]).read_text(encoding="utf-8"))
        if vector.get("bundle")
        else None
    )
    if operation == "create_v2":
        assert bundle is not None
        return create_aggregate_v2(
            bundle,
            request["machine_id"],
            request["root_instance_id"],
            request["creation_id"],
            request["bindings"],
        )["state"]
    if operation == "admit_v2":
        return admit_aggregate_v2(before, request["deliveries"], resolver)
    if operation == "step_v2":
        return step_aggregate_v2(before, request["target_runtime_id"], resolver)
    if operation == "upgrade_aggregate_v1_to_v2":
        return upgrade_aggregate_v1_to_v2(before, resolver)
    if operation == "downgrade_aggregate_v2_to_v1":
        return downgrade_aggregate_v2_to_v1(before, resolver)
    if operation == "migrate_aggregate_v2":
        return migrate_aggregate_v2(
            before,
            request["target_bundle"]["validated_bundle_fingerprint"],
            request["migration_descriptor_digest_route"],
            resolver,
            maintenance_mode=request["maintenance_mode"],
        )
    if operation == "upgrade_checkpoint_v1_to_v2":
        return upgrade_checkpoint_v1_to_v2(before, resolver)
    if operation == "checkpoint_admit_v2":
        return admit_checkpoint_v2(
            before,
            request["deliveries"],
            resolver,
            expected_revision=request["expected_revision"],
            expected_checkpoint_digest=request["expected_checkpoint_digest"],
        )
    if operation == "checkpoint_step_v2":
        return step_checkpoint_v2(
            before,
            request["target_runtime_id"],
            resolver,
            expected_revision=request["expected_revision"],
            expected_checkpoint_digest=request["expected_checkpoint_digest"],
        )
    if operation == "checkpoint_prune_v2":
        return prune_checkpoint_v2(
            before,
            request["cutoff_receipt_sequence"],
            resolver,
            expected_revision=request["expected_revision"],
            expected_checkpoint_digest=request["expected_checkpoint_digest"],
        )
    if operation == "checkpoint_v1_accept":
        assert bundle is not None
        delivery = request["deliveries"][0]
        envelope = {
            key: copy.deepcopy(value)
            for key, value in delivery["envelope"].items()
            if key not in {"cause_id", "source"}
        }
        candidate = {
            "root_instance_id": before["root_instance_id"],
            "delivery_mode": delivery["delivery_mode"],
            "origin": {"kind": "host_input"},
            "envelope": envelope,
        }
        host = ExecutionHost(
            MemoryExecutionStore(
                {before["root_instance_id"]: serialize_execution_checkpoint(before)}
            ),
            resolver,
        )
        result = host.accept_delivery(
            before["root_instance_id"],
            candidate,
            expected_revision=request["expected_revision"],
            expected_checkpoint_digest=request["expected_checkpoint_digest"],
            selected_bundle=bundle,
        )
        if result["result"] == "not_accepted":
            raise ArtifactError(result["failure"]["code"])
        return result
    raise AssertionError(f"unsupported version-2 operation: {operation}")


def run_version2_vector(item: Version2Vector) -> None:
    vector = item.vector
    request = copy.deepcopy(
        _pointer(_json(item.path / vector["request_file"]), vector["request_pointer"])
    )
    request_snapshot = copy.deepcopy(request)
    try:
        actual = _invoke(item, request)
        code = None
    except ArtifactError as error:
        actual = None
        code = error.code
    expected = vector["expect"]
    if isinstance(actual, dict) and actual.get("result") == "rejected":
        code = actual["rejection"]["code"]
        actual = None
    if expected["result"] == "failure":
        assert code == expected["code"]
    else:
        assert code is None
        assert actual == _json(item.path / expected["exact_result_file"])
    if expected.get("caller_still_owns_input"):
        assert request == request_snapshot


def validate_version2_artifact(path: Path, artifact: dict[str, Any]) -> None:
    resolver = _resolver(path)
    source = (path / artifact["file"]).read_bytes()
    try:
        if artifact["kind"] == "aggregate_state_v2":
            restored = restore_aggregate_v2(source, resolver)
            if artifact.get("canonical_of"):
                assert restored.canonical_bytes == source
        elif artifact["kind"] == "execution_checkpoint_v2":
            restore_execution_checkpoint_v2(source, resolver)
        elif artifact["kind"] == "aggregate_state_package_v2":
            restore_aggregate_package(source, resolver)
        elif artifact["kind"] in {
            "migration_descriptor_v2",
            "core_step_result_v2",
        }:
            load_json_artifact(source, artifact["kind"])
        else:
            return
        code = None
    except ArtifactError as error:
        code = error.code
        if artifact["valid"] and code == "source_definition_unavailable":
            code = None
        if artifact["kind"] == "core_step_result_v2" and code == "invalid_aggregate_state":
            code = "invalid_core_step_result"
    assert code == (None if artifact["valid"] else artifact["error"])
