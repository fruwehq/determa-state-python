"""Driver for the pinned version-2 aggregate and checkpoint vectors."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import yaml

from determa.state import (
    ArtifactError,
    ExecutionHost,
    ExecutionHostError,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    MigrationLimits,
    load_bundle,
    restore_aggregate_package,
    serialize_execution_checkpoint,
)
from determa.state.checkpoint_v2 import (
    admit_checkpoint_v2,
    prune_checkpoint_v2,
    restore_execution_checkpoint_v2,
    step_checkpoint_v2,
)
from determa.state.queueing import (
    admit_aggregate_v2,
    create_aggregate_v2,
    migrate_aggregate_v2,
    restore_aggregate_v2,
    step_aggregate_v2,
)
from determa.state.wire import canonical_bytes, load_json_artifact, migration_descriptor_digest

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


@cache
def _conformance_definitions() -> dict[str, Any]:
    definitions = {}
    for candidate in (conformance_root() / "conformance").glob("**/*.yaml"):
        try:
            bundle = load_bundle(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        definitions[bundle.fingerprint] = bundle
    return definitions


def _resolver(
    path: Path, request: dict[str, Any] | None = None
) -> MemoryArtifactResolver:
    request = request or {}
    specification = request.get("artifact_resolver") or request.get("definition_resolver")
    if specification is None:
        descriptors = {}
        for candidate in path.glob("*descriptor*.json"):
            document = _json(candidate)
            try:
                descriptors[migration_descriptor_digest(document)] = document
            except ArtifactError:
                continue
        return MemoryArtifactResolver(
            definitions=_conformance_definitions(), migration_descriptors=descriptors
        )
    definitions = {
        item["validated_bundle_fingerprint"]: (path / item["bundle_file"]).read_text(
            encoding="utf-8"
        )
        for item in specification.get("definitions", [])
    }
    descriptors = {
        item["migration_descriptor_digest"]: _json(path / item["descriptor_file"])
        for item in specification.get("migration_descriptors", [])
    }
    return MemoryArtifactResolver(
        definitions=definitions,
        migration_descriptors=descriptors,
        trusted_definitions=[
            item["validated_bundle_fingerprint"]
            for item in specification.get("definitions", [])
            if item.get("trusted")
        ],
        trusted_migration_descriptors=[
            item["migration_descriptor_digest"]
            for item in specification.get("migration_descriptors", [])
            if item.get("trusted")
        ],
    )


def _invoke(
    item: Version2Vector,
    request: dict[str, Any],
    observation: dict[str, Any] | None = None,
) -> Any:
    path = item.path
    vector = item.vector
    operation = vector["operation"]
    resolver = _resolver(path, request)
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
    if operation == "round_trip_aggregate_v2":
        return restore_aggregate_v2(before, resolver).aggregate_envelope
    if operation == "restore_package_v2":
        package = _json(path / vector["package_file"])
        restored_package = restore_aggregate_package(package, resolver)
        if vector["name"] == "put_if_absent_is_idempotent":
            restored_package = restore_aggregate_package(package, resolver)
        if vector["name"] not in {
            "attachments_seed_empty_resolver_and_drive_route",
            "put_if_absent_is_idempotent",
        }:
            return restored_package.aggregate.aggregate_envelope
        descriptor = resolver.resolve_migration_descriptor(
            restored_package.migration_route[-1]
        )
        assert descriptor is not None
        target, _ = load_json_artifact(descriptor, "migration_descriptor_v2")
        return migrate_aggregate_v2(
            restored_package.aggregate.aggregate_envelope,
            target["target_validated_bundle_fingerprint"],
            restored_package.migration_route,
            resolver,
            maintenance_mode=request["maintenance_mode"],
        )
    if operation == "migrate_aggregate_v2":
        return migrate_aggregate_v2(
            before,
            request.get("target_bundle", {}).get("validated_bundle_fingerprint"),
            request.get("migration_descriptor_digest_route"),
            resolver,
            maintenance_mode=request.get("maintenance_mode"),
            resource_limits=(
                MigrationLimits.from_mapping(request["resource_limits"])
                if "resource_limits" in request
                else None
            ),
        )
    if operation == "migrate_then_process_v2":
        migrated = migrate_aggregate_v2(
            before,
            request["target_bundle"]["validated_bundle_fingerprint"],
            request["migration_descriptor_digest_route"],
            resolver,
            maintenance_mode=request["maintenance_mode"],
        )
        admitted = admit_aggregate_v2(
            migrated["aggregate_state"], [request["delivery"]], resolver
        )
        if admitted["result"] != "accepted":
            processing = {
                "core_step_result_format": "determa.core_step_result",
                "core_step_result_schema_version": 2,
                "status": admitted["status"],
                "disposition": "rejected",
                "state": admitted["state"],
                "emissions": [],
                "lifecycle_dispositions": [],
                "fault": None,
                "rejection": admitted["rejection"],
            }
        else:
            target = request["delivery"]["envelope"]["target"]
            if "root" in target:
                runtime_id = target["root"]["root_runtime_id"]
            elif "component" in target:
                runtime_id = target["component"]["component_runtime_id"]
            else:
                runtime_id = target["spawned_instance"]["instance_id"]
            processing = step_aggregate_v2(admitted["state"], runtime_id, resolver)
        return {
            "result": "migrated_and_processed",
            "migration_audit_records": migrated["audit_records"],
            "processing": processing,
        }
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
    if operation == "checkpoint_migrate_v2":
        store = MemoryExecutionStore(
            {before["root_instance_id"]: serialize_execution_checkpoint(before)}
        )
        if observation is not None:
            observation["store"] = store
            observation["root_instance_id"] = before["root_instance_id"]
        host = ExecutionHost(store, resolver)
        result = host.maintenance_migration_v2(
            before["root_instance_id"],
            request["operation_id"],
            request["target_bundle"]["validated_bundle_fingerprint"],
            request["migration_descriptor_digest_route"],
            expected_revision=request["expected_revision"],
            expected_checkpoint_digest=request["expected_checkpoint_digest"],
            maintenance_mode=request["maintenance_mode"],
        )
        expected_after = vector.get("checkpoint_after")
        if expected_after is not None:
            restored = host.read_checkpoint(before["root_instance_id"])
            assert restored is not None
            assert restored.document == _json(path / expected_after)
        return result
    raise AssertionError(f"unsupported version-2 operation: {operation}")


def _assert_checkpoint_unchanged(
    item: Version2Vector, observation: dict[str, Any]
) -> None:
    store = observation.get("store")
    root_instance_id = observation.get("root_instance_id")
    assert isinstance(store, MemoryExecutionStore)
    assert isinstance(root_instance_id, str)
    with store.transaction(root_instance_id) as transaction:
        actual = transaction.load()
    unchanged_file = item.vector["expect"]["unchanged_file"]
    assert actual == (item.path / unchanged_file).read_bytes()


def run_version2_vector(item: Version2Vector) -> None:
    vector = item.vector
    request = copy.deepcopy(
        _pointer(_json(item.path / vector["request_file"]), vector["request_pointer"])
    )
    request_snapshot = copy.deepcopy(request)
    observation: dict[str, Any] = {}
    try:
        actual = _invoke(item, request, observation)
        code = None
    except (ArtifactError, ExecutionHostError) as error:
        actual = None
        code = error.code
    expected = vector["expect"]
    if isinstance(actual, dict) and actual.get("result") == "rejected":
        code = actual["rejection"]["code"]
        actual = None
    if expected["result"] == "failure":
        assert code == expected["code"]
        if vector["operation"] == "checkpoint_migrate_v2" and expected.get(
            "unchanged_file"
        ):
            _assert_checkpoint_unchanged(item, observation)
    else:
        assert code is None
        assert actual == _json(item.path / expected["exact_result_file"])
    if expected.get("caller_still_owns_input"):
        assert request == request_snapshot


def validate_version2_artifact(path: Path, artifact: dict[str, Any]) -> None:
    resolver = _resolver(path)
    source = (path / artifact["file"]).read_bytes()
    try:
        if artifact["valid"]:
            document, _ = load_json_artifact(source, artifact["kind"])
            if artifact.get("canonical_of"):
                assert canonical_bytes(document) == source
        elif artifact["kind"] == "aggregate_state_v2":
            restore_aggregate_v2(source, resolver)
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
    assert code == (None if artifact["valid"] else artifact["error"])
