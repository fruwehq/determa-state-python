"""Driver for the optional execution-checkpoint host profile."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

import determa.state.host as host_module
from determa.state import (
    ArtifactError,
    ExecutionHost,
    ExecutionHostError,
    ExecutionStore,
    ExecutionStoreError,
    ExecutionStoreRegistry,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    load_bundle,
    register_bundled_execution_stores,
    restore_execution_checkpoint,
    serialize_execution_checkpoint,
)

from .harness import conformance_root

PROFILE_DIR = (
    conformance_root() / "conformance" / "profiles" / "execution-checkpoint"
)


@dataclass(frozen=True)
class ExecutionCheckpointCase:
    name: str
    path: Path

    @property
    def test(self) -> dict[str, Any]:
        return yaml.safe_load((self.path / "test.yaml").read_text(encoding="utf-8"))


@dataclass(frozen=True)
class ExecutionCheckpointVector:
    case: ExecutionCheckpointCase
    vector: dict[str, Any]

    @property
    def name(self) -> str:
        return f"{self.case.name}/{self.vector['name']}"


def execution_checkpoint_cases() -> list[ExecutionCheckpointCase]:
    if not PROFILE_DIR.exists():
        return []
    return [
        ExecutionCheckpointCase(path.name, path)
        for path in sorted(PROFILE_DIR.iterdir())
        if path.is_dir() and (path / "test.yaml").exists()
    ]


def execution_checkpoint_vectors() -> list[ExecutionCheckpointVector]:
    return [
        ExecutionCheckpointVector(case, vector)
        for case in execution_checkpoint_cases()
        for vector in case.test["execution_checkpoint_profile"]["vectors"]
    ]


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _pointer(document: Any, pointer: str) -> Any:
    current = document
    for part in pointer.removeprefix("/").split("/"):
        current = current[part.replace("~1", "/").replace("~0", "~")]
    return current


def _resolver(case: ExecutionCheckpointCase) -> MemoryArtifactResolver:
    definitions = {}
    descriptors = {}
    for path in case.path.glob("*.yaml"):
        try:
            bundle = load_bundle(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        definitions[bundle.fingerprint] = bundle
    from determa.state.wire import migration_descriptor_digest

    for path in case.path.glob("*migration-descriptor*.json"):
        descriptor = _json(path)
        descriptors[migration_descriptor_digest(descriptor)] = descriptor
    return MemoryArtifactResolver(
        definitions=definitions, migration_descriptors=descriptors
    )


def _checkpoint_root(path: Path) -> str:
    return str(_json(path)["root_instance_id"])


def _delivery_candidate(request: dict[str, Any]) -> dict[str, Any] | None:
    if "candidate" in request:
        return request["candidate"]
    return {
        name: copy.deepcopy(request[name])
        for name in (
            "root_instance_id",
            "delivery_mode",
            "origin",
            "envelope",
            "envelope_digest",
        )
        if name in request
    }


def _fault_injector(boundary: str | None) -> Any:
    def inject(actual: str) -> None:
        if actual == boundary == "before_commit":
            raise ExecutionHostError("injected_pre_commit_failure")
        if actual == boundary == "after_commit_before_response":
            raise ExecutionHostError("response_lost_after_commit")

    return inject


def _invoke_host(
    host: ExecutionHost,
    operation: str,
    root_instance_id: str,
    request: dict[str, Any],
) -> dict[str, Any]:
    expected = {
        "expected_revision": request.get("expected_revision", ""),
        "expected_checkpoint_digest": request.get(
            "expected_checkpoint_digest", ""
        ),
    }
    if operation == "create":
        bundle = load_bundle(
            (Path(request["_case_path"]) / request["bundle_file"]).read_text(
                encoding="utf-8"
            )
        )
        return host.create(
            bundle,
            request["machine_id"],
            request["root_instance_id"],
            request["creation_id"],
            request["bindings"],
        )
    if operation == "accept_delivery":
        return host.accept_delivery(
            root_instance_id, _delivery_candidate(request), **expected
        )
    if operation == "process_pending_delivery":
        return host.process_pending_delivery(
            root_instance_id, _delivery_candidate(request), **expected
        )
    if operation == "foreground_process_delivery":
        return host.foreground_process_delivery(
            root_instance_id, _delivery_candidate(request), **expected
        )
    if operation == "maintenance_migration":
        return host.maintenance_migration(
            root_instance_id,
            request["operation_id"],
            request["target_validated_bundle_fingerprint"],
            request["migration_descriptor_digest_route"],
            source_aggregate_state_digest=request[
                "source_aggregate_state_digest"
            ],
            maintenance_mode=request["maintenance_mode"],
            **expected,
        )
    if operation == "update_pending_outbox":
        return host.update_pending_outbox(
            root_instance_id,
            request["effect_id"],
            request["desired_pending_state"],
            **expected,
        )
    if operation == "terminalize_outbox":
        return host.terminalize_outbox(
            root_instance_id,
            request["effect_id"],
            request["terminal_outcome"],
            **expected,
        )
    if operation == "compact_outbox":
        return host.compact_outbox(
            root_instance_id, request["effect_id"], **expected
        )
    if operation == "delete_outbox_record":
        return host.delete_outbox_record(
            root_instance_id, request["effect_id"], **expected
        )
    if operation == "update_replay_retention":
        return host.update_replay_retention(
            root_instance_id,
            request["target_replay_retention"],
            **expected,
        )
    if operation == "tombstone_root":
        return host.tombstone_root(
            root_instance_id, request["operation_id"], **expected
        )
    if operation == "delete_checkpoint":
        return host.delete_checkpoint(root_instance_id, **expected)
    raise AssertionError(f"unsupported host operation {operation}")


class _StaticStore(ExecutionStore):
    def __init__(
        self, capabilities: list[str], checkpoint_retention_mode: str = "permanent"
    ) -> None:
        self._capabilities = frozenset(capabilities)
        self._checkpoint_retention_mode = checkpoint_retention_mode

    @property
    def capabilities(self) -> frozenset[str]:
        return self._capabilities

    @property
    def checkpoint_retention_mode(self) -> str:
        return self._checkpoint_retention_mode

    def transaction(
        self,
        root_instance_id: str,
    ) -> Any:
        del root_instance_id
        raise AssertionError("profile-only store must not process roots")

    def setup_schema(self) -> None:
        return None

    def health(self) -> dict[str, Any]:
        return {"healthy": True}


def _adapter_operation(vector: dict[str, Any]) -> dict[str, Any]:
    operation = vector["operation"]
    if operation == "inject_execution_store":
        ExecutionHost(MemoryExecutionStore(), MemoryArtifactResolver())
        return {"result": "accepted"}
    capabilities = vector.get("advertised_capabilities", [])
    requested = set(vector.get("requested_capabilities", []))
    if operation == "validate_host_profile":
        ExecutionHost(
            _StaticStore(capabilities, vector["checkpoint_retention_mode"]),
            MemoryArtifactResolver(),
            required_capabilities=requested,
            profile=vector["host_profile"],
            host_features=frozenset(vector["host_features"]),
        )
        return {"result": "accepted"}

    registry = ExecutionStoreRegistry()
    identifier = vector["adapter_identifier"]

    def static_factory(uri: str, configuration: dict[str, Any]) -> ExecutionStore:
        del uri
        if not vector["configuration_valid"] or configuration:
            raise ExecutionStoreError("invalid_adapter_configuration")
        return _StaticStore(capabilities)

    if operation == "register_adapter":
        if identifier == "memory":
            register_bundled_execution_stores(registry)
            store = registry.resolve(
                "memory:", required_capabilities=frozenset(requested)
            )
            assert store.capabilities == frozenset(capabilities)
        else:
            registry.register(identifier, static_factory)
            registry.register(identifier, static_factory)
        return {"result": "accepted"}

    uri = {
        "memory": "memory:",
        "file": "file:///tmp/unused",
        "sqlite": "sqlite:///tmp/unused.sqlite",
        "postgresql": "postgresql://unused",
    }.get(identifier, f"{identifier}:")
    if vector["registration_source"] == "bundled":
        register_bundled_execution_stores(registry)
    elif identifier != "absent-store":
        registry.register(identifier, static_factory)
    store = registry.resolve(uri, required_capabilities=frozenset(requested))
    assert store.capabilities == frozenset(capabilities)
    return {"result": "accepted"}


def _expected_response(
    vector: dict[str, Any],
    checkpoint: dict[str, Any] | None,
    request: dict[str, Any],
) -> dict[str, Any]:
    expected = vector["expect"]
    result = expected["result"]
    if result in {"failure", "response_lost", "not_accepted", "unsupported"}:
        return {"result": result, "failure": {"code": expected["code"]}}
    if result == "accepted":
        return {"result": "accepted"}
    assert checkpoint is not None
    if result == "pending":
        pending = next(
            item
            for item in checkpoint["pending_deliveries"]
            if item["delivery_sequence"] == expected["delivery_sequence"]
        )
        return {
            "result": "pending",
            "event_id": pending["envelope"]["event_id"],
            "delivery_sequence": pending["delivery_sequence"],
            "accepted_revision": pending["accepted_revision"],
        }
    if result == "tombstoned":
        return {
            "result": "tombstoned",
            "tombstone": copy.deepcopy(checkpoint["root_record"]),
        }
    assert result == "committed"
    operation = vector["operation"]
    if "receipt_sequence" in expected:
        receipt = next(
            item
            for item in checkpoint["operation_receipts"]
            if item["receipt_sequence"] == expected["receipt_sequence"]
        )
        return {"result": "committed", "receipt": copy.deepcopy(receipt)}
    if operation == "update_pending_outbox":
        record = next(
            item
            for item in checkpoint["pending_outbox_intents"]
            if item["intent"]["effect_id"] == request["effect_id"]
        )
        return {"result": "committed", "record": copy.deepcopy(record)}
    if operation == "terminalize_outbox":
        records = [
            *checkpoint["terminal_outbox_records"],
            *checkpoint["outbox_effect_tombstones"],
        ]
        record = next(
            item
            for item in records
            if (
                item["intent"]["effect_id"]
                if "intent" in item
                else item["effect_id"]
            )
            == request["effect_id"]
        )
        return {"result": "committed", "record": copy.deepcopy(record)}
    if operation == "compact_outbox":
        record = next(
            item
            for item in checkpoint["outbox_effect_tombstones"]
            if item["effect_id"] == request["effect_id"]
        )
        return {"result": "committed", "record": copy.deepcopy(record)}
    if operation == "update_replay_retention":
        return {
            "result": "committed",
            "replay_retention": copy.deepcopy(checkpoint["replay_retention"]),
        }
    if operation == "delete_outbox_record":
        return {"result": "committed"}
    raise AssertionError(f"no exact response projection for {operation}")


def run_execution_checkpoint_vector(item: ExecutionCheckpointVector) -> None:
    case = item.case
    vector = item.vector
    expected = vector["expect"]
    before_name = vector.get("checkpoint_before")
    after_name = expected["checkpoint_after"]
    if vector["operation"] in {
        "inject_execution_store",
        "register_adapter",
        "resolve_adapter",
        "validate_host_profile",
    }:
        try:
            response = _adapter_operation(vector)
        except (ExecutionHostError, ExecutionStoreError) as exc:
            response = {"result": "failure", "failure": {"code": exc.code}}
        assert response == _expected_response(vector, None, {})
        return

    initial = {}
    if before_name is not None:
        before_path = case.path / before_name
        root_instance_id = _checkpoint_root(before_path)
        initial[root_instance_id] = before_path.read_bytes()
    else:
        request_reference = vector.get("input")
        assert request_reference is not None
        request_document = _json(case.path / request_reference["file"])
        request = copy.deepcopy(
            _pointer(request_document, request_reference["pointer"])
        )
        root_instance_id = request["root_instance_id"]
    store = MemoryExecutionStore(initial)
    host = ExecutionHost(
        store,
        _resolver(case),
        fault_injector=_fault_injector(vector.get("failure_boundary")),
    )
    request_reference = vector.get("input")
    request = (
        {}
        if request_reference is None
        else copy.deepcopy(
            _pointer(
                _json(case.path / request_reference["file"]),
                request_reference["pointer"],
            )
        )
    )
    request["_case_path"] = str(case.path)

    calls: list[str] = []
    originals = (
        host_module.core_create,
        host_module.core_dispatch,
        host_module.migrate_aggregate,
    )

    def observed_create(*args: Any, **kwargs: Any) -> Any:
        calls.append("create")
        return originals[0](*args, **kwargs)

    def observed_dispatch(*args: Any, **kwargs: Any) -> Any:
        calls.append("dispatch")
        return originals[1](*args, **kwargs)

    def observed_migrate(*args: Any, **kwargs: Any) -> Any:
        calls.append("migrate")
        return originals[2](*args, **kwargs)

    host_module.core_create = observed_create
    host_module.core_dispatch = observed_dispatch
    host_module.migrate_aggregate = observed_migrate
    try:
        try:
            response = _invoke_host(
                host, vector["operation"], root_instance_id, request
            )
        except ExecutionHostError as exc:
            response = {
                "result": (
                    "response_lost"
                    if exc.code == "response_lost_after_commit"
                    else "failure"
                ),
                "failure": {"code": exc.code},
            }
    finally:
        (
            host_module.core_create,
            host_module.core_dispatch,
            host_module.migrate_aggregate,
        ) = originals
    restored = host.read_checkpoint(root_instance_id)
    actual_checkpoint = None if restored is None else restored.document
    expected_checkpoint = (
        None if after_name is None else _json(case.path / after_name)
    )
    assert response == _expected_response(vector, expected_checkpoint, request)
    assert calls == ([] if expected["core_call"] == "none" else [expected["core_call"]])
    assert actual_checkpoint == expected_checkpoint


def validate_execution_checkpoint_artifact(
    case: ExecutionCheckpointCase, artifact: dict[str, Any]
) -> None:
    path = case.path / artifact["file"]
    resolver = _resolver(case)
    if artifact.get("canonical_of"):
        expected = _json(case.path / artifact["canonical_of"])
        assert path.read_bytes() == serialize_execution_checkpoint(expected)
    if artifact.get("semantic_probe") == "compact_intent_digest":
        source_path = case.path / artifact["semantic_source"]
        root_instance_id = _checkpoint_root(source_path)
        request = _pointer(
            _json(case.path / artifact["semantic_input_file"]),
            artifact["semantic_input_pointer"],
        )
        host = ExecutionHost(
            MemoryExecutionStore({root_instance_id: source_path.read_bytes()}),
            resolver,
        )
        host.compact_outbox(
            root_instance_id,
            request["effect_id"],
            expected_revision=request["expected_revision"],
            expected_checkpoint_digest=request["expected_checkpoint_digest"],
        )
        actual = host.read_checkpoint(root_instance_id)
        assert actual is not None
        assert actual.document == _json(case.path / artifact["semantic_expected"])
        assert actual.document != _json(path)
        return
    try:
        restore_execution_checkpoint(path.read_bytes(), resolver)
        code = None
    except ArtifactError as exc:
        code = exc.code
    expected = None if artifact["valid"] else artifact["error"]
    assert code == expected
