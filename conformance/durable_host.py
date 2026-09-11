"""Closed driver for the pinned durable-host conformance profiles."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator

import determa.state.checkpoint_v2 as checkpoint_module
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
    restore_execution_checkpoint_v2,
    validate_host_profile,
)
from determa.state.queueing import _runtime_id_for_target

from .harness import conformance_root
from .version2 import _json, _pointer, _resolver


@dataclass(frozen=True)
class DurableHostVector:
    path: Path
    vector: dict[str, Any]

    @property
    def name(self) -> str:
        return f"{self.path.name}/{self.vector['name']}"


def durable_host_vectors() -> list[DurableHostVector]:
    result = []
    root = conformance_root() / "conformance" / "profiles"
    for test_path in sorted(root.glob("**/test.yaml")):
        document = yaml.safe_load(test_path.read_text(encoding="utf-8")) or {}
        result.extend(
            DurableHostVector(test_path.parent, vector)
            for vector in document.get("durable_host_vectors", [])
        )
    return result


def _request(item: DurableHostVector) -> dict[str, Any]:
    reference = item.vector.get("request")
    if reference is None:
        return copy.deepcopy(item.vector["raw_admission_request"])
    return copy.deepcopy(
        _pointer(_json(item.path / reference["file"]), reference["pointer"])
    )


def _fault_injector(boundary: str | None) -> Any:
    def inject(actual: str) -> None:
        if actual == boundary == "before_commit":
            raise ExecutionHostError("injected_pre_commit_failure")
        if actual == boundary == "after_commit_before_response":
            raise ExecutionHostError("response_lost_after_commit")

    return inject


class _StaticStore(ExecutionStore):
    def __init__(self, capabilities: list[str], retention_mode: str = "permanent"):
        self._capabilities = frozenset(capabilities)
        self._retention_mode = retention_mode

    @property
    def capabilities(self) -> frozenset[str]:
        return self._capabilities

    @property
    def checkpoint_retention_mode(self) -> str:
        return self._retention_mode

    def transaction(self, root_instance_id: str) -> Any:
        del root_instance_id
        raise AssertionError("profile-only store must not open a transaction")

    def setup_schema(self) -> None:
        return None

    def health(self) -> dict[str, Any]:
        return {"healthy": True}


def _store_bytes(store: MemoryExecutionStore, root_instance_id: str) -> bytes | None:
    with store.transaction(root_instance_id) as transaction:
        return transaction.load()


def _checkpoint_source(item: DurableHostVector) -> tuple[str, bytes | None]:
    vector = item.vector
    name = vector.get("stored_checkpoint_before", vector.get("checkpoint_before"))
    if name is None:
        request = _request(item)
        return request["root_instance_id"], None
    document = _json(item.path / name)
    return document["root_instance_id"], (item.path / name).read_bytes()


def _deliveries(request: dict[str, Any]) -> list[Any]:
    if "envelopes" in request:
        return request["envelopes"]
    sources = request["ordered_member_sources"]
    result = []
    for source in sources:
        if "json_value" in source:
            result.append(source["json_value"])
        else:
            result.append(source["utf8_json"])
    return result


def _invoke_checkpoint(
    item: DurableHostVector, request: dict[str, Any], observation: dict[str, Any]
) -> tuple[dict[str, Any], bytes | None, int]:
    vector = item.vector
    operation = vector["operation"]
    root_instance_id, source = _checkpoint_source(item)
    initial = {} if source is None else {root_instance_id: source}
    store = MemoryExecutionStore(initial)
    observation["initial_source"] = source
    observation["store"] = store
    observation["root_instance_id"] = root_instance_id
    host = ExecutionHost(
        store,
        _resolver(item.path, request),
        fault_injector=_fault_injector(vector.get("failure_boundary")),
    )
    expected_checkpoint = request.get("expected_checkpoint", {})
    expected = {
        "expected_revision": expected_checkpoint.get("revision", ""),
        "expected_checkpoint_digest": expected_checkpoint.get("digest", ""),
    }
    core_calls = 0
    originals = (
        checkpoint_module.create_aggregate_v2,
        checkpoint_module.admit_aggregate_v2,
        checkpoint_module.step_aggregate_v2,
    )

    def observe(index: int) -> Any:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            nonlocal core_calls
            core_calls += 1
            observation["core_calls"] = core_calls
            return originals[index](*args, **kwargs)

        return wrapped

    checkpoint_module.create_aggregate_v2 = observe(0)
    checkpoint_module.admit_aggregate_v2 = observe(1)
    checkpoint_module.step_aggregate_v2 = observe(2)
    try:
        if operation == "checkpoint_create_v2":
            bundle = load_bundle(
                (item.path / request["bundle"]["file"]).read_text(encoding="utf-8")
            )
            response = host.create_v2(
                bundle,
                request["machine"]["machine_id"],
                request["root_instance_id"],
                request["creation_id"],
                request["bindings"],
            )
        elif operation == "checkpoint_admit_v2":
            response = host.admit_v2(
                root_instance_id, _deliveries(request), **expected
            )
        elif operation == "checkpoint_step_v2":
            response = host.process_ready_v2(
                root_instance_id,
                _runtime_id_for_target(request["target"]),
                **expected,
            )
        elif operation == "checkpoint_prune_v2":
            response = host.prune_v2(
                root_instance_id,
                request["cutoff_receipt_sequence"],
                target_mode=request["target_mode"],
                policy_identifier=request["policy_identifier"],
                **expected,
            )
        elif operation == "checkpoint_tombstone_v2":
            response = host.tombstone_root_v2(
                root_instance_id,
                request["tombstone_operation_id"],
                **expected,
            )
        elif operation == "checkpoint_update_outbox_v2":
            state = {"status": request["target_disposition"]}
            if request["outcome"]["reason_code"] is not None:
                state["reason_code"] = request["outcome"]["reason_code"]
            response = host.update_pending_outbox(
                root_instance_id, request["effect_id"], state, **expected
            )
        elif operation == "checkpoint_terminalize_outbox_v2":
            outcome = {"status": request["target_disposition"]}
            if request["outcome"]["reason_code"] is not None:
                outcome["reason_code"] = request["outcome"]["reason_code"]
            response = host.terminalize_outbox(
                root_instance_id, request["effect_id"], outcome, **expected
            )
        elif operation == "checkpoint_compact_outbox_v2":
            response = host.compact_outbox(
                root_instance_id, request["effect_id"], **expected
            )
        elif operation == "checkpoint_delete_retained_record_v2":
            raise ExecutionHostError("invalid_execution_checkpoint")
        else:
            raise AssertionError(f"unsupported checkpoint operation: {operation}")
    finally:
        (
            checkpoint_module.create_aggregate_v2,
            checkpoint_module.admit_aggregate_v2,
            checkpoint_module.step_aggregate_v2,
        ) = originals
    return response, _store_bytes(store, root_instance_id), core_calls


def _register_declared(
    registry: ExecutionStoreRegistry, registrations: list[dict[str, Any]]
) -> None:
    for registration in registrations:
        capabilities = registration["capabilities"]
        schema = registration["configuration_schema"]

        def factory(
            uri: str,
            configuration: dict[str, Any],
            *,
            declared_capabilities: list[str] = capabilities,
            declared_schema: dict[str, Any] = schema,
        ) -> ExecutionStore:
            del uri
            errors = list(Draft202012Validator(declared_schema).iter_errors(configuration))
            if errors:
                raise ExecutionStoreError("invalid_adapter_configuration")
            return _StaticStore(declared_capabilities)

        registry.register(registration["uri_scheme"], factory)


def _invoke_contract(item: DurableHostVector, request: dict[str, Any]) -> None:
    operation = item.vector["operation"]
    if operation == "checkpoint_inject_store_v2":
        ExecutionHost(
            _StaticStore(request["capabilities"]), MemoryArtifactResolver()
        )
        return
    if operation == "checkpoint_register_adapter_v2":
        registry = ExecutionStoreRegistry()
        _register_declared(registry, request["existing_registrations"])
        _register_declared(registry, [request["registration"]])
        return
    if operation == "checkpoint_resolve_adapter_v2":
        registry = ExecutionStoreRegistry()
        _register_declared(registry, request["registrations"])
        registry.resolve(
            request["uri"],
            configuration=request["configuration"],
            required_capabilities=frozenset(request["requested_capabilities"]),
        )
        return
    if operation == "checkpoint_validate_capabilities_v2":
        validate_host_profile(
            _StaticStore(request["store_capabilities"], request["retention_mode"]),
            request["host_profile"],
            host_features=frozenset(request["host_guarantees"]),
        )
        return
    if operation == "checkpoint_scope_operation_v2":
        scope = request["scope"]
        matches = [
            record
            for record in request["store_records"]
            if record["scope_id"] == scope["scope_id"]
            and record["ownership_binding"] == scope["ownership_binding"]
            and record["portable_identity"] == request["portable_identity"]
            and record["effect_id"] == request["effect_id"]
        ]
        if scope["authorization"] != "authorized" or len(matches) != 1:
            raise ExecutionHostError("invalid_store_scope")
        return
    if operation == "checkpoint_backup_restore_v2":
        source_name = item.vector.get("checkpoint_before")
        if source_name is None:
            raise ExecutionHostError("invalid_execution_checkpoint")
        source = (item.path / source_name).read_bytes()
        restored = restore_execution_checkpoint_v2(source, _resolver(item.path, request))
        if restored.document["execution_checkpoint_digest"] not in request["checkpoint_digests"]:
            raise ExecutionHostError("invalid_execution_checkpoint")
        if restored.document["replay_retention"]["mode"] != request["retention_mode"]:
            raise ExecutionHostError("invalid_execution_checkpoint")
        aggregate = restored.document["root_record"].get("aggregate_state")
        required_artifacts = (
            {
                runtime["current_definition"]["validated_bundle_fingerprint"]
                for runtime in aggregate["runtimes"]
            }
            if aggregate is not None
            else set()
        )
        if not required_artifacts.issubset(request["trusted_artifact_digests"]):
            raise ExecutionHostError("invalid_execution_checkpoint")
        return
    raise AssertionError(f"unsupported durable-host contract operation: {operation}")


def run_durable_host_vector(item: DurableHostVector) -> None:
    expected = item.vector["expect"]
    request = _request(item)
    before_name = item.vector.get("checkpoint_before")
    before_bytes = None if before_name is None else (item.path / before_name).read_bytes()
    observation: dict[str, Any] = {"core_calls": 0}
    try:
        if item.vector["operation"].startswith("persistence_"):
            from .persistence_host import run_persistence_vector

            actual, code, stored, core_calls = run_persistence_vector(item, request)
        elif item.vector["operation"] in {
            "checkpoint_backup_restore_v2",
            "checkpoint_inject_store_v2",
            "checkpoint_register_adapter_v2",
            "checkpoint_resolve_adapter_v2",
            "checkpoint_scope_operation_v2",
            "checkpoint_validate_capabilities_v2",
        }:
            _invoke_contract(item, request)
            actual, stored, core_calls = "validated", before_bytes, 0
        else:
            _response, stored, core_calls = _invoke_checkpoint(item, request, observation)
            actual = "committed" if not _same_document(stored, before_bytes) else "replayed"
        if not item.vector["operation"].startswith("persistence_"):
            code = None
    except (ArtifactError, ExecutionHostError, ExecutionStoreError) as error:
        code = error.code
        store = observation.get("store")
        root_instance_id = observation.get("root_instance_id")
        stored = (
            _store_bytes(store, root_instance_id)
            if isinstance(store, MemoryExecutionStore)
            and isinstance(root_instance_id, str)
            else before_bytes
        )
        core_calls = getattr(error, "core_calls", observation["core_calls"])
        if hasattr(error, "stored"):
            stored = error.stored
        actual = "crashed" if code in {
            "injected_pre_commit_failure",
            "response_lost_after_commit",
        } else "rejected"
    assert actual == expected["result"]
    assert code == expected.get("code")
    assert core_calls == expected["core_calls"]
    if item.vector["operation"].startswith("persistence_"):
        expected_bytes = (item.path / item.vector["store_after"]).read_bytes()
        assert _same_document(stored, expected_bytes)
        return
    after_name = item.vector.get("checkpoint_after")
    if after_name is not None:
        expected_bytes = (item.path / after_name).read_bytes()
        assert _same_document(stored, expected_bytes)
        if expected["mutation"] == "none":
            initial = observation.get("initial_source", before_bytes)
            assert stored == initial


def _same_document(left: bytes | None, right: bytes | None) -> bool:
    if left is None or right is None:
        return left is right
    return json.loads(left) == json.loads(right)
