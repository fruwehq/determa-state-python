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

_CHECKPOINT_BROKER_MANAGED_ROOTS = frozenset({"checkpoint-lifecycle-root"})


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


def _checkpoint_replayed(
    item: DurableHostVector,
    request: dict[str, Any],
    response: dict[str, Any],
    source: bytes | None,
) -> bool:
    operation = item.vector["operation"]
    if operation == "checkpoint_create_v2":
        return source is not None
    if operation == "checkpoint_admit_v2":
        if response.get("result") == "replay":
            return True
        return response.get("result") == "batch" and all(
            member["disposition"] == "replay" for member in response["members"]
        )
    if source is None:
        return False
    checkpoint = json.loads(source)
    if operation == "checkpoint_prune_v2":
        retention = checkpoint["replay_retention"]
        return (
            retention["mode"] == request["target_mode"]
            and retention["policy_identifier"] == request["policy_identifier"]
            and retention["pruned_through_receipt_sequence"]
            == request["cutoff_receipt_sequence"]
        )
    if operation == "checkpoint_tombstone_v2":
        return checkpoint["root_record"]["status"] == "tombstone"
    if operation == "checkpoint_update_outbox_v2":
        desired = {"status": request["target_disposition"]}
        if request["outcome"]["reason_code"] is not None:
            desired["reason_code"] = request["outcome"]["reason_code"]
        return any(
            record["intent"]["effect_id"] == request["effect_id"]
            and record["delivery_state"] == desired
            for record in checkpoint["pending_outbox_intents"]
        )
    if operation == "checkpoint_terminalize_outbox_v2":
        return any(
            record["intent"]["effect_id"] == request["effect_id"]
            for record in checkpoint["terminal_outbox_records"]
        ) or any(
            record["effect_id"] == request["effect_id"]
            for record in checkpoint["outbox_effect_tombstones"]
        )
    if operation == "checkpoint_compact_outbox_v2":
        return any(
            record["effect_id"] == request["effect_id"]
            for record in checkpoint["outbox_effect_tombstones"]
        )
    return response.get("result") == "not_committed"


def _replay_evidence(checkpoint: dict[str, Any], event_id: str) -> dict[str, Any]:
    for runtime in checkpoint["root_record"].get("aggregate_state", {}).get(
        "runtimes", []
    ):
        for mailbox, location in (
            ("ready_mailbox", "ready"),
            ("deferred_mailbox", "deferred"),
        ):
            for entry in runtime[mailbox]:
                if entry["envelope"]["event_id"] == event_id:
                    return {
                        "result": "replay",
                        "event_id": event_id,
                        "acceptance_sequence": entry["acceptance_sequence"],
                        "location": location,
                    }
    acceptance = next(
        (
            receipt
            for receipt in checkpoint["operation_receipts"]
            if receipt["operation_kind"] == "acceptance"
            and receipt["event_id"] == event_id
        ),
        None,
    )
    terminal = next(
        (
            receipt
            for receipt in checkpoint["operation_receipts"]
            if receipt["operation_kind"] == "event_terminal"
            and receipt["event_id"] == event_id
        ),
        None,
    )
    if terminal is not None:
        return {
            "result": "replay",
            "acceptance_receipt_sequence": (
                acceptance["receipt_sequence"] if acceptance is not None else "0"
            ),
            "terminal_receipt_sequence": terminal["receipt_sequence"],
        }
    tombstone = next(
        (
            record
            for record in checkpoint["event_identity_tombstones"]
            if record["event_id"] == event_id
        ),
        None,
    )
    assert tombstone is not None
    return {
        "result": "replay",
        "terminal_receipt_sequence": tombstone["terminal_receipt_sequence"],
        "terminal_disposition": tombstone["terminal_disposition"],
    }


def _validate_public_response(
    operation: str,
    request: dict[str, Any],
    response: dict[str, Any],
    stored: bytes | None,
) -> None:
    assert type(response) is dict
    checkpoint = None if stored is None else json.loads(stored)
    if operation == "checkpoint_create_v2":
        assert checkpoint is not None
        assert response == {
            "result": "committed",
            "receipt": checkpoint["operation_receipts"][0],
        }
        return
    if operation in {"checkpoint_step_v2", "checkpoint_prune_v2"}:
        assert checkpoint is not None
        assert response == checkpoint
        return
    if operation == "checkpoint_admit_v2":
        assert checkpoint is not None
        if response.get("execution_checkpoint_schema_version") == 2:
            assert response == checkpoint
            return
        if response.get("result") == "replay":
            deliveries = _deliveries(request)
            assert len(deliveries) == 1
            event_id = deliveries[0]["envelope"]["event_id"]
            assert response == _replay_evidence(checkpoint, event_id)
            return
        assert set(response) == {"result", "checkpoint", "members"}
        assert response["result"] == "batch"
        assert response["checkpoint"] == checkpoint
        event_ids = [
            delivery["envelope"]["event_id"] for delivery in _deliveries(request)
        ]
        assert [member.get("event_id") for member in response["members"]] == event_ids
        for member in response["members"]:
            if member.get("disposition") == "accepted":
                assert set(member) == {
                    "event_id",
                    "disposition",
                    "acceptance_sequence",
                    "queue_sequence",
                }
                assert any(
                    entry["envelope"]["event_id"] == member["event_id"]
                    and entry["acceptance_sequence"] == member["acceptance_sequence"]
                    and entry["queue_sequence"] == member["queue_sequence"]
                    for runtime in checkpoint["root_record"]["aggregate_state"][
                        "runtimes"
                    ]
                    for mailbox in ("ready_mailbox", "deferred_mailbox")
                    for entry in runtime[mailbox]
                )
            else:
                assert set(member) == {"event_id", "disposition", "evidence"}
                assert member["disposition"] == "replay"
                assert member["evidence"] == _replay_evidence(
                    checkpoint, member["event_id"]
                )
        return
    if operation == "checkpoint_tombstone_v2":
        assert set(response) == {"result", "tombstone"}
        assert response["result"] == "tombstoned"
        assert checkpoint is not None
        assert response["tombstone"] == checkpoint["root_record"]
        return
    assert checkpoint is not None
    if operation == "checkpoint_update_outbox_v2":
        records = checkpoint["pending_outbox_intents"]
    elif operation == "checkpoint_terminalize_outbox_v2":
        records = [
            *checkpoint["terminal_outbox_records"],
            *checkpoint["outbox_effect_tombstones"],
        ]
    elif operation == "checkpoint_compact_outbox_v2":
        records = checkpoint["outbox_effect_tombstones"]
    else:
        raise AssertionError(f"unvalidated production response: {operation}")
    assert set(response) == {"result", "record"}
    assert response["result"] == "committed"
    assert response["record"] in records
    record = response["record"]
    effect_id = record.get("effect_id", record.get("intent", {}).get("effect_id"))
    assert effect_id == request["effect_id"]
    if operation == "checkpoint_update_outbox_v2":
        desired = {"status": request["target_disposition"]}
        if request["outcome"]["reason_code"] is not None:
            desired["reason_code"] = request["outcome"]["reason_code"]
        assert record["delivery_state"] == desired
    elif operation == "checkpoint_terminalize_outbox_v2":
        desired = {"status": request["target_disposition"]}
        if request["outcome"]["reason_code"] is not None:
            desired["reason_code"] = request["outcome"]["reason_code"]
        assert record["outcome"] == desired


def _request_root_instance_id(request: dict[str, Any]) -> str | None:
    if isinstance(request.get("root_instance_id"), str):
        return request["root_instance_id"]
    expected = request.get("expected_checkpoint")
    if isinstance(expected, dict) and isinstance(expected.get("root_instance_id"), str):
        return expected["root_instance_id"]
    return None


class _BrokerAcknowledgementAdapter:
    """Exercise host-owned broker acknowledgement for explicitly managed roots."""

    def __init__(self, managed_root_instance_ids: frozenset[str]) -> None:
        self._managed_root_instance_ids = managed_root_instance_ids
        self._acknowledged_request_ids: set[str] = set()

    def acknowledge(self, request: dict[str, Any]) -> None:
        if _request_root_instance_id(request) in self._managed_root_instance_ids:
            self._acknowledged_request_ids.add(request["request_id"])

    def acknowledged(self, request: dict[str, Any]) -> bool:
        return request["request_id"] in self._acknowledged_request_ids


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


def _validated_result() -> dict[str, Any]:
    return {
        "result": "validated",
        "mutation": "none",
        "core_calls": 0,
        "broker_acknowledged": False,
    }


def _invoke_contract(item: DurableHostVector, request: dict[str, Any]) -> dict[str, Any]:
    operation = item.vector["operation"]
    if operation == "checkpoint_inject_store_v2":
        ExecutionHost(
            _StaticStore(request["capabilities"]), MemoryArtifactResolver()
        )
        return _validated_result()
    if operation == "checkpoint_register_adapter_v2":
        registry = ExecutionStoreRegistry()
        _register_declared(registry, request["existing_registrations"])
        _register_declared(registry, [request["registration"]])
        return _validated_result()
    if operation == "checkpoint_resolve_adapter_v2":
        registry = ExecutionStoreRegistry()
        _register_declared(registry, request["registrations"])
        registry.resolve(
            request["uri"],
            configuration=request["configuration"],
            required_capabilities=frozenset(request["requested_capabilities"]),
        )
        return _validated_result()
    if operation == "checkpoint_validate_capabilities_v2":
        validate_host_profile(
            _StaticStore(request["store_capabilities"], request["retention_mode"]),
            request["host_profile"],
            host_features=frozenset(request["host_guarantees"]),
        )
        return _validated_result()
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
        return _validated_result()
    if operation == "checkpoint_backup_restore_v2":
        source_name = item.vector.get("checkpoint_before")
        if source_name is None:
            raise ExecutionHostError("invalid_execution_checkpoint")
        source = (item.path / source_name).read_bytes()
        checkpoint = restore_execution_checkpoint_v2(
            source, _resolver(item.path, request)
        )
        root_instance_id = checkpoint.document["root_instance_id"]
        store = MemoryExecutionStore({root_instance_id: source})
        host = ExecutionHost(store, _resolver(item.path, request))
        return host.validate_backup_restore_v2(
            action=request["action"],
            checkpoint_members=[source],
            checkpoint_digests=request["checkpoint_digests"],
            trusted_artifact_digests=request["trusted_artifact_digests"],
            adapter_metadata_digest=request["adapter_metadata_digest"],
            retention_mode=request["retention_mode"],
            consistency_point={
                "scope_id": request["scope"]["scope_id"],
                "root_instance_ids": [root_instance_id],
            },
        )
    raise AssertionError(f"unsupported durable-host contract operation: {operation}")


def run_durable_host_vector(item: DurableHostVector) -> None:
    request = _request(item)
    broker = _BrokerAcknowledgementAdapter(_CHECKPOINT_BROKER_MANAGED_ROOTS)
    before_name = item.vector.get("checkpoint_before")
    before_bytes = None if before_name is None else (item.path / before_name).read_bytes()
    observation: dict[str, Any] = {"core_calls": 0}
    try:
        if item.vector["operation"].startswith("persistence_"):
            from .persistence_host import run_persistence_vector

            actual, stored = run_persistence_vector(item, request)
        elif item.vector["operation"] in {
            "checkpoint_backup_restore_v2",
            "checkpoint_inject_store_v2",
            "checkpoint_register_adapter_v2",
            "checkpoint_resolve_adapter_v2",
            "checkpoint_scope_operation_v2",
            "checkpoint_validate_capabilities_v2",
        }:
            actual = _invoke_contract(item, request)
            stored = before_bytes
        else:
            response, stored, core_calls = _invoke_checkpoint(item, request, observation)
            _validate_public_response(
                item.vector["operation"], request, response, stored
            )
            initial_source = observation.get("initial_source", before_bytes)
            replayed = _checkpoint_replayed(item, request, response, initial_source)
            result = "replayed" if replayed else "committed"
            if result in {"committed", "replayed"}:
                broker.acknowledge(request)
            actual = {
                "result": result,
                "mutation": (
                    "none" if _same_document(stored, initial_source) else "atomic"
                ),
                "core_calls": core_calls,
                "broker_acknowledged": broker.acknowledged(request),
            }
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
        result = "crashed" if code in {
            "injected_pre_commit_failure",
            "response_lost_after_commit",
        } else "rejected"
        actual = {
            "result": result,
            "mutation": (
                "atomic" if code == "response_lost_after_commit" else "none"
            ),
            "core_calls": core_calls,
            "broker_acknowledged": False,
            "code": code,
        }
    result_reference = item.vector["result"]
    expected_result = _pointer(
        _json(item.path / result_reference["file"]), result_reference["pointer"]
    )
    assert actual == expected_result, (actual, expected_result)
    if item.vector["operation"].startswith("persistence_"):
        expected_bytes = (item.path / item.vector["store_after"]).read_bytes()
        assert _same_document(stored, expected_bytes)
        return
    after_name = item.vector.get("checkpoint_after")
    if after_name is not None:
        expected_bytes = (item.path / after_name).read_bytes()
        assert _same_document(stored, expected_bytes)
        if expected_result["mutation"] == "none":
            initial = observation.get("initial_source", before_bytes)
            assert stored == initial


def _same_document(left: bytes | None, right: bytes | None) -> bool:
    if left is None or right is None:
        return left is right
    return json.loads(left) == json.loads(right)
