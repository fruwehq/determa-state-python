from __future__ import annotations

import copy
from contextlib import contextmanager

import pytest

from determa.state import (
    EPHEMERAL,
    SHARED_APPLICATION_TRANSACTION,
    ArtifactError,
    ExecutionHost,
    ExecutionHostError,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    StagedExecutionResult,
    aggregate_shape_fingerprint,
    delivery_request_digest,
    load_bundle,
    portable_envelope,
    restore_execution_checkpoint,
    seal_execution_checkpoint,
)
from determa.state.wire import migration_descriptor_digest

MACHINE = """
format: 1
namespace: test.execution_checkpoint
events:
  increment:
    direction: input
    payload:
      amount: { type: int, required: true }
  deliberate_fault: { direction: input }
machines:
  - machine_id: counter
    version: 1
    root:
      type: simple
      variables:
        count: { type: int, init: 0 }
      on_events:
        increment:
          action:
            - assign: { count: "count + event.payload.amount" }
        deliberate_fault:
          action:
            - assign: { count: "count / 0" }
"""

TERMINAL_MACHINE = """
format: 1
namespace: test.execution_checkpoint_terminal
machines:
  - machine_id: terminal
    version: 1
    root:
      type: composite
      initial: { transition_to: done }
      states:
        done: { type: final }
"""


def _host(
    *,
    store: MemoryExecutionStore | None = None,
    fault_injector=None,
) -> tuple[ExecutionHost, MemoryExecutionStore]:
    bundle = load_bundle(MACHINE)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    selected = store or MemoryExecutionStore()
    return (
        ExecutionHost(
            selected, resolver, fault_injector=fault_injector
        ),
        selected,
    )


def _created(host: ExecutionHost, root: str = "root") -> dict:
    result = host.create(
        load_bundle(MACHINE), "counter", root, f"{root}-create", {}
    )
    assert result["result"] == "committed"
    restored = host.read_checkpoint(root)
    assert restored is not None
    return restored.document


def _candidate(checkpoint: dict, event_id: str = "increment-1") -> dict:
    aggregate = checkpoint["root_record"]["aggregate_state"]
    envelope = portable_envelope(
        "increment",
        event_id,
        {
            "root": {
                "root_instance_id": checkpoint["root_instance_id"],
                "root_runtime_id": aggregate["root_runtime_id"],
            }
        },
        {"amount": 1},
    )
    return {
        "root_instance_id": checkpoint["root_instance_id"],
        "delivery_mode": "input",
        "origin": {"kind": "host_input"},
        "envelope": envelope,
        "envelope_digest": delivery_request_digest(
            checkpoint["root_instance_id"], "input", envelope
        ),
    }


def test_creation_response_loss_replays_the_committed_receipt() -> None:
    def response_loss(boundary: str) -> None:
        if boundary == "after_commit_before_response":
            raise ExecutionHostError("response_lost_after_commit")

    host, store = _host(fault_injector=response_loss)
    with pytest.raises(ExecutionHostError, match="response_lost_after_commit"):
        host.create(load_bundle(MACHINE), "counter", "root", "create", {})

    replay, _ = _host(store=store)
    result = replay.create(
        load_bundle(MACHINE), "counter", "root", "create", {}
    )
    assert result["receipt"]["receipt_sequence"] == "0"
    assert replay.read_checkpoint("root") is not None


def test_pre_commit_failure_leaves_no_checkpoint() -> None:
    def rollback(boundary: str) -> None:
        if boundary == "before_commit":
            raise ExecutionHostError("injected_pre_commit_failure")

    host, _ = _host(fault_injector=rollback)
    with pytest.raises(ExecutionHostError, match="injected_pre_commit_failure"):
        host.create(load_bundle(MACHINE), "counter", "root", "create", {})
    assert host.read_checkpoint("root") is None


class _SharedMemoryStore(MemoryExecutionStore):
    def __init__(self, *, cross_root: bool = False) -> None:
        super().__init__()
        self.business_rows: list[str] = []
        self.cross_root = cross_root

    @property
    def capabilities(self) -> frozenset[str]:
        return frozenset({EPHEMERAL, SHARED_APPLICATION_TRANSACTION})

    @contextmanager
    def shared_transaction(self, root_instance_id: str):
        transaction_root = "other-root" if self.cross_root else root_instance_id
        pending_business_rows: list[str] = []
        with self.transaction(transaction_root) as transaction:
            yield pending_business_rows, transaction
        self.business_rows.extend(pending_business_rows)


def test_host_owned_shared_transaction_returns_only_after_commit() -> None:
    store = _SharedMemoryStore()
    host, _ = _host(store=store)
    staged_results: list[StagedExecutionResult] = []

    def callback(connection, execution) -> None:
        connection.append("business-row")
        staged = execution.create(
            load_bundle(MACHINE), "counter", "create", {}
        )
        assert not isinstance(staged, dict)
        staged_results.append(staged)

    result = host.run_shared_transaction("root", callback)

    assert result["result"] == "committed"
    assert staged_results == [StagedExecutionResult("create")]
    assert store.business_rows == ["business-row"]
    assert host.read_checkpoint("root") is not None


def test_host_owned_shared_transaction_rollback_returns_no_committed_result() -> None:
    store = _SharedMemoryStore()
    host, _ = _host(store=store)

    def callback(connection, execution) -> None:
        connection.append("business-row")
        execution.create(load_bundle(MACHINE), "counter", "create", {})
        raise RuntimeError("application rollback")

    with pytest.raises(RuntimeError, match="application rollback"):
        host.run_shared_transaction("root", callback)

    assert store.business_rows == []
    assert host.read_checkpoint("root") is None


def test_shared_transaction_response_loss_occurs_only_after_outer_commit() -> None:
    def response_loss(boundary: str) -> None:
        if boundary == "after_commit_before_response":
            raise ExecutionHostError("response_lost_after_commit")

    store = _SharedMemoryStore()
    host, _ = _host(store=store, fault_injector=response_loss)

    def callback(connection, execution) -> None:
        connection.append("business-row")
        execution.create(load_bundle(MACHINE), "counter", "create", {})

    with pytest.raises(ExecutionHostError, match="response_lost_after_commit"):
        host.run_shared_transaction("root", callback)

    replay, _ = _host(store=store)
    result = replay.create(
        load_bundle(MACHINE), "counter", "root", "create", {}
    )
    assert result["result"] == "committed"
    assert store.business_rows == ["business-row"]


def test_shared_transaction_rejects_a_store_transaction_bound_to_another_root() -> None:
    host, _ = _host(store=_SharedMemoryStore(cross_root=True))
    with pytest.raises(ExecutionHostError) as error:
        host.run_shared_transaction("root", lambda _connection, _execution: None)
    assert error.value.code == "transaction_root_mismatch"


def test_shared_transaction_accepts_exactly_one_host_operation() -> None:
    store = _SharedMemoryStore()
    host, _ = _host(store=store)

    def callback(_connection, execution) -> None:
        execution.create(load_bundle(MACHINE), "counter", "create", {})
        execution.create(load_bundle(MACHINE), "counter", "create", {})

    with pytest.raises(ExecutionHostError) as error:
        host.run_shared_transaction("root", callback)
    assert error.value.code == "shared_transaction_operation_conflict"
    assert host.read_checkpoint("root") is None


def test_accept_process_and_replay_use_durable_host_receipts() -> None:
    host, _ = _host()
    created = _created(host)
    candidate = _candidate(created)
    pending = host.accept_delivery(
        "root",
        candidate,
        expected_revision=created["revision"],
        expected_checkpoint_digest=created["execution_checkpoint_digest"],
    )
    assert pending["result"] == "pending"

    accepted = host.read_checkpoint("root")
    assert accepted is not None
    committed = host.process_pending_delivery(
        "root",
        candidate,
        expected_revision=accepted.document["revision"],
        expected_checkpoint_digest=accepted.document[
            "execution_checkpoint_digest"
        ],
    )
    replay = host.accept_delivery(
        "root",
        candidate,
        expected_revision=created["revision"],
        expected_checkpoint_digest=created["execution_checkpoint_digest"],
    )
    assert replay == committed


def test_delivery_replay_precedes_origin_and_tombstone_validation() -> None:
    host, _ = _host()
    created = _created(host)
    aggregate = created["root_record"]["aggregate_state"]
    envelope = portable_envelope(
        "deliberate_fault",
        "fault-replay",
        {
            "root": {
                "root_instance_id": "root",
                "root_runtime_id": aggregate["root_runtime_id"],
            }
        },
        {},
    )
    candidate = {
        "root_instance_id": "root",
        "delivery_mode": "input",
        "origin": {"kind": "host_input"},
        "envelope": envelope,
    }
    committed = host.foreground_process_delivery(
        "root",
        candidate,
        expected_revision=created["revision"],
        expected_checkpoint_digest=created["execution_checkpoint_digest"],
    )
    faulted = host.read_checkpoint("root")
    assert faulted is not None
    host.tombstone_root(
        "root",
        "tombstone",
        expected_revision=faulted.document["revision"],
        expected_checkpoint_digest=faulted.document[
            "execution_checkpoint_digest"
        ],
    )
    invalid_origin = copy.deepcopy(candidate)
    invalid_origin["origin"] = {"kind": "invalid"}
    replay = host.accept_delivery(
        "root",
        invalid_origin,
        expected_revision=created["revision"],
        expected_checkpoint_digest=created["execution_checkpoint_digest"],
    )
    assert replay == committed

    invalid_mode = copy.deepcopy(candidate)
    invalid_mode["delivery_mode"] = "invalid"
    mode_conflict = host.accept_delivery(
        "root",
        invalid_mode,
        expected_revision=created["revision"],
        expected_checkpoint_digest=created["execution_checkpoint_digest"],
    )
    assert mode_conflict == {
        "result": "not_accepted",
        "failure": {"code": "event_id_conflict"},
    }

    conflicting = copy.deepcopy(invalid_origin)
    conflicting["envelope"]["event"] = "increment"
    conflict = host.accept_delivery(
        "root",
        conflicting,
        expected_revision=created["revision"],
        expected_checkpoint_digest=created["execution_checkpoint_digest"],
    )
    assert conflict == {
        "result": "not_accepted",
        "failure": {"code": "event_id_conflict"},
    }


def test_checkpoint_digest_mismatch_is_classified_after_structure() -> None:
    host, _ = _host()
    checkpoint = _created(host)
    checkpoint["execution_checkpoint_digest"] = "sha256:" + ("0" * 64)
    resolver = host.artifact_resolver
    with pytest.raises(ArtifactError) as error:
        restore_execution_checkpoint(checkpoint, resolver)
    assert error.value.code == "execution_checkpoint_digest_mismatch"


def test_unknown_checkpoint_member_is_structurally_rejected() -> None:
    host, _ = _host()
    checkpoint = _created(host)
    checkpoint["extra"] = True
    with pytest.raises(ArtifactError) as error:
        restore_execution_checkpoint(checkpoint, host.artifact_resolver)
    assert error.value.code == "invalid_execution_checkpoint"


def test_root_deletion_is_unsupported_and_preserves_bytes() -> None:
    host, _ = _host()
    checkpoint = _created(host)
    result = host.delete_checkpoint(
        "root",
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    assert result == {
        "result": "unsupported",
        "failure": {"code": "physical_deletion_unsupported"},
    }
    restored = host.read_checkpoint("root")
    assert restored is not None
    assert restored.document == checkpoint


def test_bounded_retention_cannot_attest_unallocated_receipts() -> None:
    host, _ = _host()
    checkpoint = _created(host)
    with pytest.raises(ExecutionHostError) as error:
        host.update_replay_retention(
            "root",
            {
                "mode": "bounded",
                "permanent_replay_eligible": False,
                "pruned_through_receipt_sequence": "1",
                "policy_identifier": "bounded-test",
            },
            expected_revision=checkpoint["revision"],
            expected_checkpoint_digest=checkpoint[
                "execution_checkpoint_digest"
            ],
        )
    assert error.value.code == "invalid_execution_checkpoint"


def test_checkpoint_restore_does_not_mutate_caller_document() -> None:
    host, _ = _host()
    checkpoint = _created(host)
    original = copy.deepcopy(checkpoint)
    restore_execution_checkpoint(checkpoint, host.artifact_resolver)
    assert checkpoint == original


def test_restore_rejects_creation_status_inconsistent_with_aggregate() -> None:
    host, _ = _host()
    checkpoint = _created(host)
    checkpoint["operation_receipts"][0]["status"] = "completed"
    mutated = seal_execution_checkpoint(checkpoint)
    with pytest.raises(ArtifactError) as error:
        restore_execution_checkpoint(mutated, host.artifact_resolver)
    assert error.value.code == "invalid_execution_checkpoint"


def test_restore_rejects_delivery_fault_inconsistent_with_aggregate() -> None:
    host, _ = _host()
    created = _created(host)
    aggregate = created["root_record"]["aggregate_state"]
    envelope = portable_envelope(
        "deliberate_fault",
        "fault-1",
        {
            "root": {
                "root_instance_id": "root",
                "root_runtime_id": aggregate["root_runtime_id"],
            }
        },
        {},
    )
    host.foreground_process_delivery(
        "root",
        {
            "root_instance_id": "root",
            "delivery_mode": "input",
            "origin": {"kind": "host_input"},
            "envelope": envelope,
        },
        expected_revision=created["revision"],
        expected_checkpoint_digest=created["execution_checkpoint_digest"],
    )
    restored = host.read_checkpoint("root")
    assert restored is not None
    checkpoint = restored.document
    checkpoint["operation_receipts"][-1]["outcome"]["fault"]["code"] = (
        "different_fault"
    )
    mutated = seal_execution_checkpoint(checkpoint)
    with pytest.raises(ArtifactError) as error:
        restore_execution_checkpoint(mutated, host.artifact_resolver)
    assert error.value.code == "invalid_execution_checkpoint"


def test_restore_rejects_completed_tombstone_relabeled_faulted() -> None:
    bundle = load_bundle(TERMINAL_MACHINE)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    host = ExecutionHost(MemoryExecutionStore(), resolver)
    host.create(bundle, "terminal", "terminal-root", "create", {})
    completed = host.read_checkpoint("terminal-root")
    assert completed is not None
    host.tombstone_root(
        "terminal-root",
        "tombstone",
        expected_revision=completed.document["revision"],
        expected_checkpoint_digest=completed.document[
            "execution_checkpoint_digest"
        ],
    )
    restored = host.read_checkpoint("terminal-root")
    assert restored is not None
    checkpoint = restored.document
    checkpoint["root_record"]["terminal_status"] = "faulted"
    mutated = seal_execution_checkpoint(checkpoint)
    with pytest.raises(ArtifactError) as error:
        restore_execution_checkpoint(mutated, resolver)
    assert error.value.code == "invalid_execution_checkpoint"


def test_bounded_pruning_through_latest_receipt_remains_valid() -> None:
    host, _ = _host()
    created = _created(host)
    host.foreground_process_delivery(
        "root",
        _candidate(created),
        expected_revision=created["revision"],
        expected_checkpoint_digest=created["execution_checkpoint_digest"],
    )
    processed = host.read_checkpoint("root")
    assert processed is not None
    result = host.update_replay_retention(
        "root",
        {
            "mode": "bounded",
            "permanent_replay_eligible": False,
            "pruned_through_receipt_sequence": "1",
            "policy_identifier": "bounded-test",
        },
        expected_revision=processed.document["revision"],
        expected_checkpoint_digest=processed.document[
            "execution_checkpoint_digest"
        ],
    )
    assert result["result"] == "committed"
    bounded = host.read_checkpoint("root")
    assert bounded is not None
    assert [
        receipt["receipt_sequence"]
        for receipt in bounded.document["operation_receipts"]
    ] == ["0"]


def test_maintenance_replay_precedes_tombstone_eligibility() -> None:
    bundle = load_bundle(TERMINAL_MACHINE)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    host = ExecutionHost(MemoryExecutionStore(), resolver)
    host.create(bundle, "terminal", "terminal-root", "create", {})
    created = host.read_checkpoint("terminal-root")
    assert created is not None
    source_digest = created.document["root_record"]["aggregate_state"][
        "aggregate_state_digest"
    ]
    committed = host.maintenance_migration(
        "terminal-root",
        "migration",
        bundle.fingerprint,
        [],
        source_aggregate_state_digest=source_digest,
        expected_revision=created.document["revision"],
        expected_checkpoint_digest=created.document[
            "execution_checkpoint_digest"
        ],
    )
    migrated = host.read_checkpoint("terminal-root")
    assert migrated is not None
    invalid_receipt = copy.deepcopy(migrated.document)
    invalid_receipt["operation_receipts"][-1][
        "resulting_aggregate_state_digest"
    ] = "sha256:" + ("0" * 64)
    with pytest.raises(ArtifactError) as invalid_error:
        restore_execution_checkpoint(
            seal_execution_checkpoint(invalid_receipt), resolver
        )
    assert invalid_error.value.code == "invalid_execution_checkpoint"
    host.tombstone_root(
        "terminal-root",
        "tombstone",
        expected_revision=migrated.document["revision"],
        expected_checkpoint_digest=migrated.document[
            "execution_checkpoint_digest"
        ],
    )
    replay = host.maintenance_migration(
        "terminal-root",
        "migration",
        bundle.fingerprint,
        [],
        source_aggregate_state_digest=source_digest,
        expected_revision=created.document["revision"],
        expected_checkpoint_digest=created.document[
            "execution_checkpoint_digest"
        ],
    )
    assert replay == committed
    with pytest.raises(ExecutionHostError) as conflict:
        host.maintenance_migration(
            "terminal-root",
            "migration",
            bundle.fingerprint,
            [],
            source_aggregate_state_digest=source_digest,
            expected_revision=created.document["revision"],
            expected_checkpoint_digest=created.document[
                "execution_checkpoint_digest"
            ],
            maintenance_mode=False,
        )
    assert conflict.value.code == "operation_id_conflict"


def test_restore_checks_status_evidence_across_maintenance_migration() -> None:
    source = load_bundle(MACHINE)
    target = load_bundle(MACHINE.replace(
        "namespace: test.execution_checkpoint",
        "namespace: test.execution_checkpoint\nmeta: {release: target}",
    ))
    shape = aggregate_shape_fingerprint(source)
    assert aggregate_shape_fingerprint(target) == shape
    descriptor = {
        "migration_descriptor_format": "determa.aggregate_migration",
        "migration_descriptor_schema_version": 1,
        "source_machine_format": 1,
        "target_machine_format": 1,
        "source_validated_bundle_fingerprint": source.fingerprint,
        "target_validated_bundle_fingerprint": target.fingerprint,
        "source_aggregate_shape_fingerprint": shape,
        "target_aggregate_shape_fingerprint": shape,
        "mode": "compatible",
        "mappings": {
            "machines": [],
            "active_states": [],
            "variables": [],
            "history": [],
            "components": [],
            "owned_runtimes": [],
            "lifetime_holders": [],
            "counters": [],
        },
        "terminal_policy": {"completed": "preserve", "faulted": "preserve"},
        "resource_requirements": {
            "maximum_transformed_output_bytes": "0",
            "maximum_cel_expression_length": "0",
            "maximum_cel_ast_nodes": "0",
            "maximum_cel_evaluation_steps": "0",
        },
    }
    descriptor["migration_descriptor_digest"] = migration_descriptor_digest(
        descriptor
    )
    resolver = MemoryArtifactResolver(
        definitions={
            source.fingerprint: source,
            target.fingerprint: target,
        },
        migration_descriptors={
            descriptor["migration_descriptor_digest"]: descriptor,
        },
    )
    host = ExecutionHost(MemoryExecutionStore(), resolver)
    host.create(source, "counter", "migration-root", "create", {})
    created = host.read_checkpoint("migration-root")
    assert created is not None
    source_digest = created.document["root_record"]["aggregate_state"][
        "aggregate_state_digest"
    ]
    host.maintenance_migration(
        "migration-root",
        "migration",
        target.fingerprint,
        [descriptor["migration_descriptor_digest"]],
        source_aggregate_state_digest=source_digest,
        expected_revision=created.document["revision"],
        expected_checkpoint_digest=created.document[
            "execution_checkpoint_digest"
        ],
    )
    migrated = host.read_checkpoint("migration-root")
    assert migrated is not None
    checkpoint = migrated.document
    assert (
        checkpoint["operation_receipts"][0]["resulting_aggregate_state_digest"]
        != checkpoint["root_record"]["aggregate_state"]["aggregate_state_digest"]
    )
    checkpoint["operation_receipts"][0]["status"] = "completed"
    with pytest.raises(ArtifactError) as error:
        restore_execution_checkpoint(
            seal_execution_checkpoint(checkpoint), resolver
        )
    assert error.value.code == "invalid_execution_checkpoint"


def test_maintenance_request_requires_exact_source_digest() -> None:
    host, _ = _host()
    created = _created(host)
    bundle = load_bundle(MACHINE)
    with pytest.raises(TypeError):
        host.maintenance_migration(
            "root",
            "migration",
            bundle.fingerprint,
            [],
            expected_revision=created["revision"],
            expected_checkpoint_digest=created[
                "execution_checkpoint_digest"
            ],
        )


def test_oversized_checkpoint_decimal_is_closed_invalidity() -> None:
    host, _ = _host()
    checkpoint = _created(host)
    checkpoint["revision"] = "9" * 5000
    mutated = seal_execution_checkpoint(checkpoint)
    with pytest.raises(ArtifactError) as error:
        restore_execution_checkpoint(mutated, host.artifact_resolver)
    assert error.value.code == "invalid_execution_checkpoint"
