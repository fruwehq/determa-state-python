from __future__ import annotations

import copy

import pytest

from determa.state import (
    ArtifactError,
    ExecutionHost,
    ExecutionHostError,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    delivery_request_digest,
    load_bundle,
    portable_envelope,
    restore_execution_checkpoint,
)

MACHINE = """
format: 1
namespace: test.execution_checkpoint
events:
  increment:
    direction: input
    payload:
      amount: { type: int, required: true }
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


def test_caller_owned_transaction_defers_after_commit_boundary() -> None:
    def response_loss(boundary: str) -> None:
        if boundary == "after_commit_before_response":
            raise ExecutionHostError("response_lost_after_commit")

    host, store = _host(fault_injector=response_loss)
    with store.transaction("root") as transaction:
        result = host.create(
            load_bundle(MACHINE),
            "counter",
            "root",
            "create",
            {},
            store_transaction=transaction,
        )
    assert result["result"] == "committed"
    assert host.read_checkpoint("root") is not None


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
