from __future__ import annotations

import copy

import pytest

import determa.state.checkpoint_v1 as checkpoint_v1
from determa.state import (
    ArtifactError,
    ExecutionHost,
    ExecutionHostError,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    create_checkpoint_v1,
    delivery_request_digest,
    load_bundle,
    portable_envelope,
    restore_execution_checkpoint_v1,
    seal_execution_checkpoint,
)
from determa.state.queueing import admit_aggregate_v1, step_aggregate_v1

MACHINE = """
format: 1
namespace: test.checkpoint_v1
events:
  increment:
    direction: input
    payload:
      amount: { type: int, required: true }
  work_requested:
    direction: output
  work_completed:
    direction: input
    correlates_to: work_requested
    payload:
      result: { type: string, required: true }
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

TERMINAL_MACHINE = """
format: 1
namespace: test.checkpoint_v1_terminal
machines:
  - machine_id: terminal
    version: 1
    root:
      type: final
"""

OUTPUT_MACHINE = """
format: 1
namespace: test.checkpoint_v1_output
events:
  trigger:
    direction: input
  output_record:
    direction: output
    payload:
      index: { type: int, required: true }
machines:
  - machine_id: outputter
    version: 1
    root:
      type: simple
      on_events:
        trigger:
          action:
            - send:
                event: output_record
                to: { external: true }
                payload: { index: "0" }
                correlation_id: "'batch'"
            - send:
                event: output_record
                to: { external: true }
                payload: { index: "1" }
                correlation_id: "'batch'"
"""


def _bundle_and_resolver():
    bundle = load_bundle(MACHINE)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    return bundle, resolver


def _host() -> tuple[ExecutionHost, MemoryExecutionStore]:
    _bundle, resolver = _bundle_and_resolver()
    store = MemoryExecutionStore()
    return ExecutionHost(store, resolver), store


def _created_checkpoint() -> tuple[dict, MemoryArtifactResolver]:
    bundle, resolver = _bundle_and_resolver()
    return create_checkpoint_v1(bundle, "counter", "root", "create", {}), resolver


def _delivery(
    checkpoint: dict,
    event: str,
    event_id: str,
    payload: dict,
    *,
    mode: str = "input",
    correlation_id: str | None = None,
) -> dict:
    aggregate = checkpoint["root_record"]["aggregate_state"]
    root = next(
        runtime
        for runtime in aggregate["runtimes"]
        if runtime["runtime_id"] == aggregate["root_runtime_id"]
    )
    envelope = portable_envelope(
        event,
        event_id,
        root["target_identity"],
        payload,
        correlation_id=correlation_id,
    )
    return {
        "delivery_mode": mode,
        "envelope": envelope,
        "envelope_digest": delivery_request_digest("root", mode, envelope),
    }


@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    [
        ("event_id", "another-event"),
        ("envelope_digest", "sha256:" + "0" * 64),
        ("acceptance_sequence", "9"),
        ("queue_sequence", "9"),
        ("target_runtime_id", "another-runtime"),
    ],
)
def test_host_process_ready_rejects_wrong_pending_identity_before_core(
    monkeypatch: pytest.MonkeyPatch, changed_field: str, changed_value: str
) -> None:
    host, _store = _host()
    bundle = load_bundle(MACHINE)
    host.create_v1(bundle, "counter", "root", "create", {})
    created = host.read_checkpoint("root")
    assert created is not None
    delivery = _delivery(created.document, "increment", "event-1", {"amount": 1})
    host.admit_v1(
        "root",
        [delivery],
        expected_revision=created.document["revision"],
        expected_checkpoint_digest=created.document["execution_checkpoint_digest"],
    )
    before = host.read_checkpoint("root")
    assert before is not None
    aggregate = before.document["root_record"]["aggregate_state"]
    target_runtime_id = aggregate["root_runtime_id"]
    entry = next(
        runtime["ready_mailbox"][0]
        for runtime in aggregate["runtimes"]
        if runtime["runtime_id"] == target_runtime_id
    )
    identity = {
        "event_id": entry["envelope"]["event_id"],
        "envelope_digest": entry["envelope_digest"],
        "acceptance_sequence": entry["acceptance_sequence"],
        "queue_sequence": entry["queue_sequence"],
    }
    if changed_field == "target_runtime_id":
        target_runtime_id = changed_value
    else:
        identity[changed_field] = changed_value

    def unexpected_core_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("core must not run for a mismatched request")

    monkeypatch.setattr(checkpoint_v1, "step_aggregate_v1", unexpected_core_call)
    with pytest.raises(ExecutionHostError) as error:
        host.process_ready_v1(
            "root",
            target_runtime_id,
            expected_revision=before.document["revision"],
            expected_checkpoint_digest=before.document["execution_checkpoint_digest"],
            **identity,
        )
    assert error.value.code == "event_id_conflict"
    after = host.read_checkpoint("root")
    assert after is not None
    assert after.source_bytes == before.source_bytes


@pytest.mark.parametrize(
    "missing_field",
    ["event_id", "envelope_digest", "acceptance_sequence", "queue_sequence"],
)
def test_host_process_ready_requires_complete_identity(
    monkeypatch: pytest.MonkeyPatch, missing_field: str
) -> None:
    host, _store = _host()
    bundle = load_bundle(MACHINE)
    host.create_v1(bundle, "counter", "root", "create", {})
    before = host.read_checkpoint("root")
    assert before is not None
    target_runtime_id = before.document["root_record"]["aggregate_state"]["root_runtime_id"]
    identity = {
        "event_id": "event-1",
        "envelope_digest": "sha256:" + "0" * 64,
        "acceptance_sequence": "0",
        "queue_sequence": "0",
    }
    del identity[missing_field]

    def unexpected_core_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("core must not run without complete identity")

    monkeypatch.setattr(checkpoint_v1, "step_aggregate_v1", unexpected_core_call)
    with pytest.raises(TypeError):
        host.process_ready_v1(
            "root",
            target_runtime_id,
            expected_revision=before.document["revision"],
            expected_checkpoint_digest=before.document["execution_checkpoint_digest"],
            **identity,
        )
    after = host.read_checkpoint("root")
    assert after is not None
    assert after.source_bytes == before.source_bytes


def test_v1_is_the_only_supported_checkpoint_schema() -> None:
    checkpoint, resolver = _created_checkpoint()
    checkpoint["execution_checkpoint_schema_version"] = 2

    with pytest.raises(ArtifactError) as error:
        restore_execution_checkpoint_v1(checkpoint, resolver)

    assert error.value.code == "unsupported_execution_checkpoint_schema_version"


def test_tombstone_response_is_exact_and_replay_is_read_only() -> None:
    bundle = load_bundle(TERMINAL_MACHINE)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    store = MemoryExecutionStore()
    host = ExecutionHost(store, resolver)
    host.create_v1(bundle, "terminal", "root", "create", {})
    before = host.read_checkpoint("root")
    assert before is not None

    committed = host.tombstone_root_v1(
        "root",
        "tombstone",
        expected_revision=before.document["revision"],
        expected_checkpoint_digest=before.document["execution_checkpoint_digest"],
    )
    assert set(committed) == {"result", "tombstone"}
    assert committed["result"] == "tombstoned"
    assert committed["tombstone"]["status"] == "tombstone"
    after = host.read_checkpoint("root")
    assert after is not None

    replay = host.tombstone_root_v1(
        "root",
        "tombstone",
        expected_revision="stale",
        expected_checkpoint_digest="sha256:" + "0" * 64,
    )
    replay_state = host.read_checkpoint("root")
    assert replay == committed
    assert replay_state is not None
    assert replay_state.source_bytes == after.source_bytes


def test_empty_maintenance_receipt_records_target_and_replays_before_cas() -> None:
    bundle, _resolver = _bundle_and_resolver()
    host, _store = _host()
    host.create_v1(bundle, "counter", "root", "create", {})
    before = host.read_checkpoint("root")
    assert before is not None

    committed = host.maintenance_migration_v1(
        "root",
        "migration-1",
        bundle.fingerprint,
        [],
        expected_revision=before.document["revision"],
        expected_checkpoint_digest=before.document["execution_checkpoint_digest"],
    )
    receipt = committed["receipt"]
    assert receipt["target_validated_bundle_fingerprint"] == bundle.fingerprint
    assert receipt["result_code"] == "migration_no_operation"
    assert receipt["migration_sequences"] == []

    replay = host.maintenance_migration_v1(
        "root",
        "migration-1",
        bundle.fingerprint,
        [],
        expected_revision="stale",
        expected_checkpoint_digest="sha256:" + "0" * 64,
    )
    assert replay == committed


def test_same_maintenance_operation_id_with_changed_target_conflicts() -> None:
    bundle, _resolver = _bundle_and_resolver()
    host, _store = _host()
    host.create_v1(bundle, "counter", "root", "create", {})
    before = host.read_checkpoint("root")
    assert before is not None
    host.maintenance_migration_v1(
        "root",
        "migration-1",
        bundle.fingerprint,
        [],
        expected_revision=before.document["revision"],
        expected_checkpoint_digest=before.document["execution_checkpoint_digest"],
    )

    with pytest.raises(ExecutionHostError) as error:
        host.maintenance_migration_v1(
            "root",
            "migration-1",
            "sha256:" + "1" * 64,
            [],
            expected_revision="stale",
            expected_checkpoint_digest="sha256:" + "0" * 64,
        )

    assert error.value.code == "operation_id_conflict"


def test_restore_rejects_maintenance_receipt_revision_regression() -> None:
    bundle, resolver = _bundle_and_resolver()
    host = ExecutionHost(MemoryExecutionStore(), resolver)
    host.create_v1(bundle, "counter", "root", "create", {})
    before = host.read_checkpoint("root")
    assert before is not None
    host.maintenance_migration_v1(
        "root",
        "migration-1",
        bundle.fingerprint,
        [],
        expected_revision=before.document["revision"],
        expected_checkpoint_digest=before.document["execution_checkpoint_digest"],
    )
    committed = host.read_checkpoint("root")
    assert committed is not None
    forged = copy.deepcopy(committed.document)
    forged["operation_receipts"][1]["committed_revision"] = "0"
    forged = seal_execution_checkpoint(forged)

    with pytest.raises(ArtifactError) as error:
        restore_execution_checkpoint_v1(forged, resolver)

    assert error.value.code == "invalid_execution_checkpoint"


def test_checkpoint_admission_validates_before_calling_aggregate_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint, resolver = _created_checkpoint()
    before = copy.deepcopy(checkpoint)
    delivery = _delivery(checkpoint, "increment", "invalid-payload", {})

    def unexpected_core_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("aggregate admission core must not run")

    monkeypatch.setattr(checkpoint_v1, "admit_aggregate_v1", unexpected_core_call)
    with pytest.raises(ArtifactError) as error:
        checkpoint_v1.admit_checkpoint_v1(
            checkpoint,
            [delivery],
            resolver,
            expected_revision=checkpoint["revision"],
            expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
        )

    assert error.value.code == "invalid_payload"
    assert checkpoint == before


def test_checkpoint_admission_payload_failure_precedes_missing_correlation() -> None:
    checkpoint, resolver = _created_checkpoint()
    delivery = _delivery(checkpoint, "work_completed", "invalid-contract", {})

    with pytest.raises(ArtifactError) as error:
        checkpoint_v1.admit_checkpoint_v1(
            checkpoint,
            [delivery],
            resolver,
            expected_revision=checkpoint["revision"],
            expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
        )

    assert error.value.code == "invalid_payload"


def test_checkpoint_admission_mode_failure_precedes_later_member_contract_failure() -> None:
    checkpoint, resolver = _created_checkpoint()
    invalid_event = _delivery(checkpoint, "not_declared", "invalid-event", {})
    invalid_mode = _delivery(
        checkpoint,
        "increment",
        "invalid-mode",
        {"amount": 1},
        mode="unsupported",
    )

    with pytest.raises(ArtifactError) as error:
        checkpoint_v1.admit_checkpoint_v1(
            checkpoint,
            [invalid_event, invalid_mode],
            resolver,
            expected_revision=checkpoint["revision"],
            expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
        )

    assert error.value.code == "invalid_delivery_mode"


def test_checkpoint_empty_mailbox_does_not_call_aggregate_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint, resolver = _created_checkpoint()
    aggregate = checkpoint["root_record"]["aggregate_state"]
    root_runtime_id = aggregate["root_runtime_id"]
    before = copy.deepcopy(checkpoint)

    def unexpected_core_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("aggregate step core must not run")

    monkeypatch.setattr(checkpoint_v1, "step_aggregate_v1", unexpected_core_call)
    result = checkpoint_v1.step_checkpoint_v1(
        checkpoint,
        root_runtime_id,
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )

    assert result["result"] == "not_committed"
    assert result["step_result"]["disposition"] == "not_runnable"
    assert result["checkpoint"] == before
    assert checkpoint == before


def test_external_action_ordinals_are_private_core_evidence_for_checkpoint_receipts() -> None:
    bundle = load_bundle(OUTPUT_MACHINE)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    checkpoint = create_checkpoint_v1(bundle, "outputter", "root", "create", {})
    delivery = _delivery(checkpoint, "trigger", "trigger-1", {})

    admitted_core = admit_aggregate_v1(
        checkpoint["root_record"]["aggregate_state"], [delivery], resolver
    )
    core_result = step_aggregate_v1(
        admitted_core["state"],
        admitted_core["state"]["root_runtime_id"],
        resolver,
    )
    assert len(core_result["emissions"]) == 2
    assert all(
        "_determa_v1_emission_index" not in emission for emission in core_result["emissions"]
    )

    admitted_checkpoint = checkpoint_v1.admit_checkpoint_v1(
        checkpoint,
        [delivery],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    processed = checkpoint_v1.step_checkpoint_v1(
        admitted_checkpoint,
        admitted_checkpoint["root_record"]["aggregate_state"]["root_runtime_id"],
        resolver,
        expected_revision=admitted_checkpoint["revision"],
        expected_checkpoint_digest=admitted_checkpoint["execution_checkpoint_digest"],
    )

    references = processed["operation_receipts"][-1]["emission_references"]
    assert [reference["emission_index"] for reference in references] == ["0", "0"]
    assert all(
        "_determa_v1_emission_index" not in pending["intent"]
        for pending in processed["pending_outbox_intents"]
    )
    restore_execution_checkpoint_v1(processed, resolver)
