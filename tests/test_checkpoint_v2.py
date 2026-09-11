from __future__ import annotations

import copy

import pytest

import determa.state.checkpoint_v2 as checkpoint_v2
from determa.state import (
    ArtifactError,
    ExecutionHost,
    ExecutionHostError,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    admit_aggregate_v2,
    create_checkpoint_v2,
    delivery_request_digest,
    load_bundle,
    portable_envelope,
    restore_execution_checkpoint_v2,
    seal_execution_checkpoint,
    step_aggregate_v2,
)

MACHINE = """
format: 1
namespace: test.checkpoint_v2
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
namespace: test.checkpoint_v2_terminal
machines:
  - machine_id: terminal
    version: 1
    root:
      type: final
"""

OUTPUT_MACHINE = """
format: 1
namespace: test.checkpoint_v2_output
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
    return create_checkpoint_v2(bundle, "counter", "root", "create", {}), resolver


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


def test_v2_is_the_only_supported_checkpoint_schema() -> None:
    checkpoint, resolver = _created_checkpoint()
    checkpoint["execution_checkpoint_schema_version"] = 1

    with pytest.raises(ArtifactError) as error:
        restore_execution_checkpoint_v2(checkpoint, resolver)

    assert error.value.code == "unsupported_execution_checkpoint_schema_version"


def test_tombstone_response_is_exact_and_replay_is_read_only() -> None:
    bundle = load_bundle(TERMINAL_MACHINE)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    store = MemoryExecutionStore()
    host = ExecutionHost(store, resolver)
    host.create_v2(bundle, "terminal", "root", "create", {})
    before = host.read_checkpoint("root")
    assert before is not None

    committed = host.tombstone_root_v2(
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

    replay = host.tombstone_root_v2(
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
    host.create_v2(bundle, "counter", "root", "create", {})
    before = host.read_checkpoint("root")
    assert before is not None

    committed = host.maintenance_migration_v2(
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

    replay = host.maintenance_migration_v2(
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
    host.create_v2(bundle, "counter", "root", "create", {})
    before = host.read_checkpoint("root")
    assert before is not None
    host.maintenance_migration_v2(
        "root",
        "migration-1",
        bundle.fingerprint,
        [],
        expected_revision=before.document["revision"],
        expected_checkpoint_digest=before.document["execution_checkpoint_digest"],
    )

    with pytest.raises(ExecutionHostError) as error:
        host.maintenance_migration_v2(
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
    host.create_v2(bundle, "counter", "root", "create", {})
    before = host.read_checkpoint("root")
    assert before is not None
    host.maintenance_migration_v2(
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
        restore_execution_checkpoint_v2(forged, resolver)

    assert error.value.code == "invalid_execution_checkpoint"


def test_checkpoint_admission_validates_before_calling_aggregate_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint, resolver = _created_checkpoint()
    before = copy.deepcopy(checkpoint)
    delivery = _delivery(checkpoint, "increment", "invalid-payload", {})

    def unexpected_core_call(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("aggregate admission core must not run")

    monkeypatch.setattr(checkpoint_v2, "admit_aggregate_v2", unexpected_core_call)
    with pytest.raises(ArtifactError) as error:
        checkpoint_v2.admit_checkpoint_v2(
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
        checkpoint_v2.admit_checkpoint_v2(
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
        checkpoint_v2.admit_checkpoint_v2(
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

    monkeypatch.setattr(checkpoint_v2, "step_aggregate_v2", unexpected_core_call)
    result = checkpoint_v2.step_checkpoint_v2(
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
    checkpoint = create_checkpoint_v2(bundle, "outputter", "root", "create", {})
    delivery = _delivery(checkpoint, "trigger", "trigger-1", {})

    admitted_core = admit_aggregate_v2(
        checkpoint["root_record"]["aggregate_state"], [delivery], resolver
    )
    core_result = step_aggregate_v2(
        admitted_core["state"],
        admitted_core["state"]["root_runtime_id"],
        resolver,
    )
    assert len(core_result["emissions"]) == 2
    assert all(
        "_determa_v2_emission_index" not in emission
        for emission in core_result["emissions"]
    )

    admitted_checkpoint = checkpoint_v2.admit_checkpoint_v2(
        checkpoint,
        [delivery],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    processed = checkpoint_v2.step_checkpoint_v2(
        admitted_checkpoint,
        admitted_checkpoint["root_record"]["aggregate_state"]["root_runtime_id"],
        resolver,
        expected_revision=admitted_checkpoint["revision"],
        expected_checkpoint_digest=admitted_checkpoint["execution_checkpoint_digest"],
    )

    references = processed["operation_receipts"][-1]["emission_references"]
    assert [reference["emission_index"] for reference in references] == ["0", "0"]
    assert all(
        "_determa_v2_emission_index" not in pending["intent"]
        for pending in processed["pending_outbox_intents"]
    )
    restore_execution_checkpoint_v2(processed, resolver)
