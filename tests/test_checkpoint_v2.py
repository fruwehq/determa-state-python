from __future__ import annotations

import copy

import pytest

from determa.state import (
    ArtifactError,
    ExecutionHost,
    ExecutionHostError,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    create_checkpoint_v2,
    load_bundle,
    restore_execution_checkpoint_v2,
    seal_execution_checkpoint,
)

MACHINE = """
format: 1
namespace: test.checkpoint_v2
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


def test_v2_is_the_only_supported_checkpoint_schema() -> None:
    checkpoint, resolver = _created_checkpoint()
    checkpoint["execution_checkpoint_schema_version"] = 1

    with pytest.raises(ArtifactError) as error:
        restore_execution_checkpoint_v2(checkpoint, resolver)

    assert error.value.code == "unsupported_execution_checkpoint_schema_version"


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
