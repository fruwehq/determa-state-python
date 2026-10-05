"""Durable safety checks for the optional single-database authority."""

from __future__ import annotations

import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from determa.state import (
    AuthoritySQLiteExecutionStore,
    ExecutionHost,
    ExecutionHostError,
    MemoryArtifactResolver,
    SQLiteExecutionStore,
    SQLiteLocalAuthority,
    load_bundle,
)
from determa.state.authority import configure_bundled_sqlite_authority
from determa.state.extensions import ExtensionError, bundled_extension_registry
from determa.state.stores.base import ExecutionStoreError
from determa.state.wire import hash_value


def _request(operation: str, operation_id: str, generation: str, arguments: dict) -> str:
    request = {
        "interface": "determa.host_authority",
        "interface_version": 1,
        "operation": operation,
        "operation_id": operation_id,
        "scope_identity": "scope-1",
        "expected_authority_epoch": "0",
        "expected_scope_generation": generation,
        "arguments": arguments,
    }
    request["request_digest"] = hash_value(["determa-host-authority-request-1", request])
    return json.dumps(request, sort_keys=True, separators=(",", ":"))


@pytest.mark.parametrize(
    "mutation",
    [
        '{"checkpoint":"one"}',
        "native mutation bytes",
        '{"execution_checkpoint_format":"determa.execution_checkpoint"}',
    ],
)
def test_lost_response_replays_committed_receipt_and_freeze_fences_writer(
    tmp_path, mutation
) -> None:
    path = tmp_path / "authority.sqlite"
    authority = SQLiteLocalAuthority(path)
    authority.setup_schema()
    assert authority.allocate(
        "scope-1", "owner-1", roots=("root-1",), definition_references=("definition-1",)
    )
    registry = bundled_extension_registry(include_postgresql=False)
    configured, authority, store = configure_bundled_sqlite_authority(
        registry, path, "scope-1", "owner-1", "0"
    )
    store.setup_schema()
    report = authority.profile_report("scope-1", "owner-1", store, registry, configured)
    assert report["guarantees"] == {
        "guarded_local_writes": True,
        "worker_fencing": False,
        "complete_scope_inventory": True,
        "safe_relocation": False,
    }
    with pytest.raises(ValueError, match="unauthorized_scope"):
        authority.profile_report("scope-1", "another-principal", store, registry, configured)
    with pytest.raises(ValueError, match="not colocated"):
        authority.profile_report(
            "scope-1", "owner-1", SQLiteExecutionStore(authority.path), registry, configured
        )
    original_transaction = AuthoritySQLiteExecutionStore.transaction
    try:
        AuthoritySQLiteExecutionStore.transaction = lambda self, root: None  # type: ignore[method-assign]
        with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
            registry.report(configured)
    finally:
        AuthoritySQLiteExecutionStore.transaction = original_transaction
    original_create = ExecutionHost.create_v1
    try:
        ExecutionHost.create_v1 = lambda self, *args: None  # type: ignore[method-assign]
        with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
            registry.report(configured)
    finally:
        ExecutionHost.create_v1 = original_create
    invocation = {
        "authenticated_principal": "owner-1",
        "authorized_scopes": ["scope-1"],
        "operation_rights": ["guarded_commit", "freeze_scope"],
    }
    commit = _request(
        "guarded_commit",
        "commit-1",
        "0",
        {"mutation_digest": "sha256:" + hashlib.sha256(mutation.encode()).hexdigest()},
    )
    assert (
        authority._perform(
            commit, invocation, native_mutation_bytes=mutation, fault="drop_response_after_commit"
        )
        is None
    )
    restarted = SQLiteLocalAuthority(path)
    replay = restarted.perform(commit, invocation, native_mutation_bytes=mutation)
    assert replay is not None and json.loads(replay)["status"] == "accepted"
    assert restarted.inspect("scope-1")["checkpoint_bytes"] == [mutation]
    freeze = _request("freeze_scope", "freeze-1", "1", {})
    assert json.loads(restarted.perform(freeze, invocation))["state"] == "frozen"
    second = _request(
        "guarded_commit",
        "commit-2",
        "2",
        {"mutation_digest": "sha256:" + hashlib.sha256(mutation.encode()).hexdigest()},
    )
    assert (
        json.loads(restarted.perform(second, invocation, native_mutation_bytes=mutation))["status"]
        == "rejected"
    )


def test_allocation_marker_survives_checkpoint_removal(tmp_path) -> None:
    authority = SQLiteLocalAuthority(tmp_path / "authority.sqlite")
    authority.setup_schema()
    assert authority.allocate("scope-1", "owner-1")
    assert not authority.allocate("scope-1", "owner-1")
    original = authority.inspect("scope-1")
    assert original is not None
    original["checkpoint_bytes"] = ["checkpoint"]
    with authority._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
            (json.dumps({**original, "checkpoint_bytes": []}), "scope-1"),
        )
        connection.commit()
    assert not SQLiteLocalAuthority(authority.path).allocate("scope-1", "owner-1")


def test_database_cannot_allocate_another_scope_with_its_own_checkpoint_guard(tmp_path) -> None:
    authority = SQLiteLocalAuthority(tmp_path / "scope-isolation.sqlite")
    authority.setup_schema()
    assert authority.allocate("scope-1", "owner-1", roots=("same-root",))
    assert not authority.allocate("scope-2", "owner-2", roots=("same-root",))
    assert not authority.allocate("scope-3", "owner-3", roots=("different-root",))
    other = AuthoritySQLiteExecutionStore(authority, "scope-2", "owner-2", "0")
    other.setup_schema()
    with pytest.raises(ExecutionStoreError, match="unauthorized_scope"):
        with other.transaction("same-root"):
            pytest.fail("a second scope obtained access to the checkpoint transaction")
    restarted = SQLiteLocalAuthority(authority.path)
    assert not restarted.allocate("scope-2", "owner-2", roots=("same-root",))


def test_concurrent_scope_allocation_has_one_permanent_winner(tmp_path) -> None:
    authority = SQLiteLocalAuthority(tmp_path / "allocation-race.sqlite")
    authority.setup_schema()
    barrier = threading.Barrier(2)

    def allocate(number: int) -> bool:
        barrier.wait()
        return SQLiteLocalAuthority(authority.path).allocate(
            f"scope-{number}", f"owner-{number}", roots=("same-root",)
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(allocate, (1, 2)))
    assert sorted(outcomes) == [False, True]
    assert not SQLiteLocalAuthority(authority.path).allocate("scope-3", "owner-3")


def test_public_checkpoint_host_commits_under_same_scope_guard(tmp_path) -> None:
    machine = """
format: 1
namespace: test.authority
machines:
  - machine_id: simple
    version: 1
    root: { type: simple }
"""
    bundle = load_bundle(machine)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    authority = SQLiteLocalAuthority(tmp_path / "colocated.sqlite")
    authority.setup_schema()
    assert authority.allocate(
        "scope-1",
        "owner-1",
        roots=("root-1",),
    )
    registry = bundled_extension_registry(include_postgresql=False)
    configured, authority, store = configure_bundled_sqlite_authority(
        registry, authority.path, "scope-1", "owner-1", "0"
    )
    store.setup_schema()
    assert set(registry.report(configured)["claims"]) == {
        "authoritative_scope_fencing",
        "consistent_scope_inventory",
    }
    host = ExecutionHost(store, resolver)
    host.create_v1(bundle, "simple", "root-1", "create-1", {})
    checkpoint = host.read_checkpoint("root-1")
    assert checkpoint is not None
    ledger = authority.inspect("scope-1")
    assert ledger is not None
    assert ledger["scope_generation"] == "1"
    assert ledger["receipts"][0]["result_bytes"] is not None
    assert ledger["checkpoint_bytes"] == [checkpoint.canonical_bytes.decode()]
    with pytest.raises(ExecutionHostError, match="creation_id_conflict"):
        host.create_v1(bundle, "simple", "root-1", "create-2", {})
    assert authority.inspect("scope-1")["scope_generation"] == "1"
    with store.transaction("root-1") as transaction:
        assert transaction.load() == checkpoint.canonical_bytes
    assert authority.inspect("scope-1")["scope_generation"] == "1"

    invocation = {
        "authenticated_principal": "owner-1",
        "authorized_scopes": ["scope-1"],
        "operation_rights": ["freeze_scope"],
    }
    response = authority.perform(_request("freeze_scope", "freeze-1", "1", {}), invocation)
    assert set(registry.report(configured)["claims"]) == {
        "authoritative_scope_fencing",
        "consistent_scope_inventory",
    }
    assert authority.profile_report("scope-1", "owner-1", store, registry, configured)[
        "guarantees"
    ]["complete_scope_inventory"]
    assert json.loads(response)["state"] == "frozen"
    inventory = authority.inspect("scope-1")["inventory"]
    assert {"kind": "definition", "identity": bundle.fingerprint} in inventory
    assert {
        "kind": "receipt",
        "identity": json.dumps(["root-1", "receipt", "0"], separators=(",", ":")),
    } in inventory
    assert any(item["kind"] == "checkpoint" for item in inventory)
    with pytest.raises(ExecutionStoreError, match="stale_scope_authority"):
        host.read_checkpoint("root-1")
    with store._connect() as connection:
        actual = connection.execute(
            "SELECT checkpoint FROM determa_execution_checkpoints WHERE root_instance_id = ?",
            ("root-1",),
        ).fetchone()
    assert actual is not None and bytes(actual[0]) == checkpoint.canonical_bytes


def test_freeze_refuses_an_untracked_native_checkpoint_without_committing(tmp_path) -> None:
    bundle = load_bundle(
        "format: 1\nnamespace: test.inventory\nmachines:\n"
        "  - machine_id: simple\n    root: {type: simple}\n"
    )
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    path = tmp_path / "untracked.sqlite"
    bare_store = SQLiteExecutionStore(path)
    bare_store.setup_schema()
    bare_host = ExecutionHost(bare_store, resolver)
    bare_host.create_v1(bundle, "simple", "foreign-root", "create-foreign", {})
    before = bare_host.read_checkpoint("foreign-root")
    authority = SQLiteLocalAuthority(path)
    authority.setup_schema()
    assert authority.allocate("scope-1", "owner-1", roots=("owned-root",))
    ledger = authority.inspect("scope-1")
    invocation = {
        "authenticated_principal": "owner-1",
        "authorized_scopes": ["scope-1"],
        "operation_rights": ["freeze_scope"],
    }
    response = authority.perform(_request("freeze_scope", "freeze-untracked", "0", {}), invocation)
    assert json.loads(response)["error_code"] == "scope_fence_unproven"
    assert authority.inspect("scope-1") == ledger
    assert bare_host.read_checkpoint("foreign-root").canonical_bytes == before.canonical_bytes


@pytest.mark.parametrize(
    "damage",
    [
        None,
        "row",
        "table",
        "history",
        "ownership",
        "missing_indexes",
        "empty_indexes",
        "journal_history",
        "missing_journal_history",
        "wrong_journal_scope",
        "unowned_work",
        "malformed_work",
    ],
)
def test_freeze_requires_complete_private_native_inventory_even_for_zero_effect_roots(
    tmp_path, damage
):
    from determa.state import create_checkpoint_v1
    from determa.state.effects import SQLiteCommittedEffectHost, seal_journal

    bundle = load_bundle(
        "format: 1\nnamespace: test.private_inventory\nmachines:\n"
        "  - machine_id: simple\n    root: {type: simple}\n"
    )
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    authority = SQLiteLocalAuthority(tmp_path / "inventory.sqlite")
    authority.setup_schema()
    assert authority.allocate("scope-1", "owner-1", roots=("root-a", "root-b"))
    host = SQLiteCommittedEffectHost(
        authority.path, resolver, {"authority_epoch": "0"}, None, authority_scope="scope-1"
    )
    host.setup_schema()
    for root in ("root-a", "root-b"):
        checkpoint = create_checkpoint_v1(bundle, "simple", root, "create-" + root, {})
        document = checkpoint
        journal = seal_journal(
            {
                "host_effect_journal_format": "determa.host_effect_journal",
                "host_effect_journal_schema_version": 1,
                "scope_identity": "scope-1",
                "root_instance_id": root,
                "checkpoint_revision": document["revision"],
                "checkpoint_digest": document["execution_checkpoint_digest"],
                "journal_revision": "0",
                "effect_records": [],
                "operation_response_references": [],
            }
        )
        host.seed(document, journal)
    before = authority.inspect("scope-1")
    if damage is not None:
        with authority._connect() as connection:
            if damage == "row":
                connection.execute(
                    "DELETE FROM determa_committed_effects WHERE root_instance_id='root-b'"
                )
            elif damage == "table":
                connection.execute("DROP TABLE determa_committed_effects")
            else:
                ledger = json.loads(
                    connection.execute("SELECT ledger FROM determa_scope_authority").fetchone()[0]
                )
                if damage == "history":
                    ledger["native_effect_document_bytes"] = [
                        source
                        for source in ledger["native_effect_document_bytes"]
                        if json.loads(source)["checkpoint"]["root_instance_id"] != "root-b"
                    ]
                elif damage == "ownership":
                    ledger["native_effect_roots"].remove("root-b")
                elif damage in {"missing_indexes", "empty_indexes"}:
                    for key in ("native_effect_roots", "native_effect_document_bytes"):
                        if damage == "missing_indexes":
                            del ledger[key]
                        else:
                            ledger[key] = []
                    connection.execute("DELETE FROM determa_committed_effects")
                elif damage == "missing_journal_history":
                    del ledger["native_effect_journal_bytes"]
                elif damage in {"journal_history", "wrong_journal_scope"}:
                    latest = json.loads(ledger["native_effect_journal_bytes"][-1])
                    if damage == "journal_history":
                        latest["journal_revision"] = "99"
                    else:
                        latest["scope_identity"] = "other-scope"
                    from determa.state.wire import canonical_bytes

                    ledger["native_effect_journal_bytes"][-1] = canonical_bytes(
                        seal_journal(latest)
                    ).decode()
                elif damage == "unowned_work":
                    ledger["native_effect_work"] = [
                        {
                            "participant": "native_effects",
                            "work_kind": "effect",
                            "root_instance_id": "unowned-root",
                        }
                    ]
                else:
                    assert damage == "malformed_work"
                    ledger["native_effect_work"] = None
                connection.execute(
                    "UPDATE determa_scope_authority SET ledger=?", (json.dumps(ledger),)
                )
    invocation = {
        "authenticated_principal": "owner-1",
        "authorized_scopes": ["scope-1"],
        "operation_rights": ["freeze_scope"],
    }
    result = json.loads(
        authority.perform(
            _request("freeze_scope", "freeze-native-inventory", before["scope_generation"], {}),
            invocation,
        )
    )
    if damage is None:
        assert result["status"] == "accepted" and result["state"] == "frozen"
        inventory = authority.inspect("scope-1")["inventory"]
        assert {item["identity"] for item in inventory if item["kind"] == "root"} == {
            "root-a",
            "root-b",
        }
        assert sum(item["kind"] == "checkpoint" for item in inventory) >= 2
    else:
        assert result["error_code"] == "scope_fence_unproven"
        assert authority.inspect("scope-1")["state"] == "active"
        assert authority.inspect("scope-1")["scope_generation"] == before["scope_generation"]


def test_public_checkpoint_commit_serializes_with_freeze(tmp_path) -> None:
    bundle = load_bundle(
        """format: 1
namespace: test.guard_race
machines:
  - machine_id: simple
    version: 1
    root: { type: simple }
"""
    )
    authority = SQLiteLocalAuthority(tmp_path / "race.sqlite")
    authority.setup_schema()
    assert authority.allocate("scope-1", "owner-1", roots=("root-1",))
    store = AuthoritySQLiteExecutionStore(authority, "scope-1", "owner-1", "0")
    store.setup_schema()
    entered = threading.Event()
    release = threading.Event()

    def pause_before_native_commit(boundary: str) -> None:
        if boundary == "before_commit":
            entered.set()
            assert release.wait(5)

    host = ExecutionHost(
        store,
        MemoryArtifactResolver(definitions={bundle.fingerprint: bundle}),
        fault_injector=pause_before_native_commit,
    )
    invocation = {
        "authenticated_principal": "owner-1",
        "authorized_scopes": ["scope-1"],
        "operation_rights": ["freeze_scope"],
    }
    with ThreadPoolExecutor(max_workers=2) as pool:
        writer = pool.submit(host.create_v1, bundle, "simple", "root-1", "create-1", {})
        assert entered.wait(5)
        freezer = pool.submit(
            authority.perform, _request("freeze_scope", "freeze-1", "1", {}), invocation
        )
        assert not freezer.done()
        release.set()
        assert writer.result(timeout=5)["result"] == "committed"
        assert json.loads(freezer.result(timeout=5))["state"] == "frozen"
    assert authority.inspect("scope-1")["scope_generation"] == "2"


@pytest.mark.parametrize("provenance", [None, {}, [7]])
def test_missing_or_malformed_native_provenance_refuses_freeze_and_writes(tmp_path, provenance):
    authority = SQLiteLocalAuthority(tmp_path / "unproved.sqlite")
    authority.setup_schema()
    assert authority.allocate("scope-1", "owner-1", roots=("root-1",))
    store = AuthoritySQLiteExecutionStore(authority, "scope-1", "owner-1", "0")
    store.setup_schema()
    ledger = authority.inspect("scope-1")
    if provenance is None:
        del ledger["native_checkpoint_bytes"]
    else:
        ledger["native_checkpoint_bytes"] = provenance
    with authority._connect() as connection:
        connection.execute(
            "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
            (json.dumps(ledger), "scope-1"),
        )
        connection.commit()
    invocation = {
        "authenticated_principal": "owner-1",
        "authorized_scopes": ["scope-1"],
        "operation_rights": ["freeze_scope"],
    }
    result = json.loads(
        authority.perform(_request("freeze_scope", "freeze-unproved", "0", {}), invocation)
    )
    assert result["error_code"] == "scope_fence_unproven"
    with pytest.raises(ExecutionStoreError, match="scope_fence_unproven"):
        with store.transaction("root-1"):
            pytest.fail("unproved history obtained a checkpoint transaction")
    assert authority.inspect("scope-1") == ledger


def test_guarded_write_cannot_adopt_a_preexisting_untracked_checkpoint(tmp_path):
    bundle = load_bundle(
        "format: 1\nnamespace: test.adoption\nmachines:\n"
        "  - machine_id: final\n    root: {type: final}\n"
    )
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    path = tmp_path / "untracked-owned.sqlite"
    bare_store = SQLiteExecutionStore(path)
    bare_store.setup_schema()
    bare_host = ExecutionHost(bare_store, resolver)
    bare_host.create_v1(bundle, "final", "root-1", "create-1", {})
    before = bare_host.read_checkpoint("root-1")
    authority = SQLiteLocalAuthority(path)
    authority.setup_schema()
    assert authority.allocate("scope-1", "owner-1", roots=("root-1",))
    ledger = authority.inspect("scope-1")
    guarded = ExecutionHost(
        AuthoritySQLiteExecutionStore(authority, "scope-1", "owner-1", "0"), resolver
    )
    with pytest.raises((ExecutionStoreError, ExecutionHostError), match="scope_fence_unproven"):
        guarded.tombstone_root_v1(
            "root-1",
            "tombstone-1",
            expected_revision=before.document["revision"],
            expected_checkpoint_digest=before.document["execution_checkpoint_digest"],
        )
    assert authority.inspect("scope-1") == ledger
    assert bare_host.read_checkpoint("root-1").canonical_bytes == before.canonical_bytes


@pytest.mark.parametrize("terminal", [False, True])
def test_freeze_discovers_native_pending_and_terminal_outbox_intents(tmp_path, terminal):
    bundle = load_bundle("""format: 1
namespace: test.inventory_outbox
events:
  emitted: {direction: output}
machines:
  - machine_id: emitter
    root:
      type: simple
      entry:
        - send:
            event: emitted
            to: {external: true}
            correlation_id: "'test'"
""")
    authority = SQLiteLocalAuthority(tmp_path / "outbox.sqlite")
    authority.setup_schema()
    assert authority.allocate("scope-1", "owner-1", roots=("root-1",))
    store = AuthoritySQLiteExecutionStore(authority, "scope-1", "owner-1", "0")
    store.setup_schema()
    host = ExecutionHost(store, MemoryArtifactResolver(definitions={bundle.fingerprint: bundle}))
    host.create_v1(bundle, "emitter", "root-1", "create-1", {})
    checkpoint = host.read_checkpoint("root-1").document
    effect_id = checkpoint["pending_outbox_intents"][0]["intent"]["effect_id"]
    if terminal:
        host.terminalize_outbox(
            "root-1",
            effect_id,
            {"status": "confirmed"},
            expected_revision=checkpoint["revision"],
            expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
        )
    generation = authority.inspect("scope-1")["scope_generation"]
    invocation = {
        "authenticated_principal": "owner-1",
        "authorized_scopes": ["scope-1"],
        "operation_rights": ["freeze_scope"],
    }
    result = json.loads(
        authority.perform(_request("freeze_scope", "freeze-outbox", generation, {}), invocation)
    )
    assert result["status"] == "accepted"
    assert {
        "kind": "terminal_intent" if terminal else "pending_intent",
        "identity": json.dumps(["root-1", "effect", effect_id], separators=(",", ":")),
    } in authority.inspect("scope-1")["inventory"]
