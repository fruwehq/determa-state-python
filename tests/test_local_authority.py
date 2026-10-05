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


def test_lost_response_replays_committed_receipt_and_freeze_fences_writer(tmp_path) -> None:
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
    mutation = '{"checkpoint":"one"}'
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
