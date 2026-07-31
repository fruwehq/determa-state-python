from __future__ import annotations

import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from determa.state import (
    COMPACT_EFFECT_IDENTITY_RETENTION,
    DURABLE_SINGLE_WRITER,
    EPHEMERAL,
    PERMANENT_OUTBOX_TERMINAL_RETENTION,
    PERMANENT_RECEIPT_RETENTION,
    RESTART_PERSISTENT,
    ROOT_IDENTITY_RETENTION,
    ExecutionHost,
    ExecutionHostError,
    ExecutionStore,
    ExecutionStoreError,
    ExecutionStoreRegistry,
    FileExecutionStore,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    SQLiteExecutionStore,
    bundled_execution_store_registry,
    load_bundle,
    portable_envelope,
)

from .test_checkpoint_host import MACHINE


def _resolver() -> MemoryArtifactResolver:
    bundle = load_bundle(MACHINE)
    return MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})


def _create(store: ExecutionStore, root: str = "root") -> ExecutionHost:
    host = ExecutionHost(store, _resolver())
    host.create(load_bundle(MACHINE), "counter", root, f"{root}-create", {})
    return host


def _factories(tmp_path: Path) -> list[Callable[[], ExecutionStore]]:
    return [
        MemoryExecutionStore,
        lambda: FileExecutionStore(tmp_path / "file-store"),
        lambda: SQLiteExecutionStore(tmp_path / "store.sqlite"),
    ]


@pytest.mark.parametrize("index", range(3))
def test_shared_adapter_contract_round_trip(tmp_path: Path, index: int) -> None:
    store = _factories(tmp_path)[index]()
    store.setup_schema()
    with store.transaction("bound-root") as transaction:
        assert transaction.root_instance_id == "bound-root"
    host = _create(store)
    restored = host.read_checkpoint("root")
    assert restored is not None
    replay = host.create(
        load_bundle(MACHINE), "counter", "root", "root-create", {}
    )
    assert replay["receipt"]["receipt_sequence"] == "0"


@pytest.mark.parametrize("index", range(3))
def test_store_transactions_reject_checkpoint_bytes_for_another_root(
    tmp_path: Path, index: int
) -> None:
    store = _factories(tmp_path)[index]()
    store.setup_schema()
    host = _create(store)
    checkpoint = host.read_checkpoint("root")
    assert checkpoint is not None
    with pytest.raises(ExecutionStoreError) as error:
        with store.transaction("other-root") as transaction:
            transaction.insert(checkpoint.canonical_bytes)
    assert error.value.code == "transaction_root_mismatch"

    _create(store, "other-root")
    other = ExecutionHost(store, _resolver()).read_checkpoint("other-root")
    assert other is not None
    with pytest.raises(ExecutionStoreError) as replace_error:
        with store.transaction("root") as transaction:
            transaction.replace(
                checkpoint.document["revision"],
                checkpoint.document["execution_checkpoint_digest"],
                other.canonical_bytes,
            )
    assert replace_error.value.code == "transaction_root_mismatch"


def test_host_rejects_checkpoint_loaded_under_another_root_key() -> None:
    source_store = MemoryExecutionStore()
    source_host = _create(source_store)
    checkpoint = source_host.read_checkpoint("root")
    assert checkpoint is not None
    mismatched_store = MemoryExecutionStore(
        {"other-root": checkpoint.canonical_bytes}
    )
    mismatched_host = ExecutionHost(mismatched_store, _resolver())
    with pytest.raises(ExecutionHostError) as error:
        mismatched_host.read_checkpoint("other-root")
    assert error.value.code == "transaction_root_mismatch"


@pytest.mark.parametrize(
    "store_factory",
    [
        lambda path: FileExecutionStore(path / "file-store"),
        lambda path: SQLiteExecutionStore(path / "store.sqlite"),
    ],
)
def test_persistent_adapters_require_explicit_schema_setup(
    tmp_path: Path, store_factory
) -> None:
    store = store_factory(tmp_path)
    host = ExecutionHost(store, _resolver())
    with pytest.raises(ExecutionStoreError) as error:
        host.read_checkpoint("root")
    assert error.value.code == "execution_store_schema_unavailable"


@pytest.mark.parametrize(
    "store_factory",
    [
        lambda path: FileExecutionStore(path / "file-store"),
        lambda path: SQLiteExecutionStore(path / "store.sqlite"),
    ],
)
def test_file_and_sqlite_survive_adapter_restart(
    tmp_path: Path, store_factory
) -> None:
    first = store_factory(tmp_path)
    first.setup_schema()
    _create(first)
    second = store_factory(tmp_path)
    restored = ExecutionHost(second, _resolver()).read_checkpoint("root")
    assert restored is not None
    assert restored.document["revision"] == "0"


@pytest.mark.parametrize("index", range(3))
def test_concurrent_stale_writer_cannot_overwrite(
    tmp_path: Path, index: int
) -> None:
    store = _factories(tmp_path)[index]()
    store.setup_schema()
    host = _create(store)
    checkpoint = host.read_checkpoint("root")
    assert checkpoint is not None
    document = checkpoint.document
    aggregate = document["root_record"]["aggregate_state"]

    def process(event_id: str) -> str:
        candidate = {
            "root_instance_id": "root",
            "delivery_mode": "input",
            "origin": {"kind": "host_input"},
            "envelope": portable_envelope(
                "increment",
                event_id,
                {
                    "root": {
                        "root_instance_id": "root",
                        "root_runtime_id": aggregate["root_runtime_id"],
                    }
                },
                {"amount": 1},
            ),
        }
        try:
            ExecutionHost(store, _resolver()).foreground_process_delivery(
                "root",
                candidate,
                expected_revision=document["revision"],
                expected_checkpoint_digest=document[
                    "execution_checkpoint_digest"
                ],
            )
        except ExecutionHostError as exc:
            return exc.code
        return "committed"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = sorted(executor.map(process, ["event-a", "event-b"]))
    assert outcomes == ["checkpoint_revision_conflict", "committed"]


def test_registry_is_empty_and_duplicate_registration_never_overrides() -> None:
    registry = ExecutionStoreRegistry()
    assert registry.identifiers == ()
    registry.register("custom", lambda _uri, _config: MemoryExecutionStore())
    with pytest.raises(ExecutionStoreError) as error:
        registry.register("custom", lambda _uri, _config: MemoryExecutionStore())
    assert error.value.code == "duplicate_adapter_registration"


def test_registry_checks_configuration_before_capabilities() -> None:
    registry = ExecutionStoreRegistry()

    def invalid(_uri, _configuration):
        raise ExecutionStoreError("invalid_adapter_configuration")

    registry.register("custom", invalid)
    with pytest.raises(ExecutionStoreError) as error:
        registry.resolve(
            "custom:", required_capabilities={DURABLE_SINGLE_WRITER}
        )
    assert error.value.code == "invalid_adapter_configuration"


def test_bundled_adapters_use_public_registration_and_exact_capabilities(
    tmp_path: Path,
) -> None:
    registry = bundled_execution_store_registry()
    assert registry.identifiers == ("file", "memory", "postgresql", "sqlite")
    assert registry.resolve("memory:").capabilities == frozenset({EPHEMERAL})
    assert registry.resolve(
        f"file://{tmp_path / 'files'}"
    ).capabilities == frozenset({RESTART_PERSISTENT})
    assert DURABLE_SINGLE_WRITER in registry.resolve(
        f"sqlite://{tmp_path / 'store.sqlite'}"
    ).capabilities
    configured_sqlite = registry.resolve(
        f"sqlite://{tmp_path / 'strict.sqlite'}"
        "?replay_retention=permanent&outbox_retention=strict"
    )
    assert {
        PERMANENT_RECEIPT_RETENTION,
        PERMANENT_OUTBOX_TERMINAL_RETENTION,
    }.issubset(configured_sqlite.capabilities)
    configured_sqlite.setup_schema()
    reopened_sqlite = registry.resolve(
        f"sqlite://{tmp_path / 'strict.sqlite'}"
        "?replay_retention=permanent&outbox_retention=strict"
    )
    reopened_sqlite.validate_schema()
    with pytest.raises(ExecutionStoreError) as sqlite_policy_mismatch:
        registry.resolve(f"sqlite://{tmp_path / 'strict.sqlite'}").validate_schema()
    assert sqlite_policy_mismatch.value.code == "execution_store_schema_mismatch"
    configured_postgresql = registry.resolve(
        "postgresql://unused",
        configuration={
            "replay_retention": "permanent",
            "outbox_retention": "compact",
        },
    )
    assert {
        PERMANENT_RECEIPT_RETENTION,
        COMPACT_EFFECT_IDENTITY_RETENTION,
    }.issubset(configured_postgresql.capabilities)


def test_unknown_adapter_and_capability_mismatch_are_closed() -> None:
    registry = bundled_execution_store_registry()
    with pytest.raises(ExecutionStoreError) as unknown:
        registry.resolve("absent:")
    assert unknown.value.code == "unknown_adapter"
    with pytest.raises(ExecutionStoreError) as mismatch:
        registry.resolve(
            "memory:", required_capabilities={DURABLE_SINGLE_WRITER}
        )
    assert mismatch.value.code == "adapter_capability_mismatch"


def test_sqlite_rejects_malformed_or_wrong_version_schema(tmp_path: Path) -> None:
    malformed_path = tmp_path / "malformed.sqlite"
    with sqlite3.connect(malformed_path) as connection:
        connection.execute(
            "CREATE TABLE determa_execution_checkpoints "
            "(root_instance_id TEXT PRIMARY KEY NOT NULL)"
        )
    malformed = SQLiteExecutionStore(malformed_path)
    with pytest.raises(ExecutionStoreError) as malformed_error:
        malformed.setup_schema()
    assert malformed_error.value.code == "execution_store_schema_mismatch"
    assert malformed.health() == {"healthy": False, "schema_ready": False}
    with pytest.raises(ExecutionStoreError) as host_error:
        ExecutionHost(
            malformed,
            _resolver(),
            required_capabilities={DURABLE_SINGLE_WRITER},
        )
    assert host_error.value.code == "execution_store_schema_mismatch"

    constrained_path = tmp_path / "extra-constraint.sqlite"
    with sqlite3.connect(constrained_path) as connection:
        connection.execute(
            "CREATE TABLE determa_execution_store_metadata "
            "(schema_key TEXT PRIMARY KEY NOT NULL, schema_value TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO determa_execution_store_metadata VALUES "
            "('execution_checkpoint_schema_version', '2')"
        )
        connection.execute(
            "CREATE TABLE determa_execution_checkpoints ("
            "root_instance_id TEXT PRIMARY KEY NOT NULL, "
            "revision TEXT NOT NULL CHECK (length(revision) > 0), "
            "checkpoint_digest TEXT NOT NULL, checkpoint BLOB NOT NULL)"
        )
    constrained = SQLiteExecutionStore(constrained_path)
    with pytest.raises(ExecutionStoreError) as constrained_error:
        constrained.setup_schema()
    assert constrained_error.value.code == "execution_store_schema_mismatch"
    assert constrained.health() == {"healthy": False, "schema_ready": False}

    versioned_path = tmp_path / "wrong-version.sqlite"
    versioned = SQLiteExecutionStore(versioned_path)
    versioned.setup_schema()
    with sqlite3.connect(versioned_path) as connection:
        connection.execute(
            "DROP TRIGGER determa_execution_metadata_forbid_update"
        )
        connection.execute(
            "UPDATE determa_execution_store_metadata SET schema_value = '3' "
            "WHERE schema_key = 'execution_checkpoint_schema_version'"
        )
    with pytest.raises(ExecutionStoreError) as version_error:
        versioned.validate_schema()
    assert version_error.value.code == "execution_store_schema_mismatch"
    assert versioned.health() == {"healthy": False, "schema_ready": False}


def test_sqlite_persists_policy_and_forbids_native_root_or_policy_mutation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bank.sqlite"
    store = SQLiteExecutionStore(
        path,
        replay_retention="permanent",
        outbox_retention="strict",
    )
    store.setup_schema()
    host = _create(store, "bank-root")

    with sqlite3.connect(path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="execution_store_immutable"):
            connection.execute(
                "DELETE FROM determa_execution_checkpoints "
                "WHERE root_instance_id = ?",
                ("bank-root",),
            )
        with pytest.raises(sqlite3.IntegrityError, match="execution_store_immutable"):
            connection.execute(
                "UPDATE determa_execution_store_metadata "
                "SET schema_value = 'bounded' "
                "WHERE schema_key = 'replay_retention'"
            )

    assert host.read_checkpoint("bank-root") is not None
    with pytest.raises(ExecutionHostError) as recreate:
        host.create(
            load_bundle(MACHINE), "counter", "bank-root", "replacement", {}
        )
    assert recreate.value.code == "creation_id_conflict"

    reopened = SQLiteExecutionStore(
        path,
        replay_retention="permanent",
        outbox_retention="strict",
    )
    reopened.validate_schema()
    assert reopened.health() == {
        "healthy": True,
        "schema_ready": True,
        "schema_version": 2,
    }
    weaker = SQLiteExecutionStore(path)
    with pytest.raises(ExecutionStoreError) as mismatch:
        weaker.validate_schema()
    assert mismatch.value.code == "execution_store_schema_mismatch"
    assert weaker.health() == {"healthy": False, "schema_ready": False}


def test_sqlite_health_requires_immutable_policy_and_root_guards(
    tmp_path: Path,
) -> None:
    path = tmp_path / "guarded.sqlite"
    store = SQLiteExecutionStore(path)
    store.setup_schema()
    with sqlite3.connect(path) as connection:
        connection.execute(
            "DROP TRIGGER determa_execution_checkpoints_forbid_delete"
        )
    assert store.health() == {"healthy": False, "schema_ready": False}


def test_direct_injection_checks_actual_store_capabilities() -> None:
    with pytest.raises(ExecutionHostError) as error:
        ExecutionHost(
            MemoryExecutionStore(),
            _resolver(),
            required_capabilities={DURABLE_SINGLE_WRITER},
        )
    assert error.value.code == "adapter_capability_mismatch"


def test_configured_sqlite_satisfies_bank_and_outbox_profiles(
    tmp_path: Path,
) -> None:
    store = SQLiteExecutionStore(
        tmp_path / "bank.sqlite",
        replay_retention="permanent",
        outbox_retention="strict",
    )
    store.setup_schema()
    assert {
        DURABLE_SINGLE_WRITER,
        ROOT_IDENTITY_RETENTION,
        PERMANENT_RECEIPT_RETENTION,
        PERMANENT_OUTBOX_TERMINAL_RETENTION,
    }.issubset(store.capabilities)
    host = ExecutionHost(
        store,
        _resolver(),
        required_capabilities={
            DURABLE_SINGLE_WRITER,
            ROOT_IDENTITY_RETENTION,
            PERMANENT_RECEIPT_RETENTION,
        },
        profile="exactly_once_committed_processing",
    )
    ExecutionHost(
        store,
        _resolver(),
        profile="strict_durable_outbox",
        host_features={
            "atomic_checkpoint_processing",
            "outbox_worker",
            "total_outbox_lifecycle",
            "retain_unresolved_outbox",
        },
    )
    checkpoint = host.create(
        load_bundle(MACHINE), "counter", "bank-root", "create", {}
    )
    assert checkpoint["result"] == "committed"
    current = host.read_checkpoint("bank-root")
    assert current is not None
    expected = {
        "expected_revision": current.document["revision"],
        "expected_checkpoint_digest": current.document[
            "execution_checkpoint_digest"
        ],
    }
    with pytest.raises(ExecutionHostError) as retention_error:
        host.update_replay_retention(
            "bank-root",
            {
                "mode": "bounded",
                "permanent_replay_eligible": False,
                "pruned_through_receipt_sequence": None,
                "policy_identifier": "forbidden",
            },
            **expected,
        )
    assert retention_error.value.code == "adapter_capability_mismatch"
    with pytest.raises(ExecutionHostError) as compact_error:
        host.compact_outbox("bank-root", "effect", **expected)
    assert compact_error.value.code == "adapter_capability_mismatch"
    with pytest.raises(ExecutionHostError) as delete_error:
        host.delete_outbox_record("bank-root", "effect", **expected)
    assert delete_error.value.code == "adapter_capability_mismatch"


def test_configured_sqlite_satisfies_compact_outbox_profile(
    tmp_path: Path,
) -> None:
    store = SQLiteExecutionStore(
        tmp_path / "compact.sqlite",
        outbox_retention="compact",
    )
    store.setup_schema()
    assert COMPACT_EFFECT_IDENTITY_RETENTION in store.capabilities
    host = ExecutionHost(
        store,
        _resolver(),
        profile="compact_durable_outbox",
        host_features={
            "atomic_checkpoint_processing",
            "outbox_worker",
            "total_outbox_lifecycle",
            "retain_referenced_effect_tombstones",
        },
    )
    with pytest.raises(ExecutionHostError) as error:
        host.delete_outbox_record(
            "root",
            "effect",
            expected_revision="0",
            expected_checkpoint_digest="sha256:" + ("0" * 64),
        )
    assert error.value.code == "adapter_capability_mismatch"
