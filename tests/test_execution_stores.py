from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from determa.state import (
    DURABLE_SINGLE_WRITER,
    EPHEMERAL,
    RESTART_PERSISTENT,
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
    host = _create(store)
    restored = host.read_checkpoint("root")
    assert restored is not None
    replay = host.create(
        load_bundle(MACHINE), "counter", "root", "root-create", {}
    )
    assert replay["receipt"]["receipt_sequence"] == "0"


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
