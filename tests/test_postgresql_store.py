from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from determa.state import (
    COMPACT_EFFECT_IDENTITY_RETENTION,
    DURABLE_CONCURRENT,
    PERMANENT_OUTBOX_TERMINAL_RETENTION,
    PERMANENT_RECEIPT_RETENTION,
    ROOT_IDENTITY_RETENTION,
    ExecutionHost,
    ExecutionHostError,
    ExecutionStoreError,
    PostgreSQLExecutionStore,
    StagedExecutionResult,
    load_bundle,
    portable_envelope,
)

from .test_checkpoint_host import MACHINE, _host

pytestmark = pytest.mark.skipif(
    not os.environ.get("DETERMA_POSTGRESQL_DSN"),
    reason="DETERMA_POSTGRESQL_DSN is not configured",
)


def _store() -> PostgreSQLExecutionStore:
    pytest.importorskip("psycopg")
    return PostgreSQLExecutionStore(
        os.environ["DETERMA_POSTGRESQL_DSN"],
        table_name=f"determa_checkpoint_test_{uuid.uuid4().hex[:16]}",
    )


def test_postgresql_cas_and_shared_native_transaction() -> None:
    psycopg = pytest.importorskip("psycopg")
    store = _store()
    store.setup_schema()
    local_host, _ = _host()
    resolver = local_host.artifact_resolver
    host = ExecutionHost(store, resolver)
    host.create(load_bundle(MACHINE), "counter", "root", "create", {})
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
            ExecutionHost(store, resolver).foreground_process_delivery(
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

    application_table = f"determa_application_test_{uuid.uuid4().hex}"
    with psycopg.connect(store.conninfo) as connection:
        connection.execute(
            f"CREATE TABLE {application_table} (root_instance_id TEXT PRIMARY KEY)"
        )

    def commit_callback(connection, execution) -> None:
        connection.execute(
            f"INSERT INTO {application_table} (root_instance_id) VALUES (%s)",
            ("shared-root",),
        )
        staged = execution.create(
            load_bundle(MACHINE), "counter", "create", {}
        )
        assert staged == StagedExecutionResult("create")

    committed = host.run_shared_transaction("shared-root", commit_callback)
    assert committed["result"] == "committed"
    with psycopg.connect(store.conninfo) as connection:
        rows = connection.execute(
            f"SELECT root_instance_id FROM {application_table}"
        ).fetchall()
        value = rows[0][0]
        if isinstance(value, bytes):
            value = value.decode("ascii")
        assert value == "shared-root"

    def rollback_callback(connection, execution) -> None:
        connection.execute(
            f"INSERT INTO {application_table} (root_instance_id) VALUES (%s)",
            ("rolled-back-root",),
        )
        execution.create(load_bundle(MACHINE), "counter", "create", {})
        raise RuntimeError("application rollback")

    with pytest.raises(RuntimeError, match="application rollback"):
        host.run_shared_transaction("rolled-back-root", rollback_callback)
    assert host.read_checkpoint("rolled-back-root") is None
    with psycopg.connect(store.conninfo) as connection:
        assert connection.execute(
            f"SELECT root_instance_id FROM {application_table} "
            "WHERE root_instance_id = %s",
            ("rolled-back-root",),
        ).fetchall() == []


def test_postgresql_rejects_a_malformed_existing_schema() -> None:
    psycopg = pytest.importorskip("psycopg")
    store = _store()
    with psycopg.connect(store.conninfo) as connection:
        connection.execute(
            f"CREATE TABLE {store.table_name} (root_instance_id TEXT PRIMARY KEY)"
        )
    with pytest.raises(ExecutionStoreError) as error:
        store.setup_schema()
    assert error.value.code == "execution_store_schema_mismatch"
    assert store.health() == {"healthy": False, "schema_ready": False}

    constrained = _store()
    with psycopg.connect(constrained.conninfo) as connection:
        connection.execute(
            f"""
            CREATE TABLE {constrained.metadata_table} (
                schema_key TEXT PRIMARY KEY NOT NULL,
                schema_version INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            f"INSERT INTO {constrained.metadata_table} VALUES (%s, %s)",
            ("execution_checkpoint", 1),
        )
        connection.execute(
            f"""
            CREATE TABLE {constrained.table_name} (
                root_instance_id TEXT PRIMARY KEY NOT NULL,
                revision TEXT NOT NULL CHECK (length(revision) > 0),
                checkpoint_digest TEXT NOT NULL,
                checkpoint BYTEA NOT NULL
            )
            """
        )
    with pytest.raises(ExecutionStoreError) as constrained_error:
        constrained.setup_schema()
    assert constrained_error.value.code == "execution_store_schema_mismatch"
    assert constrained.health() == {"healthy": False, "schema_ready": False}


def test_postgresql_configured_permanent_strict_profile() -> None:
    pytest.importorskip("psycopg")
    store = PostgreSQLExecutionStore(
        os.environ["DETERMA_POSTGRESQL_DSN"],
        table_name=f"determa_bank_{uuid.uuid4().hex[:16]}",
        replay_retention="permanent",
        outbox_retention="strict",
    )
    store.setup_schema()
    assert {
        DURABLE_CONCURRENT,
        ROOT_IDENTITY_RETENTION,
        PERMANENT_RECEIPT_RETENTION,
        PERMANENT_OUTBOX_TERMINAL_RETENTION,
    }.issubset(store.capabilities)
    local_host, _ = _host()
    ExecutionHost(
        store,
        local_host.artifact_resolver,
        profile="exactly_once_committed_processing",
    )
    ExecutionHost(
        store,
        local_host.artifact_resolver,
        profile="strict_durable_outbox",
        host_features={
            "atomic_checkpoint_processing",
            "outbox_worker",
            "total_outbox_lifecycle",
            "retain_unresolved_outbox",
        },
    )

    compact_store = PostgreSQLExecutionStore(
        os.environ["DETERMA_POSTGRESQL_DSN"],
        table_name=f"determa_compact_{uuid.uuid4().hex[:16]}",
        outbox_retention="compact",
    )
    compact_store.setup_schema()
    assert COMPACT_EFFECT_IDENTITY_RETENTION in compact_store.capabilities
    ExecutionHost(
        compact_store,
        local_host.artifact_resolver,
        profile="compact_durable_outbox",
        host_features={
            "atomic_checkpoint_processing",
            "outbox_worker",
            "total_outbox_lifecycle",
            "retain_referenced_effect_tombstones",
        },
    )
