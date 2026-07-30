from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from determa.state import (
    ExecutionHost,
    ExecutionHostError,
    PostgreSQLExecutionStore,
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
        table_name=f"determa_checkpoint_test_{uuid.uuid4().hex}",
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

    with psycopg.connect(store.conninfo) as connection:
        with pytest.raises(RuntimeError), connection.transaction():
            host.create(
                load_bundle(MACHINE),
                "counter",
                "rolled-back-root",
                "create",
                {},
                native_transaction=connection,
            )
            raise RuntimeError("application rollback")
    assert host.read_checkpoint("rolled-back-root") is None
