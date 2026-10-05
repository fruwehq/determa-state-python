"""The projection's checkpoint and application rows share one PostgreSQL commit."""

from __future__ import annotations

import os
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

import pytest

from determa.state import ApplicationProjectionFacade, PostgreSQLExecutionStore, load_bundle
from determa.state.wire import canonical_bytes

from .test_execution_stores import MACHINE, _resolver

pytestmark = pytest.mark.skipif(
    not os.environ.get("DETERMA_POSTGRESQL_DSN"),
    reason="DETERMA_POSTGRESQL_DSN is not configured",
)


class DatabaseRows:
    def __init__(self, table: str, *, fail_write: bool = False) -> None:
        self.table = table
        self.fail_write = fail_write

    def read(self, native_transaction: Any, row_ids: Sequence[str]) -> dict[str, Any]:
        row = native_transaction.execute(
            f"SELECT root_instance_id, status, supplemental FROM {self.table} "
            "WHERE row_id = %s FOR UPDATE",
            (row_ids[0],),
        ).fetchone()
        if row is None:
            raise ValueError("selected row missing")
        return {"row_id": row_ids[0], "root": row[0], "status": row[1], "supplemental": row[2]}

    def validate_selection(
        self, rows: dict[str, Any], row_ids: Sequence[str], root_instance_id: str
    ) -> None:
        if rows["row_id"] != row_ids[0] or rows["root"] != root_instance_id:
            raise ValueError("invalid selection")

    def prepare_delivery(
        self, rows: dict[str, Any], request: Mapping[str, Any], delivery: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        return delivery

    def project(self, rows: dict[str, Any], checkpoint: Mapping[str, Any]) -> dict[str, Any]:
        return {**rows, "status": "created", "supplemental": canonical_bytes(checkpoint)}

    def reconstruct(self, rows: dict[str, Any]) -> bytes:
        return bytes(rows["supplemental"])

    def write(self, native_transaction: Any, rows: dict[str, Any]) -> None:
        native_transaction.execute(
            f"UPDATE {self.table} SET status = %s, supplemental = %s WHERE row_id = %s",
            (rows["status"], rows["supplemental"], rows["row_id"]),
        )
        if self.fail_write:
            raise RuntimeError("row write failed")


def test_projection_native_shared_commit_and_rollback() -> None:
    psycopg = pytest.importorskip("psycopg")
    store = PostgreSQLExecutionStore(
        os.environ["DETERMA_POSTGRESQL_DSN"],
        table_name=f"determa_projection_{uuid.uuid4().hex[:16]}",
    )
    store.setup_schema()
    table = f"determa_projection_row_{uuid.uuid4().hex[:16]}"
    with psycopg.connect(store.conninfo) as connection:
        connection.execute(
            f"CREATE TABLE {table} (row_id TEXT PRIMARY KEY, root_instance_id TEXT NOT NULL, "
            "status TEXT NOT NULL, supplemental BYTEA)"
        )
        connection.execute(
            f"INSERT INTO {table} (row_id, root_instance_id, status) VALUES (%s, %s, %s)",
            ("row-1", "root-1", "pending"),
        )
        connection.execute(
            f"INSERT INTO {table} (row_id, root_instance_id, status) VALUES (%s, %s, %s)",
            ("row-2", "root-2", "pending"),
        )
    bundle = load_bundle(MACHINE)
    facade = ApplicationProjectionFacade(store, _resolver())

    def request(root: str, row: str) -> dict[str, Any]:
        return {
            "operation": "create",
            "root_instance_id": root,
            "selected_row_ids": [row],
            "boundary": "create_binding",
            "shared_transaction_requested": True,
            "creation": {"machine_id": "counter", "creation_id": f"create-{root}", "bindings": {}},
        }

    result = facade.run(request("root-1", "row-1"), DatabaseRows(table), create_bundle=bundle)
    assert result["status"] == "running"
    with psycopg.connect(store.conninfo) as connection:
        status, supplemental = connection.execute(
            f"SELECT status, supplemental FROM {table} WHERE row_id = %s", ("row-1",)
        ).fetchone()
    assert status == "created"
    with store.transaction("root-1") as transaction:
        assert bytes(supplemental) == transaction.load()

    with pytest.raises(RuntimeError, match="row write failed"):
        facade.run(
            request("root-2", "row-2"),
            DatabaseRows(table, fail_write=True),
            create_bundle=bundle,
        )
    with store.transaction("root-2") as transaction:
        assert transaction.load() is None
    with psycopg.connect(store.conninfo) as connection:
        assert connection.execute(
            f"SELECT status, supplemental FROM {table} WHERE row_id = %s", ("row-2",)
        ).fetchone() == ("pending", None)
