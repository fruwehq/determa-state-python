from __future__ import annotations

import sqlite3

import pytest

from determa.state import ExecutionHost, ExecutionStoreError, SQLiteExecutionStore, load_bundle
from determa.state.stores.base import SHARED_APPLICATION_TRANSACTION

from .test_execution_stores import MACHINE, _resolver


def configured(tmp_path):
    store = SQLiteExecutionStore(tmp_path / "shared.sqlite", shared_application_transactions=True)
    store.setup_schema()
    with sqlite3.connect(store.path) as connection:
        connection.execute("CREATE TABLE applications (root TEXT PRIMARY KEY)")
    return store, ExecutionHost(store, _resolver())


def test_sqlite_shared_transaction_is_explicit_and_commits_application_and_root(tmp_path):
    store, host = configured(tmp_path)
    assert SHARED_APPLICATION_TRANSACTION in store.capabilities
    assert SHARED_APPLICATION_TRANSACTION not in SQLiteExecutionStore(store.path).capabilities
    retained = []

    def callback(sql, execution):
        retained.append(sql)
        sql.execute("INSERT INTO applications VALUES (?)", ("root",))
        execution.create_v1(load_bundle(MACHINE), "counter", "create", {})
        # Another reader sees neither uncommitted write.
        with sqlite3.connect(store.path) as connection:
            assert connection.execute("SELECT root FROM applications").fetchall() == []
            assert (
                connection.execute(
                    "SELECT root_instance_id FROM determa_execution_checkpoints"
                ).fetchall()
                == []
            )

    response = host.run_shared_transaction("root", callback)
    assert response["result"] == "committed"
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT root FROM applications").fetchall() == [("root",)]
    assert (
        ExecutionHost(SQLiteExecutionStore(store.path), _resolver()).read_checkpoint("root")
        is not None
    )
    with pytest.raises(ExecutionStoreError, match="shared_transaction_closed"):
        retained[0].execute("SELECT 1")


def test_sqlite_shared_callback_failure_rolls_back_both_writes(tmp_path):
    store, host = configured(tmp_path)

    def callback(sql, execution):
        sql.execute("INSERT INTO applications VALUES (?)", ("root",))
        execution.create_v1(load_bundle(MACHINE), "counter", "create", {})
        raise RuntimeError("application refused")

    with pytest.raises(RuntimeError, match="application refused"):
        host.run_shared_transaction("root", callback)
    assert host.read_checkpoint("root") is None
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT root FROM applications").fetchall() == []


@pytest.mark.parametrize(
    "statement",
    [
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT early",
        "PRAGMA synchronous=OFF",
        "ATTACH ':memory:' AS other",
    ],
)
def test_sqlite_application_callback_cannot_commit_or_change_durability(tmp_path, statement):
    store, host = configured(tmp_path)

    def callback(sql, execution):
        sql.execute("INSERT INTO applications VALUES (?)", ("root",))
        execution.create_v1(load_bundle(MACHINE), "counter", "create", {})
        sql.execute(statement)

    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        host.run_shared_transaction("root", callback)
    assert host.read_checkpoint("root") is None
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT root FROM applications").fetchall() == []


def test_sqlite_default_store_refuses_shared_transaction(tmp_path):
    store = SQLiteExecutionStore(tmp_path / "default.sqlite")
    store.setup_schema()
    host = ExecutionHost(store, _resolver())
    with pytest.raises(Exception, match="adapter_capability_mismatch"):
        host.run_shared_transaction("root", lambda sql, execution: None)
