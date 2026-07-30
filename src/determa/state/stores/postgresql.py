"""Optional lazy Psycopg 3 PostgreSQL execution store."""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from importlib import import_module
from typing import Any
from urllib.parse import urlsplit

from .base import (
    DURABLE_CONCURRENT,
    ROOT_IDENTITY_RETENTION,
    SHARED_APPLICATION_TRANSACTION,
    ExecutionStore,
    ExecutionStoreError,
    ExecutionStoreTransaction,
    checkpoint_metadata,
)

_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*\Z")


def _psycopg() -> Any:
    try:
        return import_module("psycopg")
    except ImportError as exc:
        raise ExecutionStoreError("optional_dependency_unavailable") from exc


class _PostgreSQLTransaction(ExecutionStoreTransaction):
    def __init__(
        self, connection: Any, table_name: str, root_instance_id: str
    ) -> None:
        self._connection = connection
        self._table_name = table_name
        self._root_instance_id = root_instance_id

    def load(self) -> bytes | None:
        row = self._connection.execute(
            f"""
            SELECT checkpoint
            FROM {self._table_name}
            WHERE root_instance_id = %s
            FOR UPDATE
            """,
            (self._root_instance_id,),
        ).fetchone()
        return None if row is None else bytes(row[0])

    def insert(self, checkpoint: bytes) -> bool:
        revision, digest = checkpoint_metadata(checkpoint)
        cursor = self._connection.execute(
            f"""
            INSERT INTO {self._table_name}
                (root_instance_id, revision, checkpoint_digest, checkpoint)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (root_instance_id) DO NOTHING
            """,
            (self._root_instance_id, revision, digest, checkpoint),
        )
        return bool(cursor.rowcount == 1)

    def replace(
        self,
        expected_revision: str,
        expected_checkpoint_digest: str,
        checkpoint: bytes,
    ) -> bool:
        revision, digest = checkpoint_metadata(checkpoint)
        cursor = self._connection.execute(
            f"""
            UPDATE {self._table_name}
            SET revision = %s, checkpoint_digest = %s, checkpoint = %s
            WHERE root_instance_id = %s
              AND revision = %s
              AND checkpoint_digest = %s
            """,
            (
                revision,
                digest,
                checkpoint,
                self._root_instance_id,
                expected_revision,
                expected_checkpoint_digest,
            ),
        )
        return bool(cursor.rowcount == 1)


class PostgreSQLExecutionStore(ExecutionStore):
    """Concurrent CAS storage with optional native transaction reuse."""

    def __init__(
        self,
        conninfo: str,
        *,
        table_name: str = "determa_execution_checkpoints",
    ) -> None:
        if not conninfo or _IDENTIFIER.fullmatch(table_name) is None:
            raise ExecutionStoreError("invalid_adapter_configuration")
        self.conninfo = conninfo
        self.table_name = table_name

    @property
    def capabilities(self) -> frozenset[str]:
        return frozenset(
            {
                DURABLE_CONCURRENT,
                SHARED_APPLICATION_TRANSACTION,
                ROOT_IDENTITY_RETENTION,
            }
        )

    @contextmanager
    def transaction(
        self,
        root_instance_id: str,
        *,
        native_transaction: Any | None = None,
    ) -> Iterator[ExecutionStoreTransaction]:
        psycopg = _psycopg()
        owns_connection = native_transaction is None
        connection: Any = native_transaction
        if owns_connection:
            connection = psycopg.connect(self.conninfo)
        if not owns_connection and (
            connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        ):
            raise ExecutionStoreError("invalid_adapter_configuration")
        try:
            if owns_connection:
                connection.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
            yield _PostgreSQLTransaction(
                connection, self.table_name, root_instance_id
            )
            if owns_connection:
                connection.commit()
        except BaseException:
            if owns_connection:
                connection.rollback()
            raise
        finally:
            if owns_connection:
                connection.close()

    def setup_schema(self) -> None:
        psycopg = _psycopg()
        with psycopg.connect(self.conninfo, autocommit=True) as connection:
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    root_instance_id TEXT PRIMARY KEY,
                    revision TEXT NOT NULL,
                    checkpoint_digest TEXT NOT NULL,
                    checkpoint BYTEA NOT NULL
                )
                """
            )

    def health(self) -> Mapping[str, Any]:
        try:
            psycopg = _psycopg()
            with psycopg.connect(self.conninfo) as connection:
                row = connection.execute(
                    "SELECT to_regclass(%s)",
                    (self.table_name,),
                ).fetchone()
        except Exception:
            return {"healthy": False, "schema_ready": False}
        ready = row is not None and row[0] is not None
        return {"healthy": ready, "schema_ready": ready}


def postgresql_execution_store_factory(
    uri: str, configuration: Mapping[str, Any]
) -> ExecutionStore:
    """Create the ordinary bundled PostgreSQL adapter without importing Psycopg."""
    parsed = urlsplit(uri)
    if parsed.scheme != "postgresql" or parsed.fragment:
        raise ExecutionStoreError("invalid_adapter_configuration")
    if set(configuration) - {"table_name"}:
        raise ExecutionStoreError("invalid_adapter_configuration")
    table_name = configuration.get(
        "table_name", "determa_execution_checkpoints"
    )
    if not isinstance(table_name, str):
        raise ExecutionStoreError("invalid_adapter_configuration")
    return PostgreSQLExecutionStore(uri, table_name=table_name)
