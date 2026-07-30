"""Optional lazy Psycopg 3 PostgreSQL execution store."""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from importlib import import_module
from typing import Any
from urllib.parse import urlsplit

from .base import (
    COMPACT_EFFECT_IDENTITY_RETENTION,
    DURABLE_CONCURRENT,
    PERMANENT_OUTBOX_TERMINAL_RETENTION,
    PERMANENT_RECEIPT_RETENTION,
    ROOT_IDENTITY_RETENTION,
    SHARED_APPLICATION_TRANSACTION,
    ExecutionStore,
    ExecutionStoreError,
    ExecutionStoreTransaction,
    checkpoint_metadata,
)

_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*\Z")
_REPLAY_RETENTION_MODES = {"bounded", "permanent"}
_OUTBOX_RETENTION_MODES = {"none", "strict", "compact"}
_SCHEMA_KEY = "execution_checkpoint"
_SCHEMA_VERSION = 1


def _psycopg() -> Any:
    try:
        return import_module("psycopg")
    except ImportError as exc:
        raise ExecutionStoreError("optional_dependency_unavailable") from exc


def _database_value(value: Any) -> Any:
    return value.decode("ascii") if isinstance(value, bytes) else value


class _PostgreSQLTransaction(ExecutionStoreTransaction):
    def __init__(
        self, connection: Any, table_name: str, root_instance_id: str
    ) -> None:
        self._connection = connection
        self._table_name = table_name
        self._root_instance_id = root_instance_id

    @property
    def root_instance_id(self) -> str:
        return self._root_instance_id

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
        root_instance_id, revision, digest = checkpoint_metadata(checkpoint)
        if root_instance_id != self._root_instance_id:
            raise ExecutionStoreError("transaction_root_mismatch")
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
        root_instance_id, revision, digest = checkpoint_metadata(checkpoint)
        if root_instance_id != self._root_instance_id:
            raise ExecutionStoreError("transaction_root_mismatch")
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
    """Concurrent CAS storage with host-owned shared transactions."""

    def __init__(
        self,
        conninfo: str,
        *,
        table_name: str = "determa_execution_checkpoints",
        replay_retention: str = "bounded",
        outbox_retention: str = "none",
    ) -> None:
        metadata_table = f"{table_name}_metadata"
        if (
            not conninfo
            or len(metadata_table) > 63
            or _IDENTIFIER.fullmatch(table_name) is None
            or replay_retention not in _REPLAY_RETENTION_MODES
            or outbox_retention not in _OUTBOX_RETENTION_MODES
        ):
            raise ExecutionStoreError("invalid_adapter_configuration")
        self.conninfo = conninfo
        self.table_name = table_name
        self.metadata_table = metadata_table
        self.replay_retention = replay_retention
        self.outbox_retention = outbox_retention

    @property
    def capabilities(self) -> frozenset[str]:
        capabilities = {
            DURABLE_CONCURRENT,
            SHARED_APPLICATION_TRANSACTION,
            ROOT_IDENTITY_RETENTION,
        }
        if self.replay_retention == "permanent":
            capabilities.add(PERMANENT_RECEIPT_RETENTION)
        if self.outbox_retention == "strict":
            capabilities.add(PERMANENT_OUTBOX_TERMINAL_RETENTION)
        elif self.outbox_retention == "compact":
            capabilities.add(COMPACT_EFFECT_IDENTITY_RETENTION)
        return frozenset(capabilities)

    @property
    def checkpoint_retention_mode(self) -> str:
        return self.replay_retention

    def _relation_state(self, connection: Any) -> tuple[Any, Any]:
        row = connection.execute(
            "SELECT to_regclass(%s), to_regclass(%s)",
            (self.table_name, self.metadata_table),
        ).fetchone()
        assert row is not None
        return row[0], row[1]

    def _validate_table(
        self,
        connection: Any,
        table_name: str,
        expected_columns: list[tuple[str, str, str, Any]],
    ) -> None:
        columns = connection.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        ).fetchall()
        normalized_columns = [
            tuple(_database_value(value) for value in row) for row in columns
        ]
        if normalized_columns != expected_columns:
            raise ExecutionStoreError("execution_store_schema_mismatch")
        primary_key = connection.execute(
            """
            SELECT attribute.attname
            FROM pg_constraint AS con
            JOIN unnest(con.conkey) WITH ORDINALITY AS keys(attnum, ordinal)
              ON TRUE
            JOIN pg_attribute AS attribute
              ON attribute.attrelid = con.conrelid
             AND attribute.attnum = keys.attnum
            WHERE con.conrelid = to_regclass(%s)
              AND con.contype = 'p'
            ORDER BY keys.ordinal
            """,
            (table_name,),
        ).fetchall()
        if [_database_value(row[0]) for row in primary_key] != [
            "root_instance_id" if table_name == self.table_name else "schema_key"
        ]:
            raise ExecutionStoreError("execution_store_schema_mismatch")
        constraint_types = connection.execute(
            """
            SELECT contype
            FROM pg_constraint
            WHERE conrelid = to_regclass(%s)
            ORDER BY contype
            """,
            (table_name,),
        ).fetchall()
        index_count = connection.execute(
            "SELECT count(*) FROM pg_index WHERE indrelid = to_regclass(%s)",
            (table_name,),
        ).fetchone()
        trigger_count = connection.execute(
            """
            SELECT count(*)
            FROM pg_trigger
            WHERE tgrelid = to_regclass(%s) AND NOT tgisinternal
            """,
            (table_name,),
        ).fetchone()
        if (
            [_database_value(row[0]) for row in constraint_types] != ["p"]
            or index_count is None
            or index_count[0] != 1
            or trigger_count is None
            or trigger_count[0] != 0
        ):
            raise ExecutionStoreError("execution_store_schema_mismatch")

    def _validate_schema(self, connection: Any) -> None:
        checkpoint_relation, metadata_relation = self._relation_state(connection)
        if checkpoint_relation is None and metadata_relation is None:
            raise ExecutionStoreError("execution_store_schema_unavailable")
        if checkpoint_relation is None or metadata_relation is None:
            raise ExecutionStoreError("execution_store_schema_mismatch")
        self._validate_table(
            connection,
            self.table_name,
            [
                ("root_instance_id", "text", "NO", None),
                ("revision", "text", "NO", None),
                ("checkpoint_digest", "text", "NO", None),
                ("checkpoint", "bytea", "NO", None),
            ],
        )
        self._validate_table(
            connection,
            self.metadata_table,
            [
                ("schema_key", "text", "NO", None),
                ("schema_version", "integer", "NO", None),
            ],
        )
        rows = connection.execute(
            f"SELECT schema_key, schema_version FROM {self.metadata_table}"
        ).fetchall()
        normalized_rows = [
            tuple(_database_value(value) for value in row) for row in rows
        ]
        if normalized_rows != [(_SCHEMA_KEY, _SCHEMA_VERSION)]:
            raise ExecutionStoreError("execution_store_schema_mismatch")

    def validate_schema(self) -> None:
        psycopg = _psycopg()
        with psycopg.connect(self.conninfo) as connection:
            self._validate_schema(connection)

    @contextmanager
    def transaction(
        self,
        root_instance_id: str,
    ) -> Iterator[ExecutionStoreTransaction]:
        psycopg = _psycopg()
        with psycopg.connect(self.conninfo) as connection:
            self._validate_schema(connection)
            yield _PostgreSQLTransaction(
                connection, self.table_name, root_instance_id
            )

    @contextmanager
    def shared_transaction(
        self,
        root_instance_id: str,
    ) -> Iterator[tuple[Any, ExecutionStoreTransaction]]:
        psycopg = _psycopg()
        with psycopg.connect(self.conninfo) as connection:
            self._validate_schema(connection)
            yield (
                connection,
                _PostgreSQLTransaction(
                    connection, self.table_name, root_instance_id
                ),
            )

    def setup_schema(self) -> None:
        psycopg = _psycopg()
        with psycopg.connect(self.conninfo) as connection:
            checkpoint_relation, metadata_relation = self._relation_state(connection)
            if checkpoint_relation is None and metadata_relation is None:
                connection.execute(
                    f"""
                    CREATE TABLE {self.metadata_table} (
                        schema_key TEXT PRIMARY KEY NOT NULL,
                        schema_version INTEGER NOT NULL
                    )
                    """
                )
                connection.execute(
                    f"""
                    CREATE TABLE {self.table_name} (
                        root_instance_id TEXT PRIMARY KEY NOT NULL,
                        revision TEXT NOT NULL,
                        checkpoint_digest TEXT NOT NULL,
                        checkpoint BYTEA NOT NULL
                    )
                    """
                )
                connection.execute(
                    f"""
                    INSERT INTO {self.metadata_table}
                        (schema_key, schema_version)
                    VALUES (%s, %s)
                    """,
                    (_SCHEMA_KEY, _SCHEMA_VERSION),
                )
            self._validate_schema(connection)

    def health(self) -> Mapping[str, Any]:
        try:
            self.validate_schema()
        except Exception:
            return {"healthy": False, "schema_ready": False}
        return {"healthy": True, "schema_ready": True, "schema_version": 1}


def postgresql_execution_store_factory(
    uri: str, configuration: Mapping[str, Any]
) -> ExecutionStore:
    """Create the ordinary bundled PostgreSQL adapter without importing Psycopg."""
    parsed = urlsplit(uri)
    if parsed.scheme != "postgresql" or parsed.fragment:
        raise ExecutionStoreError("invalid_adapter_configuration")
    if set(configuration) - {
        "table_name",
        "replay_retention",
        "outbox_retention",
    }:
        raise ExecutionStoreError("invalid_adapter_configuration")
    table_name = configuration.get(
        "table_name", "determa_execution_checkpoints"
    )
    replay_retention = configuration.get("replay_retention", "bounded")
    outbox_retention = configuration.get("outbox_retention", "none")
    if not all(
        isinstance(value, str)
        for value in (table_name, replay_retention, outbox_retention)
    ):
        raise ExecutionStoreError("invalid_adapter_configuration")
    return PostgreSQLExecutionStore(
        uri,
        table_name=table_name,
        replay_retention=replay_retention,
        outbox_retention=outbox_retention,
    )
