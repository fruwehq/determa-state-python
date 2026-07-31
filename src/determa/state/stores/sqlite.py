"""Explicit-schema SQLite execution store."""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from .base import (
    COMPACT_EFFECT_IDENTITY_RETENTION,
    DURABLE_SINGLE_WRITER,
    PERMANENT_OUTBOX_TERMINAL_RETENTION,
    PERMANENT_RECEIPT_RETENTION,
    ROOT_IDENTITY_RETENTION,
    ExecutionStore,
    ExecutionStoreError,
    ExecutionStoreTransaction,
    checkpoint_metadata,
)

_TABLE = "determa_execution_checkpoints"
_METADATA_TABLE = "determa_execution_store_metadata"
_JOURNAL_MODES = {"DELETE", "WAL"}
_SYNCHRONOUS_MODES = {"FULL"}
_REPLAY_RETENTION_MODES = {"bounded", "permanent"}
_OUTBOX_RETENTION_MODES = {"none", "strict", "compact"}
_SCHEMA_VERSION = 2
_SCHEMA_VERSION_KEY = "execution_checkpoint_schema_version"
_REPLAY_RETENTION_KEY = "replay_retention"
_OUTBOX_RETENTION_KEY = "outbox_retention"
_CHECKPOINT_DELETE_TRIGGER = "determa_execution_checkpoints_forbid_delete"
_METADATA_INSERT_TRIGGER = "determa_execution_metadata_forbid_insert"
_METADATA_UPDATE_TRIGGER = "determa_execution_metadata_forbid_update"
_METADATA_DELETE_TRIGGER = "determa_execution_metadata_forbid_delete"
_IMMUTABLE_MESSAGE = "execution_store_immutable"


def _schema_tokens(source: str) -> list[str]:
    return re.findall(r"[a-z_][a-z0-9_]*|[(),]", source.lower())


class _SQLiteTransaction(ExecutionStoreTransaction):
    def __init__(
        self, connection: sqlite3.Connection, root_instance_id: str
    ) -> None:
        self._connection = connection
        self._root_instance_id = root_instance_id

    @property
    def root_instance_id(self) -> str:
        return self._root_instance_id

    def load(self) -> bytes | None:
        row = self._connection.execute(
            f"SELECT checkpoint FROM {_TABLE} WHERE root_instance_id = ?",
            (self._root_instance_id,),
        ).fetchone()
        return None if row is None else bytes(row[0])

    def insert(self, checkpoint: bytes) -> bool:
        root_instance_id, revision, digest = checkpoint_metadata(checkpoint)
        if root_instance_id != self._root_instance_id:
            raise ExecutionStoreError("transaction_root_mismatch")
        try:
            self._connection.execute(
                f"""
                INSERT INTO {_TABLE}
                    (root_instance_id, revision, checkpoint_digest, checkpoint)
                VALUES (?, ?, ?, ?)
                """,
                (self._root_instance_id, revision, digest, checkpoint),
            )
        except sqlite3.IntegrityError:
            return False
        return True

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
            UPDATE {_TABLE}
            SET revision = ?, checkpoint_digest = ?, checkpoint = ?
            WHERE root_instance_id = ?
              AND revision = ?
              AND checkpoint_digest = ?
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
        return cursor.rowcount == 1


class SQLiteExecutionStore(ExecutionStore):
    """Single-writer durable SQLite storage under verified PRAGMA settings."""

    def __init__(
        self,
        path: str | Path,
        *,
        journal_mode: str = "WAL",
        synchronous: str = "FULL",
        timeout: float = 30.0,
        replay_retention: str = "bounded",
        outbox_retention: str = "none",
    ) -> None:
        self.path = str(path)
        self.journal_mode = journal_mode.upper()
        self.synchronous = synchronous.upper()
        self.timeout = timeout
        self.replay_retention = replay_retention
        self.outbox_retention = outbox_retention
        if (
            not self.path
            or self.path == ":memory:"
            or self.journal_mode not in _JOURNAL_MODES
            or self.synchronous not in _SYNCHRONOUS_MODES
            or timeout <= 0
            or replay_retention not in _REPLAY_RETENTION_MODES
            or outbox_retention not in _OUTBOX_RETENTION_MODES
        ):
            raise ExecutionStoreError("invalid_adapter_configuration")

    @property
    def capabilities(self) -> frozenset[str]:
        capabilities = {DURABLE_SINGLE_WRITER, ROOT_IDENTITY_RETENTION}
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

    def _metadata_rows(self) -> list[tuple[str, str]]:
        return [
            (_SCHEMA_VERSION_KEY, str(_SCHEMA_VERSION)),
            (_OUTBOX_RETENTION_KEY, self.outbox_retention),
            (_REPLAY_RETENTION_KEY, self.replay_retention),
        ]

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path, timeout=self.timeout, isolation_level=None
        )
        journal_mode = connection.execute(
            f"PRAGMA journal_mode = {self.journal_mode}"
        ).fetchone()
        connection.execute(f"PRAGMA synchronous = {self.synchronous}")
        actual_synchronous = connection.execute("PRAGMA synchronous").fetchone()
        expected_synchronous = {"FULL": 2}[self.synchronous]
        if (
            journal_mode is None
            or str(journal_mode[0]).upper() != self.journal_mode
            or actual_synchronous is None
            or int(actual_synchronous[0]) != expected_synchronous
        ):
            connection.close()
            raise ExecutionStoreError("invalid_adapter_configuration")
        return connection

    def _validate_table(
        self,
        connection: sqlite3.Connection,
        table: str,
        expected_columns: list[tuple[str, str, int, Any, int, int]],
        expected_sql: str,
    ) -> None:
        columns = [
            (row[1], str(row[2]).upper(), row[3], row[4], row[5], row[6])
            for row in connection.execute(f"PRAGMA table_xinfo({table})")
        ]
        if columns != expected_columns:
            raise ExecutionStoreError("execution_store_schema_mismatch")
        schema_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        if (
            schema_row is None
            or not isinstance(schema_row[0], str)
            or _schema_tokens(schema_row[0]) != _schema_tokens(expected_sql)
        ):
            raise ExecutionStoreError("execution_store_schema_mismatch")
        indexes = [
            (row[2], row[3], row[4])
            for row in connection.execute(f"PRAGMA index_list({table})")
        ]
        if indexes != [(1, "pk", 0)]:
            raise ExecutionStoreError("execution_store_schema_mismatch")
        foreign_keys = connection.execute(
            f"PRAGMA foreign_key_list({table})"
        ).fetchall()
        if foreign_keys:
            raise ExecutionStoreError("execution_store_schema_mismatch")

    def _validate_triggers(self, connection: sqlite3.Connection) -> None:
        expected = {
            _CHECKPOINT_DELETE_TRIGGER: f"""
                CREATE TRIGGER {_CHECKPOINT_DELETE_TRIGGER}
                BEFORE DELETE ON {_TABLE}
                BEGIN
                    SELECT RAISE(ABORT, '{_IMMUTABLE_MESSAGE}');
                END
            """,
            _METADATA_INSERT_TRIGGER: f"""
                CREATE TRIGGER {_METADATA_INSERT_TRIGGER}
                BEFORE INSERT ON {_METADATA_TABLE}
                BEGIN
                    SELECT RAISE(ABORT, '{_IMMUTABLE_MESSAGE}');
                END
            """,
            _METADATA_UPDATE_TRIGGER: f"""
                CREATE TRIGGER {_METADATA_UPDATE_TRIGGER}
                BEFORE UPDATE ON {_METADATA_TABLE}
                BEGIN
                    SELECT RAISE(ABORT, '{_IMMUTABLE_MESSAGE}');
                END
            """,
            _METADATA_DELETE_TRIGGER: f"""
                CREATE TRIGGER {_METADATA_DELETE_TRIGGER}
                BEFORE DELETE ON {_METADATA_TABLE}
                BEGIN
                    SELECT RAISE(ABORT, '{_IMMUTABLE_MESSAGE}');
                END
            """,
        }
        rows = connection.execute(
            """
            SELECT name, tbl_name, sql
            FROM sqlite_master
            WHERE type = 'trigger'
              AND tbl_name IN (?, ?)
            ORDER BY name
            """,
            (_TABLE, _METADATA_TABLE),
        ).fetchall()
        if len(rows) != len(expected):
            raise ExecutionStoreError("execution_store_schema_mismatch")
        for name, table, source in rows:
            expected_source = expected.get(name)
            expected_table = (
                _TABLE
                if name == _CHECKPOINT_DELETE_TRIGGER
                else _METADATA_TABLE
            )
            if (
                table != expected_table
                or not isinstance(source, str)
                or expected_source is None
                or _schema_tokens(source) != _schema_tokens(expected_source)
            ):
                raise ExecutionStoreError("execution_store_schema_mismatch")

    def _validate_schema(self, connection: sqlite3.Connection) -> None:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?)",
                (_TABLE, _METADATA_TABLE),
            )
        }
        if not tables:
            raise ExecutionStoreError("execution_store_schema_unavailable")
        if tables != {_TABLE, _METADATA_TABLE}:
            raise ExecutionStoreError("execution_store_schema_mismatch")
        self._validate_table(
            connection,
            _TABLE,
            [
                ("root_instance_id", "TEXT", 1, None, 1, 0),
                ("revision", "TEXT", 1, None, 0, 0),
                ("checkpoint_digest", "TEXT", 1, None, 0, 0),
                ("checkpoint", "BLOB", 1, None, 0, 0),
            ],
            f"""
            CREATE TABLE {_TABLE} (
                root_instance_id TEXT PRIMARY KEY NOT NULL,
                revision TEXT NOT NULL,
                checkpoint_digest TEXT NOT NULL,
                checkpoint BLOB NOT NULL
            )
            """,
        )
        self._validate_table(
            connection,
            _METADATA_TABLE,
            [
                ("schema_key", "TEXT", 1, None, 1, 0),
                ("schema_value", "TEXT", 1, None, 0, 0),
            ],
            f"""
            CREATE TABLE {_METADATA_TABLE} (
                schema_key TEXT PRIMARY KEY NOT NULL,
                schema_value TEXT NOT NULL
            )
            """,
        )
        rows = connection.execute(
            f"SELECT schema_key, schema_value FROM {_METADATA_TABLE} "
            "ORDER BY schema_key"
        ).fetchall()
        if rows != self._metadata_rows():
            raise ExecutionStoreError("execution_store_schema_mismatch")
        self._validate_triggers(connection)

    def validate_schema(self) -> None:
        connection = self._connect()
        try:
            self._validate_schema(connection)
        finally:
            connection.close()

    @contextmanager
    def transaction(
        self,
        root_instance_id: str,
    ) -> Iterator[ExecutionStoreTransaction]:
        connection = self._connect()
        try:
            self._validate_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            transaction = _SQLiteTransaction(connection, root_instance_id)
            yield transaction
            connection.commit()
        except sqlite3.OperationalError:
            connection.rollback()
            raise
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def setup_schema(self) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name IN (?, ?)",
                    (_TABLE, _METADATA_TABLE),
                )
            }
            if not tables:
                connection.execute(
                    f"""
                    CREATE TABLE {_METADATA_TABLE} (
                        schema_key TEXT PRIMARY KEY NOT NULL,
                        schema_value TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    f"""
                    CREATE TABLE {_TABLE} (
                        root_instance_id TEXT PRIMARY KEY NOT NULL,
                        revision TEXT NOT NULL,
                        checkpoint_digest TEXT NOT NULL,
                        checkpoint BLOB NOT NULL
                    )
                    """
                )
                connection.executemany(
                    f"INSERT INTO {_METADATA_TABLE} (schema_key, schema_value) "
                    "VALUES (?, ?)",
                    self._metadata_rows(),
                )
                connection.execute(
                    f"""
                    CREATE TRIGGER {_CHECKPOINT_DELETE_TRIGGER}
                    BEFORE DELETE ON {_TABLE}
                    BEGIN
                        SELECT RAISE(ABORT, '{_IMMUTABLE_MESSAGE}');
                    END
                    """
                )
                for name, operation in (
                    (_METADATA_INSERT_TRIGGER, "INSERT"),
                    (_METADATA_UPDATE_TRIGGER, "UPDATE"),
                    (_METADATA_DELETE_TRIGGER, "DELETE"),
                ):
                    connection.execute(
                        f"""
                        CREATE TRIGGER {name}
                        BEFORE {operation} ON {_METADATA_TABLE}
                        BEGIN
                            SELECT RAISE(ABORT, '{_IMMUTABLE_MESSAGE}');
                        END
                        """
                    )
            self._validate_schema(connection)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def health(self) -> Mapping[str, Any]:
        try:
            connection = self._connect()
            try:
                self._validate_schema(connection)
            finally:
                connection.close()
        except (OSError, sqlite3.Error, ExecutionStoreError):
            return {"healthy": False, "schema_ready": False}
        return {
            "healthy": True,
            "schema_ready": True,
            "schema_version": _SCHEMA_VERSION,
        }


def _single_query(query: Mapping[str, list[str]], key: str, default: str) -> str:
    values = query.get(key)
    if values is None:
        return default
    if len(values) != 1:
        raise ExecutionStoreError("invalid_adapter_configuration")
    return values[0]


def sqlite_execution_store_factory(
    uri: str, configuration: Mapping[str, Any]
) -> ExecutionStore:
    """Create the ordinary bundled SQLite adapter."""
    parsed = urlsplit(uri)
    if (
        parsed.scheme != "sqlite"
        or parsed.netloc not in {"", "localhost"}
        or parsed.fragment
        or configuration
    ):
        raise ExecutionStoreError("invalid_adapter_configuration")
    path = unquote(parsed.path)
    if not path or not Path(path).is_absolute():
        raise ExecutionStoreError("invalid_adapter_configuration")
    query = parse_qs(parsed.query, keep_blank_values=True)
    if set(query) - {
        "journal_mode",
        "synchronous",
        "timeout",
        "replay_retention",
        "outbox_retention",
    }:
        raise ExecutionStoreError("invalid_adapter_configuration")
    journal_mode = _single_query(query, "journal_mode", "WAL")
    synchronous = _single_query(query, "synchronous", "FULL")
    timeout_text = _single_query(query, "timeout", "30")
    replay_retention = _single_query(query, "replay_retention", "bounded")
    outbox_retention = _single_query(query, "outbox_retention", "none")
    try:
        timeout = float(timeout_text)
    except ValueError as exc:
        raise ExecutionStoreError("invalid_adapter_configuration") from exc
    return SQLiteExecutionStore(
        path,
        journal_mode=journal_mode,
        synchronous=synchronous,
        timeout=timeout,
        replay_retention=replay_retention,
        outbox_retention=outbox_retention,
    )
