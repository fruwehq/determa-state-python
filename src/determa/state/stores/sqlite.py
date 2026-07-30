"""Explicit-schema SQLite execution store."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from .base import (
    DURABLE_SINGLE_WRITER,
    ROOT_IDENTITY_RETENTION,
    ExecutionStore,
    ExecutionStoreError,
    ExecutionStoreTransaction,
    checkpoint_metadata,
)

_TABLE = "determa_execution_checkpoints"
_JOURNAL_MODES = {"DELETE", "WAL"}
_SYNCHRONOUS_MODES = {"FULL"}


class _SQLiteTransaction(ExecutionStoreTransaction):
    def __init__(
        self, connection: sqlite3.Connection, root_instance_id: str
    ) -> None:
        self._connection = connection
        self._root_instance_id = root_instance_id

    def load(self) -> bytes | None:
        row = self._connection.execute(
            f"SELECT checkpoint FROM {_TABLE} WHERE root_instance_id = ?",
            (self._root_instance_id,),
        ).fetchone()
        return None if row is None else bytes(row[0])

    def insert(self, checkpoint: bytes) -> bool:
        revision, digest = checkpoint_metadata(checkpoint)
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
        revision, digest = checkpoint_metadata(checkpoint)
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
    ) -> None:
        self.path = str(path)
        self.journal_mode = journal_mode.upper()
        self.synchronous = synchronous.upper()
        self.timeout = timeout
        if (
            not self.path
            or self.path == ":memory:"
            or self.journal_mode not in _JOURNAL_MODES
            or self.synchronous not in _SYNCHRONOUS_MODES
            or timeout <= 0
        ):
            raise ExecutionStoreError("invalid_adapter_configuration")

    @property
    def capabilities(self) -> frozenset[str]:
        return frozenset({DURABLE_SINGLE_WRITER, ROOT_IDENTITY_RETENTION})

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

    @contextmanager
    def transaction(
        self,
        root_instance_id: str,
        *,
        native_transaction: Any | None = None,
    ) -> Iterator[ExecutionStoreTransaction]:
        if native_transaction is not None:
            raise ValueError("sqlite does not expose shared application transactions")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            transaction = _SQLiteTransaction(connection, root_instance_id)
            yield transaction
            connection.commit()
        except sqlite3.OperationalError as exc:
            connection.rollback()
            if "no such table" in str(exc):
                raise ExecutionStoreError(
                    "execution_store_schema_unavailable"
                ) from exc
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
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_TABLE} (
                    root_instance_id TEXT PRIMARY KEY NOT NULL,
                    revision TEXT NOT NULL,
                    checkpoint_digest TEXT NOT NULL,
                    checkpoint BLOB NOT NULL
                )
                """
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def health(self) -> Mapping[str, Any]:
        try:
            connection = self._connect()
            row = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (_TABLE,),
            ).fetchone()
            connection.close()
        except (OSError, sqlite3.Error, ExecutionStoreError):
            return {"healthy": False, "schema_ready": False}
        ready = row is not None
        return {"healthy": ready, "schema_ready": ready}


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
    if set(query) - {"journal_mode", "synchronous", "timeout"}:
        raise ExecutionStoreError("invalid_adapter_configuration")
    journal_mode = _single_query(query, "journal_mode", "WAL")
    synchronous = _single_query(query, "synchronous", "FULL")
    timeout_text = _single_query(query, "timeout", "30")
    try:
        timeout = float(timeout_text)
    except ValueError as exc:
        raise ExecutionStoreError("invalid_adapter_configuration") from exc
    return SQLiteExecutionStore(
        path,
        journal_mode=journal_mode,
        synchronous=synchronous,
        timeout=timeout,
    )
