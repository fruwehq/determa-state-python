"""Locked restart-persistent file execution store."""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import unquote, urlsplit

from .base import (
    RESTART_PERSISTENT,
    ExecutionStore,
    ExecutionStoreError,
    ExecutionStoreTransaction,
    checkpoint_metadata,
)

_SCHEMA_MARKER = ".determa-execution-store-v1"


class _FileTransaction(ExecutionStoreTransaction):
    def __init__(self, checkpoint_path: Path) -> None:
        self._checkpoint_path = checkpoint_path
        self._current: bytes | None
        try:
            self._current = checkpoint_path.read_bytes()
        except FileNotFoundError:
            self._current = None
        self._candidate: bytes | None = self._current

    def load(self) -> bytes | None:
        return self._current

    def insert(self, checkpoint: bytes) -> bool:
        if self._current is not None:
            return False
        self._candidate = bytes(checkpoint)
        return True

    def replace(
        self,
        expected_revision: str,
        expected_checkpoint_digest: str,
        checkpoint: bytes,
    ) -> bool:
        if self._current is None:
            return False
        if checkpoint_metadata(self._current) != (
            expected_revision,
            expected_checkpoint_digest,
        ):
            return False
        self._candidate = bytes(checkpoint)
        return True

    def commit(self) -> None:
        if self._candidate is self._current:
            return
        assert self._candidate is not None
        descriptor, temporary_name = tempfile.mkstemp(
            dir=self._checkpoint_path.parent,
            prefix=f".{self._checkpoint_path.name}.",
            suffix=".tmp",
        )
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(self._candidate)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_name, self._checkpoint_path)
            directory_descriptor = os.open(self._checkpoint_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise


class FileExecutionStore(ExecutionStore):
    """Atomic locked files with restart persistence but no crash-durability claim."""

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self.directory = Path(directory)

    @property
    def capabilities(self) -> frozenset[str]:
        return frozenset({RESTART_PERSISTENT})

    def _require_schema(self) -> None:
        if not (self.directory / _SCHEMA_MARKER).is_file():
            raise ExecutionStoreError("execution_store_schema_unavailable")

    def _stem(self, root_instance_id: str) -> str:
        return hashlib.sha256(root_instance_id.encode("utf-8")).hexdigest()

    @contextmanager
    def transaction(
        self,
        root_instance_id: str,
        *,
        native_transaction: Any | None = None,
    ) -> Iterator[ExecutionStoreTransaction]:
        if native_transaction is not None:
            raise ValueError("file does not accept a native transaction")
        self._require_schema()
        import fcntl

        stem = self._stem(root_instance_id)
        lock_path = self.directory / f"{stem}.lock"
        checkpoint_path = self.directory / f"{stem}.json"
        lock: BinaryIO
        with lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                transaction = _FileTransaction(checkpoint_path)
                yield transaction
                transaction.commit()
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def setup_schema(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        marker = self.directory / _SCHEMA_MARKER
        if marker.exists():
            if marker.read_text(encoding="ascii") != "1\n":
                raise ExecutionStoreError("execution_store_schema_mismatch")
            return
        descriptor, temporary_name = tempfile.mkstemp(
            dir=self.directory, prefix=f".{_SCHEMA_MARKER}.", suffix=".tmp"
        )
        try:
            with os.fdopen(descriptor, "w", encoding="ascii") as stream:
                stream.write("1\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_name, marker)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise

    def health(self) -> Mapping[str, Any]:
        ready = (self.directory / _SCHEMA_MARKER).is_file()
        return {"healthy": ready, "schema_ready": ready}


def file_execution_store_factory(
    uri: str, configuration: Mapping[str, Any]
) -> ExecutionStore:
    """Create the ordinary bundled file adapter."""
    parsed = urlsplit(uri)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        raise ExecutionStoreError("invalid_adapter_configuration")
    if parsed.query or parsed.fragment or set(configuration) - {"directory"}:
        raise ExecutionStoreError("invalid_adapter_configuration")
    configured = configuration.get("directory")
    if configured is not None and not isinstance(configured, str):
        raise ExecutionStoreError("invalid_adapter_configuration")
    directory = configured if configured is not None else unquote(parsed.path)
    if not directory or not Path(directory).is_absolute():
        raise ExecutionStoreError("invalid_adapter_configuration")
    return FileExecutionStore(directory)
