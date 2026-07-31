"""Ephemeral in-memory execution store."""

from __future__ import annotations

import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from .base import (
    EPHEMERAL,
    ExecutionStore,
    ExecutionStoreError,
    ExecutionStoreTransaction,
    checkpoint_metadata,
)


class _MemoryTransaction(ExecutionStoreTransaction):
    def __init__(
        self, records: dict[str, bytes], root_instance_id: str
    ) -> None:
        self._records = records
        self._root_instance_id = root_instance_id
        self._current = records.get(root_instance_id)
        self._candidate = self._current

    @property
    def root_instance_id(self) -> str:
        return self._root_instance_id

    def load(self) -> bytes | None:
        return self._current

    def insert(self, checkpoint: bytes) -> bool:
        root_instance_id, _, _ = checkpoint_metadata(checkpoint)
        if root_instance_id != self._root_instance_id:
            raise ExecutionStoreError("transaction_root_mismatch")
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
        root_instance_id, revision, digest = checkpoint_metadata(self._current)
        candidate_root, _, _ = checkpoint_metadata(checkpoint)
        if (
            root_instance_id != self._root_instance_id
            or candidate_root != self._root_instance_id
        ):
            raise ExecutionStoreError("transaction_root_mismatch")
        if (revision, digest) != (
            expected_revision,
            expected_checkpoint_digest,
        ):
            return False
        self._candidate = bytes(checkpoint)
        return True

    def commit(self) -> None:
        if self._candidate is not self._current:
            assert self._candidate is not None
            self._records[self._root_instance_id] = self._candidate


class MemoryExecutionStore(ExecutionStore):
    """Process-local checkpoint storage with no durability claim."""

    def __init__(self, initial: Mapping[str, bytes] | None = None) -> None:
        self._records = {
            root_instance_id: bytes(checkpoint)
            for root_instance_id, checkpoint in (initial or {}).items()
        }
        self._lock = threading.RLock()

    @property
    def capabilities(self) -> frozenset[str]:
        return frozenset({EPHEMERAL})

    @contextmanager
    def transaction(
        self,
        root_instance_id: str,
    ) -> Iterator[ExecutionStoreTransaction]:
        with self._lock:
            transaction = _MemoryTransaction(self._records, root_instance_id)
            yield transaction
            transaction.commit()

    def setup_schema(self) -> None:
        return None

    def health(self) -> Mapping[str, Any]:
        return {"healthy": True, "record_count": len(self._records)}


def memory_execution_store_factory(
    uri: str, configuration: Mapping[str, Any]
) -> ExecutionStore:
    """Create the ordinary bundled memory adapter."""
    if uri != "memory:" or configuration:
        from .base import ExecutionStoreError

        raise ExecutionStoreError("invalid_adapter_configuration")
    return MemoryExecutionStore()
