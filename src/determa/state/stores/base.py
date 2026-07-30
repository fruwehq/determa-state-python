"""Public synchronous execution-store contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import Any

from ..errors import ArtifactError, DetermaError
from ..wire import strict_json

EPHEMERAL = "ephemeral"
RESTART_PERSISTENT = "restart_persistent"
DURABLE_SINGLE_WRITER = "durable_single_writer"
DURABLE_CONCURRENT = "durable_concurrent"
SHARED_APPLICATION_TRANSACTION = "shared_application_transaction"
PERMANENT_RECEIPT_RETENTION = "permanent_receipt_retention"
ROOT_IDENTITY_RETENTION = "root_identity_retention"
PERMANENT_OUTBOX_TERMINAL_RETENTION = "permanent_outbox_terminal_retention"
COMPACT_EFFECT_IDENTITY_RETENTION = "compact_effect_identity_retention"

STANDARD_CAPABILITIES = frozenset(
    {
        EPHEMERAL,
        RESTART_PERSISTENT,
        DURABLE_SINGLE_WRITER,
        DURABLE_CONCURRENT,
        SHARED_APPLICATION_TRANSACTION,
        PERMANENT_RECEIPT_RETENTION,
        ROOT_IDENTITY_RETENTION,
        PERMANENT_OUTBOX_TERMINAL_RETENTION,
        COMPACT_EFFECT_IDENTITY_RETENTION,
    }
)


class ExecutionStoreError(DetermaError):
    """A closed execution-store or adapter failure."""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.message)


class ExecutionStoreTransaction(ABC):
    """One exclusive or serializable transaction for a single root."""

    @abstractmethod
    def load(self) -> bytes | None:
        """Read the current checkpoint bytes."""

    @abstractmethod
    def insert(self, checkpoint: bytes) -> bool:
        """Stage an absent-root insert, returning false if the root exists."""

    @abstractmethod
    def replace(
        self,
        expected_revision: str,
        expected_checkpoint_digest: str,
        checkpoint: bytes,
    ) -> bool:
        """Stage an exact revision/digest compare-and-swap."""


class ExecutionStore(ABC):
    """Configured execution-store instance suitable for direct injection."""

    @property
    @abstractmethod
    def capabilities(self) -> frozenset[str]:
        """Capabilities proved by this configured instance."""

    @abstractmethod
    def transaction(
        self,
        root_instance_id: str,
        *,
        native_transaction: Any | None = None,
    ) -> AbstractContextManager[ExecutionStoreTransaction]:
        """Open one root transaction."""

    @abstractmethod
    def setup_schema(self) -> None:
        """Explicitly create the adapter's storage schema."""

    @abstractmethod
    def health(self) -> Mapping[str, Any]:
        """Return adapter health without mutating checkpoint storage."""


def checkpoint_metadata(source: bytes) -> tuple[str, str]:
    """Extract CAS metadata from structurally closed checkpoint bytes."""
    try:
        document, _ = strict_json(source)
        revision = document["revision"]
        digest = document["execution_checkpoint_digest"]
    except (ArtifactError, KeyError, TypeError) as exc:
        raise ExecutionStoreError("invalid_execution_checkpoint") from exc
    if not isinstance(revision, str) or not isinstance(digest, str):
        raise ExecutionStoreError("invalid_execution_checkpoint")
    return revision, digest
