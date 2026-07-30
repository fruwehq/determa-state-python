"""Public execution-store adapters and registration."""

from .base import (
    COMPACT_EFFECT_IDENTITY_RETENTION,
    DURABLE_CONCURRENT,
    DURABLE_SINGLE_WRITER,
    EPHEMERAL,
    PERMANENT_OUTBOX_TERMINAL_RETENTION,
    PERMANENT_RECEIPT_RETENTION,
    RESTART_PERSISTENT,
    ROOT_IDENTITY_RETENTION,
    SHARED_APPLICATION_TRANSACTION,
    STANDARD_CAPABILITIES,
    ExecutionStore,
    ExecutionStoreError,
    ExecutionStoreTransaction,
)
from .file import FileExecutionStore, file_execution_store_factory
from .memory import MemoryExecutionStore, memory_execution_store_factory
from .postgresql import (
    PostgreSQLExecutionStore,
    postgresql_execution_store_factory,
)
from .registry import (
    ExecutionStoreFactory,
    ExecutionStoreRegistry,
    bundled_execution_store_registry,
    register_bundled_execution_stores,
)
from .sqlite import SQLiteExecutionStore, sqlite_execution_store_factory

__all__ = [
    "COMPACT_EFFECT_IDENTITY_RETENTION",
    "DURABLE_CONCURRENT",
    "DURABLE_SINGLE_WRITER",
    "EPHEMERAL",
    "ExecutionStore",
    "ExecutionStoreError",
    "ExecutionStoreFactory",
    "ExecutionStoreRegistry",
    "ExecutionStoreTransaction",
    "FileExecutionStore",
    "MemoryExecutionStore",
    "PERMANENT_OUTBOX_TERMINAL_RETENTION",
    "PERMANENT_RECEIPT_RETENTION",
    "PostgreSQLExecutionStore",
    "RESTART_PERSISTENT",
    "ROOT_IDENTITY_RETENTION",
    "SHARED_APPLICATION_TRANSACTION",
    "STANDARD_CAPABILITIES",
    "SQLiteExecutionStore",
    "bundled_execution_store_registry",
    "file_execution_store_factory",
    "memory_execution_store_factory",
    "postgresql_execution_store_factory",
    "register_bundled_execution_stores",
    "sqlite_execution_store_factory",
]
