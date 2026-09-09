"""Public execution-store adapter registration and generic URI resolution."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlsplit

from ..codes import ExecutionStoreAdapterFailureCode as AdapterCode
from .base import ExecutionStore, ExecutionStoreError

ExecutionStoreFactory = Callable[[str, Mapping[str, Any]], ExecutionStore]
_IDENTIFIER = re.compile(r"[a-z][a-z0-9+.-]*\Z")


class ExecutionStoreRegistry:
    """An initially empty, explicit adapter registry."""

    def __init__(self) -> None:
        self._factories: dict[str, ExecutionStoreFactory] = {}

    @property
    def identifiers(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))

    def register(self, identifier: str, factory: ExecutionStoreFactory) -> None:
        if _IDENTIFIER.fullmatch(identifier) is None:
            raise ExecutionStoreError(AdapterCode.INVALID_ADAPTER_CONFIGURATION)
        if identifier in self._factories:
            raise ExecutionStoreError(AdapterCode.DUPLICATE_ADAPTER_REGISTRATION)
        self._factories[identifier] = factory

    def resolve(
        self,
        uri: str,
        *,
        configuration: Mapping[str, Any] | None = None,
        required_capabilities: set[str] | frozenset[str] = frozenset(),
    ) -> ExecutionStore:
        if not isinstance(uri, str):
            raise ExecutionStoreError(AdapterCode.INVALID_ADAPTER_CONFIGURATION)
        scheme = urlsplit(uri).scheme
        factory = self._factories.get(scheme)
        if factory is None:
            raise ExecutionStoreError(AdapterCode.UNKNOWN_ADAPTER)
        try:
            store = factory(uri, dict(configuration or {}))
        except ExecutionStoreError:
            raise
        except (TypeError, ValueError) as exc:
            raise ExecutionStoreError(AdapterCode.INVALID_ADAPTER_CONFIGURATION) from exc
        if required_capabilities and not required_capabilities.issubset(
            store.capabilities
        ):
            raise ExecutionStoreError(AdapterCode.ADAPTER_CAPABILITY_MISMATCH)
        return store


def register_bundled_execution_stores(
    registry: ExecutionStoreRegistry, *, include_postgresql: bool = True
) -> None:
    """Register bundled adapters through the public operation."""
    from .file import file_execution_store_factory
    from .memory import memory_execution_store_factory
    from .sqlite import sqlite_execution_store_factory

    registry.register("memory", memory_execution_store_factory)
    registry.register("file", file_execution_store_factory)
    registry.register("sqlite", sqlite_execution_store_factory)
    if include_postgresql:
        from .postgresql import postgresql_execution_store_factory

        registry.register("postgresql", postgresql_execution_store_factory)


def bundled_execution_store_registry(
    *, include_postgresql: bool = True
) -> ExecutionStoreRegistry:
    """Return a new registry populated only through public registration."""
    registry = ExecutionStoreRegistry()
    register_bundled_execution_stores(
        registry, include_postgresql=include_postgresql
    )
    return registry
