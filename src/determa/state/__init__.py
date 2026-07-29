"""Python reference implementation of Determa State format 1."""

from __future__ import annotations

import logging

from .__about__ import __version__
from .definition import Bundle, BundleSource, load_bundle
from .engine import Delivery, Result, create, dispatch
from .errors import (
    ArtifactError,
    CelError,
    DetermaError,
    ErrorRecord,
    SchemaError,
    ValidationError,
)
from .migration import (
    MigrationDispatchResult,
    MigrationFailure,
    MigrationLimits,
    MigrationResult,
    migrate_aggregate,
    migrate_and_dispatch,
)
from .validator import collect_errors, validate
from .wire import (
    ArtifactResolver,
    DefinitionResolver,
    MemoryArtifactResolver,
    MigrationDescriptorResolver,
    RestoredAggregate,
    RestoredAggregatePackage,
    aggregate_envelope,
    aggregate_shape_fingerprint,
    restore_aggregate,
    restore_aggregate_package,
    serialize_aggregate,
)

__all__ = [
    "ArtifactError",
    "ArtifactResolver",
    "Bundle",
    "BundleSource",
    "CelError",
    "DetermaError",
    "DefinitionResolver",
    "Delivery",
    "ErrorRecord",
    "MemoryArtifactResolver",
    "MigrationDescriptorResolver",
    "MigrationDispatchResult",
    "MigrationFailure",
    "MigrationLimits",
    "MigrationResult",
    "Result",
    "RestoredAggregate",
    "RestoredAggregatePackage",
    "SchemaError",
    "ValidationError",
    "__version__",
    "aggregate_envelope",
    "aggregate_shape_fingerprint",
    "collect_errors",
    "create",
    "dispatch",
    "load_bundle",
    "migrate_aggregate",
    "migrate_and_dispatch",
    "restore_aggregate",
    "restore_aggregate_package",
    "serialize_aggregate",
    "validate",
]

logging.getLogger("determa.state").addHandler(logging.NullHandler())
