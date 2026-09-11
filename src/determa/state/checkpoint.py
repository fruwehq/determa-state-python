"""Canonical helpers for the sole supported execution-checkpoint artifact."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from functools import cache
from typing import Any

from .wire import _schema_registry, artifact_schema, canonical_bytes, hash_value


def execution_checkpoint_digest(document: Mapping[str, Any]) -> str:
    """Compute the canonical schema-v2 checkpoint digest."""
    body = copy.deepcopy(dict(document))
    body.pop("execution_checkpoint_digest", None)
    return hash_value(["determa-execution-checkpoint-digest-2", body])


def seal_execution_checkpoint(document: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copied checkpoint with its digest recomputed."""
    result = copy.deepcopy(dict(document))
    result.pop("execution_checkpoint_digest", None)
    result["execution_checkpoint_digest"] = execution_checkpoint_digest(result)
    return result


def serialize_execution_checkpoint(document: Mapping[str, Any]) -> bytes:
    """Return the exact RFC 8785 checkpoint representation."""
    return canonical_bytes(seal_execution_checkpoint(document))


@cache
def _member_validator(name: str) -> Any:
    import jsonschema

    schema = artifact_schema("execution_checkpoint_v2")
    return jsonschema.Draft202012Validator(
        {"$ref": f"{schema['$id']}#/$defs/{name}"}, registry=_schema_registry()
    )


def validate_execution_checkpoint_member(name: str, value: Any) -> bool:
    """Return whether a value matches one closed checkpoint schema member."""
    return next(_member_validator(name).iter_errors(value), None) is None
