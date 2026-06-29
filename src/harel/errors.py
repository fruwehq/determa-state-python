"""Exception types for the harel engine (SPEC §2, §13.2/§13.4).

Validation errors carry a structured list of ``{"path": str, "message": str}``
records, matching the JSON shape the CLI emits for ``validate`` (SPEC §13.4).
"""

from __future__ import annotations

from typing import TypedDict


class ErrorRecord(TypedDict):
    """A single validation error, in the §13.4 ``{path, message}`` shape."""

    path: str
    message: str


class HarelError(Exception):
    """Base class for all harel errors."""


class ValidationError(HarelError):
    """A machine definition failed schema or semantic validation.

    ``errors`` is the structured list (one record per problem). When raised
    without records it still signals failure (e.g. a malformed document).
    """

    def __init__(
        self,
        errors: list[ErrorRecord] | None = None,
        message: str | None = None,
    ) -> None:
        self.errors: list[ErrorRecord] = list(errors) if errors else []
        if message is None:
            message = (
                "; ".join(f"{e['path']}: {e['message']}" for e in self.errors)
                or "validation failed"
            )
        super().__init__(message)


class SchemaError(HarelError):
    """The bundled JSON Schema itself is unusable (should not happen)."""
