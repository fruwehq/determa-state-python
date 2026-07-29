"""Structured errors for format-1 loading and foreground execution."""

from __future__ import annotations

from dataclasses import dataclass


class DetermaError(Exception):
    """Base class for Determa State errors."""


@dataclass(frozen=True)
class ErrorRecord:
    """One load-time diagnostic."""

    code: str
    path: str = ""
    message: str = ""


class ValidationError(DetermaError):
    """A source, schema, or semantic validation failure."""

    def __init__(self, code: str, path: str = "", message: str = "") -> None:
        self.code = code
        self.path = path
        self.message = message or code
        self.errors = [ErrorRecord(code=code, path=path, message=self.message)]
        super().__init__(self.message)


class SchemaError(DetermaError):
    """The bundled normative schema is unusable."""


class CelError(DetermaError):
    """A portable CEL expression failed to compile or evaluate."""


class ArtifactError(DetermaError):
    """A portable persistence artifact is invalid or unsupported."""

    def __init__(self, code: str, path: str = "", message: str = "") -> None:
        self.code = code
        self.path = path
        self.message = message or code
        super().__init__(self.message)


class StepFault(DetermaError):
    """Internal control flow for one atomic RTC fault."""

    def __init__(self, code: str, source_locator: str) -> None:
        self.code = code
        self.source_locator = source_locator
        super().__init__(f"{code} at {source_locator}")
