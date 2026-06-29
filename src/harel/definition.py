"""Loading machine definitions from YAML text (SPEC §4).

A machine file is one or more ``---``-separated documents; the first is the
root definition (SPEC §9). Each document is validated (structure + reserved
names) before a :class:`Definition` is produced. Later build steps resolve the
raw document into a navigable state model; step 1 keeps the validated raw
mapping as the single source of structure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from . import yaml12
from .errors import ErrorRecord, ValidationError
from .validator import validate


@dataclass(frozen=True)
class Definition:
    """A validated machine definition (one YAML document)."""

    id: str
    version: int
    format: int
    raw: dict[str, Any]

    @property
    def top(self) -> dict[str, Any]:
        """The outermost state node (SPEC §4.5)."""
        return cast(dict[str, Any], self.raw["top"])


def load_definitions(text: str) -> list[Definition]:
    """Parse and validate every document in a (possibly multi-doc) machine file."""
    docs = yaml12.load_all(text)
    if not docs:
        raise ValidationError([ErrorRecord(path="(root)", message="no document")])
    defs: list[Definition] = []
    for doc in docs:
        if not isinstance(doc, dict):
            raise ValidationError(
                [ErrorRecord(path="(root)", message="a machine definition must be a mapping")]
            )
        validate(doc)
        defs.append(
            Definition(
                id=doc["id"],
                version=doc.get("version", 1),
                format=doc.get("format", 1),
                raw=doc,
            )
        )
    return defs


def load_definition(text: str) -> Definition:
    """Load a single-definition machine file (error if more than one document)."""
    defs = load_definitions(text)
    if len(defs) != 1:
        raise ValidationError(
            [
                ErrorRecord(
                    path="(root)",
                    message=f"expected one definition, got {len(defs)}",
                )
            ]
        )
    return defs[0]
