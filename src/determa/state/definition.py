"""Loading machine definitions from YAML text or native mappings (SPEC §2, §4).

A machine file is one or more ``---``-separated documents; the first is the
root definition (SPEC §9). Each document is validated (structure + reserved
names) before a :class:`Definition` is produced. Later build steps resolve the
raw document into a navigable state model; step 1 keeps the validated raw
mapping as the single source of structure.

Hosts that build machines in code can pass a native mapping (``dict``) — or a
sequence of them for a multi-document machine — instead of serializing to a
YAML string. The same ``validate()`` path runs either way, so a hand-built
machine is held to the same contract as a file-loaded one.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from . import yaml12
from .errors import ErrorRecord, ValidationError
from .validator import validate

#: A machine definition as YAML text, a single mapping, or a sequence of mappings
#: (one per ``---`` document; the first is the root, SPEC §9).
DefinitionSource = str | Mapping[str, Any] | Sequence[Mapping[str, Any]]


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


def _doc_to_definition(doc: Any, index: int) -> Definition:
    """Validate one document (mapping) and wrap it as a :class:`Definition`."""
    if not isinstance(doc, Mapping):
        raise ValidationError(
            [ErrorRecord(path=f"doc[{index}]", message="a machine definition must be a mapping")]
        )
    raw = dict(doc)  # normalize any Mapping to a plain dict (and defensive copy)
    validate(raw)
    return Definition(
        id=raw["id"],
        version=raw.get("version", 1),
        format=raw.get("format", 1),
        raw=raw,
    )


def load_definitions(source: DefinitionSource) -> list[Definition]:
    """Parse and validate every document in a machine file or native mapping(s).

    ``source`` is YAML text (``str``), a single native mapping (``dict``), or a
    sequence of mappings (multi-document). Each document runs through the same
    :func:`validate` path, so building a machine in code is held to the same
    contract as loading one from a YAML file.
    """
    if isinstance(source, str):
        docs: list[Any] = list(yaml12.load_all(source))
    elif isinstance(source, Mapping):
        docs = [source]
    else:
        docs = list(source)
    if not docs:
        raise ValidationError([ErrorRecord(path="(root)", message="no document")])
    return [_doc_to_definition(doc, i) for i, doc in enumerate(docs)]


def load_definition(source: DefinitionSource) -> Definition:
    """Load a single-definition machine (from text or a native mapping).

    Errors if the source carries more than one document.
    """
    defs = load_definitions(source)
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
