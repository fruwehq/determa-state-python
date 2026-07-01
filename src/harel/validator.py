"""Machine-definition validation (SPEC §2).

Two layers:

1. **Structural** — the document MUST validate against the normative
   ``schema/machine.schema.json`` (bundled as package data).
2. **Reserved names** (SPEC §2/§3):
   - The structural / CEL-intrinsic names ``top``, ``id``, ``parent``, ``event``
     are forbidden as state and esv identifiers (they collide with the root
     state or the guard/action intrinsics).
   - The reserved event names ``initial``, ``entry``, ``exit``, ``env``,
     ``error``, ``done`` are forbidden only as *declared* event types (they are
     implicitly provided by the engine). They MAY be used as ``on_events``
     handlers, and (occupying a different namespace) as state/esv names — e.g.
     a state named ``done`` is allowed.


Errors are returned as ``{path, message}`` records (the §13.4 ``validate``
JSON shape). Later build steps add reference-resolution and contract checks.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

import jsonschema

from .errors import ErrorRecord, ValidationError

RESERVED_NAMES = frozenset({"top", "id", "parent", "event"})
RESERVED_EVENTS = frozenset({"initial", "entry", "exit", "env", "error", "done"})
ALL_RESERVED = RESERVED_NAMES | RESERVED_EVENTS

_SCHEMA_PATH = Path(__file__).parent / "data" / "machine.schema.json"


@lru_cache(maxsize=1)
def schema() -> dict[str, Any]:
    """The bundled normative machine JSON Schema (SPEC §4)."""
    with _SCHEMA_PATH.open(encoding="utf-8") as fh:
        return cast(dict[str, Any], json.load(fh))


def _json_path(parts: list[Any]) -> str:
    if not parts:
        return "(root)"
    return "/" + "/".join(str(p) for p in parts)


def validate(doc: dict[str, Any]) -> None:
    """Validate a machine document; raise :class:`ValidationError` on failure."""
    errors = collect_errors(doc)
    if errors:
        raise ValidationError(errors)


def collect_errors(doc: dict[str, Any]) -> list[ErrorRecord]:
    """Return all validation errors (structural + reserved names)."""
    errors: list[ErrorRecord] = list(_structural_errors(doc))
    errors.extend(_reserved_name_errors(doc))
    return errors


def _structural_errors(doc: dict[str, Any]) -> list[ErrorRecord]:
    validator = jsonschema.Draft202012Validator(schema())
    out: list[ErrorRecord] = []
    for err in sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path)):
        out.append(
            ErrorRecord(path=_json_path(list(err.absolute_path)), message=err.message)
        )
    return out


def _reserved_name_errors(doc: dict[str, Any]) -> list[ErrorRecord]:
    if not isinstance(doc, dict):
        return []
    errors: list[ErrorRecord] = []
    top = doc.get("top")
    if isinstance(top, dict):
        _walk_state("/top", top, errors)
    events = doc.get("events")
    if isinstance(events, dict):
        for name in events:
            if name in ALL_RESERVED:
                errors.append(
                    ErrorRecord(
                        path=f"/events/{name}",
                        message=f"'{name}' is a reserved event name",
                    )
                )
    return errors


def _check_choice(path: str, branches: list[Any], errors: list[ErrorRecord]) -> None:
    """A choice MUST have exactly one default (unguarded) branch, and it MUST be last
    (SPEC §5.5.1)."""
    defaults = [i for i, br in enumerate(branches) if isinstance(br, dict) and "guard" not in br]
    if not defaults:
        errors.append(
            ErrorRecord(path=f"{path}/choice", message="choice has no default (else) branch")
        )
    elif len(defaults) > 1:
        errors.append(
            ErrorRecord(path=f"{path}/choice", message="choice has more than one default branch")
        )
    elif defaults[0] != len(branches) - 1:
        errors.append(
            ErrorRecord(path=f"{path}/choice", message="the default (else) branch must be last")
        )


def _forbid(
    name: object, reserved: frozenset[str], path: str, errors: list[ErrorRecord]
) -> None:
    if name in reserved:
        errors.append(
            ErrorRecord(path=path, message=f"'{name}' is a reserved name")
        )


def _walk_state(path: str, state: Any, errors: list[ErrorRecord]) -> None:
    """Recurse a StateNode, flagging reserved state/esv names.

    State and esv names are checked against the structural/intrinsic reserved
    set only; reserved event names live in a different namespace and may be
    reused (e.g. a state named ``done``).
    """
    if not isinstance(state, dict):
        return
    choice = state.get("choice")
    if isinstance(choice, list):
        _check_choice(path, choice, errors)
    esvs = state.get("esvs")
    if isinstance(esvs, dict):
        for name in esvs:
            _forbid(name, RESERVED_NAMES, f"{path}/esvs/{name}", errors)
    states = state.get("states")
    if isinstance(states, dict):
        for name, child in states.items():
            _forbid(name, RESERVED_NAMES, f"{path}/states/{name}", errors)
            _walk_state(f"{path}/states/{name}", child, errors)
    regions = state.get("regions")
    if isinstance(regions, list):
        for i, region in enumerate(regions):
            if isinstance(region, dict):
                rstates = region.get("states")
                if isinstance(rstates, dict):
                    for name, child in rstates.items():
                        _forbid(name, RESERVED_NAMES, f"{path}/regions/{i}/states/{name}", errors)
                        _walk_state(
                            f"{path}/regions/{i}/states/{name}", child, errors
                        )
