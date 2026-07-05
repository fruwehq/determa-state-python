"""Runtime type checks: esv value types and event-payload validation (SPEC §4.3).

``matches`` checks a Python value against a Determa State value type. ``payload_errors``
validates a delivered payload against an event declaration (required fields
present and typed, no extras) — used at delivery time (§4.3: invalid payloads
are rejected, not enqueued).
"""

from __future__ import annotations

from typing import Any


def matches(value: Any, type_name: str) -> bool:
    if type_name == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_name == "float":
        return isinstance(value, int | float) and not isinstance(value, bool)
    if type_name == "bool":
        return isinstance(value, bool)
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "list":
        return isinstance(value, list)
    if type_name == "map":
        return isinstance(value, dict)
    return False


def payload_errors(decl: dict[str, Any], payload: dict[str, Any] | None) -> list[str]:
    """Return a list of payload-validation problems (empty == valid)."""
    errors: list[str] = []
    fields = decl.get("payload") or {}
    payload = payload or {}
    for fname, fdef in fields.items():
        if fname in payload:
            if not matches(payload[fname], fdef["type"]):
                errors.append(f"field '{fname}' must be {fdef['type']}")
        elif fdef.get("required"):
            errors.append(f"missing required field '{fname}'")
    for fname in payload:
        if fname not in fields:
            errors.append(f"unexpected field '{fname}'")
    return errors
