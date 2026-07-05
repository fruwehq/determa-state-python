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
    """Return all validation errors (structural + reserved names + static analysis)."""
    errors: list[ErrorRecord] = list(_structural_errors(doc))
    errors.extend(_reserved_name_errors(doc))
    if isinstance(doc, dict) and isinstance(doc.get("top"), dict):
        errors.extend(_reachability_errors(doc["top"]))
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


def _check_transition_list(path: str, branches: list[Any], errors: list[ErrorRecord]) -> None:
    """In a guarded transition list, an unguarded branch shadows all later ones, so it
    MUST be last — otherwise the later branches are dead (SPEC §2)."""
    for br in branches[:-1]:
        if isinstance(br, dict) and "guard" not in br:
            errors.append(
                ErrorRecord(
                    path=path,
                    message="an unguarded transition must be last; later branches are dead",
                )
            )
            return


def _reachability_errors(top: dict[str, Any]) -> list[ErrorRecord]:
    """Flag declared states unreachable from ``top`` (SPEC §2). Conservative and
    guard-agnostic: reachability follows every ``initial``/region-initial/``on_events``/
    ``after``/``choice`` target regardless of guards, and entering a state implies its
    ancestors (whose own edges are then also followed)."""
    raws: dict[str, dict[str, Any]] = {}
    parent: dict[str, str | None] = {}
    children: dict[str, dict[str, str]] = {}

    def _build(path: str, node: dict[str, Any], par: str | None) -> None:
        raws[path] = node
        parent[path] = par
        children[path] = {}
        for cn, cd in (node.get("states") or {}).items():
            if isinstance(cd, dict):
                children[path][cn] = f"{path}.{cn}"
                _build(f"{path}.{cn}", cd, path)
        for region in node.get("regions") or []:
            for cn, cd in (region.get("states") or {}).items() if isinstance(region, dict) else []:
                if isinstance(cd, dict):
                    children[path][cn] = f"{path}.{cn}"
                    _build(f"{path}.{cn}", cd, path)

    _build("top", top, None)

    def _resolve(src: str, ref: object) -> str | None:
        if not isinstance(ref, str):
            return None
        parts = ref.split(".")
        cur: str | None = src
        anchor: str | None = None
        while cur is not None:
            if parts[0] in children.get(cur, {}):
                anchor = children[cur][parts[0]]
                break
            cur = parent.get(cur)
        if anchor is None:
            return None
        node = anchor
        for p in parts[1:]:
            node = children.get(node, {}).get(p, "")
            if not node:
                return None
        return node

    def _targets(node: dict[str, Any]) -> list[object]:
        out: list[object] = []
        initials = [node.get("initial")]
        initials += [r.get("initial") for r in node.get("regions") or [] if isinstance(r, dict)]
        for t in initials:
            if isinstance(t, dict):
                out.append(t.get("transition_to"))
        for spec in (node.get("on_events") or {}).values():
            for tr in spec if isinstance(spec, list) else [spec]:
                if isinstance(tr, dict):
                    out.append(tr.get("transition_to"))
        for group in (node.get("after") or [], node.get("choice") or []):
            for tr in group:
                if isinstance(tr, dict):
                    out.append(tr.get("transition_to"))
        return out

    reachable: set[str] = set()
    stack = ["top"]
    while stack:
        path = stack.pop()
        if path in reachable:
            continue
        reachable.add(path)
        par = parent.get(path)
        if par is not None and par not in reachable:
            stack.append(par)  # entering a state implies its ancestors; follow their edges too
        for ref in _targets(raws[path]):
            tgt = _resolve(path, ref)
            if tgt is not None and tgt not in reachable:
                stack.append(tgt)

    errors: list[ErrorRecord] = []
    for path in raws:
        if path != "top" and path not in reachable:
            errors.append(
                ErrorRecord(
                    path="/" + path.replace(".", "/"),
                    message=f"unreachable state '{path.rsplit('.', 1)[-1]}'",
                )
            )
    return sorted(errors, key=lambda e: e["path"])


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
    on_events = state.get("on_events")
    if isinstance(on_events, dict):
        for ev, spec in on_events.items():
            if isinstance(spec, list):
                _check_transition_list(f"{path}/on_events/{ev}", spec, errors)
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
