"""Validation tests: JSON Schema + reserved-name enforcement (SPEC §2)."""

from __future__ import annotations

import pytest

from harel import ValidationError, collect_errors, load_definition, validate, yaml12
from harel.validator import ALL_RESERVED, RESERVED_EVENTS, RESERVED_NAMES

VALID = """
id: m
events:
  go: {}
top:
  esvs:
    n: { type: int, init: 0 }
  initial: { transition_to: s }
  states:
    s:
      on_events:
        go: { transition_to: t }
    t: {}
"""

# A skeleton with placeholders so reserved-name cases can inject a name.
SKELETON_STATES = """
id: m
top:
  initial: {{ transition_to: s }}
  states:
    {name}: {{}}
    s: {{}}
"""

SKELETON_ESVS = """
id: m
top:
  esvs:
    {name}: {{ type: int }}
  initial: {{ transition_to: s }}
  states:
    s: {{}}
"""


def _paths(doc: str) -> set[str]:
    return {e["path"] for e in collect_errors(yaml12.load(doc))}


def test_valid_machine_has_no_errors() -> None:
    validate(yaml12.load(VALID))


def test_missing_required_top_level() -> None:
    assert _paths("id: m\n")


def test_composite_requires_initial_and_states() -> None:
    doc = yaml12.load(
        "id: m\ntop:\n  initial: { transition_to: s }\n  states:\n    s: { type: composite }\n"
    )
    assert collect_errors(doc)  # composite `s` lacks initial+states


@pytest.mark.parametrize("reserved", sorted(RESERVED_NAMES))
def test_reserved_state_name_rejected(reserved: str) -> None:
    errs = collect_errors(yaml12.load(SKELETON_STATES.format(name=reserved)))
    assert f"/top/states/{reserved}" in {e["path"] for e in errs}


@pytest.mark.parametrize("reserved", sorted(RESERVED_EVENTS))
def test_reserved_event_name_allowed_as_state(reserved: str) -> None:
    # Reserved event names occupy a different namespace and may be state names
    # (e.g. a state named `done` — conformance cases 16/21 rely on this).
    validate(yaml12.load(SKELETON_STATES.format(name=reserved)))


@pytest.mark.parametrize("reserved", sorted(RESERVED_NAMES))
def test_reserved_esv_name_rejected(reserved: str) -> None:
    errs = collect_errors(yaml12.load(SKELETON_ESVS.format(name=reserved)))
    assert f"/top/esvs/{reserved}" in {e["path"] for e in errs}


@pytest.mark.parametrize("reserved", sorted(ALL_RESERVED))
def test_reserved_declared_event_rejected(reserved: str) -> None:
    doc = yaml12.load(
        f"id: m\nevents:\n  {reserved}: {{}}\ntop:\n"
        "  initial: { transition_to: s }\n  states:\n    s: {}\n"
    )
    errs = {e["path"] for e in collect_errors(doc)}
    assert f"/events/{reserved}" in errs


def test_reserved_event_names_allowed_as_handlers() -> None:
    # env/error/done may appear as on_events handlers (SPEC §5.4/§5.6/§5.10).
    doc = yaml12.load(
        "id: m\ntop:\n  on_events:\n    error: { transition_to: f }\n"
        "  initial: { transition_to: s }\n  states:\n    s: {}\n    f: {}\n"
    )
    validate(doc)


def test_load_definition_validates() -> None:
    load_definition(VALID)


def test_load_definition_rejects_reserved() -> None:
    with pytest.raises(ValidationError):
        load_definition(
            "id: m\ntop:\n  esvs:\n    id: { type: int }\n"
            "  initial: { transition_to: s }\n  states:\n    s: {}\n"
        )


def test_error_records_have_path_and_message() -> None:
    with pytest.raises(ValidationError) as exc_info:
        load_definition("id: m\ntop:\n  esvs:\n    event: { type: int }\n")
    rec = exc_info.value.errors
    assert rec
    assert all(set(r.keys()) == {"path", "message"} for r in rec)
