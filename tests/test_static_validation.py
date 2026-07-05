"""Static validation: unreachable states & dead transition branches (SPEC §2)."""

from __future__ import annotations

import pytest
import yaml

from determa.state import collect_errors, load_definitions
from determa.state.errors import ValidationError


def _errors(src: str) -> list[str]:
    return [e["message"] for e in collect_errors(yaml.safe_load(src))]


# --- unreachable states -----------------------------------------------------
def test_unreachable_state_flagged() -> None:
    src = """\
id: m
events: { go: {} }
top:
  initial: { transition_to: a }
  states:
    a: { on_events: { go: { transition_to: b } } }
    b: {}
    orphan: {}
"""
    msgs = _errors(src)
    assert any("unreachable state 'orphan'" in m for m in msgs)
    with pytest.raises(ValidationError):
        load_definitions(src)


def test_state_reached_only_via_composite_initial_is_ok() -> None:
    src = """\
id: m
events: { go: {} }
top:
  initial: { transition_to: outer }
  states:
    outer:
      initial: { transition_to: inner }
      states:
        inner: { on_events: { go: { transition_to: fin } } }
        fin: { type: final }
"""
    assert _errors(src) == []


def test_deeply_targeted_state_marks_ancestors_reachable() -> None:
    # Transitioning straight to outer.inner must not flag outer as unreachable.
    src = """\
id: m
events: { go: {} }
top:
  initial: { transition_to: start }
  states:
    start: { on_events: { go: { transition_to: outer.inner } } }
    outer:
      initial: { transition_to: inner }
      states:
        inner: {}
"""
    assert _errors(src) == []


def test_orthogonal_region_reachability() -> None:
    src = """\
id: m
events: { go: {} }
top:
  initial: { transition_to: par }
  states:
    par:
      type: orthogonal
      regions:
        - initial: { transition_to: r1a }
          states:
            r1a: { on_events: { go: { transition_to: r1b } } }
            r1b: {}
        - initial: { transition_to: r2a }
          states: { r2a: {}, r2orphan: {} }
"""
    # r1a/r1b/r2a reachable via region initials + a transition; r2orphan is not.
    msgs = _errors(src)
    assert any("unreachable state 'r2orphan'" in m for m in msgs)
    assert not any("'r1a'" in m or "'r1b'" in m or "'r2a'" in m for m in msgs)


# --- dead branches ----------------------------------------------------------
def test_unguarded_branch_not_last_is_dead() -> None:
    src = """\
id: m
events: { check: { payload: { n: { type: int, required: true } } } }
top:
  initial: { transition_to: s }
  states:
    s:
      on_events:
        check:
          - { transition_to: a }
          - { guard: "event.payload.n > 0", transition_to: b }
    a: {}
    b: {}
"""
    assert any("must be last" in m for m in _errors(src))
    with pytest.raises(ValidationError):
        load_definitions(src)


def test_guarded_list_with_unguarded_last_is_ok() -> None:
    src = """\
id: m
events: { check: { payload: { n: { type: int, required: true } } } }
top:
  initial: { transition_to: s }
  states:
    s:
      on_events:
        check:
          - { guard: "event.payload.n > 0", transition_to: a }
          - { transition_to: b }
    a: {}
    b: {}
"""
    assert _errors(src) == []
