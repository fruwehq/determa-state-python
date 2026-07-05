"""Unit tests for the resolved model: LCA computation and target resolution."""

from __future__ import annotations

import pytest

from determa.state import load_definition
from determa.state.model import Machine

# top -> c (composite, initial a) -> a, b
NESTED = """
id: m
events: { go: {} }
top:
  initial: { transition_to: c }
  states:
    c:
      type: composite
      initial: { transition_to: a }
      states:
        a: { on_events: { go: { transition_to: b } } }
        b: { on_events: { go: { transition_to: d } } }
    d: {}
"""


@pytest.fixture()
def machine() -> Machine:
    return Machine(load_definition(NESTED))


def test_lca_siblings(machine: Machine) -> None:
    a = machine.by_path["top.c.a"]
    b = machine.by_path["top.c.b"]
    # external self-ish: LCA(a, b) is their common composite parent c
    assert machine.lca(a, b) is machine.by_path["top.c"]


def test_lca_target_is_ancestor_self_transition(machine: Machine) -> None:
    c = machine.by_path["top.c"]
    # a transition owned by c targeting c: LCA(c, c) excludes c -> top
    assert machine.lca(c, c) is machine.top


def test_lca_never_the_state_itself(machine: Machine) -> None:
    a = machine.by_path["top.c.a"]
    # a self-transition on leaf a: LCA is its container c (a is exited/re-entered)
    assert machine.lca(a, a) is machine.by_path["top.c"]


def test_resolve_single_component_finds_child(machine: Machine) -> None:
    c = machine.by_path["top.c"]
    assert machine.resolve_target(c, "a") is machine.by_path["top.c.a"]


def test_resolve_dotted_from_outer(machine: Machine) -> None:
    # from a state inside c, a dotted ref `c.a` resolves via top.c
    a = machine.by_path["top.c.a"]
    assert machine.resolve_target(a, "c.a") is a


def test_resolve_searches_upward_to_sibling(machine: Machine) -> None:
    # `d` is a sibling of c (child of top): resolvable from inside c
    a = machine.by_path["top.c.a"]
    assert machine.resolve_target(a, "d") is machine.by_path["top.d"]


def test_unresolved_reference_raises_at_build() -> None:
    from determa.state import ValidationError

    with pytest.raises(ValidationError):
        Machine(
            load_definition(
                "id: m\ntop:\n  initial: { transition_to: s }\n"
                "  states:\n    s: { on_events: { go: { transition_to: nowhere } } }\n"
            )
        )


def test_meta_is_exposed_on_machine_and_states() -> None:
    machine = Machine(
        load_definition(
            """\
id: m
meta:
  owner: ui
top:
  meta:
    role: root
  initial: { transition_to: s }
  states:
    s:
      meta:
        tools: [search, respond]
"""
        )
    )

    assert machine.meta == {"owner": "ui"}
    assert machine.top.meta == {"role": "root"}
    assert machine.by_path["top.s"].meta == {"tools": ["search", "respond"]}
