"""Choice pseudostate — dynamic branching, chaining, and validation (SPEC §5.5.1)."""

from __future__ import annotations

import pytest

from determa.state import Host, collect_errors, load_definitions
from determa.state.errors import ValidationError
from determa.state.model import Machine

ATM = """\
id: atm
events:
  withdraw: { payload: { amount: { type: int, required: true } } }
  reset: {}
top:
  esvs:
    balance: { type: int, init: 100 }
    requested: { type: int, init: 0 }
  initial: { transition_to: idle }
  states:
    idle:
      on_events:
        withdraw:
          action: [ { assign: { requested: "event.payload.amount" } } ]
          transition_to: check
    check:
      choice:
        - { guard: "requested <= balance", transition_to: dispensing,
            action: [ { assign: { balance: "balance - requested" } } ] }
        - { transition_to: insufficient }
    dispensing:
      on_events: { reset: { transition_to: idle } }
    insufficient:
      on_events: { reset: { transition_to: idle } }
"""


def _atm() -> tuple[Host, object]:
    host = Host()
    host.register_all(load_definitions(ATM))
    inst = host.create_root(host.machines["atm"], "r")
    host.run_to_quiescence()
    return host, inst


def test_choice_branches_on_freshly_assigned_esv() -> None:
    host, inst = _atm()
    host.deliver("r", "withdraw", {"amount": 40})   # requested:=40; 40<=100 -> dispensing
    host.run_to_quiescence()
    assert inst.active_leaf_names() == ["dispensing"]
    assert inst.resolved_esvs()["balance"] == 60
    assert inst.resolved_esvs()["requested"] == 40


def test_choice_else_branch() -> None:
    host, inst = _atm()
    host.deliver("r", "withdraw", {"amount": 500})  # 500<=100 false -> else -> insufficient
    host.run_to_quiescence()
    assert inst.active_leaf_names() == ["insufficient"]
    assert inst.resolved_esvs()["balance"] == 100   # unchanged


CHAIN = """\
id: chain
events:
  go: { payload: { n: { type: int, required: true } } }
top:
  esvs: { n: { type: int, init: 0 } }
  initial: { transition_to: start }
  states:
    start:
      on_events:
        go: { action: [ { assign: { n: "event.payload.n" } } ], transition_to: c1 }
    c1:
      choice:
        - { guard: "n < 0", transition_to: negative }
        - { transition_to: c2 }
    c2:
      choice:
        - { guard: "n == 0", transition_to: zero }
        - { transition_to: positive }
    negative: {}
    zero: {}
    positive: {}
"""


@pytest.mark.parametrize("n,expected", [(-3, "negative"), (0, "zero"), (7, "positive")])
def test_chained_choices(n: int, expected: str) -> None:
    host = Host()
    host.register_all(load_definitions(CHAIN))
    inst = host.create_root(host.machines["chain"], "r")
    host.run_to_quiescence()
    host.deliver("r", "go", {"n": n})
    host.run_to_quiescence()
    assert inst.active_leaf_names() == [expected]


# --- validation -------------------------------------------------------------
def _machine(pick_branches: str) -> str:
    return f"""\
id: m
events: {{ go: {{}} }}
top:
  esvs: {{ x: {{ type: int, init: 1 }} }}
  initial: {{ transition_to: a }}
  states:
    a: {{ on_events: {{ go: {{ transition_to: pick }} }} }}
    pick: {{ choice: {pick_branches} }}
    b: {{}}
    c: {{}}
"""


def test_no_else_is_rejected() -> None:
    import yaml
    src = _machine('[ { guard: "x > 0", transition_to: b }, { guard: "x < 0", transition_to: c } ]')
    errs = collect_errors(yaml.safe_load(src))
    assert any("default" in e["message"] for e in errs)
    with pytest.raises(ValidationError):
        load_definitions(src)


def test_else_must_be_last() -> None:
    import yaml
    src = _machine('[ { transition_to: b }, { guard: "x > 0", transition_to: c } ]')
    errs = collect_errors(yaml.safe_load(src))
    assert any("last" in e["message"] for e in errs)


def test_cyclic_choice_rejected() -> None:
    src = """\
id: cyc
events: { go: {} }
top:
  initial: { transition_to: a }
  states:
    a: { on_events: { go: { transition_to: c1 } } }
    c1: { choice: [ { transition_to: c2 } ] }
    c2: { choice: [ { transition_to: c1 } ] }
"""
    with pytest.raises(ValidationError):
        Machine(load_definitions(src)[0])


def test_unresolved_branch_target_rejected() -> None:
    src = _machine('[ { guard: "x > 0", transition_to: nowhere }, { transition_to: b } ]')
    with pytest.raises(ValidationError):
        Machine(load_definitions(src)[0])
