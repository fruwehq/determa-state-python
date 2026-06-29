"""Mermaid export tests (SPEC §12)."""

from __future__ import annotations

from harel import load_definition
from harel.export import export
from harel.model import Machine

TURNSTILE = """
id: turnstile
events:
  coin: { payload: { amount: { type: int, required: true } } }
  push: {}
top:
  esvs:
    fare: { type: int, init: 50 }
  initial: { transition_to: locked }
  states:
    locked:
      on_events:
        coin: { transition_to: unlocked, guard: "amount >= fare" }
    unlocked:
      on_events:
        push: { transition_to: locked }
"""


def test_static_structure() -> None:
    machine = Machine(load_definition(TURNSTILE))
    out = export(machine)
    assert out.startswith("stateDiagram-v2")
    assert "[*] --> locked" in out
    assert "locked --> unlocked : coin [amount >= fare]" in out
    assert "unlocked --> locked : push" in out


def test_current_state_highlight() -> None:
    machine = Machine(load_definition(TURNSTILE))
    # active leaf `unlocked`; its ancestor is `top`.
    leaf = machine.by_path["top.unlocked"]
    config = [leaf.path]
    out = export(machine, state_config=config)
    assert "classDef active fill:#9f9,stroke:#3a3" in out
    assert "class unlocked active" in out


def test_unsupported_format_raises() -> None:
    import pytest

    machine = Machine(load_definition(TURNSTILE))
    with pytest.raises(ValueError):
        export(machine, format="plantuml")
