"""Submachine states — synchronous reuse, seeding, completion, isolation (SPEC §5.6.1)."""

from __future__ import annotations

import pytest

from determa.state import Host, load_definitions
from determa.state.errors import ValidationError

ORDER = """\
id: order
events:
  pay: { payload: { amount: { type: int, required: true } } }
  cancel: {}
top:
  esvs: { total: { type: int, init: 100 } }
  initial: { transition_to: checkout }
  states:
    checkout:
      submachine: payment
      with: { due: "total" }
      on_events:
        cancel: { transition_to: cancelled }
        done:   { transition_to: paid }
    paid: {}
    cancelled: {}
---
id: payment
events:
  pay: { payload: { amount: { type: int, required: true } } }
top:
  esvs:
    due:  { type: int, external: true }
    paid: { type: int, init: 0 }
  initial: { transition_to: awaiting }
  states:
    awaiting:
      on_events:
        pay:
          guard: "event.payload.amount >= due"
          action: [ { assign: { paid: "event.payload.amount" } } ]
          transition_to: settled
    settled: { type: final }
"""


def _order() -> tuple[Host, object]:
    host = Host()
    host.register_all(load_definitions(ORDER))
    inst = host.create_root(host.machines["order"], "o")
    host.run_to_quiescence()
    return host, inst


def test_submachine_entered_synchronously() -> None:
    _, inst = _order()
    assert inst.active_leaf_names() == ["awaiting"]  # inlined submachine's initial state


def test_submachine_completes_to_parent_via_done() -> None:
    host, inst = _order()
    host.deliver("o", "pay", {"amount": 100})   # settles -> final -> done -> paid
    host.run_to_quiescence()
    assert inst.active_leaf_names() == ["paid"]


def test_with_seeding_reaches_the_submachine() -> None:
    # due is seeded from the parent's total (100). A pay below it fails the guard and
    # stays in the submachine — which only holds if `due` was seeded to 100 (not 0/null).
    host, inst = _order()
    host.deliver("o", "pay", {"amount": 50})
    host.run_to_quiescence()
    assert inst.active_leaf_names() == ["awaiting"]


def test_parent_interrupts_submachine() -> None:
    host, inst = _order()
    host.deliver("o", "cancel")   # unhandled by the submachine -> bubbles to parent
    host.run_to_quiescence()
    assert inst.active_leaf_names() == ["cancelled"]


def test_esv_isolation_parent_vars_not_visible_inside() -> None:
    # Inside the submachine, resolving esvs sees the submachine's own vars, not `total`.
    _, inst = _order()
    esvs = inst.resolved_esvs()
    assert "due" in esvs and "paid" in esvs
    assert "total" not in esvs


def test_unknown_submachine_rejected() -> None:
    src = "id: m\ntop:\n  initial: { transition_to: s }\n  states:\n    s: { submachine: nope }\n"
    with pytest.raises(ValidationError):
        Host().register_all(load_definitions(src))


def test_cyclic_submachine_rejected() -> None:
    src = (
        "id: a\ntop:\n  initial: { transition_to: s }\n  states:\n    s: { submachine: b }\n"
        "---\n"
        "id: b\ntop:\n  initial: { transition_to: s }\n  states:\n    s: { submachine: a }\n"
    )
    with pytest.raises(ValidationError):
        Host().register_all(load_definitions(src))
