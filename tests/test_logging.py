"""Standard-library diagnostic logging under the ``harel`` logger."""

from __future__ import annotations

import logging

import harel
from harel import Host

TURNSTILE = """\
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
        coin: { transition_to: unlocked, guard: "event.payload.amount >= fare" }
    unlocked:
      on_events:
        push: { transition_to: locked }
"""

FAULTING = """\
id: boom
events:
  go: {}
top:
  esvs:
    x: { type: int, init: 1 }
  initial: { transition_to: a }
  states:
    a:
      on_events:
        go: { transition_to: b, action: [ { assign: { x: "1 / 0" } } ] }
    b: {}
"""


def _run(machine: str, event: str, payload=None):
    host = Host()
    host.register_all(harel.load_definitions(machine))
    root = harel.load_definitions(machine)[0].id
    host.create_root(host.machines[root], "r")
    host.run_to_quiescence()
    host.deliver("r", event, payload)
    host.run_to_quiescence()
    return host


def test_silent_by_default(caplog):
    """No handler is configured by the app → nothing propagates as output, but the
    records still exist at their levels (caplog captures them)."""
    # The library attaches only a NullHandler; verify it's present.
    assert any(isinstance(h, logging.NullHandler) for h in logging.getLogger("harel").handlers)


def test_dispatch_and_transition_logged_at_debug(caplog):
    with caplog.at_level(logging.DEBUG, logger="harel"):
        _run(TURNSTILE, "coin", {"amount": 100})
    msgs = [r.getMessage() for r in caplog.records]
    assert any("dispatch instance=r event=coin" in m for m in msgs)
    assert any("transition=unlocked" in m for m in msgs)


def test_fault_logged_at_warning(caplog):
    with caplog.at_level(logging.DEBUG, logger="harel"):
        _run(FAULTING, "go")
    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("dead-letter" in r.getMessage() for r in warnings)
    assert any("faulted" in r.getMessage() for r in warnings)


def test_no_debug_records_when_level_is_warning(caplog):
    with caplog.at_level(logging.WARNING, logger="harel"):
        _run(TURNSTILE, "coin", {"amount": 100})
    assert [r for r in caplog.records if r.levelno == logging.DEBUG] == []
