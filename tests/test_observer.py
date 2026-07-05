"""Observer adapter (SPEC §8): passive per-step callback."""

from __future__ import annotations

import io
import json

import determa.state as ds
from determa.state import CollectingObserver, Host, JsonlObserver

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


def _host(observer=None) -> Host:
    host = Host(observer=observer)
    host.register_all(ds.load_definitions(TURNSTILE))
    host.create_root(host.machines["turnstile"], "t1")
    host.run_to_quiescence()
    return host


def test_observer_fires_on_auto_processing() -> None:
    obs = CollectingObserver()
    host = _host(obs)
    obs.records.clear()  # ignore initial-transition churn; focus on the send
    host.deliver("t1", "coin", {"amount": 100})
    host.run_to_quiescence()
    assert len(obs.records) == 1
    rec = obs.records[0]
    assert rec["instance"] == "t1"
    assert rec["event"] == "coin"
    assert rec["entered"] == ["unlocked"]
    assert rec["exited"] == ["locked"]
    assert rec["faulted"] is False
    assert set(rec) == {
        "instance", "event", "transition", "entered", "exited",
        "published", "spawned", "faulted",
    }


def test_observer_fires_on_manual_step() -> None:
    obs = CollectingObserver()
    host = _host(obs)
    obs.records.clear()
    host.inject("t1", "coin", {"amount": 100})  # enqueue, do not process
    assert obs.records == []                     # nothing fired yet
    host.step("t1")                              # one manual RTC step
    assert len(obs.records) == 1
    assert obs.records[0]["entered"] == ["unlocked"]


def test_observer_none_is_noop() -> None:
    host = _host(None)  # must not raise
    host.deliver("t1", "coin", {"amount": 100})
    host.run_to_quiescence()
    assert host.instances["t1"].active_leaf_names() == ["unlocked"]


def test_observer_is_passive() -> None:
    """An observer that ignores its record must not change engine behavior."""
    a = _host(CollectingObserver())
    b = _host(None)
    a.deliver("t1", "coin", {"amount": 100})
    a.run_to_quiescence()
    b.deliver("t1", "coin", {"amount": 100})
    b.run_to_quiescence()
    assert a.instances["t1"].active_leaf_names() == b.instances["t1"].active_leaf_names()


def test_jsonl_observer_writes_records() -> None:
    buf = io.StringIO()
    host = _host(JsonlObserver(buf))
    host.deliver("t1", "coin", {"amount": 100})
    host.run_to_quiescence()
    lines = [json.loads(x) for x in buf.getvalue().splitlines() if x.strip()]
    assert lines[-1]["event"] == "coin"
    assert lines[-1]["entered"] == ["unlocked"]
