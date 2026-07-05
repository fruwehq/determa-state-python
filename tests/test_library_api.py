"""The public library API (SPEC §2 "Library API").

Drives a machine end-to-end through the ``determa.state`` public surface **only** — no
``determa.state.cli`` and no file-backed store — exercising every capability the spec
requires an embeddable API to provide.
"""

from __future__ import annotations

import determa.state as ds

GATE = """\
id: gate
events:
  coin: { payload: { amount: { type: int, required: true } } }
  push: {}
top:
  esvs:
    fare: { type: int, external: true }   # host-seeded, read-only (SPEC §4.4)
  initial: { transition_to: locked }
  states:
    locked:
      on_events:
        coin: { transition_to: unlocked, guard: "event.payload.amount >= fare" }
    unlocked:
      on_events:
        push: { transition_to: locked }
"""

META_GATE = """\
id: meta_gate
meta:
  host: guardrail
events:
  go: {}
top:
  meta:
    owner: root
  initial: { transition_to: a }
  states:
    a:
      meta:
        tools: [one, two]
      on_events:
        go: { transition_to: b }
    b: {}
"""


def test_minimum_capability_set_via_public_api() -> None:
    # 1. load + validate a definition (raises ValidationError if invalid).
    defs = ds.load_definitions(GATE)
    for d in defs:
        ds.validate(d.raw)
    assert ds.collect_errors(defs[0].raw) == []

    # 2. register definitions + create a root instance with an id and external esvs.
    host = ds.Host()
    host.register_all(defs)
    inst = host.create_root(host.machines["gate"], "g1", external={"fare": 50})
    host.run_to_quiescence()

    # 3. read status, active configuration, and esvs.
    assert inst.status is ds.Status.ACTIVE
    assert inst.active_leaf_names() == ["locked"]
    assert inst.resolved_esvs()["fare"] == 50

    # 4. deliver a typed event + run to quiescence.
    assert host.deliver("g1", "coin", {"amount": 100}) is True
    host.run_to_quiescence()
    assert inst.active_leaf_names() == ["unlocked"]

    # an invalid payload is rejected, not enqueued (§4.3).
    assert host.deliver("g1", "coin", {"amount": "nope"}) is False

    # 5. advance the virtual clock.
    host.advance("30s")
    assert host.now == 30_000

    # 6. snapshot + restore an instance (§8) — state survives the round-trip.
    snaps = host.snapshot_all()
    host.deliver("g1", "push", None)
    host.run_to_quiescence()
    assert inst.active_leaf_names() == ["locked"]
    host.restore_all(snaps)
    assert host.instances["g1"].active_leaf_names() == ["unlocked"]


def test_public_surface_is_exported() -> None:
    """The documented public API stays importable from the top-level package."""
    expected = {
        "Definition",
        "Host",
        "Instance",
        "Machine",
        "State",
        "Status",
        "Event",
        "DetermaError",
        "SchemaError",
        "ValidationError",
        "ErrorRecord",
        "CelError",
        "load_definition",
        "load_definitions",
        "validate",
        "collect_errors",
        "__version__",
    }
    assert expected <= set(ds.__all__)
    for name in expected:
        assert hasattr(ds, name), f"determa.state.{name} not exported"


def test_meta_is_validation_only_model_data_not_runtime_state() -> None:
    defs = ds.load_definitions(META_GATE)
    assert ds.collect_errors(defs[0].raw) == []

    host = ds.Host()
    host.register_all(defs)
    machine = host.machines["meta_gate"]
    inst = host.create_root(machine, "m1")
    host.run_to_quiescence()

    assert machine.meta == {"host": "guardrail"}
    assert machine.top.meta == {"owner": "root"}
    assert machine.by_path["top.a"].meta == {"tools": ["one", "two"]}
    assert inst.active_leaf_names() == ["a"]

    snap = inst.to_snapshot()
    assert "meta" not in snap

    assert host.deliver("m1", "go") is True
    host.run_to_quiescence()
    assert inst.active_leaf_names() == ["b"]
