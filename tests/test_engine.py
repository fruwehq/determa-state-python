from __future__ import annotations

import copy

import pytest

from determa.state import create, dispatch, load_bundle
from determa.state.engine import _root_runtime_id

from .test_loading import FINGERPRINT_BUNDLE

COUNTER_BUNDLE = """
format: 1
namespace: example.counter
events:
  increment:
    direction: input
  explode:
    direction: input
machines:
  - machine_id: counter
    root:
      type: composite
      variables:
        count: { type: int, init: 0 }
      initial: { transition_to: running }
      states:
        running:
          on_events:
            increment:
              action:
                - assign: { count: "count + 1" }
            explode:
              action:
                - assign: { count: "count + 1" }
                - assign: { count: "count / 0" }
"""

BINDING_BUNDLE = """
format: 1
namespace: example.binding
machines:
  - machine_id: binding
    root:
      variables:
        settings: { type: map, input: true }
"""


def _root_target(state: dict) -> dict:
    return {
        "root": {
            "root_instance_id": state["root_instance_id"],
            "root_runtime_id": state["root_runtime_id"],
        }
    }


def _envelope(state: dict, event: str, event_id: str) -> dict:
    return {
        "event": event,
        "event_id": event_id,
        "target": _root_target(state),
        "payload": {},
    }


def _root_variables(state: dict) -> dict:
    root = state["runtimes"][state["root_runtime_id"]]
    values: dict = {}
    for path in root["active"]:
        values.update(root["scopes"][path])
    return values


def test_normative_root_runtime_identity_vector() -> None:
    bundle = load_bundle(FINGERPRINT_BUNDLE)
    machine = bundle.raw["machines"][0]

    assert _root_runtime_id(bundle, machine, "turnstile-42") == (
        "sha256:72dca6d0b2b3690ae28bda2f17a461179b18fbf11daad7a12709d9384a500c64"
    )


def test_dispatch_is_pure_and_success_advances_one_logical_step() -> None:
    bundle = load_bundle(COUNTER_BUNDLE)
    created = create(bundle, "counter", "counter-1", "create-1", {})
    prior = created["state"]
    frozen = copy.deepcopy(prior)

    result = dispatch(
        bundle,
        prior,
        {"input": _envelope(prior, "increment", "increment-1")},
    )

    assert prior == frozen
    assert result["state"] is not prior
    assert _root_variables(result["state"])["count"] == 1
    assert result["state"]["next_logical_step_sequence"] == 2


def test_fault_rolls_back_author_writes_and_keeps_input_caller_owned() -> None:
    bundle = load_bundle(COUNTER_BUNDLE)
    prior = create(bundle, "counter", "counter-1", "create-1", {})["state"]

    result = dispatch(
        bundle,
        prior,
        {"input": _envelope(prior, "explode", "explode-1")},
    )

    assert result["status"] == "faulted"
    assert result["disposition"] == "faulted"
    assert _root_variables(result["state"])["count"] == 0
    assert result["emissions"] == []
    assert result["fault"]["cause_id"] == "explode-1"
    assert result["fault"]["source_locator"].endswith("/on_events/explode/action/1/assign/count")
    assert "queue" not in result["state"]


def test_rejection_returns_the_exact_prior_state_object() -> None:
    bundle = load_bundle(COUNTER_BUNDLE)
    prior = create(bundle, "counter", "counter-1", "create-1", {})["state"]
    envelope = _envelope(prior, "missing", "missing-1")

    result = dispatch(bundle, prior, {"input": envelope})

    assert result["disposition"] == "rejected"
    assert result["rejection"] == {"code": "invalid_event"}
    assert result["state"] is prior


def test_changed_bundle_cannot_reinterpret_prior_state() -> None:
    bundle = load_bundle(COUNTER_BUNDLE)
    prior = create(bundle, "counter", "counter-1", "create-1", {})["state"]
    changed = load_bundle(
        COUNTER_BUNDLE.replace("events:", "meta: { revision: changed }\nevents:", 1)
    )

    result = dispatch(changed, prior)

    assert result["rejection"] == {"code": "incompatible_bundle"}
    assert result["state"] is prior


@pytest.mark.parametrize(
    "mutate",
    [
        lambda state: state["runtimes"][state["root_runtime_id"]]["active"].append("missing"),
        lambda state: state["runtimes"][state["root_runtime_id"]]["scopes"].update({"missing": {}}),
        lambda state: state.update({"next_logical_step_sequence": True}),
        lambda state: state.update({"root_instance_id": "\ud800"}),
    ],
)
def test_malformed_prior_state_is_rejected_before_dispatch(mutate) -> None:
    bundle = load_bundle(COUNTER_BUNDLE)
    prior = create(bundle, "counter", "counter-1", "create-1", {})["state"]
    mutate(prior)

    result = dispatch(bundle, prior)

    assert result["disposition"] == "rejected"
    assert result["rejection"] == {"code": "invalid_prior_state"}
    assert result["state"] is prior


def test_nested_nonportable_creation_binding_is_rejected() -> None:
    bundle = load_bundle(BINDING_BUNDLE)

    result = create(
        bundle,
        "binding",
        "binding-1",
        "create-1",
        {"input": {"settings": {"limit": 2**63}}},
    )

    assert result["status"] == "rejected"
    assert result["rejection"] == {"code": "invalid_binding"}
    assert result["state"] is None


def test_cyclic_payload_is_rejected_without_recursion() -> None:
    bundle = load_bundle(COUNTER_BUNDLE)
    prior = create(bundle, "counter", "counter-1", "create-1", {})["state"]
    payload: dict = {}
    payload["amount"] = payload
    envelope = _envelope(prior, "increment", "increment-cycle")
    envelope["payload"] = payload

    result = dispatch(bundle, prior, {"input": envelope})

    assert result["rejection"] == {"code": "invalid_payload"}
    assert result["state"] is prior


def test_root_target_is_a_closed_tagged_union_member() -> None:
    bundle = load_bundle(COUNTER_BUNDLE)
    prior = create(bundle, "counter", "counter-1", "create-1", {})["state"]
    envelope = _envelope(prior, "increment", "increment-extra-target")
    envelope["target"]["root"]["extra"] = "not-portable"

    result = dispatch(bundle, prior, {"input": envelope})

    assert result["rejection"] == {"code": "invalid_instance_target"}
    assert result["state"] is prior
