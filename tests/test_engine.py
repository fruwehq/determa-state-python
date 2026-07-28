from __future__ import annotations

import copy

import pytest

from determa.state import Bundle, create, dispatch, load_bundle
from determa.state.engine import _cause_id, _Execution, _root_runtime_id
from determa.state.errors import StepFault
from determa.state.model import BundleModel

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

FROZEN_SUBTREE_BUNDLE = """
format: 1
namespace: example.frozen_subtree
events:
  start: { direction: input }
  boom: { direction: input }
  ping: { direction: input }
  cancel_parent: { direction: input }
machines:
  - machine_id: owner
    root:
      variables:
        parent_reference:
          type: instance_reference
          nullable: true
          init: null
          machine_id: parent
      on_events:
        start:
          action:
            - spawn: { machine_id: parent, bind_to: parent_reference }
        cancel_parent:
          action:
            - cancel: { instance: parent_reference }
  - machine_id: parent
    root:
      type: parallel
      variables:
        child_reference:
          type: instance_reference
          nullable: true
          init: null
          machine_id: grandchild
        value: { type: int, init: 0 }
      entry:
        - spawn: { machine_id: grandchild, bind_to: child_reference }
      components:
        - component_id: retained_component
          machine_id: component_worker
        - component_id: retained_component_two
          machine_id: component_worker
      on_events:
        boom:
          action:
            - assign: { value: "1 / 0" }
  - machine_id: grandchild
    root:
      on_events:
        ping: { action: [] }
  - machine_id: component_worker
    events:
      internal_ping: { direction: internal }
    root:
      on_events:
        internal_ping: { action: [] }
"""

IDENTITY_EMISSION_BUNDLE = """
format: 1
namespace: example.identity_emission
events:
  emit: { direction: input }
  internal_notice:
    direction: internal
    payload:
      value: { type: int, required: true }
  external_notice:
    direction: output
    payload:
      value: { type: int, required: true }
machines:
  - machine_id: identity_emission
    root:
      on_events:
        emit:
          action:
            - send:
                event: internal_notice
                payload: { value: "1" }
            - send:
                event: external_notice
                to: { external: true }
                payload: { value: "2" }
                correlation_id: "'correlation-1'"
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


def _nested_runtime_state() -> tuple[Bundle, dict]:
    bundle = load_bundle(FROZEN_SUBTREE_BUNDLE)
    state = create(bundle, "owner", "owner-prior", "create-prior", {})["state"]
    state = dispatch(
        bundle,
        state,
        {"input": _envelope(state, "start", "start-prior")},
    )["state"]
    return bundle, state


def test_normative_root_runtime_identity_vector() -> None:
    bundle = load_bundle(FINGERPRINT_BUNDLE)
    machine = bundle.raw["machines"][0]

    assert _root_runtime_id(bundle, machine, "turnstile-42") == (
        "sha256:72dca6d0b2b3690ae28bda2f17a461179b18fbf11daad7a12709d9384a500c64"
    )


def test_normative_first_component_and_root_initialization_cause_vectors() -> None:
    bundle = load_bundle(FINGERPRINT_BUNDLE)
    result = create(bundle, "turnstile", "turnstile-42", "create-7", {})
    state = result["state"]
    left = next(
        runtime
        for runtime in state["runtimes"].values()
        if runtime.get("component_id") == "left"
    )

    assert left["runtime_id"] == (
        "sha256:43db74b6a8d6f31543f7d142fb5e25a49e33eb3bf548e7bfd20d59513778cbc3"
    )
    assert _cause_id(
        "root_initialization",
        "turnstile-42",
        state["root_runtime_id"],
        state["root_runtime_id"],
        "create-7",
        0,
        "/machines/0/root",
        0,
    ) == "sha256:c9e8e89a01362f40e9a74c01392d09abe2323f31c8f14f22e05bfcaf6dfac0ab"


def test_identity_counter_operands_use_canonical_decimal_above_javascript_range() -> None:
    bundle = load_bundle(FINGERPRINT_BUNDLE)
    state = create(bundle, "turnstile", "turnstile-42", "create-7", {})["state"]

    assert _cause_id(
        "root_initialization",
        "turnstile-42",
        state["root_runtime_id"],
        state["root_runtime_id"],
        "create-7",
        9007199254740993,
        "/machines/0/root",
        9007199254740995,
    ) == "sha256:2df23aef5335fe81713038d71e9d1d3f5c91512d995b2175070b8ab77e20da2b"


def test_exact_internal_event_and_external_effect_identity_vectors() -> None:
    bundle = load_bundle(IDENTITY_EMISSION_BUNDLE)
    state = create(
        bundle,
        "identity_emission",
        "identity-root-1",
        "identity-create-1",
        {},
    )["state"]
    result = dispatch(
        bundle,
        state,
        {"input": _envelope(state, "emit", "identity-input-1")},
    )

    assert state["root_runtime_id"] == (
        "sha256:cdfc68fdcbeef09460a7d51758a1d60fa673351d2196357c5c34bd64511ffac2"
    )
    assert result["emissions"] == [
        {
            "event": "internal_notice",
            "event_id": (
                "sha256:521c9fa9e3f1d6ba7187b89f97d9a26bd118213e1ebec9a662bcaf27f96f0e9c"
            ),
            "target": _root_target(state),
            "payload": {"value": 1},
        },
        {
            "event": "external_notice",
            "target": "external",
            "payload": {"value": 2},
            "correlation_id": "correlation-1",
            "effect_id": (
                "sha256:d01f6d7dbf678ed598a7a37fea7a025f3818e8ff777d0a9363b92f578fcee5d7"
            ),
            "sequence": 0,
        },
    ]


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
    envelope = _envelope(prior, "explode", "explode-1")
    prior_snapshot = copy.deepcopy(prior)
    envelope_snapshot = copy.deepcopy(envelope)

    result = dispatch(
        bundle,
        prior,
        {"input": envelope},
    )

    assert prior == prior_snapshot
    assert envelope == envelope_snapshot
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


@pytest.mark.parametrize(
    "corruption",
    [
        "contained_runtime_identity",
        "owner_relation",
        "component_definition_path",
        "component_owning_state_path",
        "history_key",
        "component_activation_counter",
        "spawn_counter",
        "holder_pointer",
    ],
)
def test_recursive_malformed_prior_state_is_rejected_atomically(corruption: str) -> None:
    bundle, prior = _nested_runtime_state()
    parent = next(
        runtime
        for runtime in prior["runtimes"].values()
        if runtime["role"] == "spawned" and runtime["machine_id"] == "parent"
    )
    grandchild = next(
        runtime
        for runtime in prior["runtimes"].values()
        if runtime["role"] == "spawned" and runtime["machine_id"] == "grandchild"
    )
    component = next(
        runtime
        for runtime in prior["runtimes"].values()
        if runtime["role"] == "component"
    )
    if corruption == "contained_runtime_identity":
        grandchild["runtime_id"] = "corrupt-runtime-id"
    elif corruption == "owner_relation":
        grandchild["owner_runtime_id"] = prior["root_runtime_id"]
    elif corruption == "component_definition_path":
        component["root_pointer"] = "/machines/999/root"
    elif corruption == "component_owning_state_path":
        component["owning_state_path"] = "root.missing"
    elif corruption == "history_key":
        parent["history"]["root.missing"] = None
    elif corruption == "component_activation_counter":
        pointer = component["component_definition_pointer"]
        parent["next_component_activation_sequence"][pointer] = 0
    elif corruption == "spawn_counter":
        parent["next_spawn_sequence"] = grandchild["spawn_sequence"]
    elif corruption == "holder_pointer":
        grandchild["holder"]["pointer"] = "/machines/1/root/variables/missing"
    snapshot = copy.deepcopy(prior)

    result = dispatch(bundle, prior)

    assert prior == snapshot
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


def test_running_descendants_of_faulted_runtime_are_frozen_and_owner_can_cancel() -> None:
    bundle = load_bundle(FROZEN_SUBTREE_BUNDLE)
    state = create(bundle, "owner", "owner-1", "create-1", {})["state"]
    state = dispatch(
        bundle,
        state,
        {"input": _envelope(state, "start", "start-1")},
    )["state"]
    parent = next(
        runtime
        for runtime in state["runtimes"].values()
        if runtime["role"] == "spawned" and runtime["machine_id"] == "parent"
    )
    grandchild = next(
        runtime
        for runtime in state["runtimes"].values()
        if runtime["role"] == "spawned" and runtime["machine_id"] == "grandchild"
    )
    components = [
        runtime
        for runtime in state["runtimes"].values()
        if runtime["role"] == "component"
    ]
    component = components[0]
    state = dispatch(
        bundle,
        state,
        {
            "input": {
                "event": "boom",
                "event_id": "boom-1",
                "target": {"spawned_instance": parent["instance_reference"]},
                "payload": {},
            }
        },
    )["state"]
    frozen = copy.deepcopy(state)

    spawned_result = dispatch(
        bundle,
        state,
        {
            "input": {
                "event": "ping",
                "event_id": "ping-1",
                "target": {"spawned_instance": grandchild["instance_reference"]},
                "payload": {},
            }
        },
    )
    component_result = dispatch(
        bundle,
        state,
        {
            "internal": {
                "event": "internal_ping",
                "event_id": "internal-ping-1",
                "target": component["target"],
                "payload": {},
            }
        },
    )

    assert state == frozen
    assert spawned_result["rejection"] == {"code": "invalid_instance_target"}
    assert spawned_result["state"] is state
    assert component_result["rejection"] == {"code": "inactive_component_target"}
    assert component_result["state"] is state

    execution = _Execution(
        bundle,
        BundleModel(bundle),
        copy.deepcopy(state),
        step_sequence=int(state["next_logical_step_sequence"]),
    )
    with pytest.raises(StepFault, match="invalid_instance_target"):
        execution.resolve_send_target(
            execution.state["runtimes"][execution.state["root_runtime_id"]],
            {"instance": "retained_child_reference"},
            grandchild["instance_reference"],
            "/test/send",
            0,
            False,
        )

    cancelled = dispatch(
        bundle,
        state,
        {"input": _envelope(state, "cancel_parent", "cancel-parent-1")},
    )
    retained_ids = {
        parent["runtime_id"],
        grandchild["runtime_id"],
        *(runtime["runtime_id"] for runtime in components),
    }
    assert retained_ids.isdisjoint(cancelled["state"]["runtimes"])
