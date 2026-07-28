from __future__ import annotations

import copy
import math

import pytest

from determa.state import Bundle, create, dispatch, load_bundle
from determa.state.engine import (
    _cause_id,
    _component_runtime_id,
    _Execution,
    _root_runtime_id,
    _spawned_runtime_id,
)
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

NUMERIC_BOUNDARY_BUNDLE = """
format: 1
namespace: example.numeric_boundary
events:
  ordinary_values:
    direction: input
    payload:
      mapped: { type: map, required: true }
machines:
  - machine_id: numeric_boundary
    root:
      variables:
        created_map: { type: map, input: true }
        rate: { type: float, external: true }
        stored_map: { type: map, init: {} }
        guard_selected: { type: bool, init: false }
      on_events:
        ordinary_values:
          action:
            - assign: { stored_map: "event.payload.mapped" }
        env:
          - guard: "event.payload.changed.rate == 1.0"
            action:
              - assign: { guard_selected: "true" }
              - refresh: {}
          - action:
              - assign: { guard_selected: "false" }
              - refresh: {}
"""

HOLDER_REUSE_BUNDLE = """
format: 1
namespace: example.holder_reuse
events:
  prepare: { direction: input }
  leave: { direction: input }
machines:
  - machine_id: owner
    root:
      type: composite
      initial: { transition_to: holding }
      states:
        holding:
          variables:
            child_reference:
              type: instance_reference
              machine_id: child
              nullable: true
              init: null
          on_events:
            prepare:
              action:
                - spawn: { machine_id: child, bind_to: child_reference }
                - assign: { child_reference: "null" }
                - spawn: { machine_id: child, bind_to: child_reference }
                - assign: { child_reference: "null" }
            leave: { transition_to: outside }
        outside: {}
  - machine_id: child
    root: {}
"""

FROZEN_SUBTREE_BUNDLE = """
format: 1
namespace: example.frozen_subtree
events:
  start: { direction: input }
  boom: { direction: input }
  ping: { direction: input }
  cancel_parent: { direction: input }
  cleanup: { direction: output }
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
      exit:
        - send:
            event: cleanup
            to: { external: true }
            correlation_id: "'frozen-cleanup'"
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

HIGH_COUNTER_BUNDLE = """
format: 1
namespace: example.high_counter
events:
  activate: { direction: input }
  activated: { direction: output }
machines:
  - machine_id: high_counter
    root:
      type: composite
      variables:
        worker_reference:
          type: instance_reference
          nullable: true
          init: null
          machine_id: worker
      initial: { transition_to: idle }
      states:
        idle:
          on_events:
            activate:
              action:
                - spawn: { machine_id: worker, bind_to: worker_reference }
                - send:
                    event: activated
                    to: { external: true }
                    correlation_id: "'high-correlation'"
              transition_to: group
        group:
          type: parallel
          components:
            - component_id: first
              machine_id: worker
            - component_id: second
              machine_id: worker
  - machine_id: worker
    root: {}
"""

CANCEL_OWNERSHIP_BUNDLE = """
format: 1
namespace: example.cancel_ownership
events:
  setup: { direction: input }
  spawn_descendant: { direction: input }
machines:
  - machine_id: owner
    root:
      variables:
        first:
          type: instance_reference
          nullable: true
          init: null
          machine_id: participant
        second:
          type: instance_reference
          nullable: true
          init: null
          machine_id: participant
      on_events:
        setup:
          action:
            - spawn: { machine_id: participant, bind_to: first }
            - spawn: { machine_id: participant, bind_to: second }
  - machine_id: participant
    root:
      variables:
        middle:
          type: instance_reference
          nullable: true
          init: null
          machine_id: middle
      on_events:
        spawn_descendant:
          action:
            - spawn: { machine_id: middle, bind_to: middle }
  - machine_id: middle
    root:
      variables:
        leaf:
          type: instance_reference
          nullable: true
          init: null
          machine_id: leaf
      entry:
        - spawn: { machine_id: leaf, bind_to: leaf }
  - machine_id: leaf
    root: {}
"""

CASCADE_FAULT_BUNDLE = """
format: 1
namespace: example.cascade_fault
events:
  start: { direction: input }
  cancel_child: { direction: input }
  cleanup:
    direction: output
machines:
  - machine_id: owner
    root:
      variables:
        child:
          type: instance_reference
          nullable: true
          init: null
          machine_id: child
      on_events:
        start:
          action:
            - spawn: { machine_id: child, bind_to: child }
        cancel_child:
          action:
            - cancel: { instance: child }
  - machine_id: child
    root:
      variables:
        value: { type: int, init: 0 }
      exit:
        - send:
            event: cleanup
            to: { external: true }
            correlation_id: "'cleanup'"
        - assign: { value: "1 / 0" }
"""

_CASCADE_COMPONENTS = "\n".join(
    f"""
        - component_id: component_{index}
          machine_id: cleanup_worker
          with:
            input:
              marker: "'component-{index}'"
"""
    for index in range(11)
)

CASCADE_ORDER_BUNDLE = f"""
format: 1
namespace: example.cascade_order
events:
  finish: {{ direction: input }}
  cleanup:
    direction: output
    payload:
      marker: {{ type: string, required: true }}
machines:
  - machine_id: owner
    root:
      type: parallel
      variables:
        z_reference:
          type: instance_reference
          nullable: true
          init: null
          machine_id: cleanup_worker
        a_reference:
          type: instance_reference
          nullable: true
          init: null
          machine_id: cleanup_worker
      entry:
        - spawn:
            machine_id: cleanup_worker
            bindings: {{ input: {{ marker: "'z-spawn'" }} }}
            bind_to: z_reference
        - spawn:
            machine_id: cleanup_worker
            bindings: {{ input: {{ marker: "'a-spawn'" }} }}
            bind_to: a_reference
      components:
{_CASCADE_COMPONENTS}
      on_events:
        finish:
          action:
            - stop: {{}}
  - machine_id: cleanup_worker
    root:
      variables:
        marker: {{ type: string, input: true }}
      exit:
        - send:
            event: cleanup
            to: {{ external: true }}
            payload: {{ marker: marker }}
            correlation_id: "'cleanup'"
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


def _runtime_envelope(
    runtime: dict,
    event: str,
    event_id: str,
    payload: dict | None = None,
) -> dict:
    return {
        "event": event,
        "event_id": event_id,
        "target": {"spawned_instance": copy.deepcopy(runtime["instance_reference"])},
        "payload": copy.deepcopy(payload or {}),
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


def test_dispatch_allocates_unbounded_logical_counters_with_canonical_id_operands() -> None:
    bundle = load_bundle(HIGH_COUNTER_BUNDLE)
    state = create(bundle, "high_counter", "high-root", "high-create", {})["state"]
    root = state["runtimes"][state["root_runtime_id"]]
    step_sequence = 2**63 + 9
    output_sequence = 2**63 + 11
    spawn_sequence = 2**63 + 13
    root_activation = 2**63 + 17
    idle_activation = 2**63 + 19
    group_activation = 2**63 + 23
    component_activation = 2**63 + 29
    state["next_logical_step_sequence"] = step_sequence
    state["next_output_sequence"] = output_sequence
    root["next_spawn_sequence"] = spawn_sequence
    root["state_activation_sequence"]["root"] = root_activation
    root["next_state_activation_sequence"]["root"] = root_activation + 1
    root["state_activation_sequence"]["idle"] = idle_activation
    root["next_state_activation_sequence"]["idle"] = idle_activation + 1
    root["next_state_activation_sequence"]["group"] = group_activation
    first_pointer = "/machines/0/root/states/group/components/0"
    second_pointer = "/machines/0/root/states/group/components/1"
    root["next_component_activation_sequence"][first_pointer] = component_activation
    root["next_component_activation_sequence"][second_pointer] = component_activation + 1

    result = dispatch(
        bundle,
        state,
        {"input": _envelope(state, "activate", "high-activate")},
    )

    assert result["disposition"] == "handled"
    assert result["state"]["next_logical_step_sequence"] == step_sequence + 1
    assert result["state"]["next_output_sequence"] == output_sequence + 1
    result_root = result["state"]["runtimes"][state["root_runtime_id"]]
    assert result_root["state_activation_sequence"]["group"] == group_activation
    spawned = next(
        runtime
        for runtime in result["state"]["runtimes"].values()
        if runtime["role"] == "spawned"
    )
    first = next(
        runtime
        for runtime in result["state"]["runtimes"].values()
        if runtime.get("component_id") == "first"
    )
    worker = BundleModel(bundle).machine("worker")
    assert spawned["spawn_sequence"] == spawn_sequence
    assert spawned["runtime_id"] == _spawned_runtime_id(
        bundle,
        state["root_runtime_id"],
        state["root_instance_id"],
        "/machines/0/root/states/idle/on_events/activate/action/0/spawn",
        spawn_sequence,
        worker,
    )
    assert first["component_activation_sequence"] == component_activation
    assert first["runtime_id"] == _component_runtime_id(
        bundle,
        state["root_runtime_id"],
        state["root_instance_id"],
        first_pointer,
        component_activation,
        worker,
    )
    assert spawned["runtime_id"] == (
        "sha256:92bc50f1329857e0a74ec7ec1309f8378b8af1fd67ba83cad343984ac30cb97c"
    )
    assert first["runtime_id"] == (
        "sha256:afe3002d5165a5a22ea198ff60853f7bdbbd8ff7b8fb9c32916c9215968bad5e"
    )
    assert result["emissions"] == [
        {
            "event": "activated",
            "target": "external",
            "payload": {},
            "correlation_id": "high-correlation",
            "effect_id": (
                "sha256:f5e454de1e9c3f70801148fc9719491cc1b4dc460a8129a342cbee0648f7659c"
            ),
            "sequence": output_sequence,
        }
    ]


@pytest.mark.parametrize("nested", [False, True])
def test_oversized_ordinary_prior_values_remain_invalid(nested: bool) -> None:
    if nested:
        bundle = load_bundle(BINDING_BUNDLE)
        state = create(
            bundle,
            "binding",
            "binding-high",
            "binding-create",
            {"input": {"settings": {"nested": [1]}}},
        )["state"]
        root = state["runtimes"][state["root_runtime_id"]]
        root["scopes"]["root"]["settings"]["nested"][0] = 2**63
    else:
        bundle = load_bundle(COUNTER_BUNDLE)
        state = create(bundle, "counter", "counter-high", "counter-create", {})["state"]
        root = state["runtimes"][state["root_runtime_id"]]
        root["scopes"]["root"]["count"] = 2**63

    result = dispatch(bundle, state)

    assert result["disposition"] == "rejected"
    assert result["rejection"] == {"code": "invalid_prior_state"}
    assert result["state"] is state


def test_retained_fault_step_sequence_is_an_unbounded_logical_counter() -> None:
    bundle = load_bundle(COUNTER_BUNDLE)
    state = create(bundle, "counter", "counter-fault-high", "fault-create", {})["state"]
    state = dispatch(
        bundle,
        state,
        {"input": _envelope(state, "explode", "fault-high")},
    )["state"]
    step_sequence = 2**63 + 101
    state["next_logical_step_sequence"] = step_sequence + 1
    state["fault"]["step_sequence"] = step_sequence
    root = state["runtimes"][state["root_runtime_id"]]
    root["fault"]["step_sequence"] = step_sequence
    snapshot = copy.deepcopy(state)

    result = dispatch(bundle, state)

    assert result["status"] == "faulted"
    assert result["disposition"] is None
    assert result["state"] is state
    assert result["state"] == snapshot


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
        lambda state: state.update({"next_output_sequence": -1}),
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


def test_completed_root_cannot_retain_owned_descendants() -> None:
    bundle, prior = _nested_runtime_state()
    root = prior["runtimes"][prior["root_runtime_id"]]
    prior["status"] = "completed"
    root["status"] = "completed"

    result = dispatch(bundle, prior)

    assert result["disposition"] == "rejected"
    assert result["rejection"] == {"code": "invalid_prior_state"}
    assert result["state"] is prior


def test_completed_spawned_runtime_cannot_be_retained() -> None:
    bundle, prior = _nested_runtime_state()
    grandchild = next(
        runtime
        for runtime in prior["runtimes"].values()
        if runtime["role"] == "spawned" and runtime["machine_id"] == "grandchild"
    )
    grandchild["status"] = "completed"
    grandchild["active"] = []
    grandchild["scopes"] = {}
    grandchild["state_activation_sequence"] = {}

    result = dispatch(bundle, prior)

    assert result["disposition"] == "rejected"
    assert result["rejection"] == {"code": "invalid_prior_state"}
    assert result["state"] is prior


def test_completed_component_remains_a_valid_retained_runtime() -> None:
    bundle, prior = _nested_runtime_state()
    component = next(
        runtime for runtime in prior["runtimes"].values() if runtime["role"] == "component"
    )
    component["status"] = "completed"
    component["active"] = []
    component["scopes"] = {}
    component["state_activation_sequence"] = {}
    snapshot = copy.deepcopy(prior)

    result = dispatch(bundle, prior)

    assert result["disposition"] is None
    assert result["state"] == snapshot
    assert result["state"] is prior


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("code", "arbitrary_fault"),
        ("source_locator", "not-a-pointer"),
        ("source_locator", "system:not_reserved"),
        ("step_sequence", "next"),
    ],
)
def test_root_fault_record_uses_closed_committed_domain(field: str, value: object) -> None:
    bundle = load_bundle(COUNTER_BUNDLE)
    state = create(bundle, "counter", "fault-domain", "fault-create", {})["state"]
    state = dispatch(
        bundle,
        state,
        {"input": _envelope(state, "explode", "fault-input")},
    )["state"]
    root = state["runtimes"][state["root_runtime_id"]]
    replacement = state["next_logical_step_sequence"] if value == "next" else value
    root["fault"][field] = replacement
    state["fault"][field] = replacement

    result = dispatch(bundle, state)

    assert result["disposition"] == "rejected"
    assert result["rejection"] == {"code": "invalid_prior_state"}
    assert result["state"] is state


def test_nested_fault_record_uses_closed_committed_domain() -> None:
    bundle, state = _nested_runtime_state()
    parent = next(
        runtime
        for runtime in state["runtimes"].values()
        if runtime["role"] == "spawned" and runtime["machine_id"] == "parent"
    )
    result = dispatch(
        bundle,
        state,
        {"input": _runtime_envelope(parent, "boom", "nested-fault-input")},
    )
    state = result["state"]
    parent = state["runtimes"][parent["runtime_id"]]
    parent["fault"]["code"] = "arbitrary_fault"

    rejected = dispatch(bundle, state)

    assert rejected["disposition"] == "rejected"
    assert rejected["rejection"] == {"code": "invalid_prior_state"}
    assert rejected["state"] is state


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


def test_programmatic_values_are_recursively_validated_and_normalized() -> None:
    bundle = load_bundle(NUMERIC_BOUNDARY_BUNDLE)
    invalid = create(
        bundle,
        "numeric_boundary",
        "numeric-invalid",
        "numeric-invalid-create",
        {
            "input": {"created_map": {"nested": [math.nan]}},
            "external": {"rate": 0.0},
        },
    )
    assert invalid["rejection"] == {"code": "invalid_binding"}
    assert invalid["state"] is None

    created = create(
        bundle,
        "numeric_boundary",
        "numeric-boundary",
        "numeric-create",
        {
            "input": {
                "created_map": {
                    "integer": 1,
                    "double": 1.0,
                    "nested": [-0.0],
                }
            },
            "external": {"rate": -0.0},
        },
    )
    state = created["state"]
    created_values = _root_variables(state)
    assert type(created_values["created_map"]["integer"]) is int
    assert type(created_values["created_map"]["double"]) is float
    assert math.copysign(1.0, created_values["created_map"]["nested"][0]) == 1.0
    assert math.copysign(1.0, created_values["rate"]) == 1.0

    envelope = _envelope(state, "ordinary_values", "ordinary-values")
    envelope["payload"] = {
        "mapped": {
            "integer": 1,
            "double": 1.0,
            "nested": [-0.0],
        }
    }
    envelope_snapshot = copy.deepcopy(envelope)
    handled = dispatch(bundle, state, {"input": envelope})
    assert envelope == envelope_snapshot
    stored = _root_variables(handled["state"])["stored_map"]
    assert type(stored["integer"]) is int
    assert type(stored["double"]) is float
    assert math.copysign(1.0, stored["nested"][0]) == 1.0

    rejected_envelope = _envelope(handled["state"], "ordinary_values", "non-finite")
    rejected_envelope["payload"] = {"mapped": {"nested": [math.inf]}}
    rejected = dispatch(bundle, handled["state"], {"input": rejected_envelope})
    assert rejected["rejection"] == {"code": "invalid_payload"}
    assert rejected["state"] is handled["state"]

    malformed_state = copy.deepcopy(handled["state"])
    root = malformed_state["runtimes"][malformed_state["root_runtime_id"]]
    root["scopes"]["root"]["stored_map"]["nested"][0] = math.nan
    next_step = malformed_state["next_logical_step_sequence"]
    rejected = dispatch(bundle, malformed_state)
    assert rejected["rejection"] == {"code": "invalid_prior_state"}
    assert rejected["state"] is malformed_state
    assert malformed_state["next_logical_step_sequence"] == next_step
    retained = malformed_state["runtimes"][root["runtime_id"]]["scopes"]["root"]
    assert math.isnan(retained["stored_map"]["nested"][0])


def test_env_changed_is_normalized_before_guard_and_refresh() -> None:
    bundle = load_bundle(NUMERIC_BOUNDARY_BUNDLE)
    state = create(
        bundle,
        "numeric_boundary",
        "numeric-env",
        "numeric-create",
        {
            "input": {"created_map": {}},
            "external": {"rate": 0},
        },
    )["state"]
    root = state["runtimes"][state["root_runtime_id"]]
    root["scopes"]["root"]["created_map"] = {"nested": [-0.0]}
    envelope = _envelope(state, "env", "env-change")
    envelope["payload"] = {"changed": {"rate": 1}}
    envelope_snapshot = copy.deepcopy(envelope)

    result = dispatch(bundle, state, {"input": envelope})

    assert envelope == envelope_snapshot
    values = _root_variables(result["state"])
    assert values["guard_selected"] is True
    assert type(values["rate"]) is float
    assert values["rate"] == 1.0
    assert math.copysign(1.0, values["created_map"]["nested"][0]) == 1.0
    assert math.copysign(1.0, _root_variables(state)["created_map"]["nested"][0]) == -1.0


def test_holder_association_survives_reference_clear_and_reuse() -> None:
    bundle = load_bundle(HOLDER_REUSE_BUNDLE)
    state = create(bundle, "owner", "holder-owner", "holder-create", {})["state"]
    prepared = dispatch(
        bundle,
        state,
        {"input": _envelope(state, "prepare", "prepare")},
    )
    prepared_state = prepared["state"]
    children = [
        runtime
        for runtime in prepared_state["runtimes"].values()
        if runtime["role"] == "spawned"
    ]
    assert _root_variables(prepared_state)["child_reference"] is None
    assert len(children) == 2
    assert {child["holder"]["pointer"] for child in children} == {
        "/machines/0/root/states/holding/variables/child_reference"
    }

    left = dispatch(
        bundle,
        prepared_state,
        {"input": _envelope(prepared_state, "leave", "leave")},
    )

    assert left["disposition"] == "handled"
    assert len(left["state"]["runtimes"]) == 1
    assert _root_variables(left["state"]) == {}


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
    assert cancelled["emissions"] == []


def test_cancel_only_disposes_runtime_owned_descendants() -> None:
    bundle = load_bundle(CANCEL_OWNERSHIP_BUNDLE)
    state = create(bundle, "owner", "cancel-owner", "cancel-create", {})["state"]
    state = dispatch(
        bundle,
        state,
        {"input": _envelope(state, "setup", "cancel-setup")},
    )["state"]
    participants = sorted(
        (
            runtime
            for runtime in state["runtimes"].values()
            if runtime.get("machine_id") == "participant"
        ),
        key=lambda runtime: runtime["spawn_sequence"],
    )
    first, second = participants
    state = dispatch(
        bundle,
        state,
        {
            "input": _runtime_envelope(
                first,
                "spawn_descendant",
                "spawn-descendant",
            )
        },
    )["state"]
    first = state["runtimes"][first["runtime_id"]]
    second = state["runtimes"][second["runtime_id"]]
    middle = next(
        runtime
        for runtime in state["runtimes"].values()
        if runtime.get("owner_runtime_id") == first["runtime_id"]
    )
    leaf = next(
        runtime
        for runtime in state["runtimes"].values()
        if runtime.get("owner_runtime_id") == middle["runtime_id"]
    )
    execution = _Execution(
        bundle,
        BundleModel(bundle),
        copy.deepcopy(state),
        step_sequence=int(state["next_logical_step_sequence"]),
    )
    first = execution.state["runtimes"][first["runtime_id"]]
    second = execution.state["runtimes"][second["runtime_id"]]
    middle = execution.state["runtimes"][middle["runtime_id"]]
    leaf = execution.state["runtimes"][leaf["runtime_id"]]

    execution.cancel(second, leaf["instance_reference"])
    assert leaf["runtime_id"] in execution.state["runtimes"]

    execution.cancel(leaf, first["instance_reference"])
    assert first["runtime_id"] in execution.state["runtimes"]

    execution.cancel(first, leaf["instance_reference"])
    assert leaf["runtime_id"] not in execution.state["runtimes"]
    assert middle["runtime_id"] in execution.state["runtimes"]


def test_runtime_completion_uses_canonical_component_and_holder_order() -> None:
    bundle = load_bundle(CASCADE_ORDER_BUNDLE)
    state = create(bundle, "owner", "cascade-order", "cascade-create", {})["state"]

    result = dispatch(
        bundle,
        state,
        {"input": _envelope(state, "finish", "cascade-finish")},
    )

    assert result["status"] == "completed"
    assert [
        emission["payload"]["marker"] for emission in result["emissions"]
    ] == [
        *(f"component-{index}" for index in range(10, -1, -1)),
        "a-spawn",
        "z-spawn",
    ]
    assert [emission["sequence"] for emission in result["emissions"]] == list(range(13))


def test_descendant_cleanup_failure_rolls_back_and_faults_owner_as_cascade() -> None:
    bundle = load_bundle(CASCADE_FAULT_BUNDLE)
    state = create(bundle, "owner", "cascade-fault", "cascade-create", {})["state"]
    state = dispatch(
        bundle,
        state,
        {"input": _envelope(state, "start", "cascade-start")},
    )["state"]
    prior_snapshot = copy.deepcopy(state)
    envelope = _envelope(state, "cancel_child", "cascade-cancel")
    envelope_snapshot = copy.deepcopy(envelope)

    result = dispatch(bundle, state, {"input": envelope})

    assert state == prior_snapshot
    assert envelope == envelope_snapshot
    assert result["status"] == "faulted"
    assert result["disposition"] == "faulted"
    assert result["emissions"] == []
    assert result["fault"] == {
        "runtime_id": state["root_runtime_id"],
        "cause_id": "cascade-cancel",
        "code": "cascade_fault",
        "step_sequence": state["next_logical_step_sequence"],
        "source_locator": "system:cascade_cleanup",
    }
    child_ids = {
        runtime["runtime_id"]
        for runtime in state["runtimes"].values()
        if runtime["role"] == "spawned"
    }
    assert child_ids <= set(result["state"]["runtimes"])
