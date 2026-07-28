"""Pure foreground format-1 creation and dispatch."""

from __future__ import annotations

import copy
import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, cast

from . import cel
from .definition import Bundle, BundleSource, _escape_pointer, hash_identity, load_bundle
from .errors import CelError, StepFault, ValidationError
from .model import BundleModel, MachineModel, StateNode
from .yaml12 import normalize_portable_values, validate_portable_values, validate_unicode

Result = dict[str, Any]
Delivery = dict[str, dict[str, Any]] | None
_INT_MIN = -(2**63)
_INT_MAX = 2**63 - 1


class _StopRuntime(Exception):
    pass


def _coerce_bundle(bundle: Bundle | BundleSource) -> Bundle:
    return bundle if isinstance(bundle, Bundle) else load_bundle(bundle)


def _identity(value: list[Any]) -> str:
    return hash_identity(value)


def _root_runtime_id(bundle: Bundle, machine: dict[str, Any], root_instance_id: str) -> str:
    return _identity(
        [
            "determa-root-runtime-identity-2",
            "1",
            bundle.fingerprint,
            bundle.namespace,
            machine["machine_id"],
            str(machine["version"]),
            root_instance_id,
        ]
    )


def _component_runtime_id(
    bundle: Bundle,
    owner_runtime_id: str,
    root_instance_id: str,
    pointer: str,
    activation_sequence: int,
    machine: MachineModel,
) -> str:
    namespace, machine_id, version = machine.definition_identity()
    return _identity(
        [
            "determa-component-runtime-identity-1",
            "1",
            root_instance_id,
            owner_runtime_id,
            pointer,
            str(activation_sequence),
            namespace,
            machine_id,
            str(version),
        ]
    )


def _spawned_runtime_id(
    bundle: Bundle,
    owner_runtime_id: str,
    root_instance_id: str,
    pointer: str,
    spawn_sequence: int,
    machine: MachineModel,
) -> str:
    namespace, machine_id, version = machine.definition_identity()
    return _identity(
        [
            "determa-spawned-runtime-identity-1",
            "1",
            root_instance_id,
            owner_runtime_id,
            pointer,
            str(spawn_sequence),
            namespace,
            machine_id,
            str(version),
        ]
    )


def _cause_id(
    kind: str,
    root_instance_id: str,
    source_runtime_id: str,
    target_runtime_id: str,
    parent: str,
    step_sequence: int,
    locator: str,
    ordinal: int,
) -> str:
    return _identity(
        [
            "determa-cause-identity-1",
            "1",
            kind,
            root_instance_id,
            source_runtime_id,
            target_runtime_id,
            parent,
            str(step_sequence),
            locator,
            str(ordinal),
        ]
    )


def _event_id(
    root_instance_id: str,
    source_runtime_id: str,
    target_runtime_id: str,
    cause_id: str,
    step_sequence: int,
    locator: str,
    ordinal: int,
) -> str:
    return _identity(
        [
            "determa-event-identity-1",
            "1",
            root_instance_id,
            source_runtime_id,
            target_runtime_id,
            cause_id,
            str(step_sequence),
            locator,
            str(ordinal),
        ]
    )


def _effect_id(
    machine: MachineModel,
    root_instance_id: str,
    runtime_id: str,
    cause_id: str,
    step_sequence: int,
    pointer: str,
    index: int,
) -> str:
    namespace, machine_id, version = machine.definition_identity()
    return _identity(
        [
            "determa-effect-identity-1",
            "1",
            [namespace, machine_id, str(version)],
            root_instance_id,
            runtime_id,
            cause_id,
            str(step_sequence),
            pointer,
            str(index),
        ]
    )


def _value_matches(value: Any, type_name: str) -> bool:
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "bool":
        return isinstance(value, bool)
    if type_name == "int":
        return (
            isinstance(value, int) and not isinstance(value, bool) and _INT_MIN <= value <= _INT_MAX
        )
    if type_name == "float":
        return (
            isinstance(value, int | float)
            and not isinstance(value, bool)
            and _INT_MIN <= value <= _INT_MAX
            if isinstance(value, int)
            else isinstance(value, float) and math.isfinite(value)
        )
    if type_name == "list":
        return isinstance(value, list)
    if type_name == "map":
        return isinstance(value, dict) and all(isinstance(key, str) for key in value)
    if type_name == "instance_reference":
        return value is None or _is_instance_reference(value)
    return False


def _normalize_value(value: Any, type_name: str) -> Any:
    try:
        normalized = normalize_portable_values(value)
    except ValidationError as exc:
        raise ValueError(type_name) from exc
    if not _value_matches(normalized, type_name):
        raise ValueError(type_name)
    if type_name == "float":
        number = float(normalized)
        return 0.0 if number == 0.0 else number
    return normalized


def _is_instance_reference(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == {"root_instance_id", "instance_id", "machine_id", "machine_version"}
        and all(
            isinstance(value[key], str) and value[key]
            for key in ("root_instance_id", "instance_id", "machine_id")
        )
        and isinstance(value["machine_version"], int)
        and not isinstance(value["machine_version"], bool)
        and 0 < value["machine_version"] <= _INT_MAX
    )


def _normalize_payload(declaration: dict[str, Any], payload: Any) -> dict[str, Any] | None:
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        return None
    try:
        validate_portable_values(payload)
    except ValidationError:
        return None
    if not validate_unicode(payload):
        return None
    fields = declaration.get("payload") or {}
    if set(payload) - set(fields):
        return None
    normalized: dict[str, Any] = {}
    for name, field in fields.items():
        if name in payload:
            try:
                normalized[name] = _normalize_value(payload[name], str(field["type"]))
            except ValueError:
                return None
        elif "default" in field:
            normalized[name] = copy.deepcopy(field["default"])
        elif field.get("required"):
            return None
    return normalized


def _empty_result(*, status: str, state: dict[str, Any] | None, disposition: str | None) -> Result:
    return {
        "status": status,
        "disposition": disposition,
        "state": state,
        "emissions": [],
        "fault": None,
        "rejection": None,
    }


def create(
    bundle: Bundle | BundleSource,
    machine_id: str,
    root_instance_id: str,
    creation_id: str,
    bindings: dict[str, dict[str, Any]] | None = None,
) -> Result:
    """Create and synchronously initialize one root ownership aggregate."""
    validated = _coerce_bundle(bundle)
    if (
        not isinstance(machine_id, str)
        or not isinstance(root_instance_id, str)
        or not root_instance_id
        or not isinstance(creation_id, str)
        or not creation_id
        or not validate_unicode([machine_id, root_instance_id, creation_id])
    ):
        result = _empty_result(status="rejected", state=None, disposition=None)
        result["rejection"] = {"code": "invalid_creation_request"}
        return result
    models = BundleModel(validated)
    if machine_id not in models.machines:
        result = _empty_result(status="rejected", state=None, disposition=None)
        result["rejection"] = {"code": "invalid_machine_target"}
        return result
    machine = models.machine(machine_id)
    try:
        root_bindings = _creation_bindings(machine, bindings or {})
    except ValueError:
        result = _empty_result(status="rejected", state=None, disposition=None)
        result["rejection"] = {"code": "invalid_binding"}
        return result
    root_id = _root_runtime_id(validated, machine.raw, root_instance_id)
    state: dict[str, Any] = {
        "validated_bundle_fingerprint": validated.fingerprint,
        "namespace": validated.namespace,
        "root_instance_id": root_instance_id,
        "creation_id": creation_id,
        "root_runtime_id": root_id,
        "root_machine_id": machine_id,
        "status": "running",
        "next_logical_step_sequence": 0,
        "next_output_sequence": 0,
        "runtimes": {},
        "fault": None,
    }
    execution = _Execution(validated, models, state, step_sequence=0)
    runtime = execution.new_runtime(
        machine,
        root_id,
        role="root",
        owner_runtime_id=None,
        metadata={},
    )
    cause = _cause_id(
        "root_initialization",
        root_instance_id,
        root_id,
        root_id,
        creation_id,
        0,
        machine.root.pointer,
        0,
    )
    execution.cause_id = cause
    try:
        execution.initialize_runtime(runtime, machine, root_bindings)
    except StepFault as fault:
        state["runtimes"] = {}
        diagnostic = execution.new_runtime(
            machine,
            root_id,
            role="root",
            owner_runtime_id=None,
            metadata={},
            replace=True,
        )
        execution.finalize_fault(diagnostic, fault, cause, initialization=True)
        state["status"] = "faulted"
        state["fault"] = diagnostic["fault"]
        state["next_logical_step_sequence"] = 1
        state["next_output_sequence"] = 0
        result = _empty_result(status="faulted", state=state, disposition=None)
        result["fault"] = copy.deepcopy(diagnostic["fault"])
        return result
    state["next_logical_step_sequence"] = 1
    state["status"] = state["runtimes"][root_id]["status"]
    result = _empty_result(status=state["status"], state=state, disposition=None)
    result["emissions"] = execution.emissions
    return result


def dispatch(
    bundle: Bundle | BundleSource,
    prior_state: dict[str, Any],
    delivery: Delivery = None,
) -> Result:
    """Validate and process at most one envelope against an aggregate copy."""
    validated = _coerce_bundle(bundle)
    if not _valid_prior_state(prior_state, validated):
        result = _empty_result(
            status=str(prior_state.get("status", "faulted"))
            if isinstance(prior_state, dict)
            else "faulted",
            state=prior_state,
            disposition="rejected",
        )
        result["rejection"] = {"code": "invalid_prior_state"}
        return result
    if prior_state["validated_bundle_fingerprint"] != validated.fingerprint:
        result = _empty_result(
            status=prior_state["status"], state=prior_state, disposition="rejected"
        )
        result["rejection"] = {"code": "incompatible_bundle"}
        result["fault"] = copy.deepcopy(prior_state.get("fault"))
        return result
    if delivery is None:
        result = _empty_result(status=prior_state["status"], state=prior_state, disposition=None)
        result["fault"] = copy.deepcopy(prior_state.get("fault"))
        return result
    if not isinstance(delivery, dict) or set(delivery) not in ({"input"}, {"internal"}):
        return _rejected(prior_state, "invalid_event")
    mode = next(iter(delivery))
    envelope = delivery[mode]
    models = BundleModel(validated)
    delivery_mode: Literal["input", "internal"] = "input" if mode == "input" else "internal"
    rejection = _validate_envelope(validated, models, prior_state, envelope, delivery_mode)
    if rejection is not None:
        return _rejected(prior_state, rejection)
    state = _copy_normalized_prior_state(prior_state)
    step_sequence = int(state["next_logical_step_sequence"])
    execution = _Execution(validated, models, state, step_sequence=step_sequence)
    runtime = execution.runtime_for_target(envelope["target"])
    normalized_envelope = copy.deepcopy(envelope)
    declaration = execution.event_declaration(runtime, envelope["event"])
    if envelope["event"] == "env":
        runtime_root = _pointer_get(validated.raw, runtime["root_pointer"])
        external = {
            name: variable
            for name, variable in (runtime_root.get("variables") or {}).items()
            if variable.get("external") is True
        }
        normalized_envelope["payload"] = {
            "changed": {
                name: _normalize_value(value, str(external[name]["type"]))
                for name, value in envelope["payload"]["changed"].items()
            }
        }
    elif envelope["event"] in _reserved_events():
        normalized_envelope["payload"] = copy.deepcopy(envelope["payload"])
    else:
        assert declaration is not None
        normalized_envelope["payload"] = _normalize_payload(declaration, envelope.get("payload"))
    execution.event = normalized_envelope
    execution.cause_id = str(envelope["event_id"])
    before = copy.deepcopy(state)
    try:
        handled = execution.process(runtime, normalized_envelope)
    except StepFault as fault:
        state.clear()
        state.update(before)
        execution = _Execution(validated, models, state, step_sequence=step_sequence)
        runtime = execution.runtime_for_target(envelope["target"])
        execution.finalize_fault(runtime, fault, str(envelope["event_id"]))
        state["next_logical_step_sequence"] = step_sequence + 1
        if runtime["role"] == "root":
            state["status"] = "faulted"
            state["fault"] = copy.deepcopy(runtime["fault"])
        else:
            execution.emit_failure(runtime, str(envelope["event_id"]))
        result = _empty_result(status=state["status"], state=state, disposition="faulted")
        result["fault"] = copy.deepcopy(runtime["fault"])
        result["emissions"] = execution.emissions
        return result
    if not handled:
        result = _empty_result(
            status=prior_state["status"], state=prior_state, disposition="unhandled"
        )
        result["fault"] = copy.deepcopy(prior_state.get("fault"))
        return result
    state["next_logical_step_sequence"] = step_sequence + 1
    root = state["runtimes"][state["root_runtime_id"]]
    state["status"] = root["status"]
    state["fault"] = copy.deepcopy(root.get("fault"))
    result = _empty_result(status=state["status"], state=state, disposition="handled")
    result["emissions"] = execution.emissions
    result["fault"] = copy.deepcopy(root.get("fault")) if state["status"] == "faulted" else None
    return result


def _rejected(prior_state: dict[str, Any], code: str) -> Result:
    result = _empty_result(status=prior_state["status"], state=prior_state, disposition="rejected")
    result["fault"] = copy.deepcopy(prior_state.get("fault"))
    result["rejection"] = {"code": code}
    return result


def _valid_prior_state(state: Any, bundle: Bundle) -> bool:
    if not isinstance(state, dict):
        return False
    try:
        _validate_prior_state_values(state)
        return validate_unicode(state) and _validate_prior_state(state, bundle)
    except (IndexError, KeyError, TypeError, ValueError, ValidationError):
        return False


def _is_prior_counter_path(path: tuple[str | int, ...]) -> bool:
    if path in {
        ("next_logical_step_sequence",),
        ("next_output_sequence",),
        ("fault", "step_sequence"),
    }:
        return True
    if len(path) < 3 or path[0] != "runtimes" or not isinstance(path[1], str):
        return False
    suffix = path[2:]
    if suffix in {
        ("next_spawn_sequence",),
        ("component_activation_sequence",),
        ("owning_state_activation_sequence",),
        ("spawn_sequence",),
        ("fault", "step_sequence"),
        ("holder", "state_activation_sequence"),
        ("target", "component", "activation_sequence"),
    }:
        return True
    return len(suffix) == 2 and suffix[0] in {
        "next_state_activation_sequence",
        "state_activation_sequence",
        "next_component_activation_sequence",
    }


def _validate_prior_state_values(state: dict[str, Any]) -> None:
    def visit(value: Any, path: tuple[str | int, ...], ancestors: set[int]) -> None:
        if _is_prior_counter_path(path):
            if not _logical_counter(value):
                raise ValidationError("numeric_value_out_of_range")
            return
        if isinstance(value, list):
            identity = id(value)
            if identity in ancestors:
                raise ValidationError("non_json_value")
            ancestors.add(identity)
            for index, item in enumerate(value):
                visit(item, (*path, index), ancestors)
            ancestors.remove(identity)
            return
        if isinstance(value, dict):
            identity = id(value)
            if identity in ancestors:
                raise ValidationError("non_json_value")
            ancestors.add(identity)
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ValidationError("non_string_map_key")
                visit(item, (*path, key), ancestors)
            ancestors.remove(identity)
            return
        validate_portable_values(value)

    visit(state, (), set())


def _copy_normalized_prior_state(state: dict[str, Any]) -> dict[str, Any]:
    def visit(value: Any, path: tuple[str | int, ...]) -> Any:
        if _is_prior_counter_path(path):
            return value
        if isinstance(value, float):
            return 0.0 if value == 0.0 else value
        if isinstance(value, list):
            return [visit(item, (*path, index)) for index, item in enumerate(value)]
        if isinstance(value, dict):
            return {key: visit(item, (*path, key)) for key, item in value.items()}
        return value

    return cast(dict[str, Any], visit(state, ()))


def _validate_prior_state(state: dict[str, Any], bundle: Bundle) -> bool:
    required = {
        "validated_bundle_fingerprint",
        "namespace",
        "root_instance_id",
        "creation_id",
        "root_runtime_id",
        "root_machine_id",
        "status",
        "next_logical_step_sequence",
        "next_output_sequence",
        "runtimes",
        "fault",
    }
    if not required <= set(state):
        return False
    if {"queue", "timers", "dead_letters"} & set(state):
        return False
    if (
        not isinstance(state["validated_bundle_fingerprint"], str)
        or not isinstance(state["namespace"], str)
        or not isinstance(state["root_instance_id"], str)
        or not state["root_instance_id"]
        or not isinstance(state["creation_id"], str)
        or not state["creation_id"]
        or not isinstance(state["root_runtime_id"], str)
        or not isinstance(state["root_machine_id"], str)
        or state["status"] not in {"running", "completed", "faulted"}
        or not _logical_counter(state["next_logical_step_sequence"])
        or not _logical_counter(state["next_output_sequence"])
        or not isinstance(state["runtimes"], dict)
    ):
        return False
    models = BundleModel(bundle)
    if state["root_machine_id"] not in models.machines:
        return False
    runtimes = state["runtimes"]
    root = runtimes.get(state["root_runtime_id"])
    if not isinstance(root, dict) or root.get("role") != "root":
        return False
    if root.get("owner_runtime_id") is not None or root.get("status") != state["status"]:
        return False
    expected_root_id = _identity(
        [
            "determa-root-runtime-identity-2",
            "1",
            state["validated_bundle_fingerprint"],
            state["namespace"],
            root.get("machine_id"),
            str(root.get("machine_version")),
            state["root_instance_id"],
        ]
    )
    if root.get("runtime_id") != expected_root_id or root["runtime_id"] != state["root_runtime_id"]:
        return False
    if root.get("machine_id") != state["root_machine_id"]:
        return False
    if state["status"] == "faulted":
        if state["fault"] != root.get("fault"):
            return False
    elif state["fault"] is not None:
        return False

    for runtime_id, runtime in runtimes.items():
        if not isinstance(runtime_id, str) or not isinstance(runtime, dict):
            return False
        if runtime.get("runtime_id") != runtime_id:
            return False
        if runtime_id != state["root_runtime_id"] and runtime.get("role") == "root":
            return False
        if not _validate_runtime_state(state, runtime, bundle, models):
            return False

    if state["status"] == "completed" and len(runtimes) != 1:
        return False

    for runtime in runtimes.values():
        owner_id = runtime.get("owner_runtime_id")
        if runtime["role"] == "root":
            continue
        owner = runtimes.get(owner_id)
        if not isinstance(owner, dict):
            return False
        if runtime["role"] == "component":
            if owner["components"].get(runtime.get("component_id")) != runtime["runtime_id"]:
                return False
            if not _valid_component_relation(bundle, models, owner, runtime):
                return False
        elif runtime["role"] != "spawned":
            return False
        elif not _valid_spawned_relation(bundle, models, owner, runtime):
            return False
        if _ownership_cycle(runtimes, runtime):
            return False
    for owner in runtimes.values():
        expected_components = {
            child["component_id"]: child["runtime_id"]
            for child in runtimes.values()
            if child.get("role") == "component"
            and child.get("owner_runtime_id") == owner["runtime_id"]
        }
        if owner["components"] != expected_components:
            return False
    return True


def _validate_runtime_state(
    state: dict[str, Any],
    runtime: dict[str, Any],
    bundle: Bundle,
    models: BundleModel,
) -> bool:
    required = {
        "runtime_id",
        "role",
        "owner_runtime_id",
        "machine_id",
        "machine_version",
        "root_pointer",
        "status",
        "active",
        "scopes",
        "history",
        "fault",
        "next_spawn_sequence",
        "next_state_activation_sequence",
        "state_activation_sequence",
        "next_component_activation_sequence",
        "components",
    }
    if not required <= set(runtime):
        return False
    if {"queue", "timers", "dead_letters"} & set(runtime):
        return False
    if (
        runtime["role"] not in {"root", "component", "spawned"}
        or runtime["status"] not in {"running", "completed", "faulted"}
        or (runtime["role"] == "spawned" and runtime["status"] == "completed")
        or not isinstance(runtime["machine_id"], str)
        or runtime["machine_id"] not in models.machines
        or not _bounded_nonnegative_integer(runtime["machine_version"])
        or not isinstance(runtime["root_pointer"], str)
        or not isinstance(runtime["active"], list)
        or not isinstance(runtime["scopes"], dict)
        or not isinstance(runtime["history"], dict)
        or not _logical_counter(runtime["next_spawn_sequence"])
        or not _counter_map(runtime["next_state_activation_sequence"])
        or not _counter_map(runtime["state_activation_sequence"])
        or not _counter_map(runtime["next_component_activation_sequence"])
        or not isinstance(runtime["components"], dict)
    ):
        return False
    base = models.machine(runtime["machine_id"])
    if base.version != runtime["machine_version"]:
        return False
    root = _pointer_get(bundle.raw, runtime["root_pointer"])
    if not isinstance(root, dict):
        return False
    machine = (
        base
        if runtime["root_pointer"] == base.root_pointer
        else MachineModel(
            bundle,
            base.raw,
            machine_index=base.machine_index,
            root=root,
            root_pointer=runtime["root_pointer"],
            identity_machine=base.identity_machine,
        )
    )
    active = runtime["active"]
    if not all(isinstance(path, str) and path in machine.states for path in active):
        return False
    if active:
        if active[0] != "root":
            return False
        for parent_path, child_path in zip(active, active[1:], strict=False):
            if machine.states[child_path].parent is not machine.states[parent_path]:
                return False
    elif runtime["status"] == "running":
        return False
    if set(runtime["scopes"]) != set(active):
        return False
    for path, scope in runtime["scopes"].items():
        declarations = machine.states[path].raw.get("variables") or {}
        if not isinstance(scope, dict) or set(scope) != set(declarations):
            return False
        if any(
            not _value_matches(scope[name], str(declaration["type"]))
            for name, declaration in declarations.items()
        ):
            return False
    if set(runtime["state_activation_sequence"]) != set(active):
        return False
    if any(path not in machine.states for path in runtime["next_state_activation_sequence"]):
        return False
    if any(path not in machine.states for path in runtime["state_activation_sequence"]):
        return False
    if any(
        runtime["next_state_activation_sequence"].get(path, 0) <= sequence
        for path, sequence in runtime["state_activation_sequence"].items()
    ):
        return False
    history_states = {
        "$root" if node is machine.root else node.path: node
        for node in machine.states.values()
        if node.type == "composite" and node.raw.get("history", "none") != "none"
    }
    if set(runtime["history"]) != set(history_states):
        return False
    if any(
        not isinstance(key, str) or not isinstance(value, (list, type(None)))
        for key, value in runtime["history"].items()
    ):
        return False
    if any(
        value is not None
        and (len(value) != 1 or not isinstance(value[0], str) or value[0] not in machine.states)
        for value in runtime["history"].values()
    ):
        return False
    for key, value in runtime["history"].items():
        if value is None:
            continue
        history_state = history_states[key]
        destination = machine.states[value[0]]
        if not history_state.is_ancestor_of(destination, strict=True):
            return False
        if history_state.raw["history"] == "shallow" and destination.parent is not history_state:
            return False
    component_pointers = {
        f"{node.pointer}/components/{index}"
        for node in machine.states.values()
        for index, _placement in enumerate(node.raw.get("components") or [])
    }
    if set(runtime["next_component_activation_sequence"]) - component_pointers:
        return False
    if any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in runtime["components"].items()
    ):
        return False
    if runtime["fault"] is not None and not _valid_fault(
        runtime["fault"], runtime, state["next_logical_step_sequence"]
    ):
        return False
    if runtime["status"] == "faulted" and runtime["fault"] is None:
        return False
    if runtime["status"] != "faulted" and runtime["fault"] is not None:
        return False
    if runtime["status"] == "completed" and (
        runtime["active"]
        or runtime["scopes"]
        or runtime["state_activation_sequence"]
        or runtime["components"]
    ):
        return False
    if runtime["role"] == "component":
        if not _valid_component_identity(state, runtime):
            return False
    elif runtime["role"] == "spawned":
        if not _valid_spawned_identity(state, runtime):
            return False
    return True


def _valid_component_identity(state: dict[str, Any], runtime: dict[str, Any]) -> bool:
    required = {
        "component_id",
        "component_runtime_id",
        "component_definition_pointer",
        "component_declaration_index",
        "component_activation_sequence",
        "owning_state_path",
        "owning_state_activation_sequence",
        "target",
    }
    if not required <= set(runtime):
        return False
    if (
        not isinstance(runtime["component_id"], str)
        or runtime["component_runtime_id"] != runtime["runtime_id"]
        or not isinstance(runtime["component_definition_pointer"], str)
        or not _bounded_nonnegative_integer(runtime["component_declaration_index"])
        or not _logical_counter(runtime["component_activation_sequence"])
        or not isinstance(runtime["owning_state_path"], str)
        or not _logical_counter(runtime["owning_state_activation_sequence"])
    ):
        return False
    expected_id = _identity(
        [
            "determa-component-runtime-identity-1",
            "1",
            state["root_instance_id"],
            runtime["owner_runtime_id"],
            runtime["component_definition_pointer"],
            str(runtime["component_activation_sequence"]),
            state["namespace"],
            runtime["machine_id"],
            str(runtime["machine_version"]),
        ]
    )
    expected_target = {
        "component": {
            "root_instance_id": state["root_instance_id"],
            "owner_runtime_id": runtime["owner_runtime_id"],
            "component_id": runtime["component_id"],
            "component_runtime_id": runtime["runtime_id"],
            "activation_sequence": runtime["component_activation_sequence"],
        }
    }
    return bool(runtime["runtime_id"] == expected_id and runtime["target"] == expected_target)


def _valid_spawned_identity(state: dict[str, Any], runtime: dict[str, Any]) -> bool:
    if (
        not _logical_counter(runtime.get("spawn_sequence"))
        or not isinstance(runtime.get("spawn_action_pointer"), str)
        or not _is_instance_reference(runtime.get("instance_reference"))
        or runtime["instance_reference"].get("root_instance_id") != state["root_instance_id"]
        or runtime["instance_reference"].get("instance_id") != runtime["runtime_id"]
        or runtime["instance_reference"].get("machine_id") != runtime["machine_id"]
        or runtime["instance_reference"].get("machine_version") != runtime["machine_version"]
    ):
        return False
    expected_id = _identity(
        [
            "determa-spawned-runtime-identity-1",
            "1",
            state["root_instance_id"],
            runtime["owner_runtime_id"],
            runtime["spawn_action_pointer"],
            str(runtime["spawn_sequence"]),
            state["namespace"],
            runtime["machine_id"],
            str(runtime["machine_version"]),
        ]
    )
    holder = runtime.get("holder")
    if holder is not None and (
        not isinstance(holder, dict)
        or set(holder) != {"pointer", "state_path", "state_activation_sequence"}
        or not isinstance(holder["pointer"], str)
        or not isinstance(holder["state_path"], str)
        or not _logical_counter(holder["state_activation_sequence"])
    ):
        return False
    return bool(runtime["runtime_id"] == expected_id)


def _runtime_model(
    bundle: Bundle,
    models: BundleModel,
    runtime: dict[str, Any],
) -> MachineModel:
    base = models.machine(runtime["machine_id"])
    if runtime["root_pointer"] == base.root_pointer:
        return base
    root = _pointer_get(bundle.raw, runtime["root_pointer"])
    return MachineModel(
        bundle,
        base.raw,
        machine_index=base.machine_index,
        root=root,
        root_pointer=runtime["root_pointer"],
        identity_machine=base.identity_machine,
    )


def _valid_component_relation(
    bundle: Bundle,
    models: BundleModel,
    owner: dict[str, Any],
    runtime: dict[str, Any],
) -> bool:
    owner_machine = _runtime_model(bundle, models, owner)
    owning_path = runtime["owning_state_path"]
    if owning_path not in owner["active"] or owning_path not in owner_machine.states:
        return False
    owning_state = owner_machine.states[owning_path]
    if (
        owning_state.type != "parallel"
        or owner["state_activation_sequence"].get(owning_path)
        != runtime["owning_state_activation_sequence"]
    ):
        return False
    index = runtime["component_declaration_index"]
    placements = owning_state.raw.get("components") or []
    if index >= len(placements):
        return False
    placement = placements[index]
    pointer = f"{owning_state.pointer}/components/{index}"
    if (
        runtime["component_definition_pointer"] != pointer
        or placement["component_id"] != runtime["component_id"]
        or owner["next_component_activation_sequence"].get(pointer, 0)
        <= runtime["component_activation_sequence"]
    ):
        return False
    if "machine_id" in placement:
        target = models.machine(placement["machine_id"])
        return bool(
            runtime["machine_id"] == target.machine_id
            and runtime["machine_version"] == target.version
            and runtime["root_pointer"] == target.root_pointer
        )
    return bool(
        runtime["machine_id"] == owner["machine_id"]
        and runtime["machine_version"] == owner["machine_version"]
        and runtime["root_pointer"] == f"{pointer}/root"
    )


def _valid_spawned_relation(
    bundle: Bundle,
    models: BundleModel,
    owner: dict[str, Any],
    runtime: dict[str, Any],
) -> bool:
    if owner["next_spawn_sequence"] <= runtime["spawn_sequence"]:
        return False
    spawn = _pointer_get(bundle.raw, runtime["spawn_action_pointer"])
    if not isinstance(spawn, dict) or spawn.get("machine_id") != runtime["machine_id"]:
        return False
    holder = runtime.get("holder")
    if holder is None:
        return True
    owner_machine = _runtime_model(bundle, models, owner)
    state_path = holder["state_path"]
    if (
        state_path not in owner["active"]
        or state_path not in owner_machine.states
        or owner["state_activation_sequence"].get(state_path)
        != holder["state_activation_sequence"]
    ):
        return False
    state = owner_machine.states[state_path]
    prefix = f"{state.pointer}/variables/"
    if not holder["pointer"].startswith(prefix):
        return False
    declarations = state.raw.get("variables") or {}
    return any(
        holder["pointer"] == f"{prefix}{_escape_pointer(name)}"
        and declaration.get("type") == "instance_reference"
        for name, declaration in declarations.items()
    )


def _valid_fault(
    fault: Any,
    runtime: dict[str, Any],
    next_logical_step_sequence: int,
) -> bool:
    pointer_codes = {
        "guard_fault",
        "action_fault",
        "invalid_instance_target",
        "inactive_component_target",
        "binding_not_empty",
    }
    system_locators = {
        "contained_runtime_fault": "system:unhandled_contained_failure",
        "cascade_fault": "system:cascade_cleanup",
        "invariant_fault": "system:invariant",
    }
    code = fault.get("code") if isinstance(fault, dict) else None
    locator = fault.get("source_locator") if isinstance(fault, dict) else None
    valid_locator = (
        isinstance(code, str)
        and isinstance(locator, str)
        and (
            (code in pointer_codes and locator.startswith("/"))
            or system_locators.get(code) == locator
        )
    )
    return (
        isinstance(fault, dict)
        and set(fault) == {"runtime_id", "cause_id", "code", "step_sequence", "source_locator"}
        and fault["runtime_id"] == runtime["runtime_id"]
        and isinstance(fault["cause_id"], str)
        and bool(fault["cause_id"])
        and isinstance(fault["code"], str)
        and bool(fault["code"])
        and _logical_counter(fault["step_sequence"])
        and fault["step_sequence"] < next_logical_step_sequence
        and valid_locator
    )


def _bounded_nonnegative_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= _INT_MAX


def _logical_counter(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _counter_map(value: Any) -> bool:
    return isinstance(value, dict) and all(
        isinstance(key, str) and _logical_counter(counter) for key, counter in value.items()
    )


def _ownership_cycle(runtimes: dict[str, Any], runtime: dict[str, Any]) -> bool:
    seen = {runtime["runtime_id"]}
    owner_id = runtime.get("owner_runtime_id")
    while owner_id is not None:
        if owner_id in seen:
            return True
        seen.add(owner_id)
        owner = runtimes.get(owner_id)
        if not isinstance(owner, dict):
            return True
        owner_id = owner.get("owner_runtime_id")
    return False


def _creation_bindings(
    machine: MachineModel, bindings: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    if not isinstance(bindings, dict) or set(bindings) - {"input", "external"}:
        raise ValueError
    try:
        validate_portable_values(bindings)
    except ValidationError as exc:
        raise ValueError from exc
    if not validate_unicode(bindings):
        raise ValueError
    result: dict[str, dict[str, Any]] = {"input": {}, "external": {}}
    declarations = machine.root.raw.get("variables") or {}
    for kind in ("input", "external"):
        supplied = bindings.get(kind, {})
        if not isinstance(supplied, dict):
            raise ValueError
        expected = {
            name: declaration
            for name, declaration in declarations.items()
            if declaration.get(kind) is True
        }
        if set(supplied) - set(expected):
            raise ValueError
        for name, declaration in expected.items():
            if name in supplied:
                result[kind][name] = _normalize_value(supplied[name], str(declaration["type"]))
            elif "init" in declaration:
                result[kind][name] = copy.deepcopy(declaration["init"])
            else:
                raise ValueError
    return result


def _validate_envelope(
    bundle: Bundle,
    models: BundleModel,
    state: dict[str, Any],
    envelope: Any,
    mode: Literal["input", "internal"],
) -> str | None:
    del models
    if state["status"] == "faulted":
        return "invalid_instance_target"
    if not isinstance(envelope, dict):
        return "invalid_event"
    allowed_members = {"event", "event_id", "target", "payload", "correlation_id"}
    if set(envelope) - allowed_members:
        return "invalid_event"
    event = envelope.get("event")
    event_id = envelope.get("event_id")
    if (
        not isinstance(event, str)
        or not event
        or not isinstance(event_id, str)
        or not event_id
        or not validate_unicode([event, event_id])
    ):
        return "invalid_event"
    target = envelope.get("target")
    target_code, runtime = _locate_target(state, target)
    if target_code is not None:
        return target_code
    assert runtime is not None
    try:
        validate_portable_values(target)
    except ValidationError:
        return "invalid_instance_target"
    if not validate_unicode(target):
        return "invalid_instance_target"
    if runtime["status"] != "running":
        return (
            "inactive_component_target"
            if runtime["role"] == "component"
            else "invalid_instance_target"
        )
    if mode == "input" and runtime["role"] == "component":
        return "invalid_instance_target"
    machine = next(
        item for item in bundle.raw["machines"] if item["machine_id"] == runtime["machine_id"]
    )
    declarations = dict(bundle.raw.get("events") or {})
    declarations.update(machine.get("events") or {})
    if event == "env":
        if not (
            (mode == "input" and runtime["role"] in {"root", "spawned"})
            or (mode == "internal" and runtime["role"] == "component")
        ):
            return "invalid_event"
        if "correlation_id" in envelope:
            return "invalid_correlation"
        payload = envelope.get("payload")
        if not isinstance(payload, dict) or set(payload) != {"changed"}:
            return "invalid_payload"
        changed = payload["changed"]
        if not isinstance(changed, dict) or not changed:
            return "invalid_payload"
        try:
            validate_portable_values(changed)
        except ValidationError:
            return "invalid_payload"
        if not validate_unicode(changed):
            return "invalid_payload"
        runtime_root = _pointer_get(bundle.raw, runtime["root_pointer"])
        variables = runtime_root.get("variables") or {}
        external = {
            name: declaration
            for name, declaration in variables.items()
            if declaration.get("external") is True
        }
        if set(changed) - set(external):
            return "invalid_payload"
        try:
            for name, value in changed.items():
                _normalize_value(value, str(external[name]["type"]))
        except ValueError:
            return "invalid_payload"
        return None
    declaration = declarations.get(event)
    if declaration is None:
        if mode == "internal" and event in _reserved_events():
            return _validate_reserved_payload(event, envelope)
        return "invalid_event"
    expected_direction = "input" if mode == "input" else "internal"
    if declaration["direction"] != expected_direction:
        return "invalid_event"
    correlation = envelope.get("correlation_id")
    if correlation is not None and (
        not isinstance(correlation, str) or not correlation or not validate_unicode(correlation)
    ):
        return "invalid_correlation"
    if declaration.get("correlates_to") and correlation is None:
        return "invalid_correlation"
    if "payload" not in envelope:
        return "invalid_payload"
    if _normalize_payload(declaration, envelope.get("payload")) is None:
        return "invalid_payload"
    return None


def _reserved_events() -> set[str]:
    return {
        "done",
        "determa.component_completed",
        "determa.component_failed",
        "determa.spawned_instance_failed",
    }


def _validate_reserved_payload(event: str, envelope: dict[str, Any]) -> str | None:
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return "invalid_payload"
    try:
        validate_portable_values(payload)
    except ValidationError:
        return "invalid_payload"
    if not validate_unicode(payload):
        return "invalid_payload"
    if event == "determa.component_completed":
        valid = set(payload) == {"component_id", "component_runtime_id"} and all(
            isinstance(payload[name], str) and bool(payload[name])
            for name in ("component_id", "component_runtime_id")
        )
    elif event == "determa.component_failed":
        valid = (
            set(payload) == {"component_id", "component_runtime_id", "fault"}
            and all(
                isinstance(payload[name], str) and bool(payload[name])
                for name in ("component_id", "component_runtime_id")
            )
            and _valid_public_fault(payload["fault"])
        )
    elif event == "determa.spawned_instance_failed":
        valid = (
            set(payload) == {"instance", "instance_id", "machine_id", "machine_version", "fault"}
            and _is_instance_reference(payload["instance"])
            and all(
                isinstance(payload[name], str) and bool(payload[name])
                for name in ("instance_id", "machine_id")
            )
            and _bounded_nonnegative_integer(payload["machine_version"])
            and payload["machine_version"] > 0
            and payload["instance"]["instance_id"] == payload["instance_id"]
            and payload["instance"]["machine_id"] == payload["machine_id"]
            and payload["instance"]["machine_version"] == payload["machine_version"]
            and _valid_public_fault(payload["fault"])
        )
    else:
        relationship = payload.get("relationship")
        if relationship == "parallel":
            valid = set(payload) == {"relationship", "state_path", "owner_runtime_id"} and all(
                isinstance(payload[name], str) and bool(payload[name])
                for name in ("state_path", "owner_runtime_id")
            )
        elif relationship == "spawned_instance":
            valid = (
                set(payload)
                == {
                    "relationship",
                    "instance",
                    "instance_id",
                    "machine_id",
                    "machine_version",
                }
                and _is_instance_reference(payload["instance"])
                and all(
                    isinstance(payload[name], str) and bool(payload[name])
                    for name in ("instance_id", "machine_id")
                )
                and _bounded_nonnegative_integer(payload["machine_version"])
                and payload["machine_version"] > 0
                and payload["instance"]["instance_id"] == payload["instance_id"]
                and payload["instance"]["machine_id"] == payload["machine_id"]
                and payload["instance"]["machine_version"] == payload["machine_version"]
            )
        else:
            valid = False
    return None if valid else "invalid_payload"


def _valid_public_fault(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == {"runtime_id", "cause_id", "code", "step_sequence", "source_locator"}
        and all(
            isinstance(value[name], str) and bool(value[name])
            for name in ("runtime_id", "cause_id", "code", "source_locator")
        )
        and isinstance(value["step_sequence"], str)
        and (
            value["step_sequence"] == "0"
            or (
                value["step_sequence"].isdigit()
                and value["step_sequence"].isascii()
                and not value["step_sequence"].startswith("0")
            )
        )
    )


def _locate_target(state: dict[str, Any], target: Any) -> tuple[str | None, dict[str, Any] | None]:
    if not isinstance(target, dict) or len(target) != 1:
        return "invalid_instance_target", None
    runtimes = state["runtimes"]
    if "root" in target:
        value = target["root"]
        if (
            not isinstance(value, dict)
            or set(value) != {"root_instance_id", "root_runtime_id"}
            or value.get("root_instance_id") != state["root_instance_id"]
            or value.get("root_runtime_id") != state["root_runtime_id"]
        ):
            return "invalid_instance_target", None
        runtime = runtimes[state["root_runtime_id"]]
        return _target_eligibility(state, runtime), runtime
    if "spawned_instance" in target:
        reference = target["spawned_instance"]
        if not _is_instance_reference(reference):
            return "invalid_instance_target", None
        runtime = runtimes.get(reference["instance_id"])
        if runtime is None or runtime.get("instance_reference") != reference:
            return "invalid_instance_target", None
        return _target_eligibility(state, runtime), runtime
    if "component" in target:
        value = target["component"]
        if not isinstance(value, dict):
            return "inactive_component_target", None
        runtime = runtimes.get(value.get("component_runtime_id"))
        if runtime is None or runtime.get("target") != target:
            return "inactive_component_target", None
        return _target_eligibility(state, runtime), runtime
    return "invalid_instance_target", None


def _target_eligibility(state: dict[str, Any], runtime: dict[str, Any]) -> str | None:
    code = (
        "inactive_component_target"
        if runtime["role"] == "component"
        else "invalid_instance_target"
    )
    if runtime["status"] != "running":
        return code
    owner_id = runtime.get("owner_runtime_id")
    while owner_id is not None:
        owner = state["runtimes"].get(owner_id)
        if not isinstance(owner, dict):
            return code
        if owner["status"] == "faulted":
            return code
        owner_id = owner.get("owner_runtime_id")
    return None


@dataclass
class _Execution:
    bundle: Bundle
    models: BundleModel
    state: dict[str, Any]
    step_sequence: int
    emissions: list[dict[str, Any]]
    cause_id: str
    event: dict[str, Any] | None

    def __init__(
        self,
        bundle: Bundle,
        models: BundleModel,
        state: dict[str, Any],
        *,
        step_sequence: int,
    ) -> None:
        self.bundle = bundle
        self.models = models
        self.state = state
        self.step_sequence = step_sequence
        self.emissions = []
        self.cause_id = ""
        self.event = None

    def new_runtime(
        self,
        machine: MachineModel,
        runtime_id: str,
        *,
        role: str,
        owner_runtime_id: str | None,
        metadata: dict[str, Any],
        replace: bool = False,
    ) -> dict[str, Any]:
        history = {
            state.path if state is not machine.root else "$root": None
            for state in machine.states.values()
            if state.type == "composite" and state.raw.get("history", "none") != "none"
        }
        runtime: dict[str, Any] = {
            "runtime_id": runtime_id,
            "role": role,
            "owner_runtime_id": owner_runtime_id,
            "machine_id": machine.machine_id,
            "machine_version": machine.version,
            "root_pointer": machine.root_pointer,
            "status": "running",
            "active": [],
            "scopes": {},
            "history": history,
            "fault": None,
            "next_spawn_sequence": 0,
            "next_state_activation_sequence": {},
            "state_activation_sequence": {},
            "next_component_activation_sequence": {},
            "components": {},
            **copy.deepcopy(metadata),
        }
        if not replace and runtime_id in self.state["runtimes"]:
            raise StepFault("invariant_fault", "system:invariant")
        self.state["runtimes"][runtime_id] = runtime
        return runtime

    def model_for(self, runtime: dict[str, Any]) -> MachineModel:
        base = self.models.machine(runtime["machine_id"])
        if runtime["root_pointer"] == base.root_pointer:
            return base
        root = _pointer_get(self.bundle.raw, runtime["root_pointer"])
        return MachineModel(
            self.bundle,
            base.raw,
            machine_index=base.machine_index,
            root=root,
            root_pointer=runtime["root_pointer"],
            identity_machine=base.identity_machine,
        )

    def runtime_for_target(self, target: dict[str, Any]) -> dict[str, Any]:
        code, runtime = _locate_target(self.state, target)
        if code is not None or runtime is None:
            raise StepFault(code or "invalid_instance_target", "system:invariant")
        return runtime

    def event_declaration(self, runtime: dict[str, Any], event_name: str) -> dict[str, Any] | None:
        machine = self.model_for(runtime)
        declarations = dict(self.bundle.raw.get("events") or {})
        declarations.update(machine.raw.get("events") or {})
        return declarations.get(event_name)

    def initialize_runtime(
        self,
        runtime: dict[str, Any],
        machine: MachineModel,
        bindings: dict[str, dict[str, Any]],
    ) -> None:
        try:
            self.enter_state(runtime, machine, machine.root, root_bindings=bindings)
        except _StopRuntime:
            self.complete_runtime(runtime, machine)

    def enter_state(
        self,
        runtime: dict[str, Any],
        machine: MachineModel,
        state: StateNode,
        *,
        root_bindings: dict[str, dict[str, Any]] | None = None,
        descend: bool = True,
    ) -> None:
        counter = int(runtime["next_state_activation_sequence"].get(state.path, 0))
        runtime["next_state_activation_sequence"][state.path] = counter + 1
        runtime["state_activation_sequence"][state.path] = counter
        runtime["active"].append(state.path)
        runtime["scopes"][state.path] = self.initialize_variables(
            state, root_bindings if state is machine.root else None
        )
        pending_components: list[tuple[dict[str, Any], dict[str, Any], MachineModel]] = []
        if state.type == "parallel":
            pending_components = self.allocate_components(runtime, machine, state)
        self.run_actions(
            runtime,
            machine,
            state,
            state.raw.get("entry") or [],
            f"{state.pointer}/entry",
            event_visible=False,
            context="entry",
        )
        if state.type == "parallel":
            owner_snapshot = self.visible_variables(runtime, machine, state)
            for placement, child, child_machine in pending_components:
                child = self.state["runtimes"][child["runtime_id"]]
                bindings = self.evaluate_author_bindings(
                    placement.get("with") or {},
                    child_machine,
                    owner_snapshot=owner_snapshot,
                    runtime=runtime,
                    machine=machine,
                    state=state,
                    pointer=child["component_definition_pointer"],
                )
                snapshot = copy.deepcopy(self.state)
                emissions_before = len(self.emissions)
                child_cause = _cause_id(
                    "component_initialization",
                    self.state["root_instance_id"],
                    runtime["runtime_id"],
                    child["runtime_id"],
                    self.cause_id,
                    self.step_sequence,
                    child["component_definition_pointer"],
                    child["component_declaration_index"],
                )
                previous_cause = self.cause_id
                self.cause_id = child_cause
                try:
                    self.initialize_runtime(child, child_machine, bindings)
                except StepFault as fault:
                    self.restore_contained(snapshot, child["runtime_id"])
                    del self.emissions[emissions_before:]
                    child = self.state["runtimes"][child["runtime_id"]]
                    self.finalize_fault(child, fault, child_cause, initialization=True)
                    self.emit_failure(child, child_cause)
                finally:
                    self.cause_id = previous_cause
        if state.type == "final":
            self.complete_runtime(runtime, machine)
            return
        if state.type == "composite" and descend:
            initial = state.raw["initial"]
            target, history = self.resolve_compound_transition(
                runtime,
                machine,
                state,
                initial,
                f"{state.pointer}/initial",
                event_visible=False,
            )
            assert target is not None
            self.enter_path(runtime, machine, state, target, history=history)

    def initialize_variables(
        self,
        state: StateNode,
        bindings: dict[str, dict[str, Any]] | None,
    ) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for name, declaration in (state.raw.get("variables") or {}).items():
            selected = None
            if bindings is not None:
                if declaration.get("input") and name in bindings["input"]:
                    selected = bindings["input"][name]
                elif declaration.get("external") and name in bindings["external"]:
                    selected = bindings["external"][name]
            if selected is None and "init" in declaration:
                selected = declaration["init"]
            if selected is None and declaration["type"] != "instance_reference":
                raise StepFault("invariant_fault", "system:invariant")
            values[name] = copy.deepcopy(selected)
        return values

    def visible_variables(
        self, runtime: dict[str, Any], machine: MachineModel, state: StateNode
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for node in reversed(state.ancestors(include_self=True)):
            result.update(runtime["scopes"].get(node.path, {}))
        return result

    def variable_slot(
        self, runtime: dict[str, Any], state: StateNode, name: str
    ) -> tuple[str, dict[str, Any]]:
        current: StateNode | None = state
        while current is not None:
            declarations = current.raw.get("variables") or {}
            if name in declarations and current.path in runtime["scopes"]:
                return current.path, declarations[name]
            current = current.parent
        raise StepFault("invariant_fault", "system:invariant")

    def activation(
        self,
        runtime: dict[str, Any],
        machine: MachineModel,
        state: StateNode,
        *,
        event_visible: bool,
        owner_variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        activation = self.visible_variables(runtime, machine, state)
        if event_visible and self.event is not None:
            activation["event"] = {"payload": copy.deepcopy(self.event["payload"])}
        if owner_variables is not None:
            activation = {"owner": {"variables": copy.deepcopy(owner_variables)}}
        return activation

    def evaluate(
        self,
        expression: str,
        activation: dict[str, Any],
        pointer: str,
        *,
        guard: bool = False,
    ) -> Any:
        try:
            return cel.evaluate(expression, activation)
        except CelError as exc:
            raise StepFault("guard_fault" if guard else "action_fault", pointer) from exc

    def allocate_components(
        self, runtime: dict[str, Any], machine: MachineModel, state: StateNode
    ) -> list[tuple[dict[str, Any], dict[str, Any], MachineModel]]:
        result: list[tuple[dict[str, Any], dict[str, Any], MachineModel]] = []
        for index, placement in enumerate(state.raw["components"]):
            pointer = f"{state.pointer}/components/{index}"
            counter = int(runtime["next_component_activation_sequence"].get(pointer, 0))
            runtime["next_component_activation_sequence"][pointer] = counter + 1
            child_machine = (
                self.models.machine(placement["machine_id"])
                if "machine_id" in placement
                else self.models.inline_component(machine, placement, pointer)
            )
            child_id = _component_runtime_id(
                self.bundle,
                runtime["runtime_id"],
                self.state["root_instance_id"],
                pointer,
                counter,
                child_machine,
            )
            target = {
                "component": {
                    "root_instance_id": self.state["root_instance_id"],
                    "owner_runtime_id": runtime["runtime_id"],
                    "component_id": placement["component_id"],
                    "component_runtime_id": child_id,
                    "activation_sequence": counter,
                }
            }
            child = self.new_runtime(
                child_machine,
                child_id,
                role="component",
                owner_runtime_id=runtime["runtime_id"],
                metadata={
                    "component_id": placement["component_id"],
                    "component_runtime_id": child_id,
                    "component_definition_pointer": pointer,
                    "component_declaration_index": index,
                    "component_activation_sequence": counter,
                    "owning_state_path": state.path,
                    "owning_state_activation_sequence": runtime["state_activation_sequence"][
                        state.path
                    ],
                    "target": target,
                },
            )
            runtime["components"][placement["component_id"]] = child_id
            result.append((placement, child, child_machine))
        return result

    def evaluate_author_bindings(
        self,
        bindings: dict[str, Any],
        target: MachineModel,
        *,
        owner_snapshot: dict[str, Any] | None,
        runtime: dict[str, Any],
        machine: MachineModel,
        state: StateNode,
        pointer: str,
    ) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {"input": {}, "external": {}}
        declarations = target.root.raw.get("variables") or {}
        activation = self.activation(
            runtime,
            machine,
            state,
            event_visible=self.event is not None,
            owner_variables=owner_snapshot,
        )
        for kind in ("input", "external"):
            supplied = bindings.get(kind) or {}
            for name in sorted(supplied, key=lambda item: item.encode("utf-8")):
                expression = supplied[name]
                value = self.evaluate(
                    expression, activation, f"{pointer}/with/{kind}/{_escape_pointer(name)}"
                )
                declaration = declarations[name]
                try:
                    result[kind][name] = _normalize_value(value, str(declaration["type"]))
                except ValueError as exc:
                    raise StepFault(
                        "action_fault", f"{pointer}/with/{kind}/{_escape_pointer(name)}"
                    ) from exc
            for name, declaration in declarations.items():
                if declaration.get(kind) and name not in result[kind]:
                    result[kind][name] = copy.deepcopy(declaration["init"])
        return result

    def process(self, runtime: dict[str, Any], envelope: dict[str, Any]) -> bool:
        machine = self.model_for(runtime)
        active = machine.states[runtime["active"][-1]] if runtime["active"] else machine.root
        selected: tuple[StateNode, dict[str, Any], str] | None = None
        current: StateNode | None = active
        while current is not None:
            transition_or_list = (current.raw.get("on_events") or {}).get(envelope["event"])
            if transition_or_list is not None:
                transitions = (
                    transition_or_list
                    if isinstance(transition_or_list, list)
                    else [transition_or_list]
                )
                for index, transition in enumerate(transitions):
                    pointer = (
                        f"{current.pointer}/on_events/{_escape_pointer(envelope['event'])}"
                        + (f"/{index}" if isinstance(transition_or_list, list) else "")
                    )
                    guard = transition.get("guard")
                    if guard is None:
                        selected = (current, transition, pointer)
                        break
                    value = self.evaluate(
                        guard,
                        self.activation(runtime, machine, current, event_visible=True),
                        f"{pointer}/guard",
                        guard=True,
                    )
                    if value is True:
                        selected = (current, transition, pointer)
                        break
                if selected is not None:
                    break
            current = current.parent
        if selected is None:
            if envelope["event"] in {
                "determa.component_failed",
                "determa.spawned_instance_failed",
            }:
                raise StepFault("contained_runtime_fault", "system:unhandled_contained_failure")
            return False
        source, transition, pointer = selected
        try:
            target, history = self.resolve_compound_transition(
                runtime,
                machine,
                source,
                transition,
                pointer,
                event_visible=True,
            )
            if target is None:
                return True
            self.apply_transition(
                runtime,
                machine,
                source,
                target,
                local=transition.get("local") is True,
                history=history,
            )
        except _StopRuntime:
            self.complete_runtime(runtime, machine)
        return True

    def resolve_compound_transition(
        self,
        runtime: dict[str, Any],
        machine: MachineModel,
        source: StateNode,
        transition: dict[str, Any],
        pointer: str,
        *,
        event_visible: bool,
    ) -> tuple[StateNode | None, bool]:
        self.run_actions(
            runtime,
            machine,
            source,
            transition.get("action") or [],
            f"{pointer}/action",
            event_visible=event_visible,
            context="transition",
        )
        target_spec = transition.get("transition_to")
        if target_spec is None:
            return None, False
        history = isinstance(target_spec, dict)
        target = machine.resolve(target_spec, source)
        seen: set[str] = set()
        while target.is_choice:
            if target.path in seen:
                raise StepFault("invariant_fault", "system:invariant")
            seen.add(target.path)
            branch_selected = None
            for index, branch in enumerate(target.raw["choice"]):
                guard = branch.get("guard")
                branch_pointer = f"{target.pointer}/choice/{index}"
                if (
                    guard is None
                    or self.evaluate(
                        guard,
                        self.activation(runtime, machine, source, event_visible=False),
                        f"{branch_pointer}/guard",
                        guard=True,
                    )
                    is True
                ):
                    branch_selected = (branch, branch_pointer)
                    break
            if branch_selected is None:
                raise StepFault("invariant_fault", "system:invariant")
            branch, branch_pointer = branch_selected
            self.run_actions(
                runtime,
                machine,
                source,
                branch.get("action") or [],
                f"{branch_pointer}/action",
                event_visible=False,
                context="choice",
            )
            target_spec = branch["transition_to"]
            history = isinstance(target_spec, dict)
            target = machine.resolve(target_spec, target)
        return target, history

    def run_actions(
        self,
        runtime: dict[str, Any],
        machine: MachineModel,
        state: StateNode,
        actions: list[dict[str, Any]],
        pointer: str,
        *,
        event_visible: bool,
        context: str,
    ) -> None:
        for index, action in enumerate(actions):
            action_pointer = f"{pointer}/{index}"
            if "assign" in action:
                name, expression = next(iter(action["assign"].items()))
                value = self.evaluate(
                    expression,
                    self.activation(runtime, machine, state, event_visible=event_visible),
                    f"{action_pointer}/assign/{_escape_pointer(name)}",
                )
                scope_path, declaration = self.variable_slot(runtime, state, name)
                try:
                    runtime["scopes"][scope_path][name] = _normalize_value(
                        value, str(declaration["type"])
                    )
                except ValueError as exc:
                    raise StepFault(
                        "action_fault",
                        f"{action_pointer}/assign/{_escape_pointer(name)}",
                    ) from exc
            elif "send" in action:
                self.send(
                    runtime,
                    machine,
                    state,
                    action["send"],
                    f"{action_pointer}/send",
                    event_visible=event_visible,
                )
            elif "refresh" in action:
                self.refresh(runtime, machine, state, action["refresh"], action_pointer)
            elif "spawn" in action:
                self.spawn(
                    runtime,
                    machine,
                    state,
                    action["spawn"],
                    f"{action_pointer}/spawn",
                )
            elif "cancel" in action:
                expression = action["cancel"]["instance"]
                reference = self.evaluate(
                    expression,
                    self.activation(runtime, machine, state, event_visible=event_visible),
                    f"{action_pointer}/cancel/instance",
                )
                self.cancel(runtime, reference)
            elif "stop" in action:
                raise _StopRuntime

    def send(
        self,
        runtime: dict[str, Any],
        machine: MachineModel,
        state: StateNode,
        send: dict[str, Any],
        pointer: str,
        *,
        event_visible: bool,
    ) -> None:
        activation = self.activation(runtime, machine, state, event_visible=event_visible)
        declaration = self.event_declaration(runtime, send["event"])
        payload_values: dict[str, Any] = {}
        payload_expressions = send.get("payload") or {}
        normalized_payload: dict[str, Any]
        if send["event"] == "env":
            changed_expression = payload_expressions["changed"]
            payload_values["changed"] = self.evaluate(
                changed_expression, activation, f"{pointer}/payload/changed"
            )
        else:
            assert declaration is not None
            for name in sorted(payload_expressions, key=lambda item: item.encode("utf-8")):
                payload_values[name] = self.evaluate(
                    payload_expressions[name],
                    activation,
                    f"{pointer}/payload/{_escape_pointer(name)}",
                )
        correlation = None
        if "correlation_id" in send:
            correlation = self.evaluate(
                send["correlation_id"], activation, f"{pointer}/correlation_id"
            )
        target_specs = send.get("targets") or [send.get("to", {"self": True})]
        evaluated_targets: list[tuple[dict[str, Any], Any]] = []
        for index, target_spec in enumerate(target_specs):
            value = None
            if "instance" in target_spec:
                suffix = f"/targets/{index}/instance" if "targets" in send else "/to/instance"
                value = self.evaluate(target_spec["instance"], activation, f"{pointer}{suffix}")
            evaluated_targets.append((target_spec, value))
        if send["event"] == "env":
            if not isinstance(payload_values["changed"], dict):
                raise StepFault("action_fault", f"{pointer}/payload/changed")
            normalized_payload = {"changed": copy.deepcopy(payload_values["changed"])}
        else:
            assert declaration is not None
            payload_result = _normalize_payload(declaration, payload_values)
            if payload_result is None:
                supplied = sorted(payload_values, key=lambda item: item.encode("utf-8"))
                locator = (
                    f"{pointer}/payload/{_escape_pointer(supplied[0])}"
                    if supplied
                    else f"{pointer}/payload"
                )
                raise StepFault("action_fault", locator)
            normalized_payload = payload_result
        resolved = [
            self.resolve_send_target(runtime, target_spec, value, pointer, index, "targets" in send)
            for index, (target_spec, value) in enumerate(evaluated_targets)
        ]
        for index, target in enumerate(resolved):
            if target == "external":
                sequence = int(self.state["next_output_sequence"])
                self.state["next_output_sequence"] = sequence + 1
                emission = {
                    "event": send["event"],
                    "target": "external",
                    "payload": copy.deepcopy(normalized_payload),
                    "correlation_id": correlation,
                    "effect_id": _effect_id(
                        machine,
                        self.state["root_instance_id"],
                        runtime["runtime_id"],
                        self.cause_id,
                        self.step_sequence,
                        pointer,
                        index,
                    ),
                    "sequence": sequence,
                }
            else:
                assert isinstance(target, dict)
                target_runtime_id = _target_runtime_id(target)
                emission = {
                    "event": send["event"],
                    "event_id": _event_id(
                        self.state["root_instance_id"],
                        runtime["runtime_id"],
                        target_runtime_id,
                        self.cause_id,
                        self.step_sequence,
                        pointer,
                        index,
                    ),
                    "target": copy.deepcopy(target),
                    "payload": copy.deepcopy(normalized_payload),
                }
                if correlation is not None:
                    emission["correlation_id"] = correlation
            self.emissions.append(emission)

    def resolve_send_target(
        self,
        runtime: dict[str, Any],
        target_spec: dict[str, Any],
        evaluated: Any,
        pointer: str,
        index: int,
        target_list: bool,
    ) -> dict[str, Any] | str:
        suffix = f"/targets/{index}" if target_list else "/to"
        if target_spec.get("self") is True:
            return self.target_for(runtime)
        if target_spec.get("owner") is True:
            owner_id = runtime.get("owner_runtime_id")
            if owner_id is None or owner_id not in self.state["runtimes"]:
                raise StepFault("invalid_instance_target", f"{pointer}{suffix}")
            return self.target_for(self.state["runtimes"][owner_id])
        if "component" in target_spec:
            child_id = runtime["components"].get(target_spec["component"])
            child = self.state["runtimes"].get(child_id)
            if child is None or _target_eligibility(self.state, child) is not None:
                raise StepFault("inactive_component_target", f"{pointer}{suffix}")
            return cast(dict[str, Any], copy.deepcopy(child["target"]))
        if "instance" in target_spec:
            if not _is_instance_reference(evaluated):
                raise StepFault("invalid_instance_target", f"{pointer}{suffix}/instance")
            child = self.state["runtimes"].get(evaluated["instance_id"])
            if child is None or _target_eligibility(self.state, child) is not None:
                raise StepFault("invalid_instance_target", f"{pointer}{suffix}/instance")
            return {"spawned_instance": copy.deepcopy(evaluated)}
        if target_spec.get("external") is True:
            return "external"
        raise StepFault("invalid_instance_target", f"{pointer}{suffix}")

    def target_for(self, runtime: dict[str, Any]) -> dict[str, Any]:
        if runtime["role"] == "root":
            return {
                "root": {
                    "root_instance_id": self.state["root_instance_id"],
                    "root_runtime_id": runtime["runtime_id"],
                }
            }
        if runtime["role"] == "component":
            return copy.deepcopy(runtime["target"])
        return {"spawned_instance": copy.deepcopy(runtime["instance_reference"])}

    def refresh(
        self,
        runtime: dict[str, Any],
        machine: MachineModel,
        state: StateNode,
        refresh: dict[str, Any],
        pointer: str,
    ) -> None:
        assert self.event is not None
        changed = self.event["payload"]["changed"]
        selected = refresh.get("only", list(changed))
        for index, name in enumerate(selected):
            if name not in changed:
                raise StepFault("action_fault", f"{pointer}/refresh/only/{index}")
        for name in selected:
            scope_path, declaration = self.variable_slot(runtime, state, name)
            runtime["scopes"][scope_path][name] = _normalize_value(
                changed[name], str(declaration["type"])
            )

    def spawn(
        self,
        runtime: dict[str, Any],
        machine: MachineModel,
        state: StateNode,
        spawn: dict[str, Any],
        pointer: str,
    ) -> None:
        sequence = int(runtime["next_spawn_sequence"])
        runtime["next_spawn_sequence"] = sequence + 1
        child_machine = self.models.machine(spawn["machine_id"])
        child_id = _spawned_runtime_id(
            self.bundle,
            runtime["runtime_id"],
            self.state["root_instance_id"],
            pointer,
            sequence,
            child_machine,
        )
        bindings = self.evaluate_author_bindings(
            spawn.get("bindings") or {},
            child_machine,
            owner_snapshot=None,
            runtime=runtime,
            machine=machine,
            state=state,
            pointer=pointer,
        )
        reference = {
            "root_instance_id": self.state["root_instance_id"],
            "instance_id": child_id,
            "machine_id": child_machine.machine_id,
            "machine_version": child_machine.version,
        }
        holder = None
        if "bind_to" in spawn:
            name = spawn["bind_to"]
            scope_path, declaration = self.variable_slot(runtime, state, name)
            if runtime["scopes"][scope_path][name] is not None:
                raise StepFault("binding_not_empty", f"{pointer}/bind_to")
            runtime["scopes"][scope_path][name] = copy.deepcopy(reference)
            holder_state = machine.states[scope_path]
            holder = {
                "pointer": f"{holder_state.pointer}/variables/{_escape_pointer(name)}",
                "state_path": scope_path,
                "state_activation_sequence": runtime["state_activation_sequence"][scope_path],
            }
            del declaration
        child = self.new_runtime(
            child_machine,
            child_id,
            role="spawned",
            owner_runtime_id=runtime["runtime_id"],
            metadata={
                "spawn_sequence": sequence,
                "spawn_action_pointer": pointer,
                "instance_reference": reference,
                "holder": holder,
            },
        )
        snapshot = copy.deepcopy(self.state)
        emissions_before = len(self.emissions)
        child_cause = _cause_id(
            "spawned_initialization",
            self.state["root_instance_id"],
            runtime["runtime_id"],
            child_id,
            self.cause_id,
            self.step_sequence,
            pointer,
            sequence,
        )
        previous_cause = self.cause_id
        self.cause_id = child_cause
        try:
            self.initialize_runtime(child, child_machine, bindings)
        except StepFault as fault:
            self.restore_contained(snapshot, child_id)
            del self.emissions[emissions_before:]
            child = self.state["runtimes"][child_id]
            self.finalize_fault(child, fault, child_cause, initialization=True)
            self.emit_failure(child, child_cause)
        finally:
            self.cause_id = previous_cause

    def restore_contained(self, snapshot: dict[str, Any], child_runtime_id: str) -> None:
        """Roll back one contained initialization without replacing its owner object."""
        current_runtimes = self.state["runtimes"]
        snapshot_runtimes = snapshot["runtimes"]
        for runtime_id in list(current_runtimes):
            if runtime_id not in snapshot_runtimes:
                current_runtimes.pop(runtime_id)
        child = current_runtimes[child_runtime_id]
        child.clear()
        child.update(copy.deepcopy(snapshot_runtimes[child_runtime_id]))
        self.state["next_output_sequence"] = snapshot["next_output_sequence"]

    def cancel(self, runtime: dict[str, Any], reference: Any) -> None:
        if not _is_instance_reference(reference):
            return
        child = self.state["runtimes"].get(reference["instance_id"])
        if (
            child is None
            or child["role"] != "spawned"
            or not self.owns_descendant(runtime, child)
        ):
            return
        self.cleanup_descendant(child)

    def owns_descendant(
        self,
        runtime: dict[str, Any],
        descendant: dict[str, Any],
    ) -> bool:
        owner_id = descendant.get("owner_runtime_id")
        while owner_id is not None:
            if owner_id == runtime["runtime_id"]:
                return True
            owner = self.state["runtimes"].get(owner_id)
            if not isinstance(owner, dict):
                return False
            owner_id = owner.get("owner_runtime_id")
        return False

    def apply_transition(
        self,
        runtime: dict[str, Any],
        machine: MachineModel,
        source: StateNode,
        target: StateNode,
        *,
        local: bool,
        history: bool,
    ) -> None:
        boundary = _boundary(machine, source, target, local)
        exit_nodes = [
            machine.states[path]
            for path in reversed(runtime["active"])
            if machine.states[path] is not boundary
            and boundary.is_ancestor_of(machine.states[path], strict=True)
        ]
        leaf = machine.states[runtime["active"][-1]]
        for node in exit_nodes:
            if node.type == "composite" and node.raw.get("history", "none") != "none":
                key = "$root" if node is machine.root else node.path
                if node.raw["history"] == "shallow":
                    direct = leaf
                    while direct.parent is not node:
                        assert direct.parent is not None
                        direct = direct.parent
                    runtime["history"][key] = [direct.path]
                else:
                    runtime["history"][key] = [leaf.path]
        for node in exit_nodes:
            self.exit_state(runtime, machine, node)
        if target is boundary:
            if target.type == "composite":
                self.descend_composite(runtime, machine, target, history=history)
            return
        self.enter_path(runtime, machine, boundary, target, history=history)

    def enter_path(
        self,
        runtime: dict[str, Any],
        machine: MachineModel,
        boundary: StateNode,
        target: StateNode,
        *,
        history: bool,
    ) -> None:
        path: list[StateNode] = []
        current = target
        while current is not boundary:
            path.append(current)
            assert current.parent is not None
            current = current.parent
        for node in reversed(path):
            self.enter_state(runtime, machine, node, descend=False)
            if runtime["status"] != "running":
                return
        if target.type == "composite" and target.path in runtime["active"]:
            self.descend_composite(runtime, machine, target, history=history)

    def descend_composite(
        self,
        runtime: dict[str, Any],
        machine: MachineModel,
        state: StateNode,
        *,
        history: bool,
    ) -> None:
        if history:
            key = "$root" if state is machine.root else state.path
            record = runtime["history"].get(key)
            if record:
                destination = machine.states[record[0]]
                if state.raw["history"] == "shallow":
                    self.enter_path(runtime, machine, state, destination, history=False)
                else:
                    self.enter_path(runtime, machine, state, destination, history=False)
                return
        initial = state.raw["initial"]
        target, target_history = self.resolve_compound_transition(
            runtime,
            machine,
            state,
            initial,
            f"{state.pointer}/initial",
            event_visible=False,
        )
        assert target is not None
        self.enter_path(runtime, machine, state, target, history=target_history)

    def exit_state(self, runtime: dict[str, Any], machine: MachineModel, state: StateNode) -> None:
        self.cleanup_state_children(runtime, state)
        self.run_actions(
            runtime,
            machine,
            state,
            state.raw.get("exit") or [],
            f"{state.pointer}/exit",
            event_visible=False,
            context="exit",
        )
        runtime["scopes"].pop(state.path, None)
        runtime["state_activation_sequence"].pop(state.path, None)
        if state.path in runtime["active"]:
            runtime["active"].remove(state.path)

    def cleanup_state_children(self, runtime: dict[str, Any], state: StateNode) -> None:
        activation_sequence = runtime["state_activation_sequence"].get(state.path)

        def selected(child: dict[str, Any]) -> bool:
            if child["role"] == "component":
                return child.get("owning_state_path") == state.path
            holder = child.get("holder")
            return bool(
                holder is not None
                and holder["state_path"] == state.path
                and holder["state_activation_sequence"] == activation_sequence
            )

        for child in self.ordered_children(runtime, selected):
            self.cleanup_descendant(child)

    def ordered_children(
        self,
        runtime: dict[str, Any],
        selected: Callable[[dict[str, Any]], bool] | None = None,
    ) -> list[dict[str, Any]]:
        children = [
            child
            for child in list(self.state["runtimes"].values())
            if child.get("owner_runtime_id") == runtime["runtime_id"]
            and (selected is None or selected(child))
        ]
        components = sorted(
            (child for child in children if child["role"] == "component"),
            key=_component_cleanup_key,
            reverse=True,
        )
        spawned = sorted(
            (child for child in children if child["role"] == "spawned"),
            key=_spawn_cleanup_key,
        )
        return [*components, *spawned]

    def cleanup_descendant(
        self,
        runtime: dict[str, Any],
        *,
        frozen: bool = False,
    ) -> None:
        try:
            self.cleanup_runtime(runtime, dispose=True, frozen=frozen)
        except StepFault as exc:
            raise StepFault("cascade_fault", "system:cascade_cleanup") from exc

    def cleanup_runtime(
        self,
        runtime: dict[str, Any],
        *,
        dispose: bool,
        frozen: bool = False,
    ) -> None:
        machine = self.model_for(runtime)
        frozen = frozen or runtime["status"] == "faulted"
        for child in self.ordered_children(runtime):
            self.cleanup_descendant(child, frozen=frozen)
        if runtime["status"] == "running" and not frozen:
            for path in list(reversed(runtime["active"])):
                self.exit_state(runtime, machine, machine.states[path])
        if dispose:
            owner_id = runtime.get("owner_runtime_id")
            if runtime["role"] == "component" and owner_id in self.state["runtimes"]:
                owner = self.state["runtimes"][owner_id]
                if owner["components"].get(runtime["component_id"]) == runtime["runtime_id"]:
                    owner["components"].pop(runtime["component_id"], None)
            self.state["runtimes"].pop(runtime["runtime_id"], None)

    def complete_runtime(self, runtime: dict[str, Any], machine: MachineModel) -> None:
        if runtime["status"] != "running":
            return
        for child in self.ordered_children(runtime):
            self.cleanup_descendant(child)
        for path in list(reversed(runtime["active"])):
            self.exit_state(runtime, machine, machine.states[path])
        runtime["status"] = "completed"
        if runtime["role"] == "root":
            self.state["status"] = "completed"
            return
        if runtime["role"] == "component":
            self.emit_component_completion(runtime)
            return
        self.emit_spawned_completion(runtime)
        self.state["runtimes"].pop(runtime["runtime_id"], None)

    def emit_component_completion(self, runtime: dict[str, Any]) -> None:
        owner = self.state["runtimes"][runtime["owner_runtime_id"]]
        payload = {
            "component_id": runtime["component_id"],
            "component_runtime_id": runtime["runtime_id"],
        }
        self.emit_internal_system(
            runtime,
            owner,
            "determa.component_completed",
            payload,
            "system:component_completion",
        )
        component_ids = owner["components"].values()
        if component_ids and all(
            self.state["runtimes"][runtime_id]["status"] == "completed"
            for runtime_id in component_ids
        ):
            payload = {
                "relationship": "parallel",
                "state_path": runtime["owning_state_path"],
                "owner_runtime_id": owner["runtime_id"],
            }
            self.emit_internal_system(
                owner, owner, "done", payload, "system:component_completion", ordinal=1
            )

    def emit_spawned_completion(self, runtime: dict[str, Any]) -> None:
        owner = self.state["runtimes"][runtime["owner_runtime_id"]]
        payload = {
            "relationship": "spawned_instance",
            "instance": copy.deepcopy(runtime["instance_reference"]),
            "instance_id": runtime["runtime_id"],
            "machine_id": runtime["machine_id"],
            "machine_version": runtime["machine_version"],
        }
        self.emit_internal_system(runtime, owner, "done", payload, "system:spawned_completion")

    def emit_internal_system(
        self,
        source: dict[str, Any],
        target_runtime: dict[str, Any],
        event: str,
        payload: dict[str, Any],
        locator: str,
        *,
        ordinal: int = 0,
    ) -> None:
        target = self.target_for(target_runtime)
        event_id = _event_id(
            self.state["root_instance_id"],
            source["runtime_id"],
            target_runtime["runtime_id"],
            self.cause_id,
            self.step_sequence,
            locator,
            ordinal,
        )
        self.emissions.append(
            {
                "event": event,
                "event_id": event_id,
                "target": target,
                "payload": copy.deepcopy(payload),
            }
        )

    def finalize_fault(
        self,
        runtime: dict[str, Any],
        fault: StepFault,
        cause_id: str,
        *,
        initialization: bool = False,
    ) -> None:
        runtime["status"] = "faulted"
        if initialization:
            runtime["active"] = []
            runtime["scopes"] = {}
            runtime["history"] = {}
            runtime["components"] = {}
            runtime["next_spawn_sequence"] = 0
            runtime["next_state_activation_sequence"] = {}
            runtime["state_activation_sequence"] = {}
            runtime["next_component_activation_sequence"] = {}
        runtime["fault"] = {
            "runtime_id": runtime["runtime_id"],
            "cause_id": cause_id,
            "code": fault.code,
            "step_sequence": self.step_sequence,
            "source_locator": fault.source_locator,
        }

    def emit_failure(self, runtime: dict[str, Any], cause_id: str) -> None:
        owner = self.state["runtimes"][runtime["owner_runtime_id"]]
        public_fault = copy.deepcopy(runtime["fault"])
        public_fault["step_sequence"] = str(public_fault["step_sequence"])
        if runtime["role"] == "component":
            event = "determa.component_failed"
            payload = {
                "component_id": runtime["component_id"],
                "component_runtime_id": runtime["runtime_id"],
                "fault": public_fault,
            }
            locator = "system:component_failure"
        else:
            event = "determa.spawned_instance_failed"
            payload = {
                "instance": copy.deepcopy(runtime["instance_reference"]),
                "instance_id": runtime["runtime_id"],
                "machine_id": runtime["machine_id"],
                "machine_version": runtime["machine_version"],
                "fault": public_fault,
            }
            locator = "system:spawned_failure"
        previous = self.cause_id
        self.cause_id = cause_id
        self.emit_internal_system(runtime, owner, event, payload, locator)
        self.cause_id = previous


def _boundary(
    machine: MachineModel, source: StateNode, target: StateNode, local: bool
) -> StateNode:
    if source is target:
        assert source.parent is not None
        return source.parent
    if source.is_ancestor_of(target, strict=True):
        if local or source is machine.root:
            return source
        assert source.parent is not None
        return source.parent
    if target.is_ancestor_of(source, strict=True):
        return target
    target_paths = {node.path: node for node in target.ancestors(include_self=True)}
    for node in source.ancestors(include_self=True):
        if node.path in target_paths:
            return node
    return machine.root


def _target_runtime_id(target: dict[str, Any]) -> str:
    if "root" in target:
        return str(target["root"]["root_runtime_id"])
    if "component" in target:
        return str(target["component"]["component_runtime_id"])
    return str(target["spawned_instance"]["instance_id"])


def _spawn_cleanup_key(runtime: dict[str, Any]) -> tuple[int, bytes, int, int]:
    holder = runtime.get("holder")
    if holder is None:
        return (1, b"", 0, int(runtime["spawn_sequence"]))
    return (
        0,
        holder["pointer"].encode("utf-8"),
        int(holder["state_activation_sequence"]),
        int(runtime["spawn_sequence"]),
    )


def _component_cleanup_key(runtime: dict[str, Any]) -> tuple[bytes, int, int, int]:
    owning_state_pointer = runtime["component_definition_pointer"].rsplit("/components/", 1)[0]
    return (
        owning_state_pointer.encode("utf-8"),
        int(runtime["owning_state_activation_sequence"]),
        int(runtime["component_declaration_index"]),
        int(runtime["component_activation_sequence"]),
    )


def _pointer_get(document: dict[str, Any], pointer: str) -> dict[str, Any]:
    current: Any = document
    for part in pointer.split("/")[1:]:
        key = part.replace("~1", "/").replace("~0", "~")
        current = current[int(key)] if isinstance(current, list) else current[key]
    return cast(dict[str, Any], current)
