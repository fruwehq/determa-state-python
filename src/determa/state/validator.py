"""Structural and semantic validation for Determa State format 1."""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

from . import cel
from .definition import Bundle, _escape_pointer, normalize_bundle
from .errors import CelError, ErrorRecord, ValidationError
from .model import BundleModel, MachineModel, StateNode

_SCHEMA_PATH = Path(__file__).parent / "data" / "machine.schema.json"
_RESERVED_EVENTS = frozenset(
    {
        "env",
        "done",
        "determa.component_completed",
        "determa.component_failed",
        "determa.spawned_instance_failed",
    }
)


@lru_cache(maxsize=1)
def schema() -> dict[str, Any]:
    with _SCHEMA_PATH.open(encoding="utf-8") as source:
        return cast(dict[str, Any], json.load(source))


def validate(document: dict[str, Any]) -> None:
    """Validate one parsed bundle or raise the first exact load-layer code."""
    _validate_schema(document)
    normalized = normalize_bundle(document)
    provisional = Bundle(raw=normalized, fingerprint="")
    model = BundleModel(provisional)
    _validate_semantics(provisional, model)


def collect_errors(document: dict[str, Any]) -> list[ErrorRecord]:
    try:
        validate(document)
    except ValidationError as exc:
        return exc.errors
    return []


def _validate_schema(document: dict[str, Any]) -> None:
    import jsonschema

    validator = jsonschema.Draft202012Validator(schema())
    errors = sorted(validator.iter_errors(document), key=lambda error: list(error.absolute_path))
    if errors:
        error = errors[0]
        path = "/" + "/".join(str(part) for part in error.absolute_path)
        raise ValidationError("structural_validation", path=path, message=error.message)


def _compatible(actual: str, expected: str) -> bool:
    return actual == expected or (expected == "float" and actual == "int") or actual == "unknown"


def _literal_matches(value: Any, expected: str) -> bool:
    if expected == "string":
        return isinstance(value, str)
    if expected == "bool":
        return isinstance(value, bool)
    if expected == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "float":
        return isinstance(value, int | float) and not isinstance(value, bool)
    if expected == "list":
        return isinstance(value, list)
    if expected == "map":
        return isinstance(value, dict)
    if expected == "instance_reference":
        return value is None
    return False


def _event_declarations(bundle: Bundle, machine: MachineModel) -> dict[str, dict[str, Any]]:
    declarations = dict(bundle.raw.get("events") or {})
    declarations.update(machine.raw.get("events") or {})
    return declarations


def _built_in_event_fields(event_name: str) -> dict[str, str]:
    if event_name == "env":
        return {"changed": "map"}
    if event_name == "determa.component_completed":
        return {"component_id": "string", "component_runtime_id": "string"}
    if event_name == "determa.component_failed":
        return {"component_id": "string", "component_runtime_id": "string", "fault": "map"}
    if event_name == "determa.spawned_instance_failed":
        return {
            "instance": "instance_reference",
            "instance_id": "string",
            "machine_id": "string",
            "machine_version": "int",
            "fault": "map",
        }
    if event_name == "done":
        return {
            "relationship": "string",
            "state_path": "string",
            "owner_runtime_id": "string",
            "instance": "instance_reference",
            "instance_id": "string",
            "machine_id": "string",
            "machine_version": "int",
        }
    return {}


def _payload_types(declaration: dict[str, Any] | None, event_name: str) -> dict[str, str]:
    if declaration is None:
        return _built_in_event_fields(event_name)
    return {name: str(field["type"]) for name, field in (declaration.get("payload") or {}).items()}


def _scope(
    machine: MachineModel, state: StateNode
) -> tuple[dict[str, str], dict[str, tuple[StateNode, dict[str, Any], str]]]:
    chain = list(reversed(state.ancestors(include_self=True)))
    types: dict[str, str] = {}
    declarations: dict[str, tuple[StateNode, dict[str, Any], str]] = {}
    for node in chain:
        for name, declaration in (node.raw.get("variables") or {}).items():
            types[name] = str(declaration["type"])
            declarations[name] = (
                node,
                declaration,
                f"{node.pointer}/variables/{_escape_pointer(name)}",
            )
    return types, declarations


def _check_expression(
    expression: str,
    *,
    scope: dict[str, str],
    expected: str | None,
    event_fields: dict[str, str] | None,
    owner_fields: dict[str, str] | None,
    allow_event: bool,
    allow_owner: bool,
) -> str:
    references = cel.referenced_names(expression)
    allowed = set(scope)
    if allow_event:
        allowed.add("event")
    if allow_owner:
        allowed.add("owner")
    instance_names = {
        name for name, type_name in scope.items() if type_name == "instance_reference"
    }
    try:
        cel.compile_expression(expression)
    except CelError as exc:
        raise ValidationError("semantic_validation", message=str(exc)) from exc
    if cel.profile_error(expression, instance_names):
        raise ValidationError("cel_profile_error")
    if references - allowed:
        raise ValidationError("semantic_validation", message="unknown CEL activation name")
    if not allow_event and re.search(r"\bevent\b", expression):
        raise ValidationError("semantic_validation")
    if not allow_owner and re.search(r"\bowner\b", expression):
        raise ValidationError("semantic_validation")
    if re.search(r"\bevent\.(?!payload\b)", expression):
        raise ValidationError("semantic_validation")
    if re.search(r"\bowner\.(?!variables\b)", expression):
        raise ValidationError("semantic_validation")
    for field in re.findall(r"\bevent\.payload\.([A-Za-z_][A-Za-z0-9_]*)", expression):
        if event_fields is None or field not in event_fields:
            raise ValidationError("semantic_validation")
    for field in re.findall(r"\bowner\.variables\.([A-Za-z_][A-Za-z0-9_]*)", expression):
        if owner_fields is None or field not in owner_fields:
            raise ValidationError("semantic_validation")
    inferred = cel.infer_type(
        expression, scope, event_fields=event_fields, owner_fields=owner_fields
    )
    if expected is not None and not _compatible(inferred, expected):
        raise ValidationError("semantic_validation")
    return inferred


def _validate_semantics(bundle: Bundle, model: BundleModel) -> None:
    if not isinstance(bundle.raw.get("format"), int) or isinstance(bundle.raw.get("format"), bool):
        raise ValidationError("unsupported_format")
    events = bundle.raw.get("events") or {}
    for _name, declaration in events.items():
        correlation = declaration.get("correlates_to")
        if correlation is not None:
            target = events.get(correlation)
            if declaration["direction"] != "input" or not target or target["direction"] != "output":
                raise ValidationError("semantic_validation")
    graph: dict[str, set[str]] = {machine_id: set() for machine_id in model.machines}
    for machine in model.machines.values():
        if not isinstance(machine.raw["version"], int) or isinstance(machine.raw["version"], bool):
            raise ValidationError("semantic_validation")
        _validate_machine(bundle, model, machine, graph)
    _reject_initialization_cycles(graph)


def _validate_machine(
    bundle: Bundle,
    bundle_model: BundleModel,
    machine: MachineModel,
    graph: dict[str, set[str]],
) -> None:
    declarations = _event_declarations(bundle, machine)
    for name, declaration in (machine.raw.get("events") or {}).items():
        if name in _RESERVED_EVENTS or declaration["direction"] != "internal":
            raise ValidationError("semantic_validation")
    component_ids: set[str] = set()
    for state in machine.states.values():
        _validate_variable_literals(state)
        _validate_state_structure(
            bundle, bundle_model, machine, state, declarations, component_ids, graph
        )
    _validate_reachability(machine)


def _validate_variable_literals(state: StateNode) -> None:
    for declaration in (state.raw.get("variables") or {}).values():
        expected = str(declaration["type"])
        if "init" in declaration and not _literal_matches(declaration["init"], expected):
            raise ValidationError("semantic_validation")
        if expected == "int" and isinstance(declaration.get("init"), float):
            raise ValidationError("semantic_validation")


def _validate_payload_literals(declaration: dict[str, Any]) -> None:
    for field in (declaration.get("payload") or {}).values():
        if "default" in field and not _literal_matches(field["default"], str(field["type"])):
            raise ValidationError("semantic_validation")
        if field["type"] == "int" and isinstance(field.get("default"), float):
            raise ValidationError("semantic_validation")


def _validate_state_structure(
    bundle: Bundle,
    bundle_model: BundleModel,
    machine: MachineModel,
    state: StateNode,
    events: dict[str, dict[str, Any]],
    component_ids: set[str],
    graph: dict[str, set[str]],
) -> None:
    scope, scope_declarations = _scope(machine, state)
    if state is machine.root:
        for declaration in (bundle.raw.get("events") or {}).values():
            _validate_payload_literals(declaration)
        for declaration in (machine.raw.get("events") or {}).values():
            _validate_payload_literals(declaration)
    _validate_actions(
        state.raw.get("entry") or [],
        bundle=bundle,
        bundle_model=bundle_model,
        machine=machine,
        state=state,
        scope=scope,
        scope_declarations=scope_declarations,
        events=events,
        event_name=None,
        owner_fields=None,
        context="entry",
        graph=graph,
    )
    _validate_actions(
        state.raw.get("exit") or [],
        bundle=bundle,
        bundle_model=bundle_model,
        machine=machine,
        state=state,
        scope=scope,
        scope_declarations=scope_declarations,
        events=events,
        event_name=None,
        owner_fields=None,
        context="exit",
        graph=graph,
    )
    initial = state.raw.get("initial")
    if isinstance(initial, dict):
        target = machine.resolve(initial["transition_to"], state)
        if not state.is_ancestor_of(target, strict=True):
            raise ValidationError("semantic_validation")
        _validate_transition(
            initial,
            bundle=bundle,
            bundle_model=bundle_model,
            machine=machine,
            source=state,
            scope=scope,
            scope_declarations=scope_declarations,
            events=events,
            event_name=None,
            pointer=f"{state.pointer}/initial",
            context="initial",
            graph=graph,
        )
    for event_name, transition_or_list in (state.raw.get("on_events") or {}).items():
        declaration = events.get(event_name)
        if declaration is None and event_name not in _RESERVED_EVENTS:
            raise ValidationError("semantic_validation")
        transitions = (
            transition_or_list if isinstance(transition_or_list, list) else [transition_or_list]
        )
        for index, transition in enumerate(transitions):
            if index < len(transitions) - 1 and "guard" not in transition:
                raise ValidationError("semantic_validation")
            suffix = f"/{index}" if isinstance(transition_or_list, list) else ""
            _validate_transition(
                transition,
                bundle=bundle,
                bundle_model=bundle_model,
                machine=machine,
                source=state,
                scope=scope,
                scope_declarations=scope_declarations,
                events=events,
                event_name=event_name,
                pointer=f"{state.pointer}/on_events/{_escape_pointer(event_name)}{suffix}",
                context="event",
                graph=graph,
            )
    if state.is_choice:
        branches = state.raw["choice"]
        defaults = [index for index, branch in enumerate(branches) if "guard" not in branch]
        if defaults != [len(branches) - 1]:
            raise ValidationError("semantic_validation")
        for index, branch in enumerate(branches):
            _validate_transition(
                branch,
                bundle=bundle,
                bundle_model=bundle_model,
                machine=machine,
                source=state,
                scope=scope,
                scope_declarations=scope_declarations,
                events=events,
                event_name=None,
                pointer=f"{state.pointer}/choice/{index}",
                context="choice",
                graph=graph,
            )
    for index, placement in enumerate(state.raw.get("components") or []):
        component_id = str(placement["component_id"])
        if component_id in component_ids:
            raise ValidationError("semantic_validation")
        component_ids.add(component_id)
        pointer = f"{state.pointer}/components/{index}"
        if "machine_id" in placement:
            component_machine = bundle_model.machine(str(placement["machine_id"]))
            graph[machine.machine_id].add(component_machine.machine_id)
        else:
            component_machine = bundle_model.inline_component(machine, placement, pointer)
            _validate_inline_machine(bundle, bundle_model, component_machine, graph)
        _validate_bindings(
            placement.get("with") or {},
            component_machine,
            scope={},
            owner_fields=scope,
            allow_owner=True,
        )


def _validate_inline_machine(
    bundle: Bundle,
    bundle_model: BundleModel,
    machine: MachineModel,
    graph: dict[str, set[str]],
) -> None:
    events = _event_declarations(bundle, machine)
    component_ids: set[str] = set()
    for state in machine.states.values():
        _validate_variable_literals(state)
        _validate_state_structure(
            bundle, bundle_model, machine, state, events, component_ids, graph
        )
    _validate_reachability(machine)


def _validate_transition(
    transition: dict[str, Any],
    *,
    bundle: Bundle,
    bundle_model: BundleModel,
    machine: MachineModel,
    source: StateNode,
    scope: dict[str, str],
    scope_declarations: dict[str, tuple[StateNode, dict[str, Any], str]],
    events: dict[str, dict[str, Any]],
    event_name: str | None,
    pointer: str,
    context: str,
    graph: dict[str, set[str]],
) -> None:
    event_declaration = events.get(event_name) if event_name is not None else None
    event_fields = _payload_types(event_declaration, event_name or "")
    guard = transition.get("guard")
    if guard is not None:
        _check_expression(
            guard,
            scope=scope,
            expected="bool",
            event_fields=event_fields if context == "event" else None,
            owner_fields=None,
            allow_event=context == "event",
            allow_owner=False,
        )
    target = transition.get("transition_to")
    target_state = machine.resolve(target, source) if target is not None else None
    if target_state is not None:
        assert isinstance(target, str | dict)
        _validate_target_shape(machine, source, target_state, target, transition)
    _validate_actions(
        transition.get("action") or [],
        bundle=bundle,
        bundle_model=bundle_model,
        machine=machine,
        state=source,
        scope=scope,
        scope_declarations=scope_declarations,
        events=events,
        event_name=event_name if context == "event" else None,
        owner_fields=None,
        context=context,
        graph=graph,
    )
    if target_state is not None:
        _validate_destroyed_destinations(
            machine, source, target_state, transition, scope_declarations
        )


def _validate_target_shape(
    machine: MachineModel,
    source: StateNode,
    target: StateNode,
    target_spec: str | dict[str, str],
    transition: dict[str, Any],
) -> None:
    if target is machine.root:
        raise ValidationError("root_reentry")
    local = transition.get("local") is True
    if local:
        if source is machine.root:
            raise ValidationError("root_local_transition")
        if source.type != "composite" or not source.is_ancestor_of(target, strict=True):
            raise ValidationError("semantic_validation")
    if isinstance(target_spec, dict):
        if target.type != "composite" or target.raw.get("history", "none") == "none":
            raise ValidationError("semantic_validation")
        if target.is_ancestor_of(source, strict=True):
            raise ValidationError("semantic_validation")


def _transition_boundary(
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
    target_ancestors = {node.path: node for node in target.ancestors(include_self=True)}
    for node in source.ancestors(include_self=True):
        if node.path in target_ancestors:
            return node
    return machine.root


def _validate_destroyed_destinations(
    machine: MachineModel,
    source: StateNode,
    target: StateNode,
    transition: dict[str, Any],
    declarations: dict[str, tuple[StateNode, dict[str, Any], str]],
) -> None:
    boundary = _transition_boundary(machine, source, target, transition.get("local") is True)
    for action in transition.get("action") or []:
        if "assign" in action:
            name = next(iter(action["assign"]))
            declaration_state = declarations[name][0]
            if boundary.is_ancestor_of(declaration_state, strict=True):
                raise ValidationError("destroyed_variable_write")
        if "refresh" in action and target.type == "final" and target.parent is machine.root:
            raise ValidationError("destroyed_variable_write")
        if "spawn" in action and "bind_to" in action["spawn"]:
            name = action["spawn"]["bind_to"]
            declaration_state = declarations[name][0]
            if boundary.is_ancestor_of(declaration_state, strict=True):
                raise ValidationError("destroyed_reference_binding")


def _validate_actions(
    actions: list[dict[str, Any]],
    *,
    bundle: Bundle,
    bundle_model: BundleModel,
    machine: MachineModel,
    state: StateNode,
    scope: dict[str, str],
    scope_declarations: dict[str, tuple[StateNode, dict[str, Any], str]],
    events: dict[str, dict[str, Any]],
    event_name: str | None,
    owner_fields: dict[str, str] | None,
    context: str,
    graph: dict[str, set[str]],
) -> None:
    event_fields = _payload_types(events.get(event_name), event_name or "") if event_name else None
    for action in actions:
        if "assign" in action:
            name, expression = next(iter(action["assign"].items()))
            if name not in scope_declarations or scope_declarations[name][1].get("external"):
                raise ValidationError("semantic_validation")
            _check_expression(
                expression,
                scope=scope,
                expected=scope[name],
                event_fields=event_fields,
                owner_fields=owner_fields,
                allow_event=event_name is not None,
                allow_owner=owner_fields is not None,
            )
        elif "send" in action:
            _validate_send(
                action["send"],
                bundle=bundle,
                bundle_model=bundle_model,
                machine=machine,
                state=state,
                scope=scope,
                events=events,
                event_fields=event_fields,
                allow_event=event_name is not None,
            )
        elif "spawn" in action:
            spawn = action["spawn"]
            target = bundle_model.machine(str(spawn["machine_id"]))
            if context in {"entry", "initial", "choice"}:
                graph[machine.machine_id].add(target.machine_id)
            _validate_bindings(
                spawn.get("bindings") or {},
                target,
                scope=scope,
                owner_fields=None,
                allow_owner=False,
            )
            bind_to = spawn.get("bind_to")
            if bind_to is not None:
                if bind_to not in scope_declarations:
                    raise ValidationError("semantic_validation")
                declaration = scope_declarations[bind_to][1]
                if declaration["type"] != "instance_reference":
                    raise ValidationError("semantic_validation")
                constraint = declaration.get("machine_id")
                if constraint is not None and constraint != target.machine_id:
                    raise ValidationError("semantic_validation")
        elif "cancel" in action:
            _check_expression(
                action["cancel"]["instance"],
                scope=scope,
                expected="instance_reference",
                event_fields=event_fields,
                owner_fields=None,
                allow_event=event_name is not None,
                allow_owner=False,
            )
        elif "refresh" in action:
            if event_name != "env":
                raise ValidationError("semantic_validation")


def _validate_send(
    send: dict[str, Any],
    *,
    bundle: Bundle,
    bundle_model: BundleModel,
    machine: MachineModel,
    state: StateNode,
    scope: dict[str, str],
    events: dict[str, dict[str, Any]],
    event_fields: dict[str, str] | None,
    allow_event: bool,
) -> None:
    event_name = str(send["event"])
    declaration = events.get(event_name)
    targets = send.get("targets") or [send.get("to", {"self": True})]
    external = any(target.get("external") is True for target in targets)
    if event_name == "env":
        if len(targets) != 1 or "component" not in targets[0]:
            raise ValidationError("semantic_validation")
        changed = (send.get("payload") or {}).get("changed")
        if not isinstance(changed, str) or not changed.strip().startswith("{"):
            raise ValidationError("semantic_validation")
        if changed.strip() == "{}" or "correlation_id" in send:
            raise ValidationError("semantic_validation")
        component_id = targets[0]["component"]
        placement = next(
            (
                item
                for item in state.raw.get("components") or []
                if item["component_id"] == component_id
            ),
            None,
        )
        if placement is None:
            raise ValidationError("semantic_validation")
        pointer = (
            f"{state.pointer}/components/{(state.raw.get('components') or []).index(placement)}"
        )
        target_machine = (
            bundle_model.machine(placement["machine_id"])
            if "machine_id" in placement
            else bundle_model.inline_component(machine, placement, pointer)
        )
        external_variables = {
            name: declaration
            for name, declaration in (target_machine.root.raw.get("variables") or {}).items()
            if declaration.get("external") is True
        }
        changed_members = _parse_cel_map_literal(changed)
        if changed_members is None or set(changed_members) - set(external_variables):
            raise ValidationError("semantic_validation")
        for name, expression in changed_members.items():
            _check_expression(
                expression,
                scope=scope,
                expected=str(external_variables[name]["type"]),
                event_fields=event_fields,
                owner_fields=None,
                allow_event=allow_event,
                allow_owner=False,
            )
        return
    if declaration is None or event_name in _RESERVED_EVENTS:
        raise ValidationError("semantic_validation")
    expected_direction = "output" if external else "internal"
    if declaration["direction"] != expected_direction:
        raise ValidationError("semantic_validation")
    if external and "correlation_id" not in send:
        raise ValidationError("semantic_validation")
    payload_types = _payload_types(declaration, event_name)
    supplied = send.get("payload") or {}
    if set(supplied) - set(payload_types):
        raise ValidationError("semantic_validation")
    for name, expression in supplied.items():
        _check_expression(
            expression,
            scope=scope,
            expected=payload_types[name],
            event_fields=event_fields,
            owner_fields=None,
            allow_event=allow_event,
            allow_owner=False,
        )
    if "correlation_id" in send:
        _check_expression(
            send["correlation_id"],
            scope=scope,
            expected="string",
            event_fields=event_fields,
            owner_fields=None,
            allow_event=allow_event,
            allow_owner=False,
        )
    for target in targets:
        if "instance" in target:
            _check_expression(
                target["instance"],
                scope=scope,
                expected="instance_reference",
                event_fields=event_fields,
                owner_fields=None,
                allow_event=allow_event,
                allow_owner=False,
            )


def _parse_cel_map_literal(expression: str) -> dict[str, str] | None:
    body = expression.strip()
    if not (body.startswith("{") and body.endswith("}")):
        return None
    body = body[1:-1].strip()
    if not body:
        return {}
    members: dict[str, str] = {}
    for item in body.split(","):
        match = re.fullmatch(r"\s*(['\"])([A-Za-z_][A-Za-z0-9_]*)\1\s*:\s*(.+?)\s*", item)
        if match is None:
            return None
        members[match.group(2)] = match.group(3)
    return members


def _validate_bindings(
    bindings: dict[str, Any],
    target: MachineModel,
    *,
    scope: dict[str, str],
    owner_fields: dict[str, str] | None,
    allow_owner: bool,
) -> None:
    root_variables = target.root.raw.get("variables") or {}
    for kind in ("input", "external"):
        expected = {
            name: declaration
            for name, declaration in root_variables.items()
            if declaration.get(kind) is True
        }
        supplied = bindings.get(kind) or {}
        if set(supplied) - set(expected):
            raise ValidationError("invalid_binding")
        missing = {
            name
            for name, declaration in expected.items()
            if name not in supplied and "init" not in declaration
        }
        if missing:
            raise ValidationError("invalid_binding")
        for name, expression in supplied.items():
            inferred = _check_expression(
                expression,
                scope=scope,
                expected=str(expected[name]["type"]),
                event_fields=None,
                owner_fields=owner_fields,
                allow_event=False,
                allow_owner=allow_owner,
            )
            if not _compatible(inferred, str(expected[name]["type"])):
                raise ValidationError("invalid_binding")


def _validate_reachability(machine: MachineModel) -> None:
    reachable: set[str] = set()

    def enter(state: StateNode) -> None:
        if state.path in reachable:
            return
        reachable.add(state.path)
        for ancestor in state.ancestors():
            reachable.add(ancestor.path)
        if state.is_choice:
            for branch in state.raw["choice"]:
                enter(machine.resolve(branch["transition_to"], state))
        elif state.type == "composite":
            enter(machine.resolve(state.raw["initial"]["transition_to"], state))

    enter(machine.root)
    changed = True
    while changed:
        before = len(reachable)
        for path in list(reachable):
            state = machine.states[path]
            for transition_or_list in (state.raw.get("on_events") or {}).values():
                transitions = (
                    transition_or_list
                    if isinstance(transition_or_list, list)
                    else [transition_or_list]
                )
                for transition in transitions:
                    if "transition_to" in transition:
                        enter(machine.resolve(transition["transition_to"], state))
        changed = len(reachable) != before
    unreachable = [state for state in machine.states.values() if state.path not in reachable]
    if unreachable:
        raise ValidationError("semantic_validation", path=unreachable[0].pointer)


def _reject_initialization_cycles(graph: dict[str, set[str]]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(machine_id: str) -> None:
        if machine_id in visiting:
            raise ValidationError("semantic_validation")
        if machine_id in visited:
            return
        visiting.add(machine_id)
        for target in graph[machine_id]:
            visit(target)
        visiting.remove(machine_id)
        visited.add(machine_id)

    for machine_id in graph:
        visit(machine_id)
