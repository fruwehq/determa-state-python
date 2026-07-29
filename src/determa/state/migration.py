"""Deterministic portable aggregate migration and atomic dispatch composition."""

from __future__ import annotations

import copy
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, cast

from . import cel
from .definition import Bundle, _escape_pointer
from .engine import Delivery, dispatch
from .errors import ArtifactError, CelError
from .model import BundleModel, MachineModel, StateNode
from .wire import (
    ArtifactResolver,
    ArtifactSource,
    _bundle_from_resolver,
    _source_bytes,
    aggregate_shape_fingerprint,
    aggregate_state_digest,
    canonical_bytes,
    decimal,
    decoded_typed_value,
    load_json_artifact,
    migration_descriptor_digest,
    restore_aggregate,
    typed_value,
)


@dataclass(frozen=True)
class MigrationLimits:
    """Configured per-operation resource limits, at least the portable floors."""

    maximum_aggregate_bytes: int = 1_048_576
    maximum_definition_bytes: int = 1_048_576
    maximum_descriptor_bytes: int = 65_536
    maximum_transformed_output_bytes: int = 65_536
    maximum_json_nesting_depth: int = 64
    maximum_runtimes: int = 256
    maximum_active_states_per_runtime: int = 1_024
    maximum_variables_per_runtime: int = 4_096
    maximum_map_members: int = 4_096
    maximum_list_members: int = 4_096
    maximum_string_utf8_bytes: int = 65_536
    maximum_chain_length: int = 8
    maximum_descriptor_rules: int = 1_024
    maximum_cel_expression_length: int = 65_536
    maximum_cel_ast_nodes: int = 65_536
    maximum_cel_evaluation_steps: int = 1_000_000

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> MigrationLimits:
        expected = set(cls.__dataclass_fields__)
        if set(value) != expected:
            raise ArtifactError("invalid_migration_request")
        try:
            parsed = {name: decimal(value[name]) for name in expected}
        except ArtifactError as exc:
            raise ArtifactError("invalid_migration_request") from exc
        return cls(**parsed)


@dataclass(frozen=True)
class MigrationFailure:
    code: str


@dataclass(frozen=True)
class MigrationResult:
    """Pure migration success or one closed deterministic failure."""

    aggregate_envelope: dict[str, Any] | None
    aggregate_bytes: bytes | None
    audit_records: tuple[dict[str, Any], ...]
    failure: MigrationFailure | None

    @property
    def succeeded(self) -> bool:
        return self.failure is None


@dataclass(frozen=True)
class MigrationDispatchResult:
    """One atomic migration plus optional ordinary dispatch result."""

    aggregate_envelope: dict[str, Any] | None
    aggregate_bytes: bytes | None
    audit_records: tuple[dict[str, Any], ...]
    status: str | None
    disposition: str | None
    emissions: tuple[dict[str, Any], ...]
    fault: dict[str, Any] | None
    rejection: dict[str, Any] | None
    failure: MigrationFailure | None

    @property
    def succeeded(self) -> bool:
        return self.failure is None


def _failure(code: str) -> MigrationResult:
    return MigrationResult(None, None, (), MigrationFailure(code))


def _dispatch_failure(code: str) -> MigrationDispatchResult:
    return MigrationDispatchResult(
        None, None, (), None, None, (), None, None, MigrationFailure(code)
    )


def _resource_metrics(value: Any, depth: int = 0) -> tuple[int, int, int, int]:
    maximum_depth = depth
    maximum_map_members = 0
    maximum_list_members = 0
    maximum_string_bytes = 0
    if isinstance(value, dict):
        maximum_depth = depth + 1
        maximum_map_members = len(value)
        for key, child in value.items():
            maximum_string_bytes = max(maximum_string_bytes, len(key.encode("utf-8")))
            child_metrics = _resource_metrics(child, depth + 1)
            maximum_depth = max(maximum_depth, child_metrics[0])
            maximum_map_members = max(maximum_map_members, child_metrics[1])
            maximum_list_members = max(maximum_list_members, child_metrics[2])
            maximum_string_bytes = max(maximum_string_bytes, child_metrics[3])
    elif isinstance(value, list):
        maximum_depth = depth + 1
        maximum_list_members = len(value)
        for child in value:
            child_metrics = _resource_metrics(child, depth + 1)
            maximum_depth = max(maximum_depth, child_metrics[0])
            maximum_map_members = max(maximum_map_members, child_metrics[1])
            maximum_list_members = max(maximum_list_members, child_metrics[2])
            maximum_string_bytes = max(maximum_string_bytes, child_metrics[3])
    elif isinstance(value, str):
        maximum_string_bytes = len(value.encode("utf-8"))
    return (
        maximum_depth,
        maximum_map_members,
        maximum_list_members,
        maximum_string_bytes,
    )


def _check_shape_limits(
    aggregate: dict[str, Any],
    definitions: list[Bundle],
    descriptors: list[dict[str, Any]],
    limits: MigrationLimits,
) -> None:
    if len(canonical_bytes(aggregate)) > limits.maximum_aggregate_bytes:
        raise ArtifactError("migration_resource_limit_exceeded")
    if any(
        len(canonical_bytes(typed_value(bundle.raw))) > limits.maximum_definition_bytes
        for bundle in definitions
    ):
        raise ArtifactError("migration_resource_limit_exceeded")
    if any(
        len(canonical_bytes(descriptor)) > limits.maximum_descriptor_bytes
        for descriptor in descriptors
    ):
        raise ArtifactError("migration_resource_limit_exceeded")
    values: list[Any] = [aggregate, *[bundle.raw for bundle in definitions], *descriptors]
    metrics = [_resource_metrics(value) for value in values]
    if (
        max(item[0] for item in metrics) > limits.maximum_json_nesting_depth
        or max(item[1] for item in metrics) > limits.maximum_map_members
        or max(item[2] for item in metrics) > limits.maximum_list_members
        or max(item[3] for item in metrics) > limits.maximum_string_utf8_bytes
        or len(aggregate["runtimes"]) > limits.maximum_runtimes
        or any(
            len(runtime["active_state_activations"])
            > limits.maximum_active_states_per_runtime
            for runtime in aggregate["runtimes"]
        )
        or any(
            len(runtime["variables"]) > limits.maximum_variables_per_runtime
            for runtime in aggregate["runtimes"]
        )
    ):
        raise ArtifactError("migration_resource_limit_exceeded")


def _ast_nodes(value: Any) -> int:
    children = getattr(value, "children", None)
    if children is None:
        return 1
    return 1 + sum(_ast_nodes(child) for child in children)


def _descriptor_static_requirements(
    descriptor: dict[str, Any], limits: MigrationLimits
) -> tuple[int, int]:
    rule_count = sum(len(items) for items in descriptor["mappings"].values())
    if rule_count > limits.maximum_descriptor_rules:
        raise ArtifactError("migration_resource_limit_exceeded")
    expressions = {
        rule["expression"]
        for rule in descriptor["mappings"]["variables"]
        if "expression" in rule
    }
    expression_bytes = sum(len(expression.encode("utf-8")) for expression in expressions)
    ast_nodes = sum(_ast_nodes(cel._tree(expression)) for expression in expressions)
    requirements = descriptor["resource_requirements"]
    if (
        expression_bytes > limits.maximum_cel_expression_length
        or ast_nodes > limits.maximum_cel_ast_nodes
        or expression_bytes > decimal(requirements["maximum_cel_expression_length"])
        or ast_nodes > decimal(requirements["maximum_cel_ast_nodes"])
    ):
        raise ArtifactError("migration_resource_limit_exceeded")
    return expression_bytes, ast_nodes


def _pointer_parts(pointer: str) -> list[str]:
    if not pointer.startswith("/"):
        raise ArtifactError("invalid_migration_descriptor")
    return [
        item.replace("~1", "/").replace("~0", "~")
        for item in pointer[1:].split("/")
    ]


def _pointer_get(document: Any, pointer: str) -> Any:
    current = document
    for part in _pointer_parts(pointer):
        if isinstance(current, list):
            try:
                current = current[int(part)]
            except (IndexError, ValueError) as exc:
                raise ArtifactError("invalid_migration_descriptor") from exc
        elif isinstance(current, dict) and part in current:
            current = current[part]
        else:
            raise ArtifactError("invalid_migration_descriptor")
    return current


def _machine_identity_for_pointer(bundle: Bundle, root_pointer: str) -> dict[str, Any]:
    parts = _pointer_parts(root_pointer)
    if len(parts) < 3 or parts[0] != "machines":
        raise ArtifactError("migration_totality_failure")
    try:
        machine = bundle.raw["machines"][int(parts[1])]
    except (IndexError, ValueError) as exc:
        raise ArtifactError("migration_totality_failure") from exc
    return {
        "namespace": bundle.namespace,
        "machine_id": machine["machine_id"],
        "machine_version": str(machine["version"]),
        "root_definition_pointer": root_pointer,
    }


def _definition_binding(bundle: Bundle, root_pointer: str) -> dict[str, Any]:
    return {
        "validated_bundle_fingerprint": bundle.fingerprint,
        "machine": _machine_identity_for_pointer(bundle, root_pointer),
    }


def _machine_model_for_root(bundle: Bundle, root_pointer: str) -> MachineModel:
    models = BundleModel(bundle)
    identity = _machine_identity_for_pointer(bundle, root_pointer)
    base = models.machine(identity["machine_id"])
    if root_pointer == base.root_pointer:
        return base
    root = _pointer_get(bundle.raw, root_pointer)
    return MachineModel(
        bundle,
        base.raw,
        machine_index=base.machine_index,
        root=root,
        root_pointer=root_pointer,
        identity_machine=base.identity_machine,
    )


def _state_nodes(machine: MachineModel) -> dict[str, StateNode]:
    return {node.pointer: node for node in machine.states.values()}


def _variable_declaration(bundle: Bundle, pointer: str) -> dict[str, Any]:
    declaration = _pointer_get(bundle.raw, pointer)
    if not isinstance(declaration, dict) or "type" not in declaration:
        raise ArtifactError("invalid_migration_descriptor")
    return declaration


def _state_pointer_for_variable(pointer: str) -> str:
    marker = "/variables/"
    if marker not in pointer:
        raise ArtifactError("invalid_migration_descriptor")
    return pointer.split(marker, 1)[0]


def _history_pointers(machine: MachineModel) -> list[str]:
    return sorted(
        (
            f"{node.pointer}/history"
            for node in machine.states.values()
            if node.type == "composite" and node.raw.get("history", "none") != "none"
        ),
        key=lambda item: item.encode("utf-8"),
    )


def _active_ancestor_pointers(machine: MachineModel, leaves: list[str]) -> list[str]:
    result: set[str] = set()
    nodes = _state_nodes(machine)
    for pointer in leaves:
        node = nodes.get(pointer)
        if node is None:
            raise ArtifactError("migration_totality_failure")
        result.update(item.pointer for item in node.ancestors(include_self=True))
    return sorted(result, key=lambda item: item.encode("utf-8"))


def _unique_mapping(
    rules: list[dict[str, Any]], source_member: str, target_member: str
) -> dict[str, str]:
    result: dict[str, str] = {}
    targets: set[str] = set()
    for rule in rules:
        source = rule[source_member]
        target = rule[target_member]
        if source in result or target in targets:
            raise ArtifactError("invalid_migration_descriptor")
        result[source] = target
        targets.add(target)
    return result


def _validate_descriptor_semantics(
    descriptor: dict[str, Any],
    source_bundle: Bundle,
    target_bundle: Bundle,
    limits: MigrationLimits,
) -> None:
    if (
        descriptor["source_validated_bundle_fingerprint"] != source_bundle.fingerprint
        or descriptor["target_validated_bundle_fingerprint"] != target_bundle.fingerprint
        or descriptor["source_aggregate_shape_fingerprint"]
        != aggregate_shape_fingerprint(source_bundle)
        or descriptor["target_aggregate_shape_fingerprint"]
        != aggregate_shape_fingerprint(target_bundle)
    ):
        raise ArtifactError("invalid_migration_descriptor")
    mappings = descriptor["mappings"]
    if descriptor["mode"] == "compatible":
        if (
            descriptor["source_aggregate_shape_fingerprint"]
            != descriptor["target_aggregate_shape_fingerprint"]
            or any(mappings.values())
        ):
            raise ArtifactError("invalid_migration_descriptor")
    _unique_mapping(
        mappings["machines"], "source_definition_pointer", "target_definition_pointer"
    )
    _unique_mapping(
        mappings["components"],
        "source_component_definition_pointer",
        "target_component_definition_pointer",
    )
    _unique_mapping(
        mappings["owned_runtimes"],
        "source_spawn_action_pointer",
        "target_spawn_action_pointer",
    )
    _unique_mapping(
        mappings["lifetime_holders"],
        "source_variable_declaration_pointer",
        "target_variable_declaration_pointer",
    )
    active_sources: set[str] = set()
    active_targets: set[str] = set()
    for rule in mappings["active_states"]:
        source = rule["source_leaf_state_definition_pointer"]
        targets = rule["target_leaf_state_definition_pointers"]
        if source in active_sources or any(target in active_targets for target in targets):
            raise ArtifactError("invalid_migration_descriptor")
        active_sources.add(source)
        active_targets.update(targets)
    consumed: set[str] = set()
    produced: set[str] = set()
    for rule in mappings["variables"]:
        sources = (
            rule.get("source_declaration_pointers")
            or (
                [rule["source_declaration_pointer"]]
                if "source_declaration_pointer" in rule
                else []
            )
        )
        target = rule.get("target_declaration_pointer")
        if any(source in consumed for source in sources) or (
            target is not None and target in produced
        ):
            raise ArtifactError("invalid_migration_descriptor")
        consumed.update(sources)
        if target is not None:
            produced.add(target)
            _variable_declaration(target_bundle, target)
        for source in sources:
            _variable_declaration(source_bundle, source)
        if "expression" in rule:
            scope = {
                f"source_{index}": cel.type_from_declaration(
                    _variable_declaration(source_bundle, source)
                )
                for index, source in enumerate(sources)
            }
            target_declaration = _variable_declaration(target_bundle, cast(str, target))
            if target_declaration["type"] == "instance_reference" or any(
                declaration.kind == "instance_reference" for declaration in scope.values()
            ):
                raise ArtifactError("invalid_migration_descriptor")
            try:
                cel.check_expression(
                    rule["expression"],
                    scope,
                    expected=cel.type_from_declaration(target_declaration),
                    event_fields=None,
                    owner_fields=None,
                )
            except CelError as exc:
                raise ArtifactError("invalid_migration_descriptor") from exc
    _descriptor_static_requirements(descriptor, limits)


def _resolve_descriptor(
    resolver: ArtifactResolver, digest: str
) -> tuple[dict[str, Any], bytes]:
    source = resolver.resolve_migration_descriptor(digest)
    if source is None:
        raise ArtifactError("migration_route_mismatch")
    if not resolver.migration_descriptor_is_trusted(digest):
        raise ArtifactError("migration_descriptor_untrusted")
    document, _raw = load_json_artifact(source, "migration_descriptor")
    encoded = canonical_bytes(document)
    if migration_descriptor_digest(document) != digest:
        raise ArtifactError("invalid_migration_descriptor")
    return document, encoded


def _compatible_candidate(
    source: dict[str, Any],
    target_bundle: Bundle,
) -> dict[str, Any]:
    candidate = copy.deepcopy(source)
    candidate["validated_bundle_fingerprint"] = target_bundle.fingerprint
    candidate["namespace"] = target_bundle.namespace
    for runtime in candidate["runtimes"]:
        runtime["current_definition"]["validated_bundle_fingerprint"] = (
            target_bundle.fingerprint
        )
        runtime["current_definition"]["machine"]["namespace"] = target_bundle.namespace
    candidate["migration_sequence"] = str(decimal(candidate["migration_sequence"]) + 1)
    candidate["aggregate_state_digest"] = aggregate_state_digest(candidate)
    return candidate


def _counter_transform(
    items: list[dict[str, Any]], rules: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    source = {item["definition_pointer"]: decimal(item["next_sequence"]) for item in items}
    consumed: set[str] = set()
    result: list[dict[str, Any]] = []
    targets: set[str] = set()
    for rule in rules:
        operation = rule["operation"]
        target = rule["target_definition_pointer"]
        if target in targets:
            raise ArtifactError("invalid_migration_descriptor")
        targets.add(target)
        if operation == "map":
            pointer = rule["source_definition_pointer"]
            if pointer not in source or pointer in consumed:
                continue
            value = source[pointer]
            consumed.add(pointer)
        elif operation == "initialize_zero":
            value = 0
        else:
            pointers = rule["source_definition_pointers"]
            if any(pointer not in source or pointer in consumed for pointer in pointers):
                continue
            value = max(source[pointer] for pointer in pointers)
            consumed.update(pointers)
        result.append({"definition_pointer": target, "next_sequence": str(value)})
    if consumed != set(source):
        raise ArtifactError("migration_totality_failure")
    result.sort(key=lambda item: item["definition_pointer"].encode("utf-8"))
    return result


def _mapped_active(
    runtime: dict[str, Any],
    source_machine: MachineModel,
    target_machine: MachineModel,
    descriptor: dict[str, Any],
) -> tuple[list[str], list[dict[str, Any]]]:
    active_rules = descriptor["mappings"]["active_states"]
    targets: list[str] = []
    for leaf in runtime["active_leaf_state_definition_pointers"]:
        matching = [
            rule
            for rule in active_rules
            if rule["source_leaf_state_definition_pointer"] == leaf
        ]
        if len(matching) != 1:
            raise ArtifactError(
                "invalid_migration_descriptor"
                if len(matching) > 1
                else "migration_totality_failure"
            )
        targets.extend(matching[0]["target_leaf_state_definition_pointers"])
    if len(set(targets)) != len(targets):
        raise ArtifactError("migration_totality_failure")
    targets.sort(key=lambda item: item.encode("utf-8"))
    source_activations = {
        item["state_definition_pointer"]: item["activation_sequence"]
        for item in runtime["active_state_activations"]
    }
    counter_rules = descriptor["mappings"]["counters"]
    pointer_map: dict[str, str] = {}
    for rule in counter_rules:
        if rule["operation"] == "map":
            pointer_map[rule["source_definition_pointer"]] = rule[
                "target_definition_pointer"
            ]
    target_ancestors = _active_ancestor_pointers(target_machine, targets)
    activations: list[dict[str, Any]] = []
    for target in target_ancestors:
        sources = [
            source
            for source, mapped in pointer_map.items()
            if mapped == target and source in source_activations
        ]
        if len(sources) != 1:
            raise ArtifactError("migration_totality_failure")
        activations.append(
            {
                "state_definition_pointer": target,
                "activation_sequence": source_activations[sources[0]],
            }
        )
    return targets, activations


def _target_variable_pointers(
    target_machine: MachineModel, active_activations: list[dict[str, Any]]
) -> dict[str, str]:
    states = _state_nodes(target_machine)
    result: dict[str, str] = {}
    for activation in active_activations:
        state = states[activation["state_definition_pointer"]]
        for name in (state.raw.get("variables") or {}):
            pointer = f"{state.pointer}/variables/{_escape_pointer(name)}"
            result[pointer] = activation["activation_sequence"]
    return result


def _transform_variables(
    runtime: dict[str, Any],
    source_bundle: Bundle,
    target_bundle: Bundle,
    target_machine: MachineModel,
    target_activations: list[dict[str, Any]],
    descriptor: dict[str, Any],
) -> tuple[list[dict[str, Any]], int, int]:
    source_values: dict[str, dict[str, Any]] = {}
    for occurrence in runtime["variables"]:
        pointer = occurrence["variable_declaration_pointer"]
        if pointer in source_values:
            raise ArtifactError("migration_totality_failure")
        source_values[pointer] = occurrence
    target_required = _target_variable_pointers(target_machine, target_activations)
    produced: dict[str, dict[str, Any]] = {}
    consumed: set[str] = set()
    transformed_bytes = 0
    evaluation_steps = 0
    for rule in descriptor["mappings"]["variables"]:
        operation = rule["operation"]
        target = rule.get("target_declaration_pointer")
        sources = (
            rule.get("source_declaration_pointers")
            or (
                [rule["source_declaration_pointer"]]
                if "source_declaration_pointer" in rule
                else []
            )
        )
        applicable_sources = [source_values.get(pointer) for pointer in sources]
        if operation == "drop":
            if applicable_sources[0] is not None:
                consumed.add(sources[0])
            continue
        if target not in target_required:
            continue
        if target in produced:
            raise ArtifactError("invalid_migration_descriptor")
        if operation in {"copy", "transform"} and any(
            value is None for value in applicable_sources
        ):
            raise ArtifactError("migration_totality_failure")
        if operation == "copy":
            value = copy.deepcopy(cast(dict[str, Any], applicable_sources[0])["value"])
        else:
            activation: dict[str, Any] = {}
            if operation == "transform":
                for index, occurrence in enumerate(applicable_sources):
                    activation[f"source_{index}"] = decoded_typed_value(
                        cast(dict[str, Any], occurrence)["value"]
                    )
                consumed.update(sources)
            expression = rule["expression"]
            nodes = _ast_nodes(cel._tree(expression))
            evaluation_steps += nodes
            try:
                evaluated = cel.evaluate(expression, activation)
            except CelError as exc:
                raise ArtifactError("migration_transform_fault") from exc
            value = typed_value(evaluated)
            transformed_bytes += len(canonical_bytes(value))
        if operation == "copy":
            consumed.update(sources)
        produced[target] = {
            "variable_declaration_pointer": target,
            "declaring_state_activation_sequence": target_required[target],
            "value": value,
        }
    if consumed != set(source_values) or set(produced) != set(target_required):
        raise ArtifactError("migration_totality_failure")
    result = sorted(
        produced.values(),
        key=lambda item: (
            item["variable_declaration_pointer"].encode("utf-8"),
            int(item["declaring_state_activation_sequence"]),
        ),
    )
    return result, transformed_bytes, evaluation_steps


def _transform_history(
    runtime: dict[str, Any],
    target_machine: MachineModel,
    descriptor: dict[str, Any],
) -> list[dict[str, Any]]:
    source = {
        item["history_declaration_pointer"]: item for item in runtime["history"]
    }
    consumed: set[str] = set()
    produced: dict[str, dict[str, Any]] = {}
    for rule in descriptor["mappings"]["history"]:
        operation = rule["operation"]
        if operation == "initialize_null":
            target = rule["target_history_declaration_pointer"]
            produced[target] = {
                "history_declaration_pointer": target,
                "recorded_state_definition_pointers": None,
            }
            continue
        source_pointer = rule["source_history_declaration_pointer"]
        occurrence = source.get(source_pointer)
        if occurrence is None:
            continue
        consumed.add(source_pointer)
        if operation == "drop":
            continue
        target = rule["target_history_declaration_pointer"]
        recorded = occurrence["recorded_state_definition_pointers"]
        mapping = {
            item["source_definition_pointer"]: item["target_definition_pointer"]
            for item in rule["recorded_state_mappings"]
        }
        if recorded is not None:
            if any(pointer not in mapping for pointer in recorded):
                raise ArtifactError("migration_totality_failure")
            recorded = sorted(
                (mapping[pointer] for pointer in recorded),
                key=lambda item: item.encode("utf-8"),
            )
        produced[target] = {
            "history_declaration_pointer": target,
            "recorded_state_definition_pointers": recorded,
        }
    if consumed != set(source) or set(produced) != set(_history_pointers(target_machine)):
        raise ArtifactError("migration_totality_failure")
    return sorted(
        produced.values(),
        key=lambda item: item["history_declaration_pointer"].encode("utf-8"),
    )


def _component_counter_transform(
    items: list[dict[str, Any]], component_rules: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    mapping = {
        rule["source_component_definition_pointer"]: rule[
            "target_component_definition_pointer"
        ]
        for rule in component_rules
    }
    result = []
    for item in items:
        source = item["definition_pointer"]
        if source not in mapping:
            raise ArtifactError("migration_totality_failure")
        result.append(
            {
                "definition_pointer": mapping[source],
                "next_sequence": item["next_sequence"],
            }
        )
    result.sort(key=lambda item: item["definition_pointer"].encode("utf-8"))
    return result


def _target_root_for_component(bundle: Bundle, pointer: str) -> str:
    placement = _pointer_get(bundle.raw, pointer)
    if not isinstance(placement, dict):
        raise ArtifactError("migration_totality_failure")
    if "root" in placement:
        return f"{pointer}/root"
    machine_id = placement.get("machine_id")
    for index, machine in enumerate(bundle.raw["machines"]):
        if machine["machine_id"] == machine_id:
            return f"/machines/{index}/root"
    raise ArtifactError("migration_totality_failure")


def _target_root_for_machine(bundle: Bundle, machine_id: str) -> str:
    for index, machine in enumerate(bundle.raw["machines"]):
        if machine["machine_id"] == machine_id:
            return f"/machines/{index}/root"
    raise ArtifactError("migration_totality_failure")


def _transform_candidate(
    source: dict[str, Any],
    source_bundle: Bundle,
    target_bundle: Bundle,
    descriptor: dict[str, Any],
    limits: MigrationLimits,
) -> dict[str, Any]:
    candidate = copy.deepcopy(source)
    mappings = descriptor["mappings"]
    machine_mapping = _unique_mapping(
        mappings["machines"], "source_definition_pointer", "target_definition_pointer"
    )
    component_mapping = {
        rule["source_component_definition_pointer"]: rule
        for rule in mappings["components"]
    }
    owned_mapping = {
        (rule["source_spawn_action_pointer"], rule["source_machine_id"]): rule
        for rule in mappings["owned_runtimes"]
    }
    holder_mapping = _unique_mapping(
        mappings["lifetime_holders"],
        "source_variable_declaration_pointer",
        "target_variable_declaration_pointer",
    )
    total_transformed_bytes = 0
    total_evaluation_steps = 0
    for runtime in candidate["runtimes"]:
        relation = runtime["relation"]
        source_root = runtime["current_definition"]["machine"]["root_definition_pointer"]
        if relation["kind"] == "root":
            target_root = machine_mapping.get(source_root)
            if target_root is None:
                raise ArtifactError("migration_totality_failure")
        elif relation["kind"] == "component":
            source_component = relation["current_component_definition_pointer"]
            rule = component_mapping.get(source_component)
            if rule is None:
                raise ArtifactError("migration_totality_failure")
            target_component = rule["target_component_definition_pointer"]
            relation["current_component_definition_pointer"] = target_component
            relation["component_id"] = rule["target_component_id"]
            parts = _pointer_parts(target_component)
            relation["declaration_index"] = str(int(parts[-1]))
            target_root = _target_root_for_component(target_bundle, target_component)
        else:
            source_spawn = relation["current_spawn_action_pointer"]
            source_machine = runtime["current_definition"]["machine"]["machine_id"]
            rule = owned_mapping.get((source_spawn, source_machine))
            if rule is None:
                raise ArtifactError("migration_totality_failure")
            relation["current_spawn_action_pointer"] = rule["target_spawn_action_pointer"]
            target_root = _target_root_for_machine(target_bundle, rule["target_machine_id"])
            holder = relation["lifetime_holder"]
            if holder is not None:
                source_holder = holder["variable_declaration_pointer"]
                if source_holder not in holder_mapping:
                    raise ArtifactError("migration_totality_failure")
                holder["variable_declaration_pointer"] = holder_mapping[source_holder]
        source_machine_model = _machine_model_for_root(source_bundle, source_root)
        target_machine_model = _machine_model_for_root(target_bundle, target_root)
        leaves, activations = _mapped_active(
            runtime, source_machine_model, target_machine_model, descriptor
        )
        variables, transformed_bytes, evaluation_steps = _transform_variables(
            runtime,
            source_bundle,
            target_bundle,
            target_machine_model,
            activations,
            descriptor,
        )
        runtime["active_leaf_state_definition_pointers"] = leaves
        runtime["active_state_activations"] = activations
        runtime["variables"] = variables
        runtime["history"] = _transform_history(
            runtime, target_machine_model, descriptor
        )
        runtime["next_state_activation_sequences"] = _counter_transform(
            runtime["next_state_activation_sequences"], mappings["counters"]
        )
        runtime["next_component_activation_sequences"] = _component_counter_transform(
            runtime["next_component_activation_sequences"], mappings["components"]
        )
        runtime["current_definition"] = _definition_binding(target_bundle, target_root)
        total_transformed_bytes += transformed_bytes
        total_evaluation_steps += evaluation_steps
    requirements = descriptor["resource_requirements"]
    if (
        total_transformed_bytes
        > min(
            limits.maximum_transformed_output_bytes,
            decimal(requirements["maximum_transformed_output_bytes"]),
        )
        or total_evaluation_steps
        > min(
            limits.maximum_cel_evaluation_steps,
            decimal(requirements["maximum_cel_evaluation_steps"]),
        )
    ):
        raise ArtifactError("migration_resource_limit_exceeded")
    root = next(
        runtime
        for runtime in candidate["runtimes"]
        if runtime["runtime_id"] == candidate["root_runtime_id"]
    )
    candidate["validated_bundle_fingerprint"] = target_bundle.fingerprint
    candidate["namespace"] = target_bundle.namespace
    candidate["root_machine_id"] = root["current_definition"]["machine"]["machine_id"]
    candidate["root_machine_version"] = root["current_definition"]["machine"][
        "machine_version"
    ]
    candidate["migration_sequence"] = str(decimal(candidate["migration_sequence"]) + 1)
    candidate["aggregate_state_digest"] = aggregate_state_digest(candidate)
    return candidate


def _apply_descriptor(
    source: dict[str, Any],
    source_bundle: Bundle,
    target_bundle: Bundle,
    descriptor: dict[str, Any],
    artifact_resolver: ArtifactResolver,
    limits: MigrationLimits,
    maintenance_mode: bool,
) -> dict[str, Any]:
    root = next(
        runtime
        for runtime in source["runtimes"]
        if runtime["runtime_id"] == source["root_runtime_id"]
    )
    if root["status"] in {"completed", "faulted"}:
        if not maintenance_mode:
            raise ArtifactError("terminal_migration_requires_maintenance")
        if descriptor["terminal_policy"][root["status"]] != "preserve":
            raise ArtifactError("terminal_migration_rejected")
    _validate_descriptor_semantics(descriptor, source_bundle, target_bundle, limits)
    candidate = (
        _compatible_candidate(source, target_bundle)
        if descriptor["mode"] == "compatible"
        else _transform_candidate(
            source, source_bundle, target_bundle, descriptor, limits
        )
    )
    restore_aggregate(canonical_bytes(candidate), artifact_resolver)
    return candidate


def migrate_aggregate(
    aggregate: ArtifactSource,
    target_validated_bundle_fingerprint: str,
    migration_route: Sequence[str],
    artifact_resolver: ArtifactResolver,
    *,
    maintenance_mode: bool,
    resource_limits: MigrationLimits | None = None,
) -> MigrationResult:
    """Apply one exact trusted descriptor route to an aggregate copy."""
    source_copy = _source_bytes(aggregate)
    limits = resource_limits or MigrationLimits()
    try:
        if (
            not isinstance(target_validated_bundle_fingerprint, str)
            or not target_validated_bundle_fingerprint
            or not isinstance(migration_route, Sequence)
            or isinstance(migration_route, str | bytes)
            or not all(isinstance(item, str) and item for item in migration_route)
            or not isinstance(maintenance_mode, bool)
        ):
            raise ArtifactError("invalid_migration_request")
        route = list(migration_route)
        if len(route) > limits.maximum_chain_length:
            raise ArtifactError("migration_resource_limit_exceeded")
        restored = restore_aggregate(source_copy, artifact_resolver)
        if not route:
            if (
                restored.aggregate_envelope["validated_bundle_fingerprint"]
                != target_validated_bundle_fingerprint
            ):
                raise ArtifactError("migration_route_missing")
            return MigrationResult(
                copy.deepcopy(restored.aggregate_envelope), source_copy, (), None
            )
        if len(set(route)) != len(route):
            raise ArtifactError("migration_route_mismatch")
        descriptors_with_bytes = [
            _resolve_descriptor(artifact_resolver, digest) for digest in route
        ]
        descriptors = [item[0] for item in descriptors_with_bytes]
        if (
            descriptors[0]["source_validated_bundle_fingerprint"]
            != restored.bundle.fingerprint
            or descriptors[-1]["target_validated_bundle_fingerprint"]
            != target_validated_bundle_fingerprint
            or any(
                left["target_validated_bundle_fingerprint"]
                != right["source_validated_bundle_fingerprint"]
                for left, right in zip(descriptors, descriptors[1:], strict=False)
            )
        ):
            raise ArtifactError("migration_route_mismatch")
        fingerprints = [descriptors[0]["source_validated_bundle_fingerprint"]] + [
            descriptor["target_validated_bundle_fingerprint"]
            for descriptor in descriptors
        ]
        if len(set(fingerprints)) != len(fingerprints):
            raise ArtifactError("migration_route_mismatch")
        definitions = [
            _bundle_from_resolver(
                artifact_resolver,
                fingerprint,
                source=index == 0,
                require_trust=True,
            )
            for index, fingerprint in enumerate(fingerprints)
        ]
        _check_shape_limits(
            restored.aggregate_envelope, definitions, descriptors, limits
        )
        candidate = copy.deepcopy(restored.aggregate_envelope)
        audits: list[dict[str, Any]] = []
        for index, descriptor in enumerate(descriptors):
            before_digest = candidate["aggregate_state_digest"]
            candidate = _apply_descriptor(
                candidate,
                definitions[index],
                definitions[index + 1],
                descriptor,
                artifact_resolver,
                limits,
                maintenance_mode,
            )
            audits.append(
                {
                    "migration_audit_record_schema_version": 1,
                    "root_instance_id": candidate["root_instance_id"],
                    "root_runtime_id": candidate["root_runtime_id"],
                    "migration_sequence": candidate["migration_sequence"],
                    "source_validated_bundle_fingerprint": descriptor[
                        "source_validated_bundle_fingerprint"
                    ],
                    "target_validated_bundle_fingerprint": descriptor[
                        "target_validated_bundle_fingerprint"
                    ],
                    "migration_descriptor_digest": descriptor[
                        "migration_descriptor_digest"
                    ],
                    "source_aggregate_state_digest": before_digest,
                    "target_aggregate_state_digest": candidate[
                        "aggregate_state_digest"
                    ],
                    "result_code": "migration_applied",
                }
            )
        encoded = canonical_bytes(candidate)
        return MigrationResult(candidate, encoded, tuple(audits), None)
    except ArtifactError as exc:
        return _failure(exc.code)
    except (CelError, KeyError, TypeError, ValueError) as exc:
        del exc
        return _failure("invalid_migration_descriptor")


def migrate_and_dispatch(
    aggregate: ArtifactSource,
    target_validated_bundle_fingerprint: str,
    migration_route: Sequence[str],
    artifact_resolver: ArtifactResolver,
    delivery: Delivery,
    *,
    maintenance_mode: bool,
    resource_limits: MigrationLimits | None = None,
) -> MigrationDispatchResult:
    """Migrate and dispatch as one commit-ready pure result boundary."""
    migrated = migrate_aggregate(
        aggregate,
        target_validated_bundle_fingerprint,
        migration_route,
        artifact_resolver,
        maintenance_mode=maintenance_mode,
        resource_limits=resource_limits,
    )
    if migrated.failure is not None:
        return _dispatch_failure(migrated.failure.code)
    assert migrated.aggregate_bytes is not None
    try:
        restored = restore_aggregate(migrated.aggregate_bytes, artifact_resolver)
        core = dispatch(restored.bundle, restored.state, delivery)
        state = core["state"]
        if state is None:
            raise ArtifactError("invalid_aggregate_state")
        from .wire import aggregate_envelope

        envelope = aggregate_envelope(restored.bundle, state)
        encoded = canonical_bytes(envelope)
        return MigrationDispatchResult(
            envelope,
            encoded,
            migrated.audit_records,
            core["status"],
            core["disposition"],
            tuple(copy.deepcopy(core["emissions"])),
            copy.deepcopy(core["fault"]),
            copy.deepcopy(core["rejection"]),
            None,
        )
    except ArtifactError as exc:
        return _dispatch_failure(exc.code)
