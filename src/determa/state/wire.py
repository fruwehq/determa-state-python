"""Portable aggregate artifacts, canonical values, and definition resolution."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import struct
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import cache, lru_cache
from pathlib import Path
from typing import Any, Protocol, cast

import rfc8785

from .definition import Bundle, BundleSource, _escape_pointer, load_bundle
from .errors import ArtifactError, ValidationError
from .model import BundleModel, MachineModel, StateNode
from .yaml12 import validate_portable_values, validate_unicode

ArtifactSource = bytes | str | Mapping[str, Any]
SHA256_PREFIX = "sha256:"
_DATA = Path(__file__).parent / "data"
_INT_MIN = -(2**63)
_INT_MAX = 2**63 - 1
_MAX_DECIMAL_DIGITS = 4096


class DefinitionResolver(Protocol):
    """Resolve one trusted normalized definition by its content fingerprint."""

    def resolve_definition(self, fingerprint: str) -> Bundle | BundleSource | None: ...

    def definition_is_trusted(self, fingerprint: str) -> bool: ...


class MigrationDescriptorResolver(Protocol):
    """Resolve one trusted migration descriptor by digest."""

    def resolve_migration_descriptor(self, digest: str) -> ArtifactSource | None: ...

    def migration_descriptor_is_trusted(self, digest: str) -> bool: ...


class ArtifactResolver(DefinitionResolver, MigrationDescriptorResolver, Protocol):
    """Resolve definitions and migration descriptors."""


@dataclass(frozen=True)
class RestoredAggregate:
    """One verified aggregate and its resolved current definition."""

    bundle: Bundle
    state: dict[str, Any]
    aggregate_envelope: dict[str, Any]
    canonical_bytes: bytes
    source_bytes: bytes


@dataclass(frozen=True)
class RestoredAggregatePackage:
    """A verified package after idempotently seeding its resolver."""

    aggregate: RestoredAggregate
    migration_route: tuple[str, ...]
    package_document: dict[str, Any]


class MemoryArtifactResolver:
    """Small deterministic resolver suitable for local caches and tests."""

    def __init__(
        self,
        *,
        definitions: Mapping[str, Bundle | BundleSource] | None = None,
        migration_descriptors: Mapping[str, ArtifactSource] | None = None,
        trusted_definitions: Sequence[str] | None = None,
        trusted_migration_descriptors: Sequence[str] | None = None,
    ) -> None:
        self._definitions = dict(definitions or {})
        self._migration_descriptors = dict(migration_descriptors or {})
        self._trusted_definitions = set(
            self._definitions if trusted_definitions is None else trusted_definitions
        )
        self._trusted_migration_descriptors = set(
            self._migration_descriptors
            if trusted_migration_descriptors is None
            else trusted_migration_descriptors
        )

    def resolve_definition(self, fingerprint: str) -> Bundle | BundleSource | None:
        return self._definitions.get(fingerprint)

    def definition_is_trusted(self, fingerprint: str) -> bool:
        return fingerprint in self._trusted_definitions

    def resolve_migration_descriptor(self, digest: str) -> ArtifactSource | None:
        return self._migration_descriptors.get(digest)

    def migration_descriptor_is_trusted(self, digest: str) -> bool:
        return digest in self._trusted_migration_descriptors

    def put_definition(
        self, fingerprint: str, definition: Bundle | BundleSource, *, trusted: bool = True
    ) -> None:
        bundle = definition if isinstance(definition, Bundle) else load_bundle(definition)
        if bundle.fingerprint != fingerprint:
            raise ArtifactError("definition_fingerprint_mismatch")
        existing = self._definitions.get(fingerprint)
        if existing is not None:
            current = existing if isinstance(existing, Bundle) else load_bundle(existing)
            if canonical_bytes(typed_value(current.raw)) != canonical_bytes(
                typed_value(bundle.raw)
            ):
                raise ArtifactError("definition_fingerprint_mismatch")
        else:
            self._definitions[fingerprint] = bundle
        if trusted:
            self._trusted_definitions.add(fingerprint)

    def put_migration_descriptor(
        self, digest: str, descriptor: ArtifactSource, *, trusted: bool = True
    ) -> None:
        document, _ = load_json_artifact(descriptor, "migration_descriptor")
        if migration_descriptor_digest(document) != digest:
            raise ArtifactError("invalid_migration_descriptor")
        existing = self._migration_descriptors.get(digest)
        if existing is not None:
            current, _ = load_json_artifact(existing, "migration_descriptor")
            if canonical_bytes(current) != canonical_bytes(document):
                raise ArtifactError("invalid_migration_descriptor")
        else:
            self._migration_descriptors[digest] = copy.deepcopy(document)
        if trusted:
            self._trusted_migration_descriptors.add(digest)

    def snapshot(self) -> dict[str, list[str]]:
        return {
            "definitions": sorted(self._definitions),
            "migration_descriptors": sorted(self._migration_descriptors),
        }


def _reject_constant(value: str) -> None:
    raise ArtifactError("invalid_json_value", message=f"invalid JSON constant {value}")


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ArtifactError("duplicate_json_name")
        result[key] = value
    return result


def _source_bytes(source: ArtifactSource) -> bytes:
    if isinstance(source, bytes):
        return source
    if isinstance(source, str):
        return source.encode("utf-8", errors="strict")
    try:
        validate_portable_values(source)
    except ValidationError as exc:
        raise ArtifactError("invalid_json_value") from exc
    if not validate_unicode(source):
        raise ArtifactError("invalid_unicode")
    return canonical_bytes(copy.deepcopy(dict(source)))


def strict_json(source: ArtifactSource) -> tuple[Any, bytes]:
    """Parse strict UTF-8 JSON while rejecting duplicate names and invalid values."""
    raw = _source_bytes(source)
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ArtifactError("invalid_unicode") from exc
    try:
        value = json.loads(
            text,
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
        )
    except ArtifactError:
        raise
    except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise ArtifactError("invalid_json_value") from exc
    try:
        validate_portable_values(value)
    except ValidationError as exc:
        raise ArtifactError("invalid_json_value") from exc
    if not validate_unicode(value):
        raise ArtifactError("invalid_unicode")
    return value, raw


def canonical_bytes(value: Any) -> bytes:
    """Return exact RFC 8785 bytes for a portable JSON value."""
    try:
        return bytes(rfc8785.dumps(value))
    except (rfc8785.CanonicalizationError, UnicodeError, ValueError, TypeError) as exc:
        raise ArtifactError("invalid_json_value") from exc


def hash_value(value: Any) -> str:
    return f"{SHA256_PREFIX}{hashlib.sha256(canonical_bytes(value)).hexdigest()}"


def typed_value(value: Any) -> list[Any]:
    """Project one Determa value into the exact tagged portable representation."""
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["boolean", value]
    if isinstance(value, str):
        if not validate_unicode(value):
            raise ArtifactError("invalid_aggregate_state")
        return ["string", value]
    if isinstance(value, int):
        if not _INT_MIN <= value <= _INT_MAX:
            raise ArtifactError("invalid_aggregate_state")
        return ["integer", str(value)]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ArtifactError("invalid_aggregate_state")
        return ["float", struct.pack("!d", 0.0 if value == 0.0 else value).hex()]
    if isinstance(value, list):
        return ["list", [typed_value(item) for item in value]]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ArtifactError("invalid_aggregate_state")
        entries = [
            [key, typed_value(value[key])]
            for key in sorted(value, key=lambda item: item.encode("utf-8"))
        ]
        return ["map", entries]
    raise ArtifactError("invalid_aggregate_state")


def decoded_typed_value(value: Any) -> Any:
    """Decode one exact typed value, rejecting noncanonical representations."""
    if not isinstance(value, list) or not value or not isinstance(value[0], str):
        raise ArtifactError("invalid_aggregate_state")
    tag = value[0]
    if tag == "null" and value == ["null"]:
        return None
    if len(value) != 2:
        raise ArtifactError("invalid_aggregate_state")
    payload = value[1]
    if tag == "boolean" and isinstance(payload, bool):
        return payload
    if tag == "string" and isinstance(payload, str) and validate_unicode(payload):
        return payload
    if tag == "integer" and isinstance(payload, str):
        integer = _signed_decimal(payload)
        if _INT_MIN <= integer <= _INT_MAX:
            return integer
    if tag == "float" and isinstance(payload, str) and len(payload) == 16:
        try:
            number = struct.unpack("!d", bytes.fromhex(payload))[0]
        except (ValueError, struct.error):
            pass
        else:
            if payload == payload.lower() and math.isfinite(number):
                return 0.0 if number == 0.0 else number
    if tag == "list" and isinstance(payload, list):
        return [decoded_typed_value(item) for item in payload]
    if tag == "map" and isinstance(payload, list):
        result: dict[str, Any] = {}
        previous: bytes | None = None
        for entry in payload:
            if (
                not isinstance(entry, list)
                or len(entry) != 2
                or not isinstance(entry[0], str)
                or not validate_unicode(entry[0])
            ):
                raise ArtifactError("invalid_aggregate_state")
            encoded = entry[0].encode("utf-8")
            if previous is not None and encoded <= previous:
                raise ArtifactError("invalid_aggregate_state")
            previous = encoded
            result[entry[0]] = decoded_typed_value(entry[1])
        return result
    raise ArtifactError("invalid_aggregate_state")


def _signed_decimal(value: str) -> int:
    if value == "0":
        return 0
    negative = value.startswith("-")
    digits = value[1:] if negative else value
    if (
        not digits
        or len(digits) > _MAX_DECIMAL_DIGITS
        or not digits.isascii()
        or not digits.isdigit()
        or digits.startswith("0")
    ):
        raise ArtifactError("invalid_aggregate_state")
    try:
        return int(value)
    except ValueError as exc:
        raise ArtifactError("invalid_aggregate_state") from exc


def decimal(value: Any, *, positive: bool = False) -> int:
    if not isinstance(value, str):
        raise ArtifactError("invalid_aggregate_state")
    number = _signed_decimal(value)
    if number < 0 or (positive and number == 0):
        raise ArtifactError("invalid_aggregate_state")
    return number


@cache
def artifact_schema(kind: str) -> dict[str, Any]:
    filename = {
        "aggregate_state": "aggregate-state.schema.json",
        "migration_descriptor": "migration-descriptor.schema.json",
        "aggregate_state_package": "aggregate-state-package.schema.json",
        "execution_checkpoint": "execution-checkpoint.schema.json",
    }[kind]
    return cast(
        dict[str, Any], json.loads((_DATA / filename).read_text(encoding="utf-8"))
    )


@lru_cache(maxsize=1)
def _schema_registry() -> Any:
    from referencing import Registry, Resource

    registry = Registry()
    for kind in (
        "aggregate_state",
        "migration_descriptor",
        "aggregate_state_package",
        "execution_checkpoint",
    ):
        document = artifact_schema(kind)
        registry = registry.with_resource(
            document["$id"], Resource.from_contents(document)
        )
    return registry


def _format_code(document: Any, kind: str) -> str | None:
    if not isinstance(document, dict):
        return None
    definitions = {
        "aggregate_state": (
            "aggregate_state_format",
            "determa.aggregate_state",
            "aggregate_state_schema_version",
            1,
            "unsupported_aggregate_state_format",
            "unsupported_aggregate_state_schema_version",
        ),
        "migration_descriptor": (
            "migration_descriptor_format",
            "determa.aggregate_migration",
            "migration_descriptor_schema_version",
            1,
            "unsupported_migration_descriptor_format",
            "unsupported_migration_descriptor_schema_version",
        ),
        "aggregate_state_package": (
            "aggregate_state_package_format",
            "determa.aggregate_state_package",
            "aggregate_state_package_schema_version",
            1,
            "unsupported_aggregate_state_package_format",
            "unsupported_aggregate_state_package_schema_version",
        ),
        "execution_checkpoint": (
            "execution_checkpoint_format",
            "determa.execution_checkpoint",
            "execution_checkpoint_schema_version",
            1,
            "unsupported_execution_checkpoint_format",
            "unsupported_execution_checkpoint_schema_version",
        ),
    }
    format_member, expected_format, version_member, expected_version, format_code, version_code = (
        definitions[kind]
    )
    if format_member in document and document[format_member] != expected_format:
        return format_code
    if version_member in document and document[version_member] != expected_version:
        return version_code
    return None


def load_json_artifact(
    source: ArtifactSource, kind: str
) -> tuple[dict[str, Any], bytes]:
    """Parse and structurally validate one recognized persistence artifact."""
    try:
        document, raw = strict_json(source)
    except ArtifactError as exc:
        code = {
            "aggregate_state": "invalid_aggregate_state",
            "migration_descriptor": "invalid_migration_descriptor",
            "aggregate_state_package": "invalid_aggregate_state_package",
            "execution_checkpoint": "invalid_execution_checkpoint",
        }[kind]
        raise ArtifactError(code) from exc
    unsupported = _format_code(document, kind)
    if unsupported is not None:
        raise ArtifactError(unsupported)
    import jsonschema

    validator = jsonschema.Draft202012Validator(
        artifact_schema(kind), registry=_schema_registry()
    )
    if not isinstance(document, dict) or next(validator.iter_errors(document), None) is not None:
        code = {
            "aggregate_state": "invalid_aggregate_state",
            "migration_descriptor": "invalid_migration_descriptor",
            "aggregate_state_package": "invalid_aggregate_state_package",
            "execution_checkpoint": "invalid_execution_checkpoint",
        }[kind]
        raise ArtifactError(code)
    return document, raw


def aggregate_state_digest(document: Mapping[str, Any]) -> str:
    body = dict(document)
    body.pop("aggregate_state_digest", None)
    return hash_value(["determa-aggregate-state-digest-1", body])


def migration_descriptor_digest(document: Mapping[str, Any]) -> str:
    body = dict(document)
    body.pop("migration_descriptor_digest", None)
    return hash_value(["determa-migration-descriptor-1", body])


def normalized_definition_attachment(bundle: Bundle) -> dict[str, Any]:
    return {
        "validated_bundle_fingerprint": bundle.fingerprint,
        "normalized_bundle": typed_value(bundle.raw),
    }


def bundle_from_attachment(attachment: Mapping[str, Any]) -> Bundle:
    try:
        raw = decoded_typed_value(attachment["normalized_bundle"])
        fingerprint = attachment["validated_bundle_fingerprint"]
    except (ArtifactError, KeyError, TypeError) as exc:
        raise ArtifactError("invalid_aggregate_state_package") from exc
    if not isinstance(raw, dict) or not isinstance(fingerprint, str):
        raise ArtifactError("invalid_aggregate_state_package")
    bundle = load_bundle(raw)
    if bundle.fingerprint != fingerprint:
        raise ArtifactError("invalid_aggregate_state_package")
    return bundle


def _bundle_from_resolver(
    resolver: DefinitionResolver,
    fingerprint: str,
    *,
    source: bool,
    require_trust: bool = True,
) -> Bundle:
    definition = resolver.resolve_definition(fingerprint)
    if definition is None:
        raise ArtifactError(
            "source_definition_unavailable" if source else "target_definition_unavailable"
        )
    if require_trust and not resolver.definition_is_trusted(fingerprint):
        raise ArtifactError("definition_untrusted")
    try:
        bundle = definition if isinstance(definition, Bundle) else load_bundle(definition)
    except ValidationError as exc:
        raise ArtifactError("definition_fingerprint_mismatch") from exc
    if bundle.fingerprint != fingerprint:
        raise ArtifactError("definition_fingerprint_mismatch")
    return bundle


def _definition_binding(bundle: Bundle, runtime: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "validated_bundle_fingerprint": bundle.fingerprint,
        "machine": {
            "namespace": bundle.namespace,
            "machine_id": runtime["machine_id"],
            "machine_version": str(runtime["machine_version"]),
            "root_definition_pointer": runtime["root_pointer"],
        },
    }


def _wire_target(target: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(target))
    if "component" in result:
        result["component"]["activation_sequence"] = str(
            result["component"]["activation_sequence"]
        )
    elif "spawned_instance" in result:
        result["spawned_instance"]["machine_version"] = str(
            result["spawned_instance"]["machine_version"]
        )
    return result


def _runtime_identity_origin(
    bundle: Bundle, runtime: Mapping[str, Any]
) -> dict[str, Any]:
    stored = runtime.get("_identity_origin")
    if isinstance(stored, dict):
        return copy.deepcopy(stored)
    definition = _definition_binding(bundle, runtime)
    role = runtime["role"]
    if role == "root":
        return {
            "kind": "root",
            "definition": definition,
            "root_instance_id": runtime.get("_root_instance_id"),
        }
    if role == "component":
        return {
            "kind": "component",
            "definition": definition,
            "owner_runtime_id": runtime["owner_runtime_id"],
            "component_definition_pointer": runtime["component_definition_pointer"],
            "activation_sequence": str(runtime["component_activation_sequence"]),
            "declaration_index": str(runtime["component_declaration_index"]),
        }
    return {
        "kind": "owned_spawned_instance",
        "definition": definition,
        "owner_runtime_id": runtime["owner_runtime_id"],
        "spawn_action_pointer": runtime["spawn_action_pointer"],
        "spawn_sequence": str(runtime["spawn_sequence"]),
    }


def _runtime_relation(runtime: Mapping[str, Any]) -> dict[str, Any]:
    stored = runtime.get("_relation")
    if isinstance(stored, dict):
        return copy.deepcopy(stored)
    if runtime["role"] == "root":
        return {"kind": "root"}
    if runtime["role"] == "component":
        return {
            "kind": "component",
            "owner_runtime_id": runtime["owner_runtime_id"],
            "component_id": runtime["component_id"],
            "current_component_definition_pointer": runtime[
                "component_definition_pointer"
            ],
            "activation_sequence": str(runtime["component_activation_sequence"]),
            "declaration_index": str(runtime["component_declaration_index"]),
        }
    holder = runtime.get("holder")
    lifetime_holder = (
        None
        if holder is None
        else {
            "holder_runtime_id": runtime["owner_runtime_id"],
            "variable_declaration_pointer": holder["pointer"],
            "holder_state_activation_sequence": str(
                holder["state_activation_sequence"]
            ),
        }
    )
    return {
        "kind": "owned_spawned_instance",
        "owner_runtime_id": runtime["owner_runtime_id"],
        "current_spawn_action_pointer": runtime["spawn_action_pointer"],
        "spawn_sequence": str(runtime["spawn_sequence"]),
        "lifetime_holder": lifetime_holder,
    }


def _node_for_runtime(machine: MachineModel, path: str) -> StateNode:
    try:
        return machine.states[path]
    except KeyError as exc:
        raise ArtifactError("invalid_aggregate_state") from exc


def _runtime_wire(
    bundle: Bundle,
    models: BundleModel,
    aggregate: Mapping[str, Any],
    runtime: Mapping[str, Any],
) -> dict[str, Any]:
    from .engine import _runtime_model

    machine = _runtime_model(bundle, models, cast(dict[str, Any], runtime))
    active_nodes = [_node_for_runtime(machine, path) for path in runtime["active"]]
    leaves = (
        []
        if not active_nodes
        else [active_nodes[-1].pointer]
    )
    activations = sorted(
        (
            {
                "state_definition_pointer": _node_for_runtime(machine, path).pointer,
                "activation_sequence": str(sequence),
            }
            for path, sequence in runtime["state_activation_sequence"].items()
        ),
        key=lambda item: (
            item["state_definition_pointer"].encode("utf-8"),
            int(item["activation_sequence"]),
        ),
    )
    variables: list[dict[str, Any]] = []
    for path, values in runtime["scopes"].items():
        node = _node_for_runtime(machine, path)
        activation = runtime["state_activation_sequence"][path]
        for name, value in values.items():
            variables.append(
                {
                    "variable_declaration_pointer": (
                        f"{node.pointer}/variables/{_escape_pointer(name)}"
                    ),
                    "declaring_state_activation_sequence": str(activation),
                    "value": typed_value(value),
                }
            )
    variables.sort(
        key=lambda item: (
            item["variable_declaration_pointer"].encode("utf-8"),
            int(item["declaring_state_activation_sequence"]),
        )
    )
    history: list[dict[str, Any]] = []
    for path, recorded in runtime["history"].items():
        node = machine.root if path == "$root" else _node_for_runtime(machine, path)
        pointers = (
            None
            if recorded is None
            else sorted(
                (_node_for_runtime(machine, item).pointer for item in recorded),
                key=lambda item: item.encode("utf-8"),
            )
        )
        history.append(
            {
                "history_declaration_pointer": f"{node.pointer}/history",
                "recorded_state_definition_pointers": pointers,
            }
        )
    history.sort(key=lambda item: item["history_declaration_pointer"].encode("utf-8"))
    state_counters = sorted(
        (
            {
                "definition_pointer": _node_for_runtime(machine, path).pointer,
                "next_sequence": str(sequence),
            }
            for path, sequence in runtime["next_state_activation_sequence"].items()
        ),
        key=lambda item: item["definition_pointer"].encode("utf-8"),
    )
    component_counters = sorted(
        (
            {"definition_pointer": pointer, "next_sequence": str(sequence)}
            for pointer, sequence in runtime["next_component_activation_sequence"].items()
        ),
        key=lambda item: item["definition_pointer"].encode("utf-8"),
    )
    target = runtime.get("_target_identity")
    if not isinstance(target, dict):
        if runtime["role"] == "root":
            target = {
                "root": {
                    "root_instance_id": aggregate["root_instance_id"],
                    "root_runtime_id": runtime["runtime_id"],
                }
            }
        elif runtime["role"] == "component":
            target = runtime["target"]
        else:
            target = {"spawned_instance": runtime["instance_reference"]}
    current = runtime.get("_current_definition")
    if not isinstance(current, dict):
        current = _definition_binding(bundle, runtime)
    fault = runtime["fault"]
    if fault is not None:
        fault = copy.deepcopy(fault)
        fault["step_sequence"] = str(fault["step_sequence"])
        fault["definition_fingerprint"] = runtime.get(
            "_fault_definition_fingerprint", current["validated_bundle_fingerprint"]
        )
    origin = _runtime_identity_origin(bundle, runtime)
    if origin["kind"] == "root" and origin.get("root_instance_id") is None:
        origin["root_instance_id"] = aggregate["root_instance_id"]
    return {
        "runtime_id": runtime["runtime_id"],
        "identity_origin": origin,
        "target_identity": _wire_target(target),
        "current_definition": copy.deepcopy(current),
        "relation": _runtime_relation(runtime),
        "status": runtime["status"],
        "active_leaf_state_definition_pointers": leaves,
        "active_state_activations": activations,
        "variables": variables,
        "history": history,
        "next_spawn_sequence": str(runtime["next_spawn_sequence"]),
        "next_state_activation_sequences": state_counters,
        "next_component_activation_sequences": component_counters,
        "fault": fault,
    }


def aggregate_envelope(bundle: Bundle | BundleSource, state: dict[str, Any]) -> dict[str, Any]:
    """Project one validated engine aggregate into the neutral wire envelope."""
    validated = bundle if isinstance(bundle, Bundle) else load_bundle(bundle)
    from .engine import _valid_prior_state

    if not _valid_prior_state(state, validated):
        raise ArtifactError("invalid_aggregate_state")
    models = BundleModel(validated)
    root = state["runtimes"][state["root_runtime_id"]]
    restored_order = state.get("_wire_runtime_order")
    order_rank = (
        {runtime_id: index for index, runtime_id in enumerate(restored_order)}
        if isinstance(restored_order, list)
        and set(restored_order) == set(state["runtimes"])
        else None
    )
    document: dict[str, Any] = {
        "aggregate_state_format": "determa.aggregate_state",
        "aggregate_state_schema_version": 1,
        "machine_format": 1,
        "validated_bundle_fingerprint": validated.fingerprint,
        "namespace": validated.namespace,
        "root_machine_id": root["machine_id"],
        "root_machine_version": str(root["machine_version"]),
        "root_instance_id": state["root_instance_id"],
        "creation_id": state["creation_id"],
        "root_runtime_id": state["root_runtime_id"],
        "migration_sequence": str(state.get("migration_sequence", 0)),
        "next_logical_step_sequence": str(state["next_logical_step_sequence"]),
        "next_output_sequence": str(state["next_output_sequence"]),
        "runtimes": sorted(
            (
                _runtime_wire(validated, models, state, runtime)
                for runtime in state["runtimes"].values()
            ),
            key=(
                (lambda item: order_rank[item["runtime_id"]])
                if order_rank is not None
                else (lambda item: item["runtime_id"].encode("utf-8"))
            ),
        ),
    }
    document["aggregate_state_digest"] = aggregate_state_digest(document)
    return document


def serialize_aggregate(
    bundle: Bundle | BundleSource, state: dict[str, Any]
) -> bytes:
    """Serialize one engine aggregate to exact canonical portable bytes."""
    return canonical_bytes(aggregate_envelope(bundle, state))


def _state_path_for_pointer(machine: MachineModel, pointer: str) -> str:
    for path, node in machine.states.items():
        if node.pointer == pointer:
            return path
    raise ArtifactError("invalid_aggregate_state")


def _variable_for_pointer(
    machine: MachineModel, pointer: str
) -> tuple[str, str, dict[str, Any]]:
    for path, node in machine.states.items():
        prefix = f"{node.pointer}/variables/"
        if pointer.startswith(prefix):
            for name, declaration in (node.raw.get("variables") or {}).items():
                if pointer == f"{prefix}{_escape_pointer(name)}":
                    return path, name, declaration
    raise ArtifactError("invalid_aggregate_state")


def _history_path_for_pointer(machine: MachineModel, pointer: str) -> str:
    for path, node in machine.states.items():
        if pointer == f"{node.pointer}/history":
            return "$root" if node is machine.root else path
    raise ArtifactError("invalid_aggregate_state")


def _machine_for_binding(
    bundle: Bundle, models: BundleModel, binding: Mapping[str, Any]
) -> MachineModel:
    machine_data = binding["machine"]
    machine_id = machine_data["machine_id"]
    base = models.machine(machine_id)
    if (
        machine_data["namespace"] != bundle.namespace
        or decimal(machine_data["machine_version"], positive=True) != base.version
    ):
        raise ArtifactError("invalid_aggregate_state")
    root_pointer = machine_data["root_definition_pointer"]
    if root_pointer == base.root_pointer:
        return base
    from .engine import _pointer_get

    root = _pointer_get(bundle.raw, root_pointer)
    return MachineModel(
        bundle,
        base.raw,
        machine_index=base.machine_index,
        root=root,
        root_pointer=root_pointer,
        identity_machine=base.identity_machine,
    )


def _origin_machine(
    resolver: DefinitionResolver, origin: Mapping[str, Any]
) -> tuple[Bundle, MachineModel]:
    definition = origin.get("definition")
    if not isinstance(definition, Mapping):
        raise ArtifactError("invalid_aggregate_state")
    fingerprint = definition.get("validated_bundle_fingerprint")
    if not isinstance(fingerprint, str):
        raise ArtifactError("invalid_aggregate_state")
    bundle = _bundle_from_resolver(resolver, fingerprint, source=True, require_trust=True)
    return bundle, _machine_for_binding(bundle, BundleModel(bundle), definition)


def _validate_immutable_identity(
    resolver: DefinitionResolver,
    aggregate: Mapping[str, Any],
    document: Mapping[str, Any],
    role: str,
    target: Mapping[str, Any],
) -> None:
    origin = document["identity_origin"]
    if not isinstance(origin, Mapping) or origin.get("kind") != {
        "root": "root",
        "component": "component",
        "spawned": "owned_spawned_instance",
    }[role]:
        raise ArtifactError("invalid_aggregate_state")
    origin_bundle, origin_machine = _origin_machine(resolver, origin)
    root_instance_id = aggregate["root_instance_id"]
    runtime_id = document["runtime_id"]
    if role == "root":
        if (
            origin.get("root_instance_id") != root_instance_id
            or target
            != {
                "root": {
                    "root_instance_id": root_instance_id,
                    "root_runtime_id": runtime_id,
                }
            }
        ):
            raise ArtifactError("invalid_aggregate_state")
        return
    if role == "component":
        component = target.get("component")
        if not isinstance(component, Mapping):
            raise ArtifactError("invalid_aggregate_state")
        pointer = origin.get("component_definition_pointer")
        if not isinstance(pointer, str):
            raise ArtifactError("invalid_aggregate_state")
        try:
            from .engine import _pointer_get

            placement = _pointer_get(origin_bundle.raw, pointer)
            declaration_index = int(pointer.rsplit("/", 1)[1])
        except (IndexError, KeyError, TypeError, ValueError):
            raise ArtifactError("invalid_aggregate_state") from None
        if (
            not isinstance(placement, dict)
            or decimal(origin.get("declaration_index")) != declaration_index
            or component
            != {
                "root_instance_id": root_instance_id,
                "owner_runtime_id": origin.get("owner_runtime_id"),
                "component_id": placement.get("component_id"),
                "component_runtime_id": runtime_id,
                "activation_sequence": decimal(origin.get("activation_sequence")),
            }
        ):
            raise ArtifactError("invalid_aggregate_state")
        return
    spawned = target.get("spawned_instance")
    if (
        not isinstance(spawned, Mapping)
        or spawned
        != {
            "root_instance_id": root_instance_id,
            "instance_id": runtime_id,
            "machine_id": origin_machine.machine_id,
            "machine_version": origin_machine.version,
        }
    ):
        raise ArtifactError("invalid_aggregate_state")


def _runtime_from_wire(
    bundle: Bundle,
    models: BundleModel,
    resolver: DefinitionResolver,
    aggregate: Mapping[str, Any],
    document: Mapping[str, Any],
) -> dict[str, Any]:
    current = document["current_definition"]
    if current["validated_bundle_fingerprint"] != bundle.fingerprint:
        raise ArtifactError("invalid_aggregate_state")
    machine = _machine_for_binding(bundle, models, current)
    relation = document["relation"]
    role = {
        "root": "root",
        "component": "component",
        "owned_spawned_instance": "spawned",
    }[relation["kind"]]
    activations: dict[str, int] = {}
    for item in document["active_state_activations"]:
        path = _state_path_for_pointer(machine, item["state_definition_pointer"])
        if path in activations:
            raise ArtifactError("invalid_aggregate_state")
        activations[path] = decimal(item["activation_sequence"])
    leaf_pointers = document["active_leaf_state_definition_pointers"]
    if len(leaf_pointers) > 1:
        raise ArtifactError("invalid_aggregate_state")
    active: list[str] = []
    if leaf_pointers:
        leaf = machine.states[_state_path_for_pointer(machine, leaf_pointers[0])]
        active = [node.path for node in reversed(leaf.ancestors(include_self=True))]
    if set(active) != set(activations):
        raise ArtifactError("invalid_aggregate_state")
    scopes: dict[str, dict[str, Any]] = {path: {} for path in active}
    for item in document["variables"]:
        path, name, declaration = _variable_for_pointer(
            machine, item["variable_declaration_pointer"]
        )
        if path not in scopes or name in scopes[path]:
            raise ArtifactError("invalid_aggregate_state")
        if decimal(item["declaring_state_activation_sequence"]) != activations[path]:
            raise ArtifactError("invalid_aggregate_state")
        value = decoded_typed_value(item["value"])
        from .engine import _value_matches

        if not _value_matches(value, str(declaration["type"])):
            raise ArtifactError("invalid_aggregate_state")
        scopes[path][name] = value
    for path in active:
        declarations = machine.states[path].raw.get("variables") or {}
        if set(scopes[path]) != set(declarations):
            raise ArtifactError("invalid_aggregate_state")
    history: dict[str, list[str] | None] = {}
    for item in document["history"]:
        path = _history_path_for_pointer(machine, item["history_declaration_pointer"])
        if path in history:
            raise ArtifactError("invalid_aggregate_state")
        recorded = item["recorded_state_definition_pointers"]
        history[path] = (
            None
            if recorded is None
            else [_state_path_for_pointer(machine, pointer) for pointer in recorded]
        )
    state_counters: dict[str, int] = {}
    for item in document["next_state_activation_sequences"]:
        path = _state_path_for_pointer(machine, item["definition_pointer"])
        if path in state_counters:
            raise ArtifactError("invalid_aggregate_state")
        state_counters[path] = decimal(item["next_sequence"])
    component_counters: dict[str, int] = {}
    for item in document["next_component_activation_sequences"]:
        pointer = item["definition_pointer"]
        if pointer in component_counters:
            raise ArtifactError("invalid_aggregate_state")
        component_counters[pointer] = decimal(item["next_sequence"])
    target = copy.deepcopy(document["target_identity"])
    if "component" in target:
        target["component"]["activation_sequence"] = decimal(
            target["component"]["activation_sequence"]
        )
    elif "spawned_instance" in target:
        target["spawned_instance"]["machine_version"] = decimal(
            target["spawned_instance"]["machine_version"], positive=True
        )
    _validate_immutable_identity(resolver, aggregate, document, role, target)
    runtime: dict[str, Any] = {
        "runtime_id": document["runtime_id"],
        "role": role,
        "owner_runtime_id": relation.get("owner_runtime_id"),
        "machine_id": machine.machine_id,
        "machine_version": machine.version,
        "root_pointer": machine.root_pointer,
        "status": document["status"],
        "active": active,
        "scopes": scopes,
        "history": history,
        "fault": None,
        "next_spawn_sequence": decimal(document["next_spawn_sequence"]),
        "next_state_activation_sequence": state_counters,
        "state_activation_sequence": activations,
        "next_component_activation_sequence": component_counters,
        "components": {},
        "_identity_origin": copy.deepcopy(document["identity_origin"]),
        "_target_identity": copy.deepcopy(target),
        "_current_definition": copy.deepcopy(current),
        "_relation": copy.deepcopy(relation),
    }
    if role == "component":
        runtime.update(
            {
                "component_id": relation["component_id"],
                "component_runtime_id": document["runtime_id"],
                "component_definition_pointer": relation[
                    "current_component_definition_pointer"
                ],
                "component_declaration_index": decimal(relation["declaration_index"]),
                "component_activation_sequence": decimal(relation["activation_sequence"]),
                "owning_state_path": "",
                "owning_state_activation_sequence": 0,
                "target": target,
            }
        )
    elif role == "spawned":
        holder = relation["lifetime_holder"]
        runtime.update(
            {
                "spawn_sequence": decimal(relation["spawn_sequence"]),
                "spawn_action_pointer": relation["current_spawn_action_pointer"],
                "instance_reference": target["spawned_instance"],
                "holder": (
                    None
                    if holder is None
                    else {
                        "pointer": holder["variable_declaration_pointer"],
                        "state_path": "",
                        "state_activation_sequence": decimal(
                            holder["holder_state_activation_sequence"]
                        ),
                    }
                ),
            }
        )
    fault = document["fault"]
    if fault is not None:
        runtime["fault"] = {
            key: copy.deepcopy(value)
            for key, value in fault.items()
            if key != "definition_fingerprint"
        }
        runtime["fault"]["step_sequence"] = decimal(fault["step_sequence"])
        runtime["_fault_definition_fingerprint"] = fault["definition_fingerprint"]
    return runtime


def _finish_relationships(
    bundle: Bundle, state: dict[str, Any], models: BundleModel
) -> None:
    from .engine import _runtime_model

    for runtime in state["runtimes"].values():
        if runtime["role"] == "component":
            owner = state["runtimes"].get(runtime["owner_runtime_id"])
            if owner is None:
                raise ArtifactError("invalid_aggregate_state")
            relation = runtime["_relation"]
            owner_machine = _runtime_model(bundle, models, owner)
            pointer = relation["current_component_definition_pointer"]
            owning = next(
                (
                    node
                    for node in owner_machine.states.values()
                    if pointer.startswith(f"{node.pointer}/components/")
                ),
                None,
            )
            if owning is None or owning.path not in owner["state_activation_sequence"]:
                raise ArtifactError("invalid_aggregate_state")
            runtime["owning_state_path"] = owning.path
            runtime["owning_state_activation_sequence"] = owner[
                "state_activation_sequence"
            ][owning.path]
            if runtime["component_id"] in owner["components"]:
                raise ArtifactError("invalid_aggregate_state")
            owner["components"][runtime["component_id"]] = runtime["runtime_id"]
        elif runtime["role"] == "spawned" and runtime["holder"] is not None:
            owner = state["runtimes"].get(runtime["owner_runtime_id"])
            if owner is None:
                raise ArtifactError("invalid_aggregate_state")
            owner_machine = _runtime_model(bundle, models, owner)
            holder_pointer = runtime["holder"]["pointer"]
            path, _name, _declaration = _variable_for_pointer(
                owner_machine, holder_pointer
            )
            if path not in owner["state_activation_sequence"]:
                raise ArtifactError("invalid_aggregate_state")
            runtime["holder"]["state_path"] = path


def restore_aggregate(
    source: ArtifactSource, definition_resolver: DefinitionResolver
) -> RestoredAggregate:
    """Verify and restore one portable aggregate without changing the source."""
    document, raw = load_json_artifact(source, "aggregate_state")
    if aggregate_state_digest(document) != document["aggregate_state_digest"]:
        raise ArtifactError("aggregate_state_digest_mismatch")
    fingerprint = document["validated_bundle_fingerprint"]
    bundle = _bundle_from_resolver(
        definition_resolver, fingerprint, source=True, require_trust=True
    )
    if document["namespace"] != bundle.namespace:
        raise ArtifactError("invalid_aggregate_state")
    models = BundleModel(bundle)
    state: dict[str, Any] = {
        "validated_bundle_fingerprint": fingerprint,
        "namespace": document["namespace"],
        "root_instance_id": document["root_instance_id"],
        "creation_id": document["creation_id"],
        "root_runtime_id": document["root_runtime_id"],
        "root_machine_id": document["root_machine_id"],
        "status": "running",
        "next_logical_step_sequence": decimal(document["next_logical_step_sequence"]),
        "next_output_sequence": decimal(document["next_output_sequence"]),
        "runtimes": {},
        "fault": None,
        "migration_sequence": decimal(document["migration_sequence"]),
        "_wire_runtime_order": [
            runtime_document["runtime_id"] for runtime_document in document["runtimes"]
        ],
    }
    for runtime_document in document["runtimes"]:
        runtime = _runtime_from_wire(
            bundle, models, definition_resolver, document, runtime_document
        )
        if runtime["runtime_id"] in state["runtimes"]:
            raise ArtifactError("invalid_aggregate_state")
        state["runtimes"][runtime["runtime_id"]] = runtime
    _finish_relationships(bundle, state, models)
    root = state["runtimes"].get(state["root_runtime_id"])
    if root is None or root["role"] != "root":
        raise ArtifactError("invalid_aggregate_state")
    if (
        document["root_machine_id"] != root["machine_id"]
        or decimal(document["root_machine_version"], positive=True)
        != root["machine_version"]
        or root["_current_definition"]["machine"]["root_definition_pointer"]
        != root["root_pointer"]
    ):
        raise ArtifactError("invalid_aggregate_state")
    state["status"] = root["status"]
    state["fault"] = copy.deepcopy(root["fault"])
    from .engine import _valid_prior_state

    if not _valid_prior_state(state, bundle):
        raise ArtifactError("invalid_aggregate_state")
    canonical = canonical_bytes(document)
    return RestoredAggregate(
        bundle=bundle,
        state=state,
        aggregate_envelope=copy.deepcopy(document),
        canonical_bytes=canonical,
        source_bytes=raw,
    )


def restore_aggregate_package(
    source: ArtifactSource, artifact_resolver: ArtifactResolver
) -> RestoredAggregatePackage:
    """Verify a transport package and seed one mutable resolver atomically."""
    document, _raw = load_json_artifact(source, "aggregate_state_package")
    definitions: dict[str, Bundle] = {}
    descriptors: dict[str, dict[str, Any]] = {}
    try:
        for attachment in document["normalized_definitions"]:
            bundle = bundle_from_attachment(attachment)
            if bundle.fingerprint in definitions:
                raise ArtifactError("invalid_aggregate_state_package")
            definitions[bundle.fingerprint] = bundle
        for descriptor in document["migration_descriptors"]:
            digest = descriptor["migration_descriptor_digest"]
            if digest in descriptors or migration_descriptor_digest(descriptor) != digest:
                raise ArtifactError("invalid_aggregate_state_package")
            descriptors[digest] = copy.deepcopy(descriptor)
        route = tuple(document["migration_route"])
        if len(set(route)) != len(route):
            raise ArtifactError("invalid_aggregate_state_package")
        for fingerprint, bundle in definitions.items():
            existing_definition = artifact_resolver.resolve_definition(fingerprint)
            if existing_definition is not None:
                current_bundle = (
                    existing_definition
                    if isinstance(existing_definition, Bundle)
                    else load_bundle(existing_definition)
                )
                if (
                    current_bundle.fingerprint != fingerprint
                    or canonical_bytes(typed_value(current_bundle.raw))
                    != canonical_bytes(typed_value(bundle.raw))
                ):
                    raise ArtifactError("definition_fingerprint_mismatch")
        for digest, descriptor in descriptors.items():
            existing_descriptor = artifact_resolver.resolve_migration_descriptor(digest)
            if existing_descriptor is not None:
                current_descriptor, _ = load_json_artifact(
                    existing_descriptor, "migration_descriptor"
                )
                if canonical_bytes(current_descriptor) != canonical_bytes(descriptor):
                    raise ArtifactError("invalid_migration_descriptor")
    except (ArtifactError, KeyError, TypeError, ValidationError) as exc:
        if isinstance(exc, ArtifactError) and (
            exc.code.startswith("unsupported_")
            or exc.code
            in {"definition_fingerprint_mismatch", "invalid_migration_descriptor"}
        ):
            raise exc
        raise ArtifactError("invalid_aggregate_state_package") from exc
    put_definition = getattr(artifact_resolver, "put_definition", None)
    put_descriptor = getattr(artifact_resolver, "put_migration_descriptor", None)
    if (definitions and not callable(put_definition)) or (
        descriptors and not callable(put_descriptor)
    ):
        raise ArtifactError("invalid_aggregate_state_package")
    overlay = _PackageResolver(artifact_resolver, definitions, descriptors)
    try:
        aggregate = restore_aggregate(document["aggregate_state"], overlay)
        store_definition = cast(Callable[[str, Bundle], None], put_definition)
        store_descriptor = cast(
            Callable[[str, Mapping[str, Any]], None], put_descriptor
        )
        for fingerprint, bundle in definitions.items():
            store_definition(fingerprint, bundle)
        for digest, descriptor in descriptors.items():
            store_descriptor(digest, descriptor)
    except ArtifactError:
        raise
    return RestoredAggregatePackage(
        aggregate=aggregate,
        migration_route=route,
        package_document=copy.deepcopy(document),
    )


class _PackageResolver:
    def __init__(
        self,
        parent: ArtifactResolver,
        definitions: Mapping[str, Bundle],
        descriptors: Mapping[str, dict[str, Any]],
    ) -> None:
        self.parent = parent
        self.definitions = definitions
        self.descriptors = descriptors

    def resolve_definition(self, fingerprint: str) -> Bundle | BundleSource | None:
        return self.definitions.get(fingerprint) or self.parent.resolve_definition(
            fingerprint
        )

    def definition_is_trusted(self, fingerprint: str) -> bool:
        return fingerprint in self.definitions or self.parent.definition_is_trusted(
            fingerprint
        )

    def resolve_migration_descriptor(self, digest: str) -> ArtifactSource | None:
        return self.descriptors.get(digest) or self.parent.resolve_migration_descriptor(
            digest
        )

    def migration_descriptor_is_trusted(self, digest: str) -> bool:
        return digest in self.descriptors or self.parent.migration_descriptor_is_trusted(
            digest
        )


def aggregate_shape_fingerprint(bundle: Bundle | BundleSource) -> str:
    """Compute the exact state-bearing definition fingerprint from SPEC §16.6."""
    validated = bundle if isinstance(bundle, Bundle) else load_bundle(bundle)

    def variable_projection(
        declaration: Mapping[str, Any], pointer: str
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "declaration_pointer": pointer,
            "type": declaration["type"],
            "nullable": (
                bool(declaration.get("nullable"))
                if declaration["type"] == "instance_reference"
                else False
            ),
            "input": bool(declaration.get("input")),
            "external": bool(declaration.get("external")),
        }
        if declaration.get("machine_id") is not None:
            result["machine_id"] = declaration["machine_id"]
        return result

    def action_spawn_sites(
        machine: MachineModel,
        state: StateNode,
        actions: Any,
        pointer: str,
    ) -> list[dict[str, Any]]:
        sites: list[dict[str, Any]] = []
        for index, action in enumerate(actions or []):
            if "spawn" not in action:
                continue
            spawn = action["spawn"]
            holder_pointer = None
            if "bind_to" in spawn:
                holder_pointer = _resolve_variable_pointer(
                    machine, state, spawn["bind_to"]
                )
            sites.append(
                {
                    "action_pointer": f"{pointer}/{index}/spawn",
                    "machine_id": spawn["machine_id"],
                    "holder_variable_declaration_pointer": holder_pointer,
                }
            )
        return sites

    def state_projection(
        machine: MachineModel, state: StateNode
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "definition_pointer": state.pointer,
            "type": state.type,
        }
        if state.type == "composite":
            result["history"] = state.raw.get("history", "none")
        variables = [
            variable_projection(
                declaration, f"{state.pointer}/variables/{_escape_pointer(name)}"
            )
            for name, declaration in (state.raw.get("variables") or {}).items()
        ]
        variables.sort(key=lambda item: item["declaration_pointer"].encode("utf-8"))
        if variables:
            result["variables"] = variables
        children = [
            state_projection(machine, child)
            for _name, child in sorted(
                state.children.items(), key=lambda item: item[0].encode("utf-8")
            )
        ]
        if children:
            result["states"] = children
        components: list[dict[str, Any]] = []
        for index, placement in enumerate(state.raw.get("components") or []):
            item: dict[str, Any] = {
                "declaration_pointer": f"{state.pointer}/components/{index}",
                "declaration_index": index,
                "component_id": placement["component_id"],
            }
            if "machine_id" in placement:
                item["machine_id"] = placement["machine_id"]
            else:
                inline = BundleModel(validated).inline_component(
                    machine, placement, f"{state.pointer}/components/{index}"
                )
                item["inline_root"] = state_projection(inline, inline.root)
            components.append(item)
        if components:
            result["components"] = components
        sites = action_spawn_sites(
            machine, state, state.raw.get("entry"), f"{state.pointer}/entry"
        )
        sites += action_spawn_sites(
            machine, state, state.raw.get("exit"), f"{state.pointer}/exit"
        )
        for event_name, transition_or_list in (state.raw.get("on_events") or {}).items():
            transitions = (
                transition_or_list
                if isinstance(transition_or_list, list)
                else [transition_or_list]
            )
            for transition_index, transition in enumerate(transitions):
                suffix = (
                    f"/{transition_index}" if isinstance(transition_or_list, list) else ""
                )
                sites += action_spawn_sites(
                    machine,
                    state,
                    transition.get("action"),
                    (
                        f"{state.pointer}/on_events/{_escape_pointer(event_name)}"
                        f"{suffix}/action"
                    ),
                )
        sites.sort(key=lambda item: item["action_pointer"].encode("utf-8"))
        if sites:
            result["spawn_sites"] = sites
        return result

    models = BundleModel(validated)
    machine_values = [
        {
            "machine_id": machine.machine_id,
            "version": machine.version,
            "root": state_projection(machine, machine.root),
        }
        for machine in models.machines.values()
    ]
    tree = {
        "format": 1,
        "namespace": validated.namespace,
        "machines": machine_values,
    }
    return hash_value(
        ["determa-aggregate-shape-fingerprint-1", typed_value(tree)]
    )


def _resolve_variable_pointer(
    machine: MachineModel, state: StateNode, name: str
) -> str:
    current: StateNode | None = state
    while current is not None:
        if name in (current.raw.get("variables") or {}):
            return f"{current.pointer}/variables/{_escape_pointer(name)}"
        current = current.parent
    raise ArtifactError("invalid_migration_descriptor")
