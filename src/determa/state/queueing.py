"""Pure queue-bearing aggregate-state version 2 operations."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from functools import cache
from typing import Any, Literal, cast

from .codes import (
    CheckpointPreAcceptanceFailureCode as AdmissionCode,
)
from .codes import (
    DispatchRejectionCode,
    DispositionCode,
    EngineFaultCode,
    PersistenceFailureCode,
)
from .definition import Bundle, BundleSource, load_bundle
from .engine import (
    _Execution,
    _normalize_payload,
    _runtime_model,
    _validate_envelope,
    create,
    dispatch,
)
from .errors import ArtifactError, StepFault
from .migration import MigrationLimits, migrate_aggregate
from .model import BundleModel, StateNode
from .wire import (
    ArtifactSource,
    DefinitionResolver,
    MemoryArtifactResolver,
    RestoredAggregate,
    _schema_registry,
    aggregate_envelope,
    aggregate_state_digest,
    artifact_schema,
    canonical_bytes,
    decimal,
    decoded_typed_value,
    hash_value,
    load_json_artifact,
    migration_descriptor_digest,
    restore_aggregate,
    typed_value,
)


def seal_aggregate_v2(document: Mapping[str, Any]) -> dict[str, Any]:
    """Copy, canonically order, and seal one version-2 aggregate."""
    result = copy.deepcopy(dict(document))
    for runtime in result.get("runtimes", []):
        runtime["active_leaf_state_definition_pointers"].sort(key=_utf8)
        runtime["active_state_activations"].sort(
            key=lambda item: (
                _utf8(item["state_definition_pointer"]),
                int(item["activation_sequence"]),
            )
        )
        runtime["variables"].sort(
            key=lambda item: (
                _utf8(item["variable_declaration_pointer"]),
                int(item["declaring_state_activation_sequence"]),
            )
        )
        runtime["history"].sort(key=lambda item: _utf8(item["history_declaration_pointer"]))
        for name in (
            "next_state_activation_sequences",
            "next_component_activation_sequences",
        ):
            runtime[name].sort(key=lambda item: _utf8(item["definition_pointer"]))
        for name in ("ready_mailbox", "deferred_mailbox"):
            runtime[name].sort(key=lambda item: int(item["queue_sequence"]))
    result["runtimes"].sort(key=lambda item: _utf8(item["runtime_id"]))
    result.pop("aggregate_state_digest", None)
    result["aggregate_state_digest"] = aggregate_state_digest(result)
    return result


def _utf8(value: str) -> bytes:
    return value.encode("utf-8", errors="strict")


def _project_v1(document: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(document))
    result["aggregate_state_schema_version"] = 1
    result.pop("next_acceptance_sequence", None)
    result.pop("next_queue_sequence", None)
    for runtime in result["runtimes"]:
        runtime.pop("ready_mailbox", None)
        runtime.pop("deferred_mailbox", None)
    result.pop("aggregate_state_digest", None)
    result["aggregate_state_digest"] = aggregate_state_digest(result)
    return result


def _upgrade_document(document: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(document))
    result["aggregate_state_schema_version"] = 2
    result["next_acceptance_sequence"] = "0"
    result["next_queue_sequence"] = "0"
    for runtime in result["runtimes"]:
        runtime["ready_mailbox"] = []
        runtime["deferred_mailbox"] = []
    return seal_aggregate_v2(result)


def upgrade_aggregate_v1_to_v2(
    source: ArtifactSource, definition_resolver: DefinitionResolver
) -> dict[str, Any]:
    """Explicitly convert a valid version-1 aggregate to empty version-2 mailboxes."""
    del definition_resolver
    document, _ = load_json_artifact(source, "aggregate_state")
    if aggregate_state_digest(document) != document["aggregate_state_digest"]:
        raise ArtifactError(PersistenceFailureCode.AGGREGATE_STATE_DIGEST_MISMATCH)
    return _upgrade_document(document)


def downgrade_aggregate_v2_to_v1(
    source: ArtifactSource, definition_resolver: DefinitionResolver
) -> dict[str, Any]:
    """Explicitly convert a version-2 aggregate only when no queued work exists."""
    del definition_resolver
    document, _ = load_json_artifact(source, "aggregate_state_v2")
    if aggregate_state_digest(document) != document["aggregate_state_digest"]:
        raise ArtifactError(PersistenceFailureCode.AGGREGATE_STATE_DIGEST_MISMATCH)
    _validate_mailboxes(document)
    if document["next_acceptance_sequence"] != "0" or document["next_queue_sequence"] != "0":
        raise ArtifactError(PersistenceFailureCode.MIGRATION_TOTALITY_FAILURE)
    if any(
        runtime[mailbox]
        for runtime in document["runtimes"]
        for mailbox in ("ready_mailbox", "deferred_mailbox")
    ):
        raise ArtifactError(PersistenceFailureCode.MIGRATION_TOTALITY_FAILURE)
    return {"result": "success", "aggregate_state": _project_v1(document)}


def create_aggregate_v2(
    bundle: Bundle | BundleSource,
    machine_id: str,
    root_instance_id: str,
    creation_id: str,
    bindings: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Create one queue-bearing aggregate and route initialization emissions."""
    validated = bundle if isinstance(bundle, Bundle) else load_bundle(bundle)
    result = create(
        validated,
        machine_id,
        root_instance_id,
        creation_id,
        bindings,
        _capture_emission_provenance=True,
    )
    if result["state"] is None:
        return {**result, "lifecycle_dispositions": []}
    encoded = _upgrade_document(aggregate_envelope(validated, result["state"]))
    root_runtime = next(
        runtime
        for runtime in encoded["runtimes"]
        if runtime["runtime_id"] == encoded["root_runtime_id"]
    )
    lifecycle: list[dict[str, Any]] = []
    emissions = _enqueue_emissions(
        encoded,
        encoded,
        root_runtime,
        cast(list[Mapping[str, Any]], result["emissions"]),
        lifecycle,
    )
    encoded = seal_aggregate_v2(encoded)
    restore_aggregate_v2(encoded, resolver_for_bundle(validated))
    result_fault = copy.deepcopy(root_runtime["fault"]) if result["status"] == "faulted" else None
    return {
        "status": result["status"],
        "disposition": None,
        "state": encoded,
        "emissions": emissions,
        "lifecycle_dispositions": lifecycle,
        "fault": result_fault,
        "rejection": copy.deepcopy(result["rejection"]),
    }


@cache
def _envelope_validator() -> Any:
    import jsonschema

    schema = artifact_schema("aggregate_state_v2")
    return jsonschema.Draft202012Validator(
        {"$ref": f"{schema['$id']}#/$defs/envelope"}, registry=_schema_registry()
    )


def _valid_envelope_shape(value: Any) -> bool:
    return (
        isinstance(value, Mapping) and next(_envelope_validator().iter_errors(value), None) is None
    )


def _entry_digest(root_instance_id: str, mode: str, envelope: Mapping[str, Any]) -> str:
    return hash_value(["determa-inbox-envelope-digest-2", "2", root_instance_id, mode, envelope])


def _validate_mailboxes(document: dict[str, Any]) -> None:
    next_acceptance = decimal(document["next_acceptance_sequence"])
    next_queue = decimal(document["next_queue_sequence"])
    acceptance_sequences: list[int] = []
    queue_sequences: list[int] = []
    event_ids: set[str] = set()
    for runtime in document["runtimes"]:
        for mailbox in ("ready_mailbox", "deferred_mailbox"):
            prior_queue = -1
            for entry in runtime[mailbox]:
                acceptance = decimal(entry["acceptance_sequence"])
                queue = decimal(entry["queue_sequence"])
                if queue <= prior_queue:
                    raise ArtifactError(PersistenceFailureCode.INVALID_AGGREGATE_STATE)
                prior_queue = queue
                if acceptance >= next_acceptance or queue >= next_queue:
                    raise ArtifactError(PersistenceFailureCode.INVALID_AGGREGATE_STATE)
                envelope = entry["envelope"]
                event_id = envelope["event_id"]
                if (
                    envelope["target"] != runtime["target_identity"]
                    or event_id in event_ids
                    or entry["envelope_digest"]
                    != _entry_digest(document["root_instance_id"], entry["delivery_mode"], envelope)
                    or not isinstance(decoded_typed_value(envelope["payload"]), dict)
                ):
                    raise ArtifactError(PersistenceFailureCode.INVALID_AGGREGATE_STATE)
                event_ids.add(event_id)
                acceptance_sequences.append(acceptance)
                queue_sequences.append(queue)
    if len(acceptance_sequences) != len(set(acceptance_sequences)) or len(queue_sequences) != len(
        set(queue_sequences)
    ):
        raise ArtifactError(PersistenceFailureCode.INVALID_AGGREGATE_STATE)


def _valid_envelope_source(document: Mapping[str, Any], entry: Mapping[str, Any]) -> bool:
    envelope = entry["envelope"]
    source = envelope["source"]
    mode = entry["delivery_mode"]
    if mode == "input":
        return bool(source == {"host": True} and envelope["cause_id"] == envelope["event_id"])
    if "runtime" in source:
        return any(
            runtime["target_identity"] == source["runtime"] for runtime in document["runtimes"]
        )
    if "system" in source:
        return source["system"] in {
            "system:component_completion",
            "system:spawned_completion",
            "system:component_failure",
            "system:spawned_failure",
        }
    return "legacy_v1_internal" in source


def _validate_mailbox_semantics(document: Mapping[str, Any], restored: RestoredAggregate) -> None:
    for runtime in document["runtimes"]:
        for mailbox in ("ready_mailbox", "deferred_mailbox"):
            for entry in runtime[mailbox]:
                if not _valid_envelope_source(document, entry):
                    raise ArtifactError(PersistenceFailureCode.INVALID_AGGREGATE_STATE)
                try:
                    mode = entry["delivery_mode"]
                    envelope = entry["envelope"]
                    relation = runtime["relation"]["kind"]
                    machine_id = runtime["current_definition"]["machine"]["machine_id"]
                    machine = restored.bundle.machine(machine_id)
                    declarations = dict(restored.bundle.raw.get("events") or {})
                    declarations.update((machine or {}).get("events") or {})
                    event = envelope["event"]
                    declaration = declarations.get(event)
                    if mode == "input" and relation == "component":
                        raise ValueError
                    if event == "env":
                        valid_event = (mode == "input" and relation != "component") or (
                            mode == "internal" and relation == "component"
                        )
                    elif declaration is None:
                        valid_event = mode == "internal" and event in {
                            "done",
                            "determa.component_completed",
                            "determa.component_failed",
                            "determa.spawned_instance_failed",
                        }
                    else:
                        expected = "input" if mode == "input" else "internal"
                        valid_event = declaration["direction"] == expected
                        correlation = envelope.get("correlation_id")
                        valid_event = valid_event and bool(declaration.get("correlates_to")) == (
                            correlation is not None
                        )
                except (ArtifactError, KeyError, TypeError, ValueError) as error:
                    raise ArtifactError(PersistenceFailureCode.INVALID_AGGREGATE_STATE) from error
                if not valid_event:
                    raise ArtifactError(PersistenceFailureCode.INVALID_AGGREGATE_STATE)


def restore_aggregate_v2(
    source: ArtifactSource, definition_resolver: DefinitionResolver
) -> RestoredAggregate:
    """Structurally and semantically restore one queue-bearing aggregate."""
    document, raw = load_json_artifact(source, "aggregate_state_v2")
    if aggregate_state_digest(document) != document["aggregate_state_digest"]:
        raise ArtifactError(PersistenceFailureCode.AGGREGATE_STATE_DIGEST_MISMATCH)
    if document["runtimes"] != sorted(
        document["runtimes"], key=lambda item: _utf8(item["runtime_id"])
    ):
        raise ArtifactError(PersistenceFailureCode.INVALID_AGGREGATE_STATE)
    _validate_mailboxes(document)
    restored = restore_aggregate(_project_v1(document), definition_resolver)
    _validate_mailbox_semantics(document, restored)
    return RestoredAggregate(
        bundle=restored.bundle,
        state=restored.state,
        aggregate_envelope=copy.deepcopy(document),
        canonical_bytes=canonical_bytes(document),
        source_bytes=raw,
    )


def _wire_target_to_native(target: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(target))
    if "component" in result:
        result["component"]["activation_sequence"] = decimal(
            result["component"]["activation_sequence"]
        )
    elif "spawned_instance" in result:
        result["spawned_instance"]["machine_version"] = decimal(
            result["spawned_instance"]["machine_version"], positive=True
        )
    return result


def _native_envelope(entry: Mapping[str, Any]) -> dict[str, Any]:
    wire = entry["envelope"]
    result = {
        "event": wire["event"],
        "event_id": wire["event_id"],
        "target": _wire_target_to_native(wire["target"]),
        "payload": decoded_typed_value(wire["payload"]),
    }
    if "correlation_id" in wire:
        result["correlation_id"] = wire["correlation_id"]
    return result


def _runtime_id_for_target(target: Mapping[str, Any]) -> str:
    if "root" in target:
        return str(target["root"]["root_runtime_id"])
    if "component" in target:
        return str(target["component"]["component_runtime_id"])
    return str(target["spawned_instance"]["instance_id"])


def _admission_rejection(code: str, state: dict[str, Any]) -> dict[str, Any]:
    return {
        "result": "rejected",
        "status": _root_status(state),
        "accepted": [],
        "state": state,
        "rejection": {"code": code},
    }


def _root_status(document: Mapping[str, Any]) -> str:
    return str(
        next(
            runtime["status"]
            for runtime in document["runtimes"]
            if runtime["runtime_id"] == document["root_runtime_id"]
        )
    )


def _dispatch_code_to_admission(code: DispatchRejectionCode) -> str:
    return {
        DispatchRejectionCode.INVALID_EVENT: AdmissionCode.INVALID_EVENT.value,
        DispatchRejectionCode.INVALID_PAYLOAD: AdmissionCode.INVALID_PAYLOAD.value,
        DispatchRejectionCode.INVALID_CORRELATION: AdmissionCode.INVALID_CORRELATION.value,
        DispatchRejectionCode.INVALID_INSTANCE_TARGET: AdmissionCode.INVALID_INSTANCE_TARGET.value,
        DispatchRejectionCode.INACTIVE_COMPONENT_TARGET: (
            AdmissionCode.INACTIVE_COMPONENT_TARGET.value
        ),
        DispatchRejectionCode.INVALID_PRIOR_STATE: AdmissionCode.INVALID_INSTANCE_TARGET.value,
        DispatchRejectionCode.INCOMPATIBLE_BUNDLE: AdmissionCode.INVALID_INSTANCE_TARGET.value,
    }[code]


def admit_aggregate_v2(
    source: ArtifactSource,
    deliveries: Sequence[Mapping[str, Any]],
    definition_resolver: DefinitionResolver,
) -> dict[str, Any]:
    """Atomically admit or replay an ordered delivery batch."""
    restored = restore_aggregate_v2(source, definition_resolver)
    document = restored.aggregate_envelope
    if (
        not isinstance(deliveries, Sequence)
        or isinstance(deliveries, str | bytes)
        or not deliveries
    ):
        return _admission_rejection(AdmissionCode.MALFORMED_DELIVERY.value, document)
    event_ids: list[str] = []
    target_roots: list[str] = []
    for delivery in deliveries:
        if (
            not isinstance(delivery, Mapping)
            or set(delivery) != {"delivery_mode", "envelope", "envelope_digest"}
            or not isinstance(delivery.get("delivery_mode"), str)
            or not isinstance(delivery.get("envelope_digest"), str)
            or not _valid_envelope_shape(delivery.get("envelope"))
        ):
            return _admission_rejection(AdmissionCode.MALFORMED_DELIVERY.value, document)
        event_id = delivery["envelope"].get("event_id")
        if not isinstance(event_id, str) or not event_id:
            return _admission_rejection(AdmissionCode.MALFORMED_DELIVERY.value, document)
        try:
            target_root = next(iter(delivery["envelope"]["target"].values()))["root_instance_id"]
        except (KeyError, StopIteration, TypeError):
            return _admission_rejection(AdmissionCode.MALFORMED_DELIVERY.value, document)
        event_ids.append(event_id)
        target_roots.append(target_root)
    if any(target_root != document["root_instance_id"] for target_root in target_roots):
        return _admission_rejection(AdmissionCode.WRONG_ROOT.value, document)
    if len(event_ids) != len(set(event_ids)):
        return _admission_rejection(AdmissionCode.DUPLICATE_EVENT_ID_IN_BATCH.value, document)

    existing: dict[str, tuple[str, dict[str, Any]]] = {}
    for runtime in document["runtimes"]:
        for mailbox, location in (
            ("ready_mailbox", "ready"),
            ("deferred_mailbox", "deferred"),
        ):
            for entry in runtime[mailbox]:
                existing[entry["envelope"]["event_id"]] = (location, entry)

    replayed: list[tuple[str, dict[str, Any]]] = []
    new_deliveries: list[Mapping[str, Any]] = []
    for delivery in deliveries:
        mode = delivery.get("delivery_mode")
        envelope = delivery["envelope"]
        event_id = envelope["event_id"]
        prior = existing.get(event_id)
        candidate_digest = _entry_digest(document["root_instance_id"], str(mode), envelope)
        if prior is not None:
            if candidate_digest != prior[1]["envelope_digest"]:
                return _admission_rejection(AdmissionCode.EVENT_ID_CONFLICT.value, document)
            replayed.append(prior)
        else:
            new_deliveries.append(delivery)
    if replayed and not new_deliveries and len(replayed) == 1:
        location, entry = replayed[0]
        return {
            "result": "replay",
            "status": _root_status(document),
            "event_id": entry["envelope"]["event_id"],
            "acceptance_sequence": entry["acceptance_sequence"],
            "location": location,
            "state": document,
            "rejection": None,
        }

    models = BundleModel(restored.bundle)
    accepted: list[dict[str, str]] = []
    candidate = copy.deepcopy(document)
    runtime_by_id = {runtime["runtime_id"]: runtime for runtime in candidate["runtimes"]}
    for delivery in new_deliveries:
        mode = delivery.get("delivery_mode")
        envelope = delivery["envelope"]
        if mode not in {"input", "internal"}:
            return _admission_rejection(AdmissionCode.INVALID_DELIVERY_MODE.value, document)
        source_value = envelope.get("source")
        if (
            (mode == "input" and source_value != {"host": True})
            or (mode == "internal" and not isinstance(source_value, Mapping))
            or (mode == "input" and envelope.get("cause_id") != envelope.get("event_id"))
        ):
            return _admission_rejection(AdmissionCode.INVALID_DELIVERY_SOURCE.value, document)
        if mode == "internal":
            assert isinstance(source_value, Mapping)
            source_runtime = source_value.get("runtime")
            source_system = source_value.get("system")
            valid_runtime_source = source_runtime is not None and any(
                runtime["target_identity"] == source_runtime for runtime in document["runtimes"]
            )
            valid_system_source = source_system in {
                "system:component_completion",
                "system:spawned_completion",
                "system:component_failure",
                "system:spawned_failure",
            }
            if not valid_runtime_source and not valid_system_source:
                return _admission_rejection(AdmissionCode.INVALID_DELIVERY_SOURCE.value, document)
        supplied_digest = delivery.get("envelope_digest")
        expected_digest = _entry_digest(document["root_instance_id"], str(mode), envelope)
        if supplied_digest != expected_digest:
            return _admission_rejection(AdmissionCode.DELIVERY_DIGEST_MISMATCH.value, document)
        try:
            native = _native_envelope(delivery)
        except (ArtifactError, KeyError, TypeError):
            return _admission_rejection(AdmissionCode.INVALID_PAYLOAD.value, document)
        try:
            runtime_id = _runtime_id_for_target(envelope["target"])
        except (KeyError, TypeError):
            return _admission_rejection(AdmissionCode.INVALID_INSTANCE_TARGET.value, document)
        target_runtime = runtime_by_id.get(runtime_id)
        if target_runtime is None or target_runtime["target_identity"] != envelope["target"]:
            return _admission_rejection(AdmissionCode.INVALID_INSTANCE_TARGET.value, document)
        rejection = _validate_envelope(
            restored.bundle,
            models,
            restored.state,
            native,
            cast(Literal["input", "internal"], mode),
        )
        if rejection is not None:
            return _admission_rejection(_dispatch_code_to_admission(rejection), document)
        acceptance = candidate["next_acceptance_sequence"]
        queue = candidate["next_queue_sequence"]
        entry = {
            "acceptance_sequence": acceptance,
            "queue_sequence": queue,
            "delivery_mode": mode,
            "envelope": copy.deepcopy(envelope),
            "envelope_digest": expected_digest,
            "deferral_count": "0",
        }
        runtime_by_id[runtime_id]["ready_mailbox"].append(entry)
        accepted.append(
            {
                "event_id": envelope["event_id"],
                "acceptance_sequence": acceptance,
                "queue_sequence": queue,
            }
        )
        candidate["next_acceptance_sequence"] = str(int(acceptance) + 1)
        candidate["next_queue_sequence"] = str(int(queue) + 1)
    candidate = seal_aggregate_v2(candidate)
    return {
        "result": "accepted",
        "status": _root_status(candidate),
        "accepted": accepted,
        "state": candidate,
        "rejection": None,
    }


def _mailbox_maps(
    document: Mapping[str, Any],
) -> dict[str, tuple[list[Any], list[Any]]]:
    return {
        runtime["runtime_id"]: (
            copy.deepcopy(runtime["ready_mailbox"]),
            copy.deepcopy(runtime["deferred_mailbox"]),
        )
        for runtime in document["runtimes"]
    }


def _wire_target(target: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(target))
    if "component" in result:
        result["component"]["activation_sequence"] = str(result["component"]["activation_sequence"])
    elif "spawned_instance" in result:
        result["spawned_instance"]["machine_version"] = str(
            result["spawned_instance"]["machine_version"]
        )
    return result


def _wire_source(source: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(source))
    runtime = result.get("runtime")
    if isinstance(runtime, Mapping):
        result["runtime"] = _wire_target(runtime)
    return result


def _lifecycle_disposition(
    entry: Mapping[str, Any], runtime_id: str, reason: str
) -> dict[str, Any]:
    return {
        "event_id": entry["envelope"]["event_id"],
        "request_digest": entry["envelope_digest"],
        "acceptance_sequence": entry["acceptance_sequence"],
        "final_queue_sequence": entry["queue_sequence"],
        "target_runtime_id": runtime_id,
        "reason": reason,
    }


def _enqueue_emissions(
    encoded: dict[str, Any],
    before: Mapping[str, Any],
    selected_runtime: Mapping[str, Any],
    native_emissions: Sequence[Mapping[str, Any]],
    lifecycle: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    projected: list[dict[str, Any]] = []
    runtimes = {runtime["runtime_id"]: runtime for runtime in encoded["runtimes"]}
    root_completed = _root_status(encoded) == "completed"
    for index, emission in enumerate(native_emissions):
        if emission.get("target") == "external":
            projected.append(
                {
                    "effect_id": emission["effect_id"],
                    "sequence": str(emission["sequence"]),
                    "event": emission["event"],
                    "payload": typed_value(emission["payload"]),
                    "correlation_id": emission["correlation_id"],
                }
            )
            continue
        target = cast(Mapping[str, Any], emission["target"])
        target_wire = _wire_target(target)
        target_runtime_id = _runtime_id_for_target(target_wire)
        acceptance = encoded["next_acceptance_sequence"]
        queue = encoded["next_queue_sequence"]
        encoded["next_acceptance_sequence"] = str(int(acceptance) + 1)
        encoded["next_queue_sequence"] = str(int(queue) + 1)
        provenance = emission.get("_determa_v2_provenance")
        if not isinstance(provenance, Mapping):
            raise ArtifactError(PersistenceFailureCode.INVALID_AGGREGATE_STATE)
        envelope = {
            "event": emission["event"],
            "event_id": emission["event_id"],
            "cause_id": provenance["cause_id"],
            "source": _wire_source(cast(Mapping[str, Any], provenance["source"])),
            "target": target_wire,
            "payload": typed_value(emission["payload"]),
        }
        if emission.get("correlation_id") is not None:
            envelope["correlation_id"] = emission["correlation_id"]
        entry = {
            "acceptance_sequence": acceptance,
            "queue_sequence": queue,
            "delivery_mode": "internal",
            "envelope": envelope,
            "envelope_digest": _entry_digest(encoded["root_instance_id"], "internal", envelope),
            "deferral_count": "0",
        }
        target_runtime = runtimes.get(target_runtime_id)
        reason: str | None = None
        if target_runtime is None:
            reason = "runtime_cancelled"
        elif root_completed:
            reason = "aggregate_completed"
        elif target_runtime["status"] == "completed":
            reason = "runtime_completed"
        if reason is None:
            assert target_runtime is not None
            target_runtime["ready_mailbox"].append(entry)
            projected.append(
                {
                    "kind": "internal_mailbox",
                    "emission_index": str(index),
                    "event_id": emission["event_id"],
                    "acceptance_sequence": acceptance,
                    "queue_sequence": queue,
                }
            )
        else:
            disposition_index = str(len(lifecycle))
            lifecycle.append(_lifecycle_disposition(entry, target_runtime_id, reason))
            projected.append(
                {
                    "kind": "internal_disposed",
                    "emission_index": str(index),
                    "event_id": emission["event_id"],
                    "acceptance_sequence": acceptance,
                    "lifecycle_disposition_index": disposition_index,
                }
            )
    return projected


def _encode_after_dispatch(
    bundle: Bundle,
    state: dict[str, Any],
    before: Mapping[str, Any],
    mailboxes: Mapping[str, tuple[list[Any], list[Any]]],
) -> dict[str, Any]:
    result = _upgrade_document(aggregate_envelope(bundle, state))
    result["next_acceptance_sequence"] = before["next_acceptance_sequence"]
    result["next_queue_sequence"] = before["next_queue_sequence"]
    for runtime in result["runtimes"]:
        ready, deferred = mailboxes.get(runtime["runtime_id"], ([], []))
        runtime["ready_mailbox"] = ready
        runtime["deferred_mailbox"] = deferred
    return seal_aggregate_v2(result)


def _runtime_capacity(bundle: Bundle, state: dict[str, Any], runtime_id: str) -> int | None:
    runtime = state["runtimes"][runtime_id]
    machine = _runtime_model(bundle, BundleModel(bundle), runtime)
    value = machine.root.raw.get("deferred_event_capacity")
    return int(value) if value is not None else None


def _structurally_deferred(
    bundle: Bundle, state: dict[str, Any], runtime_id: str, event: str
) -> bool:
    runtime = state["runtimes"][runtime_id]
    machine = _runtime_model(bundle, BundleModel(bundle), runtime)
    current: StateNode | None = (
        machine.states[runtime["active"][-1]] if runtime["active"] else machine.root
    )
    while current is not None:
        if event in (current.raw.get("on_events") or {}):
            return False
        if event in (current.raw.get("deferred_events") or []):
            return True
        current = current.parent
    return False


def _runtime_is_runnable(state: Mapping[str, Any], runtime_id: str) -> bool:
    runtime = state["runtimes"].get(runtime_id)
    while isinstance(runtime, Mapping):
        if runtime["status"] != "running":
            return False
        owner_id = runtime.get("owner_runtime_id")
        if owner_id is None:
            return True
        runtime = state["runtimes"].get(owner_id)
    return False


def _lifecycle_runtime_order(bundle: Bundle, state: dict[str, Any]) -> list[str]:
    execution = _Execution(
        bundle,
        BundleModel(bundle),
        state,
        step_sequence=state["next_logical_step_sequence"],
    )
    ordered: list[str] = []

    def visit(runtime: dict[str, Any]) -> None:
        for child in execution.ordered_children(runtime):
            visit(child)
        ordered.append(runtime["runtime_id"])

    visit(state["runtimes"][state["root_runtime_id"]])
    return ordered


def _step_result(
    state: dict[str, Any],
    disposition: str,
    *,
    emissions: list[dict[str, Any]] | None = None,
    lifecycle: list[dict[str, Any]] | None = None,
    fault: dict[str, Any] | None = None,
    rejection: str | None = None,
) -> dict[str, Any]:
    return {
        "core_step_result_format": "determa.core_step_result",
        "core_step_result_schema_version": 2,
        "status": _root_status(state),
        "disposition": disposition,
        "state": state,
        "emissions": emissions or [],
        "lifecycle_dispositions": lifecycle or [],
        "fault": fault,
        "rejection": None if rejection is None else {"code": rejection},
    }


def step_aggregate_v2(
    source: ArtifactSource,
    target_runtime_id: str,
    definition_resolver: DefinitionResolver,
) -> dict[str, Any]:
    """Process at most the selected runtime's ready-mailbox head."""
    restored = restore_aggregate_v2(source, definition_resolver)
    before = restored.aggregate_envelope
    wire_runtime = next(
        (runtime for runtime in before["runtimes"] if runtime["runtime_id"] == target_runtime_id),
        None,
    )
    if wire_runtime is None:
        return _step_result(
            before, DispositionCode.REJECTED.value, rejection="invalid_instance_target"
        )
    if _root_status(before) != "running":
        return _step_result(
            before,
            DispositionCode.REJECTED.value,
            rejection="invalid_instance_target",
        )
    if wire_runtime["status"] != "running":
        code = (
            "inactive_component_target"
            if wire_runtime["relation"]["kind"] == "component"
            else "invalid_instance_target"
        )
        return _step_result(before, DispositionCode.REJECTED.value, rejection=code)
    if not wire_runtime["ready_mailbox"]:
        return _step_result(before, DispositionCode.NOT_RUNNABLE.value)

    selected = wire_runtime["ready_mailbox"][0]
    mailboxes = _mailbox_maps(before)
    ready, deferred = mailboxes[target_runtime_id]
    native = _native_envelope(selected)
    result = dispatch(
        restored.bundle,
        restored.state,
        {selected["delivery_mode"]: native},
        _capture_emission_provenance=True,
    )
    disposition = result["disposition"]
    if disposition == DispositionCode.REJECTED.value:
        return _step_result(
            before,
            disposition,
            fault=copy.deepcopy(result.get("fault")),
            rejection=(result.get("rejection") or {}).get("code"),
        )
    ready.pop(0)
    if disposition == DispositionCode.DEFERRED.value:
        capacity = _runtime_capacity(restored.bundle, restored.state, target_runtime_id)
        if capacity is not None and len(deferred) >= capacity:
            state = copy.deepcopy(restored.state)
            runtime = state["runtimes"][target_runtime_id]
            execution = _Execution(
                restored.bundle,
                BundleModel(restored.bundle),
                state,
                step_sequence=state["next_logical_step_sequence"],
                capture_emission_provenance=True,
            )
            cause_id = selected["envelope"]["cause_id"]
            execution.cause_id = cause_id
            execution.finalize_fault(
                runtime,
                StepFault(
                    EngineFaultCode.DEFERRED_EVENT_CAPACITY_EXCEEDED,
                    "system:deferred_event_capacity",
                ),
                cause_id,
            )
            state["next_logical_step_sequence"] += 1
            if runtime["role"] == "root":
                state["status"] = "faulted"
                state["fault"] = copy.deepcopy(runtime["fault"])
            else:
                execution.emit_failure(runtime, cause_id)
            encoded = _encode_after_dispatch(restored.bundle, state, before, mailboxes)
            overflow_lifecycle: list[dict[str, Any]] = []
            emissions = _enqueue_emissions(
                encoded, before, wire_runtime, execution.emissions, overflow_lifecycle
            )
            encoded_fault = copy.deepcopy(runtime["fault"])
            encoded_fault["step_sequence"] = str(encoded_fault["step_sequence"])
            encoded_fault["definition_fingerprint"] = restored.bundle.fingerprint
            return _step_result(
                seal_aggregate_v2(encoded),
                DispositionCode.FAULTED.value,
                emissions=emissions,
                lifecycle=overflow_lifecycle,
                fault=encoded_fault,
            )
        moved = copy.deepcopy(selected)
        moved["queue_sequence"] = before["next_queue_sequence"]
        moved["deferral_count"] = str(int(moved["deferral_count"]) + 1)
        deferred.append(moved)
        candidate = copy.deepcopy(before)
        candidate["next_logical_step_sequence"] = str(
            int(candidate["next_logical_step_sequence"]) + 1
        )
        candidate["next_queue_sequence"] = str(int(candidate["next_queue_sequence"]) + 1)
        for runtime in candidate["runtimes"]:
            if runtime["runtime_id"] == target_runtime_id:
                runtime["ready_mailbox"] = ready
                runtime["deferred_mailbox"] = deferred
        return _step_result(seal_aggregate_v2(candidate), disposition)

    state = cast(dict[str, Any], result["state"])
    encoded = _encode_after_dispatch(restored.bundle, state, before, mailboxes)
    lifecycle: list[dict[str, Any]] = []
    live = {runtime["runtime_id"]: runtime for runtime in encoded["runtimes"]}
    root_completed = state["status"] == "completed"
    before_by_id = {runtime["runtime_id"]: runtime for runtime in before["runtimes"]}
    for runtime_id in _lifecycle_runtime_order(restored.bundle, restored.state):
        runtime = before_by_id[runtime_id]
        current = live.get(runtime["runtime_id"])
        if not root_completed and current is not None and current["status"] != "completed":
            continue
        reason = (
            "aggregate_completed"
            if root_completed
            else "runtime_completed"
            if current is not None
            else "runtime_cancelled"
        )
        entries = [
            *mailboxes[runtime["runtime_id"]][0],
            *mailboxes[runtime["runtime_id"]][1],
        ]
        for entry in entries:
            lifecycle.append(_lifecycle_disposition(entry, runtime["runtime_id"], reason))
        if current is not None:
            current["ready_mailbox"] = []
            current["deferred_mailbox"] = []
    emissions = _enqueue_emissions(
        encoded,
        before,
        wire_runtime,
        cast(list[Mapping[str, Any]], result["emissions"]),
        lifecycle,
    )
    if (
        disposition == DispositionCode.HANDLED.value
        and target_runtime_id in live
        and live[target_runtime_id]["status"] == "running"
    ):
        runtime = next(
            item for item in encoded["runtimes"] if item["runtime_id"] == target_runtime_id
        )
        current_ready = runtime["ready_mailbox"]
        current_deferred = runtime["deferred_mailbox"]
        for deferred_entry in list(current_deferred):
            if not _structurally_deferred(
                restored.bundle,
                state,
                target_runtime_id,
                deferred_entry["envelope"]["event"],
            ):
                current_deferred.remove(deferred_entry)
                deferred_entry["queue_sequence"] = encoded["next_queue_sequence"]
                encoded["next_queue_sequence"] = str(int(encoded["next_queue_sequence"]) + 1)
                current_ready.append(deferred_entry)
    encoded = seal_aggregate_v2(encoded)
    result_fault = result.get("fault")
    if isinstance(result_fault, dict):
        result_fault = copy.deepcopy(result_fault)
        result_fault["step_sequence"] = str(result_fault["step_sequence"])
        result_fault["definition_fingerprint"] = restored.bundle.fingerprint
    return _step_result(
        encoded,
        disposition,
        emissions=emissions,
        lifecycle=lifecycle,
        fault=result_fault,
        rejection=(result.get("rejection") or {}).get("code"),
    )


def resolver_for_bundle(bundle: Bundle | BundleSource) -> MemoryArtifactResolver:
    """Build a trusted resolver for a single normalized bundle."""
    validated = bundle if isinstance(bundle, Bundle) else load_bundle(bundle)
    return MemoryArtifactResolver(definitions={validated.fingerprint: validated})


class _V1MigrationResolver:
    def __init__(
        self,
        parent: DefinitionResolver,
        descriptors: Mapping[str, Mapping[str, Any]],
    ) -> None:
        self.parent = parent
        self.descriptors = descriptors

    def resolve_definition(self, fingerprint: str) -> Bundle | BundleSource | None:
        return self.parent.resolve_definition(fingerprint)

    def definition_is_trusted(self, fingerprint: str) -> bool:
        return self.parent.definition_is_trusted(fingerprint)

    def resolve_migration_descriptor(self, digest: str) -> ArtifactSource | None:
        return self.descriptors.get(digest)

    def migration_descriptor_is_trusted(self, digest: str) -> bool:
        return digest in self.descriptors


def _queue_rule(
    descriptor: Mapping[str, Any], runtime: Mapping[str, Any], entry: Mapping[str, Any]
) -> Mapping[str, Any] | None:
    machine_id = runtime["current_definition"]["machine"]["machine_id"]
    for rule in descriptor["queued_event_rules"]:
        if (
            rule["machine_id"] == machine_id
            and rule["delivery_mode"] == entry["delivery_mode"]
            and rule["event"] == entry["envelope"]["event"]
        ):
            return cast(Mapping[str, Any], rule)
    return None


def _queue_compatible(
    bundle: Bundle,
    runtime: Mapping[str, Any],
    entry: Mapping[str, Any],
) -> bool:
    machine_id = runtime["current_definition"]["machine"]["machine_id"]
    machine = bundle.machine(machine_id)
    if machine is None:
        return False
    event = entry["envelope"]["event"]
    declarations = dict(bundle.raw.get("events") or {})
    declarations.update(machine.get("events") or {})
    declaration = declarations.get(event)
    if declaration is None:
        return event in {
            "done",
            "determa.component_completed",
            "determa.component_failed",
            "determa.spawned_instance_failed",
            "env",
        }
    expected_direction = "input" if entry["delivery_mode"] == "input" else "internal"
    if declaration["direction"] != expected_direction:
        return False
    envelope = entry["envelope"]
    correlation = envelope.get("correlation_id")
    if bool(declaration.get("correlates_to")) != (correlation is not None):
        return False
    try:
        payload = decoded_typed_value(envelope["payload"])
    except ArtifactError:
        return False
    normalized = _normalize_payload(declaration, payload)
    return normalized is not None and typed_value(normalized) == envelope["payload"]


def migrate_aggregate_v2(
    aggregate: ArtifactSource,
    target_validated_bundle_fingerprint: str,
    migration_route: Sequence[str],
    artifact_resolver: Any,
    *,
    maintenance_mode: bool,
    resource_limits: MigrationLimits | None = None,
    _include_host_evidence: bool = False,
) -> dict[str, Any]:
    """Migrate one queue-bearing aggregate through exact version-2 descriptors."""
    restored = restore_aggregate_v2(aggregate, artifact_resolver)
    limits = resource_limits or MigrationLimits()
    if (
        not isinstance(migration_route, Sequence)
        or isinstance(migration_route, str | bytes)
        or not all(isinstance(item, str) and item for item in migration_route)
        or not isinstance(target_validated_bundle_fingerprint, str)
        or not target_validated_bundle_fingerprint
        or not isinstance(maintenance_mode, bool)
    ):
        raise ArtifactError(PersistenceFailureCode.INVALID_MIGRATION_REQUEST)
    if len(migration_route) > limits.maximum_chain_length:
        raise ArtifactError(PersistenceFailureCode.MIGRATION_RESOURCE_LIMIT_EXCEEDED)
    descriptors: list[dict[str, Any]] = []
    base_descriptors: dict[str, Mapping[str, Any]] = {}
    for digest in migration_route:
        source = artifact_resolver.resolve_migration_descriptor(digest)
        if source is None or not artifact_resolver.migration_descriptor_is_trusted(digest):
            raise ArtifactError(PersistenceFailureCode.MIGRATION_DESCRIPTOR_UNTRUSTED)
        descriptor, _ = load_json_artifact(source, "migration_descriptor_v2")
        if migration_descriptor_digest(descriptor) != digest:
            raise ArtifactError(PersistenceFailureCode.INVALID_MIGRATION_DESCRIPTOR)
        base = descriptor["base_descriptor"]
        base_digest = base["migration_descriptor_digest"]
        base_descriptors[base_digest] = base
        descriptors.append(descriptor)
    if not descriptors:
        if (
            restored.aggregate_envelope["validated_bundle_fingerprint"]
            != target_validated_bundle_fingerprint
        ):
            raise ArtifactError(PersistenceFailureCode.MIGRATION_ROUTE_MISSING)
        return {
            "result": "success",
            "aggregate_state": copy.deepcopy(restored.aggregate_envelope),
            "dispositions": [],
        }
    if len({item["migration_descriptor_digest"] for item in descriptors}) != len(descriptors):
        raise ArtifactError(PersistenceFailureCode.MIGRATION_ROUTE_MISMATCH)
    fingerprints = [descriptors[0]["base_descriptor"]["source_validated_bundle_fingerprint"]] + [
        descriptor["base_descriptor"]["target_validated_bundle_fingerprint"]
        for descriptor in descriptors
    ]
    if (
        fingerprints[0] != restored.aggregate_envelope["validated_bundle_fingerprint"]
        or fingerprints[-1] != target_validated_bundle_fingerprint
        or len(set(fingerprints)) != len(fingerprints)
        or any(
            left["base_descriptor"]["target_validated_bundle_fingerprint"]
            != right["base_descriptor"]["source_validated_bundle_fingerprint"]
            for left, right in zip(descriptors, descriptors[1:], strict=False)
        )
    ):
        raise ArtifactError(PersistenceFailureCode.MIGRATION_ROUTE_MISMATCH)

    resolver = cast(Any, _V1MigrationResolver(artifact_resolver, base_descriptors))
    candidate = copy.deepcopy(restored.aggregate_envelope)
    dispositions: list[dict[str, Any]] = []
    disposed_entries: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    for descriptor in descriptors:
        before_digest = candidate["aggregate_state_digest"]
        source_runtimes = candidate["runtimes"]
        base = descriptor["base_descriptor"]
        migration = migrate_aggregate(
            _project_v1(candidate),
            base["target_validated_bundle_fingerprint"],
            [base["migration_descriptor_digest"]],
            resolver,
            maintenance_mode=maintenance_mode,
            resource_limits=limits,
        )
        if not migration.succeeded or migration.aggregate_envelope is None:
            assert migration.failure is not None
            raise ArtifactError(migration.failure.code)
        hop = _upgrade_document(migration.aggregate_envelope)
        hop["next_acceptance_sequence"] = candidate["next_acceptance_sequence"]
        hop["next_queue_sequence"] = candidate["next_queue_sequence"]
        target_runtimes = {runtime["runtime_id"]: runtime for runtime in hop["runtimes"]}
        target_bundle_source = artifact_resolver.resolve_definition(
            base["target_validated_bundle_fingerprint"]
        )
        if target_bundle_source is None:
            raise ArtifactError(PersistenceFailureCode.TARGET_DEFINITION_UNAVAILABLE)
        target_bundle = (
            target_bundle_source
            if isinstance(target_bundle_source, Bundle)
            else load_bundle(target_bundle_source)
        )
        target_state = restore_aggregate(_project_v1(hop), artifact_resolver).state
        for source_runtime in source_runtimes:
            runtime = target_runtimes.get(source_runtime["runtime_id"])
            for mailbox_name in ("ready_mailbox", "deferred_mailbox"):
                output = runtime[mailbox_name] if runtime is not None else None
                for entry in source_runtime[mailbox_name]:
                    rule = _queue_rule(descriptor, source_runtime, entry)
                    if rule is not None and rule["action"] == "dispose":
                        dispositions.append(
                            {
                                "disposition": "migration_disposed",
                                "reason": rule["reason"],
                                "migration_descriptor_digest": descriptor[
                                    "migration_descriptor_digest"
                                ],
                            }
                        )
                        disposed_entries.append(copy.deepcopy(entry))
                        continue
                    if runtime is None or not _queue_compatible(target_bundle, runtime, entry):
                        raise ArtifactError(PersistenceFailureCode.MIGRATION_TOTALITY_FAILURE)
                    assert output is not None
                    output.append(copy.deepcopy(entry))
        for runtime in hop["runtimes"]:
            ready = runtime["ready_mailbox"]
            deferred = runtime["deferred_mailbox"]
            if _runtime_is_runnable(target_state, runtime["runtime_id"]):
                for entry in list(deferred):
                    if not _structurally_deferred(
                        target_bundle,
                        target_state,
                        runtime["runtime_id"],
                        entry["envelope"]["event"],
                    ):
                        deferred.remove(entry)
                        entry["queue_sequence"] = hop["next_queue_sequence"]
                        hop["next_queue_sequence"] = str(int(hop["next_queue_sequence"]) + 1)
                        ready.append(entry)
            capacity = _runtime_capacity(target_bundle, target_state, runtime["runtime_id"])
            if capacity is not None and len(deferred) > capacity:
                raise ArtifactError(PersistenceFailureCode.MIGRATION_TOTALITY_FAILURE)
        candidate = seal_aggregate_v2(hop)
        restore_aggregate_v2(candidate, artifact_resolver)
        audits.append(
            {
                "migration_audit_record_schema_version": 1,
                "root_instance_id": candidate["root_instance_id"],
                "root_runtime_id": candidate["root_runtime_id"],
                "migration_sequence": candidate["migration_sequence"],
                "source_validated_bundle_fingerprint": base["source_validated_bundle_fingerprint"],
                "target_validated_bundle_fingerprint": base["target_validated_bundle_fingerprint"],
                "migration_descriptor_digest": descriptor["migration_descriptor_digest"],
                "source_aggregate_state_digest": before_digest,
                "target_aggregate_state_digest": candidate["aggregate_state_digest"],
                "result_code": "migration_applied",
            }
        )
    result: dict[str, Any] = {
        "result": "success",
        "aggregate_state": candidate,
        "dispositions": dispositions,
    }
    if _include_host_evidence:
        result["_audit_records"] = audits
        result["_disposed_entries"] = disposed_entries
    return result
