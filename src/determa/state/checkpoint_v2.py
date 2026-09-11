"""Queue-bearing execution-checkpoint version 2 operations."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .checkpoint import (
    execution_checkpoint_digest,
    seal_execution_checkpoint,
)
from .codes import CheckpointArtifactFailureCode, CheckpointHostFailureCode
from .errors import ArtifactError
from .host import (
    creation_request_digest,
    maintenance_migration_request_digest,
)
from .queueing import (
    _entry_digest,
    _valid_envelope_shape,
    _validate_new_deliveries,
    admit_aggregate_v2,
    create_aggregate_v2,
    restore_aggregate_v2,
    step_aggregate_v2,
)
from .wire import (
    ArtifactSource,
    DefinitionResolver,
    canonical_bytes,
    decimal,
    load_json_artifact,
)


@dataclass(frozen=True)
class RestoredExecutionCheckpoint:
    """One verified execution checkpoint."""

    document: dict[str, Any]
    canonical_bytes: bytes
    source_bytes: bytes


def _invalid() -> ArtifactError:
    return ArtifactError(CheckpointArtifactFailureCode.INVALID_EXECUTION_CHECKPOINT)


def _mailbox_entries(aggregate: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        entry
        for runtime in aggregate["runtimes"]
        for mailbox in ("ready_mailbox", "deferred_mailbox")
        for entry in runtime[mailbox]
    ]


def _synchronize_mailbox_references(checkpoint: dict[str, Any]) -> None:
    aggregate = checkpoint["root_record"].get("aggregate_state")
    if aggregate is None:
        return
    locations = {
        (entry["envelope"]["event_id"], entry["acceptance_sequence"]): entry["queue_sequence"]
        for entry in _mailbox_entries(aggregate)
    }
    for receipt in checkpoint["operation_receipts"]:
        for reference in receipt.get("emission_references", []):
            if reference.get("kind") != "internal_mailbox":
                continue
            key = (reference["event_id"], reference["acceptance_sequence"])
            if key in locations:
                reference["queue_sequence"] = locations[key]


def _terminalize_mailbox_reference(
    checkpoint: dict[str, Any],
    entry: Mapping[str, Any],
    terminal_receipt_sequence: str,
) -> None:
    for receipt in checkpoint["operation_receipts"]:
        for reference in receipt.get("emission_references", []):
            if (
                reference.get("kind") == "internal_mailbox"
                and reference.get("event_id") == entry["envelope"]["event_id"]
                and reference.get("acceptance_sequence") == entry["acceptance_sequence"]
            ):
                reference.pop("queue_sequence")
                reference["kind"] = "internal_terminal"
                reference["terminal_receipt_sequence"] = terminal_receipt_sequence


def _validate_checkpoint_semantics(document: dict[str, Any]) -> None:
    revision = decimal(document["revision"])
    next_receipt = decimal(document["next_operation_receipt_sequence"])
    receipts = document["operation_receipts"]
    sequences = [decimal(receipt["receipt_sequence"]) for receipt in receipts]
    if (
        not receipts
        or sequences != sorted(sequences)
        or len(sequences) != len(set(sequences))
        or any(sequence >= next_receipt for sequence in sequences)
        or receipts[0]["receipt_sequence"] != "0"
    ):
        raise _invalid()
    cutoff_value = document["replay_retention"]["pruned_through_receipt_sequence"]
    cutoff = decimal(cutoff_value) if cutoff_value is not None else None
    if cutoff is not None and cutoff >= next_receipt:
        raise _invalid()
    first_retained = 1 if cutoff is None else cutoff + 1
    if len(sequences) != 1 + max(0, next_receipt - first_retained):
        raise _invalid()
    if any(
        sequence != (0 if index == 0 else first_retained + index - 1)
        for index, sequence in enumerate(sequences)
    ):
        raise _invalid()

    root = document["root_record"]
    aggregate = root.get("aggregate_state")
    if aggregate is not None and (aggregate["root_instance_id"] != document["root_instance_id"]):
        raise _invalid()
    creation = receipts[0]
    root_creation_id = (
        aggregate["creation_id"] if aggregate is not None else root.get("creation_id")
    )
    if creation.get("creation_id") != root_creation_id:
        raise _invalid()
    if (
        aggregate is not None
        and revision == 0
        and creation.get("resulting_aggregate_state_digest")
        != aggregate["aggregate_state_digest"]
    ):
        raise _invalid()

    pending_entries = _mailbox_entries(aggregate) if aggregate is not None else []
    pending_by_event = {entry["envelope"]["event_id"]: entry for entry in pending_entries}
    if len(pending_by_event) != len(pending_entries):
        raise _invalid()
    acceptances: dict[str, dict[str, Any]] = {}
    terminals: dict[str, dict[str, Any]] = {}
    referenced_pending: dict[str, dict[str, Any]] = {}
    referenced_terminal: dict[str, dict[str, Any]] = {}
    pending_producers: dict[str, dict[str, Any]] = {}
    terminal_producers: dict[str, dict[str, Any]] = {}
    referenced_effects: set[str] = set()
    for receipt in receipts:
        kind = receipt["operation_kind"]
        if kind == "acceptance":
            if decimal(receipt["accepted_revision"]) > revision:
                raise _invalid()
            event_id = receipt["event_id"]
            if event_id in acceptances:
                raise _invalid()
            acceptances[event_id] = receipt
        elif kind == "event_terminal":
            if decimal(receipt["committed_revision"]) > revision:
                raise _invalid()
            event_id = receipt["event_id"]
            if event_id in terminals:
                raise _invalid()
            terminals[event_id] = receipt
        elif kind == "creation" and decimal(receipt["committed_revision"]) > revision:
            raise _invalid()
        references = receipt.get("emission_references")
        for reference in references or []:
            reference_kind = reference["kind"]
            if reference_kind == "internal_mailbox":
                event_id = reference["event_id"]
                if event_id in referenced_pending:
                    raise _invalid()
                referenced_pending[event_id] = reference
                pending_producers[event_id] = receipt
            elif reference_kind == "internal_terminal":
                event_id = reference["event_id"]
                if event_id in referenced_terminal:
                    raise _invalid()
                referenced_terminal[event_id] = reference
                terminal_producers[event_id] = receipt
            elif reference_kind == "external_outbox":
                effect_id = reference["effect_id"]
                if effect_id in referenced_effects:
                    raise _invalid()
                referenced_effects.add(effect_id)
    for event_id, entry in pending_by_event.items():
        acceptance = acceptances.get(event_id)
        producer = referenced_pending.get(event_id)
        if acceptance is None and not producer:
            raise _invalid()
        if acceptance is not None and producer is not None:
            raise _invalid()
        if acceptance is not None and (
            acceptance["request_digest"] != entry["envelope_digest"]
            or acceptance["acceptance_sequence"] != entry["acceptance_sequence"]
        ):
            raise _invalid()
        if producer is not None and (
            producer["acceptance_sequence"] != entry["acceptance_sequence"]
            or producer["queue_sequence"] != entry["queue_sequence"]
        ):
            raise _invalid()
        source = entry["envelope"]["source"]
        if source == {"host": True}:
            if acceptance is None:
                raise _invalid()
        elif acceptance is not None:
            raise _invalid()
    if set(pending_by_event) & set(terminals):
        raise _invalid()

    for event_id, terminal in terminals.items():
        acceptance = acceptances.get(event_id)
        producer = referenced_terminal.get(event_id)
        if acceptance is None and producer is None and cutoff is None:
            raise _invalid()
        if acceptance is not None and producer is not None:
            raise _invalid()
        if acceptance is not None and (
            terminal["request_digest"] != acceptance["request_digest"]
            or terminal["acceptance_sequence"] != acceptance["acceptance_sequence"]
            or decimal(acceptance["receipt_sequence"]) >= decimal(terminal["receipt_sequence"])
            or decimal(acceptance["accepted_revision"]) > decimal(terminal["committed_revision"])
        ):
            raise _invalid()
        if producer is not None and (
            producer["terminal_receipt_sequence"] != terminal["receipt_sequence"]
            or producer["acceptance_sequence"] != terminal["acceptance_sequence"]
            or decimal(terminal_producers[event_id]["receipt_sequence"])
            >= decimal(terminal["receipt_sequence"])
            or decimal(terminal_producers[event_id].get("committed_revision", "0"))
            > decimal(terminal["committed_revision"])
        ):
            raise _invalid()
    tombstones = document["event_identity_tombstones"]
    tombstone_events = [item["event_id"] for item in tombstones]
    tombstone_sequences = [decimal(item["terminal_receipt_sequence"]) for item in tombstones]
    if (
        len(tombstone_events) != len(set(tombstone_events))
        or set(tombstone_events) & set(pending_by_event)
        or set(tombstone_events) & set(acceptances)
        or set(tombstone_events) & set(terminals)
        or set(tombstone_sequences)
        & {decimal(item["receipt_sequence"]) for item in terminals.values()}
        or any(sequence >= next_receipt for sequence in tombstone_sequences)
        or tombstone_sequences != sorted(tombstone_sequences)
    ):
        raise _invalid()
    tombstones_by_event = {item["event_id"]: item for item in tombstones}
    if any(
        event_id not in pending_by_event and event_id not in terminals for event_id in acceptances
    ):
        raise _invalid()
    if any(
        decimal(producer["receipt_sequence"]) >= next_receipt
        for producer in [*pending_producers.values(), *terminal_producers.values()]
    ):
        raise _invalid()
    for event_id, reference in referenced_terminal.items():
        terminal_evidence = terminals.get(event_id)
        tombstone = tombstones_by_event.get(event_id)
        evidence = terminal_evidence if terminal_evidence is not None else tombstone
        evidence_sequence = (
            evidence["receipt_sequence"]
            if terminal_evidence is not None and evidence is not None
            else evidence["terminal_receipt_sequence"]
            if evidence is not None
            else None
        )
        if evidence_sequence is None or reference["terminal_receipt_sequence"] != evidence_sequence:
            raise _invalid()

    receipt_chronology: list[tuple[int, int]] = []
    greatest_prior_revision = -1
    for receipt, receipt_sequence in zip(receipts, sequences, strict=True):
        if "committed_revision" in receipt:
            effective_revision = receipt["committed_revision"]
        else:
            effective_revision = receipt["accepted_revision"]
        effective_revision_value = decimal(effective_revision)
        if (
            receipt["operation_kind"] == "maintenance_migration"
            and effective_revision_value <= greatest_prior_revision
        ):
            raise _invalid()
        receipt_chronology.append((effective_revision_value, receipt_sequence))
        greatest_prior_revision = max(greatest_prior_revision, effective_revision_value)
    if receipt_chronology != sorted(receipt_chronology):
        raise _invalid()

    allocation_acceptances = (
        [decimal(entry["acceptance_sequence"]) for entry in pending_entries]
        + [decimal(item["acceptance_sequence"]) for item in acceptances.values()]
        + [decimal(item["acceptance_sequence"]) for item in terminals.values()]
        + [decimal(item["acceptance_sequence"]) for item in tombstones]
    )
    allocation_queues = [decimal(entry["queue_sequence"]) for entry in pending_entries] + [
        decimal(item["final_queue_sequence"]) for item in terminals.values()
    ]
    if len(allocation_queues) != len(set(allocation_queues)):
        raise _invalid()
    acceptance_owners: dict[int, str] = {}
    for item in [
        *pending_entries,
        *acceptances.values(),
        *terminals.values(),
        *tombstones,
    ]:
        sequence = decimal(item["acceptance_sequence"])
        envelope = item.get("envelope", {})
        event_id = item.get("event_id", envelope.get("event_id"))
        prior_owner = acceptance_owners.setdefault(sequence, event_id)
        if prior_owner != event_id:
            raise _invalid()
    if aggregate is not None and (
        any(
            value >= decimal(aggregate["next_acceptance_sequence"])
            for value in allocation_acceptances
        )
        or any(value >= decimal(aggregate["next_queue_sequence"]) for value in allocation_queues)
    ):
        raise _invalid()
    current_aggregate_digest = (
        aggregate["aggregate_state_digest"]
        if aggregate is not None
        else root.get("final_aggregate_state_digest")
    )
    digests_by_revision: dict[int, str] = {}
    for terminal in terminals.values():
        committed_revision = decimal(terminal["committed_revision"])
        digest = terminal["resulting_aggregate_state_digest"]
        prior_digest = digests_by_revision.setdefault(committed_revision, digest)
        if prior_digest != digest or (
            committed_revision == revision and digest != current_aggregate_digest
        ):
            raise _invalid()

    pending_effects = {item["intent"]["effect_id"] for item in document["pending_outbox_intents"]}
    terminal_effects = {item["intent"]["effect_id"] for item in document["terminal_outbox_records"]}
    effect_tombstones = {item["effect_id"] for item in document["outbox_effect_tombstones"]}
    if (
        len(pending_effects) != len(document["pending_outbox_intents"])
        or len(terminal_effects) != len(document["terminal_outbox_records"])
        or len(effect_tombstones) != len(document["outbox_effect_tombstones"])
        or pending_effects & terminal_effects
        or pending_effects & effect_tombstones
        or terminal_effects & effect_tombstones
        or referenced_effects != pending_effects | terminal_effects | effect_tombstones
    ):
        raise _invalid()
    terminal_sequences = [
        decimal(item["terminal_sequence"]) for item in document["terminal_outbox_records"]
    ] + [decimal(item["terminal_sequence"]) for item in document["outbox_effect_tombstones"]]
    terminal_record_sequences = [
        decimal(item["terminal_sequence"]) for item in document["terminal_outbox_records"]
    ]
    effect_tombstone_sequences = [
        decimal(item["terminal_sequence"]) for item in document["outbox_effect_tombstones"]
    ]
    if (
        len(terminal_sequences) != len(set(terminal_sequences))
        or terminal_record_sequences != sorted(terminal_record_sequences)
        or effect_tombstone_sequences != sorted(effect_tombstone_sequences)
        or any(
            value >= decimal(document["next_outbox_terminal_sequence"])
            for value in terminal_sequences
        )
    ):
        raise _invalid()

    audits = document["migration_audit_records"]
    audit_sequences = [decimal(item["migration_sequence"]) for item in audits]
    if (
        audit_sequences != sorted(audit_sequences)
        or len(audit_sequences) != len(set(audit_sequences))
        or any(item["root_instance_id"] != document["root_instance_id"] for item in audits)
        or (
            aggregate is not None
            and any(
                sequence > decimal(aggregate["migration_sequence"]) for sequence in audit_sequences
            )
        )
    ):
        raise _invalid()

    audit_by_sequence = {item["migration_sequence"]: item for item in audits}
    root_runtime_id = (
        aggregate["root_runtime_id"] if aggregate is not None else root["root_runtime_id"]
    )
    if any(item["root_runtime_id"] != root_runtime_id for item in audits):
        raise _invalid()
    if aggregate is not None and audit_sequences and max(audit_sequences) > decimal(
        aggregate["migration_sequence"]
    ):
        raise _invalid()

    operation_ids: set[str] = set()
    referenced_audit_sequences: list[str] = []
    for receipt in receipts:
        if receipt["operation_kind"] != "maintenance_migration":
            continue
        operation_id = receipt["operation_id"]
        committed_revision = decimal(receipt["committed_revision"])
        if (
            operation_id in operation_ids
            or committed_revision == 0
            or committed_revision > revision
        ):
            raise _invalid()
        operation_ids.add(operation_id)
        sequences_for_receipt = receipt["migration_sequences"]
        selected = [audit_by_sequence.get(sequence) for sequence in sequences_for_receipt]
        if any(item is None for item in selected):
            raise _invalid()
        linked = [item for item in selected if item is not None]
        referenced_audit_sequences.extend(sequences_for_receipt)
        if receipt["result_code"] == "migration_no_operation":
            if (
                sequences_for_receipt
                or receipt["source_aggregate_state_digest"]
                != receipt["resulting_aggregate_state_digest"]
            ):
                raise _invalid()
            descriptor_route: list[str] = []
        else:
            if (
                not linked
                or [item["migration_sequence"] for item in linked]
                != sequences_for_receipt
                or any(
                    decimal(right["migration_sequence"])
                    != decimal(left["migration_sequence"]) + 1
                    for left, right in zip(linked, linked[1:], strict=False)
                )
                or linked[0]["source_aggregate_state_digest"]
                != receipt["source_aggregate_state_digest"]
                or linked[-1]["target_aggregate_state_digest"]
                != receipt["resulting_aggregate_state_digest"]
                or any(
                    left["target_aggregate_state_digest"]
                    != right["source_aggregate_state_digest"]
                    or left["target_validated_bundle_fingerprint"]
                    != right["source_validated_bundle_fingerprint"]
                    for left, right in zip(linked, linked[1:], strict=False)
                )
            ):
                raise _invalid()
            descriptor_route = [item["migration_descriptor_digest"] for item in linked]
            if (
                linked[-1]["target_validated_bundle_fingerprint"]
                != receipt["target_validated_bundle_fingerprint"]
            ):
                raise _invalid()
        possible_request_digests = {
            maintenance_migration_request_digest(
                document["root_instance_id"],
                operation_id,
                receipt["source_aggregate_state_digest"],
                receipt["target_validated_bundle_fingerprint"],
                descriptor_route,
                maintenance_mode,
            )
            for maintenance_mode in (False, True)
        }
        if receipt["request_digest"] not in possible_request_digests:
            raise _invalid()
    if len(referenced_audit_sequences) != len(set(referenced_audit_sequences)):
        raise _invalid()
    referenced_audits = set(referenced_audit_sequences)
    available_audits = set(audit_by_sequence)
    if not referenced_audits.issubset(available_audits):
        raise _invalid()


def restore_execution_checkpoint_v2(
    source: ArtifactSource, definition_resolver: DefinitionResolver
) -> RestoredExecutionCheckpoint:
    """Restore one structurally and relationally valid version-2 checkpoint."""
    document, raw = load_json_artifact(source, "execution_checkpoint_v2")
    if execution_checkpoint_digest(document) != document["execution_checkpoint_digest"]:
        raise ArtifactError(CheckpointArtifactFailureCode.EXECUTION_CHECKPOINT_DIGEST_MISMATCH)
    aggregate = document["root_record"].get("aggregate_state")
    if aggregate is not None:
        try:
            restore_aggregate_v2(aggregate, definition_resolver)
        except ArtifactError as error:
            if error.code in {
                "invalid_aggregate_state",
                "aggregate_state_digest_mismatch",
            }:
                raise _invalid() from error
            raise
    try:
        _validate_checkpoint_semantics(document)
    except (ArtifactError, KeyError, TypeError, ValueError) as error:
        if isinstance(error, ArtifactError) and error.code == str(
            CheckpointArtifactFailureCode.INVALID_EXECUTION_CHECKPOINT.value
        ):
            raise
        raise _invalid() from error
    return RestoredExecutionCheckpoint(
        document=copy.deepcopy(document),
        canonical_bytes=canonical_bytes(document),
        source_bytes=raw,
    )


def _append_external_intent(
    checkpoint: dict[str, Any],
    references: list[dict[str, Any]],
    emission: Mapping[str, Any],
    emission_index: int,
) -> None:
    intent = {
        "effect_id": emission["effect_id"],
        "sequence": emission["sequence"],
        "event": emission["event"],
        "payload": copy.deepcopy(emission["payload"]),
        "correlation_id": emission["correlation_id"],
    }
    checkpoint["pending_outbox_intents"].append(
        {
            "intent": intent,
            "state_revision": checkpoint["revision"],
            "delivery_state": {"status": "not_attempted"},
        }
    )
    references.append(
        {
            "kind": "external_outbox",
            "emission_index": str(emission_index),
            "effect_id": emission["effect_id"],
        }
    )


def create_checkpoint_v2(
    bundle: Any,
    machine_id: str,
    root_instance_id: str,
    creation_id: str,
    bindings: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Create a fresh queue-bearing checkpoint from the public v2 core result."""
    result = create_aggregate_v2(
        bundle,
        machine_id,
        root_instance_id,
        creation_id,
        bindings,
        _include_host_evidence=True,
    )
    aggregate = result["state"]
    if aggregate is None:
        raise ArtifactError(CheckpointHostFailureCode.CREATION_REJECTED)
    receipt = {
        "operation_kind": "creation",
        "receipt_sequence": "0",
        "creation_id": creation_id,
        "request_digest": creation_request_digest(
            bundle, machine_id, root_instance_id, creation_id, bindings or {}
        ),
        "committed_revision": "0",
        "resulting_aggregate_state_digest": aggregate["aggregate_state_digest"],
        "status": result["status"],
        "fault": copy.deepcopy(result["fault"]),
        "emission_references": [],
    }
    checkpoint: dict[str, Any] = {
        "execution_checkpoint_format": "determa.execution_checkpoint",
        "execution_checkpoint_schema_version": 2,
        "root_instance_id": root_instance_id,
        "revision": "0",
        "root_record": {"status": "retained", "aggregate_state": aggregate},
        "replay_retention": {
            "mode": "permanent",
            "permanent_replay_eligible": True,
            "pruned_through_receipt_sequence": None,
            "policy_identifier": None,
        },
        "next_operation_receipt_sequence": "1",
        "operation_receipts": [receipt],
        "event_identity_tombstones": [],
        "pending_outbox_intents": [],
        "next_outbox_terminal_sequence": "0",
        "terminal_outbox_records": [],
        "outbox_effect_tombstones": [],
        "migration_audit_records": [],
    }
    lifecycle_sequences = [str(index + 1) for index in range(len(result["lifecycle_dispositions"]))]
    checkpoint["next_operation_receipt_sequence"] = str(1 + len(lifecycle_sequences))
    for index, emission in enumerate(result["emissions"]):
        if "kind" not in emission:
            emission_index = int(emission.get("_determa_v2_emission_index", index))
            _append_external_intent(
                checkpoint, receipt["emission_references"], emission, emission_index
            )
        elif emission["kind"] == "internal_mailbox":
            receipt["emission_references"].append(copy.deepcopy(emission))
        else:
            lifecycle_index = int(emission["lifecycle_disposition_index"])
            receipt["emission_references"].append(
                {
                    "kind": "internal_terminal",
                    "emission_index": emission["emission_index"],
                    "event_id": emission["event_id"],
                    "acceptance_sequence": emission["acceptance_sequence"],
                    "terminal_receipt_sequence": lifecycle_sequences[lifecycle_index],
                }
            )
    for lifecycle, sequence in zip(
        result["lifecycle_dispositions"], lifecycle_sequences, strict=True
    ):
        checkpoint["operation_receipts"].append(
            {
                "operation_kind": "event_terminal",
                "receipt_sequence": sequence,
                "event_id": lifecycle["event_id"],
                "request_digest": lifecycle["request_digest"],
                "acceptance_sequence": lifecycle["acceptance_sequence"],
                "final_queue_sequence": lifecycle["final_queue_sequence"],
                "committed_revision": "0",
                "resulting_aggregate_state_digest": aggregate["aggregate_state_digest"],
                "outcome": {
                    "status": result["status"],
                    "disposition": "disposed",
                    "reason": lifecycle["reason"],
                    "fault": None,
                    "rejection": None,
                },
                "emission_references": [],
            }
        )
    return seal_execution_checkpoint(checkpoint)


def _check_cas(document: Mapping[str, Any], revision: str, digest: str) -> None:
    if document["revision"] != revision or document["execution_checkpoint_digest"] != digest:
        raise ArtifactError(CheckpointHostFailureCode.CHECKPOINT_REVISION_CONFLICT)


def _pending_replay(
    document: Mapping[str, Any], event_id: str, request_digest: str
) -> dict[str, Any] | None:
    aggregate = document["root_record"].get("aggregate_state")
    if aggregate is None:
        return None
    for runtime in aggregate["runtimes"]:
        for mailbox, location in (
            ("ready_mailbox", "ready"),
            ("deferred_mailbox", "deferred"),
        ):
            for entry in runtime[mailbox]:
                if entry["envelope"]["event_id"] == event_id:
                    if entry["envelope_digest"] != request_digest:
                        raise ArtifactError(CheckpointHostFailureCode.EVENT_ID_CONFLICT)
                    return {
                        "result": "replay",
                        "event_id": event_id,
                        "acceptance_sequence": entry["acceptance_sequence"],
                        "location": location,
                    }
    return None


def _retained_replay(
    document: Mapping[str, Any], event_id: str, request_digest: str
) -> dict[str, Any] | None:
    terminal = next(
        (
            receipt
            for receipt in document["operation_receipts"]
            if receipt["operation_kind"] == "event_terminal" and receipt["event_id"] == event_id
        ),
        None,
    )
    acceptance = next(
        (
            receipt
            for receipt in document["operation_receipts"]
            if receipt["operation_kind"] == "acceptance" and receipt["event_id"] == event_id
        ),
        None,
    )
    if terminal is not None:
        if terminal["request_digest"] != request_digest:
            raise ArtifactError(CheckpointHostFailureCode.EVENT_ID_CONFLICT)
        return {
            "result": "replay",
            "acceptance_receipt_sequence": (
                acceptance["receipt_sequence"] if acceptance is not None else "0"
            ),
            "terminal_receipt_sequence": terminal["receipt_sequence"],
        }
    tombstone = next(
        (item for item in document["event_identity_tombstones"] if item["event_id"] == event_id),
        None,
    )
    if tombstone is not None:
        if tombstone["request_digest"] != request_digest:
            raise ArtifactError(CheckpointHostFailureCode.EVENT_ID_CONFLICT)
        result = {
            "result": "replay",
            "terminal_receipt_sequence": tombstone["terminal_receipt_sequence"],
            "terminal_disposition": tombstone["terminal_disposition"],
        }
        return result
    return None


def admit_checkpoint_v2(
    source: ArtifactSource,
    deliveries: Sequence[Mapping[str, Any]],
    definition_resolver: DefinitionResolver,
    *,
    expected_revision: str,
    expected_checkpoint_digest: str,
) -> dict[str, Any]:
    """Atomically admit or replay a delivery batch against one v2 checkpoint."""
    restored = restore_execution_checkpoint_v2(source, definition_resolver)
    document = restored.document
    snapshot = copy.deepcopy(deliveries)
    terminal_code: str | None
    if document["root_record"]["status"] == "tombstone":
        terminal_code = "tombstoned_root"
    else:
        aggregate = document["root_record"].get("aggregate_state")
        terminal_code = (
            "terminal_root"
            if aggregate is not None
            and next(
                runtime
                for runtime in aggregate["runtimes"]
                if runtime["runtime_id"] == aggregate["root_runtime_id"]
            )["status"]
            != "running"
            else None
        )
    if (
        not isinstance(deliveries, Sequence)
        or isinstance(deliveries, str | bytes)
        or not deliveries
    ):
        raise ArtifactError("malformed_delivery")

    canonical_digests: list[str] = []
    event_ids: list[str] = []
    target_roots: list[Any] = []
    for delivery in deliveries:
        expected_members = {"delivery_mode", "envelope", "envelope_digest"}
        envelope_is_valid = (
            _valid_envelope_shape(delivery.get("envelope"))
            if isinstance(delivery, Mapping)
            else False
        )
        if (
            not isinstance(delivery, Mapping)
            or set(delivery) != expected_members
            or not isinstance(delivery.get("delivery_mode"), str)
            or not isinstance(delivery.get("envelope_digest"), str)
            or not envelope_is_valid
        ):
            raise ArtifactError("malformed_delivery")
        envelope = delivery["envelope"]
        mode = delivery.get("delivery_mode")
        event_id = envelope.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            raise ArtifactError("malformed_delivery")
        target = envelope.get("target")
        if not isinstance(target, Mapping) or len(target) != 1:
            raise ArtifactError("malformed_delivery")
        identity = next(iter(target.values()))
        if not isinstance(identity, Mapping):
            raise ArtifactError("malformed_delivery")
        event_ids.append(event_id)
        target_roots.append(identity.get("root_instance_id"))
        digest = _entry_digest(document["root_instance_id"], str(mode), envelope)
        canonical_digests.append(digest)

    if any(root != document["root_instance_id"] for root in target_roots):
        raise ArtifactError("wrong_root")
    if len(event_ids) != len(set(event_ids)):
        raise ArtifactError("duplicate_event_id_in_batch")

    replay_evidence: list[dict[str, Any] | None] = []
    for delivery, digest in zip(deliveries, canonical_digests, strict=True):
        envelope = delivery["envelope"]
        event_id = envelope["event_id"]
        replay = _pending_replay(document, event_id, digest)
        if replay is None:
            replay = _retained_replay(document, event_id, digest)
        replay_evidence.append(replay)

    if all(evidence is not None for evidence in replay_evidence):
        replay_members = [
            {
                "event_id": delivery["envelope"]["event_id"],
                "disposition": "replay",
                "evidence": evidence,
            }
            for delivery, evidence in zip(deliveries, replay_evidence, strict=True)
        ]
        if len(replay_members) == 1:
            return dict(replay_members[0]["evidence"])
        return {"result": "batch", "checkpoint": document, "members": replay_members}
    if terminal_code is not None:
        raise ArtifactError(terminal_code)
    _check_cas(document, expected_revision, expected_checkpoint_digest)
    assert document["root_record"].get("aggregate_state") is not None
    aggregate = document["root_record"]["aggregate_state"]
    new_deliveries = [
        delivery
        for delivery, evidence in zip(deliveries, replay_evidence, strict=True)
        if evidence is None
    ]
    restored_aggregate = restore_aggregate_v2(aggregate, definition_resolver)
    validation_code = _validate_new_deliveries(restored_aggregate, new_deliveries)
    if validation_code is not None:
        raise ArtifactError(validation_code)
    admission = admit_aggregate_v2(aggregate, new_deliveries, definition_resolver)
    if admission["result"] == "rejected":
        raise ArtifactError(admission["rejection"]["code"])

    candidate = copy.deepcopy(document)
    candidate["revision"] = str(int(candidate["revision"]) + 1)
    candidate["root_record"]["aggregate_state"] = admission["state"]
    members: list[dict[str, Any]] = []
    accepted_by_id = {item["event_id"]: item for item in admission.get("accepted", [])}
    for delivery, evidence, digest in zip(
        deliveries, replay_evidence, canonical_digests, strict=True
    ):
        event_id = delivery["envelope"]["event_id"]
        if evidence is not None:
            members.append({"event_id": event_id, "disposition": "replay", "evidence": evidence})
            continue
        accepted = accepted_by_id[event_id]
        receipt_sequence = candidate["next_operation_receipt_sequence"]
        candidate["next_operation_receipt_sequence"] = str(int(receipt_sequence) + 1)
        candidate["operation_receipts"].append(
            {
                "operation_kind": "acceptance",
                "receipt_sequence": receipt_sequence,
                "event_id": event_id,
                "request_digest": digest,
                "acceptance_sequence": accepted["acceptance_sequence"],
                "accepted_revision": candidate["revision"],
                "delivery_mode": delivery["delivery_mode"],
            }
        )
        members.append({"event_id": event_id, "disposition": "accepted", **accepted})
        members[-1].pop("event_id", None)
        members[-1] = {"event_id": event_id, **members[-1]}
    sealed = seal_execution_checkpoint(candidate)
    assert deliveries == snapshot
    if all(member["disposition"] == "accepted" for member in members):
        return sealed
    return {"result": "batch", "checkpoint": sealed, "members": members}


def step_checkpoint_v2(
    source: ArtifactSource,
    target_runtime_id: str,
    definition_resolver: DefinitionResolver,
    *,
    expected_revision: str,
    expected_checkpoint_digest: str,
) -> dict[str, Any]:
    """Process one ready mailbox head and append its terminal receipt."""
    restored = restore_execution_checkpoint_v2(source, definition_resolver)
    document = restored.document
    _check_cas(document, expected_revision, expected_checkpoint_digest)
    aggregate = document["root_record"].get("aggregate_state")
    if aggregate is None:
        raise ArtifactError("tombstoned_root")
    runtime = next(
        (item for item in aggregate["runtimes"] if item["runtime_id"] == target_runtime_id),
        None,
    )
    selected = (
        copy.deepcopy(runtime["ready_mailbox"][0]) if runtime and runtime["ready_mailbox"] else None
    )
    if runtime is not None and selected is None:
        root_status = next(
            item["status"]
            for item in aggregate["runtimes"]
            if item["runtime_id"] == aggregate["root_runtime_id"]
        )
        return {
            "result": "not_committed",
            "step_result": {
                "core_step_result_format": "determa.core_step_result",
                "core_step_result_schema_version": 2,
                "status": root_status,
                "disposition": "not_runnable",
                "state": copy.deepcopy(aggregate),
                "emissions": [],
                "lifecycle_dispositions": [],
                "fault": None,
                "rejection": None,
            },
            "checkpoint": document,
        }
    result = step_aggregate_v2(
        aggregate,
        target_runtime_id,
        definition_resolver,
        _include_host_evidence=True,
    )
    if selected is None or result["disposition"] in {"not_runnable", "rejected"}:
        return {
            "result": "not_committed",
            "step_result": result,
            "checkpoint": document,
        }
    candidate = copy.deepcopy(document)
    candidate["revision"] = str(int(candidate["revision"]) + 1)
    candidate["root_record"]["aggregate_state"] = result["state"]
    if result["disposition"] == "deferred":
        _synchronize_mailbox_references(candidate)
        return seal_execution_checkpoint(candidate)
    receipt_sequence = candidate["next_operation_receipt_sequence"]
    lifecycle_sequences = [
        str(int(receipt_sequence) + index + 1)
        for index in range(len(result["lifecycle_dispositions"]))
    ]
    candidate["next_operation_receipt_sequence"] = str(
        int(receipt_sequence) + 1 + len(lifecycle_sequences)
    )
    _terminalize_mailbox_reference(candidate, selected, receipt_sequence)
    for lifecycle, terminal_sequence in zip(
        result["lifecycle_dispositions"], lifecycle_sequences, strict=True
    ):
        lifecycle_entry = {
            "acceptance_sequence": lifecycle["acceptance_sequence"],
            "envelope": {"event_id": lifecycle["event_id"]},
        }
        _terminalize_mailbox_reference(candidate, lifecycle_entry, terminal_sequence)
    references: list[dict[str, Any]] = []
    for emission_index, emission in enumerate(result["emissions"]):
        if "kind" not in emission:
            action_emission_index = int(
                emission.get("_determa_v2_emission_index", emission_index)
            )
            _append_external_intent(
                candidate, references, emission, action_emission_index
            )
            continue
        if emission["kind"] != "internal_disposed":
            references.append(copy.deepcopy(emission))
            continue
        index = int(emission["lifecycle_disposition_index"])
        references.append(
            {
                "kind": "internal_terminal",
                "emission_index": emission["emission_index"],
                "event_id": emission["event_id"],
                "acceptance_sequence": emission["acceptance_sequence"],
                "terminal_receipt_sequence": lifecycle_sequences[index],
            }
        )
    candidate["operation_receipts"].append(
        {
            "operation_kind": "event_terminal",
            "receipt_sequence": receipt_sequence,
            "event_id": selected["envelope"]["event_id"],
            "request_digest": selected["envelope_digest"],
            "acceptance_sequence": selected["acceptance_sequence"],
            "final_queue_sequence": selected["queue_sequence"],
            "committed_revision": candidate["revision"],
            "resulting_aggregate_state_digest": result["state"]["aggregate_state_digest"],
            "outcome": {
                "status": result["status"],
                "disposition": result["disposition"],
                "fault": copy.deepcopy(result["fault"]),
                "rejection": copy.deepcopy(result["rejection"]),
            },
            "emission_references": references,
        }
    )
    for lifecycle, terminal_sequence in zip(
        result["lifecycle_dispositions"], lifecycle_sequences, strict=True
    ):
        candidate["operation_receipts"].append(
            {
                "operation_kind": "event_terminal",
                "receipt_sequence": terminal_sequence,
                "event_id": lifecycle["event_id"],
                "request_digest": lifecycle["request_digest"],
                "acceptance_sequence": lifecycle["acceptance_sequence"],
                "final_queue_sequence": lifecycle["final_queue_sequence"],
                "committed_revision": candidate["revision"],
                "resulting_aggregate_state_digest": result["state"]["aggregate_state_digest"],
                "outcome": {
                    "status": result["status"],
                    "disposition": "disposed",
                    "reason": lifecycle["reason"],
                    "fault": None,
                    "rejection": None,
                },
                "emission_references": [],
            }
        )
    _synchronize_mailbox_references(candidate)
    return seal_execution_checkpoint(candidate)


def prune_checkpoint_v2(
    source: ArtifactSource,
    cutoff_receipt_sequence: str,
    definition_resolver: DefinitionResolver,
    *,
    target_mode: str | None = None,
    policy_identifier: str | None = None,
    expected_revision: str,
    expected_checkpoint_digest: str,
) -> dict[str, Any]:
    """Advance bounded replay retention through one dependency-closed cutoff."""
    restored = restore_execution_checkpoint_v2(source, definition_resolver)
    document = restored.document
    current_mode = document["replay_retention"]["mode"]
    selected_mode = current_mode if target_mode is None else target_mode
    if selected_mode not in {"permanent", "bounded"}:
        raise _invalid()
    if current_mode == "bounded" and selected_mode != "bounded":
        raise _invalid()
    if selected_mode == "permanent":
        raise _invalid()
    selected_policy = (
        document["replay_retention"]["policy_identifier"]
        if policy_identifier is None
        else policy_identifier
    )
    if not isinstance(selected_policy, str) or not selected_policy:
        raise _invalid()
    prior_value = document["replay_retention"]["pruned_through_receipt_sequence"]
    prior = decimal(prior_value) if prior_value is not None else -1
    try:
        cutoff = decimal(cutoff_receipt_sequence)
    except ArtifactError as error:
        raise _invalid() from error
    if cutoff == prior:
        return document
    if cutoff < prior or cutoff >= int(document["next_operation_receipt_sequence"]):
        raise _invalid()
    _check_cas(document, expected_revision, expected_checkpoint_digest)
    receipts = document["operation_receipts"]
    removed = [
        receipt
        for receipt in receipts
        if receipt["receipt_sequence"] != "0" and int(receipt["receipt_sequence"]) <= cutoff
    ]
    retained = [receipt for receipt in receipts if receipt not in removed]
    pending_ids = {
        entry["envelope"]["event_id"]
        for entry in _mailbox_entries(
            document["root_record"].get("aggregate_state") or {"runtimes": []}
        )
    }
    removed_sequences = {receipt["receipt_sequence"] for receipt in removed}
    pending_effects = {item["intent"]["effect_id"] for item in document["pending_outbox_intents"]}
    acceptance_by_event = {
        receipt["event_id"]: receipt
        for receipt in receipts
        if receipt["operation_kind"] == "acceptance"
    }
    terminals_by_event = {
        receipt["event_id"]: receipt
        for receipt in receipts
        if receipt["operation_kind"] == "event_terminal"
    }
    if any(
        receipt["operation_kind"] == "acceptance"
        and (
            receipt["event_id"] in pending_ids
            or terminals_by_event.get(receipt["event_id"], {}).get("receipt_sequence")
            not in removed_sequences
        )
        for receipt in removed
    ):
        raise _invalid()
    if any(
        reference.get("kind") == "internal_mailbox" and reference.get("event_id") in pending_ids
        for receipt in removed
        for reference in receipt.get("emission_references", [])
    ):
        raise _invalid()
    if any(
        reference.get("kind") == "external_outbox" and reference.get("effect_id") in pending_effects
        for receipt in removed
        for reference in receipt.get("emission_references", [])
    ):
        raise _invalid()
    for receipt in retained:
        for reference in receipt.get("emission_references", []):
            if reference.get("kind") == "internal_mailbox" and reference["event_id"] in pending_ids:
                continue
        if receipt["operation_kind"] == "event_terminal":
            acceptance = acceptance_by_event.get(receipt["event_id"])
            if acceptance and acceptance["receipt_sequence"] in removed_sequences:
                raise _invalid()

    candidate = copy.deepcopy(document)
    candidate["replay_retention"] = {
        "mode": "bounded",
        "permanent_replay_eligible": False,
        "pruned_through_receipt_sequence": candidate["replay_retention"][
            "pruned_through_receipt_sequence"
        ],
        "policy_identifier": selected_policy,
    }
    tombstones = list(candidate["event_identity_tombstones"])
    for receipt in removed:
        if receipt["operation_kind"] == "event_terminal":
            tombstones.append(
                {
                    "event_id": receipt["event_id"],
                    "request_digest": receipt["request_digest"],
                    "request_digest_domain": "determa-inbox-envelope-digest-2",
                    "acceptance_sequence": receipt["acceptance_sequence"],
                    "terminal_receipt_sequence": receipt["receipt_sequence"],
                    "terminal_disposition": receipt["outcome"]["disposition"],
                }
            )
    candidate["operation_receipts"] = retained
    referenced_migrations = {
        sequence
        for receipt in retained
        if receipt["operation_kind"] == "maintenance_migration"
        for sequence in receipt["migration_sequences"]
    }
    candidate["migration_audit_records"] = [
        audit
        for audit in candidate["migration_audit_records"]
        if audit["migration_sequence"] in referenced_migrations
    ]
    candidate["event_identity_tombstones"] = sorted(
        tombstones, key=lambda item: int(item["terminal_receipt_sequence"])
    )
    candidate["replay_retention"]["pruned_through_receipt_sequence"] = cutoff_receipt_sequence
    candidate["revision"] = str(int(candidate["revision"]) + 1)
    return seal_execution_checkpoint(candidate)
