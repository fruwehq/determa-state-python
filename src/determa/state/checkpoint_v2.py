"""Queue-bearing execution-checkpoint version 2 operations."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .checkpoint import (
    execution_checkpoint_digest,
    restore_execution_checkpoint,
    seal_execution_checkpoint,
    validate_execution_checkpoint_member,
)
from .codes import CheckpointArtifactFailureCode, CheckpointHostFailureCode
from .errors import ArtifactError
from .host import creation_request_digest, delivery_request_digest
from .queueing import (
    _entry_digest,
    _runtime_id_for_target,
    _valid_envelope_shape,
    admit_aggregate_v2,
    create_aggregate_v2,
    restore_aggregate_v2,
    seal_aggregate_v2,
    step_aggregate_v2,
    upgrade_aggregate_v1_to_v2,
)
from .wire import (
    ArtifactSource,
    DefinitionResolver,
    canonical_bytes,
    decimal,
    hash_value,
    load_json_artifact,
)


@dataclass(frozen=True)
class RestoredExecutionCheckpointV2:
    """One verified queue-bearing checkpoint."""

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
    legacy_creation = creation.get("legacy_receipt", creation)
    root_creation_id = (
        aggregate["creation_id"] if aggregate is not None else root.get("creation_id")
    )
    if legacy_creation.get("creation_id") != root_creation_id:
        raise _invalid()
    if (
        aggregate is not None
        and revision == 0
        and legacy_creation.get("resulting_aggregate_state_digest")
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
    legacy_wrappers: list[dict[str, Any]] = []
    legacy_producer_references: dict[
        str, list[tuple[dict[str, Any], dict[str, Any]]]
    ] = {}
    legacy_terminal_events: dict[str, dict[str, Any]] = {}
    referenced_effects: set[str] = set()
    committed_order: list[tuple[int, int]] = []
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
            committed_order.append(
                (
                    decimal(receipt["committed_revision"]),
                    decimal(receipt["receipt_sequence"]),
                )
            )
        elif kind == "creation" and decimal(receipt["committed_revision"]) > revision:
            raise _invalid()
        elif kind == "legacy_v1_operation":
            legacy_receipt = receipt["legacy_receipt"]
            if decimal(legacy_receipt["committed_revision"]) > revision:
                raise _invalid()
        if kind in {"legacy_v1_creation", "legacy_v1_operation"}:
            legacy_wrappers.append(receipt)
            legacy_receipt = receipt["legacy_receipt"]
            if legacy_receipt["receipt_sequence"] != receipt["receipt_sequence"]:
                raise _invalid()
            for reference in legacy_receipt.get("emission_references", []):
                if reference["kind"] == "internal_delivery":
                    legacy_producer_references.setdefault(reference["event_id"], []).append(
                        (receipt, reference)
                    )
        references = receipt.get("emission_references")
        if references is None and kind.startswith("legacy_v1_"):
            references = receipt["legacy_receipt"].get("emission_references", [])
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
    if legacy_wrappers and receipts[: len(legacy_wrappers)] != legacy_wrappers:
        raise _invalid()
    if committed_order != sorted(committed_order) or len(committed_order) != len(
        set(committed_order)
    ):
        raise _invalid()

    def validate_internal_legacy_producer(
        event_id: str,
        acceptance_sequence: str,
        accepted_revision: str,
        origin: Mapping[str, Any],
    ) -> None:
        producers = legacy_producer_references.get(event_id, [])
        if (
            len(producers) != 1
            or producers[0][0]["receipt_sequence"]
            != origin["producing_receipt_sequence"]
            or producers[0][1]["emission_index"] != origin["emission_index"]
            or producers[0][1]["delivery_sequence"] != acceptance_sequence
            or producers[0][0]["legacy_receipt"]["committed_revision"]
            != accepted_revision
        ):
            raise _invalid()

    legacy_terminal_acceptance_sequences: set[str] = set()
    for wrapper in legacy_wrappers:
        legacy_receipt = wrapper["legacy_receipt"]
        if legacy_receipt["operation_kind"] != "delivery":
            continue
        event_id = legacy_receipt["event_id"]
        acceptance_sequence = legacy_receipt["accepted_delivery_sequence"]
        if (
            event_id in legacy_terminal_events
            or acceptance_sequence in legacy_terminal_acceptance_sequences
        ):
            raise _invalid()
        legacy_terminal_events[event_id] = legacy_receipt
        legacy_terminal_acceptance_sequences.add(acceptance_sequence)
        if legacy_receipt["delivery_mode"] == "internal":
            origin = legacy_receipt["origin"]
            if not (
                origin.get("kind") == "internal_emission"
                and set(origin)
                == {"kind", "producing_receipt_sequence", "emission_index"}
            ):
                raise _invalid()
            validate_internal_legacy_producer(
                event_id,
                acceptance_sequence,
                legacy_receipt["accepted_revision"],
                origin,
            )

    def validate_legacy_evidence(
        acceptance: Mapping[str, Any], entry: Mapping[str, Any] | None
    ) -> None:
        evidence = acceptance.get("legacy_v1_delivery")
        if evidence is None:
            return
        origin = evidence["origin"]
        mode = acceptance["delivery_mode"]
        if (
            evidence["delivery_sequence"] != acceptance["acceptance_sequence"]
            or evidence["envelope_digest"] == "sha256:" + ("0" * 64)
            or evidence["envelope_digest"] == acceptance["request_digest"]
        ):
            raise _invalid()
        if mode == "input":
            if origin != {"kind": "host_input"}:
                raise _invalid()
        elif not (
            origin.get("kind") == "internal_emission"
            and set(origin) == {"kind", "producing_receipt_sequence", "emission_index"}
        ):
            raise _invalid()
        if entry is not None:
            source = entry["envelope"]["source"]
            legacy_envelope = copy.deepcopy(entry["envelope"])
            legacy_envelope.pop("cause_id")
            legacy_envelope.pop("source")
            expected_legacy_digest = hash_value(
                [
                    "determa-inbox-envelope-digest-1",
                    "1",
                    document["root_instance_id"],
                    entry["delivery_mode"],
                    legacy_envelope,
                ]
            )
            source_matches = (source == {"host": True} and mode == "input") or (
                mode == "internal"
                and source
                == {
                    "legacy_v1_internal": {
                        "producing_receipt_sequence": origin["producing_receipt_sequence"],
                        "emission_index": origin["emission_index"],
                    }
                }
            )
            if evidence["envelope_digest"] != expected_legacy_digest or not source_matches:
                raise _invalid()
        if mode == "internal":
            validate_internal_legacy_producer(
                acceptance["event_id"],
                acceptance["acceptance_sequence"],
                acceptance["accepted_revision"],
                origin,
            )

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
        if "legacy_v1_internal" in source:
            if acceptance is None or acceptance.get("legacy_v1_delivery") is None:
                raise _invalid()
            validate_legacy_evidence(acceptance, entry)
        elif source == {"host": True}:
            if acceptance is None:
                raise _invalid()
            validate_legacy_evidence(acceptance, entry)
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
        if acceptance is not None:
            validate_legacy_evidence(acceptance, None)

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
    if (
        set(legacy_terminal_events) & set(pending_by_event)
        or set(legacy_terminal_events) & set(terminals)
        or set(legacy_terminal_events) & set(tombstones_by_event)
        or set(legacy_terminal_events) & set(acceptances)
    ):
        raise _invalid()
    if any(
        event_id not in pending_by_event and event_id not in terminals for event_id in acceptances
    ):
        raise _invalid()
    if any(
        decimal(producer["receipt_sequence"]) >= next_receipt
        for producer in [*pending_producers.values(), *terminal_producers.values()]
    ):
        raise _invalid()
    for event_id, references in legacy_producer_references.items():
        if len(references) != 1:
            raise _invalid()
        located_entry = pending_by_event.get(event_id)
        located_terminal = terminals.get(event_id)
        located_tombstone = tombstones_by_event.get(event_id)
        located_legacy_terminal = legacy_terminal_events.get(event_id)
        locations = [
            item
            for item in (
                located_entry,
                located_terminal,
                located_tombstone,
                located_legacy_terminal,
            )
            if item is not None
        ]
        if len(locations) != 1:
            raise _invalid()
        allocation = locations[0].get(
            "acceptance_sequence", locations[0].get("accepted_delivery_sequence")
        )
        if references[0][1]["delivery_sequence"] != allocation:
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

    allocation_acceptances = (
        [decimal(entry["acceptance_sequence"]) for entry in pending_entries]
        + [decimal(item["acceptance_sequence"]) for item in acceptances.values()]
        + [decimal(item["acceptance_sequence"]) for item in terminals.values()]
        + [decimal(item["acceptance_sequence"]) for item in tombstones]
        + [
            decimal(item["accepted_delivery_sequence"])
            for item in legacy_terminal_events.values()
        ]
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
    for event_id, item in legacy_terminal_events.items():
        sequence = decimal(item["accepted_delivery_sequence"])
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


def restore_execution_checkpoint_v2(
    source: ArtifactSource, definition_resolver: DefinitionResolver
) -> RestoredExecutionCheckpointV2:
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
    return RestoredExecutionCheckpointV2(
        document=copy.deepcopy(document),
        canonical_bytes=canonical_bytes(document),
        source_bytes=raw,
    )


def upgrade_checkpoint_v1_to_v2(
    source: ArtifactSource, definition_resolver: DefinitionResolver
) -> dict[str, Any]:
    """Explicitly convert a complete version-1 checkpoint and pending history."""
    restored = restore_execution_checkpoint(source, definition_resolver)
    document = restored.document
    if document["root_record"]["status"] != "retained":
        raise _invalid()
    aggregate = upgrade_aggregate_v1_to_v2(
        document["root_record"]["aggregate_state"], definition_resolver
    )
    receipts: list[dict[str, Any]] = []
    for receipt in document["operation_receipts"]:
        receipts.append(
            {
                "operation_kind": (
                    "legacy_v1_creation"
                    if receipt["operation_kind"] == "creation"
                    else "legacy_v1_operation"
                ),
                "receipt_sequence": receipt["receipt_sequence"],
                "legacy_receipt": copy.deepcopy(receipt),
            }
        )
    next_receipt = int(document["next_operation_receipt_sequence"])
    next_queue = 0
    aggregate["next_acceptance_sequence"] = document["next_delivery_sequence"]
    runtime_by_id = {runtime["runtime_id"]: runtime for runtime in aggregate["runtimes"]}
    for pending in sorted(
        document["pending_deliveries"], key=lambda item: int(item["delivery_sequence"])
    ):
        origin = pending["origin"]
        envelope = copy.deepcopy(pending["envelope"])
        envelope["cause_id"] = envelope["event_id"]
        envelope["source"] = (
            {"host": True}
            if origin["kind"] == "host_input"
            else {
                "legacy_v1_internal": {
                    "producing_receipt_sequence": origin["producing_receipt_sequence"],
                    "emission_index": origin["emission_index"],
                }
            }
        )
        digest = _entry_digest(document["root_instance_id"], pending["delivery_mode"], envelope)
        entry = {
            "acceptance_sequence": pending["delivery_sequence"],
            "queue_sequence": str(next_queue),
            "delivery_mode": pending["delivery_mode"],
            "envelope": envelope,
            "envelope_digest": digest,
            "deferral_count": "0",
        }
        runtime_id = _runtime_id_for_target(envelope["target"])
        if runtime_id not in runtime_by_id:
            raise _invalid()
        runtime_by_id[runtime_id]["ready_mailbox"].append(entry)
        acceptance = {
            "operation_kind": "acceptance",
            "receipt_sequence": str(next_receipt),
            "event_id": envelope["event_id"],
            "request_digest": digest,
            "acceptance_sequence": pending["delivery_sequence"],
            "accepted_revision": pending["accepted_revision"],
            "delivery_mode": pending["delivery_mode"],
        }
        if pending["delivery_mode"] == "internal":
            acceptance["legacy_v1_delivery"] = {
                "delivery_sequence": pending["delivery_sequence"],
                "envelope_digest": pending["envelope_digest"],
                "origin": copy.deepcopy(origin),
            }
        receipts.append(acceptance)
        next_receipt += 1
        next_queue += 1
    aggregate["next_queue_sequence"] = str(next_queue)
    aggregate = seal_aggregate_v2(aggregate)
    result = {
        "execution_checkpoint_format": "determa.execution_checkpoint",
        "execution_checkpoint_schema_version": 2,
        "root_instance_id": document["root_instance_id"],
        "revision": str(int(document["revision"]) + 1),
        "root_record": {"status": "retained", "aggregate_state": aggregate},
        "replay_retention": copy.deepcopy(document["replay_retention"]),
        "next_operation_receipt_sequence": str(next_receipt),
        "operation_receipts": receipts,
        "event_identity_tombstones": [],
        "pending_outbox_intents": copy.deepcopy(document["pending_outbox_intents"]),
        "next_outbox_terminal_sequence": document["next_outbox_terminal_sequence"],
        "terminal_outbox_records": copy.deepcopy(document["terminal_outbox_records"]),
        "outbox_effect_tombstones": copy.deepcopy(document["outbox_effect_tombstones"]),
        "migration_audit_records": copy.deepcopy(document["migration_audit_records"]),
    }
    return seal_execution_checkpoint(result)


def _append_external_intent(
    checkpoint: dict[str, Any],
    references: list[dict[str, Any]],
    emission: Mapping[str, Any],
    emission_index: int,
) -> None:
    checkpoint["pending_outbox_intents"].append(
        {
            "intent": copy.deepcopy(dict(emission)),
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
    result = create_aggregate_v2(bundle, machine_id, root_instance_id, creation_id, bindings)
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
            _append_external_intent(checkpoint, receipt["emission_references"], emission, index)
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
    document: Mapping[str, Any], event_id: str, digests: Mapping[str, str]
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
        if terminal["request_digest"] != digests["determa-inbox-envelope-digest-2"]:
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
        if tombstone["request_digest"] != digests[tombstone["request_digest_domain"]]:
            raise ArtifactError(CheckpointHostFailureCode.EVENT_ID_CONFLICT)
        result = {
            "result": "replay",
            "terminal_receipt_sequence": tombstone["terminal_receipt_sequence"],
            "terminal_disposition": tombstone["terminal_disposition"],
        }
        if tombstone["request_digest_domain"] == "determa-inbox-envelope-digest-1":
            result.update(
                {
                    "event_id": event_id,
                    "acceptance_sequence": tombstone["acceptance_sequence"],
                    "request_digest_domain": tombstone["request_digest_domain"],
                }
            )
        return result
    return None


def _legacy_replay(
    document: Mapping[str, Any], event_id: str, request_digest: str
) -> dict[str, Any] | None:
    for receipt in document["operation_receipts"]:
        if receipt["operation_kind"] != "legacy_v1_operation":
            continue
        legacy = receipt["legacy_receipt"]
        if legacy.get("operation_kind") != "delivery" or legacy.get("event_id") != event_id:
            continue
        if legacy["request_digest"] != request_digest:
            raise ArtifactError(CheckpointHostFailureCode.EVENT_ID_CONFLICT)
        return {
            "result": "replay",
            "event_id": event_id,
            "acceptance_sequence": legacy["accepted_delivery_sequence"],
            "terminal_receipt_sequence": receipt["receipt_sequence"],
            "terminal_disposition": legacy["outcome"]["disposition"],
            "request_digest_domain": "determa-inbox-envelope-digest-1",
        }
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
    legacy_domains: list[bool] = []
    for delivery in deliveries:
        legacy_domain = (
            isinstance(delivery, Mapping)
            and delivery.get("request_digest_domain") == "determa-inbox-envelope-digest-1"
        )
        expected_members = {"delivery_mode", "envelope", "envelope_digest"}
        if legacy_domain:
            expected_members.add("request_digest_domain")
        envelope_is_valid = (
            validate_execution_checkpoint_member("envelope", delivery.get("envelope"))
            if legacy_domain and isinstance(delivery, Mapping)
            else _valid_envelope_shape(delivery.get("envelope"))
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
        legacy_domains.append(legacy_domain)
        digest = (
            delivery_request_digest(document["root_instance_id"], str(mode), envelope)
            if legacy_domain
            else _entry_digest(document["root_instance_id"], str(mode), envelope)
        )
        canonical_digests.append(digest)

    if any(root != document["root_instance_id"] for root in target_roots):
        raise ArtifactError("wrong_root")
    if len(event_ids) != len(set(event_ids)):
        raise ArtifactError("duplicate_event_id_in_batch")

    replay_evidence: list[dict[str, Any] | None] = []
    for delivery, digest, legacy_domain in zip(
        deliveries, canonical_digests, legacy_domains, strict=True
    ):
        envelope = delivery["envelope"]
        mode = delivery["delivery_mode"]
        event_id = envelope["event_id"]
        v1_envelope = {
            key: copy.deepcopy(value)
            for key, value in envelope.items()
            if key not in {"cause_id", "source"}
        }
        replay_digests = {
            "determa-inbox-envelope-digest-2": "" if legacy_domain else digest,
            "determa-inbox-envelope-digest-1": delivery_request_digest(
                document["root_instance_id"], str(mode), v1_envelope
            ),
        }
        replay = _pending_replay(document, event_id, digest)
        if replay is None:
            replay = _retained_replay(document, event_id, replay_digests)
        if replay is None:
            replay = _legacy_replay(
                document, event_id, replay_digests["determa-inbox-envelope-digest-1"]
            )
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
    if any(delivery.get("request_digest_domain") is not None for delivery in deliveries):
        raise ArtifactError("malformed_delivery")
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
    result = step_aggregate_v2(aggregate, target_runtime_id, definition_resolver)
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
            _append_external_intent(candidate, references, emission, emission_index)
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
    expected_revision: str,
    expected_checkpoint_digest: str,
) -> dict[str, Any]:
    """Advance bounded replay retention through one dependency-closed cutoff."""
    restored = restore_execution_checkpoint_v2(source, definition_resolver)
    document = restored.document
    if document["replay_retention"]["mode"] == "permanent":
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
        legacy = receipt.get("legacy_receipt", {})
        origin = legacy.get("origin", {})
        if (
            origin.get("kind") == "internal_emission"
            and origin.get("producing_receipt_sequence") in removed_sequences
        ):
            raise _invalid()
        converted_origin = receipt.get("legacy_v1_delivery", {}).get("origin", {})
        if (
            converted_origin.get("kind") == "internal_emission"
            and converted_origin.get("producing_receipt_sequence") in removed_sequences
        ):
            raise _invalid()

    candidate = copy.deepcopy(document)
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
        elif (
            receipt["operation_kind"] == "legacy_v1_operation"
            and receipt["legacy_receipt"].get("operation_kind") == "delivery"
        ):
            legacy = receipt["legacy_receipt"]
            tombstones.append(
                {
                    "event_id": legacy["event_id"],
                    "request_digest": legacy["request_digest"],
                    "request_digest_domain": "determa-inbox-envelope-digest-1",
                    "acceptance_sequence": legacy["accepted_delivery_sequence"],
                    "terminal_receipt_sequence": receipt["receipt_sequence"],
                    "terminal_disposition": legacy["outcome"]["disposition"],
                }
            )
    candidate["operation_receipts"] = retained
    candidate["event_identity_tombstones"] = sorted(
        tombstones, key=lambda item: int(item["terminal_receipt_sequence"])
    )
    candidate["replay_retention"]["pruned_through_receipt_sequence"] = cutoff_receipt_sequence
    candidate["revision"] = str(int(candidate["revision"]) + 1)
    return seal_execution_checkpoint(candidate)
