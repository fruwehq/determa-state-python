"""Portable execution-checkpoint artifacts and semantic validation."""

from __future__ import annotations

import copy
import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from typing import Any

from .errors import ArtifactError
from .wire import (
    ArtifactSource,
    DefinitionResolver,
    RestoredAggregate,
    _schema_registry,
    artifact_schema,
    canonical_bytes,
    hash_value,
    load_json_artifact,
    restore_aggregate,
)

_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)\Z")
_MAX_DECIMAL_DIGITS = 4096


@dataclass(frozen=True)
class RestoredExecutionCheckpoint:
    """One verified checkpoint and its optional retained aggregate."""

    document: dict[str, Any]
    aggregate: RestoredAggregate | None
    canonical_bytes: bytes
    source_bytes: bytes


def execution_checkpoint_digest(document: Mapping[str, Any]) -> str:
    """Compute the exact schema-version-1 checkpoint digest."""
    body = copy.deepcopy(dict(document))
    body.pop("execution_checkpoint_digest", None)
    return hash_value(["determa-execution-checkpoint-digest-1", body])


def seal_execution_checkpoint(document: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copied checkpoint with its digest recomputed."""
    result = copy.deepcopy(dict(document))
    result.pop("execution_checkpoint_digest", None)
    result["execution_checkpoint_digest"] = execution_checkpoint_digest(result)
    return result


def serialize_execution_checkpoint(document: Mapping[str, Any]) -> bytes:
    """Return the exact RFC 8785 checkpoint representation."""
    return canonical_bytes(seal_execution_checkpoint(document))


def _invalid() -> ArtifactError:
    return ArtifactError("invalid_execution_checkpoint")


@cache
def _member_validator(name: str) -> Any:
    import jsonschema

    schema = artifact_schema("execution_checkpoint")
    return jsonschema.Draft202012Validator(
        {"$ref": f"{schema['$id']}#/$defs/{name}"},
        registry=_schema_registry(),
    )


def validate_execution_checkpoint_member(name: str, value: Any) -> bool:
    """Return whether a value matches one closed checkpoint schema member."""
    return next(_member_validator(name).iter_errors(value), None) is None


def _decimal(value: Any) -> int:
    if (
        not isinstance(value, str)
        or len(value) > _MAX_DECIMAL_DIGITS
        or _DECIMAL.fullmatch(value) is None
    ):
        raise _invalid()
    try:
        return int(value)
    except ValueError as exc:
        raise _invalid() from exc


def _ordered_unique(values: list[int]) -> bool:
    return values == sorted(values) and len(values) == len(set(values))


def _target_root_instance_id(target: Any) -> str:
    if not isinstance(target, dict) or len(target) != 1:
        raise _invalid()
    member = next(iter(target.values()))
    if not isinstance(member, dict):
        raise _invalid()
    root_instance_id = member.get("root_instance_id")
    if not isinstance(root_instance_id, str):
        raise _invalid()
    return root_instance_id


def _validate_receipts(
    document: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], set[str], set[str]]:
    revision = _decimal(document["revision"])
    receipts = document["operation_receipts"]
    creation = receipts[0]
    if creation["operation_kind"] != "creation" or creation["receipt_sequence"] != "0":
        raise _invalid()

    next_receipt = _decimal(document["next_operation_receipt_sequence"])
    sequences = [_decimal(receipt["receipt_sequence"]) for receipt in receipts]
    if not _ordered_unique(sequences) or any(sequence >= next_receipt for sequence in sequences):
        raise _invalid()

    retention = document["replay_retention"]
    cutoff_value = retention["pruned_through_receipt_sequence"]
    cutoff = _decimal(cutoff_value) if cutoff_value is not None else None
    if retention["mode"] == "permanent" or cutoff is None:
        expected = list(range(next_receipt))
    else:
        expected = [0, *range(cutoff + 1, next_receipt)]
    if sequences != expected:
        raise _invalid()

    by_sequence = {receipt["receipt_sequence"]: receipt for receipt in receipts}
    delivery_receipts: list[dict[str, Any]] = []
    referenced_effects: set[str] = set()
    referenced_migrations: set[str] = set()
    operation_ids: set[str] = set()
    prior_revision = -1
    for receipt in receipts:
        committed_revision = _decimal(receipt["committed_revision"])
        if committed_revision > revision or committed_revision <= prior_revision:
            raise _invalid()
        prior_revision = committed_revision
        operation_kind = receipt["operation_kind"]
        if operation_kind == "creation":
            if committed_revision != 0 or receipt is not creation:
                raise _invalid()
        elif operation_kind == "delivery":
            delivery_receipts.append(receipt)
            accepted_revision = _decimal(receipt["accepted_revision"])
            accepted_sequence = _decimal(receipt["accepted_delivery_sequence"])
            if (
                accepted_revision > committed_revision
                or accepted_sequence >= _decimal(document["next_delivery_sequence"])
                or (
                    receipt["delivery_mode"] == "input"
                    and accepted_revision == 0
                )
                or (
                    receipt["delivery_mode"] == "internal"
                    and accepted_revision == committed_revision
                )
            ):
                raise _invalid()
        else:
            operation_id = receipt["operation_id"]
            if operation_id in operation_ids:
                raise _invalid()
            operation_ids.add(operation_id)
            migration_sequences = [
                _decimal(sequence) for sequence in receipt["migration_sequences"]
            ]
            if migration_sequences != sorted(migration_sequences):
                raise _invalid()
            if (
                receipt["result_code"] == "migration_no_operation"
                and receipt["source_aggregate_state_digest"]
                != receipt["resulting_aggregate_state_digest"]
            ):
                raise _invalid()
            referenced_migrations.update(receipt["migration_sequences"])

        for index, emission in enumerate(receipt.get("emission_references", [])):
            if _decimal(emission["emission_index"]) != index:
                raise _invalid()
            if emission["kind"] == "external_outbox":
                referenced_effects.add(emission["effect_id"])
    return by_sequence, delivery_receipts, referenced_effects, referenced_migrations


def _validate_deliveries(
    document: dict[str, Any],
    receipts_by_sequence: dict[str, dict[str, Any]],
    delivery_receipts: list[dict[str, Any]],
) -> None:
    revision = _decimal(document["revision"])
    root_instance_id = document["root_instance_id"]
    next_delivery = _decimal(document["next_delivery_sequence"])
    pending = document["pending_deliveries"]
    pending_sequences = [_decimal(item["delivery_sequence"]) for item in pending]
    if (
        not _ordered_unique(pending_sequences)
        or any(sequence >= next_delivery for sequence in pending_sequences)
    ):
        raise _invalid()

    pending_event_ids = [item["envelope"]["event_id"] for item in pending]
    receipt_event_ids = [receipt["event_id"] for receipt in delivery_receipts]
    if (
        len(pending_event_ids) != len(set(pending_event_ids))
        or len(receipt_event_ids) != len(set(receipt_event_ids))
        or set(pending_event_ids) & set(receipt_event_ids)
    ):
        raise _invalid()

    allocated_sequences = [
        *pending_sequences,
        *[_decimal(receipt["accepted_delivery_sequence"]) for receipt in delivery_receipts],
    ]
    if len(allocated_sequences) != len(set(allocated_sequences)):
        raise _invalid()
    if document["replay_retention"]["mode"] == "permanent" and sorted(
        allocated_sequences
    ) != list(range(next_delivery)):
        raise _invalid()

    deliveries: dict[str, tuple[str, str, str, dict[str, Any]]] = {}
    for item in pending:
        parsed_accepted_revision = _decimal(item["accepted_revision"])
        if parsed_accepted_revision > revision or (
            item["delivery_mode"] == "input"
            and parsed_accepted_revision == 0
        ):
            raise _invalid()
        expected_digest = hash_value(
            [
                "determa-inbox-envelope-digest-1",
                "1",
                root_instance_id,
                item["delivery_mode"],
                item["envelope"],
            ]
        )
        if (
            item["envelope_digest"] != expected_digest
            or _target_root_instance_id(item["envelope"]["target"]) != root_instance_id
        ):
            raise _invalid()
        deliveries[item["delivery_sequence"]] = (
            item["envelope"]["event_id"],
            item["accepted_revision"],
            item["delivery_mode"],
            item["origin"],
        )
    for receipt in delivery_receipts:
        deliveries[receipt["accepted_delivery_sequence"]] = (
            receipt["event_id"],
            receipt["accepted_revision"],
            receipt["delivery_mode"],
            receipt["origin"],
        )

    for sequence, (event_id, accepted_revision, mode, origin) in deliveries.items():
        if mode != "internal":
            continue
        producer = receipts_by_sequence.get(origin["producing_receipt_sequence"])
        if producer is None or accepted_revision != producer["committed_revision"]:
            raise _invalid()
        emission_index = _decimal(origin["emission_index"])
        emissions = producer.get("emission_references", [])
        if emission_index >= len(emissions):
            raise _invalid()
        emission = emissions[emission_index]
        if (
            emission.get("kind") != "internal_delivery"
            or emission.get("event_id") != event_id
            or emission.get("delivery_sequence") != sequence
        ):
            raise _invalid()

    permanent = document["replay_retention"]["mode"] == "permanent"
    for receipt in document["operation_receipts"]:
        for emission in receipt.get("emission_references", []):
            if emission["kind"] != "internal_delivery":
                continue
            linked = deliveries.get(emission["delivery_sequence"])
            if permanent and (linked is None or linked[0] != emission["event_id"]):
                raise _invalid()


def _validate_outbox(document: dict[str, Any], referenced_effects: set[str]) -> None:
    revision = _decimal(document["revision"])
    pending = document["pending_outbox_intents"]
    terminal = document["terminal_outbox_records"]
    tombstones = document["outbox_effect_tombstones"]
    pending_sequences = [_decimal(item["intent"]["sequence"]) for item in pending]
    terminal_sequences = [_decimal(item["terminal_sequence"]) for item in terminal]
    tombstone_sequences = [_decimal(item["terminal_sequence"]) for item in tombstones]
    if (
        pending_sequences != sorted(pending_sequences)
        or terminal_sequences != sorted(terminal_sequences)
        or tombstone_sequences != sorted(tombstone_sequences)
    ):
        raise _invalid()

    full_sequences = [
        *pending_sequences,
        *[_decimal(item["intent"]["sequence"]) for item in terminal],
    ]
    if len(full_sequences) != len(set(full_sequences)):
        raise _invalid()
    all_terminal_sequences = [*terminal_sequences, *tombstone_sequences]
    if (
        len(all_terminal_sequences) != len(set(all_terminal_sequences))
        or any(
            sequence >= _decimal(document["next_outbox_terminal_sequence"])
            for sequence in all_terminal_sequences
        )
    ):
        raise _invalid()

    pending_effects = [item["intent"]["effect_id"] for item in pending]
    terminal_effects = [item["intent"]["effect_id"] for item in terminal]
    tombstone_effects = [item["effect_id"] for item in tombstones]
    all_effects = [*pending_effects, *terminal_effects, *tombstone_effects]
    if len(all_effects) != len(set(all_effects)):
        raise _invalid()
    effect_set = set(all_effects)
    if not referenced_effects.issubset(effect_set):
        raise _invalid()
    if document["replay_retention"]["mode"] == "permanent" and effect_set != referenced_effects:
        raise _invalid()

    for item in pending:
        state_revision = _decimal(item["state_revision"])
        if state_revision > revision:
            raise _invalid()
        effect_id = item["intent"]["effect_id"]
        producers = [
            receipt
            for receipt in document["operation_receipts"]
            if any(
                emission.get("kind") == "external_outbox"
                and emission.get("effect_id") == effect_id
                for emission in receipt.get("emission_references", [])
            )
        ]
        if len(producers) != 1:
            if document["replay_retention"]["mode"] == "permanent" or producers:
                raise _invalid()
            continue
        producer_revision = _decimal(producers[0]["committed_revision"])
        if item["delivery_state"]["status"] == "not_attempted":
            if state_revision != producer_revision:
                raise _invalid()
        elif state_revision <= producer_revision:
            raise _invalid()
    receipt_by_effect = {
        emission["effect_id"]: receipt
        for receipt in document["operation_receipts"]
        for emission in receipt.get("emission_references", [])
        if emission["kind"] == "external_outbox"
    }
    for item in [*terminal, *tombstones]:
        committed_revision = _decimal(item["committed_revision"])
        if committed_revision > revision:
            raise _invalid()
        effect_id = (
            item["intent"]["effect_id"]
            if "intent" in item
            else item["effect_id"]
        )
        producer = receipt_by_effect.get(effect_id)
        if producer is not None and committed_revision <= _decimal(
            producer["committed_revision"]
        ):
            raise _invalid()


def _validate_audit_and_root(
    document: dict[str, Any], referenced_migrations: set[str]
) -> None:
    root_instance_id = document["root_instance_id"]
    root_record = document["root_record"]
    creation = document["operation_receipts"][0]
    if root_record["status"] == "retained":
        aggregate = root_record["aggregate_state"]
        if aggregate["root_instance_id"] != root_instance_id:
            raise _invalid()
        creation_id = aggregate["creation_id"]
        root_runtime_id = aggregate["root_runtime_id"]
    else:
        creation_id = root_record["creation_id"]
        root_runtime_id = root_record["root_runtime_id"]
        if document["pending_deliveries"] or document["pending_outbox_intents"]:
            raise _invalid()
    if creation["creation_id"] != creation_id:
        raise _invalid()

    audits = document["migration_audit_records"]
    audit_sequences = [_decimal(item["migration_sequence"]) for item in audits]
    if not _ordered_unique(audit_sequences):
        raise _invalid()
    available = {item["migration_sequence"] for item in audits}
    if not referenced_migrations.issubset(available):
        raise _invalid()
    if document["replay_retention"]["mode"] == "permanent" and referenced_migrations != available:
        raise _invalid()
    if any(
        audit["root_instance_id"] != root_instance_id
        or audit["root_runtime_id"] != root_runtime_id
        for audit in audits
    ):
        raise _invalid()
    audit_by_sequence = {item["migration_sequence"]: item for item in audits}
    for receipt in document["operation_receipts"]:
        if receipt["operation_kind"] != "maintenance_migration":
            continue
        linked = [
            audit_by_sequence[sequence]
            for sequence in receipt["migration_sequences"]
            if sequence in audit_by_sequence
        ]
        if len(linked) != len(receipt["migration_sequences"]):
            raise _invalid()
        if linked and (
            linked[0]["source_aggregate_state_digest"]
            != receipt["source_aggregate_state_digest"]
            or linked[-1]["target_aggregate_state_digest"]
            != receipt["resulting_aggregate_state_digest"]
            or any(
                left["target_aggregate_state_digest"]
                != right["source_aggregate_state_digest"]
                for left, right in zip(linked, linked[1:], strict=False)
            )
        ):
            raise _invalid()

    final_digest = (
        root_record["final_aggregate_state_digest"]
        if root_record["status"] == "tombstone"
        else root_record["aggregate_state"]["aggregate_state_digest"]
    )
    retention = document["replay_retention"]
    cutoff = retention["pruned_through_receipt_sequence"]
    last_receipt = document["operation_receipts"][-1]
    has_final_receipt_evidence = (
        cutoff is None or last_receipt["receipt_sequence"] != "0"
    )
    if (
        has_final_receipt_evidence
        and last_receipt["resulting_aggregate_state_digest"] != final_digest
    ):
        raise _invalid()

    status_evidence = next(
        (
            receipt
            for receipt in reversed(document["operation_receipts"])
            if receipt["operation_kind"] in {"creation", "delivery"}
        ),
        None,
    )
    if (
        status_evidence is None
        or (
            cutoff is not None
            and status_evidence["receipt_sequence"] == "0"
        )
    ):
        return
    if status_evidence["operation_kind"] == "creation":
        status = status_evidence["status"]
        fault = status_evidence["fault"]
    else:
        status = status_evidence["outcome"]["status"]
        fault = status_evidence["outcome"]["fault"]

    if root_record["status"] == "tombstone":
        if status != root_record["terminal_status"]:
            raise _invalid()
        return

    aggregate = root_record["aggregate_state"]
    root_runtime = next(
        (
            runtime
            for runtime in aggregate["runtimes"]
            if runtime["runtime_id"] == aggregate["root_runtime_id"]
        ),
        None,
    )
    if (
        root_runtime is None
        or root_runtime["status"] != status
        or root_runtime["fault"] != fault
    ):
        raise _invalid()


def validate_execution_checkpoint_semantics(document: dict[str, Any]) -> None:
    """Validate all schema-version-1 portable cross-field invariants."""
    receipts, deliveries, referenced_effects, referenced_migrations = _validate_receipts(
        document
    )
    _validate_deliveries(document, receipts, deliveries)
    _validate_outbox(document, referenced_effects)
    _validate_audit_and_root(document, referenced_migrations)


def restore_execution_checkpoint(
    source: ArtifactSource, definition_resolver: DefinitionResolver
) -> RestoredExecutionCheckpoint:
    """Parse, verify, and restore one strict execution checkpoint."""
    document, raw = load_json_artifact(source, "execution_checkpoint")
    aggregate: RestoredAggregate | None = None
    if document["root_record"]["status"] == "retained":
        try:
            aggregate = restore_aggregate(
                document["root_record"]["aggregate_state"], definition_resolver
            )
        except ArtifactError as exc:
            if exc.code in {
                "source_definition_unavailable",
                "definition_untrusted",
                "definition_fingerprint_mismatch",
            }:
                raise
            raise _invalid() from exc
    if execution_checkpoint_digest(document) != document["execution_checkpoint_digest"]:
        raise ArtifactError("execution_checkpoint_digest_mismatch")
    validate_execution_checkpoint_semantics(document)
    return RestoredExecutionCheckpoint(
        document=copy.deepcopy(document),
        aggregate=aggregate,
        canonical_bytes=canonical_bytes(document),
        source_bytes=raw,
    )
