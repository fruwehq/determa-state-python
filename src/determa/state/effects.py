"""Committed native effects over one guarded SQLite checkpoint/journal boundary.

The handler is called only after the producing transaction has committed. Its
native objects stay inside the callback; only declared portable values enter the
journal. A disappeared worker leaves an ambiguous attempt for reconciliation.
"""

from __future__ import annotations

import base64
import copy
import json
import re
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar, cast

from .checkpoint import seal_execution_checkpoint
from .checkpoint_v1 import (
    _preflight_checkpoint_admission,
    admit_checkpoint_v1,
    restore_execution_checkpoint_v1,
    step_checkpoint_v1,
)
from .errors import ArtifactError
from .extensions import ConfiguredExtension, ExtensionError, ExtensionRegistry
from .host import outbox_intent_digest
from .wire import (
    ArtifactResolver,
    _schema_registry,
    canonical_bytes,
    decoded_typed_value,
    hash_value,
    typed_value,
)

_NANOSECONDS = re.compile(r"(?:0|-?[1-9][0-9]*)\Z")
_OUTCOMES = frozenset({"succeeded", "domain_rejected", "terminal_failure", "cancelled"})
_REPORTS = _OUTCOMES | {"retryable_failure", "ambiguous"}
_T = TypeVar("_T")


class EffectError(ValueError):
    """A closed refusal from the committed-effect boundary."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def journal_digest(journal: Mapping[str, Any]) -> str:
    body = {
        key: copy.deepcopy(value)
        for key, value in journal.items()
        if key != "host_effect_journal_digest"
    }
    return hash_value(["determa-host-effect-journal-digest-1", body])


def seal_journal(journal: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(journal))
    result["host_effect_journal_digest"] = journal_digest(result)
    return result


def _journal_validator() -> Any:
    from jsonschema import Draft202012Validator
    from referencing import Resource

    source = json.loads(
        (Path(__file__).parent / "data/host-effect-journal-v1.schema.json").read_text()
    )
    registry = _schema_registry().with_resource(source["$id"], Resource.from_contents(source))
    provider = json.loads(
        (Path(__file__).parent / "data/provider-reference-v1.schema.json").read_text()
    )
    registry = registry.with_resource(provider["$id"], Resource.from_contents(provider))
    return Draft202012Validator(source, registry=registry)


def _validate_request(request: Mapping[str, Any], name: str) -> dict[str, Any]:
    from jsonschema import Draft202012Validator

    if not isinstance(request, Mapping):
        raise EffectError("invalid_host_request")
    normalized = copy.deepcopy(dict(request))
    source = json.loads((Path(__file__).parent / "data" / name).read_text())
    if not Draft202012Validator(source, registry=_schema_registry()).is_valid(normalized):
        raise EffectError("invalid_host_request")
    return normalized


def _now(value: str) -> int:
    if type(value) is not str or not _NANOSECONDS.fullmatch(value):
        raise EffectError("stale_attempt_fence")
    parsed = int(value)
    if not -(2**63) <= parsed < 2**63:
        raise EffectError("stale_attempt_fence")
    return parsed


def _record(journal: Mapping[str, Any], effect_id: str) -> dict[str, Any]:
    for item in journal["effect_records"]:
        if item["effect_id"] == effect_id:
            return cast(dict[str, Any], item)
    raise EffectError("effect_not_outstanding")


def _mapping(record: Mapping[str, Any], kind: str) -> dict[str, Any]:
    for item in record["result_mapping"]:
        if item["outcome_kind"] == kind:
            return cast(dict[str, Any], item)
    raise EffectError("invalid_host_request")


def _bump(journal: dict[str, Any], checkpoint: Mapping[str, Any] | None = None) -> None:
    journal["journal_revision"] = str(int(journal["journal_revision"]) + 1)
    if checkpoint is not None:
        journal["checkpoint_revision"] = checkpoint["revision"]
        journal["checkpoint_digest"] = checkpoint["execution_checkpoint_digest"]
    journal["host_effect_journal_digest"] = journal_digest(journal)


def _result_delivery_evidence(
    checkpoint: Mapping[str, Any],
    record: Mapping[str, Any],
    kind: str,
    payload: Any,
    target: Mapping[str, Any],
) -> dict[str, Any]:
    """Build pinned admission bytes without consulting current runtime liveness."""
    mapping = _mapping(record, kind)
    event_id = hash_value(
        ["determa-effect-result-event-1", record["effect_id"], mapping["result_slot"]]
    )
    envelope = {
        "event": mapping["event"],
        "event_id": event_id,
        "cause_id": event_id,
        "source": {"host": True},
        "target": copy.deepcopy(dict(target)),
        "payload": copy.deepcopy(payload),
    }
    location = mapping["operation_token_location"]
    if location is not None:
        if location["kind"] == "correlation_id":
            envelope["correlation_id"] = record["operation_token"]
        else:
            try:
                logical = decoded_typed_value(payload)
                pointer = location["pointer"]
                if not pointer.startswith("/") or re.search(r"~(?![01])", pointer):
                    raise ValueError("invalid token pointer")
                parts = [
                    part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")
                ]
                parent = logical
                for part in parts[:-1]:
                    parent = parent[part]
                if not isinstance(parent, dict) or (
                    parts[-1] in parent and not isinstance(parent[parts[-1]], str)
                ):
                    raise ValueError("token field must be a string")
                parent[parts[-1]] = record["operation_token"]
                envelope["payload"] = typed_value(logical)
            except (KeyError, TypeError, ValueError) as exc:
                raise EffectError("invalid_host_request") from exc
    from .queueing import _entry_digest

    return {
        "delivery_mode": "input",
        "envelope": envelope,
        "envelope_digest": _entry_digest(checkpoint["root_instance_id"], "input", envelope),
    }


def _pinned_result_target(
    checkpoint: Mapping[str, Any],
    record: Mapping[str, Any],
    resolver: ArtifactResolver | None,
) -> dict[str, Any]:
    target = record["target"]
    origin = target["runtime_incarnation"]
    root, runtime_id = checkpoint["root_instance_id"], target["runtime_id"]
    if target["root_instance_id"] != root:
        raise EffectError("invalid_effect_journal")
    definition = origin["definition"]
    machine = definition["machine"]
    identity = [machine["namespace"], machine["machine_id"], machine["machine_version"]]
    if origin["kind"] == "root":
        root_record = checkpoint["root_record"]
        aggregate = root_record.get("aggregate_state")
        actual_root_runtime_id = (
            aggregate["root_runtime_id"] if aggregate else root_record["root_runtime_id"]
        )
        if origin["root_instance_id"] != root or runtime_id != actual_root_runtime_id:
            raise EffectError("invalid_effect_journal")
        expected_id = hash_value(
            [
                "determa-root-runtime-identity-1",
                "1",
                definition["validated_bundle_fingerprint"],
                *identity,
                origin["root_instance_id"],
            ]
        )
    elif origin["kind"] == "component":
        if origin["declaration_index"] != origin["component_definition_pointer"].rsplit("/", 1)[-1]:
            raise EffectError("invalid_effect_journal")
        expected_id = hash_value(
            [
                "determa-component-runtime-identity-1",
                "1",
                root,
                origin["owner_runtime_id"],
                origin["component_definition_pointer"],
                origin["activation_sequence"],
                *identity,
            ]
        )
    else:
        expected_id = hash_value(
            [
                "determa-spawned-runtime-identity-1",
                "1",
                root,
                origin["owner_runtime_id"],
                origin["spawn_action_pointer"],
                origin["spawn_sequence"],
                *identity,
            ]
        )
    if expected_id != runtime_id:
        raise EffectError("invalid_effect_journal")
    if resolver is not None:
        from .wire import _origin_machine

        _origin_machine(resolver, origin)
    aggregate = checkpoint["root_record"].get("aggregate_state")
    for runtime in aggregate["runtimes"] if aggregate else []:
        if runtime["runtime_id"] == target["runtime_id"] and runtime["identity_origin"] == origin:
            return copy.deepcopy(runtime["target_identity"])
    root, runtime_id = checkpoint["root_instance_id"], target["runtime_id"]
    if origin["kind"] == "root" and origin["root_instance_id"] == root:
        return {"root": {"root_instance_id": root, "root_runtime_id": runtime_id}}
    if origin["kind"] == "owned_spawned_instance":
        machine = origin["definition"]["machine"]
        return {
            "spawned_instance": {
                "root_instance_id": root,
                "instance_id": runtime_id,
                "machine_id": machine["machine_id"],
                "machine_version": machine["machine_version"],
            }
        }
    if origin["kind"] == "component" and resolver is not None:
        from .engine import _pointer_get
        from .wire import _origin_machine

        bundle, _ = _origin_machine(resolver, origin)
        placement = _pointer_get(bundle.raw, origin["component_definition_pointer"])
        return {
            "component": {
                "root_instance_id": root,
                "owner_runtime_id": origin["owner_runtime_id"],
                "component_id": placement["component_id"],
                "component_runtime_id": runtime_id,
                "activation_sequence": origin["activation_sequence"],
            }
        }
    raise EffectError("invalid_effect_journal")


def validate_journal(
    checkpoint: Mapping[str, Any],
    journal: Mapping[str, Any],
    resolver: ArtifactResolver | None = None,
) -> None:
    """Reject torn or internally inconsistent journal/checkpoint pairs."""
    if not _journal_validator().is_valid(journal):
        raise EffectError("invalid_effect_journal")
    if (
        journal.get("host_effect_journal_format") != "determa.host_effect_journal"
        or journal.get("host_effect_journal_schema_version") != 1
        or journal.get("root_instance_id") != checkpoint.get("root_instance_id")
        or journal.get("checkpoint_revision") != checkpoint.get("revision")
        or journal.get("checkpoint_digest") != checkpoint.get("execution_checkpoint_digest")
        or journal.get("host_effect_journal_digest") != journal_digest(journal)
    ):
        raise EffectError("invalid_effect_journal")
    records = journal.get("effect_records")
    if not isinstance(records, list) or [r["effect_id"] for r in records] != sorted(
        {r["effect_id"] for r in records}
    ):
        raise EffectError("invalid_effect_journal")
    references = journal["operation_response_references"]
    if [item["operation_id"] for item in references] != sorted(
        {item["operation_id"] for item in references}
    ):
        raise EffectError("invalid_effect_journal")
    intents = {
        item["intent"]["effect_id"]: item["intent"] for item in checkpoint["pending_outbox_intents"]
    }
    intents.update(
        {
            item["intent"]["effect_id"]: item["intent"]
            for item in checkpoint["terminal_outbox_records"]
        }
    )
    compact = {item["effect_id"]: item for item in checkpoint["outbox_effect_tombstones"]}
    location_ids = [
        item["intent"]["effect_id"]
        for item in checkpoint["pending_outbox_intents"] + checkpoint["terminal_outbox_records"]
    ] + [item["effect_id"] for item in checkpoint["outbox_effect_tombstones"]]
    for record in records:
        # Validate every pinned incarnation before dispatch or native seeding,
        # including nonterminal work. A self-consistent alternate trusted origin
        # cannot replace the checkpoint's immutable root runtime identity.
        try:
            pinned_target = _pinned_result_target(checkpoint, record, resolver)
        except (ArtifactError, KeyError, TypeError, ValueError) as exc:
            raise EffectError("invalid_effect_journal") from exc
        intent = intents.get(record["effect_id"])
        tombstone = compact.get(record["effect_id"])
        if location_ids.count(record["effect_id"]) != 1 or record["intent_digest"] != (
            outbox_intent_digest(checkpoint["root_instance_id"], intent)
            if intent is not None
            else cast(dict[str, Any], tombstone)["intent_digest"]
        ):
            raise EffectError("invalid_effect_journal")
        reports = record["attempt_records"]
        fences = [int(item["attempt_fence"]) for item in reports]
        if fences != sorted(set(fences)) or any(f > int(record["attempt_fence"]) for f in fences):
            raise EffectError("invalid_effect_journal")
        state = record["invocation_state"]
        if state in {"unclaimed", "leased", "ambiguous"} and any(
            record[key] is not None for key in ("outcome", "result_event_id", "admission_receipt")
        ):
            raise EffectError("invalid_effect_journal")
        if state in {"outcome_recorded", "result_admitted", "closed"} and any(
            record[key] is None for key in ("outcome", "result_event_id")
        ):
            raise EffectError("invalid_effect_journal")
        if state == "outcome_recorded" and record["admission_receipt"] is not None:
            raise EffectError("invalid_effect_journal")
        if state in {"result_admitted", "closed"} and (
            record["admission_receipt"] is None
            or record["admission_receipt"]["event_id"] != record["result_event_id"]
            or record["admission_receipt"] not in checkpoint["operation_receipts"]
        ):
            raise EffectError("invalid_effect_journal")
        outcome = record["outcome"]
        if outcome is not None:
            if outcome["digest"] != hash_value(
                [
                    "determa-effect-outcome-1",
                    record["effect_id"],
                    record["operation_token"],
                    outcome["kind"],
                    outcome["payload"],
                    outcome["attempt_fence"],
                ]
            ):
                raise EffectError("invalid_effect_journal")
            mapping = next(
                (
                    item
                    for item in record["result_mapping"]
                    if item["outcome_kind"] == outcome["kind"]
                ),
                None,
            )
            if mapping is None or record["result_event_id"] != hash_value(
                [
                    "determa-effect-result-event-1",
                    record["effect_id"],
                    mapping["result_slot"],
                ]
            ):
                raise EffectError("invalid_effect_journal")
            if state in {"result_admitted", "closed"}:
                try:
                    delivery = _result_delivery_evidence(
                        checkpoint,
                        record,
                        outcome["kind"],
                        outcome["payload"],
                        pinned_target,
                    )
                    receipt = record["admission_receipt"]
                    if (
                        receipt["operation_kind"] != "acceptance"
                        or receipt["delivery_mode"] != "input"
                        or receipt["request_digest"] != delivery["envelope_digest"]
                    ):
                        raise EffectError("invalid_effect_journal")
                except (ArtifactError, KeyError, TypeError, ValueError) as exc:
                    raise EffectError("invalid_effect_journal") from exc
            preclaim_cancel = (
                outcome["kind"] == "cancelled"
                and outcome["attempt_fence"] == "0"
                and record["attempt_fence"] == "0"
                and record["cancellation"] is not None
                and record["cancellation"]["state"] == "prevented_start"
            )
            if preclaim_cancel:
                if reports:
                    raise EffectError("invalid_effect_journal")
            else:
                report = next(
                    (item for item in reports if item["attempt_fence"] == outcome["attempt_fence"]),
                    None,
                )
                if (
                    report is None
                    or report["report_kind"] != outcome["kind"]
                    or report["report_digest"]
                    != hash_value(
                        [
                            "determa-effect-attempt-report-1",
                            record["effect_id"],
                            record["operation_token"],
                            outcome["attempt_fence"],
                            outcome["kind"],
                            outcome["payload"],
                            report["reason"],
                        ]
                    )
                ):
                    raise EffectError("invalid_effect_journal")


def _result_response(
    record: Mapping[str, Any],
    journal: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    status: str,
    report: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "effect_id": record["effect_id"],
        "attempt_fence": record["attempt_fence"],
        "attempt_report": copy.deepcopy(report),
        "outcome": copy.deepcopy(record["outcome"]) if status == "committed" else None,
        "result_event_id": record["result_event_id"] if status == "committed" else None,
        "admission_receipt": copy.deepcopy(record["admission_receipt"])
        if status == "committed"
        else None,
        "checkpoint_revision": checkpoint["revision"],
        "journal_revision": journal["journal_revision"],
        "error_code": None,
    }


def _rejected_result(effect_id: str, fence: str, code: str) -> dict[str, Any]:
    return {
        "status": "rejected",
        "effect_id": effect_id,
        "attempt_fence": fence,
        "attempt_report": None,
        "outcome": None,
        "result_event_id": None,
        "admission_receipt": None,
        "checkpoint_revision": None,
        "journal_revision": None,
        "error_code": code,
    }


def _report_reason(request: Mapping[str, Any]) -> str | None:
    kind = request["outcome_kind"]
    if kind == "retryable_failure":
        return "no_call_proven"
    if kind == "ambiguous":
        return "provider_acceptance_unknown"
    return None


def _retained_report(
    record: Mapping[str, Any], request: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Compare complete immutable report evidence even after a process restart."""
    fence, kind = request["attempt_fence"], request["outcome_kind"]
    digest = hash_value(
        [
            "determa-effect-attempt-report-1",
            record["effect_id"],
            record["operation_token"],
            fence,
            kind,
            request["payload"],
            _report_reason(request),
        ]
    )
    return next(
        (
            item
            for item in record["attempt_records"]
            if item["attempt_fence"] == fence and item["report_digest"] == digest
        ),
        None,
    )


def _reconstruct_producer_response(
    checkpoint: Mapping[str, Any],
    journal: Mapping[str, Any],
    operation_id: str,
    request_identity: Mapping[str, Any],
    resolver: ArtifactResolver,
) -> dict[str, Any] | None:
    """Resolve a retained simple producer response from complete checkpoint evidence."""
    reference = next(
        (
            item
            for item in journal["operation_response_references"]
            if item["operation_id"] == operation_id
        ),
        None,
    )
    if reference is None:
        return None
    receipts = [
        item
        for item in checkpoint["operation_receipts"]
        if item["operation_kind"] == "event_terminal"
        and any(ref["kind"] == "external_outbox" for ref in item["emission_references"])
    ]
    if len(receipts) != 1:
        raise EffectError("replay_evidence_expired")
    receipt = receipts[0]
    emission_ids = {
        item["effect_id"]
        for item in receipt["emission_references"]
        if item["kind"] == "external_outbox"
    }
    records = [
        record for record in journal["effect_records"] if record["effect_id"] in emission_ids
    ]
    if len(records) != len(emission_ids) or not records:
        raise EffectError("replay_evidence_expired")
    if any(record["operation_token"] != request_identity["operation_token"] for record in records):
        raise EffectError("operation_id_conflict")
    intents = {
        item["intent"]["effect_id"]: item["intent"] for item in checkpoint["pending_outbox_intents"]
    }
    intents.update(
        {
            item["intent"]["effect_id"]: item["intent"]
            for item in checkpoint["terminal_outbox_records"]
        }
    )
    if not emission_ids.issubset(intents):
        raise EffectError("replay_evidence_expired")
    emissions = [
        copy.deepcopy(intents[item["effect_id"]])
        for item in receipt["emission_references"]
        if item["kind"] == "external_outbox"
    ]
    if len(emissions) != len(receipt["emission_references"]):
        raise EffectError("replay_evidence_expired")
    # Result routing can name a different runtime from the emitting producer.
    # Derive producer identity from actual effect IDs and trusted author send
    # locations. Complex lifecycle emission histories without this evidence
    # remain unavailable for portable reconstruction.
    from .engine import _pointer_get
    from .wire import _origin_machine

    try:
        aggregate = checkpoint["root_record"]["aggregate_state"]
        source_runtime = next(
            item
            for item in aggregate["runtimes"]
            if item["runtime_id"] == request_identity["target_runtime_id"]
        )
        bundle, machine = _origin_machine(
            resolver, {"definition": source_runtime["current_definition"]}
        )
        namespace, machine_id, version = machine.definition_identity()
        root_pointer = source_runtime["current_definition"]["machine"]["root_definition_pointer"]
        pointers: list[str] = []

        def visit(value: Any, pointer: str) -> None:
            if isinstance(value, dict):
                if "send" in value:
                    pointers.append(pointer + "/send")
                for name, child in value.items():
                    visit(child, pointer + "/" + name.replace("~", "~0").replace("/", "~1"))
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    visit(child, pointer + "/" + str(index))

        visit(_pointer_get(bundle.raw, root_pointer), root_pointer)
        step_sequence = str(int(aggregate["next_logical_step_sequence"]) - 1)
        for emission in receipt["emission_references"]:
            if emission["kind"] != "external_outbox":
                continue
            if not any(
                hash_value(
                    [
                        "determa-effect-identity-1",
                        "1",
                        [namespace, machine_id, str(version)],
                        request_identity["root_instance_id"],
                        request_identity["target_runtime_id"],
                        receipt["event_id"],
                        step_sequence,
                        pointer,
                        emission["emission_index"],
                    ]
                )
                == emission["effect_id"]
                for pointer in pointers
            ):
                raise EffectError("replay_evidence_expired")
    except (ArtifactError, KeyError, TypeError, ValueError, StopIteration) as exc:
        raise EffectError("replay_evidence_expired") from exc
    outcome = receipt["outcome"]
    response = {
        "kind": "processing",
        "body": {
            "core_result": {
                "core_step_result_format": "determa.core_step_result",
                "core_step_result_schema_version": 1,
                "status": outcome["status"],
                "disposition": outcome["disposition"],
                "state": copy.deepcopy(checkpoint["root_record"]["aggregate_state"]),
                "emissions": emissions,
                "lifecycle_dispositions": [],
                "fault": copy.deepcopy(outcome["fault"]),
                "rejection": copy.deepcopy(outcome["rejection"]),
            },
            "receipt": copy.deepcopy(receipt),
        },
    }
    if hash_value(["determa-host-operation-response-1", response]) != reference["response_digest"]:
        raise EffectError("replay_evidence_expired")
    return response


def _validate_authority_pair(ledger: Mapping[str, Any], document: Mapping[str, Any]) -> None:
    """Check the old native pair before any journal transition can be staged."""
    from .authority import _native_checkpoint_history

    try:
        history = _native_checkpoint_history(ledger)
    except ValueError as error:
        raise EffectError("unauthorized_scope") from error
    journal = document["journal"]
    if (
        journal["scope_identity"] != ledger["scope_identity"]
        or journal["root_instance_id"] not in ledger["roots"]
        or canonical_bytes(document["checkpoint"]).decode() not in history
    ):
        raise EffectError("unauthorized_scope")
    for record in journal["effect_records"]:
        binding = {
            "root_instance_id": journal["root_instance_id"],
            "work_kind": "effect",
            "work_identity": record["effect_id"],
            "operation_token": record["operation_token"],
            "participant": "native_effects",
        }
        if binding not in ledger.get("native_effect_work", []):
            raise EffectError("unauthorized_scope")
        issued = next(
            (
                entry
                for entry in ledger["journal_entries"]
                if entry["work_identity"] == record["effect_id"]
            ),
            None,
        )
        if issued is None or issued["attempt_fence"] != record["attempt_fence"]:
            raise EffectError("stale_attempt_fence")
        if any(
            claim["work_identity"] == record["effect_id"]
            and int(claim["attempt_fence"]) > int(record["attempt_fence"])
            for claim in ledger.get("effect_claim_history", [])
        ):
            raise EffectError("stale_attempt_fence")
        live = next(
            (
                claim
                for claim in ledger["active_claims"]
                if claim["work_identity"] == record["effect_id"]
            ),
            None,
        )
        if record["invocation_state"] == "leased":
            if live is None or live != document["claims"].get(record["effect_id"]):
                raise EffectError("stale_attempt_fence")
        elif live is not None:
            raise EffectError("stale_attempt_fence")


def _issue_effect_claim(
    document: dict[str, Any],
    effect_id: str,
    principal: str,
    epoch: str,
    expires_at: str,
    trusted_now: str,
    *,
    deduplication_proven: bool = False,
) -> dict[str, Any]:
    now, expiry = _now(trusted_now), _now(expires_at)
    if expiry <= now:
        raise EffectError("stale_attempt_fence")
    journal = document["journal"]
    record = _record(journal, effect_id)
    if record["invocation_state"] not in {"unclaimed", "ambiguous"}:
        raise EffectError("effect_not_outstanding")
    if record["invocation_state"] == "ambiguous" and not (
        deduplication_proven and record["idempotency_policy"] == "destination_deduplicates"
    ):
        raise EffectError("effect_not_outstanding")
    fence = str(int(record["attempt_fence"]) + 1)
    claim = {
        "scope_identity": journal["scope_identity"],
        "root_instance_id": journal["root_instance_id"],
        "work_kind": "effect",
        "work_identity": effect_id,
        "operation_token": record["operation_token"],
        "scope_authority_epoch": epoch,
        "attempt_fence": fence,
        "worker_principal": principal,
        "expires_at": expires_at,
        "state": "active",
    }
    record["attempt_fence"] = fence
    record["invocation_state"] = "leased"
    document["claims"][effect_id] = claim
    _bump(journal)
    return copy.deepcopy(claim)


@dataclass(frozen=True, slots=True, init=False)
class VerifiedNativeHandler:
    """An exact native-handler instance installed through the public registry.

    The host-owned registry verifier must verify its executing source and
    dependency closure. This handle never promotes provider claims into proof.
    """

    _registry: ExtensionRegistry
    _configured: ConfiguredExtension
    _reference: dict[str, Any]
    _configuration: dict[str, Any]

    def __init__(self, registry: ExtensionRegistry, configured: ConfiguredExtension) -> None:
        object.__setattr__(self, "_registry", registry)
        object.__setattr__(self, "_configured", configured)
        descriptor, _provider, _instance, _evaluator = registry._bound(configured)
        if descriptor["category"] != "native_handler":
            raise EffectError("host_capability_mismatch")
        object.__setattr__(self, "_reference", copy.deepcopy(descriptor["provider_reference"]))
        object.__setattr__(self, "_configuration", copy.deepcopy(configured._configuration))
        destination = self._configuration.get("destination_binding_digest")
        if type(destination) is not str or not re.fullmatch(r"sha256:[0-9a-f]{64}", destination):
            raise EffectError("host_capability_mismatch")

    def verify(self, reference: Mapping[str, Any], destination: str) -> tuple[Any, Any]:
        from .runtime_providers import _bound_provider_method

        try:
            descriptor, provider, instance, _evaluator = self._registry._bound(self._configured)
            if (
                descriptor["category"] != "native_handler"
                or descriptor["provider_reference"] != self._reference
                or reference != self._reference
                or self._configured._configuration != self._configuration
                or destination != self._configuration["destination_binding_digest"]
                or not isinstance(instance, Mapping)
                or instance.get("destination_binding_digest") != destination
                or self._registry.health(self._configured) != "healthy"
            ):
                raise EffectError("host_capability_mismatch")
            return _bound_provider_method(provider, "invoke"), instance
        except (ExtensionError, ValueError, TypeError, KeyError) as error:
            raise EffectError("host_capability_mismatch") from error

    def verify_deduplication_evidence(
        self,
        reference: Mapping[str, Any],
        destination: str,
        evidence: Mapping[str, Any],
    ) -> None:
        """Verify actual destination evidence through the installed native provider.

        The provider must independently authenticate receipts against its native
        destination, rather than trust caller-supplied equal bytes.
        """
        from .runtime_providers import _bound_provider_method

        VerifiedNativeHandler.verify(self, reference, destination)
        try:
            _descriptor, provider, instance, _evaluator = self._registry._bound(self._configured)
            method = _bound_provider_method(provider, "verify_deduplication_evidence")
            if method(instance, copy.deepcopy(dict(evidence))) is not True:
                raise EffectError("host_capability_mismatch")
            VerifiedNativeHandler.verify(self, reference, destination)
        except Exception as exc:
            raise EffectError("host_capability_mismatch") from exc

    def invoke(
        self,
        reference: Mapping[str, Any],
        payload: Any,
        metadata: Mapping[str, Any],
        attempt: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        method, instance = VerifiedNativeHandler.verify(
            self, reference, metadata["destination_binding_digest"]
        )
        return cast(Mapping[str, Any], method(instance, payload, metadata, attempt))


class SQLiteCommittedEffectHost:
    """One local host with checkpoint, journal, claims and responses in one SQLite row.

    ``route`` is a trusted installed route, including the exact handler reference,
    destination binding and declared outcome mappings. ``handler`` receives a typed
    payload and immutable metadata. Worker authentication is supplied independently
    of request data. The caller must obtain trusted host time from its own clock.
    """

    def __init__(
        self,
        path: str | Path,
        resolver: ArtifactResolver,
        route: Mapping[str, Any],
        handler: VerifiedNativeHandler | None,
        *,
        authority_scope: str | None = None,
        core_observer: Callable[[str, str, Mapping[str, Any]], None] | None = None,
        trusted_clock: Callable[[], str] | None = None,
    ) -> None:
        if handler is not None and type(handler) is not VerifiedNativeHandler:
            raise EffectError("host_capability_mismatch")
        self.path = str(Path(path).resolve())
        self.resolver = resolver
        self.route = copy.deepcopy(dict(route))
        self.handler = handler
        self._installed_handler = handler
        self.authority_scope = authority_scope
        self.core_observer = core_observer
        self.trusted_clock = trusted_clock

    def _verified_handler(
        self, reference: Mapping[str, Any], destination: str
    ) -> VerifiedNativeHandler:
        if (
            type(self.handler) is not VerifiedNativeHandler
            or self.handler is not self._installed_handler
        ):
            raise EffectError("host_capability_mismatch")
        VerifiedNativeHandler.verify(self.handler, reference, destination)
        return self.handler

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, isolation_level=None, timeout=30)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def setup_schema(self) -> None:
        with self._connect() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS determa_committed_effects ("
                "root_instance_id TEXT PRIMARY KEY NOT NULL, document BLOB NOT NULL)"
            )

    def seed(
        self,
        checkpoint: Mapping[str, Any],
        journal: Mapping[str, Any],
        claim: Mapping[str, Any] | None = None,
    ) -> None:
        validate_journal(checkpoint, journal, self.resolver)
        restore_execution_checkpoint_v1(checkpoint, self.resolver)
        document: dict[str, Any] = {
            "checkpoint": checkpoint,
            "journal": journal,
            "claims": {},
            "responses": {},
            "result_requests": {},
            "result_responses": {},
            "cancel_requests": {},
            "producer_requests": {},
        }
        if claim is not None:
            document["claims"][claim["work_identity"]] = copy.deepcopy(dict(claim))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO determa_committed_effects VALUES (?, ?)",
                (checkpoint["root_instance_id"], canonical_bytes(document)),
            )
            if self.authority_scope is not None:
                row = connection.execute(
                    "SELECT ledger FROM determa_scope_authority WHERE scope_identity = ?",
                    (self.authority_scope,),
                ).fetchone()
                if row is None:
                    raise EffectError("stale_scope_authority")
                ledger = json.loads(row[0])
                if (
                    ledger["state"] != "active"
                    or self.route.get("authority_epoch") != ledger["authority_epoch"]
                ):
                    raise EffectError("stale_scope_authority")
                if (
                    checkpoint["root_instance_id"] not in ledger["roots"]
                    or journal["scope_identity"] != self.authority_scope
                ):
                    raise EffectError("unauthorized_scope")
                # Restoring an already claimed invocation cannot mint authority.
                # Its exact claim must already have been issued by this ledger.
                if claim is not None and (
                    dict(claim)
                    not in ledger["active_claims"] + ledger.get("effect_claim_history", [])
                    or claim.get("scope_identity") != self.authority_scope
                    or claim.get("root_instance_id") != checkpoint["root_instance_id"]
                    or claim.get("scope_authority_epoch") != ledger["authority_epoch"]
                ):
                    raise EffectError("stale_attempt_fence")
                for record in journal["effect_records"]:
                    if any(
                        previous["work_identity"] == record["effect_id"]
                        and int(previous["attempt_fence"]) > int(record["attempt_fence"])
                        for previous in ledger.get("effect_claim_history", [])
                    ):
                        raise EffectError("stale_attempt_fence")
                    expected = {
                        "work_identity": record["effect_id"],
                        "attempt_fence": record["attempt_fence"],
                    }
                    issued = next(
                        (
                            item
                            for item in ledger["journal_entries"]
                            if item["work_identity"] == record["effect_id"]
                        ),
                        None,
                    )
                    if (issued is not None and issued != expected) or (
                        issued is None and record["attempt_fence"] != "0"
                    ):
                        raise EffectError("stale_attempt_fence")
                    if (
                        record["invocation_state"] == "leased"
                        and document["claims"].get(record["effect_id"])
                        not in ledger["active_claims"]
                    ):
                        raise EffectError("stale_attempt_fence")
                    if any(
                        item["work_identity"] == record["effect_id"]
                        and (
                            document["claims"].get(record["effect_id"]) != item
                            or record["invocation_state"] != "leased"
                            or item["attempt_fence"] != record["attempt_fence"]
                            or item["operation_token"] != record["operation_token"]
                        )
                        for item in ledger["active_claims"]
                    ):
                        raise EffectError("stale_attempt_fence")
                self._mirror_authority(ledger, document)
                connection.execute(
                    "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
                    (
                        json.dumps(ledger, sort_keys=True, separators=(",", ":")),
                        self.authority_scope,
                    ),
                )
            connection.commit()

    @staticmethod
    def _mirror_authority(
        ledger: dict[str, Any], document: dict[str, Any], *, advance_generation: bool = True
    ) -> None:
        """Mirror effect fences and exact committed bytes in the colocated authority ledger."""
        native_roots = ledger.setdefault("native_effect_roots", [])
        root = document["checkpoint"]["root_instance_id"]
        if root not in native_roots:
            native_roots.append(root)
        records = document["journal"]["effect_records"]
        for record in records:
            binding = {
                "root_instance_id": root,
                "work_kind": "effect",
                "work_identity": record["effect_id"],
                "operation_token": record["operation_token"],
                "participant": "native_effects",
            }
            bindings = ledger.setdefault("native_effect_work", [])
            existing = next(
                (item for item in bindings if item["work_identity"] == record["effect_id"]), None
            )
            if existing is not None and existing != binding:
                raise EffectError("stale_attempt_fence")
            if existing is None:
                bindings.append(binding)
            entry = next(
                (
                    item
                    for item in ledger["journal_entries"]
                    if item["work_identity"] == record["effect_id"]
                ),
                None,
            )
            if entry is None:
                ledger["journal_entries"].append(
                    {"work_identity": record["effect_id"], "attempt_fence": record["attempt_fence"]}
                )
            else:
                if int(record["attempt_fence"]) < int(entry["attempt_fence"]):
                    raise EffectError("stale_attempt_fence")
                entry["attempt_fence"] = record["attempt_fence"]
        work = {record["effect_id"] for record in records}
        ledger["active_claims"] = [
            claim for claim in ledger["active_claims"] if claim["work_identity"] not in work
        ] + [
            copy.deepcopy(document["claims"][record["effect_id"]])
            for record in records
            if record["invocation_state"] == "leased" and record["effect_id"] in document["claims"]
        ]
        history_claims = ledger.setdefault("effect_claim_history", [])
        for claim in document["claims"].values():
            if claim not in history_claims:
                history_claims.append(copy.deepcopy(claim))
        from .authority import _native_checkpoint_history

        source = canonical_bytes(document["checkpoint"]).decode()
        history = _native_checkpoint_history(ledger)
        if source not in history:
            history.append(source)
        ledger["checkpoint_bytes"].append(source)
        ledger["mutation_bytes"].append(canonical_bytes(document["journal"]).decode())
        if advance_generation:
            ledger["scope_generation"] = str(int(ledger["scope_generation"]) + 1)

    def snapshot(self, root_instance_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT document FROM determa_committed_effects WHERE root_instance_id = ?",
                (root_instance_id,),
            ).fetchone()
        if row is None:
            raise EffectError("wrong_root")
        document: dict[str, Any] = json.loads(row[0])
        validate_journal(document["checkpoint"], document["journal"], self.resolver)
        restore_execution_checkpoint_v1(document["checkpoint"], self.resolver)
        return document

    def _transact(
        self,
        root: str,
        change: Callable[[dict[str, Any]], _T],
        *,
        expected_epoch: str | None = None,
        authority_mutation: Callable[[dict[str, Any], dict[str, Any]], None] | None = None,
        worker_guard: tuple[str, str, str | None, str] | None = None,
    ) -> _T:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT document FROM determa_committed_effects WHERE root_instance_id = ?", (root,)
            ).fetchone()
            if row is None:
                raise EffectError("wrong_root")
            document: dict[str, Any] = json.loads(row[0])
            validate_journal(document["checkpoint"], document["journal"], self.resolver)
            restore_execution_checkpoint_v1(document["checkpoint"], self.resolver)
            expiry_guard: tuple[Callable[[], str], int] | None = None
            if worker_guard is not None:
                effect_id, _principal, _fence, _trusted_now = worker_guard
                record = _record(document["journal"], effect_id)
                if record["invocation_state"] in {"leased", "outcome_recorded"}:
                    claim = document["claims"].get(effect_id)
                    clock = self.trusted_clock
                    if clock is None or claim is None:
                        raise EffectError("stale_attempt_fence")
                    expiry_guard = (clock, _now(claim["expires_at"]))
                    self._check_live_clock(*expiry_guard)
            ledger = None
            if self.authority_scope is not None:
                authority = connection.execute(
                    "SELECT ledger FROM determa_scope_authority WHERE scope_identity = ?",
                    (self.authority_scope,),
                ).fetchone()
                if authority is None:
                    raise EffectError("stale_scope_authority")
                ledger = json.loads(authority[0])
                from .authority import _native_checkpoint_history

                if (
                    root not in ledger["roots"]
                    or document["journal"]["scope_identity"] != self.authority_scope
                    or canonical_bytes(document["checkpoint"]).decode()
                    not in _native_checkpoint_history(ledger)
                ):
                    raise EffectError("unauthorized_scope")
                if ledger["state"] != "active" or (
                    expected_epoch is not None and ledger["authority_epoch"] != expected_epoch
                ):
                    raise EffectError("stale_scope_authority")
                _validate_authority_pair(ledger, document)
                if worker_guard is not None:
                    effect_id, principal, fence, trusted_now = worker_guard
                    journal_record = _record(document["journal"], effect_id)
                    if fence is None:
                        fence = journal_record["attempt_fence"]
                    local = document["claims"].get(effect_id)
                    if local is not None and local["worker_principal"] != principal:
                        raise EffectError("unauthorized_scope")
                    replay = journal_record["invocation_state"] in {
                        "result_admitted",
                        "closed",
                        "unclaimed",
                        "ambiguous",
                    } and any(
                        item["attempt_fence"] == fence for item in journal_record["attempt_records"]
                    )
                    if not replay:
                        current = next(
                            (
                                item
                                for item in ledger["active_claims"]
                                if item["work_identity"] == effect_id
                            ),
                            None,
                        )
                        if (
                            current is None
                            or current != local
                            or current["worker_principal"] != principal
                            or current["attempt_fence"] != fence
                            or current["scope_authority_epoch"] != ledger["authority_epoch"]
                            or current["state"] != "active"
                            or _now(trusted_now) >= _now(current["expires_at"])
                        ):
                            raise EffectError("stale_attempt_fence")
            before = canonical_bytes(document)
            value = change(document)
            validate_journal(document["checkpoint"], document["journal"], self.resolver)
            restore_execution_checkpoint_v1(document["checkpoint"], self.resolver)
            if (
                ledger is not None
                and authority_mutation is not None
                and canonical_bytes(document) != before
            ):
                authority_mutation(ledger, document)
                connection.execute(
                    "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
                    (
                        json.dumps(ledger, sort_keys=True, separators=(",", ":")),
                        self.authority_scope,
                    ),
                )
            if canonical_bytes(document) != before:
                connection.execute(
                    "UPDATE determa_committed_effects SET document = ? WHERE root_instance_id = ?",
                    (canonical_bytes(document), root),
                )
            if expiry_guard is not None:
                # Keep the original deadline even when this transaction closes
                # or revokes the claim. Worker rights must still hold at commit.
                self._check_live_clock(*expiry_guard)
            connection.commit()
            return value

    @staticmethod
    def _check_live_clock(clock: Callable[[], str], deadline: int) -> None:
        try:
            now = _now(clock())
        except Exception as exc:
            raise EffectError("stale_attempt_fence") from exc
        if now >= deadline:
            raise EffectError("stale_attempt_fence")

    def produce(
        self,
        root: str,
        operation_id: str,
        target_runtime_id: str,
        *,
        expected_revision: str,
        expected_digest: str,
        route_generation: str,
        operation_token: str,
        after_route_resolved: Callable[[Mapping[str, Any]], None] | None = None,
        before_commit: Callable[[Mapping[str, Any], Mapping[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Step one selected event, pin every emitted external intent, then commit."""

        # CAS and route generation guard new work. Replay identifies the original
        # production operation independently of current admission preconditions.
        request_identity = {
            "operation_kind": "produce",
            "root_instance_id": root,
            "target_runtime_id": target_runtime_id,
            "operation_token": operation_token,
        }

        def change(document: dict[str, Any]) -> dict[str, Any]:
            saved = document["responses"].get(operation_id)
            if saved is not None:
                if (
                    document.get("producer_requests", {}).get(operation_id) != request_identity
                    or saved.get("kind") != "processing"
                ):
                    raise EffectError("operation_id_conflict")
                return copy.deepcopy(saved)
            reconstructed = _reconstruct_producer_response(
                document["checkpoint"],
                document["journal"],
                operation_id,
                request_identity,
                self.resolver,
            )
            if reconstructed is not None:
                return reconstructed
            if route_generation != self.route["generation"]:
                raise EffectError("scope_generation_conflict")
            self._verified_handler(
                self.route["handler_reference"], self.route["destination_binding_digest"]
            )
            if after_route_resolved is not None:
                after_route_resolved(self.route)
            checkpoint, journal = document["checkpoint"], document["journal"]
            if self.core_observer is not None:
                runtime = next(
                    item
                    for item in checkpoint["root_record"]["aggregate_state"]["runtimes"]
                    if item["runtime_id"] == target_runtime_id
                )
                self.core_observer(
                    "step",
                    runtime["ready_mailbox"][0]["envelope"]["event_id"],
                    runtime["target_identity"],
                )
            result = step_checkpoint_v1(
                checkpoint,
                target_runtime_id,
                self.resolver,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_digest,
                _include_host_response=True,
            )
            candidate = result["checkpoint"]
            if "core_result" not in result:
                raise EffectError("invalid_host_request")
            for item in candidate["pending_outbox_intents"]:
                intent = item["intent"]
                if any(
                    item["effect_id"] == intent["effect_id"] for item in journal["effect_records"]
                ):
                    continue
                if intent["correlation_id"] != operation_token:
                    raise EffectError("effect_not_outstanding")
                journal["effect_records"].append(
                    {
                        "effect_id": intent["effect_id"],
                        "operation_token": operation_token,
                        "intent_digest": outbox_intent_digest(root, intent),
                        "handler_reference": copy.deepcopy(self.route["handler_reference"]),
                        "destination_binding_digest": self.route["destination_binding_digest"],
                        "route_configuration_generation": route_generation,
                        "result_mapping": copy.deepcopy(self.route["result_mapping"]),
                        "target": copy.deepcopy(self.route["target"]),
                        "idempotency_policy": self.route["idempotency_policy"],
                        "attempt_fence": "0",
                        "attempt_records": [],
                        "invocation_state": "unclaimed",
                        "outcome": None,
                        "result_event_id": None,
                        "admission_receipt": None,
                        "cancellation": None,
                    }
                )
            journal["effect_records"].sort(key=lambda item: item["effect_id"].encode())
            response = {
                "kind": "processing",
                "body": {"core_result": result["core_result"], "receipt": result["receipt"]},
            }
            journal["operation_response_references"].append(
                {
                    "operation_id": operation_id,
                    "response_digest": hash_value(["determa-host-operation-response-1", response]),
                }
            )
            journal["operation_response_references"].sort(
                key=lambda item: item["operation_id"].encode()
            )
            document["responses"][operation_id] = copy.deepcopy(response)
            document.setdefault("producer_requests", {})[operation_id] = copy.deepcopy(
                request_identity
            )
            document["checkpoint"] = candidate
            _bump(journal, candidate)
            if before_commit is not None:
                before_commit(candidate, journal)
            if route_generation != self.route["generation"]:
                raise EffectError("scope_generation_conflict")
            self._verified_handler(
                self.route["handler_reference"], self.route["destination_binding_digest"]
            )
            return response

        return self._transact(
            root,
            change,
            expected_epoch=self.route.get("authority_epoch"),
            authority_mutation=self._mirror_authority,
        )

    def claim(
        self,
        root: str,
        effect_id: str,
        principal: str,
        epoch: str,
        *,
        expires_at: str,
        trusted_now: str,
        deduplication_evidence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        def change(document: dict[str, Any]) -> dict[str, Any]:
            record = _record(document["journal"], effect_id)
            proof = None
            if deduplication_evidence is not None:
                if not isinstance(deduplication_evidence, Mapping):
                    raise EffectError("host_capability_mismatch")
                proof = copy.deepcopy(dict(deduplication_evidence))
                fields = {
                    "scope_identity",
                    "effect_id",
                    "destination_binding_digest",
                    "first_attempt_receipt_bytes_base64",
                    "repeat_attempt_receipt_bytes_base64",
                }
                if (
                    set(proof) != fields
                    or proof["scope_identity"] != document["journal"]["scope_identity"]
                    or proof["effect_id"] != effect_id
                    or proof["destination_binding_digest"] != record["destination_binding_digest"]
                ):
                    raise EffectError("host_capability_mismatch")
                try:
                    first = base64.b64decode(
                        proof["first_attempt_receipt_bytes_base64"], validate=True
                    )
                    repeated = base64.b64decode(
                        proof["repeat_attempt_receipt_bytes_base64"], validate=True
                    )
                    if not first or first != repeated:
                        raise ValueError("destination receipts differ")
                except (ValueError, TypeError) as exc:
                    raise EffectError("host_capability_mismatch") from exc
                handler = self._verified_handler(
                    record["handler_reference"], record["destination_binding_digest"]
                )
                VerifiedNativeHandler.verify_deduplication_evidence(
                    handler,
                    record["handler_reference"],
                    record["destination_binding_digest"],
                    proof,
                )
            claim = _issue_effect_claim(
                document,
                effect_id,
                principal,
                epoch,
                expires_at,
                trusted_now,
                deduplication_proven=proof is not None,
            )
            if proof is not None:
                document.setdefault("destination_evidence", {}).setdefault(effect_id, []).append(
                    {
                        "root_instance_id": root,
                        "operation_token": record["operation_token"],
                        "handler_reference": copy.deepcopy(record["handler_reference"]),
                        "attempt_fence": claim["attempt_fence"],
                        "evidence": proof,
                    }
                )
            return claim

        return self._transact(
            root, change, expected_epoch=epoch, authority_mutation=self._mirror_authority
        )

    def terminalize_outbox(
        self, root: str, effect_id: str, outcome: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Record adapter acceptance without closing the business invocation."""

        def change(document: dict[str, Any]) -> dict[str, Any]:
            checkpoint, journal = document["checkpoint"], document["journal"]
            _record(journal, effect_id)
            for record in checkpoint["terminal_outbox_records"]:
                if record["intent"]["effect_id"] == effect_id:
                    if record["outcome"] != dict(outcome):
                        raise EffectError("effect_result_conflict")
                    return copy.deepcopy(record)
            pending = next(
                (
                    item
                    for item in checkpoint["pending_outbox_intents"]
                    if item["intent"]["effect_id"] == effect_id
                ),
                None,
            )
            if pending is None:
                raise EffectError("effect_not_outstanding")
            candidate = copy.deepcopy(checkpoint)
            candidate["revision"] = str(int(candidate["revision"]) + 1)
            candidate["pending_outbox_intents"] = [
                item
                for item in candidate["pending_outbox_intents"]
                if item["intent"]["effect_id"] != effect_id
            ]
            sequence = candidate["next_outbox_terminal_sequence"]
            candidate["next_outbox_terminal_sequence"] = str(int(sequence) + 1)
            record = {
                "terminal_sequence": sequence,
                "intent": copy.deepcopy(pending["intent"]),
                "committed_revision": candidate["revision"],
                "outcome": copy.deepcopy(dict(outcome)),
            }
            candidate["terminal_outbox_records"].append(record)
            document["checkpoint"] = seal_execution_checkpoint(candidate)
            _bump(journal, document["checkpoint"])
            return copy.deepcopy(record)

        return self._transact(
            root,
            change,
            expected_epoch=self.route.get("authority_epoch"),
            authority_mutation=self._mirror_authority,
        )

    @staticmethod
    def _authorize(
        document: Mapping[str, Any],
        effect_id: str,
        token: str,
        fence: str,
        principal: str,
        scope: str,
        epoch: str,
        trusted_now: str,
        *,
        allow_replay: bool = False,
    ) -> dict[str, Any]:
        journal = document["journal"]
        if scope != journal["scope_identity"]:
            raise EffectError("unauthorized_scope")
        record = _record(journal, effect_id)
        if token != record["operation_token"]:
            raise EffectError("effect_not_outstanding")
        claim = document["claims"].get(effect_id)
        if claim is None or claim["worker_principal"] != principal:
            raise EffectError("unauthorized_scope")
        if epoch != claim["scope_authority_epoch"]:
            raise EffectError("stale_scope_authority")
        if fence != record["attempt_fence"] or fence != claim["attempt_fence"]:
            raise EffectError("stale_attempt_fence")
        if not allow_replay and (
            claim["state"] != "active" or _now(trusted_now) >= _now(claim["expires_at"])
        ):
            raise EffectError("stale_attempt_fence")
        return record

    def dispatch(
        self,
        root: str,
        effect_id: str,
        principal: str,
        scope: str,
        epoch: str,
        trusted_now: str,
        *,
        credential: Any,
        authorized: bool = True,
    ) -> Mapping[str, Any]:
        """Run an installed native handler after durable intent and live guard proof."""
        clock = self.trusted_clock
        if clock is None:
            raise EffectError("stale_attempt_fence")

        def call(document: dict[str, Any]) -> Mapping[str, Any]:
            record = _record(document["journal"], effect_id)
            if record["invocation_state"] != "leased":
                raise EffectError("effect_not_outstanding")
            self._authorize(
                document,
                effect_id,
                record["operation_token"],
                record["attempt_fence"],
                principal,
                scope,
                epoch,
                clock(),
            )
            if (
                not authorized
                or credential is None
                or record["handler_reference"] != self.route["handler_reference"]
                or record["destination_binding_digest"] != self.route["destination_binding_digest"]
            ):
                raise EffectError("unauthorized_scope")
            intent = next(
                item["intent"]
                for item in (
                    document["checkpoint"]["pending_outbox_intents"]
                    + document["checkpoint"]["terminal_outbox_records"]
                )
                if item["intent"]["effect_id"] == effect_id
            )
            metadata = {
                "scope_identity": scope,
                "effect_id": effect_id,
                "operation_token": record["operation_token"],
                "destination_binding_digest": record["destination_binding_digest"],
                "credential": credential,
                "route_configuration_generation": record["route_configuration_generation"],
                "handler_reference": copy.deepcopy(record["handler_reference"]),
            }
            handler = self._verified_handler(
                record["handler_reference"], record["destination_binding_digest"]
            )
            result = VerifiedNativeHandler.invoke(
                handler,
                record["handler_reference"],
                copy.deepcopy(intent["payload"]),
                metadata,
                {"attempt_fence": record["attempt_fence"]},
            )
            # External acceptance cannot extend an expired worker's mutation
            # rights. A refusal leaves its leased journal for reconciliation.
            self._authorize(
                document,
                effect_id,
                record["operation_token"],
                record["attempt_fence"],
                principal,
                scope,
                epoch,
                clock(),
            )
            return result

        return self._transact(
            root, call, expected_epoch=epoch, worker_guard=(effect_id, principal, None, trusted_now)
        )

    def submit_result(
        self,
        root: str,
        request: Mapping[str, Any],
        *,
        principal: str,
        scope: str,
        epoch: str,
        trusted_now: str,
    ) -> dict[str, Any]:
        if (
            not isinstance(request, Mapping)
            or type(request.get("effect_id")) is not str
            or re.fullmatch(r"sha256:[0-9a-f]{64}", request["effect_id"]) is None
            or type(request.get("attempt_fence")) is not str
            or re.fullmatch(r"0|[1-9][0-9]*", request["attempt_fence"]) is None
        ):
            raise EffectError("invalid_host_request")
        effect_id, fence = request["effect_id"], request["attempt_fence"]
        try:
            request = _validate_request(request, "effect-result-request-v1.schema.json")
            response = self._record_result(
                root,
                request,
                principal=principal,
                scope=scope,
                epoch=epoch,
                trusted_now=trusted_now,
            )
            if request["outcome_kind"] in _OUTCOMES:
                # The authenticated outcome is already durable. Admission is a
                # separate host-owned transaction; its failure cannot erase it.
                def admission(document: dict[str, Any]) -> dict[str, Any]:
                    record = _record(document["journal"], effect_id)
                    saved = document.setdefault("result_responses", {}).get(effect_id + ":" + fence)
                    if saved is not None:
                        return copy.deepcopy(saved)
                    if record["invocation_state"] == "outcome_recorded":
                        self._admit(document, record)
                    response = _result_response(
                        record, document["journal"], document["checkpoint"], "committed"
                    )
                    document["result_responses"][effect_id + ":" + fence] = copy.deepcopy(response)
                    return response

                return self._transact(
                    root, admission, expected_epoch=epoch, authority_mutation=self._mirror_authority
                )
            return response
        except EffectError as exc:
            return _rejected_result(effect_id, fence, exc.code)

    def _record_result(
        self,
        root: str,
        request: Mapping[str, Any],
        *,
        principal: str,
        scope: str,
        epoch: str,
        trusted_now: str,
    ) -> dict[str, Any]:
        """Private durable-outcome stage; never a completed public result."""
        return self._transact(
            root,
            lambda document: self._submit(document, request, principal, scope, epoch, trusted_now),
            expected_epoch=epoch,
            authority_mutation=self._mirror_authority,
            worker_guard=(request["effect_id"], principal, request["attempt_fence"], trusted_now),
        )

    def _submit(
        self,
        document: dict[str, Any],
        request: Mapping[str, Any],
        principal: str,
        scope: str,
        epoch: str,
        trusted_now: str,
    ) -> dict[str, Any]:
        journal, checkpoint = document["journal"], document["checkpoint"]
        effect_id, fence, kind = (
            request["effect_id"],
            request["attempt_fence"],
            request["outcome_kind"],
        )
        if kind not in _REPORTS:
            raise EffectError("invalid_host_request")
        preexisting = _record(journal, effect_id)
        retained = _retained_report(preexisting, request)
        record = self._authorize(
            document,
            effect_id,
            request["operation_token"],
            fence,
            principal,
            scope,
            epoch,
            trusted_now,
            allow_replay=retained is not None
            and (kind not in _OUTCOMES or preexisting["admission_receipt"] is not None),
        )
        key = effect_id + ":" + fence
        prior = document["result_requests"].get(key)
        if prior is not None or any(
            item["attempt_fence"] == fence for item in record["attempt_records"]
        ):
            if prior is not None and prior != dict(request) or retained is None:
                raise EffectError("effect_result_conflict")
            saved = document.setdefault("result_responses", {}).get(key)
            if saved is not None:
                return copy.deepcopy(saved)
            if record["invocation_state"] in {"result_admitted", "closed"}:
                return _result_response(record, journal, checkpoint, "committed")
            return _result_response(
                record,
                journal,
                checkpoint,
                "report_recorded" if kind not in _OUTCOMES else "outcome_recorded",
                next(
                    (item for item in record["attempt_records"] if item["attempt_fence"] == fence),
                    None,
                ),
            )
        if record["invocation_state"] != "leased":
            raise EffectError("effect_not_outstanding")
        payload = copy.deepcopy(request["payload"])
        if kind in _OUTCOMES:
            self._result_delivery(checkpoint, record, kind, payload)
        reason = _report_reason(request)
        report = {
            "attempt_fence": fence,
            "report_kind": kind,
            "report_digest": hash_value(
                [
                    "determa-effect-attempt-report-1",
                    effect_id,
                    record["operation_token"],
                    fence,
                    kind,
                    payload,
                    reason,
                ]
            ),
            "reason": reason,
        }
        record["attempt_records"].append(report)
        document["result_requests"][key] = copy.deepcopy(dict(request))
        if kind not in _OUTCOMES:
            record["invocation_state"] = "unclaimed" if kind == "retryable_failure" else "ambiguous"
            _bump(journal)
            response = _result_response(record, journal, checkpoint, "report_recorded", report)
            document.setdefault("result_responses", {})[key] = copy.deepcopy(response)
            return response
        mapping = _mapping(record, kind)
        record["outcome"] = {
            "kind": kind,
            "payload": payload,
            "digest": hash_value(
                [
                    "determa-effect-outcome-1",
                    effect_id,
                    record["operation_token"],
                    kind,
                    payload,
                    fence,
                ]
            ),
            "attempt_fence": fence,
        }
        record["result_event_id"] = hash_value(
            ["determa-effect-result-event-1", effect_id, mapping["result_slot"]]
        )
        record["invocation_state"] = "outcome_recorded"
        _bump(journal)
        return _result_response(record, journal, document["checkpoint"], "outcome_recorded")

    def _result_delivery(
        self, checkpoint: dict[str, Any], record: dict[str, Any], kind: str, payload: Any
    ) -> dict[str, Any]:
        aggregate = checkpoint["root_record"].get("aggregate_state")
        runtime = next(
            (
                item
                for item in (aggregate["runtimes"] if aggregate else [])
                if item["runtime_id"] == record["target"]["runtime_id"]
            ),
            None,
        )
        if (
            runtime is None
            or runtime["identity_origin"] != record["target"]["runtime_incarnation"]
            or record["target"]["root_instance_id"] != checkpoint["root_instance_id"]
        ):
            raise EffectError("effect_not_outstanding")
        delivery = _result_delivery_evidence(
            checkpoint, record, kind, payload, runtime["target_identity"]
        )
        try:
            _preflight_checkpoint_admission(
                checkpoint,
                [delivery],
                self.resolver,
                expected_revision=checkpoint["revision"],
                expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
            )
        except ArtifactError as exc:
            raise EffectError("invalid_host_request") from exc
        return delivery

    def _admit(self, document: dict[str, Any], record: dict[str, Any]) -> None:
        checkpoint = document["checkpoint"]
        delivery = self._result_delivery(
            checkpoint, record, record["outcome"]["kind"], record["outcome"]["payload"]
        )
        if self.core_observer is not None:
            self.core_observer("admit", record["result_event_id"], delivery["envelope"]["target"])
        candidate = admit_checkpoint_v1(
            checkpoint,
            [delivery],
            self.resolver,
            expected_revision=checkpoint["revision"],
            expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
        )
        document["checkpoint"] = candidate
        record["admission_receipt"] = copy.deepcopy(candidate["operation_receipts"][-1])
        record["invocation_state"] = "result_admitted"
        _bump(document["journal"], candidate)
        key = record["effect_id"] + ":" + record["outcome"]["attempt_fence"]
        document.setdefault("result_responses", {})[key] = _result_response(
            record, document["journal"], candidate, "committed"
        )

    def recover(self, root: str) -> dict[str, Any]:
        # Each recovered effect has its own atomic journal transition. A crash
        # between records leaves the remaining records available to recover.
        snapshot = self.snapshot(root)
        for selected in snapshot["journal"]["effect_records"]:
            effect_id = selected["effect_id"]

            def change(document: dict[str, Any], effect_id: str = effect_id) -> None:
                record = _record(document["journal"], effect_id)
                if record["invocation_state"] == "outcome_recorded":
                    self._admit(document, record)
                elif record["invocation_state"] == "leased":
                    reason = "provider_acceptance_unknown"
                    record["attempt_records"].append(
                        {
                            "attempt_fence": record["attempt_fence"],
                            "report_kind": "ambiguous",
                            "reason": reason,
                            "report_digest": hash_value(
                                [
                                    "determa-effect-attempt-report-1",
                                    record["effect_id"],
                                    record["operation_token"],
                                    record["attempt_fence"],
                                    "ambiguous",
                                    ["map", []],
                                    reason,
                                ]
                            ),
                        }
                    )
                    record["invocation_state"] = "ambiguous"
                    document["claims"][record["effect_id"]]["state"] = "revoked"
                    _bump(document["journal"])

            self._transact(
                root,
                change,
                expected_epoch=self.route.get("authority_epoch"),
                authority_mutation=self._mirror_authority,
            )
        return self.snapshot(root)

    def cancel(self, root: str, request: Mapping[str, Any]) -> dict[str, Any]:
        if (
            not isinstance(request, Mapping)
            or type(request.get("effect_id")) is not str
            or re.fullmatch(r"sha256:[0-9a-f]{64}", request["effect_id"]) is None
            or type(request.get("operation_id")) is not str
            or not request["operation_id"]
        ):
            raise EffectError("invalid_host_request")
        try:
            request = _validate_request(request, "effect-cancellation-request-v1.schema.json")
        except EffectError:
            return {
                "status": "rejected",
                "operation_id": request["operation_id"],
                "effect_id": request["effect_id"],
                "cancellation": None,
                "outcome": None,
                "result_event_id": None,
                "journal_revision": None,
                "error_code": "invalid_host_request",
            }

        def change(document: dict[str, Any]) -> dict[str, Any]:
            journal = document["journal"]
            record = _record(journal, request["effect_id"])
            operation_id = request["operation_id"]
            saved = document["responses"].get(operation_id)
            if saved is not None:
                if document["cancel_requests"].get(operation_id) != dict(request):
                    raise EffectError("operation_id_conflict")
                return copy.deepcopy(saved)
            previous = record["cancellation"]
            if previous is not None and previous["operation_id"] == operation_id:
                if previous["reason"] != request["reason"] or (
                    previous["state"] == "prevented_start"
                    and record["outcome"]["payload"] != request["payload"]
                ):
                    raise EffectError("operation_id_conflict")
                replay = {
                    "status": "committed"
                    if previous["state"] == "prevented_start"
                    else "reconciliation_required",
                    "operation_id": operation_id,
                    "effect_id": record["effect_id"],
                    "cancellation": copy.deepcopy(previous),
                    "outcome": copy.deepcopy(record["outcome"])
                    if previous["state"] == "prevented_start"
                    else None,
                    "result_event_id": record["result_event_id"]
                    if previous["state"] == "prevented_start"
                    else None,
                    "journal_revision": journal["journal_revision"],
                    "error_code": None,
                }
                reference = next(
                    (
                        item
                        for item in journal["operation_response_references"]
                        if item["operation_id"] == operation_id
                    ),
                    None,
                )
                if reference is None or reference["response_digest"] != hash_value(
                    ["determa-host-operation-response-1", replay]
                ):
                    raise EffectError("operation_id_conflict")
                return replay
            if record["invocation_state"] == "unclaimed" and record["attempt_fence"] == "0":
                mapping = _mapping(record, "cancelled")
                self._result_delivery(
                    document["checkpoint"], record, "cancelled", request["payload"]
                )
                state = "prevented_start"
                outcome = {
                    "kind": "cancelled",
                    "payload": copy.deepcopy(request["payload"]),
                    "digest": hash_value(
                        [
                            "determa-effect-outcome-1",
                            record["effect_id"],
                            record["operation_token"],
                            "cancelled",
                            request["payload"],
                            "0",
                        ]
                    ),
                    "attempt_fence": "0",
                }
                record["outcome"] = outcome
                record["result_event_id"] = hash_value(
                    ["determa-effect-result-event-1", record["effect_id"], mapping["result_slot"]]
                )
                record["invocation_state"] = "outcome_recorded"
            else:
                state = "reconciliation_required"
                outcome = None
            cancellation = {
                "operation_id": operation_id,
                "reason": request["reason"],
                "state": state,
            }
            record["cancellation"] = cancellation
            _bump(journal)
            response = {
                "status": "committed" if outcome else "reconciliation_required",
                "operation_id": operation_id,
                "effect_id": record["effect_id"],
                "cancellation": cancellation,
                "outcome": outcome,
                "result_event_id": record["result_event_id"] if outcome else None,
                "journal_revision": journal["journal_revision"],
                "error_code": None,
            }
            document["responses"][operation_id] = copy.deepcopy(response)
            document["cancel_requests"][operation_id] = copy.deepcopy(dict(request))
            journal["operation_response_references"].append(
                {
                    "operation_id": operation_id,
                    "response_digest": hash_value(["determa-host-operation-response-1", response]),
                }
            )
            journal["operation_response_references"].sort(
                key=lambda item: item["operation_id"].encode()
            )
            journal["host_effect_journal_digest"] = journal_digest(journal)
            return response

        return self._transact(
            root,
            change,
            expected_epoch=self.route.get("authority_epoch"),
            authority_mutation=self._mirror_authority,
        )
