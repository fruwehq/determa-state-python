"""Optional synchronous host for portable execution checkpoints."""

from __future__ import annotations

import copy
from collections.abc import Callable, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, cast

from .checkpoint import (
    RestoredExecutionCheckpoint,
    restore_execution_checkpoint,
    seal_execution_checkpoint,
    serialize_execution_checkpoint,
    validate_execution_checkpoint_member,
)
from .definition import Bundle, BundleSource, load_bundle
from .engine import create as core_create
from .engine import dispatch as core_dispatch
from .errors import DetermaError
from .migration import MigrationLimits, migrate_aggregate
from .stores import (
    COMPACT_EFFECT_IDENTITY_RETENTION,
    DURABLE_CONCURRENT,
    DURABLE_SINGLE_WRITER,
    PERMANENT_OUTBOX_TERMINAL_RETENTION,
    PERMANENT_RECEIPT_RETENTION,
    ROOT_IDENTITY_RETENTION,
    SHARED_APPLICATION_TRANSACTION,
    ExecutionStore,
    ExecutionStoreRegistry,
    ExecutionStoreTransaction,
)
from .wire import (
    ArtifactResolver,
    aggregate_envelope,
    decoded_typed_value,
    hash_value,
    typed_value,
)

FaultInjector = Callable[[str], None]
_MAX_DECIMAL_DIGITS = 4096


class ExecutionHostError(DetermaError):
    """A closed host-layer failure."""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        self.message = message or code
        super().__init__(self.message)


def _checkpoint_number(value: Any) -> int:
    if (
        not isinstance(value, str)
        or len(value) > _MAX_DECIMAL_DIGITS
        or (
            value != "0"
            and (
                not value
                or value[0] == "0"
                or not value.isascii()
                or not value.isdigit()
            )
        )
    ):
        raise ExecutionHostError("invalid_execution_checkpoint")
    try:
        return int(value)
    except ValueError as exc:
        raise ExecutionHostError("invalid_execution_checkpoint") from exc


def _increment_checkpoint_number(value: Any) -> str:
    result = str(_checkpoint_number(value) + 1)
    if len(result) > _MAX_DECIMAL_DIGITS:
        raise ExecutionHostError("invalid_execution_checkpoint")
    return result


def creation_request_digest(
    bundle: Bundle | BundleSource,
    machine_id: str,
    root_instance_id: str,
    creation_id: str,
    bindings: Mapping[str, Any],
) -> str:
    """Compute one canonical creation operation identity."""
    validated = bundle if isinstance(bundle, Bundle) else load_bundle(bundle)
    machine = next(
        (
            item
            for item in validated.raw["machines"]
            if item["machine_id"] == machine_id
        ),
        None,
    )
    machine_version = "0" if machine is None else str(machine["version"])
    return hash_value(
        [
            "determa-creation-request-digest-1",
            "1",
            validated.fingerprint,
            validated.namespace,
            machine_id,
            machine_version,
            root_instance_id,
            creation_id,
            typed_value(dict(bindings)),
        ]
    )


def delivery_request_digest(
    root_instance_id: str, delivery_mode: str, envelope: Mapping[str, Any]
) -> str:
    """Compute one canonical pending/receipt delivery identity."""
    return hash_value(
        [
            "determa-inbox-envelope-digest-1",
            "1",
            root_instance_id,
            delivery_mode,
            dict(envelope),
        ]
    )


def portable_envelope(
    event: str,
    event_id: str,
    target: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    """Project one native host envelope into the checkpoint wire shape."""
    result = {
        "event": event,
        "event_id": event_id,
        "target": copy.deepcopy(dict(target)),
        "payload": typed_value(dict(payload)),
    }
    if correlation_id is not None:
        result["correlation_id"] = correlation_id
    if not validate_execution_checkpoint_member("envelope", result):
        raise ExecutionHostError("malformed_delivery")
    return result


def maintenance_migration_request_digest(
    root_instance_id: str,
    operation_id: str,
    source_aggregate_state_digest: str,
    target_validated_bundle_fingerprint: str,
    migration_descriptor_digest_route: Sequence[str],
    maintenance_mode: bool,
) -> str:
    """Compute one canonical keyed maintenance-migration identity."""
    return hash_value(
        [
            "determa-maintenance-migration-request-digest-1",
            "1",
            root_instance_id,
            operation_id,
            source_aggregate_state_digest,
            target_validated_bundle_fingerprint,
            list(migration_descriptor_digest_route),
            maintenance_mode,
        ]
    )


def outbox_intent_digest(
    root_instance_id: str, intent: Mapping[str, Any]
) -> str:
    """Compute the compact evidence digest for one complete outbox intent."""
    return hash_value(
        [
            "determa-outbox-intent-digest-1",
            "1",
            root_instance_id,
            dict(intent),
        ]
    )


def validate_host_profile(
    store: ExecutionStore,
    profile: str,
    *,
    host_features: set[str] | frozenset[str],
) -> None:
    """Validate one composed checkpoint-host profile without name inference."""
    capabilities = store.capabilities
    checkpoint_retention_mode = store.checkpoint_retention_mode
    durable = bool(
        {DURABLE_SINGLE_WRITER, DURABLE_CONCURRENT}.intersection(capabilities)
    )
    common = durable and ROOT_IDENTITY_RETENTION in capabilities
    atomic = "atomic_checkpoint_processing" in host_features
    valid = False
    if profile == "durable_embedded_processing":
        valid = common and atomic
    elif profile == "exactly_once_committed_processing":
        valid = (
            common
            and atomic
            and checkpoint_retention_mode == "permanent"
            and PERMANENT_RECEIPT_RETENTION in capabilities
        )
    elif profile == "broker_integrated":
        valid = common and atomic and {
            "acknowledge_after_checkpoint_commit",
            "durable_redelivery",
            "outbox_worker",
        }.issubset(host_features)
    elif profile == "strict_durable_outbox":
        valid = (
            common
            and atomic
            and PERMANENT_OUTBOX_TERMINAL_RETENTION in capabilities
            and {
                "outbox_worker",
                "total_outbox_lifecycle",
                "retain_unresolved_outbox",
            }.issubset(host_features)
        )
    elif profile == "compact_durable_outbox":
        valid = (
            common
            and atomic
            and COMPACT_EFFECT_IDENTITY_RETENTION in capabilities
            and {
                "outbox_worker",
                "total_outbox_lifecycle",
                "retain_referenced_effect_tombstones",
            }.issubset(host_features)
        )
    elif profile == "shared_application_transaction":
        valid = (
            common
            and atomic
            and SHARED_APPLICATION_TRANSACTION in capabilities
            and "native_shared_application_transaction" in host_features
        )
    if not valid:
        raise ExecutionHostError("adapter_capability_mismatch")


@dataclass(frozen=True)
class StagedExecutionResult:
    """An operation staged inside a host-owned shared transaction."""

    operation: str
    state: str = "staged"


def _project_fault(
    result: Mapping[str, Any], aggregate: Mapping[str, Any] | None
) -> dict[str, Any] | None:
    fault = result["fault"]
    if fault is None or aggregate is None:
        return None
    for runtime in aggregate["runtimes"]:
        candidate = runtime["fault"]
        if candidate is not None and candidate["runtime_id"] == fault["runtime_id"]:
            return cast(dict[str, Any], copy.deepcopy(candidate))
    raise ExecutionHostError("invalid_execution_checkpoint")


def _project_emission(emission: Mapping[str, Any]) -> dict[str, Any]:
    if emission["target"] == "external":
        return {
            "kind": "external",
            "effect_id": emission["effect_id"],
            "sequence": str(emission["sequence"]),
            "event": emission["event"],
            "payload": typed_value(emission["payload"]),
            "correlation_id": emission["correlation_id"],
        }
    projected = {
        "kind": "internal",
        "event": emission["event"],
        "event_id": emission["event_id"],
        "target": copy.deepcopy(emission["target"]),
        "payload": typed_value(emission["payload"]),
    }
    if "correlation_id" in emission:
        projected["correlation_id"] = emission["correlation_id"]
    return projected


def _project_core_result(
    bundle: Bundle, result: Mapping[str, Any]
) -> dict[str, Any]:
    aggregate = (
        aggregate_envelope(bundle, result["state"])
        if result["state"] is not None
        else None
    )
    return {
        "status": result["status"],
        "disposition": result["disposition"],
        "aggregate_state": aggregate,
        "emissions": [_project_emission(item) for item in result["emissions"]],
        "fault": _project_fault(result, aggregate),
        "rejection": copy.deepcopy(result["rejection"]),
    }


def _append_emissions(
    checkpoint: dict[str, Any],
    receipt: dict[str, Any],
    projected_result: Mapping[str, Any],
) -> None:
    for index, emission in enumerate(projected_result["emissions"]):
        if emission["kind"] == "internal":
            sequence = checkpoint["next_delivery_sequence"]
            checkpoint["next_delivery_sequence"] = _increment_checkpoint_number(
                sequence
            )
            origin = {
                "kind": "internal_emission",
                "producing_receipt_sequence": receipt["receipt_sequence"],
                "emission_index": str(index),
            }
            envelope = {
                name: copy.deepcopy(emission[name])
                for name in ("event", "event_id", "target", "payload")
            }
            if "correlation_id" in emission:
                envelope["correlation_id"] = emission["correlation_id"]
            checkpoint["pending_deliveries"].append(
                {
                    "delivery_sequence": sequence,
                    "accepted_revision": checkpoint["revision"],
                    "delivery_mode": "internal",
                    "origin": origin,
                    "envelope": envelope,
                    "envelope_digest": delivery_request_digest(
                        checkpoint["root_instance_id"], "internal", envelope
                    ),
                }
            )
            receipt["emission_references"].append(
                {
                    "kind": "internal_delivery",
                    "emission_index": str(index),
                    "event_id": emission["event_id"],
                    "delivery_sequence": sequence,
                }
            )
        else:
            checkpoint["pending_outbox_intents"].append(
                {
                    "intent": {
                        "effect_id": emission["effect_id"],
                        "sequence": emission["sequence"],
                        "event": emission["event"],
                        "payload": copy.deepcopy(emission["payload"]),
                        "correlation_id": emission["correlation_id"],
                    },
                    "state_revision": checkpoint["revision"],
                    "delivery_state": {"status": "not_attempted"},
                }
            )
            receipt["emission_references"].append(
                {
                    "kind": "external_outbox",
                    "emission_index": str(index),
                    "effect_id": emission["effect_id"],
                }
            )


def _new_checkpoint(
    aggregate: dict[str, Any],
    request_digest: str,
    projected_result: Mapping[str, Any],
) -> dict[str, Any]:
    receipt = {
        "operation_kind": "creation",
        "receipt_sequence": "0",
        "creation_id": aggregate["creation_id"],
        "request_digest": request_digest,
        "committed_revision": "0",
        "resulting_aggregate_state_digest": aggregate["aggregate_state_digest"],
        "status": projected_result["status"],
        "fault": copy.deepcopy(projected_result["fault"]),
        "emission_references": [],
    }
    checkpoint = {
        "execution_checkpoint_format": "determa.execution_checkpoint",
        "execution_checkpoint_schema_version": 1,
        "root_instance_id": aggregate["root_instance_id"],
        "revision": "0",
        "root_record": {
            "status": "retained",
            "aggregate_state": copy.deepcopy(aggregate),
        },
        "replay_retention": {
            "mode": "permanent",
            "permanent_replay_eligible": True,
            "pruned_through_receipt_sequence": None,
            "policy_identifier": None,
        },
        "next_delivery_sequence": "0",
        "pending_deliveries": [],
        "next_operation_receipt_sequence": "1",
        "operation_receipts": [receipt],
        "pending_outbox_intents": [],
        "next_outbox_terminal_sequence": "0",
        "terminal_outbox_records": [],
        "outbox_effect_tombstones": [],
        "migration_audit_records": [],
    }
    _append_emissions(checkpoint, receipt, projected_result)
    return seal_execution_checkpoint(checkpoint)


def _mutate(checkpoint: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(checkpoint))
    result.pop("execution_checkpoint_digest", None)
    result["revision"] = _increment_checkpoint_number(result["revision"])
    return result


def _delivery_from_wire(mode: str, envelope: Mapping[str, Any]) -> dict[str, Any]:
    target = copy.deepcopy(envelope["target"])
    if "component" in target:
        target["component"]["activation_sequence"] = _checkpoint_number(
            target["component"]["activation_sequence"]
        )
    elif "spawned_instance" in target:
        target["spawned_instance"]["machine_version"] = _checkpoint_number(
            target["spawned_instance"]["machine_version"]
        )
    native_envelope = {
        "event": envelope["event"],
        "event_id": envelope["event_id"],
        "target": target,
        "payload": decoded_typed_value(envelope["payload"]),
    }
    if "correlation_id" in envelope:
        native_envelope["correlation_id"] = envelope["correlation_id"]
    return {mode: native_envelope}


class ExecutionHost:
    """Synchronous checkpoint orchestration around the pure core."""

    def __init__(
        self,
        store: ExecutionStore,
        artifact_resolver: ArtifactResolver,
        *,
        required_capabilities: set[str] | frozenset[str] = frozenset(),
        profile: str | None = None,
        host_features: set[str] | frozenset[str] = frozenset(
            {"atomic_checkpoint_processing"}
        ),
        fault_injector: FaultInjector | None = None,
    ) -> None:
        if not required_capabilities.issubset(store.capabilities):
            raise ExecutionHostError("adapter_capability_mismatch")
        if required_capabilities or profile is not None:
            store.validate_schema()
        if profile is not None:
            validate_host_profile(store, profile, host_features=host_features)
        self.store = store
        self.artifact_resolver = artifact_resolver
        self.fault_injector = fault_injector
        self._bound_transaction: ExecutionStoreTransaction | None = None

    @classmethod
    def from_uri(
        cls,
        uri: str,
        artifact_resolver: ArtifactResolver,
        registry: ExecutionStoreRegistry,
        *,
        configuration: Mapping[str, Any] | None = None,
        required_capabilities: set[str] | frozenset[str] = frozenset(),
        profile: str | None = None,
        host_features: set[str] | frozenset[str] = frozenset(
            {"atomic_checkpoint_processing"}
        ),
        fault_injector: FaultInjector | None = None,
    ) -> ExecutionHost:
        store = registry.resolve(
            uri,
            configuration=configuration,
            required_capabilities=required_capabilities,
        )
        return cls(
            store,
            artifact_resolver,
            required_capabilities=required_capabilities,
            profile=profile,
            host_features=host_features,
            fault_injector=fault_injector,
        )

    def _fault(self, boundary: str) -> None:
        if self.fault_injector is not None:
            self.fault_injector(boundary)

    def _after_commit(self) -> None:
        if self._bound_transaction is None:
            self._fault("after_commit_before_response")

    def _restore(
        self,
        source: bytes,
        root_instance_id: str,
    ) -> RestoredExecutionCheckpoint:
        restored = restore_execution_checkpoint(source, self.artifact_resolver)
        if restored.document["root_instance_id"] != root_instance_id:
            raise ExecutionHostError("transaction_root_mismatch")
        if (
            PERMANENT_RECEIPT_RETENTION in self.store.capabilities
            and restored.document["replay_retention"]["mode"] != "permanent"
        ):
            raise ExecutionHostError("adapter_capability_mismatch")
        if (
            PERMANENT_OUTBOX_TERMINAL_RETENTION in self.store.capabilities
            and restored.document["outbox_effect_tombstones"]
        ):
            raise ExecutionHostError("adapter_capability_mismatch")
        return restored

    def _transaction(
        self,
        root_instance_id: str,
    ) -> Any:
        if self._bound_transaction is not None:
            if self._bound_transaction.root_instance_id != root_instance_id:
                raise ExecutionHostError("transaction_root_mismatch")
            return nullcontext(self._bound_transaction)
        return self.store.transaction(root_instance_id)

    def _bound(self, transaction: ExecutionStoreTransaction) -> ExecutionHost:
        bound = copy.copy(self)
        bound._bound_transaction = transaction
        return bound

    def run_shared_transaction(
        self,
        root_instance_id: str,
        callback: Callable[[Any, SharedExecutionTransaction], None],
    ) -> dict[str, Any]:
        """Commit application writes and exactly one staged host operation together."""
        if SHARED_APPLICATION_TRANSACTION not in self.store.capabilities:
            raise ExecutionHostError("adapter_capability_mismatch")
        with self.store.shared_transaction(root_instance_id) as (
            native_transaction,
            store_transaction,
        ):
            if store_transaction.root_instance_id != root_instance_id:
                raise ExecutionHostError("transaction_root_mismatch")
            shared = SharedExecutionTransaction(
                self._bound(store_transaction), root_instance_id
            )
            try:
                callback(native_transaction, shared)
                response = shared._finish()
            finally:
                shared._deactivate()
        self._fault("after_commit_before_response")
        return response

    def _check_expected(
        self,
        checkpoint: Mapping[str, Any],
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> None:
        if (
            checkpoint["revision"] != expected_revision
            or checkpoint["execution_checkpoint_digest"]
            != expected_checkpoint_digest
        ):
            raise ExecutionHostError("checkpoint_revision_conflict")

    def _stage_insert(
        self, transaction: ExecutionStoreTransaction, candidate: dict[str, Any]
    ) -> None:
        restore_execution_checkpoint(candidate, self.artifact_resolver)
        self._fault("before_commit")
        if not transaction.insert(serialize_execution_checkpoint(candidate)):
            raise ExecutionHostError("checkpoint_revision_conflict")

    def _stage_replace(
        self,
        transaction: ExecutionStoreTransaction,
        previous: Mapping[str, Any],
        candidate: dict[str, Any],
    ) -> None:
        restore_execution_checkpoint(candidate, self.artifact_resolver)
        self._fault("before_commit")
        if not transaction.replace(
            previous["revision"],
            previous["execution_checkpoint_digest"],
            serialize_execution_checkpoint(candidate),
        ):
            raise ExecutionHostError("checkpoint_revision_conflict")

    def read_checkpoint(
        self,
        root_instance_id: str,
    ) -> RestoredExecutionCheckpoint | None:
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            return (
                None
                if source is None
                else self._restore(source, root_instance_id)
            )

    def create(
        self,
        bundle: Bundle | BundleSource,
        machine_id: str,
        root_instance_id: str,
        creation_id: str,
        bindings: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        validated = bundle if isinstance(bundle, Bundle) else load_bundle(bundle)
        normalized_bindings = {
            name: copy.deepcopy(dict(value))
            for name, value in (bindings or {}).items()
        }
        request_digest = creation_request_digest(
            validated,
            machine_id,
            root_instance_id,
            creation_id,
            normalized_bindings,
        )
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is not None:
                checkpoint = self._restore(source, root_instance_id).document
                receipt = checkpoint["operation_receipts"][0]
                if (
                    receipt["creation_id"] == creation_id
                    and receipt["request_digest"] == request_digest
                ):
                    return {"result": "committed", "receipt": copy.deepcopy(receipt)}
                raise ExecutionHostError("creation_id_conflict")
            result = core_create(
                validated,
                machine_id,
                root_instance_id,
                creation_id,
                normalized_bindings,
            )
            projected = _project_core_result(validated, result)
            aggregate = projected["aggregate_state"]
            if aggregate is None:
                raise ExecutionHostError("creation_rejected")
            candidate = _new_checkpoint(aggregate, request_digest, projected)
            self._stage_insert(transaction, candidate)
            receipt = copy.deepcopy(candidate["operation_receipts"][0])
        self._after_commit()
        return {"result": "committed", "receipt": receipt}

    def _delivery_candidate(
        self, candidate: Any
    ) -> tuple[str | None, str | None, Any, dict[str, Any] | None, str | None]:
        if not isinstance(candidate, Mapping):
            return None, None, None, None, None
        allowed = {
            "root_instance_id",
            "delivery_mode",
            "origin",
            "envelope",
            "envelope_digest",
        }
        required = {"root_instance_id", "delivery_mode", "origin", "envelope"}
        if not required.issubset(candidate) or not set(candidate).issubset(allowed):
            return None, None, None, None, None
        root_instance_id = candidate["root_instance_id"]
        mode = candidate["delivery_mode"]
        origin = candidate["origin"]
        envelope = candidate["envelope"]
        supplied_digest = candidate.get("envelope_digest")
        if (
            not isinstance(root_instance_id, str)
            or not root_instance_id
            or not isinstance(mode, str)
            or not isinstance(envelope, dict)
            or not validate_execution_checkpoint_member("envelope", envelope)
            or (supplied_digest is not None and not isinstance(supplied_digest, str))
        ):
            return None, None, None, None, None
        return (
            root_instance_id,
            mode,
            copy.deepcopy(origin),
            copy.deepcopy(envelope),
            supplied_digest,
        )

    def _not_accepted(self, code: str) -> dict[str, Any]:
        return {"result": "not_accepted", "failure": {"code": code}}

    def _delivery_replay(
        self,
        checkpoint: Mapping[str, Any],
        event_id: str,
        digest: str,
    ) -> dict[str, Any] | None:
        for pending in checkpoint["pending_deliveries"]:
            if pending["envelope"]["event_id"] == event_id:
                if pending["envelope_digest"] != digest:
                    return self._not_accepted("event_id_conflict")
                return {
                    "result": "pending",
                    "event_id": event_id,
                    "delivery_sequence": pending["delivery_sequence"],
                    "accepted_revision": pending["accepted_revision"],
                }
        for receipt in checkpoint["operation_receipts"]:
            if receipt["operation_kind"] == "delivery" and receipt["event_id"] == event_id:
                if receipt["request_digest"] != digest:
                    return self._not_accepted("event_id_conflict")
                return {"result": "committed", "receipt": copy.deepcopy(receipt)}
        return None

    def _prepare_acceptance(
        self,
        checkpoint: Mapping[str, Any],
        candidate: Any,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        parsed = self._delivery_candidate(candidate)
        root_instance_id, mode, origin, envelope, supplied_digest = parsed
        if root_instance_id is None or mode is None or envelope is None:
            return None, self._not_accepted("malformed_delivery")
        if root_instance_id != checkpoint["root_instance_id"]:
            return None, self._not_accepted("wrong_root")

        digest = delivery_request_digest(root_instance_id, mode, envelope)
        replay = self._delivery_replay(
            checkpoint, envelope["event_id"], digest
        )
        if replay is not None:
            return None, replay
        if checkpoint["root_record"]["status"] == "tombstone":
            return None, self._not_accepted("tombstoned_root")

        valid_mode = mode in {"input", "internal"}
        valid_origin = validate_execution_checkpoint_member("deliveryOrigin", origin)
        valid_pair = (
            mode == "input" and origin == {"kind": "host_input"}
        ) or (
            mode == "internal"
            and isinstance(origin, Mapping)
            and origin.get("kind") == "internal_emission"
        )
        if not valid_mode:
            return None, self._not_accepted("invalid_delivery_mode")
        if not valid_origin or not valid_pair:
            return None, self._not_accepted("invalid_delivery_origin")
        if supplied_digest is not None and supplied_digest != digest:
            return None, self._not_accepted("delivery_digest_mismatch")
        if (
            _target_root_instance_id(envelope["target"])
            != checkpoint["root_instance_id"]
        ):
            return None, self._not_accepted("wrong_root")
        return {
            "root_instance_id": root_instance_id,
            "delivery_mode": mode,
            "origin": origin,
            "envelope": envelope,
            "envelope_digest": digest,
        }, None

    def accept_delivery(
        self,
        root_instance_id: str,
        candidate: Any,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                return self._not_accepted("wrong_root")
            checkpoint = self._restore(source, root_instance_id).document
            prepared, result = self._prepare_acceptance(checkpoint, candidate)
            if result is not None:
                return result
            assert prepared is not None
            self._check_expected(
                checkpoint, expected_revision, expected_checkpoint_digest
            )
            next_checkpoint = _mutate(checkpoint)
            sequence = next_checkpoint["next_delivery_sequence"]
            next_checkpoint["next_delivery_sequence"] = _increment_checkpoint_number(
                sequence
            )
            pending = {
                "delivery_sequence": sequence,
                "accepted_revision": next_checkpoint["revision"],
                "delivery_mode": prepared["delivery_mode"],
                "origin": prepared["origin"],
                "envelope": prepared["envelope"],
                "envelope_digest": prepared["envelope_digest"],
            }
            next_checkpoint["pending_deliveries"].append(pending)
            next_checkpoint = seal_execution_checkpoint(next_checkpoint)
            self._stage_replace(transaction, checkpoint, next_checkpoint)
            response = {
                "result": "pending",
                "event_id": pending["envelope"]["event_id"],
                "delivery_sequence": sequence,
                "accepted_revision": pending["accepted_revision"],
            }
        self._after_commit()
        return response

    def _commit_delivery(
        self,
        checkpoint: Mapping[str, Any],
        restored: RestoredExecutionCheckpoint,
        request: dict[str, Any],
        projected: Mapping[str, Any],
        *,
        foreground: bool,
        pending: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        candidate = _mutate(checkpoint)
        if foreground:
            delivery_sequence = candidate["next_delivery_sequence"]
            candidate["next_delivery_sequence"] = _increment_checkpoint_number(
                delivery_sequence
            )
            accepted_revision = candidate["revision"]
        else:
            assert pending is not None
            candidate["pending_deliveries"] = [
                item
                for item in candidate["pending_deliveries"]
                if item["delivery_sequence"] != pending["delivery_sequence"]
            ]
            delivery_sequence = pending["delivery_sequence"]
            accepted_revision = pending["accepted_revision"]
            request = {
                "delivery_mode": pending["delivery_mode"],
                "origin": copy.deepcopy(pending["origin"]),
                "envelope": copy.deepcopy(pending["envelope"]),
                "envelope_digest": pending["envelope_digest"],
            }
        receipt_sequence = candidate["next_operation_receipt_sequence"]
        candidate["next_operation_receipt_sequence"] = (
            _increment_checkpoint_number(receipt_sequence)
        )
        aggregate = copy.deepcopy(projected["aggregate_state"])
        if aggregate is None or restored.aggregate is None:
            raise ExecutionHostError("invalid_execution_checkpoint")
        candidate["root_record"]["aggregate_state"] = aggregate
        receipt = {
            "operation_kind": "delivery",
            "receipt_sequence": receipt_sequence,
            "event_id": request["envelope"]["event_id"],
            "request_digest": request["envelope_digest"],
            "accepted_delivery_sequence": delivery_sequence,
            "accepted_revision": accepted_revision,
            "delivery_mode": request["delivery_mode"],
            "origin": copy.deepcopy(request["origin"]),
            "committed_revision": candidate["revision"],
            "resulting_aggregate_state_digest": aggregate[
                "aggregate_state_digest"
            ],
            "outcome": {
                "status": projected["status"],
                "disposition": projected["disposition"],
                "fault": copy.deepcopy(projected["fault"]),
                "rejection": copy.deepcopy(projected["rejection"]),
            },
            "emission_references": [],
        }
        candidate["operation_receipts"].append(receipt)
        _append_emissions(candidate, receipt, projected)
        return seal_execution_checkpoint(candidate), receipt

    def process_pending_delivery(
        self,
        root_instance_id: str,
        candidate: Any,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError("wrong_root")
            restored = self._restore(source, root_instance_id)
            checkpoint = restored.document
            parsed = self._delivery_candidate(candidate)
            candidate_root, mode, origin, envelope, supplied_digest = parsed
            if (
                candidate_root != root_instance_id
                or mode not in {"input", "internal"}
                or origin is None
                or envelope is None
            ):
                raise ExecutionHostError("malformed_delivery")
            digest = delivery_request_digest(root_instance_id, mode, envelope)
            if supplied_digest is not None and supplied_digest != digest:
                raise ExecutionHostError("delivery_digest_mismatch")
            replay = self._delivery_replay(
                checkpoint, envelope["event_id"], digest
            )
            if replay is not None and replay["result"] == "committed":
                return replay
            if replay is not None and replay["result"] == "not_accepted":
                raise ExecutionHostError(replay["failure"]["code"])
            pending = next(
                (
                    item
                    for item in checkpoint["pending_deliveries"]
                    if item["envelope"]["event_id"] == envelope["event_id"]
                ),
                None,
            )
            if pending is None or pending["envelope_digest"] != digest:
                raise ExecutionHostError("event_id_conflict")
            self._check_expected(
                checkpoint, expected_revision, expected_checkpoint_digest
            )
            if restored.aggregate is None:
                raise ExecutionHostError("tombstoned_root")
            result = core_dispatch(
                restored.aggregate.bundle,
                restored.aggregate.state,
                _delivery_from_wire(
                    pending["delivery_mode"], pending["envelope"]
                ),
            )
            projected = _project_core_result(restored.aggregate.bundle, result)
            next_checkpoint, receipt = self._commit_delivery(
                checkpoint,
                restored,
                {},
                projected,
                foreground=False,
                pending=pending,
            )
            self._stage_replace(transaction, checkpoint, next_checkpoint)
            response = {"result": "committed", "receipt": copy.deepcopy(receipt)}
        self._after_commit()
        return response

    def foreground_process_delivery(
        self,
        root_instance_id: str,
        candidate: Any,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError("wrong_root")
            restored = self._restore(source, root_instance_id)
            checkpoint = restored.document
            prepared, replay = self._prepare_acceptance(checkpoint, candidate)
            if replay is not None:
                return replay
            assert prepared is not None
            self._check_expected(
                checkpoint, expected_revision, expected_checkpoint_digest
            )
            if restored.aggregate is None:
                raise ExecutionHostError("tombstoned_root")
            result = core_dispatch(
                restored.aggregate.bundle,
                restored.aggregate.state,
                _delivery_from_wire(
                    prepared["delivery_mode"], prepared["envelope"]
                ),
            )
            projected = _project_core_result(restored.aggregate.bundle, result)
            next_checkpoint, receipt = self._commit_delivery(
                checkpoint,
                restored,
                prepared,
                projected,
                foreground=True,
            )
            self._stage_replace(transaction, checkpoint, next_checkpoint)
            response = {"result": "committed", "receipt": copy.deepcopy(receipt)}
        self._after_commit()
        return response

    def maintenance_migration(
        self,
        root_instance_id: str,
        operation_id: str,
        target_validated_bundle_fingerprint: str,
        migration_descriptor_digest_route: Sequence[str],
        *,
        source_aggregate_state_digest: str,
        expected_revision: str,
        expected_checkpoint_digest: str,
        maintenance_mode: bool = True,
        limits: MigrationLimits | None = None,
    ) -> dict[str, Any]:
        if (
            not operation_id
            or not validate_execution_checkpoint_member(
                "sha256", source_aggregate_state_digest
            )
        ):
            raise ExecutionHostError("invalid_migration_request")
        request_digest = maintenance_migration_request_digest(
            root_instance_id,
            operation_id,
            source_aggregate_state_digest,
            target_validated_bundle_fingerprint,
            migration_descriptor_digest_route,
            maintenance_mode,
        )
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError("wrong_root")
            restored = self._restore(source, root_instance_id)
            checkpoint = restored.document
            for receipt in checkpoint["operation_receipts"]:
                if (
                    receipt["operation_kind"] == "maintenance_migration"
                    and receipt["operation_id"] == operation_id
                ):
                    if receipt["request_digest"] == request_digest:
                        return {
                            "result": "committed",
                            "receipt": copy.deepcopy(receipt),
                        }
                    raise ExecutionHostError("operation_id_conflict")
            if restored.aggregate is None:
                raise ExecutionHostError("tombstoned_root")
            current_source_digest = restored.aggregate.aggregate_envelope[
                "aggregate_state_digest"
            ]
            if source_aggregate_state_digest != current_source_digest:
                raise ExecutionHostError("invalid_migration_request")
            self._check_expected(
                checkpoint, expected_revision, expected_checkpoint_digest
            )
            result = migrate_aggregate(
                restored.aggregate.aggregate_envelope,
                target_validated_bundle_fingerprint,
                migration_descriptor_digest_route,
                self.artifact_resolver,
                maintenance_mode=maintenance_mode,
                resource_limits=limits,
            )
            if result.failure is not None or result.aggregate_envelope is None:
                code = (
                    "migration_failed"
                    if result.failure is None
                    else result.failure.code
                )
                raise ExecutionHostError(code)
            candidate = _mutate(checkpoint)
            receipt_sequence = candidate["next_operation_receipt_sequence"]
            candidate["next_operation_receipt_sequence"] = (
                _increment_checkpoint_number(receipt_sequence)
            )
            migration_sequences = [
                item["migration_sequence"] for item in result.audit_records
            ]
            receipt = {
                "operation_kind": "maintenance_migration",
                "receipt_sequence": receipt_sequence,
                "operation_id": operation_id,
                "request_digest": request_digest,
                "committed_revision": candidate["revision"],
                "source_aggregate_state_digest": source_aggregate_state_digest,
                "resulting_aggregate_state_digest": result.aggregate_envelope[
                    "aggregate_state_digest"
                ],
                "migration_sequences": migration_sequences,
                "result_code": (
                    "migration_applied"
                    if migration_sequences
                    else "migration_no_operation"
                ),
            }
            candidate["root_record"]["aggregate_state"] = copy.deepcopy(
                result.aggregate_envelope
            )
            candidate["operation_receipts"].append(receipt)
            candidate["migration_audit_records"].extend(
                copy.deepcopy(result.audit_records)
            )
            candidate = seal_execution_checkpoint(candidate)
            self._stage_replace(transaction, checkpoint, candidate)
            response = {"result": "committed", "receipt": copy.deepcopy(receipt)}
        self._after_commit()
        return response

    def update_pending_outbox(
        self,
        root_instance_id: str,
        effect_id: str,
        desired_pending_state: Mapping[str, Any],
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        desired = copy.deepcopy(dict(desired_pending_state))
        if not validate_execution_checkpoint_member("pendingOutboxState", desired):
            raise ExecutionHostError("invalid_execution_checkpoint")
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError("wrong_root")
            checkpoint = self._restore(source, root_instance_id).document
            item = next(
                (
                    value
                    for value in checkpoint["pending_outbox_intents"]
                    if value["intent"]["effect_id"] == effect_id
                ),
                None,
            )
            if item is None:
                raise ExecutionHostError("effect_id_conflict")
            if item["delivery_state"] == desired:
                return {"result": "committed", "record": copy.deepcopy(item)}
            self._check_expected(
                checkpoint, expected_revision, expected_checkpoint_digest
            )
            candidate = _mutate(checkpoint)
            candidate_item = next(
                value
                for value in candidate["pending_outbox_intents"]
                if value["intent"]["effect_id"] == effect_id
            )
            candidate_item["delivery_state"] = desired
            candidate_item["state_revision"] = candidate["revision"]
            candidate = seal_execution_checkpoint(candidate)
            self._stage_replace(transaction, checkpoint, candidate)
            record = copy.deepcopy(candidate_item)
        self._after_commit()
        return {"result": "committed", "record": record}

    def terminalize_outbox(
        self,
        root_instance_id: str,
        effect_id: str,
        terminal_outcome: Mapping[str, Any],
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        outcome = copy.deepcopy(dict(terminal_outcome))
        if not validate_execution_checkpoint_member("terminalOutboxOutcome", outcome):
            raise ExecutionHostError("invalid_execution_checkpoint")
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError("wrong_root")
            checkpoint = self._restore(source, root_instance_id).document
            for record in checkpoint["terminal_outbox_records"]:
                if record["intent"]["effect_id"] == effect_id:
                    if record["outcome"] == outcome:
                        return {
                            "result": "committed",
                            "record": copy.deepcopy(record),
                        }
                    raise ExecutionHostError("effect_id_conflict")
            for record in checkpoint["outbox_effect_tombstones"]:
                if record["effect_id"] == effect_id:
                    if record["outcome"] == outcome:
                        return {
                            "result": "committed",
                            "record": copy.deepcopy(record),
                        }
                    raise ExecutionHostError("effect_id_conflict")
            pending = next(
                (
                    value
                    for value in checkpoint["pending_outbox_intents"]
                    if value["intent"]["effect_id"] == effect_id
                ),
                None,
            )
            if pending is None:
                raise ExecutionHostError("effect_id_conflict")
            self._check_expected(
                checkpoint, expected_revision, expected_checkpoint_digest
            )
            candidate = _mutate(checkpoint)
            candidate_pending = next(
                value
                for value in candidate["pending_outbox_intents"]
                if value["intent"]["effect_id"] == effect_id
            )
            candidate["pending_outbox_intents"].remove(candidate_pending)
            terminal_sequence = candidate["next_outbox_terminal_sequence"]
            candidate["next_outbox_terminal_sequence"] = (
                _increment_checkpoint_number(terminal_sequence)
            )
            record = {
                "terminal_sequence": terminal_sequence,
                "intent": candidate_pending["intent"],
                "committed_revision": candidate["revision"],
                "outcome": outcome,
            }
            candidate["terminal_outbox_records"].append(record)
            candidate = seal_execution_checkpoint(candidate)
            self._stage_replace(transaction, checkpoint, candidate)
            response = {"result": "committed", "record": copy.deepcopy(record)}
        self._after_commit()
        return response

    def compact_outbox(
        self,
        root_instance_id: str,
        effect_id: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        if PERMANENT_OUTBOX_TERMINAL_RETENTION in self.store.capabilities:
            raise ExecutionHostError("adapter_capability_mismatch")
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError("wrong_root")
            checkpoint = self._restore(source, root_instance_id).document
            existing = next(
                (
                    record
                    for record in checkpoint["outbox_effect_tombstones"]
                    if record["effect_id"] == effect_id
                ),
                None,
            )
            if existing is not None:
                return {"result": "committed", "record": copy.deepcopy(existing)}
            terminal = next(
                (
                    record
                    for record in checkpoint["terminal_outbox_records"]
                    if record["intent"]["effect_id"] == effect_id
                ),
                None,
            )
            if terminal is None:
                raise ExecutionHostError("effect_id_conflict")
            self._check_expected(
                checkpoint, expected_revision, expected_checkpoint_digest
            )
            candidate = _mutate(checkpoint)
            candidate_terminal = next(
                record
                for record in candidate["terminal_outbox_records"]
                if record["intent"]["effect_id"] == effect_id
            )
            candidate["terminal_outbox_records"].remove(candidate_terminal)
            tombstone = {
                "terminal_sequence": candidate_terminal["terminal_sequence"],
                "effect_id": effect_id,
                "intent_digest": outbox_intent_digest(
                    root_instance_id, candidate_terminal["intent"]
                ),
                "committed_revision": candidate_terminal["committed_revision"],
                "outcome": candidate_terminal["outcome"],
            }
            candidate["outbox_effect_tombstones"].append(tombstone)
            candidate["outbox_effect_tombstones"].sort(
                key=lambda item: _checkpoint_number(item["terminal_sequence"])
            )
            candidate = seal_execution_checkpoint(candidate)
            self._stage_replace(transaction, checkpoint, candidate)
            response = {"result": "committed", "record": copy.deepcopy(tombstone)}
        self._after_commit()
        return response

    def delete_outbox_record(
        self,
        root_instance_id: str,
        effect_id: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        if {
            PERMANENT_OUTBOX_TERMINAL_RETENTION,
            COMPACT_EFFECT_IDENTITY_RETENTION,
        }.intersection(self.store.capabilities):
            raise ExecutionHostError("adapter_capability_mismatch")
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError("wrong_root")
            checkpoint = self._restore(source, root_instance_id).document
            if any(
                emission.get("kind") == "external_outbox"
                and emission.get("effect_id") == effect_id
                for receipt in checkpoint["operation_receipts"]
                for emission in receipt.get("emission_references", [])
            ):
                raise ExecutionHostError("invalid_execution_checkpoint")
            self._check_expected(
                checkpoint, expected_revision, expected_checkpoint_digest
            )
            candidate = _mutate(checkpoint)
            prior_count = len(candidate["terminal_outbox_records"]) + len(
                candidate["outbox_effect_tombstones"]
            )
            candidate["terminal_outbox_records"] = [
                item
                for item in candidate["terminal_outbox_records"]
                if item["intent"]["effect_id"] != effect_id
            ]
            candidate["outbox_effect_tombstones"] = [
                item
                for item in candidate["outbox_effect_tombstones"]
                if item["effect_id"] != effect_id
            ]
            if prior_count == len(candidate["terminal_outbox_records"]) + len(
                candidate["outbox_effect_tombstones"]
            ):
                raise ExecutionHostError("effect_id_conflict")
            candidate = seal_execution_checkpoint(candidate)
            self._stage_replace(transaction, checkpoint, candidate)
        self._after_commit()
        return {"result": "committed"}

    def update_replay_retention(
        self,
        root_instance_id: str,
        target_replay_retention: Mapping[str, Any],
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        target = copy.deepcopy(dict(target_replay_retention))
        if not validate_execution_checkpoint_member("replayRetention", target):
            raise ExecutionHostError("invalid_execution_checkpoint")
        if (
            PERMANENT_RECEIPT_RETENTION in self.store.capabilities
            and target["mode"] != "permanent"
        ):
            raise ExecutionHostError("adapter_capability_mismatch")
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError("wrong_root")
            checkpoint = self._restore(source, root_instance_id).document
            current = checkpoint["replay_retention"]
            if current == target:
                return {"result": "committed", "replay_retention": copy.deepcopy(current)}
            if current["mode"] == "bounded" and target["mode"] == "permanent":
                raise ExecutionHostError("invalid_execution_checkpoint")
            current_cutoff = current["pruned_through_receipt_sequence"]
            target_cutoff = target["pruned_through_receipt_sequence"]
            if target["mode"] == "bounded":
                if (
                    current["mode"] == "bounded"
                    and current["policy_identifier"]
                    != target["policy_identifier"]
                ):
                    raise ExecutionHostError("invalid_execution_checkpoint")
                if (
                    current_cutoff is not None
                    and (
                        target_cutoff is None
                        or _checkpoint_number(target_cutoff)
                        < _checkpoint_number(current_cutoff)
                    )
                ):
                    raise ExecutionHostError("invalid_execution_checkpoint")
                if (
                    target_cutoff is not None
                    and _checkpoint_number(target_cutoff)
                    >= _checkpoint_number(
                        checkpoint["next_operation_receipt_sequence"]
                    )
                ):
                    raise ExecutionHostError("invalid_execution_checkpoint")
            self._check_expected(
                checkpoint, expected_revision, expected_checkpoint_digest
            )
            candidate = _mutate(checkpoint)
            candidate["replay_retention"] = target
            if target_cutoff is not None:
                cutoff = _checkpoint_number(target_cutoff)
                candidate["operation_receipts"] = [
                    receipt
                    for receipt in candidate["operation_receipts"]
                    if receipt["receipt_sequence"] == "0"
                    or _checkpoint_number(receipt["receipt_sequence"]) > cutoff
                ]
                referenced_migrations = {
                    sequence
                    for receipt in candidate["operation_receipts"]
                    if receipt["operation_kind"] == "maintenance_migration"
                    for sequence in receipt["migration_sequences"]
                }
                candidate["migration_audit_records"] = [
                    item
                    for item in candidate["migration_audit_records"]
                    if item["migration_sequence"] in referenced_migrations
                ]
                referenced_effects = {
                    emission["effect_id"]
                    for receipt in candidate["operation_receipts"]
                    for emission in receipt.get("emission_references", [])
                    if emission["kind"] == "external_outbox"
                }
                candidate["terminal_outbox_records"] = [
                    item
                    for item in candidate["terminal_outbox_records"]
                    if item["intent"]["effect_id"] in referenced_effects
                ]
                candidate["outbox_effect_tombstones"] = [
                    item
                    for item in candidate["outbox_effect_tombstones"]
                    if item["effect_id"] in referenced_effects
                ]
            candidate = seal_execution_checkpoint(candidate)
            try:
                self._stage_replace(transaction, checkpoint, candidate)
            except Exception as exc:
                if getattr(exc, "code", None) == "invalid_execution_checkpoint":
                    raise ExecutionHostError("invalid_execution_checkpoint") from exc
                raise
            response = {
                "result": "committed",
                "replay_retention": copy.deepcopy(target),
            }
        self._after_commit()
        return response

    def tombstone_root(
        self,
        root_instance_id: str,
        operation_id: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        if not operation_id:
            raise ExecutionHostError("invalid_execution_checkpoint")
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError("wrong_root")
            restored = self._restore(source, root_instance_id)
            checkpoint = restored.document
            root_record = checkpoint["root_record"]
            if root_record["status"] == "tombstone":
                if root_record["tombstone_operation_id"] == operation_id:
                    return {
                        "result": "tombstoned",
                        "tombstone": copy.deepcopy(root_record),
                    }
                raise ExecutionHostError("operation_id_conflict")
            self._check_expected(
                checkpoint, expected_revision, expected_checkpoint_digest
            )
            if restored.aggregate is None:
                raise ExecutionHostError("invalid_execution_checkpoint")
            root_runtime = restored.aggregate.state["runtimes"][
                restored.aggregate.state["root_runtime_id"]
            ]
            if (
                root_runtime["status"] not in {"completed", "faulted"}
                or checkpoint["pending_deliveries"]
                or checkpoint["pending_outbox_intents"]
            ):
                raise ExecutionHostError("invalid_execution_checkpoint")
            aggregate = root_record["aggregate_state"]
            candidate = _mutate(checkpoint)
            tombstone = {
                "status": "tombstone",
                "root_runtime_id": aggregate["root_runtime_id"],
                "creation_id": aggregate["creation_id"],
                "terminal_status": root_runtime["status"],
                "final_aggregate_state_digest": aggregate[
                    "aggregate_state_digest"
                ],
                "tombstone_operation_id": operation_id,
            }
            candidate["root_record"] = tombstone
            candidate = seal_execution_checkpoint(candidate)
            self._stage_replace(transaction, checkpoint, candidate)
            response = {
                "result": "tombstoned",
                "tombstone": copy.deepcopy(tombstone),
            }
        self._after_commit()
        return response

    def delete_checkpoint(
        self,
        root_instance_id: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        del root_instance_id, expected_revision, expected_checkpoint_digest
        return {
            "result": "unsupported",
            "failure": {"code": "physical_deletion_unsupported"},
        }


class SharedExecutionTransaction:
    """Root-bound staging surface for one host-owned shared transaction."""

    def __init__(self, host: ExecutionHost, root_instance_id: str) -> None:
        self._host = host
        self.root_instance_id = root_instance_id
        self._active = True
        self._response: dict[str, Any] | None = None

    def _stage(
        self,
        operation: str,
        invoke: Callable[[], dict[str, Any]],
    ) -> StagedExecutionResult:
        if not self._active:
            raise ExecutionHostError("shared_transaction_closed")
        if self._response is not None:
            raise ExecutionHostError("shared_transaction_operation_conflict")
        self._response = invoke()
        return StagedExecutionResult(operation)

    def _finish(self) -> dict[str, Any]:
        if self._response is None:
            raise ExecutionHostError("shared_transaction_operation_required")
        if self._response["result"] not in {
            "committed",
            "pending",
            "tombstoned",
        }:
            failure = self._response.get("failure", {})
            raise ExecutionHostError(
                failure.get("code", "shared_transaction_operation_failed")
            )
        return self._response

    def _deactivate(self) -> None:
        self._active = False

    def create(
        self,
        bundle: Bundle | BundleSource,
        machine_id: str,
        creation_id: str,
        bindings: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> StagedExecutionResult:
        return self._stage(
            "create",
            lambda: self._host.create(
                bundle,
                machine_id,
                self.root_instance_id,
                creation_id,
                bindings,
            ),
        )

    def accept_delivery(
        self,
        candidate: Any,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "accept_delivery",
            lambda: self._host.accept_delivery(
                self.root_instance_id,
                candidate,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )

    def process_pending_delivery(
        self,
        candidate: Any,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "process_pending_delivery",
            lambda: self._host.process_pending_delivery(
                self.root_instance_id,
                candidate,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )

    def foreground_process_delivery(
        self,
        candidate: Any,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "foreground_process_delivery",
            lambda: self._host.foreground_process_delivery(
                self.root_instance_id,
                candidate,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )

    def maintenance_migration(
        self,
        operation_id: str,
        target_validated_bundle_fingerprint: str,
        migration_descriptor_digest_route: Sequence[str],
        *,
        source_aggregate_state_digest: str,
        expected_revision: str,
        expected_checkpoint_digest: str,
        maintenance_mode: bool = True,
        limits: MigrationLimits | None = None,
    ) -> StagedExecutionResult:
        return self._stage(
            "maintenance_migration",
            lambda: self._host.maintenance_migration(
                self.root_instance_id,
                operation_id,
                target_validated_bundle_fingerprint,
                migration_descriptor_digest_route,
                source_aggregate_state_digest=source_aggregate_state_digest,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
                maintenance_mode=maintenance_mode,
                limits=limits,
            ),
        )

    def update_pending_outbox(
        self,
        effect_id: str,
        desired_pending_state: Mapping[str, Any],
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "update_pending_outbox",
            lambda: self._host.update_pending_outbox(
                self.root_instance_id,
                effect_id,
                desired_pending_state,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )

    def terminalize_outbox(
        self,
        effect_id: str,
        terminal_outcome: Mapping[str, Any],
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "terminalize_outbox",
            lambda: self._host.terminalize_outbox(
                self.root_instance_id,
                effect_id,
                terminal_outcome,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )

    def compact_outbox(
        self,
        effect_id: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "compact_outbox",
            lambda: self._host.compact_outbox(
                self.root_instance_id,
                effect_id,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )

    def delete_outbox_record(
        self,
        effect_id: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "delete_outbox_record",
            lambda: self._host.delete_outbox_record(
                self.root_instance_id,
                effect_id,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )

    def update_replay_retention(
        self,
        target_replay_retention: Mapping[str, Any],
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "update_replay_retention",
            lambda: self._host.update_replay_retention(
                self.root_instance_id,
                target_replay_retention,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )

    def tombstone_root(
        self,
        operation_id: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "tombstone_root",
            lambda: self._host.tombstone_root(
                self.root_instance_id,
                operation_id,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )


def _target_root_instance_id(target: Mapping[str, Any]) -> str:
    member = next(iter(target.values()))
    return str(member["root_instance_id"])
