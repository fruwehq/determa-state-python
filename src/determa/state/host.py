"""Optional synchronous host for portable execution checkpoints."""

from __future__ import annotations

import copy
from collections.abc import Callable, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .checkpoint import (
    seal_execution_checkpoint,
    serialize_execution_checkpoint,
    validate_execution_checkpoint_member,
)
from .codes import (
    CheckpointHostFailureCode as HostCode,
)
from .codes import (
    CheckpointPreAcceptanceFailureCode as PreAcceptanceCode,
)
from .codes import (
    ExecutionStoreAdapterFailureCode as AdapterCode,
)
from .codes import (
    PersistenceFailureCode as PersistenceCode,
)
from .definition import Bundle, BundleSource, load_bundle
from .errors import DetermaError
from .migration import MigrationLimits
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
    decoded_typed_value,
    hash_value,
    typed_value,
)

if TYPE_CHECKING:
    from .checkpoint_v2 import RestoredExecutionCheckpoint

FaultInjector = Callable[[str], None]
_MAX_DECIMAL_DIGITS = 4096


class ExecutionHostError(DetermaError):
    """A closed host-layer failure."""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = str(code)
        self.message = message or self.code
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
        raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
    try:
        return int(value)
    except ValueError as exc:
        raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT) from exc


def _increment_checkpoint_number(value: Any) -> str:
    result = str(_checkpoint_number(value) + 1)
    if len(result) > _MAX_DECIMAL_DIGITS:
        raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
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
            "determa-creation-request-digest-2",
            "2",
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
            "determa-inbox-envelope-digest-2",
            "2",
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
        raise ExecutionHostError(PreAcceptanceCode.MALFORMED_DELIVERY)
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
            "determa-maintenance-migration-request-digest-2",
            "2",
            root_instance_id,
            operation_id,
            source_aggregate_state_digest,
            target_validated_bundle_fingerprint,
            list(migration_descriptor_digest_route),
            maintenance_mode,
        ]
    )


def outbox_intent_digest(root_instance_id: str, intent: Mapping[str, Any]) -> str:
    """Compute the compact evidence digest for one complete outbox intent."""
    return hash_value(
        [
            "determa-outbox-intent-digest-2",
            "2",
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
        valid = (
            common
            and atomic
            and {
                "acknowledge_after_checkpoint_commit",
                "durable_redelivery",
                "outbox_worker",
            }.issubset(host_features)
        )
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
        raise ExecutionHostError(AdapterCode.ADAPTER_CAPABILITY_MISMATCH)


@dataclass(frozen=True)
class StagedExecutionResult:
    """An operation staged inside a host-owned shared transaction."""

    operation: str
    state: str = "staged"


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
            raise ExecutionHostError(AdapterCode.ADAPTER_CAPABILITY_MISMATCH)
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
    ) -> Any:
        from .checkpoint_v2 import restore_execution_checkpoint_v2

        restored = restore_execution_checkpoint_v2(source, self.artifact_resolver)
        if restored.document["root_instance_id"] != root_instance_id:
            raise ExecutionHostError("transaction_root_mismatch")
        if (
            PERMANENT_RECEIPT_RETENTION in self.store.capabilities
            and restored.document["replay_retention"]["mode"] != "permanent"
        ):
            raise ExecutionHostError(AdapterCode.ADAPTER_CAPABILITY_MISMATCH)
        if (
            PERMANENT_OUTBOX_TERMINAL_RETENTION in self.store.capabilities
            and restored.document["outbox_effect_tombstones"]
        ):
            raise ExecutionHostError(AdapterCode.ADAPTER_CAPABILITY_MISMATCH)
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
            raise ExecutionHostError(AdapterCode.ADAPTER_CAPABILITY_MISMATCH)
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
            or checkpoint["execution_checkpoint_digest"] != expected_checkpoint_digest
        ):
            raise ExecutionHostError(HostCode.CHECKPOINT_REVISION_CONFLICT)

    def _stage_insert(
        self, transaction: ExecutionStoreTransaction, candidate: dict[str, Any]
    ) -> None:
        self._restore(
            serialize_execution_checkpoint(candidate), candidate["root_instance_id"]
        )
        self._fault("before_commit")
        if not transaction.insert(serialize_execution_checkpoint(candidate)):
            raise ExecutionHostError(HostCode.CHECKPOINT_REVISION_CONFLICT)

    def _stage_replace(
        self,
        transaction: ExecutionStoreTransaction,
        previous: Mapping[str, Any],
        candidate: dict[str, Any],
    ) -> None:
        self._restore(
            serialize_execution_checkpoint(candidate), candidate["root_instance_id"]
        )
        self._fault("before_commit")
        if not transaction.replace(
            previous["revision"],
            previous["execution_checkpoint_digest"],
            serialize_execution_checkpoint(candidate),
        ):
            raise ExecutionHostError(HostCode.CHECKPOINT_REVISION_CONFLICT)

    def read_checkpoint(
        self,
        root_instance_id: str,
    ) -> RestoredExecutionCheckpoint | None:
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            return None if source is None else self._restore(source, root_instance_id)

    def create_v2(
        self,
        bundle: Bundle | BundleSource,
        machine_id: str,
        root_instance_id: str,
        creation_id: str,
        bindings: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Create and transactionally retain one queue-bearing checkpoint."""
        from .checkpoint_v2 import create_checkpoint_v2

        normalized = {name: dict(value) for name, value in (bindings or {}).items()}
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is not None:
                restored = self._restore(source, root_instance_id)
                receipt = restored.document["operation_receipts"][0]
                request_digest = creation_request_digest(
                    bundle, machine_id, root_instance_id, creation_id, normalized
                )
                if (
                    receipt["creation_id"] == creation_id
                    and receipt["request_digest"] == request_digest
                ):
                    return {"result": "committed", "receipt": copy.deepcopy(receipt)}
                raise ExecutionHostError(HostCode.CREATION_ID_CONFLICT)
            candidate = create_checkpoint_v2(
                bundle, machine_id, root_instance_id, creation_id, normalized
            )
            self._stage_insert(transaction, candidate)
            receipt = copy.deepcopy(candidate["operation_receipts"][0])
        self._after_commit()
        return {"result": "committed", "receipt": receipt}

    @staticmethod
    def _v2_committed_checkpoint(result: Mapping[str, Any]) -> dict[str, Any] | None:
        if result.get("execution_checkpoint_schema_version") == 2:
            return copy.deepcopy(dict(result))
        checkpoint = result.get("checkpoint")
        if (
            isinstance(checkpoint, Mapping)
            and checkpoint.get("execution_checkpoint_schema_version") == 2
        ):
            return copy.deepcopy(dict(checkpoint))
        return None

    def admit_v2(
        self,
        root_instance_id: str,
        deliveries: Sequence[Mapping[str, Any]],
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        """Admit one v2 batch inside the store transaction."""
        from .checkpoint_v2 import admit_checkpoint_v2

        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError(PreAcceptanceCode.WRONG_ROOT)
            prior: dict[str, Any] = self._restore(source, root_instance_id).document
            result = admit_checkpoint_v2(
                prior,
                deliveries,
                self.artifact_resolver,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            )
            candidate = self._v2_committed_checkpoint(result)
            if (
                candidate is not None
                and candidate["execution_checkpoint_digest"]
                != prior["execution_checkpoint_digest"]
            ):
                self._stage_replace(transaction, prior, candidate)
        if (
            candidate is not None
            and candidate["execution_checkpoint_digest"]
            != prior["execution_checkpoint_digest"]
        ):
            self._after_commit()
        return result

    def process_ready_v2(
        self,
        root_instance_id: str,
        target_runtime_id: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        """Process one v2 ready head inside the store transaction."""
        from .checkpoint_v2 import step_checkpoint_v2

        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError(PreAcceptanceCode.WRONG_ROOT)
            prior = self._restore(source, root_instance_id).document
            result = step_checkpoint_v2(
                prior,
                target_runtime_id,
                self.artifact_resolver,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            )
            candidate = self._v2_committed_checkpoint(result)
            if (
                candidate is not None
                and candidate["execution_checkpoint_digest"]
                != prior["execution_checkpoint_digest"]
            ):
                self._stage_replace(transaction, prior, candidate)
        if (
            candidate is not None
            and candidate["execution_checkpoint_digest"]
            != prior["execution_checkpoint_digest"]
        ):
            self._after_commit()
        return result

    def prune_v2(
        self,
        root_instance_id: str,
        cutoff_receipt_sequence: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        """Atomically prune one dependency-closed bounded v2 checkpoint."""
        from .checkpoint_v2 import prune_checkpoint_v2

        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError(PreAcceptanceCode.WRONG_ROOT)
            prior = self._restore(source, root_instance_id).document
            candidate = prune_checkpoint_v2(
                prior,
                cutoff_receipt_sequence,
                self.artifact_resolver,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            )
            if (
                candidate["execution_checkpoint_digest"]
                != prior["execution_checkpoint_digest"]
            ):
                self._stage_replace(transaction, prior, candidate)
        if (
            candidate["execution_checkpoint_digest"]
            != prior["execution_checkpoint_digest"]
        ):
            self._after_commit()
        return candidate

    @staticmethod
    def _terminalize_v2_entries(
        checkpoint: dict[str, Any],
        entries: Sequence[Mapping[str, Any]],
        *,
        disposition: str,
        reason: str,
        resulting_digest: str,
        status: str,
        migration_descriptor_digest: str | None = None,
    ) -> None:
        from .checkpoint_v2 import _terminalize_mailbox_reference

        for entry in entries:
            receipt_sequence = checkpoint["next_operation_receipt_sequence"]
            checkpoint["next_operation_receipt_sequence"] = (
                _increment_checkpoint_number(receipt_sequence)
            )
            event_id = entry["envelope"]["event_id"]
            _terminalize_mailbox_reference(checkpoint, entry, receipt_sequence)
            outcome: dict[str, Any] = {
                "status": status,
                "disposition": disposition,
                "reason": reason,
                "fault": None,
                "rejection": None,
            }
            if migration_descriptor_digest is not None:
                outcome["migration_descriptor_digest"] = migration_descriptor_digest
            checkpoint["operation_receipts"].append(
                {
                    "operation_kind": "event_terminal",
                    "receipt_sequence": receipt_sequence,
                    "event_id": event_id,
                    "request_digest": entry["envelope_digest"],
                    "acceptance_sequence": entry["acceptance_sequence"],
                    "final_queue_sequence": entry["queue_sequence"],
                    "committed_revision": checkpoint["revision"],
                    "resulting_aggregate_state_digest": resulting_digest,
                    "outcome": outcome,
                    "emission_references": [],
                }
            )

    def maintenance_migration_v2(
        self,
        root_instance_id: str,
        operation_id: str,
        target_validated_bundle_fingerprint: str,
        migration_descriptor_digest_route: Sequence[str],
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
        maintenance_mode: bool = True,
        limits: MigrationLimits | None = None,
    ) -> dict[str, Any]:
        """Commit one keyed v2 aggregate migration and its durable receipt."""
        from .checkpoint_v2 import _synchronize_mailbox_references
        from .queueing import migrate_aggregate_v2

        if not operation_id:
            raise ExecutionHostError(PersistenceCode.INVALID_MIGRATION_REQUEST)
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError(PreAcceptanceCode.WRONG_ROOT)
            prior = self._restore(source, root_instance_id).document
            for receipt in prior["operation_receipts"]:
                if (
                    receipt["operation_kind"] == "maintenance_migration"
                    and receipt["operation_id"] == operation_id
                ):
                    replay_digest = maintenance_migration_request_digest(
                        root_instance_id,
                        operation_id,
                        receipt["source_aggregate_state_digest"],
                        target_validated_bundle_fingerprint,
                        migration_descriptor_digest_route,
                        maintenance_mode,
                    )
                    if receipt["request_digest"] == replay_digest:
                        return {
                            "result": "committed",
                            "receipt": copy.deepcopy(receipt),
                        }
                    raise ExecutionHostError(HostCode.OPERATION_ID_CONFLICT)
            self._check_expected(prior, expected_revision, expected_checkpoint_digest)
            aggregate = prior["root_record"].get("aggregate_state")
            if aggregate is None:
                raise ExecutionHostError(PreAcceptanceCode.TOMBSTONED_ROOT)
            source_aggregate_state_digest = aggregate["aggregate_state_digest"]
            request_digest = maintenance_migration_request_digest(
                root_instance_id,
                operation_id,
                source_aggregate_state_digest,
                target_validated_bundle_fingerprint,
                migration_descriptor_digest_route,
                maintenance_mode,
            )
            migration = migrate_aggregate_v2(
                aggregate,
                target_validated_bundle_fingerprint,
                migration_descriptor_digest_route,
                self.artifact_resolver,
                maintenance_mode=maintenance_mode,
                resource_limits=limits,
                _include_host_evidence=True,
            )
            migrated = migration["aggregate_state"]
            candidate = _mutate(prior)
            candidate["root_record"]["aggregate_state"] = migrated
            candidate["migration_audit_records"].extend(
                copy.deepcopy(migration["audit_records"])
            )
            receipt_sequence = candidate["next_operation_receipt_sequence"]
            candidate["next_operation_receipt_sequence"] = (
                _increment_checkpoint_number(receipt_sequence)
            )
            migration_sequences = [
                item["migration_sequence"] for item in migration["audit_records"]
            ]
            receipt = {
                "operation_kind": "maintenance_migration",
                "receipt_sequence": receipt_sequence,
                "operation_id": operation_id,
                "request_digest": request_digest,
                "committed_revision": candidate["revision"],
                "source_aggregate_state_digest": source_aggregate_state_digest,
                "target_validated_bundle_fingerprint": target_validated_bundle_fingerprint,
                "resulting_aggregate_state_digest": migrated[
                    "aggregate_state_digest"
                ],
                "migration_sequences": migration_sequences,
                "result_code": (
                    "migration_applied"
                    if migration_sequences
                    else "migration_no_operation"
                ),
            }
            candidate["operation_receipts"].append(receipt)
            for entry, disposition in zip(
                migration["_disposed_entries"],
                migration["dispositions"],
                strict=True,
            ):
                self._terminalize_v2_entries(
                    candidate,
                    [entry],
                    disposition="migration_disposed",
                    reason=disposition["reason"],
                    resulting_digest=migrated["aggregate_state_digest"],
                    status=next(
                        runtime["status"]
                        for runtime in migrated["runtimes"]
                        if runtime["runtime_id"] == migrated["root_runtime_id"]
                    ),
                    migration_descriptor_digest=disposition[
                        "migration_descriptor_digest"
                    ],
                )
            _synchronize_mailbox_references(candidate)
            candidate = seal_execution_checkpoint(candidate)
            self._stage_replace(transaction, prior, candidate)
        self._after_commit()
        return {"result": "committed", "receipt": copy.deepcopy(receipt)}

    def tombstone_root_v2(
        self,
        root_instance_id: str,
        operation_id: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> dict[str, Any]:
        """Tombstone one v2 root and terminalize all engine-owned work."""
        if not operation_id:
            raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError(PreAcceptanceCode.WRONG_ROOT)
            prior: dict[str, Any] = self._restore(source, root_instance_id).document
            root_record = prior["root_record"]
            if root_record["status"] == "tombstone":
                if root_record["tombstone_operation_id"] == operation_id:
                    return prior
                raise ExecutionHostError(HostCode.OPERATION_ID_CONFLICT)
            self._check_expected(prior, expected_revision, expected_checkpoint_digest)
            aggregate = root_record["aggregate_state"]
            root_runtime = next(
                runtime
                for runtime in aggregate["runtimes"]
                if runtime["runtime_id"] == aggregate["root_runtime_id"]
            )
            if (
                root_runtime["status"] not in {"completed", "faulted"}
                or prior["pending_outbox_intents"]
            ):
                raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
            from .queueing import _lifecycle_runtime_order, restore_aggregate_v2

            restored_aggregate = restore_aggregate_v2(aggregate, self.artifact_resolver)
            runtime_by_id = {
                runtime["runtime_id"]: runtime for runtime in aggregate["runtimes"]
            }
            entries = [
                entry
                for runtime_id in _lifecycle_runtime_order(
                    restored_aggregate.bundle, restored_aggregate.state
                )
                for mailbox in ("ready_mailbox", "deferred_mailbox")
                for entry in runtime_by_id[runtime_id][mailbox]
            ]
            candidate = _mutate(prior)
            self._terminalize_v2_entries(
                candidate,
                entries,
                disposition="disposed",
                reason="root_tombstoned",
                resulting_digest=aggregate["aggregate_state_digest"],
                status=root_runtime["status"],
            )
            candidate["root_record"] = {
                "status": "tombstone",
                "root_runtime_id": aggregate["root_runtime_id"],
                "creation_id": aggregate["creation_id"],
                "terminal_status": root_runtime["status"],
                "final_aggregate_state_digest": aggregate["aggregate_state_digest"],
                "tombstone_operation_id": operation_id,
            }
            candidate = seal_execution_checkpoint(candidate)
            self._stage_replace(transaction, prior, candidate)
        self._after_commit()
        return candidate

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
            raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError(PreAcceptanceCode.WRONG_ROOT)
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
                raise ExecutionHostError(HostCode.EFFECT_ID_CONFLICT)
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
            raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError(PreAcceptanceCode.WRONG_ROOT)
            checkpoint = self._restore(source, root_instance_id).document
            for record in checkpoint["terminal_outbox_records"]:
                if record["intent"]["effect_id"] == effect_id:
                    if record["outcome"] == outcome:
                        return {
                            "result": "committed",
                            "record": copy.deepcopy(record),
                        }
                    raise ExecutionHostError(HostCode.EFFECT_ID_CONFLICT)
            for record in checkpoint["outbox_effect_tombstones"]:
                if record["effect_id"] == effect_id:
                    if record["outcome"] == outcome:
                        return {
                            "result": "committed",
                            "record": copy.deepcopy(record),
                        }
                    raise ExecutionHostError(HostCode.EFFECT_ID_CONFLICT)
            pending = next(
                (
                    value
                    for value in checkpoint["pending_outbox_intents"]
                    if value["intent"]["effect_id"] == effect_id
                ),
                None,
            )
            if pending is None:
                raise ExecutionHostError(HostCode.EFFECT_ID_CONFLICT)
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
            candidate["next_outbox_terminal_sequence"] = _increment_checkpoint_number(
                terminal_sequence
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
            raise ExecutionHostError(AdapterCode.ADAPTER_CAPABILITY_MISMATCH)
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError(PreAcceptanceCode.WRONG_ROOT)
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
                raise ExecutionHostError(HostCode.EFFECT_ID_CONFLICT)
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
            raise ExecutionHostError(AdapterCode.ADAPTER_CAPABILITY_MISMATCH)
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError(PreAcceptanceCode.WRONG_ROOT)
            checkpoint = self._restore(source, root_instance_id).document
            if any(
                emission.get("kind") == "external_outbox"
                and emission.get("effect_id") == effect_id
                for receipt in checkpoint["operation_receipts"]
                for emission in receipt.get("emission_references", [])
            ):
                raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
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
                raise ExecutionHostError(HostCode.EFFECT_ID_CONFLICT)
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
            raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
        if (
            PERMANENT_RECEIPT_RETENTION in self.store.capabilities
            and target["mode"] != "permanent"
        ):
            raise ExecutionHostError(AdapterCode.ADAPTER_CAPABILITY_MISMATCH)
        with self._transaction(root_instance_id) as transaction:
            source = transaction.load()
            if source is None:
                raise ExecutionHostError(PreAcceptanceCode.WRONG_ROOT)
            checkpoint = self._restore(source, root_instance_id).document
            current = checkpoint["replay_retention"]
            if current == target:
                return {
                    "result": "committed",
                    "replay_retention": copy.deepcopy(current),
                }
            if current["mode"] == "bounded" and target["mode"] == "permanent":
                raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
            current_cutoff = current["pruned_through_receipt_sequence"]
            target_cutoff = target["pruned_through_receipt_sequence"]
            if target["mode"] == "bounded":
                if (
                    current["mode"] == "bounded"
                    and current["policy_identifier"] != target["policy_identifier"]
                ):
                    raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
                if current_cutoff is not None and (
                    target_cutoff is None
                    or _checkpoint_number(target_cutoff)
                    < _checkpoint_number(current_cutoff)
                ):
                    raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
                if target_cutoff is not None and _checkpoint_number(
                    target_cutoff
                ) >= _checkpoint_number(checkpoint["next_operation_receipt_sequence"]):
                    raise ExecutionHostError(HostCode.INVALID_EXECUTION_CHECKPOINT)
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
                if getattr(exc, "code", None) == HostCode.INVALID_EXECUTION_CHECKPOINT:
                    raise ExecutionHostError(
                        HostCode.INVALID_EXECUTION_CHECKPOINT
                    ) from exc
                raise
            response = {
                "result": "committed",
                "replay_retention": copy.deepcopy(target),
            }
        self._after_commit()
        return response

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

    def create_v2(
        self,
        bundle: Bundle | BundleSource,
        machine_id: str,
        creation_id: str,
        bindings: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> StagedExecutionResult:
        return self._stage(
            "create_v2",
            lambda: self._host.create_v2(
                bundle,
                machine_id,
                self.root_instance_id,
                creation_id,
                bindings,
            ),
        )

    def admit_v2(
        self,
        deliveries: Sequence[Mapping[str, Any]],
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "admit_v2",
            lambda: self._host.admit_v2(
                self.root_instance_id,
                deliveries,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )

    def process_ready_v2(
        self,
        target_runtime_id: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "process_ready_v2",
            lambda: self._host.process_ready_v2(
                self.root_instance_id,
                target_runtime_id,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )

    def prune_v2(
        self,
        cutoff_receipt_sequence: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "prune_v2",
            lambda: self._host.prune_v2(
                self.root_instance_id,
                cutoff_receipt_sequence,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
            ),
        )

    def maintenance_migration_v2(
        self,
        operation_id: str,
        target_validated_bundle_fingerprint: str,
        migration_descriptor_digest_route: Sequence[str],
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
        maintenance_mode: bool = True,
        limits: MigrationLimits | None = None,
    ) -> StagedExecutionResult:
        return self._stage(
            "maintenance_migration_v2",
            lambda: self._host.maintenance_migration_v2(
                self.root_instance_id,
                operation_id,
                target_validated_bundle_fingerprint,
                migration_descriptor_digest_route,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
                maintenance_mode=maintenance_mode,
                limits=limits,
            ),
        )

    def tombstone_root_v2(
        self,
        operation_id: str,
        *,
        expected_revision: str,
        expected_checkpoint_digest: str,
    ) -> StagedExecutionResult:
        return self._stage(
            "tombstone_root_v2",
            lambda: self._host.tombstone_root_v2(
                self.root_instance_id,
                operation_id,
                expected_revision=expected_revision,
                expected_checkpoint_digest=expected_checkpoint_digest,
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

def _target_root_instance_id(target: Mapping[str, Any]) -> str:
    member = next(iter(target.values()))
    return str(member["root_instance_id"])
