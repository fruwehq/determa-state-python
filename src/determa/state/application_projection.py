"""Lossless selected-row projection in an application-owned shared transaction."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from .checkpoint import serialize_execution_checkpoint
from .checkpoint_v1 import (
    admit_checkpoint_v1,
    create_checkpoint_v1,
    restore_execution_checkpoint_v1,
    step_checkpoint_v1,
)
from .definition import Bundle, BundleSource
from .errors import ArtifactError
from .queueing import _entry_digest, _valid_envelope_shape, step_aggregate_v1
from .stores import SHARED_APPLICATION_TRANSACTION, ExecutionStore
from .wire import DefinitionResolver, canonical_bytes, restore_aggregate


class ProjectionError(ArtifactError):
    """A closed projection failure; the enclosing transaction rolls back."""


class ApplicationRowMapping(Protocol):
    """Application plugin for one explicitly selected finite set of rows.

    Methods receive the host-owned native transaction. Implementations must use it
    for every selected-row and supplemental write. They may never commit it.
    """

    def read(self, native_transaction: Any, row_ids: Sequence[str]) -> Any: ...

    def validate_selection(
        self, rows: Any, row_ids: Sequence[str], root_instance_id: str
    ) -> None: ...

    def prepare_delivery(
        self, rows: Any, request: Mapping[str, Any], delivery: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...

    def project(self, rows: Any, checkpoint: Mapping[str, Any]) -> Any: ...

    def reconstruct(self, rows: Any) -> bytes: ...

    def write(self, native_transaction: Any, rows: Any) -> None: ...


class ApplicationProjectionFacade:
    """Invoke one core operation and atomically commit selected rows and checkpoint.

    A shared claim is accepted only from the configured execution store's actual
    shared transaction. The application plugin has no access to portable store
    transactions and receives no precommit result as a committed response.
    """

    def __init__(self, store: ExecutionStore, resolver: DefinitionResolver) -> None:
        self.store = store
        self.resolver = resolver

    def run(
        self,
        request: Mapping[str, Any],
        mapping: ApplicationRowMapping,
        *,
        create_bundle: Bundle | BundleSource | None = None,
    ) -> dict[str, Any]:
        if SHARED_APPLICATION_TRANSACTION not in self.store.capabilities:
            raise ProjectionError("projection_transaction_unavailable")
        if request.get("shared_transaction_requested") is not True:
            raise ProjectionError("projection_transaction_unavailable")
        operation = request.get("operation")
        boundary = request.get("boundary")
        if operation not in {"create", "admit", "step"} or boundary not in {
            "create_binding",
            "declared_input",
            "external_refresh",
        }:
            raise ProjectionError("unsupported_projection_boundary")
        root = request.get("root_instance_id")
        row_ids = request.get("selected_row_ids")
        if (
            not isinstance(root, str)
            or not root
            or not isinstance(row_ids, list)
            or not row_ids
            or any(not isinstance(row_id, str) or not row_id for row_id in row_ids)
            or len(row_ids) != len(set(row_ids))
        ):
            raise ProjectionError("invalid_projection_selection")
        result: dict[str, Any]
        with self.store.shared_transaction(root) as (native, transaction):
            if transaction.root_instance_id != root:
                raise ProjectionError("invalid_projection_selection")
            rows = mapping.read(native, row_ids)
            mapping.validate_selection(rows, row_ids, root)
            source = transaction.load()
            if source is None:
                if operation != "create":
                    if operation != "step" or not hasattr(mapping, "reconstruct_aggregate"):
                        raise ProjectionError("projection_not_lossless")
                    aggregate_mapping: Any = mapping
                    try:
                        prior_aggregate = restore_aggregate(
                            aggregate_mapping.reconstruct_aggregate(rows), self.resolver
                        ).aggregate_envelope
                    except (ArtifactError, TypeError, ValueError) as exc:
                        raise ProjectionError("projection_not_lossless") from exc
                    result = step_aggregate_v1(
                        prior_aggregate, str(request.get("target_runtime_id")), self.resolver
                    )
                    candidate_aggregate = result["state"]
                    proposed_rows = aggregate_mapping.project_aggregate(rows, candidate_aggregate)
                    try:
                        round_trip = restore_aggregate(
                            aggregate_mapping.reconstruct_aggregate(proposed_rows), self.resolver
                        )
                    except (ArtifactError, TypeError, ValueError) as exc:
                        raise ProjectionError("projection_not_lossless") from exc
                    if round_trip.canonical_bytes != canonical_bytes(candidate_aggregate):
                        raise ProjectionError("projection_not_lossless")
                    mapping.write(native, proposed_rows)
                    return result
                creation = request.get("creation")
                if not isinstance(creation, Mapping):
                    raise ProjectionError("invalid_projection_input")
                if create_bundle is None:
                    raise ProjectionError("invalid_projection_input")
                operation_result = create_checkpoint_v1(
                    create_bundle,
                    creation["machine_id"],
                    root,
                    creation["creation_id"],
                    creation.get("bindings"),
                    _include_projection_result=True,
                )
                candidate = operation_result["checkpoint"]
                result = copy.deepcopy(operation_result["core_result"])
                result.pop("result", None)
                result.pop("disposition", None)
                for emission in result["emissions"]:
                    emission.pop("_determa_v1_emission_index", None)
                inserting = True
            else:
                if operation == "create":
                    raise ProjectionError("checkpoint_revision_conflict")
                try:
                    prior = restore_execution_checkpoint_v1(source, self.resolver).document
                    reconstructed = restore_execution_checkpoint_v1(
                        mapping.reconstruct(rows), self.resolver
                    ).document
                except (ArtifactError, TypeError, ValueError) as exc:
                    raise ProjectionError("projection_not_lossless") from exc
                if prior != reconstructed or prior["root_instance_id"] != root:
                    raise ProjectionError("projection_not_lossless")
                expected = request.get("expected_checkpoint")
                if not isinstance(expected, Mapping) or expected.get("root_instance_id") != root:
                    raise ProjectionError("checkpoint_revision_conflict")
                revision = expected.get("revision")
                digest = expected.get("digest")
                if not isinstance(revision, str) or not isinstance(digest, str):
                    raise ProjectionError("checkpoint_revision_conflict")
                if operation == "admit":
                    supplied = request.get("delivery")
                    if not isinstance(supplied, Mapping):
                        raise ProjectionError("invalid_projection_input")
                    delivery = copy.deepcopy(dict(supplied))
                    envelope = delivery.get("envelope")
                    if not isinstance(envelope, Mapping) or not _valid_envelope_shape(envelope):
                        raise ProjectionError("invalid_projection_input")
                    event_id = envelope["event_id"]
                    retained = any(
                        item.get("event_id") == event_id
                        for item in prior["operation_receipts"] + prior["event_identity_tombstones"]
                    ) or any(
                        entry["envelope"]["event_id"] == event_id
                        for runtime in (prior["root_record"].get("aggregate_state") or {}).get(
                            "runtimes", []
                        )
                        for mailbox in ("ready_mailbox", "deferred_mailbox")
                        for entry in runtime[mailbox]
                    )
                    if retained:
                        # Identity is settled from the immutable caller request before
                        # the current row is read as input or declaration-checked.
                        admit_checkpoint_v1(
                            prior,
                            [delivery],
                            self.resolver,
                            expected_revision=revision,
                            expected_checkpoint_digest=digest,
                        )
                        receipt = next(
                            (
                                item
                                for item in reversed(prior["operation_receipts"])
                                if item.get("event_id") == event_id
                            ),
                            None,
                        )
                        if receipt is None:
                            receipt = next(
                                (
                                    item
                                    for item in prior["event_identity_tombstones"]
                                    if item["event_id"] == event_id
                                ),
                                None,
                            )
                        if receipt is None:
                            raise ProjectionError("projection_not_lossless")
                        return copy.deepcopy(receipt)
                    delivery = dict(mapping.prepare_delivery(rows, request, delivery))
                    prepared_envelope = delivery.get("envelope")
                    if not isinstance(prepared_envelope, Mapping):
                        raise ProjectionError("invalid_projection_input")
                    identity = {key: value for key, value in envelope.items() if key != "payload"}
                    mapped_identity = {
                        key: value for key, value in prepared_envelope.items() if key != "payload"
                    }
                    if delivery.get("delivery_mode") != supplied.get("delivery_mode") or (
                        mapped_identity != identity
                    ):
                        raise ProjectionError("invalid_projection_input")
                    delivery["envelope_digest"] = _entry_digest(
                        root, str(delivery.get("delivery_mode")), prepared_envelope
                    )
                    operation_result = admit_checkpoint_v1(
                        prior,
                        [delivery],
                        self.resolver,
                        expected_revision=revision,
                        expected_checkpoint_digest=digest,
                        _include_projection_result=True,
                    )
                    candidate = operation_result["checkpoint"]
                    result = copy.deepcopy(operation_result["core_result"])
                    result.pop("result", None)
                    result["accepted"] = bool(result["accepted"])
                else:
                    operation_result = step_checkpoint_v1(
                        prior,
                        str(request.get("target_runtime_id")),
                        self.resolver,
                        expected_revision=revision,
                        expected_checkpoint_digest=digest,
                        _include_host_response=True,
                        _defer_projection_cas=True,
                    )
                    candidate = operation_result["checkpoint"]
                    step_result = operation_result.get(
                        "core_result", operation_result.get("step_result")
                    )
                    if not isinstance(step_result, dict):
                        raise ProjectionError("projection_not_lossless")
                    result = copy.deepcopy(step_result)
                inserting = False
            proposed_rows = mapping.project(rows, candidate)
            try:
                reconstructed_candidate = restore_execution_checkpoint_v1(
                    mapping.reconstruct(proposed_rows), self.resolver
                )
            except (ArtifactError, TypeError, ValueError) as exc:
                raise ProjectionError("projection_not_lossless") from exc
            if reconstructed_candidate.canonical_bytes != canonical_bytes(candidate):
                raise ProjectionError("projection_not_lossless")
            if (
                not inserting
                and candidate["execution_checkpoint_digest"] == prior["execution_checkpoint_digest"]
            ):
                if proposed_rows != rows:
                    raise ProjectionError("projection_not_lossless")
                return result
            encoded = serialize_execution_checkpoint(candidate)
            if inserting:
                changed = transaction.insert(encoded)
            else:
                changed = transaction.replace(
                    prior["revision"], prior["execution_checkpoint_digest"], encoded
                )
            if not changed:
                raise ProjectionError("checkpoint_revision_conflict")
            mapping.write(native, proposed_rows)
        return result
