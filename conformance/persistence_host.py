"""Executable in-memory driver for the optional durable persistence profile."""

from __future__ import annotations

import copy
from typing import Any

from determa.state import ArtifactError, seal_execution_checkpoint
from determa.state.checkpoint_v2 import admit_checkpoint_v2, step_checkpoint_v2
from determa.state.queueing import _runtime_id_for_target, migrate_aggregate_v2

from .version2 import _json, _resolver


def _delivery(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "delivery_mode": "input",
        "envelope": copy.deepcopy(request["presented_envelope"]),
        "envelope_digest": request["envelope_digest"],
    }


def _call_log(item: Any) -> list[str]:
    return _json(item.path / item.vector["call_log"])["calls"]


def run_persistence_vector(
    item: Any, request: dict[str, Any]
) -> tuple[str, str | None, bytes, int]:
    before = _json(item.path / item.vector["store_before"])
    candidate = copy.deepcopy(before)
    calls = ["select_scope", "resolve_artifacts", "validate_capabilities"]
    policy = request.get("transaction_inputs", {}).get("failure_policy")

    if item.vector["operation"] == "persistence_release_quarantine_v2":
        quarantine = candidate.get("quarantine")
        if (
            quarantine is None
            or quarantine["event_id"] != request["event_id"]
        ):
            raise ArtifactError("invalid_execution_checkpoint")
        quarantine["released"] = True
        calls.append("release_quarantine")
        assert calls == _call_log(item)
        return "released", None, _store_bytes(candidate), 0

    event_id = request["presented_envelope"]["event_id"]
    prior = next(
        (record for record in candidate["inbox"] if record["event_id"] == event_id),
        None,
    )
    released = (
        prior is not None
        and prior["disposition"] == "quarantined"
        and (candidate.get("quarantine") or {}).get("event_id") == event_id
        and candidate["quarantine"]["released"] is True
    )
    if policy == "permanent_quarantine" and prior is None:
        candidate["quarantine"] = {
            "event_id": event_id,
            "reason_code": "permanent_processing_failure",
            "released": False,
        }
        candidate["inbox"].append(
            {
                "event_id": event_id,
                "request_digest": request["envelope_digest"],
                "disposition": "quarantined",
            }
        )
        calls.append("quarantine")
        assert calls == _call_log(item)
        return (
            "quarantined",
            "permanent_processing_failure",
            _store_bytes(candidate),
            0,
        )

    calls.extend(["begin_transaction", "read_checkpoint", "check_replay"])
    if prior is not None and not released:
        if prior["request_digest"] != request["envelope_digest"]:
            raise ArtifactError("event_id_conflict")
        calls.append("acknowledge")
        assert calls == _call_log(item)
        return "replayed", None, _store_bytes(candidate), 0

    if policy == "transient_retry":
        calls.append("rollback")
        assert calls == _call_log(item)
        assert candidate == _json(item.path / item.vector["store_after"])
        raise _PersistenceFailure(
            "transient_processing_failure", _store_bytes(before), 0
        )
    checkpoint = candidate["checkpoint"]
    resolver = _resolver(item.path, request)
    expected_checkpoint = request["expected_checkpoint"]
    if (
        checkpoint["revision"] != expected_checkpoint["revision"]
        or checkpoint["execution_checkpoint_digest"] != expected_checkpoint["digest"]
    ):
        raise ArtifactError("checkpoint_revision_conflict")
    transaction_inputs = request.get("transaction_inputs", {})
    migration_route = transaction_inputs.get("migration_descriptor_digest_route", [])
    target_fingerprint = transaction_inputs.get(
        "target_validated_bundle_fingerprint",
        checkpoint["root_record"]["aggregate_state"]["validated_bundle_fingerprint"],
    )
    if migration_route:
        migration = migrate_aggregate_v2(
            checkpoint["root_record"]["aggregate_state"],
            target_fingerprint,
            migration_route,
            resolver,
            maintenance_mode=False,
        )
        checkpoint = copy.deepcopy(checkpoint)
        checkpoint["root_record"]["aggregate_state"] = migration["aggregate_state"]
        checkpoint["migration_audit_records"].extend(migration["audit_records"])
        checkpoint["revision"] = str(int(expected_checkpoint["revision"]) + 1)
        checkpoint = seal_execution_checkpoint(checkpoint)
    original_receipt_count = len(checkpoint["operation_receipts"])
    calls.append("call_core")
    admitted = admit_checkpoint_v2(
        checkpoint,
        [_delivery(request)],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    if admitted.get("execution_checkpoint_schema_version") != 2:
        admitted = admitted["checkpoint"]
    target_runtime_id = _runtime_id_for_target(request["presented_envelope"]["target"])
    processed = step_checkpoint_v2(
        admitted,
        target_runtime_id,
        resolver,
        expected_revision=admitted["revision"],
        expected_checkpoint_digest=admitted["execution_checkpoint_digest"],
    )
    if processed.get("execution_checkpoint_schema_version") != 2:
        processed = processed["checkpoint"]
    committed_revision = str(int(expected_checkpoint["revision"]) + 1)
    processed["revision"] = committed_revision
    for receipt in processed["operation_receipts"][original_receipt_count:]:
        if "accepted_revision" in receipt:
            receipt["accepted_revision"] = committed_revision
        if "committed_revision" in receipt:
            receipt["committed_revision"] = committed_revision
    for intent in processed["pending_outbox_intents"]:
        if int(intent["state_revision"]) > int(expected_checkpoint["revision"]):
            intent["state_revision"] = committed_revision
    processed = seal_execution_checkpoint(processed)
    candidate["checkpoint"] = processed
    committed_identity = {
        "event_id": event_id,
        "request_digest": request["envelope_digest"],
        "disposition": "committed",
    }
    if released:
        assert prior is not None
        prior.clear()
        prior.update(committed_identity)
        candidate["quarantine"] = None
    else:
        candidate["inbox"].append(committed_identity)
    candidate["application_rows"].update(
        request["transaction_inputs"].get("application_writes", {})
    )
    calls.extend(["stage_checkpoint", "stage_inbox", "stage_outbox", "stage_audit"])
    if policy == "inject_pre_commit":
        calls.append("rollback")
        assert calls == _call_log(item)
        raise _PersistenceFailure(
            "injected_pre_commit_failure", _store_bytes(before), 1
        )
    if request["transaction_inputs"].get("application_writes"):
        calls.append("stage_application_rows")
    calls.append("commit")
    if policy == "inject_post_commit_response_loss":
        assert calls == _call_log(item)
        raise _PersistenceFailure(
            "response_lost_after_commit", _store_bytes(candidate), 1
        )
    calls.append("acknowledge")
    assert calls == _call_log(item)
    return "committed", None, _store_bytes(candidate), 1


def _store_bytes(document: dict[str, Any]) -> bytes:
    import json

    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


class _PersistenceFailure(ArtifactError):
    def __init__(self, code: str, stored: bytes, core_calls: int):
        super().__init__(code)
        self.stored = stored
        self.core_calls = core_calls
