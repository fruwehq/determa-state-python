#!/usr/bin/env python3
"""Production SQLite and installed native-handler bridge for the §19 profile."""

from __future__ import annotations

import base64
import copy
import hashlib
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from conformance.authority_adapter import _PATH
from determa.state import load_bundle
from determa.state.authority import (
    AuthoritySQLiteExecutionStore,
    SQLiteLocalAuthority,
    _compact,
    configure_bundled_sqlite_authority,
)
from determa.state.effects import (
    EffectError,
    SQLiteCommittedEffectHost,
    _result_response,
)
from determa.state.extensions import bundled_extension_registry
from determa.state.wire import (
    MemoryArtifactResolver,
    canonical_bytes,
    decoded_typed_value,
)

_CASE = (
    Path(os.environ["DETERMA_CONFORMANCE_DIR"])
    / "conformance/profiles/committed-native-effects/effect-01-result"
)
_DESTINATION_PATH = _PATH.with_name("determa-effect-fake-destination.sqlite")
_PROOFS_PATH = _PATH.with_name("determa-effect-native-proofs.json")
_AUTHORITY_SCOPE = "effect-scope-1"
_ROOT = "effect-root-1"
_HANDLER_SOURCE = "handler/test_handler.py"
_CONTROL_ROOT = _PATH.with_name("determa-effect-control")


def _await_file(path: Path, seconds: float = 15) -> None:
    deadline = time.monotonic() + seconds
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"native control barrier was not reached: {path.name}")
        time.sleep(0.02)


def _sha(source: bytes) -> str:
    return "sha256:" + hashlib.sha256(source).hexdigest()


def _handler_closure() -> bytes:
    result = bytearray(b"determa-effect-handler-closure-1\0")
    for path in sorted((_CASE / "handler").glob("test_handler.*")):
        name = str(path.relative_to(_CASE)).encode()
        source = path.read_bytes()
        result += len(name).to_bytes(8, "big") + name
        result += len(source).to_bytes(8, "big") + source
    return bytes(result)


def _destination_configuration() -> bytes:
    return canonical_bytes(json.loads((_CASE / "data/destination-configuration.json").read_bytes()))


def _route(checkpoint: dict[str, Any], generation: str) -> dict[str, Any]:
    runtime = checkpoint["root_record"]["aggregate_state"]["runtimes"][0]
    return {
        "generation": generation,
        "authority_epoch": "3",
        "handler_reference": {
            "identifier": "conformance.native-effect-handler",
            "version": "1.0.0",
            "content_digest": _sha(_handler_closure()),
        },
        "destination_binding_digest": _sha(_destination_configuration()),
        "result_mapping": [
            {
                "outcome_kind": "succeeded",
                "event": "native_succeeded",
                "result_slot": "success",
                "operation_token_location": {"kind": "correlation_id"},
            },
            {
                "outcome_kind": "cancelled",
                "event": "native_cancelled",
                "result_slot": "cancelled",
                "operation_token_location": {"kind": "correlation_id"},
            },
        ],
        "target": {
            "root_instance_id": checkpoint["root_instance_id"],
            "runtime_id": runtime["runtime_id"],
            "runtime_incarnation": copy.deepcopy(runtime["identity_origin"]),
        },
        "idempotency_policy": "destination_deduplicates",
    }


def _authority_ledger() -> dict[str, Any]:
    return {
        "allocated_scope_identities": [_AUTHORITY_SCOPE],
        "scope_identity": _AUTHORITY_SCOPE,
        "owner_principal": "owner-1",
        "authority_epoch": "3",
        "scope_generation": "4",
        "state": "active",
        "receipts": [],
        "mutation_bytes": [],
        "checkpoint_bytes": [],
        "native_checkpoint_bytes": [],
        "journal_entries": [],
        "ingress_acknowledgements": [],
        "active_claims": [],
        "roots": [_ROOT],
        "tombstones": [],
        "pending_intents": [],
        "terminal_intents": [],
        "definition_references": [],
        "migration_references": [],
        "required_participant_records": ["journal:journal-1", "worker:worker-1"],
        "freeze": None,
        "inventory": [],
        "retirement_grants": [],
        "destination_activations": [],
    }


def _reset_authority() -> tuple[SQLiteLocalAuthority, dict[str, Any], dict[str, Any]]:
    for suffix in ("", "-wal", "-shm"):
        _PATH.with_name(_PATH.name + suffix).unlink(missing_ok=True)
    authority = SQLiteLocalAuthority(_PATH, worker_fencing=True, worker_lease_nanoseconds=1)
    authority.setup_schema()
    AuthoritySQLiteExecutionStore(authority, _AUTHORITY_SCOPE, "owner-1", "3").setup_schema()
    SQLiteCommittedEffectHost(_PATH, MemoryArtifactResolver(), {}, lambda *_: {}).setup_schema()
    if not authority._insert_ledger(_authority_ledger()):
        raise RuntimeError("effect scope allocation failed")
    registry = bundled_extension_registry(include_postgresql=False)
    configured, authority, store = configure_bundled_sqlite_authority(
        registry,
        _PATH,
        _AUTHORITY_SCOPE,
        "owner-1",
        "3",
        worker_fencing=True,
        worker_lease_nanoseconds=1,
    )
    report = authority.profile_report(_AUTHORITY_SCOPE, "owner-1", store, registry, configured)
    installation = authority.profile_descriptor(store, registry, configured)[
        "installation_evidence"
    ]
    return authority, report, installation


def _destination_reset() -> None:
    for suffix in ("", "-wal", "-shm"):
        _DESTINATION_PATH.with_name(_DESTINATION_PATH.name + suffix).unlink(missing_ok=True)
    with sqlite3.connect(_DESTINATION_PATH) as connection:
        connection.execute(
            "CREATE TABLE effect_receipts (scope_identity TEXT NOT NULL, "
            "effect_id TEXT NOT NULL, receipt BLOB NOT NULL, calls INTEGER NOT NULL, "
            "PRIMARY KEY(scope_identity, effect_id))"
        )


def _destination_call(scope: str, effect_id: str, payload: Any) -> tuple[str, bool]:
    """Durable controlled destination with scoped first-response deduplication."""
    del payload
    with sqlite3.connect(_DESTINATION_PATH) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT receipt, calls FROM effect_receipts WHERE scope_identity = ? AND effect_id = ?",
            (scope, effect_id),
        ).fetchone()
        if row is None:
            receipt = canonical_bytes(
                {
                    "reference": "accepted-42",
                    "accepted": True,
                    "scope_identity": scope,
                    "effect_id": effect_id,
                }
            )
            connection.execute(
                "INSERT INTO effect_receipts VALUES (?, ?, ?, 1)", (scope, effect_id, receipt)
            )
        else:
            receipt = bytes(row[0])
            connection.execute(
                "UPDATE effect_receipts SET calls = calls + 1 "
                "WHERE scope_identity = ? AND effect_id = ?",
                (scope, effect_id),
            )
    result = json.loads(receipt)
    return result["reference"], result["accepted"]


def _destination_proof(binding: str, effect_id: str) -> dict[str, Any]:
    with sqlite3.connect(_DESTINATION_PATH) as connection:
        row = connection.execute(
            "SELECT receipt, calls FROM effect_receipts WHERE scope_identity = ? AND effect_id = ?",
            (_AUTHORITY_SCOPE, effect_id),
        ).fetchone()
    if row is None or row[1] < 2:
        raise RuntimeError("scoped destination deduplication was not observed")
    receipt = base64.b64encode(bytes(row[0])).decode()
    return {
        "scope_identity": _AUTHORITY_SCOPE,
        "effect_id": effect_id,
        "destination_binding_digest": _sha(_destination_configuration()),
        "first_attempt_receipt_bytes_base64": receipt,
        "repeat_attempt_receipt_bytes_base64": receipt,
    }


def _proof_id(payload: dict[str, Any]) -> str:
    names = (
        "operation",
        "checkpoint_before",
        "journal_before",
        "claim",
        "auth_context",
        "host_configuration",
        "arguments",
        "fault",
    )
    return _sha(
        canonical_bytes(
            [
                "determa-effect-native-proof-1",
                payload["run_id"],
                {key: payload[key] for key in names},
            ]
        )
    )


def _loaded_handler() -> Any:
    source = _CASE / _HANDLER_SOURCE
    spec = importlib.util.spec_from_file_location("determa_conformance_native_handler", source)
    if spec is None or spec.loader is None:
        raise RuntimeError("installed handler source unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _run_operation(payload: dict[str, Any], control: Path | None = None) -> dict[str, Any]:
    checkpoint, journal = payload["checkpoint_before"], payload["journal_before"]
    authority, authority_report, _installation = _reset_authority()
    machine_source = payload["machine_source_utf8"]
    if machine_source.encode() != (_CASE / "machine.yaml").read_bytes():
        raise RuntimeError("machine source differs from installed fixture")
    for path, source in payload["handler_source_files"].items():
        if source.encode() != (_CASE / path).read_bytes():
            raise RuntimeError("handler source differs from installed closure")
    bundle = load_bundle(machine_source)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    configuration = payload["host_configuration"]
    route = _route(checkpoint, configuration["route_generation"])
    handler_module = _loaded_handler()
    provider_calls: list[dict[str, Any]] = []
    core_calls: list[dict[str, Any]] = []
    new_claims: list[dict[str, Any]] = []
    loaded_handler_source: dict[str, str] = {}
    destination_call_evidence: dict[str, Any] | None = None

    class CallLog:
        def call(self, scope: str, effect_id: str, portable_payload: Any) -> tuple[str, bool]:
            nonlocal destination_call_evidence
            destination_call_evidence = {
                "idempotency_key": [scope, effect_id],
                "destination_binding_digest": route["destination_binding_digest"],
                "attempt_fence": journal["effect_records"][0]["attempt_fence"],
            }
            return _destination_call(scope, effect_id, portable_payload)

    def handler(
        typed_payload: Any, metadata: dict[str, Any], attempt: dict[str, Any]
    ) -> dict[str, Any]:
        record = journal["effect_records"][0]
        provider_calls.append(
            {
                "effect_id": record["effect_id"],
                "handler_reference": record["handler_reference"],
                "destination_binding_digest": record["destination_binding_digest"],
                "route_configuration_generation": record["route_configuration_generation"],
                "attempt_fence": record["attempt_fence"],
                "scope_identity": _AUTHORITY_SCOPE,
                "credential_generation": configuration["credential_generation"],
            }
        )
        loaded_handler_source[_HANDLER_SOURCE] = _sha((_CASE / _HANDLER_SOURCE).read_bytes())
        return handler_module.invoke(typed_payload, metadata, attempt, CallLog())

    def observe_core(kind: str, event_id: str, target: dict[str, Any]) -> None:
        core_calls.append(
            {"operation_kind": kind, "event_id": event_id, "target": copy.deepcopy(target)}
        )

    host = SQLiteCommittedEffectHost(
        _PATH,
        resolver,
        route,
        handler,
        authority_scope=_AUTHORITY_SCOPE,
        core_observer=observe_core,
    )
    # Install the vector's pre-existing authority facts as trusted test setup.
    # The production seed path may restore these claims, but cannot issue them.
    with authority._connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT ledger FROM determa_scope_authority WHERE scope_identity = ?",
            (_AUTHORITY_SCOPE,),
        ).fetchone()
        ledger = json.loads(row[0])
        ledger["active_claims"] = [copy.deepcopy(payload["claim"])] if payload["claim"] else []
        ledger["journal_entries"] = [
            {"work_identity": record["effect_id"], "attempt_fence": record["attempt_fence"]}
            for record in journal["effect_records"]
        ]
        connection.execute(
            "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
            (json.dumps(ledger), _AUTHORITY_SCOPE),
        )
        connection.commit()
    host.seed(checkpoint, journal, payload["claim"])
    before = host.snapshot(_ROOT)
    operation = payload["operation"]
    arguments = payload["arguments"]
    context = payload["auth_context"]
    fault = payload["fault"]
    response: dict[str, Any] | None = None
    caller_kind = "completed"
    if operation in {"produce", "produce_replay"}:
        request = arguments["original_request"]
        aggregate = checkpoint["root_record"]["aggregate_state"]
        if aggregate["runtimes"][0]["ready_mailbox"]:
            token = decoded_typed_value(
                aggregate["runtimes"][0]["ready_mailbox"][0]["envelope"]["payload"]
            )["operation_token"]
        else:
            token = journal["effect_records"][0]["operation_token"]

        def route_resolved(selected: dict[str, Any]) -> None:
            if control is None:
                return
            (control / "route_resolved.json").write_bytes(
                canonical_bytes(
                    {
                        "resolved_route_generation": selected["generation"],
                        "handler_reference": selected["handler_reference"],
                        "destination_binding_digest": selected["destination_binding_digest"],
                    }
                )
            )
            _await_file(control / "release_route_resolved")

        def before_commit(candidate: dict[str, Any], proposed_journal: dict[str, Any]) -> None:
            if control is None:
                return
            (control / "before_commit_guard.json").write_bytes(
                canonical_bytes(
                    {
                        "core_event_id": request["event_id"],
                        "proposed_checkpoint_digest": candidate["execution_checkpoint_digest"],
                        "proposed_journal_digest": proposed_journal["host_effect_journal_digest"],
                    }
                )
            )
            _await_file(control / "release_before_commit_guard")
            host.route["generation"] = (control / "route_generation").read_text()

        try:
            response = host.produce(
                _ROOT,
                request["request_id"],
                aggregate["root_runtime_id"],
                expected_revision=request["expected_checkpoint"]["revision"],
                expected_digest=request["expected_checkpoint"]["digest"],
                route_generation=configuration["route_generation"],
                operation_token=token,
                after_route_resolved=route_resolved if control is not None else None,
                before_commit=before_commit if control is not None else None,
            )
            caller_kind = "response"
            if fault == "after_intent_commit_before_response":
                caller_kind, response = "no_response", None
        except EffectError:
            if fault != "route_generation_changed_at_commit_guard":
                raise
            caller_kind = "aborted"
    elif operation == "claim":
        try:
            claim = host.claim(
                _ROOT,
                arguments["effect_id"],
                context["principal"],
                context["scope_authority_epoch"],
                expires_at=str(int(context["trusted_host_now"]) + 1),
                trusted_now=context["trusted_host_now"],
                deduplication_proven=configuration["destination_deduplication_proven"],
            )
            new_claims.append(
                {
                    "effect_id": claim["work_identity"],
                    "attempt_fence": claim["attempt_fence"],
                    "worker_principal": claim["worker_principal"],
                }
            )
        except EffectError:
            caller_kind = "aborted"
    elif operation == "dispatch":
        try:
            host.dispatch(
                _ROOT,
                arguments["effect_id"],
                context["principal"],
                context["scope_identity"],
                context["scope_authority_epoch"],
                context["trusted_host_now"],
                credential=configuration["credential_generation"]
                if configuration["credential_available"]
                else None,
                authorized=configuration["route_authorized"]
                and configuration["handler_authorized"],
            )
        except EffectError:
            caller_kind = "aborted"
        if fault == "before_intent_commit":
            caller_kind = "aborted"
        if fault == "after_provider_acceptance_before_outcome":
            caller_kind = "no_response"
    elif operation == "terminalize_outbox":
        host.terminalize_outbox(_ROOT, arguments["effect_id"], {"status": arguments["status"]})
    elif operation == "submit_result":
        response = host.submit_result(
            _ROOT,
            arguments,
            principal=context["principal"],
            scope=context["scope_identity"],
            epoch=context["scope_authority_epoch"],
            trusted_now=context["trusted_host_now"],
            admit=fault != "after_outcome_before_admission",
        )
        caller_kind = "response"
        if fault == "after_outcome_before_admission":
            caller_kind, response = "no_response", None
    elif operation == "recover":
        before_state = (
            journal["effect_records"][0]["invocation_state"] if journal["effect_records"] else None
        )
        after_document = host.recover(_ROOT)
        if before_state == "result_admitted":
            record = after_document["journal"]["effect_records"][0]
            response = _result_response(
                record, after_document["journal"], after_document["checkpoint"], "committed"
            )
            caller_kind = "response"
        elif fault == "after_admission_before_response":
            caller_kind = "no_response"
    elif operation == "cancel_effect":
        try:
            response = host.cancel(_ROOT, arguments)
        except EffectError as error:
            response = {
                "status": "rejected",
                "operation_id": arguments["operation_id"],
                "effect_id": arguments["effect_id"],
                "cancellation": None,
                "outcome": None,
                "result_event_id": None,
                "journal_revision": None,
                "error_code": error.code,
            }
        caller_kind = "response"
    else:
        raise RuntimeError("unsupported effect operation")
    after = host.snapshot(_ROOT)
    before_checkpoint = canonical_bytes(before["checkpoint"])
    before_journal = canonical_bytes(before["journal"])
    after_checkpoint = canonical_bytes(after["checkpoint"])
    after_journal = canonical_bytes(after["journal"])
    changed = before_checkpoint != after_checkpoint or before_journal != after_journal
    report_bytes = canonical_bytes(
        {
            "format": "determa.committed_native_effects.configured_profile",
            "schema_version": 1,
            "scope_identity": _AUTHORITY_SCOPE,
            "handler_reference": route["handler_reference"],
            "destination_binding_digest": route["destination_binding_digest"],
            "idempotency_policy": route["idempotency_policy"],
            "authority_report_bytes": canonical_bytes(authority_report).decode(),
        }
    )
    proof_id = _proof_id(payload)
    previous_proofs = json.loads(_PROOFS_PATH.read_bytes()) if _PROOFS_PATH.exists() else []
    _PROOFS_PATH.write_bytes(canonical_bytes(sorted(set(previous_proofs) | {proof_id})))
    kind = caller_kind
    return {
        "response_utf8": None if response is None else canonical_bytes(response).decode(),
        "checkpoint_before_utf8": before_checkpoint.decode(),
        "checkpoint_after_utf8": after_checkpoint.decode(),
        "journal_before_utf8": before_journal.decode(),
        "journal_after_utf8": after_journal.decode(),
        "provider_calls": provider_calls,
        "core_calls": core_calls,
        "new_claims": new_claims,
        "loaded_machine_sha256": _sha((_CASE / "machine.yaml").read_bytes()),
        "loaded_handler_source": loaded_handler_source,
        "caller_result": {
            "kind": kind,
            "operation": operation,
            "checkpoint_digest": None
            if kind == "no_response"
            else after["checkpoint"]["execution_checkpoint_digest"],
            "journal_digest": None
            if kind == "no_response"
            else after["journal"]["host_effect_journal_digest"],
        },
        "native_evidence": {
            "proof_id": proof_id,
            "run_id": payload["run_id"],
            "report_digest": _sha(report_bytes),
            "authority_report_digest": _sha(canonical_bytes(authority_report)),
            "topology_identifier": authority_report["topology"]["identifier"],
            "scope_identity": _AUTHORITY_SCOPE,
            "authority_epoch": authority_report["authority_epoch"],
            "handler_reference": route["handler_reference"],
            "destination_binding_digest": route["destination_binding_digest"],
            "guard_fate": (
                "rolled_back"
                if fault == "route_generation_changed_at_commit_guard"
                else "committed"
                if changed
                else "no_mutation"
            ),
            "claim_guard_observed": operation in {"claim", "dispatch", "submit_result"},
            "native_transaction_id": str(uuid.uuid4()),
            "destination_call_evidence": destination_call_evidence,
        },
    }


def _profile(payload: dict[str, Any]) -> dict[str, Any]:
    if payload["phase"] == "before":
        _PROOFS_PATH.unlink(missing_ok=True)
        _destination_reset()
    _authority, authority_report, installation = _reset_authority()
    report = {
        "format": "determa.committed_native_effects.configured_profile",
        "schema_version": 1,
        "scope_identity": _AUTHORITY_SCOPE,
        "handler_reference": {
            "identifier": "conformance.native-effect-handler",
            "version": "1.0.0",
            "content_digest": _sha(_handler_closure()),
        },
        "destination_binding_digest": _sha(_destination_configuration()),
        "idempotency_policy": "destination_deduplicates",
        "authority_report_bytes": canonical_bytes(authority_report).decode(),
    }
    proofs = json.loads(_PROOFS_PATH.read_bytes()) if _PROOFS_PATH.exists() else []
    deduplication = None
    if payload["phase"] == "after":
        # The scoped destination retained one first receipt through repeated real calls.
        with sqlite3.connect(_DESTINATION_PATH) as connection:
            row = connection.execute(
                "SELECT effect_id FROM effect_receipts WHERE scope_identity = ? AND calls >= 2",
                (_AUTHORITY_SCOPE,),
            ).fetchone()
        if row is not None:
            deduplication = _destination_proof(report["scope_identity"], row[0])
    return {
        "report_bytes": canonical_bytes(report).decode(),
        "installation_evidence": {
            "run_id": payload["run_id"],
            "handler_closure_bytes_base64": base64.b64encode(_handler_closure()).decode(),
            "destination_configuration_bytes_base64": base64.b64encode(
                _destination_configuration()
            ).decode(),
            "executing_source_path": _HANDLER_SOURCE,
            "loaded_source_sha256": _sha((_CASE / _HANDLER_SOURCE).read_bytes()),
            "observed_health": "healthy",
            "authority_closure_bytes_base64": installation["closure_bytes_base64"],
            "authority_configuration_bytes_base64": installation["configuration_bytes_base64"],
            "participant_installations": installation["participant_installations"],
            "native_proof_ids": proofs,
            "destination_deduplication_proof": deduplication,
        },
    }


def _control(payload: dict[str, Any]) -> dict[str, Any]:
    session = str(uuid.UUID(payload["session"]))
    directory = _CONTROL_ROOT / session
    action = payload["control"]["action"]
    if action == "start_call":
        directory.mkdir(parents=True, exist_ok=False)
        operation = payload["operation_input"]
        (directory / "input.json").write_bytes(canonical_bytes(operation))
        (directory / "route_generation").write_text(
            operation["host_configuration"]["route_generation"]
        )
        with (
            (directory / "stdout").open("wb") as output,
            (directory / "stderr").open("wb") as error,
        ):
            worker = subprocess.Popen(
                [sys.executable, "-m", "conformance.effects_adapter", "--control-worker", session],
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=error,
            )
        (directory / "pid").write_text(str(worker.pid))
        return {"session": session, "event": "started"}
    if action == "await_barrier":
        barrier = payload["control"]["barrier"]
        path = directory / f"{barrier}.json"
        _await_file(path)
        return {
            "session": session,
            "event": "barrier_reached",
            "barrier": barrier,
            **json.loads(path.read_bytes()),
        }
    if action == "release_barrier":
        barrier = payload["control"]["barrier"]
        (directory / f"release_{barrier}").touch()
        return {"session": session, "event": "released", "barrier": barrier}
    if action == "set_route_generation":
        generation = payload["control"]["route_generation"]
        (directory / "route_generation").write_text(generation)
        return {
            "session": session,
            "event": "configuration_changed",
            "route_generation": generation,
        }
    if action == "observe_native_fate":
        _await_file(directory / "observation.json")
        observation = json.loads((directory / "observation.json").read_bytes())
        checkpoint = json.loads(observation["checkpoint_after_utf8"])
        journal = json.loads(observation["journal_after_utf8"])
        transaction = observation["native_evidence"]["native_transaction_id"]
        return {
            "session": session,
            "event": "native_fate",
            "guard_result": "scope_generation_conflict",
            "transaction_fate": "rolled_back",
            "checkpoint_digest": checkpoint["execution_checkpoint_digest"],
            "journal_digest": journal["host_effect_journal_digest"],
            "observation": observation,
            "native_transaction_id": transaction,
        }
    raise ValueError("unknown native control action")


def _control_worker(session: str) -> None:
    directory = _CONTROL_ROOT / str(uuid.UUID(session))
    payload = json.loads((directory / "input.json").read_bytes())
    try:
        observation = _run_operation(payload, control=directory)
        (directory / "observation.json").write_bytes(canonical_bytes(observation))
    except BaseException:
        import traceback

        (directory / "error.txt").write_text(traceback.format_exc())
        raise


def run(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("kind") == "configured_profile":
        return _profile(payload)
    if payload.get("kind") == "control":
        return _control(payload)
    return _run_operation(payload)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--control-worker":
        _control_worker(sys.argv[2])
    else:
        print(_compact(run(json.loads(sys.stdin.buffer.read()))))
