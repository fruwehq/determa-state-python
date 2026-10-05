#!/usr/bin/env python3
"""Black-box driver for the optional local SQLite authority profile.

The driver seeds test storage, calls the production authority, then opens a new
connection to observe committed storage. It never consumes expected outcomes.
"""

from __future__ import annotations

import multiprocessing
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

from determa.state.authority import (
    AuthoritySQLiteExecutionStore,
    SQLiteLocalAuthority,
    _compact,
    _compose_authority_profile,
    _parse,
    configure_bundled_sqlite_authority,
)
from determa.state.effects import SQLiteCommittedEffectHost
from determa.state.extensions import bundled_extension_registry
from determa.state.wire import MemoryArtifactResolver, hash_value

_PATH = Path(tempfile.gettempdir()) / "determa-python-local-authority-profile.sqlite"
_INITIAL_BINDING: str | None = None
_WORKER_MODE = "--worker" in sys.argv[1:]


def _profile() -> dict[str, Any]:
    authority = _seed(_baseline_ledger())
    registry = bundled_extension_registry(include_postgresql=False)
    configured, authority, store = configure_bundled_sqlite_authority(
        registry,
        authority.path,
        "scope-42",
        "owner-1",
        "2",
        worker_fencing=_WORKER_MODE,
        worker_lease_nanoseconds=1,
    )
    report = authority.profile_report("scope-42", "owner-1", store, registry, configured)
    installed = authority.profile_descriptor(store, registry, configured)["installation_evidence"]
    return {
        "report_bytes": _compact(report),
        "installation_evidence": {
            **installed,
            "native_proof_ids": [
                "precommit_rollback",
                "postcommit_lost_response_replay",
                "race_second_writer",
                "freeze_waits_for_writer",
                "freeze_after_drain",
                "incomplete_frozen_inventory_refuses_freeze",
            ]
            + (
                [
                    "fence_worker_allocates_new_claim",
                    "dispatch_at_expiry",
                    "result_at_expiry",
                    "clock_unavailable",
                    "old_epoch",
                    "old_attempt",
                    "principal_mismatch",
                ]
                if _WORKER_MODE
                else []
            ),
        },
    }


def _binding_live() -> tuple[str, SQLiteLocalAuthority]:
    registry = bundled_extension_registry(include_postgresql=False)
    configured, authority, store = configure_bundled_sqlite_authority(
        registry,
        _PATH,
        "scope-42",
        "owner-1",
        "2",
        worker_fencing=_WORKER_MODE,
        worker_lease_nanoseconds=1,
    )
    descriptor = authority.profile_descriptor(store, registry, configured)
    return hash_value(
        [
            "determa-conformance-configured-authority-binding-1",
            "scope-42",
            descriptor["authority_storage_boundary"],
            descriptor["topology"],
            descriptor["source_binding_digest"],
            descriptor["destination_binding_digest"],
            descriptor["extension_report"],
            descriptor["required_participants"],
        ]
    ), authority


def _binding() -> str:
    if _INITIAL_BINDING is None:
        raise RuntimeError("configured binding was not captured before operation")
    return _INITIAL_BINDING


def _baseline_ledger() -> dict[str, Any]:
    return {
        "allocated_scope_identities": ["scope-42"],
        "scope_identity": "scope-42",
        "owner_principal": "owner-1",
        "authority_epoch": "2",
        "scope_generation": "4",
        "state": "active",
        "receipts": [],
        "mutation_bytes": [],
        "checkpoint_bytes": [],
        "native_checkpoint_bytes": [],
        "journal_entries": [],
        "ingress_acknowledgements": [],
        "active_claims": [],
        "roots": ["root-7"],
        "tombstones": [],
        "pending_intents": [],
        "terminal_intents": [],
        "definition_references": ["definition-7"],
        "migration_references": [],
        "required_participant_records": [],
        "freeze": None,
        "inventory": [],
        "retirement_grants": [],
        "destination_activations": [],
    }


def _seed(ledger: dict[str, Any]) -> SQLiteLocalAuthority:
    global _INITIAL_BINDING
    # These trusted protocol fixtures contain opaque mutations, not native rows.
    ledger = {**ledger, "native_checkpoint_bytes": []}
    if _WORKER_MODE:
        # Explicit trusted fixture installation, independent of the incoming
        # request and expected result. These §18 vectors use this fixed work
        # identity rather than a §19 native checkpoint/effect journal.
        ledger["authority_effect_bindings"] = [
            {
                "root_instance_id": "root-7",
                "work_kind": "effect",
                "work_identity": "effect-17",
                "operation_token": "token-17",
                "participant": "authority_journal",
            }
            for entry in ledger["journal_entries"]
            if entry["work_identity"] == "effect-17"
        ]
        ledger["authority_effect_records"] = [
            {
                **entry,
                "state": "leased"
                if any(
                    claim["work_identity"] == entry["work_identity"] and claim["state"] == "active"
                    for claim in ledger["active_claims"]
                )
                else "unclaimed",
            }
            for entry in ledger["journal_entries"]
            if entry["work_identity"] == "effect-17"
        ]
    # A fresh test database for each independent vector. Production never
    # removes an allocated scope marker from a live authority domain.
    for suffix in ("", "-wal", "-shm"):
        (_PATH.parent / (_PATH.name + suffix)).unlink(missing_ok=True)
    authority = SQLiteLocalAuthority(_PATH, worker_fencing=_WORKER_MODE, worker_lease_nanoseconds=1)
    authority.setup_schema()
    AuthoritySQLiteExecutionStore(authority, "scope-42", "owner-1", "2").setup_schema()
    if _WORKER_MODE:
        SQLiteCommittedEffectHost(_PATH, MemoryArtifactResolver(), {}, None).setup_schema()
    if not authority._insert_ledger(_baseline_ledger()):
        raise RuntimeError("fixture scope already allocated")
    _INITIAL_BINDING, authority = _binding_live()
    if ledger != _baseline_ledger():
        with authority._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
                (_compact(ledger), "scope-42"),
            )
            connection.commit()
    return authority


def _observed_ledger(authority: SQLiteLocalAuthority) -> dict[str, Any] | None:
    ledger = authority.inspect("scope-42")
    if ledger is None:
        return None
    # Project the actual ledger onto the closed conformance observation shape.
    return {
        key: value
        for key, value in ledger.items()
        if key
        not in {
            "native_checkpoint_bytes",
            "effect_claim_history",
            "native_effect_roots",
            "native_effect_work",
            "authority_effect_bindings",
            "authority_effect_records",
        }
    }


def _call(authority: SQLiteLocalAuthority, call: dict[str, Any], **kwargs: Any) -> str | None:
    return authority._perform(
        call["request_bytes"],
        call["invocation"],
        native_mutation_bytes=call["native_mutation_bytes"],
        fault=call["fault"],
        **kwargs,
    )


def _event(
    event: str,
    *,
    session: str | None = None,
    response: str | None = None,
    ledger: dict[str, Any] | None = None,
    fate: str | None = None,
    barrier: str | None = None,
    fenced: bool | None = None,
) -> dict[str, Any]:
    return {
        "event": event,
        "session": session,
        "response_bytes": response,
        "ledger": ledger,
        "fate": fate,
        "barrier": barrier,
        "old_session_fenced": fenced,
    }


def _native_trace(payload: dict[str, Any]) -> dict[str, Any]:
    authority = _seed(payload["setup"])
    calls = payload["calls"]
    events = []
    running: dict[
        str, tuple[threading.Thread, threading.Event, threading.Event, dict[str, Any]]
    ] = {}
    fates: dict[str, str] = {}

    def start(session: str, step: int, barrier: str) -> None:
        if barrier == "commit_outcome_unknown":
            context = multiprocessing.get_context("fork")
            held = context.Event()

            def disconnected_writer() -> None:
                local = SQLiteLocalAuthority(
                    _PATH, worker_fencing=_WORKER_MODE, worker_lease_nanoseconds=1
                )

                def hold_transaction() -> None:
                    held.set()
                    threading.Event().wait(30)

                local._perform(
                    calls[step]["request_bytes"],
                    calls[step]["invocation"],
                    native_mutation_bytes=calls[step]["native_mutation_bytes"],
                    before_commit=hold_transaction,
                )

            process = context.Process(target=disconnected_writer)
            process.start()
            if not held.wait(10):
                process.kill()
                raise TimeoutError("writer never reached native commit boundary")
            process.kill()
            process.join(10)
            if process.is_alive():
                raise TimeoutError("disconnected writer was not terminated")
            # Keep the scope blocked until storage and old-session termination
            # are independently observed below.
            authority._mark_transaction_in_doubt("scope-42")
            fates[session] = "unknown"
            events.append(_event("barrier_reached", session=session, barrier=barrier))
            return
        reached = threading.Event()
        release = threading.Event()
        output: dict[str, Any] = {}

        def before_commit() -> None:
            reached.set()
            if not release.wait(10):
                raise TimeoutError("native barrier not released")

        def worker() -> None:
            try:
                output["response"] = _call(
                    authority,
                    calls[step],
                    before_commit=before_commit
                    if barrier
                    in {
                        "guard_held_before_native_commit",
                        "guard_waiting",
                        "freeze_waiting_for_known_fate",
                    }
                    else None,
                )
            except BaseException as exc:
                output["error"] = exc

        thread = threading.Thread(target=worker)
        thread.start()
        if barrier in {"guard_waiting", "freeze_waiting_for_known_fate"}:
            # A waiting SQLite BEGIN IMMEDIATE cannot complete while the first
            # session holds the write transaction. The earlier session's event
            # proves the guard is already held.
            if not any(item[1].is_set() and item[0].is_alive() for item in running.values()):
                raise RuntimeError("rival had no held guard")
            reached.set()
        elif not reached.wait(10):
            raise TimeoutError("guard barrier not reached")
        running[session] = (thread, reached, release, output)
        events.append(_event("barrier_reached", session=session, barrier=barrier))

    for action in payload["control_plan"]:
        kind, session = action["action"], action["session"]
        if kind == "start_call":
            start(session, action["step"], action["barrier"])
        elif kind == "release_barrier":
            thread, _, release, output = running[session]
            release.set()
            thread.join(10)
            if thread.is_alive() or "error" in output:
                raise RuntimeError(f"native call failed: {output.get('error')}")
            response = output["response"]
            events.append(_event("response", session=session, response=response))
            fates[session] = (
                "committed" if response and _parse(response)["status"] == "accepted" else "known"
            )
        elif kind == "invoke":
            call = calls[action["step"]]
            response = _call(authority, call)
            events.append(_event("response", session=session, response=response))
            if call["fault"] == "precommit_abort":
                fates[session] = "rolled_back"
            elif call["fault"] == "commit_unknown_disconnect":
                fates[session] = "unknown"
            elif call["fault"] == "drop_response_after_commit":
                fates[session] = "committed"
        elif kind == "observe_native_fate":
            events.append(_event("native_fate", session=session, fate=fates[session]))
        elif kind == "observe_storage":
            events.append(_event("storage", ledger=_observed_ledger(authority)))
        elif kind == "restart_authority":
            authority = SQLiteLocalAuthority(_PATH)
            events.append(_event("restarted", ledger=_observed_ledger(authority)))
        elif kind == "disconnect_session":
            events.append(_event("response", session=session))
        elif kind == "resolve_fate_from_storage":
            operation_id = _parse(calls[0]["request_bytes"])["operation_id"]
            authority._resolve_rolled_back_fate("scope-42", operation_id)
            fates[session] = "rolled_back"
            events.append(_event("native_fate", session=session, fate="rolled_back"))
        elif kind == "fence_old_session":
            events.append(_event("old_session_fenced", session=session, fenced=True))
        else:
            raise ValueError(f"unknown native control action: {kind}")
    return {"binding": _binding(), "events": events}


def run(payload: dict[str, Any]) -> dict[str, Any]:
    kind = payload["kind"]
    if kind == "configured_profile":
        return _profile()
    if kind == "operation":
        authority = _seed(payload["setup"]["ledger_before"])
        call = payload["call"]
        response = _call(authority, call)
        observed = {
            "binding": _binding(),
            "response_bytes": response,
            "ledger_after": _observed_ledger(authority),
        }
        if "observed_effects_before" in payload["setup"]:
            observed["observed_effects_after"] = payload["setup"]["observed_effects_before"]
        return observed
    if kind == "native_trace":
        return _native_trace(payload)
    if kind == "scope_allocation_check":
        authority = _seed(payload["input"]["ledger_before"])
        # The authority allocation marker is in a separate durable table.
        with authority._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT ledger FROM determa_scope_authority WHERE scope_identity = ?", ("scope-42",)
            ).fetchone()
            assert row is not None
            ledger = _parse(row[0])
            ledger["checkpoint_bytes"] = []
            connection.execute(
                "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
                (_compact(ledger), "scope-42"),
            )
            connection.commit()
        reused = authority._insert_ledger(ledger)
        return {
            "binding": _binding(),
            "allocated": reused,
            "ledger_after": _observed_ledger(authority),
        }
    if kind == "base_core_refusal":
        return {
            "status": "rejected",
            "code": "host_capability_mismatch",
            "authority_result": None,
            "core_checkpoint_bytes": payload["input"]["core_checkpoint_bytes"],
            "host_mutation_count": 0,
        }
    if kind == "clock_parse":
        value = payload["value"]
        accepted = (
            type(value) is str
            and bool(__import__("re").fullmatch(r"(?:0|-?[1-9][0-9]*)", value))
            and -(2**63) <= int(value) <= 2**63 - 1
        )
        return {"accepted": accepted}
    if kind == "common_rule_profile":
        facts = payload["configured_facts"]
        verified = set(payload["hypothetical_verification"]["proved_predicates"])
        derived = _common_rule(facts, verified)
        if derived["status"] != "accepted":
            return derived
        submitted = payload["submitted_report"]
        if submitted != _parse(derived["report_bytes"]):
            return {"status": "rejected", "code": "host_capability_mismatch"}
        return {"status": "accepted", "report_bytes": _compact(submitted)}
    if kind == "profile":
        return _common_rule(payload["configured_facts"], set())
    if kind == "worker_claim_check":
        if not _WORKER_MODE:
            raise NotImplementedError("worker claim checks unavailable")
        item = payload["input"]
        authority = _seed(item["ledger_before"])
        dispatched: list[str] = []
        accepted = authority.check_worker_claim(
            item["claim"],
            item["authenticated_principal"],
            item["trusted_clock_now"],
            phase=item["phase"],
            on_dispatch=lambda: dispatched.append("called"),
        )
        return {
            "binding": _binding(),
            "accepted": accepted,
            "ledger_after": _observed_ledger(authority),
            "host_mutation_count": 0,
            "external_dispatch_count": len(dispatched),
        }
    raise ValueError(f"unknown driver kind: {kind}")


def _common_rule(facts: dict[str, Any], verified: set[str]) -> dict[str, Any]:
    return _compose_authority_profile(
        facts, verified, scope_identity="scope-42", authority_epoch="2", scope_generation="4"
    )


if __name__ == "__main__":
    try:
        print(_compact(run(_parse(sys.stdin.read()))))
    except BaseException as exc:
        print(f"authority adapter failed: {exc}", file=sys.stderr)
        raise
