from __future__ import annotations

import copy

import pytest

from determa.state.public_client import (
    EndpointBinding,
    PublicHostClient,
    PublicHostError,
    public_request_digest,
)
from determa.state.wire import hash_value


def request(operation_id="create-1"):
    return {
        "protocol": "determa.execution_host",
        "protocol_version": 1,
        "operation_id": operation_id,
        "scope_binding_identity": None,
        "operation": "create",
        "target": {"root_instance_id": "root", "runtime_id": None, "runtime_incarnation": None},
        "precondition": None,
        "arguments": {
            "validated_bundle_fingerprint": "sha256:" + "a" * 64,
            "namespace": "example",
            "machine_id": "counter",
            "machine_version": "1",
            "root_instance_id": "root",
            "creation_id": "root:create",
            "bindings": ["map", []],
        },
    }


def capabilities(candidate):
    result = {
        "scope_binding_identity": "immutable-binding-1",
        "supported_operations": ["capabilities", "create", "receipt"],
        "supported_scope_actions": [],
        "supported_determa_capabilities": [],
        "supported_timer_commands": [],
        "extension_reports": [],
        "authority_profile": None,
        "guarantees": {
            "inspection_structural": False,
            "inspection_semantic": False,
            "retained_history": False,
            "saved_response_replay": False,
            "deterministic_reexecution": False,
        },
    }
    result["profile_digest"] = hash_value(
        ["determa-public-host-profile-1", "1", "immutable-binding-1", result]
    )
    return {
        "protocol": "determa.execution_host",
        "protocol_version": 1,
        "operation_id": candidate["operation_id"],
        "status": "committed",
        "receipt": None,
        "value": {"operation": "capabilities", "result": result},
        "error": None,
    }


def rejected(candidate):
    return {
        "protocol": "determa.execution_host",
        "protocol_version": 1,
        "operation_id": candidate["operation_id"],
        "status": "rejected",
        "receipt": None,
        "value": None,
        "error": {"operation": candidate["operation"], "code": "host_capability_mismatch"},
    }


def test_lost_response_pins_request_and_endpoint_across_restart_and_alias_change(tmp_path):
    calls = []

    def transport(endpoint, candidate):
        calls.append((endpoint, copy.deepcopy(candidate)))
        if candidate["operation"] == "capabilities":
            return capabilities(candidate)
        raise TimeoutError("response lost")

    path = tmp_path / "client.db"
    client = PublicHostClient(
        path, {"one": EndpointBinding("old-endpoint", "old-scope")}, transport
    )
    client.setup_schema()
    with pytest.raises(TimeoutError):
        client.submit("one", request())
    first = calls[-1]
    assert first[1]["scope_binding_identity"] == "immutable-binding-1"
    assert public_request_digest(first[1]).startswith("sha256:")
    assert len(calls) == 2

    def restarted_transport(endpoint, candidate):
        calls.append((endpoint, copy.deepcopy(candidate)))
        return rejected(candidate)

    restarted = PublicHostClient(
        path, {"one": EndpointBinding("new-endpoint", "new-scope")}, restarted_transport
    )
    response = restarted.submit("one", request())
    assert response["status"] == "rejected"
    assert calls[-1] == first
    assert len(calls) == 3  # No new discovery after the lost response.
    assert restarted.retry("create-1") == response
    assert len(calls) == 3  # The saved complete response is returned without transport.


def test_unequal_operation_reuse_fails_without_transport(tmp_path):
    calls = []

    def transport(endpoint, candidate):
        calls.append(candidate)
        return (
            capabilities(candidate)
            if candidate["operation"] == "capabilities"
            else rejected(candidate)
        )

    client = PublicHostClient(
        tmp_path / "client.db", {"one": EndpointBinding("endpoint", "scope")}, transport
    )
    client.setup_schema()
    client.submit("one", request())
    changed = request()
    changed["arguments"]["creation_id"] = "another-creation"
    with pytest.raises(PublicHostError, match="operation_id_conflict"):
        client.submit("one", changed)
    assert len(calls) == 2


def test_unknown_operation_is_not_rollback_or_automatic_replacement(tmp_path):
    def forbidden_transport(*_args):
        raise AssertionError("unknown local work cannot be sent")

    client = PublicHostClient(tmp_path / "client.db", {}, forbidden_transport)
    client.setup_schema()
    with pytest.raises(PublicHostError, match="outcome_unknown"):
        client.retry("missing")
    with pytest.raises(PublicHostError, match="outcome_unknown"):
        client.receipt("missing")


def test_native_client_journal_preserves_request_and_first_response(tmp_path):
    import sqlite3

    client = PublicHostClient(
        tmp_path / "client.db",
        {"one": EndpointBinding("endpoint", "scope")},
        lambda _endpoint, candidate: (
            capabilities(candidate)
            if candidate["operation"] == "capabilities"
            else rejected(candidate)
        ),
    )
    client.setup_schema()
    response = client.submit("one", request())
    with sqlite3.connect(client.path) as db:
        for mutation in (
            "DELETE FROM determa_public_client_requests",
            "UPDATE determa_public_client_requests SET endpoint='replacement'",
            "UPDATE determa_public_client_requests SET request=X'00'",
            "UPDATE determa_public_client_requests SET response=NULL",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="public_client_immutable"):
                db.execute(mutation)
    assert client.retry("create-1") == response


def test_client_with_missing_retention_guard_refuses_before_transport(tmp_path):
    import sqlite3

    client = PublicHostClient(
        tmp_path / "client.db",
        {"one": EndpointBinding("endpoint", "scope")},
        lambda *_args: pytest.fail("changed native storage must refuse before transport"),
    )
    client.setup_schema()
    with sqlite3.connect(client.path) as db:
        db.execute("DROP TRIGGER determa_public_client_guard_update")
    with pytest.raises(PublicHostError, match="host_capability_mismatch"):
        client.submit("one", request())
