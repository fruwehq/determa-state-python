from __future__ import annotations

import copy

import determa.state as ds
from determa.state.public_client import validate_public_message
from determa.state.public_host import SQLitePublicExecutionHost

MACHINE = """
format: 1
namespace: example
events:
  increment:
    direction: input
    payload:
      amount: {type: int, required: true}
machines:
  - machine_id: counter
    version: 1
    root:
      variables:
        count: {type: int, init: 0}
      on_events:
        increment:
          action:
            - assign: {count: "count + event.payload.amount"}
"""


def base(operation, operation_id, root="root"):
    return {
        "protocol": "determa.execution_host",
        "protocol_version": 1,
        "scope_binding_identity": "binding-1",
        "operation": operation,
        "operation_id": operation_id,
        "target": {"root_instance_id": root, "runtime_id": None, "runtime_incarnation": None},
        "precondition": None,
        "arguments": {},
    }


def open_host(tmp_path):
    bundle = ds.load_bundle(MACHINE)
    host = SQLitePublicExecutionHost(
        tmp_path / "host.db",
        scope_alias="scope",
        scope_binding_identity="binding-1",
        authorized_principals=frozenset({"alice"}),
        resolver=ds.MemoryArtifactResolver(definitions={bundle.fingerprint: bundle}),
    )
    host.setup_schema()
    return host, bundle


def creation(bundle):
    request = base("create", "operation-create")
    request["arguments"] = {
        "validated_bundle_fingerprint": bundle.fingerprint,
        "namespace": "example",
        "machine_id": "counter",
        "machine_version": "1",
        "root_instance_id": "root",
        "creation_id": "root:create",
        "bindings": ["map", []],
    }
    return request


def test_public_host_commits_mailbox_processing_and_exact_saved_response(tmp_path):
    host, bundle = open_host(tmp_path)
    request = creation(bundle)
    response = host.handle(request, principal="alice")
    validate_public_message(response, response=True)
    assert response["status"] == "committed"
    assert response["receipt"] is not None
    checkpoint = response["value"]["result"]["checkpoint"]
    aggregate = checkpoint["root_record"]["aggregate_state"]
    root = aggregate["runtimes"][0]
    envelope = ds.portable_envelope("increment", "event-1", root["target_identity"], {"amount": 4})
    admit = base("admit", "operation-admit")
    admit["arguments"] = {
        "ordered_deliveries": [
            {
                "delivery_mode": "input",
                "envelope": envelope,
                "envelope_digest": ds.delivery_request_digest("root", "input", envelope),
            }
        ]
    }
    admit["precondition"] = {
        "revision": checkpoint["revision"],
        "checkpoint_digest": checkpoint["execution_checkpoint_digest"],
    }
    admitted = host.handle(admit, principal="alice")
    assert admitted["status"] == "committed"
    checkpoint = admitted["value"]["result"]["checkpoint"]
    process = base("process", "operation-process")
    process["target"].update(
        runtime_id=root["runtime_id"], runtime_incarnation=root["identity_origin"]
    )
    process["precondition"] = {
        "revision": checkpoint["revision"],
        "checkpoint_digest": checkpoint["execution_checkpoint_digest"],
    }
    completed = host.handle(process, principal="alice")
    assert completed["status"] == "committed"
    assert completed["value"]["result"]["core_result"]["disposition"] == "handled"
    reopened, _ = open_host(tmp_path)
    assert reopened.handle(request, principal="alice") == response
    assert reopened.handle(process, principal="alice") == completed
    changed = copy.deepcopy(process)
    changed["precondition"]["revision"] = "99"
    assert reopened.handle(changed, principal="alice")["error"]["code"] == "operation_id_conflict"
    observed = reopened.handle(base("read", "read-1"), principal="alice")
    checkpoint = observed["value"]["result"]["checkpoint"]
    count = checkpoint["root_record"]["aggregate_state"]["runtimes"][0]["variables"][0]["value"]
    assert count == ["integer", "4"]
    assert checkpoint["revision"] == "2"


def test_unauthorized_existing_and_absent_roots_have_same_denial(tmp_path):
    host, bundle = open_host(tmp_path)
    host.handle(creation(bundle), principal="alice")
    existing = host.handle(base("read", "read-denied", "root"), principal="mallory")
    absent = host.handle(base("read", "read-denied", "absent"), principal="mallory")
    assert existing == absent
    assert existing["error"]["code"] == "unauthorized_scope"


def test_empty_mailbox_process_replays_after_later_admission(tmp_path, monkeypatch):
    host, bundle = open_host(tmp_path)
    checkpoint = host.handle(creation(bundle), principal="alice")["value"]["result"]["checkpoint"]
    aggregate = checkpoint["root_record"]["aggregate_state"]
    root = aggregate["runtimes"][0]
    idle = base("process", "idle-process")
    idle["target"].update(
        runtime_id=root["runtime_id"], runtime_incarnation=root["identity_origin"]
    )
    idle["precondition"] = {
        "revision": checkpoint["revision"],
        "checkpoint_digest": checkpoint["execution_checkpoint_digest"],
    }
    original = host.handle(idle, principal="alice")
    assert original["value"]["result"]["core_result"]["disposition"] == "not_runnable"
    envelope = ds.portable_envelope("increment", "later", root["target_identity"], {"amount": 4})
    admission = base("admit", "later-admission")
    admission["precondition"] = idle["precondition"]
    admission["arguments"] = {
        "ordered_deliveries": [
            {
                "delivery_mode": "input",
                "envelope": envelope,
                "envelope_digest": ds.delivery_request_digest("root", "input", envelope),
            }
        ]
    }
    assert host.handle(admission, principal="alice")["status"] == "committed"

    def forbidden_core(*_args, **_kwargs):
        raise AssertionError("saved process must not call core")

    monkeypatch.setattr("determa.state.public_host.step_checkpoint_v1", forbidden_core)
    assert host.handle(idle, principal="alice") == original


def test_creation_identity_replay_under_new_operation_never_reinitializes(tmp_path, monkeypatch):
    host, bundle = open_host(tmp_path)
    request = creation(bundle)
    first = host.handle(request, principal="alice")

    def forbidden_core(*_args, **_kwargs):
        raise AssertionError("retained creation must not call core")

    monkeypatch.setattr("determa.state.public_host.create_checkpoint_v1", forbidden_core)
    repeated = copy.deepcopy(request)
    repeated["operation_id"] = "new-public-operation"
    replay = host.handle(repeated, principal="alice")
    assert replay["value"] == first["value"]
    assert replay["receipt"]["operation_id"] == "new-public-operation"


def test_malformed_read_target_rejected_before_root_lookup(tmp_path):
    host, bundle = open_host(tmp_path)
    host.handle(creation(bundle), principal="alice")
    request = base("read", "malformed-target")
    request["target"]["runtime_id"] = "sha256:" + "f" * 64
    request["target"]["runtime_incarnation"] = {
        "kind": "root",
        "root_instance_id": "root",
        "definition": {
            "validated_bundle_fingerprint": bundle.fingerprint,
            "machine": {
                "namespace": "example",
                "machine_id": "counter",
                "machine_version": "1",
                "root_definition_pointer": "/machines/0/root",
            },
        },
    }
    assert host.handle(request, principal="alice")["error"]["code"] == "invalid_host_request"


def test_client_rejects_mismatched_nested_receipt_evidence(tmp_path):
    import pytest

    from determa.state.public_client import EndpointBinding, PublicHostClient, PublicHostError

    host, bundle = open_host(tmp_path)

    def transport(_endpoint, request):
        response = host.handle(request, principal="alice")
        if request["operation"] == "create":
            raise TimeoutError("committed response lost")
        if request["operation"] == "receipt":
            response["value"]["result"]["saved_response"]["receipt"]["request_digest"] = (
                "sha256:" + "f" * 64
            )
        return response

    client = PublicHostClient(
        tmp_path / "client.db", {"one": EndpointBinding("host", "scope")}, transport
    )
    client.setup_schema()
    with pytest.raises(TimeoutError):
        client.submit("one", creation(bundle))
    with pytest.raises(PublicHostError, match="invalid_host_request"):
        client.receipt("operation-create")


def test_native_response_insert_failure_rolls_back_checkpoint_and_ledger(tmp_path, monkeypatch):
    import sqlite3

    import pytest

    host, bundle = open_host(tmp_path)
    original_connect = sqlite3.connect

    class FailingConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if sql.startswith("INSERT INTO determa_public_host_responses"):
                raise sqlite3.OperationalError("injected response write failure")
            return super().execute(sql, parameters)

    monkeypatch.setattr(
        "determa.state.public_host.sqlite3.connect",
        lambda path: original_connect(path, factory=FailingConnection),
    )
    with pytest.raises(sqlite3.OperationalError):
        host.handle(creation(bundle), principal="alice")
    monkeypatch.undo()
    observed = host.handle(base("read", "after-failure"), principal="alice")
    assert observed["value"]["result"]["checkpoint"] is None
    with original_connect(host.path) as db:
        assert db.execute("SELECT COUNT(*) FROM determa_public_host_responses").fetchone()[0] == 0


def test_native_commit_with_lost_fate_reconciles_to_first_response(tmp_path, monkeypatch):
    import sqlite3

    import pytest

    host, bundle = open_host(tmp_path)
    original_connect = sqlite3.connect

    class LostCommitConnection(sqlite3.Connection):
        def __exit__(self, *args):
            result = super().__exit__(*args)
            if args[0] is None:
                raise sqlite3.OperationalError("commit response lost after native commit")
            return result

    monkeypatch.setattr(
        "determa.state.public_host.sqlite3.connect",
        lambda path: original_connect(path, factory=LostCommitConnection),
    )
    request = creation(bundle)
    with pytest.raises(sqlite3.OperationalError):
        host.handle(request, principal="alice")
    monkeypatch.undo()

    def forbidden_core(*_args, **_kwargs):
        raise AssertionError("unknown native commit must reconcile without core execution")

    monkeypatch.setattr("determa.state.public_host.create_checkpoint_v1", forbidden_core)
    result = host.handle(request, principal="alice")
    assert result["status"] == "committed"
    assert result["receipt"] is not None
    with original_connect(host.path) as db:
        assert db.execute("SELECT COUNT(*) FROM determa_public_host_responses").fetchone()[0] == 1


def test_changed_native_schema_refuses_before_mutation(tmp_path):
    import sqlite3

    host, bundle = open_host(tmp_path)
    with sqlite3.connect(host.path) as db:
        db.execute("DROP TRIGGER determa_public_host_responses_forbid_delete")
    response = host.handle(creation(bundle), principal="alice")
    assert response["error"]["code"] == "host_capability_mismatch"
    with sqlite3.connect(host.path) as db:
        assert db.execute("SELECT COUNT(*) FROM determa_public_host_checkpoints").fetchone()[0] == 0
