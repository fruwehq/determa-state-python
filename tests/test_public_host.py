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
