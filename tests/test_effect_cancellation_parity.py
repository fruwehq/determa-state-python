"""Late cancellation validates pinned payloads and preserves terminal evidence."""

import copy
import json
import sqlite3

import pytest

from determa.state.effects import EffectError
from tests.test_committed_effects import (
    authority_claim_request,
    authority_effect_fixture,
    installed_test_handler,
)


def database_snapshot(host):
    with sqlite3.connect(host.path) as connection:
        return tuple(connection.iterdump())


def invoked_host(tmp_path, state):
    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    command, invocation = authority_claim_request(authority, scope, root, record)
    result = json.loads(authority.perform(command, invocation))
    assert result["status"] == "accepted"
    claim = result["claim"]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    host.trusted_clock = lambda: "0"
    calls = []

    def sdk_call(*args):
        calls.append(args)
        return {"provider_reference": record["handler_reference"]["identifier"]}

    host = installed_test_handler(host, sdk_call)
    context = {
        "principal": claim["worker_principal"],
        "scope": scope,
        "epoch": "0",
        "trusted_now": "0",
    }
    candidate = host.dispatch(
        root, record["effect_id"], credential="native-test-credential", **context
    )
    assert len(calls) == 1
    request = {
        "effect_id": record["effect_id"],
        "operation_token": record["operation_token"],
        "attempt_fence": claim["attempt_fence"],
        "outcome_kind": "succeeded",
        "payload": ["map", [["provider_reference", ["string", candidate["provider_reference"]]]]],
    }
    if state == "outcome_recorded":
        host._record_result(root, request, **context)
    elif state == "result_admitted":
        assert host.submit_result(root, request, **context)["status"] == "committed"
    else:
        assert state == "possible"
    return host, root, record, calls


def cancellation(record, operation="late-cancel"):
    return {
        "operation_id": operation,
        "effect_id": record["effect_id"],
        "reason": "owner-stop",
        "payload": ["map", []],
    }


@pytest.mark.parametrize("state", ["outcome_recorded", "result_admitted"])
def test_terminal_winner_returns_too_late_and_preserves_exact_record(tmp_path, state):
    host, root, record, calls = invoked_host(tmp_path, state)
    before = host.snapshot(root)
    winning = copy.deepcopy(before["journal"]["effect_records"][0])
    request = cancellation(record)
    reply = host.cancel(root, request)
    assert reply["status"] == "committed"
    assert reply["cancellation"]["state"] == "too_late"
    assert reply["outcome"] == winning["outcome"]
    assert reply["result_event_id"] == winning["result_event_id"]
    after = host.snapshot(root)
    assert after["checkpoint"] == before["checkpoint"]
    assert after["journal"]["effect_records"][0] == winning
    retained = database_snapshot(host)
    assert host.cancel(root, request) == reply
    assert database_snapshot(host) == retained
    assert len(calls) == 1


@pytest.mark.parametrize("state", ["possible", "outcome_recorded", "result_admitted"])
def test_invalid_late_cancellation_payload_preserves_all_native_roles(tmp_path, state):
    host, root, record, calls = invoked_host(tmp_path, state)
    request = cancellation(record)
    request["payload"] = ["map", [["undeclared", ["string", "value"]]]]
    before = database_snapshot(host)
    with pytest.raises(EffectError, match="invalid_host_request"):
        host.cancel(root, request)
    assert database_snapshot(host) == before
    assert len(calls) == 1


def test_new_late_cancel_preserves_original_prevented_start_evidence(tmp_path):
    _, host, _, root, record = authority_effect_fixture(tmp_path)
    first_request = cancellation(record, "cancel-first")
    first = host.cancel(root, first_request)
    assert first["cancellation"]["state"] == "prevented_start"
    before = host.snapshot(root)
    reply = host.cancel(root, cancellation(record, "cancel-late"))
    assert reply["status"] == "committed"
    assert reply["cancellation"]["state"] == "too_late"
    assert reply["outcome"] == first["outcome"]
    assert host.snapshot(root)["journal"]["effect_records"] == before["journal"]["effect_records"]
