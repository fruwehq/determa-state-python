from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from determa.state import load_bundle
from determa.state.timers import SQLiteTimerHelper, timer_request_digest

_ROOT_RUNTIME = "sha256:9b0e3238e782cb7d912febc1acc4e4c03a121d201764d0bc9658b969feac4dc8"


def bundle():
    return load_bundle(
        {
            "format": 1,
            "namespace": "timer.tests",
            "events": {"received": {"direction": "input"}},
            "machines": [
                {
                    "machine_id": "timer",
                    "root": {
                        "type": "composite",
                        "initial": {"transition_to": "waiting"},
                        "states": {"waiting": {"on_events": {"received": {}}}},
                    },
                }
            ],
        }
    )


def request(operation="schedule", operation_id="schedule-A", arguments=None):
    value = {
        "interface": "determa.timer_helper",
        "interface_version": 1,
        "scope_identity": "scope-archive-example",
        "root_instance_id": "server-1",
        "root_runtime_id": _ROOT_RUNTIME,
        "timer_id": "timer-A",
        "operation": operation,
        "operation_id": operation_id,
        "arguments": arguments
        if arguments is not None
        else {
            "clock_basis": "unix_nanoseconds",
            "delay_nanoseconds": "10",
            "event_name": "received",
            "payload": ["map", []],
            "correlation_id": None,
        },
    }
    value["request_digest"] = timer_request_digest(value)
    return value


def helper(path: Path, clock):
    value = request()
    return SQLiteTimerHelper(
        path,
        bundle(),
        scope_identity=value["scope_identity"],
        root_instance_id=value["root_instance_id"],
        root_runtime_id=value["root_runtime_id"],
        principals=frozenset({"operator"}),
        trusted_clock=clock,
    )


def test_committed_schedule_survives_restart_and_replays_original_result_after_cancel(tmp_path):
    calls = []
    initial = helper(tmp_path / "timer.sqlite", lambda: calls.append("clock") or "100")
    initial.setup_schema()
    scheduled = initial.execute(request(), principal="operator")
    assert scheduled["status"] == "accepted"
    assert initial.snapshot()["records"][0]["deadline_at"] == "110"
    assert calls == ["clock"]

    def offline():
        raise AssertionError("retained replay/cancel/read must not access the clock")

    restarted = helper(tmp_path / "timer.sqlite", offline)
    cancelled = restarted.execute(
        request("cancel", "cancel-A", {"expected_revision": "1"}), principal="operator"
    )
    assert cancelled["status"] == "accepted" and cancelled["record_revision"] == "2"
    before = restarted.snapshot()
    assert restarted.execute(request(), principal="operator") == scheduled
    assert restarted.snapshot() == before
    read = restarted.execute(request("read_timer", "read-A", {}), principal="operator")
    assert read["record_revision"] == "2" and read["event_id"] == scheduled["event_id"]
    assert restarted.snapshot() == before


@pytest.mark.parametrize(
    "time", ["-0", "+1", "01", "1.5", "1e2", "9223372036854775808", "-1", 1, True]
)
def test_invalid_duration_changes_no_records_and_never_reads_clock(tmp_path, time):
    def forbidden_clock():
        raise AssertionError("invalid time reached the clock")

    installed = helper(tmp_path / "timer.sqlite", forbidden_clock)
    installed.setup_schema()
    before = installed.snapshot()
    value = request()
    value["arguments"]["delay_nanoseconds"] = time
    value["request_digest"] = timer_request_digest(value)
    result = installed.execute(value, principal="operator")
    assert result["error_code"] == "invalid_timer_time"
    assert installed.snapshot() == before


def test_target_incarnation_and_principal_are_authorized_before_replay(tmp_path):
    installed = helper(tmp_path / "timer.sqlite", lambda: "100")
    installed.setup_schema()
    assert installed.execute(request(), principal="operator")["status"] == "accepted"
    before = installed.snapshot()
    for value, principal in [(request(), "stranger"), (request(), "operator")]:
        if principal == "operator":
            value["root_runtime_id"] = "sha256:" + "0" * 64
            value["request_digest"] = timer_request_digest(value)
        result = installed.execute(value, principal=principal)
        assert result["error_code"] == "unauthorized_timer_scope"
        assert result["record_revision"] is None and result["event_id"] is None
    assert installed.snapshot() == before


def test_operation_conflict_preserves_exact_durable_schedule(tmp_path):
    installed = helper(tmp_path / "timer.sqlite", lambda: "100")
    installed.setup_schema()
    value = request()
    assert installed.execute(value, principal="operator")["status"] == "accepted"
    before = installed.snapshot()
    changed = copy.deepcopy(value)
    changed["arguments"]["delay_nanoseconds"] = "20"
    changed["request_digest"] = timer_request_digest(changed)
    assert (
        installed.execute(changed, principal="operator")["error_code"] == "timer_operation_conflict"
    )
    assert installed.snapshot() == before


def test_invalid_record_digest_refuses_read_after_restart(tmp_path):
    import sqlite3

    path = tmp_path / "timer.sqlite"
    installed = helper(path, lambda: "100")
    installed.setup_schema()
    assert installed.execute(request(), principal="operator")["status"] == "accepted"
    damaged = installed.snapshot()
    damaged["records"][0]["deadline_at"] = "999"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE determa_timer_helpers SET document=?", (json.dumps(damaged).encode(),)
        )
    restarted = helper(path, lambda: "100")
    result = restarted.execute(request("read_timer", "read-A", {}), principal="operator")
    assert result["error_code"] == "timer_capability_mismatch"
