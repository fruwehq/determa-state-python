from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from determa.state import load_bundle
from determa.state.timers import (
    SQLiteTimerHelper,
    TimerError,
    seal_timer_records,
    timer_request_digest,
)

_ROOT_RUNTIME = "sha256:9b0e3238e782cb7d912febc1acc4e4c03a121d201764d0bc9658b969feac4dc8"


@pytest.mark.parametrize("damage", ["record", "receipt", "unretained_record"])
def test_resealed_portable_records_do_not_replace_retained_native_evidence(tmp_path, damage):
    import sqlite3

    from determa.state.wire import canonical_bytes

    installed = helper(tmp_path / "native-evidence.sqlite", lambda: "100")
    installed.setup_schema()
    assert installed.execute(request(), principal="operator")["status"] == "accepted"
    artifact = installed.snapshot()
    if damage == "record":
        artifact["records"][0]["deadline_at"] = "999"
    elif damage == "receipt":
        artifact["operation_receipts"][0]["result"]["record_revision"] = "999"
    else:
        artifact["operation_receipts"] = []
    with sqlite3.connect(installed.path) as connection:
        connection.execute(
            "UPDATE determa_timer_helpers SET document=?",
            (canonical_bytes(seal_timer_records(artifact)),),
        )
    result = installed.execute(request("read_timer", "read-A", {}), principal="operator")
    assert result["error_code"] == "timer_capability_mismatch"


def test_native_origin_is_storage_bound_and_setup_cannot_repair_missing_evidence(tmp_path):
    import sqlite3

    original = helper(tmp_path / "original.sqlite", lambda: "100")
    original.setup_schema()
    original.execute(request(), principal="operator")
    before = original.snapshot()
    copied_path = tmp_path / "copied.sqlite"
    with sqlite3.connect(original.path) as source, sqlite3.connect(copied_path) as copied:
        source.backup(copied)
    inert = helper(copied_path, lambda: "100")
    with pytest.raises(TimerError, match="timer_capability_mismatch"):
        inert.snapshot()
    with sqlite3.connect(copied_path) as connection:
        connection.execute("DROP TABLE determa_timer_origin")
        recorded = connection.execute("SELECT document FROM determa_timer_helpers").fetchall()
    with pytest.raises(TimerError, match="timer_capability_mismatch"):
        inert.setup_schema()
    with sqlite3.connect(copied_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='determa_timer_origin'"
        ).fetchone() == (0,)
        assert (
            connection.execute("SELECT document FROM determa_timer_helpers").fetchall() == recorded
        )
    assert original.snapshot() == before


def test_native_operation_history_is_immutable_and_configuration_changes_fail_closed(tmp_path):
    import sqlite3

    installed = helper(tmp_path / "configuration.sqlite", lambda: "100")
    installed.setup_schema()
    installed.execute(request(), principal="operator")
    with sqlite3.connect(installed.path) as connection:
        for statement in (
            "DELETE FROM determa_timer_commits",
            "UPDATE determa_timer_commits SET operation_id='changed'",
            "DELETE FROM determa_timer_origin",
            "UPDATE determa_timer_origin SET configuration=X'00'",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="timer_native_evidence_immutable"):
                connection.execute(statement)
    installed.claim_lease_nanoseconds += 1
    result = installed.execute(request("read_timer", "read-A", {}), principal="operator")
    assert result["error_code"] == "timer_capability_mismatch"


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


def worker_helper(path, clock):
    return SQLiteTimerHelper(
        path,
        bundle(),
        scope_identity="scope-archive-example",
        root_instance_id="server-1",
        root_runtime_id=_ROOT_RUNTIME,
        principals=frozenset({"operator", "worker-A", "worker-B"}),
        worker_principals=frozenset({"worker-A", "worker-B"}),
        trusted_clock=clock,
    )


def test_due_claim_is_durable_and_replay_does_not_recheck_clock_or_revision(tmp_path):
    path = tmp_path / "timers.sqlite"
    now = ["100"]
    installed = worker_helper(path, lambda: now[0])
    installed.setup_schema()
    installed.execute(request(), principal="operator")
    claim = request(
        "claim_fire",
        "claim-A",
        {
            "expected_revision": "1",
            "clock_basis": "unix_nanoseconds",
            "worker_principal": "worker-A",
        },
    )
    before = installed.snapshot()
    now[0] = "109"
    assert installed.execute(claim, principal="worker-A")["error_code"] == "timer_not_due"
    assert installed.snapshot() == before
    now[0] = "110"
    result = installed.execute(claim, principal="worker-A")
    assert result["status"] == "accepted" and result["attempt_fence"] == "1"
    assert result["record_revision"] == "2" and result["expires_at"] == "130"
    committed = installed.snapshot()

    def offline():
        raise AssertionError("retained claim replay read the clock")

    restarted = worker_helper(path, offline)
    assert restarted.execute(claim, principal="worker-A") == result
    assert restarted.snapshot() == committed
    assert (
        restarted.execute(
            request("cancel", "cancel-A", {"expected_revision": "2"}), principal="operator"
        )["error_code"]
        == "timer_fire_in_progress"
    )
    assert restarted.snapshot() == committed


def test_expired_claim_without_commit_fate_proof_never_grants_a_new_fence(tmp_path):
    path = tmp_path / "timers.sqlite"
    now = ["100"]
    installed = worker_helper(path, lambda: now[0])
    installed.setup_schema()
    installed.execute(request(), principal="operator")
    now[0] = "110"
    claim = request(
        "claim_fire",
        "claim-A",
        {
            "expected_revision": "1",
            "clock_basis": "unix_nanoseconds",
            "worker_principal": "worker-A",
        },
    )
    assert installed.execute(claim, principal="worker-A")["status"] == "accepted"
    before = installed.snapshot()
    retry = request(
        "claim_fire",
        "retry-B",
        {
            "expected_revision": "2",
            "clock_basis": "unix_nanoseconds",
            "worker_principal": "worker-B",
        },
    )
    now[0] = "131"
    refused = worker_helper(path, lambda: now[0]).execute(retry, principal="worker-B")
    assert refused["error_code"] == "delivery_ambiguous" and refused["attempt_fence"] == "1"
    assert installed.snapshot() == before
    assert installed.execute(retry, principal="worker-A")["error_code"] == "timer_worker_mismatch"
    assert installed.snapshot() == before


@pytest.mark.parametrize("encoding", ["duplicate", "noncanonical", "invalid"])
def test_stored_artifact_requires_strict_canonical_json(tmp_path, encoding):
    import sqlite3

    from determa.state.wire import canonical_bytes

    path = tmp_path / "timers.sqlite"
    installed = helper(path, lambda: "100")
    installed.setup_schema()
    installed.execute(request(), principal="operator")
    document = installed.snapshot()
    raw = canonical_bytes(document)
    if encoding == "duplicate":
        raw = b'{"records":[],' + raw[1:]
    elif encoding == "noncanonical":
        raw = json.dumps(document, indent=2).encode()
    else:
        raw = b"invalid"
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE determa_timer_helpers SET document=?", (raw,))
    assert (
        installed.execute(request("read_timer", "read-A", {}), principal="operator")["error_code"]
        == "timer_capability_mismatch"
    )
