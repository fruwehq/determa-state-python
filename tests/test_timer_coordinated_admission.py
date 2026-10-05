from __future__ import annotations

import copy
import multiprocessing
import os
import signal

import pytest

from determa.state import ExecutionHost, MemoryArtifactResolver, SQLiteExecutionStore
from determa.state.checkpoint_v1 import admit_checkpoint_v1
from determa.state.host import delivery_request_digest
from determa.state.timers import SQLiteTimerHelper, timer_request_digest
from determa.state.wire import hash_value

from .test_timers import bundle, request


def _killed_timer_worker(path, complete, boundary, control):
    definition = bundle()
    host = ExecutionHost(
        SQLiteExecutionStore(
            path, replay_retention="permanent", shared_application_transactions=True
        ),
        MemoryArtifactResolver(definitions={definition.fingerprint: definition}),
    )
    helper = SQLiteTimerHelper(
        path,
        definition,
        scope_identity=complete["scope_identity"],
        root_instance_id=complete["root_instance_id"],
        root_runtime_id=complete["root_runtime_id"],
        principals=frozenset({"operator", "worker-A"}),
        worker_principals=frozenset({"worker-A"}),
        trusted_clock=lambda: "120",
        coordinated_host=host,
    )

    def pause():
        control.send(boundary)
        control.recv()
        raise AssertionError("the parent must kill this process at the selected boundary")

    if boundary == "both_writes_staged":
        persist = helper._persist

        def pause_after_staging(sql, artifact):
            persist(sql, artifact)
            pause()

        helper._persist = pause_after_staging
    else:

        def pause_after_commit(observed):
            if observed == "after_commit_before_response":
                pause()

        host.fault_injector = pause_after_commit
    helper.execute(complete, principal="worker-A")


@pytest.mark.skipif(os.name != "posix", reason="requires actual SIGKILL process termination")
@pytest.mark.parametrize("boundary", ["both_writes_staged", "after_commit_before_response"])
def test_sigkill_restart_resolves_actual_native_timer_and_admission_fate(tmp_path, boundary):
    helper, host, complete, expected, now, calls = configured(tmp_path)
    before_helper = helper.snapshot()
    before_checkpoint = host.read_checkpoint("server-1").document
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    worker = context.Process(
        target=_killed_timer_worker, args=(helper.path, complete, boundary, child)
    )
    worker.start()
    child.close()
    try:
        assert parent.poll(30), "worker never reached the selected native boundary"
        assert parent.recv() == boundary
        worker.kill()
        worker.join(10)
        assert worker.exitcode == -signal.SIGKILL
    finally:
        if worker.is_alive():
            worker.kill()
            worker.join(10)
        parent.close()
        worker.close()

    # Reopen all production objects: no child response or exception is recovery proof.
    restarted_host = ExecutionHost(
        SQLiteExecutionStore(
            helper.path, replay_retention="permanent", shared_application_transactions=True
        ),
        host.artifact_resolver,
    )
    restarted = SQLiteTimerHelper(
        helper.path,
        helper.bundle,
        scope_identity=helper.scope_identity,
        root_instance_id=helper.root_instance_id,
        root_runtime_id=helper.root_runtime_id,
        principals=helper.principals,
        worker_principals=helper.worker_principals,
        trusted_clock=lambda: "120",
        coordinated_host=restarted_host,
    )
    if boundary == "both_writes_staged":
        assert restarted.snapshot() == before_helper
        assert restarted_host.read_checkpoint("server-1").document == before_checkpoint
    else:
        assert restarted.snapshot()["records"][0]["state"] == "fired"
        assert restarted_host.read_checkpoint("server-1").document == expected

        def offline():
            raise AssertionError("committed replay must not read the clock")

        restarted.trusted_clock = offline
    result = restarted.execute(complete, principal="worker-A")
    assert result["status"] == "accepted" and result["delivery_state"] == "admitted"
    committed = restarted.snapshot()
    assert restarted_host.read_checkpoint("server-1").document == expected
    assert restarted.execute(complete, principal="worker-A") == result
    assert restarted.snapshot() == committed
    assert restarted_host.read_checkpoint("server-1").document == expected


def configured(tmp_path):
    path = tmp_path / "coordinated.sqlite"
    definition = bundle()
    resolver = MemoryArtifactResolver(definitions={definition.fingerprint: definition})
    store = SQLiteExecutionStore(
        path, replay_retention="permanent", shared_application_transactions=True
    )
    store.setup_schema()
    host = ExecutionHost(store, resolver)
    host.create_v1(definition, "timer", "server-1", "create-timer", {})
    checkpoint = host.read_checkpoint("server-1").document
    runtime = checkpoint["root_record"]["aggregate_state"]["root_runtime_id"]
    now = ["100"]
    clock_calls = []

    def clock():
        clock_calls.append(now[0])
        return now[0]

    helper = SQLiteTimerHelper(
        path,
        definition,
        scope_identity="scope-archive-example",
        root_instance_id="server-1",
        root_runtime_id=runtime,
        principals=frozenset({"operator", "worker-A"}),
        worker_principals=frozenset({"worker-A"}),
        trusted_clock=clock,
        coordinated_host=host,
    )
    helper.setup_schema()

    def command(operation, identifier, arguments=None):
        value = request(operation, identifier, arguments)
        value["root_runtime_id"] = runtime
        value["request_digest"] = timer_request_digest(value)
        return value

    assert (
        helper.execute(command("schedule", "schedule-A"), principal="operator")["status"]
        == "accepted"
    )
    now[0] = "110"
    assert (
        helper.execute(
            command(
                "claim_fire",
                "claim-A",
                {
                    "expected_revision": "1",
                    "clock_basis": "unix_nanoseconds",
                    "worker_principal": "worker-A",
                },
            ),
            principal="worker-A",
        )["status"]
        == "accepted"
    )
    record = helper.snapshot()["records"][0]
    envelope = {
        "event": "received",
        "event_id": record["event_id"],
        "cause_id": record["event_id"],
        "source": {"host": True},
        "target": {"root": {"root_instance_id": "server-1", "root_runtime_id": runtime}},
        "payload": ["map", []],
    }
    delivery = {
        "delivery_mode": "input",
        "envelope": envelope,
        "envelope_digest": delivery_request_digest("server-1", "input", envelope),
    }
    expected = admit_checkpoint_v1(
        checkpoint,
        [delivery],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    receipt = expected["operation_receipts"][-1]
    complete = command(
        "complete_fire",
        "complete-A",
        {
            "expected_revision": "2",
            "attempt_fence": "1",
            "worker_principal": "worker-A",
            "event_id": record["event_id"],
            "admission_receipt_digest": hash_value(["determa-timer-admission-receipt-1", receipt]),
        },
    )
    now[0] = "120"
    return helper, host, complete, expected, now, clock_calls


def test_actual_admission_and_timer_fire_commit_together_and_replay_after_restart(tmp_path):
    helper, host, complete, expected, now, calls = configured(tmp_path)
    result = helper.execute(complete, principal="worker-A")
    assert result["status"] == "accepted" and result["delivery_state"] == "admitted"
    assert result["record_revision"] == "3" and result["expires_at"] is None
    assert host.read_checkpoint("server-1").document == expected
    committed = helper.snapshot()
    assert (
        committed["records"][0]["admission_receipt_digest"]
        == complete["arguments"]["admission_receipt_digest"]
    )

    def offline():
        raise AssertionError("retained fire replay read the clock")

    restarted_host = ExecutionHost(
        SQLiteExecutionStore(
            helper.path, replay_retention="permanent", shared_application_transactions=True
        ),
        host.artifact_resolver,
    )
    restarted = SQLiteTimerHelper(
        helper.path,
        helper.bundle,
        scope_identity=helper.scope_identity,
        root_instance_id=helper.root_instance_id,
        root_runtime_id=helper.root_runtime_id,
        principals=helper.principals,
        worker_principals=helper.worker_principals,
        trusted_clock=offline,
        coordinated_host=restarted_host,
    )
    assert restarted.execute(complete, principal="worker-A") == result
    assert restarted.snapshot() == committed
    assert restarted_host.read_checkpoint("server-1").document == expected
    assert calls == ["100", "110", "120", "120"]


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("attempt_fence", "0", "timer_stale_fence"),
        ("event_id", "sha256:" + "0" * 64, "timer_event_conflict"),
        ("admission_receipt_digest", "sha256:" + "0" * 64, "timer_event_conflict"),
    ],
)
def test_false_fire_identity_or_receipt_rolls_back_actual_admission(tmp_path, field, value, code):
    helper, host, complete, expected, now, calls = configured(tmp_path)
    before_helper = helper.snapshot()
    before_checkpoint = host.read_checkpoint("server-1").document
    changed = copy.deepcopy(complete)
    changed["arguments"][field] = value
    changed["request_digest"] = timer_request_digest(changed)
    assert helper.execute(changed, principal="worker-A")["error_code"] == code
    assert helper.snapshot() == before_helper
    assert host.read_checkpoint("server-1").document == before_checkpoint


def test_failure_after_staging_timer_and_actual_acceptance_rolls_back_both(tmp_path, monkeypatch):
    helper, host, complete, expected, now, calls = configured(tmp_path)
    before_helper = helper.snapshot()
    before_checkpoint = host.read_checkpoint("server-1").document
    persist = helper._persist

    def fail_after_write(sql, artifact):
        persist(sql, artifact)
        raise RuntimeError("native commit interrupted")

    monkeypatch.setattr(helper, "_persist", fail_after_write)
    with pytest.raises(RuntimeError, match="native commit interrupted"):
        helper.execute(complete, principal="worker-A")
    assert helper.snapshot() == before_helper
    assert host.read_checkpoint("server-1").document == before_checkpoint


def test_commit_before_response_failure_is_reconciled_from_retained_fire(tmp_path):
    helper, host, complete, expected, now, calls = configured(tmp_path)

    def interrupted(boundary):
        if boundary == "after_commit_before_response":
            raise RuntimeError("lost response")

    host.fault_injector = interrupted
    with pytest.raises(RuntimeError, match="lost response"):
        helper.execute(complete, principal="worker-A")
    assert helper.snapshot()["records"][0]["state"] == "fired"
    assert host.read_checkpoint("server-1").document == expected
    # Retained replay aborts the read-only callback before the response fault hook.
    assert helper.execute(complete, principal="worker-A")["status"] == "accepted"
    assert calls == ["100", "110", "120", "120"]


@pytest.mark.parametrize("fate", ["expired", "offline"])
def test_lease_is_rechecked_after_staging_and_before_native_commit(tmp_path, fate):
    helper, host, complete, expected, now, calls = configured(tmp_path)
    before_helper = helper.snapshot()
    before_checkpoint = host.read_checkpoint("server-1").document
    reads = []

    def advancing_clock():
        reads.append(now[0])
        if len(reads) == 1:
            now[0] = "1000"
            return "120"
        if fate == "offline":
            raise RuntimeError("clock offline")
        return now[0]

    helper.trusted_clock = advancing_clock
    result = helper.execute(complete, principal="worker-A")
    assert result["error_code"] == (
        "timer_stale_fence" if fate == "expired" else "timer_clock_unavailable"
    )
    assert result["record_revision"] == "2" and result["attempt_fence"] == "1"
    assert helper.snapshot() == before_helper
    assert host.read_checkpoint("server-1").document == before_checkpoint
    assert reads == ["120", "1000"]
