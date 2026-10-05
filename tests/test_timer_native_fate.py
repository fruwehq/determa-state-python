"""Recovery uses native atomic history under its writer lock, never caller fate."""

from __future__ import annotations

import copy
import multiprocessing
import os
import signal
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from determa.state import ExecutionHost, SQLiteExecutionStore
from determa.state.host import delivery_request_digest
from determa.state.timers import SQLiteTimerHelper, timer_request_digest

from .test_timer_coordinated_admission import _killed_timer_worker, configured


def reclaim(complete, revision="2"):
    value = copy.deepcopy(complete)
    value.update(operation="claim_fire", operation_id="reclaim-B")
    value["arguments"] = {
        "expected_revision": revision,
        "clock_basis": "unix_nanoseconds",
        "worker_principal": "worker-A",
    }
    value["request_digest"] = timer_request_digest(value)
    return value


def reopened(helper, host, clock):
    return SQLiteTimerHelper(
        helper.path,
        helper.bundle,
        scope_identity=helper.scope_identity,
        root_instance_id=helper.root_instance_id,
        root_runtime_id=helper.root_runtime_id,
        principals=helper.principals,
        worker_principals=helper.worker_principals,
        trusted_clock=clock,
        coordinated_host=host,
    )


def test_expired_local_claim_reclaims_with_new_fence_then_rejects_old_worker(tmp_path):
    helper, host, complete, expected, now, _ = configured(tmp_path)
    before_checkpoint = host.read_checkpoint("server-1").document
    now[0] = "130"
    result = helper.execute(reclaim(complete), principal="worker-A")
    assert result["status"] == "accepted" and result["attempt_fence"] == "2"
    assert result["record_revision"] == "3" and result["expires_at"] == "150"
    assert host.read_checkpoint("server-1").document == before_checkpoint
    before = helper.snapshot()
    assert helper.execute(complete, principal="worker-A")["error_code"] == "timer_stale_fence"
    assert helper.snapshot() == before
    now[0] = "140"
    replacement = copy.deepcopy(complete)
    replacement["operation_id"] = "complete-B"
    replacement["arguments"].update(expected_revision="3", attempt_fence="2")
    replacement["request_digest"] = timer_request_digest(replacement)
    assert helper.execute(replacement, principal="worker-A")["status"] == "accepted"
    assert host.read_checkpoint("server-1").document == expected
    # Original reclaim's exact receipt remains replayable after fired state and offline clock.
    helper.trusted_clock = lambda: (_ for _ in ()).throw(AssertionError("replay read clock"))
    assert helper.execute(reclaim(complete), principal="worker-A") == result


@pytest.mark.parametrize("replace_class", [False, True])
def test_replaced_fate_proof_cannot_bypass_its_own_guard(tmp_path, monkeypatch, replace_class):
    helper, host, complete, _, now, _ = configured(tmp_path, replay_retention="bounded")
    before_helper, before_root = helper.snapshot(), host.read_checkpoint("server-1").document
    now[0] = "130"
    monkeypatch.setattr(
        SQLiteTimerHelper if replace_class else helper,
        "_prove_local_uncommitted_fire",
        lambda *_: None,
    )
    assert (
        helper.execute(reclaim(complete), principal="worker-A")["error_code"]
        == "delivery_ambiguous"
    )
    assert helper.snapshot() == before_helper
    assert host.read_checkpoint("server-1").document == before_root


@pytest.mark.parametrize("changed_content", [False, True])
def test_independently_admitted_matching_id_never_becomes_helper_fate_proof(
    tmp_path, changed_content
):
    helper, host, complete, _, now, _ = configured(tmp_path)
    before = host.read_checkpoint("server-1").document
    envelope = {
        "event": "received",
        "event_id": complete["arguments"]["event_id"],
        "cause_id": complete["arguments"]["event_id"],
        "source": {"host": True},
        "target": {
            "root": {"root_instance_id": "server-1", "root_runtime_id": helper.root_runtime_id}
        },
        "payload": ["map", []],
    }
    if changed_content:
        envelope["correlation_id"] = "unrelated"
    host.admit_v1(
        "server-1",
        [
            {
                "delivery_mode": "input",
                "envelope": envelope,
                "envelope_digest": delivery_request_digest("server-1", "input", envelope),
            }
        ],
        expected_revision=before["revision"],
        expected_checkpoint_digest=before["execution_checkpoint_digest"],
    )
    before_helper, before_root = helper.snapshot(), host.read_checkpoint("server-1").document
    now[0] = "130"
    assert (
        helper.execute(reclaim(complete), principal="worker-A")["error_code"]
        == "delivery_ambiguous"
    )
    assert helper.snapshot() == before_helper
    assert host.read_checkpoint("server-1").document == before_root


@pytest.mark.parametrize("changed", ["implementation", "store_policy", "clock"])
def test_changed_implementation_policy_or_invalid_clock_refuses_without_reclaim(tmp_path, changed):
    helper, host, complete, _, now, _ = configured(tmp_path)
    before = helper.snapshot()
    now[0] = "130"
    if changed == "implementation":
        host.run_shared_transaction = lambda *_: None
        code = "delivery_ambiguous"
    elif changed == "store_policy":
        host.store.replay_retention = "bounded"
        code = "timer_capability_mismatch"
    else:
        now[0] = "invalid"
        code = "timer_clock_unavailable"
    assert helper.execute(reclaim(complete), principal="worker-A")["error_code"] == code
    if changed == "store_policy":
        host.store.replay_retention = "permanent"
    assert helper.snapshot() == before


def test_completion_staged_before_reclaim_rolls_back_at_expiry_then_new_fence_wins(tmp_path):
    helper, host, complete, _, now, _ = configured(tmp_path)
    contender = reopened(helper, host, lambda: now[0])
    staged, release = threading.Event(), threading.Event()
    persist = helper._persist

    def stop_after_staging(sql, artifact):
        persist(sql, artifact)
        staged.set()
        assert release.wait(20), "test did not release staged transaction"

    helper._persist = stop_after_staging
    with ThreadPoolExecutor(max_workers=2) as pool:
        previous = pool.submit(helper.execute, complete, principal="worker-A")
        try:
            assert staged.wait(20), "completion did not stage actual native writes"
            now[0] = "130"
            replacement = pool.submit(contender.execute, reclaim(complete), principal="worker-A")
        finally:
            release.set()
        assert previous.result(20)["error_code"] == "timer_stale_fence"
        assert replacement.result(20)["attempt_fence"] == "2"
    assert contender.snapshot()["records"][0]["state"] == "claimed"
    assert not any(
        item.get("event_id") == complete["arguments"]["event_id"]
        for item in host.read_checkpoint("server-1").document["operation_receipts"]
    )


@pytest.mark.skipif(os.name != "posix", reason="requires actual SIGKILL")
@pytest.mark.parametrize("boundary", ["both_writes_staged", "after_commit_before_response"])
def test_sigkill_native_fate_reclaim_or_terminal_replay(tmp_path, boundary):
    helper, host, complete, expected, now, _ = configured(tmp_path)
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    worker = context.Process(
        target=_killed_timer_worker, args=(helper.path, complete, boundary, child)
    )
    worker.start()
    child.close()
    try:
        assert parent.poll(30), "worker did not reach native boundary"
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
    restarted_host = ExecutionHost(
        SQLiteExecutionStore(
            helper.path, replay_retention="permanent", shared_application_transactions=True
        ),
        host.artifact_resolver,
    )
    now[0] = "130"
    restarted = reopened(helper, restarted_host, lambda: now[0])
    if boundary == "both_writes_staged":
        result = restarted.execute(reclaim(complete), principal="worker-A")
        assert result["status"] == "accepted" and result["attempt_fence"] == "2"
        before = restarted.snapshot()
        assert (
            restarted.execute(complete, principal="worker-A")["error_code"] == "timer_stale_fence"
        )
        assert restarted.snapshot() == before
        replacement = copy.deepcopy(complete)
        replacement["operation_id"] = "complete-B"
        replacement["arguments"].update(expected_revision="3", attempt_fence="2")
        replacement["request_digest"] = timer_request_digest(replacement)
        now[0] = "140"
        assert restarted.execute(replacement, principal="worker-A")["status"] == "accepted"
    else:
        before = restarted.snapshot()
        assert (
            restarted.execute(reclaim(complete, "3"), principal="worker-A")["error_code"]
            == "timer_already_fired"
        )
        assert restarted.snapshot() == before
        assert restarted.execute(complete, principal="worker-A")["status"] == "accepted"
    assert restarted_host.read_checkpoint("server-1").document == expected
    assert restarted.snapshot()["records"][0]["state"] == "fired"


def test_bounded_receipt_history_cannot_prove_uncommitted_fire(tmp_path):
    helper, host, complete, _, now, _ = configured(tmp_path, replay_retention="bounded")
    before_helper, before_root = helper.snapshot(), host.read_checkpoint("server-1").document
    now[0] = "130"
    assert (
        helper.execute(reclaim(complete), principal="worker-A")["error_code"]
        == "delivery_ambiguous"
    )
    assert helper.snapshot() == before_helper
    assert host.read_checkpoint("server-1").document == before_root


def test_changed_shared_admission_implementation_cannot_establish_native_fate(
    tmp_path, monkeypatch
):
    from determa.state.host import SharedExecutionTransaction

    helper, _, complete, _, now, _ = configured(tmp_path)
    before = helper.snapshot()
    now[0] = "130"
    monkeypatch.setattr(SharedExecutionTransaction, "admit_v1", lambda *_: None)
    assert (
        helper.execute(reclaim(complete), principal="worker-A")["error_code"]
        == "delivery_ambiguous"
    )
    assert helper.snapshot() == before


@pytest.mark.parametrize("missing", ["determa_timer_origin", "determa_timer_commits"])
def test_missing_origin_or_native_history_cannot_be_rebuilt_for_reclaim(tmp_path, missing):
    import sqlite3

    helper, _, complete, _, now, _ = configured(tmp_path)
    now[0] = "130"
    with sqlite3.connect(helper.path) as connection:
        connection.execute(f"DROP TABLE {missing}")
        before = connection.execute("SELECT document FROM determa_timer_helpers").fetchall()
    assert (
        helper.execute(reclaim(complete), principal="worker-A")["error_code"]
        == "timer_capability_mismatch"
    )
    with sqlite3.connect(helper.path) as connection:
        assert connection.execute("SELECT document FROM determa_timer_helpers").fetchall() == before
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name=?", (missing,)
        ).fetchone() == (0,)
