"""SIGKILL retains native start/outcome facts across actual independent SQLite I/O."""

from __future__ import annotations

import copy
import json
import multiprocessing
import os
import signal
import sqlite3
import time
from pathlib import Path

import pytest

from determa.state import MemoryArtifactResolver, load_bundle
from determa.state.effects import EffectError, SQLiteCommittedEffectHost

from .test_committed_effects import (
    CASE,
    NativeTestProvider,
    authority_claim_request,
    authority_effect_fixture,
    installed_test_handler,
)


def _dispatch_worker(directory, cut, control):
    authority, host, scope, root, record = authority_effect_fixture(Path(directory))
    command, invocation = authority_claim_request(authority, scope, root, record)
    claim = json.loads(authority.perform(command, invocation))["claim"]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    host.trusted_clock = lambda: "0"
    context = {
        "principal": claim["worker_principal"],
        "scope": scope,
        "epoch": "0",
        "trusted_now": "0",
    }
    information = {"root": root, "scope": scope, "effect_id": record["effect_id"], "claim": claim}
    provider_path = Path(directory) / "provider.sqlite"

    def health(self, instance):
        if cut == "start":
            with sqlite3.connect(host.path) as connection:
                document = json.loads(
                    connection.execute("SELECT document FROM determa_committed_effects").fetchone()[
                        0
                    ]
                )
            if any(
                response.get("kind") == "effect_invocation_start"
                for response in document["responses"].values()
            ):
                control.send(information)
                time.sleep(60)
        return "healthy"

    NativeTestProvider.health = health

    def native_call(payload, metadata, attempt):
        # SDK objects stay in this native callback. The independent destination
        # transaction commits before acceptance is reported to the host.
        class SdkReceipt:
            def __init__(self):
                self.reference = "provider:" + metadata["effect_id"]

        receipt = SdkReceipt()
        evidence = {
            "scope_identity": metadata["scope_identity"],
            "effect_id": metadata["effect_id"],
            "operation_token": metadata["operation_token"],
            "attempt_fence": attempt["attempt_fence"],
            "request": payload,
            "provider_reference": receipt.reference,
        }
        assert metadata["credential"] == "test-credential"
        with sqlite3.connect(provider_path) as destination:
            destination.execute("PRAGMA journal_mode=WAL")
            destination.execute("PRAGMA synchronous=FULL")
            destination.execute(
                "CREATE TABLE receipts (scope TEXT NOT NULL, effect TEXT NOT NULL, "
                "receipt TEXT NOT NULL, PRIMARY KEY(scope,effect))"
            )
            destination.execute(
                "INSERT INTO receipts VALUES (?,?,?)",
                (scope, record["effect_id"], json.dumps(evidence, sort_keys=True)),
            )
            destination.commit()
        if cut == "acceptance":
            control.send(information)
            time.sleep(60)
        return {
            "report_kind": "succeeded",
            "payload": ["map", [["provider_reference", ["string", receipt.reference]]]],
            "reason": None,
        }

    host = installed_test_handler(host, native_call)
    reply = host.dispatch(root, record["effect_id"], credential="test-credential", **context)
    if cut == "response":
        control.send(information)
        time.sleep(60)

    def outcome_committed(*args):
        control.send(information)
        time.sleep(60)

    if cut == "outcome":
        host.core_observer = outcome_committed
    request = {
        "effect_id": record["effect_id"],
        "operation_token": record["operation_token"],
        "attempt_fence": claim["attempt_fence"],
        "outcome_kind": reply["report_kind"],
        "payload": reply["payload"],
    }
    response = host.submit_result(root, request, **context)
    assert cut == "admission" and response["status"] == "committed"
    information["response"] = response
    control.send(information)
    time.sleep(60)


@pytest.mark.skipif(os.name != "posix", reason="requires actual SIGKILL")
@pytest.mark.parametrize("cut", ["start", "acceptance", "response", "outcome", "admission"])
def test_sigkill_native_dispatch_never_repeats_uncertain_provider_call(tmp_path, cut):
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    worker = context.Process(target=_dispatch_worker, args=(str(tmp_path), cut, child))
    worker.start()
    child.close()
    try:
        assert parent.poll(20), f"worker did not reach {cut} (exit={worker.exitcode})"
        information = parent.recv()
        worker.kill()
        worker.join(5)
        assert worker.exitcode == -signal.SIGKILL
    finally:
        if worker.is_alive():
            worker.kill()
            worker.join(5)
        parent.close()
    bundle = load_bundle((CASE / "machine.yaml").read_text())
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    route = {"authority_epoch": "0"}
    base = SQLiteCommittedEffectHost(
        tmp_path / "paired.sqlite",
        resolver,
        route,
        None,
        authority_scope=information["scope"],
        trusted_clock=lambda: "0",
    )
    before = base.snapshot(information["root"])
    record = before["journal"]["effect_records"][0]
    base.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    calls = []
    host = installed_test_handler(base, lambda *args: calls.append(args) or {})
    invocation = {
        "principal": information["claim"]["worker_principal"],
        "scope": information["scope"],
        "epoch": "0",
        "trusted_now": "0",
    }
    with pytest.raises(EffectError):
        host.dispatch(
            information["root"],
            information["effect_id"],
            credential="test-credential",
            **invocation,
        )
    assert host.snapshot(information["root"]) == before
    assert calls == []
    assert (
        len(
            [
                reply
                for reply in before["responses"].values()
                if reply.get("kind") == "effect_invocation_start"
            ]
        )
        == 1
    )
    assert record["invocation_state"] == (
        "result_admitted"
        if cut == "admission"
        else "outcome_recorded"
        if cut == "outcome"
        else "leased"
    )
    if cut == "start":
        assert not (tmp_path / "provider.sqlite").exists()
    else:
        with sqlite3.connect(tmp_path / "provider.sqlite") as destination:
            rows = destination.execute("SELECT scope,effect,receipt FROM receipts").fetchall()
        assert len(rows) == 1
        scope, effect, encoded = rows[0]
        receipt = json.loads(encoded)
        assert (scope, effect) == (information["scope"], information["effect_id"])
        assert receipt["operation_token"] == record["operation_token"]
        assert receipt["attempt_fence"] == "1"
        assert (
            receipt["request"]
            == before["checkpoint"]["pending_outbox_intents"][0]["intent"]["payload"]
        )
    host.trusted_clock = lambda: information["claim"]["expires_at"]
    recovered = host.recover(information["root"])
    assert calls == []
    assert recovered["journal"]["effect_records"][0]["invocation_state"] == (
        "result_admitted" if cut in {"outcome", "admission"} else "ambiguous"
    )
    if cut in {"outcome", "admission"}:
        assert int(recovered["checkpoint"]["revision"]) == 3
        assert recovered["journal"]["effect_records"][0]["outcome"] == record["outcome"]
    else:
        assert recovered["checkpoint"] == before["checkpoint"]
        assert recovered["journal"]["effect_records"][0]["outcome"] is None
    saved = copy.deepcopy(recovered)
    assert host.recover(information["root"]) == saved
