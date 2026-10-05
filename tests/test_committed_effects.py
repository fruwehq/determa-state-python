"""Native effect outcome and replay regressions against the pinned v1 fixtures."""

import copy
import json

import pytest

from conformance.harness import conformance_root
from determa.state import MemoryArtifactResolver, load_bundle
from determa.state.effects import (
    EffectError,
    SQLiteCommittedEffectHost,
    seal_journal,
    validate_journal,
)

CASE = conformance_root() / "conformance/profiles/committed-native-effects/effect-01-result"


def read(name):
    return json.loads((CASE / name).read_text())


def host_fixture(tmp_path, observer=None):
    bundle = load_bundle((CASE / "machine.yaml").read_text())
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    host = SQLiteCommittedEffectHost(
        tmp_path / "effects.sqlite", resolver, {}, lambda *_: {}, core_observer=observer
    )
    host.setup_schema()
    checkpoint = read("pending-checkpoint.json")
    claim = read("data/active-claim.json")
    host.seed(checkpoint, read("data/leased-journal.json"), claim)
    context = {
        "principal": claim["worker_principal"],
        "scope": claim["scope_identity"],
        "epoch": claim["scope_authority_epoch"],
        "trusted_now": "0",
    }
    return host, checkpoint["root_instance_id"], read("data/result-request.json"), context


def test_admission_failure_keeps_outcome_durable_and_recovery_admits_once(tmp_path):
    def fail(*_):
        raise RuntimeError("crash before admission")

    host, root, request, context = host_fixture(tmp_path, fail)
    with pytest.raises(RuntimeError, match="crash before admission"):
        host.submit_result(root, request, **context)
    saved = host.snapshot(root)
    record = saved["journal"]["effect_records"][0]
    assert record["invocation_state"] == "outcome_recorded"
    assert record["outcome"]["payload"] == request["payload"]
    assert saved["journal"]["journal_revision"] == "3"
    calls = []
    host.core_observer = lambda *args: calls.append(args)
    recovered = host.recover(root)
    assert recovered["journal"]["effect_records"][0]["invocation_state"] == "result_admitted"
    assert len(calls) == 1
    host.recover(root)
    assert len(calls) == 1


def test_equal_result_replay_retains_first_response_after_outbox_change(tmp_path):
    host, root, request, context = host_fixture(tmp_path)
    first = host.submit_result(root, request, **context)
    host.terminalize_outbox(root, request["effect_id"], {"status": "confirmed"})
    before = host.snapshot(root)
    assert host.submit_result(root, request, **context) == first
    assert host.snapshot(root) == before


def test_terminal_outcome_requires_immutable_attempt_evidence():
    journal = read("data/outcome-recorded-journal.json")
    journal["effect_records"][0]["attempt_records"] = []
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        validate_journal(read("pending-checkpoint.json"), seal_journal(journal))


@pytest.mark.parametrize("field", ["digest", "attempt_fence"])
def test_terminal_outcome_rejects_forged_evidence(field):
    journal = copy.deepcopy(read("data/outcome-recorded-journal.json"))
    journal["effect_records"][0]["outcome"][field] = (
        "sha256:" + "0" * 64 if field == "digest" else "0"
    )
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        validate_journal(read("pending-checkpoint.json"), seal_journal(journal))


def test_invalid_result_payload_is_rejected_before_journal_or_core_mutation(tmp_path):
    calls = []
    host, root, request, context = host_fixture(tmp_path, lambda *args: calls.append(args))
    request["payload"] = ["map", [["provider_reference", ["int", "42"]]]]
    before = host.snapshot(root)
    assert host.submit_result(root, request, **context)["status"] == "rejected"
    assert host.snapshot(root) == before
    assert calls == []


def test_payload_pointer_inserts_pinned_token_before_declared_input_admission(tmp_path):
    bundle = load_bundle((CASE / "machine.yaml").read_text())
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    host = SQLiteCommittedEffectHost(tmp_path / "pointer.sqlite", resolver, {}, lambda *_: {})
    host.setup_schema()
    checkpoint = read("pending-checkpoint.json")
    journal = read("data/leased-journal.json")
    journal["effect_records"][0]["result_mapping"][0]["operation_token_location"] = {
        "kind": "payload",
        "pointer": "/provider_reference",
    }
    claim = read("data/active-claim.json")
    host.seed(checkpoint, seal_journal(journal), claim)
    request = read("data/result-request.json")
    response = host.submit_result(
        checkpoint["root_instance_id"],
        request,
        principal=claim["worker_principal"],
        scope=claim["scope_identity"],
        epoch=claim["scope_authority_epoch"],
        trusted_now="0",
    )
    assert response["status"] == "committed"
    saved = host.snapshot(checkpoint["root_instance_id"])
    runtime = saved["checkpoint"]["root_record"]["aggregate_state"]["runtimes"][0]
    assert runtime["ready_mailbox"][-1]["envelope"]["payload"] == [
        "map",
        [["provider_reference", ["string", request["operation_token"]]]],
    ]


def test_expired_worker_cannot_admit_recorded_outcome_but_host_recovery_can(tmp_path):
    host, root, request, context = host_fixture(tmp_path)
    host.submit_result(root, request, admit=False, **context)
    before = host.snapshot(root)
    context["trusted_now"] = read("data/active-claim.json")["expires_at"]
    assert host.submit_result(root, request, **context)["error_code"] == "stale_attempt_fence"
    assert host.snapshot(root) == before
    assert (
        host.recover(root)["journal"]["effect_records"][0]["invocation_state"] == "result_admitted"
    )


@pytest.mark.parametrize("boundary", ["frozen", "wrong_epoch", "wrong_scope", "unissued_claim"])
def test_authority_seed_cannot_bypass_scope_or_claim_guard(tmp_path, boundary):
    from determa.state.authority import SQLiteLocalAuthority
    from determa.state.wire import hash_value

    bundle = load_bundle((CASE / "machine.yaml").read_text())
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    checkpoint = read("pending-checkpoint.json")
    journal = read("data/leased-journal.json")
    claim = read("data/active-claim.json")
    scope, root = journal["scope_identity"], checkpoint["root_instance_id"]
    authority = SQLiteLocalAuthority(tmp_path / "authority.sqlite")
    authority.setup_schema()
    assert authority.allocate(scope, "owner", roots=(root,))
    claim["scope_authority_epoch"] = "0"
    epoch = "9" if boundary == "wrong_epoch" else "0"
    host = SQLiteCommittedEffectHost(
        authority.path, resolver, {"authority_epoch": epoch}, lambda *_: {}, authority_scope=scope
    )
    host.setup_schema()
    if boundary == "frozen":
        request = {
            "interface": "determa.host_authority",
            "interface_version": 1,
            "operation": "freeze_scope",
            "operation_id": "freeze",
            "scope_identity": scope,
            "expected_authority_epoch": "0",
            "expected_scope_generation": "0",
            "arguments": {},
        }
        request["request_digest"] = hash_value(["determa-host-authority-request-1", request])
        response = authority.perform(
            json.dumps(request),
            {
                "authenticated_principal": "owner",
                "authorized_scopes": [scope],
                "operation_rights": ["freeze_scope"],
            },
        )
        assert json.loads(response)["state"] == "frozen"
    if boundary == "wrong_scope":
        journal["scope_identity"] = "other-scope"
        journal = seal_journal(journal)
    before = authority.inspect(scope)
    with pytest.raises(EffectError):
        host.seed(checkpoint, journal, claim)
    assert authority.inspect(scope) == before
    with host._connect() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM determa_committed_effects").fetchone()[0] == 0
        )
