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


@pytest.mark.parametrize(
    "journal_name,with_claim",
    [
        ("unclaimed-journal.json", False),
        ("leased-journal.json", False),
        ("retryable-journal.json", True),
    ],
)
def test_seed_cannot_rewind_an_issued_fence_or_remove_its_claim(tmp_path, journal_name, with_claim):
    from determa.state.authority import SQLiteLocalAuthority

    bundle = load_bundle((CASE / "machine.yaml").read_text())
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    checkpoint = read("pending-checkpoint.json")
    journal = read("data/" + journal_name)
    claim = read("data/active-claim.json")
    claim["scope_authority_epoch"] = "0"
    scope, root = journal["scope_identity"], checkpoint["root_instance_id"]
    authority = SQLiteLocalAuthority(tmp_path / "issued.sqlite")
    authority.setup_schema()
    assert authority.allocate(scope, "owner", roots=(root,))
    ledger = authority.inspect(scope)
    ledger["active_claims"] = [claim]
    ledger["journal_entries"] = [{"work_identity": claim["work_identity"], "attempt_fence": "1"}]
    with authority._connect() as connection:
        connection.execute(
            "UPDATE determa_scope_authority SET ledger=? WHERE scope_identity=?",
            (json.dumps(ledger), scope),
        )
        connection.commit()
    host = SQLiteCommittedEffectHost(
        authority.path, resolver, {"authority_epoch": "0"}, lambda *_: {}, authority_scope=scope
    )
    host.setup_schema()
    with pytest.raises(EffectError, match="stale_attempt_fence"):
        host.seed(checkpoint, journal, claim if with_claim else None)
    assert authority.inspect(scope) == ledger
    with host._connect() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM determa_committed_effects").fetchone()[0] == 0
        )


def authority_effect_fixture(tmp_path):
    from determa.state.authority import SQLiteLocalAuthority

    bundle = load_bundle((CASE / "machine.yaml").read_text())
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    checkpoint, journal = read("pending-checkpoint.json"), read("data/unclaimed-journal.json")
    scope, root = journal["scope_identity"], checkpoint["root_instance_id"]
    authority = SQLiteLocalAuthority(
        tmp_path / "paired.sqlite", worker_fencing=True, worker_lease_nanoseconds=10
    )
    authority.setup_schema()
    assert authority.allocate(scope, "owner", roots=(root,))
    host = SQLiteCommittedEffectHost(
        authority.path, resolver, {"authority_epoch": "0"}, lambda *_: {}, authority_scope=scope
    )
    host.setup_schema()
    host.seed(checkpoint, journal)
    return authority, host, scope, root, journal["effect_records"][0]


def authority_claim_request(
    authority, scope, root, record, operation="claim-first", expected=None, **overrides
):
    from determa.state.wire import hash_value

    arguments = {
        "root_instance_id": root,
        "work_kind": "effect",
        "work_identity": record["effect_id"],
        "operation_token": record["operation_token"],
        "expected_attempt_fence": expected,
        "expected_worker_principal": "worker-a",
    }
    arguments.update(overrides)
    request = {
        "interface": "determa.host_authority",
        "interface_version": 1,
        "operation": "fence_worker",
        "operation_id": operation,
        "scope_identity": scope,
        "expected_authority_epoch": "0",
        "expected_scope_generation": authority.inspect(scope)["scope_generation"],
        "arguments": arguments,
    }
    request["request_digest"] = hash_value(["determa-host-authority-request-1", request])
    invocation = {
        "authenticated_principal": "worker-a",
        "authorized_scopes": [scope],
        "operation_rights": ["fence_worker"],
        "assigned_worker_principal": "worker-a",
        "clock_available": True,
        "trusted_clock_now": "0",
    }
    return json.dumps(request), invocation


def test_authority_claim_atomically_updates_native_journal_and_replays_once(tmp_path):
    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    request, invocation = authority_claim_request(authority, scope, root, record)
    before = host.snapshot(root)
    response = authority.perform(request, invocation)
    result = json.loads(response)
    assert result["status"] == "accepted"
    after = host.snapshot(root)
    assert after["claims"][record["effect_id"]] == result["claim"]
    assert after["journal"]["effect_records"][0]["invocation_state"] == "leased"
    assert after["journal"]["effect_records"][0]["attempt_fence"] == "1"
    assert (
        int(after["journal"]["journal_revision"]) == int(before["journal"]["journal_revision"]) + 1
    )
    ledger = authority.inspect(scope)
    assert ledger["active_claims"] == [result["claim"]]
    assert authority.perform(request, invocation) == response
    assert host.snapshot(root) == after
    assert authority.inspect(scope) == ledger
    # Lease expiration alone does not authorize another external attempt.
    retry, invocation = authority_claim_request(authority, scope, root, record, "unsafe-retry", "1")
    invocation["trusted_clock_now"] = "20"
    assert json.loads(authority.perform(retry, invocation))["status"] == "rejected"
    assert host.snapshot(root) == after
    assert authority.inspect(scope) == ledger


def test_authority_claim_rolls_back_both_native_records_before_commit(tmp_path):
    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    request, invocation = authority_claim_request(authority, scope, root, record)
    before, ledger = host.snapshot(root), authority.inspect(scope)
    assert authority._perform(request, invocation, fault="precommit_abort") is None
    assert host.snapshot(root) == before
    assert authority.inspect(scope) == ledger


@pytest.mark.parametrize(
    "overrides", [{"operation_token": "wrong-token"}, {"root_instance_id": "wrong-root"}]
)
def test_authority_claim_cannot_invent_native_identity(tmp_path, overrides):
    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    request, invocation = authority_claim_request(authority, scope, root, record, **overrides)
    before, ledger = host.snapshot(root), authority.inspect(scope)
    assert json.loads(authority.perform(request, invocation))["status"] == "rejected"
    assert host.snapshot(root) == before
    assert authority.inspect(scope) == ledger


@pytest.mark.parametrize("changed", ["ledger", "journal"])
def test_effect_mutations_refuse_a_torn_authority_journal_pair(tmp_path, changed):
    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    if changed == "ledger":
        ledger = authority.inspect(scope)
        ledger["journal_entries"][0]["attempt_fence"] = "1"
        with authority._connect() as connection:
            connection.execute("UPDATE determa_scope_authority SET ledger=?", (json.dumps(ledger),))
            connection.commit()
    else:
        document = host.snapshot(root)
        document["journal"]["effect_records"][0]["attempt_fence"] = "1"
        document["journal"] = seal_journal(document["journal"])
        with host._connect() as connection:
            connection.execute(
                "UPDATE determa_committed_effects SET document=?", (json.dumps(document).encode(),)
            )
            connection.commit()
    before, ledger = host.snapshot(root), authority.inspect(scope)
    with pytest.raises(EffectError, match="stale_attempt_fence"):
        host.claim(root, record["effect_id"], "worker-a", "0", expires_at="10", trusted_now="0")
    assert host.snapshot(root) == before
    assert authority.inspect(scope) == ledger


@pytest.mark.parametrize("damage", ["provenance", "native_row", "native_table", "work_identity"])
def test_authority_claim_refuses_missing_native_participant_evidence(tmp_path, damage):
    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    if damage == "provenance":
        ledger = authority.inspect(scope)
        ledger["native_checkpoint_bytes"] = []
        with authority._connect() as connection:
            connection.execute("UPDATE determa_scope_authority SET ledger=?", (json.dumps(ledger),))
            connection.commit()
    elif damage in {"native_row", "native_table"}:
        with host._connect() as connection:
            connection.execute(
                "DELETE FROM determa_committed_effects"
                if damage == "native_row"
                else "DROP TABLE determa_committed_effects"
            )
            connection.commit()
    else:
        record = {**record, "effect_id": "unrelated-work"}
        ledger = authority.inspect(scope)
        ledger["journal_entries"].append({"work_identity": "unrelated-work", "attempt_fence": "0"})
        with authority._connect() as connection:
            connection.execute("UPDATE determa_scope_authority SET ledger=?", (json.dumps(ledger),))
            connection.commit()
    request, invocation = authority_claim_request(authority, scope, root, record)
    ledger = authority.inspect(scope)
    before = None if damage in {"native_row", "native_table"} else host.snapshot(root)
    assert json.loads(authority.perform(request, invocation))["status"] == "rejected"
    assert authority.inspect(scope) == ledger
    if before is not None:
        assert host.snapshot(root) == before


def test_seed_preserves_historical_claim_without_reviving_it(tmp_path):
    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    request, invocation = authority_claim_request(authority, scope, root, record)
    claim = json.loads(authority.perform(request, invocation))["claim"]
    # Model a retained completed attempt whose current journal permits a retry.
    journal = read("data/retryable-journal.json")
    ledger = authority.inspect(scope)
    ledger["active_claims"] = []
    assert claim in ledger["effect_claim_history"]
    checkpoint = host.snapshot(root)["checkpoint"]
    with authority._connect() as connection:
        connection.execute("UPDATE determa_scope_authority SET ledger=?", (json.dumps(ledger),))
        connection.execute("DELETE FROM determa_committed_effects")
        connection.commit()
    host.seed(checkpoint, journal, claim)
    assert authority.inspect(scope)["active_claims"] == []
    assert host.snapshot(root)["claims"][record["effect_id"]] == claim
    new_claim = host.claim(
        root, record["effect_id"], "worker-b", "0", expires_at="30", trusted_now="20"
    )
    assert new_claim["attempt_fence"] == "2"
    assert authority.inspect(scope)["active_claims"] == [new_claim]
    assert claim in authority.inspect(scope)["effect_claim_history"]
