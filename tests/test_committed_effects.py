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


class NativeTestProvider:
    def __init__(self, callback):
        self.callback = callback

    def validate_configuration(self, configuration):
        return copy.deepcopy(configuration)

    def capabilities(self, instance):
        return []

    def health(self, instance):
        return "healthy"

    def invoke(self, instance, payload, metadata, attempt):
        return self.callback(payload, metadata, attempt)


def installed_test_handler(host, callback):
    from determa.state.effects import VerifiedNativeHandler
    from determa.state.extensions import ExtensionRegistry

    provider = NativeTestProvider(callback)

    def factory():
        return provider

    callback_code = callback.__code__
    factory_code = factory.__code__
    methods = {
        name: member.__code__
        for name, member in vars(NativeTestProvider).items()
        if callable(member)
    }
    descriptor = {
        "category": "native_handler",
        "provider_reference": host.route["handler_reference"],
        "interface_version": 1,
        "supported_capabilities": [],
    }

    def verify(source, actual):
        return (
            actual == descriptor
            and (source is factory or source is provider)
            and factory.__code__ is factory_code
            and type(provider) is NativeTestProvider
            and provider.callback is callback
            and callback.__code__ is callback_code
            and all(
                getattr(NativeTestProvider, name).__code__ is code for name, code in methods.items()
            )
        )

    registry = ExtensionRegistry(source_verifier=verify)
    registry.register(descriptor, factory)
    configured = registry.validate_configuration(
        descriptor,
        {
            "instance_id": "test-handler",
            "destination_binding_digest": host.route["destination_binding_digest"],
        },
    )
    handler = VerifiedNativeHandler(registry, configured)
    return SQLiteCommittedEffectHost(
        host.path, host.resolver, host.route, handler, trusted_clock=host.trusted_clock
    )


def host_fixture(tmp_path, observer=None):
    bundle = load_bundle((CASE / "machine.yaml").read_text())
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    host = SQLiteCommittedEffectHost(
        tmp_path / "effects.sqlite",
        resolver,
        {},
        lambda *_: {},
        core_observer=observer,
        trusted_clock=lambda: "0",
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
    host = SQLiteCommittedEffectHost(
        tmp_path / "pointer.sqlite", resolver, {}, lambda *_: {}, trusted_clock=lambda: "0"
    )
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


def test_worker_result_crossing_expiry_rolls_back_outcome_before_commit(tmp_path, monkeypatch):
    host, root, request, context = host_fixture(tmp_path)
    current_time = ["0"]
    host.trusted_clock = lambda: current_time[0]
    before = host.snapshot(root)
    submit = host._submit
    observed = []

    def expires_during_submission(*args):
        response = submit(*args)
        observed.append(args[0]["journal"]["effect_records"][0]["invocation_state"])
        current_time[0] = read("data/active-claim.json")["expires_at"]
        return response

    monkeypatch.setattr(host, "_submit", expires_during_submission)
    assert host.submit_result(root, request, **context)["error_code"] == "stale_attempt_fence"
    assert observed == ["outcome_recorded"]
    assert host.snapshot(root) == before


@pytest.mark.parametrize("clock_state", ["missing", "unavailable", "invalid", "expired"])
def test_worker_result_requires_current_host_clock_not_request_time(tmp_path, clock_state):
    host, root, request, context = host_fixture(tmp_path)
    before = host.snapshot(root)

    def unavailable():
        raise RuntimeError("clock unavailable")

    host.trusted_clock = {
        "missing": None,
        "unavailable": unavailable,
        "invalid": lambda: "-0",
        "expired": lambda: read("data/active-claim.json")["expires_at"],
    }[clock_state]
    assert context["trusted_now"] == "0"
    assert host.submit_result(root, request, **context)["error_code"] == "stale_attempt_fence"
    assert host.snapshot(root) == before


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


def generic_authority_fixture(tmp_path):
    from determa.state.authority import SQLiteLocalAuthority

    authority = SQLiteLocalAuthority(
        tmp_path / "generic.sqlite", worker_fencing=True, worker_lease_nanoseconds=10
    )
    authority.setup_schema()
    assert authority.allocate("scope", "owner", roots=("root", "other-root"))
    authority.register_effect_work("scope", "owner", "0", "root", "effect", "token")
    return authority, {"effect_id": "effect", "operation_token": "token"}


def test_generic_claim_commits_bound_journal_and_receipt_atomically(tmp_path):
    authority, record = generic_authority_fixture(tmp_path)
    request, invocation = authority_claim_request(authority, "scope", "root", record)
    before = authority.inspect("scope")
    assert authority._perform(request, invocation, fault="precommit_abort") is None
    assert authority.inspect("scope") == before
    assert authority._perform(request, invocation, fault="drop_response_after_commit") is None
    committed = authority.inspect("scope")
    claim = committed["active_claims"][0]
    assert committed["authority_effect_records"] == [
        {"work_identity": "effect", "attempt_fence": "1", "state": "leased"}
    ]
    assert committed["effect_claim_history"] == [claim]
    assert json.loads(authority.perform(request, invocation))["claim"] == claim
    assert authority.inspect("scope") == committed
    assert authority.check_worker_claim(claim, "worker-a", "0", phase="dispatch")
    assert not authority.check_worker_claim(claim, "worker-a", "10", phase="dispatch")


@pytest.mark.parametrize("change", ["root", "token", "binding", "record", "fence"])
def test_generic_claim_cannot_rebind_work_or_fall_back_from_missing_participant(tmp_path, change):
    authority, record = generic_authority_fixture(tmp_path)
    overrides = {}
    if change == "root":
        overrides["root_instance_id"] = "other-root"
    elif change == "token":
        overrides["operation_token"] = "other-token"
    else:
        with authority._connect() as connection:
            ledger = authority.inspect("scope")
            if change == "binding":
                ledger["authority_effect_bindings"] = []
            elif change == "record":
                ledger["authority_effect_records"] = []
            else:
                ledger["authority_effect_records"][0]["attempt_fence"] = "2"
            connection.execute(
                "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
                (json.dumps(ledger), "scope"),
            )
    request, invocation = authority_claim_request(authority, "scope", "root", record, **overrides)
    before = authority.inspect("scope")
    assert json.loads(authority.perform(request, invocation))["status"] == "rejected"
    assert authority.inspect("scope") == before


def test_generic_registration_preserves_binding_and_expiry_does_not_prove_retry_safe(tmp_path):
    authority, record = generic_authority_fixture(tmp_path)
    before = authority.inspect("scope")
    authority.register_effect_work("scope", "owner", "0", "root", "effect", "token")
    assert authority.inspect("scope") == before
    with pytest.raises(ValueError, match="invalid_host_request"):
        authority.register_effect_work("scope", "owner", "0", "other-root", "effect", "token")
    request, invocation = authority_claim_request(authority, "scope", "root", record)
    assert json.loads(authority.perform(request, invocation))["status"] == "accepted"
    request, invocation = authority_claim_request(authority, "scope", "root", record, "retry", "1")
    invocation["trusted_clock_now"] = "10"
    before = authority.inspect("scope")
    assert json.loads(authority.perform(request, invocation))["error_code"] == "stale_attempt_fence"
    assert authority.inspect("scope") == before


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


def test_missing_native_work_cannot_be_rebound_to_another_authorized_root(tmp_path):
    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    ledger = authority.inspect(scope)
    ledger["roots"].append("other-authorized-root")
    with authority._connect() as connection:
        connection.execute("UPDATE determa_scope_authority SET ledger=?", (json.dumps(ledger),))
        connection.execute("DELETE FROM determa_committed_effects")
        connection.commit()
    request, invocation = authority_claim_request(authority, scope, "other-authorized-root", record)
    assert json.loads(authority.perform(request, invocation))["status"] == "rejected"
    assert authority.inspect(scope) == ledger


def test_invalid_terminal_outcome_cannot_corrupt_persisted_checkpoint(tmp_path):
    from determa.state.wire import ArtifactError

    host, root, request, _ = host_fixture(tmp_path)
    before = host.snapshot(root)
    with pytest.raises(ArtifactError):
        host.terminalize_outbox(root, request["effect_id"], {"status": "bogus"})
    assert host.snapshot(root) == before


def test_confirmed_outbox_does_not_remove_the_business_invocation_payload(tmp_path):
    host, root, request, context = host_fixture(tmp_path)
    record = host.snapshot(root)["journal"]["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    calls = []
    host = installed_test_handler(
        host, lambda *arguments: calls.append(arguments) or {"invoked": True}
    )
    host.trusted_clock = lambda: "0"
    host.terminalize_outbox(root, request["effect_id"], {"status": "confirmed"})
    assert host.dispatch(root, request["effect_id"], credential="test-credential", **context) == {
        "invoked": True
    }
    assert len(calls) == 1
    assert (
        calls[0][0]
        == host.snapshot(root)["checkpoint"]["terminal_outbox_records"][0]["intent"]["payload"]
    )


def test_multi_effect_recovery_commits_one_journal_revision_per_effect(tmp_path):
    from determa.state.checkpoint_v1 import admit_checkpoint_v1, step_checkpoint_v1
    from determa.state.host import outbox_intent_digest
    from determa.state.queueing import _entry_digest

    original, root, _, _ = host_fixture(tmp_path)
    checkpoint = original.snapshot(root)["checkpoint"]
    entry = read("accepted-checkpoint.json")["root_record"]["aggregate_state"]["runtimes"][0][
        "ready_mailbox"
    ][0]
    envelope = copy.deepcopy(entry["envelope"])
    envelope["event_id"] = "invoke-second"
    envelope["cause_id"] = "invoke-second"
    delivery = {
        "delivery_mode": entry["delivery_mode"],
        "envelope": envelope,
        "envelope_digest": _entry_digest(root, entry["delivery_mode"], envelope),
    }
    admitted = admit_checkpoint_v1(
        checkpoint,
        [delivery],
        original.resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    checkpoint = step_checkpoint_v1(
        admitted,
        checkpoint["root_record"]["aggregate_state"]["root_runtime_id"],
        original.resolver,
        expected_revision=admitted["revision"],
        expected_checkpoint_digest=admitted["execution_checkpoint_digest"],
    )
    journal = read("data/unclaimed-journal.json")
    template = journal["effect_records"][0]
    journal["effect_records"] = []
    for item in checkpoint["pending_outbox_intents"]:
        record = copy.deepcopy(template)
        record["effect_id"] = item["intent"]["effect_id"]
        record["intent_digest"] = outbox_intent_digest(root, item["intent"])
        journal["effect_records"].append(record)
    journal["effect_records"].sort(key=lambda record: record["effect_id"].encode())
    journal["checkpoint_revision"] = checkpoint["revision"]
    journal["checkpoint_digest"] = checkpoint["execution_checkpoint_digest"]
    journal = seal_journal(journal)
    host = SQLiteCommittedEffectHost(tmp_path / "two.sqlite", original.resolver, {}, lambda *_: {})
    host.setup_schema()
    host.seed(checkpoint, journal)
    for record in journal["effect_records"]:
        host.claim(root, record["effect_id"], "worker", "0", expires_at="10", trusted_now="0")
    initial_revision = int(host.snapshot(root)["journal"]["journal_revision"])
    committed_revisions = []
    transact = host._transact

    def observe_transaction(*args, **kwargs):
        result = transact(*args, **kwargs)
        committed_revisions.append(int(host.snapshot(root)["journal"]["journal_revision"]))
        return result

    host._transact = observe_transaction
    recovered = host.recover(root)
    assert len(recovered["journal"]["effect_records"]) == 2
    assert {record["invocation_state"] for record in recovered["journal"]["effect_records"]} == {
        "ambiguous"
    }
    assert committed_revisions == [initial_revision + 1, initial_revision + 2]


def test_dispatch_crossing_lease_expiry_preserves_unresolved_work_for_recovery(tmp_path):
    host, root, request, context = host_fixture(tmp_path)
    record = host.snapshot(root)["journal"]["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    expiry = read("data/active-claim.json")["expires_at"]
    current_time = ["0"]
    host.trusted_clock = lambda: current_time[0]
    calls = []

    def external_acceptance(*args):
        calls.append(args)
        current_time[0] = expiry
        return {"accepted": True}

    host = installed_test_handler(host, external_acceptance)
    before = host.snapshot(root)
    with pytest.raises(EffectError, match="stale_attempt_fence"):
        host.dispatch(root, request["effect_id"], credential="test-credential", **context)
    assert len(calls) == 1
    assert host.snapshot(root) == before
    assert host.recover(root)["journal"]["effect_records"][0]["invocation_state"] == "ambiguous"


@pytest.mark.parametrize(
    "replacement", ["raw_callback", "callback_code", "method", "configuration", "instance"]
)
def test_native_handler_binding_rejects_changed_execution_or_destination(
    tmp_path, monkeypatch, replacement
):
    host, root, request, context = host_fixture(tmp_path)
    record = host.snapshot(root)["journal"]["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    calls = []

    def callback(*args):
        return calls.append(args) or {}

    host = installed_test_handler(host, callback)
    host.trusted_clock = lambda: "0"
    if replacement == "raw_callback":
        host.handler = callback
    elif replacement == "callback_code":

        def other(*args):
            return calls or {"changed": True}

        monkeypatch.setattr(callback, "__code__", other.__code__)
    elif replacement == "method":
        monkeypatch.setattr(NativeTestProvider, "invoke", lambda *args: {})
    elif replacement == "configuration":
        host.handler._configured._configuration["destination_binding_digest"] = "sha256:" + "0" * 64
    else:
        host.handler._configured._instance["destination_binding_digest"] = "sha256:" + "0" * 64
    before = host.snapshot(root)
    with pytest.raises(EffectError, match="host_capability_mismatch"):
        host.dispatch(root, request["effect_id"], credential="test-credential", **context)
    assert calls == []
    assert host.snapshot(root) == before


@pytest.mark.parametrize("member", ["__init__", "__repr__", "__eq__", "__setattr__", "invoke"])
def test_handler_fixture_verifier_rejects_changed_generated_or_executable_code(monkeypatch, member):
    from conformance.effects_adapter import _loaded_handler, _verified_fixture_handler

    module = _loaded_handler()
    assert _verified_fixture_handler(module)
    if member == "invoke":
        monkeypatch.setattr(module, member, lambda *args: {})
    else:
        monkeypatch.setattr(module.NativeReply, member, lambda *args: None)
    assert not _verified_fixture_handler(module)


@pytest.mark.parametrize("method", ["invoke", "verify"])
def test_verified_native_handle_cannot_shadow_its_trusted_methods(tmp_path, method):
    host, root, request, context = host_fixture(tmp_path)
    record = host.snapshot(root)["journal"]["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    calls = []
    host = installed_test_handler(host, lambda *args: calls.append(args) or {"accepted": True})
    host.trusted_clock = lambda: "0"
    with pytest.raises((AttributeError, TypeError)):
        setattr(host.handler, method, lambda *args: {"unverified": True})
    with pytest.raises(AttributeError):
        object.__setattr__(host.handler, method, lambda *args: {"unverified": True})
    assert host.dispatch(root, request["effect_id"], credential="test-credential", **context) == {
        "accepted": True
    }
    assert len(calls) == 1
