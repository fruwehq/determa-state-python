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
    def __init__(self, callback, proof_verifier=None):
        self.callback = callback
        self.proof_verifier = proof_verifier

    def validate_configuration(self, configuration):
        return copy.deepcopy(configuration)

    def capabilities(self, instance):
        return []

    def health(self, instance):
        return "healthy"

    def invoke(self, instance, payload, metadata, attempt):
        return self.callback(payload, metadata, attempt)

    def verify_deduplication_evidence(self, instance, evidence):
        return self.proof_verifier is not None and self.proof_verifier(evidence)


def installed_test_handler(host, callback, proof_verifier=None):
    from determa.state.effects import VerifiedNativeHandler
    from determa.state.extensions import ExtensionRegistry

    provider = NativeTestProvider(callback, proof_verifier)

    def factory():
        return provider

    proof_code = proof_verifier.__code__ if proof_verifier is not None else None
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
            and provider.proof_verifier is proof_verifier
            and (proof_verifier is None or proof_verifier.__code__ is proof_code)
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
        host.path,
        host.resolver,
        host.route,
        handler,
        authority_scope=host.authority_scope,
        trusted_clock=host.trusted_clock,
    )


def installed_retry_handler(host, root):
    """Authenticate scoped native receipt bytes independently of worker input."""
    import base64
    import sqlite3

    saved = host.snapshot(root)
    record = saved["journal"]["effect_records"][0]
    context = SQLiteCommittedEffectHost._retry_context(saved, record)
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    destination = host.path + ".destination"
    receipt = json.dumps(
        {key: value for key, value in context.items() if key != "attempt_fence"}, sort_keys=True
    ).encode()
    with sqlite3.connect(destination) as connection:
        connection.execute("CREATE TABLE receipts (scope TEXT, effect TEXT, receipt BLOB)")
        connection.execute(
            "INSERT INTO receipts VALUES (?, ?, ?)",
            (context["scope_identity"], record["effect_id"], receipt),
        )
    observations = []

    def verify_receipts(evidence):
        observations.append(copy.deepcopy(evidence))
        with sqlite3.connect(destination) as connection:
            row = connection.execute(
                "SELECT receipt FROM receipts WHERE scope=? AND effect=?",
                (evidence["scope_identity"], evidence["effect_id"]),
            ).fetchone()
        return (
            row is not None
            and all(
                base64.b64decode(evidence[name], validate=True) == row[0]
                for name in (
                    "first_attempt_receipt_bytes_base64",
                    "repeat_attempt_receipt_bytes_base64",
                )
            )
            and json.loads(row[0])
            == {
                key: value
                for key, value in evidence.items()
                if key
                not in {
                    "attempt_fence",
                    "first_attempt_receipt_bytes_base64",
                    "repeat_attempt_receipt_bytes_base64",
                }
            }
        )

    host = installed_test_handler(host, lambda *_: {}, verify_receipts)
    proof = {
        **context,
        "first_attempt_receipt_bytes_base64": base64.b64encode(receipt).decode(),
        "repeat_attempt_receipt_bytes_base64": base64.b64encode(receipt).decode(),
    }
    return host, proof, observations


def host_fixture(tmp_path, observer=None):
    bundle = load_bundle((CASE / "machine.yaml").read_text())
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    host = SQLiteCommittedEffectHost(
        tmp_path / "effects.sqlite",
        resolver,
        {},
        None,
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


def test_other_trusted_root_incarnation_refuses_before_native_seed(tmp_path):
    """An independently resolvable origin is not this checkpoint's root identity."""
    from determa.state.wire import hash_value

    checkpoint = read("pending-checkpoint.json")
    journal = read("data/leased-journal.json")
    original = load_bundle((CASE / "machine.yaml").read_text())
    alternate = load_bundle(
        (CASE / "machine.yaml")
        .read_text()
        .replace(
            "namespace: conformance.committed_native_effects",
            "namespace: conformance.other_trusted_definition",
        )
    )
    resolver = MemoryArtifactResolver(
        definitions={original.fingerprint: original, alternate.fingerprint: alternate}
    )
    record = journal["effect_records"][0]
    origin = record["target"]["runtime_incarnation"]
    definition = origin["definition"]
    definition["validated_bundle_fingerprint"] = alternate.fingerprint
    definition["machine"]["namespace"] = alternate.namespace
    machine = definition["machine"]
    record["target"]["runtime_id"] = hash_value(
        [
            "determa-root-runtime-identity-1",
            "1",
            alternate.fingerprint,
            machine["namespace"],
            machine["machine_id"],
            machine["machine_version"],
            checkpoint["root_instance_id"],
        ]
    )
    journal = seal_journal(journal)
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        validate_journal(checkpoint, journal, resolver)
    host = SQLiteCommittedEffectHost(tmp_path / "invalid-seed.sqlite", resolver, {}, None)
    host.setup_schema()
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        host.seed(checkpoint, journal)
    with host._connect() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM determa_committed_effects").fetchone()[0] == 0
        )


@pytest.mark.parametrize("state", ["unclaimed", "leased", "ambiguous"])
def test_prevented_start_without_cancelled_outcome_cannot_be_native_work(tmp_path, state):
    checkpoint = read("pending-checkpoint.json")
    journal = read("data/unclaimed-journal.json")
    record = journal["effect_records"][0]
    record["invocation_state"] = state
    record["attempt_fence"] = "0" if state == "unclaimed" else "1"
    record["cancellation"] = {
        "operation_id": "cancel-before-claim",
        "reason": "user_requested",
        "state": "prevented_start",
    }
    _assert_invalid_native_seed(tmp_path, checkpoint, seal_journal(journal))


@pytest.mark.parametrize("state", ["unclaimed", "leased", "ambiguous"])
def test_stripping_terminal_outcome_never_makes_native_work_claimable(tmp_path, state):
    checkpoint = read("pending-checkpoint.json")
    journal = read("data/outcome-recorded-journal.json")
    journal["effect_records"][0].update(
        invocation_state=state, outcome=None, result_event_id=None, admission_receipt=None
    )
    _assert_invalid_native_seed(tmp_path, checkpoint, seal_journal(journal))


def _assert_invalid_native_seed(tmp_path, checkpoint, journal):
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        validate_journal(checkpoint, journal)
    bundle = load_bundle((CASE / "machine.yaml").read_text())
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    fresh = SQLiteCommittedEffectHost(tmp_path / "bad-seed.sqlite", resolver, {}, None)
    fresh.setup_schema()
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        fresh.seed(checkpoint, journal)
    with fresh._connect() as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM determa_committed_effects").fetchone()[0] == 0
        )


@pytest.mark.parametrize(
    ("state", "report_kind", "report_fence"),
    [
        ("unclaimed", "retryable_failure", "0"),
        ("unclaimed", "ambiguous", "1"),
        ("ambiguous", "retryable_failure", "1"),
        ("leased", "retryable_failure", "1"),
    ],
)
def test_completed_attempt_report_cannot_disagree_with_native_invocation_state(
    state, report_kind, report_fence
):
    from determa.state.wire import hash_value

    checkpoint = read("pending-checkpoint.json")
    journal = read("data/leased-journal.json")
    record = journal["effect_records"][0]
    reason = (
        "no_call_proven" if report_kind == "retryable_failure" else "provider_acceptance_unknown"
    )
    record["invocation_state"] = state
    record["attempt_records"] = [
        {
            "attempt_fence": report_fence,
            "report_kind": report_kind,
            "report_digest": hash_value(
                [
                    "determa-effect-attempt-report-1",
                    record["effect_id"],
                    record["operation_token"],
                    report_fence,
                    report_kind,
                    ["map", []],
                    reason,
                ]
            ),
            "reason": reason,
        }
    ]
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        validate_journal(checkpoint, seal_journal(journal))


def test_terminal_outcome_cannot_supersede_a_newer_attempt_fence():
    journal = read("data/outcome-recorded-journal.json")
    journal["effect_records"][0]["attempt_fence"] = "2"
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        validate_journal(read("pending-checkpoint.json"), seal_journal(journal))


def test_positive_fence_unclaimed_requires_current_safe_retry_report(tmp_path):
    journal = read("data/leased-journal.json")
    journal["effect_records"][0]["invocation_state"] = "unclaimed"
    _assert_invalid_native_seed(tmp_path, read("pending-checkpoint.json"), seal_journal(journal))


def test_terminal_outcome_cannot_follow_an_earlier_terminal_attempt(tmp_path):
    from determa.state.wire import hash_value

    journal = read("data/outcome-recorded-journal.json")
    record = journal["effect_records"][0]
    outcome = record["outcome"]
    report = copy.deepcopy(record["attempt_records"][0])
    record["attempt_fence"] = outcome["attempt_fence"] = report["attempt_fence"] = "2"
    report["report_digest"] = hash_value(
        [
            "determa-effect-attempt-report-1",
            record["effect_id"],
            record["operation_token"],
            "2",
            outcome["kind"],
            outcome["payload"],
            report["reason"],
        ]
    )
    outcome["digest"] = hash_value(
        [
            "determa-effect-outcome-1",
            record["effect_id"],
            record["operation_token"],
            outcome["kind"],
            outcome["payload"],
            "2",
        ]
    )
    record["attempt_records"].append(report)
    _assert_invalid_native_seed(tmp_path, read("pending-checkpoint.json"), seal_journal(journal))


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
        tmp_path / "pointer.sqlite", resolver, {}, None, trusted_clock=lambda: "0"
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
    host._record_result(root, request, **context)
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

    def expires_during_submission(*args, **kwargs):
        response = submit(*args, **kwargs)
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
        authority.path, resolver, {"authority_epoch": epoch}, None, authority_scope=scope
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
        authority.path, resolver, {"authority_epoch": "0"}, None, authority_scope=scope
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
        authority.path, resolver, {"authority_epoch": "0"}, None, authority_scope=scope
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


def test_native_worker_guard_refuses_unbound_claim_even_with_valid_native_root(tmp_path):
    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    request, invocation = authority_claim_request(authority, scope, root, record)
    claim = json.loads(authority.perform(request, invocation))["claim"]
    assert authority.check_worker_claim(claim, "worker-a", "0", phase="dispatch")
    unbound = {**claim, "work_identity": "unbound-work", "operation_token": "unbound-token"}
    with authority._connect() as connection:
        ledger = authority.inspect(scope)
        ledger["journal_entries"].append(
            {"work_identity": unbound["work_identity"], "attempt_fence": unbound["attempt_fence"]}
        )
        ledger["active_claims"].append(unbound)
        connection.execute(
            "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
            (json.dumps(ledger), scope),
        )
    before = host.snapshot(root)
    calls = []
    assert not authority.check_worker_claim(
        unbound, "worker-a", "0", phase="dispatch", on_dispatch=lambda: calls.append("called")
    )
    assert calls == []
    assert host.snapshot(root) == before


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
        # Keep the journal internally valid so this exercises the separate
        # native authority/journal fence mismatch, not missing retry evidence.
        document["journal"]["effect_records"][0]["attempt_records"] = read(
            "data/retryable-journal.json"
        )["effect_records"][0]["attempt_records"]
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
    command, invocation = authority_claim_request(authority, scope, root, record)
    claim = json.loads(authority.perform(command, invocation))["claim"]
    # Use the actual committed report transition rather than fabricating an older
    # portable journal and editing native authority to make it look admissible.
    report = {
        "effect_id": record["effect_id"],
        "operation_token": record["operation_token"],
        "attempt_fence": claim["attempt_fence"],
        "outcome_kind": "retryable_failure",
        "payload": ["map", []],
    }
    host.trusted_clock = lambda: "0"
    host, proof, _ = installed_retry_handler(host, root)
    response = host.submit_result(
        root,
        report,
        principal=claim["worker_principal"],
        scope=scope,
        epoch="0",
        trusted_now="0",
        deduplication_evidence=proof,
    )
    assert response["status"] == "report_recorded"
    saved = host.snapshot(root)
    ledger = authority.inspect(scope)
    assert ledger["active_claims"] == []
    assert claim in ledger["effect_claim_history"]
    with authority._connect() as connection:
        connection.execute("DELETE FROM determa_committed_effects")
        connection.commit()
    host.seed(saved["checkpoint"], saved["journal"], claim)
    assert authority.inspect(scope)["active_claims"] == []
    assert host.snapshot(root)["claims"][record["effect_id"]] == claim
    new_claim = host.claim(
        root,
        record["effect_id"],
        "worker-b",
        "0",
        expires_at="30",
        trusted_now="20",
        deduplication_evidence=proof,
    )
    assert new_claim["attempt_fence"] == "2"


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
    host = SQLiteCommittedEffectHost(
        tmp_path / "two.sqlite", original.resolver, {}, None, trusted_clock=lambda: "0"
    )
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
    after = host.snapshot(root)
    assert after["checkpoint"] == before["checkpoint"]
    assert after["journal"]["effect_records"] == before["journal"]["effect_records"]
    assert after["journal"] == before["journal"]
    assert after["responses"] == before["responses"]
    assert len(after["invocation_starts"]) == 1
    current_time[0] = "0"
    with pytest.raises(EffectError, match="native_invocation_already_started"):
        host.dispatch(root, request["effect_id"], credential="test-credential", **context)
    assert len(calls) == 1
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


@pytest.mark.parametrize(
    ("member", "value"), [("__firstlineno__", 999), ("__static_attributes__", ("unexpected",))]
)
def test_handler_fixture_verifier_checks_compiler_metadata_exactly(monkeypatch, member, value):
    from conformance.effects_adapter import _loaded_handler, _verified_fixture_handler

    module = _loaded_handler()
    assert _verified_fixture_handler(module)
    monkeypatch.setattr(module.NativeReply, member, value, raising=False)
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


def test_plain_callback_handler_is_refused_before_opening_storage(tmp_path):
    path = tmp_path / "unverified.sqlite"
    calls = []
    with pytest.raises(EffectError, match="host_capability_mismatch"):
        SQLiteCommittedEffectHost(
            path, MemoryArtifactResolver(), {}, lambda *args: calls.append(args) or {}
        )
    assert calls == []
    assert not path.exists()


def test_public_result_requires_admission_before_committed_response(tmp_path):
    host, root, request, context = host_fixture(tmp_path)
    before = host.snapshot(root)
    with pytest.raises(TypeError, match="admit"):
        host.submit_result(root, request, admit=False, **context)
    assert host.snapshot(root) == before
    staged = host._record_result(root, request, **context)
    assert staged["status"] == "outcome_recorded"
    assert staged["admission_receipt"] is None
    response = host.submit_result(root, request, **context)
    assert response["status"] == "committed"
    assert response["admission_receipt"] in host.snapshot(root)["checkpoint"]["operation_receipts"]


@pytest.mark.parametrize("operation", ["result", "cancel"])
@pytest.mark.parametrize(
    "damage", ["extra", "missing_payload", "native_payload", "missing_identity"]
)
def test_effect_requests_fail_closed_before_mutation_or_core_call(tmp_path, operation, damage):
    calls = []
    host, root, request, context = host_fixture(tmp_path, lambda *args: calls.append(args))
    if operation == "cancel":
        request = read("data/cancel-request.json")
    if damage == "extra":
        request["unexpected"] = True
    elif damage == "missing_payload":
        del request["payload"]
    elif damage == "native_payload":
        request["payload"] = object()
    else:
        del request["effect_id"]
    before = host.snapshot(root)

    def invoke():
        return (
            host.submit_result(root, request, **context)
            if operation == "result"
            else host.cancel(root, request)
        )

    if damage == "missing_identity":
        with pytest.raises(EffectError, match="invalid_host_request"):
            invoke()
    else:
        response = invoke()
        assert response["status"] == "rejected"
        assert response["error_code"] == "invalid_host_request"
    assert calls == []
    assert host.snapshot(root) == before


def test_noncanonical_result_fence_is_refused_before_storage_mutation(tmp_path):
    host, root, request, context = host_fixture(tmp_path)
    request["attempt_fence"] = "01"
    before = host.snapshot(root)
    with pytest.raises(EffectError, match="invalid_host_request"):
        host.submit_result(root, request, **context)
    assert host.snapshot(root) == before


@pytest.mark.parametrize("operation_ids", [["z", "a"], ["dup", "dup"]])
def test_journal_response_references_require_strictly_ordered_unique_ids(operation_ids):
    journal = read("data/leased-journal.json")
    journal["operation_response_references"] = [
        {"operation_id": name, "response_digest": "sha256:" + str(index) * 64}
        for index, name in enumerate(operation_ids)
    ]
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        validate_journal(read("pending-checkpoint.json"), seal_journal(journal))


def test_restoration_rejects_admitted_journal_without_checkpoint_admission(tmp_path):
    host, root, request, context = host_fixture(tmp_path)
    before = host.snapshot(root)["checkpoint"]
    host.submit_result(root, request, **context)
    journal = host.snapshot(root)["journal"]
    journal["checkpoint_revision"] = before["revision"]
    journal["checkpoint_digest"] = before["execution_checkpoint_digest"]
    restored = SQLiteCommittedEffectHost(tmp_path / "torn.sqlite", host.resolver, {}, None)
    restored.setup_schema()
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        restored.seed(before, seal_journal(journal))
    with restored._connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM determa_committed_effects").fetchone() == (
            0,
        )


def test_compact_outbox_location_retains_valid_admitted_journal_pair(tmp_path):
    from determa.state.checkpoint import seal_execution_checkpoint
    from determa.state.checkpoint_v1 import restore_execution_checkpoint_v1
    from determa.state.host import outbox_intent_digest

    host, root, request, context = host_fixture(tmp_path)
    host.submit_result(root, request, **context)
    host.terminalize_outbox(root, request["effect_id"], {"status": "confirmed"})
    saved = host.snapshot(root)
    checkpoint, journal = saved["checkpoint"], saved["journal"]
    terminal = checkpoint["terminal_outbox_records"].pop()
    checkpoint["outbox_effect_tombstones"].append(
        {
            "terminal_sequence": terminal["terminal_sequence"],
            "effect_id": terminal["intent"]["effect_id"],
            "intent_digest": outbox_intent_digest(root, terminal["intent"]),
            "committed_revision": terminal["committed_revision"],
            "outcome": terminal["outcome"],
        }
    )
    checkpoint["revision"] = str(int(checkpoint["revision"]) + 1)
    checkpoint = seal_execution_checkpoint(checkpoint)
    restore_execution_checkpoint_v1(checkpoint, host.resolver)
    journal["checkpoint_revision"] = checkpoint["revision"]
    journal["checkpoint_digest"] = checkpoint["execution_checkpoint_digest"]
    journal["journal_revision"] = str(int(journal["journal_revision"]) + 1)
    journal = seal_journal(journal)
    validate_journal(checkpoint, journal)
    restored = SQLiteCommittedEffectHost(tmp_path / "compact.sqlite", host.resolver, {}, None)
    restored.setup_schema()
    restored.seed(checkpoint, journal)
    assert restored.recover(root)["journal"] == journal
    before = restored.snapshot(root)
    record = journal["effect_records"][0]
    with pytest.raises(EffectError, match="replay_evidence_expired"):
        restored.produce(
            root,
            "produce-1",
            checkpoint["root_record"]["aggregate_state"]["root_runtime_id"],
            expected_revision="stale",
            expected_digest="stale",
            route_generation="999",
            operation_token=record["operation_token"],
        )
    assert restored.snapshot(root) == before


@pytest.mark.parametrize(
    "changed",
    ["payload", "token", "event", "target", "target_root", "origin", "receipt_kind", "mode"],
)
def test_admission_receipt_authenticates_exact_pinned_result(tmp_path, changed):
    from determa.state.wire import hash_value

    host, root, request, context = host_fixture(tmp_path)
    host.submit_result(root, request, **context)
    saved = host.snapshot(root)
    journal = saved["journal"]
    record = journal["effect_records"][0]
    outcome = record["outcome"]
    if changed == "payload":
        outcome["payload"][1][0][1][1] = "altered"
    elif changed == "token":
        record["operation_token"] = "altered"
    elif changed == "event":
        record["result_mapping"][0]["event"] = "native_cancelled"
    elif changed == "target":
        record["target"]["runtime_id"] = "different-runtime"
    elif changed == "target_root":
        record["target"]["root_instance_id"] = "foreign-root"
    elif changed == "origin":
        record["target"]["runtime_incarnation"]["definition"]["validated_bundle_fingerprint"] = (
            "sha256:" + "0" * 64
        )
    elif changed == "receipt_kind":
        record["admission_receipt"]["operation_kind"] = "event_terminal"
    else:
        record["admission_receipt"]["delivery_mode"] = "signal"
    outcome["digest"] = hash_value(
        [
            "determa-effect-outcome-1",
            record["effect_id"],
            record["operation_token"],
            outcome["kind"],
            outcome["payload"],
            outcome["attempt_fence"],
        ]
    )
    report = record["attempt_records"][0]
    report["report_digest"] = hash_value(
        [
            "determa-effect-attempt-report-1",
            record["effect_id"],
            record["operation_token"],
            outcome["attempt_fence"],
            outcome["kind"],
            outcome["payload"],
            report["reason"],
        ]
    )
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        validate_journal(saved["checkpoint"], seal_journal(journal), host.resolver)


def test_historical_result_evidence_survives_processing(tmp_path):
    from determa.state.checkpoint_v1 import step_checkpoint_v1

    host, root, request, context = host_fixture(tmp_path)
    host.submit_result(root, request, **context)
    saved = host.snapshot(root)
    checkpoint = saved["checkpoint"]
    checkpoint = step_checkpoint_v1(
        checkpoint,
        checkpoint["root_record"]["aggregate_state"]["root_runtime_id"],
        host.resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    journal = saved["journal"]
    journal["checkpoint_revision"] = checkpoint["revision"]
    journal["checkpoint_digest"] = checkpoint["execution_checkpoint_digest"]
    journal = seal_journal(journal)
    validate_journal(checkpoint, journal, host.resolver)
    restored = SQLiteCommittedEffectHost(tmp_path / "processed.sqlite", host.resolver, {}, None)
    restored.setup_schema()
    restored.seed(checkpoint, journal)
    assert restored.recover(root)["journal"] == journal


@pytest.mark.parametrize(
    "damage",
    [
        None,
        "boolean",
        "fabricated",
        "wrong_work",
        "wrong_destination",
        "wrong_root",
        "wrong_token",
        "wrong_fence",
        "wrong_handler",
        "no_native_receipt",
        "unavailable",
    ],
)
def test_ambiguous_retry_requires_verified_native_destination_evidence(tmp_path, damage):
    import base64
    import sqlite3

    original, root, request, _ = host_fixture(tmp_path)
    journal = read("data/ambiguous-journal.json")
    host = SQLiteCommittedEffectHost(
        tmp_path / "ambiguous.sqlite", original.resolver, {}, None, trusted_clock=lambda: "0"
    )
    host.setup_schema()
    host.seed(read("pending-checkpoint.json"), journal)
    record = journal["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    native_path = tmp_path / "destination.sqlite"
    receipt_bytes = b"actual native receipt for scoped effect"
    with sqlite3.connect(native_path) as connection:
        connection.execute(
            "CREATE TABLE receipts (scope TEXT, effect TEXT, receipt BLOB, calls INTEGER)"
        )
        if damage != "no_native_receipt":
            connection.execute(
                "INSERT INTO receipts VALUES (?, ?, ?, 2)",
                (
                    journal["scope_identity"],
                    record["effect_id"],
                    receipt_bytes,
                ),
            )

    def verify_receipts(evidence):
        if damage == "unavailable":
            raise RuntimeError("destination proof unavailable")
        with sqlite3.connect(native_path) as connection:
            row = connection.execute(
                "SELECT receipt, calls FROM receipts WHERE scope = ? AND effect = ?",
                (
                    evidence["scope_identity"],
                    evidence["effect_id"],
                ),
            ).fetchone()
        return (
            row is not None
            and row[1] >= 2
            and all(
                base64.b64decode(evidence[name], validate=True) == row[0]
                for name in (
                    "first_attempt_receipt_bytes_base64",
                    "repeat_attempt_receipt_bytes_base64",
                )
            )
        )

    host = installed_test_handler(host, lambda *_: {}, verify_receipts)
    encoded = base64.b64encode(receipt_bytes).decode()
    proof = {
        "kind": "destination_deduplication",
        "root_instance_id": root,
        "operation_token": record["operation_token"],
        "attempt_fence": record["attempt_fence"],
        "handler_reference": copy.deepcopy(record["handler_reference"]),
        "scope_identity": journal["scope_identity"],
        "effect_id": record["effect_id"],
        "destination_binding_digest": record["destination_binding_digest"],
        "first_attempt_receipt_bytes_base64": encoded,
        "repeat_attempt_receipt_bytes_base64": encoded,
    }
    if damage == "boolean":
        proof = True
    elif damage == "fabricated":
        proof["first_attempt_receipt_bytes_base64"] = proof[
            "repeat_attempt_receipt_bytes_base64"
        ] = base64.b64encode(b"fabricated equal bytes").decode()
    elif damage == "wrong_work":
        proof["effect_id"] = "sha256:" + "0" * 64
    elif damage == "wrong_destination":
        proof["destination_binding_digest"] = "sha256:" + "0" * 64
    elif damage == "wrong_root":
        proof["root_instance_id"] = "other-root"
    elif damage == "wrong_token":
        proof["operation_token"] = "other-token"
    elif damage == "wrong_fence":
        proof["attempt_fence"] = "0"
    elif damage == "wrong_handler":
        proof["handler_reference"]["identifier"] = "other-handler"
    before = host.snapshot(root)
    if damage is not None:
        with pytest.raises(EffectError, match="host_capability_mismatch"):
            host.claim(
                root,
                request["effect_id"],
                "worker",
                "0",
                expires_at="10",
                trusted_now="0",
                deduplication_evidence=proof,
            )
        assert host.snapshot(root) == before
    else:
        claim = host.claim(
            root,
            request["effect_id"],
            "worker",
            "0",
            expires_at="10",
            trusted_now="0",
            deduplication_evidence=proof,
        )
        assert int(claim["attempt_fence"]) == int(record["attempt_fence"]) + 1
        saved = host.snapshot(root)["destination_evidence"][record["effect_id"]][0]
        assert saved["evidence"] == proof
        assert saved["root_instance_id"] == root
        assert saved["attempt_fence"] == claim["attempt_fence"]
        reopened = SQLiteCommittedEffectHost(host.path, host.resolver, {}, None)
        assert (
            reopened.snapshot(root)["destination_evidence"]
            == host.snapshot(root)["destination_evidence"]
        )


def test_ambiguous_retry_does_not_accept_legacy_boolean_keyword(tmp_path):
    host, root, request, _ = host_fixture(tmp_path)
    with pytest.raises(TypeError, match="deduplication_proven"):
        host.claim(
            root,
            request["effect_id"],
            "worker",
            "0",
            expires_at="10",
            trusted_now="0",
            deduplication_proven=True,
        )


@pytest.mark.parametrize(
    "damage",
    [
        None,
        "missing",
        "boolean",
        "fabricated",
        "wrong_root",
        "wrong_token",
        "wrong_fence",
        "wrong_handler",
        "unavailable",
    ],
)
def test_retryable_report_requires_native_original_invocation_proof(tmp_path, damage):
    import base64

    host, root, request, context = host_fixture(tmp_path)
    host, proof, checks = installed_retry_handler(host, root)
    report = {**request, "outcome_kind": "retryable_failure", "payload": ["map", []]}
    if damage == "missing":
        proof = None
    elif damage == "boolean":
        proof = True
    elif damage == "fabricated":
        proof["first_attempt_receipt_bytes_base64"] = proof[
            "repeat_attempt_receipt_bytes_base64"
        ] = base64.b64encode(b"fabricated equal receipts").decode()
    elif damage == "wrong_root":
        proof["root_instance_id"] = "other-root"
    elif damage == "wrong_token":
        proof["operation_token"] = "other-token"
    elif damage == "wrong_fence":
        proof["attempt_fence"] = "0"
    elif damage == "wrong_handler":
        proof["handler_reference"]["identifier"] = "other-handler"
    elif damage == "unavailable":
        host.handler._configured._provider.proof_verifier = None
    before = host.snapshot(root)
    response = host.submit_result(root, report, **context, deduplication_evidence=proof)
    if damage is not None:
        assert response["error_code"] == "host_capability_mismatch"
        assert host.snapshot(root) == before
    else:
        assert response["status"] == "report_recorded"
        assert response["attempt_report"]["reason"] == "destination_deduplication_proven"
        assert checks == [proof, proof]
        retained = host.snapshot(root)["destination_evidence"][request["effect_id"]][0]
        assert retained == {
            "decision": "report",
            "attempt_fence": request["attempt_fence"],
            "evidence": proof,
        }


def test_safe_report_replay_and_later_claim_never_reuse_old_verification(tmp_path):
    host, root, request, context = host_fixture(tmp_path)
    host, proof, checks = installed_retry_handler(host, root)
    report = {**request, "outcome_kind": "retryable_failure", "payload": ["map", []]}
    first = host.submit_result(root, report, **context, deduplication_evidence=proof)
    assert first["status"] == "report_recorded"
    before = host.snapshot(root)
    with pytest.raises(EffectError, match="host_capability_mismatch"):
        host.claim(
            root,
            request["effect_id"],
            "worker-b",
            context["epoch"],
            expires_at="10",
            trusted_now="0",
        )
    assert host.snapshot(root) == before
    host.claim(
        root,
        request["effect_id"],
        "worker-b",
        context["epoch"],
        expires_at="10",
        trusted_now="0",
        deduplication_evidence=proof,
    )
    assert len(checks) == 4
    host.handler._configured._provider.proof_verifier = None
    host.resolver = MemoryArtifactResolver(definitions={})
    with host._connect() as connection:
        before_bytes = connection.execute(
            "SELECT document FROM determa_committed_effects"
        ).fetchone()[0]
    assert host.submit_result(root, report, **context) == first
    assert len(checks) == 4
    changed = {**report, "payload": ["map", [["changed", ["boolean", True]]]]}
    assert host.submit_result(root, changed, **context)["error_code"] == "effect_result_conflict"
    with host._connect() as connection:
        assert (
            connection.execute("SELECT document FROM determa_committed_effects").fetchone()[0]
            == before_bytes
        )


def test_authority_noninitial_native_claim_cannot_bypass_retry_verifier(tmp_path):
    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    command, invocation = authority_claim_request(authority, scope, root, record)
    claim = json.loads(authority.perform(command, invocation))["claim"]
    host.trusted_clock = lambda: "0"
    host, proof, checks = installed_retry_handler(host, root)
    report = {
        "effect_id": record["effect_id"],
        "operation_token": record["operation_token"],
        "attempt_fence": claim["attempt_fence"],
        "outcome_kind": "retryable_failure",
        "payload": ["map", []],
    }
    assert (
        host.submit_result(
            root,
            report,
            principal=claim["worker_principal"],
            scope=scope,
            epoch="0",
            trusted_now="0",
            deduplication_evidence=proof,
        )["status"]
        == "report_recorded"
    )
    before = host.snapshot(root), authority.inspect(scope)
    command, invocation = authority_claim_request(
        authority,
        scope,
        root,
        record,
        expected="1",
        operation="second-claim-without-native-verification",
    )
    response = json.loads(authority.perform(command, invocation))
    assert response["status"] == "rejected"
    assert response["error_code"] == "host_capability_mismatch"
    assert (host.snapshot(root), authority.inspect(scope)) == before
    assert len(checks) == 2


@pytest.mark.parametrize("damage", [None, "unavailable", "method", "configuration", "instance"])
@pytest.mark.parametrize("operation", ["claim", "report"])
def test_retry_verifier_is_rechecked_after_native_sql_staging(
    tmp_path, monkeypatch, damage, operation
):
    import base64
    import sqlite3

    original, root, request, context = host_fixture(tmp_path)
    journal = read(
        "data/ambiguous-journal.json" if operation == "claim" else "data/leased-journal.json"
    )
    host = SQLiteCommittedEffectHost(tmp_path / "retry-staging.sqlite", original.resolver, {}, None)
    host.setup_schema()
    host.seed(
        read("pending-checkpoint.json"),
        journal,
        read("data/active-claim.json") if operation == "report" else None,
    )
    host.trusted_clock = lambda: "0"
    record = journal["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    receipt = b"independently stored destination receipt"
    native_path = tmp_path / "retry-destination.sqlite"
    with sqlite3.connect(native_path) as connection:
        connection.execute("CREATE TABLE receipts (receipt BLOB NOT NULL)")
        connection.execute("INSERT INTO receipts VALUES (?)", (receipt,))
    state = {"staged": False, "checks": 0}

    def verify_receipts(evidence):
        state["checks"] += 1
        if state["staged"] and damage == "unavailable":
            raise RuntimeError("native verifier unavailable after staging")
        with sqlite3.connect(native_path) as connection:
            stored = connection.execute("SELECT receipt FROM receipts").fetchone()[0]
        return all(
            base64.b64decode(evidence[name], validate=True) == stored
            for name in (
                "first_attempt_receipt_bytes_base64",
                "repeat_attempt_receipt_bytes_base64",
            )
        )

    host = installed_test_handler(host, lambda *_: {}, verify_receipts)
    encoded = base64.b64encode(receipt).decode()
    proof = {
        "kind": "destination_deduplication",
        "root_instance_id": root,
        "operation_token": record["operation_token"],
        "attempt_fence": record["attempt_fence"],
        "handler_reference": copy.deepcopy(record["handler_reference"]),
        "scope_identity": journal["scope_identity"],
        "effect_id": record["effect_id"],
        "destination_binding_digest": record["destination_binding_digest"],
        "first_attempt_receipt_bytes_base64": encoded,
        "repeat_attempt_receipt_bytes_base64": encoded,
    }
    before = host.snapshot(root)

    class StagingConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            cursor = super().execute(sql, parameters)
            if sql.startswith("UPDATE determa_committed_effects SET document"):
                # Observe the actual uncommitted row after SQLite has written it.
                staged = json.loads(
                    super()
                    .execute(
                        "SELECT document FROM determa_committed_effects WHERE root_instance_id=?",
                        (root,),
                    )
                    .fetchone()[0]
                )
                assert staged["claims"][request["effect_id"]]["attempt_fence"] == (
                    "2" if operation == "claim" else "1"
                )
                if operation == "report":
                    assert staged["journal"]["effect_records"][0]["invocation_state"] == "unclaimed"
                state["staged"] = True
                if damage == "method":
                    monkeypatch.setattr(
                        NativeTestProvider, "verify_deduplication_evidence", lambda *_: True
                    )
                elif damage == "configuration":
                    host.handler._configured._configuration["instance_id"] = "substituted"
                elif damage == "instance":
                    object.__setattr__(host.handler._configured, "_instance", {})
            return cursor

    monkeypatch.setattr(
        host,
        "_connect",
        lambda: sqlite3.connect(host.path, isolation_level=None, factory=StagingConnection),
    )
    arguments = {"expires_at": "10", "trusted_now": "0", "deduplication_evidence": proof}

    def execute():
        if operation == "claim":
            return host.claim(root, request["effect_id"], "worker", "0", **arguments)
        response = host.submit_result(
            root,
            {**request, "outcome_kind": "retryable_failure", "payload": ["map", []]},
            **context,
            deduplication_evidence=proof,
        )
        if response["status"] == "rejected":
            raise EffectError(response["error_code"])
        return response

    if damage is None:
        response = execute()
        assert response["attempt_fence"] == ("2" if operation == "claim" else "1")
        assert state["staged"] and state["checks"] >= 2
    else:
        with pytest.raises(EffectError, match="host_capability_mismatch"):
            execute()
        assert state["staged"]
        assert host.snapshot(root) == before


@pytest.mark.parametrize("clock", ["expired", "unavailable", "invalid", "missing"])
def test_initial_claim_requires_fresh_clock_after_actual_sql_staging(tmp_path, monkeypatch, clock):
    import sqlite3

    original, root, request, _ = host_fixture(tmp_path)
    state = {"staged": False}

    def now():
        if not state["staged"]:
            return "0"
        if clock == "unavailable":
            raise RuntimeError("clock unavailable")
        return "10" if clock == "expired" else "invalid"

    host = SQLiteCommittedEffectHost(
        tmp_path / "claim-clock.sqlite", original.resolver, {}, None, trusted_clock=now
    )
    host.setup_schema()
    host.seed(read("pending-checkpoint.json"), read("data/unclaimed-journal.json"))
    before = host.snapshot(root)

    class StagingConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            result = super().execute(sql, parameters)
            if sql.startswith("UPDATE determa_committed_effects SET document"):
                row = (
                    super()
                    .execute(
                        "SELECT document FROM determa_committed_effects WHERE root_instance_id=?",
                        (root,),
                    )
                    .fetchone()
                )
                assert json.loads(row[0])["claims"][request["effect_id"]]["attempt_fence"] == "1"
                state["staged"] = True
                if clock == "missing":
                    host.trusted_clock = None
            return result

    monkeypatch.setattr(
        host,
        "_connect",
        lambda: sqlite3.connect(host.path, isolation_level=None, factory=StagingConnection),
    )
    with pytest.raises(EffectError, match="stale_attempt_fence"):
        host.claim(root, request["effect_id"], "worker", "0", expires_at="10", trusted_now="0")
    assert state["staged"] and host.snapshot(root) == before


def test_historical_admission_survives_actual_root_completion_and_tombstone(tmp_path):
    import yaml

    from determa.state import ExecutionHost, MemoryExecutionStore, create_checkpoint_v1
    from determa.state.checkpoint_v1 import admit_checkpoint_v1, step_checkpoint_v1
    from determa.state.host import outbox_intent_digest
    from determa.state.queueing import _entry_digest
    from determa.state.wire import canonical_bytes

    machine = yaml.safe_load((CASE / "machine.yaml").read_text())
    events = machine["machines"][0]["root"]["on_events"]
    events["native_succeeded"] = {"transition_to": "done"}
    machine["machines"][0]["root"] = {
        "type": "composite",
        "initial": {"transition_to": "working"},
        "states": {"working": {"on_events": events}, "done": {"type": "final"}},
    }
    bundle = load_bundle(machine)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    root = "historical-root"
    checkpoint = create_checkpoint_v1(bundle, "workflow", root, "creation")
    runtime = checkpoint["root_record"]["aggregate_state"]["runtimes"][0]
    envelope = {
        "event": "invoke",
        "event_id": "invoke",
        "cause_id": "invoke",
        "source": {"host": True},
        "target": runtime["target_identity"],
        "payload": ["map", [["operation_token", ["string", "business-order-42"]]]],
    }
    checkpoint = admit_checkpoint_v1(
        checkpoint,
        [
            {
                "delivery_mode": "input",
                "envelope": envelope,
                "envelope_digest": _entry_digest(root, "input", envelope),
            }
        ],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    checkpoint = step_checkpoint_v1(
        checkpoint,
        runtime["runtime_id"],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    journal = read("data/unclaimed-journal.json")
    journal.update(
        root_instance_id=root,
        checkpoint_revision=checkpoint["revision"],
        checkpoint_digest=checkpoint["execution_checkpoint_digest"],
        operation_response_references=[],
    )
    record = journal["effect_records"][0]
    intent = checkpoint["pending_outbox_intents"][0]["intent"]
    record.update(
        effect_id=intent["effect_id"],
        intent_digest=outbox_intent_digest(root, intent),
        target={
            "root_instance_id": root,
            "runtime_id": runtime["runtime_id"],
            "runtime_incarnation": runtime["identity_origin"],
        },
    )
    host = SQLiteCommittedEffectHost(
        tmp_path / "terminal.sqlite", resolver, {}, None, trusted_clock=lambda: "0"
    )
    host.setup_schema()
    host.seed(checkpoint, seal_journal(journal))
    host.claim(root, record["effect_id"], "worker", "0", expires_at="10", trusted_now="0")
    request = read("data/result-request.json")
    request["effect_id"] = record["effect_id"]
    host.submit_result(
        root,
        request,
        principal="worker",
        scope=journal["scope_identity"],
        epoch="0",
        trusted_now="0",
    )
    host.terminalize_outbox(root, record["effect_id"], {"status": "confirmed"})
    saved = host.snapshot(root)
    checkpoint = saved["checkpoint"]
    checkpoint = step_checkpoint_v1(
        checkpoint,
        runtime["runtime_id"],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    assert checkpoint["root_record"]["aggregate_state"]["runtimes"][0]["status"] == "completed"
    portable_host = ExecutionHost(
        MemoryExecutionStore({root: canonical_bytes(checkpoint)}), resolver
    )
    portable_host.tombstone_root_v1(
        root,
        "tombstone",
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    checkpoint = portable_host.read_checkpoint(root).document
    journal = saved["journal"]
    journal["checkpoint_revision"] = checkpoint["revision"]
    journal["checkpoint_digest"] = checkpoint["execution_checkpoint_digest"]
    journal = seal_journal(journal)
    restored = SQLiteCommittedEffectHost(tmp_path / "tombstoned.sqlite", resolver, {}, None)
    restored.setup_schema()
    restored.seed(checkpoint, journal)
    assert restored.recover(root)["journal"] == journal


@pytest.mark.parametrize(
    "case, machine_id, kind",
    [
        ("54-stale-component-target", "owner", "component"),
        ("30-owned-spawn-cancel", "order", "owned_spawned_instance"),
    ],
)
def test_historical_child_target_uses_original_definition_after_removal(case, machine_id, kind):
    from determa.state import create_checkpoint_v1
    from determa.state.checkpoint_v1 import admit_checkpoint_v1, step_checkpoint_v1
    from determa.state.effects import _pinned_result_target
    from determa.state.queueing import _entry_digest

    bundle = load_bundle(
        (conformance_root() / "conformance/core" / case / "machine.yaml").read_text()
    )
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    checkpoint = create_checkpoint_v1(bundle, machine_id, "root", "creation")
    runtime = checkpoint["root_record"]["aggregate_state"]["runtimes"][0]
    envelope = {
        "event": "start",
        "event_id": "start",
        "cause_id": "start",
        "source": {"host": True},
        "target": runtime["target_identity"],
        "payload": ["map", []],
    }
    checkpoint = admit_checkpoint_v1(
        checkpoint,
        [
            {
                "delivery_mode": "input",
                "envelope": envelope,
                "envelope_digest": _entry_digest("root", "input", envelope),
            }
        ],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    checkpoint = step_checkpoint_v1(
        checkpoint,
        runtime["runtime_id"],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    child = next(
        item
        for item in checkpoint["root_record"]["aggregate_state"]["runtimes"]
        if item["identity_origin"]["kind"] == kind
    )
    record = {
        "target": {
            "root_instance_id": "root",
            "runtime_id": child["runtime_id"],
            "runtime_incarnation": copy.deepcopy(child["identity_origin"]),
        }
    }
    event = "leave" if kind == "component" else "cancel_payment"
    envelope.update(event=event, event_id=event, cause_id=event)
    checkpoint = admit_checkpoint_v1(
        checkpoint,
        [
            {
                "delivery_mode": "input",
                "envelope": envelope,
                "envelope_digest": _entry_digest("root", "input", envelope),
            }
        ],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    checkpoint = step_checkpoint_v1(
        checkpoint,
        runtime["runtime_id"],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    assert all(
        item["runtime_id"] != child["runtime_id"]
        for item in checkpoint["root_record"]["aggregate_state"]["runtimes"]
    )
    assert _pinned_result_target(checkpoint, record, resolver) == child["target_identity"]
    origin = record["target"]["runtime_incarnation"]
    counter = "activation_sequence" if kind == "component" else "spawn_sequence"
    origin[counter] = str(int(origin[counter]) + 1)
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        _pinned_result_target(checkpoint, record, resolver)


def test_producer_replay_cannot_return_a_cancellation_response(tmp_path):
    host, root, _, _ = host_fixture(tmp_path)
    request = read("data/cancel-request.json")
    request["operation_id"] = "shared-id"
    host.cancel(root, request)
    before = host.snapshot(root)
    with pytest.raises(EffectError, match="operation_id_conflict"):
        host.produce(
            root,
            "shared-id",
            "different-runtime",
            expected_revision="999",
            expected_digest="bad",
            route_generation="999",
            operation_token="different-token",
        )
    assert host.snapshot(root) == before


@pytest.mark.parametrize("damage", [None, "target", "token"])
def test_portable_producer_replay_requires_original_work_identity(tmp_path, damage):
    host, root, _, _ = host_fixture(tmp_path)
    record = host.snapshot(root)["journal"]["effect_records"][0]
    # Restoration has the normative response hash and immutable intent evidence,
    # but no process-local producer response cache or current route.
    target = record["target"]["runtime_id"] if damage != "target" else "different-runtime"
    token = record["operation_token"] if damage != "token" else "different-token"
    before = host.snapshot(root)
    arguments = {
        "expected_revision": "stale",
        "expected_digest": "stale",
        "route_generation": "999",
        "operation_token": token,
    }
    if damage is not None:
        with pytest.raises(
            EffectError,
            match="replay_evidence_expired" if damage == "target" else "operation_id_conflict",
        ):
            host.produce(root, "produce-1", target, **arguments)
    else:
        # Fixture journal response hash describes its pre-claim producer response.
        # Use the matching unclaimed journal for exact reconstruction.
        restored = SQLiteCommittedEffectHost(tmp_path / "producer.sqlite", host.resolver, {}, None)
        restored.setup_schema()
        restored.seed(read("pending-checkpoint.json"), read("data/unclaimed-journal.json"))
        assert restored.produce(root, "produce-1", target, **arguments) == read(
            "data/producer-response.json"
        )
    assert host.snapshot(root) == before


@pytest.mark.parametrize("damage", [None, "target", "token", "cancel"])
def test_native_producer_replay_retains_exact_kind_and_request_identity(tmp_path, damage):
    original, root, _, _ = host_fixture(tmp_path)
    checkpoint = read("accepted-checkpoint.json")
    journal = read("data/empty-journal.json")
    record = read("data/unclaimed-journal.json")["effect_records"][0]
    route = {
        key: record[key]
        for key in (
            "handler_reference",
            "destination_binding_digest",
            "result_mapping",
            "target",
            "idempotency_policy",
        )
    }
    route["generation"] = record["route_configuration_generation"]
    host = SQLiteCommittedEffectHost(tmp_path / "production.sqlite", original.resolver, route, None)
    host.setup_schema()
    host.seed(checkpoint, journal)
    host = installed_test_handler(host, lambda *_: {})
    runtime = checkpoint["root_record"]["aggregate_state"]["root_runtime_id"]
    arguments = {
        "expected_revision": checkpoint["revision"],
        "expected_digest": checkpoint["execution_checkpoint_digest"],
        "route_generation": route["generation"],
        "operation_token": record["operation_token"],
    }
    first = host.produce(root, "shared-id", runtime, **arguments)
    before = host.snapshot(root)
    if damage == "cancel":
        request = read("data/cancel-request.json")
        request["operation_id"] = "shared-id"
        with pytest.raises(EffectError, match="operation_id_conflict"):
            host.cancel(root, request)
    else:
        host.route["generation"] = "999"
        host.handler = None
        arguments.update(expected_revision="stale", expected_digest="stale", route_generation="999")
        if damage == "target":
            runtime = "different-runtime"
        elif damage == "token":
            arguments["operation_token"] = "different-token"
        if damage is not None:
            with pytest.raises(EffectError, match="operation_id_conflict"):
                host.produce(root, "shared-id", runtime, **arguments)
        else:
            assert host.produce(root, "shared-id", runtime, **arguments) == first
    assert host.snapshot(root) == before


def test_portable_producer_identity_is_not_inferred_from_result_destination(tmp_path):
    from determa.state.effects import _reconstruct_producer_response

    original, root, _, _ = host_fixture(tmp_path)
    checkpoint = read("pending-checkpoint.json")
    journal = read("data/unclaimed-journal.json")
    record = journal["effect_records"][0]
    source_runtime = checkpoint["root_record"]["aggregate_state"]["root_runtime_id"]
    record["target"]["runtime_id"] = "another-result-runtime"
    host = SQLiteCommittedEffectHost(
        tmp_path / "different-target.sqlite", original.resolver, {}, None
    )
    host.setup_schema()
    journal = seal_journal(journal)
    # This deliberately inconsistent destination must not become native work.
    # The pure receipt reconstruction still identifies its producing operation
    # from retained checkpoint evidence, rather than the result destination.
    with pytest.raises(EffectError, match="invalid_effect_journal"):
        host.seed(checkpoint, journal)
    identity = {
        "operation_kind": "produce",
        "root_instance_id": root,
        "target_runtime_id": source_runtime,
        "operation_token": record["operation_token"],
    }
    assert _reconstruct_producer_response(
        checkpoint, journal, "produce-1", identity, original.resolver
    ) == read("data/producer-response.json")
    identity["target_runtime_id"] = record["target"]["runtime_id"]
    with pytest.raises(EffectError, match="replay_evidence_expired"):
        _reconstruct_producer_response(
            checkpoint, journal, "produce-1", identity, original.resolver
        )


def test_dispatch_retained_start_denies_repeated_and_restarted_calls(tmp_path):
    host, root, request, context = host_fixture(tmp_path)
    record = host.snapshot(root)["journal"]["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    calls = []
    host = installed_test_handler(host, lambda *args: calls.append(args) or {"accepted": True})
    assert host.dispatch(root, request["effect_id"], credential="test-credential", **context) == {
        "accepted": True
    }
    after = host.snapshot(root)
    assert len(calls) == 1
    with pytest.raises(EffectError, match="native_invocation_already_started"):
        host.dispatch(root, request["effect_id"], credential="test-credential", **context)
    reopened = SQLiteCommittedEffectHost(
        host.path, host.resolver, host.route, host.handler, trusted_clock=lambda: "0"
    )
    with pytest.raises(EffectError, match="native_invocation_already_started"):
        reopened.dispatch(root, request["effect_id"], credential="test-credential", **context)
    assert reopened.snapshot(root) == after
    assert len(calls) == 1


def test_dispatch_provider_error_retains_start_and_cannot_be_retried_by_reopening(tmp_path):
    host, root, request, context = host_fixture(tmp_path)
    before = host.snapshot(root)
    record = before["journal"]["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    calls = []

    def failed_call(*args):
        calls.append(args)
        raise RuntimeError("external acceptance is unknown")

    host = installed_test_handler(host, failed_call)
    with pytest.raises(RuntimeError, match="acceptance is unknown"):
        host.dispatch(root, request["effect_id"], credential="test-credential", **context)
    after = host.snapshot(root)
    assert after["checkpoint"] == before["checkpoint"]
    assert after["journal"]["effect_records"] == before["journal"]["effect_records"]
    reopened = SQLiteCommittedEffectHost(
        host.path, host.resolver, host.route, host.handler, trusted_clock=lambda: "0"
    )
    with pytest.raises(EffectError, match="native_invocation_already_started"):
        reopened.dispatch(root, request["effect_id"], credential="test-credential", **context)
    recovered = reopened.recover(root)
    assert recovered["journal"]["effect_records"][0]["invocation_state"] == "ambiguous"
    assert len(calls) == 1


def test_dispatch_clock_is_rechecked_after_provider_verification_before_io(tmp_path, monkeypatch):
    import sqlite3

    host, root, request, context = host_fixture(tmp_path)
    before = host.snapshot(root)
    record = before["journal"]["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    current_time = ["0"]
    expiry = read("data/active-claim.json")["expires_at"]

    def health(self, instance):
        with sqlite3.connect(host.path) as connection:
            document = json.loads(
                connection.execute("SELECT document FROM determa_committed_effects").fetchone()[0]
            )
        if document["invocation_starts"]:
            current_time[0] = expiry
        return "healthy"

    monkeypatch.setattr(NativeTestProvider, "health", health)
    calls = []
    host.trusted_clock = lambda: current_time[0]
    host = installed_test_handler(host, lambda *args: calls.append(args) or {})
    with pytest.raises(EffectError, match="stale_attempt_fence"):
        host.dispatch(root, request["effect_id"], credential="test-credential", **context)
    after = host.snapshot(root)
    assert after["checkpoint"] == before["checkpoint"]
    assert after["journal"]["effect_records"] == before["journal"]["effect_records"]
    assert len(after["invocation_starts"]) == 1
    assert calls == []


def test_dispatch_postcall_health_loss_retains_start_without_returning_verified_output(
    tmp_path, monkeypatch
):
    host, root, request, context = host_fixture(tmp_path)
    record = host.snapshot(root)["journal"]["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    healthy = [True]
    monkeypatch.setattr(
        NativeTestProvider, "health", lambda *args: "healthy" if healthy[0] else "unavailable"
    )
    calls = []

    def external_acceptance(*args):
        calls.append(args)
        healthy[0] = False
        return {"accepted": True}

    host = installed_test_handler(host, external_acceptance)
    with pytest.raises(EffectError, match="host_capability_mismatch"):
        host.dispatch(root, request["effect_id"], credential="test-credential", **context)
    after = host.snapshot(root)
    healthy[0] = True
    with pytest.raises(EffectError, match="native_invocation_already_started"):
        host.dispatch(root, request["effect_id"], credential="test-credential", **context)
    assert host.snapshot(root) == after
    assert len(calls) == 1


def test_identical_dispatch_contenders_issue_one_native_call_under_consuming_lock(tmp_path):
    import sqlite3
    import threading
    from concurrent.futures import ThreadPoolExecutor

    host, root, request, context = host_fixture(tmp_path)
    record = host.snapshot(root)["journal"]["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    entered, release = threading.Event(), threading.Event()
    calls = []

    def native_call(*args):
        calls.append(args)
        entered.set()
        assert release.wait(5)
        return {"accepted": True}

    host = installed_test_handler(host, native_call)
    rendezvous = threading.Barrier(3)

    def contender():
        rendezvous.wait(timeout=5)
        try:
            return host.dispatch(
                root, request["effect_id"], credential="test-credential", **context
            )
        except EffectError as error:
            return str(error)

    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(contender) for _ in range(2)]
        rendezvous.wait(timeout=5)
        try:
            assert entered.wait(5)
            with sqlite3.connect(host.path, timeout=0, isolation_level=None) as writer:
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    writer.execute("BEGIN IMMEDIATE")
            assert len(calls) == 1
        finally:
            release.set()
        replies = [future.result(timeout=5) for future in futures]
    assert replies.count({"accepted": True}) == 1
    assert replies.count("native_invocation_already_started") == 1
    assert len(calls) == 1


def test_dispatch_native_call_excludes_a_real_competing_result_transaction(tmp_path, monkeypatch):
    import sqlite3

    host, root, request, context = host_fixture(tmp_path)
    record = host.snapshot(root)["journal"]["effect_records"][0]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    other = SQLiteCommittedEffectHost(
        host.path, host.resolver, host.route, None, trusted_clock=lambda: "0"
    )
    native_connect = other._connect

    def without_waiting():
        connection = native_connect()
        connection.execute("PRAGMA busy_timeout=0")
        return connection

    monkeypatch.setattr(other, "_connect", without_waiting)
    calls = []

    def native_call(*args):
        calls.append(args)
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            other.submit_result(root, request, **context)
        return {"accepted": True}

    host = installed_test_handler(host, native_call)
    assert host.dispatch(root, request["effect_id"], credential="test-credential", **context) == {
        "accepted": True
    }
    after = host.snapshot(root)
    assert after["journal"]["effect_records"][0]["invocation_state"] == "leased"
    assert after["journal"]["effect_records"][0]["outcome"] is None
    assert len(calls) == 1
    assert other.submit_result(root, request, **context)["status"] == "committed"


@pytest.mark.parametrize("damage", [None, "missing_indexes", "empty_indexes"])
def test_native_authority_seed_cannot_erase_a_committed_dispatch_start(tmp_path, damage):
    import sqlite3

    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    command, invocation = authority_claim_request(authority, scope, root, record)
    assert json.loads(authority.perform(command, invocation))["status"] == "accepted"
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    host.trusted_clock = lambda: "0"
    calls = []
    host = installed_test_handler(host, lambda *args: calls.append(args) or {"accepted": True})
    before = host.snapshot(root)
    claim = before["claims"][record["effect_id"]]
    context = {
        "principal": claim["worker_principal"],
        "scope": scope,
        "epoch": "0",
        "trusted_now": "0",
    }
    host.dispatch(root, record["effect_id"], credential="test-credential", **context)
    with sqlite3.connect(host.path) as connection:
        if damage is not None:
            retained = json.loads(
                connection.execute("SELECT ledger FROM determa_scope_authority").fetchone()[0]
            )
            for key in ("native_effect_roots", "native_effect_document_bytes"):
                if damage == "missing_indexes":
                    del retained[key]
                else:
                    retained[key] = []
            # Dedicated journal/work/issued-claim evidence still proves prior ownership.
            assert retained["native_effect_journal_bytes"] and retained["native_effect_work"]
            connection.execute(
                "UPDATE determa_scope_authority SET ledger=?", (json.dumps(retained),)
            )
        connection.execute(
            "DELETE FROM determa_committed_effects WHERE root_instance_id=?", (root,)
        )
    ledger = authority.inspect(scope)
    with pytest.raises(EffectError, match="unauthorized_scope"):
        host.seed(before["checkpoint"], before["journal"], claim)
    assert authority.inspect(scope) == ledger
    assert len(calls) == 1


@pytest.mark.parametrize(
    "damage", ["erase_start", "older_document", "missing_native_history", "missing_native_work"]
)
def test_private_dispatch_start_loss_cannot_be_blessed_by_dispatch_or_authority(tmp_path, damage):
    import sqlite3

    authority, host, scope, root, record = authority_effect_fixture(tmp_path)
    command, invocation = authority_claim_request(authority, scope, root, record)
    claim = json.loads(authority.perform(command, invocation))["claim"]
    host.route.update(
        {key: record[key] for key in ("handler_reference", "destination_binding_digest")}
    )
    host.trusted_clock = lambda: "0"
    calls = []
    host = installed_test_handler(host, lambda *args: calls.append(args) or {"accepted": True})
    context = {
        "principal": claim["worker_principal"],
        "scope": scope,
        "epoch": "0",
        "trusted_now": "0",
    }
    before = host.snapshot(root)
    host.dispatch(root, record["effect_id"], credential="test-credential", **context)
    after = host.snapshot(root)
    assert after["checkpoint"] == before["checkpoint"]
    assert after["journal"] == before["journal"]
    assert after["responses"] == before["responses"]
    assert len(after["invocation_starts"]) == 1
    fresh_command, fresh_invocation = authority_claim_request(
        authority, scope, root, record, operation="after-private-start", expected="1"
    )
    with sqlite3.connect(host.path) as connection:
        if damage in {"missing_native_history", "missing_native_work"}:
            ledger = json.loads(
                connection.execute("SELECT ledger FROM determa_scope_authority").fetchone()[0]
            )
            del ledger[
                "native_effect_document_bytes"
                if damage == "missing_native_history"
                else "native_effect_work"
            ]
            connection.execute("UPDATE determa_scope_authority SET ledger=?", (json.dumps(ledger),))
        else:
            corrupted = copy.deepcopy(after if damage == "erase_start" else before)
            corrupted["invocation_starts"] = {}
            connection.execute(
                "UPDATE determa_committed_effects SET document=?", (json.dumps(corrupted).encode(),)
            )
    with pytest.raises(EffectError, match="unauthorized_scope"):
        host.dispatch(root, record["effect_id"], credential="test-credential", **context)
    # The authority's generic mutation validation must not certify the corrupt
    # private inventory by mirroring it into a new trusted native history entry.
    assert json.loads(authority.perform(command, invocation))["claim"] == claim
    assert json.loads(authority.perform(fresh_command, fresh_invocation))["status"] == "rejected"
    assert len(calls) == 1
