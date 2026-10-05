"""Full format-1 core conformance gate."""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator
from referencing import Resource

import determa.state.checkpoint_v1 as checkpoint_module
from determa.state import (
    PORTABLE_CODE_SETS,
    ExecutionHost,
    ExecutionHostError,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    load_bundle,
    restore_execution_checkpoint_v1,
    seal_execution_checkpoint,
)
from determa.state.host import (
    _required_backup_artifact_digests,
    _required_backup_artifacts,
    _validate_required_backup_artifacts,
)
from determa.state.persistence import PersistenceHost
from determa.state.validator import schema as bundled_schema
from determa.state.wire import (
    _schema_registry,
    artifact_schema,
    hash_value,
    migration_descriptor_digest,
)

from .durable_host import (
    _BrokerAcknowledgementAdapter,
    durable_host_vectors,
    run_durable_host_vector,
)
from .harness import CORE_DIR, CoreCase, conformance_root, core_cases, run_case
from .version1 import (
    _assert_checkpoint_unchanged,
    _resolver,
    run_version1_vector,
    validate_version1_artifact,
    version1_vectors,
)

_PORTABLE_ARTIFACT_KINDS = {
    "aggregate_state_v1",
    "migration_descriptor_v1",
    "aggregate_state_package_v1",
    "execution_checkpoint_v1",
    "core_step_result_v1",
}
_CONFORMANCE_ARTIFACT_SCHEMAS = {
    "application_projection_v1": "application-projection-v1.schema.json",
    "lossless_delivery_v1": "lossless-delivery-v1.schema.json",
    "durable_host_call_log_v1": "durable-host-call-log-v1.schema.json",
    "durable_host_inputs_v1": "durable-host-inputs-v1.schema.json",
    "durable_host_results_v1": "durable-host-results-v1.schema.json",
    "durable_host_responses_v1": "durable-host-responses-v1.schema.json",
    "durable_host_store_v1": "durable-host-store-v1.schema.json",
    "version1_operation_inputs": "version1-operation-inputs.schema.json",
    "version1_operation_result": "version1-operation-result.schema.json",
    "version1_operation_failures": "version1-operation-failures.schema.json",
}


def _manifest_artifacts() -> list[tuple[Path, dict]]:
    root = conformance_root() / "conformance"
    return [
        (test_path.parent, artifact)
        for test_path in sorted(root.glob("**/test.yaml"))
        for artifact in (yaml.safe_load(test_path.read_text(encoding="utf-8")) or {})
        .get("artifacts", {})
        .get("documents", [])
    ]


def _version1_artifacts() -> list[tuple[Path, dict]]:
    return [
        (path, artifact)
        for path, artifact in _manifest_artifacts()
        if artifact["kind"] in _PORTABLE_ARTIFACT_KINDS
    ]


def _validate_manifest_artifact(path: Path, artifact: dict) -> None:
    if artifact["kind"] in _PORTABLE_ARTIFACT_KINDS:
        validate_version1_artifact(path, artifact)
        return
    if artifact["kind"] == "json_value":
        try:
            json.loads((path / artifact["file"]).read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeError):
            valid = False
        else:
            valid = True
        assert valid is artifact["valid"]
        return
    schema_name = _CONFORMANCE_ARTIFACT_SCHEMAS[artifact["kind"]]
    schema_path = conformance_root() / "scripts" / "schemas" / schema_name
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    registry = _schema_registry()
    for reference_path in sorted(schema_path.parent.glob("*.schema.json")):
        reference = json.loads(reference_path.read_text(encoding="utf-8"))
        if "$id" in reference:
            registry = registry.with_resource(reference["$id"], Resource.from_contents(reference))
    spec_root = _spec_root()
    if spec_root is not None:
        for reference_path in sorted((spec_root / "schema").glob("*.schema.json")):
            reference = json.loads(reference_path.read_text(encoding="utf-8"))
            registry = registry.with_resource(reference["$id"], Resource.from_contents(reference))
    validator = Draft202012Validator(schema, registry=registry)
    try:
        document = json.loads((path / artifact["file"]).read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeError):
        valid = False
    else:
        valid = validator.is_valid(document)
    assert valid is artifact["valid"]


def _spec_schema() -> dict | None:
    override = os.environ.get("DETERMA_SPEC_DIR")
    if not override:
        return None
    path = Path(override) / "schema" / "machine.schema.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _spec_root() -> Path | None:
    override = os.environ.get("DETERMA_SPEC_DIR")
    if not override:
        return None
    root = Path(override)
    return root if root.exists() else None


def test_suite_present() -> None:
    assert CORE_DIR.exists(), "pinned conformance suite is unavailable"
    assert len(core_cases()) == 99
    assert len(version1_vectors()) == 162
    assert len(durable_host_vectors()) == 142
    assert len(_manifest_artifacts()) == 484


@pytest.mark.parametrize("stored", [b"mutated", None])
def test_v1_maintenance_failure_requires_exact_unchanged_checkpoint(
    stored: bytes | None,
) -> None:
    item = next(
        item
        for item in version1_vectors()
        if item.vector["name"] == "native_v1_maintenance_stale_writer"
    )
    before = json.loads((item.path / item.vector["checkpoint_before"]).read_text(encoding="utf-8"))
    initial = {} if stored is None else {before["root_instance_id"]: stored}
    observation = {
        "store": MemoryExecutionStore(initial),
        "root_instance_id": before["root_instance_id"],
    }

    with pytest.raises(AssertionError):
        _assert_checkpoint_unchanged(item, observation)


@pytest.mark.parametrize(
    "vector_name",
    ["outbox_forbidden_deletion", "checkpoint_physical_deletion_unsupported"],
)
@pytest.mark.parametrize("response", [None, "wrong_error"])
def test_retained_deletion_vectors_exercise_production_host(
    monkeypatch: pytest.MonkeyPatch, vector_name: str, response: str | None
) -> None:
    item = next(item for item in durable_host_vectors() if item.vector["name"] == vector_name)

    def altered_host_response(*args: object, **kwargs: object) -> None:
        if response == "wrong_error":
            raise ExecutionHostError("effect_id_conflict")

    monkeypatch.setattr(ExecutionHost, "delete_retained_record", altered_host_response)
    with pytest.raises(AssertionError):
        run_durable_host_vector(item)


@pytest.mark.parametrize(
    "vector_name",
    ["checkpoint_root_tombstone", "checkpoint_root_tombstone_replay"],
)
def test_tombstone_response_uses_fixture_oracle(
    monkeypatch: pytest.MonkeyPatch, vector_name: str
) -> None:
    item = next(item for item in durable_host_vectors() if item.vector["name"] == vector_name)
    original = ExecutionHost.tombstone_root_v1

    def altered_response(self: ExecutionHost, *args: object, **kwargs: object) -> dict:
        response = original(self, *args, **kwargs)
        result = copy.deepcopy(response)
        result["tombstone"]["final_aggregate_state_digest"] = "sha256:" + "0" * 64
        return result

    monkeypatch.setattr(ExecutionHost, "tombstone_root_v1", altered_response)
    with pytest.raises(AssertionError):
        run_durable_host_vector(item)


@pytest.mark.parametrize(
    ("vector_name", "method_name"),
    [
        ("checkpoint_admission_commit", "admit_v1"),
        ("checkpoint_processing_commit", "process_ready_v1"),
    ],
)
def test_literal_host_response_rejects_malformed_production_body(
    monkeypatch: pytest.MonkeyPatch, vector_name: str, method_name: str
) -> None:
    item = next(item for item in durable_host_vectors() if item.vector["name"] == vector_name)
    original = getattr(ExecutionHost, method_name)

    def malformed(self: ExecutionHost, *args: object, **kwargs: object) -> dict:
        result = copy.deepcopy(original(self, *args, **kwargs))
        result["unexpected"] = True
        return result

    monkeypatch.setattr(ExecutionHost, method_name, malformed)
    with pytest.raises(AssertionError):
        run_durable_host_vector(item)


def test_literal_capability_report_rejects_boolean_integer_substitution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from . import durable_host as driver

    item = next(
        item for item in durable_host_vectors() if item.vector["name"] == "exactly_once_positive"
    )
    original = driver.validate_host_profile_report

    def malformed(*args: object, **kwargs: object) -> dict:
        response = original(*args, **kwargs)
        return {**response, "validated": 1}

    monkeypatch.setattr(driver, "validate_host_profile_report", malformed)
    with pytest.raises(AssertionError):
        run_durable_host_vector(item)


def test_literal_failure_rejects_wrong_code_type(monkeypatch: pytest.MonkeyPatch) -> None:
    item = next(
        item
        for item in durable_host_vectors()
        if item.vector["name"] == "checkpoint_creation_conflict"
    )

    def malformed(*args: object, **kwargs: object) -> dict:
        raise ExecutionHostError(True)  # type: ignore[arg-type]

    monkeypatch.setattr(ExecutionHost, "create_v1", malformed)
    with pytest.raises(AssertionError):
        run_durable_host_vector(item)


def test_resealed_checkpoint_requires_resolved_definition() -> None:
    path = (
        conformance_root()
        / "conformance"
        / "profiles"
        / "execution-checkpoint"
        / "checkpoint-01-native-lifecycle"
    )
    original = json.loads((path / "processed-checkpoint-v1.json").read_text())
    resealed = seal_execution_checkpoint(original)
    assert resealed == original
    with pytest.raises(Exception) as error:
        restore_execution_checkpoint_v1(resealed, MemoryArtifactResolver())
    assert getattr(error.value, "code", None) == "source_definition_unavailable"


def test_valid_artifact_manifest_does_not_accept_forged_digest(tmp_path: Path) -> None:
    case, artifact = next(
        (case, artifact)
        for case, artifact in _version1_artifacts()
        if artifact["kind"] == "aggregate_state_v1" and artifact["valid"]
    )
    document = json.loads((case / artifact["file"]).read_text(encoding="utf-8"))
    document["aggregate_state_digest"] = "sha256:" + "0" * 64
    forged = tmp_path / "forged-aggregate.json"
    forged.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(AssertionError):
        validate_version1_artifact(
            tmp_path,
            {"file": forged.name, "kind": "aggregate_state_v1", "valid": True},
        )


def test_backup_manifest_requires_migration_recovery_route() -> None:
    path = (
        conformance_root()
        / "conformance"
        / "profiles"
        / "execution-checkpoint"
        / "checkpoint-04-version1-mailboxes"
    )
    source = (path / "maintenance-one-hop-checkpoint-v1.json").read_bytes()
    checkpoint = json.loads(source)
    required = _required_backup_artifact_digests(checkpoint)
    audit = checkpoint["migration_audit_records"][0]
    expected = {
        audit["source_validated_bundle_fingerprint"],
        audit["target_validated_bundle_fingerprint"],
        audit["migration_descriptor_digest"],
    }
    assert expected.issubset(required)
    root_instance_id = checkpoint["root_instance_id"]
    host = ExecutionHost(MemoryExecutionStore({root_instance_id: source}), _resolver(path))
    arguments = {
        "action": "backup",
        "checkpoint_members": [source],
        "checkpoint_digests": [checkpoint["execution_checkpoint_digest"]],
        "adapter_metadata_digest": hash_value(
            ["determa-backup-adapter-metadata-1", "migration-backup"]
        ),
        "retention_mode": checkpoint["replay_retention"]["mode"],
        "consistency_point": {
            "scope_id": "migration-backup",
            "root_instance_ids": [root_instance_id],
        },
    }
    host.validate_backup_restore_v1(**arguments, trusted_artifact_digests=sorted(required))
    for omitted in expected:
        with pytest.raises(ExecutionHostError) as error:
            host.validate_backup_restore_v1(
                **arguments,
                trusted_artifact_digests=sorted(required - {omitted}),
            )
        assert error.value.code == "invalid_execution_checkpoint"


@pytest.mark.parametrize(
    ("artifact_kind", "failure"),
    [
        ("definition", "missing"),
        ("definition", "untrusted"),
        ("definition", "mismatched"),
        ("descriptor", "missing"),
        ("descriptor", "untrusted"),
        ("descriptor", "mismatched"),
    ],
)
def test_backup_manifest_requires_restorable_artifacts(artifact_kind: str, failure: str) -> None:
    path = (
        conformance_root()
        / "conformance"
        / "profiles"
        / "execution-checkpoint"
        / "checkpoint-04-version1-mailboxes"
    )
    source = (path / "maintenance-one-hop-checkpoint-v1.json").read_bytes()
    checkpoint = json.loads(source)
    definitions, descriptors = _required_backup_artifacts(checkpoint)
    bundles = {}
    for name in ["maintenance-source.yaml", "maintenance-target-one.yaml"]:
        text = (path / name).read_text(encoding="utf-8")
        bundles[load_bundle(text).fingerprint] = text
    descriptor_documents = {
        migration_descriptor_digest(document): document
        for document in [
            json.loads((path / "maintenance-descriptor-one.json").read_text()),
            json.loads((path / "maintenance-descriptor-two.json").read_text()),
        ]
    }
    required_definition = next(iter(definitions))
    required_descriptor = next(iter(descriptors))
    resolver_definitions = dict(bundles)
    resolver_descriptors = dict(descriptor_documents)
    trusted_definitions = list(resolver_definitions)
    trusted_descriptors = list(resolver_descriptors)

    if artifact_kind == "definition":
        if failure == "missing":
            resolver_definitions.pop(required_definition)
        elif failure == "untrusted":
            trusted_definitions.remove(required_definition)
        else:
            resolver_definitions[required_definition] = next(
                bundle
                for fingerprint, bundle in bundles.items()
                if fingerprint != required_definition
            )
    elif failure == "missing":
        resolver_descriptors.pop(required_descriptor)
    elif failure == "untrusted":
        trusted_descriptors.remove(required_descriptor)
    else:
        resolver_descriptors[required_descriptor] = next(
            descriptor
            for digest, descriptor in descriptor_documents.items()
            if digest != required_descriptor
        )

    root_instance_id = checkpoint["root_instance_id"]
    host = ExecutionHost(
        MemoryExecutionStore({root_instance_id: source}),
        MemoryArtifactResolver(
            definitions=resolver_definitions,
            migration_descriptors=resolver_descriptors,
            trusted_definitions=trusted_definitions,
            trusted_migration_descriptors=trusted_descriptors,
        ),
    )
    if artifact_kind == "definition":
        with pytest.raises(ExecutionHostError) as error:
            _validate_required_backup_artifacts(
                host.artifact_resolver,
                set(definitions | descriptors),
                checkpoint,
            )
        assert error.value.code == "invalid_execution_checkpoint"
        return

    with pytest.raises(ExecutionHostError) as error:
        host.validate_backup_restore_v1(
            action="backup",
            checkpoint_members=[source],
            checkpoint_digests=[checkpoint["execution_checkpoint_digest"]],
            trusted_artifact_digests=sorted(definitions | descriptors),
            adapter_metadata_digest=hash_value(
                ["determa-backup-adapter-metadata-1", "migration-backup"]
            ),
            retention_mode=checkpoint["replay_retention"]["mode"],
            consistency_point={
                "scope_id": "migration-backup",
                "root_instance_ids": [root_instance_id],
            },
        )
    assert error.value.code == "invalid_execution_checkpoint"


def test_backup_manifest_includes_historical_fault_definition() -> None:
    path = conformance_root() / "conformance" / "core" / "118-version1-persistence"
    result = json.loads((path / "migration-historical-fault-result.json").read_text())
    runtime = result["aggregate_state"]["runtimes"][0]
    definitions, descriptors = _required_backup_artifacts(result)
    required = definitions | descriptors

    assert runtime["fault"]["definition_fingerprint"] in required
    assert runtime["identity_origin"]["definition"]["validated_bundle_fingerprint"] in required
    assert runtime["current_definition"]["validated_bundle_fingerprint"] in required
    assert result["audit_records"][0]["migration_descriptor_digest"] in required

    resolver = _resolver(path)
    _validate_required_backup_artifacts(resolver, set(required), result)
    origin = runtime["identity_origin"]["definition"]["validated_bundle_fingerprint"]
    with pytest.raises(ExecutionHostError) as error:
        _validate_required_backup_artifacts(
            MemoryArtifactResolver(
                definitions={
                    fingerprint: resolver.resolve_definition(fingerprint)
                    for fingerprint in definitions - {origin}
                },
                migration_descriptors={
                    digest: resolver.resolve_migration_descriptor(digest) for digest in descriptors
                },
            ),
            set(required),
            result,
        )
    assert error.value.code == "invalid_execution_checkpoint"


@pytest.mark.parametrize("malformation", ["extra_field", "wrong_record"])
def test_durable_host_rejects_malformed_outbox_response(
    monkeypatch: pytest.MonkeyPatch, malformation: str
) -> None:
    item = next(
        item for item in durable_host_vectors() if item.vector["name"] == "outbox_retryable_failure"
    )
    original = ExecutionHost.update_pending_outbox

    def malformed_response(*args, **kwargs):
        response = original(*args, **kwargs)
        if malformation == "extra_field":
            return {**response, "unexpected": True}
        forged = copy.deepcopy(response)
        forged["record"]["delivery_state"] = {"status": "ambiguous"}
        return forged

    monkeypatch.setattr(ExecutionHost, "update_pending_outbox", malformed_response)
    with pytest.raises(AssertionError):
        run_durable_host_vector(item)


@pytest.mark.parametrize("sabotage", ["core_call", "checkpoint_mutation"])
def test_contract_gate_observes_production_effects(
    monkeypatch: pytest.MonkeyPatch, sabotage: str
) -> None:
    item = next(
        item for item in durable_host_vectors() if item.vector["name"] == "permanent_backup"
    )
    original = ExecutionHost.validate_backup_restore_v1

    def altered(self, **kwargs):
        response = original(self, **kwargs)
        source = json.loads(kwargs["checkpoint_members"][0])
        root_id = source["root_instance_id"]
        if sabotage == "core_call":
            probe = load_bundle(
                "format: 1\nnamespace: test.contract_probe\nmachines:\n"
                "  - machine_id: probe\n    version: 1\n    root:\n      type: final\n"
            )
            checkpoint_module.create_aggregate_v1(probe, "probe", "probe-root", "probe-create")
        else:
            source["unexpected_store_change"] = True
            assert isinstance(self.store, MemoryExecutionStore)
            self.store._records[root_id] = json.dumps(source).encode("utf-8")
        return response

    monkeypatch.setattr(ExecutionHost, "validate_backup_restore_v1", altered)
    with pytest.raises(AssertionError):
        run_durable_host_vector(item)


def test_contract_gate_observes_unexpected_acknowledgement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = next(
        item for item in durable_host_vectors() if item.vector["name"] == "permanent_backup"
    )
    monkeypatch.setattr(_BrokerAcknowledgementAdapter, "acknowledged", lambda *_: True)
    with pytest.raises(AssertionError):
        run_durable_host_vector(item)


def test_persistence_no_response_requires_call_to_abort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = next(
        item
        for item in durable_host_vectors()
        if item.vector["name"] == "persistence_crash_after_commit"
    )
    original = PersistenceHost.process_v1

    def unexpected_reply(self, request):
        try:
            return original(self, request)
        except ExecutionHostError as error:
            assert error.code == "response_lost_after_commit"
            return {
                "result": "committed",
                "mutation": "atomic",
                "core_calls": self.core_calls,
                "broker_acknowledged": True,
            }

    monkeypatch.setattr(PersistenceHost, "process_v1", unexpected_reply)
    with pytest.raises(AssertionError):
        run_durable_host_vector(item)


def test_durable_gate_rejects_boolean_for_persisted_integer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    item = next(
        item
        for item in durable_host_vectors()
        if item.vector["name"] == "persistence_inbox_first_commit"
    )
    original = PersistenceHost.process_v1

    def corrupt_persisted_row(self, request):
        response = original(self, request)
        root_id = request["expected_checkpoint"]["root_instance_id"]
        with self.store.host_transaction(root_id) as (document, _transaction):
            assert type(document["application_rows"]["aggregate_version"]) is int
            assert document["application_rows"]["aggregate_version"] == 1
            document["application_rows"]["aggregate_version"] = True
        return response

    monkeypatch.setattr(PersistenceHost, "process_v1", corrupt_persisted_row)
    with pytest.raises(AssertionError):
        run_durable_host_vector(item)


def test_portable_code_sets_match_authoritative_registry() -> None:
    vector_path = (
        conformance_root() / "conformance" / "closed-code-registry" / "vectors.generated.json"
    )
    vectors = json.loads(vector_path.read_text(encoding="utf-8"))
    expected = {
        category["id"]: frozenset(category["codes"])
        for category in vectors["categories"]
        if category["id"] != "execution_store_failure"
    }

    missing_categories = sorted(set(expected) - set(PORTABLE_CODE_SETS))
    extra_categories = sorted(set(PORTABLE_CODE_SETS) - set(expected))
    mismatches = []
    for category in sorted(set(expected) & set(PORTABLE_CODE_SETS)):
        missing = sorted(expected[category] - PORTABLE_CODE_SETS[category])
        extra = sorted(PORTABLE_CODE_SETS[category] - expected[category])
        if missing or extra:
            mismatches.append(f"{category}: missing={missing}, extra={extra}")

    assert not (missing_categories or extra_categories or mismatches), "\n".join(
        [
            f"categories: missing={missing_categories}, extra={extra_categories}",
            *mismatches,
        ]
    )


def test_bundled_schema_matches_pinned_spec() -> None:
    upstream = _spec_schema()
    assert upstream is not None, "pinned specification is unavailable"
    assert bundled_schema() == upstream


@pytest.mark.parametrize(
    ("name", "kind"),
    [
        ("aggregate-state-v1.schema.json", "aggregate_state_v1"),
        ("migration-descriptor-v1.schema.json", "migration_descriptor_v1"),
        ("aggregate-state-package-v1.schema.json", "aggregate_state_package_v1"),
        ("execution-checkpoint-v1.schema.json", "execution_checkpoint_v1"),
        ("core-step-result-v1.schema.json", "core_step_result_v1"),
    ],
)
def test_bundled_artifact_schemas_match_pinned_spec(name: str, kind: str) -> None:
    root = _spec_root()
    assert root is not None, "pinned specification is unavailable"
    upstream = json.loads((root / "schema" / name).read_text(encoding="utf-8"))
    assert artifact_schema(kind) == upstream


@pytest.mark.parametrize(
    "kind",
    [
        "aggregate_state_v1",
        "migration_descriptor_v1",
        "aggregate_state_package_v1",
        "execution_checkpoint_v1",
        "core_step_result_v1",
    ],
)
def test_bundled_artifact_schema_is_valid_draft_2020_12(kind: str) -> None:
    Draft202012Validator.check_schema(artifact_schema(kind))


@pytest.mark.parametrize(
    "name",
    [
        "runtime-action-output-v1.schema.json",
        "runtime-provider-descriptor-v1.schema.json",
        "language-source-v1.schema.json",
        "compilation-manifest-v1.schema.json",
    ],
)
def test_optional_runtime_schemas_match_pinned_spec(name: str) -> None:
    root = _spec_root()
    assert root is not None, "pinned specification is unavailable"
    installed = Path(__file__).resolve().parents[1] / "src/determa/state/data" / name
    upstream = json.loads((root / "schema" / name).read_text(encoding="utf-8"))
    bundled = json.loads(installed.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(bundled)
    assert bundled == upstream


def test_bundled_schema_is_valid_draft_2020_12() -> None:
    Draft202012Validator.check_schema(bundled_schema())


@pytest.mark.parametrize("name", ["minimal.yaml", "full.yaml"])
def test_authoritative_spec_examples_load_semantically(name: str) -> None:
    root = _spec_root()
    assert root is not None, "pinned specification is unavailable"

    bundle = load_bundle((root / "examples" / name).read_text(encoding="utf-8"))

    assert bundle.raw["format"] == 1


@pytest.mark.parametrize("case", core_cases(), ids=lambda case: case.name)
def test_core_case(case: CoreCase) -> None:
    run_case(case)


@pytest.mark.parametrize("item", version1_vectors(), ids=lambda item: item.name)
def test_version1_vector(item) -> None:
    run_version1_vector(item)


@pytest.mark.parametrize("item", durable_host_vectors(), ids=lambda item: item.name)
def test_durable_host_vector(item) -> None:
    run_durable_host_vector(item)


@pytest.mark.parametrize(
    ("case", "artifact"),
    _manifest_artifacts(),
)
def test_version1_artifact(case, artifact) -> None:
    path = case.path if isinstance(case, CoreCase) else case
    _validate_manifest_artifact(path, artifact)
