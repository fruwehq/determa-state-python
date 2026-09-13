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

from determa.state import (
    PORTABLE_CODE_SETS,
    ExecutionHost,
    ExecutionHostError,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    load_bundle,
)
from determa.state.host import (
    _required_backup_artifact_digests,
    _required_backup_artifacts,
    _validate_required_backup_artifacts,
)
from determa.state.validator import schema as bundled_schema
from determa.state.wire import (
    _schema_registry,
    artifact_schema,
    hash_value,
    migration_descriptor_digest,
)

from .durable_host import durable_host_vectors, run_durable_host_vector
from .harness import CORE_DIR, CoreCase, conformance_root, core_cases, run_case
from .version2 import (
    _assert_checkpoint_unchanged,
    _resolver,
    run_version2_vector,
    validate_version2_artifact,
    version2_vectors,
)

_PORTABLE_ARTIFACT_KINDS = {
    "aggregate_state_v2",
    "migration_descriptor_v2",
    "aggregate_state_package_v2",
    "execution_checkpoint_v2",
    "core_step_result_v2",
}
_CONFORMANCE_ARTIFACT_SCHEMAS = {
    "durable_host_call_log_v2": "durable-host-call-log-v2.schema.json",
    "durable_host_inputs_v2": "durable-host-inputs-v2.schema.json",
    "durable_host_results_v2": "durable-host-results-v2.schema.json",
    "durable_host_store_v2": "durable-host-store-v2.schema.json",
    "version2_operation_inputs": "version2-operation-inputs.schema.json",
    "version2_operation_result": "version2-operation-result.schema.json",
}


def _manifest_artifacts() -> list[tuple[Path, dict]]:
    root = conformance_root() / "conformance"
    return [
        (test_path.parent, artifact)
        for test_path in sorted(root.glob("**/test.yaml"))
        for artifact in (
            yaml.safe_load(test_path.read_text(encoding="utf-8")) or {}
        ).get("artifacts", {}).get("documents", [])
    ]


def _version2_artifacts() -> list[tuple[Path, dict]]:
    return [
        (path, artifact)
        for path, artifact in _manifest_artifacts()
        if artifact["kind"] in _PORTABLE_ARTIFACT_KINDS
    ]


def _validate_manifest_artifact(path: Path, artifact: dict) -> None:
    if artifact["kind"] in _PORTABLE_ARTIFACT_KINDS:
        validate_version2_artifact(path, artifact)
        return
    schema_name = _CONFORMANCE_ARTIFACT_SCHEMAS[artifact["kind"]]
    schema_path = conformance_root() / "scripts" / "schemas" / schema_name
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    registry = _schema_registry().with_resource(
        schema["$id"], Resource.from_contents(schema)
    )
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
    assert len(core_cases()) == 98
    assert len(version2_vectors()) == 162
    assert len(durable_host_vectors()) == 138
    assert len(_manifest_artifacts()) == 381


@pytest.mark.parametrize("stored", [b"mutated", None])
def test_v2_maintenance_failure_requires_exact_unchanged_checkpoint(
    stored: bytes | None,
) -> None:
    item = next(
        item
        for item in version2_vectors()
        if item.vector["name"] == "native_v2_maintenance_stale_writer"
    )
    before = json.loads(
        (item.path / item.vector["checkpoint_before"]).read_text(encoding="utf-8")
    )
    initial = {} if stored is None else {before["root_instance_id"]: stored}
    observation = {
        "store": MemoryExecutionStore(initial),
        "root_instance_id": before["root_instance_id"],
    }

    with pytest.raises(AssertionError):
        _assert_checkpoint_unchanged(item, observation)


def test_valid_artifact_manifest_does_not_accept_forged_digest(tmp_path: Path) -> None:
    case, artifact = next(
        (case, artifact)
        for case, artifact in _version2_artifacts()
        if artifact["kind"] == "aggregate_state_v2" and artifact["valid"]
    )
    document = json.loads((case / artifact["file"]).read_text(encoding="utf-8"))
    document["aggregate_state_digest"] = "sha256:" + "0" * 64
    forged = tmp_path / "forged-aggregate.json"
    forged.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(AssertionError):
        validate_version2_artifact(
            tmp_path,
            {"file": forged.name, "kind": "aggregate_state_v2", "valid": True},
        )


def test_backup_manifest_requires_migration_recovery_route() -> None:
    path = (
        conformance_root()
        / "conformance"
        / "profiles"
        / "execution-checkpoint"
        / "checkpoint-04-version2-mailboxes"
    )
    source = (path / "maintenance-one-hop-checkpoint-v2.json").read_bytes()
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
    host = ExecutionHost(
        MemoryExecutionStore({root_instance_id: source}), _resolver(path)
    )
    arguments = {
        "action": "backup",
        "checkpoint_members": [source],
        "checkpoint_digests": [checkpoint["execution_checkpoint_digest"]],
        "adapter_metadata_digest": hash_value(
            ["determa-backup-adapter-metadata-2", "migration-backup"]
        ),
        "retention_mode": checkpoint["replay_retention"]["mode"],
        "consistency_point": {
            "scope_id": "migration-backup",
            "root_instance_ids": [root_instance_id],
        },
    }
    host.validate_backup_restore_v2(
        **arguments, trusted_artifact_digests=sorted(required)
    )
    for omitted in expected:
        with pytest.raises(ExecutionHostError) as error:
            host.validate_backup_restore_v2(
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
def test_backup_manifest_requires_restorable_artifacts(
    artifact_kind: str, failure: str
) -> None:
    path = (
        conformance_root()
        / "conformance"
        / "profiles"
        / "execution-checkpoint"
        / "checkpoint-04-version2-mailboxes"
    )
    source = (path / "maintenance-one-hop-checkpoint-v2.json").read_bytes()
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
        host.validate_backup_restore_v2(
            action="backup",
            checkpoint_members=[source],
            checkpoint_digests=[checkpoint["execution_checkpoint_digest"]],
            trusted_artifact_digests=sorted(definitions | descriptors),
            adapter_metadata_digest=hash_value(
                ["determa-backup-adapter-metadata-2", "migration-backup"]
            ),
            retention_mode=checkpoint["replay_retention"]["mode"],
            consistency_point={
                "scope_id": "migration-backup",
                "root_instance_ids": [root_instance_id],
            },
        )
    assert error.value.code == "invalid_execution_checkpoint"


def test_backup_manifest_includes_historical_fault_definition() -> None:
    path = (
        conformance_root()
        / "conformance"
        / "core"
        / "118-version2-persistence"
    )
    result = json.loads((path / "migration-historical-fault-result.json").read_text())
    runtime = result["aggregate_state"]["runtimes"][0]
    definitions, descriptors = _required_backup_artifacts(result)
    required = definitions | descriptors

    assert runtime["fault"]["definition_fingerprint"] in required
    assert (
        runtime["identity_origin"]["definition"]["validated_bundle_fingerprint"]
        in required
    )
    assert runtime["current_definition"]["validated_bundle_fingerprint"] in required
    assert result["audit_records"][0]["migration_descriptor_digest"] in required

    resolver = _resolver(path)
    _validate_required_backup_artifacts(resolver, set(required), result)
    origin = runtime["identity_origin"]["definition"][
        "validated_bundle_fingerprint"
    ]
    with pytest.raises(ExecutionHostError) as error:
        _validate_required_backup_artifacts(
            MemoryArtifactResolver(
                definitions={
                    fingerprint: resolver.resolve_definition(fingerprint)
                    for fingerprint in definitions - {origin}
                },
                migration_descriptors={
                    digest: resolver.resolve_migration_descriptor(digest)
                    for digest in descriptors
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
        item
        for item in durable_host_vectors()
        if item.vector["name"] == "outbox_retryable_failure"
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


def test_portable_code_sets_match_authoritative_registry() -> None:
    vector_path = (
        conformance_root()
        / "conformance"
        / "closed-code-registry"
        / "vectors.generated.json"
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
        ("aggregate-state-v2.schema.json", "aggregate_state_v2"),
        ("migration-descriptor-v2.schema.json", "migration_descriptor_v2"),
        ("aggregate-state-package-v2.schema.json", "aggregate_state_package_v2"),
        ("execution-checkpoint-v2.schema.json", "execution_checkpoint_v2"),
        ("core-step-result-v2.schema.json", "core_step_result_v2"),
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
        "aggregate_state_v2",
        "migration_descriptor_v2",
        "aggregate_state_package_v2",
        "execution_checkpoint_v2",
        "core_step_result_v2",
    ],
)
def test_bundled_artifact_schema_is_valid_draft_2020_12(kind: str) -> None:
    Draft202012Validator.check_schema(artifact_schema(kind))


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


@pytest.mark.parametrize("item", version2_vectors(), ids=lambda item: item.name)
def test_version2_vector(item) -> None:
    run_version2_vector(item)


@pytest.mark.parametrize("item", durable_host_vectors(), ids=lambda item: item.name)
def test_durable_host_vector(item) -> None:
    run_durable_host_vector(item)


@pytest.mark.parametrize(
    ("case", "artifact"),
    _manifest_artifacts(),
)
def test_version2_artifact(case, artifact) -> None:
    path = case.path if isinstance(case, CoreCase) else case
    _validate_manifest_artifact(path, artifact)
