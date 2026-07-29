"""Driver for portable aggregate and migration conformance vectors."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from determa.state import (
    ArtifactError,
    MemoryArtifactResolver,
    MigrationLimits,
    aggregate_envelope,
    create,
    dispatch,
    load_bundle,
    migrate_aggregate,
    migrate_and_dispatch,
    restore_aggregate,
    restore_aggregate_package,
    serialize_aggregate,
)
from determa.state.wire import (
    canonical_bytes,
    migration_descriptor_digest,
    strict_json,
)

from .harness import CoreCase


def persistence_vector_cases(cases: list[CoreCase]) -> list[CoreCase]:
    return [
        case
        for case in cases
        if (_load_yaml(case.test_file).get("persistence_vectors") or [])
    ]


def run_persistence_vectors(case: CoreCase) -> None:
    test = _load_yaml(case.test_file)
    for vector in test.get("persistence_vectors") or []:
        _run_vector(case.path, vector)


def _load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _json(path: Path) -> Any:
    value, _ = strict_json(path.read_bytes())
    return value


def _resolver(path: Path, vector: dict[str, Any]) -> MemoryArtifactResolver:
    if "artifact_resolver" in vector:
        fixture = _json(path / vector["artifact_resolver"])
        definitions = {
            entry["validated_bundle_fingerprint"]: (path / entry["bundle_file"]).read_text(
                encoding="utf-8"
            )
            for entry in fixture["definitions"]
        }
        descriptors = {
            entry["migration_descriptor_digest"]: (
                path / entry["descriptor_file"]
            ).read_bytes()
            for entry in fixture["migration_descriptors"]
        }
        return MemoryArtifactResolver(
            definitions=definitions,
            migration_descriptors=descriptors,
            trusted_definitions=[
                entry["validated_bundle_fingerprint"]
                for entry in fixture["definitions"]
                if entry["trusted"]
            ],
            trusted_migration_descriptors=[
                entry["migration_descriptor_digest"]
                for entry in fixture["migration_descriptors"]
                if entry["trusted"]
            ],
        )
    definitions: dict[str, Any] = {}
    for filename in vector.get("definitions") or []:
        bundle = load_bundle((path / filename).read_text(encoding="utf-8"))
        definitions[bundle.fingerprint] = bundle
    descriptors: dict[str, Any] = {}
    for filename in vector.get("migration_descriptors") or []:
        document = _json(path / filename)
        digest = document.get("migration_descriptor_digest")
        descriptors[
            digest if isinstance(digest, str) else migration_descriptor_digest(document)
        ] = document
    return MemoryArtifactResolver(
        definitions=definitions, migration_descriptors=descriptors
    )


def _limits(path: Path, vector: dict[str, Any]) -> MigrationLimits | None:
    filename = vector.get("resource_limits")
    return MigrationLimits.from_mapping(_json(path / filename)) if filename else None


def _run_vector(path: Path, vector: dict[str, Any]) -> None:
    operation = vector["operation"]
    expected = vector["expect"]
    source_key = (
        "aggregate_state_package"
        if operation.startswith("restore_package")
        else "aggregate_state"
    )
    source = (path / vector[source_key]).read_bytes() if source_key in vector else None
    source_snapshot = bytes(source) if source is not None else None
    resolver = _resolver(path, vector)
    try:
        if operation == "serialize_created_aggregate":
            bundle = load_bundle((path / vector["source_bundle"]).read_text(encoding="utf-8"))
            result = create(bundle, **vector["creation"])
            assert result["state"] is not None
            document = aggregate_envelope(bundle, result["state"])
            encoded = serialize_aggregate(bundle, result["state"])
            audits: tuple[dict[str, Any], ...] = ()
            emissions: tuple[dict[str, Any], ...] = ()
            disposition = None
        elif operation == "restore_and_serialize":
            assert source is not None
            restored = restore_aggregate(source, resolver)
            document = aggregate_envelope(restored.bundle, restored.state)
            encoded = serialize_aggregate(restored.bundle, restored.state)
            audits = ()
            emissions = ()
            disposition = None
        elif operation == "restore_and_dispatch":
            assert source is not None
            restored = restore_aggregate(source, resolver)
            core = dispatch(
                restored.bundle,
                restored.state,
                {"input": _json(path / vector["input_envelope"])},
            )
            assert core["state"] is not None
            document = aggregate_envelope(restored.bundle, core["state"])
            encoded = canonical_bytes(document)
            audits = ()
            emissions = tuple(core["emissions"])
            disposition = core["disposition"]
        elif operation == "restore_package":
            assert source is not None
            package = restore_aggregate_package(source, resolver)
            document = package.aggregate.aggregate_envelope
            encoded = package.aggregate.canonical_bytes
            audits = ()
            emissions = ()
            disposition = None
        elif operation == "restore_package_and_migrate":
            assert source is not None
            package = restore_aggregate_package(source, resolver)
            migrated = migrate_aggregate(
                package.aggregate.canonical_bytes,
                vector["target_validated_bundle_fingerprint"],
                vector["migration_route"],
                resolver,
                maintenance_mode=vector["maintenance_mode"],
                resource_limits=_limits(path, vector),
            )
            if migrated.failure is not None:
                raise ArtifactError(migrated.failure.code)
            assert migrated.aggregate_envelope is not None
            assert migrated.aggregate_bytes is not None
            document = migrated.aggregate_envelope
            encoded = migrated.aggregate_bytes
            audits = migrated.audit_records
            emissions = ()
            disposition = None
        elif operation == "migrate_aggregate":
            assert source is not None
            request = {
                "target_validated_bundle_fingerprint": vector.get(
                    "target_validated_bundle_fingerprint"
                ),
                "migration_route": vector.get("migration_route"),
                "maintenance_mode": vector.get("maintenance_mode"),
            }
            if "migration_request" in vector:
                request_value = vector["migration_request"]
                request = (
                    _json(path / request_value)
                    if isinstance(request_value, str)
                    else copy.deepcopy(request_value)
                )
            migrated = migrate_aggregate(
                source,
                request.get("target_validated_bundle_fingerprint"),
                request.get("migration_route"),
                resolver,
                maintenance_mode=request.get("maintenance_mode"),
                resource_limits=_limits(path, vector),
            )
            if migrated.failure is not None:
                raise ArtifactError(migrated.failure.code)
            assert migrated.aggregate_envelope is not None
            assert migrated.aggregate_bytes is not None
            document = migrated.aggregate_envelope
            encoded = migrated.aggregate_bytes
            audits = migrated.audit_records
            emissions = ()
            disposition = None
        elif operation == "migrate_and_dispatch":
            assert source is not None
            migrated_dispatch = migrate_and_dispatch(
                source,
                vector["target_validated_bundle_fingerprint"],
                vector["migration_route"],
                resolver,
                {"input": _json(path / vector["input_envelope"])},
                maintenance_mode=vector["maintenance_mode"],
                resource_limits=_limits(path, vector),
            )
            if migrated_dispatch.failure is not None:
                raise ArtifactError(migrated_dispatch.failure.code)
            assert migrated_dispatch.aggregate_envelope is not None
            assert migrated_dispatch.aggregate_bytes is not None
            document = migrated_dispatch.aggregate_envelope
            encoded = migrated_dispatch.aggregate_bytes
            audits = migrated_dispatch.audit_records
            emissions = migrated_dispatch.emissions
            disposition = migrated_dispatch.disposition
        else:
            raise AssertionError(f"unsupported persistence operation: {operation}")
    except ArtifactError as error:
        assert expected["result"] == "failure", (vector["name"], error.code)
        assert error.code == expected["code"], (vector["name"], error.code)
        if expected.get("caller_still_owns_aggregate"):
            assert source == source_snapshot
        if vector.get("repeat_count", 1) > 1:
            repeated = copy.deepcopy(vector)
            repeated["repeat_count"] -= 1
            _run_vector(path, repeated)
        return
    assert expected["result"] == "success", vector["name"]
    assert document == _json(path / expected["aggregate_state_file"])
    assert encoded == (path / expected["exact_bytes_file"]).read_bytes()
    if "migration_audit_file" in expected:
        assert list(audits) == _json(path / expected["migration_audit_file"])
    if "emissions_file" in expected:
        assert list(emissions) == _json(path / expected["emissions_file"])
    if "disposition" in expected:
        assert disposition == expected["disposition"]
    if "artifact_resolver_file" in expected:
        _assert_resolver(path, resolver, expected["artifact_resolver_file"])
    if vector.get("repeat_count", 1) > 1:
        repeated = copy.deepcopy(vector)
        repeated["repeat_count"] -= 1
        _run_vector(path, repeated)


def _assert_resolver(
    path: Path, resolver: MemoryArtifactResolver, fixture_name: str
) -> None:
    fixture = _json(path / fixture_name)
    assert resolver.snapshot() == {
        "definitions": sorted(
            entry["validated_bundle_fingerprint"] for entry in fixture["definitions"]
        ),
        "migration_descriptors": sorted(
            entry["migration_descriptor_digest"]
            for entry in fixture["migration_descriptors"]
        ),
    }
    for entry in fixture["definitions"]:
        assert resolver.definition_is_trusted(entry["validated_bundle_fingerprint"]) is entry[
            "trusted"
        ]
    for entry in fixture["migration_descriptors"]:
        assert resolver.migration_descriptor_is_trusted(
            entry["migration_descriptor_digest"]
        ) is entry["trusted"]
