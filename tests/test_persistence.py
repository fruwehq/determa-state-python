from __future__ import annotations

import copy

import pytest

from determa.state import (
    ArtifactError,
    MemoryArtifactResolver,
    aggregate_envelope,
    aggregate_shape_fingerprint,
    create,
    load_bundle,
    migrate_aggregate,
    restore_aggregate,
    serialize_aggregate,
)
from determa.state.wire import (
    aggregate_state_digest,
    canonical_bytes,
    migration_descriptor_digest,
    strict_json,
)

SOURCE = """
format: 1
namespace: tests.persistence
machines:
  - machine_id: job
    version: 1
    root:
      variables:
        count: {type: int, init: 3}
"""

COMPONENT_SOURCE = """
format: 1
namespace: tests.persistence_components
machines:
  - machine_id: owner
    root:
      type: parallel
      components:
        - component_id: worker
          machine_id: worker
        - component_id: second_worker
          machine_id: worker
  - machine_id: worker
    root: {}
"""

SPAWN_SOURCE = """
format: 1
namespace: tests.persistence_spawn
machines:
  - machine_id: owner
    root:
      variables:
        worker_reference:
          type: instance_reference
          machine_id: worker
          nullable: true
          init: null
      entry:
        - spawn:
            machine_id: worker
            bind_to: worker_reference
  - machine_id: worker
    version: 1
    root: {}
"""


def _resolver_for(bundle):
    return MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})


def _redigest(document):
    document["aggregate_state_digest"] = aggregate_state_digest(document)
    return canonical_bytes(document)


def test_aggregate_round_trip_is_canonical_and_does_not_mutate_state() -> None:
    bundle = load_bundle(SOURCE)
    created = create(bundle, "job", "job-1", "create-1", {})
    state = created["state"]
    assert state is not None
    snapshot = copy.deepcopy(state)

    encoded = serialize_aggregate(bundle, state)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    restored = restore_aggregate(encoded, resolver)

    assert state == snapshot
    assert restored.aggregate_envelope == aggregate_envelope(bundle, restored.state)
    assert restored.canonical_bytes == encoded
    assert canonical_bytes(restored.aggregate_envelope) == encoded


def test_restore_rejects_root_header_version_mismatch() -> None:
    bundle = load_bundle(SOURCE)
    created = create(bundle, "job", "job-1", "create-1", {})
    assert created["state"] is not None
    document = aggregate_envelope(bundle, created["state"])
    document["root_machine_version"] = "2"

    with pytest.raises(ArtifactError) as raised:
        restore_aggregate(_redigest(document), _resolver_for(bundle))

    assert raised.value.code == "invalid_aggregate_state"


@pytest.mark.parametrize(
    ("field", "forged"),
    [
        ("root_instance_id", "forged-root"),
        ("owner_runtime_id", "sha256:" + "0" * 64),
        ("component_id", "forged_component"),
        ("component_runtime_id", "sha256:" + "1" * 64),
        ("activation_sequence", "7"),
    ],
)
def test_restore_rejects_forged_component_target_identity(
    field: str, forged: str
) -> None:
    bundle = load_bundle(COMPONENT_SOURCE)
    created = create(bundle, "owner", "owner-1", "create-1", {})
    assert created["state"] is not None
    document = aggregate_envelope(bundle, created["state"])
    runtime = next(
        item for item in document["runtimes"] if item["relation"]["kind"] == "component"
    )
    runtime["target_identity"]["component"][field] = forged

    with pytest.raises(ArtifactError) as raised:
        restore_aggregate(_redigest(document), _resolver_for(bundle))

    assert raised.value.code == "invalid_aggregate_state"


@pytest.mark.parametrize(
    ("field", "forged"),
    [
        ("root_instance_id", "forged-root"),
        ("instance_id", "sha256:" + "2" * 64),
        ("machine_id", "owner"),
        ("machine_version", "7"),
    ],
)
def test_restore_rejects_forged_spawned_target_identity(
    field: str, forged: str
) -> None:
    bundle = load_bundle(SPAWN_SOURCE)
    created = create(bundle, "owner", "owner-1", "create-1", {})
    assert created["state"] is not None
    document = aggregate_envelope(bundle, created["state"])
    runtime = next(
        item
        for item in document["runtimes"]
        if item["relation"]["kind"] == "owned_spawned_instance"
    )
    runtime["target_identity"]["spawned_instance"][field] = forged

    with pytest.raises(ArtifactError) as raised:
        restore_aggregate(_redigest(document), _resolver_for(bundle))

    assert raised.value.code == "invalid_aggregate_state"


def test_migration_definition_byte_limit_uses_typed_normalized_bundle() -> None:
    version = 9_007_199_254_740_992
    source = load_bundle(
        f"""
format: 1
namespace: tests.persistence_unsafe_version
meta: {{release: source}}
machines:
  - machine_id: job
    version: {version}
    root: {{}}
"""
    )
    target = load_bundle(
        f"""
format: 1
namespace: tests.persistence_unsafe_version
meta: {{release: target}}
events:
  notice: {{direction: internal}}
machines:
  - machine_id: job
    version: {version}
    root: {{}}
"""
    )
    shape = aggregate_shape_fingerprint(source)
    assert shape == aggregate_shape_fingerprint(target)
    descriptor = {
        "migration_descriptor_format": "determa.aggregate_migration",
        "migration_descriptor_schema_version": 1,
        "source_machine_format": 1,
        "target_machine_format": 1,
        "source_validated_bundle_fingerprint": source.fingerprint,
        "target_validated_bundle_fingerprint": target.fingerprint,
        "source_aggregate_shape_fingerprint": shape,
        "target_aggregate_shape_fingerprint": shape,
        "mode": "compatible",
        "mappings": {
            "machines": [],
            "active_states": [],
            "variables": [],
            "history": [],
            "components": [],
            "owned_runtimes": [],
            "lifetime_holders": [],
            "counters": [],
        },
        "terminal_policy": {"completed": "preserve", "faulted": "preserve"},
        "resource_requirements": {
            "maximum_transformed_output_bytes": "0",
            "maximum_cel_expression_length": "0",
            "maximum_cel_ast_nodes": "0",
            "maximum_cel_evaluation_steps": "0",
        },
    }
    descriptor["migration_descriptor_digest"] = migration_descriptor_digest(descriptor)
    resolver = MemoryArtifactResolver(
        definitions={source.fingerprint: source, target.fingerprint: target},
        migration_descriptors={
            descriptor["migration_descriptor_digest"]: descriptor,
        },
    )
    created = create(source, "job", "job-1", "create-1", {})
    assert created["state"] is not None

    result = migrate_aggregate(
        serialize_aggregate(source, created["state"]),
        target.fingerprint,
        [descriptor["migration_descriptor_digest"]],
        resolver,
        maintenance_mode=False,
    )

    assert result.failure is None
    assert result.aggregate_envelope is not None
    assert result.aggregate_envelope["root_machine_version"] == str(version)


def test_untrusted_definition_fails_closed() -> None:
    bundle = load_bundle(SOURCE)
    created = create(bundle, "job", "job-1", "create-1", {})
    assert created["state"] is not None
    encoded = serialize_aggregate(bundle, created["state"])
    resolver = MemoryArtifactResolver(
        definitions={bundle.fingerprint: bundle}, trusted_definitions=[]
    )

    with pytest.raises(ArtifactError, match="definition_untrusted") as raised:
        restore_aggregate(encoded, resolver)

    assert raised.value.code == "definition_untrusted"


def test_strict_json_rejects_duplicate_members_and_nonfinite_numbers() -> None:
    with pytest.raises(ArtifactError) as duplicate:
        strict_json(b'{"value":1,"value":2}')
    assert duplicate.value.code == "duplicate_json_name"

    with pytest.raises(ArtifactError) as nonfinite:
        strict_json(b'{"value":NaN}')
    assert nonfinite.value.code == "invalid_json_value"
