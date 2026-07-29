from __future__ import annotations

import copy

import pytest

from determa.state import (
    ArtifactError,
    MemoryArtifactResolver,
    aggregate_envelope,
    create,
    load_bundle,
    restore_aggregate,
    serialize_aggregate,
)
from determa.state.wire import canonical_bytes, strict_json

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
