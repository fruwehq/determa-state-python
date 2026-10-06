"""Ordinary public checkpoint writes must not split native effects ownership."""

import json
import sqlite3

import pytest

from determa.state import delivery_request_digest, portable_envelope
from determa.state.authority import AuthoritySQLiteExecutionStore
from determa.state.host import ExecutionHost, ExecutionHostError
from determa.state.stores.base import ExecutionStoreError
from determa.state.wire import canonical_bytes

from .test_committed_effects import authority_effect_fixture


def database_snapshot(path):
    with sqlite3.connect(path) as connection:
        return "\n".join(connection.iterdump())


def fixture(tmp_path, *, legacy_checkpoint=False):
    authority, effects, scope, root, _record = authority_effect_fixture(tmp_path)
    checkpoint = effects.snapshot(root)["checkpoint"]
    store = AuthoritySQLiteExecutionStore(authority, scope, "owner", "0")
    store.setup_schema()
    if legacy_checkpoint:
        # Earlier adapters permitted an equal second checkpoint representation.
        # Reproduce that extant native state without using the now-refused writer.
        with sqlite3.connect(authority.path) as connection:
            connection.execute(
                "INSERT INTO determa_execution_checkpoints VALUES (?,?,?,?)",
                (
                    root,
                    checkpoint["revision"],
                    checkpoint["execution_checkpoint_digest"],
                    canonical_bytes(checkpoint),
                ),
            )
    return authority, effects, root, checkpoint, store


@pytest.mark.parametrize("mutation", ["insert", "external_admission"])
def test_ordinary_checkpoint_mutations_refuse_effects_owned_root(tmp_path, mutation):
    authority, effects, root, checkpoint, store = fixture(
        tmp_path, legacy_checkpoint=mutation == "external_admission"
    )
    before = database_snapshot(authority.path)
    initial = effects.snapshot(root)
    with pytest.raises((ExecutionStoreError, ExecutionHostError), match="scope_fence_unproven"):
        if mutation == "insert":
            with store.transaction(root) as transaction:
                transaction.insert(canonical_bytes(checkpoint))
        else:
            runtime = checkpoint["root_record"]["aggregate_state"]["runtimes"][0]
            envelope = portable_envelope(
                "native_cancelled",
                "external-admission",
                {"root": {"root_instance_id": root, "root_runtime_id": runtime["runtime_id"]}},
                {},
            )
            delivery = {
                "delivery_mode": "input",
                "envelope": envelope,
                "envelope_digest": delivery_request_digest(root, "input", envelope),
            }
            ExecutionHost(store, effects.resolver).admit_v1(
                root,
                [delivery],
                expected_revision=checkpoint["revision"],
                expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
            )
    assert database_snapshot(authority.path) == before
    assert effects.snapshot(root) == initial


def test_ordinary_read_preserves_an_equal_legacy_checkpoint_and_native_history(tmp_path):
    authority, effects, root, checkpoint, store = fixture(tmp_path, legacy_checkpoint=True)
    before = database_snapshot(authority.path)
    actual = ExecutionHost(store, effects.resolver).read_checkpoint(root)
    assert actual is not None and actual.document == checkpoint
    assert database_snapshot(authority.path) == before


@pytest.mark.parametrize("damage", ["missing_indexes", "empty_indexes"])
def test_lost_native_indexes_cannot_reclassify_ordinary_writer_permission(tmp_path, damage):
    authority, effects, root, checkpoint, store = fixture(tmp_path)
    scope = effects.authority_scope
    ledger = authority.inspect(scope)
    for key in ("native_effect_roots", "native_effect_document_bytes"):
        if damage == "missing_indexes":
            del ledger[key]
        else:
            ledger[key] = []
    with authority._connect() as connection:
        connection.execute(
            "UPDATE determa_scope_authority SET ledger=? WHERE scope_identity=?",
            (json.dumps(ledger), scope),
        )
        connection.commit()
    before = database_snapshot(authority.path)
    entered = False
    with pytest.raises(ExecutionStoreError, match="scope_fence_unproven"):
        with store.transaction(root) as transaction:
            entered = True
            transaction.insert(canonical_bytes(checkpoint))
    assert not entered
    assert database_snapshot(authority.path) == before
