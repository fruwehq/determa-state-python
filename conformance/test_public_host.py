"""Pinned public wire goldens and executable minimal local-host profile."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from determa.state import MemoryArtifactResolver, load_bundle
from determa.state.public_client import (
    PublicHostError,
    public_request_digest,
    validate_public_message,
)
from determa.state.public_host import SQLitePublicExecutionHost
from determa.state.wire import canonical_bytes, hash_value

from .harness import conformance_root


def goldens(name):
    root = Path(os.environ["DETERMA_SPEC_DIR"])
    return json.loads((root / "examples/public-host" / name).read_text())


def test_complete_public_golden_wire_and_hash_matrix():
    positive = goldens("positive-v1.json")
    negative = goldens("negative-v1.json")
    for case in positive["cases"]:
        validate_public_message(case["request"])
        validate_public_message(case["response"], response=True)
        assert public_request_digest(case["request"]) == case["request_digest"]
        assert (
            canonical_bytes(["determa-public-host-request-digest-1", "1", case["request"]]).decode()
            == case["request_hash_operand_jcs"]
        )
    for case in negative["cases"]:
        if "request_hash_operand_jcs" in case:
            assert (
                hash_value(["determa-public-host-request-digest-1", "1", case["request"]])
                == case["request_digest"]
            )
        if "response" in case:
            validate_public_message(case["response"], response=True)
    for case in negative["invalid_responses"]:
        if case["expected_error"] == "schema_validation":
            with pytest.raises(PublicHostError):
                validate_public_message(case["response"], response=True)


def test_local_host_exact_public_core_goldens(tmp_path):
    bundle = load_bundle(
        (
            conformance_root()
            / "conformance/core/119-native-v1-aggregate-integrity/root-machine.yaml"
        ).read_text()
    )
    host = SQLitePublicExecutionHost(
        tmp_path / "host.db",
        scope_alias="scope",
        scope_binding_identity="binding-local-1",
        authorized_principals=frozenset({"alice"}),
        resolver=MemoryArtifactResolver(definitions={bundle.fingerprint: bundle}),
    )
    host.setup_schema()
    names = {
        "create_committed",
        "read_existing_checkpoint",
        "inspect_absent_target_in_checkpoint",
        "process_empty_mailbox",
        "retained_operation_receipt",
    }
    observed = set()
    for case in goldens("positive-v1.json")["cases"]:
        if case["name"] in names:
            assert host.handle(case["request"], principal="alice") == case["response"], case["name"]
            observed.add(case["name"])
    assert observed == names
