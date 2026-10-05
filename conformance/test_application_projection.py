"""Black-box application projection vectors through the public facade."""

from __future__ import annotations

import copy
import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

import pytest

import determa.state.application_projection as projection_module
import determa.state.checkpoint_v1 as checkpoint_module
from determa.state import (
    ApplicationProjectionFacade,
    MemoryArtifactResolver,
    ProjectionError,
    load_bundle,
)
from determa.state.errors import ArtifactError
from determa.state.stores.base import (
    SHARED_APPLICATION_TRANSACTION,
    ExecutionStore,
    ExecutionStoreTransaction,
)
from determa.state.wire import canonical_bytes

from .harness import conformance_root

PROFILE = (
    conformance_root() / "conformance/profiles/application-projection/projection-01-lossless-facade"
)
VECTORS = json.loads((PROFILE / "projection-v1.json").read_text())["vectors"]


class Transaction(ExecutionStoreTransaction):
    def __init__(self, native: dict[str, Any], root: str) -> None:
        self.native = native
        self._root = root

    @property
    def root_instance_id(self) -> str:
        return self._root

    def load(self) -> bytes | None:
        return self.native["checkpoint"]

    def insert(self, checkpoint: bytes) -> bool:
        if self.native["checkpoint"] is not None:
            return False
        self.native["checkpoint"] = checkpoint
        return True

    def replace(
        self, expected_revision: str, expected_checkpoint_digest: str, checkpoint: bytes
    ) -> bool:
        current = json.loads(self.native["checkpoint"])
        if (current["revision"], current["execution_checkpoint_digest"]) != (
            expected_revision,
            expected_checkpoint_digest,
        ):
            return False
        self.native["checkpoint"] = checkpoint
        return True


class Store(ExecutionStore):
    def __init__(self, before: dict[str, Any], available: bool) -> None:
        self.saved = before
        self.available = available

    @property
    def capabilities(self) -> frozenset[str]:
        return frozenset({SHARED_APPLICATION_TRANSACTION}) if self.available else frozenset()

    @contextmanager
    def transaction(self, root_instance_id: str) -> Iterator[ExecutionStoreTransaction]:
        raise AssertionError("projection must use shared transaction")
        yield Transaction({}, root_instance_id)

    @contextmanager
    def shared_transaction(
        self, root_instance_id: str
    ) -> Iterator[tuple[Any, ExecutionStoreTransaction]]:
        native = copy.deepcopy(self.saved)
        yield native, Transaction(native, root_instance_id)
        self.saved = native

    def setup_schema(self) -> None:
        pass

    def health(self) -> Mapping[str, Any]:
        return {}


class Rows:
    def __init__(self, configuration: dict[str, Any], capacity: str) -> None:
        self.configuration = configuration
        self.capacity = capacity

    def read(self, native: dict[str, Any], row_ids: Sequence[str]) -> dict[str, Any]:
        return copy.deepcopy(native)

    def validate_selection(
        self, rows: dict[str, Any], row_ids: Sequence[str], root_instance_id: str
    ) -> None:
        selected = rows["selected_rows"]
        if (
            len(selected) != len(row_ids)
            or {row["row_id"] for row in selected} != set(row_ids)
            or any(row["root_instance_id"] != root_instance_id for row in selected)
        ):
            raise ProjectionError("invalid_projection_selection")

    def prepare_delivery(
        self, rows: dict[str, Any], request: Mapping[str, Any], delivery: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        mapped = request.get("mapped_input")
        if mapped is None:
            return delivery
        config = self.configuration
        field = mapped["field"]
        if "source_field" in config:
            declared = config["input_declaration"]
            allowed = field == config["source_field"]
        else:
            declarations = dict(zip(config["source_fields"], config["declarations"], strict=True))
            declared = declarations.get(field)
            allowed = field in declarations
        value = rows["selected_rows"][0].get(field)
        if (
            not allowed
            or mapped["declaration"] != declared
            or value != mapped["value"]
            or value[0] != declared
        ):
            raise ProjectionError("invalid_projection_input")
        prepared = copy.deepcopy(dict(delivery))
        if request["boundary"] == "declared_input":
            prepared["envelope"]["payload"] = ["map", [[config["input_payload_field"], value]]]
        else:
            original_changed = prepared["envelope"]["payload"][1][0][1][1]
            fields = [name for name, _ in original_changed]
            allowed_fields = dict(zip(config["source_fields"], config["declarations"], strict=True))
            if any(name not in allowed_fields for name in fields):
                raise ProjectionError("invalid_projection_input")
            changed = []
            for name in fields:
                row_value = rows["selected_rows"][0].get(name)
                if not isinstance(row_value, list) or row_value[0] != allowed_fields[name]:
                    raise ProjectionError("invalid_projection_input")
                changed.append([name, row_value])
            prepared["envelope"]["payload"] = ["map", [["changed", ["map", changed]]]]
        return prepared

    def project(self, rows: dict[str, Any], checkpoint: Mapping[str, Any]) -> dict[str, Any]:
        if self.capacity != "complete":
            raise ProjectionError("projection_not_lossless")
        proposed = copy.deepcopy(rows)
        proposed["supplemental_checkpoint"] = canonical_bytes(checkpoint)
        aggregate = checkpoint["root_record"].get("aggregate_state")
        if (
            aggregate is not None
            and self.configuration.get("status_rule") == "done_when_count_positive"
        ):
            values = aggregate["runtimes"][0]["variables"]
            count = next(
                (
                    item["value"]
                    for item in values
                    if item["variable_declaration_pointer"].endswith("/count")
                ),
                None,
            )
            if count is not None and int(count[1]) > 0:
                proposed["selected_rows"][0]["status"] = "done"
        return proposed

    def reconstruct(self, rows: dict[str, Any]) -> bytes:
        source = rows.get("supplemental_checkpoint")
        if not isinstance(source, bytes):
            raise ProjectionError("projection_not_lossless")
        return source

    def reconstruct_aggregate(self, rows: dict[str, Any]) -> bytes:
        source = rows.get("supplemental_aggregate")
        if not isinstance(source, bytes):
            raise ProjectionError("projection_not_lossless")
        return source

    def project_aggregate(
        self, rows: dict[str, Any], aggregate: Mapping[str, Any]
    ) -> dict[str, Any]:
        if self.capacity != "complete":
            raise ProjectionError("projection_not_lossless")
        proposed = copy.deepcopy(rows)
        proposed["supplemental_aggregate"] = canonical_bytes(aggregate)
        return proposed

    def write(self, native_transaction: dict[str, Any], rows: dict[str, Any]) -> None:
        for key in ("selected_rows", "supplemental_checkpoint", "supplemental_aggregate"):
            native_transaction[key] = copy.deepcopy(rows[key])


def _member(value: str | None) -> bytes | None:
    return None if value is None else (PROFILE / value).read_bytes()


@pytest.mark.parametrize("vector", VECTORS, ids=lambda vector: vector["name"])
def test_application_projection(vector: dict[str, Any]) -> None:
    request = vector["request"]
    before = vector["before"]
    initial = {
        "selected_rows": copy.deepcopy(before["selected_rows"]),
        "checkpoint": _member(before.get("checkpoint")),
        "supplemental_checkpoint": _member(before.get("supplemental_checkpoint")),
        "supplemental_aggregate": _member(before.get("supplemental_aggregate")),
    }
    store = Store(initial, request["shared_transaction_available"])
    bundles = [
        load_bundle((PROFILE / file).read_text())
        for file in (
            "machine.yaml",
            "external-machine.yaml",
            "deferral-machine.yaml",
            "outbox-machine.yaml",
            "replay-target.yaml",
        )
    ]
    resolver = MemoryArtifactResolver(
        definitions={bundle.fingerprint: bundle for bundle in bundles}
    )
    facade = ApplicationProjectionFacade(store, resolver)
    mapping = Rows(request["mapping"], request["supplemental_capacity"])
    creation = request.get("creation") or {}
    create_bundle = next(
        (
            bundle
            for bundle in bundles
            if any(
                machine["machine_id"] == creation.get("machine_id")
                for machine in bundle.raw["machines"]
            )
        ),
        None,
    )
    expected = vector["outcome"]
    with (
        patch.object(
            checkpoint_module, "create_aggregate_v1", wraps=checkpoint_module.create_aggregate_v1
        ) as create_core,
        patch.object(
            checkpoint_module, "admit_aggregate_v1", wraps=checkpoint_module.admit_aggregate_v1
        ) as admit_core,
        patch.object(
            checkpoint_module, "step_aggregate_v1", wraps=checkpoint_module.step_aggregate_v1
        ) as step_core,
        patch.object(
            projection_module, "step_aggregate_v1", wraps=projection_module.step_aggregate_v1
        ) as direct_step_core,
    ):
        try:
            actual = facade.run(request, mapping, create_bundle=create_bundle)
        except (ProjectionError, ArtifactError) as exc:
            assert expected["kind"] == "failure"
            assert exc.code == expected["code"]
        else:
            assert expected["kind"] != "failure"
            assert actual == expected["result_value"]
        assert (
            create_core.call_count
            + admit_core.call_count
            + step_core.call_count
            + direct_step_core.call_count
        ) == expected["core_calls"]
    observed = store.saved
    after = vector["after"]
    assert observed["selected_rows"] == after["selected_rows"]
    for field in ("checkpoint", "supplemental_checkpoint", "supplemental_aggregate"):
        expected_member = _member(after.get(field))
        if expected_member is None:
            assert observed[field] is None
        else:
            assert json.loads(observed[field]) == json.loads(expected_member)


def test_application_row_write_failure_rolls_back_checkpoint() -> None:
    """A failed row write after checkpoint staging cannot expose a commit."""
    vector = next(item for item in VECTORS if item["name"] == "create_result_shape")
    request = vector["request"]
    initial = {
        "selected_rows": copy.deepcopy(vector["before"]["selected_rows"]),
        "checkpoint": None,
        "supplemental_checkpoint": None,
        "supplemental_aggregate": None,
    }
    store = Store(copy.deepcopy(initial), True)
    bundle = load_bundle((PROFILE / "machine.yaml").read_text())
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})

    class FailingRows(Rows):
        def write(self, native_transaction: dict[str, Any], rows: dict[str, Any]) -> None:
            raise RuntimeError("application row write failed")

    with pytest.raises(RuntimeError, match="application row write failed"):
        ApplicationProjectionFacade(store, resolver).run(
            request,
            FailingRows(request["mapping"], "complete"),
            create_bundle=bundle,
        )
    assert store.saved == initial
