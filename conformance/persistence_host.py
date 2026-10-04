"""Production-backed driver for the optional durable persistence profile."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from determa.state import ExecutionHostError
from determa.state.persistence import PersistenceHost, SQLiteDurableHostStore

from .durable_host import _same_typed_json
from .version1 import _json, _pointer, _resolver


def _fault_injector(boundary: str | None):
    def inject(actual: str) -> None:
        if actual == boundary:
            if actual == "after_commit_before_acknowledgement":
                raise ExecutionHostError("response_lost_after_commit")
            raise ExecutionHostError("injected_pre_commit_failure")

    return inject


def _call_log(item: Any) -> list[str]:
    return _json(item.path / item.vector["call_log"])["calls"]


def run_persistence_vector(item: Any, request: dict[str, Any]) -> tuple[dict[str, Any], bytes]:
    before = (item.path / item.vector["store_before"]).read_bytes()
    root_instance_id = _json(item.path / item.vector["store_before"])["checkpoint"][
        "root_instance_id"
    ]
    with tempfile.TemporaryDirectory(prefix="determa-persistence-") as directory:
        store = SQLiteDurableHostStore(Path(directory) / "host.sqlite")
        store.setup_schema()
        store.seed(root_instance_id, before)
        host = PersistenceHost(
            store,
            _resolver(item.path, request),
            fault_injector=_fault_injector(item.vector.get("failure_boundary")),
        )
        caller_returned = False
        caller_response: dict[str, Any] | None = None
        failure_code: str | None = None
        try:
            if item.vector["operation"] == "persistence_release_quarantine_v1":
                caller_response = host.release_quarantine_v1(request)
            else:
                caller_response = host.process_v1(request)
            caller_returned = True
            response = caller_response
        except ExecutionHostError as error:
            failure_code = error.code
            response = {
                "result": (
                    "crashed"
                    if error.code
                    in {
                        "injected_pre_commit_failure",
                        "response_lost_after_commit",
                    }
                    else "rejected"
                ),
                "mutation": ("atomic" if error.code == "response_lost_after_commit" else "none"),
                "core_calls": host.core_calls,
                "broker_acknowledged": False,
                "code": error.code,
            }
        stored = store.snapshot(root_instance_id)
    raw_reference = item.vector["raw_response"]
    oracle = _pointer(_json(item.path / raw_reference["file"]), raw_reference["pointer"])
    if oracle["kind"] == "typed_failure":
        assert not caller_returned
        assert _same_typed_json({"code": failure_code}, oracle["body"])
    elif oracle["kind"] == "no_response":
        assert not caller_returned
        assert "body" not in oracle
    else:
        assert caller_returned
        assert _same_typed_json(host.raw_response, oracle["body"])
    assert host.calls == _call_log(item)
    return response, stored
