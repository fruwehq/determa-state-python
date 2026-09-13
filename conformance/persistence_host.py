"""Production-backed driver for the optional durable persistence profile."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from determa.state import ExecutionHostError
from determa.state.persistence import PersistenceHost, SQLiteDurableHostStore

from .version2 import _json, _resolver


def _call_log(item: Any) -> list[str]:
    return _json(item.path / item.vector["call_log"])["calls"]


def run_persistence_vector(
    item: Any, request: dict[str, Any]
) -> tuple[dict[str, Any], bytes]:
    before = (item.path / item.vector["store_before"]).read_bytes()
    root_instance_id = _json(item.path / item.vector["store_before"])["checkpoint"][
        "root_instance_id"
    ]
    with tempfile.TemporaryDirectory(prefix="determa-persistence-") as directory:
        store = SQLiteDurableHostStore(Path(directory) / "host.sqlite")
        store.setup_schema()
        store.seed(root_instance_id, before)
        host = PersistenceHost(store, _resolver(item.path, request))
        try:
            if item.vector["operation"] == "persistence_release_quarantine_v2":
                response = host.release_quarantine_v2(request)
            else:
                response = host.process_v2(request)
        except ExecutionHostError as error:
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
                "mutation": (
                    "atomic"
                    if error.code == "response_lost_after_commit"
                    else "none"
                ),
                "core_calls": host.core_calls,
                "broker_acknowledged": False,
                "code": error.code,
            }
        stored = store.snapshot(root_instance_id)
    assert host.calls == _call_log(item)
    return response, stored
