"""Execute pinned exact-target inspection vectors through the public operation."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from determa.state import MemoryArtifactResolver, inspect_candidate, load_bundle
from determa.state.wire import canonical_bytes

from .harness import conformance_root

_DIRECTORY = conformance_root() / "conformance" / "core" / "125-exact-candidate-inspection"


def _vectors() -> list[dict[str, Any]]:
    manifest = yaml.safe_load((_DIRECTORY / "test.yaml").read_text(encoding="utf-8"))
    return manifest["inspection_vectors"]


def _pointer(source: Path, pointer: str) -> Any:
    value: Any = json.loads(source.read_bytes())
    for token in pointer.split("/")[1:]:
        value = value[token.replace("~1", "/").replace("~0", "~")]
    return value


@pytest.mark.parametrize("vector", _vectors(), ids=lambda vector: vector["name"])
def test_exact_candidate_inspection(
    vector: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bind requests/results to real restored artifacts and verify zero mutation."""
    bundle = load_bundle((_DIRECTORY / vector["bundle"]).read_text(encoding="utf-8"))
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    before = (_DIRECTORY / vector["aggregate_before"]).read_bytes()
    after = (_DIRECTORY / vector["aggregate_after"]).read_bytes()
    request = _pointer(_DIRECTORY / vector["request_file"], vector["request_pointer"])
    expected = _pointer(_DIRECTORY / vector["outcome_file"], vector["outcome_pointer"])
    request_snapshot = copy.deepcopy(request)
    resolver_snapshot = resolver.snapshot()
    import determa.state.cel as ordinary_cel
    import determa.state.inspection_cel as safe_cel

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("inspection called ordinary CEL evaluation")

    monkeypatch.setattr(ordinary_cel, "evaluate", forbidden)
    calls = 0
    original = safe_cel._Interpreter.eval

    def observed(self: Any, node: Any) -> Any:
        nonlocal calls
        if self.spent == 0:
            calls += 1
        return original(self, node)

    monkeypatch.setattr(safe_cel._Interpreter, "eval", observed)
    supplied = json.loads(before)
    supplied_snapshot = copy.deepcopy(supplied)
    result = inspect_candidate(
        supplied,
        request,
        resolver,
        semantic_enabled=vector["profile"] != "without_safe_semantic",
    )
    assert canonical_bytes(result) == canonical_bytes(expected)
    assert supplied == supplied_snapshot
    assert request == request_snapshot
    assert resolver.snapshot() == resolver_snapshot
    assert before == after
    assert calls == vector["expect"]["guard_evaluations"]
    assert vector["expect"]["action_invocations"] == 0
    assert vector["expect"]["external_calls"] == 0
    assert vector["expect"]["emissions"] == 0


def test_inspection_oracle_rejects_sabotaged_production_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vector = _vectors()[0]
    with monkeypatch.context() as patches:
        patches.setattr(
            "conformance.test_inspection.inspect_candidate",
            lambda *_args, **_kwargs: {
                "code": "invalid_inspection_request",
                "source_locator": None,
            },
        )
        with pytest.raises(AssertionError):
            test_exact_candidate_inspection(vector, patches)
