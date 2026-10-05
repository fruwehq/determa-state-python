"""Execute all seven native inspection vectors through the public operation."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from determa.state import MemoryArtifactResolver, inspect_candidate

from .harness import conformance_root
from .provider_fixture import installed_inspection_bundle

_ROOT = conformance_root() / "conformance/profiles/inspection-provider/provider-01-exact-closure"
_VECTORS = yaml.safe_load((_ROOT / "test.yaml").read_text())["inspection_vectors"]


def _member(path: Path, pointer: str) -> Any:
    result: Any = json.loads(path.read_bytes())
    for token in pointer.split("/")[1:]:
        result = result[token.replace("~1", "/").replace("~0", "~")]
    return result


@pytest.mark.parametrize("vector", _VECTORS, ids=lambda vector: vector["name"])
def test_native_inspection_profile(vector: dict[str, Any]) -> None:
    bundle, registry = installed_inspection_bundle(_ROOT)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    aggregate = (_ROOT / vector["aggregate_before"]).read_bytes()
    unchanged = (_ROOT / vector["aggregate_after"]).read_bytes()
    request = _member(_ROOT / vector["request_file"], vector["request_pointer"])
    expected = _member(_ROOT / vector["outcome_file"], vector["outcome_pointer"])
    prior_state = {
        key: copy.deepcopy(vars(selected.provider)) for key, selected in registry._active.items()
    }
    observed = inspect_candidate(aggregate, request, resolver)
    assert observed == expected
    assert aggregate == unchanged
    assert {
        key: vars(selected.provider) for key, selected in registry._active.items()
    } == prior_state
    assert all(selected.provider.ordinary_calls == 0 for selected in registry._active.values())
    assert all(selected.provider.external_calls == 0 for selected in registry._active.values())
