"""Run all 30 optional source vectors against the production provider adapter."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from .harness import conformance_root
from .runtime_provider_adapter import execute

_ROOT = conformance_root() / "conformance/profiles/runtime-provider/provider-01-exact-source"
_VECTORS = json.loads((_ROOT / "vectors.generated.json").read_text())["vectors"]
_SOURCES = ("provider/test_provider.py", "provider/test_provider.rs")


@pytest.mark.parametrize("vector", _VECTORS, ids=lambda item: item["name"])
def test_runtime_provider_source_profile(vector: dict[str, Any], tmp_path: Path) -> None:
    request = vector["request"]
    names = {request["bundle"], "provider-closure.json", *_SOURCES}
    for member in ("source_file", "manifest_file", "generated_bundle_file"):
        if request["arguments"].get(member) is not None:
            names.add(request["arguments"][member])
    for name in names:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(_ROOT / name, target)
    result = execute(
        {
            "request": request,
            "profile_root": str(tmp_path),
            "source_files": list(_SOURCES),
            "source_closure_file": "provider-closure.json",
        }
    )
    assert result["observation"] == vector["expected"]
    closure = json.loads((_ROOT / "provider-closure.json").read_text())
    assert result["loaded_closure_digest"] == _VECTORS[0]["request"]["installed"]["closure_digest"]
    assert all(
        path in _SOURCES
        and digest == next(item["sha256"] for item in closure["files"] if item["path"] == path)
        for path, digest in result["loaded_source"].items()
    )
