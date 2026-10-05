"""Install the exact pinned native inspection fixture through the public registry."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import yaml

from determa.state import Bundle, load_bundle
from determa.state.runtime_providers import RuntimeProviderRegistry, SourceClosure


def installed_inspection_bundle(root: Path) -> tuple[Bundle, RuntimeProviderRegistry]:
    source = root / "provider/test_provider.py"
    module_name = "determa_conformance_inspection_provider"
    spec = importlib.util.spec_from_file_location(module_name, source)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    closure = SourceClosure(
        root,
        ("provider/test_provider.py", "provider/test_provider.rs"),
        "provider-closure.json",
        b"determa-test-inspection-provider-closure-1\0",
        "provider/test_provider.py",
    )
    registry = RuntimeProviderRegistry()
    document = yaml.safe_load((root / "machine.yaml").read_text(encoding="utf-8"))
    events = document["machines"][0]["root"]["states"]["waiting"]["on_events"]
    safe_type = module.SafeGuard

    def safe_proof(provider: Any, _binding: Any) -> frozenset[str]:
        if (
            type(provider) is not safe_type
            or closure.digest()
            != events["safe"]["guard"]["provider"]["provider_reference"]["content_digest"]
        ):
            return frozenset()
        return frozenset(
            {"deterministic", "pure", "process_contained", "semantically_introspectable"}
        )

    for name, provider_type in (("safe", safe_type), ("unsafe", module.UnsafeGuard)):
        registry.register(
            {"kind": "guard", "binding": events[name]["guard"]["provider"]},
            provider_type,
            closure,
            guard_method="evaluate",
            capability_proof=safe_proof if name == "safe" else None,
        )
    return load_bundle(document, runtime_providers=registry), registry
