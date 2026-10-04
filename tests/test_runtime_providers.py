"""Operational source and compiler boundaries for optional native slots."""

from __future__ import annotations

import importlib.util
import json
import shutil
import sys
from pathlib import Path

import pytest
import yaml

from conformance.harness import conformance_root
from determa.state import (
    MemoryArtifactResolver,
    RuntimeProviderError,
    RuntimeProviderRegistry,
    SourceClosure,
    compile_language_source,
    create,
    load_bundle,
    restore_aggregate,
)

_PROFILE = conformance_root() / "conformance/profiles/runtime-provider/provider-01-exact-source"
_DOMAIN = b"determa-test-runtime-provider-closure-1\0"


def _installed_compiler(root: Path) -> tuple[RuntimeProviderRegistry, dict]:
    spec = importlib.util.spec_from_file_location(
        "test_e_source_provider", root / "provider/test_provider.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    closure = SourceClosure(
        root,
        ("provider/test_provider.py", "provider/test_provider.rs"),
        "provider-closure.json",
        _DOMAIN,
        "provider/test_provider.py",
    )
    source = json.loads((root / "source-package.json").read_text())
    registry = RuntimeProviderRegistry()
    registry.register_dependency(source["content"]["dependencies"][0], closure)
    registry.register_compiler(
        source["content"]["regions"][0]["provider_reference"],
        module.compile_region,
        closure,
        capability_proof=lambda compiler: (
            frozenset({"deterministic", "pure", "portable", "semantically_introspectable"})
            if compiler is module.compile_region
            else frozenset()
        ),
    )
    return registry, source


def test_exact_compilation_and_manifest_fingerprint() -> None:
    registry, source = _installed_compiler(_PROFILE)
    manifest = json.loads((_PROFILE / "source-manifest.json").read_text())
    bundle = compile_language_source(source, registry, manifest=manifest)
    assert bundle.fingerprint == manifest["content"]["generated_validated_bundle_fingerprint"]
    manifest["content"]["generated_validated_bundle_fingerprint"] = "sha256:" + "0" * 64
    with pytest.raises(RuntimeProviderError, match="language_compilation_failed"):
        compile_language_source(source, registry, manifest=manifest)


def test_compiler_source_drift_refused_before_callback(tmp_path: Path) -> None:
    shutil.copytree(_PROFILE, tmp_path / "fixture")
    root = tmp_path / "fixture"
    registry, source = _installed_compiler(root)
    path = root / "provider/test_provider.py"
    path.write_bytes(path.read_bytes() + b"\n# altered installed source\n")
    with pytest.raises(RuntimeProviderError, match="runtime_provider_unavailable"):
        compile_language_source(source, registry)


def test_compiler_alias_replacement_refused(tmp_path: Path) -> None:
    shutil.copytree(_PROFILE, tmp_path / "fixture")
    registry, source = _installed_compiler(tmp_path / "fixture")
    reference = source["content"]["regions"][0]["provider_reference"]
    key = (reference["identifier"], reference["version"], reference["content_digest"])
    selected, closure, proof = registry._compilers[key]

    def substituted(_source: str) -> str:
        raise AssertionError("unverified compiler invoked")

    registry._compilers[key] = substituted, closure, proof
    with pytest.raises(RuntimeProviderError, match="runtime_provider_unavailable"):
        compile_language_source(source, registry)
    registry._compilers[key] = selected, closure, proof


def test_native_instance_shadow_and_health_revocation_fail_before_callback() -> None:
    registry, _ = _installed_compiler(_PROFILE)
    module = sys.modules["test_e_source_provider"]
    descriptor = json.loads((_PROFILE / "guard-descriptor.json").read_text())
    closure = SourceClosure(
        _PROFILE,
        ("provider/test_provider.py", "provider/test_provider.rs"),
        "provider-closure.json",
        _DOMAIN,
        "provider/test_provider.py",
    )
    healthy = True
    registry.register(
        descriptor,
        module.Provider,
        closure,
        health_check=lambda _provider: "healthy" if healthy else "unavailable",
    )
    binding = descriptor["binding"]
    active = registry.resolve("guard", binding)
    healthy = False
    with pytest.raises(RuntimeProviderError, match="runtime_provider_unavailable"):
        registry.invoke_guard(binding, {"event": {"payload": ["map", []]}})
    healthy = True
    invoked = False

    def substituted(*_args: object, **_kwargs: object) -> bool:
        nonlocal invoked
        invoked = True
        return True

    active.provider.evaluate_guard = substituted
    with pytest.raises(RuntimeProviderError, match="runtime_provider_unavailable"):
        registry.invoke_guard(binding, {"event": {"payload": ["map", []]}})
    assert not invoked


def test_native_restore_rechecks_loaded_source_after_creation(tmp_path: Path) -> None:
    shutil.copytree(_PROFILE, tmp_path / "fixture")
    root = tmp_path / "fixture"
    registry, _ = _installed_compiler(root)
    module = sys.modules["test_e_source_provider"]
    closure = SourceClosure(
        root,
        ("provider/test_provider.py", "provider/test_provider.rs"),
        "provider-closure.json",
        _DOMAIN,
        "provider/test_provider.py",
    )
    for name in ("guard-descriptor.json", "actions-descriptor.json"):
        registry.register(json.loads((root / name).read_text()), module.Provider, closure)
    bundle = load_bundle(
        yaml.safe_load((root / "machine.yaml").read_text()), runtime_providers=registry
    )
    aggregate = create(bundle, "order", "restore-root", "restore-create", {})["state"]
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    restore_aggregate(aggregate, resolver)
    path = root / "provider/test_provider.py"
    path.write_bytes(path.read_bytes() + b"\n# changed since creation\n")
    with pytest.raises(RuntimeProviderError, match="runtime_provider_unavailable"):
        restore_aggregate(aggregate, resolver)


def test_required_native_guarantee_is_rechecked_before_creation() -> None:
    registry, _ = _installed_compiler(_PROFILE)
    module = sys.modules["test_e_source_provider"]
    closure = SourceClosure(
        _PROFILE,
        ("provider/test_provider.py", "provider/test_provider.rs"),
        "provider-closure.json",
        _DOMAIN,
        "provider/test_provider.py",
    )
    proven = True

    def proof(_provider: object, _binding: object) -> frozenset[str]:
        return frozenset({"pure"}) if proven else frozenset()

    for name in ("safe-guard-descriptor.json", "safe-actions-descriptor.json"):
        registry.register(
            json.loads((_PROFILE / name).read_text()),
            module.Provider,
            closure,
            capability_proof=proof,
        )
    bundle = load_bundle(
        yaml.safe_load((_PROFILE / "machine-safe.yaml").read_text()),
        runtime_providers=registry,
        required_capabilities=frozenset({"pure"}),
    )
    proven = False
    with pytest.raises(RuntimeProviderError, match="extension_capability_mismatch"):
        create(bundle, "order", "policy-root", "policy-create", {})


def test_compiler_capability_proof_is_current() -> None:
    _unused_registry, source = _installed_compiler(_PROFILE)
    module = sys.modules["test_e_source_provider"]
    reference = source["content"]["regions"][0]["provider_reference"]
    closure = SourceClosure(
        _PROFILE,
        ("provider/test_provider.py", "provider/test_provider.rs"),
        "provider-closure.json",
        _DOMAIN,
        "provider/test_provider.py",
    )
    granted = True
    registry = RuntimeProviderRegistry()
    registry.register_dependency(source["content"]["dependencies"][0], closure)
    registry.register_compiler(
        reference,
        module.compile_region,
        closure,
        capability_proof=lambda selected: (
            frozenset({"pure"}) if granted and selected is module.compile_region else frozenset()
        ),
    )
    assert registry.compiler_capabilities(reference)["pure"]
    granted = False
    assert not registry.compiler_capabilities(reference)["pure"]
