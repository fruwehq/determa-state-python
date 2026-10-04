"""Operational source and compiler boundaries for optional native slots."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import shutil
import sys
from pathlib import Path

import pytest
import yaml

from conformance.harness import conformance_root
from conformance.provider_fixture import installed_inspection_bundle
from determa.state import (
    MemoryArtifactResolver,
    RuntimeProviderError,
    RuntimeProviderRegistry,
    SourceClosure,
    admit,
    compile_language_source,
    create,
    delivery_request_digest,
    inspect_candidate,
    load_bundle,
    restore_aggregate,
    step,
)
from determa.state.wire import hash_value, typed_value

_PROFILE = conformance_root() / "conformance/profiles/runtime-provider/provider-01-exact-source"
_DOMAIN = b"determa-test-runtime-provider-closure-1\0"


def _modified_runtime_bundle(
    tmp_path: Path, source_transform: str | None = None, *, local_destination: bool = False
) -> tuple[object, RuntimeProviderRegistry]:
    root = tmp_path / "fixture"
    shutil.copytree(_PROFILE, root)
    provider_path = root / "provider/test_provider.py"
    if source_transform is not None:
        source = provider_path.read_text()
        source = source.replace('["string", "provider-correlation"]', source_transform)
        source = source.replace('"to": {"external": True}', '"to": {"self": True}')
        provider_path.write_text(source)
    manifest_path = root / "provider-closure.json"
    manifest = json.loads(manifest_path.read_text())
    for entry in manifest["files"]:
        entry["sha256"] = (
            "sha256:" + hashlib.sha256((root / entry["path"]).read_bytes()).hexdigest()
        )
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")))
    closure = SourceClosure(
        root,
        ("provider/test_provider.py", "provider/test_provider.rs"),
        "provider-closure.json",
        _DOMAIN,
        "provider/test_provider.py",
    )
    old = json.loads((_PROFILE / "guard-descriptor.json").read_text())["binding"]
    old_digest = old["provider_reference"]["content_digest"]
    old_source = old["source_digest"]
    for name in ("guard-descriptor.json", "actions-descriptor.json", "machine.yaml"):
        path = root / name
        path.write_text(
            path.read_text()
            .replace(old_digest, closure.digest())
            .replace(old_source, closure.manifest_digest())
        )
    spec = importlib.util.spec_from_file_location(
        f"test_e_modified_{tmp_path.name.replace('-', '_')}", provider_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    registry = RuntimeProviderRegistry()
    guard = json.loads((root / "guard-descriptor.json").read_text())
    actions = json.loads((root / "actions-descriptor.json").read_text())
    registry.register_dependency(guard["binding"]["dependencies"][0], closure)
    registry.register(
        guard,
        module.Provider,
        closure,
        evaluation_options={"guard_override": True} if source_transform is not None else None,
    )
    registry.register(actions, module.Provider, closure)
    document = yaml.safe_load((root / "machine.yaml").read_text())
    if source_transform is not None:
        document["events"]["accepted"]["direction"] = "internal"
        pending = document["machines"][0]["root"]["states"]["pending"]
        pending["on_events"]["submit"][1]["action"][1]["send"]["to"] = {"self": True}
    if local_destination:
        composite = document["machines"][0]["root"]
        pending = composite["states"]["pending"]
        composite["states"]["complete"] = {"type": "final"}
        pending["variables"] = composite.pop("variables")
        pending["on_events"]["submit"] = {
            "action": [{"provider_actions": actions["binding"]}],
            "transition_to": "complete",
        }
    return load_bundle(document, runtime_providers=registry), registry


def _step_modified(bundle: object, *, root_id: str) -> dict:
    created = create(bundle, "order", root_id, "modified-create", {})
    aggregate = created["state"]
    runtime = next(
        item for item in aggregate["runtimes"] if item["runtime_id"] == aggregate["root_runtime_id"]
    )
    envelope = {
        "event": "submit",
        "event_id": "modified-submit",
        "cause_id": "modified-submit",
        "source": {"host": True},
        "target": runtime["target_identity"],
        "payload": ["map", [["approved", ["boolean", False]]]],
    }
    delivery = {
        "delivery_mode": "input",
        "envelope": envelope,
        "envelope_digest": delivery_request_digest(root_id, "input", envelope),
    }
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    admitted = admit(aggregate, [delivery], resolver)
    assert admitted["state"] is not None
    return step(admitted["state"], runtime["runtime_id"], resolver)


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


def test_compiler_manifest_drift_refused_at_use(tmp_path: Path) -> None:
    shutil.copytree(_PROFILE, tmp_path / "fixture")
    root = tmp_path / "fixture"
    registry, source = _installed_compiler(root)
    manifest = root / "provider-closure.json"
    manifest.write_bytes(manifest.read_bytes() + b" ")
    with pytest.raises(RuntimeProviderError, match="runtime_provider_unavailable"):
        compile_language_source(source, registry)


@pytest.mark.parametrize(
    "locator",
    [
        "/machines/00/root/states/pending/on_events/submit/guard",
        "/machines/~2/root/states/pending/on_events/submit/guard",
    ],
)
def test_noncanonical_compilation_locator_rejected_before_compiler(locator: str) -> None:
    registry, source = _installed_compiler(_PROFILE)
    source["content"]["regions"].append({**source["content"]["regions"][0], "locator": locator})
    source["artifact_digest"] = hash_value(
        ["determa.language_source", "1", typed_value(source["content"])]
    )
    with pytest.raises(RuntimeProviderError, match="language_compilation_failed"):
        compile_language_source(source, registry, maximum_compilation_steps=0)


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


def test_native_new_class_hook_rejected_before_invocation_and_restore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, _ = _installed_compiler(_PROFILE)
    module = sys.modules["test_e_source_provider"]
    closure = SourceClosure(
        _PROFILE,
        ("provider/test_provider.py", "provider/test_provider.rs"),
        "provider-closure.json",
        _DOMAIN,
        "provider/test_provider.py",
    )
    for name in ("guard-descriptor.json", "actions-descriptor.json"):
        registry.register(json.loads((_PROFILE / name).read_text()), module.Provider, closure)
    bundle = load_bundle(
        yaml.safe_load((_PROFILE / "machine.yaml").read_text()), runtime_providers=registry
    )
    aggregate = create(bundle, "order", "hook-root", "hook-create", {})["state"]
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    binding = json.loads((_PROFILE / "guard-descriptor.json").read_text())["binding"]
    invoked = []
    original = module.Provider.__getattribute__

    def redirect(instance: object, name: str) -> object:
        if name == "evaluate_guard":
            invoked.append(name)
            return lambda *_args, **_kwargs: False
        return original(instance, name)

    monkeypatch.setattr(module.Provider, "__getattribute__", redirect)
    with pytest.raises(RuntimeProviderError, match="runtime_provider_unavailable"):
        registry.invoke_guard(binding, {"event": {"payload": ["map", []]}})
    with pytest.raises(RuntimeProviderError, match="runtime_provider_unavailable"):
        restore_aggregate(aggregate, resolver)
    assert invoked == []


def test_native_new_class_hook_rejected_during_inspection_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = conformance_root() / "conformance/profiles/inspection-provider/provider-01-exact-closure"
    bundle, _registry = installed_inspection_bundle(root)
    module = sys.modules["determa_conformance_inspection_provider"]
    requests = json.loads((root / "requests.json").read_text())
    aggregate = (root / "aggregate-before.json").read_bytes()
    invoked = []
    original = module.SafeGuard.__getattribute__

    def redirect(instance: object, name: str) -> object:
        if name in {"evaluate", "inspect_guard"}:
            invoked.append(name)
            return lambda *_args, **_kwargs: (False, 1, 2)
        return original(instance, name)

    monkeypatch.setattr(module.SafeGuard, "__getattribute__", redirect)
    with pytest.raises(RuntimeProviderError, match="runtime_provider_unavailable"):
        inspect_candidate(
            aggregate,
            requests["semantic_safe"],
            MemoryArtifactResolver(definitions={bundle.fingerprint: bundle}),
        )
    assert invoked == []


def test_typed_internal_correlation_must_be_nonempty_string(tmp_path: Path) -> None:
    bundle, _registry = _modified_runtime_bundle(tmp_path, '["integer", "7"]')
    result = _step_modified(bundle, root_id="typed-correlation-root")
    assert result["disposition"] == "faulted"
    assert result["fault"]["code"] == "action_fault"
    assert result["emissions"] == []


def test_native_write_to_exited_scope_faults_and_rolls_back(tmp_path: Path) -> None:
    bundle, registry = _modified_runtime_bundle(tmp_path, local_destination=True)
    result = _step_modified(bundle, root_id="exited-scope-root")
    assert result["disposition"] == "faulted"
    assert result["fault"]["code"] == "action_fault"
    assert result["emissions"] == []
    assert any(selected.provider.action_calls == 1 for selected in registry._active.values())


def test_semantic_and_ordinary_guards_see_same_native_event(tmp_path: Path) -> None:
    root = tmp_path / "inspection"
    original_root = (
        conformance_root() / "conformance/profiles/inspection-provider/provider-01-exact-closure"
    )
    shutil.copytree(original_root, root)
    source_path = root / "provider/test_provider.py"
    source = (
        source_path.read_text()
        .replace(
            "self.ordinary_calls += 1\n        return True",
            'self.ordinary_calls += 1\n        return "source" not in snapshot["event"] '
            'and "cause_id" not in snapshot["event"]',
        )
        .replace(
            "return True, 1, 2",
            'return "source" not in snapshot["event"] '
            'and "cause_id" not in snapshot["event"], 1, 2',
        )
    )
    source_path.write_text(source)
    manifest_path = root / "provider-closure.json"
    manifest = json.loads(manifest_path.read_text())
    for entry in manifest["files"]:
        entry["sha256"] = (
            "sha256:" + hashlib.sha256((root / entry["path"]).read_bytes()).hexdigest()
        )
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")))
    closure = SourceClosure(
        root,
        ("provider/test_provider.py", "provider/test_provider.rs"),
        "provider-closure.json",
        b"determa-test-inspection-provider-closure-1\0",
        "provider/test_provider.py",
    )
    machine_path = root / "machine.yaml"
    old = yaml.safe_load(machine_path.read_text())["machines"][0]["root"]["states"]["waiting"][
        "on_events"
    ]["safe"]["guard"]["provider"]
    machine_path.write_text(
        machine_path.read_text()
        .replace(old["provider_reference"]["content_digest"], closure.digest())
        .replace(old["source_digest"], closure.manifest_digest())
    )
    bundle, registry = installed_inspection_bundle(root)
    aggregate = create(bundle, "inspector", "same-snapshot-root", "same-snapshot-create", {})[
        "state"
    ]
    runtime = next(
        item for item in aggregate["runtimes"] if item["runtime_id"] == aggregate["root_runtime_id"]
    )
    envelope = {
        "event": "safe",
        "event_id": "same-snapshot-event",
        "cause_id": "same-snapshot-event",
        "source": {"host": True},
        "target": runtime["target_identity"],
        "payload": ["map", []],
    }
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    request = {
        "mode": "semantic",
        "aggregate_state_digest": aggregate["aggregate_state_digest"],
        "runtime_id": runtime["runtime_id"],
        "runtime_incarnation": runtime["identity_origin"],
        "envelope": envelope,
        "limits": {"maximum_guard_evaluations": "1", "maximum_evaluation_steps": "2"},
    }
    unchanged = json.dumps(aggregate, sort_keys=True)
    observed = inspect_candidate(aggregate, request, resolver)
    assert observed["disposition"] == "handled_now", observed.get("reason")
    assert json.dumps(aggregate, sort_keys=True) == unchanged
    assert next(iter(registry._active.values())).provider.ordinary_calls == 0
    delivery = {
        "delivery_mode": "input",
        "envelope": envelope,
        "envelope_digest": delivery_request_digest("same-snapshot-root", "input", envelope),
    }
    admitted = admit(aggregate, [delivery], resolver)
    assert admitted["state"] is not None
    result = step(admitted["state"], runtime["runtime_id"], resolver)
    assert result["disposition"] == "handled"


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
