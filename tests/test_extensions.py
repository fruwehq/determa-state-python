"""Public extension identity, source closure, and current configured proof."""

from __future__ import annotations

import copy
from dataclasses import replace

import pytest

import determa.state.codes as codes
import determa.state.extensions as extensions
import determa.state.stores as bundled_stores
import determa.state.stores.memory as memory_module
import determa.state.stores.sqlite as sqlite_module
from determa.state.extensions import (
    ExtensionError,
    ExtensionRegistry,
    bundled_extension_registry,
)


class Provider:
    def validate_configuration(self, configuration: dict[str, object]) -> dict[str, object]:
        return configuration

    def capabilities(self, instance: dict[str, object]) -> list[str]:
        return list(instance["claims"])  # type: ignore[arg-type]

    def health(self, instance: dict[str, object]) -> str:
        return str(instance["health"])


def descriptor() -> dict[str, object]:
    return {
        "category": "execution_store",
        "provider_reference": {
            "identifier": "example.store",
            "version": "1.0.0",
            "content_digest": "sha256:" + "a" * 64,
        },
        "interface_version": 1,
        "supported_capabilities": ["ephemeral"],
    }


def test_unverified_provider_is_not_executed() -> None:
    registry = ExtensionRegistry()
    factory_calls = 0

    def factory() -> Provider:
        nonlocal factory_calls
        factory_calls += 1
        return Provider()

    registry.register(descriptor(), factory)
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(
            descriptor(), {"instance_id": "primary", "claims": [], "health": "healthy"}
        )
    assert factory_calls == 0


def test_self_asserted_claim_and_changed_source_are_refused() -> None:
    trusted = True
    registry = ExtensionRegistry(source_verifier=lambda _provider, _descriptor: trusted)
    provider = Provider()
    record = descriptor()
    registry.inject(record, provider)
    configuration = {"instance_id": "primary", "claims": ["ephemeral"], "health": "healthy"}
    report = registry.negotiate(record, configuration)
    assert report["claims"] == []
    requirement = {
        "category": "execution_store",
        "provider_reference": record["provider_reference"],
        "instance_id": "primary",
        "capability": "ephemeral",
    }
    with pytest.raises(ExtensionError, match="extension_capability_mismatch"):
        registry.negotiate(record, configuration, requirement)
    trusted = False
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(record, configuration)


def test_strict_registration_and_duplicate_preserve_first() -> None:
    registry = ExtensionRegistry(source_verifier=lambda _provider, _descriptor: True)
    source = b'{"category":"execution_store","category":"execution_store"}'
    with pytest.raises(ExtensionError, match="invalid_extension_descriptor"):
        registry.register_bytes(source, Provider)
    registry.register(descriptor(), Provider)
    altered = copy.deepcopy(descriptor())
    altered["provider_reference"]["content_digest"] = "sha256:" + "b" * 64  # type: ignore[index]
    with pytest.raises(ExtensionError, match="duplicate_extension_registration"):
        registry.register(altered, Provider)
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(altered, {"instance_id": "primary", "claims": [], "health": "healthy"})


def test_bundled_store_uses_public_registration_and_current_health() -> None:
    registry = bundled_extension_registry(include_postgresql=False)
    descriptor = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    configuration = {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
    requirement = {
        "category": "execution_store",
        "provider_reference": descriptor["provider_reference"],
        "instance_id": "primary",
        "capability": "ephemeral",
    }
    assert registry.negotiate(descriptor, configuration, requirement)["claims"] == ["ephemeral"]
    with pytest.raises(ExtensionError, match="extension_capability_mismatch"):
        registry.negotiate(descriptor, configuration, {**requirement, "instance_id": "other"})


def test_composition_treats_missing_io_attestation_as_hazard() -> None:
    registry = ExtensionRegistry(source_verifier=lambda _source, _descriptor: True)
    record = descriptor()
    registry.inject(record, Provider())
    host = dict.fromkeys(
        ("pure", "deterministic", "portable", "semantically_introspectable", "process_contained"),
        True,
    )
    accepted = registry.evaluate_profile(
        [
            {
                "category": "execution_store",
                "provider_reference": record["provider_reference"],
                "configuration": {"instance_id": "primary", "claims": [], "health": "healthy"},
            }
        ],
        [],
        compose=True,
        host_guarantees=host,
        projection=("external_io_capable", "pure", "weak_profile_opt_in_required"),
        weak_profile_opt_in=True,
    )
    effective = accepted["effective"]
    assert effective["external_io_capable"] is True
    assert effective["pure"] is False
    assert effective["weak_profile_opt_in_required"] is True


def test_weak_composition_requires_explicit_policy_opt_in() -> None:
    registry = ExtensionRegistry(source_verifier=lambda _source, _descriptor: True)
    record = descriptor()
    registry.inject(record, Provider())
    configurations = [
        {
            "category": "execution_store",
            "provider_reference": record["provider_reference"],
            "configuration": {"instance_id": "primary", "claims": [], "health": "healthy"},
        }
    ]
    with pytest.raises(ExtensionError, match="extension_capability_mismatch"):
        registry.evaluate_profile(configurations, [], compose=True)
    accepted = registry.evaluate_profile(configurations, [], compose=True, weak_profile_opt_in=True)
    assert accepted["status"] == "accepted"


def test_source_order_claim_remains_unproved_on_public_path() -> None:
    registry = ExtensionRegistry(source_verifier=lambda _source, _descriptor: True)
    record = descriptor()
    record["category"] = "transport"
    record["supported_capabilities"] = ["source_ordered"]
    registry.inject(record, Provider())
    configuration = {
        "instance_id": "primary",
        "claims": ["source_ordered"],
        "health": "healthy",
    }
    assert registry.negotiate(record, configuration)["claims"] == []
    requirement = {
        "category": "transport",
        "provider_reference": record["provider_reference"],
        "instance_id": "primary",
        "capability": "source_ordered",
    }
    with pytest.raises(ExtensionError, match="extension_capability_mismatch"):
        registry.negotiate(record, configuration, requirement)


def test_unhealthy_participant_blocks_profile_without_requirement() -> None:
    registry = ExtensionRegistry(source_verifier=lambda _source, _descriptor: True)
    record = descriptor()
    registry.inject(record, Provider())
    participant = {
        "category": "execution_store",
        "provider_reference": record["provider_reference"],
        "configuration": {"instance_id": "primary", "claims": [], "health": "degraded"},
    }
    with pytest.raises(ExtensionError, match="extension_capability_mismatch"):
        registry.evaluate_profile([participant], [])


def test_public_calls_reobserve_current_health_and_bind_configured_instance() -> None:
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    configuration = {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
    requirement = {
        "category": "execution_store",
        "provider_reference": record["provider_reference"],
        "instance_id": "primary",
        "capability": "ephemeral",
    }
    configured = registry.validate_configuration(record, configuration)
    assert registry.capabilities(configured) == ["ephemeral"]
    assert registry.health(configured) == "healthy"
    configured._instance["store"].health = lambda: {"healthy": False}
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.health(configured)
    assert not hasattr(registry, "negotiate_observed")
    # A new negotiation configures a new store and observes that store afresh.
    assert registry.negotiate(record, configuration, requirement)["health"] == "healthy"
    configured._instance["instance_id"] = "other"
    with pytest.raises(ExtensionError, match="invalid_extension_configuration"):
        registry.capabilities(configured)

    custom = ExtensionRegistry(source_verifier=lambda _source, _descriptor: True)
    custom.inject(descriptor(), Provider())
    dynamic = custom.validate_configuration(
        descriptor(), {"instance_id": "primary", "claims": [], "health": "healthy"}
    )
    dynamic._instance["health"] = "unavailable"
    assert custom.health(dynamic) == "unavailable"


def test_public_calls_reject_unregistered_and_foreign_provider() -> None:
    calls = 0

    class EvilProvider:
        def capabilities(self, _instance: object) -> list[str]:
            nonlocal calls
            calls += 1
            return ["ephemeral"]

        def health(self, _instance: object) -> str:
            nonlocal calls
            calls += 1
            return "healthy"

    empty = ExtensionRegistry()
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        empty.capabilities(EvilProvider(), {})
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        empty.health(EvilProvider(), {})
    assert calls == 0
    first = ExtensionRegistry(source_verifier=lambda _source, _descriptor: True)
    first.inject(descriptor(), Provider())
    configured = first.validate_configuration(
        descriptor(), {"instance_id": "primary", "claims": [], "health": "healthy"}
    )
    second = ExtensionRegistry(source_verifier=lambda _source, _descriptor: True)
    second.inject(descriptor(), Provider())
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        second.health(configured)
    forged = replace(
        configured, _instance={"instance_id": "primary", "claims": [], "health": "healthy"}
    )
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        first.health(forged)


def test_bundled_factory_replacement_cannot_retain_source_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def replacement(_uri: str, _configuration: object) -> object:
        nonlocal calls
        calls += 1
        return object()

    replacement.__module__ = "determa.state.stores.memory"
    monkeypatch.setattr(bundled_stores, "memory_execution_store_factory", replacement)
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(
            record, {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
        )
    assert calls == 0


def test_bundled_factory_dependency_replacement_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    monkeypatch.setattr(memory_module, "MemoryExecutionStore", lambda: object())
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(
            record, {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
        )


def test_bundled_configured_objects_cannot_change_dispatch_targets() -> None:
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    configuration = {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
    configured = registry.validate_configuration(record, configuration)
    calls = 0

    class SubstituteStore:
        capabilities = frozenset({"ephemeral"})

        def health(self) -> dict[str, bool]:
            nonlocal calls
            calls += 1
            return {"healthy": True}

    configured._instance["store"] = SubstituteStore()
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.capabilities(configured)
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.health(configured)
    assert calls == 0

    configured = registry.validate_configuration(record, configuration)
    original_type = type(configured._provider)

    class SubstituteProvider(original_type):
        def capabilities(self, _instance: object) -> list[str]:
            nonlocal calls
            calls += 1
            return ["ephemeral"]

    configured._provider.__class__ = SubstituteProvider
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.capabilities(configured)
    assert calls == 0

    configured = registry.validate_configuration(record, configuration)
    configured._provider.capabilities = lambda _instance: ["ephemeral"]
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.capabilities(configured)


def test_bundled_wrapper_and_transaction_callbacks_match_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = extensions._BundledStoreProvider.capabilities
    calls = 0

    def delegated(self: object, instance: object) -> list[str]:
        nonlocal calls
        calls += 1
        return original(self, instance)

    monkeypatch.setattr(extensions._BundledStoreProvider, "capabilities", delegated)
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    configuration = {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(record, configuration)
    assert calls == 0

    monkeypatch.undo()
    monkeypatch.setattr(memory_module, "_MemoryTransaction", lambda *_args: object())
    registry = bundled_extension_registry(include_postgresql=False)
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(record, configuration)


def test_bundled_absolute_imports_match_installed_bindings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    original_rlock = memory_module.threading.RLock

    class SubstituteThreading:
        @staticmethod
        def RLock() -> object:
            nonlocal calls
            calls += 1
            return original_rlock()

    monkeypatch.setattr(memory_module, "threading", SubstituteThreading)
    registry = bundled_extension_registry(include_postgresql=False)
    memory_record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(
            memory_record, {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
        )
    assert calls == 0

    original_path = sqlite_module.Path

    def substitute_path(value: str) -> object:
        nonlocal calls
        calls += 1
        return original_path(value)

    monkeypatch.setattr(sqlite_module, "Path", substitute_path)
    registry = bundled_extension_registry(include_postgresql=False)
    sqlite_record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.sqlite"
    )
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(
            sqlite_record,
            {
                "instance_id": "primary",
                "uri": "sqlite:///tmp/b1-imports.sqlite",
                "store_configuration": {},
            },
        )
    assert calls == 0


def test_bundled_added_class_dispatch_hook_refuses_capability_and_health(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    configuration = {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
    configured = registry.validate_configuration(record, configuration)
    called = 0
    original = memory_module.MemoryExecutionStore.__getattribute__

    def substituted(self: object, name: str) -> object:
        nonlocal called
        if name == "health":
            called += 1
            return lambda: {"healthy": True}
        if name == "capabilities":
            called += 1
            return frozenset({"ephemeral"})
        return original(self, name)

    monkeypatch.setattr(memory_module.MemoryExecutionStore, "__getattribute__", substituted)
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.capabilities(configured)
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.health(configured)
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(record, configuration)
    assert called == 0


def test_bundled_rejects_rebound_stdlib_module_callable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    original = memory_module.threading.RLock
    called = 0

    def substituted() -> object:
        nonlocal called
        called += 1
        return original()

    with monkeypatch.context() as patch:
        patch.setattr(memory_module.threading, "RLock", substituted)
        with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
            registry.negotiate(
                record, {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
            )
    assert called == 0


def test_bundled_contextmanager_rejects_forged_wrapped_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = memory_module.MemoryExecutionStore.transaction
    called = 0

    def substituted(*args: object, **kwargs: object) -> object:
        nonlocal called
        called += 1
        return original(*args, **kwargs)

    substituted.__wrapped__ = original.__wrapped__  # type: ignore[attr-defined]
    monkeypatch.setattr(memory_module.MemoryExecutionStore, "transaction", substituted)
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(
            record, {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
        )
    assert called == 0


def test_bundled_rejects_transitive_stdlib_callback_before_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    original = memory_module.threading._CRLock  # type: ignore[attr-defined]
    called = 0

    def substituted(*args: object, **kwargs: object) -> object:
        nonlocal called
        called += 1
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(memory_module.threading, "_CRLock", substituted)
        with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
            registry.negotiate(
                record, {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
            )
    assert called == 0


def test_bundled_rejects_rebound_decorator_before_reference_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import contextlib
    import sys

    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    original = contextlib.contextmanager
    called = 0

    def substituted(*args: object, **kwargs: object) -> object:
        nonlocal called
        called += 1
        return original(*args, **kwargs)  # type: ignore[arg-type]

    with monkeypatch.context() as patch:
        patch.setattr(contextlib, "contextmanager", substituted)
        for name, module in tuple(sys.modules.items()):
            if name.startswith("determa.state.") and vars(module).get("contextmanager") is original:
                patch.setattr(module, "contextmanager", substituted)
        with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
            registry.negotiate(
                record, {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
            )
    assert called == 0


def test_bundled_rejects_changed_descriptor_but_allows_mutable_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    configuration = {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
    metrics: dict[str, int] = {}
    monkeypatch.setattr(memory_module.MemoryExecutionStore, "metrics", metrics, raising=False)
    assert registry.negotiate(record, configuration)["claims"] == ["ephemeral"]
    metrics["checks"] = 1
    assert registry.negotiate(record, configuration)["health"] == "healthy"

    original = memory_module.MemoryExecutionStore.capabilities

    class SubstituteProperty(property):
        def __get__(self, instance: object, owner: type | None = None) -> frozenset[str]:
            return frozenset({"ephemeral"})

    monkeypatch.setattr(
        memory_module.MemoryExecutionStore, "capabilities", SubstituteProperty(original.fget)
    )
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(record, configuration)


def test_bundled_rejects_changed_function_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    monkeypatch.setattr(memory_module.MemoryExecutionStore.__init__, "__defaults__", (object(),))
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        registry.negotiate(
            record, {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
        )


@pytest.mark.parametrize(
    ("target", "member"),
    [
        (extensions.ConfiguredExtension, "__init__"),
        (extensions._InstanceBinding, "__eq__"),
        (extensions.ExtensionProvider, "__subclasshook__"),
        (codes.ExtensionNegotiationFailureCode, "_generate_next_value_"),
    ],
)
@pytest.mark.parametrize("after_configuration", [False, True])
def test_bundled_rejects_changed_generated_methods(
    monkeypatch: pytest.MonkeyPatch,
    target: type,
    member: str,
    after_configuration: bool,
) -> None:
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    configuration = {"instance_id": "primary", "uri": "memory:", "store_configuration": {}}
    configured = (
        registry.validate_configuration(record, configuration) if after_configuration else None
    )
    called = 0

    def substituted(*_args: object, **_kwargs: object) -> object:
        nonlocal called
        called += 1
        return object()

    monkeypatch.setattr(target, member, substituted)
    with pytest.raises(ExtensionError, match="extension_identity_mismatch"):
        if configured is None:
            registry.negotiate(record, configuration)
        else:
            registry.health(configured)
    assert called == 0


def test_requested_profile_cannot_skip_composition() -> None:
    registry = bundled_extension_registry(include_postgresql=False)
    record = next(
        item
        for item in registry.descriptors
        if item["provider_reference"]["identifier"] == "determa.store.memory"
    )
    participants = [
        {
            "category": "execution_store",
            "provider_reference": record["provider_reference"],
            "configuration": {
                "instance_id": "primary",
                "uri": "memory:",
                "store_configuration": {},
            },
        }
    ]
    with pytest.raises(ExtensionError, match="invalid_extension_configuration"):
        registry.evaluate_profile(
            participants, [], requested_profile="automatic_retry_without_external_io"
        )
    with pytest.raises(ExtensionError, match="extension_capability_mismatch"):
        registry.evaluate_profile(
            participants,
            [],
            requested_profile="automatic_retry_without_external_io",
            compose=True,
            weak_profile_opt_in=True,
        )
    with pytest.raises(ExtensionError, match="extension_capability_mismatch"):
        registry.evaluate_profile(
            participants,
            [],
            requested_profile="deterministic",
            compose=True,
            weak_profile_opt_in=True,
            host_guarantees={"deterministic": True},
        )
