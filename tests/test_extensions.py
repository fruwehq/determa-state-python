"""Public extension identity, source closure, and current configured proof."""

from __future__ import annotations

import copy

import pytest

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
