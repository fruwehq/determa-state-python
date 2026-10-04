"""Implementation adapter for the pinned optional extension negotiation profile."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

from determa.state.codes import ExtensionNegotiationFailureCode as Code
from determa.state.extensions import (
    ExtensionError,
    ExtensionRegistry,
)


def _decision(call: Any) -> dict[str, Any]:
    try:
        return {"status": "accepted", "report": call()}
    except ExtensionError as exc:
        return {"status": "rejected", "code": exc.code}


def _closure(request: dict[str, Any]) -> tuple[Path, str, Any]:
    profile = Path(request["profile_path"]).resolve()
    declared = request["provider_closure"]
    files = declared["closure_files"]
    if files != ["provider/test_provider.py", "provider/test_provider.rs"]:
        raise ValueError("unexpected provider closure")
    digest = hashlib.sha256(b"determa-test-provider-closure-1\0")
    for name in files:
        content = (profile / name).read_bytes()
        encoded = name.encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    observed = "sha256:" + digest.hexdigest()
    if observed != declared["content_digest"]:
        raise ValueError("provider closure changed")
    source = profile / files[0]
    spec = importlib.util.spec_from_file_location("determa_conformance_extension_provider", source)
    if spec is None or spec.loader is None:
        raise ValueError("provider could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.dont_write_bytecode = True
    spec.loader.exec_module(module)
    if Path(module.__file__).resolve() != source:
        raise ValueError("loaded provider path mismatch")
    # Verify again after module execution: registration cannot use changed source.
    second = hashlib.sha256(b"determa-test-provider-closure-1\0")
    for name in files:
        content = (profile / name).read_bytes()
        encoded = name.encode()
        second.update(len(encoded).to_bytes(8, "big"))
        second.update(encoded)
        second.update(len(content).to_bytes(8, "big"))
        second.update(content)
    if "sha256:" + second.hexdigest() != observed:
        raise ValueError("provider source changed while loading")
    return source, observed, module


class _FixtureProvider:
    def __init__(self, module: Any) -> None:
        self.module = module

    def validate_configuration(self, configuration: dict[str, Any]) -> Any:
        return self.module.validate_configuration(configuration)

    def capabilities(self, instance: Any) -> list[str]:
        return self.module.capabilities(instance)

    def health(self, instance: Any) -> str:
        return self.module.health(instance)


def _public(
    value: dict[str, Any], module: Any, closure_request: dict[str, Any], closure_digest: str
) -> dict[str, Any]:
    def verify(provider: Any, descriptor: Any) -> bool:
        if not isinstance(provider, _FixtureProvider) or provider.module is not module:
            return False
        if descriptor["provider_reference"]["content_digest"] != closure_digest:
            return False
        return _closure(closure_request)[1] == closure_digest

    registry = ExtensionRegistry(source_verifier=verify)
    provider = _FixtureProvider(module)
    trace: list[dict[str, Any]] = []
    descriptor = value["registration"]
    for raw in value["registration_bytes"]:
        operation = value["installation"]
        try:
            parsed = json.loads(raw)
            if operation == "register":
                registry.register_bytes(raw.encode("utf-8"), lambda: provider)
            else:
                registry.inject(parsed, provider)
            output: dict[str, Any] = {"status": "accepted", "value": None}
        except ExtensionError as exc:
            output = {"status": "rejected", "code": exc.code}
        trace.append({"operation": operation, "input": raw, "output": output})
        if output["status"] == "rejected":
            return {"decision": output, "stages": trace}
    if not value["registration_bytes"]:
        requirement = value["requirement"]
        try:
            if requirement is None:
                raise ExtensionError(Code.UNKNOWN_EXTENSION)
            registry._entry(requirement["category"], requirement["provider_reference"])
            raise ExtensionError(Code.UNKNOWN_EXTENSION)
        except ExtensionError as exc:
            result = {"status": "rejected", "code": exc.code}
        trace.append(
            {
                "operation": "negotiate",
                "input": {"requirement": requirement, "lookup_uri": value["lookup_uri"]},
                "output": result,
            }
        )
        return {"decision": result, "stages": trace}
    configuration = value["configuration"]
    try:
        configured_provider, instance = registry.validate_configuration(descriptor, configuration)
        output = {"status": "accepted", "value": None}
    except ExtensionError as exc:
        output = {"status": "rejected", "code": exc.code}
    trace.append({"operation": "validate_configuration", "input": configuration, "output": output})
    if output["status"] == "rejected":
        return {"decision": output, "stages": trace}
    claims = registry.capabilities(configured_provider, instance)
    trace.append({"operation": "capabilities", "input": instance["instance_id"], "output": claims})
    health = registry.health(configured_provider, instance)
    trace.append({"operation": "health", "input": instance["instance_id"], "output": health})
    result = _decision(
        lambda: registry.negotiate_observed(
            descriptor,
            configuration,
            configured_provider,
            instance,
            claims,
            health,
            value["requirement"],
        )
    )
    trace.append(
        {
            "operation": "negotiate",
            "input": {"requirement": value["requirement"], "lookup_uri": value["lookup_uri"]},
            "output": result,
        }
    )
    return {"decision": result, "stages": trace}


class _HypotheticalProvider:
    """Private seam: expose only the vector's configured provider observations."""

    def validate_configuration(self, configuration: dict[str, Any]) -> dict[str, Any]:
        if set(configuration) != {"instance_id", "claims", "health"}:
            raise ValueError("invalid_extension_configuration")
        return configuration

    def capabilities(self, instance: dict[str, Any]) -> list[str]:
        return instance["claims"]

    def health(self, instance: dict[str, Any]) -> str:
        return instance["health"]


def _common(value: dict[str, Any]) -> dict[str, Any]:
    """Run production profile decisions with private, stipulated proof premises."""
    premise = value["hypothetical_verification"]
    proofs = premise["proofs"]
    provider = _HypotheticalProvider()
    registry = ExtensionRegistry(
        source_verifier=lambda candidate, _descriptor: candidate is provider
    )

    def proof_for(
        descriptor: dict[str, Any], instance: Any, _health: str, _claims: list[str]
    ) -> frozenset[str]:
        configuration = instance
        reference = descriptor["provider_reference"]
        candidate = next(
            (
                item
                for item in proofs
                if item["category"] == descriptor["category"]
                and item["provider_reference"]["identifier"] == reference["identifier"]
                and item["instance_id"] == configuration["instance_id"]
            ),
            None,
        )
        if candidate is None:
            return frozenset()
        if candidate["provider_reference"] != reference:
            raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        digest = (
            "sha256:"
            + hashlib.sha256(
                json.dumps(configuration, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
        )
        return (
            frozenset(candidate["claims"])
            if candidate["configuration_digest"] == digest
            else frozenset()
        )

    try:
        for descriptor in value["registrations"]:
            registry.register(descriptor, lambda: provider)
            registry._set_operational_evaluator(
                descriptor,
                lambda instance, health, claims, descriptor=descriptor: proof_for(
                    descriptor, instance, health, claims
                ),
            )
        operation = value["operation"]
        return registry.evaluate_profile(
            value["configurations"],
            value["requirements"],
            compose=operation["kind"] == "compose",
            host_guarantees=premise["host_guarantees"],
            projection=operation["projection"],
            requested_profile=operation["requested_profile"],
            weak_profile_opt_in=premise["weak_profile_opt_in"],
        )
    except ExtensionError as exc:
        return {"status": "rejected", "code": exc.code}


def main() -> None:
    request = json.load(sys.stdin)
    source, digest, module = _closure(request)
    if request["mode"] == "public_api":
        result = _public(request["input"], module, request, digest)
        output = {
            **result,
            "core_mutations": 0,
            "loaded_source": "provider/test_provider.py",
            "loaded_closure_digest": digest,
        }
    else:
        output = {"decision": _common(request["input"]), "core_mutations": 0}
    print(json.dumps(output, separators=(",", ":")))


if __name__ == "__main__":
    main()
