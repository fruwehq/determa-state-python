"""Exact public extension identity and configured capability negotiation.

A provider's own claims are candidates. A host evaluator supplies operational proof;
without one, a registered extension remains usable as a weak extension with no claims.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable, Mapping, Sequence
from functools import cache
from pathlib import Path
from typing import Any, Protocol

from .codes import ExtensionNegotiationFailureCode as Code
from .errors import DetermaError

_DATA = Path(__file__).parent / "data"


@cache
def _schema(name: str) -> Any:
    from jsonschema import Draft202012Validator

    return Draft202012Validator(json.loads((_DATA / f"{name}-v1.schema.json").read_text()))


_GUARANTEES = frozenset(
    ("deterministic", "pure", "portable", "semantically_introspectable", "process_contained")
)


class ExtensionError(DetermaError):
    """Closed public extension negotiation failure."""

    def __init__(self, code: Code | str) -> None:
        self.code = str(code)
        super().__init__(self.code)


class ExtensionProvider(Protocol):
    """Configured provider operations; factories may return any object implementing these."""

    def validate_configuration(self, configuration: Mapping[str, Any]) -> Any: ...
    def capabilities(self, instance: Any) -> Sequence[str]: ...
    def health(self, instance: Any) -> str: ...


ExtensionFactory = Callable[[], ExtensionProvider]
# Operational evaluators are host-owned, category-specific code. They are never provider
# report fields or flags supplied by a caller's JSON configuration.
OperationalEvaluator = Callable[[Any, str, Sequence[str]], frozenset[str]]


def _check(name: str, document: Any, code: Code) -> dict[str, Any]:
    if not isinstance(document, dict) or not _schema(name).is_valid(document):
        raise ExtensionError(code)
    return copy.deepcopy(document)


def _reference_key(descriptor: Mapping[str, Any]) -> tuple[str, str, str]:
    reference = descriptor["provider_reference"]
    return descriptor["category"], reference["identifier"], reference["version"]


def _validate_configuration(configuration: Any) -> dict[str, Any]:
    if not isinstance(configuration, dict):
        raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)
    try:
        # JSON configurations cannot carry duplicate keys, non-finite numbers, or
        # implementation-specific object types across the public boundary.
        result = json.loads(json.dumps(configuration, allow_nan=False))
        if not isinstance(result, dict):
            raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)
        return result
    except (TypeError, ValueError, OverflowError) as exc:
        raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION) from exc


class ExtensionRegistry:
    """Explicit, initially empty public registry with exact-reference lookup."""

    def __init__(
        self,
        *,
        source_verifier: Callable[[ExtensionProvider, Mapping[str, Any]], bool] | None = None,
    ) -> None:
        self._source_verifier = source_verifier
        self._entries: dict[
            tuple[str, str, str],
            tuple[dict[str, Any], ExtensionFactory, OperationalEvaluator | None],
        ] = {}

    def register(
        self,
        descriptor: Mapping[str, Any],
        factory: ExtensionFactory,
    ) -> None:
        validated = _check("extension-descriptor", descriptor, Code.INVALID_EXTENSION_DESCRIPTOR)
        key = _reference_key(validated)
        if key in self._entries:
            raise ExtensionError(Code.DUPLICATE_EXTENSION_REGISTRATION)
        self._entries[key] = validated, factory, None

    def register_bytes(
        self,
        descriptor_bytes: bytes,
        factory: ExtensionFactory,
    ) -> None:
        """Register strict UTF-8 JSON bytes, rejecting duplicate keys and nonfinite numbers."""

        def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate member")
                result[key] = value
            return result

        def invalid_constant(value: str) -> Any:
            raise ValueError(f"nonfinite JSON constant: {value}")

        try:
            parsed = json.loads(
                descriptor_bytes.decode("utf-8"),
                object_pairs_hook=unique,
                parse_constant=invalid_constant,
            )
        except (UnicodeDecodeError, ValueError, TypeError) as exc:
            raise ExtensionError(Code.INVALID_EXTENSION_DESCRIPTOR) from exc
        self.register(parsed, factory)

    def inject(
        self,
        descriptor: Mapping[str, Any],
        provider: ExtensionProvider,
    ) -> None:
        """Use exactly the registration validation path for a directly injected object."""
        self.register(descriptor, lambda: provider)

    def _set_operational_evaluator(
        self, descriptor: Mapping[str, Any], evaluator: OperationalEvaluator
    ) -> None:
        """Install category proof from trusted host code, never provider configuration."""
        checked, factory, _ = self._entry(descriptor["category"], descriptor["provider_reference"])
        if checked != descriptor:
            raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        self._entries[_reference_key(checked)] = checked, factory, evaluator

    def _entry(
        self, category: str, reference: Mapping[str, Any]
    ) -> tuple[dict[str, Any], ExtensionFactory, OperationalEvaluator | None]:
        validated = _check("provider-reference", reference, Code.INVALID_EXTENSION_DESCRIPTOR)
        entry = self._entries.get((category, validated["identifier"], validated["version"]))
        if entry is not None:
            if entry[0]["provider_reference"] != validated:
                raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
            return entry
        if any(k[:2] == (category, validated["identifier"]) for k in self._entries):
            raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        raise ExtensionError(Code.UNKNOWN_EXTENSION)

    def validate_configuration(
        self, descriptor: Mapping[str, Any], configuration: Mapping[str, Any]
    ) -> tuple[ExtensionProvider, Any]:
        checked = _check(
            "extension-descriptor", dict(descriptor), Code.INVALID_EXTENSION_DESCRIPTOR
        )
        registered, factory, _ = self._entry(checked["category"], checked["provider_reference"])
        if registered != checked:
            raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        clean = _validate_configuration(configuration)
        try:
            provider = factory()
            if self._source_verifier is None or not self._source_verifier(provider, registered):
                raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
            instance = provider.validate_configuration(clean)
        except (TypeError, ValueError, KeyError) as exc:
            raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION) from exc
        return provider, instance

    def capabilities(self, provider: ExtensionProvider, instance: Any) -> list[str]:
        return list(provider.capabilities(instance))

    def health(self, provider: ExtensionProvider, instance: Any) -> str:
        return provider.health(instance)

    def negotiate(
        self,
        descriptor: Mapping[str, Any],
        configuration: Mapping[str, Any],
        requirement: Mapping[str, Any] | None = None,
        *,
        lookup_uri: str | None = None,
    ) -> dict[str, Any]:
        """Evaluate current configured instance before any core or host mutation."""
        del lookup_uri  # URI is only a resolver hint; it cannot change exact identity.
        provider, instance = self.validate_configuration(descriptor, configuration)
        claims = self.capabilities(provider, instance)
        health = self.health(provider, instance)
        return self.negotiate_observed(
            descriptor, configuration, provider, instance, claims, health, requirement
        )

    def negotiate_observed(
        self,
        descriptor: Mapping[str, Any],
        configuration: Mapping[str, Any],
        provider: ExtensionProvider,
        instance: Any,
        claims: Sequence[str],
        health: str,
        requirement: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Decide from already observed provider calls; useful for host call tracing."""
        checked = _check(
            "extension-descriptor", dict(descriptor), Code.INVALID_EXTENSION_DESCRIPTOR
        )
        registered, _, evaluator = self._entry(checked["category"], checked["provider_reference"])
        if (
            registered != checked
            or self._source_verifier is None
            or not self._source_verifier(provider, registered)
        ):
            raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        clean = _validate_configuration(configuration)
        if not isinstance(clean.get("instance_id"), str):
            raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)
        candidate = {
            "category": checked["category"],
            "provider_reference": checked["provider_reference"],
            "instance_id": clean["instance_id"],
            "health": health,
            "claims": list(claims),
        }
        report = _check(
            "extension-capability-report", candidate, Code.INVALID_EXTENSION_CONFIGURATION
        )
        if not set(report["claims"]).issubset(checked["supported_capabilities"]):
            raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)
        proven = evaluator(instance, health, claims) if evaluator is not None else frozenset()
        report["claims"] = [claim for claim in claims if claim in proven and health == "healthy"]
        if requirement is not None:
            requested = _check(
                "extension-capability-requirement",
                dict(requirement),
                Code.INVALID_EXTENSION_DESCRIPTOR,
            )
            target, _, _ = self._entry(requested["category"], requested["provider_reference"])
            if target != checked:
                raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
            if (
                requested["instance_id"] != report["instance_id"]
                or requested["capability"] not in report["claims"]
            ):
                raise ExtensionError(Code.EXTENSION_CAPABILITY_MISMATCH)
        return report

    def evaluate_profile(
        self,
        configurations: Sequence[Mapping[str, Any]],
        requirements: Sequence[Mapping[str, Any]],
        *,
        compose: bool = False,
        host_guarantees: Mapping[str, bool] | None = None,
        projection: Sequence[str] = (),
        requested_profile: str | None = None,
        weak_profile_opt_in: bool = False,
    ) -> dict[str, Any]:
        """Resolve a complete profile before any aggregate or checkpoint operation."""
        for requirement in requirements:
            _check(
                "extension-capability-requirement", requirement, Code.INVALID_EXTENSION_DESCRIPTOR
            )
        reports: list[dict[str, Any]] = []
        for participant in configurations:
            if set(participant) != {"category", "provider_reference", "configuration"}:
                raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)
            descriptor, _, _ = self._entry(
                participant["category"], participant["provider_reference"]
            )
            reports.append(self.negotiate(descriptor, participant["configuration"]))
        for requirement in requirements:
            self._entry(requirement["category"], requirement["provider_reference"])
            if not any(
                report["category"] == requirement["category"]
                and report["provider_reference"] == requirement["provider_reference"]
                and report["instance_id"] == requirement["instance_id"]
                and requirement["capability"] in report["claims"]
                for report in reports
            ):
                raise ExtensionError(Code.EXTENSION_CAPABILITY_MISMATCH)
        effective: dict[str, bool] = {}
        if compose:
            full = compose_capabilities(
                reports, host_guarantees or {}, weak_profile_opt_in=weak_profile_opt_in
            )
            effective = {name: full[name] for name in projection}
            if (
                requested_profile == "automatic_retry_without_external_io"
                and full["external_io_capable"]
            ):
                raise ExtensionError(Code.EXTENSION_CAPABILITY_MISMATCH)
        return {"status": "accepted", "reports": reports, "effective": effective}


def compose_capabilities(
    reports: Sequence[Mapping[str, Any]],
    host_guarantees: Mapping[str, bool],
    *,
    weak_profile_opt_in: bool = False,
) -> dict[str, bool]:
    """Compose all-party guarantees and any-party external I/O hazard."""
    effective = {
        guarantee: bool(host_guarantees.get(guarantee, False))
        and all(
            report.get("health") == "healthy" and guarantee in report.get("claims", ())
            for report in reports
        )
        for guarantee in _GUARANTEES
    }
    hazard = any(
        report.get("health") != "healthy"
        or (
            "external_io_capable" in report.get("claims", ())
            or "pure" not in report.get("claims", ())
        )
        for report in reports
    )
    effective["external_io_capable"] = hazard
    effective["weak_profile_opt_in_required"] = hazard or not all(effective[g] for g in _GUARANTEES)
    if effective["weak_profile_opt_in_required"] and not weak_profile_opt_in:
        # Composition is still reportable; the caller checks its requested profile.
        pass
    return effective


class _BundledStoreProvider:
    """Ordinary public extension wrapper over an execution-store factory."""

    def __init__(self, factory: Callable[[str, Mapping[str, Any]], Any]) -> None:
        self.factory = factory

    def validate_configuration(self, configuration: Mapping[str, Any]) -> Any:
        from .stores.base import ExecutionStoreError

        if (
            set(configuration) != {"instance_id", "uri", "store_configuration"}
            or not isinstance(configuration["instance_id"], str)
            or not isinstance(configuration["uri"], str)
            or not isinstance(configuration["store_configuration"], dict)
        ):
            raise ValueError("invalid_extension_configuration")
        try:
            store = self.factory(configuration["uri"], configuration["store_configuration"])
        except ExecutionStoreError as exc:
            raise ValueError("invalid_extension_configuration") from exc
        return {"instance_id": configuration["instance_id"], "store": store}

    def capabilities(self, instance: Any) -> list[str]:
        return sorted(instance["store"].capabilities)

    def health(self, instance: Any) -> str:
        return "healthy" if instance["store"].health().get("healthy") is True else "unavailable"


def _bundled_source_digest() -> str:
    """Hash the installed package source closure, including schema and store dependencies."""
    import hashlib

    package = Path(__file__).parent
    files = sorted(
        path for path in package.rglob("*") if path.is_file() and path.suffix in {".py", ".json"}
    )
    digest = hashlib.sha256(b"determa-python-extension-closure-1\0")
    for path in files:
        name = path.relative_to(package).as_posix().encode("utf-8")
        content = path.read_bytes()
        digest.update(len(name).to_bytes(8, "big"))
        digest.update(name)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return "sha256:" + digest.hexdigest()


def bundled_extension_registry(*, include_postgresql: bool = True) -> ExtensionRegistry:
    """Register bundled stores through the same public operation as custom providers.

    Claims requiring a category-specific proof are withheld until such proof is
    installed by a host. Memory's `ephemeral` only states that loss is permitted.
    """
    from .__about__ import __version__
    from .stores import (
        file_execution_store_factory,
        memory_execution_store_factory,
        postgresql_execution_store_factory,
        sqlite_execution_store_factory,
    )
    from .stores.base import STANDARD_CAPABILITIES

    digest = _bundled_source_digest()

    def verify(provider: ExtensionProvider, descriptor: Mapping[str, Any]) -> bool:
        return (
            isinstance(provider, _BundledStoreProvider)
            and descriptor["provider_reference"]["content_digest"] == _bundled_source_digest()
            and provider.factory.__module__.startswith("determa.state.stores.")
        )

    registry = ExtensionRegistry(source_verifier=verify)
    stores = [
        ("memory", memory_execution_store_factory),
        ("file", file_execution_store_factory),
        ("sqlite", sqlite_execution_store_factory),
    ]
    if include_postgresql:
        stores.append(("postgresql", postgresql_execution_store_factory))
    for name, factory in stores:
        descriptor = {
            "category": "execution_store",
            "provider_reference": {
                "identifier": f"determa.store.{name}",
                "version": __version__,
                "content_digest": digest,
            },
            "interface_version": 1,
            "supported_capabilities": sorted(STANDARD_CAPABILITIES),
        }
        evaluator: OperationalEvaluator | None = None
        if name == "memory":

            def memory_proof(_instance: Any, health: str, claims: Sequence[str]) -> frozenset[str]:
                if health == "healthy" and "ephemeral" in claims:
                    return frozenset({"ephemeral"})
                return frozenset()

            evaluator = memory_proof

        def make_provider(
            selected_factory: Callable[[str, Mapping[str, Any]], Any] = factory,
        ) -> ExtensionProvider:
            return _BundledStoreProvider(selected_factory)

        registry.register(descriptor, make_provider)
        if evaluator is not None:
            registry._set_operational_evaluator(descriptor, evaluator)
    return registry
