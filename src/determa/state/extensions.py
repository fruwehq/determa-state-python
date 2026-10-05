"""Exact public extension identity and configured capability negotiation.

A provider's own claims are candidates. A host evaluator supplies operational proof;
without one, a registered extension remains usable as a weak extension with no claims.
"""

from __future__ import annotations

import copy
import json
import re
import weakref
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar

from ._platform_bindings import PLATFORM_BINDINGS
from .codes import ExtensionNegotiationFailureCode as Code
from .errors import DetermaError

_CopyValue = TypeVar("_CopyValue")

_DATA = Path(__file__).parent / "data"
_INSTANCE_ID = re.compile(r"[a-z][a-z0-9.-]*\Z")


def _schema(name: str) -> Any:
    from jsonschema import Draft202012Validator

    if name not in _SCHEMA_CACHE:
        _SCHEMA_CACHE[name] = Draft202012Validator(
            json.loads((_DATA / f"{name}-v1.schema.json").read_text())
        )
    return _SCHEMA_CACHE[name]


_SCHEMA_CACHE: dict[str, Any] = {}
_INSTALLED_SOURCE_CACHE: dict[str, tuple[bytes, Any, Any]] = {}
_INSTALLED_CLASS_CACHE: dict[tuple[str, str], tuple[bytes, type]] = {}


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


@dataclass(frozen=True, slots=True, weakref_slot=True)
class ConfiguredExtension:
    """A validated configured instance bound to exactly one registry."""

    _token: object
    _descriptor: dict[str, Any]
    _configuration: dict[str, Any]
    _provider: ExtensionProvider
    _instance: Any


@dataclass(frozen=True, slots=True)
class _InstanceBinding:
    provider: ExtensionProvider
    provider_type: type
    instance: Any
    store: Any | None
    store_type: type | None


def _verified_copy(value: _CopyValue) -> _CopyValue:
    if not PLATFORM_BINDINGS.matches(copy, "deepcopy"):
        raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
    return copy.deepcopy(value)


def _check(name: str, document: Any, code: Code) -> dict[str, Any]:
    if type(document) is not dict or not _schema(name).is_valid(document):
        raise ExtensionError(code)
    return _verified_copy(document)


def _reference_key(descriptor: Mapping[str, Any]) -> tuple[str, str, str]:
    reference = descriptor["provider_reference"]
    return descriptor["category"], reference["identifier"], reference["version"]


def _validate_configuration(configuration: Any) -> dict[str, Any]:
    if type(configuration) is not dict:
        raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)

    def valid(value: Any) -> bool:
        if value is None or type(value) in (str, bool, int):
            return True
        if type(value) is float:
            import math

            return math.isfinite(value)
        if type(value) is list:
            return all(valid(item) for item in value)
        if type(value) is dict:
            return all(type(key) is str and valid(item) for key, item in value.items())
        return False

    if not valid(configuration):
        raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)
    return _verified_copy(configuration)


class ExtensionRegistry:
    """Explicit, initially empty public registry with exact-reference lookup."""

    def __init__(
        self,
        *,
        source_verifier: Callable[[Any, Mapping[str, Any]], bool] | None = None,
    ) -> None:
        self._source_verifier = source_verifier
        self._token = object()
        self._handles: weakref.WeakValueDictionary[int, ConfiguredExtension] = (
            weakref.WeakValueDictionary()
        )
        self._bindings: dict[int, _InstanceBinding] = {}
        self._entries: dict[
            tuple[str, str, str],
            tuple[dict[str, Any], ExtensionFactory, OperationalEvaluator | None],
        ] = {}

    @property
    def descriptors(self) -> tuple[dict[str, Any], ...]:
        """Discover exact registered descriptors without opening providers."""
        return tuple(_verified_copy(self._entries[key][0]) for key in sorted(self._entries))

    def _verified_source(self, provider: Any, descriptor: Mapping[str, Any]) -> bool:
        if self._source_verifier is None:
            return False
        try:
            return self._source_verifier(provider, descriptor) is True
        except Exception:
            return False

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
        if type(category) is not str:
            raise ExtensionError(Code.INVALID_EXTENSION_DESCRIPTOR)
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
    ) -> ConfiguredExtension:
        checked = _check("extension-descriptor", descriptor, Code.INVALID_EXTENSION_DESCRIPTOR)
        registered, factory, _ = self._entry(checked["category"], checked["provider_reference"])
        if registered != checked:
            raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        clean = _validate_configuration(configuration)
        try:
            if not self._verified_source(factory, registered):
                raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
            provider = factory()
            if not self._verified_source(provider, registered):
                raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
            instance = provider.validate_configuration(_verified_copy(clean))
        except (TypeError, ValueError, KeyError) as exc:
            raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION) from exc
        configured = ConfiguredExtension(self._token, registered, clean, provider, instance)
        store = (
            instance.get("store")
            if isinstance(instance, Mapping) and _is_bundled_provider(provider)
            else None
        )
        self._bindings[id(configured)] = _InstanceBinding(
            provider, type(provider), instance, store, type(store) if store is not None else None
        )
        weakref.finalize(configured, self._bindings.pop, id(configured), None)
        self._handles[id(configured)] = configured
        self._bound(configured)
        return configured

    def _bound(
        self, configured: ConfiguredExtension
    ) -> tuple[dict[str, Any], ExtensionProvider, Any, OperationalEvaluator | None]:
        if (
            not isinstance(configured, ConfiguredExtension)
            or configured._token is not self._token
            or self._handles.get(id(configured)) is not configured
        ):
            raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        binding = self._bindings.get(id(configured))
        if (
            binding is None
            or configured._provider is not binding.provider
            or type(configured._provider) is not binding.provider_type
            or configured._instance is not binding.instance
        ):
            raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        descriptor, factory, evaluator = self._entry(
            configured._descriptor["category"], configured._descriptor["provider_reference"]
        )
        if (
            descriptor != configured._descriptor
            or not self._verified_source(factory, descriptor)
            or not self._verified_source(configured._provider, descriptor)
        ):
            raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        instance = configured._instance
        instance_id = (
            instance.get("instance_id")
            if isinstance(instance, Mapping)
            else getattr(instance, "instance_id", None)
        )
        if instance_id != configured._configuration.get("instance_id"):
            raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)
        if _is_bundled_provider(configured._provider):
            store = instance.get("store") if isinstance(instance, Mapping) else None
            if (
                store is not binding.store
                or type(store) is not binding.store_type
                or _shadows_executable(configured._provider)
                or _shadows_executable(store)
            ):
                raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
            from .authority import (
                AuthoritySQLiteExecutionStore,
                SQLiteLocalAuthority,
                _BundledAuthorityProvider,
            )

            if isinstance(configured._provider, _BundledAuthorityProvider):
                authority = instance.get("authority") if isinstance(instance, Mapping) else None
                if (
                    not isinstance(authority, SQLiteLocalAuthority)
                    or not isinstance(store, AuthoritySQLiteExecutionStore)
                    or type(authority) is not SQLiteLocalAuthority
                    or type(store) is not AuthoritySQLiteExecutionStore
                    or _shadows_executable(authority)
                    or authority.path != configured._configuration.get("path")
                    or store.scope_identity != configured._configuration.get("scope_identity")
                    or store.owner_principal != configured._configuration.get("owner_principal")
                    or store.authority_epoch != configured._configuration.get("authority_epoch")
                    or store.replay_retention != configured._configuration.get("replay_retention")
                    or store.outbox_retention != configured._configuration.get("outbox_retention")
                ):
                    raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        return descriptor, configured._provider, instance, evaluator

    def capabilities(self, configured: ConfiguredExtension, *extra: Any) -> list[str]:
        if extra:
            raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        _, provider, instance, _ = self._bound(configured)
        try:
            return list(provider.capabilities(instance))
        except Exception as exc:
            raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION) from exc

    def health(self, configured: ConfiguredExtension, *extra: Any) -> str:
        if extra:
            raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
        _, provider, instance, _ = self._bound(configured)
        try:
            return provider.health(instance)
        except Exception as exc:
            raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION) from exc

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
        configured = self.validate_configuration(descriptor, configuration)
        return self.report(configured, requirement)

    def report(
        self,
        configured: ConfiguredExtension,
        requirement: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Recheck source, identity, current health and proof for a configured instance."""
        claims = self.capabilities(configured)
        health = self.health(configured)
        checked, _, instance, evaluator = self._bound(configured)
        clean = configured._configuration
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
        try:
            proven = evaluator(instance, health, claims) if evaluator is not None else frozenset()
        except ExtensionError:
            raise
        except Exception as exc:
            raise ExtensionError(Code.EXTENSION_CAPABILITY_MISMATCH) from exc
        # An observed I/O hazard prevents a contradictory pure guarantee even
        # before its category-specific public claim becomes available.
        report["claims"] = [
            claim
            for claim in claims
            if claim in proven
            and health == "healthy"
            and not (claim == "pure" and "external_io_capable" in claims)
        ]
        if requirement is not None:
            requested = _check(
                "extension-capability-requirement",
                requirement,
                Code.INVALID_EXTENSION_DESCRIPTOR,
            )
            target, _, _ = self._entry(requested["category"], requested["provider_reference"])
            if target != checked:
                raise ExtensionError(Code.EXTENSION_IDENTITY_MISMATCH)
            if (
                requested["instance_id"] != report["instance_id"]
                or report["health"] != "healthy"
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
        if requested_profile is not None and not compose:
            raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)
        if requested_profile not in (None, "automatic_retry_without_external_io", *_GUARANTEES):
            raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)
        for requirement in requirements:
            _check(
                "extension-capability-requirement", requirement, Code.INVALID_EXTENSION_DESCRIPTOR
            )
        reports: list[dict[str, Any]] = []
        for participant in configurations:
            if type(participant) is not dict or set(participant) != {
                "category",
                "provider_reference",
                "configuration",
            }:
                raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)
            descriptor, _, _ = self._entry(
                participant["category"], participant["provider_reference"]
            )
            reports.append(self.negotiate(descriptor, participant["configuration"]))
        if any(report["health"] != "healthy" for report in reports):
            raise ExtensionError(Code.EXTENSION_CAPABILITY_MISMATCH)
        for requirement in requirements:
            self._entry(requirement["category"], requirement["provider_reference"])
            if not any(
                report["category"] == requirement["category"]
                and report["provider_reference"] == requirement["provider_reference"]
                and report["instance_id"] == requirement["instance_id"]
                and report["health"] == "healthy"
                and requirement["capability"] in report["claims"]
                for report in reports
            ):
                raise ExtensionError(Code.EXTENSION_CAPABILITY_MISMATCH)
        effective: dict[str, bool] = {}
        if compose:
            full = _compose_capabilities(reports, host_guarantees or {})
            if any(name not in full for name in projection):
                raise ExtensionError(Code.INVALID_EXTENSION_CONFIGURATION)
            effective = {name: full[name] for name in projection}
            if full["weak_profile_opt_in_required"] and not weak_profile_opt_in:
                raise ExtensionError(Code.EXTENSION_CAPABILITY_MISMATCH)
            if requested_profile in _GUARANTEES and not full[requested_profile]:
                raise ExtensionError(Code.EXTENSION_CAPABILITY_MISMATCH)
            if requested_profile == "automatic_retry_without_external_io" and (
                full["external_io_capable"] or not full["pure"] or not full["deterministic"]
            ):
                raise ExtensionError(Code.EXTENSION_CAPABILITY_MISMATCH)
        return {"status": "accepted", "reports": reports, "effective": effective}


def _compose_capabilities(
    reports: Sequence[Mapping[str, Any]],
    host_guarantees: Mapping[str, bool],
) -> dict[str, bool]:
    """Compose all-party guarantees and any-party external I/O hazard."""
    effective = {
        guarantee: host_guarantees.get(guarantee) is True
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
    return effective


class _BundledStoreProvider:
    """Ordinary public extension wrapper over an execution-store factory."""

    def __init__(self, factory: Callable[[str, Mapping[str, Any]], Any]) -> None:
        self.factory = factory

    def validate_configuration(self, configuration: Mapping[str, Any]) -> Any:
        from .stores.base import ExecutionStoreError

        if (
            set(configuration) != {"instance_id", "uri", "store_configuration"}
            or type(configuration["instance_id"]) is not str
            or _INSTANCE_ID.fullmatch(configuration["instance_id"]) is None
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


def _is_bundled_provider(provider: Any) -> bool:
    from .authority import _BundledAuthorityProvider

    return isinstance(provider, (_BundledStoreProvider, _BundledAuthorityProvider))


def _shadows_executable(instance: Any) -> bool:
    own = vars(instance)
    return any(
        name in own and (callable(value) or isinstance(value, property))
        for ancestor in type(instance).__mro__
        for name, value in vars(ancestor).items()
    )


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


def _bundled_factory_matches_source(name: str, factory: Any) -> bool:
    """Bind the loaded package callbacks used by a bundled store to hashed source."""
    import ast
    import importlib
    import sys
    import types

    package = Path(__file__).parent
    authority = name == "authority"
    module = importlib.import_module(
        "determa.state.authority" if authority else f"determa.state.stores.{name}"
    )
    if type(module) is not types.ModuleType or not isinstance(factory, types.FunctionType):
        return False
    factory_name = (
        "bundled_sqlite_authority_provider_factory"
        if authority
        else f"{name}_execution_store_factory"
    )
    if factory is not getattr(module, factory_name, None) or (
        not authority
        and factory
        is not getattr(importlib.import_module("determa.state.stores"), factory_name, None)
    ):
        return False

    compiled: dict[str, types.CodeType] = {}
    syntax: dict[str, ast.Module] = {}
    seen: set[tuple[str, str]] = set()

    def installed_module_attribute(module: types.ModuleType, attribute: str) -> bool:
        """Check a called stdlib attribute against installed source or a native anchor."""
        import sys
        import sysconfig

        if type(module) is not types.ModuleType:
            return False
        root_name = module.__name__.split(".", 1)[0]
        if root_name not in sys.stdlib_module_names:
            # External validator implementations are a separate installed
            # dependency; this check binds executable stdlib imports.
            return root_name != "determa"
        if not PLATFORM_BINDINGS.matches(module, attribute):
            return False
        value = vars(module).get(attribute)
        defining_name = getattr(value, "__module__", None)
        if not isinstance(defining_name, str) or not isinstance(
            value, (types.FunctionType, types.BuiltinFunctionType, type)
        ):
            return False
        defining_module = importlib.import_module(defining_name)
        if type(defining_module) is not types.ModuleType:
            return False
        source_file = getattr(defining_module, "__file__", None)
        stdlib = Path(sysconfig.get_paths()["stdlib"]).resolve()
        if isinstance(source_file, str) and not Path(source_file).resolve().is_relative_to(stdlib):
            return False
        if isinstance(value, types.BuiltinFunctionType):
            return (
                (isinstance(source_file, str) or defining_name in sys.builtin_module_names)
                and value.__name__ in {attribute, f"openssl_{attribute}"}
                and vars(defining_module).get(value.__name__) is value
            )
        if isinstance(value, type) and defining_name == "ast":
            # Native AST classes are exported through ast from the built-in _ast.
            native = importlib.import_module("_ast")
            return vars(native).get(attribute) is value
        if not isinstance(source_file, str) or Path(source_file).suffix != ".py":
            return False
        source = Path(source_file).read_bytes()
        cached = _INSTALLED_SOURCE_CACHE.get(source_file)
        if cached is not None and cached[0] == source:
            _, tree, code = cached
        else:
            tree = ast.parse(source)
            code = compile(source, source_file, "exec", dont_inherit=True)
            _INSTALLED_SOURCE_CACHE[source_file] = source, tree, code
        declared = next(
            (
                node
                for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name == attribute
            ),
            None,
        )
        expected = direct_code(code, attribute)
        if declared is None or expected is None:
            return False
        if isinstance(value, types.FunctionType):
            return (
                isinstance(declared, ast.FunctionDef)
                and value.__code__ == expected
                and value.__globals__ is vars(defining_module)
                and value.__name__ == attribute
                and value.__module__ == defining_name
            )
        if not isinstance(declared, ast.ClassDef) or value.__module__ != defining_name:
            return False
        class_key = source_file, attribute
        class_cache = _INSTALLED_CLASS_CACHE.get(class_key)
        if class_cache is not None and class_cache[0] == source:
            reference_class = class_cache[1]
        else:
            reference_namespace = dict(vars(defining_module))
            exec(
                compile(
                    ast.Module(body=[declared], type_ignores=[]),
                    source_file,
                    "exec",
                    dont_inherit=True,
                ),
                reference_namespace,
            )
            reference_class = reference_namespace[attribute]
            _INSTALLED_CLASS_CACHE[class_key] = source, reference_class

        def declared_code(qualname: str, name: str, line: int) -> types.CodeType | None:
            pending = [code]
            while pending:
                parent = pending.pop()
                for child in parent.co_consts:
                    if isinstance(child, types.CodeType):
                        if (
                            child.co_qualname == qualname
                            and child.co_name == name
                            and child.co_firstlineno == line
                        ):
                            return child
                        pending.append(child)
            return None

        def same_executable(current: Any, reference: Any) -> bool:
            if type(current) is not type(reference):
                return False
            if isinstance(reference, types.FunctionType):
                return (
                    current.__code__
                    == declared_code(
                        current.__qualname__, current.__name__, reference.__code__.co_firstlineno
                    )
                    and current.__globals__ is vars(defining_module)
                    and current.__defaults__ == reference.__defaults__
                    and current.__kwdefaults__ == reference.__kwdefaults__
                    and current.__module__ == reference.__module__
                    and current.__qualname__ == reference.__qualname__
                )
            if isinstance(reference, property):
                return all(
                    (actual is None and pristine is None)
                    or actual is not None
                    and pristine is not None
                    and same_executable(actual, pristine)
                    for actual, pristine in zip(
                        (current.fget, current.fset, current.fdel),
                        (reference.fget, reference.fset, reference.fdel),
                        strict=True,
                    )
                )
            if isinstance(reference, (classmethod, staticmethod)):
                return same_executable(current.__func__, reference.__func__)
            if isinstance(reference, type):
                if current.__bases__ != reference.__bases__:
                    return False
                return all(
                    key in vars(reference) and same_executable(member, vars(reference)[key])
                    for key, member in vars(current).items()
                    if callable(member) or hasattr(type(member), "__get__")
                )
            if isinstance(reference, (types.GetSetDescriptorType, types.MemberDescriptorType)):
                return bool(current.__name__ == reference.__name__)
            if callable(reference) or hasattr(type(reference), "__get__"):
                return current is reference
            return True

        return same_executable(value, reference_class)

    def source_code(origin: types.ModuleType) -> types.CodeType | None:
        if type(origin) is not types.ModuleType:
            return None
        path = getattr(origin, "__file__", None)
        if not isinstance(path, str):
            return None
        source_path = package / Path(*origin.__name__.split(".")[2:])
        source_path = (
            source_path / "__init__.py" if source_path.is_dir() else source_path.with_suffix(".py")
        )
        if Path(path).resolve() != source_path.resolve() or not source_path.is_file():
            return None
        if origin.__name__ not in compiled:
            source = source_path.read_bytes()
            tree = ast.parse(source)
            module_imports: dict[str, types.ModuleType] = {}
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imported = importlib.import_module(alias.name)
                        bound_name = alias.asname or alias.name.split(".", 1)[0]
                        module_imports[bound_name] = imported
            for call in ast.walk(tree):
                if (
                    isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Attribute)
                    and isinstance(call.func.value, ast.Name)
                    and call.func.value.id in module_imports
                    and not installed_module_attribute(
                        module_imports[call.func.value.id], call.func.attr
                    )
                ):
                    return None
            for node in tree.body:
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imported = importlib.import_module(alias.name)
                        if alias.asname is None and "." in alias.name:
                            imported = importlib.import_module(alias.name.split(".", 1)[0])
                        bound_name = alias.asname or alias.name.split(".", 1)[0]
                        if vars(origin).get(bound_name) is not imported:
                            return None
                elif isinstance(node, ast.ImportFrom) and node.module != "__future__":
                    imported = importlib.import_module(
                        "." * node.level + (node.module or ""), origin.__package__
                    )
                    if type(imported) is not types.ModuleType:
                        return None
                    for alias in node.names:
                        if alias.name == "*" or vars(origin).get(
                            alias.asname or alias.name
                        ) is not getattr(imported, alias.name, None):
                            return None
                        if (
                            callable(getattr(imported, alias.name, None))
                            and imported.__name__.split(".", 1)[0] in sys.stdlib_module_names
                            and not PLATFORM_BINDINGS.matches(imported, alias.name)
                        ):
                            return None
            compiled[origin.__name__] = compile(source, str(source_path), "exec")
            syntax[origin.__name__] = tree
        return compiled[origin.__name__]

    def direct_code(parent: types.CodeType, member_name: str) -> types.CodeType | None:
        return next(
            (
                child
                for child in parent.co_consts
                if isinstance(child, types.CodeType) and child.co_name == member_name
            ),
            None,
        )

    def generated_member_matches(current: Any, expected: Any, owner: type, reference: type) -> bool:
        """Compare compiler/stdlib members with a class rebuilt from installed source."""
        if expected is reference:
            return current is owner
        if type(current) is not type(expected):
            return False
        if (
            isinstance(expected, type)
            and expected.__module__ == reference.__module__
            and expected.__qualname__ == reference.__qualname__
        ):
            # slots=True creates an intermediate class captured by frozen
            # dataclass methods; it is distinct from the published class.
            return (
                current is not owner
                and expected is not reference
                and current.__module__ == owner.__module__
                and current.__qualname__ == owner.__qualname__
                and current.__bases__ == owner.__bases__
            )
        if isinstance(expected, types.FunctionType):
            if (
                current.__code__ != expected.__code__
                or current.__globals__ is not expected.__globals__
                or current.__name__ != expected.__name__
                or current.__qualname__ != expected.__qualname__
                or current.__module__ != expected.__module__
                or current.__defaults__ != expected.__defaults__
                or current.__kwdefaults__ != expected.__kwdefaults__
            ):
                return False
            current_closure = current.__closure__ or ()
            expected_closure = expected.__closure__ or ()
            return len(current_closure) == len(expected_closure) and all(
                generated_member_matches(a.cell_contents, b.cell_contents, owner, reference)
                for a, b in zip(current_closure, expected_closure, strict=True)
            )
        if isinstance(expected, (staticmethod, classmethod)):
            return generated_member_matches(current.__func__, expected.__func__, owner, reference)
        if isinstance(expected, (types.GetSetDescriptorType, types.MemberDescriptorType)):
            expected_owner = getattr(expected, "__objclass__", None)
            return current.__name__ == expected.__name__ and (
                current.__objclass__ is owner
                if expected_owner is reference
                else current.__objclass__ is expected_owner
            )
        if callable(expected) or hasattr(type(expected), "__get__"):
            return current is expected
        # A closure's cache/counter data can change without replacing its code.
        return True

    def verify_definition(origin: types.ModuleType, member_name: str) -> bool:
        key = (origin.__name__, member_name)
        if key in seen:
            return True
        seen.add(key)
        top = source_code(origin)
        expected = direct_code(top, member_name) if top is not None else None
        value = vars(origin).get(member_name)
        if (
            expected is None
            or value is None
            or getattr(value, "__module__", None) != origin.__name__
        ):
            return False

        def verify_function(
            candidate: Any,
            code: types.CodeType,
            declaration: ast.FunctionDef | ast.AsyncFunctionDef,
            owner: type | None = None,
        ) -> bool:
            # A contextmanager decorator installs the only executable wrapper used
            # by bundled classes. Do not trust an arbitrary __wrapped__ pointer:
            # it can name the original function while a different wrapper runs.
            decorators = [
                decorator.id if isinstance(decorator, ast.Name) else None
                for decorator in declaration.decorator_list
            ]
            actual = candidate
            if "contextmanager" in decorators:
                from contextlib import contextmanager

                def probe() -> Iterator[None]:
                    yield None

                reference = contextmanager(probe)
                if (
                    not isinstance(candidate, types.FunctionType)
                    or candidate.__code__ != reference.__code__
                    or candidate.__globals__ is not reference.__globals__
                    or candidate.__name__ != code.co_name
                    or candidate.__module__ != origin.__name__
                    or candidate.__closure__ is None
                    or len(candidate.__closure__) != 1
                    or vars(candidate).get("__wrapped__")
                    is not candidate.__closure__[0].cell_contents
                ):
                    return False
                actual = candidate.__closure__[0].cell_contents
            elif isinstance(candidate, types.FunctionType) and "__wrapped__" in vars(candidate):
                return False
            if (
                not isinstance(actual, types.FunctionType)
                or actual.__code__ != code
                or actual.__globals__ is not vars(origin)
                or actual.__name__ != code.co_name
                or actual.__qualname__ != code.co_qualname
                or actual.__module__ != origin.__name__
            ):
                return False
            closure = actual.__closure__ or ()
            if len(closure) != len(code.co_freevars) or any(
                name != "__class__" or owner is None or cell.cell_contents is not owner
                for name, cell in zip(code.co_freevars, closure, strict=True)
            ):
                return False
            try:
                defaults = tuple(ast.literal_eval(node) for node in declaration.args.defaults)
                kwdefaults = {
                    argument.arg: ast.literal_eval(default)
                    for argument, default in zip(
                        declaration.args.kwonlyargs, declaration.args.kw_defaults, strict=True
                    )
                    if default is not None
                }
            except (ValueError, TypeError, RecursionError):
                return False
            actual_defaults = actual.__defaults__ or ()
            actual_kwdefaults = actual.__kwdefaults__ or {}
            if (
                len(actual_defaults) != len(defaults)
                or any(
                    type(current) is not type(expected) or current != expected
                    for current, expected in zip(actual_defaults, defaults, strict=True)
                )
                or actual_kwdefaults.keys() != kwdefaults.keys()
                or any(
                    type(actual_kwdefaults[key]) is not type(expected)
                    or actual_kwdefaults[key] != expected
                    for key, expected in kwdefaults.items()
                )
            ):
                return False
            for global_name in code.co_names:
                dependency = actual.__globals__.get(global_name)
                dependency_module = getattr(dependency, "__module__", "")
                if (
                    isinstance(dependency, (types.FunctionType, type))
                    and isinstance(dependency_module, str)
                    and dependency_module.startswith("determa.state.")
                ):
                    source_module = importlib.import_module(dependency_module)
                    dependency_name = getattr(dependency, "__name__", None)
                    if (
                        not isinstance(dependency_name, str)
                        or getattr(source_module, dependency_name, None) is not dependency
                        or not verify_definition(source_module, dependency_name)
                    ):
                        return False
            return True

        if isinstance(value, type):
            declaration = next(
                (
                    node
                    for node in syntax[origin.__name__].body
                    if isinstance(node, ast.ClassDef) and node.name == member_name
                ),
                None,
            )
            if (
                declaration is None
                or type(value) is not (type(value.__bases__[-1]) if value.__bases__ else type)
                or len(value.__bases__) != max(1, len(declaration.bases))
                or any(
                    not isinstance(base, ast.Name)
                    or vars(origin).get(base.id) is not value.__bases__[index]
                    for index, base in enumerate(declaration.bases)
                )
            ):
                return False
            declared = {
                node.name: node
                for node in declaration.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            generated = {
                "__dict__",
                "__weakref__",
                "__annotations__",
                "__firstlineno__",
                "__static_attributes__",
            }
            if any(
                isinstance(decorator, ast.Name)
                and decorator.id == "dataclass"
                or isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "dataclass"
                for decorator in declaration.decorator_list
            ):
                generated.update(
                    {
                        "__init__",
                        "__repr__",
                        "__eq__",
                        "__setattr__",
                        "__delattr__",
                        "__hash__",
                        "__getstate__",
                        "__setstate__",
                        "__replace__",
                        "__dataclass_params__",
                        "__dataclass_fields__",
                        "__match_args__",
                        "__slots__",
                        *(
                            node.target.id
                            for node in declaration.body
                            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                        ),
                    }
                )
            if any(
                isinstance(base, ast.Name) and base.id == "Protocol" for base in declaration.bases
            ):
                generated.update(
                    {"__subclasshook__", "__init__", "__parameters__", "__protocol_attrs__"}
                )
            if any(
                isinstance(base, ast.Name) and base.id in {"Enum", "StrEnum"}
                for base in declaration.bases
            ):
                generated.update(
                    {
                        "_generate_next_value_",
                        "_new_member_",
                        "_member_type_",
                        "_value_repr_",
                        "__str__",
                        "__format__",
                        "__new__",
                    }
                )
            # Data attributes may change with the configured store. Additional
            # descriptors or callables change the loaded class's executable path.
            if any(
                name not in declared
                and name not in generated
                and name not in {"__module__", "__doc__", "__abstractmethods__", "_abc_impl"}
                and (
                    callable(member)
                    or hasattr(type(member), "__get__")
                    or name.startswith("__")
                    and name.endswith("__")
                )
                for name, member in vars(value).items()
            ):
                return False
            # Rebuild the class definition from the installed file. The result
            # supplies the generated methods and descriptors for this interpreter,
            # including dataclass, Protocol, Enum, and implicit slot members.
            reference_namespace = dict(vars(origin))
            import __future__

            exec(
                compile(
                    ast.Module(body=[declaration], type_ignores=[]),
                    str(Path(origin.__file__ or "")),
                    "exec",
                    flags=__future__.annotations.compiler_flag,
                ),
                reference_namespace,
            )
            reference_class = reference_namespace[member_name]
            if any(
                name in generated
                and (
                    name not in vars(reference_class)
                    or not generated_member_matches(
                        member, vars(reference_class)[name], value, reference_class
                    )
                )
                for name, member in vars(value).items()
            ):
                return False
            for child in expected.co_consts:
                if not isinstance(child, types.CodeType) or child.co_name.startswith("<"):
                    continue
                method = vars(value).get(child.co_name)
                if isinstance(method, property):
                    if (
                        type(method) is not property
                        or method.fset is not None
                        or method.fdel is not None
                    ):
                        return False
                    method = method.fget
                elif isinstance(method, (staticmethod, classmethod)):
                    if type(method) not in {staticmethod, classmethod}:
                        return False
                    method = method.__func__
                if child.co_name not in declared or not verify_function(
                    method, child, declared[child.co_name], value
                ):
                    return False
            return True
        source_function = next(
            (
                node
                for node in syntax[origin.__name__].body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == member_name
            ),
            None,
        )
        return source_function is not None and verify_function(value, expected, source_function)

    origins = [importlib.import_module(__name__), module]
    if authority:
        origins.extend((importlib.import_module("determa.state.stores.sqlite"),))
    origins.append(importlib.import_module("determa.state.stores.base"))
    for origin in origins:
        top = source_code(origin)
        if top is None:
            return False
        for child in top.co_consts:
            if isinstance(child, types.CodeType) and not child.co_name.startswith("<"):
                if not verify_definition(origin, child.co_name):
                    return False
    if authority:
        host = importlib.import_module("determa.state.host")
        host_code = source_code(host)
        if host_code is None:
            return False
        for child in host_code.co_consts:
            if not isinstance(child, types.CodeType) or child.co_name.startswith("<"):
                continue
            loaded = vars(host).get(child.co_name)
            if isinstance(loaded, type):
                for method_code in child.co_consts:
                    if not isinstance(
                        method_code, types.CodeType
                    ) or method_code.co_name.startswith("<"):
                        continue
                    method = vars(loaded).get(method_code.co_name)
                    if isinstance(method, (staticmethod, classmethod)):
                        method = method.__func__
                    if isinstance(method, property):
                        method = method.fget
                    if getattr(method, "__code__", None) != method_code:
                        return False
            elif getattr(loaded, "__code__", None) != child:
                return False
    return True


def _bundled_provider_factory_matches_source(factory: Any) -> bool:
    """The registry's selected wrapper factory must itself be the installed code."""
    import types

    if (
        not isinstance(factory, types.FunctionType)
        or factory.__name__ != "make_provider"
        or factory.__module__ != __name__
        or factory.__globals__ is not globals()
        or factory.__closure__ is not None
    ):
        return False
    compiled = compile(Path(__file__).read_bytes(), __file__, "exec")
    outer = next(
        (
            child
            for child in compiled.co_consts
            if isinstance(child, types.CodeType) and child.co_name == "bundled_extension_registry"
        ),
        None,
    )
    inner = (
        next(
            (
                child
                for child in outer.co_consts
                if isinstance(child, types.CodeType) and child.co_name == "make_provider"
            ),
            None,
        )
        if outer is not None
        else None
    )
    return factory.__code__ == inner


def bundled_extension_registry(
    *,
    include_postgresql: bool = True,
    source_verifier: Callable[[Any, Mapping[str, Any]], bool] | None = None,
) -> ExtensionRegistry:
    """Register bundled stores through the same public operation as custom providers.

    Claims requiring a category-specific proof are withheld until such proof is
    installed by a host. Memory's `ephemeral` only states that loss is permitted.
    """
    from .__about__ import __version__
    from .authority import (
        AuthoritySQLiteExecutionStore,
        SQLiteLocalAuthority,
        _authority_closure,
        _BundledAuthorityProvider,
        bundled_authority_descriptor,
        bundled_sqlite_authority_provider_factory,
    )
    from .stores import (
        file_execution_store_factory,
        memory_execution_store_factory,
        postgresql_execution_store_factory,
        sqlite_execution_store_factory,
    )

    digest = _bundled_source_digest()

    def verify(provider: Any, descriptor: Mapping[str, Any]) -> bool:
        reference = descriptor["provider_reference"]
        identifier = reference["identifier"]
        if identifier == "reference.sqlite-authority":
            import hashlib

            installed_authority = (
                descriptor == bundled_authority_descriptor()
                and reference["content_digest"]
                == "sha256:" + hashlib.sha256(_authority_closure()).hexdigest()
            )
            if isinstance(provider, _BundledAuthorityProvider):
                return (
                    installed_authority
                    and _bundled_factory_matches_source(
                        "authority", bundled_sqlite_authority_provider_factory
                    )
                    and not _shadows_executable(provider)
                )
            return (
                installed_authority
                and provider is bundled_sqlite_authority_provider_factory
                and _bundled_factory_matches_source("authority", provider)
            )
        installed = (
            identifier
            in {
                "determa.store.memory",
                "determa.store.file",
                "determa.store.sqlite",
                "determa.store.postgresql",
            }
            and reference["content_digest"] == _bundled_source_digest()
        )
        if isinstance(provider, _BundledStoreProvider):
            return installed and _bundled_factory_matches_source(
                identifier.removeprefix("determa.store."), provider.factory
            )
        if _bundled_provider_factory_matches_source(provider):
            selected = getattr(provider, "__defaults__", None)
            return (
                installed
                and isinstance(selected, tuple)
                and len(selected) == 1
                and _bundled_factory_matches_source(
                    identifier.removeprefix("determa.store."), selected[0]
                )
            )
        if identifier.startswith("determa.store."):
            return False
        return source_verifier is not None and source_verifier(provider, descriptor)

    registry = ExtensionRegistry(source_verifier=verify)
    stores = [
        ("memory", memory_execution_store_factory),
        ("file", file_execution_store_factory),
        ("sqlite", sqlite_execution_store_factory),
    ]
    if include_postgresql:
        stores.append(("postgresql", postgresql_execution_store_factory))
    supported = {
        "memory": {"ephemeral"},
        "file": {"restart_persistent"},
        "sqlite": {
            "durable_single_writer",
            "shared_application_transaction",
            "root_identity_retention",
            "permanent_receipt_retention",
            "permanent_outbox_terminal_retention",
            "compact_effect_identity_retention",
        },
        "postgresql": {
            "durable_concurrent",
            "shared_application_transaction",
            "root_identity_retention",
            "permanent_receipt_retention",
            "permanent_outbox_terminal_retention",
            "compact_effect_identity_retention",
        },
    }
    for name, factory in stores:
        descriptor = {
            "category": "execution_store",
            "provider_reference": {
                "identifier": f"determa.store.{name}",
                "version": __version__,
                "content_digest": digest,
            },
            "interface_version": 1,
            "supported_capabilities": sorted(supported[name]),
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
    authority_descriptor = bundled_authority_descriptor()
    registry.register(authority_descriptor, bundled_sqlite_authority_provider_factory)

    def authority_proof(instance: Any, health: str, claims: Sequence[str]) -> frozenset[str]:
        authority = instance.get("authority") if isinstance(instance, dict) else None
        store = instance.get("store") if isinstance(instance, dict) else None
        if (
            health != "healthy"
            or type(authority) is not SQLiteLocalAuthority
            or type(store) is not AuthoritySQLiteExecutionStore
            or store.authority is not authority
            or store.path != authority.path
            or store.journal_mode != "WAL"
            or store.synchronous != "FULL"
        ):
            return frozenset()
        try:
            authority.validate_schema()
            store.validate_schema()
            ledger = authority.inspect(store.scope_identity)
        except (ValueError, KeyError):
            return frozenset()
        if (
            ledger is None
            or ledger["owner_principal"] != store.owner_principal
            or ledger["authority_epoch"] != store.authority_epoch
            or ledger["state"] not in ("active", "frozen")
        ):
            return frozenset()
        return frozenset(claims) & frozenset(authority_descriptor["supported_capabilities"])

    registry._set_operational_evaluator(authority_descriptor, authority_proof)
    return registry
