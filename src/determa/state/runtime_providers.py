"""Exact installed runtime providers for optional format-1 language slots.

The registry owns source and configured-instance checks. A binding in a definition
names code; it does not install code or establish a capability by itself.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import importlib
import inspect
import json
import types
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .extensions import ExtensionError, ExtensionRegistry


class RuntimeProviderError(ValueError):
    """Closed optional provider, source, or output boundary failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _reference_key(reference: Mapping[str, str]) -> tuple[str, str, str]:
    return reference["identifier"], reference["version"], reference["content_digest"]


def _valid_reference(reference: Any) -> bool:
    import jsonschema

    schema = json.loads(
        (Path(__file__).parent / "data" / "provider-reference-v1.schema.json").read_text()
    )
    return type(reference) is dict and jsonschema.Draft202012Validator(schema).is_valid(reference)


@dataclass(frozen=True)
class SourceClosure:
    """Host-installed source files and their exact executable binding.

    ``domain`` is part of the provider's declared digest contract. All paths are
    relative to ``root`` and are read again at every selection.
    """

    root: Path
    paths: tuple[str, ...]
    manifest: str
    domain: bytes
    python_source: str

    def __post_init__(self) -> None:
        if (
            not self.paths
            or len(set(self.paths)) != len(self.paths)
            or self.python_source not in self.paths
            or self.manifest in self.paths
            or any(
                not name
                or Path(name).is_absolute()
                or ".." in Path(name).parts
                or (self.root / name).is_symlink()
                for name in (*self.paths, self.manifest)
            )
        ):
            raise RuntimeProviderError("runtime_provider_unavailable")

    def digest(self) -> str:
        result = hashlib.sha256(self.domain)
        for name in self.paths:
            body = (self.root / name).read_bytes()
            path = name.encode("utf-8")
            result.update(len(path).to_bytes(8, "big"))
            result.update(path)
            result.update(len(body).to_bytes(8, "big"))
            result.update(body)
        return "sha256:" + result.hexdigest()

    def digest_matches(self, expected: str) -> bool:
        try:
            return self.digest() == expected
        except (OSError, ValueError):
            return False

    def verified(self, binding: Mapping[str, Any], executable: Any) -> bool:
        try:
            if not self.digest_matches(binding["provider_reference"]["content_digest"]):
                return False
            expected_source = (
                "sha256:" + hashlib.sha256(binding["source"].encode("utf-8")).hexdigest()
                if "source" in binding
                else self.manifest_digest()
            )
            if expected_source != binding["source_digest"]:
                return False
            if not self.manifest_verified():
                return False
            return _loaded_code_matches(self.root / self.python_source, executable)
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return False

    def manifest_digest(self) -> str:
        return "sha256:" + hashlib.sha256((self.root / self.manifest).read_bytes()).hexdigest()

    def manifest_verified(self) -> bool:
        try:
            body = (self.root / self.manifest).read_bytes()
            manifest = json.loads(body)
            expected_files = [
                {
                    "path": name,
                    "sha256": "sha256:"
                    + hashlib.sha256((self.root / name).read_bytes()).hexdigest(),
                }
                for name in self.paths
            ]
            return (
                type(manifest) is dict
                and set(manifest) == {"files", "format", "version"}
                and manifest["version"] == 1
                and type(manifest["format"]) is str
                and manifest["files"] == expected_files
                and body
                == json.dumps(
                    manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
            )
        except (OSError, ValueError, TypeError, KeyError):
            return False


def _loaded_code_matches(source_path: Path, executable: Any) -> bool:
    """Compare selected Python code and its module callbacks to installed bytes."""
    module_name = getattr(executable, "__module__", None)
    if not isinstance(module_name, str):
        return False
    module = importlib.import_module(module_name)
    if Path(getattr(module, "__file__", "")).resolve() != source_path.resolve():
        return False
    source = source_path.read_bytes()
    tree = ast.parse(source)
    compiled = compile(source, str(source_path), "exec", dont_inherit=True)
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                loaded = importlib.import_module(alias.name)
                if alias.asname is None and "." in alias.name:
                    loaded = importlib.import_module(alias.name.split(".", 1)[0])
                if vars(module).get(alias.asname or alias.name.split(".", 1)[0]) is not loaded:
                    return False
        elif isinstance(node, ast.ImportFrom) and node.module != "__future__":
            loaded = importlib.import_module(
                "." * node.level + (node.module or ""), module.__package__
            )
            if any(
                alias.name == "*"
                or vars(module).get(alias.asname or alias.name)
                is not getattr(loaded, alias.name, None)
                for alias in node.names
            ):
                return False

    def matches(actual: Any, expected: types.CodeType) -> bool:
        unwrapped = inspect.unwrap(actual)
        return (
            isinstance(unwrapped, types.FunctionType)
            and unwrapped.__code__ == expected
            and unwrapped.__globals__ is vars(module)
        )

    for code in compiled.co_consts:
        if not isinstance(code, types.CodeType) or code.co_name.startswith("<"):
            continue
        value = vars(module).get(code.co_name)
        if getattr(value, "__module__", None) != module_name:
            return False
        if isinstance(value, type):
            if any(base.__module__ != module_name for base in value.__mro__[1:-1]):
                return False
            declaration = next(
                (
                    node
                    for node in tree.body
                    if isinstance(node, ast.ClassDef) and node.name == code.co_name
                ),
                None,
            )
            if (
                declaration is None
                or (not declaration.bases and value.__bases__ != (object,))
                or (
                    declaration.bases
                    and (
                        len(value.__bases__) != len(declaration.bases)
                        or any(
                            not isinstance(base, ast.Name)
                            or vars(module).get(base.id) is not value.__bases__[index]
                            for index, base in enumerate(declaration.bases)
                        )
                    )
                )
            ):
                return False
            for method_code in code.co_consts:
                if not isinstance(method_code, types.CodeType) or method_code.co_name.startswith(
                    "<"
                ):
                    continue
                method = vars(value).get(method_code.co_name)
                if isinstance(method, property):
                    method = method.fget
                elif isinstance(method, (staticmethod, classmethod)):
                    method = method.__func__
                if not matches(method, method_code):
                    return False
        elif not matches(value, code):
            return False
    return getattr(module, getattr(executable, "__name__", ""), None) is executable


@dataclass(frozen=True)
class ResolvedRuntimeProvider:
    kind: str
    binding: dict[str, Any]
    provider: Any
    capabilities: dict[str, bool]


CapabilityProof = Callable[[Any, Mapping[str, Any]], frozenset[str]]
_CAPABILITIES = frozenset(
    {
        "deterministic",
        "pure",
        "portable",
        "semantically_introspectable",
        "process_contained",
        "external_io_capable",
    }
)


def _proved(value: Any) -> frozenset[str]:
    if type(value) is not frozenset or not value.issubset(_CAPABILITIES):
        raise RuntimeProviderError("extension_capability_mismatch")
    return value


def _invoke_proof(callback: Callable[..., Any], *args: Any) -> frozenset[str]:
    try:
        return _proved(callback(*args))
    except RuntimeProviderError:
        raise
    except Exception as exc:
        raise RuntimeProviderError("extension_capability_mismatch") from exc


class RuntimeProviderRegistry:
    """Public exact-reference installation and resolution for runtime slots."""

    def __init__(self) -> None:
        self._common = ExtensionRegistry(source_verifier=self._verify_common)
        self._installed: dict[
            tuple[str, tuple[str, str, str]],
            tuple[dict[str, Any], Callable[[], Any], SourceClosure, CapabilityProof],
        ] = {}
        self._dependencies: dict[tuple[str, str, str], SourceClosure] = {}
        self._active: dict[tuple[str, tuple[str, str, str]], ResolvedRuntimeProvider] = {}
        self._options: dict[tuple[str, tuple[str, str, str]], dict[str, Any]] = {}
        self._guard_methods: dict[tuple[str, tuple[str, str, str]], str] = {}
        self._configured: dict[tuple[str, tuple[str, str, str]], Any] = {}
        self._provider_types: dict[tuple[str, tuple[str, str, str]], type] = {}
        self._compilers: dict[
            tuple[str, str, str], tuple[Callable[[str], Any], SourceClosure, frozenset[str]]
        ] = {}
        self._compiler_proofs: dict[
            tuple[str, str, str], Callable[[Callable[[str], Any]], frozenset[str]] | None
        ] = {}

    def _verify_common(self, source: Any, descriptor: Mapping[str, Any]) -> bool:
        key = (descriptor["category"], _reference_key(descriptor["provider_reference"]))
        if key[0] == "compiler":
            compiler_entry = self._compilers.get(key[1])
            return bool(
                compiler_entry is not None
                and getattr(source, "_selected_compiler", None) is compiler_entry[0]
                and compiler_entry[1].digest_matches(key[1][2])
                and _loaded_code_matches(
                    compiler_entry[1].root / compiler_entry[1].python_source,
                    compiler_entry[0],
                )
            )
        entry = self._installed.get(key)
        if entry is None:
            return False
        descriptor, factory, closure, _ = entry
        binding = descriptor["binding"]
        selected = getattr(source, "_selected_factory", None)
        return selected is factory and closure.verified(binding, factory)

    def register_dependency(self, reference: Mapping[str, str], closure: SourceClosure) -> None:
        if not _valid_reference(reference):
            raise RuntimeProviderError("invalid_extension_descriptor")
        key = _reference_key(reference)
        if key in self._dependencies:
            raise RuntimeProviderError("duplicate_extension_registration")
        if (
            not closure.digest_matches(reference["content_digest"])
            or not closure.manifest_verified()
        ):
            raise RuntimeProviderError("runtime_provider_unavailable")
        self._dependencies[key] = closure

    def register_compiler(
        self,
        reference: Mapping[str, str],
        compiler: Callable[[str], Any],
        closure: SourceClosure,
        *,
        capability_proof: Callable[[Callable[[str], Any]], frozenset[str]] | None = None,
    ) -> None:
        """Install an exact source compiler under its own common extension category."""
        if not _valid_reference(reference):
            raise RuntimeProviderError("invalid_extension_descriptor")
        key = _reference_key(reference)
        if key in self._compilers:
            raise RuntimeProviderError("duplicate_extension_registration")
        if (
            not closure.digest_matches(reference["content_digest"])
            or not closure.manifest_verified()
            or not _loaded_code_matches(closure.root / closure.python_source, compiler)
        ):
            raise RuntimeProviderError("runtime_provider_unavailable")
        proven = (
            _invoke_proof(capability_proof, compiler)
            if capability_proof is not None
            else frozenset()
        )
        self._compilers[key] = compiler, closure, proven
        self._compiler_proofs[key] = capability_proof

        def make_wrapper() -> _CompilerWrapper:
            return _CompilerWrapper(compiler)

        make_wrapper._selected_compiler = compiler  # type: ignore[attr-defined]
        try:
            self._common.register(
                {
                    "category": "compiler",
                    "provider_reference": dict(reference),
                    "interface_version": 1,
                    "supported_capabilities": [],
                },
                make_wrapper,
            )
        except Exception:
            del self._compilers[key]
            del self._compiler_proofs[key]
            raise

    def compiler(self, reference: Mapping[str, str]) -> Callable[[str], Any]:
        key = _reference_key(reference)
        entry = self._compilers.get(key)
        if (
            entry is None
            or not entry[1].digest_matches(reference["content_digest"])
            or not _loaded_code_matches(entry[1].root / entry[1].python_source, entry[0])
        ):
            raise RuntimeProviderError("runtime_provider_unavailable")
        try:
            handle = self._common.validate_configuration(
                {
                    "category": "compiler",
                    "provider_reference": dict(reference),
                    "interface_version": 1,
                    "supported_capabilities": [],
                },
                {"instance_id": "source-compiler"},
            )
            if self._common.health(handle) != "healthy":
                raise RuntimeProviderError("runtime_provider_unavailable")
        except ExtensionError as exc:
            raise RuntimeProviderError("runtime_provider_unavailable") from exc
        return entry[0]

    def compiler_capabilities(self, reference: Mapping[str, str]) -> dict[str, bool]:
        selected = self.compiler(reference)
        callback = self._compiler_proofs[_reference_key(reference)]
        proof = _invoke_proof(callback, selected) if callback is not None else frozenset()
        return {
            name: name in proof
            for name in (
                "deterministic",
                "pure",
                "portable",
                "semantically_introspectable",
                "process_contained",
            )
        } | {"external_io_capable": "pure" not in proof or "external_io_capable" in proof}

    def register(
        self,
        descriptor: Mapping[str, Any],
        factory: Callable[[], Any],
        closure: SourceClosure,
        *,
        capability_proof: CapabilityProof | None = None,
        evaluation_options: Mapping[str, Any] | None = None,
        guard_method: str = "evaluate_guard",
        health_check: Callable[[Any], str] | None = None,
    ) -> None:
        import jsonschema

        from .wire import _schema_registry

        kind = descriptor.get("kind")
        binding = descriptor.get("binding")
        if kind not in ("guard", "actions") or not isinstance(binding, dict):
            raise RuntimeProviderError("invalid_extension_descriptor")
        descriptor_schema = json.loads(
            (
                Path(__file__).parent / "data" / "runtime-provider-descriptor-v1.schema.json"
            ).read_text()
        )
        if not jsonschema.Draft202012Validator(
            descriptor_schema, registry=_schema_registry()
        ).is_valid(descriptor):
            raise RuntimeProviderError("invalid_extension_descriptor")
        dependencies = [_reference_key(item) for item in binding["dependencies"]]
        if dependencies != sorted(set(dependencies)):
            raise RuntimeProviderError("invalid_extension_descriptor")
        if not closure.verified(binding, factory):
            raise RuntimeProviderError("runtime_provider_unavailable")
        key = ("runtime_provider", _reference_key(binding["provider_reference"]))
        if key in self._installed:
            raise RuntimeProviderError("duplicate_extension_registration")
        checked = copy.deepcopy(dict(descriptor))
        proof = capability_proof or (lambda _provider, _binding: frozenset())
        self._installed[key] = checked, factory, closure, proof
        self._options[key] = copy.deepcopy(dict(evaluation_options or {}))
        self._guard_methods[key] = guard_method

        def make_wrapper() -> _RuntimeWrapper:
            return _RuntimeWrapper(factory, checked["binding"], health_check)

        make_wrapper._selected_factory = factory  # type: ignore[attr-defined]
        extension_descriptor = {
            "category": "runtime_provider",
            "provider_reference": checked["binding"]["provider_reference"],
            "interface_version": 1,
            "supported_capabilities": sorted(
                key for key, value in checked["binding"]["capabilities"].items() if value
            ),
        }
        try:
            self._common.register(extension_descriptor, make_wrapper)
        except Exception:
            del self._installed[key]
            del self._options[key]
            del self._guard_methods[key]
            raise

    def resolve(self, kind: str, binding: Mapping[str, Any]) -> ResolvedRuntimeProvider:
        key = ("runtime_provider", _reference_key(binding["provider_reference"]))
        entry = self._installed.get(key)
        if entry is None or entry[0] != {"kind": kind, "binding": binding}:
            raise RuntimeProviderError("runtime_provider_unavailable")
        installed_descriptor, factory, closure, proof = entry
        installed = installed_descriptor["binding"]
        if not closure.verified(installed, factory) or any(
            _reference_key(dependency) not in self._dependencies
            or not self._dependencies[_reference_key(dependency)].digest_matches(
                dependency["content_digest"]
            )
            or not self._dependencies[_reference_key(dependency)].manifest_verified()
            for dependency in binding["dependencies"]
        ):
            raise RuntimeProviderError("runtime_provider_unavailable")
        active = self._active.get(key)
        if active is not None:
            if (
                active.kind != kind
                or active.binding != binding
                or type(active.provider) is not self._provider_types[key]
                or not _loaded_code_matches(
                    closure.root / closure.python_source, type(active.provider)
                )
                or any(
                    name in getattr(active.provider, "__dict__", {})
                    for name in (
                        "evaluate_guard",
                        "evaluate_actions",
                        "inspect_guard",
                        "compile_region",
                        "health",
                    )
                )
            ):
                raise RuntimeProviderError("runtime_provider_unavailable")
            try:
                if self._common.health(self._configured[key]) != "healthy":
                    raise RuntimeProviderError("runtime_provider_unavailable")
            except ExtensionError as exc:
                raise RuntimeProviderError("runtime_provider_unavailable") from exc
            actual = _invoke_proof(proof, active.provider, binding)
            claims = {
                name: bool(binding["capabilities"].get(name) is True and name in actual)
                for name in binding["capabilities"]
            }
            claims["external_io_capable"] = claims["external_io_capable"] or "pure" not in actual
            refreshed = ResolvedRuntimeProvider(
                kind, copy.deepcopy(dict(binding)), active.provider, claims
            )
            self._active[key] = refreshed
            return refreshed
        extension_descriptor = {
            "category": "runtime_provider",
            "provider_reference": installed["provider_reference"],
            "interface_version": 1,
            "supported_capabilities": sorted(
                name for name, value in installed["capabilities"].items() if value
            ),
        }
        try:
            configured = self._common.validate_configuration(
                extension_descriptor, {"instance_id": "runtime-provider"}
            )
            provider = configured._instance["provider"]
        except (ExtensionError, KeyError) as exc:
            raise RuntimeProviderError("runtime_provider_unavailable") from exc
        if not _loaded_code_matches(closure.root / closure.python_source, type(provider)) or any(
            name in getattr(provider, "__dict__", {})
            for name in (
                "evaluate_guard",
                "evaluate_actions",
                "inspect_guard",
                "compile_region",
                "health",
            )
        ):
            raise RuntimeProviderError("runtime_provider_unavailable")
        try:
            if self._common.health(configured) != "healthy":
                raise RuntimeProviderError("runtime_provider_unavailable")
        except ExtensionError as exc:
            raise RuntimeProviderError("runtime_provider_unavailable") from exc
        if kind == "guard" and (
            self._guard_methods[key] not in vars(type(provider))
            or not callable(getattr(provider, self._guard_methods[key], None))
        ):
            raise RuntimeProviderError("runtime_provider_unavailable")
        if kind == "actions" and (
            "evaluate_actions" not in vars(type(provider))
            or not callable(getattr(provider, "evaluate_actions", None))
        ):
            raise RuntimeProviderError("runtime_provider_unavailable")
        actual = _invoke_proof(proof, provider, binding)
        claims = {
            name: bool(binding["capabilities"].get(name) is True and name in actual)
            for name in binding["capabilities"]
        }
        claims["external_io_capable"] = claims["external_io_capable"] or "pure" not in actual
        resolved = ResolvedRuntimeProvider(kind, copy.deepcopy(dict(binding)), provider, claims)
        self._active[key] = resolved
        self._configured[key] = configured
        self._provider_types[key] = type(provider)
        return resolved

    def resolve_definition(self, definition: Mapping[str, Any]) -> None:
        """Preflight every executable slot, including unreachable declarations."""

        def visit(value: Any) -> None:
            if isinstance(value, dict):
                if isinstance(value.get("guard"), dict) and "provider" in value["guard"]:
                    self.resolve("guard", value["guard"]["provider"])
                if "provider_actions" in value:
                    self.resolve("actions", value["provider_actions"])
                for child in value.values():
                    visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)

        visit(definition)

    def effective_capabilities(self, definition: Mapping[str, Any]) -> dict[str, bool]:
        """Compose verified provider guarantees for one complete definition."""
        names = (
            "deterministic",
            "pure",
            "portable",
            "semantically_introspectable",
            "process_contained",
        )
        report = dict.fromkeys(names, True)
        report["external_io_capable"] = False

        def visit(value: Any) -> None:
            if isinstance(value, dict):
                if isinstance(value.get("guard"), dict) and "provider" in value["guard"]:
                    combine(self.resolve("guard", value["guard"]["provider"]).capabilities)
                if "provider_actions" in value:
                    combine(self.resolve("actions", value["provider_actions"]).capabilities)
                for child in value.values():
                    visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)

        def combine(claims: Mapping[str, bool]) -> None:
            for name in names:
                report[name] = report[name] and claims.get(name) is True
            report["external_io_capable"] = (
                report["external_io_capable"] or claims.get("external_io_capable") is True
            )

        visit(definition)
        return report

    def invoke_guard(self, binding: Mapping[str, Any], snapshot: Mapping[str, Any]) -> bool:
        selected = self.resolve("guard", binding)
        key = ("runtime_provider", _reference_key(binding["provider_reference"]))
        result = getattr(selected.provider, self._guard_methods[key])(
            immutable_snapshot(snapshot), **copy.deepcopy(self._options[key])
        )
        if type(result) is not bool:
            raise RuntimeProviderError("runtime_provider_output_invalid")
        return result

    def invoke_actions(
        self, binding: Mapping[str, Any], snapshot: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        import jsonschema

        from .wire import _schema_registry

        selected = self.resolve("actions", binding)
        key = ("runtime_provider", _reference_key(binding["provider_reference"]))
        result = selected.provider.evaluate_actions(
            immutable_snapshot(snapshot), **copy.deepcopy(self._options[key])
        )
        schema = json.loads(
            (Path(__file__).parent / "data" / "runtime-action-output-v1.schema.json").read_text()
        )
        if not jsonschema.Draft202012Validator(schema, registry=_schema_registry()).is_valid(
            result
        ):
            raise RuntimeProviderError("runtime_provider_output_invalid")
        return copy.deepcopy(result["actions"])

    def can_inspect(self, binding: Mapping[str, Any]) -> bool:
        selected = self.resolve("guard", binding)
        return (
            selected.capabilities.get("semantically_introspectable") is True
            and "inspect_guard" in vars(type(selected.provider))
            and callable(getattr(selected.provider, "inspect_guard", None))
        )

    def inspect_guard(
        self,
        binding: Mapping[str, Any],
        snapshot: Mapping[str, Any],
        maximum_guard_evaluations: int,
        maximum_evaluation_steps: int,
    ) -> tuple[bool, int, int]:
        selected = self.resolve("guard", binding)
        if not self.can_inspect(binding):
            raise RuntimeProviderError("inspection_capability_unavailable")
        try:
            before = copy.deepcopy(getattr(selected.provider, "__dict__", {}))
        except Exception as exc:
            raise RuntimeProviderError("inspection_guard_failure") from exc
        result = selected.provider.inspect_guard(
            immutable_snapshot(snapshot), maximum_guard_evaluations, maximum_evaluation_steps
        )
        if (
            getattr(selected.provider, "__dict__", {}) != before
            or not isinstance(result, tuple)
            or len(result) != 3
            or type(result[0]) is not bool
            or type(result[1]) is not int
            or type(result[2]) is not int
            or result[1] != 1
            or result[2] < 0
            or result[2] > maximum_evaluation_steps
        ):
            raise RuntimeProviderError("inspection_guard_failure")
        return result


def immutable_snapshot(value: Any) -> Any:
    """Copy portable inputs and seal every container before native invocation."""
    if isinstance(value, Mapping):
        return types.MappingProxyType(
            {key: immutable_snapshot(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(immutable_snapshot(item) for item in value)
    return copy.deepcopy(value)


class _RuntimeWrapper:
    def __init__(
        self,
        factory: Callable[[], Any],
        binding: Mapping[str, Any],
        health_check: Callable[[Any], str] | None,
    ) -> None:
        self._selected_factory = factory
        self._binding = binding
        self._health_check = health_check

    def validate_configuration(self, configuration: Mapping[str, Any]) -> dict[str, Any]:
        return {"instance_id": configuration["instance_id"], "provider": self._selected_factory()}

    def capabilities(self, _instance: Any) -> Sequence[str]:
        return [name for name, value in self._binding["capabilities"].items() if value]

    def health(self, instance: Any) -> str:
        provider = instance["provider"]
        if self._health_check is not None:
            return self._health_check(provider)
        provider_health = getattr(provider, "health", None)
        return provider_health() if callable(provider_health) else "healthy"


class _CompilerWrapper:
    def __init__(self, compiler: Callable[[str], Any]) -> None:
        self._selected_compiler = compiler

    def validate_configuration(self, configuration: Mapping[str, Any]) -> dict[str, Any]:
        return {"instance_id": configuration["instance_id"], "compiler": self._selected_compiler}

    def capabilities(self, _instance: Any) -> Sequence[str]:
        return []

    def health(self, _instance: Any) -> str:
        return "healthy"
