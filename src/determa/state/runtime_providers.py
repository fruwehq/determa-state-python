"""Exact installed runtime providers for optional format-1 language slots.

The registry owns source and configured-instance checks. A binding in a definition
names code; it does not install code or establish a capability by itself.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import importlib
import importlib.util
import json
import sys
import types
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .extensions import ExtensionError, ExtensionRegistry

SourceIdentityVerifier = Callable[[Path, Any], bool]


def _default_record(value: Any) -> tuple[str, Any]:
    if type(value) is tuple:
        return "tuple", tuple(_default_record(item) for item in value)
    if type(value) is frozenset:
        return "frozenset", frozenset(_default_record(item) for item in value)
    if type(value) in {type(None), bool, int, float, str, bytes, complex}:
        return "value", (type(value), value)
    return "identity", value


def _default_still_matches(record: tuple[str, Any], value: Any) -> bool:
    kind, saved = record
    if kind == "tuple":
        return (
            type(value) is tuple
            and len(value) == len(saved)
            and all(
                _default_still_matches(item, actual)
                for item, actual in zip(saved, value, strict=True)
            )
        )
    if kind == "frozenset":
        return type(value) is frozenset and _default_record(value) == record
    if kind == "value":
        expected_type, expected_value = saved
        return type(value) is expected_type and value == expected_value
    return value is saved


@dataclass(frozen=True)
class _CallableBinding:
    function: types.FunctionType
    positional: tuple[tuple[str, Any], ...]
    keyword: dict[str, tuple[str, Any]]


def _bound_provider_method(provider: Any, name: str) -> types.MethodType:
    """Select the first concrete MRO descriptor without invoking instance hooks."""
    try:
        if name in object.__getattribute__(provider, "__dict__"):
            raise RuntimeProviderError("runtime_provider_unavailable")
    except AttributeError:
        pass
    for owner in type.__getattribute__(type(provider), "__mro__"):
        descriptor = type.__getattribute__(owner, "__dict__").get(name)
        if descriptor is not None:
            if not isinstance(descriptor, types.FunctionType):
                raise RuntimeProviderError("runtime_provider_unavailable")
            return types.MethodType(descriptor, provider)
    raise RuntimeProviderError("runtime_provider_unavailable")


def _literal_matches(actual: Any, expected: Any) -> bool:
    if type(actual) is not type(expected):
        return False
    if type(actual) in {list, tuple}:
        return len(actual) == len(expected) and all(
            _literal_matches(a, b) for a, b in zip(actual, expected, strict=True)
        )
    if type(actual) in {set, frozenset}:
        return len(actual) == len(expected) and all(
            any(_literal_matches(item, candidate) for candidate in expected) for item in actual
        )
    if type(actual) is dict:
        return len(actual) == len(expected) and all(
            any(
                _literal_matches(actual_key, expected_key)
                and _literal_matches(actual_value, expected_value)
                for expected_key, expected_value in expected.items()
            )
            for actual_key, actual_value in actual.items()
        )
    return bool(actual == expected)


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

    def verified(
        self,
        binding: Mapping[str, Any],
        executable: Any,
        *,
        anchors: dict[tuple[str, str, str], _CallableBinding] | None = None,
        capture: bool = False,
        identity_verifier: SourceIdentityVerifier | None = None,
        trusted_sources: frozenset[Path] | None = None,
    ) -> bool:
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
            return _loaded_code_matches(
                self.root / self.python_source,
                executable,
                anchors=anchors,
                capture=capture,
                identity_verifier=identity_verifier,
                trusted_sources=trusted_sources or self.trusted_python_sources(),
            )
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return False

    def manifest_digest(self) -> str:
        return "sha256:" + hashlib.sha256((self.root / self.manifest).read_bytes()).hexdigest()

    def trusted_python_sources(self) -> frozenset[Path]:
        return frozenset(
            (self.root / name).resolve() for name in self.paths if name.endswith(".py")
        )

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


def _loaded_code_matches(
    source_path: Path,
    executable: Any,
    *,
    anchors: dict[tuple[str, str, str], _CallableBinding] | None = None,
    capture: bool = False,
    identity_verifier: SourceIdentityVerifier | None = None,
    trusted_sources: frozenset[Path] | None = None,
    visited: set[tuple[Path, str]] | None = None,
) -> bool:
    """Compare selected Python code and its module callbacks to installed bytes."""
    module_name = (
        vars(executable).get("__name__")
        if isinstance(executable, types.ModuleType)
        else (
            type.__getattribute__(executable, "__module__")
            if isinstance(executable, type)
            else getattr(executable, "__module__", None)
        )
    )
    if not isinstance(module_name, str):
        return False
    module = sys.modules.get(module_name)
    if (
        module is None
        or (isinstance(executable, types.ModuleType) and module is not executable)
        or not isinstance(vars(module).get("__file__"), str)
        or Path(vars(module)["__file__"]).resolve() != source_path.resolve()
    ):
        return False
    if trusted_sources is None:
        trusted_sources = frozenset({source_path.resolve()})
    if visited is None:
        visited = set()
    visit_key = (source_path.resolve(), module_name)
    if visit_key in visited:
        return True
    visited.add(visit_key)

    def host_verified(selected: Any) -> bool:
        if identity_verifier is None:
            return False
        try:
            return identity_verifier(source_path, selected) is True
        except Exception:
            return False

    if source_path.suffix != ".py":
        return host_verified(executable)
    source = source_path.read_bytes()
    tree = ast.parse(source)
    compiled = compile(source, str(source_path), "exec", dont_inherit=True)

    def import_verified(imported: Any) -> bool:
        """An import can execute only from declared source or host attestation."""
        if not isinstance(imported, types.ModuleType):
            return False
        imported_file = vars(imported).get("__file__")
        if type(imported) is not types.ModuleType:
            return host_verified(imported)
        if not isinstance(imported_file, str) or not imported_file:
            return host_verified(imported)
        imported_path = Path(imported_file).resolve()
        if imported_path in trusted_sources:
            return _loaded_code_matches(
                imported_path,
                imported,
                anchors=anchors,
                capture=capture,
                identity_verifier=identity_verifier,
                trusted_sources=trusted_sources,
                visited=visited,
            )
        if identity_verifier is None:
            return False
        try:
            return identity_verifier(imported_path, imported) is True
        except Exception:
            return False

    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported = sys.modules.get(alias.name)
                loaded = (
                    sys.modules.get(alias.name.split(".", 1)[0])
                    if alias.asname is None and "." in alias.name
                    else imported
                )
                if (
                    loaded is None
                    or imported is None
                    or vars(module).get(alias.asname or alias.name.split(".", 1)[0]) is not loaded
                    or not import_verified(loaded)
                    or (imported is not loaded and not import_verified(imported))
                ):
                    return False
        elif isinstance(node, ast.ImportFrom) and node.module != "__future__":
            resolved_name = importlib.util.resolve_name(
                "." * node.level + (node.module or ""), vars(module).get("__package__")
            )
            loaded = sys.modules.get(resolved_name)
            if loaded is None or not import_verified(loaded):
                return False
            if any(
                alias.name == "*"
                or (
                    isinstance(vars(module).get(alias.asname or alias.name), types.ModuleType)
                    and not import_verified(vars(module)[alias.asname or alias.name])
                )
                or (
                    (
                        callable(vars(module).get(alias.asname or alias.name))
                        or isinstance(
                            vars(module).get(alias.asname or alias.name), types.ModuleType
                        )
                        or callable(vars(loaded).get(alias.name))
                        or isinstance(vars(loaded).get(alias.name), types.ModuleType)
                    )
                    and vars(module).get(alias.asname or alias.name)
                    is not vars(loaded).get(alias.name)
                )
                for alias in node.names
            ):
                return False

    # Local imports do not create module globals. Require their target modules to
    # have been loaded and verified before selecting code that could import them.
    for nested in ast.walk(tree):
        if nested in tree.body:
            continue
        if isinstance(nested, ast.Import):
            if any(not import_verified(sys.modules.get(alias.name)) for alias in nested.names):
                return False
        elif isinstance(nested, ast.ImportFrom) and nested.module != "__future__":
            resolved_name = importlib.util.resolve_name(
                "." * nested.level + (nested.module or ""), vars(module).get("__package__")
            )
            if not import_verified(sys.modules.get(resolved_name)):
                return False

    def matches(
        actual: Any,
        expected: types.CodeType,
        declaration: ast.AST | None,
        key: tuple[str, str, str],
    ) -> bool:
        if not isinstance(actual, types.FunctionType) or not isinstance(
            declaration, (ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            return False
        source_backed = (
            actual.__code__ == expected
            and actual.__globals__ is vars(module)
            and actual.__closure__ is None
        )
        if not source_backed and not host_verified(actual):
            return False
        positional = actual.__defaults__
        keyword = actual.__kwdefaults__
        if (positional is not None and type(positional) is not tuple) or (
            keyword is not None and type(keyword) is not dict
        ):
            return False
        positional_values = positional or ()
        keyword_values = keyword or {}
        expected_keywords = {
            arg.arg
            for arg, default in zip(
                declaration.args.kwonlyargs, declaration.args.kw_defaults, strict=True
            )
            if default is not None
        }
        if source_backed and (
            len(positional_values) != len(declaration.args.defaults)
            or set(keyword_values) != expected_keywords
        ):
            return False
        if anchors is not None and not capture:
            anchored = anchors.get(key)
            return bool(
                anchored is not None
                and actual is anchored.function
                and len(positional_values) == len(anchored.positional)
                and all(
                    _default_still_matches(saved, value)
                    for saved, value in zip(anchored.positional, positional_values, strict=True)
                )
                and keyword_values.keys() == anchored.keyword.keys()
                and all(
                    _default_still_matches(anchored.keyword[name], keyword_values[name])
                    for name in anchored.keyword
                )
            )
        if source_backed:
            try:
                source_positional = [ast.literal_eval(item) for item in declaration.args.defaults]
                source_keyword = {
                    arg.arg: ast.literal_eval(default)
                    for arg, default in zip(
                        declaration.args.kwonlyargs, declaration.args.kw_defaults, strict=True
                    )
                    if default is not None
                }
                if not (
                    all(
                        _literal_matches(actual_value, source_value)
                        for actual_value, source_value in zip(
                            positional_values, source_positional, strict=True
                        )
                    )
                    and all(
                        _literal_matches(keyword_values[name], source_keyword[name])
                        for name in source_keyword
                    )
                ):
                    return False
            except (ValueError, TypeError, SyntaxError, MemoryError):
                if not host_verified(actual):
                    return False
        if anchors is not None and capture:
            anchors[key] = _CallableBinding(
                actual,
                tuple(_default_record(value) for value in positional_values),
                {name: _default_record(value) for name, value in keyword_values.items()},
            )
        return True

    for code in compiled.co_consts:
        if not isinstance(code, types.CodeType) or code.co_name.startswith("<"):
            continue
        value = vars(module).get(code.co_name)
        if not isinstance(value, (type, types.FunctionType)):
            return False
        loaded_module = (
            type.__getattribute__(value, "__module__")
            if isinstance(value, type)
            else object.__getattribute__(value, "__module__")
        )
        if loaded_module != module_name:
            return False
        if isinstance(value, type):
            if type(value) is not type and not host_verified(value):
                return False
            mro = type.__getattribute__(value, "__mro__")
            for base in mro[1:-1]:
                if type.__getattribute__(base, "__module__") == module_name:
                    continue
                base_module = sys.modules.get(type.__getattribute__(base, "__module__"))
                if base_module is None:
                    return False
                base_path = Path(vars(base_module).get("__file__", "")).resolve()
                if base_path in trusted_sources:
                    if not _loaded_code_matches(
                        base_path,
                        base,
                        anchors=anchors,
                        capture=capture,
                        identity_verifier=identity_verifier,
                        trusted_sources=trusted_sources,
                        visited=visited,
                    ):
                        return False
                elif not host_verified(base):
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
                or (
                    not declaration.bases and type.__getattribute__(value, "__bases__") != (object,)
                )
                or (
                    declaration.bases
                    and (
                        len(type.__getattribute__(value, "__bases__")) != len(declaration.bases)
                        or any(
                            not isinstance(base, ast.Name)
                            or vars(module).get(base.id)
                            is not type.__getattribute__(value, "__bases__")[index]
                            for index, base in enumerate(declaration.bases)
                        )
                    )
                )
            ):
                return False
            declared_names: set[str] = set()
            for statement in declaration.body:
                if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    declared_names.add(statement.name)
                elif isinstance(statement, (ast.Assign, ast.AnnAssign)):
                    targets = (
                        statement.targets
                        if isinstance(statement, ast.Assign)
                        else [statement.target]
                    )
                    declared_names.update(
                        target.id for target in targets if isinstance(target, ast.Name)
                    )
                    if any(
                        isinstance(target, ast.Name) and target.id == "__slots__"
                        for target in targets
                    ):
                        try:
                            if statement.value is None:
                                raise ValueError("missing slots expression")
                            slots = ast.literal_eval(statement.value)
                        except (ValueError, TypeError, SyntaxError):
                            if not host_verified(value):
                                return False
                        else:
                            declared_names.update((slots,) if isinstance(slots, str) else slots)
            implicit_names = {
                "__module__",
                "__doc__",
                "__dict__",
                "__weakref__",
                "__annotations__",
                "__firstlineno__",
                "__static_attributes__",
            }
            class_dictionary = type.__getattribute__(value, "__dict__")
            if not set(class_dictionary).issubset(declared_names | implicit_names):
                return False
            for method_code in code.co_consts:
                if not isinstance(method_code, types.CodeType) or method_code.co_name.startswith(
                    "<"
                ):
                    continue
                method = class_dictionary.get(method_code.co_name)
                if isinstance(method, property):
                    method = method.fget
                elif isinstance(method, (staticmethod, classmethod)):
                    method = method.__func__
                method_declaration = next(
                    (
                        item
                        for item in declaration.body
                        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and item.name == method_code.co_name
                    ),
                    None,
                )
                if not matches(
                    method,
                    method_code,
                    method_declaration,
                    (module_name, code.co_name, method_code.co_name),
                ):
                    return False
        elif not matches(
            value,
            code,
            next(
                (
                    item
                    for item in tree.body
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and item.name == code.co_name
                ),
                None,
            ),
            (module_name, "", code.co_name),
        ):
            return False
    if isinstance(executable, types.ModuleType):
        return sys.modules.get(module_name) is executable
    name = (
        type.__getattribute__(executable, "__name__")
        if isinstance(executable, type)
        else getattr(executable, "__name__", "")
    )
    return vars(module).get(name) is executable


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

    def __init__(self, *, source_identity_verifier: SourceIdentityVerifier | None = None) -> None:
        self._common = ExtensionRegistry(source_verifier=self._verify_common)
        self._source_identity_verifier = source_identity_verifier
        self._loaded_anchors: dict[
            tuple[str, tuple[str, str, str]], dict[tuple[str, str, str], _CallableBinding]
        ] = {}
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
                and compiler_entry[1].manifest_verified()
                and _loaded_code_matches(
                    compiler_entry[1].root / compiler_entry[1].python_source,
                    compiler_entry[0],
                    anchors=self._loaded_anchors.get(key),
                    identity_verifier=self._source_identity_verifier,
                    trusted_sources=compiler_entry[1].trusted_python_sources(),
                )
            )
        entry = self._installed.get(key)
        if entry is None:
            return False
        descriptor, factory, closure, _ = entry
        binding = descriptor["binding"]
        selected = getattr(source, "_selected_factory", None)
        return selected is factory and closure.verified(
            binding,
            factory,
            anchors=self._loaded_anchors.get(key),
            identity_verifier=self._source_identity_verifier,
            trusted_sources=self._trusted_sources(binding, closure),
        )

    def _trusted_sources(
        self, binding: Mapping[str, Any], closure: SourceClosure
    ) -> frozenset[Path]:
        sources = set(closure.trusted_python_sources())
        for reference in binding["dependencies"]:
            dependency = self._dependencies.get(_reference_key(reference))
            if (
                dependency is not None
                and dependency.digest_matches(reference["content_digest"])
                and dependency.manifest_verified()
            ):
                sources.update(dependency.trusted_python_sources())
        return frozenset(sources)

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
        anchors: dict[tuple[str, str, str], _CallableBinding] = {}
        if (
            not closure.digest_matches(reference["content_digest"])
            or not closure.manifest_verified()
            or not _loaded_code_matches(
                closure.root / closure.python_source,
                compiler,
                anchors=anchors,
                capture=True,
                identity_verifier=self._source_identity_verifier,
                trusted_sources=closure.trusted_python_sources(),
            )
        ):
            raise RuntimeProviderError("runtime_provider_unavailable")
        proven = (
            _invoke_proof(capability_proof, compiler)
            if capability_proof is not None
            else frozenset()
        )
        self._compilers[key] = compiler, closure, proven
        self._loaded_anchors[("compiler", key)] = anchors
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
            del self._loaded_anchors[("compiler", key)]
            raise

    def compiler(self, reference: Mapping[str, str]) -> Callable[[str], Any]:
        key = _reference_key(reference)
        entry = self._compilers.get(key)
        if (
            entry is None
            or not entry[1].digest_matches(reference["content_digest"])
            or not entry[1].manifest_verified()
            or not _loaded_code_matches(
                entry[1].root / entry[1].python_source,
                entry[0],
                anchors=self._loaded_anchors.get(("compiler", key)),
                identity_verifier=self._source_identity_verifier,
                trusted_sources=entry[1].trusted_python_sources(),
            )
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
        key = ("runtime_provider", _reference_key(binding["provider_reference"]))
        if key in self._installed:
            raise RuntimeProviderError("duplicate_extension_registration")
        anchors: dict[tuple[str, str, str], _CallableBinding] = {}
        if not closure.verified(
            binding,
            factory,
            anchors=anchors,
            capture=True,
            identity_verifier=self._source_identity_verifier,
            trusted_sources=self._trusted_sources(binding, closure),
        ):
            raise RuntimeProviderError("runtime_provider_unavailable")
        checked = copy.deepcopy(dict(descriptor))
        proof = capability_proof or (lambda _provider, _binding: frozenset())
        self._installed[key] = checked, factory, closure, proof
        self._loaded_anchors[key] = anchors
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
            del self._loaded_anchors[key]
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
        if not closure.verified(
            installed,
            factory,
            anchors=self._loaded_anchors.get(key),
            identity_verifier=self._source_identity_verifier,
            trusted_sources=self._trusted_sources(binding, closure),
        ) or any(
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
                    closure.root / closure.python_source,
                    type(active.provider),
                    anchors=self._loaded_anchors.get(key),
                    identity_verifier=self._source_identity_verifier,
                    trusted_sources=self._trusted_sources(binding, closure),
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
        if not _loaded_code_matches(
            closure.root / closure.python_source,
            type(provider),
            anchors=self._loaded_anchors.get(key),
            identity_verifier=self._source_identity_verifier,
            trusted_sources=self._trusted_sources(binding, closure),
        ) or any(
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
        _bound_provider_method(
            provider, self._guard_methods[key] if kind == "guard" else "evaluate_actions"
        )
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
        result = _bound_provider_method(selected.provider, self._guard_methods[key])(
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
        result = _bound_provider_method(selected.provider, "evaluate_actions")(
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
        if selected.capabilities.get("semantically_introspectable") is not True:
            return False
        try:
            _bound_provider_method(selected.provider, "inspect_guard")
        except RuntimeProviderError:
            return False
        return True

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
        result = _bound_provider_method(selected.provider, "inspect_guard")(
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


def guard_snapshot(
    activation: Mapping[str, Any], event: Mapping[str, Any] | None, binding: Mapping[str, Any]
) -> dict[str, Any]:
    """The one native provider view used by dispatch and candidate inspection."""
    from .wire import typed_value

    result: dict[str, Any] = {}
    inputs = binding["input_types"]
    if "event" in inputs and event is not None:
        native_event = copy.deepcopy(dict(event))
        native_event["payload"] = typed_value(native_event["payload"])
        result["event"] = native_event
    if "variables" in inputs:
        result["variables"] = typed_value(
            {
                name: value
                for name, value in activation.items()
                if name not in {"event", "owner", "env"}
            }
        )
    for name in ("owner", "env"):
        if name in inputs and name in activation:
            result[name] = typed_value(activation[name])
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
