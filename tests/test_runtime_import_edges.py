"""Compiler selection binds import edges to the declared executable closure."""

import hashlib
import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest

from determa.state import RuntimeProviderError, RuntimeProviderRegistry, SourceClosure


def installed_compiler(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str):
    sources = {
        "edgepkg/__init__.py": "values = []\n",
        "edgepkg/sub/__init__.py": "",
        "edgepkg/sub/helper.py": "def choose(value):\n    return value\n",
        "compiler.py": source,
    }
    modules = {}
    for path, body in sources.items():
        file = tmp_path / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(body)
        name = path.removesuffix(".py").replace("/", ".").removesuffix(".__init__")
        spec = importlib.util.spec_from_file_location(name, file)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        modules[name] = module
        if "." in name:
            parent, attr = name.rsplit(".", 1)
            setattr(modules[parent], attr, module)
    manifest = {
        "format": "test-import-edges",
        "version": 1,
        "files": [
            {"path": path, "sha256": "sha256:" + hashlib.sha256(body.encode()).hexdigest()}
            for path, body in sources.items()
        ],
    }
    (tmp_path / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    )
    closure = SourceClosure(
        tmp_path, tuple(sources), "manifest.json", b"test-edges\0", "compiler.py"
    )
    reference = {
        "identifier": "edge-compiler",
        "version": "1.0.0",
        "content_digest": closure.digest(),
    }
    return modules, closure, reference


@pytest.mark.parametrize("local", [False, True])
@pytest.mark.parametrize("edge", ["sub", "helper"])
def test_dotted_import_rebinding_refused_before_compiler_execution(
    tmp_path, monkeypatch, local, edge
):
    source = (
        "def compile_source(value):\n    import edgepkg.sub.helper\n"
        "    return edgepkg.sub.helper.choose(value)\n"
        if local
        else "import edgepkg.sub.helper\ndef compile_source(value):\n"
        "    return edgepkg.sub.helper.choose(value)\n"
    )
    modules, closure, reference = installed_compiler(tmp_path, monkeypatch, source)
    registry = RuntimeProviderRegistry()
    registry.register_compiler(reference, modules["compiler"].compile_source, closure)
    assert registry.compiler(reference)("original") == "original"
    rogue = types.ModuleType("rogue")
    rogue.choose = lambda _: "substituted"
    rogue.helper = rogue
    parent = modules["edgepkg" if edge == "sub" else "edgepkg.sub"]
    setattr(parent, edge, rogue)
    with pytest.raises(RuntimeProviderError, match="runtime_provider_unavailable"):
        registry.compiler(reference)


@pytest.mark.parametrize("member_kind", ["module", "callable"])
@pytest.mark.parametrize("after_registration", [False, True])
def test_local_from_import_rejects_undeclared_executable_member(
    tmp_path, monkeypatch, member_kind, after_registration
):
    expression = "helper.choose(value)" if member_kind == "module" else "helper(value)"
    source = (
        f"def compile_source(value):\n    from edgepkg.sub import helper\n    return {expression}\n"
    )
    modules, closure, reference = installed_compiler(tmp_path, monkeypatch, source)
    registry = RuntimeProviderRegistry()
    if after_registration:
        registry.register_compiler(reference, modules["compiler"].compile_source, closure)
    rogue = types.ModuleType("rogue")
    rogue.choose = lambda _: "substituted"
    modules["edgepkg.sub"].helper = rogue if member_kind == "module" else rogue.choose
    with pytest.raises(RuntimeProviderError, match="runtime_provider_unavailable"):
        if after_registration:
            registry.compiler(reference)
        else:
            registry.register_compiler(reference, modules["compiler"].compile_source, closure)


def test_local_from_import_preserves_mutable_data(tmp_path, monkeypatch):
    source = (
        "def compile_source(value):\n    from edgepkg import values\n"
        "    values.append(value)\n    return values\n"
    )
    modules, closure, reference = installed_compiler(tmp_path, monkeypatch, source)
    registry = RuntimeProviderRegistry()
    registry.register_compiler(reference, modules["compiler"].compile_source, closure)
    assert registry.compiler(reference)("first") == ["first"]
    assert registry.compiler(reference)("second") == ["first", "second"]
