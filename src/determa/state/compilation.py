"""Optional exact source compilation into a strict format-1 definition."""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .definition import Bundle, load_bundle
from .runtime_providers import RuntimeProviderError, RuntimeProviderRegistry, _reference_key
from .wire import _schema_registry, hash_value, typed_value


def _artifact(value: Any, schema_name: str, artifact_format: str) -> dict[str, Any]:
    import jsonschema

    path = Path(__file__).parent / "data" / schema_name
    schema = json.loads(path.read_text())
    if type(value) is not dict or not jsonschema.Draft202012Validator(
        schema, registry=_schema_registry()
    ).is_valid(value):
        raise RuntimeProviderError("language_compilation_failed")
    expected = hash_value([artifact_format, "1", typed_value(value["content"])])
    if value["artifact_digest"] != expected:
        raise RuntimeProviderError("language_compilation_failed")
    return copy.deepcopy(value)


def _tokens(locator: str) -> tuple[str, ...]:
    parts = locator.split("/")
    if not locator.startswith("/") or len(parts) < 2:
        raise RuntimeProviderError("language_compilation_failed")
    tokens = []
    for part in parts[1:]:
        if any(
            part[index] == "~" and part[index : index + 2] not in {"~0", "~1"}
            for index in range(len(part))
        ):
            raise RuntimeProviderError("language_compilation_failed")
        decoded = part.replace("~1", "/").replace("~0", "~")
        if decoded.replace("~", "~0").replace("/", "~1") != part:
            raise RuntimeProviderError("language_compilation_failed")
        tokens.append(decoded)
    return tuple(tokens)


def _array_index(token: str) -> int:
    if not token.isascii() or not token.isdecimal() or (len(token) > 1 and token[0] == "0"):
        raise RuntimeProviderError("language_compilation_failed")
    return int(token)


def _slot(template: dict[str, Any], tokens: tuple[str, ...]) -> tuple[Any, str]:
    parent: Any = template
    try:
        for token in tokens[:-1]:
            parent = parent[_array_index(token)] if isinstance(parent, list) else parent[token]
        key = tokens[-1]
        _ = parent[_array_index(key)] if isinstance(parent, list) else parent[key]
    except (IndexError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeProviderError("language_compilation_failed") from exc
    return parent, key


def compile_language_source(
    source: Mapping[str, Any],
    registry: RuntimeProviderRegistry,
    *,
    manifest: Mapping[str, Any] | None = None,
    maximum_compilation_steps: int = 1000,
) -> Bundle:
    """Verify provenance, compile disjoint regions, and strictly load the generated bundle."""
    source_doc = _artifact(
        dict(source), "language-source-v1.schema.json", "determa.language_source"
    )
    content = source_doc["content"]
    regions = content["regions"]
    locations = [_tokens(region["locator"]) for region in regions]
    if len(set(locations)) != len(locations) or any(
        a != b and (a[: len(b)] == b or b[: len(a)] == a) for a in locations for b in locations
    ):
        raise RuntimeProviderError("language_compilation_failed")
    dependencies = content["dependencies"]
    if dependencies != sorted(dependencies, key=_reference_key) or len(
        {_reference_key(item) for item in dependencies}
    ) != len(dependencies):
        raise RuntimeProviderError("language_compilation_failed")
    for dependency in dependencies:
        if _reference_key(dependency) not in registry._dependencies:
            raise RuntimeProviderError("runtime_provider_unavailable")
        closure = registry._dependencies[_reference_key(dependency)]
        if (
            not closure.digest_matches(dependency["content_digest"])
            or not closure.manifest_verified()
        ):
            raise RuntimeProviderError("runtime_provider_unavailable")
    generated = copy.deepcopy(content["template"])
    for location, region in zip(locations, regions, strict=True):
        parent, key = _slot(generated, location)
        value = parent[_array_index(key)] if isinstance(parent, list) else parent[key]
        if (region["kind"] == "guard" and (key != "guard" or type(value) is not str)) or (
            region["kind"] == "actions"
            and (key not in {"action", "entry", "exit"} or type(value) is not list)
        ):
            raise RuntimeProviderError("language_compilation_failed")
    compilers = [region["provider_reference"] for region in regions]
    compiled = [registry.compiler(region["provider_reference"]) for region in regions]
    for location, region, compiler in zip(locations, regions, compiled, strict=True):
        if maximum_compilation_steps <= 0:
            raise RuntimeProviderError("language_compilation_limit_exceeded")
        maximum_compilation_steps -= 1
        try:
            replacement = compiler(region["source"])
        except Exception as exc:
            raise RuntimeProviderError("language_compilation_failed") from exc
        parent, key = _slot(generated, location)
        if isinstance(parent, list):
            parent[_array_index(key)] = replacement
        else:
            parent[key] = replacement
    try:
        bundle = load_bundle(generated, runtime_providers=registry)
    except Exception as exc:
        raise RuntimeProviderError("language_compilation_failed") from exc
    if manifest is not None:
        manifest_doc = _artifact(
            dict(manifest), "compilation-manifest-v1.schema.json", "determa.compilation_manifest"
        )
        record = manifest_doc["content"]
        expected_closure = sorted(
            {
                *(_reference_key(item) for item in dependencies),
                *(_reference_key(item) for item in compilers),
            }
        )
        effective = dict.fromkeys(
            (
                "deterministic",
                "pure",
                "portable",
                "semantically_introspectable",
                "process_contained",
            ),
            True,
        )
        effective["external_io_capable"] = False
        for reference in compilers:
            claims = registry.compiler_capabilities(reference)
            for name in effective:
                effective[name] = (
                    effective[name] or claims[name]
                    if name == "external_io_capable"
                    else effective[name] and claims[name]
                )
        runtime_claims = registry.effective_capabilities(bundle.raw)
        for name in effective:
            effective[name] = (
                effective[name] or runtime_claims[name]
                if name == "external_io_capable"
                else effective[name] and runtime_claims[name]
            )
        if (
            record["source_artifact_digest"] != source_doc["artifact_digest"]
            or [_reference_key(item) for item in record["compiler_providers"]] != expected_closure
            or record["generated_validated_bundle_fingerprint"] != bundle.fingerprint
            or record["source_capabilities"] != effective
        ):
            raise RuntimeProviderError("language_compilation_failed")
    return bundle
