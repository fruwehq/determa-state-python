"""Optional exact source compilation into a strict format-1 definition."""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from .definition import Bundle, load_bundle
from .runtime_providers import (
    RuntimeProviderError,
    RuntimeProviderRegistry,
    _executable_slots,
    _reference_key,
)
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


def _source_preflight(source: Mapping[str, Any]) -> tuple[dict[str, Any], list[tuple[str, ...]]]:
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
    generated = content["template"]
    try:
        slots = {location: (kind, value) for kind, location, value in _executable_slots(generated)}
    except (AttributeError, TypeError, ValueError) as exc:
        raise RuntimeProviderError("language_compilation_failed") from exc
    for location, region in zip(locations, regions, strict=True):
        slot = slots.get(location)
        if (
            slot is None
            or slot[0] != region["kind"]
            or (
                type(slot[1]) is not str if region["kind"] == "guard" else type(slot[1]) is not list
            )
        ):
            raise RuntimeProviderError("language_compilation_failed")
    return source_doc, locations


def _verify_compilation_manifest(manifest: Mapping[str, Any], computed: dict[str, Any]) -> None:
    provided = _artifact(
        dict(manifest), "compilation-manifest-v1.schema.json", "determa.compilation_manifest"
    )
    if provided != computed:
        raise RuntimeProviderError("language_compilation_failed")


def compile_language_source(
    source: Mapping[str, Any],
    registry: RuntimeProviderRegistry,
    *,
    manifest: Mapping[str, Any] | None = None,
    maximum_compilation_steps: int = 1000,
) -> Bundle:
    """Compile grammar slots and retain sealed version-1 source/manifest evidence.

    ``Bundle.source_compilation`` describes historical source guarantees; the
    generated bundle keeps its independently verified runtime capability profile.
    """
    source_doc, locations = _source_preflight(source)
    content = source_doc["content"]
    regions = content["regions"]
    generated = copy.deepcopy(content["template"])
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
    compilers = [region["provider_reference"] for region in regions]
    for region in regions:
        registry.compiler(region["provider_reference"])
    compiler_claims_before = [registry.compiler_capabilities(reference) for reference in compilers]
    for location, region in zip(locations, regions, strict=True):
        if maximum_compilation_steps <= 0:
            raise RuntimeProviderError("language_compilation_limit_exceeded")
        maximum_compilation_steps -= 1
        compiler = registry.compiler(region["provider_reference"])
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
    expected_closure = sorted(
        {
            *(_reference_key(item) for item in dependencies),
            *(_reference_key(item) for item in compilers),
        }
    )
    effective = dict.fromkeys(
        ("deterministic", "pure", "portable", "semantically_introspectable", "process_contained"),
        True,
    )
    effective["external_io_capable"] = False
    for claims in [
        *compiler_claims_before,
        *(registry.compiler_capabilities(reference) for reference in compilers),
        registry.effective_capabilities(bundle.raw),
    ]:
        for name in effective:
            effective[name] = (
                effective[name] or claims[name]
                if name == "external_io_capable"
                else effective[name] and claims[name]
            )
    record = {
        "source_artifact_digest": source_doc["artifact_digest"],
        "compiler_providers": [
            dict(zip(("identifier", "version", "content_digest"), item, strict=True))
            for item in expected_closure
        ],
        "generated_validated_bundle_fingerprint": bundle.fingerprint,
        "source_capabilities": effective,
    }
    manifest_doc = {
        "artifact_format": "determa.compilation_manifest",
        "artifact_schema_version": 1,
        "content": record,
        "artifact_digest": hash_value(["determa.compilation_manifest", "1", typed_value(record)]),
    }
    if manifest is not None:
        _verify_compilation_manifest(manifest, manifest_doc)
    return replace(bundle, source_compilation={"source": source_doc, "manifest": manifest_doc})
