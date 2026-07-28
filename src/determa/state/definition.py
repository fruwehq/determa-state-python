"""Format-1 bundle loading, normalization, and deterministic fingerprinting."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from . import yaml12
from .errors import ValidationError

BundleSource = str | Mapping[str, Any]


def _utf8_key(value: str) -> bytes:
    return value.encode("utf-8", errors="strict")


def _normalize_typed_literal(declaration: dict[str, Any], member: str) -> None:
    if member not in declaration:
        return
    value = declaration[member]
    if (
        declaration.get("type") == "float"
        and isinstance(value, int)
        and not isinstance(value, bool)
    ):
        value = float(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError("numeric_value_out_of_range")
        value = 0.0 if value == 0.0 else value
    declaration[member] = value


def _normalize_event(declaration: dict[str, Any]) -> None:
    declaration.setdefault("direction", "internal")
    for field in (declaration.get("payload") or {}).values():
        field.setdefault("required", False)
        _normalize_typed_literal(field, "default")


def _normalize_action(action: dict[str, Any]) -> None:
    send = action.get("send")
    if isinstance(send, dict) and "to" not in send and "targets" not in send:
        send["to"] = {"self": True}


def _normalize_transition(transition: dict[str, Any], *, event_transition: bool) -> None:
    if event_transition:
        transition.setdefault("lang", "cel")
    for action in transition.get("action") or []:
        _normalize_action(action)


def _normalize_state(state: dict[str, Any], *, pointer: str) -> None:
    if "choice" in state:
        for branch in state["choice"]:
            _normalize_transition(branch, event_transition=False)
        return
    state.setdefault("type", "simple")
    if state["type"] == "composite":
        state.setdefault("history", "none")
    for declaration in (state.get("variables") or {}).values():
        if declaration.get("type") != "instance_reference":
            declaration.setdefault("input", False)
            declaration.setdefault("external", False)
        _normalize_typed_literal(declaration, "init")
    for action in state.get("entry") or []:
        _normalize_action(action)
    for action in state.get("exit") or []:
        _normalize_action(action)
    initial = state.get("initial")
    if isinstance(initial, dict):
        _normalize_transition(initial, event_transition=False)
    for transition_or_list in (state.get("on_events") or {}).values():
        transitions = (
            transition_or_list if isinstance(transition_or_list, list) else [transition_or_list]
        )
        for transition in transitions:
            _normalize_transition(transition, event_transition=True)
    for name, child in (state.get("states") or {}).items():
        _normalize_state(child, pointer=f"{pointer}/states/{_escape_pointer(name)}")
    for index, placement in enumerate(state.get("components") or []):
        inline_root = placement.get("root")
        if isinstance(inline_root, dict):
            _normalize_state(inline_root, pointer=f"{pointer}/components/{index}/root")


def _escape_pointer(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def normalize_bundle(raw: dict[str, Any]) -> dict[str, Any]:
    """Materialize only the normative format-1 defaults."""
    normalized = copy.deepcopy(raw)
    for declaration in (normalized.get("events") or {}).values():
        _normalize_event(declaration)
    for machine_index, machine in enumerate(normalized.get("machines") or []):
        machine.setdefault("version", 1)
        languages = machine.setdefault("languages", {})
        languages.setdefault("guard", "cel")
        languages.setdefault("action", "determa")
        for declaration in (machine.get("events") or {}).values():
            _normalize_event(declaration)
        _normalize_state(machine["root"], pointer=f"/machines/{machine_index}/root")
    return normalized


def _typed_value(value: Any) -> list[Any]:
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["boolean", value]
    if isinstance(value, str):
        return ["string", value]
    if isinstance(value, int):
        return ["integer", str(value)]
    if isinstance(value, float):
        bits = struct.pack("!d", 0.0 if value == 0.0 else value).hex()
        return ["float", bits]
    if isinstance(value, list):
        return ["list", [_typed_value(item) for item in value]]
    if isinstance(value, dict):
        entries = [[key, _typed_value(value[key])] for key in sorted(value, key=_utf8_key)]
        return ["map", entries]
    raise ValidationError("non_json_value")


def canonical_json(value: Any) -> str:
    """Canonical JSON for identity tuples, whose members are already normalized."""
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def hash_identity(value: Any) -> str:
    encoded = canonical_json(value).encode("utf-8", errors="strict")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def bundle_fingerprint(normalized: dict[str, Any]) -> str:
    return hash_identity(["determa-validated-bundle-fingerprint-1", _typed_value(normalized)])


@dataclass(frozen=True)
class Bundle:
    """A validated, normalized format-1 bundle."""

    raw: dict[str, Any]
    fingerprint: str

    @property
    def namespace(self) -> str:
        return str(self.raw["namespace"])

    @property
    def machines(self) -> list[dict[str, Any]]:
        return list(self.raw["machines"])

    def machine(self, machine_id: str) -> dict[str, Any] | None:
        return next((m for m in self.raw["machines"] if m["machine_id"] == machine_id), None)


def load_bundle(source: BundleSource) -> Bundle:
    """Parse, structurally validate, semantically validate, and normalize one bundle."""
    if isinstance(source, str):
        document = yaml12.load(source)
    elif isinstance(source, Mapping):
        document = copy.deepcopy(dict(source))
        yaml12.validate_portable_values(document)
        if not yaml12.validate_unicode(document):
            raise ValidationError("invalid_unicode")
    else:
        raise ValidationError("non_json_value")
    if not isinstance(document, dict):
        raise ValidationError("structural_validation")
    if document.get("format") != 1 or isinstance(document.get("format"), bool):
        raise ValidationError("unsupported_format")
    from .validator import validate

    validate(document)
    normalized = normalize_bundle(document)
    return Bundle(raw=normalized, fingerprint=bundle_fingerprint(normalized))
