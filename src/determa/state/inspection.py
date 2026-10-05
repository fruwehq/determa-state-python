"""Exact, read-only candidate inspection over a restored aggregate."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any, cast

from .definition import _escape_pointer
from .engine import _runtime_model
from .model import BundleModel, StateNode
from .queueing import (
    _entry_digest,
    _native_envelope,
    _validate_new_deliveries,
    restore_aggregate_v1,
)
from .wire import (
    ArtifactSource,
    DefinitionResolver,
    _schema_registry,
    artifact_schema,
    hash_value,
    typed_value,
)

_ORDER = ("handled_now", "deferred", "unhandled", "invalid")


def _failure(code: str, locator: str | None = None) -> dict[str, Any]:
    return {"code": code, "source_locator": locator}


def _valid_request(request: Any) -> bool:
    import jsonschema

    schema = artifact_schema("inspection_v1")
    validator = jsonschema.Draft202012Validator(
        {"$ref": f"{schema['$id']}#/$defs/request"}, registry=_schema_registry()
    )
    if next(validator.iter_errors(request), None) is not None:
        return False
    if request["mode"] == "semantic":
        limits = request["limits"]
        return (
            len(limits["maximum_guard_evaluations"]) <= 2
            and len(limits["maximum_evaluation_steps"]) <= 7
            and int(limits["maximum_guard_evaluations"]) <= 64
            and int(limits["maximum_evaluation_steps"]) <= 1_000_000
        )
    return True


def _branch_data(state: StateNode, event: str, fingerprint: str) -> list[dict[str, Any]]:
    declaration = (state.raw.get("on_events") or {}).get(event)
    if declaration is None:
        return []
    branches = declaration if isinstance(declaration, list) else [declaration]
    base = f"{state.pointer}/on_events/{_escape_pointer(event)}"
    result = []
    for index, branch in enumerate(branches):
        locator = f"{base}/{index}/guard" if isinstance(declaration, list) else f"{base}/guard"
        guard = branch.get("guard")
        result.append(
            {
                "branch_index": str(index),
                "guard_locator": locator if guard is not None else None,
                "guard_binding_digest": (
                    hash_value(
                        ["determa-guard-binding-1", fingerprint, locator, typed_value(guard)]
                    )
                    if guard is not None
                    else None
                ),
            }
        )
    return result


def _structural(levels: list[dict[str, Any]]) -> list[str]:
    possible: set[str] = {"unhandled"}
    for level in reversed(levels):
        branches = level["handler_branches"]
        if branches:
            if any(branch["guard_locator"] is None for branch in branches):
                possible = {"handled_now"}
            else:
                possible.add("handled_now")
                if level["defers"]:
                    possible = {"handled_now", "deferred"}
        elif level["defers"]:
            possible = {"deferred"}
    return [value for value in _ORDER if value in possible]


def _success(
    request: Mapping[str, Any],
    fingerprint: str,
    dispositions: list[str],
    levels: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    reason: str | None = None,
) -> dict[str, Any]:
    return {
        "aggregate_state_digest": request["aggregate_state_digest"],
        "definition_fingerprint": fingerprint,
        "runtime_id": request["runtime_id"],
        "runtime_incarnation": copy.deepcopy(request["runtime_incarnation"]),
        "classification": "definitive" if len(dispositions) == 1 else "conditional",
        "possible_dispositions": dispositions,
        "disposition": dispositions[0] if len(dispositions) == 1 else None,
        "reason": reason,
        "levels": levels,
        "guard_evidence": evidence,
    }


def inspect_candidate(
    aggregate: ArtifactSource,
    request: Mapping[str, Any],
    definition_resolver: DefinitionResolver,
    *,
    semantic_enabled: bool = True,
) -> dict[str, Any]:
    """Inspect one exact runtime and candidate without admitting or dispatching it.

    Operation failures are returned as closed failure objects, like successful outcomes.
    Invalid aggregates still raise the ordinary artifact validation error.
    """
    if not _valid_request(request):
        return _failure("invalid_inspection_request")
    restored = restore_aggregate_v1(aggregate, definition_resolver)
    document = restored.aggregate_envelope
    if request["aggregate_state_digest"] != document["aggregate_state_digest"]:
        return _failure("invalid_inspection_request")
    root = next(
        runtime
        for runtime in document["runtimes"]
        if runtime["runtime_id"] == document["root_runtime_id"]
    )
    runtime = next(
        (
            runtime
            for runtime in document["runtimes"]
            if runtime["runtime_id"] == request["runtime_id"]
        ),
        None,
    )
    fingerprint = (runtime or root)["current_definition"]["validated_bundle_fingerprint"]
    if runtime is None:
        return _success(request, fingerprint, ["invalid"], [], [], "target_not_found")
    if runtime["identity_origin"] != request["runtime_incarnation"]:
        return _success(request, fingerprint, ["invalid"], [], [], "target_incarnation_mismatch")
    if runtime["status"] != "running":
        return _success(request, fingerprint, ["invalid"], [], [], "runtime_inactive")
    envelope = request["envelope"]
    mode = "input" if envelope["source"] == {"host": True} else "internal"
    delivery = {
        "delivery_mode": mode,
        "envelope": envelope,
        "envelope_digest": _entry_digest(document["root_instance_id"], mode, envelope),
    }
    if (
        envelope["target"] != runtime["target_identity"]
        or _validate_new_deliveries(restored, [delivery]) is not None
    ):
        return _success(request, fingerprint, ["invalid"], [], [], "invalid_envelope")
    model = _runtime_model(
        restored.bundle,
        BundleModel(restored.bundle),
        restored.state["runtimes"][runtime["runtime_id"]],
    )
    native = restored.state["runtimes"][runtime["runtime_id"]]
    active = model.states[native["active"][-1]] if native["active"] else model.root
    nodes = active.ancestors(include_self=True)
    levels = [
        {
            "state_id": node.pointer,
            "handler_branches": _branch_data(node, envelope["event"], fingerprint),
            "defers": envelope["event"] in (node.raw.get("deferred_events") or []),
        }
        for node in nodes
    ]
    if request["mode"] == "structural":
        return _success(request, fingerprint, _structural(levels), levels, [])
    if not semantic_enabled:
        return _failure("inspection_capability_unavailable")
    # Inspect every potentially reached guard before evaluating any of them.
    runtime_providers = restored.bundle.runtime_providers
    for node in nodes:
        declaration = (node.raw.get("on_events") or {}).get(envelope["event"])
        branches = (
            declaration if isinstance(declaration, list) else [declaration] if declaration else []
        )
        for branch in branches:
            guard = branch.get("guard")
            if guard is None or isinstance(guard, str):
                continue
            if runtime_providers is None or not runtime_providers.can_inspect(guard["provider"]):
                return _failure("inspection_capability_unavailable")
    from .inspection_cel import InspectionLimit, safe_evaluate, value_units

    evidence: list[dict[str, Any]] = []
    remaining = int(request["limits"]["maximum_evaluation_steps"])
    guard_count = int(request["limits"]["maximum_guard_evaluations"])
    candidate = _native_envelope(delivery)
    for node, level in zip(nodes, levels, strict=True):
        declaration = (node.raw.get("on_events") or {}).get(envelope["event"])
        branches = (
            declaration if isinstance(declaration, list) else [declaration] if declaration else []
        )
        branch_data = cast(list[dict[str, Any]], level["handler_branches"])
        for branch, branch_info in zip(branches, branch_data, strict=True):
            guard = branch.get("guard")
            if guard is None:
                return _success(request, fingerprint, ["handled_now"], levels, evidence)
            locator = branch_info["guard_locator"]
            assert locator is not None
            if guard_count == 0:
                return _failure("inspection_limit_exceeded", locator)
            guard_count -= 1
            variables: dict[str, Any] = {}
            for scoped in reversed(node.ancestors(include_self=True)):
                scope = native["scopes"].get(scoped.path, {})
                variables.update(scope)
            visible_units = sum(value_units(value) for value in variables.values())
            activation = {**variables, "event": {"payload": candidate["payload"]}}
            try:
                if isinstance(guard, str):
                    value, cost = safe_evaluate(
                        guard,
                        activation,
                        remaining,
                        snapshot_units=value_units(
                            {
                                **candidate,
                                "source": envelope["source"],
                                "cause_id": envelope["cause_id"],
                            }
                        )
                        + visible_units,
                    )
                else:
                    assert runtime_providers is not None
                    from .runtime_providers import RuntimeProviderError, guard_snapshot

                    binding = guard["provider"]
                    snapshot = guard_snapshot(
                        activation,
                        {
                            **candidate,
                            "source": copy.deepcopy(envelope["source"]),
                            "cause_id": envelope["cause_id"],
                        },
                        binding,
                    )
                    try:
                        value, _charged, cost = runtime_providers.inspect_guard(
                            binding, snapshot, guard_count + 1, remaining
                        )
                    except (ValueError, RuntimeProviderError) as exc:
                        if str(exc) == "inspection_limit_exceeded":
                            return _failure("inspection_limit_exceeded", locator)
                        return _failure("inspection_guard_failure", locator)
            except InspectionLimit:
                return _failure("inspection_limit_exceeded", locator)
            except Exception:
                return _failure("inspection_guard_failure", locator)
            remaining -= cost
            evidence.append({"state_id": node.pointer, **branch_info, "value": value})
            if value:
                return _success(request, fingerprint, ["handled_now"], levels, evidence)
        if level["defers"]:
            return _success(request, fingerprint, ["deferred"], levels, evidence)
    return _success(request, fingerprint, ["unhandled"], levels, evidence)
