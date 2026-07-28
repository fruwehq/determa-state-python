"""Driver for the language-neutral format-1 core conformance cases."""

from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from determa.state import ValidationError, create, dispatch, load_bundle

from .pins import CONFORMANCE_CACHE


def conformance_root() -> Path:
    override = os.environ.get("DETERMA_CONFORMANCE_DIR")
    return Path(override) if override else CONFORMANCE_CACHE


CORE_DIR = conformance_root() / "conformance" / "core"


@dataclass(frozen=True)
class CoreCase:
    name: str
    path: Path

    @property
    def machine_file(self) -> Path | None:
        path = self.path / "machine.yaml"
        return path if path.exists() else None

    @property
    def test_file(self) -> Path:
        return self.path / "test.yaml"


def core_cases() -> list[CoreCase]:
    if not CORE_DIR.exists():
        return []
    return [
        CoreCase(path.name, path)
        for path in sorted(CORE_DIR.iterdir())
        if path.is_dir() and (path / "test.yaml").exists()
    ]


def _load_test(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _materialize_driver_value(value: Any) -> Any:
    if isinstance(value, dict) and set(value) == {"invalid_unicode_scalar"}:
        return chr(int(value["invalid_unicode_scalar"], 16))
    if isinstance(value, dict):
        return {key: _materialize_driver_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_materialize_driver_value(item) for item in value]
    return value


def run_case(case: CoreCase) -> None:
    test = _load_test(case.test_file)
    _run_static_documents(case, test)
    machine_file = case.machine_file
    static = test.get("static") or {}
    primary_invalid = (
        isinstance(static, dict) and "documents" not in static and static.get("valid") is False
    )
    if machine_file is None or primary_invalid:
        return
    source = machine_file.read_text(encoding="utf-8")
    try:
        bundle = load_bundle(source)
    except ValidationError:
        documents = static.get("documents") if isinstance(static, dict) else None
        primary_declared_invalid = isinstance(documents, list) and any(
            document.get("file") == "machine.yaml" and document.get("valid") is False
            for document in documents
        )
        if primary_declared_invalid and not (
            test.get("steps") or test.get("create") or test.get("load")
        ):
            return
        raise
    if "load" in test:
        assert test["load"].get("valid", True) is True
    create_spec = test.get("create") or {}
    machine_id = bundle.raw["machines"][0]["machine_id"]
    root_instance_id = _materialize_driver_value(
        create_spec.get("root_instance_id", f"conformance:{case.name}:root")
    )
    creation_id = _materialize_driver_value(
        create_spec.get("creation_id", f"conformance:{case.name}:create")
    )
    bindings = _materialize_driver_value(create_spec.get("bindings") or {})
    result = create(
        bundle,
        machine_id,
        root_instance_id,
        creation_id,
        bindings,
    )
    _assert_result(result, create_spec.get("expect") or {}, None, {})
    if result["state"] is None:
        assert not test.get("steps")
        return
    state = result["state"]
    captures: dict[str, list[dict[str, Any]]] = {}
    for index, step in enumerate(test.get("steps") or []):
        prior_state = state
        target_runtime_id = state["root_runtime_id"]
        dispatch_bundle = bundle
        if "send" in step:
            send = _materialize_driver_value(step["send"])
            if "bundle" in send:
                dispatch_bundle = load_bundle(
                    (case.path / send["bundle"]).read_text(encoding="utf-8")
                )
            target = _root_target(state)
            if "bound_instance" in send:
                reference = _visible_variables(state, state["runtimes"][state["root_runtime_id"]])[
                    send["bound_instance"]
                ]
                target = {"spawned_instance": copy.deepcopy(reference)}
                target_runtime_id = reference["instance_id"]
            envelope = {
                "event": send["event"],
                "event_id": send.get("event_id", f"conformance:{case.name}:step:{index}:input"),
                "target": target,
                "payload": copy.deepcopy(send.get("payload") or {}),
            }
            if "correlation_id" in send:
                envelope["correlation_id"] = send["correlation_id"]
            result = dispatch(dispatch_bundle, state, {"input": envelope})
        elif "deliver" in step:
            delivery = step["deliver"]
            envelope = copy.deepcopy(captures[delivery["captured"]][delivery["index"]])
            target_runtime_id = _target_runtime_id(envelope["target"])
            result = dispatch(dispatch_bundle, state, {"internal": envelope})
        else:
            raise AssertionError(f"{case.name} step {index}: unsupported driver step")
        _assert_result(
            result,
            step.get("expect") or {},
            target_runtime_id,
            captures,
            prior_state=prior_state,
        )
        state = result["state"]
        if "capture_emissions_as" in step:
            captures[step["capture_emissions_as"]] = copy.deepcopy(result["emissions"])


def _run_static_documents(case: CoreCase, test: dict[str, Any]) -> None:
    documents: list[dict[str, Any]] = []
    static = test.get("static")
    if isinstance(static, dict):
        if "documents" in static:
            documents = static["documents"]
        elif case.machine_file is not None:
            documents = [{"file": "machine.yaml", **static}]
    for document in documents:
        source = (case.path / document["file"]).read_text(encoding="utf-8")
        try:
            load_bundle(source)
            actual = (True, None)
        except ValidationError as error:
            actual = (False, error.code)
        assert actual == (document["valid"], document.get("error")), (
            f"{case.name}/{document['file']}: {actual}"
        )


def _assert_result(
    result: dict[str, Any],
    expected: dict[str, Any],
    target_runtime_id: str | None,
    captures: dict[str, list[dict[str, Any]]],
    *,
    prior_state: dict[str, Any] | None = None,
) -> None:
    del captures
    for name in ("status", "disposition"):
        if name in expected:
            assert result[name] == expected[name], (name, result[name], expected[name])
    if "rejection" in expected:
        _assert_partial(result["rejection"], expected["rejection"], state=result["state"])
    if "fault" in expected:
        _assert_partial(result["fault"], expected["fault"], state=result["state"])
    if expected.get("caller_still_owns_input"):
        assert result["state"] is not None
        assert not {"queue", "timers", "dead_letters"} & set(result["state"])
        assert all(
            not {"queue", "timers", "dead_letters"} & set(runtime)
            for runtime in result["state"]["runtimes"].values()
        )
        if result["disposition"] == "rejected":
            assert result["state"] is prior_state
    if result["state"] is None:
        return
    state = result["state"]
    runtime = state["runtimes"][state["root_runtime_id"]]
    _assert_runtime(state, runtime, expected)
    if "emissions" in expected:
        assert len(result["emissions"]) == len(expected["emissions"])
        for actual, wanted in zip(result["emissions"], expected["emissions"], strict=True):
            _assert_emission(
                state,
                runtime,
                target_runtime_id,
                prior_state,
                actual,
                wanted,
            )


def _assert_runtime(
    state: dict[str, Any], runtime: dict[str, Any], expected: dict[str, Any]
) -> None:
    if "config" in expected:
        actual = _config(runtime)
        assert actual == expected["config"], (actual, expected["config"])
    if "variables" in expected:
        _assert_partial(_visible_variables(state, runtime), expected["variables"], state=state)
    if "history" in expected:
        assert runtime["history"] == expected["history"], (
            runtime["history"],
            expected["history"],
        )
    if "components" in expected:
        wanted = expected["components"]
        assert set(runtime["components"]) == set(wanted)
        for component_id, component_expected in wanted.items():
            child = state["runtimes"][runtime["components"][component_id]]
            if "status" in component_expected:
                _assert_partial(child, {"status": component_expected["status"]})
            _assert_runtime(state, child, component_expected)
    if "owned_instances" in expected:
        children = sorted(
            [
                child
                for child in state["runtimes"].values()
                if child.get("role") == "spawned" and _is_descendant_runtime(state, child, runtime)
            ],
            key=lambda child: (
                child["owner_runtime_id"].encode("utf-8"),
                child["spawn_sequence"],
            ),
        )
        assert len(children) == len(expected["owned_instances"])
        for child, child_expected in zip(children, expected["owned_instances"], strict=True):
            key = child_expected["key"]
            if key.get("owner") == "root":
                assert child["owner_runtime_id"] == state["root_runtime_id"]
            if "spawn_sequence" in key:
                assert child["spawn_sequence"] == key["spawn_sequence"]
            if "machine_id" in child_expected:
                assert child["machine_id"] == child_expected["machine_id"]
            _assert_runtime(state, child, child_expected)
    if isinstance(expected.get("fault"), dict) and "code" in expected["fault"]:
        _assert_partial(runtime["fault"], expected["fault"])


def _assert_emission(
    state: dict[str, Any],
    runtime: dict[str, Any],
    processed_runtime_id: str | None,
    prior_state: dict[str, Any] | None,
    actual: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    for key, value in expected.items():
        if key == "target":
            if value == "external":
                assert actual["target"] == "external"
            elif value == "root":
                assert actual["target"] == _root_target(state)
            elif value == "owner":
                emitter = _emitting_runtime(state, prior_state, processed_runtime_id, actual)
                if emitter is not None and emitter.get("owner_runtime_id") is not None:
                    owner = _runtime_from_either(state, prior_state, emitter["owner_runtime_id"])
                    assert owner is not None
                    owner_target = _runtime_target(state, owner)
                else:
                    owner_target = _root_target(state)
                assert actual["target"] == owner_target
            elif isinstance(value, dict) and "component" in value:
                component = next(
                    (
                        candidate
                        for candidate in state["runtimes"].values()
                        if candidate.get("role") == "component"
                        and candidate.get("component_id") == value["component"]
                        and (
                            processed_runtime_id is None
                            or candidate.get("owner_runtime_id") == processed_runtime_id
                            or candidate.get("owner_runtime_id") == runtime["runtime_id"]
                        )
                    ),
                    None,
                )
                assert component is not None
                assert actual["target"] == component["target"]
            elif isinstance(value, dict) and "bound_instance" in value:
                reference = _visible_variables(state, runtime)[value["bound_instance"]]
                assert actual["target"] == {"spawned_instance": reference}
        elif key == "payload":
            _assert_partial(actual["payload"], value, state=state)
        else:
            assert actual.get(key) == value, (key, actual.get(key), value)


def _assert_partial(actual: Any, expected: Any, *, state: dict[str, Any] | None = None) -> None:
    if isinstance(expected, dict):
        assert isinstance(actual, dict), (actual, expected)
        if set(expected) == {"instance_reference"}:
            assertion = expected["instance_reference"]
            assert _is_reference(actual)
            if "machine_id" in assertion:
                assert actual["machine_id"] == assertion["machine_id"]
            if "targetable" in assertion:
                assert state is not None
                target = state["runtimes"].get(actual["instance_id"])
                targetable = (
                    state["status"] == "running"
                    and target is not None
                    and target.get("role") == "spawned"
                    and target.get("status") == "running"
                    and target.get("instance_reference") == actual
                )
                assert targetable is assertion["targetable"]
            return
        for key, value in expected.items():
            assert key in actual, (key, actual)
            _assert_partial(actual[key], value, state=state)
    elif isinstance(expected, list):
        assert isinstance(actual, list) and len(actual) == len(expected)
        for left, right in zip(actual, expected, strict=True):
            _assert_partial(left, right, state=state)
    else:
        assert type(actual) is type(expected) and actual == expected, (actual, expected)


def _is_reference(value: Any) -> bool:
    return isinstance(value, dict) and set(value) == {
        "root_instance_id",
        "instance_id",
        "machine_id",
        "machine_version",
    }


def _config(runtime: dict[str, Any]) -> list[str]:
    if not runtime["active"]:
        return []
    leaf = runtime["active"][-1]
    return [] if leaf == "root" else [leaf]


def _visible_variables(state: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
    del state
    result: dict[str, Any] = {}
    for path in runtime["active"]:
        result.update(runtime["scopes"].get(path, {}))
    return copy.deepcopy(result)


def _root_target(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "root": {
            "root_instance_id": state["root_instance_id"],
            "root_runtime_id": state["root_runtime_id"],
        }
    }


def _target_runtime_id(target: dict[str, Any]) -> str:
    if "root" in target:
        return target["root"]["root_runtime_id"]
    if "component" in target:
        return target["component"]["component_runtime_id"]
    return target["spawned_instance"]["instance_id"]


def _runtime_from_either(
    state: dict[str, Any],
    prior_state: dict[str, Any] | None,
    runtime_id: str,
) -> dict[str, Any] | None:
    runtime = state["runtimes"].get(runtime_id)
    if runtime is not None or prior_state is None:
        return runtime
    return prior_state["runtimes"].get(runtime_id)


def _emitting_runtime(
    state: dict[str, Any],
    prior_state: dict[str, Any] | None,
    processed_runtime_id: str | None,
    emission: dict[str, Any],
) -> dict[str, Any] | None:
    payload = emission.get("payload") or {}
    source_id = payload.get("component_runtime_id")
    if source_id is None and emission.get("event") in {
        "done",
        "determa.spawned_instance_failed",
    }:
        source_id = payload.get("instance_id")
    if not isinstance(source_id, str):
        source_id = processed_runtime_id
    if source_id is None:
        return None
    return _runtime_from_either(state, prior_state, source_id)


def _runtime_target(state: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
    if runtime["role"] == "root":
        return _root_target(state)
    if runtime["role"] == "component":
        return copy.deepcopy(runtime["target"])
    return {"spawned_instance": copy.deepcopy(runtime["instance_reference"])}


def _is_descendant_runtime(
    state: dict[str, Any],
    candidate: dict[str, Any],
    owner: dict[str, Any],
) -> bool:
    owner_id = candidate.get("owner_runtime_id")
    while owner_id is not None:
        if owner_id == owner["runtime_id"]:
            return True
        parent = state["runtimes"].get(owner_id)
        owner_id = parent.get("owner_runtime_id") if parent is not None else None
    return False
