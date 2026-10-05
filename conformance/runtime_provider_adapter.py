"""Production API driver for the optional exact runtime-provider source profile.

The driver receives only operation inputs. It imports the installed fixture after
checking the complete source closure, then projects public API results into the
profile's language-independent observation shape.
"""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import yaml

from determa.state import (
    ExecutionHost,
    ExecutionHostError,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    RuntimeProviderError,
    RuntimeProviderRegistry,
    SourceClosure,
    ValidationError,
    admit,
    compile_language_source,
    create,
    delivery_request_digest,
    inspect_candidate,
    load_bundle,
    restore_aggregate,
    step,
)

_SOURCE_DOMAIN = b"determa-test-runtime-provider-closure-1\0"
_PURE_CLAIMS = frozenset({"deterministic", "pure", "portable", "semantically_introspectable"})
_SAFE_CLAIMS = frozenset(
    {"deterministic", "pure", "process_contained", "semantically_introspectable"}
)
_GUARANTEES = (
    "deterministic",
    "pure",
    "portable",
    "semantically_introspectable",
    "process_contained",
)


def _profile(*, process_contained: bool = True) -> dict[str, bool]:
    return {name: name != "process_contained" or process_contained for name in _GUARANTEES} | {
        "external_io_capable": False
    }


def _empty() -> dict[str, Any]:
    return {
        "stages": [],
        "result": "rejected",
        "code": None,
        "value": None,
        "calls": {"guard": 0, "actions": 0, "inspect_guard": 0, "compile_region": 0, "external": 0},
        "external_effects": [],
        "irreversible_side_effects": 0,
        "determa_state_committed": False,
        "state_before": None,
        "state_after": None,
        "effective_capabilities": None,
    }


def _source(
    root: Path, payload: dict[str, Any], installed: dict[str, Any]
) -> tuple[SourceClosure, str]:
    closure = SourceClosure(
        root,
        tuple(payload["source_files"]),
        payload["source_closure_file"],
        _SOURCE_DOMAIN,
        "provider/test_provider.py",
    )
    digest = closure.digest()
    manifest = (root / payload["source_closure_file"]).read_bytes()
    manifest_digest = "sha256:" + hashlib.sha256(manifest).hexdigest()
    if (
        not installed["trusted"]
        or digest != installed["closure_digest"]
        or manifest_digest != installed["source_digest"]
    ):
        raise RuntimeProviderError("runtime_provider_unavailable")
    return closure, digest


def _installed(
    root: Path,
    payload: dict[str, Any],
    installed: dict[str, Any],
    arguments: dict[str, Any],
    *,
    compiler: bool,
) -> tuple[RuntimeProviderRegistry, Any, dict[str, str]]:
    closure, _ = _source(root, payload, installed)
    references = {item["identifier"]: item for item in installed["providers"]}
    bundle_file = root / payload["request"]["bundle"]
    bundle_document = (
        json.loads(bundle_file.read_text())
        if bundle_file.suffix == ".json"
        else yaml.safe_load(bundle_file.read_text())
    )
    descriptors: list[dict[str, Any]] = []

    from determa.state.runtime_providers import _runtime_bindings

    descriptors = [
        {"kind": kind, "binding": binding} for kind, binding in _runtime_bindings(bundle_document)
    ]
    names = (
        {"example.native-common", "example.guard-compiler"}
        if compiler
        else {
            descriptor["binding"]["provider_reference"]["identifier"] for descriptor in descriptors
        }
        | ({"example.native-common"} if descriptors else set())
    )
    if not names.issubset(references):
        raise RuntimeProviderError("runtime_provider_unavailable")
    if any(references[name]["content_digest"] != closure.digest() for name in names):
        raise RuntimeProviderError("runtime_provider_unavailable")
    loaded = {}
    source_path = root / closure.python_source
    module_name = "determa_conformance_runtime_provider"
    spec = importlib.util.spec_from_file_location(module_name, source_path)
    if spec is None or spec.loader is None:
        raise RuntimeProviderError("runtime_provider_unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    loaded[closure.python_source] = "sha256:" + hashlib.sha256(source_path.read_bytes()).hexdigest()
    registry = RuntimeProviderRegistry()
    if "example.native-common" in references:
        registry.register_dependency(references["example.native-common"], closure)
    if compiler:
        registry.register_compiler(
            references["example.guard-compiler"],
            module.compile_region,
            closure,
            capability_proof=lambda selected: (
                _PURE_CLAIMS
                if selected is module.compile_region and not arguments.get("weak_compiler", False)
                else frozenset()
            ),
        )
    else:
        for descriptor in descriptors:
            kind = descriptor["kind"]
            registry.register(
                descriptor,
                module.Provider,
                closure,
                capability_proof=(
                    (
                        lambda selected, _binding: (
                            _SAFE_CLAIMS if type(selected) is module.Provider else frozenset()
                        )
                    )
                    if descriptor["binding"]["provider_reference"]["identifier"].startswith(
                        "example.native-safe"
                    )
                    else None
                ),
                evaluation_options=(
                    {
                        "guard_override": arguments.get("guard_override"),
                        "external_io": bool(arguments.get("guard_external_io", False)),
                    }
                    if kind == "guard"
                    else {
                        "invalid": bool(arguments.get("invalid_output", False)),
                        "fail": bool(arguments.get("action_fail", False)),
                        "external_io": bool(arguments.get("action_external_io", False)),
                        **({"repeat_send": True} if arguments.get("repeat_send") else {}),
                        **({"mixed_send": True} if arguments.get("mixed_send") else {}),
                        **(
                            {"environment_send": arguments["environment_send"]}
                            if "environment_send" in arguments
                            else {}
                        ),
                    }
                ),
            )
    return registry, module, loaded


def _provider_counts(registry: RuntimeProviderRegistry, observation: dict[str, Any]) -> None:
    for active in registry._active.values():
        provider = active.provider
        observation["calls"]["guard"] += getattr(provider, "guard_calls", 0)
        observation["calls"]["actions"] += getattr(provider, "action_calls", 0)
        observation["calls"]["external"] += getattr(provider, "external_calls", 0)
        observation["external_effects"].extend(getattr(provider, "external_effect_log", []))
        observation["irreversible_side_effects"] += getattr(provider, "irreversible_effects", 0)


def _state(aggregate: dict[str, Any]) -> dict[str, Any]:
    runtime = next(
        item for item in aggregate["runtimes"] if item["runtime_id"] == aggregate["root_runtime_id"]
    )
    return {
        "active_leaf": (
            runtime["active_leaf_state_definition_pointers"][-1]
            if runtime["active_leaf_state_definition_pointers"]
            else None
        ),
        "status": runtime["status"],
        "variables": {
            item["variable_declaration_pointer"].split("/")[-1]: item["value"]
            for item in runtime["variables"]
        },
        "ready_mailbox_length": len(runtime["ready_mailbox"]),
        "deferred_mailbox_length": len(runtime["deferred_mailbox"]),
        "output_count": int(aggregate["next_output_sequence"]),
    }


def _component_states(aggregate: dict[str, Any]) -> dict[str, Any]:
    result = {}
    for runtime in aggregate["runtimes"]:
        component = runtime["target_identity"].get("component")
        if component is not None:
            observed = _state({**aggregate, "root_runtime_id": runtime["runtime_id"]})
            result[component["component_id"]] = {
                key: observed[key]
                for key in (
                    "status",
                    "variables",
                    "ready_mailbox_length",
                    "deferred_mailbox_length",
                )
            }
    return result


def _run_step(
    bundle: Any,
    registry: RuntimeProviderRegistry,
    request: dict[str, Any],
    observation: dict[str, Any],
) -> None:
    setup = request["setup"]
    creation = setup["create_request"]
    created = create(
        bundle,
        creation["machine_id"],
        creation["root_instance_id"],
        creation["creation_id"],
        creation["bindings"],
    )
    aggregate = created["state"]
    observation["stages"].append("create")
    envelope = copy.deepcopy(setup["envelope"])
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    delivery = {
        "delivery_mode": "input",
        "envelope": envelope,
        "envelope_digest": delivery_request_digest(creation["root_instance_id"], "input", envelope),
    }
    admitted = admit(aggregate, [delivery], resolver)
    if admitted["state"] is None:
        raise RuntimeProviderError(admitted["rejection"]["code"])
    aggregate = admitted["state"]
    observation["stages"].append("admit")
    observation["state_before"] = _state(aggregate)
    observation["stages"].append("evaluate_cel")
    result = step(
        aggregate,
        setup["target_runtime_id"],
        resolver,
        _include_host_evidence=bool(
            request["arguments"].get("repeat_send") or request["arguments"].get("mixed_send")
        ),
    )
    _provider_counts(registry, observation)
    if observation["calls"]["guard"]:
        observation["stages"].append("evaluate_guard")
    if observation["calls"]["actions"]:
        observation["stages"].append("evaluate_actions")
        if not request["arguments"].get("action_fail"):
            observation["stages"].append("validate_output")
    if result["disposition"] == "faulted":
        observation["result"] = "faulted"
        observation["code"] = result["fault"]["code"]
        observation["state_after"] = observation["state_before"]
        if "environment_send" in request["arguments"]:
            observation["value"] = {
                "source_locator": result["fault"]["source_locator"],
                "component_states_before": _component_states(aggregate),
                "component_states_after": _component_states(result["state"]),
            }
        if request["arguments"].get("invalid_output") or request["arguments"].get(
            "destroyed_write"
        ):
            observation["value"] = {"boundary_code": "runtime_provider_output_invalid"}
            if request["arguments"].get("destroyed_write"):
                observation["value"]["source_locator"] = result["fault"]["source_locator"]
        return
    observation["stages"].append("commit")
    observation["result"] = (
        "handled_now" if result["disposition"] == "handled" else result["disposition"]
    )
    observation["determa_state_committed"] = True
    observation["state_after"] = _state(result["state"])
    observation["value"] = {"emissions": len(result["emissions"])}
    if "environment_send" in request["arguments"]:
        before_child = next(
            item
            for item in aggregate["runtimes"]
            if item.get("target_identity", {}).get("component", {}).get("component_id") == "replica"
        )
        after_child = next(
            item
            for item in result["state"]["runtimes"]
            if item.get("target_identity", {}).get("component", {}).get("component_id") == "replica"
        )
        internal = next(
            item for item in result["emissions"] if item.get("kind") == "internal_mailbox"
        )
        emission = next(
            item["envelope"]
            for item in after_child["ready_mailbox"]
            if item["envelope"]["event_id"] == internal["event_id"]
        )
        observation["value"].update(
            forwarded_event={
                "event": emission["event"],
                "component_id": after_child["target_identity"]["component"]["component_id"],
                "payload": emission["payload"],
            },
            component_variables_before=_state(
                {**aggregate, "root_runtime_id": before_child["runtime_id"]}
            )["variables"],
            component_ready_before_delivery=len(after_child["ready_mailbox"]),
        )
        delivered = step(result["state"], after_child["runtime_id"], resolver)
        observation["stages"].append("deliver_env")
        refreshed = next(
            item
            for item in delivered["state"]["runtimes"]
            if item["runtime_id"] == after_child["runtime_id"]
        )
        observation["value"].update(
            component_variables_after=_state(
                {**delivered["state"], "root_runtime_id": refreshed["runtime_id"]}
            )["variables"],
            component_ready_after_delivery=len(refreshed["ready_mailbox"]),
        )
    if "accepted" in observation["state_after"]["variables"]:
        observation["value"]["accepted"] = observation["state_after"]["variables"]["accepted"]
    if observation["state_after"]["status"] == "completed":
        observation["value"].update(
            status="completed", exit_correlation=result["emissions"][-1]["correlation_id"]
        )
    if request["arguments"].get("repeat_send") or request["arguments"].get("mixed_send"):
        observation["value"]["emission_identities"] = [
            (
                {
                    "event_id": item["event_id"],
                    "emission_index": item["emission_index"],
                    "acceptance_sequence": item["acceptance_sequence"],
                    "queue_sequence": item["queue_sequence"],
                }
                if "kind" in item
                else {
                    "effect_id": item["effect_id"],
                    "sequence": item["sequence"],
                    "emission_index": item["_determa_v1_emission_index"],
                }
            )
            for item in result["emissions"]
        ]

    if request["arguments"].get("capture_snapshot"):
        for active in registry._active.values():
            for key in ("guard_snapshot", "action_snapshot"):
                captured = getattr(active.provider, key, None)
                if captured is not None:
                    observation["value"][key] = captured


def _run_create(
    bundle: Any,
    registry: RuntimeProviderRegistry,
    request: dict[str, Any],
    observation: dict[str, Any],
) -> None:
    creation = request["setup"]["create_request"]
    result = create(
        bundle,
        creation["machine_id"],
        creation["root_instance_id"],
        creation["creation_id"],
        creation["bindings"],
    )
    observation["stages"].append("create")
    _provider_counts(registry, observation)
    if observation["calls"]["actions"]:
        observation["stages"].extend(("evaluate_actions", "validate_output"))
    observation["state_after"] = _state(result["state"])
    observation["result"] = result["status"]
    observation["code"] = result["fault"]["code"] if result["fault"] else None
    if result["fault"]:
        observation["value"] = {
            "boundary_code": "runtime_provider_output_invalid",
            "source_locator": result["fault"]["source_locator"],
            "emissions": len(result["emissions"]),
            "status": result["status"],
        }


class _ConflictStore(MemoryExecutionStore):
    """Test store that reports one real replace CAS conflict after core evaluation."""

    conflict = False
    replace_attempts = 0

    @contextmanager
    def transaction(self, root_instance_id: str) -> Any:
        with super().transaction(root_instance_id) as transaction:
            store = self

            class _Transaction:
                def load(self) -> Any:
                    return transaction.load()

                def insert(self, checkpoint: bytes) -> bool:
                    return transaction.insert(checkpoint)

                def replace(
                    self,
                    expected_revision: str,
                    expected_checkpoint_digest: str,
                    checkpoint: bytes,
                ) -> bool:
                    store.replace_attempts += 1
                    if store.conflict:
                        return False
                    return transaction.replace(
                        expected_revision, expected_checkpoint_digest, checkpoint
                    )

            yield _Transaction()


def _run_host_commit(
    bundle: Any,
    registry: RuntimeProviderRegistry,
    request: dict[str, Any],
    observation: dict[str, Any],
) -> None:
    setup = request["setup"]
    creation = setup["create_request"]
    root_id = creation["root_instance_id"]
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    store = _ConflictStore()
    host = ExecutionHost(store, resolver)
    host.create_v1(
        bundle, creation["machine_id"], root_id, creation["creation_id"], creation["bindings"]
    )
    observation["stages"].append("create")
    created = host.read_checkpoint(root_id)
    assert created is not None
    envelope = copy.deepcopy(setup["envelope"])
    delivery = {
        "delivery_mode": "input",
        "envelope": envelope,
        "envelope_digest": delivery_request_digest(root_id, "input", envelope),
    }
    host.admit_v1(
        root_id,
        [delivery],
        expected_revision=created.document["revision"],
        expected_checkpoint_digest=created.document["execution_checkpoint_digest"],
    )
    observation["stages"].append("admit")
    admitted = host.read_checkpoint(root_id)
    assert admitted is not None
    checkpoint = admitted.document
    aggregate = checkpoint["root_record"]["aggregate_state"]
    observation["state_before"] = _state(aggregate)
    observation["state_after"] = copy.deepcopy(observation["state_before"])
    observation["stages"].append("evaluate_cel")
    runtime = next(
        item for item in aggregate["runtimes"] if item["runtime_id"] == setup["target_runtime_id"]
    )
    entry = runtime["ready_mailbox"][0]
    store.conflict = request["arguments"]["cas_conflict"]
    store.replace_attempts = 0
    process_arguments = {
        "expected_revision": checkpoint["revision"],
        "expected_checkpoint_digest": checkpoint["execution_checkpoint_digest"],
        "event_id": entry["envelope"]["event_id"],
        "envelope_digest": entry["envelope_digest"],
        "acceptance_sequence": entry["acceptance_sequence"],
        "queue_sequence": entry["queue_sequence"],
    }
    try:
        first = host.process_ready_v1(root_id, setup["target_runtime_id"], **process_arguments)

    except ExecutionHostError as exc:
        _provider_counts(registry, observation)
        if observation["calls"]["guard"]:
            observation["stages"].append("evaluate_guard")
        if observation["calls"]["actions"]:
            observation["stages"].extend(("evaluate_actions", "validate_output"))
        observation["stages"].append("compare_and_swap")
        if exc.code != "checkpoint_revision_conflict" or store.replace_attempts != 1:
            raise
        observation["result"] = "uncommitted"
        observation["code"] = "compare_and_swap_conflict"
        after = host.read_checkpoint(root_id)
        assert after is not None and after.source_bytes == admitted.source_bytes
        return
    if request["arguments"]["cas_conflict"]:
        raise AssertionError("expected one real store compare-and-swap conflict")
    _provider_counts(registry, observation)
    if observation["calls"]["guard"]:
        observation["stages"].append("evaluate_guard")
    if observation["calls"]["actions"]:
        observation["stages"].extend(("evaluate_actions", "validate_output"))
    observation["stages"].extend(("compare_and_swap", "commit"))
    committed = host.read_checkpoint(root_id)
    assert committed is not None and store.replace_attempts == 1
    observation["state_after"] = _state(committed.document["root_record"]["aggregate_state"])
    receipt = first["receipt"]
    references = receipt["emission_references"]
    intents = committed.document["pending_outbox_intents"]
    observation["value"] = {
        "accepted": observation["state_after"]["variables"]["accepted"],
        "emissions": len(first["core_result"]["emissions"]),
        "emission_identities": [
            {
                "effect_id": item["effect_id"],
                "emission_index": next(
                    reference["emission_index"]
                    for reference in references
                    if reference.get("effect_id") == item["effect_id"]
                ),
                "sequence": item["sequence"],
            }
            for item in first["core_result"]["emissions"]
        ],
        "checkpoint_revision": committed.document["revision"],
        "retained_effect_references": copy.deepcopy(references),
        "pending_outbox_entries": copy.deepcopy(intents),
    }
    providers_before = [
        copy.deepcopy(active.provider.__dict__) for active in registry._active.values()
    ]
    replay = host.process_ready_v1(root_id, setup["target_runtime_id"], **process_arguments)
    after_replay = host.read_checkpoint(root_id)
    assert after_replay is not None
    observation["value"].update(
        replay_receipt_equal=replay == receipt,
        replay_checkpoint_unchanged=after_replay.source_bytes == committed.source_bytes,
        replay_provider_calls_unchanged=providers_before
        == [active.provider.__dict__ for active in registry._active.values()],
    )
    observation["stages"].append("replay")
    observation["result"] = (
        "handled_now"
        if receipt["outcome"]["disposition"] == "handled"
        else receipt["outcome"]["disposition"]
    )
    observation["determa_state_committed"] = True


def _run_inspect(
    bundle: Any,
    registry: RuntimeProviderRegistry,
    request: dict[str, Any],
    observation: dict[str, Any],
) -> None:
    setup = request["setup"]
    creation = setup["create_request"]
    aggregate = create(
        bundle,
        creation["machine_id"],
        creation["root_instance_id"],
        creation["creation_id"],
        creation["bindings"],
    )["state"]
    observation["stages"].append("create")
    observation["state_before"] = _state(aggregate)
    observation["state_after"] = _state(aggregate)
    runtime = next(
        item for item in aggregate["runtimes"] if item["runtime_id"] == aggregate["root_runtime_id"]
    )
    arguments = request["arguments"]
    mode = arguments["mode"]
    candidate = copy.deepcopy(setup["envelope"])
    if "approved" in arguments:
        candidate["payload"] = ["map", [["approved", ["boolean", arguments["approved"]]]]]
    inspection = {
        "mode": mode,
        "aggregate_state_digest": aggregate["aggregate_state_digest"],
        "runtime_id": runtime["runtime_id"],
        "runtime_incarnation": runtime["identity_origin"],
        "envelope": candidate,
        "limits": (
            None
            if mode == "structural"
            else {
                "maximum_guard_evaluations": str(arguments["maximum_guard_evaluations"]),
                "maximum_evaluation_steps": str(arguments["maximum_evaluation_steps"]),
            }
        ),
    }
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    invoke = registry.inspect_guard

    def observed_invoke(*args: Any, **kwargs: Any) -> Any:
        observation["calls"]["inspect_guard"] += 1
        return invoke(*args, **kwargs)

    registry.inspect_guard = observed_invoke  # type: ignore[method-assign]
    if mode == "semantic":
        observation["stages"].append("preflight_inspection")
    else:
        observation["stages"].append("structural_inspection")
    outcome = inspect_candidate(aggregate, inspection, resolver)
    if "code" in outcome:
        observation["code"] = outcome["code"]
        if observation["calls"]["inspect_guard"]:
            observation["stages"].append("invoke_inspect_guard")
        return
    observation["result"] = "accepted"
    if mode == "semantic":
        observation["stages"].append("invoke_inspect_guard")
        observation["value"] = {
            "classification": outcome["classification"],
            "disposition": outcome["disposition"],
            "fuel": arguments["maximum_evaluation_steps"],
        }
    else:
        observation["value"] = {"classification": outcome["classification"]}


def execute(payload: dict[str, Any]) -> dict[str, Any]:
    request = payload["request"]
    root = Path(payload["profile_root"])
    installed = request["installed"]
    arguments = request["arguments"]
    operation = request["operation"]
    observation = _empty()
    loaded: dict[str, str] = {}
    closure = SourceClosure(
        root,
        tuple(payload["source_files"]),
        payload["source_closure_file"],
        _SOURCE_DOMAIN,
        "provider/test_provider.py",
    )
    closure_digest = closure.digest()
    try:
        if operation == "compile":
            observation["stages"].append("validate_source")
            source = json.loads((root / arguments["source_file"]).read_text())
            if "source_digest_override" in arguments:
                source["artifact_digest"] = arguments["source_digest_override"]
            if "source_override" in arguments:
                source["content"]["regions"][0]["source"] = arguments["source_override"]
                from determa.state.wire import hash_value, typed_value

                source["artifact_digest"] = hash_value(
                    [source["artifact_format"], "1", typed_value(source["content"])]
                )
            if "invalid_slot" in arguments:
                content = source["content"]
                region = content["regions"][0]
                if arguments["invalid_slot"] == "metadata_guard":
                    content["template"]["meta"] = {"guard": "true"}
                    region["locator"] = "/meta/guard"
                else:
                    content["template"]["machines"][0]["root"]["variables"] = {
                        "data": {"type": "map", "init": {"action": []}}
                    }
                    region.update(
                        kind="actions", locator="/machines/0/root/variables/data/init/action"
                    )
                from determa.state.wire import hash_value, typed_value

                source["artifact_digest"] = hash_value(
                    [source["artifact_format"], "1", typed_value(content)]
                )
            from determa.state.compilation import _source_preflight, _verify_compilation_manifest

            _source_preflight(source)
            observation["stages"].append("resolve_compiler_closure")
            registry, _module, loaded = _installed(
                root, payload, installed, arguments, compiler=True
            )
            observation["stages"].append("compile_region")
            if (
                "maximum_compilation_steps" in arguments
                and arguments["maximum_compilation_steps"] <= 0
            ):
                raise RuntimeProviderError("language_compilation_limit_exceeded")
            # Compile through the public API. The source and manifest remain exact
            # inputs; the generated file, when supplied, is an independent check.
            if "manifest_file" in arguments:
                manifest = json.loads((root / arguments["manifest_file"]).read_text())
                if "manifest_fingerprint_override" in arguments:
                    manifest["content"]["generated_validated_bundle_fingerprint"] = arguments[
                        "manifest_fingerprint_override"
                    ]
                    from determa.state.wire import hash_value, typed_value

                    manifest["artifact_digest"] = hash_value(
                        [manifest["artifact_format"], "1", typed_value(manifest["content"])]
                    )
            else:
                manifest = None
            previous_profile = sys.getprofile()

            def trace(frame, event, arg):
                if event == "call":
                    if frame.f_code is _module.compile_region.__code__:
                        observation["calls"]["compile_region"] += 1
                    elif (
                        frame.f_code is load_bundle.__code__
                        and frame.f_back.f_code is compile_language_source.__code__
                    ):
                        observation["stages"].append("strict_load")
                    elif frame.f_code is _verify_compilation_manifest.__code__:
                        observation["stages"].append("verify_manifest")

            sys.setprofile(trace)
            try:
                bundle = compile_language_source(
                    source,
                    registry,
                    manifest=manifest,
                    maximum_compilation_steps=arguments.get("maximum_compilation_steps", 1000),
                )
            finally:
                sys.setprofile(previous_profile)
            if "generated_bundle_file" in arguments:
                supplied = load_bundle(
                    json.loads((root / arguments["generated_bundle_file"]).read_text())
                )
                if supplied.fingerprint != bundle.fingerprint:
                    raise RuntimeProviderError("language_compilation_failed")
            observation["result"] = "accepted"
            observation["value"] = {
                "generated_guard": bundle.raw["machines"][0]["root"]["states"]["pending"][
                    "on_events"
                ]["submit"]["guard"]
            }
            assert bundle.source_compilation is not None
            evidence = bundle.source_compilation["manifest"]["content"]
            observation["effective_capabilities"] = evidence["source_capabilities"]
            if arguments.get("without_manifest") and arguments.get("weak_compiler"):
                generated_registry = RuntimeProviderRegistry()
                generated = load_bundle(bundle.raw, runtime_providers=generated_registry)
                created = create(generated, "order", "compiled-restore-root", "compiled-create", {})
                before_restore_calls = observation["calls"]["compile_region"]
                sys.setprofile(trace)
                try:
                    restored = restore_aggregate(
                        created["state"],
                        MemoryArtifactResolver(definitions={generated.fingerprint: generated}),
                    )
                finally:
                    sys.setprofile(previous_profile)
                observation["value"].update(
                    source_artifact_digest=evidence["source_artifact_digest"],
                    compiler_providers=evidence["compiler_providers"],
                    generated_validated_bundle_fingerprint=evidence[
                        "generated_validated_bundle_fingerprint"
                    ],
                    generated_runtime_capabilities=generated_registry.effective_capabilities(
                        generated.raw
                    ),
                    restored_runtime_capabilities=generated_registry.effective_capabilities(
                        restored.bundle.raw
                    ),
                    restore_compiler_calls=observation["calls"]["compile_region"]
                    - before_restore_calls,
                )
        else:
            observation["stages"].append(
                "resolve_runtime_closure" if operation == "restore" else "resolve_closure"
            )
            if operation == "restore" and not installed["providers"]:
                registry = RuntimeProviderRegistry()
                module = None
            else:
                registry, module, loaded = _installed(
                    root, payload, installed, arguments, compiler=False
                )
            source_bundle = root / request["bundle"]
            document = (
                json.loads(source_bundle.read_text())
                if source_bundle.suffix == ".json"
                else yaml.safe_load(source_bundle.read_text())
            )
            if operation == "load":
                observation["stages"].append("verify_capabilities")
            bundle = load_bundle(
                document,
                runtime_providers=registry,
                required_capabilities=(
                    frozenset(_GUARANTEES)
                    if operation == "load" and not arguments["opt_in_weak"]
                    else frozenset()
                ),
            )
            observation["effective_capabilities"] = registry.effective_capabilities(bundle.raw)
            if operation == "load":
                observation["stages"].append("load")
                observation["result"] = "accepted"
            elif operation == "step":
                _run_step(bundle, registry, request, observation)
            elif operation == "create":
                _run_create(bundle, registry, request, observation)
            elif operation == "host_commit":
                _run_host_commit(bundle, registry, request, observation)
            elif operation == "inspect":
                _run_inspect(bundle, registry, request, observation)
            elif operation == "restore":
                if bundle.fingerprint != arguments["definition_fingerprint"]:
                    raise RuntimeProviderError("runtime_provider_unavailable")
                resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
                creation = create(
                    bundle, bundle.machines[0]["machine_id"], "restore-root", "restore-create", {}
                )
                restore_aggregate(creation["state"], resolver)
                observation["stages"].append("restore")
                observation["result"] = "accepted"
                if module is None:
                    observation["effective_capabilities"] = _profile()
                else:
                    _provider_counts(registry, observation)
    except ValidationError as exc:
        # Static validation precedes provider capability verification inside the loader.
        if observation["stages"][-1:] == ["verify_capabilities"]:
            observation["stages"].pop()
        observation["stages"].append("load")
        observation["code"] = exc.code
    except RuntimeProviderError as exc:
        observation["code"] = exc.code
    return {
        "observation": observation,
        "loaded_source": loaded,
        "loaded_closure_digest": closure_digest,
    }


def main() -> None:
    payload = json.load(sys.stdin)
    json.dump(execute(payload), sys.stdout, ensure_ascii=False, allow_nan=False)


if __name__ == "__main__":
    main()
