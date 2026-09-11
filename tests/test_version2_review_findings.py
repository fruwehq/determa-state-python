from __future__ import annotations

import copy
from typing import Any

import pytest

from determa.state import (
    ArtifactError,
    ExecutionHost,
    MemoryArtifactResolver,
    MemoryExecutionStore,
    admit_aggregate_v2,
    admit_checkpoint_v2,
    aggregate_shape_fingerprint,
    create_aggregate_v2,
    create_checkpoint_v2,
    downgrade_aggregate_v2_to_v1,
    load_bundle,
    migrate_aggregate_v2,
    restore_aggregate_package,
    restore_aggregate_v2,
    restore_execution_checkpoint_v2,
    seal_aggregate_v2,
    seal_execution_checkpoint,
    serialize_execution_checkpoint,
    step_aggregate_v2,
    step_checkpoint_v2,
)
from determa.state.queueing import _entry_digest
from determa.state.wire import (
    aggregate_state_digest,
    load_json_artifact,
    migration_descriptor_digest,
    typed_value,
)


def _bundle(*, deferred: bool = False, external: bool = False) -> Any:
    events: dict[str, Any] = {
        "go": {"direction": "input"},
        "hold": {"direction": "input"},
        "tail": {"direction": "input"},
        "work": {"direction": "internal"},
    }
    action: list[dict[str, Any]] = []
    if external:
        events["outside"] = {"direction": "output"}
        action = [
            {
                "send": {
                    "event": "outside",
                    "to": {"external": True},
                    "correlation_id": "'effect'",
                }
            }
        ]
    root: dict[str, Any] = {
        "type": "simple",
        "on_events": {"go": {"action": action}, "tail": {}, "work": {}},
    }
    if deferred:
        root["deferred_events"] = ["hold"]
    return load_bundle(
        {
            "format": 1,
            "namespace": f"tests.review82.{deferred}.{external}",
            "events": events,
            "machines": [{"machine_id": "machine", "root": root}],
        }
    )


def _resolver(bundle: Any) -> MemoryArtifactResolver:
    return MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})


def _created(bundle: Any, root: str = "root") -> dict[str, Any]:
    result = create_aggregate_v2(bundle, "machine", root, "create", {})
    assert result["state"] is not None
    return result["state"]


def _delivery(aggregate: dict[str, Any], event_id: str, event: str = "go") -> dict[str, Any]:
    runtime = next(
        item for item in aggregate["runtimes"] if item["runtime_id"] == aggregate["root_runtime_id"]
    )
    envelope = {
        "event": event,
        "event_id": event_id,
        "cause_id": event_id,
        "source": {"host": True},
        "target": copy.deepcopy(runtime["target_identity"]),
        "payload": ["map", []],
    }
    return {
        "delivery_mode": "input",
        "envelope": envelope,
        "envelope_digest": _entry_digest(aggregate["root_instance_id"], "input", envelope),
    }


def _admit(aggregate: dict[str, Any], bundle: Any, *events: tuple[str, str]) -> dict[str, Any]:
    result = admit_aggregate_v2(
        aggregate,
        [_delivery(aggregate, event_id, event) for event_id, event in events],
        _resolver(bundle),
    )
    assert result["result"] == "accepted"
    return result["state"]


def _compatible_v2_descriptor(
    source: Any,
    target: Any,
    *,
    queued_event_rules: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    base = {
        "migration_descriptor_format": "determa.aggregate_migration",
        "migration_descriptor_schema_version": 1,
        "source_machine_format": 1,
        "target_machine_format": 1,
        "source_validated_bundle_fingerprint": source.fingerprint,
        "target_validated_bundle_fingerprint": target.fingerprint,
        "source_aggregate_shape_fingerprint": aggregate_shape_fingerprint(source),
        "target_aggregate_shape_fingerprint": aggregate_shape_fingerprint(target),
        "mode": "compatible",
        "mappings": {
            name: []
            for name in (
                "machines",
                "active_states",
                "variables",
                "history",
                "components",
                "owned_runtimes",
                "lifetime_holders",
                "counters",
            )
        },
        "terminal_policy": {"completed": "preserve", "faulted": "preserve"},
        "resource_requirements": {
            "maximum_transformed_output_bytes": "0",
            "maximum_cel_expression_length": "0",
            "maximum_cel_ast_nodes": "0",
            "maximum_cel_evaluation_steps": "0",
        },
    }
    base["migration_descriptor_digest"] = migration_descriptor_digest(base)
    descriptor = {
        "migration_descriptor_format": "determa.aggregate_migration",
        "migration_descriptor_schema_version": 2,
        "base_descriptor": base,
        "queued_event_default": "preserve_if_compatible",
        "queued_event_rules": queued_event_rules or [],
    }
    descriptor["migration_descriptor_digest"] = migration_descriptor_digest(descriptor)
    return descriptor


def test_create_v2_routes_initial_internal_and_external_work_with_provenance() -> None:
    bundle = load_bundle(
        {
            "format": 1,
            "namespace": "tests.review82.creation",
            "events": {
                "inside": {"direction": "internal"},
                "outside": {"direction": "output"},
            },
            "machines": [
                {
                    "machine_id": "machine",
                    "root": {
                        "type": "simple",
                        "entry": [
                            {"send": {"event": "inside"}},
                            {
                                "send": {
                                    "event": "outside",
                                    "to": {"external": True},
                                    "correlation_id": "'creation'",
                                }
                            },
                        ],
                        "on_events": {"inside": {}},
                    },
                }
            ],
        }
    )
    result = create_aggregate_v2(bundle, "machine", "root", "create", {})
    state = result["state"]
    assert state["next_acceptance_sequence"] == "1"
    entry = state["runtimes"][0]["ready_mailbox"][0]
    assert entry["envelope"]["cause_id"] != entry["envelope"]["event_id"]
    assert entry["envelope"]["source"] == {"runtime": state["runtimes"][0]["target_identity"]}
    assert ["kind" not in item for item in result["emissions"]] == [False, True]
    checkpoint = create_checkpoint_v2(bundle, "machine", "other", "create", {})
    assert len(checkpoint["pending_outbox_intents"]) == 1
    assert len(checkpoint["operation_receipts"][0]["emission_references"]) == 2


def test_pre_step_rejection_preserves_ready_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = _bundle()
    aggregate = _admit(_created(bundle), bundle, ("event", "go"))
    before = copy.deepcopy(aggregate)

    def rejected(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {
            "status": "running",
            "disposition": "rejected",
            "state": args[1],
            "emissions": [],
            "fault": None,
            "rejection": {"code": "invalid_instance_target"},
        }

    monkeypatch.setattr("determa.state.queueing.dispatch", rejected)
    result = step_aggregate_v2(aggregate, aggregate["root_runtime_id"], _resolver(bundle))
    assert result["state"] == before


def test_pending_replay_uses_canonical_digest_not_supplied_digest() -> None:
    bundle = _bundle()
    checkpoint = create_checkpoint_v2(bundle, "machine", "root", "create", {})
    delivery = _delivery(checkpoint["root_record"]["aggregate_state"], "same")
    admitted = admit_checkpoint_v2(
        checkpoint,
        [delivery],
        _resolver(bundle),
        expected_revision="0",
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    changed = copy.deepcopy(delivery)
    changed["envelope"]["event"] = "tail"
    with pytest.raises(ArtifactError, match="event_id_conflict"):
        admit_checkpoint_v2(
            admitted,
            [changed],
            _resolver(bundle),
            expected_revision=admitted["revision"],
            expected_checkpoint_digest=admitted["execution_checkpoint_digest"],
        )


def test_checkpoint_step_creates_external_outbox_reference() -> None:
    bundle = _bundle(external=True)
    checkpoint = create_checkpoint_v2(bundle, "machine", "root", "create", {})
    delivery = _delivery(checkpoint["root_record"]["aggregate_state"], "go")
    admitted = admit_checkpoint_v2(
        checkpoint,
        [delivery],
        _resolver(bundle),
        expected_revision="0",
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    stepped = step_checkpoint_v2(
        admitted,
        admitted["root_record"]["aggregate_state"]["root_runtime_id"],
        _resolver(bundle),
        expected_revision=admitted["revision"],
        expected_checkpoint_digest=admitted["execution_checkpoint_digest"],
    )
    assert len(stepped["pending_outbox_intents"]) == 1
    assert stepped["operation_receipts"][-1]["emission_references"][0]["kind"] == (
        "external_outbox"
    )


def test_v1_host_gate_is_real_and_store_is_unchanged() -> None:
    bundle = _bundle(deferred=True)
    host = ExecutionHost(MemoryExecutionStore(), _resolver(bundle))
    host.create(bundle, "machine", "root", "create", {})
    before = host.read_checkpoint("root")
    assert before is not None
    aggregate = before.document["root_record"]["aggregate_state"]
    candidate = {
        "root_instance_id": "root",
        "delivery_mode": "input",
        "origin": {"kind": "host_input"},
        "envelope": {
            key: value
            for key, value in _delivery(aggregate, "go")["envelope"].items()
            if key not in {"cause_id", "source"}
        },
    }
    result = host.accept_delivery(
        "root",
        candidate,
        expected_revision=before.document["revision"],
        expected_checkpoint_digest=before.document["execution_checkpoint_digest"],
        selected_bundle=bundle,
    )
    assert result["failure"]["code"] == "checkpoint_upgrade_required"
    assert host.read_checkpoint("root").source_bytes == before.source_bytes


def test_execution_host_round_trips_v2_transactions() -> None:
    bundle = _bundle()
    host = ExecutionHost(MemoryExecutionStore(), _resolver(bundle))
    host.create_v2(bundle, "machine", "root", "create", {})
    restored = host.read_checkpoint("root")
    assert restored is not None
    assert restored.document["execution_checkpoint_schema_version"] == 2
    delivery = _delivery(restored.document["root_record"]["aggregate_state"], "go")
    host.admit_v2(
        "root",
        [delivery],
        expected_revision=restored.document["revision"],
        expected_checkpoint_digest=restored.document["execution_checkpoint_digest"],
    )
    admitted = host.read_checkpoint("root")
    host.process_ready_v2(
        "root",
        admitted.document["root_record"]["aggregate_state"]["root_runtime_id"],
        expected_revision=admitted.document["revision"],
        expected_checkpoint_digest=admitted.document["execution_checkpoint_digest"],
    )
    assert (
        host.read_checkpoint("root").document["operation_receipts"][-1]["operation_kind"]
        == "event_terminal"
    )


def test_execution_host_prunes_and_tombstones_v2_transactionally() -> None:
    bundle = _bundle()
    host = ExecutionHost(MemoryExecutionStore(), _resolver(bundle))
    host.create_v2(bundle, "machine", "root", "create", {})
    created = host.read_checkpoint("root")
    delivery = _delivery(created.document["root_record"]["aggregate_state"], "go")
    host.admit_v2(
        "root",
        [delivery],
        expected_revision=created.document["revision"],
        expected_checkpoint_digest=created.document["execution_checkpoint_digest"],
    )
    admitted = host.read_checkpoint("root")
    host.process_ready_v2(
        "root",
        admitted.document["root_record"]["aggregate_state"]["root_runtime_id"],
        expected_revision=admitted.document["revision"],
        expected_checkpoint_digest=admitted.document["execution_checkpoint_digest"],
    )
    terminal = host.read_checkpoint("root")
    host.update_replay_retention(
        "root",
        {
            "mode": "bounded",
            "permanent_replay_eligible": False,
            "pruned_through_receipt_sequence": None,
            "policy_identifier": "test-policy",
        },
        expected_revision=terminal.document["revision"],
        expected_checkpoint_digest=terminal.document["execution_checkpoint_digest"],
    )
    bounded = host.read_checkpoint("root")
    host.prune_v2(
        "root",
        "2",
        expected_revision=bounded.document["revision"],
        expected_checkpoint_digest=bounded.document["execution_checkpoint_digest"],
    )
    assert host.read_checkpoint("root").document["event_identity_tombstones"][0]["event_id"] == "go"

    terminal_bundle = load_bundle(
        {
            "format": 1,
            "namespace": "tests.review82.tombstone",
            "machines": [
                {
                    "machine_id": "machine",
                    "root": {"type": "simple", "entry": [{"stop": {}}]},
                }
            ],
        }
    )
    terminal_host = ExecutionHost(MemoryExecutionStore(), _resolver(terminal_bundle))
    terminal_host.create_v2(terminal_bundle, "machine", "terminal", "create", {})
    before = terminal_host.read_checkpoint("terminal")
    terminal_host.tombstone_root_v2(
        "terminal",
        "delete",
        expected_revision=before.document["revision"],
        expected_checkpoint_digest=before.document["execution_checkpoint_digest"],
    )
    assert terminal_host.read_checkpoint("terminal").document["root_record"]["status"] == (
        "tombstone"
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda delivery: delivery["envelope"].update({"extra": True}),
        lambda delivery: delivery["envelope"].update({"source": {"host": False}}),
        lambda delivery: delivery.update({"extra": True}),
        lambda delivery: delivery["envelope"].update({"target": {}}),
    ],
)
def test_admission_rejects_closed_envelope_shapes_without_leaking(
    mutate: Any,
) -> None:
    bundle = _bundle()
    aggregate = _created(bundle)
    delivery = _delivery(aggregate, "event")
    mutate(delivery)
    result = admit_aggregate_v2(aggregate, [delivery], _resolver(bundle))
    assert result["result"] == "rejected"
    assert result["state"] == aggregate


def _terminal_checkpoint() -> tuple[Any, MemoryArtifactResolver, dict[str, Any]]:
    bundle = _bundle()
    resolver = _resolver(bundle)
    checkpoint = create_checkpoint_v2(bundle, "machine", "root", "create", {})
    delivery = _delivery(checkpoint["root_record"]["aggregate_state"], "go")
    admitted = admit_checkpoint_v2(
        checkpoint,
        [delivery],
        resolver,
        expected_revision="0",
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    terminal = step_checkpoint_v2(
        admitted,
        admitted["root_record"]["aggregate_state"]["root_runtime_id"],
        resolver,
        expected_revision=admitted["revision"],
        expected_checkpoint_digest=admitted["execution_checkpoint_digest"],
    )
    return bundle, resolver, terminal


@pytest.mark.parametrize("case", ["counter", "digest", "sequence", "overlap", "evidence"])
def test_checkpoint_restore_rejects_identity_allocation_corruption(case: str) -> None:
    _bundle_value, resolver, checkpoint = _terminal_checkpoint()
    candidate = copy.deepcopy(checkpoint)
    aggregate = candidate["root_record"]["aggregate_state"]
    terminal = candidate["operation_receipts"][-1]
    if case == "counter":
        aggregate["next_acceptance_sequence"] = "0"
    elif case == "digest":
        terminal["request_digest"] = "sha256:" + "0" * 64
    elif case == "sequence":
        aggregate["next_acceptance_sequence"] = "2"
        terminal["acceptance_sequence"] = "1"
    elif case == "overlap":
        candidate["event_identity_tombstones"].append(
            {
                "event_id": terminal["event_id"],
                "request_digest": terminal["request_digest"],
                "request_digest_domain": "determa-inbox-envelope-digest-2",
                "acceptance_sequence": terminal["acceptance_sequence"],
                "terminal_receipt_sequence": terminal["receipt_sequence"],
                "terminal_disposition": terminal["outcome"]["disposition"],
            }
        )
    else:
        terminal["event_id"] = "unowned-terminal"
    candidate["root_record"]["aggregate_state"] = seal_aggregate_v2(aggregate)
    candidate = seal_execution_checkpoint(candidate)
    with pytest.raises(ArtifactError, match="invalid_execution_checkpoint"):
        restore_execution_checkpoint_v2(candidate, resolver)


def test_permanent_prune_rejects_without_mutation() -> None:
    _bundle_value, resolver, checkpoint = _terminal_checkpoint()
    before = copy.deepcopy(checkpoint)
    from determa.state import prune_checkpoint_v2

    with pytest.raises(ArtifactError, match="invalid_execution_checkpoint"):
        prune_checkpoint_v2(
            checkpoint,
            "1",
            resolver,
            expected_revision=checkpoint["revision"],
            expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
        )
    assert checkpoint == before


def test_unhandled_delivery_allocates_no_logical_step() -> None:
    bundle = _bundle()
    aggregate = _admit(_created(bundle), bundle, ("unhandled", "hold"))
    before = aggregate["next_logical_step_sequence"]
    result = step_aggregate_v2(aggregate, aggregate["root_runtime_id"], _resolver(bundle))
    assert result["disposition"] == "unhandled"
    assert result["state"]["next_logical_step_sequence"] == before


def test_downgrade_requires_zero_mailbox_counters() -> None:
    bundle = _bundle()
    aggregate = _created(bundle)
    aggregate["next_queue_sequence"] = "1"
    aggregate = seal_aggregate_v2(aggregate)
    with pytest.raises(ArtifactError, match="migration_totality_failure"):
        downgrade_aggregate_v2_to_v1(aggregate, _resolver(bundle))


def test_resource_bound_counter_validation_does_not_expand_counter() -> None:
    bundle = _bundle()
    checkpoint = create_checkpoint_v2(bundle, "machine", "root", "create", {})
    checkpoint["next_operation_receipt_sequence"] = "9" * 4097
    checkpoint = seal_execution_checkpoint(checkpoint)
    with pytest.raises(ArtifactError, match="invalid_execution_checkpoint"):
        restore_execution_checkpoint_v2(checkpoint, _resolver(bundle))


def test_lifecycle_dispositions_are_ready_then_deferred() -> None:
    bundle = load_bundle(
        {
            "format": 1,
            "namespace": "tests.review82.lifecycle",
            "events": {
                "finish": {"direction": "input"},
                "hold": {"direction": "input"},
                "tail": {"direction": "input"},
            },
            "machines": [
                {
                    "machine_id": "machine",
                    "root": {
                        "type": "simple",
                        "deferred_events": ["hold"],
                        "on_events": {
                            "finish": {"action": [{"stop": {}}]},
                            "tail": {},
                        },
                    },
                }
            ],
        }
    )
    aggregate = _admit(
        _created(bundle),
        bundle,
        ("hold", "hold"),
        ("finish", "finish"),
        ("tail", "tail"),
    )
    first = step_aggregate_v2(aggregate, aggregate["root_runtime_id"], _resolver(bundle))
    result = step_aggregate_v2(first["state"], aggregate["root_runtime_id"], _resolver(bundle))
    assert [item["event_id"] for item in result["lifecycle_dispositions"]] == [
        "tail",
        "hold",
    ]


def test_current_step_emissions_precede_structural_recall() -> None:
    bundle = load_bundle(
        {
            "format": 1,
            "namespace": "tests.review82.recall_order",
            "events": {
                "go": {"direction": "input"},
                "hold": {"direction": "input"},
                "work": {"direction": "internal"},
            },
            "machines": [
                {
                    "machine_id": "machine",
                    "root": {
                        "type": "composite",
                        "initial": {"transition_to": "waiting"},
                        "states": {
                            "waiting": {
                                "deferred_events": ["hold"],
                                "on_events": {
                                    "go": {
                                        "action": [{"send": {"event": "work"}}],
                                        "transition_to": "active",
                                    }
                                },
                            },
                            "active": {"on_events": {"work": {}}},
                        },
                    },
                }
            ],
        }
    )
    aggregate = _admit(_created(bundle), bundle, ("hold", "hold"), ("go", "go"))
    aggregate = step_aggregate_v2(aggregate, aggregate["root_runtime_id"], _resolver(bundle))[
        "state"
    ]
    result = step_aggregate_v2(aggregate, aggregate["root_runtime_id"], _resolver(bundle))
    ready = result["state"]["runtimes"][0]["ready_mailbox"]
    assert [entry["envelope"]["event"] for entry in ready] == ["work", "hold"], result
    assert int(ready[0]["queue_sequence"]) < int(ready[1]["queue_sequence"])


def test_contained_capacity_fault_notifies_owner_and_freezes_other_work() -> None:
    bundle = load_bundle(
        {
            "format": 1,
            "namespace": "tests.review82.contained_capacity",
            "events": {
                "start": {"direction": "input"},
                "work": {"direction": "internal"},
            },
            "machines": [
                {
                    "machine_id": "machine",
                    "root": {
                        "type": "parallel",
                        "on_events": {
                            "start": {
                                "action": [
                                    {
                                        "send": {
                                            "event": "work",
                                            "to": {"component": "worker"},
                                        }
                                    },
                                    {
                                        "send": {
                                            "event": "work",
                                            "to": {"component": "worker"},
                                        }
                                    },
                                ]
                            },
                            "determa.component_failed": {},
                        },
                        "components": [
                            {
                                "component_id": "worker",
                                "root": {
                                    "type": "simple",
                                    "deferred_event_capacity": 0,
                                    "deferred_events": ["work"],
                                },
                            },
                            {"component_id": "peer", "root": {"type": "simple"}},
                        ],
                    },
                }
            ],
        }
    )
    aggregate = _admit(_created(bundle), bundle, ("start", "start"))
    emitted = step_aggregate_v2(aggregate, aggregate["root_runtime_id"], _resolver(bundle))["state"]
    child = next(
        runtime for runtime in emitted["runtimes"] if runtime["relation"]["kind"] == "component"
    )
    result = step_aggregate_v2(emitted, child["runtime_id"], _resolver(bundle))
    child_after = next(
        runtime
        for runtime in result["state"]["runtimes"]
        if runtime["runtime_id"] == child["runtime_id"]
    )
    assert child_after["status"] == "faulted"
    assert len(child_after["ready_mailbox"]) == 1
    owner = next(
        runtime
        for runtime in result["state"]["runtimes"]
        if runtime["runtime_id"] == result["state"]["root_runtime_id"]
    )
    notification = owner["ready_mailbox"][-1]["envelope"]
    assert notification["event"] == "determa.component_failed"
    assert notification["source"] == {"system": "system:component_failure"}


def test_migration_structurally_recalls_with_fresh_queue_sequence() -> None:
    source = _bundle(deferred=True)
    target_document = copy.deepcopy(source.raw)
    target_document["machines"][0]["root"].pop("deferred_events")
    target = load_bundle(target_document)
    aggregate = _admit(_created(source), source, ("hold", "hold"))
    aggregate = step_aggregate_v2(aggregate, aggregate["root_runtime_id"], _resolver(source))[
        "state"
    ]
    shape = aggregate_shape_fingerprint(source)
    base = {
        "migration_descriptor_format": "determa.aggregate_migration",
        "migration_descriptor_schema_version": 1,
        "source_machine_format": 1,
        "target_machine_format": 1,
        "source_validated_bundle_fingerprint": source.fingerprint,
        "target_validated_bundle_fingerprint": target.fingerprint,
        "source_aggregate_shape_fingerprint": shape,
        "target_aggregate_shape_fingerprint": aggregate_shape_fingerprint(target),
        "mode": "compatible",
        "mappings": {
            name: []
            for name in (
                "machines",
                "active_states",
                "variables",
                "history",
                "components",
                "owned_runtimes",
                "lifetime_holders",
                "counters",
            )
        },
        "terminal_policy": {"completed": "preserve", "faulted": "preserve"},
        "resource_requirements": {
            "maximum_transformed_output_bytes": "0",
            "maximum_cel_expression_length": "0",
            "maximum_cel_ast_nodes": "0",
            "maximum_cel_evaluation_steps": "0",
        },
    }
    base["migration_descriptor_digest"] = migration_descriptor_digest(base)
    descriptor = {
        "migration_descriptor_format": "determa.aggregate_migration",
        "migration_descriptor_schema_version": 2,
        "base_descriptor": base,
        "queued_event_default": "preserve_if_compatible",
        "queued_event_rules": [],
    }
    descriptor["migration_descriptor_digest"] = migration_descriptor_digest(descriptor)
    resolver = MemoryArtifactResolver(
        definitions={source.fingerprint: source, target.fingerprint: target},
        migration_descriptors={descriptor["migration_descriptor_digest"]: descriptor},
    )
    prior_next = int(aggregate["next_queue_sequence"])
    migrated = migrate_aggregate_v2(
        aggregate,
        target.fingerprint,
        [descriptor["migration_descriptor_digest"]],
        resolver,
        maintenance_mode=True,
    )["aggregate_state"]
    runtime = migrated["runtimes"][0]
    assert not runtime["deferred_mailbox"]
    assert runtime["ready_mailbox"][0]["queue_sequence"] == str(prior_next)

    checkpoint = create_checkpoint_v2(source, "machine", "host-root", "create", {})
    host_delivery = _delivery(checkpoint["root_record"]["aggregate_state"], "host-hold", "hold")
    checkpoint = admit_checkpoint_v2(
        checkpoint,
        [host_delivery],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    checkpoint = step_checkpoint_v2(
        checkpoint,
        checkpoint["root_record"]["aggregate_state"]["root_runtime_id"],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    host = ExecutionHost(
        MemoryExecutionStore({"host-root": serialize_execution_checkpoint(checkpoint)}),
        resolver,
    )
    host.maintenance_migration_v2(
        "host-root",
        target.fingerprint,
        [descriptor["migration_descriptor_digest"]],
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    restored = host.read_checkpoint("host-root")
    assert (
        restored.document["root_record"]["aggregate_state"]["runtimes"][0]["ready_mailbox"][0][
            "envelope"
        ]["event"]
        == "hold"
    )


def test_restore_aggregate_package_accepts_strict_v2_package() -> None:
    bundle = _bundle()
    aggregate = _created(bundle)
    package = {
        "aggregate_state_package_format": "determa.aggregate_state_package",
        "aggregate_state_package_schema_version": 2,
        "aggregate_state": aggregate,
        "normalized_definitions": [
            {
                "validated_bundle_fingerprint": bundle.fingerprint,
                "normalized_bundle": typed_value(bundle.raw),
            }
        ],
        "migration_descriptors": [],
        "migration_route": [],
    }
    restored = restore_aggregate_package(package, MemoryArtifactResolver())
    assert restored.aggregate.aggregate_envelope == aggregate


def test_created_component_emission_uses_canonical_wire_source_and_restores() -> None:
    bundle = load_bundle(
        {
            "format": 1,
            "namespace": "tests.review83.component_source",
            "events": {"notice": {"direction": "internal"}},
            "machines": [
                {
                    "machine_id": "machine",
                    "root": {
                        "type": "parallel",
                        "on_events": {"notice": {}},
                        "components": [
                            {
                                "component_id": "sender",
                                "root": {
                                    "type": "simple",
                                    "entry": [
                                        {
                                            "send": {
                                                "event": "notice",
                                                "to": {"owner": True},
                                            }
                                        },
                                        {"stop": {}},
                                    ],
                                },
                            },
                            {"component_id": "peer", "root": {"type": "simple"}},
                        ],
                    },
                }
            ],
        }
    )
    result = create_aggregate_v2(bundle, "machine", "root", "create", {})
    aggregate = result["state"]
    entry = next(
        entry
        for runtime in aggregate["runtimes"]
        for entry in runtime["ready_mailbox"]
        if entry["envelope"]["event"] == "notice"
    )
    source = entry["envelope"]["source"]["runtime"]["component"]
    assert isinstance(source["activation_sequence"], str)
    assert aggregate_state_digest(aggregate) == aggregate["aggregate_state_digest"]
    assert restore_aggregate_v2(aggregate, _resolver(bundle)).aggregate_envelope == aggregate


def test_checkpoint_updates_internal_reference_on_deferral_and_recall() -> None:
    bundle = load_bundle(
        {
            "format": 1,
            "namespace": "tests.review83.reference_recall",
            "events": {
                "go": {"direction": "input"},
                "work": {"direction": "internal"},
            },
            "machines": [
                {
                    "machine_id": "machine",
                    "root": {
                        "type": "composite",
                        "entry": [{"send": {"event": "work"}}],
                        "initial": {"transition_to": "waiting"},
                        "states": {
                            "waiting": {
                                "deferred_events": ["work"],
                                "on_events": {"go": {"transition_to": "active"}},
                            },
                            "active": {"on_events": {"work": {}}},
                        },
                    },
                }
            ],
        }
    )
    resolver = _resolver(bundle)
    checkpoint = create_checkpoint_v2(bundle, "machine", "root", "create", {})
    runtime_id = checkpoint["root_record"]["aggregate_state"]["root_runtime_id"]
    checkpoint = step_checkpoint_v2(
        checkpoint,
        runtime_id,
        resolver,
        expected_revision="0",
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    deferred = checkpoint["root_record"]["aggregate_state"]["runtimes"][0]["deferred_mailbox"][0]
    reference = checkpoint["operation_receipts"][0]["emission_references"][0]
    assert reference["queue_sequence"] == deferred["queue_sequence"]
    delivery = _delivery(checkpoint["root_record"]["aggregate_state"], "go")
    checkpoint = admit_checkpoint_v2(
        checkpoint,
        [delivery],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    checkpoint = step_checkpoint_v2(
        checkpoint,
        runtime_id,
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    recalled = checkpoint["root_record"]["aggregate_state"]["runtimes"][0]["ready_mailbox"][0]
    reference = checkpoint["operation_receipts"][0]["emission_references"][0]
    assert reference["queue_sequence"] == recalled["queue_sequence"]
    restore_execution_checkpoint_v2(checkpoint, resolver)


def test_checkpoint_terminalizes_selected_and_lifecycle_disposed_references() -> None:
    bundle = load_bundle(
        {
            "format": 1,
            "namespace": "tests.review83.reference_terminal",
            "events": {
                "start": {"direction": "input"},
                "finish": {"direction": "internal"},
                "work": {"direction": "internal"},
            },
            "machines": [
                {
                    "machine_id": "machine",
                    "root": {
                        "type": "parallel",
                        "on_events": {
                            "start": {
                                "action": [
                                    {
                                        "send": {
                                            "event": "finish",
                                            "to": {"component": "worker"},
                                        }
                                    },
                                    {
                                        "send": {
                                            "event": "work",
                                            "to": {"component": "worker"},
                                        }
                                    },
                                ]
                            }
                        },
                        "components": [
                            {
                                "component_id": "worker",
                                "root": {
                                    "type": "simple",
                                    "on_events": {
                                        "finish": {"action": [{"stop": {}}]},
                                        "work": {},
                                    },
                                },
                            },
                            {"component_id": "peer", "root": {"type": "simple"}},
                        ],
                    },
                }
            ],
        }
    )
    resolver = _resolver(bundle)
    checkpoint = create_checkpoint_v2(bundle, "machine", "root", "create", {})
    checkpoint = admit_checkpoint_v2(
        checkpoint,
        [_delivery(checkpoint["root_record"]["aggregate_state"], "start", "start")],
        resolver,
        expected_revision="0",
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    checkpoint = step_checkpoint_v2(
        checkpoint,
        checkpoint["root_record"]["aggregate_state"]["root_runtime_id"],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    worker = next(
        runtime
        for runtime in checkpoint["root_record"]["aggregate_state"]["runtimes"]
        if runtime["relation"].get("component_id") == "worker"
    )
    producer = checkpoint["operation_receipts"][-1]
    checkpoint = step_checkpoint_v2(
        checkpoint,
        worker["runtime_id"],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    assert [reference["kind"] for reference in producer["emission_references"]] == [
        "internal_mailbox",
        "internal_mailbox",
    ]
    committed_producer = next(
        receipt
        for receipt in checkpoint["operation_receipts"]
        if receipt.get("event_id") == "start" and receipt["operation_kind"] == "event_terminal"
    )
    assert [reference["kind"] for reference in committed_producer["emission_references"]] == [
        "internal_terminal",
        "internal_terminal",
    ]
    restore_execution_checkpoint_v2(checkpoint, resolver)


@pytest.mark.parametrize("case", ["orphan", "receipt_order", "revision_order"])
def test_checkpoint_rejects_reverse_orphan_and_terminal_chronology(case: str) -> None:
    _bundle_value, resolver, checkpoint = _terminal_checkpoint()
    candidate = copy.deepcopy(checkpoint)
    acceptance = candidate["operation_receipts"][1]
    terminal = candidate["operation_receipts"][2]
    if case == "orphan":
        candidate["operation_receipts"].pop()
        candidate["next_operation_receipt_sequence"] = "2"
    elif case == "receipt_order":
        acceptance["receipt_sequence"] = "2"
        terminal["receipt_sequence"] = "1"
        candidate["operation_receipts"] = [
            candidate["operation_receipts"][0],
            terminal,
            acceptance,
        ]
    else:
        acceptance["accepted_revision"] = "2"
        terminal["committed_revision"] = "1"
    candidate = seal_execution_checkpoint(candidate)
    with pytest.raises(ArtifactError, match="invalid_execution_checkpoint"):
        restore_execution_checkpoint_v2(candidate, resolver)


@pytest.mark.parametrize("case", ["cause", "source", "event", "direction"])
def test_aggregate_restore_validates_mailbox_envelope_semantics(case: str) -> None:
    bundle = _bundle()
    aggregate = _admit(_created(bundle), bundle, ("event", "go"))
    entry = aggregate["runtimes"][0]["ready_mailbox"][0]
    if case == "cause":
        entry["envelope"]["cause_id"] = "other"
    elif case == "source":
        entry["envelope"]["source"] = {
            "runtime": copy.deepcopy(aggregate["runtimes"][0]["target_identity"])
        }
    elif case == "event":
        entry["envelope"]["event"] = "missing"
    else:
        entry["envelope"]["event"] = "work"
    entry["envelope_digest"] = _entry_digest(
        aggregate["root_instance_id"], entry["delivery_mode"], entry["envelope"]
    )
    aggregate = seal_aggregate_v2(aggregate)
    with pytest.raises(ArtifactError, match="invalid_aggregate_state"):
        restore_aggregate_v2(aggregate, _resolver(bundle))


def test_v1_upgrade_gate_finds_nested_inline_component_deferral() -> None:
    bundle = load_bundle(
        {
            "format": 1,
            "namespace": "tests.review83.inline_gate",
            "events": {"hold": {"direction": "internal"}},
            "machines": [
                {
                    "machine_id": "machine",
                    "root": {
                        "type": "parallel",
                        "components": [
                            {
                                "component_id": "nested",
                                "root": {
                                    "type": "composite",
                                    "initial": {"transition_to": "waiting"},
                                    "states": {"waiting": {"deferred_events": ["hold"]}},
                                },
                            },
                            {"component_id": "peer", "root": {"type": "simple"}},
                        ],
                    },
                }
            ],
        }
    )
    host = ExecutionHost(MemoryExecutionStore(), _resolver(bundle))
    host.create(bundle, "machine", "root", "create", {})
    before = host.read_checkpoint("root")
    aggregate = before.document["root_record"]["aggregate_state"]
    result = host.accept_delivery(
        "root",
        {
            "root_instance_id": "root",
            "delivery_mode": "input",
            "origin": {"kind": "host_input"},
            "envelope": {
                key: value
                for key, value in _delivery(aggregate, "event")["envelope"].items()
                if key not in {"cause_id", "source"}
            },
        },
        expected_revision=before.document["revision"],
        expected_checkpoint_digest=before.document["execution_checkpoint_digest"],
        selected_bundle=bundle,
    )
    assert result["failure"]["code"] == "checkpoint_upgrade_required"
    assert host.read_checkpoint("root").source_bytes == before.source_bytes


def test_pruning_rejects_pending_outbox_producer_removal() -> None:
    bundle = _bundle(external=True)
    resolver = _resolver(bundle)
    checkpoint = create_checkpoint_v2(bundle, "machine", "root", "create", {})
    checkpoint = admit_checkpoint_v2(
        checkpoint,
        [_delivery(checkpoint["root_record"]["aggregate_state"], "go")],
        resolver,
        expected_revision="0",
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    checkpoint = step_checkpoint_v2(
        checkpoint,
        checkpoint["root_record"]["aggregate_state"]["root_runtime_id"],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    checkpoint["replay_retention"] = {
        "mode": "bounded",
        "permanent_replay_eligible": False,
        "pruned_through_receipt_sequence": None,
        "policy_identifier": "test",
    }
    checkpoint = seal_execution_checkpoint(checkpoint)
    with pytest.raises(ArtifactError, match="invalid_execution_checkpoint"):
        from determa.state import prune_checkpoint_v2

        prune_checkpoint_v2(
            checkpoint,
            "2",
            resolver,
            expected_revision=checkpoint["revision"],
            expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
        )


def test_migration_rejects_payload_default_materialization() -> None:
    source = _bundle(deferred=True)
    target_document = copy.deepcopy(source.raw)
    target_document["events"]["hold"]["payload"] = {
        "added": {"type": "string", "default": "defaulted"}
    }
    target = load_bundle(target_document)
    descriptor = _compatible_v2_descriptor(source, target)
    aggregate = _admit(_created(source), source, ("hold", "hold"))
    aggregate = step_aggregate_v2(aggregate, aggregate["root_runtime_id"], _resolver(source))[
        "state"
    ]
    resolver = MemoryArtifactResolver(
        definitions={source.fingerprint: source, target.fingerprint: target},
        migration_descriptors={descriptor["migration_descriptor_digest"]: descriptor},
    )
    with pytest.raises(ArtifactError, match="migration_totality_failure"):
        migrate_aggregate_v2(
            aggregate,
            target.fingerprint,
            [descriptor["migration_descriptor_digest"]],
            resolver,
            maintenance_mode=True,
        )


def test_multihop_migration_recalls_each_hop_and_host_appends_exact_audit() -> None:
    source = _bundle(deferred=True)
    middle_document = copy.deepcopy(source.raw)
    middle_document["machines"][0]["root"].pop("deferred_events")
    middle = load_bundle(middle_document)
    target_document = copy.deepcopy(source.raw)
    target_document["events"]["extra"] = {"direction": "input"}
    target = load_bundle(target_document)
    first = _compatible_v2_descriptor(source, middle)
    second = _compatible_v2_descriptor(middle, target)
    resolver = MemoryArtifactResolver(
        definitions={
            source.fingerprint: source,
            middle.fingerprint: middle,
            target.fingerprint: target,
        },
        migration_descriptors={
            first["migration_descriptor_digest"]: first,
            second["migration_descriptor_digest"]: second,
        },
    )
    checkpoint = create_checkpoint_v2(source, "machine", "root", "create", {})
    checkpoint = admit_checkpoint_v2(
        checkpoint,
        [_delivery(checkpoint["root_record"]["aggregate_state"], "hold", "hold")],
        resolver,
        expected_revision="0",
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    checkpoint = step_checkpoint_v2(
        checkpoint,
        checkpoint["root_record"]["aggregate_state"]["root_runtime_id"],
        resolver,
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    old_queue = checkpoint["root_record"]["aggregate_state"]["runtimes"][0]["deferred_mailbox"][0][
        "queue_sequence"
    ]
    host = ExecutionHost(
        MemoryExecutionStore({"root": serialize_execution_checkpoint(checkpoint)}),
        resolver,
    )
    migrated = host.maintenance_migration_v2(
        "root",
        target.fingerprint,
        [first["migration_descriptor_digest"], second["migration_descriptor_digest"]],
        expected_revision=checkpoint["revision"],
        expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
    )
    runtime = migrated["root_record"]["aggregate_state"]["runtimes"][0]
    assert not runtime["deferred_mailbox"]
    assert int(runtime["ready_mailbox"][0]["queue_sequence"]) > int(old_queue)
    audits = migrated["migration_audit_records"]
    assert len(audits) == 2
    assert (
        audits[0]["source_aggregate_state_digest"]
        == checkpoint["root_record"]["aggregate_state"]["aggregate_state_digest"]
    )
    assert audits[0]["target_aggregate_state_digest"] == audits[1]["source_aggregate_state_digest"]
    assert (
        audits[1]["target_aggregate_state_digest"]
        == migrated["root_record"]["aggregate_state"]["aggregate_state_digest"]
    )
    restore_execution_checkpoint_v2(migrated, resolver)


def test_lifecycle_cleanup_orders_component_mailboxes_reverse_declaration() -> None:
    bundle = load_bundle(
        {
            "format": 1,
            "namespace": "tests.review83.cleanup_order",
            "events": {
                "start": {"direction": "input"},
                "finish": {"direction": "input"},
                "work": {"direction": "internal"},
            },
            "machines": [
                {
                    "machine_id": "machine",
                    "root": {
                        "type": "parallel",
                        "on_events": {
                            "start": {
                                "action": [
                                    {
                                        "send": {
                                            "event": "work",
                                            "to": {"component": "first"},
                                        }
                                    },
                                    {
                                        "send": {
                                            "event": "work",
                                            "to": {"component": "second"},
                                        }
                                    },
                                ]
                            },
                            "finish": {"action": [{"stop": {}}]},
                        },
                        "components": [
                            {"component_id": "first", "root": {"type": "simple"}},
                            {"component_id": "second", "root": {"type": "simple"}},
                        ],
                    },
                }
            ],
        }
    )
    aggregate = _admit(_created(bundle), bundle, ("start", "start"))
    aggregate = step_aggregate_v2(aggregate, aggregate["root_runtime_id"], _resolver(bundle))[
        "state"
    ]
    aggregate = _admit(aggregate, bundle, ("finish", "finish"))
    result = step_aggregate_v2(aggregate, aggregate["root_runtime_id"], _resolver(bundle))
    component_by_runtime = {
        runtime["runtime_id"]: runtime["relation"].get("component_id")
        for runtime in aggregate["runtimes"]
    }
    assert [
        component_by_runtime[item["target_runtime_id"]] for item in result["lifecycle_dispositions"]
    ] == ["second", "first"]


@pytest.mark.parametrize("first_failure", ["wrong_root", "conflict"])
def test_batch_malformed_precedes_wrong_root_and_conflict(first_failure: str) -> None:
    bundle = _bundle()
    resolver = _resolver(bundle)
    aggregate = _created(bundle)
    if first_failure == "conflict":
        aggregate = _admit(aggregate, bundle, ("same", "go"))
        first = _delivery(aggregate, "same", "tail")
    else:
        first = _delivery(aggregate, "first")
        first["envelope"]["target"]["root"]["root_instance_id"] = "wrong"
        first["envelope_digest"] = _entry_digest("root", "input", first["envelope"])
    malformed = _delivery(aggregate, "malformed")
    malformed["envelope"]["extra"] = True
    result = admit_aggregate_v2(aggregate, [first, malformed], resolver)
    assert result["rejection"]["code"] == "malformed_delivery"

    checkpoint = create_checkpoint_v2(bundle, "machine", "checkpoint", "create", {})
    if first_failure == "conflict":
        original = _delivery(checkpoint["root_record"]["aggregate_state"], "same")
        checkpoint = admit_checkpoint_v2(
            checkpoint,
            [original],
            resolver,
            expected_revision="0",
            expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
        )
        first = _delivery(checkpoint["root_record"]["aggregate_state"], "same", "tail")
    else:
        first = _delivery(checkpoint["root_record"]["aggregate_state"], "first")
        first["envelope"]["target"]["root"]["root_instance_id"] = "wrong"
        first["envelope_digest"] = _entry_digest("checkpoint", "input", first["envelope"])
    malformed = _delivery(checkpoint["root_record"]["aggregate_state"], "malformed")
    malformed["envelope"]["extra"] = True
    with pytest.raises(ArtifactError, match="malformed_delivery"):
        admit_checkpoint_v2(
            checkpoint,
            [first, malformed],
            resolver,
            expected_revision=checkpoint["revision"],
            expected_checkpoint_digest=checkpoint["execution_checkpoint_digest"],
        )


def test_invalid_core_step_result_reports_its_public_artifact_code() -> None:
    with pytest.raises(ArtifactError, match="invalid_core_step_result"):
        load_json_artifact(
            {
                "core_step_result_format": "determa.core_step_result",
                "core_step_result_schema_version": 2,
            },
            "core_step_result_v2",
        )
