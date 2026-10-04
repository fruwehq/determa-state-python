"""Read-only exact-target inspection and abstract-fuel boundaries."""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from determa.state import (
    MemoryArtifactResolver,
    create,
    create_checkpoint_v1,
    inspect_candidate,
    load_bundle,
)
from determa.state.inspection_cel import InspectionLimit, safe_evaluate
from determa.state.queueing import _entry_digest, admit_aggregate_v1
from determa.state.wire import canonical_bytes, typed_value

_MACHINE = """
format: 1
namespace: test.inspection
events:
  go: { direction: input }
machines:
  - machine_id: inspector
    root:
      type: composite
      initial: { transition_to: child }
      states:
        child:
          deferred_events: [go]
          on_events:
            go:
              - guard: "false"
                action: []
"""


def _case() -> tuple[dict, dict, MemoryArtifactResolver]:
    bundle = load_bundle(_MACHINE)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    aggregate = create(bundle, "inspector", "root-1", "create-1", {})["state"]
    runtime = aggregate["runtimes"][0]
    envelope = {
        "event": "go",
        "event_id": "candidate-1",
        "cause_id": "candidate-1",
        "source": {"host": True},
        "target": runtime["target_identity"],
        "payload": typed_value({}),
    }
    request = {
        "mode": "structural",
        "aggregate_state_digest": aggregate["aggregate_state_digest"],
        "runtime_id": runtime["runtime_id"],
        "runtime_incarnation": runtime["identity_origin"],
        "envelope": envelope,
        "limits": None,
    }
    return aggregate, request, resolver


def _semantic_case(
    guard: str,
    *,
    root_variables: dict | None = None,
    child_variables: dict | None = None,
    root_guard: str | None = None,
) -> tuple[dict, dict, MemoryArtifactResolver]:
    root: dict[str, Any] = {
        "type": "composite",
        "variables": root_variables or {},
        "initial": {"transition_to": "child"},
        "states": {
            "child": {
                "variables": child_variables or {},
                "on_events": {"go": {"guard": guard}},
            }
        },
    }
    if root_guard is not None:
        root["on_events"] = {"go": {"guard": root_guard}}
    bundle = load_bundle(
        {
            "format": 1,
            "namespace": "test.inspection.semantic",
            "events": {"go": {"direction": "input"}},
            "machines": [{"machine_id": "inspector", "root": root}],
        }
    )
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    aggregate = create(bundle, "inspector", "root-1", "create-1", {})["state"]
    runtime = aggregate["runtimes"][0]
    request = {
        "mode": "semantic",
        "aggregate_state_digest": aggregate["aggregate_state_digest"],
        "runtime_id": runtime["runtime_id"],
        "runtime_incarnation": runtime["identity_origin"],
        "envelope": {
            "event": "go",
            "event_id": "candidate-1",
            "cause_id": "candidate-1",
            "source": {"host": True},
            "target": runtime["target_identity"],
            "payload": typed_value({}),
        },
        "limits": {"maximum_guard_evaluations": "2", "maximum_evaluation_steps": "100"},
    }
    return aggregate, request, resolver


@pytest.mark.parametrize(
    ("source", "budget", "result"),
    [
        ("true", 1, True),
        ("!true", 3, False),
        ("false && true", 4, False),
        ('size("ab") == 2', 9, True),
    ],
)
def test_shared_fuel_exact_success(source: str, budget: int, result: bool) -> None:
    assert safe_evaluate(source, {}, budget) == (result, budget)
    with pytest.raises(InspectionLimit):
        safe_evaluate(source, {}, budget - 1)


def test_map_lookup_charges_all_entries_after_typed_record_selection() -> None:
    bindings = {"event": {"payload": {"mapped": {"foo": 1, "bar": 2}}}}
    assert safe_evaluate("event.payload.mapped.foo == 1", bindings, 34) == (True, 34)
    with pytest.raises(InspectionLimit):
        safe_evaluate("event.payload.mapped.foo == 1", bindings, 33)
    assert safe_evaluate("has(event.payload.mapped.foo)", bindings, 31) == (True, 31)


@pytest.mark.parametrize(
    ("source", "steps"),
    [
        ('string(true) == "true"', 20),
        ('string(1.0) == "1"', 8),
        ('string(-0.0) == "0"', 8),
        ('string(0.000001) == "0.000001"', 36),
        ('string(9223372036854775807) == "9223372036854775807"', 80),
    ],
)
def test_string_conversion_uses_canonical_values_and_fuel(source: str, steps: int) -> None:
    assert safe_evaluate(source, {}, steps) == (True, steps)
    with pytest.raises(InspectionLimit):
        safe_evaluate(source, {}, steps - 1)


def test_collection_equality_preserves_portable_scalar_types() -> None:
    assert safe_evaluate('{"x": true} == {"x": 1}', {}, 100)[0] is False
    assert safe_evaluate("true in [1]", {}, 100)[0] is False


@pytest.mark.parametrize(
    "guard",
    [
        "-3 / 2 == -1",
        "-3 % 2 == -1",
        "3 / -2 == -1",
        "3 % -2 == 1",
        "9223372036854775807 / 3 == 3074457345618258602",
        "9223372036854775807 % 3 == 1",
    ],
)
def test_semantic_integer_quotient_and_remainder_match_portable_cel(guard: str) -> None:
    from determa.state.cel import evaluate

    assert evaluate(guard, {}) is True
    aggregate, request, resolver = _semantic_case(guard)
    original = canonical_bytes(aggregate)
    result = inspect_candidate(aggregate, request, resolver)
    assert result["disposition"] == "handled_now"
    assert result["guard_evidence"][0]["value"] is True
    assert canonical_bytes(aggregate) == original


@pytest.mark.parametrize("guard", ["1 / 0 == 0", "-9223372036854775808 / -1 == 0"])
def test_semantic_integer_errors_have_no_partial_evidence(guard: str) -> None:
    aggregate, request, resolver = _semantic_case(guard)
    original = canonical_bytes(aggregate)
    result = inspect_candidate(aggregate, request, resolver)
    assert result == {
        "code": "inspection_guard_failure",
        "source_locator": "/machines/0/root/states/child/on_events/go/guard",
    }
    assert canonical_bytes(aggregate) == original


def test_fuel_exhaustion_precedes_integer_division_error() -> None:
    aggregate, request, resolver = _semantic_case("1 / 0 == 0")
    request["limits"]["maximum_evaluation_steps"] = "4"
    assert inspect_candidate(aggregate, request, resolver) == {
        "code": "inspection_limit_exceeded",
        "source_locator": "/machines/0/root/states/child/on_events/go/guard",
    }
    request["limits"]["maximum_evaluation_steps"] = "5"
    assert inspect_candidate(aggregate, request, resolver) == {
        "code": "inspection_guard_failure",
        "source_locator": "/machines/0/root/states/child/on_events/go/guard",
    }


def test_shadowed_ancestor_value_is_not_guard_visible() -> None:
    oversized = "x" * 65530
    aggregate, request, resolver = _semantic_case(
        "x == 'a'",
        root_variables={"x": {"type": "string", "init": oversized}},
        child_variables={"x": {"type": "string", "init": "a"}},
    )
    original = canonical_bytes(aggregate)
    result = inspect_candidate(aggregate, request, resolver)
    assert result["disposition"] == "handled_now"
    assert canonical_bytes(aggregate) == original


def test_all_unshadowed_visible_values_count_even_when_guard_does_not_read_them() -> None:
    aggregate, request, resolver = _semantic_case(
        "x == 'a'",
        root_variables={"other": {"type": "string", "init": "x" * 65530}},
        child_variables={"x": {"type": "string", "init": "a"}},
    )
    assert inspect_candidate(aggregate, request, resolver) == {
        "code": "inspection_limit_exceeded",
        "source_locator": "/machines/0/root/states/child/on_events/go/guard",
    }


def test_each_reached_guard_uses_its_own_lexical_visibility() -> None:
    aggregate, request, resolver = _semantic_case(
        "x == 'b'",
        root_variables={"x": {"type": "string", "init": "x" * 65530}},
        child_variables={"x": {"type": "string", "init": "a"}},
        root_guard="x == 'a'",
    )
    original = canonical_bytes(aggregate)
    assert inspect_candidate(aggregate, request, resolver) == {
        "code": "inspection_limit_exceeded",
        "source_locator": "/machines/0/root/on_events/go/guard",
    }
    assert canonical_bytes(aggregate) == original


def test_inspection_is_read_only_on_queued_aggregate(monkeypatch: pytest.MonkeyPatch) -> None:
    aggregate, request, resolver = _case()
    envelope = copy.deepcopy(request["envelope"])
    delivery = {
        "delivery_mode": "input",
        "envelope": envelope,
        "envelope_digest": _entry_digest(aggregate["root_instance_id"], "input", envelope),
    }
    queued = admit_aggregate_v1(aggregate, [delivery], resolver)["state"]
    request["aggregate_state_digest"] = queued["aggregate_state_digest"]
    snapshot = canonical_bytes(queued)
    request_snapshot = json.dumps(request, sort_keys=True)
    resolver_snapshot = resolver.snapshot()

    def ordinary_eval_forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("ordinary guard evaluator called")

    monkeypatch.setattr("determa.state.cel.evaluate", ordinary_eval_forbidden)
    structural = inspect_candidate(queued, request, resolver)
    assert structural["possible_dispositions"] == ["handled_now", "deferred"]
    assert structural["guard_evidence"] == []
    request["mode"] = "semantic"
    request["limits"] = {"maximum_guard_evaluations": "1", "maximum_evaluation_steps": "1"}
    semantic = inspect_candidate(queued, request, resolver)
    assert semantic["possible_dispositions"] == ["deferred"]
    assert semantic["guard_evidence"][0]["value"] is False
    assert canonical_bytes(queued) == snapshot
    assert resolver.snapshot() == resolver_snapshot
    assert queued["runtimes"][0]["ready_mailbox"]
    assert (
        json.dumps({**request, "mode": "structural", "limits": None}, sort_keys=True)
        == request_snapshot
    )


def test_request_and_target_precedence() -> None:
    aggregate, request, resolver = _case()
    bad = copy.deepcopy(request)
    bad["extra"] = True
    assert inspect_candidate(aggregate, bad, resolver) == {
        "code": "invalid_inspection_request",
        "source_locator": None,
    }
    bad = copy.deepcopy(request)
    bad["runtime_id"] = "sha256:" + "0" * 64
    bad["envelope"]["event"] = "unknown"
    assert inspect_candidate(aggregate, bad, resolver)["reason"] == "target_not_found"
    bad = copy.deepcopy(request)
    bad["runtime_incarnation"]["root_instance_id"] = "stale"
    bad["envelope"]["event"] = "unknown"
    assert inspect_candidate(aggregate, bad, resolver)["reason"] == "target_incarnation_mismatch"
    bad = copy.deepcopy(request)
    bad["aggregate_state_digest"] = "sha256:" + "0" * 64
    assert inspect_candidate(aggregate, bad, resolver) == {
        "code": "invalid_inspection_request",
        "source_locator": None,
    }


def test_inspection_leaves_checkpoint_receipts_and_outbox_unchanged() -> None:
    bundle = load_bundle(_MACHINE)
    resolver = MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
    checkpoint = create_checkpoint_v1(bundle, "inspector", "root-1", "create-1", {})
    aggregate = checkpoint["root_record"]["aggregate_state"]
    runtime = aggregate["runtimes"][0]
    request = {
        "mode": "semantic",
        "aggregate_state_digest": aggregate["aggregate_state_digest"],
        "runtime_id": runtime["runtime_id"],
        "runtime_incarnation": runtime["identity_origin"],
        "envelope": {
            "event": "go",
            "event_id": "candidate",
            "cause_id": "candidate",
            "source": {"host": True},
            "target": runtime["target_identity"],
            "payload": typed_value({}),
        },
        "limits": {"maximum_guard_evaluations": "1", "maximum_evaluation_steps": "1"},
    }
    snapshot = canonical_bytes(checkpoint)
    result = inspect_candidate(aggregate, request, resolver)
    assert result["disposition"] == "deferred"
    assert canonical_bytes(checkpoint) == snapshot
    assert checkpoint["revision"] == "0"
    assert len(checkpoint["operation_receipts"]) == 1
    assert checkpoint["pending_outbox_intents"] == []
    assert checkpoint["terminal_outbox_records"] == []
