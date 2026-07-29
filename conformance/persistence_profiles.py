"""In-memory transactional driver for the optional persistence profile."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from determa.state import MemoryArtifactResolver, load_bundle, migrate_and_dispatch
from determa.state.wire import migration_descriptor_digest, strict_json

from .harness import conformance_root

PROFILE_DIR = conformance_root() / "conformance" / "profiles" / "persistence"

_RESOLVED = [
    "resolve_target_definition",
    "resolve_route",
    "resolve_descriptors",
    "verify_trust",
]
_TRANSACTION = ["begin_transaction", "lock_aggregate", "read_inbox"]
_COMMITTED = [
    *_RESOLVED,
    *_TRANSACTION,
    "migrate_aggregate",
    "dispatch_once",
    "stage_aggregate",
    "stage_inbox",
    "stage_outbox",
    "stage_audit",
    "commit",
]
_ROLLED_BACK = [*_RESOLVED, *_TRANSACTION, "migrate_aggregate", "rollback"]
_REPLAYED = [*_RESOLVED, *_TRANSACTION, "return_recorded_outcome", "commit"]
_QUARANTINED = [
    *_RESOLVED,
    *_TRANSACTION,
    "migration_failed_permanently",
    "stage_blocked_inbox",
    "stage_quarantine",
    "stage_failure_audit",
    "commit",
]


@dataclass(frozen=True)
class PersistenceProfileCase:
    name: str
    path: Path


def persistence_profile_cases() -> list[PersistenceProfileCase]:
    if not PROFILE_DIR.exists():
        return []
    return [
        PersistenceProfileCase(path.name, path)
        for path in sorted(PROFILE_DIR.iterdir())
        if path.is_dir() and (path / "test.yaml").exists()
    ]


def run_persistence_profile(case: PersistenceProfileCase) -> None:
    test = yaml.safe_load((case.path / "test.yaml").read_text(encoding="utf-8"))
    profile = test["persistence_profile"]
    store = _json(case.path / profile["initial_store"])
    resolver = _profile_resolver(case.path)
    for step in profile["steps"]:
        envelope = _json(case.path / step["input_envelope"])
        event_id = envelope["event_id"]
        operation = step["operation"]
        if operation == "replay":
            assert any(item["event_id"] == event_id for item in store["inbox"])
            call_log = list(_REPLAYED)
            if event_id not in store["acknowledged_event_ids"]:
                store["acknowledged_event_ids"].append(event_id)
                call_log.append("acknowledge_input")
        elif (
            operation == "inject_failure"
            and step["failure_class"] == "permanent"
        ):
            call_log = list(_QUARANTINED)
            store["inbox"].append({"event_id": event_id, "status": "blocked"})
            store["quarantine"].append(
                {
                    "event_id": event_id,
                    "code": "migration_totality_failure",
                    "aggregate_state_digest": store["aggregate_state"][
                        "aggregate_state_digest"
                    ],
                }
            )
        else:
            candidate = _process(
                store,
                envelope,
                resolver,
                step["target_validated_bundle_fingerprint"],
                step["migration_route"],
            )
            boundary = step.get("failure_boundary")
            if operation == "inject_failure" and boundary != (
                "after_commit_before_acknowledgement"
            ):
                call_log = list(_ROLLED_BACK)
            else:
                store = candidate
                call_log = list(_COMMITTED)
                if boundary != "after_commit_before_acknowledgement":
                    store["acknowledged_event_ids"].append(event_id)
                    call_log.append("acknowledge_input")
        assert store == _json(case.path / step["expect_store"]), step
        assert call_log == _json(case.path / step["expect_call_log"]), step


def _process(
    store: dict[str, Any],
    envelope: dict[str, Any],
    resolver: MemoryArtifactResolver,
    target_fingerprint: str,
    route: list[str],
) -> dict[str, Any]:
    target_bundle = resolver.resolve_definition(target_fingerprint)
    assert target_bundle is not None
    bundle = target_bundle if hasattr(target_bundle, "raw") else load_bundle(target_bundle)
    event_declaration = (bundle.raw.get("events") or {}).get(envelope["event"])
    delivery_kind = (
        "input"
        if isinstance(event_declaration, dict)
        and event_declaration.get("direction") == "input"
        else "internal"
    )
    result = migrate_and_dispatch(
        store["aggregate_state"],
        target_fingerprint,
        route,
        resolver,
        {delivery_kind: copy.deepcopy(envelope)},
        maintenance_mode=False,
    )
    assert result.failure is None
    assert result.aggregate_envelope is not None
    disposition = (
        "unhandled"
        if result.disposition == "rejected"
        and result.rejection == {"code": "invalid_event"}
        else result.disposition
    )
    candidate = copy.deepcopy(store)
    candidate["aggregate_state"] = result.aggregate_envelope
    candidate["inbox"] = [
        item for item in candidate["inbox"] if item["event_id"] != envelope["event_id"]
    ]
    candidate["inbox"].append(
        {
            "event_id": envelope["event_id"],
            "disposition": disposition,
            "status": "committed",
        }
    )
    candidate["outbox"].extend(copy.deepcopy(result.emissions))
    candidate["migration_audit"].extend(copy.deepcopy(result.audit_records))
    candidate["quarantine"] = [
        item
        for item in candidate["quarantine"]
        if item["event_id"] != envelope["event_id"]
    ]
    return candidate


def _profile_resolver(path: Path) -> MemoryArtifactResolver:
    definitions = {}
    for filename in ("machine.yaml", "target.yaml"):
        bundle = load_bundle((path / filename).read_text(encoding="utf-8"))
        definitions[bundle.fingerprint] = bundle
    descriptor = _json(path / "migration-descriptor.json")
    digest = migration_descriptor_digest(descriptor)
    return MemoryArtifactResolver(
        definitions=definitions,
        migration_descriptors={digest: descriptor},
    )


def _json(path: Path) -> Any:
    value, _ = strict_json(path.read_bytes())
    return value
