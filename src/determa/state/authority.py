"""Optional local scope authority for one SQLite storage boundary.

This module is deliberately separate from the pure evaluator.  The database is
the authority domain: every operation, receipt, checkpoint payload and inventory
member is serialized by the same BEGIN IMMEDIATE transaction.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import re
import sqlite3
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from functools import cache
from pathlib import Path
from typing import Any

from .__about__ import __version__
from .checkpoint import execution_checkpoint_digest, validate_execution_checkpoint_member
from .extensions import ConfiguredExtension, ExtensionRegistry
from .stores.base import ExecutionStoreError, ExecutionStoreTransaction, checkpoint_metadata
from .stores.sqlite import SQLiteExecutionStore, _SQLiteTransaction
from .wire import hash_value

_INTERFACE = "determa.host_authority"
_OPERATIONS = frozenset(
    {"read_authority", "guarded_commit", "freeze_scope", "fence_worker", "prove_retirement"}
)
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)\Z")
_ARGUMENTS = {
    "read_authority": frozenset(),
    "guarded_commit": frozenset({"mutation_digest"}),
    "freeze_scope": frozenset(),
    "fence_worker": frozenset(
        {
            "root_instance_id",
            "work_kind",
            "work_identity",
            "operation_token",
            "expected_attempt_fence",
            "expected_worker_principal",
        }
    ),
    "prove_retirement": frozenset({"destination_binding_digest", "freeze_evidence_digest"}),
}
_AUTHORITY_TABLE_SQL = (
    "CREATE TABLE determa_scope_authority (scope_identity TEXT PRIMARY KEY, ledger TEXT NOT NULL)"
)
_ALLOCATION_TABLE_SQL = "CREATE TABLE determa_scope_allocations (scope_identity TEXT PRIMARY KEY)"
_ALLOCATION_TRIGGER_SQL = (
    "CREATE TRIGGER determa_scope_allocations_forbid_delete "
    "BEFORE DELETE ON determa_scope_allocations BEGIN "
    "SELECT RAISE(ABORT, 'scope_allocation_immutable'); END"
)


@cache
def _request_validator() -> Any:
    from jsonschema import Draft202012Validator

    schema = json.loads(
        (Path(__file__).parent / "data/host-authority-operation-v1.schema.json").read_text()
    )
    return Draft202012Validator({"$defs": schema["$defs"], **schema["$defs"]["request"]})


def _compact(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON member")
        result[key] = value
    return result


def _parse(source: str) -> Any:
    return json.loads(
        source,
        object_pairs_hook=_unique,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")),
    )


def _result(
    request: Mapping[str, Any] | None,
    ledger: Mapping[str, Any] | None,
    code: str | None,
    *,
    early: bool = False,
) -> dict[str, Any]:
    response = {
        "interface": _INTERFACE,
        "interface_version": 1,
        "operation": None if early else request["operation"] if request else None,
        "operation_id": None if early else request["operation_id"] if request else None,
        "status": "rejected" if code else "accepted",
        "scope_identity": None
        if early or code == "unauthorized_scope"
        else request["scope_identity"]
        if request
        else None,
        "authority_epoch": None if early or ledger is None else ledger["authority_epoch"],
        "scope_generation": None if early or ledger is None else ledger["scope_generation"],
        "state": None if early or ledger is None else ledger["state"],
        "evidence_digest": None,
        "error_code": code,
        "claim": None,
    }
    if not code and request is not None:
        response["evidence_digest"] = hash_value(
            [
                "determa-host-authority-evidence-1",
                request["request_digest"],
                {key: value for key, value in response.items() if key != "evidence_digest"},
            ]
        )
    return response


def _valid_request(request: Any) -> str | None:
    if not isinstance(request, dict):
        return "invalid_host_request"
    if request.get("interface") != _INTERFACE:
        return "unsupported_host_protocol"
    if type(request.get("interface_version")) is not int or request["interface_version"] != 1:
        return "unsupported_host_protocol_version"
    if set(request) != {
        "interface",
        "interface_version",
        "operation",
        "operation_id",
        "scope_identity",
        "expected_authority_epoch",
        "expected_scope_generation",
        "arguments",
        "request_digest",
    }:
        return "invalid_host_request"
    if not _request_validator().is_valid(request):
        return "invalid_host_request"
    operation = request["operation"]
    if operation not in _OPERATIONS or not all(
        type(request[key]) is str and request[key] for key in ("operation_id", "scope_identity")
    ):
        return "invalid_host_request"
    for key in ("expected_authority_epoch", "expected_scope_generation"):
        value = request[key]
        if value is not None and (type(value) is not str or _DECIMAL.fullmatch(value) is None):
            return "invalid_host_request"
    if operation == "read_authority" and (
        request["expected_authority_epoch"] is not None
        or request["expected_scope_generation"] is not None
    ):
        return "invalid_host_request"
    arguments = request["arguments"]
    if type(arguments) is not dict or set(arguments) != _ARGUMENTS[operation]:
        return "invalid_host_request"
    for key, value in arguments.items():
        if key.endswith("_digest") and (type(value) is not str or _DIGEST.fullmatch(value) is None):
            return "invalid_host_request"
    if operation == "fence_worker":
        if arguments["work_kind"] != "effect" or not all(
            type(arguments[key]) is str and arguments[key]
            for key in (
                "root_instance_id",
                "work_identity",
                "operation_token",
                "expected_worker_principal",
            )
        ):
            return "invalid_host_request"
        fence = arguments["expected_attempt_fence"]
        if fence is not None and (type(fence) is not str or _DECIMAL.fullmatch(fence) is None):
            return "invalid_host_request"
    expected = hash_value(
        [
            "determa-host-authority-request-1",
            {key: value for key, value in request.items() if key != "request_digest"},
        ]
    )
    if request["request_digest"] != expected:
        return "invalid_host_request"
    return None


def _inventory(
    ledger: Mapping[str, Any], checkpoints: list[dict[str, Any]]
) -> list[dict[str, str]]:
    members: list[dict[str, str]] = []
    for field, kind in (
        ("roots", "root"),
        ("tombstones", "tombstone"),
        ("pending_intents", "pending_intent"),
        ("terminal_intents", "terminal_intent"),
        ("definition_references", "definition"),
        ("migration_references", "migration"),
    ):
        members.extend({"kind": kind, "identity": item} for item in ledger[field])
    members.extend(
        {"kind": "receipt", "identity": item["operation_id"]} for item in ledger["receipts"]
    )
    members.extend(
        {"kind": "checkpoint", "identity": str(index)}
        for index, _ in enumerate(ledger["checkpoint_bytes"])
    )
    members.extend(
        {"kind": "journal", "identity": item["work_identity"]} for item in ledger["journal_entries"]
    )
    members.extend(
        {"kind": "participant", "identity": item} for item in ledger["required_participant_records"]
    )
    # Discover logical references in retained checkpoint bytes as well as the
    # current native rows. Allocation-time hints cannot establish completeness.
    retained = []
    for source in ledger["checkpoint_bytes"]:
        document = _parse(source)
        if (
            isinstance(document, dict)
            and document.get("execution_checkpoint_format") == "determa.execution_checkpoint"
        ):
            if not validate_execution_checkpoint_member("executionCheckpoint", document):
                raise ValueError("invalid retained checkpoint inventory")
            if document["execution_checkpoint_digest"] != execution_checkpoint_digest(document):
                raise ValueError("invalid retained checkpoint digest")
            retained.append(document)
    for document in retained + checkpoints:
        root = document["root_instance_id"]
        members.append({"kind": "root", "identity": root})
        if document["root_record"]["status"] == "tombstone":
            members.append({"kind": "tombstone", "identity": root})
        for receipt in document["operation_receipts"]:
            members.append(
                {
                    "kind": "receipt",
                    "identity": _compact([root, "receipt", receipt["receipt_sequence"]]),
                }
            )
        for record in document["migration_audit_records"]:
            members.append({"kind": "migration", "identity": record["migration_descriptor_digest"]})
        for field, identity in (
            ("event_identity_tombstones", "event_id"),
            ("outbox_effect_tombstones", "effect_id"),
        ):
            members.extend(
                {"kind": "tombstone", "identity": _compact([root, identity, item[identity]])}
                for item in document[field]
            )
        pending: list[Any] = [document]
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                for key, item in value.items():
                    if key in {
                        "validated_bundle_fingerprint",
                        "source_validated_bundle_fingerprint",
                        "target_validated_bundle_fingerprint",
                    }:
                        members.append({"kind": "definition", "identity": item})
                    pending.append(item)
            elif isinstance(value, list):
                pending.extend(value)
    for document in checkpoints:
        root = document["root_instance_id"]
        members.append(
            {
                "kind": "checkpoint",
                "identity": (
                    f"{root}:{document['revision']}:{document['execution_checkpoint_digest']}"
                ),
            }
        )
        for field, kind in (
            ("pending_outbox_intents", "pending_intent"),
            ("terminal_outbox_records", "terminal_intent"),
        ):
            members.extend(
                {"kind": kind, "identity": _compact([root, "effect", item["effect_id"]])}
                for item in document[field]
            )
    return [
        {"kind": kind, "identity": identity}
        for kind, identity in sorted({(item["kind"], item["identity"]) for item in members})
    ]


def _native_checkpoints(
    connection: sqlite3.Connection, ledger: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Read the entire dedicated checkpoint table under the freeze transaction."""
    if (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'determa_execution_checkpoints'"
        ).fetchone()
        is None
    ):
        return []
    documents = []
    for root, payload in connection.execute(
        "SELECT root_instance_id, checkpoint FROM determa_execution_checkpoints "
        "ORDER BY root_instance_id"
    ):
        source = bytes(payload).decode("utf-8", "strict")
        if root not in ledger["roots"] or source not in ledger["checkpoint_bytes"]:
            raise ValueError("untracked native checkpoint")
        document = _parse(source)
        if not validate_execution_checkpoint_member("executionCheckpoint", document):
            raise ValueError("invalid native checkpoint")
        if document["root_instance_id"] != root or document[
            "execution_checkpoint_digest"
        ] != execution_checkpoint_digest(document):
            raise ValueError("native checkpoint identity mismatch")
        documents.append(document)
    return documents


def _compose_authority_profile(
    facts: Mapping[str, Any],
    verified: set[str],
    *,
    scope_identity: str,
    authority_epoch: str,
    scope_generation: str,
) -> dict[str, Any]:
    """Compose guarantees from configured identity and independently proved operations."""
    requirement = facts["extension_requirement"]
    claims = set(requirement["required_claims"]) if requirement else set()
    guarded = (
        requirement is not None
        and "authoritative_scope_fencing" in claims
        and "scope_guard_through_native_commit" in verified
    )
    inventory = (
        requirement is not None
        and "consistent_scope_inventory" in claims
        and "frozen_authoritative_inventory" in verified
    )
    worker = (
        guarded
        and {"guarded_journal_claim", "authenticated_worker_checks"} <= verified
        and {item["role"] for item in facts["required_participants"]} >= {"journal", "worker"}
    )
    relocation = (
        guarded
        and inventory
        and "safe_relocation" in claims
        and "same_authority_transfer_proof" in verified
        and facts["destination_binding_digest"] is not None
    )
    if (requirement is None and verified) or (
        requirement is not None and not guarded and not inventory
    ):
        return {"status": "rejected", "code": "host_capability_mismatch"}
    if "safe_relocation" in verified and not relocation:
        return {"status": "rejected", "code": "host_capability_mismatch"}
    report = {
        "profile_report_format": "determa.host_authority_profile_report",
        "profile_report_schema_version": 1,
        "scope_identity": scope_identity,
        "authority_epoch": authority_epoch if requirement else None,
        "scope_generation": scope_generation if requirement else None,
        "extension_report": {
            "category": "authority",
            "provider_reference": requirement["provider_reference"],
            "instance_id": requirement["instance_id"],
            "health": "healthy",
            "claims": requirement["required_claims"],
        }
        if requirement
        else None,
        "authority_storage_boundary": facts["storage_boundary"],
        "topology": facts["topology"],
        "source_binding_digest": facts["source_binding_digest"],
        "destination_binding_digest": facts["destination_binding_digest"],
        "required_participants": facts["required_participants"],
        "guarantees": {
            "guarded_local_writes": guarded,
            "worker_fencing": worker,
            "complete_scope_inventory": inventory,
            "safe_relocation": relocation,
        },
    }
    if facts["destination_binding_digest"] is None and "safe_relocation" in verified:
        return {"status": "rejected", "code": "host_capability_mismatch"}
    return {"status": "accepted", "report_bytes": _compact(report)}


class SQLiteLocalAuthority:
    """Single-database guarded authority; no worker or relocation guarantee."""

    def __init__(self, path: str | Path, *, timeout: float = 30.0) -> None:
        if not str(path) or str(path) == ":memory:" or timeout <= 0:
            raise ValueError("a persistent SQLite file and positive timeout are required")
        self.path = str(Path(path).resolve())
        self.timeout = timeout

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=self.timeout, isolation_level=None)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def setup_schema(self) -> None:
        with self._connect() as connection:
            connection.execute(
                _AUTHORITY_TABLE_SQL.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
            )
            connection.execute(
                _ALLOCATION_TABLE_SQL.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
            )
            connection.execute(
                _ALLOCATION_TRIGGER_SQL.replace("CREATE TRIGGER", "CREATE TRIGGER IF NOT EXISTS", 1)
            )
        self.validate_schema()

    def validate_schema(self) -> None:
        """Fail closed if policy, tables or permanent allocation trigger differ."""
        with self._connect() as connection:
            if connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                raise ValueError("authority database integrity failed")
            if connection.execute("PRAGMA journal_mode").fetchone() != ("wal",):
                raise ValueError("authority journal mode mismatch")
            if connection.execute("PRAGMA synchronous").fetchone() != (2,):
                raise ValueError("authority synchronous mode mismatch")
            expected = {
                ("table", "determa_scope_authority"): _AUTHORITY_TABLE_SQL,
                ("table", "determa_scope_allocations"): _ALLOCATION_TABLE_SQL,
                ("trigger", "determa_scope_allocations_forbid_delete"): _ALLOCATION_TRIGGER_SQL,
            }
            rows = connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE name LIKE 'determa_scope_%' ORDER BY type, name"
            ).fetchall()
            if {(kind, name): source for kind, name, source in rows} != expected:
                raise ValueError("authority schema mismatch")
            if (
                connection.execute("SELECT COUNT(*) FROM determa_scope_allocations").fetchone()[0]
                > 1
            ):
                raise ValueError("authority topology requires one permanent scope per database")

    def profile_descriptor(
        self,
        execution_store: SQLiteExecutionStore,
        registry: ExtensionRegistry,
        configured: ConfiguredExtension,
    ) -> dict[str, Any]:
        """Describe only a colocated and healthy configured execution store."""
        self.validate_schema()
        if (
            not isinstance(execution_store, AuthoritySQLiteExecutionStore)
            or execution_store.authority is not self
            or str(Path(execution_store.path).resolve()) != self.path
            or execution_store.journal_mode != "WAL"
            or execution_store.synchronous != "FULL"
        ):
            raise ValueError("authority and execution store are not colocated")
        execution_store.validate_schema()
        _, _, instance, _ = registry._bound(configured)
        if (
            not isinstance(instance, dict)
            or instance.get("authority") is not self
            or instance.get("store") is not execution_store
        ):
            raise ValueError("configured authority instance mismatch")
        extension_report = registry.report(configured)
        if extension_report["health"] != "healthy" or set(extension_report["claims"]) != {
            "authoritative_scope_fencing",
            "consistent_scope_inventory",
        }:
            raise ValueError("host_capability_mismatch")
        closure = _authority_closure()
        configuration = _compact(
            {
                "sqlite_path": self.path,
                "journal_mode": "WAL",
                "synchronous": "FULL",
                "topology": "single-sqlite-database",
                "replay_retention": execution_store.replay_retention,
                "outbox_retention": execution_store.outbox_retention,
            }
        ).encode()
        descriptor: dict[str, Any] = {
            "extension_report": extension_report,
            "authority_storage_boundary": "local-sqlite-authority-db",
            "topology": {
                "identifier": "single-sqlite-database",
                "configuration_digest": "sha256:" + hashlib.sha256(configuration).hexdigest(),
                "process_boundary": "one configured authority database",
                "host_boundary": "one local host",
            },
            "source_binding_digest": hash_value(["determa-local-authority-source-1", self.path]),
            "destination_binding_digest": None,
            "required_participants": [],
            "installation_evidence": {
                "closure_bytes_base64": base64.b64encode(closure).decode(),
                "configuration_bytes_base64": base64.b64encode(configuration).decode(),
                "observed_health": "healthy",
                "participant_installations": [],
            },
        }
        ledger = self.inspect(execution_store.scope_identity)
        if ledger is None:
            raise ValueError("host_capability_mismatch")
        composed = _compose_authority_profile(
            {
                "extension_requirement": {
                    "provider_reference": extension_report["provider_reference"],
                    "instance_id": extension_report["instance_id"],
                    "required_claims": extension_report["claims"],
                },
                "storage_boundary": descriptor["authority_storage_boundary"],
                **{
                    key: descriptor[key]
                    for key in (
                        "topology",
                        "source_binding_digest",
                        "destination_binding_digest",
                        "required_participants",
                    )
                },
            },
            {"scope_guard_through_native_commit", "frozen_authoritative_inventory"},
            scope_identity=execution_store.scope_identity,
            authority_epoch=ledger["authority_epoch"],
            scope_generation=ledger["scope_generation"],
        )
        if composed["status"] != "accepted":
            raise ValueError("host_capability_mismatch")
        descriptor["guarantees"] = _parse(composed["report_bytes"])["guarantees"]
        return descriptor

    def profile_report(
        self,
        scope_identity: str,
        authenticated_principal: str,
        execution_store: SQLiteExecutionStore,
        registry: ExtensionRegistry,
        configured: ConfiguredExtension,
    ) -> dict[str, Any]:
        """Return an authorized current report for this exact SQLite instance."""
        ledger = self.inspect(scope_identity)
        if ledger is None or authenticated_principal != ledger["owner_principal"]:
            raise ValueError("unauthorized_scope")
        if not isinstance(execution_store, AuthoritySQLiteExecutionStore):
            raise ValueError("authority and execution store are not colocated")
        if (
            execution_store.scope_identity != scope_identity
            or execution_store.owner_principal != authenticated_principal
            or execution_store.authority_epoch != ledger["authority_epoch"]
        ):
            raise ValueError("host_capability_mismatch")
        descriptor = self.profile_descriptor(execution_store, registry, configured)
        descriptor.pop("installation_evidence")
        return {
            "profile_report_format": "determa.host_authority_profile_report",
            "profile_report_schema_version": 1,
            "scope_identity": scope_identity,
            "authority_epoch": ledger["authority_epoch"],
            "scope_generation": ledger["scope_generation"],
            **descriptor,
        }

    def allocate(
        self,
        scope_identity: str,
        owner_principal: str,
        *,
        roots: tuple[str, ...] = (),
        definition_references: tuple[str, ...] = (),
    ) -> bool:
        """Create trusted authority data and a permanent no-reuse marker."""
        if (
            type(scope_identity) is not str
            or not scope_identity
            or type(owner_principal) is not str
            or not owner_principal
        ):
            raise ValueError("scope and owner identities are required")
        if (
            any(type(item) is not str or not item for item in (*roots, *definition_references))
            or len(set(roots)) != len(roots)
            or len(set(definition_references)) != len(definition_references)
        ):
            raise ValueError("invalid initial scope inventory")
        ledger = {
            "allocated_scope_identities": [scope_identity],
            "scope_identity": scope_identity,
            "owner_principal": owner_principal,
            "authority_epoch": "0",
            "scope_generation": "0",
            "state": "active",
            "receipts": [],
            "mutation_bytes": [],
            "checkpoint_bytes": [],
            "journal_entries": [],
            "ingress_acknowledgements": [],
            "active_claims": [],
            "roots": list(roots),
            "tombstones": [],
            "pending_intents": [],
            "terminal_intents": [],
            "definition_references": list(definition_references),
            "migration_references": [],
            "required_participant_records": [],
            "freeze": None,
            "inventory": [],
            "retirement_grants": [],
            "destination_activations": [],
        }
        return self._insert_ledger(ledger)

    def _insert_ledger(self, ledger: Mapping[str, Any]) -> bool:
        """Seed trusted ledger storage; profile fixtures use this private seam."""
        scope = ledger["scope_identity"]
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            # Checkpoint keys are root-bound, not scope-qualified. This local
            # topology therefore dedicates the database permanently to one scope.
            # The immutable allocation marker also prevents reuse after retirement.
            if connection.execute("SELECT 1 FROM determa_scope_allocations LIMIT 1").fetchone():
                connection.rollback()
                return False
            try:
                connection.execute("INSERT INTO determa_scope_allocations VALUES (?)", (scope,))
                connection.execute(
                    "INSERT INTO determa_scope_authority VALUES (?, ?)", (scope, _compact(ledger))
                )
            except sqlite3.IntegrityError:
                connection.rollback()
                return False
            connection.commit()
            return True

    def inspect(self, scope_identity: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT ledger FROM determa_scope_authority WHERE scope_identity = ?",
                (scope_identity,),
            ).fetchone()
            return None if row is None else _parse(row[0])

    def perform(
        self,
        request_bytes: str,
        invocation: Mapping[str, Any],
        *,
        native_mutation_bytes: str | None = None,
    ) -> str:
        """Execute one host-authorized authority request and return committed bytes."""
        result = self._perform(
            request_bytes,
            invocation,
            native_mutation_bytes=native_mutation_bytes,
        )
        assert result is not None
        return result

    def _perform(
        self,
        request_bytes: str,
        invocation: Mapping[str, Any],
        *,
        native_mutation_bytes: str | None = None,
        fault: Mapping[str, Any] | str | None = None,
        before_commit: Callable[[], None] | None = None,
    ) -> str | None:
        """Execute one authenticated operation with a guard held through commit.

        ``native_mutation_bytes`` are proposed host bytes, never caller checkpoint
        state. Hosted integration prepares them internally before calling this seam.
        """
        try:
            request = _parse(request_bytes)
            code = _valid_request(request)
        except (ValueError, TypeError, KeyError):
            request, code = None, "invalid_host_request"
        if code:
            return _compact(_result(None, None, code, early=True))
        assert isinstance(request, dict)
        scope = request["scope_identity"]
        if (
            not invocation.get("authenticated_principal")
            or scope not in invocation.get("authorized_scopes", ())
            or request["operation"] not in invocation.get("operation_rights", ())
        ):
            return _compact(_result(request, None, "unauthorized_scope"))
        try:
            self.validate_schema()
        except (OSError, sqlite3.Error, ValueError):
            return _compact(_result(request, None, "host_capability_mismatch"))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT ledger FROM determa_scope_authority WHERE scope_identity = ?", (scope,)
            ).fetchone()
            if row is None:
                return _compact(_result(request, None, "unauthorized_scope"))
            ledger = _parse(row[0])
            operation = request["operation"]
            if operation != "read_authority":
                receipt = next(
                    (
                        item
                        for item in ledger["receipts"]
                        if item["operation_id"] == request["operation_id"]
                    ),
                    None,
                )
                if receipt is not None:
                    if (
                        receipt["request_digest"] != request["request_digest"]
                        or receipt["request_bytes"] != request_bytes
                    ):
                        return _compact(_result(request, ledger, "scope_operation_conflict"))
                    if receipt["result_bytes"] is None:
                        return _compact(_result(request, ledger, "replay_evidence_expired"))
                    return str(receipt["result_bytes"])
            if operation == "read_authority":
                return _compact(_result(request, ledger, None))
            if operation == "prove_retirement":
                invalid_proof = any(
                    _parse(receipt["request_bytes"])["operation"] == "freeze_scope"
                    and receipt["request_digest"] == request["arguments"]["freeze_evidence_digest"]
                    for receipt in ledger["receipts"]
                )
                return _compact(
                    _result(
                        request,
                        ledger,
                        "scope_fence_unproven" if invalid_proof else "host_capability_mismatch",
                    )
                )
            if operation == "fence_worker":
                return _compact(_result(request, ledger, "host_capability_mismatch"))
            if isinstance(fault, dict) and fault.get("epoch_check_separate_from_commit"):
                return _compact(_result(request, ledger, "host_capability_mismatch"))
            if (
                isinstance(fault, dict)
                and fault.get("native_transaction_fate") == "unknown"
                or fault == "commit_unknown_disconnect"
            ):
                ledger["state"] = "transaction_in_doubt"
                connection.execute(
                    "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
                    (_compact(ledger), scope),
                )
                connection.commit()
                return _compact(_result(request, ledger, "scope_transaction_in_doubt"))
            if ledger["state"] == "transaction_in_doubt":
                return _compact(_result(request, ledger, "scope_transaction_in_doubt"))
            if request["expected_authority_epoch"] != ledger["authority_epoch"]:
                return _compact(_result(request, ledger, "stale_scope_authority"))
            if request["expected_scope_generation"] != ledger["scope_generation"]:
                return _compact(_result(request, ledger, "scope_generation_conflict"))
            if (
                ledger["state"] != "active"
                or invocation["authenticated_principal"] != ledger["owner_principal"]
            ):
                return _compact(_result(request, ledger, "stale_scope_authority"))
            if operation == "guarded_commit":
                if (
                    native_mutation_bytes is None
                    or "sha256:" + hashlib.sha256(native_mutation_bytes.encode()).hexdigest()
                    != request["arguments"]["mutation_digest"]
                ):
                    return _compact(_result(request, ledger, "invalid_host_request"))
                ledger["mutation_bytes"].append(native_mutation_bytes)
                ledger["checkpoint_bytes"].append(native_mutation_bytes)
            if operation == "freeze_scope":
                if fault == "omit_receipt_from_inventory":
                    return _compact(_result(request, ledger, "scope_fence_unproven"))
                if (
                    ledger["required_participant_records"]
                    or ledger["journal_entries"]
                    or ledger["active_claims"]
                ):
                    return _compact(_result(request, ledger, "host_capability_mismatch"))
                try:
                    checkpoints = _native_checkpoints(connection, ledger)
                    _inventory(ledger, checkpoints)
                except (ValueError, TypeError, KeyError):
                    return _compact(_result(request, ledger, "scope_fence_unproven"))
            ledger["scope_generation"] = str(int(ledger["scope_generation"]) + 1)
            if operation == "freeze_scope":
                ledger["state"] = "frozen"
                ledger["active_claims"] = []
            response = _result(request, ledger, None)
            ledger["receipts"].append(
                {
                    "operation_id": request["operation_id"],
                    "request_digest": request["request_digest"],
                    "request_bytes": request_bytes,
                    "result_bytes": _compact(response),
                    "evidence_digest": response["evidence_digest"],
                }
            )
            if operation == "freeze_scope":
                ledger["inventory"] = _inventory(ledger, checkpoints)
                ledger["freeze"] = {
                    "evidence_digest": response["evidence_digest"],
                    "generation": ledger["scope_generation"],
                    "inventory_digest": hash_value(
                        [
                            "determa-host-authority-frozen-inventory-1",
                            response["evidence_digest"],
                            ledger["inventory"],
                        ]
                    ),
                    "required_participants": copy.deepcopy(ledger["required_participant_records"]),
                }
            connection.execute(
                "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
                (_compact(ledger), scope),
            )
            if before_commit is not None:
                before_commit()
            if fault == "precommit_abort":
                connection.rollback()
                return None
            connection.commit()
            if fault == "drop_response_after_commit":
                return None
            return _compact(response)

    def _resolve_rolled_back_fate(self, scope_identity: str, operation_id: str) -> None:
        """Clear doubt after the host has terminated the old session and checked storage."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT ledger FROM determa_scope_authority WHERE scope_identity = ?",
                (scope_identity,),
            ).fetchone()
            if row is None:
                raise ValueError("unknown scope")
            ledger = _parse(row[0])
            if ledger["state"] != "transaction_in_doubt":
                raise ValueError("scope has no uncertain transaction")
            if any(item["operation_id"] == operation_id for item in ledger["receipts"]):
                raise ValueError("operation committed; rollback not proved")
            ledger["state"] = "active"
            connection.execute(
                "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
                (_compact(ledger), scope_identity),
            )
            connection.commit()

    def _mark_transaction_in_doubt(self, scope_identity: str) -> None:
        """Block freeze while a disconnected native writer's fate is unresolved."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT ledger FROM determa_scope_authority WHERE scope_identity = ?",
                (scope_identity,),
            ).fetchone()
            if row is None:
                raise ValueError("unknown scope")
            ledger = _parse(row[0])
            if ledger["state"] != "active":
                raise ValueError("scope is not active")
            ledger["state"] = "transaction_in_doubt"
            connection.execute(
                "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
                (_compact(ledger), scope_identity),
            )
            connection.commit()


class AuthoritySQLiteExecutionStore(SQLiteExecutionStore):
    """Colocated execution store whose checkpoint writes hold the scope guard.

    Ordinary ``ExecutionHost`` operations use this adapter through the public
    execution-store contract. The adapter owns the native SQLite transaction; no
    portable host operation receives a raw connection or transaction.
    """

    def __init__(
        self,
        authority: SQLiteLocalAuthority,
        scope_identity: str,
        owner_principal: str,
        authority_epoch: str,
        *,
        replay_retention: str = "bounded",
        outbox_retention: str = "none",
    ) -> None:
        super().__init__(
            authority.path,
            journal_mode="WAL",
            synchronous="FULL",
            replay_retention=replay_retention,
            outbox_retention=outbox_retention,
        )
        self.authority = authority
        self.scope_identity = scope_identity
        self.owner_principal = owner_principal
        self.authority_epoch = authority_epoch

    @contextmanager
    def transaction(self, root_instance_id: str) -> Iterator[ExecutionStoreTransaction]:
        """Commit checkpoint, authority generation and receipt atomically."""
        self.authority.validate_schema()
        connection = self._connect()
        try:
            self._validate_schema(connection)
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT ledger FROM determa_scope_authority WHERE scope_identity = ?",
                (self.scope_identity,),
            ).fetchone()
            if row is None:
                raise ExecutionStoreError("unauthorized_scope")
            ledger = _parse(row[0])
            if (
                ledger["owner_principal"] != self.owner_principal
                or ledger["authority_epoch"] != self.authority_epoch
                or ledger["state"] != "active"
            ):
                raise ExecutionStoreError("stale_scope_authority")
            if root_instance_id not in ledger["roots"]:
                raise ExecutionStoreError("unauthorized_scope")
            transaction = _SQLiteTransaction(connection, root_instance_id)
            previous = transaction.load()
            yield transaction
            current = transaction.load()
            if current != previous:
                if current is None:
                    raise ExecutionStoreError("invalid_execution_checkpoint")
                _, revision, digest = checkpoint_metadata(current)
                operation_id = f"checkpoint:{root_instance_id}:{revision}:{digest}"
                if any(item["operation_id"] == operation_id for item in ledger["receipts"]):
                    raise ExecutionStoreError("scope_operation_conflict")
                mutation_digest = "sha256:" + hashlib.sha256(current).hexdigest()
                request = {
                    "interface": _INTERFACE,
                    "interface_version": 1,
                    "operation": "guarded_commit",
                    "operation_id": operation_id,
                    "scope_identity": self.scope_identity,
                    "expected_authority_epoch": ledger["authority_epoch"],
                    "expected_scope_generation": ledger["scope_generation"],
                    "arguments": {"mutation_digest": mutation_digest},
                }
                request["request_digest"] = hash_value(
                    ["determa-host-authority-request-1", request]
                )
                ledger["scope_generation"] = str(int(ledger["scope_generation"]) + 1)
                response = _result(request, ledger, None)
                ledger["receipts"].append(
                    {
                        "operation_id": operation_id,
                        "request_digest": request["request_digest"],
                        "request_bytes": _compact(request),
                        "result_bytes": _compact(response),
                        "evidence_digest": response["evidence_digest"],
                    }
                )
                encoded = current.decode("utf-8", "strict")
                ledger["mutation_bytes"].append(encoded)
                ledger["checkpoint_bytes"].append(encoded)
                connection.execute(
                    "UPDATE determa_scope_authority SET ledger = ? WHERE scope_identity = ?",
                    (_compact(ledger), self.scope_identity),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()


def _authority_closure() -> bytes:
    source_directory = Path(__file__).parent
    return b"".join(
        path.relative_to(source_directory).as_posix().encode() + b"\0" + path.read_bytes()
        for path in sorted(source_directory.rglob("*"))
        if path.is_file() and path.suffix in {".py", ".json"}
    )


def bundled_authority_descriptor() -> dict[str, Any]:
    """Exact public descriptor for the installed local SQLite provider."""
    return {
        "category": "authority",
        "provider_reference": {
            "identifier": "reference.sqlite-authority",
            "version": __version__,
            "content_digest": "sha256:" + hashlib.sha256(_authority_closure()).hexdigest(),
        },
        "interface_version": 1,
        "supported_capabilities": [
            "authoritative_scope_fencing",
            "consistent_scope_inventory",
        ],
    }


class _BundledAuthorityProvider:
    def validate_configuration(self, configuration: Mapping[str, Any]) -> dict[str, Any]:
        if set(configuration) != {
            "instance_id",
            "path",
            "scope_identity",
            "owner_principal",
            "authority_epoch",
            "replay_retention",
            "outbox_retention",
        } or any(
            type(configuration[key]) is not str or not configuration[key] for key in configuration
        ):
            raise ValueError("invalid_extension_configuration")
        if configuration["instance_id"] != "local-authority":
            raise ValueError("invalid_extension_configuration")
        authority = SQLiteLocalAuthority(configuration["path"])
        if authority.path != configuration["path"]:
            raise ValueError("invalid_extension_configuration")
        store = AuthoritySQLiteExecutionStore(
            authority,
            configuration["scope_identity"],
            configuration["owner_principal"],
            configuration["authority_epoch"],
            replay_retention=configuration["replay_retention"],
            outbox_retention=configuration["outbox_retention"],
        )
        return {"instance_id": configuration["instance_id"], "authority": authority, "store": store}

    def capabilities(self, instance: Any) -> list[str]:
        return ["authoritative_scope_fencing", "consistent_scope_inventory"]

    def health(self, instance: Any) -> str:
        authority = instance["authority"]
        store = instance["store"]
        try:
            authority.validate_schema()
            store.validate_schema()
            ledger = authority.inspect(store.scope_identity)
            if (
                type(authority) is not SQLiteLocalAuthority
                or type(store) is not AuthoritySQLiteExecutionStore
                or store.authority is not authority
                or store.path != authority.path
                or ledger is None
                or ledger["owner_principal"] != store.owner_principal
                or ledger["authority_epoch"] != store.authority_epoch
                or ledger["state"] not in ("active", "frozen")
            ):
                return "unavailable"
        except (ValueError, sqlite3.Error, KeyError):
            return "unavailable"
        return "healthy"


def bundled_sqlite_authority_provider_factory() -> _BundledAuthorityProvider:
    return _BundledAuthorityProvider()


def configure_bundled_sqlite_authority(
    registry: ExtensionRegistry,
    path: str | Path,
    scope_identity: str,
    owner_principal: str,
    authority_epoch: str,
    *,
    replay_retention: str = "bounded",
    outbox_retention: str = "none",
) -> tuple[ConfiguredExtension, SQLiteLocalAuthority, AuthoritySQLiteExecutionStore]:
    """Configure and return the exact authority/store instance used for host operations."""
    configuration = {
        "instance_id": "local-authority",
        "path": str(Path(path).resolve()),
        "scope_identity": scope_identity,
        "owner_principal": owner_principal,
        "authority_epoch": authority_epoch,
        "replay_retention": replay_retention,
        "outbox_retention": outbox_retention,
    }
    configured = registry.validate_configuration(bundled_authority_descriptor(), configuration)
    _, _, instance, _ = registry._bound(configured)
    return configured, instance["authority"], instance["store"]
