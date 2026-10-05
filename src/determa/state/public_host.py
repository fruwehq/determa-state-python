"""Minimal public v1 local SQLite host; no authority or helper claims."""

from __future__ import annotations

import copy
import json
import re
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

from .checkpoint_v1 import (
    admit_checkpoint_v1,
    create_checkpoint_v1,
    restore_execution_checkpoint_v1,
    step_checkpoint_v1,
)
from .definition import Bundle, load_bundle
from .errors import ArtifactError
from .inspection import inspect_candidate
from .public_client import PublicHostError, public_request_digest, validate_public_message
from .wire import DefinitionResolver, canonical_bytes, decoded_typed_value, hash_value

_OPERATIONS = ["capabilities", "create", "admit", "process", "read", "inspect", "receipt"]
_MUTATIONS = {"create", "admit", "process"}

_TABLES = {
    "determa_public_host_binding": "singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
    "schema_version INTEGER NOT NULL CHECK(schema_version=1), scope_binding_identity TEXT NOT NULL",
    "determa_public_host_checkpoints": (
        "root_instance_id TEXT PRIMARY KEY, checkpoint BLOB NOT NULL"
    ),
    "determa_public_host_responses": "operation_id TEXT PRIMARY KEY, request BLOB NOT NULL, "
    "request_digest TEXT NOT NULL, response BLOB NOT NULL",
}
_TRIGGERS = {
    f"{table}_forbid_{action.lower()}": f"CREATE TRIGGER {table}_forbid_{action.lower()} "
    f"BEFORE {action} ON {table} "
    "BEGIN SELECT RAISE(ABORT,'public_host_immutable'); END"
    for table, actions in {
        "determa_public_host_binding": ("INSERT", "UPDATE", "DELETE"),
        "determa_public_host_checkpoints": ("DELETE",),
        "determa_public_host_responses": ("UPDATE", "DELETE"),
    }.items()
    for action in actions
}


def _sql_tokens(source: str) -> list[str]:
    return re.findall(r"[a-z_][a-z0-9_]*|[0-9]+|[(),=]", source.lower())


class SQLitePublicExecutionHost:
    """One authenticated logical scope in one explicitly initialized SQLite file.

    Transport adapters authenticate the principal before calling ``handle``. This
    host authorizes that identity before any scope, root or receipt lookup. Native
    effects, authority, timers, archive staging and recovery are not advertised.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        scope_alias: str,
        scope_binding_identity: str,
        authorized_principals: frozenset[str],
        resolver: DefinitionResolver,
    ) -> None:
        if (
            (not str(path) or str(path) == ":memory:")
            or not scope_alias
            or not scope_binding_identity
            or not authorized_principals
        ):
            raise PublicHostError("host_capability_mismatch")
        self.path = str(path)
        self.scope_alias = scope_alias
        self.scope_binding_identity = scope_binding_identity
        self.authorized_principals = frozenset(authorized_principals)
        self.resolver = resolver

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path)
        try:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            if (
                db.execute("PRAGMA journal_mode").fetchone()[0] != "wal"
                or db.execute("PRAGMA synchronous").fetchone()[0] != 2
            ):
                raise PublicHostError("host_capability_mismatch")
            with db:
                yield db
        finally:
            db.close()

    def setup_schema(self) -> None:
        """Create the exact local schema without replacing prior scope evidence."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for name, columns in _TABLES.items():
                db.execute(f"CREATE TABLE IF NOT EXISTS {name} ({columns})")
            if not db.execute("SELECT 1 FROM determa_public_host_binding").fetchone():
                db.execute(
                    "INSERT INTO determa_public_host_binding VALUES (1,1,?)",
                    (self.scope_binding_identity,),
                )
            for name, definition in _TRIGGERS.items():
                existing = db.execute(
                    "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (name,)
                ).fetchone()
                if existing is None:
                    db.execute(definition)
            self._check_binding(db)

    @staticmethod
    def _check_schema(db: sqlite3.Connection) -> None:
        for name, columns in _TABLES.items():
            row = db.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (name,)
            ).fetchone()
            if row is None or _sql_tokens(row[0]) != _sql_tokens(
                f"CREATE TABLE {name} ({columns})"
            ):
                raise PublicHostError("host_capability_mismatch")
        actual = dict(
            db.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name IN (?,?,?)",
                tuple(_TABLES),
            )
        )
        if set(actual) != set(_TRIGGERS) or any(
            _sql_tokens(actual[name]) != _sql_tokens(definition)
            for name, definition in _TRIGGERS.items()
        ):
            raise PublicHostError("host_capability_mismatch")

    def _check_binding(self, db: sqlite3.Connection) -> None:
        self._check_schema(db)
        rows = db.execute(
            "SELECT singleton,schema_version,scope_binding_identity "
            "FROM determa_public_host_binding"
        ).fetchall()
        if rows != [(1, 1, self.scope_binding_identity)]:
            raise PublicHostError("binding_unavailable")

    @staticmethod
    def _response(
        request: Mapping[str, Any], *, result: Any = None, code: str | None = None
    ) -> dict[str, Any]:
        return {
            "protocol": "determa.execution_host",
            "protocol_version": 1,
            "operation_id": request["operation_id"],
            "status": "rejected" if code else "committed",
            "receipt": None,
            "value": None if code else {"operation": request["operation"], "result": result},
            "error": {"operation": request["operation"], "code": code} if code else None,
        }

    def _capabilities(self) -> dict[str, Any]:
        result = {
            "scope_binding_identity": self.scope_binding_identity,
            "supported_operations": list(_OPERATIONS),
            "supported_scope_actions": [],
            "supported_determa_capabilities": [],
            "supported_timer_commands": [],
            "extension_reports": [],
            "authority_profile": None,
            "guarantees": {
                "inspection_structural": True,
                "inspection_semantic": False,
                "retained_history": True,
                "saved_response_replay": True,
                "deterministic_reexecution": False,
            },
        }
        result["profile_digest"] = hash_value(
            ["determa-public-host-profile-1", "1", self.scope_binding_identity, result]
        )
        return result

    def handle(self, request: Mapping[str, Any], *, principal: str) -> dict[str, Any]:
        """Return a response only after its native transaction has committed."""
        candidate = copy.deepcopy(dict(request))
        validate_public_message(candidate)
        if principal not in self.authorized_principals:
            return self._response(candidate, code="unauthorized_scope")
        operation = candidate["operation"]
        if operation == "capabilities":
            if candidate["arguments"]["scope_alias"] != self.scope_alias:
                return self._response(candidate, code="unauthorized_scope")
            if candidate["scope_binding_identity"] not in {None, self.scope_binding_identity}:
                return self._response(candidate, code="binding_unavailable")
        elif candidate["scope_binding_identity"] != self.scope_binding_identity:
            return self._response(candidate, code="binding_unavailable")
        if operation not in _OPERATIONS:
            return self._response(candidate, code="host_capability_mismatch")
        target = candidate["target"]
        if operation == "capabilities":
            applicable = all(value is None for value in target.values())
        elif operation == "receipt":
            applicable = target["runtime_id"] is None and target["runtime_incarnation"] is None
        elif operation in {"create", "admit", "read"}:
            applicable = (
                target["root_instance_id"] is not None
                and target["runtime_id"] is None
                and target["runtime_incarnation"] is None
            )
        else:
            applicable = all(value is not None for value in target.values())
        if not applicable or (
            operation in {"capabilities", "receipt", "create"}
            and candidate["precondition"] is not None
        ):
            return self._response(candidate, code="invalid_host_request")
        digest = public_request_digest(candidate)
        try:
            with self._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                self._check_binding(db)
                if operation in _MUTATIONS:
                    saved = db.execute(
                        "SELECT request, request_digest, response "
                        "FROM determa_public_host_responses "
                        "WHERE operation_id=?",
                        (candidate["operation_id"],),
                    ).fetchone()
                    if saved is not None:
                        if saved[0] != canonical_bytes(candidate) or saved[1] != digest:
                            return self._response(candidate, code="operation_id_conflict")
                        response = json.loads(saved[2])
                        validate_public_message(response, response=True)
                        return cast(dict[str, Any], response)
                result, checkpoint, changed = self._execute(db, candidate)
                response = self._response(candidate, result=result)
                if operation in _MUTATIONS and changed:
                    response["receipt"] = {
                        "scope_binding_identity": self.scope_binding_identity,
                        "operation_id": candidate["operation_id"],
                        "request_digest": digest,
                        "receipt_kind": "committed",
                        "acceptance_receipt": None,
                        "evidence_digest": hash_value(
                            ["determa-public-host-evidence-1", "1", response["value"]]
                        ),
                    }
                validate_public_message(response, response=True)
                if operation in _MUTATIONS:
                    assert checkpoint is not None
                    if changed:
                        db.execute(
                            "INSERT INTO determa_public_host_checkpoints VALUES (?,?) "
                            "ON CONFLICT(root_instance_id) DO UPDATE "
                            "SET checkpoint=excluded.checkpoint",
                            (checkpoint["root_instance_id"], canonical_bytes(checkpoint)),
                        )
                    db.execute(
                        "INSERT INTO determa_public_host_responses VALUES (?,?,?,?)",
                        (
                            candidate["operation_id"],
                            canonical_bytes(candidate),
                            digest,
                            canonical_bytes(response),
                        ),
                    )
            return response
        except ArtifactError as error:
            # Pure refusals roll back the transaction; native transaction failures
            # escape with no protocol response and must be reconciled by the client.
            response = self._response(candidate, code=error.code)
            validate_public_message(response, response=True)
            return response

    @staticmethod
    def _check_precondition(checkpoint: Mapping[str, Any], precondition: Mapping[str, Any]) -> None:
        if (
            checkpoint["revision"] != precondition["revision"]
            or checkpoint["execution_checkpoint_digest"] != precondition["checkpoint_digest"]
        ):
            raise PublicHostError("checkpoint_revision_conflict")

    def _execute(self, db: sqlite3.Connection, request: dict[str, Any]) -> tuple[Any, Any, bool]:
        operation = request["operation"]
        arguments = request["arguments"]
        if operation == "capabilities":
            return self._capabilities(), None, False
        if operation == "receipt":
            row = db.execute(
                "SELECT request_digest,response,request FROM determa_public_host_responses "
                "WHERE operation_id=?",
                (arguments["queried_operation_id"],),
            ).fetchone()
            if row is not None and row[0] != arguments["request_digest"]:
                raise PublicHostError("operation_id_conflict")
            if row is not None and request["target"]["root_instance_id"] is not None:
                if (
                    json.loads(row[2])["target"]["root_instance_id"]
                    != request["target"]["root_instance_id"]
                ):
                    raise PublicHostError("invalid_host_request")
            return (
                {
                    "saved_response": None if row is None else json.loads(row[1]),
                    "retention": "unknown" if row is None else "retained",
                },
                None,
                False,
            )
        target = request["target"]
        root_id = target["root_instance_id"]
        if not root_id:
            raise PublicHostError("invalid_host_request")
        row = db.execute(
            "SELECT checkpoint FROM determa_public_host_checkpoints WHERE root_instance_id=?",
            (root_id,),
        ).fetchone()
        if operation == "read":
            checkpoint = (
                None
                if row is None
                else restore_execution_checkpoint_v1(row[0], self.resolver).document
            )
            if request["precondition"] is not None and checkpoint is not None:
                self._check_precondition(checkpoint, request["precondition"])
            return (
                {
                    "checkpoint": checkpoint,
                    "observed_checkpoint_digest": None
                    if checkpoint is None
                    else checkpoint["execution_checkpoint_digest"],
                },
                None,
                False,
            )
        if operation == "create":
            if arguments["root_instance_id"] != root_id:
                raise PublicHostError("invalid_host_request")
            if row is not None:
                for saved_request, saved_response in db.execute(
                    "SELECT request,response FROM determa_public_host_responses"
                ):
                    previous = json.loads(saved_request)
                    if (
                        previous["operation"] == "create"
                        and previous["target"]["root_instance_id"] == root_id
                    ):
                        if previous["arguments"] != arguments:
                            raise PublicHostError("creation_id_conflict")
                        result = json.loads(saved_response)["value"]["result"]
                        return result, json.loads(row[0]), True
                raise PublicHostError("replay_evidence_expired")
            source = self.resolver.resolve_definition(arguments["validated_bundle_fingerprint"])
            if source is None or not self.resolver.definition_is_trusted(
                arguments["validated_bundle_fingerprint"]
            ):
                raise PublicHostError("missing_required_artifact")
            bundle = source if isinstance(source, Bundle) else load_bundle(source)
            if bundle.fingerprint != arguments["validated_bundle_fingerprint"]:
                raise PublicHostError("invalid_host_request")
            selected = next(
                (m for m in bundle.raw["machines"] if m["machine_id"] == arguments["machine_id"]),
                None,
            )
            if (
                selected is None
                or bundle.raw["namespace"] != arguments["namespace"]
                or str(selected.get("version", 1)) != arguments["machine_version"]
            ):
                raise PublicHostError("invalid_host_request")
            result = create_checkpoint_v1(
                bundle,
                arguments["machine_id"],
                root_id,
                arguments["creation_id"],
                decoded_typed_value(arguments["bindings"]),
                _include_projection_result=True,
            )
            checkpoint = result["checkpoint"]
            core = result["core_result"]
            return (
                {
                    "checkpoint": checkpoint,
                    "creation_receipt": checkpoint["operation_receipts"][0],
                    **{
                        k: core[k]
                        for k in ("status", "emissions", "lifecycle_dispositions", "fault")
                    },
                },
                checkpoint,
                True,
            )
        if row is None:
            raise PublicHostError("invalid_instance_target")
        checkpoint = restore_execution_checkpoint_v1(row[0], self.resolver).document
        aggregate = checkpoint["root_record"].get("aggregate_state")
        if aggregate is None:
            raise PublicHostError("terminal_root")
        if request["precondition"] is not None and operation == "inspect":
            self._check_precondition(checkpoint, request["precondition"])
        if operation == "inspect":
            inspection = arguments["candidate"]
            if (
                target["runtime_id"] != inspection["runtime_id"]
                or target["runtime_incarnation"] != inspection["runtime_incarnation"]
            ):
                raise PublicHostError("invalid_host_request")
            outcome = inspect_candidate(
                aggregate, inspection, self.resolver, semantic_enabled=False
            )
            if "code" in outcome:
                raise PublicHostError(outcome["code"])
            return (
                {
                    "outcome": outcome,
                    "observed_aggregate_state_digest": aggregate["aggregate_state_digest"],
                },
                None,
                False,
            )
        precondition = request["precondition"]
        if precondition is None:
            raise PublicHostError("invalid_host_request")
        guard = {
            "expected_revision": precondition["revision"],
            "expected_checkpoint_digest": precondition["checkpoint_digest"],
        }
        if operation == "admit":
            deliveries = arguments["ordered_deliveries"]
            for delivery in deliveries:
                destination = delivery["envelope"]["target"]
                if next(iter(destination.values()))["root_instance_id"] != root_id:
                    raise PublicHostError("invalid_host_request")
            result = admit_checkpoint_v1(
                checkpoint, deliveries, self.resolver, **guard, _include_projection_result=True
            )
            updated = result.get("checkpoint", result)
            if "execution_checkpoint_digest" not in updated:
                updated = checkpoint
            receipts = [
                next(
                    r
                    for r in updated["operation_receipts"]
                    if r["operation_kind"] == "acceptance"
                    and r["event_id"] == d["envelope"]["event_id"]
                )
                for d in deliveries
            ]
            root = next(
                r
                for r in updated["root_record"]["aggregate_state"]["runtimes"]
                if r["runtime_id"] == aggregate["root_runtime_id"]
            )
            return (
                {
                    "checkpoint": updated,
                    "acceptance_receipts": receipts,
                    "status": root["status"],
                    "accepted": [d["envelope"] for d in deliveries],
                },
                updated,
                True,
            )
        runtime = next(
            (r for r in aggregate["runtimes"] if r["runtime_id"] == target["runtime_id"]), None
        )
        if runtime is None or runtime["identity_origin"] != target["runtime_incarnation"]:
            raise PublicHostError("invalid_instance_target")
        result = step_checkpoint_v1(
            checkpoint, target["runtime_id"], self.resolver, **guard, _include_host_response=True
        )
        updated = result["checkpoint"]
        core = result.get("core_result", result.get("step_result"))
        return (
            {"checkpoint": updated, "core_result": core, "terminal_receipt": result.get("receipt")},
            updated,
            (updated["execution_checkpoint_digest"] != checkpoint["execution_checkpoint_digest"]),
        )
