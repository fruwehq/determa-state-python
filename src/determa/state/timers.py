"""Explicit foreground SQLite timer records, separate from portable checkpoints.

This implementation checkpoint supports durable schedule/cancel/read, initial claims
and coordinated SQLite admission with strictly local native-fate reclaim. Verified
provider installation,
archive and recovery integration remain unfinished; no completed profile is advertised.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import sqlite3
from collections.abc import Callable, Mapping
from contextlib import closing
from pathlib import Path
from typing import Any

from .checkpoint_v1 import restore_execution_checkpoint_v1
from .definition import Bundle
from .engine import _normalize_payload
from .errors import ArtifactError
from .host import (
    ExecutionHost,
    ExecutionHostError,
    SharedExecutionTransaction,
    delivery_request_digest,
)
from .stores.base import ExecutionStoreError
from .stores.sqlite import SQLiteApplicationTransaction, SQLiteExecutionStore
from .wire import _schema_registry, canonical_bytes, decoded_typed_value, hash_value, strict_json

_TIME = re.compile(r"(?:0|-?[1-9][0-9]*)\Z")
_TABLE = (
    "CREATE TABLE determa_timer_helpers "
    "(scope_identity TEXT PRIMARY KEY NOT NULL, document BLOB NOT NULL)"
)
_ORIGIN_TABLE = (
    "CREATE TABLE determa_timer_origin "
    "(singleton INTEGER PRIMARY KEY NOT NULL CHECK(singleton=1), configuration BLOB NOT NULL)"
)
_COMMIT_TABLE = (
    "CREATE TABLE determa_timer_commits "
    "(scope_identity TEXT NOT NULL, sequence INTEGER NOT NULL, operation_id TEXT NOT NULL, "
    "journal BLOB NOT NULL, PRIMARY KEY(scope_identity, sequence), "
    "UNIQUE(scope_identity, operation_id))"
)
_NATIVE_TABLES = {
    "determa_timer_helpers": _TABLE,
    "determa_timer_origin": _ORIGIN_TABLE,
    "determa_timer_commits": _COMMIT_TABLE,
}
_NATIVE_TRIGGERS = {
    f"{table}_forbid_{operation.lower()}": (
        f"CREATE TRIGGER {table}_forbid_{operation.lower()} BEFORE {operation} ON {table} "
        "BEGIN SELECT RAISE(ABORT, 'timer_native_evidence_immutable'); END"
    )
    for table in ("determa_timer_origin", "determa_timer_commits")
    for operation in ("UPDATE", "DELETE")
}
_IDENTITIES = (
    "operation",
    "operation_id",
    "scope_identity",
    "root_instance_id",
    "root_runtime_id",
    "timer_id",
)


class TimerError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _TimerReturn(Exception):
    """Return a replay/refusal only after rolling back the shared host callback."""

    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result


def timer_request_digest(request: Mapping[str, Any]) -> str:
    return hash_value(
        [
            "determa-timer-request-1",
            {key: value for key, value in request.items() if key != "request_digest"},
        ]
    )


def seal_timer_records(artifact: Mapping[str, Any]) -> dict[str, Any]:
    body = copy.deepcopy(
        {key: value for key, value in artifact.items() if key != "timer_artifact_digest"}
    )
    return {**body, "timer_artifact_digest": hash_value(["determa-timer-artifact-1", body])}


def _validator(filename: str, definition: str | None = None) -> Any:
    from jsonschema import Draft202012Validator
    from referencing import Resource

    schema = json.loads((Path(__file__).parent / "data" / filename).read_text())
    registry = _schema_registry()
    for name in ("timer-helper-operation-v1.schema.json", "timer-record-v1.schema.json"):
        resource = json.loads((Path(__file__).parent / "data" / name).read_text())
        registry = registry.with_resource(resource["$id"], Resource.from_contents(resource))
    if definition is not None:
        schema = {"$ref": schema["$id"] + "#/$defs/" + definition}
    return Draft202012Validator(schema, registry=registry)


def _time(value: Any, *, duration: bool = False) -> int:
    if type(value) is not str or not _TIME.fullmatch(value):
        raise TimerError("invalid_timer_time")
    parsed = int(value)
    if not (0 if duration else -(2**63)) <= parsed <= 2**63 - 1:
        raise TimerError("invalid_timer_time")
    return parsed


def _execute(
    connection: sqlite3.Connection | SQLiteApplicationTransaction,
    statement: str,
    parameters: Any = (),
) -> Any:
    if isinstance(connection, SQLiteApplicationTransaction):
        return connection._execute_host_owned(statement, parameters)
    return connection.execute(statement, parameters)


def _result(
    request: Mapping[str, Any] | None, record: dict[str, Any] | None, code: str | None = None
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "interface": "determa.timer_helper",
        "interface_version": 1,
        **{key: request[key] if request is not None else None for key in _IDENTITIES},
        "status": "accepted" if code is None else "rejected",
        "record_revision": record["revision"] if record else None,
        "attempt_fence": record["attempt_fence"]
        if record and record["attempt_fence"] != "0"
        else None,
        "clock_basis": "unix_nanoseconds",
        "expires_at": record["expires_at"]
        if code is None and request is not None and request["operation"] == "claim_fire" and record
        else None,
        "event_id": record["event_id"] if record else None,
        "delivery_state": "admitted" if record and record["admission_receipt_digest"] else "none",
        "error_code": code,
        "result_digest": None,
    }
    if code is None and request is not None:
        result["result_digest"] = hash_value(
            [
                "determa-timer-result-1",
                request["request_digest"],
                {key: value for key, value in result.items() if key != "result_digest"},
            ]
        )
    return result


class SQLiteTimerHelper:
    """One configured scope and immutable target incarnation; no background work.

    Principals and the clock are trusted host configuration, never request fields.
    Schema setup is explicit. Records and exact successful receipts share one native
    SQLite commit; a refusal does not write a receipt or change the artifact.
    """

    def __init__(
        self,
        path: str | Path,
        bundle: Bundle,
        *,
        scope_identity: str,
        root_instance_id: str,
        root_runtime_id: str,
        principals: frozenset[str],
        trusted_clock: Callable[[], str],
        worker_principals: frozenset[str] = frozenset(),
        claim_lease_nanoseconds: str = "20",
        coordinated_host: ExecutionHost | None = None,
    ) -> None:
        if (
            str(path) == ":memory:"
            or not str(path)
            or not all((scope_identity, root_instance_id, root_runtime_id))
            or not principals
        ):
            raise ValueError(
                "persistent storage and a configured scope, target and principals are required"
            )
        self.path = str(Path(path).resolve())
        self.bundle = bundle
        self.scope_identity = scope_identity
        self.root_instance_id = root_instance_id
        self.root_runtime_id = root_runtime_id
        self.principals = frozenset(principals)
        self.trusted_clock = trusted_clock
        self.coordinated_host = coordinated_host
        if coordinated_host is not None and (
            type(coordinated_host.store) is not SQLiteExecutionStore
            or Path(coordinated_host.store.path).resolve() != Path(self.path)
            or not coordinated_host.store.shared_application_transactions
        ):
            raise ValueError("coordinated admission requires the same host-owned SQLite database")
        self.worker_principals = frozenset(worker_principals)
        self.claim_lease_nanoseconds = _time(claim_lease_nanoseconds, duration=True)
        if self.claim_lease_nanoseconds == 0 or not self.worker_principals <= self.principals:
            raise ValueError(
                "workers must be authorized principals and the claim lease must be positive"
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, isolation_level=None, timeout=30)
        journal = connection.execute("PRAGMA journal_mode=WAL").fetchone()
        connection.execute("PRAGMA synchronous=FULL")
        if journal != ("wal",) or connection.execute("PRAGMA synchronous").fetchone() != (2,):
            connection.close()
            raise TimerError("timer_capability_mismatch")
        return connection

    def setup_schema(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'determa_timer_%'"
                ).fetchone()[0]
                if existing:
                    # Never reconstruct origin or retained evidence around prior records.
                    self._schema(connection)
                else:
                    for statement in (*_NATIVE_TABLES.values(), *_NATIVE_TRIGGERS.values()):
                        connection.execute(statement)
                    connection.execute(
                        "INSERT INTO determa_timer_origin VALUES (1,?)",
                        (canonical_bytes(self._configuration()),),
                    )
                    self._schema(connection)
                connection.commit()
            finally:
                connection.rollback()

    def _implementation_digest(self) -> str:
        # An existing native origin cannot silently change completion implementation.
        # Independent loaded-source registration is still required before a profile claim.
        directory = Path(__file__).parent
        closure = b"".join(
            path.relative_to(directory).as_posix().encode() + b"\0" + path.read_bytes()
            for path in sorted(directory.rglob("*"))
            if path.is_file() and path.suffix in {".py", ".json"}
        )
        return "sha256:" + hashlib.sha256(closure).hexdigest()

    def _configuration(self) -> dict[str, Any]:
        return {
            "native_timer_configuration_version": 1,
            "implementation_digest": self._implementation_digest(),
            "coordinated_store_configuration": {
                "journal_mode": self.coordinated_host.store.journal_mode,
                "synchronous": self.coordinated_host.store.synchronous,
                "replay_retention": self.coordinated_host.store.replay_retention,
                "outbox_retention": self.coordinated_host.store.outbox_retention,
                "shared_application_transactions": (
                    self.coordinated_host.store.shared_application_transactions
                ),
            }
            if self.coordinated_host is not None
            and type(self.coordinated_host.store) is SQLiteExecutionStore
            else None,
            "storage_path": self.path,
            "scope_identity": self.scope_identity,
            "root_instance_id": self.root_instance_id,
            "root_runtime_id": self.root_runtime_id,
            "validated_bundle_fingerprint": self.bundle.fingerprint,
            "principals": sorted(self.principals),
            "worker_principals": sorted(self.worker_principals),
            "claim_lease_nanoseconds": str(self.claim_lease_nanoseconds),
            "admission_strategy": "coordinated_sqlite"
            if self.coordinated_host is not None
            else "records_only",
        }

    def _schema(self, connection: sqlite3.Connection | SQLiteApplicationTransaction) -> None:
        rows = _execute(
            connection,
            "SELECT name,type,sql FROM sqlite_master WHERE tbl_name IN "
            "('determa_timer_helpers','determa_timer_origin','determa_timer_commits') "
            "OR name LIKE 'determa_timer_%' ORDER BY name",
        ).fetchall()
        actual = {name: (kind, sql) for name, kind, sql in rows if sql is not None}
        expected = {
            **{name: ("table", sql) for name, sql in _NATIVE_TABLES.items()},
            **{name: ("trigger", sql) for name, sql in _NATIVE_TRIGGERS.items()},
        }
        if (
            actual != expected
            or len(rows) != 10
            or _execute(connection, "PRAGMA integrity_check").fetchone() != ("ok",)
        ):
            raise TimerError("timer_capability_mismatch")
        origin = _execute(connection, "SELECT configuration FROM determa_timer_origin").fetchall()
        if origin != [(canonical_bytes(self._configuration()),)]:
            raise TimerError("timer_capability_mismatch")
        for table in ("determa_timer_helpers", "determa_timer_commits"):
            if _execute(
                connection,
                f"SELECT COUNT(*) FROM {table} WHERE scope_identity!=?",
                (self.scope_identity,),
            ).fetchone() != (0,):
                raise TimerError("timer_capability_mismatch")

    def validate_schema(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN")
            try:
                self._schema(connection)
            finally:
                connection.rollback()

    def _load(
        self, connection: sqlite3.Connection | SQLiteApplicationTransaction
    ) -> dict[str, Any]:
        row = _execute(
            connection,
            "SELECT document FROM determa_timer_helpers WHERE scope_identity=?",
            (self.scope_identity,),
        ).fetchone()
        if row is None:
            artifact = seal_timer_records(
                {
                    "timer_artifact_format": "determa.timer_records",
                    "timer_artifact_schema_version": 1,
                    "records": [],
                    "operation_receipts": [],
                }
            )
        else:
            try:
                parsed, raw = strict_json(bytes(row[0]))
                if not isinstance(parsed, dict) or raw != canonical_bytes(parsed):
                    raise TimerError("timer_capability_mismatch")
                artifact = parsed
            except (ArtifactError, TypeError, ValueError) as error:
                raise TimerError("timer_capability_mismatch") from error
        if (
            not _validator("timer-record-v1.schema.json").is_valid(artifact)
            or seal_timer_records(artifact) != artifact
        ):
            raise TimerError("timer_capability_mismatch")
        if any(record["scope_identity"] != self.scope_identity for record in artifact["records"]):
            raise TimerError("timer_capability_mismatch")
        self._history(connection, artifact)
        return artifact

    def _history(
        self,
        connection: sqlite3.Connection | SQLiteApplicationTransaction,
        artifact: dict[str, Any],
    ) -> None:
        rows = _execute(
            connection,
            "SELECT sequence,operation_id,journal FROM determa_timer_commits "
            "WHERE scope_identity=? ORDER BY sequence",
            (self.scope_identity,),
        ).fetchall()
        if len(rows) != len(artifact["operation_receipts"]):
            raise TimerError("timer_capability_mismatch")
        if not rows and artifact["records"]:
            raise TimerError("timer_capability_mismatch")
        last_digest = None
        for sequence, (native_sequence, operation_id, raw) in enumerate(rows, start=1):
            try:
                journal, original = strict_json(bytes(raw))
                if (
                    native_sequence != sequence
                    or not isinstance(journal, dict)
                    or original != canonical_bytes(journal)
                    or set(journal) != {"request", "result", "timer_artifact_digest"}
                ):
                    raise TimerError("timer_capability_mismatch")
                request = journal["request"]
                result = journal["result"]
                receipt = artifact["operation_receipts"][sequence - 1]
                if (
                    not _validator("timer-helper-operation-v1.schema.json", "request").is_valid(
                        request
                    )
                    or request["request_digest"] != timer_request_digest(request)
                    or request["operation_id"] != operation_id
                    or request["scope_identity"] != self.scope_identity
                    or request["root_instance_id"] != self.root_instance_id
                    or request["root_runtime_id"] != self.root_runtime_id
                    or result["status"] != "accepted"
                    or any(result[key] != request[key] for key in _IDENTITIES)
                    or result["result_digest"]
                    != hash_value(
                        [
                            "determa-timer-result-1",
                            request["request_digest"],
                            {key: value for key, value in result.items() if key != "result_digest"},
                        ]
                    )
                    or receipt
                    != {
                        "operation_id": operation_id,
                        "request_digest": request["request_digest"],
                        "result": result,
                    }
                ):
                    raise TimerError("timer_capability_mismatch")
                last_digest = journal["timer_artifact_digest"]
            except (ArtifactError, TypeError, ValueError, KeyError) as error:
                raise TimerError("timer_capability_mismatch") from error
        if last_digest is not None and last_digest != artifact["timer_artifact_digest"]:
            raise TimerError("timer_capability_mismatch")

    def _record_commit(
        self,
        connection: sqlite3.Connection | SQLiteApplicationTransaction,
        request: Mapping[str, Any],
        result: dict[str, Any],
        artifact: dict[str, Any],
    ) -> None:
        self._persist(connection, artifact)
        sealed = seal_timer_records(artifact)
        _execute(
            connection,
            "INSERT INTO determa_timer_commits VALUES (?,?,?,?)",
            (
                self.scope_identity,
                len(artifact["operation_receipts"]),
                request["operation_id"],
                canonical_bytes(
                    {
                        "request": request,
                        "result": result,
                        "timer_artifact_digest": sealed["timer_artifact_digest"],
                    }
                ),
            ),
        )

    def snapshot(self) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            connection.execute("BEGIN")
            try:
                self._schema(connection)
                return self._load(connection)
            finally:
                connection.rollback()

    def _persist(
        self,
        connection: sqlite3.Connection | SQLiteApplicationTransaction,
        artifact: dict[str, Any],
    ) -> None:
        artifact["records"].sort(
            key=lambda record: (
                record["scope_identity"],
                record["root_instance_id"],
                record["root_runtime_id"],
                record["timer_id"],
            )
        )
        artifact = seal_timer_records(artifact)
        if not _validator("timer-record-v1.schema.json").is_valid(artifact):
            raise TimerError("timer_capability_mismatch")
        _execute(
            connection,
            "INSERT INTO determa_timer_helpers VALUES (?,?) "
            "ON CONFLICT(scope_identity) DO UPDATE SET document=excluded.document",
            (self.scope_identity, canonical_bytes(artifact)),
        )

    def _event(self, arguments: Mapping[str, Any]) -> None:
        declaration = self.bundle.raw.get("events", {}).get(arguments["event_name"])
        try:
            payload = decoded_typed_value(arguments["payload"])
            valid = (
                declaration
                and declaration.get("direction") == "input"
                and _normalize_payload(declaration, payload) is not None
            )
        except (TypeError, ValueError, KeyError):
            valid = False
        if not valid:
            raise TimerError("invalid_timer_request")

    def _now(self) -> int:
        try:
            return _time(self.trusted_clock())
        except Exception as error:
            raise TimerError("timer_clock_unavailable") from error

    def _prove_local_uncommitted_fire(
        self, connection: sqlite3.Connection, artifact: dict[str, Any], record: dict[str, Any]
    ) -> None:
        """Resolve the previous native transaction before committing a replacement fence.

        Caller time, absence of an event, and portable claims are not this proof.
        The caller owns BEGIN IMMEDIATE, and _load has validated the complete immutable
        native commit history against this storage-bound origin. Every completion by
        this exact coordinated implementation commits admission and fired evidence
        together. The preceding writer has therefore resolved before this decision.
        """
        host = self.coordinated_host
        if (
            type(self) is not SQLiteTimerHelper
            or type(host) is not ExecutionHost
            or type(host.store) is not SQLiteExecutionStore
            or Path(host.store.path).resolve() != Path(self.path)
            or not host.store.shared_application_transactions
            or host.store.journal_mode != "WAL"
            or host.store.synchronous != "FULL"
            or host.store.replay_retention != "permanent"
            or not connection.in_transaction
            or any(
                getattr(SharedExecutionTransaction, name) is not method
                or getattr(method, "__code__", None) is not code
                for name, method, code in _FATE_SHARED_METHODS
            )
            or any(
                getattr(getattr(instance, name), "__func__", None) is not method
                or getattr(method, "__code__", None) is not code
                for instance, methods in (
                    (host, _FATE_HOST_METHODS),
                    (host.store, _FATE_STORE_METHODS),
                )
                for name, method, code in methods
            )
            or any(
                getattr(getattr(self, name), "__func__", None) is not method
                or getattr(method, "__code__", None) is not code
                for name, method, code in _FATE_METHODS
            )
        ):
            raise TimerError("delivery_ambiguous")
        retained_claim = any(
            receipt["result"]["operation"] == "claim_fire"
            and receipt["result"]["timer_id"] == record["timer_id"]
            and receipt["result"]["record_revision"] == record["revision"]
            and receipt["result"]["attempt_fence"] == record["attempt_fence"]
            and receipt["result"]["expires_at"] == record["expires_at"]
            for receipt in artifact["operation_receipts"]
        )
        if not retained_claim or any(
            receipt["result"]["operation"] == "complete_fire"
            and receipt["result"]["timer_id"] == record["timer_id"]
            for receipt in artifact["operation_receipts"]
        ):
            raise TimerError("delivery_ambiguous")
        try:
            host.store._validate_schema(connection)
            row = connection.execute(
                "SELECT revision,checkpoint_digest,checkpoint FROM determa_execution_checkpoints "
                "WHERE root_instance_id=?",
                (self.root_instance_id,),
            ).fetchone()
            if row is None:
                raise TimerError("delivery_ambiguous")
            checkpoint = restore_execution_checkpoint_v1(
                bytes(row[2]), host.artifact_resolver
            ).document
            aggregate = checkpoint["root_record"].get("aggregate_state")
            if (
                checkpoint["revision"] != row[0]
                or checkpoint["execution_checkpoint_digest"] != row[1]
                or checkpoint["root_instance_id"] != self.root_instance_id
                or not aggregate
                or aggregate["root_runtime_id"] != self.root_runtime_id
                or aggregate["validated_bundle_fingerprint"] != self.bundle.fingerprint
                or checkpoint["replay_retention"]["mode"] != "permanent"
                or checkpoint["replay_retention"]["pruned_through_receipt_sequence"] is not None
                # An independently admitted matching event is not helper completion;
                # never promote it or infer safe recovery from its receipt or absence.
                or any(
                    receipt.get("event_id") == record["event_id"]
                    for receipt in checkpoint["operation_receipts"]
                )
            ):
                raise TimerError("delivery_ambiguous")
        except (ArtifactError, ExecutionStoreError, sqlite3.Error) as error:
            raise TimerError("delivery_ambiguous") from error

    def _complete(self, request: Mapping[str, Any], principal: str) -> dict[str, Any]:
        host = self.coordinated_host
        assert host is not None
        if (
            type(host.store) is not SQLiteExecutionStore
            or Path(host.store.path).resolve() != Path(self.path)
            or not host.store.shared_application_transactions
        ):
            return _result(request, None, "timer_capability_mismatch")
        committed: list[dict[str, Any]] = []

        def callback(
            sql: SQLiteApplicationTransaction, execution: SharedExecutionTransaction
        ) -> None:
            record = None
            claimed_record = None
            try:
                self._schema(sql)
                artifact = self._load(sql)
                record = next(
                    (
                        item
                        for item in artifact["records"]
                        if item["timer_id"] == request["timer_id"]
                        and item["root_instance_id"] == self.root_instance_id
                        and item["root_runtime_id"] == self.root_runtime_id
                    ),
                    None,
                )
                claimed_record = copy.deepcopy(record)
                if request["arguments"]["worker_principal"] != principal:
                    raise TimerError("timer_worker_mismatch")
                previous = next(
                    (
                        item
                        for item in artifact["operation_receipts"]
                        if item["operation_id"] == request["operation_id"]
                    ),
                    None,
                )
                if previous is not None:
                    raise _TimerReturn(
                        copy.deepcopy(previous["result"])
                        if previous["request_digest"] == request["request_digest"]
                        else _result(request, record, "timer_operation_conflict")
                    )
                if record is None:
                    raise TimerError("timer_not_found")
                claimed_record = copy.deepcopy(record)
                arguments = request["arguments"]
                if record["attempt_fence"] != arguments["attempt_fence"]:
                    raise TimerError("timer_stale_fence")
                if record["revision"] != arguments["expected_revision"]:
                    raise TimerError("timer_revision_conflict")
                if record["state"] != "claimed":
                    raise TimerError(
                        "timer_already_fired"
                        if record["state"] == "fired"
                        else "timer_cancelled"
                        if record["state"] == "cancelled"
                        else "timer_stale_fence"
                    )
                if record["worker_principal"] != principal:
                    raise TimerError("timer_worker_mismatch")
                if record["event_id"] != arguments["event_id"]:
                    raise TimerError("timer_event_conflict")
                claim_expiry = _time(record["expires_at"])
                if self._now() >= claim_expiry:
                    raise TimerError("timer_stale_fence")
                restored = execution.read_checkpoint()
                if restored is None:
                    raise TimerError("timer_admission_rejected")
                before = restored.document
                aggregate = before["root_record"].get("aggregate_state")
                if (
                    not aggregate
                    or aggregate["root_runtime_id"] != self.root_runtime_id
                    or (aggregate["validated_bundle_fingerprint"] != self.bundle.fingerprint)
                ):
                    raise TimerError("unauthorized_timer_scope")
                envelope = {
                    "event": record["event_name"],
                    "event_id": record["event_id"],
                    "cause_id": record["event_id"],
                    "source": {"host": True},
                    "target": {
                        "root": {
                            "root_instance_id": self.root_instance_id,
                            "root_runtime_id": self.root_runtime_id,
                        }
                    },
                    "payload": record["payload"],
                }
                if record["correlation_id"] is not None:
                    envelope["correlation_id"] = record["correlation_id"]
                envelope_digest = delivery_request_digest(self.root_instance_id, "input", envelope)
                execution.admit_v1(
                    [
                        {
                            "delivery_mode": "input",
                            "envelope": envelope,
                            "envelope_digest": envelope_digest,
                        }
                    ],
                    expected_revision=before["revision"],
                    expected_checkpoint_digest=before["execution_checkpoint_digest"],
                )
                candidate = execution.read_checkpoint()
                assert candidate is not None
                receipt = next(
                    (
                        item
                        for item in candidate.document["operation_receipts"]
                        if item["operation_kind"] == "acceptance"
                        and item["event_id"] == record["event_id"]
                    ),
                    None,
                )
                if receipt is None or receipt["request_digest"] != envelope_digest:
                    raise TimerError("timer_admission_rejected")
                receipt_digest = hash_value(["determa-timer-admission-receipt-1", receipt])
                if receipt_digest != arguments["admission_receipt_digest"]:
                    raise TimerError("timer_event_conflict")
                record.update(
                    state="fired",
                    revision=str(int(record["revision"]) + 1),
                    worker_principal=None,
                    expires_at=None,
                    admission_receipt_digest=receipt_digest,
                )
                result = _result(request, record)
                artifact["operation_receipts"].append(
                    {
                        "operation_id": request["operation_id"],
                        "request_digest": request["request_digest"],
                        "result": result,
                    }
                )
                self._record_commit(sql, request, result, artifact)

                def check_lease() -> None:
                    try:
                        if self._now() >= claim_expiry:
                            raise TimerError("timer_stale_fence")
                    except TimerError as error:
                        raise _TimerReturn(_result(request, claimed_record, error.code)) from error

                sql._before_commit(check_lease)
                committed.append(result)
            except TimerError as error:
                raise _TimerReturn(_result(request, claimed_record, error.code)) from error
            except ExecutionHostError as error:
                raise _TimerReturn(
                    _result(request, claimed_record, "timer_admission_rejected")
                ) from error

        try:
            host.run_shared_transaction(self.root_instance_id, callback)
        except _TimerReturn as returned:
            return returned.result
        return committed[0]

    def execute(self, request: Mapping[str, Any], *, principal: str) -> dict[str, Any]:
        if not isinstance(request, Mapping) or request.get("interface") != "determa.timer_helper":
            return _result(None, None, "unsupported_timer_protocol")
        if type(request.get("interface_version")) is not int or request["interface_version"] != 1:
            return _result(None, None, "unsupported_timer_protocol_version")
        request = copy.deepcopy(dict(request))
        # Normative invalid-time vectors retain recognized operation identities.
        # Check the closed structure with a canonical time placeholder, then check
        # the original time domain before any clock read or record write.
        structural = copy.deepcopy(request)
        if structural.get("operation") == "schedule" and isinstance(
            structural.get("arguments"), dict
        ):
            for field in ("deadline_at", "delay_nanoseconds"):
                if field in structural["arguments"]:
                    structural["arguments"][field] = "0"
        try:
            valid = _validator("timer-helper-operation-v1.schema.json", "request").is_valid(
                structural
            ) and request.get("request_digest") == timer_request_digest(request)
        except (ArtifactError, TypeError, ValueError, OverflowError):
            valid = False
        if not valid:
            return _result(None, None, "invalid_timer_request")
        if principal not in self.principals or (
            request["scope_identity"],
            request["root_instance_id"],
            request["root_runtime_id"],
        ) != (self.scope_identity, self.root_instance_id, self.root_runtime_id):
            return _result(request, None, "unauthorized_timer_scope")
        if (
            request["operation"] in ("claim_fire", "complete_fire")
            and principal not in self.worker_principals
        ):
            return _result(request, None, "unauthorized_timer_scope")
        if request["operation"] == "complete_fire" and self.coordinated_host is not None:
            return self._complete(request, principal)
        record = None
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._schema(connection)
            artifact = self._load(connection)
            record = next(
                (
                    record
                    for record in artifact["records"]
                    if record["timer_id"] == request["timer_id"]
                    and record["root_instance_id"] == self.root_instance_id
                    and record["root_runtime_id"] == self.root_runtime_id
                ),
                None,
            )
            previous = next(
                (
                    receipt
                    for receipt in artifact["operation_receipts"]
                    if receipt["operation_id"] == request["operation_id"]
                ),
                None,
            )
            if (
                request["operation"] in ("claim_fire", "complete_fire")
                and request["arguments"]["worker_principal"] != principal
            ):
                return _result(request, record, "timer_worker_mismatch")
            if previous is not None:
                return (
                    copy.deepcopy(previous["result"])
                    if previous["request_digest"] == request["request_digest"]
                    else _result(request, record, "timer_operation_conflict")
                )
            operation = request["operation"]
            if operation == "schedule":
                if record is not None:
                    return _result(request, record, "timer_id_conflict")
                self._event(request["arguments"])
                arguments = request["arguments"]
                if "deadline_at" in arguments:
                    deadline = _time(arguments["deadline_at"])
                else:
                    delay = _time(arguments["delay_nanoseconds"], duration=True)
                    now = self._now()
                    deadline = now + delay
                    if not -(2**63) <= deadline <= 2**63 - 1:
                        raise TimerError("timer_deadline_overflow")
                record = {
                    key: request[key]
                    for key in ("scope_identity", "root_instance_id", "root_runtime_id", "timer_id")
                }
                record.update(
                    schedule_request_digest=request["request_digest"],
                    clock_basis="unix_nanoseconds",
                    deadline_at=str(deadline),
                    event_name=arguments["event_name"],
                    payload=arguments["payload"],
                    correlation_id=arguments["correlation_id"],
                    event_id=hash_value(
                        [
                            "determa-timer-fire-event-1",
                            "1",
                            self.scope_identity,
                            self.root_instance_id,
                            self.root_runtime_id,
                            request["timer_id"],
                        ]
                    ),
                    state="pending",
                    revision="1",
                    attempt_fence="0",
                    worker_principal=None,
                    expires_at=None,
                    admission_receipt_digest=None,
                )
                artifact["records"].append(record)
            elif operation == "read_timer":
                return (
                    _result(request, record)
                    if record
                    else _result(request, None, "timer_not_found")
                )
            elif operation == "cancel":
                if record is None:
                    return _result(request, None, "timer_not_found")
                if request["arguments"]["expected_revision"] != record["revision"]:
                    return _result(request, record, "timer_revision_conflict")
                if record["state"] == "claimed":
                    return _result(request, record, "timer_fire_in_progress")
                if record["state"] == "fired":
                    return _result(request, record, "timer_already_fired")
                if record["state"] == "pending":
                    record.update(state="cancelled", revision=str(int(record["revision"]) + 1))
            elif operation == "claim_fire":
                if record is None:
                    return _result(request, None, "timer_not_found")
                if request["arguments"]["expected_revision"] != record["revision"]:
                    return _result(request, record, "timer_revision_conflict")
                if record["state"] == "fired":
                    return _result(request, record, "timer_already_fired")
                if record["state"] == "cancelled":
                    return _result(request, record, "timer_cancelled")
                now = self._now()
                if record["state"] == "claimed":
                    if now < _time(record["expires_at"]):
                        return _result(request, record, "timer_fire_in_progress")
                    # Dispatch through the retained implementation: an instance or
                    # class replacement must not bypass the checks inside the proof.
                    # Function objects are mutable too: attest the retained code
                    # before entering it, rather than letting it attest itself.
                    if _FATE_PROOF.__code__ is not _FATE_PROOF_CODE:
                        raise TimerError("delivery_ambiguous")
                    _FATE_PROOF(self, connection, artifact, record)
                if now < _time(record["deadline_at"]):
                    return _result(request, record, "timer_not_due")
                expires_at = now + self.claim_lease_nanoseconds
                if expires_at > 2**63 - 1:
                    return _result(request, record, "timer_deadline_overflow")
                record.update(
                    state="claimed",
                    revision=str(int(record["revision"]) + 1),
                    attempt_fence=str(int(record["attempt_fence"]) + 1),
                    worker_principal=principal,
                    expires_at=str(expires_at),
                )
            else:
                return _result(request, record, "timer_capability_mismatch")
            result = _result(request, record)
            artifact["operation_receipts"].append(
                {
                    "operation_id": request["operation_id"],
                    "request_digest": request["request_digest"],
                    "result": result,
                }
            )
            self._record_commit(connection, request, result, artifact)
            connection.commit()
            return result
        except TimerError as error:
            return _result(request, record, error.code)
        finally:
            connection.rollback()
            connection.close()


# Trusted local factory methods remain bound while an instance proves native fate.
# This guard supplements native origin/history; it is not external registration proof.
_FATE_PROOF = SQLiteTimerHelper._prove_local_uncommitted_fire
_FATE_PROOF_CODE = _FATE_PROOF.__code__
_FATE_METHODS = tuple(
    (name, getattr(SQLiteTimerHelper, name), getattr(SQLiteTimerHelper, name).__code__)
    for name in (
        "execute",
        "_complete",
        "_load",
        "_history",
        "_record_commit",
        "_persist",
        "_schema",
        "_configuration",
        "_implementation_digest",
        "_prove_local_uncommitted_fire",
    )
)

_FATE_HOST_METHODS = tuple(
    (name, getattr(ExecutionHost, name), getattr(ExecutionHost, name).__code__)
    for name in ("run_shared_transaction", "_bound", "_restore", "_stage_replace", "admit_v1")
)
_FATE_STORE_METHODS = tuple(
    (name, getattr(SQLiteExecutionStore, name), getattr(SQLiteExecutionStore, name).__code__)
    for name in ("shared_transaction", "_connect", "_validate_schema")
)

_FATE_SHARED_METHODS = tuple(
    (
        name,
        getattr(SharedExecutionTransaction, name),
        getattr(SharedExecutionTransaction, name).__code__,
    )
    for name in ("admit_v1", "read_checkpoint", "_stage", "_finish")
)
