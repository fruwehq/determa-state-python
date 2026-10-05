"""Explicit foreground SQLite timer records, separate from portable checkpoints.

This implementation checkpoint supports durable schedule/cancel/read operations.
Fire, coordinated admission, verified provider installation and archive integration
remain unfinished; no completed timer profile is advertised yet.
"""

from __future__ import annotations

import copy
import json
import re
import sqlite3
from collections.abc import Callable, Mapping
from contextlib import closing
from pathlib import Path
from typing import Any

from .definition import Bundle
from .engine import _normalize_payload
from .wire import _schema_registry, canonical_bytes, decoded_typed_value, hash_value

_TIME = re.compile(r"(?:0|-?[1-9][0-9]*)\Z")
_TABLE = (
    "CREATE TABLE determa_timer_helpers "
    "(scope_identity TEXT PRIMARY KEY NOT NULL, document BLOB NOT NULL)"
)
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
        "expires_at": None,
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

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, isolation_level=None, timeout=30)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def setup_schema(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute(_TABLE.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1))
        self.validate_schema()

    def _schema(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='determa_timer_helpers'"
        ).fetchall()
        if rows != [(_TABLE,)] or connection.execute("PRAGMA integrity_check").fetchone() != (
            "ok",
        ):
            raise TimerError("timer_capability_mismatch")

    def validate_schema(self) -> None:
        with closing(self._connect()) as connection:
            self._schema(connection)

    def _load(self, connection: sqlite3.Connection) -> dict[str, Any]:
        row = connection.execute(
            "SELECT document FROM determa_timer_helpers WHERE scope_identity=?",
            (self.scope_identity,),
        ).fetchone()
        if row is None:
            return seal_timer_records(
                {
                    "timer_artifact_format": "determa.timer_records",
                    "timer_artifact_schema_version": 1,
                    "records": [],
                    "operation_receipts": [],
                }
            )
        artifact: dict[str, Any] = json.loads(bytes(row[0]))
        if (
            not _validator("timer-record-v1.schema.json").is_valid(artifact)
            or seal_timer_records(artifact) != artifact
        ):
            raise TimerError("timer_capability_mismatch")
        if any(record["scope_identity"] != self.scope_identity for record in artifact["records"]):
            raise TimerError("timer_capability_mismatch")
        return artifact

    def snapshot(self) -> dict[str, Any]:
        with closing(self._connect()) as connection:
            self._schema(connection)
            return self._load(connection)

    def _persist(self, connection: sqlite3.Connection, artifact: dict[str, Any]) -> None:
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
        connection.execute(
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
        if not _validator("timer-helper-operation-v1.schema.json", "request").is_valid(
            structural
        ) or request.get("request_digest") != timer_request_digest(request):
            return _result(None, None, "invalid_timer_request")
        if principal not in self.principals or (
            request["scope_identity"],
            request["root_instance_id"],
            request["root_runtime_id"],
        ) != (self.scope_identity, self.root_instance_id, self.root_runtime_id):
            return _result(request, None, "unauthorized_timer_scope")
        connection = self._connect()
        try:
            self._schema(connection)
            connection.execute("BEGIN IMMEDIATE")
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
                    try:
                        now = _time(self.trusted_clock())
                    except Exception as error:
                        raise TimerError("timer_clock_unavailable") from error
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
            self._persist(connection, artifact)
            connection.commit()
            return result
        except TimerError as error:
            return _result(request, None, error.code)
        finally:
            connection.rollback()
            connection.close()
