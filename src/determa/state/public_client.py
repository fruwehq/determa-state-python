"""Named-binding public v1 client with durable, pinned mutation retry.

Transport authentication stays outside these messages. A transport exception means
unknown fate; the saved request remains available for receipt lookup or exact retry.
"""

from __future__ import annotations

import copy
import json
import sqlite3
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any, cast

from .errors import ArtifactError
from .wire import canonical_bytes, hash_value

_DATA = Path(__file__).with_name("data")
_PROTOCOL = "determa.execution_host"
_READS = {"capabilities", "read", "inspect", "receipt"}


class PublicHostError(ArtifactError):
    """A closed protocol or pinned-binding failure."""


@cache
def _validator(filename: str) -> Any:
    import jsonschema
    from referencing import Registry, Resource
    from referencing.jsonschema import DRAFT202012

    registry = Registry()
    for path in _DATA.glob("*.schema.json"):
        document = json.loads(path.read_text())
        resource = Resource.from_contents(document, default_specification=DRAFT202012)
        registry = registry.with_resource(path.name, resource)
        registry = registry.with_resource(f"https://determa.dev/state/schema/{path.name}", resource)
        if "$id" in document:
            registry = registry.with_resource(document["$id"], resource)
    document = json.loads((_DATA / filename).read_text())
    return jsonschema.Draft202012Validator(document, registry=registry)


def validate_public_message(document: Mapping[str, Any], *, response: bool = False) -> None:
    """Validate a closed public wire message against the pinned v1 schema."""
    filename = f"public-host-{'response' if response else 'request'}-v1.schema.json"
    if next(_validator(filename).iter_errors(document), None) is not None:
        raise PublicHostError("invalid_host_request")
    canonical_bytes(document)


def public_request_digest(request: Mapping[str, Any]) -> str:
    validate_public_message(request)
    return hash_value(["determa-public-host-request-digest-1", "1", request])


@dataclass(frozen=True, slots=True)
class EndpointBinding:
    """A deployment endpoint and scope alias; never part of machine definitions."""

    endpoint: str
    scope_alias: str

    def __post_init__(self) -> None:
        if not self.endpoint or not self.scope_alias:
            raise PublicHostError("binding_unavailable")


class PublicHostClient:
    """Persist complete requests before sending any mutation.

    ``transport`` receives the saved endpoint and closed request. It supplies its
    own authenticated transport context and returns a closed protocol response.
    Connections and credential objects never enter request bytes or this journal.
    """

    def __init__(
        self,
        path: str | Path,
        bindings: Mapping[str, EndpointBinding],
        transport: Callable[[str, dict[str, Any]], dict[str, Any]],
    ) -> None:
        if str(path) == ":memory:":
            raise PublicHostError("binding_unavailable")
        self.path = str(path)
        self.bindings = dict(bindings)
        self.transport = transport

    def setup_schema(self) -> None:
        """Explicitly create a durable client request journal."""
        with self._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS determa_public_client_requests ("
                "operation_id TEXT PRIMARY KEY, binding_name TEXT NOT NULL, "
                "endpoint TEXT NOT NULL, request BLOB NOT NULL, "
                "request_digest TEXT NOT NULL, response BLOB)"
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path)
        try:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            with db:
                yield db
        finally:
            db.close()

    def _send(self, endpoint: str, request: dict[str, Any]) -> dict[str, Any]:
        response = self.transport(endpoint, copy.deepcopy(request))
        return self._checked_response(request, response)

    def _checked_response(
        self, request: dict[str, Any], response: dict[str, Any]
    ) -> dict[str, Any]:
        validate_public_message(response, response=True)
        if response["operation_id"] != request["operation_id"]:
            raise PublicHostError("invalid_host_request")
        if response["status"] == "committed":
            operation = request["operation"]
            required = operation in {"create", "admit", "effect_result", "cancel_effect"}
            if operation == "process":
                required = (
                    response["value"]["result"]["core_result"]["disposition"] != "not_runnable"
                )
            if required and response["receipt"] is None:
                raise PublicHostError("invalid_host_request")
        receipt = response["receipt"]
        if receipt is not None:
            if (
                receipt["scope_binding_identity"] != request["scope_binding_identity"]
                or receipt["operation_id"] != request["operation_id"]
                or receipt["request_digest"] != public_request_digest(request)
            ):
                raise PublicHostError("invalid_host_request")
            if receipt["receipt_kind"] == "committed":
                digest = hash_value(["determa-public-host-evidence-1", "1", response["value"]])
            else:
                digest = hash_value(
                    [
                        "determa-public-host-acceptance-evidence-1",
                        "1",
                        request["scope_binding_identity"],
                        request["operation_id"],
                        receipt["request_digest"],
                        receipt["acceptance_receipt"],
                    ]
                )
            if receipt["evidence_digest"] != digest:
                raise PublicHostError("invalid_host_request")
        if response["value"] is not None and response["value"]["operation"] != request["operation"]:
            raise PublicHostError("invalid_host_request")
        if response["error"] is not None and response["error"]["operation"] != request["operation"]:
            raise PublicHostError("invalid_host_request")
        if request["operation"] == "capabilities" and response["status"] == "committed":
            profile = copy.deepcopy(response["value"]["result"])
            digest = profile.pop("profile_digest")
            if digest != hash_value(
                ["determa-public-host-profile-1", "1", profile["scope_binding_identity"], profile]
            ):
                raise PublicHostError("invalid_host_request")
        if request["operation"] == "receipt" and response["status"] == "committed":
            saved = response["value"]["result"]["saved_response"]
            if saved is not None:
                if saved["operation_id"] != request["arguments"]["queried_operation_id"]:
                    raise PublicHostError("invalid_host_request")
                with self._connect() as db:
                    local = db.execute(
                        "SELECT request,request_digest FROM determa_public_client_requests "
                        "WHERE operation_id=?",
                        (saved["operation_id"],),
                    ).fetchone()
                if local is not None:
                    original = json.loads(local[0])
                    if (
                        public_request_digest(original) != local[1]
                        or local[1] != request["arguments"]["request_digest"]
                        or original["scope_binding_identity"] != request["scope_binding_identity"]
                    ):
                        raise PublicHostError("invalid_host_request")
                    self._checked_response(original, saved)
                evidence = saved["receipt"]
                if evidence is not None and (
                    evidence["scope_binding_identity"] != request["scope_binding_identity"]
                    or evidence["request_digest"] != request["arguments"]["request_digest"]
                    or evidence["operation_id"] != saved["operation_id"]
                ):
                    raise PublicHostError("invalid_host_request")
        return copy.deepcopy(response)

    def discover(self, name: str) -> dict[str, Any]:
        binding = self.bindings.get(name)
        if binding is None:
            raise PublicHostError("binding_unavailable")
        request = {
            "protocol": _PROTOCOL,
            "protocol_version": 1,
            "operation_id": str(uuid.uuid4()),
            "scope_binding_identity": None,
            "operation": "capabilities",
            "target": {"root_instance_id": None, "runtime_id": None, "runtime_incarnation": None},
            "precondition": None,
            "arguments": {"scope_alias": binding.scope_alias},
        }
        validate_public_message(request)
        return self._send(binding.endpoint, request)

    def submit(self, name: str, request: Mapping[str, Any]) -> dict[str, Any]:
        candidate = copy.deepcopy(dict(request))
        operation_id = candidate.get("operation_id")
        if not isinstance(operation_id, str) or not operation_id:
            raise PublicHostError("invalid_host_request")
        with self._connect() as db:
            previous = db.execute(
                "SELECT binding_name, endpoint, request, response FROM "
                "determa_public_client_requests WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        if previous is not None:
            saved = json.loads(previous[2])
            if candidate.get("scope_binding_identity") is None:
                candidate["scope_binding_identity"] = saved["scope_binding_identity"]
            validate_public_message(candidate)
            if name != previous[0] or canonical_bytes(candidate) != previous[2]:
                raise PublicHostError("operation_id_conflict")
            return self.retry(operation_id)
        binding = self.bindings.get(name)
        if binding is None:
            raise PublicHostError("binding_unavailable")
        discovery = self.discover(name)
        if discovery["status"] != "committed":
            raise PublicHostError("binding_unavailable")
        profile = discovery["value"]["result"]
        resolved = profile["scope_binding_identity"]
        if candidate.get("scope_binding_identity") not in {None, resolved}:
            raise PublicHostError("binding_unavailable")
        candidate["scope_binding_identity"] = resolved
        if candidate.get("operation") not in profile["supported_operations"]:
            raise PublicHostError("host_capability_mismatch")
        validate_public_message(candidate)
        if candidate["operation"] in _READS:
            return self._send(binding.endpoint, candidate)
        encoded = canonical_bytes(candidate)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                db.execute(
                    "INSERT INTO determa_public_client_requests VALUES (?,?,?,?,?,NULL)",
                    (
                        operation_id,
                        name,
                        binding.endpoint,
                        encoded,
                        public_request_digest(candidate),
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise PublicHostError("operation_id_conflict") from error
        return self.retry(operation_id)

    def retry(self, operation_id: str) -> dict[str, Any]:
        """Send only saved bytes to the saved endpoint; never rediscover an alias."""
        with self._connect() as db:
            row = db.execute(
                "SELECT endpoint, request, request_digest, response FROM "
                "determa_public_client_requests WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        if row is None:
            raise PublicHostError("outcome_unknown")
        request = json.loads(row[1])
        if canonical_bytes(request) != row[1] or public_request_digest(request) != row[2]:
            raise PublicHostError("invalid_host_request")
        if row[3] is not None:
            response = json.loads(row[3])
            return self._checked_response(request, cast(dict[str, Any], response))
        response = self._send(row[0], request)
        if response["status"] != "pending":
            with self._connect() as db:
                db.execute(
                    "UPDATE determa_public_client_requests SET response=? "
                    "WHERE operation_id=? AND response IS NULL",
                    (canonical_bytes(response), operation_id),
                )
        return response

    def receipt(self, operation_id: str) -> dict[str, Any]:
        """Query retained host evidence at the original pinned binding."""
        with self._connect() as db:
            row = db.execute(
                "SELECT endpoint, request, request_digest FROM "
                "determa_public_client_requests WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        if row is None:
            raise PublicHostError("outcome_unknown")
        saved = json.loads(row[1])
        if canonical_bytes(saved) != row[1] or public_request_digest(saved) != row[2]:
            raise PublicHostError("invalid_host_request")

        request = {
            "protocol": _PROTOCOL,
            "protocol_version": 1,
            "operation_id": str(uuid.uuid4()),
            "scope_binding_identity": saved["scope_binding_identity"],
            "operation": "receipt",
            "target": {"root_instance_id": None, "runtime_id": None, "runtime_incarnation": None},
            "precondition": None,
            "arguments": {"queried_operation_id": operation_id, "request_digest": row[2]},
        }
        validate_public_message(request)
        return self._send(row[0], request)
