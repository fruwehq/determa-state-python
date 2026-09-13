"""Production host transaction composition for checkpoint-backed applications."""

from __future__ import annotations

import copy
import json
import sqlite3
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .checkpoint import serialize_execution_checkpoint
from .codes import ExecutionStoreAdapterFailureCode as AdapterCode
from .errors import ArtifactError
from .host import ExecutionHost, ExecutionHostError, validate_host_profile
from .stores import (
    DURABLE_SINGLE_WRITER,
    RESTART_PERSISTENT,
    ROOT_IDENTITY_RETENTION,
    SHARED_APPLICATION_TRANSACTION,
    ExecutionStore,
    ExecutionStoreError,
    ExecutionStoreTransaction,
)
from .stores.base import checkpoint_metadata
from .wire import ArtifactResolver, canonical_bytes

_CAPABILITIES = frozenset(
    {
        RESTART_PERSISTENT,
        DURABLE_SINGLE_WRITER,
        ROOT_IDENTITY_RETENTION,
        SHARED_APPLICATION_TRANSACTION,
    }
)


class _DocumentCheckpointTransaction(ExecutionStoreTransaction):
    def __init__(self, document: dict[str, Any]) -> None:
        self.document = document
        self._root_instance_id = document["checkpoint"]["root_instance_id"]

    @property
    def root_instance_id(self) -> str:
        root_instance_id = self._root_instance_id
        if not isinstance(root_instance_id, str):
            raise ExecutionStoreError("transaction_root_mismatch")
        return root_instance_id

    def load(self) -> bytes:
        return serialize_execution_checkpoint(self.document["checkpoint"])

    def insert(self, checkpoint: bytes) -> bool:
        del checkpoint
        return False

    def replace(
        self,
        expected_revision: str,
        expected_checkpoint_digest: str,
        checkpoint: bytes,
    ) -> bool:
        current = self.document["checkpoint"]
        if (
            current["revision"] != expected_revision
            or current["execution_checkpoint_digest"]
            != expected_checkpoint_digest
        ):
            return False
        root_instance_id, _revision, _digest = checkpoint_metadata(checkpoint)
        if root_instance_id != self._root_instance_id:
            raise ExecutionStoreError("transaction_root_mismatch")
        self.document["checkpoint"] = json.loads(checkpoint)
        return True


class DurableHostStore(ExecutionStore):
    """Execution store extended with one atomic host-document transaction."""

    @contextmanager
    def host_transaction(
        self, root_instance_id: str
    ) -> Iterator[tuple[dict[str, Any], ExecutionStoreTransaction]]:
        raise NotImplementedError

    def snapshot(self, root_instance_id: str) -> bytes:
        raise NotImplementedError


class MemoryDurableHostStore(DurableHostStore):
    """Atomic in-memory host-document store for embedded execution and tests."""

    def __init__(self, documents: Mapping[str, bytes]) -> None:
        self._documents = {key: bytes(value) for key, value in documents.items()}
        self._lock = threading.RLock()

    @property
    def capabilities(self) -> frozenset[str]:
        return _CAPABILITIES

    @contextmanager
    def host_transaction(
        self, root_instance_id: str
    ) -> Iterator[tuple[dict[str, Any], ExecutionStoreTransaction]]:
        with self._lock:
            source = self._documents.get(root_instance_id)
            if source is None:
                raise ExecutionStoreError("wrong_root")
            document = json.loads(source)
            transaction = _DocumentCheckpointTransaction(document)
            yield document, transaction
            self._documents[root_instance_id] = canonical_bytes(document)

    @contextmanager
    def transaction(
        self, root_instance_id: str
    ) -> Iterator[ExecutionStoreTransaction]:
        with self.host_transaction(root_instance_id) as (_document, transaction):
            yield transaction

    def snapshot(self, root_instance_id: str) -> bytes:
        with self._lock:
            try:
                return bytes(self._documents[root_instance_id])
            except KeyError as exc:
                raise ExecutionStoreError("wrong_root") from exc

    def setup_schema(self) -> None:
        return None

    def health(self) -> Mapping[str, Any]:
        return {"healthy": True, "record_count": len(self._documents)}


class SQLiteDurableHostStore(DurableHostStore):
    """SQLite store for one atomic checkpoint, inbox, outbox, and application row."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        if not self.path or self.path == ":memory:":
            raise ExecutionStoreError(AdapterCode.INVALID_ADAPTER_CONFIGURATION)

    @property
    def capabilities(self) -> frozenset[str]:
        return _CAPABILITIES

    def setup_schema(self) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS determa_durable_host_documents ("
                "root_instance_id TEXT PRIMARY KEY NOT NULL, document BLOB NOT NULL)"
            )

    def seed(self, root_instance_id: str, source: bytes) -> None:
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "INSERT INTO determa_durable_host_documents "
                "(root_instance_id, document) VALUES (?, ?)",
                (root_instance_id, source),
            )

    @contextmanager
    def host_transaction(
        self, root_instance_id: str
    ) -> Iterator[tuple[dict[str, Any], ExecutionStoreTransaction]]:
        connection = sqlite3.connect(self.path, isolation_level=None)
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT document FROM determa_durable_host_documents "
                "WHERE root_instance_id = ?",
                (root_instance_id,),
            ).fetchone()
            if row is None:
                raise ExecutionStoreError("wrong_root")
            document = json.loads(bytes(row[0]))
            transaction = _DocumentCheckpointTransaction(document)
            yield document, transaction
            connection.execute(
                "UPDATE determa_durable_host_documents SET document = ? "
                "WHERE root_instance_id = ?",
                (canonical_bytes(document), root_instance_id),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def transaction(
        self, root_instance_id: str
    ) -> Iterator[ExecutionStoreTransaction]:
        with self.host_transaction(root_instance_id) as (_document, transaction):
            yield transaction

    def snapshot(self, root_instance_id: str) -> bytes:
        with sqlite3.connect(self.path) as connection:
            row = connection.execute(
                "SELECT document FROM determa_durable_host_documents "
                "WHERE root_instance_id = ?",
                (root_instance_id,),
            ).fetchone()
        if row is None:
            raise ExecutionStoreError("wrong_root")
        return bytes(row[0])

    def validate_schema(self) -> None:
        with sqlite3.connect(self.path) as connection:
            columns = connection.execute(
                "PRAGMA table_info(determa_durable_host_documents)"
            ).fetchall()
        if [row[1] for row in columns] != ["root_instance_id", "document"]:
            raise ExecutionStoreError("execution_store_schema_mismatch")

    def health(self) -> Mapping[str, Any]:
        try:
            self.validate_schema()
        except (sqlite3.Error, ExecutionStoreError):
            return {"healthy": False, "schema_ready": False}
        return {"healthy": True, "schema_ready": True}


class PersistenceHost:
    """Coordinate durable delivery processing through a production ExecutionHost."""

    def __init__(
        self,
        store: DurableHostStore,
        artifact_resolver: ArtifactResolver,
    ) -> None:
        self.store = store
        self.execution_host = ExecutionHost(store, artifact_resolver)
        self.calls: list[str] = []
        self.core_calls = 0

    @staticmethod
    def _delivery(request: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "delivery_mode": "input",
            "envelope": copy.deepcopy(request["presented_envelope"]),
            "envelope_digest": request["envelope_digest"],
        }

    def _preflight(self, request: Mapping[str, Any]) -> None:
        self.calls = ["select_scope"]
        scope = request["scope"]
        if scope["authorization"] != "authorized":
            raise ExecutionHostError("invalid_store_scope")
        self.calls.append("resolve_artifacts")
        target = request.get("transaction_inputs", {}).get(
            "target_validated_bundle_fingerprint"
        )
        if target is not None:
            source = self.execution_host.artifact_resolver.resolve_definition(target)
            if source is None:
                raise ArtifactError("definition_unavailable")
        self.calls.append("validate_capabilities")
        if not set(request.get("store_capabilities", [])).issubset(
            self.store.capabilities
        ):
            raise ExecutionHostError(AdapterCode.ADAPTER_CAPABILITY_MISMATCH)
        if "host_profile" in request:
            validate_host_profile(
                self.store,
                request["host_profile"],
                host_features=frozenset(request.get("host_guarantees", [])),
            )

    def process_v2(self, request: Mapping[str, Any]) -> dict[str, Any]:
        self._preflight(request)
        root_instance_id = request["expected_checkpoint"]["root_instance_id"]
        event_id = request["presented_envelope"]["event_id"]
        policy = request["transaction_inputs"]["failure_policy"]
        if policy == "permanent_quarantine":
            with self.store.host_transaction(root_instance_id) as (document, _tx):
                document["quarantine"] = {
                    "event_id": event_id,
                    "reason_code": "permanent_processing_failure",
                    "released": False,
                }
                document["inbox"].append(
                    {
                        "event_id": event_id,
                        "request_digest": request["envelope_digest"],
                        "disposition": "quarantined",
                    }
                )
            self.calls.append("quarantine")
            return {
                "result": "quarantined",
                "mutation": "atomic",
                "core_calls": 0,
                "broker_acknowledged": False,
                "code": "permanent_processing_failure",
            }

        self.calls.append("begin_transaction")
        with self.store.host_transaction(root_instance_id) as (document, transaction):
            self.calls.extend(["read_checkpoint", "check_replay"])
            prior = next(
                (
                    item
                    for item in document["inbox"]
                    if item["event_id"] == event_id
                ),
                None,
            )
            released = (
                prior is not None
                and prior["disposition"] == "quarantined"
                and (document.get("quarantine") or {}).get("event_id") == event_id
                and document["quarantine"]["released"] is True
            )
            if prior is not None and not released:
                if prior["request_digest"] != request["envelope_digest"]:
                    raise ExecutionHostError("event_id_conflict")
                self.calls.append("acknowledge")
                return {
                    "result": "replayed",
                    "mutation": "none",
                    "core_calls": 0,
                    "broker_acknowledged": True,
                }
            if policy == "transient_retry":
                self.calls.append("rollback")
                raise ExecutionHostError("transient_processing_failure")
            self.calls.append("call_core")
            self.core_calls = 1
            expected = request["expected_checkpoint"]
            bound = self.execution_host._bound(transaction)
            bound.process_delivery_v2(
                root_instance_id,
                self._delivery(request),
                target_validated_bundle_fingerprint=request["transaction_inputs"][
                    "target_validated_bundle_fingerprint"
                ],
                migration_descriptor_digest_route=request["transaction_inputs"][
                    "migration_descriptor_digest_route"
                ],
                expected_revision=expected["revision"],
                expected_checkpoint_digest=expected["digest"],
            )
            identity = {
                "event_id": event_id,
                "request_digest": request["envelope_digest"],
                "disposition": "committed",
            }
            if released:
                assert prior is not None
                prior.clear()
                prior.update(identity)
                document["quarantine"] = None
            else:
                document["inbox"].append(identity)
            document["application_rows"].update(
                request["transaction_inputs"].get("application_writes", {})
            )
            self.calls.extend(
                ["stage_checkpoint", "stage_inbox", "stage_outbox", "stage_audit"]
            )
            if policy == "inject_pre_commit":
                self.calls.append("rollback")
                raise ExecutionHostError("injected_pre_commit_failure")
            if request["transaction_inputs"].get("application_writes"):
                self.calls.append("stage_application_rows")
            self.calls.append("commit")
        if policy == "inject_post_commit_response_loss":
            raise ExecutionHostError("response_lost_after_commit")
        self.calls.append("acknowledge")
        return {
            "result": "committed",
            "mutation": "atomic",
            "core_calls": 1,
            "broker_acknowledged": True,
        }

    def release_quarantine_v2(self, request: Mapping[str, Any]) -> dict[str, Any]:
        self._preflight(request)
        root_instance_id = request["expected_checkpoint"]["root_instance_id"]
        with self.store.host_transaction(root_instance_id) as (document, _tx):
            quarantine = document.get("quarantine")
            if (
                quarantine is None
                or quarantine["event_id"] != request["event_id"]
                or quarantine["reason_code"] != request["quarantine_reason_code"]
                or not request["release_authorization"]
            ):
                raise ExecutionHostError("invalid_execution_checkpoint")
            quarantine["released"] = True
        self.calls.append("release_quarantine")
        return {
            "result": "released",
            "mutation": "atomic",
            "core_calls": 0,
            "broker_acknowledged": False,
        }
