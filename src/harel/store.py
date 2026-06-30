"""Store backends for the CLI (SPEC §8, §13.1).

A store holds the registered definitions, instance snapshots, the virtual clock,
and the processing mode (§14). It is selected by a ``--store <spec>`` scheme:

- ``file:<dir>`` (or a bare ``<dir>``) — JSON snapshot files under a directory.
  **Default** (``./.harel``).
- ``mem:`` — in-memory, ephemeral; only meaningful within a single process
  (e.g. one ``run`` batch/streaming session, §13.7, or a test).
- ``sqlite:<path>`` — a single-file SQLite database.

All backends are behaviorally identical (same CLI results, same snapshot JSON, §8);
the on-disk/in-memory layout is an implementation detail. ``open_store(spec)``
parses the scheme.
"""

from __future__ import annotations

import abc
import copy
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_SCHEMES = {"file", "mem", "sqlite"}


@dataclass
class StoreState:
    defs: dict[str, str] = field(default_factory=dict)  # "id@version" -> yaml text
    instances: list[dict[str, Any]] = field(default_factory=list)
    now: int = 0
    spawn_counters: dict[str, int] = field(default_factory=dict)
    mode: str = "auto"  # processing mode, auto|manual (SPEC §14)


def _state_from_parts(
    defs: dict[str, Any] | None,
    instances: list[dict[str, Any]] | None,
    meta: dict[str, Any] | None,
) -> StoreState:
    meta = meta or {}
    return StoreState(
        defs=defs or {},
        instances=instances or [],
        now=int(meta.get("now", 0)),
        spawn_counters=dict(meta.get("spawn_counters") or {}),
        mode=str(meta.get("mode", "auto")),
    )


def _meta_json(state: StoreState) -> dict[str, Any]:
    return {"now": state.now, "spawn_counters": state.spawn_counters, "mode": state.mode}


class Store(abc.ABC):
    """The store adapter interface (SPEC §8): load/save a ``StoreState``."""

    @abc.abstractmethod
    def load(self) -> StoreState: ...

    @abc.abstractmethod
    def save(self, state: StoreState) -> None: ...


class FileStore(Store):
    """JSON snapshot files under a directory (``file:<dir>`` / bare ``<dir>``)."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def _ensure(self) -> None:
        self.path.mkdir(parents=True, exist_ok=True)

    def _read_json(self, name: str) -> Any:
        p = self.path / name
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))

    def load(self) -> StoreState:
        return _state_from_parts(
            self._read_json("defs.json"),
            self._read_json("instances.json"),
            self._read_json("meta.json"),
        )

    def save(self, state: StoreState) -> None:
        self._ensure()
        (self.path / "defs.json").write_text(
            json.dumps(state.defs, indent=2), encoding="utf-8"
        )
        (self.path / "instances.json").write_text(
            json.dumps(state.instances, indent=2), encoding="utf-8"
        )
        (self.path / "meta.json").write_text(
            json.dumps(_meta_json(state), indent=2), encoding="utf-8"
        )


class MemoryStore(Store):
    """In-process, ephemeral store (``mem:``); not shared across processes."""

    def __init__(self) -> None:
        self._state: StoreState = StoreState()

    def load(self) -> StoreState:
        return copy.deepcopy(self._state)

    def save(self, state: StoreState) -> None:
        self._state = copy.deepcopy(state)


class SqliteStore(Store):
    """A single-file SQLite database (``sqlite:<path>``); ``sqlite3`` is stdlib."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if self.path.parent and str(self.path.parent) not in ("", "."):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path))
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS harel_state ("
            "  key TEXT PRIMARY KEY,"
            "  value TEXT NOT NULL"
            ")"
        )
        self._conn.commit()

    def _get(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM harel_state WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row is not None else None

    def _set(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO harel_state (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def load(self) -> StoreState:
        defs = json.loads(self._get("defs") or "{}")
        instances = json.loads(self._get("instances") or "[]")
        meta = json.loads(self._get("meta") or "{}")
        return _state_from_parts(defs, instances, meta)

    def save(self, state: StoreState) -> None:
        self._set("defs", json.dumps(state.defs))
        self._set("instances", json.dumps(state.instances))
        self._set("meta", json.dumps(_meta_json(state)))
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


def _split_scheme(spec: str) -> tuple[str, str]:
    scheme, sep, rest = spec.partition(":")
    if sep and scheme in _SCHEMES:
        return scheme, rest
    return "file", spec


def open_store(spec: str) -> Store:
    """Select a backend from a ``--store <spec>`` scheme (SPEC §13.1)."""
    scheme, rest = _split_scheme(spec)
    if scheme == "mem":
        return MemoryStore()
    if scheme == "sqlite":
        return SqliteStore(rest)
    return FileStore(rest)  # "file:<dir>" or a bare "<dir>"
