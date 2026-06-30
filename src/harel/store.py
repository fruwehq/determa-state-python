"""File-backed store for the CLI (SPEC §13.1).

A store is a directory (``--store <dir>``, default ``$HAREL_STORE`` or
``./.harel``) holding the registered definitions, instance snapshots, and the
virtual clock. The on-disk layout is an implementation detail; the normative
contract is CLI behaviour + the JSON I/O (§13.4). State-changing commands load
the affected instances, run all instances to quiescence, and persist atomically.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class StoreState:
    defs: dict[str, str] = field(default_factory=dict)  # "id@version" -> yaml text
    instances: list[dict[str, Any]] = field(default_factory=list)
    now: int = 0
    spawn_counters: dict[str, int] = field(default_factory=dict)
    mode: str = "auto"  # processing mode, auto|manual (SPEC §14)


class Store:
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
        defs = self._read_json("defs.json") or {}
        instances = self._read_json("instances.json") or []
        meta = self._read_json("meta.json") or {}
        return StoreState(
            defs=defs,
            instances=instances,
            now=int(meta.get("now", 0)),
            spawn_counters=dict(meta.get("spawn_counters") or {}),
            mode=str(meta.get("mode", "auto")),
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
            json.dumps(
                {
                    "now": state.now,
                    "spawn_counters": state.spawn_counters,
                    "mode": state.mode,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
