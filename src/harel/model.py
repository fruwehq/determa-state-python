"""Resolved machine model — a navigable state tree built from a Definition.

The raw validated YAML (``Definition.raw``) is the source of structure; this
module adds parent links, lookup, dotted-target resolution, and reference
validation so the engine can dispatch and transition (SPEC §4.5, §5.5).

State types are inferred when not stated: ``top`` (and any state with
``states``) is ``composite``; a state with ``regions`` is ``orthogonal``;
otherwise ``simple`` (or ``final`` if declared).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .definition import Definition
from .errors import ErrorRecord, ValidationError

TYPE_ORDER = ("simple", "composite", "orthogonal", "final")


@dataclass
class State:
    name: str
    path: str  # dotted from top, e.g. "top.work.step1"; "top" for the root
    parent: State | None
    type: str
    depth: int
    raw: dict[str, Any]
    children: dict[str, State] = field(default_factory=dict)
    declares_esvs: set[str] = field(default_factory=set)
    region_index: int | None = None  # 0-based region for orthogonal substates


def _infer_type(raw: dict[str, Any]) -> str:
    declared = raw.get("type")
    if isinstance(declared, str):
        return declared
    if "regions" in raw:
        return "orthogonal"
    if "states" in raw:
        return "composite"
    return "simple"


class Machine:
    """A resolved machine definition (navigable state tree + lookups)."""

    def __init__(self, definition: Definition) -> None:
        self.definition = definition
        self.id = definition.id
        self.version = definition.version
        self.format = definition.format
        top_raw = definition.top
        self.top = self._build("top", "top", None, 0, top_raw, None)
        self.by_path: dict[str, State] = {}
        self._index(self.top)
        self._validate_references()

    # --- construction -------------------------------------------------------
    def _build(
        self,
        name: str,
        path: str,
        parent: State | None,
        depth: int,
        raw: dict[str, Any],
        region_index: int | None,
    ) -> State:
        state = State(
            name=name,
            path=path,
            parent=parent,
            type=_infer_type(raw),
            depth=depth,
            raw=raw,
            region_index=region_index,
        )
        esvs = raw.get("esvs") or {}
        state.declares_esvs = set(esvs.keys())
        for cname, cdef in (raw.get("states") or {}).items():
            state.children[cname] = self._build(
                cname, f"{path}.{cname}", state, depth + 1, cdef, region_index
            )
        for i, region in enumerate(raw.get("regions") or []):
            for cname, cdef in (region.get("states") or {}).items():
                state.children[cname] = self._build(
                    cname, f"{path}.{cname}", state, depth + 1, cdef, i
                )
        return state

    def _index(self, state: State) -> None:
        self.by_path[state.path] = state
        for child in state.children.values():
            self._index(child)

    # --- navigation ---------------------------------------------------------
    def proper_ancestors(self, state: State) -> list[State]:
        """Ancestors excluding ``state`` itself, nearest first, up to ``top``."""
        out: list[State] = []
        cur = state.parent
        while cur is not None:
            out.append(cur)
            cur = cur.parent
        return out

    def lca(self, a: State, b: State) -> State:
        """Least common *proper* ancestor of ``a`` and ``b`` (never a/b itself)."""
        anc_a = {s.path for s in self.proper_ancestors(a)}
        for x in self.proper_ancestors(b):  # nearest first
            if x.path in anc_a:
                return x
        return self.top

    def resolve_target(self, source: State, ref: str) -> State:
        """Resolve a dotted ``transition_to`` reference (SPEC §4.6).

        The first component is found by searching from ``source`` upward (a
        state may reference its own children, siblings, or outer states); the
        remaining components descend from there.
        """
        parts = ref.split(".")
        anchor: State | None = None
        cur: State | None = source
        while cur is not None:
            if parts[0] in cur.children:
                anchor = cur.children[parts[0]]
                break
            cur = cur.parent
        if anchor is None:
            raise KeyError(ref)
        node = anchor
        for p in parts[1:]:
            if p not in node.children:
                raise KeyError(ref)
            node = node.children[p]
        return node

    # --- static checks ------------------------------------------------------
    def _validate_references(self) -> None:
        errors: list[ErrorRecord] = []
        for state in self.by_path.values():
            refs: list[tuple[str, str]] = []  # (ref, path)
            initial = state.raw.get("initial")
            if isinstance(initial, dict) and "transition_to" in initial:
                refs.append((initial["transition_to"], f"{state.path}/initial"))
            for ev, spec in (state.raw.get("on_events") or {}).items():
                for t in _as_transition_list(spec):
                    if "transition_to" in t:
                        refs.append((t["transition_to"], f"{state.path}/on_events/{ev}"))
            for after in state.raw.get("after") or []:
                if "transition_to" in after:
                    refs.append((after["transition_to"], f"{state.path}/after"))
            for ref, where in refs:
                try:
                    self.resolve_target(state, ref)
                except KeyError:
                    errors.append(
                        ErrorRecord(
                            path=f"/top/{where}/transition_to",
                            message=f"unresolved target '{ref}' from '{state.name}'",
                        )
                    )
        if errors:
            raise ValidationError(errors)


def _as_transition_list(spec: Any) -> list[dict[str, Any]]:
    if isinstance(spec, list):
        return spec
    if isinstance(spec, dict):
        return [spec]
    return []
