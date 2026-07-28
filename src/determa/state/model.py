"""Resolved format-1 bundle and state-tree model."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .definition import Bundle, _escape_pointer
from .errors import ValidationError


@dataclass
class StateNode:
    """One named active state or choice pseudostate."""

    name: str
    path: str
    pointer: str
    raw: dict[str, Any]
    parent: StateNode | None
    order: int
    children: dict[str, StateNode] = field(default_factory=dict)

    @property
    def type(self) -> str:
        if "choice" in self.raw:
            return "choice"
        return str(self.raw.get("type", "simple"))

    @property
    def is_choice(self) -> bool:
        return self.type == "choice"

    def ancestors(self, *, include_self: bool = False) -> list[StateNode]:
        result: list[StateNode] = [self] if include_self else []
        current = self.parent
        while current is not None:
            result.append(current)
            current = current.parent
        return result

    def is_ancestor_of(self, other: StateNode, *, strict: bool = False) -> bool:
        if not strict and self is other:
            return True
        return self in other.ancestors()


class MachineModel:
    """Resolved state tree for one bundle machine or inline component root."""

    def __init__(
        self,
        bundle: Bundle,
        raw: dict[str, Any],
        *,
        machine_index: int,
        root: dict[str, Any] | None = None,
        root_pointer: str | None = None,
        identity_machine: dict[str, Any] | None = None,
    ) -> None:
        self.bundle = bundle
        self.raw = raw
        self.machine_index = machine_index
        self.machine_id = str(raw["machine_id"])
        self.version = int(raw["version"])
        self.identity_machine = identity_machine or raw
        self.root_pointer = root_pointer or f"/machines/{machine_index}/root"
        self._order = 0
        self.root = self._build(
            "root",
            "root",
            self.root_pointer,
            root if root is not None else raw["root"],
            None,
        )
        self.states: dict[str, StateNode] = {}
        self._index(self.root)

    def _build(
        self,
        name: str,
        path: str,
        pointer: str,
        raw: dict[str, Any],
        parent: StateNode | None,
    ) -> StateNode:
        state = StateNode(
            name=name,
            path=path,
            pointer=pointer,
            raw=raw,
            parent=parent,
            order=self._order,
        )
        self._order += 1
        for child_name, child_raw in (raw.get("states") or {}).items():
            child_path = child_name if path == "root" else f"{path}.{child_name}"
            child_pointer = f"{pointer}/states/{_escape_pointer(child_name)}"
            state.children[child_name] = self._build(
                child_name, child_path, child_pointer, child_raw, state
            )
        return state

    def _index(self, state: StateNode) -> None:
        if state.path in self.states:
            raise ValidationError("semantic_validation", path=state.pointer)
        self.states[state.path] = state
        for child in state.children.values():
            self._index(child)

    def resolve(self, target: str | dict[str, str], source: StateNode | None = None) -> StateNode:
        path = target["history"] if isinstance(target, dict) else target
        if path == "root":
            return self.root
        if path in self.states:
            return self.states[path]
        parts = path.split(".")
        current = source
        while current is not None:
            if parts[0] in current.children:
                resolved = current.children[parts[0]]
                for part in parts[1:]:
                    if part not in resolved.children:
                        break
                    resolved = resolved.children[part]
                else:
                    return resolved
            current = current.parent
        try:
            return self.states[path]
        except KeyError as exc:
            raise ValidationError("semantic_validation", message=f"unknown state {path}") from exc

    def leaves_under(self, state: StateNode) -> list[StateNode]:
        return [
            candidate
            for candidate in self.states.values()
            if not candidate.is_choice
            and not candidate.children
            and state.is_ancestor_of(candidate)
        ]

    def definition_identity(self) -> tuple[str, str, int]:
        return (
            self.bundle.namespace,
            str(self.identity_machine["machine_id"]),
            int(self.identity_machine["version"]),
        )


class BundleModel:
    """Resolved models for all same-bundle machine definitions."""

    def __init__(self, bundle: Bundle) -> None:
        self.bundle = bundle
        self.machines: dict[str, MachineModel] = {}
        for index, raw in enumerate(bundle.raw["machines"]):
            machine_id = str(raw["machine_id"])
            if machine_id in self.machines:
                raise ValidationError("semantic_validation", message="duplicate machine_id")
            self.machines[machine_id] = MachineModel(bundle, raw, machine_index=index)

    def machine(self, machine_id: str) -> MachineModel:
        try:
            return self.machines[machine_id]
        except KeyError as exc:
            raise ValidationError(
                "semantic_validation", message=f"unknown machine {machine_id}"
            ) from exc

    def inline_component(
        self, owner: MachineModel, placement: dict[str, Any], placement_pointer: str
    ) -> MachineModel:
        root = placement["root"]
        return MachineModel(
            self.bundle,
            owner.raw,
            machine_index=owner.machine_index,
            root=root,
            root_pointer=f"{placement_pointer}/root",
            identity_machine=owner.identity_machine,
        )
