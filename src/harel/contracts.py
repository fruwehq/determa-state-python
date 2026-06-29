"""Static contract validation (SPEC §7).

A contract declares required events (handled somewhere), states (declared), and
spawns (defs referenced by a `spawn` action). A machine's declared contracts
(its top-level `contracts: [...]`) are checked against these requirements as
part of static validation (the §9 `static:` mode and the CLI `validate`).
"""

from __future__ import annotations

from typing import Any

from . import yaml12
from .errors import ErrorRecord


def load_contract(text: str) -> dict[str, Any]:
    doc = yaml12.load(text)
    if not isinstance(doc, dict):
        raise ValueError("a contract must be a mapping")
    return doc


def validate_contracts(
    machine_raw: dict[str, Any], contracts: dict[str, dict[str, Any]]
) -> list[ErrorRecord]:
    errors: list[ErrorRecord] = []
    handled, state_names, spawns = _collect(machine_raw.get("top") or {})
    for cid in machine_raw.get("contracts") or []:
        contract = contracts.get(cid)
        if contract is None:
            errors.append(
                ErrorRecord(path="/contracts", message=f"contract '{cid}' not found")
            )
            continue
        requires = contract.get("requires") or {}
        for ev in requires.get("events") or []:
            if ev not in handled:
                errors.append(
                    ErrorRecord(
                        path=f"/contracts/{cid}",
                        message=f"required event '{ev}' has no handler",
                    )
                )
        for st in requires.get("states") or []:
            if st not in state_names:
                errors.append(
                    ErrorRecord(
                        path=f"/contracts/{cid}",
                        message=f"required state '{st}' is not declared",
                    )
                )
        for sp in requires.get("spawns") or []:
            if sp not in spawns:
                errors.append(
                    ErrorRecord(
                        path=f"/contracts/{cid}",
                        message=f"required spawn '{sp}' is never used",
                    )
                )
    return errors


def _actions(node: dict[str, Any]) -> list[list[dict[str, Any]]]:
    """All action-lists attached to a state node (entry/exit/transitions/after)."""
    lists: list[list[dict[str, Any]]] = []
    lists.append(node.get("entry") or [])
    lists.append(node.get("exit") or [])
    for spec in (node.get("on_events") or {}).values():
        transitions = spec if isinstance(spec, list) else [spec]
        for t in transitions:
            lists.append(t.get("action") or [])
    for after in node.get("after") or []:
        lists.append(after.get("action") or [])
    return lists


def _collect(top: dict[str, Any]) -> tuple[set[str], set[str], set[str]]:
    handled: set[str] = set()
    state_names: set[str] = set()
    spawns: set[str] = set()

    def walk(node: dict[str, Any]) -> None:
        handled.update((node.get("on_events") or {}).keys())
        for action_list in _actions(node):
            for action in action_list:
                if "spawn" in action:
                    spawns.add(action["spawn"]["def"])
        for name, child in (node.get("states") or {}).items():
            state_names.add(name)
            walk(child)
        for region in node.get("regions") or []:
            for name, child in (region.get("states") or {}).items():
                state_names.add(name)
                walk(child)

    walk(top)
    return handled, state_names, spawns
