"""Machine instance — the active object that runs one statechart (SPEC §3, §5).

Holds the state configuration, live esv values, FIFO event queue, deferred set,
and timers. ``dispatch`` runs one run-to-completion (RTC) step: find a handler
by searching from the active leaf up, then execute the transition (LCA +
exit/entry ordering per PSiCC, internal/local/external kinds, initial descent).
Extended-state lifetime (init on entry before entry actions, destroy on exit,
re-init on re-entry) and hierarchical scoping (inner shadows outer) live here.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from . import cel, values
from .errors import HarelError
from .model import Machine, State

if TYPE_CHECKING:
    from .engine import Host

DELIVERABLE_RESERVED_EVENTS = frozenset({"env", "error", "done"})


class Status(StrEnum):
    ACTIVE = "active"
    FAULTED = "faulted"
    TERMINATED = "terminated"


@dataclass
class Event:
    type: str
    payload: dict[str, Any] | None = None


class Instance:
    def __init__(
        self,
        machine: Machine,
        id: str,
        parent_id: str | None,
        host: Host,
        external: dict[str, Any] | None = None,
    ) -> None:
        self.machine = machine
        self.id = id
        self.parent_id = parent_id
        self.host = host
        self.status = Status.ACTIVE
        self.config: set[str] = set()  # paths of all active states
        self.esv_values: dict[str, dict[str, Any]] = {}  # state path -> {var: value}
        self.queue: deque[Event] = deque()
        self.deferred: deque[Event] = deque()
        self.timers: list[dict[str, Any]] = []
        self.dead_letter: list[dict[str, Any]] = []
        self.external: dict[str, Any] = dict(external or {})
        self.current_event: Event | None = None
        self._enter_top()

    # --- creation -----------------------------------------------------------
    def _enter_top(self) -> None:
        top = self.machine.top
        self.config.add(top.path)
        self.init_esvs(top)
        self.run_entry(top)
        self.descend(top)

    # --- esv lifecycle ------------------------------------------------------
    def init_esvs(self, state: State) -> None:
        esvs = state.raw.get("esvs")
        if not esvs:
            return
        live: dict[str, Any] = {}
        for var, decl in esvs.items():
            if decl.get("external"):
                live[var] = self.external.get(var, decl.get("init"))
            elif "init" in decl:
                live[var] = decl["init"]
            else:
                live[var] = None
        self.esv_values[state.path] = live

    def destroy_esvs(self, state: State) -> None:
        self.esv_values.pop(state.path, None)

    def _scope_chain(self, root: State) -> list[State]:
        return [root, *self.machine.proper_ancestors(root)]

    def scope(self, root: State, event: Event | None) -> dict[str, Any]:
        """Resolved in-scope bindings for a guard/action (inner shadows outer)."""
        bindings: dict[str, Any] = {}
        for s in reversed(self._scope_chain(root)):  # outermost first
            live = self.esv_values.get(s.path)
            if live:
                bindings.update(live)
        bindings["id"] = self.id
        bindings["parent"] = self.parent_id
        ev = {"type": "", "payload": {}}
        if event is not None:
            ev = {"type": event.type, "payload": event.payload or {}}
        bindings["event"] = ev
        return bindings

    def assign_esv(self, root: State, name: str, value: Any) -> None:
        cur: State | None = root
        while cur is not None:
            if name in cur.declares_esvs:
                live = self.esv_values.get(cur.path)
                if live is None:
                    raise HarelError(f"esv '{name}' not live")
                decl = cur.raw["esvs"][name]
                if decl.get("external"):
                    raise HarelError(f"external esv '{name}' is read-only")
                if not values.matches(value, decl["type"]):
                    raise HarelError(f"'{name}' must be {decl['type']}")
                live[name] = value
                return
            cur = cur.parent
        raise HarelError(f"no in-scope esv '{name}' to assign")

    # --- actions ------------------------------------------------------------
    def run_actions(self, actions: list[dict[str, Any]], root: State, event: Event | None) -> None:
        for action in actions:
            self.run_action(action, root, event)

    def run_action(self, action: dict[str, Any], root: State, event: Event | None) -> None:
        if "assign" in action:
            scope = None
            for var, expr in action["assign"].items():
                scope = self.scope(root, event) if scope is None else scope
                self.assign_esv(root, var, cel.evaluate(expr, scope))
            return
        if "publish" in action:
            self.host.publish(self, action["publish"], root, event)
            return
        if "spawn" in action:
            self.host.spawn_action(self, action["spawn"], root, event)
            return
        if "refresh" in action:
            self.host.refresh(self, action["refresh"], event)
            return
        if "stop" in action:
            self.host.stop(self)
            return
        raise HarelError(f"unknown action: {action}")

    def run_entry(self, state: State) -> None:
        self.run_actions(state.raw.get("entry") or [], state, self.current_event)

    def run_exit(self, state: State) -> None:
        self.run_actions(state.raw.get("exit") or [], state, self.current_event)

    # --- configuration queries ---------------------------------------------
    def active_leaves(self) -> list[State]:
        out: list[State] = []
        for path in self.config:
            s = self.machine.by_path[path]
            if not any(c.path in self.config for c in s.children.values()):
                out.append(s)
        return out

    def active_leaf_names(self) -> list[str]:
        return sorted(s.name for s in self.active_leaves())

    def effective_defer_set(self) -> set[str]:
        out: set[str] = set()
        for path in self.config:
            out.update(self.machine.by_path[path].raw.get("defer") or [])
        return out

    def resolved_esvs(self) -> dict[str, Any]:
        """In-scope esv values resolved from the active leaf (as a guard reads)."""
        leaves = self.active_leaves()
        root = leaves[0] if leaves else self.machine.top
        bindings: dict[str, Any] = {}
        for s in reversed(self._scope_chain(root)):
            live = self.esv_values.get(s.path)
            if live:
                bindings.update(live)
        return bindings

    # --- dispatch -----------------------------------------------------------
    def find_handler(
        self, event: Event
    ) -> tuple[State, dict[str, Any], State] | None:
        """Search from each active leaf up; first state with a passing handler."""
        for leaf in self.active_leaves():
            cur: State | None = leaf
            while cur is not None:
                spec = (cur.raw.get("on_events") or {}).get(event.type)
                if spec is not None:
                    chosen = self._select(spec, cur, event)
                    if chosen is not None:
                        return cur, chosen, leaf
                cur = cur.parent
        return None

    def _select(
        self, spec: Any, owner: State, event: Event
    ) -> dict[str, Any] | None:
        transitions: list[dict[str, Any]] = spec if isinstance(spec, list) else [spec]
        for t in transitions:
            guard = t.get("guard")
            if guard is None:
                return t
            if cel.evaluate(guard, self.scope(owner, event)):
                return t
        return None

    def dispatch(self, event: Event) -> bool:
        """Run one RTC step. Returns whether the active-leaf config changed."""
        self.current_event = event
        found = self.find_handler(event)
        if found is None:
            if event.type in self.effective_defer_set():
                self.deferred.append(event)
            self.current_event = None
            return False
        owner, transition, leaf = found
        before = self.active_leaf_names()
        self.run_transition(owner, transition, event, leaf)
        self.current_event = None
        after = self.active_leaf_names()
        return before != after

    def run_transition(
        self, owner: State, transition: dict[str, Any], event: Event, leaf: State
    ) -> None:
        target_ref = transition.get("transition_to")
        actions = transition.get("action") or []
        if target_ref is None:
            # internal transition: actions only, no exit/entry (SPEC §5.5)
            self.run_actions(actions, owner, event)
            return
        target = self.machine.resolve_target(owner, target_ref)
        if transition.get("local"):
            lca = owner  # the containing composite is not exited/re-entered
        else:
            lca = self.machine.lca(owner, target)
        self.exit_to(leaf, lca)
        self.run_actions(actions, owner, event)
        self.enter_to(lca, target)

    # --- entry / exit (PSiCC ordering) -------------------------------------
    def exit_to(self, leaf: State, lca: State) -> None:
        chain: list[State] = []
        cur: State | None = leaf
        while cur is not None and cur is not lca:
            chain.append(cur)
            cur = cur.parent
        for s in chain:  # innermost first
            self.run_exit(s)
            self.destroy_esvs(s)
            self.config.discard(s.path)

    def enter_to(self, lca: State, target: State) -> None:
        path: list[State] = []
        cur: State | None = target
        while cur is not None and cur is not lca:
            path.append(cur)
            cur = cur.parent
        for s in reversed(path):  # outermost first
            self.config.add(s.path)
            self.init_esvs(s)
            self.run_entry(s)
        self.descend(target)

    def descend(self, state: State) -> None:
        if state.type == "composite":
            initial = state.raw["initial"]
            self.run_actions(initial.get("action") or [], state, self.current_event)
            tgt = self.machine.resolve_target(state, initial["transition_to"])
            self.config.add(tgt.path)
            self.init_esvs(tgt)
            self.run_entry(tgt)
            self.descend(tgt)
        elif state.type == "orthogonal":
            raise NotImplementedError("orthogonal regions (SPEC §5.6)")

    # --- defer + RTC step ---------------------------------------------------
    def step(self, event: Event) -> None:
        if self.dispatch(event):
            self._undefer()

    def _undefer(self) -> None:
        """Edge-triggered: on a config change, reinsert no-longer-deferred events
        at the front of the queue (SPEC §5.8)."""
        if not self.deferred:
            return
        current = self.effective_defer_set()
        still: deque[Event] = deque()
        moved: deque[Event] = deque()
        for ev in self.deferred:
            if ev.type in current:
                still.append(ev)
            else:
                moved.append(ev)
        self.deferred = still
        self.queue.extendleft(reversed(moved))
