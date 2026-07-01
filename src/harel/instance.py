"""Machine instance — the active object that runs one statechart (SPEC §3, §5).

Holds the state configuration, live esv values, FIFO event queue, deferred set,
and timers. ``dispatch`` runs one run-to-completion (RTC) step: find a handler
by searching from the active leaf up, then execute the transition (LCA +
exit/entry ordering per PSiCC, internal/local/external kinds, initial descent).
Extended-state lifetime (init on entry before entry actions, destroy on exit,
re-init on re-entry) and hierarchical scoping (inner shadows outer) live here.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from . import cel, values
from .cel import CelError
from .errors import HarelError
from .model import Machine, State

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .engine import Host

DELIVERABLE_RESERVED_EVENTS = frozenset({"env", "error", "done"})

_DURATION_UNITS = {"ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000}


def _duration_ms(duration: str) -> int:
    unit = duration[-2:] if duration.endswith("ms") else duration[-1:]
    n = int(duration[: -len(unit)])
    return n * _DURATION_UNITS[unit]


class Status(StrEnum):
    ACTIVE = "active"
    FAULTED = "faulted"
    TERMINATED = "terminated"


@dataclass
class Event:
    type: str
    payload: dict[str, Any] | None = None
    # For a timer firing: the (state_path, after-spec) to run as a transition.
    after: tuple[str, dict[str, Any]] | None = None


class Instance:
    def __init__(
        self,
        machine: Machine,
        id: str,
        parent_id: str | None,
        host: Host,
        external: dict[str, Any] | None = None,
        auto_enter: bool = True,
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
        self.history: dict[str, tuple[str, Any]] = {}
        self.external: dict[str, Any] = dict(external or {})
        self.current_event: Event | None = None
        self._pending_terminate = False
        self._last_target: str | None = None  # target of the last transition (§14)
        if auto_enter:
            self._enter_top()

    # --- creation -----------------------------------------------------------
    def _enter_top(self) -> None:
        top = self.machine.top
        self._enter_state(top)
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
    def _search_up(self, leaf: State, event: Event) -> tuple[State, dict[str, Any]] | None:
        """Find the first passing handler from ``leaf`` up the parent chain."""
        cur: State | None = leaf
        while cur is not None:
            spec = (cur.raw.get("on_events") or {}).get(event.type)
            if spec is not None:
                chosen = self._select(spec, cur, event)
                if chosen is not None:
                    return cur, chosen
            cur = cur.parent
        return None

    def _collect_enabled(self, event: Event) -> list[tuple[State, dict[str, Any]]]:
        """One enabled transition per active leaf, deduplicated by identity.

        Orthogonal regions are independent: an event is offered to every region
        in declared order. A handler reached at a shared ancestor (e.g. the
        orthogonal state's own ``done``) is found via multiple leaves but run
        once (dedup by transition object identity).
        """
        enabled: list[tuple[State, dict[str, Any]]] = []
        seen: set[int] = set()
        for leaf in sorted(self.active_leaves(), key=lambda s: s.order):
            found = self._search_up(leaf, event)
            if found is not None and id(found[1]) not in seen:
                seen.add(id(found[1]))
                enabled.append(found)
        return enabled

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
        if event.after is not None:
            return self._dispatch_after(event)
        enabled = self._collect_enabled(event)
        if not enabled:
            if event.type in self.effective_defer_set():
                self.deferred.append(event)
            self.current_event = None
            return False
        before = self.active_leaf_names()
        before_complete = self._complete_composites()
        for owner, transition in enabled:
            self.run_transition(owner, transition, event)
        self._completion(before_complete)
        self.current_event = None
        return before != self.active_leaf_names()

    def _dispatch_after(self, event: Event) -> bool:
        """Fire a due `after` timer as a transition owned by its state (§5.9)."""
        assert event.after is not None
        state_path, spec = event.after
        state = self.machine.by_path.get(state_path)
        self.current_event = event
        if state is None or state_path not in self.config:
            self.current_event = None
            return False  # stale timer (state exited)
        guard = spec.get("guard")
        if guard is not None and not cel.evaluate(guard, self.scope(state, event)):
            self.current_event = None
            return False
        before = self.active_leaf_names()
        before_complete = self._complete_composites()
        self.run_transition(state, spec, event)
        self._completion(before_complete)
        self.current_event = None
        return before != self.active_leaf_names()

    def _completion(self, before_complete: set[str]) -> None:
        """Enqueue `done` for newly-complete composites / flag termination."""
        for path in self._complete_composites() - before_complete:
            if path == "top":
                # top reached final: a spawned instance terminates (done -> its
                # parent); the root has no parent and rests in the final state.
                if self.parent_id is not None:
                    self._pending_terminate = True
            else:
                self.queue.append(Event("done", {"state": self._leaf_name(path)}))

    def run_transition(
        self, owner: State, transition: dict[str, Any], event: Event
    ) -> None:
        target_ref = transition.get("transition_to")
        actions = transition.get("action") or []
        if target_ref is None:
            # internal transition: actions only, no exit/entry (SPEC §5.5)
            self._last_target = None
            self.run_actions(actions, owner, event)
            return
        target = self.machine.resolve_target(owner, target_ref)
        if target.type == "choice":
            # Dynamic branching (§5.5.1): run the triggering action, then resolve the
            # choice chain in the SOURCE scope (branch guards see the just-assigned
            # esvs), then execute as an external transition to the real target.
            self.run_actions(actions, owner, event)
            target = self._resolve_choice(target, owner, event)
            self._last_target = target.name
            lca = self.machine.lca(owner, target)
            self.exit_states(owner, lca, False)
            self.enter_to(lca, target)
            return
        self._last_target = target.name
        local = bool(transition.get("local"))
        if local:
            lca = owner  # the containing composite is not exited/re-entered
        else:
            lca = self.machine.lca(owner, target)
        self.exit_states(owner, lca, local)
        self.run_actions(actions, owner, event)
        self.enter_to(lca, target)

    def _resolve_choice(self, node: State, owner: State, event: Event) -> State:
        """Resolve a choice pseudostate chain to a real target state (SPEC §5.5.1).

        Branches are tried in order (first passing guard, or the unguarded default);
        the chosen branch's action runs in the source scope; chained choices repeat.
        """
        seen: set[str] = set()
        while node.type == "choice":
            if node.path in seen:
                raise HarelError(f"cyclic choice '{node.name}'")
            seen.add(node.path)
            chosen: dict[str, Any] | None = None
            for br in node.raw.get("choice") or []:
                guard = br.get("guard")
                if guard is None or cel.evaluate(guard, self.scope(owner, event)):
                    chosen = br
                    break
            if chosen is None:
                raise HarelError(f"choice '{node.name}' has no matching branch")
            self.run_actions(chosen.get("action") or [], owner, event)
            node = self.machine.resolve_target(node, chosen["transition_to"])
        return node

    # --- completion ---------------------------------------------------------
    def _complete_composites(self) -> set[str]:
        """Active composite/orthogonal states whose region(s) all reached final."""
        out: set[str] = set()
        for path in self.config:
            s = self.machine.by_path[path]
            if self._state_complete(s):
                out.add(path)
        return out

    def _state_complete(self, state: State) -> bool:
        if state.type == "orthogonal":
            regions = state.raw.get("regions") or []
            return bool(regions) and all(
                self._region_final(state, i) for i in range(len(regions))
            )
        if state.type == "composite":
            leaf = self._composite_leaf(state)
            return leaf is not None and leaf.type == "final"
        return False

    def _composite_leaf(self, composite: State) -> State | None:
        for leaf in self.active_leaves():
            if self._descendant_or_self(leaf, composite):
                return leaf
        return None

    def _region_final(self, ortho: State, region_idx: int) -> bool:
        leaf = self._region_leaf(ortho, region_idx)
        return leaf is not None and leaf.type == "final"

    def _region_leaf(self, ortho: State, region_idx: int) -> State | None:
        for leaf in self.active_leaves():
            if leaf.region_index == region_idx and self._descendant_or_self(leaf, ortho):
                return leaf
        return None

    @staticmethod
    def _leaf_name(path: str) -> str:
        return path.rsplit(".", 1)[-1]

    # --- entry / exit (PSiCC ordering) -------------------------------------
    def exit_states(self, owner: State, lca: State, local: bool) -> None:
        """Exit the source-root subtree (confined to the owner's region), innermost
        first. For external transitions the exit root is the state just below the
        LCA on the owner's path; for local transitions it is the owner's proper
        descendants (the owner itself is not re-entered). History of any exited
        history-state is recorded first, while the configuration is intact.
        """
        to_exit = sorted(
            self._exit_set(owner, lca, local),
            key=lambda s: (-s.depth, s.order),
        )
        for s in to_exit:
            self._record_history(s)
        exited = {s.path for s in to_exit}
        for s in to_exit:  # deepest first
            self.run_exit(s)
            self.destroy_esvs(s)
            self.config.discard(s.path)
        # exiting a state cancels its outstanding timers (SPEC §5.9)
        if exited:
            self.timers = [t for t in self.timers if t["state_path"] not in exited]

    def _exit_set(self, owner: State, lca: State, local: bool) -> list[State]:
        states = [self.machine.by_path[p] for p in self.config]
        # When the transition is local, or its source *is* the LCA (a transition
        # owned by the root composite targeting one of its children), only the
        # LCA's proper descendants are exited — the LCA itself stays put.
        if local or owner is lca:
            return [s for s in states if self._strict_descendant(s, lca)]
        exit_root = self._source_root(owner, lca)
        return [s for s in states if self._descendant_or_self(s, exit_root)]

    @staticmethod
    def _source_root(owner: State, lca: State) -> State:
        """The state just below ``lca`` on the path to ``owner``."""
        cur: State = owner
        while cur.parent is not None and cur.parent is not lca:
            cur = cur.parent
        return cur

    @staticmethod
    def _descendant_or_self(state: State, root: State) -> bool:
        cur: State | None = state
        while cur is not None:
            if cur is root:
                return True
            cur = cur.parent
        return False

    @staticmethod
    def _strict_descendant(state: State, root: State) -> bool:
        return state is not root and Instance._descendant_or_self(state, root)

    def enter_to(self, lca: State, target: State) -> None:
        path: list[State] = []
        cur: State | None = target
        while cur is not None and cur is not lca:
            path.append(cur)
            cur = cur.parent
        for s in reversed(path):  # outermost first
            self._enter_state(s)
        self.descend(target)

    def descend(self, state: State) -> None:
        if state.type == "composite":
            if self._restore_history(state):
                return
            initial = state.raw["initial"]
            self.run_actions(initial.get("action") or [], state, self.current_event)
            tgt = self.machine.resolve_target(state, initial["transition_to"])
            self._enter_state(tgt)
            self.descend(tgt)
        elif state.type == "orthogonal":
            if self._restore_history(state):
                return
            for region in state.raw.get("regions") or []:
                initial = region["initial"]
                self.run_actions(initial.get("action") or [], state, self.current_event)
                tgt = self.machine.resolve_target(state, initial["transition_to"])
                self._enter_state(tgt)
                self.descend(tgt)

    def _enter_state(self, state: State) -> None:
        """Enter one state: activate, init esvs, run entry, arm `after` timers."""
        self.config.add(state.path)
        self.init_esvs(state)
        self.run_entry(state)
        self._arm_timers(state)

    def _arm_timers(self, state: State) -> None:
        for spec in state.raw.get("after") or []:
            self.timers.append(
                {
                    "fire_at": self.host.now + _duration_ms(spec["duration"]),
                    "state_path": state.path,
                    "spec": spec,
                    "seq": self.host.next_seq(),
                }
            )

    # --- history (SPEC §5.6) ------------------------------------------------
    def _record_history(self, state: State) -> None:
        kind = state.raw.get("history", "none")
        if kind == "none":
            return
        if kind == "deep":
            sub = [
                self.machine.by_path[p].path
                for p in self.config
                if self._strict_descendant(self.machine.by_path[p], state)
            ]
            self.history[state.path] = ("deep", sub)
        elif kind == "shallow":
            child = next(
                (
                    p
                    for p in self.config
                    if self.machine.by_path[p].parent is state
                ),
                None,
            )
            self.history[state.path] = ("shallow", child)

    def _restore_history(self, state: State) -> bool:
        """Re-enter a recorded configuration; return False to take the initial."""
        record = self.history.get(state.path)
        if record is None:
            return False
        kind, data = record
        if kind == "deep" and data:
            states = sorted(
                (self.machine.by_path[p] for p in data),
                key=lambda s: (s.depth, s.order),
            )
            for s in states:  # outermost first
                self.config.add(s.path)
                self.init_esvs(s)
                self.run_entry(s)
            return True
        if kind == "shallow" and data:
            child = self.machine.by_path[data]
            self._enter_state(child)
            self.descend(child)
            return True
        return False

    # --- defer + RTC step ---------------------------------------------------
    def step(self, event: Event) -> None:
        # An RTC step is atomic: if an action faults, abort and roll back (§5.10).
        snapshot = self._snapshot()
        try:
            changed = self.dispatch(event)
        except (CelError, HarelError) as exc:
            self._restore(snapshot)
            self._handle_fault(event, exc)
            return
        if changed:
            self._undefer()
        if self._pending_terminate:
            self._pending_terminate = False
            self.host.terminate(self)

    # --- faults (SPEC §5.10) ------------------------------------------------
    def _snapshot(self) -> dict[str, Any]:
        return {
            "config": set(self.config),
            "esvs": {p: dict(v) for p, v in self.esv_values.items()},
            "history": dict(self.history),
            "deferred": deque(self.deferred),
            "timers": list(self.timers),
            "pub": len(self.host.published),
            "sp": len(self.host.spawned),
            "instances": set(self.host.instances.keys()),
        }

    def _restore(self, snap: dict[str, Any]) -> None:
        self.config = set(snap["config"])
        self.esv_values = {p: dict(v) for p, v in snap["esvs"].items()}
        self.history = dict(snap["history"])
        self.deferred = deque(snap["deferred"])
        self.timers = list(snap["timers"])
        del self.host.published[snap["pub"]:]
        del self.host.spawned[snap["sp"]:]
        for iid in list(self.host.instances):
            if iid not in snap["instances"]:
                del self.host.instances[iid]

    def _handle_fault(self, event: Event, exc: Exception) -> None:
        self.dead_letter.append({"event": event.type, "error": str(exc)})
        log.warning("dead-letter instance=%s event=%s: %s", self.id, event.type, exc)
        error_event = Event("error", {"event": event.type, "error": str(exc)})
        # If some active state handles the reserved `error` event, dispatch it
        # (the instance recovers); otherwise the instance faults.
        self.current_event = error_event
        handled = bool(self._collect_enabled(error_event))
        self.current_event = None
        if not handled:
            log.warning("instance %s faulted: no handler for the error event", self.id)
            self.status = Status.FAULTED
            return
        snapshot = self._snapshot()
        try:
            changed = self.dispatch(error_event)
        except (CelError, HarelError):
            self._restore(snapshot)
            log.warning("instance %s faulted: error handler itself faulted", self.id)
            self.status = Status.FAULTED
            return
        if changed:
            self._undefer()
        if self._pending_terminate:
            self._pending_terminate = False
            self.host.terminate(self)

    # --- termination (SPEC §5.7) -------------------------------------------
    def to_snapshot(self) -> dict[str, Any]:
        """Serialize the instance (SPEC §8); JSON/YAML-representable."""
        return {
            "def_id": self.machine.id,
            "def_version": self.machine.version,
            "id": self.id,
            "parent_id": self.parent_id,
            "status": self.status.value,
            "state_config": sorted(self.config),
            "esvs": {p: dict(v) for p, v in self.esv_values.items()},
            "queue": [self._event_to_snap(e) for e in self.queue],
            "deferred": [self._event_to_snap(e) for e in self.deferred],
            "timers": [
                {"fire_at": t["fire_at"], "state_path": t["state_path"], "spec": t["spec"]}
                for t in self.timers
            ],
            "dead_letter": list(self.dead_letter),
            "history": {p: {"kind": k, "data": d} for p, (k, d) in self.history.items()},
        }

    def load_snapshot(self, snap: dict[str, Any]) -> None:
        self.status = Status(snap["status"])
        self.config = set(snap["state_config"])
        self.esv_values = {p: dict(v) for p, v in snap["esvs"].items()}
        self.queue = deque(self._snap_to_event(e) for e in snap["queue"])
        self.deferred = deque(self._snap_to_event(e) for e in snap["deferred"])
        self.timers = [dict(t) for t in snap["timers"]]
        self.dead_letter = list(snap["dead_letter"])
        self.history = {
            p: (rec["kind"], rec["data"]) for p, rec in snap["history"].items()
        }

    @staticmethod
    def _event_to_snap(event: Event) -> dict[str, Any]:
        out: dict[str, Any] = {"type": event.type, "payload": event.payload}
        if event.after is not None:
            state_path, spec = event.after
            out["after"] = [state_path, spec]
        return out

    @staticmethod
    def _snap_to_event(snap: dict[str, Any]) -> Event:
        after = None
        if snap.get("after") is not None:
            after = (snap["after"][0], snap["after"][1])
        return Event(snap["type"], snap.get("payload"), after=after)

    def terminate_exits(self) -> None:
        """Run exit actions up the active tree, innermost first."""
        states = sorted(
            (self.machine.by_path[p] for p in self.config),
            key=lambda s: (-s.depth, s.order),
        )
        for s in states:
            self.run_exit(s)
            self.destroy_esvs(s)
        self.config.clear()

    # --- external esvs / refresh (SPEC §5.4) --------------------------------
    def refresh_external(self, name: str, value: Any) -> None:
        """Adopt a host change into the in-scope external esv ``name``."""
        for path in self.config:
            s = self.machine.by_path[path]
            decl = (s.raw.get("esvs") or {}).get(name)
            if decl is not None and decl.get("external"):
                live = self.esv_values.get(path)
                if live is not None:
                    live[name] = value
                return
        raise HarelError(f"no external esv '{name}' to refresh")

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
