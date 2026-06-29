"""Host — owns machine instances and the adapters (SPEC §5.7, §8).

The host registers definitions, creates the root instance, validates/delivers
events, and runs all instances to quiescence. Bus / queue / clock / store are
adapters with simple in-memory defaults; active-object spawning and the bus are
wired in later build steps. For now the host drives a single instance tree and
records published/spawned events for the conformance harness.
"""

from __future__ import annotations

from typing import Any

from . import cel, values
from .definition import Definition
from .instance import DELIVERABLE_RESERVED_EVENTS, Event, Instance, Status
from .model import Machine


class Host:
    def __init__(self) -> None:
        self.machines: dict[str, Machine] = {}
        self.instances: dict[str, Instance] = {}
        self.published: list[str] = []  # event names handed to the bus, in order
        self.spawned: list[str] = []  # child defIds, in order
        self._spawn_counters: dict[str, int] = {}
        self.now: int = 0  # virtual clock, in milliseconds (SPEC §5.9)
        self._seq: int = 0

    # --- registration / creation -------------------------------------------
    def register(self, definition: Definition) -> Machine:
        machine = Machine(definition)
        self.machines[machine.id] = machine
        return machine

    def register_all(self, definitions: list[Definition]) -> None:
        for d in definitions:
            self.register(d)

    def create_root(
        self,
        machine: Machine,
        id: str,
        external: dict[str, Any] | None = None,
    ) -> Instance:
        inst = Instance(machine, id, None, self, external)
        self.instances[id] = inst
        return inst

    # --- event delivery -----------------------------------------------------
    def validate_event(
        self, machine: Machine, event_type: str, payload: dict[str, Any] | None
    ) -> tuple[bool, str | None]:
        if event_type in DELIVERABLE_RESERVED_EVENTS:
            return True, None
        events = machine.definition.raw.get("events") or {}
        if event_type not in events:
            return False, f"undeclared event '{event_type}'"
        errs = values.payload_errors(events[event_type], payload)
        if errs:
            return False, "; ".join(errs)
        return True, None

    def deliver(
        self,
        instance_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> bool:
        """Validate and enqueue an event; return False if rejected (§4.3)."""
        inst = self.instances[instance_id]
        ok, _reason = self.validate_event(inst.machine, event_type, payload)
        if not ok:
            return False
        inst.queue.append(Event(event_type, payload))
        return True

    # --- execution ----------------------------------------------------------
    def next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def advance(self, duration: str) -> None:
        """Advance the virtual clock, enqueueing due `after` timers (§5.9)."""
        from .instance import _duration_ms

        self.now += _duration_ms(duration)
        due: list[tuple[Instance, dict[str, Any]]] = []
        for inst in self.instances.values():
            if inst.status is not Status.ACTIVE:
                continue
            for timer in inst.timers:
                if timer["fire_at"] <= self.now:
                    due.append((inst, timer))
        due.sort(key=lambda pair: (pair[1]["fire_at"], pair[1]["seq"]))
        for inst, timer in due:
            if timer in inst.timers:
                inst.timers.remove(timer)
            if (
                inst.status is Status.ACTIVE
                and timer["state_path"] in inst.config
            ):
                inst.queue.append(
                    Event("__time__", after=(timer["state_path"], timer["spec"]))
                )

    def run_to_quiescence(self) -> None:
        progress = True
        while progress:
            progress = False
            for inst in list(self.instances.values()):
                if inst.status is not Status.ACTIVE:
                    continue
                while inst.queue:
                    ev = inst.queue.popleft()
                    inst.step(ev)
                    progress = True

    # --- structured-action hooks (SPEC §6) ----------------------------------
    def spawn_action(
        self,
        parent: Instance,
        spec: dict[str, Any],
        root: Any,
        event: Event | None,
    ) -> None:
        def_id = spec["def"]
        machine = self.machines[def_id]
        n = self._spawn_counters[parent.id] = self._spawn_counters.get(parent.id, 0) + 1
        child_id = f"{parent.id}/{n}"
        external: dict[str, Any] | None = None
        if "payload" in spec:
            scope = parent.scope(root, event)
            external = {k: cel.evaluate(v, scope) for k, v in spec["payload"].items()}
        child = Instance(machine, child_id, parent.id, self, external=external)
        self.instances[child_id] = child
        self.spawned.append(def_id)
        result = spec.get("result")
        if result:
            parent.assign_esv(root, result, child_id)

    def publish(
        self,
        src: Instance,
        spec: dict[str, Any],
        root: Any,
        event: Event | None,
    ) -> None:
        name = spec["event"]
        scope = src.scope(root, event)
        payload = {
            k: cel.evaluate(v, scope) for k, v in (spec.get("payload") or {}).items()
        }
        self.published.append(name)
        if "to" in spec:
            target = cel.evaluate(spec["to"], scope)
            ids = target if isinstance(target, list) else [target]
            for tid in ids:
                tid = str(tid)
                tgt = self.instances.get(tid)
                if tgt is not None and tgt.status is Status.ACTIVE:
                    tgt.queue.append(Event(name, payload))
        else:
            self._undirected(src, name, payload)

    def _undirected(self, src: Instance, name: str, payload: dict[str, Any]) -> None:
        scope_kind = self._event_scope(src.machine, name)
        if scope_kind == "internal":
            src.queue.append(Event(name, payload))
            return
        if scope_kind == "local":
            candidate_ids = self._tree_ids(src.id)
        else:  # global
            candidate_ids = list(self.instances.keys())
        for tid in candidate_ids:
            t = self.instances.get(tid)
            if t is None or t.status is not Status.ACTIVE:
                continue
            subs = t.machine.definition.raw.get("subscribe") or []
            if name in subs:
                t.queue.append(Event(name, payload))

    def _event_scope(self, machine: Machine, name: str) -> str:
        decl = (machine.definition.raw.get("events") or {}).get(name)
        if isinstance(decl, dict):
            scope = decl.get("scope", "internal")
            return scope if isinstance(scope, str) else "internal"
        return "internal"

    def _tree_ids(self, root_id: str) -> list[str]:
        out = [root_id]
        out.extend(
            iid
            for iid, inst in self.instances.items()
            if iid != root_id and self._under_root(iid, root_id)
        )
        return out

    def _under_root(self, iid: str, root_id: str) -> bool:
        inst = self.instances.get(iid)
        cur = inst.parent_id if inst else None
        while cur is not None:
            if cur == root_id:
                return True
            parent = self.instances.get(cur) if cur else None
            cur = parent.parent_id if parent else None
        return False

    def refresh(
        self, inst: Instance, spec: dict[str, Any], event: Event | None
    ) -> None:
        if event is None or event.type != "env":
            raise ValueError("refresh is only valid while handling an env event")
        changed = ((event.payload or {}).get("changed")) or {}
        only = spec.get("only")
        names = only if only is not None else list(changed.keys())
        for nm in names:
            if nm in changed:
                inst.refresh_external(nm, changed[nm])

    def stop(self, inst: Instance) -> None:
        inst._pending_terminate = True

    # --- termination (SPEC §5.7) -------------------------------------------
    def terminate(self, inst: Instance) -> None:
        if inst.status is Status.TERMINATED:
            return
        for child in [
            i
            for i in self.instances.values()
            if i.parent_id == inst.id and i.status is Status.ACTIVE
        ]:
            self.terminate(child)
        inst.terminate_exits()
        if inst.parent_id and inst.parent_id in self.instances:
            parent = self.instances[inst.parent_id]
            if parent.status is Status.ACTIVE:
                parent.queue.append(Event("done", {"instance": inst.id}))
        inst.status = Status.TERMINATED
        inst.queue.clear()
