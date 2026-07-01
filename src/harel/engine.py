"""Host — owns machine instances and the adapters (SPEC §5.7, §8).

The host registers definitions, creates the root instance, validates/delivers
events, and runs all instances to quiescence. Bus / queue / clock / store are
adapters with simple in-memory defaults; active-object spawning and the bus are
wired in later build steps. For now the host drives a single instance tree and
records published/spawned events for the conformance harness.
"""

from __future__ import annotations

import logging
from typing import Any

from . import cel, values
from .definition import Definition
from .instance import DELIVERABLE_RESERVED_EVENTS, Event, Instance, Status
from .model import Machine, inline_submachines
from .observer import Observer

log = logging.getLogger(__name__)


class Host:
    def __init__(self, observer: Observer | None = None) -> None:
        self.machines: dict[str, Machine] = {}
        self.versions: dict[tuple[str, int], Machine] = {}
        self.instances: dict[str, Instance] = {}
        self.published: list[str] = []  # event names handed to the bus, in order
        self.spawned: list[str] = []  # child defIds, in order
        self._spawn_counters: dict[str, int] = {}
        # Passive per-step observer (SPEC §8); None = no-op.
        self.observer: Observer | None = observer
        self.now: int = 0  # virtual clock, in milliseconds (SPEC §5.9)
        self.mode: str = "auto"  # processing mode, auto|manual (SPEC §14)
        self._seq: int = 0

    # --- registration / creation -------------------------------------------
    def register(self, definition: Definition) -> Machine:
        registry = {mid: m.definition.top for mid, m in self.machines.items()}
        registry[definition.id] = definition.top
        return self._register(definition, registry)

    def register_all(self, definitions: list[Definition]) -> None:
        # Two-phase so submachine references can resolve in any order within the batch.
        registry = {mid: m.definition.top for mid, m in self.machines.items()}
        for d in definitions:
            registry[d.id] = d.top
        for d in definitions:
            self._register(d, registry)

    def _register(self, definition: Definition, registry: dict[str, Any]) -> Machine:
        top = inline_submachines(definition.top, registry)
        machine = Machine(definition, top_override=top)
        self.machines[machine.id] = machine
        self.versions[(machine.id, machine.version)] = machine
        return machine

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
        return self.inject(instance_id, event_type, payload)

    def inject(
        self,
        instance_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> bool:
        """Validate and enqueue without processing, in either mode (SPEC §14)."""
        inst = self.instances[instance_id]
        ok, _reason = self.validate_event(inst.machine, event_type, payload)
        if not ok:
            return False
        inst.queue.append(Event(event_type, payload))
        return True

    # --- execution ----------------------------------------------------------
    def maybe_run(self) -> None:
        """Run all instances to quiescence in auto mode only (SPEC §14)."""
        if self.mode == "auto":
            self.run_to_quiescence()

    def step(self, instance: Instance | str, n: int = 1) -> list[dict[str, Any]]:
        """Process exactly ``n`` RTC steps of one instance (SPEC §14).

        Returns one per-step record per step taken:
        ``{ event, transition, entered, exited, published, spawned, faulted }``.
        Stops early if the instance faults or its queue drains.
        """
        inst = instance if isinstance(instance, Instance) else self.instances[instance]
        records: list[dict[str, Any]] = []
        for _ in range(n):
            if inst.status is not Status.ACTIVE or not inst.queue:
                break
            records.append(self._run_one_step(inst))
        return records

    def _run_one_step(self, inst: Instance) -> dict[str, Any]:
        """Dequeue and process one event; build the per-step record and notify the
        observer (SPEC §8/§14). The caller guarantees ``inst.queue`` is non-empty."""
        ev = inst.queue.popleft()
        before = set(inst.active_leaf_names())
        pub_before = len(self.published)
        sp_before = len(self.spawned)
        inst._last_target = None  # noqa: SLF001
        inst.step(ev)
        after = set(inst.active_leaf_names())
        record = {
            "event": ev.type,
            "transition": inst._last_target,  # noqa: SLF001
            "entered": sorted(after - before),
            "exited": sorted(before - after),
            "published": list(self.published[pub_before:]),
            "spawned": list(self.spawned[sp_before:]),
            "faulted": inst.status is Status.FAULTED,
        }
        if self.observer is not None:
            self.observer({"instance": inst.id, **record})
        log.debug(
            "dispatch instance=%s event=%s transition=%s entered=%s exited=%s",
            inst.id, record["event"], record["transition"], record["entered"], record["exited"],
        )
        return record

    def inspect(self, instance: Instance | str) -> dict[str, Any]:
        """Full internal state for debugging, beyond ``state`` (SPEC §14)."""
        inst = instance if isinstance(instance, Instance) else self.instances[instance]
        out: dict[str, Any] = {
            "status": inst.status.value,
            "config": inst.active_leaf_names(),
            "esvs": inst.resolved_esvs(),
            "enabled": inst.enabled_events(),
            "queue": [Instance._event_to_snap(e) for e in inst.queue],
            "deferred": [Instance._event_to_snap(e) for e in inst.deferred],
            "timers": [
                {"fire_at": t["fire_at"], "state_path": t["state_path"], "spec": t["spec"]}
                for t in inst.timers
            ],
            "history": {
                p: {"kind": k, "data": d} for p, (k, d) in inst.history.items()
            },
        }
        if inst.dead_letter:
            out["dead_letter"] = list(inst.dead_letter)
        return out

    def enabled_events(self, instance: Instance | str) -> list[str]:
        """Sorted declared event types the active configuration can handle (§14)."""
        inst = instance if isinstance(instance, Instance) else self.instances[instance]
        return inst.enabled_events()

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
                log.debug(
                    "timer fired instance=%s state=%s after=%s",
                    inst.id, timer["state_path"], timer["spec"],
                )

    def run_to_quiescence(self) -> None:
        progress = True
        while progress:
            progress = False
            for inst in list(self.instances.values()):
                if inst.status is not Status.ACTIVE:
                    continue
                while inst.queue:
                    self._run_one_step(inst)
                    progress = True

    # --- snapshot round-trip (SPEC §8) -------------------------------------
    def snapshot_all(self) -> list[dict[str, Any]]:
        return [inst.to_snapshot() for inst in self.instances.values()]

    def restore_all(self, snapshots: list[dict[str, Any]]) -> None:
        new_instances: dict[str, Instance] = {}
        for snap in snapshots:
            machine = self.versions.get(
                (snap["def_id"], snap["def_version"])
            ) or self.machines[snap["def_id"]]
            inst = Instance(
                machine, snap["id"], snap["parent_id"], self, auto_enter=False
            )
            inst.load_snapshot(snap)
            new_instances[snap["id"]] = inst
        self.instances = new_instances

    # --- versioning / migration (SPEC §10) ---------------------------------
    def upgrade(self, target_version: int, root_def_id: str | None = None) -> None:
        """Register a newer definition version and migrate eligible instances."""
        if root_def_id is None:
            root_def_id = next(iter(self.machines)) if self.machines else None
        if root_def_id is None:
            return
        new_machine = self.versions.get((root_def_id, target_version))
        if new_machine is None:
            return
        self.machines[root_def_id] = new_machine
        for inst in list(self.instances.values()):
            if (
                inst.machine.id == root_def_id
                and inst.machine.version < target_version
                and inst.status is Status.ACTIVE
            ):
                self._try_migrate(inst, new_machine)

    def _try_migrate(self, inst: Instance, new_machine: Machine) -> None:
        migrations = new_machine.definition.raw.get("migrations") or []
        mig = next(
            (
                m
                for m in migrations
                if m.get("from") == inst.machine.version
                and m.get("to") == new_machine.version
            ),
            None,
        )
        if mig is None:
            return
        # only at a safe point: quiescent (empty queue + deferred)
        if inst.queue or inst.deferred:
            return
        leaves = inst.active_leaves()
        state_binding = leaves[0].name if len(leaves) == 1 else [lf.name for lf in leaves]
        when = mig.get("when")
        if when is not None and not cel.evaluate(when, {"state": state_binding}):
            return
        state_map = mig.get("state_map") or {}
        if any(leaf.name not in state_map for leaf in leaves):
            return
        # remap the configuration onto the new machine
        inst.machine = new_machine
        inst.config = self._remap_config(new_machine, leaves, state_map)
        # transform esvs (carried over; actions run against the live scope)
        esv_actions = mig.get("esvs") or []
        if esv_actions:
            inst.run_actions(esv_actions, new_machine.top, None)

    def _remap_config(
        self,
        new_machine: Machine,
        old_leaves: list[Any],
        state_map: dict[str, str],
    ) -> set[str]:
        config: set[str] = set()
        config.add(new_machine.top.path)
        for leaf in old_leaves:
            new_name = state_map[leaf.name]
            target = new_machine.find_by_name(new_name)
            if target is None:
                continue
            cur: Any = target
            while cur is not None:
                config.add(cur.path)
                cur = cur.parent
        return config

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
        log.debug("spawn parent=%s child=%s def=%s", parent.id, child_id, def_id)
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
        log.debug("publish event=%s from=%s", name, src.id)
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
