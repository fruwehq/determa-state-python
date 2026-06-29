"""Host — owns machine instances and the adapters (SPEC §5.7, §8).

The host registers definitions, creates the root instance, validates/delivers
events, and runs all instances to quiescence. Bus / queue / clock / store are
adapters with simple in-memory defaults; active-object spawning and the bus are
wired in later build steps. For now the host drives a single instance tree and
records published/spawned events for the conformance harness.
"""

from __future__ import annotations

from typing import Any

from . import values
from .definition import Definition
from .instance import DELIVERABLE_RESERVED_EVENTS, Event, Instance, Status
from .model import Machine


class Host:
    def __init__(self) -> None:
        self.machines: dict[str, Machine] = {}
        self.instances: dict[str, Instance] = {}
        self.published: list[str] = []  # event names handed to the bus, in order
        self.spawned: list[str] = []  # child defIds, in order

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

    # --- structured-action hooks (later build steps) -----------------------
    def publish(
        self, inst: Instance, spec: dict[str, Any], root: Any, event: Event | None
    ) -> None:
        raise NotImplementedError("publish / bus (SPEC §5.7)")

    def spawn_action(
        self, inst: Instance, spec: dict[str, Any], root: Any, event: Event | None
    ) -> None:
        raise NotImplementedError("spawn (SPEC §5.7)")

    def refresh(
        self, inst: Instance, spec: dict[str, Any], event: Event | None
    ) -> None:
        raise NotImplementedError("external esvs / refresh (SPEC §5.4)")

    def stop(self, inst: Instance) -> None:
        raise NotImplementedError("stop / termination (SPEC §5.7)")
