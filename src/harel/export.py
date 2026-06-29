"""Machine/instance visualization (SPEC §12, informative).

Exporters are pluggable by format; ``mermaid`` (``stateDiagram-v2``) is the
built-in default. Without a state config the static structure is rendered; with
one (from a snapshot/observer) the active leaves and their ancestors are
highlighted for current-state visualization.
"""

from __future__ import annotations

from .model import Machine, State


def export(
    machine: Machine,
    format: str = "mermaid",
    state_config: list[str] | None = None,
) -> str:
    """Render ``machine`` (optionally highlighting ``state_config``)."""
    if format != "mermaid":
        raise ValueError(f"unsupported export format: {format}")
    return _to_mermaid(machine, state_config)


def _to_mermaid(machine: Machine, state_config: list[str] | None) -> str:
    lines: list[str] = ["stateDiagram-v2"]
    _emit_state(machine, machine.top, lines, indent=1, in_root=True)
    if state_config:
        lines.append("  classDef active fill:#9f9,stroke:#3a3")
        for name in sorted(_active_names(machine, state_config)):
            lines.append(f"  class {name} active")
    return "\n".join(lines) + "\n"


def _emit_state(
    machine: Machine,
    state: State,
    lines: list[str],
    indent: int,
    in_root: bool,
) -> None:
    pad = "  " * indent
    composite = state.type in ("composite", "orthogonal")
    if in_root:
        # top is the diagram root; its initial and transitions emit at top level.
        _emit_initial(state, lines, indent)
        _emit_transitions(state, lines, indent)
        for child in state.children.values():
            _emit_state(machine, child, lines, indent, in_root=False)
        return
    if composite:
        lines.append(f"{pad}state {state.name} {{")
        _emit_initial(state, lines, indent + 1)
        _emit_transitions(state, lines, indent + 1)
        for child in state.children.values():
            _emit_state(machine, child, lines, indent + 1, in_root=False)
        if state.type == "orthogonal":
            regions = state.raw.get("regions") or []
            for _ in range(len(regions) - 1):
                lines.append(f"{pad}  --")
        lines.append(f"{pad}}}")
    else:
        _emit_transitions(state, lines, indent)
    if state.type == "final":
        lines.append(f"{pad}{state.name} --> [*]")


def _emit_initial(state: State, lines: list[str], indent: int) -> None:
    initial = state.raw.get("initial")
    if not isinstance(initial, dict):
        return
    target = _short(initial["transition_to"])
    label = _label(None, initial.get("guard"))
    pad = "  " * indent
    lines.append(f"{pad}[*] --> {target}{label}")


def _emit_transitions(state: State, lines: list[str], indent: int) -> None:
    pad = "  " * indent
    for event, spec in (state.raw.get("on_events") or {}).items():
        transitions = spec if isinstance(spec, list) else [spec]
        for t in transitions:
            target = t.get("transition_to")
            if target is None:
                continue  # internal transition: no edge
            lines.append(f"{pad}{state.name} --> {_short(target)}{_label(event, t.get('guard'))}")
    for after in state.raw.get("after") or []:
        target = after.get("transition_to")
        if target is None:
            continue
        lines.append(f"{pad}{state.name} --> {_short(target)} : after({after['duration']})")


def _label(event: str | None, guard: str | None) -> str:
    """Edge label `` : event [guard]`` (§12)."""
    text = event or ""
    if guard:
        text = f"{text} [{guard}]" if text else f"[{guard}]"
    return f" : {text}" if text else ""


def _short(ref: str) -> str:
    """A transition_to ref -> the final (leaf-most) component name."""
    return ref.split(".")[-1]


def _active_names(machine: Machine, state_config: list[str]) -> set[str]:
    """Names of the active leaves and their ancestors (excluding the root `top`)."""
    names: set[str] = set()
    for path in state_config:
        cur = machine.by_path.get(path)
        while cur is not None and cur.parent is not None:
            names.add(cur.name)
            cur = cur.parent
    return names
