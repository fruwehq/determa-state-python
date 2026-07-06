"""Standard CLI (SPEC §13).

Every implementation exposes the same command surface so operators and tests
interact with any language's engine identically. State persists in a
file-backed store; a state-changing command loads the affected instances, runs
all to quiescence, and persists. Diagnostics go to stderr; the result to stdout.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, cast

from . import collect_errors, load_definitions
from . import export as export_mod
from .contracts import load_contract, validate_contracts
from .engine import Host
from .errors import DetermaError
from .instance import Instance, Status
from .model import Machine
from .store import Store, StoreState, open_store

# Exit codes (SPEC §13.2).
EXIT_OK = 0
EXIT_OTHER = 1
EXIT_USAGE = 2
EXIT_VALIDATION = 3
EXIT_NOT_FOUND = 4
EXIT_FAULTED = 5


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    store_dir = args.store or os.environ.get("DETERMA_STORE", "./.determa")
    try:
        return int(args.cmd(args, open_store(store_dir)))
    except DetermaError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_OTHER


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="determa-state",
        description="Determa State statechart engine",
        formatter_class=_GroupedHelpFormatter,
        epilog=(
            "store specification (--store / DETERMA_STORE):\n"
            "  file:<dir>    portable snapshot files (the default, ./.determa)\n"
            "  mem:          in-memory, ephemeral\n"
            "  sqlite:<path> a single-file database\n"
            "examples:\n"
            "  determa-state --store mem: new t1 machine.yaml\n"
            "  determa-state --store sqlite:./state.db list --json"
        ),
    )
    p.add_argument(
        "--store",
        default=None,
        help="store spec: file:<dir> | mem: | sqlite:<path> (default ./.determa or $DETERMA_STORE)",
    )
    p.add_argument("--version", action="version", version=f"determa-state {_pkg_version()}")
    sub = p.add_subparsers(dest="command", required=True, metavar="<command>")

    # `--json` is accepted per-subcommand (after the positionals).
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--json", action="store_true", help="machine-readable output")

    def add(cmd: str, group: str, desc: str, example: str, **kw: Any) -> argparse.ArgumentParser:
        sp = sub.add_parser(
            cmd,
            parents=[common],
            help=desc,
            description=desc,
            epilog=f"example:\n  {example}",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            **kw,
        )
        sp._cli_group = group  # type: ignore[attr-defined]
        sp._cli_help = desc  # type: ignore[attr-defined]
        return sp

    v = add("validate", "Authoring", "validate a machine definition file",
            "determa-state validate machine.yaml")
    v.add_argument("machine")
    v.set_defaults(cmd=cmd_validate)

    e = add("export", "Authoring", "render a machine to a diagram",
            "determa-state export machine.yaml --format mermaid")
    e.add_argument("machine")
    e.add_argument("--format", default="mermaid", choices=["mermaid"],
                   help="output format (currently only 'mermaid')")
    e.add_argument("--state", default=None, help="instance id whose active config to highlight")
    e.set_defaults(cmd=cmd_export)

    n = add("new", "Instances", "create a new instance from a machine",
            "determa-state new t1 machine.yaml --external token=abc")
    n.add_argument("id")
    n.add_argument("machine")
    n.add_argument("--external", action="append", default=[],
                   help="seed an external esv: k=v (repeatable)")
    n.set_defaults(cmd=cmd_new)

    s = add("send", "Instances", "deliver an event to an instance",
            "determa-state send t1 coin --payload amount=100")
    s.add_argument("instance")
    s.add_argument("event")
    s.add_argument("--payload", action="append", default=[], help="event field k=v (repeatable)")
    s.add_argument("--payload-json", default=None, help="whole payload as one JSON object")
    s.set_defaults(cmd=cmd_send)

    a = add("advance", "Instances", "advance the virtual clock by a duration",
            "determa-state advance 5s")
    a.add_argument("duration", help="e.g. 500ms, 5s, 2m")
    a.set_defaults(cmd=cmd_advance)

    env = add("env", "Instances", "notify an instance of environment changes",
              "determa-state env t1 --changed level=high")
    env.add_argument("instance")
    env.add_argument("--changed", required=True, help="comma-separated k=v pairs")
    env.set_defaults(cmd=cmd_env)

    st = add("state", "Instances", "print an instance's current state",
             "determa-state state t1 --json")
    st.add_argument("instance")
    st.set_defaults(cmd=cmd_state)

    en = add("enabled", "Instances", "list events an instance can currently handle",
             "determa-state enabled t1")
    en.add_argument("instance")
    en.set_defaults(cmd=cmd_enabled)

    ip = add("inspect", "Instances", "show full internal state for debugging",
             "determa-state inspect t1 --json")
    ip.add_argument("instance")
    ip.set_defaults(cmd=cmd_inspect)

    md = add("mode", "Stepping", "get or set auto vs manual processing mode",
             "determa-state mode manual")
    md.add_argument("mode", nargs="?", choices=["auto", "manual"])
    md.set_defaults(cmd=cmd_mode)

    ij = add("inject", "Stepping", "enqueue an event without processing (manual mode)",
             "determa-state inject t1 coin --payload amount=100")
    ij.add_argument("instance")
    ij.add_argument("event")
    ij.add_argument("--payload", action="append", default=[], help="event field k=v (repeatable)")
    ij.add_argument("--payload-json", default=None, help="whole payload as one JSON object")
    ij.set_defaults(cmd=cmd_inject)

    sp = add("step", "Stepping", "process N RTC steps (manual mode)",
             "determa-state step t1 --steps 1")
    sp.add_argument("instance")
    sp.add_argument("--steps", type=int, default=1, help="number of RTC steps (default 1)")
    sp.set_defaults(cmd=cmd_step)

    snap = add("snapshot", "Persistence", "serialize an instance to a snapshot",
               "determa-state snapshot t1 > t1.json")
    snap.add_argument("instance")
    snap.set_defaults(cmd=cmd_snapshot)

    r = add("restore", "Persistence", "recreate an instance from a snapshot",
            "determa-state restore t1.json")
    r.add_argument("snapshot")
    r.set_defaults(cmd=cmd_restore)

    ls = add("list", "Persistence", "list all instances",
             "determa-state list --json")
    ls.set_defaults(cmd=cmd_list)

    run = add("run", "Batch", "drive many commands from NDJSON stdin (§13.7)",
              "echo '[\"new\",\"t1\",\"machine.yaml\"]' | determa-state run -")
    run.add_argument("source", nargs="?", default="-", help="'-' for stdin, or an NDJSON file")
    run.set_defaults(cmd=cmd_run)
    return p


# Command groups, shown gcloud-style in --help (alphabetical within each group).
_GROUP_ORDER = ["Authoring", "Instances", "Stepping", "Persistence", "Batch"]


class _GroupedHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Renders the subcommand list grouped by category (gcloud-style) in top-level help."""

    def _format_action(self, action: argparse.Action) -> str:
        if not isinstance(action, argparse._SubParsersAction):
            return super()._format_action(action)
        groups: dict[str, list[tuple[str, argparse.ArgumentParser]]] = {}
        for name, parser in action.choices.items():
            groups.setdefault(getattr(parser, "_cli_group", "Commands"), []).append(
                (name, parser)
            )
        width = max((len(n) for items in groups.values() for n, _ in items), default=0)
        lines: list[str] = []
        ordered = _GROUP_ORDER + sorted(g for g in groups if g not in _GROUP_ORDER)
        for group_name in ordered:
            items = groups.get(group_name)
            if not items:
                continue
            lines.append(f"  {group_name}:")
            for name, parser in sorted(items):
                lines.append(f"    {name:<{width}}  {getattr(parser, '_cli_help', '')}")
        return "\n".join(lines) + "\n"


# --- host (de)serialization -------------------------------------------------
def _build_host(state: StoreState) -> Host:
    host = Host()
    host.now = state.now
    host.mode = state.mode
    host._spawn_counters = dict(state.spawn_counters)  # noqa: SLF001
    for text in state.defs.values():
        host.register_all(load_definitions(text))
    host.restore_all(state.instances)
    return host


def _persist(store: Store, state: StoreState, host: Host) -> None:
    state.instances = host.snapshot_all()
    state.now = host.now
    state.mode = host.mode
    state.spawn_counters = dict(host._spawn_counters)  # noqa: SLF001
    store.save(state)


def _resolve_machine_path(arg: str) -> Path:
    return Path(arg)


# --- commands ---------------------------------------------------------------
def cmd_validate(args: argparse.Namespace, store: Store) -> int:
    raw_text = _resolve_machine_path(args.machine).read_text(encoding="utf-8")
    defs = load_definitions(raw_text)
    root = defs[0]
    errors = list(collect_errors(root.raw))
    cdir = _resolve_machine_path(args.machine).parent / "contracts"
    if cdir.exists():
        contracts = {}
        for cf in sorted(cdir.glob("*.yaml")):
            c = load_contract(cf.read_text(encoding="utf-8"))
            contracts[c["id"]] = c
        errors.extend(validate_contracts(root.raw, contracts))
    valid = not errors
    if args.json:
        print(
            json.dumps(
                {
                    "valid": valid,
                    "errors": [{"path": e["path"], "message": e["message"]} for e in errors],
                }
            )
        )
    elif not valid:
        for e in errors:
            print(f"{e['path']}: {e['message']}", file=sys.stderr)
    return EXIT_OK if valid else EXIT_VALIDATION


def cmd_new(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    if any(s["id"] == args.id for s in state.instances):
        print(f"instance '{args.id}' already exists", file=sys.stderr)
        return EXIT_USAGE
    text = _resolve_machine_path(args.machine).read_text(encoding="utf-8")
    defs = load_definitions(text)
    key = f"{defs[0].id}@{defs[0].version}"
    state.defs[key] = text
    host = _build_host(state)
    external = _parse_kv(args.external, _external_types(host.machines[defs[0].id]))
    host.create_root(host.machines[defs[0].id], args.id, external=external)
    host.run_to_quiescence()
    inst = host.instances[args.id]
    _print_state(args, host, inst)
    _persist(store, state, host)
    return EXIT_OK


def cmd_send(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    host = _build_host(state)
    inst = host.instances.get(args.instance)
    if inst is None:
        print(f"no such instance: {args.instance}", file=sys.stderr)
        return EXIT_NOT_FOUND
    payload = _build_payload(args, inst.machine)
    before = len(host.published)
    if not host.deliver(args.instance, args.event, payload):
        print(f"rejected: {args.event}", file=sys.stderr)
        return EXIT_VALIDATION
    host.maybe_run()
    if args.json:
        obj = _state_json(host, host.instances[args.instance])
        obj["published"] = host.published[before:]
        print(json.dumps(obj))
    _persist(store, state, host)
    inst = host.instances.get(args.instance)
    if inst is not None and inst.status is Status.FAULTED:
        return EXIT_FAULTED
    return EXIT_OK


def cmd_advance(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    host = _build_host(state)
    host.advance(args.duration)
    host.maybe_run()
    if args.json:
        print(json.dumps({"now": host.now}))
    _persist(store, state, host)
    return EXIT_OK


def cmd_env(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    host = _build_host(state)
    if args.instance not in host.instances:
        print(f"no such instance: {args.instance}", file=sys.stderr)
        return EXIT_NOT_FOUND
    changed = _parse_csv_kv(args.changed)
    host.deliver(args.instance, "env", {"changed": changed})
    host.maybe_run()
    _print_state(args, host, host.instances[args.instance])
    _persist(store, state, host)
    return EXIT_OK


def cmd_state(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    host = _build_host(state)
    inst = host.instances.get(args.instance)
    if inst is None:
        print(f"no such instance: {args.instance}", file=sys.stderr)
        return EXIT_NOT_FOUND
    _print_state(args, host, inst)
    return EXIT_OK


def cmd_list(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    host = _build_host(state)
    if args.json:
        rows = [
            {
                "id": i.id,
                "def": f"{i.machine.id}@{i.machine.version}",
                "parent": i.parent_id,
                "status": i.status.value,
                "config": i.active_leaf_names(),
            }
            for i in sorted(host.instances.values(), key=lambda x: x.id)
        ]
        print(json.dumps(rows))
    else:
        for i in sorted(host.instances.values(), key=lambda x: x.id):
            print(f"{i.id}\t{i.status.value}\t{i.active_leaf_names()}")
    return EXIT_OK


def cmd_snapshot(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    host = _build_host(state)
    inst = host.instances.get(args.instance)
    if inst is None:
        print(f"no such instance: {args.instance}", file=sys.stderr)
        return EXIT_NOT_FOUND
    print(json.dumps(inst.to_snapshot()))
    return EXIT_OK


def cmd_restore(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    snap = json.loads(_resolve_machine_path(args.snapshot).read_text(encoding="utf-8"))
    host = _build_host(state)
    machine = host.versions.get((snap["def_id"], snap["def_version"]))
    if machine is None:
        print(f"unknown definition: {snap['def_id']}@{snap['def_version']}", file=sys.stderr)
        return EXIT_NOT_FOUND
    inst = Instance(machine, snap["id"], snap["parent_id"], host, auto_enter=False)
    inst.load_snapshot(snap)
    host.instances[snap["id"]] = inst
    _persist(store, state, host)
    return EXIT_OK


def cmd_export(args: argparse.Namespace, store: Store) -> int:
    defs = load_definitions(_resolve_machine_path(args.machine).read_text(encoding="utf-8"))
    machine = Machine(defs[0])
    state_config = None
    if args.state:
        st = store.load()
        host = _build_host(st)
        inst = host.instances.get(args.state)
        if inst is None:
            print(f"no such instance: {args.state}", file=sys.stderr)
            return EXIT_NOT_FOUND
        state_config = sorted(inst.config)
    print(export_mod.export(machine, format=args.format, state_config=state_config))
    return EXIT_OK


# --- introspection & stepping (SPEC §14) ------------------------------------
def cmd_mode(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    if args.mode is not None:
        state.mode = args.mode
        store.save(state)
    if args.json:
        print(json.dumps({"mode": state.mode}))
    else:
        print(state.mode)
    return EXIT_OK


def cmd_inject(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    host = _build_host(state)
    inst = host.instances.get(args.instance)
    if inst is None:
        print(f"no such instance: {args.instance}", file=sys.stderr)
        return EXIT_NOT_FOUND
    payload = _build_payload(args, inst.machine)
    if not host.inject(args.instance, args.event, payload):
        print(f"rejected: {args.event}", file=sys.stderr)
        return EXIT_VALIDATION
    _print_state(args, host, inst)
    _persist(store, state, host)
    return EXIT_OK


def cmd_step(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    host = _build_host(state)
    inst = host.instances.get(args.instance)
    if inst is None:
        print(f"no such instance: {args.instance}", file=sys.stderr)
        return EXIT_NOT_FOUND
    records = host.step(inst, args.steps)
    if args.json:
        obj = _state_json(host, inst)
        obj["steps"] = records
        print(json.dumps(obj))
    _persist(store, state, host)
    if inst.status is Status.FAULTED:
        return EXIT_FAULTED
    return EXIT_OK


def cmd_enabled(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    host = _build_host(state)
    inst = host.instances.get(args.instance)
    if inst is None:
        print(f"no such instance: {args.instance}", file=sys.stderr)
        return EXIT_NOT_FOUND
    enabled = host.enabled_events(inst)
    if args.json:
        print(json.dumps({"instance": inst.id, "enabled": enabled}))
    else:
        for event_type in enabled:
            print(event_type)
    return EXIT_OK


def cmd_inspect(args: argparse.Namespace, store: Store) -> int:
    state = store.load()
    host = _build_host(state)
    inst = host.instances.get(args.instance)
    if inst is None:
        print(f"no such instance: {args.instance}", file=sys.stderr)
        return EXIT_NOT_FOUND
    if args.json:
        obj = {"instance": inst.id, **host.inspect(inst)}
        print(json.dumps(obj))
    else:
        _print_inspect(inst, host.inspect(inst))
    return EXIT_OK


def _print_inspect(inst: Instance, info: dict[str, Any]) -> None:
    print(
        f"{inst.id}\t{info['status']}\t{info['config']}\t"
        f"queue={len(info['queue'])} deferred={len(info['deferred'])} "
        f"timers={len(info['timers'])}"
    )


# --- batch / streaming mode (SPEC §13.7) ------------------------------------
def cmd_run(args: argparse.Namespace, store: Store) -> int:
    """Drive many commands from NDJSON stdin against one store + virtual clock.

    Each input line is a JSON array of argv tokens (one §13.3 command). For each
    line, exactly one NDJSON result object is written to stdout in input order:
    ``{ "ok": bool, "exit": int, "result": <value>, "error": {"message": str}? }``.
    A failing line does not abort the stream; the process exit code is the first
    non-zero line exit, else 0.
    """
    if args.source in (None, "-"):
        lines = sys.stdin.read().splitlines()
    else:
        lines = Path(args.source).read_text(encoding="utf-8").splitlines()
    parser = _build_parser()
    first_nonzero = 0
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        exit_code, result, error = _run_one(parser, store, line)
        record: dict[str, Any] = {"ok": exit_code == 0, "exit": exit_code, "result": result}
        if error is not None:
            record["error"] = {"message": error}
        print(json.dumps(record), flush=True)
        if exit_code != 0 and first_nonzero == 0:
            first_nonzero = exit_code
    return first_nonzero


def _run_one(
    parser: argparse.ArgumentParser, store: Store, line: str
) -> tuple[int, Any, str | None]:
    """Execute one batch line; return (exit_code, result_value, error_message)."""
    try:
        argv = json.loads(line)
        if not isinstance(argv, list) or not all(isinstance(t, str) for t in argv):
            raise ValueError("each line must be a JSON array of strings")
    except (ValueError, json.JSONDecodeError) as exc:
        return EXIT_USAGE, None, str(exc)

    out, err = io.StringIO(), io.StringIO()
    try:
        with redirect_stdout(out), redirect_stderr(err):
            sub = parser.parse_args([*argv, "--json"])
            if getattr(sub, "command", None) == "run":
                return EXIT_USAGE, None, "nested 'run' is not allowed in batch mode"
            rc = int(sub.cmd(sub, store))
    except SystemExit as exc:  # argparse usage error
        code = exc.code if isinstance(exc.code, int) else EXIT_USAGE
        return code, None, (err.getvalue().strip() or "usage error")
    except DetermaError as exc:
        return EXIT_OTHER, None, str(exc)

    result = _parse_captured(out.getvalue())
    message = (err.getvalue().strip() or None) if rc != 0 else None
    return rc, result, message


def _parse_captured(text: str) -> Any:
    """A command's captured stdout as JSON when it is JSON, else the raw string/None."""
    s = text.strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return s


# --- output helpers ---------------------------------------------------------
def _state_json(host: Host, inst: Instance) -> dict[str, Any]:
    return {
        "instance": inst.id,
        "def": f"{inst.machine.id}@{inst.machine.version}",
        "status": inst.status.value,
        "config": inst.active_leaf_names(),
        "esvs": inst.resolved_esvs(),
    }


def _print_state(args: argparse.Namespace, host: Host, inst: Instance) -> None:
    if args.json:
        print(json.dumps(_state_json(host, inst)))


def _build_payload(args: argparse.Namespace, machine: Machine) -> dict[str, Any] | None:
    if args.payload_json:
        return cast(dict[str, Any], json.loads(args.payload_json))
    if not args.payload:
        return None
    types = _event_payload_types(machine, args.event)
    return _parse_kv(args.payload, types)


def _event_payload_types(machine: Machine, event: str) -> dict[str, str]:
    decl = (machine.definition.raw.get("events") or {}).get(event)
    if not isinstance(decl, dict):
        return {}
    return {k: v["type"] for k, v in (decl.get("payload") or {}).items()}


def _external_types(machine: Machine) -> dict[str, str]:
    types: dict[str, str] = {}
    for var, decl in (machine.top.raw.get("esvs") or {}).items():
        if decl.get("external"):
            types[var] = decl["type"]
    return types


def _parse_kv(items: list[str], types: dict[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        out[k] = _coerce(v, types.get(k))
    return out


def _parse_csv_kv(items: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for part in items.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k] = _coerce(v, None)
    return out


def _coerce(value: str, type_name: str | None) -> Any:
    if type_name == "int":
        return int(value)
    if type_name == "float":
        return float(value)
    if type_name == "bool":
        return value.lower() in {"true", "yes", "1"}
    if type_name == "list":
        return json.loads(value)
    if type_name == "map":
        return json.loads(value)
    if type_name is None:
        for caster in (int, float):
            try:
                return caster(value)
            except ValueError:
                continue
        if value.lower() in {"true", "false"}:
            return value.lower() == "true"
    return value


def _pkg_version() -> str:
    from . import __version__

    return __version__
