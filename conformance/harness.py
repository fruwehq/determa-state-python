"""Conformance-suite harness helpers.

The language-agnostic suite (SPEC §9) lives in ``fruwehq/harel-conformance`` and is
fetched at the matching release tag by ``conftest.py`` (no git submodule). These helpers
locate the fetched suite, enumerate cases, and run engine cases against this
implementation (create the root as id ``root``, per step ``send``/``advance``, run all
instances to quiescence, then check ``expect``).
"""

from __future__ import annotations

import importlib.util
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from harel import Host

REPO_ROOT = Path(__file__).resolve().parent.parent


def conformance_root() -> Path:
    """Root of the fetched ``harel-conformance`` checkout.

    ``HAREL_CONFORMANCE_DIR`` overrides with a local checkout (offline/dev); otherwise
    the cache populated by ``conftest.py`` is used.
    """
    env = os.environ.get("HAREL_CONFORMANCE_DIR")
    return Path(env) if env else REPO_ROOT / ".cache" / "harel-conformance"


CONFORMANCE_DIR = conformance_root() / "conformance"

# Cases the engine is known to pass. Others are skipped until their features
# land; extend this set as build-order steps are completed.
SUPPORTED = frozenset(
    {
        "01-guarded-leaf",
        "02-hierarchy-bubbling",
        "03-initial-action",
        "04-defer",
        "05-esvs-scope",
        "06-payload-typing",
        "07-internal-external",
        "08-local-vs-external",
        "09-orthogonal",
        "10-history-deep",
        "11-history-shallow",
        "12-guarded-list",
        "13-spawn-publish",
        "14-subscription",
        "15-external-env-refresh",
        "16-timer",
        "17-fault-handled",
        "18-fault-unhandled",
        "19-contract-pass",
        "20-contract-fail",
        "21-snapshot-roundtrip",
        "22-migration",
        "23-choice",
        "24-choice-chain",
        "25-choice-invalid",
        "26-unreachable",
        "27-dead-branch",
        "28-reachable-ok",
        "29-submachine",
        "30-submachine-interrupt",
        "31-enabled-events",
    }
)


@dataclass(frozen=True)
class EngineCase:
    name: str
    path: Path
    machine_files: list[Path]
    test_file: Path


def _machine_files(case_dir: Path) -> list[Path]:
    """The machine-definition file(s) for a case.

    Most cases have ``machine.yaml``; migration cases have versioned
    ``v1.yaml``/``v2.yaml``/… files instead (SPEC §9).
    """
    single = case_dir / "machine.yaml"
    if single.exists():
        return [single]
    versioned = sorted(case_dir.glob("v*.yaml"))
    if versioned:
        return versioned
    return []


def engine_cases() -> list[EngineCase]:
    """All engine conformance cases (``conformance/01``–``22``), sorted."""
    if not CONFORMANCE_DIR.exists():
        return []
    cases: list[EngineCase] = []
    for case_dir in sorted(p for p in CONFORMANCE_DIR.iterdir() if p.is_dir()):
        machine_files = _machine_files(case_dir)
        if not machine_files:
            continue
        test_file = case_dir / "test.yaml"
        cases.append(
            EngineCase(
                name=case_dir.name,
                path=case_dir,
                machine_files=machine_files,
                test_file=test_file,
            )
        )
    return cases


def cli_cases() -> list[Path]:
    """All CLI conformance case directories (``conformance/cli/*``)."""
    cli_dir = CONFORMANCE_DIR / "cli"
    if not cli_dir.exists():
        return []
    return sorted(p for p in cli_dir.iterdir() if p.is_dir())


# --- CLI case runner (SPEC §13.6): true black box via the spec repo's runner --
def run_cli_case(case_dir: Path) -> None:
    """Run a CLI case **black-box** via the spec repo's reference runner (§13.6).

    Invokes this package as a subprocess (``python -m harel``), so packaging and
    entry-point regressions are caught — not an in-process import. Delegating to the
    suite's ``conformance/run_cli.py`` also avoids harness drift.
    """
    runner = _load_cli_runner()
    rc = runner.main(
        [
            "--cmd",
            f"{sys.executable} -m harel",
            "--conformance-dir",
            str(CONFORMANCE_DIR / "cli"),
            case_dir.name,
        ]
    )
    assert rc == 0, f"cli/{case_dir.name}: black-box CLI runner reported failure"


def _load_cli_runner() -> ModuleType:
    path = CONFORMANCE_DIR / "run_cli.py"
    spec = importlib.util.spec_from_file_location("harel_cli_runner", path)
    assert spec is not None and spec.loader is not None, f"runner not found: {path}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --- engine case runner -----------------------------------------------------
def run_engine_case(case: EngineCase) -> None:
    """Execute one engine conformance case, asserting every ``expect`` (SPEC §9)."""
    from harel import collect_errors, load_definition, load_definitions
    from harel.contracts import load_contract, validate_contracts

    test = _load_yaml(case.test_file)
    assert case.machine_files, f"{case.name}: no machine files"

    if "static" in test:
        from harel import ValidationError

        expected = bool(test["static"]["valid"])
        try:
            root_raw = load_definitions(case.machine_files[0].read_text(encoding="utf-8"))[0].raw
        except ValidationError:
            # invalid at load time (schema / structural / choice rules)
            assert expected is False, f"{case.name}: expected valid but load failed"
            return
        errors = list(collect_errors(root_raw))
        contracts: dict[str, dict[str, Any]] = {}
        cdir = case.path / "contracts"
        if cdir.exists():
            for cf in sorted(cdir.glob("*.yaml")):
                c = load_contract(cf.read_text(encoding="utf-8"))
                contracts[c["id"]] = c
        errors.extend(validate_contracts(root_raw, contracts))
        valid = not errors
        assert valid is expected, (
            f"{case.name}: static valid={valid} != {expected} ({errors})"
        )
        return

    external = test.get("external") or {}
    host = Host()
    files = case.machine_files
    versioned = bool(files) and all(
        f.name[:1] == "v" and f.stem[1:].isdigit() for f in files
    )
    if versioned:
        ordered = sorted(files)
        for f in ordered:
            host.register(load_definition(f.read_text(encoding="utf-8")))
        root_id = load_definition(ordered[0].read_text(encoding="utf-8")).id
        lowest = min(v for (iid, v) in host.versions if iid == root_id)
        root_machine = host.versions[(root_id, lowest)]
    else:
        defs = load_definitions(files[0].read_text(encoding="utf-8"))
        host.register_all(defs)
        root_machine = host.machines[defs[0].id]
    host.create_root(root_machine, "root", external=external)
    host.run_to_quiescence()

    roundtrip = bool(test.get("roundtrip"))
    for i, step in enumerate(test.get("steps", [])):
        step_label = f"{case.name} step {i}"
        before_pub, before_sp = len(host.published), len(host.spawned)
        if "send" in step:
            delivered = _do_send(host, step["send"], step_label)
            host.run_to_quiescence()
        elif "advance" in step:
            host.advance(step["advance"])
            delivered = True
            host.run_to_quiescence()
        elif "upgrade" in step:
            host.upgrade(int(step["upgrade"]), root_machine.id)
            delivered = True
            host.run_to_quiescence()
        else:
            raise AssertionError(f"{step_label}: unsupported step {list(step)}")
        _check_expect(
            host,
            step.get("expect") or {},
            step_label,
            delivered=delivered,
            published=host.published[before_pub:],
            spawned=host.spawned[before_sp:],
        )
        if roundtrip:
            host.restore_all(host.snapshot_all())


def _do_send(host: Host, send: dict[str, Any], label: str) -> bool:
    instance = send.get("instance", "root")
    event = send["event"]
    payload = send.get("payload")
    return host.deliver(instance, event, payload)


def _check_expect(
    host: Host,
    expect: dict[str, Any],
    label: str,
    delivered: bool,
    published: list[str],
    spawned: list[str],
) -> None:
    if "rejected" in expect:
        rejected = bool(expect["rejected"])
        assert delivered is (not rejected), (
            f"{label}: rejected={delivered} != expected {rejected}"
        )
    instance_id = expect.get("instance", "root")
    inst = host.instances.get(instance_id)
    if "config" in expect:
        assert inst is not None, f"{label}: instance {instance_id} missing"
        assert inst.active_leaf_names() == sorted(expect["config"]), (
            f"{label}: config {inst.active_leaf_names()} != {sorted(expect['config'])}"
        )
    if "esvs" in expect and inst is not None:
        actual = inst.resolved_esvs()
        for name, val in expect["esvs"].items():
            assert actual.get(name) == val, (
                f"{label}: esv {name}={actual.get(name)!r} != {val!r}"
            )
    if "enabled" in expect:
        assert inst is not None, f"{label}: instance {instance_id} missing"
        assert host.enabled_events(inst) == sorted(expect["enabled"]), (
            f"{label}: enabled {host.enabled_events(inst)} != {sorted(expect['enabled'])}"
        )
    if "status" in expect:
        assert inst is not None
        assert inst.status.value == expect["status"], (
            f"{label}: status {inst.status.value} != {expect['status']}"
        )
    if "published" in expect:
        assert published == expect["published"], (
            f"{label}: published {published} != {expect['published']}"
        )
    if "spawned" in expect:
        assert spawned == expect["spawned"], (
            f"{label}: spawned {spawned} != {expect['spawned']}"
        )
    if expect.get("dead_letter"):
        assert inst is not None
        assert inst.dead_letter, f"{label}: expected a dead-letter record"
    if expect.get("instances"):
        for iid, sub in expect["instances"].items():
            target = host.instances.get(iid)
            if "status" in sub and target is not None:
                assert target.status.value == sub["status"], (
                    f"{label}: {iid} status {target.status.value} != {sub['status']}"
                )
            if "config" in sub and target is not None:
                assert target.active_leaf_names() == sorted(sub["config"]), (
                    f"{label}: {iid} config mismatch"
                )


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml  # conformance test.yaml is a scenario, not a machine; core schema ok

    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data or {}
