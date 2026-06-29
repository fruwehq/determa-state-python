"""Conformance-suite harness helpers.

The suite is consumed from the ``vendor/harel`` git submodule (SPEC §9). These
helpers locate the suite, enumerate cases, and run engine cases against this
implementation (create the root as id ``root``, per step ``send``/``advance``,
run all instances to quiescence, then check ``expect``).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from harel import Host

REPO_ROOT = Path(__file__).resolve().parent.parent
SUITE_DIR = REPO_ROOT / "vendor" / "harel"
CONFORMANCE_DIR = SUITE_DIR / "conformance"

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


# --- engine case runner -----------------------------------------------------
def run_engine_case(case: EngineCase) -> None:
    """Execute one engine conformance case, asserting every ``expect``.

    Loads the definitions, creates the root (id ``root``) seeded with any
    top-level ``external`` esvs, then runs each step (``send`` so far) to
    quiescence and checks the step's expectations (SPEC §9).
    """
    from harel import load_definitions

    test = _load_yaml(case.test_file)
    external = test.get("external") or {}
    assert case.machine_files, f"{case.name}: no machine files"
    defs = load_definitions(case.machine_files[0].read_text(encoding="utf-8"))
    host = Host()
    host.register_all(defs)
    host.create_root(host.machines[defs[0].id], "root", external=external)
    host.run_to_quiescence()

    for i, step in enumerate(test.get("steps", [])):
        step_label = f"{case.name} step {i}"
        assert "send" in step, f"{step_label}: only `send` steps supported so far"
        before_pub, before_sp = len(host.published), len(host.spawned)
        delivered = _do_send(host, step["send"], step_label)
        host.run_to_quiescence()
        _check_expect(
            host,
            step.get("expect") or {},
            step_label,
            delivered=delivered,
            published=host.published[before_pub:],
            spawned=host.spawned[before_sp:],
        )


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
