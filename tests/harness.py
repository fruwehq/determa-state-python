"""Conformance-suite harness helpers.

The suite is consumed from the ``vendor/harel`` git submodule (SPEC §9). These
helpers locate the suite and enumerate cases so the harness has a single source
of truth and no copy-paste drift.

Step 1 only exercises loading + validation; the run-to-expect harness grows in
later build steps (engine dispatch, CLI).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SUITE_DIR = REPO_ROOT / "vendor" / "harel"
CONFORMANCE_DIR = SUITE_DIR / "conformance"


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
