"""Optional §11.5 public extension negotiation runtime profile."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from .harness import conformance_root


def test_extension_negotiation_profile() -> None:
    root = conformance_root()
    runner = root / "scripts" / "run_extension_negotiation_profile.py"
    if not runner.is_file():
        pytest.skip("extension negotiation profile is not present in this conformance pin")
    specification = os.environ.get("DETERMA_SPEC_DIR")
    if specification is None:
        pytest.skip("pinned specification checkout is not available")
    adapter = Path(__file__).with_name("extension_negotiation_adapter.py")
    subprocess.run(
        [
            sys.executable,
            str(runner),
            "--spec-root",
            specification,
            "--adapter",
            sys.executable,
            str(adapter),
        ],
        check=True,
        cwd=Path(__file__).resolve().parent.parent,
    )
