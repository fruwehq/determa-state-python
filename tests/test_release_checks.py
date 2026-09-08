from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from scripts.verify_release_tag import expected_tag, package_version

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_release_tag_matches_single_package_version() -> None:
    assert package_version() == "0.1.0"
    assert expected_tag() == "v0.1.0"


def test_release_tag_rejects_mismatch() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/verify_release_tag.py", "v999.999.999"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "release tag mismatch" in result.stderr
