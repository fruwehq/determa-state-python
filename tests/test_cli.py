from __future__ import annotations

import json
import subprocess
import sys

from determa.state import __version__
from determa.state.cli import main


def test_version_matches_release_metadata() -> None:
    assert __version__ == "0.1.0"


def test_validate_command_reports_fingerprint(tmp_path, capsys) -> None:
    machine = tmp_path / "machine.yaml"
    machine.write_text(
        """
format: 1
namespace: example.cli
machines:
  - machine_id: cli
    root: {}
""",
        encoding="utf-8",
    )

    assert main(["validate", str(machine)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["valid"] is True
    assert result["fingerprint"].startswith("sha256:")


def test_package_import_keeps_heavy_validators_lazy() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; import determa.state; "
                "print("
                "'celpy' in sys.modules, "
                "'jsonschema' in sys.modules, "
                "'psycopg' in sys.modules"
                ")"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == "False False False"
