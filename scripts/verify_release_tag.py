#!/usr/bin/env python3
"""Fail closed unless a release tag exactly identifies the package version."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
VERSION_SOURCE = PROJECT_ROOT / "src" / "determa" / "state" / "__about__.py"


def package_version(path: Path = VERSION_SOURCE) -> str:
    """Read the one literal __version__ assignment used by hatchling."""
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    values = [
        statement.value.value
        for statement in module.body
        if isinstance(statement, ast.Assign)
        for assignment in statement.targets
        if isinstance(assignment, ast.Name)
        and assignment.id == "__version__"
        and isinstance(statement.value, ast.Constant)
        and isinstance(statement.value.value, str)
    ]
    if len(values) != 1 or not values[0]:
        raise ValueError(
            f"{path} must contain exactly one non-empty literal __version__ assignment"
        )
    return values[0]


def expected_tag(path: Path = VERSION_SOURCE) -> str:
    return f"v{package_version(path)}"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: verify_release_tag.py vX.Y.Z", file=sys.stderr)
        return 2

    tag = argv[1]
    try:
        expected = expected_tag()
    except (OSError, SyntaxError, ValueError) as error:
        print(f"unable to determine package version: {error}", file=sys.stderr)
        return 1

    if tag != expected:
        print(f"release tag mismatch: expected {expected!r}, received {tag!r}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
