#!/usr/bin/env python3
"""Fail closed unless a release tag exactly identifies the package version."""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
VERSION_SOURCE = PROJECT_ROOT / "src" / "determa" / "state" / "__about__.py"
STABLE_VERSION_PATTERN = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)")
STABLE_TAG_PATTERN = re.compile(r"v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)")


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
    if len(values) != 1:
        raise ValueError(
            f"{path} must contain exactly one literal __version__ assignment"
        )
    version = values[0]
    if STABLE_VERSION_PATTERN.fullmatch(version) is None:
        raise ValueError(f"package version must be an exact stable X.Y.Z version, got {version!r}")
    return version


def expected_tag(path: Path = VERSION_SOURCE) -> str:
    return f"v{package_version(path)}"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: verify_release_tag.py vX.Y.Z", file=sys.stderr)
        return 2

    tag = argv[1]
    if STABLE_TAG_PATTERN.fullmatch(tag) is None:
        print(
            f"invalid release tag: expected exact stable vX.Y.Z, received {tag!r}",
            file=sys.stderr,
        )
        return 1

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
