"""Small implementation-local command line entry point."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from .__about__ import __version__
from .definition import load_bundle
from .errors import ValidationError


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="determa-state")
    parser.add_argument("--version", action="version", version=__version__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    validate = subcommands.add_parser("validate", help="validate one format-1 bundle")
    validate.add_argument("file", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Validate a bundle; execution remains an explicit library foreground call."""
    arguments = _parser().parse_args(argv)
    if arguments.command != "validate":
        return 2
    try:
        bundle = load_bundle(arguments.file.read_text(encoding="utf-8"))
    except (OSError, ValidationError) as error:
        code = error.code if isinstance(error, ValidationError) else "source_error"
        print(json.dumps({"valid": False, "code": code}, separators=(",", ":")))
        return 1
    print(
        json.dumps(
            {"valid": True, "fingerprint": bundle.fingerprint},
            separators=(",", ":"),
        )
    )
    return 0
