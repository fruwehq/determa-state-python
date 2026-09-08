#!/usr/bin/env python3
"""Verify release artifacts contain canonical metadata and every JSON Schema."""

from __future__ import annotations

import email
import sys
import tarfile
import zipfile
from email.message import Message
from pathlib import Path

from verify_release_tag import package_version

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SOURCE_DIRECTORY = PROJECT_ROOT / "src" / "determa" / "state" / "data"
SCHEMA_RELATIVE_PATHS = tuple(
    path.relative_to(PROJECT_ROOT / "src").as_posix()
    for path in sorted(SCHEMA_SOURCE_DIRECTORY.glob("*.schema.json"))
)
if not SCHEMA_RELATIVE_PATHS:
    raise RuntimeError(f"no JSON Schemas found in {SCHEMA_SOURCE_DIRECTORY}")


def _metadata(message: bytes, artifact: Path) -> Message:
    return email.message_from_bytes(message)


def _verify_metadata(metadata: Message, artifact: Path) -> None:
    if metadata["Name"] != "determa-state":
        raise ValueError(f"{artifact.name} has unexpected package name {metadata['Name']!r}")
    if metadata["Version"] != package_version():
        raise ValueError(f"{artifact.name} has unexpected version {metadata['Version']!r}")
    if metadata["Requires-Python"] != ">=3.11":
        raise ValueError(
            f"{artifact.name} has unexpected Requires-Python {metadata['Requires-Python']!r}"
        )


def _verify_wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        metadata_paths = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_paths) != 1:
            raise ValueError(f"{path.name} must contain exactly one dist-info METADATA file")
        _verify_metadata(_metadata(archive.read(metadata_paths[0]), path), path)
        missing = [schema for schema in SCHEMA_RELATIVE_PATHS if schema not in names]
        if missing:
            raise ValueError(f"{path.name} omits packaged schemas: {', '.join(missing)}")


def _verify_sdist(path: Path) -> None:
    with tarfile.open(path, "r:gz") as archive:
        names = set(archive.getnames())
        metadata_paths = [name for name in names if name.endswith("/PKG-INFO")]
        if len(metadata_paths) != 1:
            raise ValueError(f"{path.name} must contain exactly one PKG-INFO file")
        member = archive.getmember(metadata_paths[0])
        source = archive.extractfile(member)
        if source is None:
            raise ValueError(f"{path.name} cannot read {metadata_paths[0]}")
        _verify_metadata(_metadata(source.read(), path), path)
        missing = [
            schema
            for schema in SCHEMA_RELATIVE_PATHS
            if not any(name.endswith(f"/src/{schema}") for name in names)
        ]
        if missing:
            raise ValueError(f"{path.name} omits packaged schemas: {', '.join(missing)}")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: verify_distribution.py DIST_DIRECTORY", file=sys.stderr)
        return 2

    directory = Path(argv[1])
    wheels = sorted(directory.glob("*.whl"))
    sdists = sorted(directory.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        print("expected exactly one wheel and one source distribution", file=sys.stderr)
        return 1

    try:
        _verify_wheel(wheels[0])
        _verify_sdist(sdists[0])
    except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as error:
        print(f"distribution verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
