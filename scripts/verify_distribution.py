#!/usr/bin/env python3
"""Verify release artifacts contain canonical metadata and every JSON Schema."""

from __future__ import annotations

import email
import json
import sys
import tarfile
import zipfile
from collections import Counter
from email.message import Message
from pathlib import Path

import jsonschema

if __package__:
    from .verify_release_tag import package_version
else:
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


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-JSON numeric constant {value!r}")


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def _verify_schema(contents: bytes, relative_path: str, artifact: Path) -> None:
    try:
        document = json.loads(
            contents.decode("utf-8"),
            parse_constant=_reject_json_constant,
            object_pairs_hook=_strict_json_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ValueError(
            f"{artifact.name} contains invalid JSON in {relative_path}: {error}"
        ) from error
    if not isinstance(document, dict):
        raise ValueError(f"{artifact.name} schema {relative_path} must be a JSON object")
    try:
        jsonschema.Draft202012Validator.check_schema(document)
    except jsonschema.SchemaError as error:
        raise ValueError(
            f"{artifact.name} contains an invalid Draft 2020-12 schema in {relative_path}: "
            f"{error.message}"
        ) from error

    canonical = (PROJECT_ROOT / "src" / relative_path).read_bytes()
    if contents != canonical:
        raise ValueError(f"{artifact.name} schema {relative_path} differs from canonical source")


def _verify_wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        member_names = [member.filename for member in archive.infolist()]
        duplicates = sorted(name for name, count in Counter(member_names).items() if count > 1)
        if duplicates:
            raise ValueError(f"{path.name} contains duplicate archive members: {duplicates!r}")
        names = set(member_names)
        metadata_paths = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_paths) != 1:
            raise ValueError(f"{path.name} must contain exactly one dist-info METADATA file")
        _verify_metadata(_metadata(archive.read(metadata_paths[0]), path), path)
        packaged_schemas = {
            name
            for name in names
            if name.startswith("determa/state/data/") and name.endswith(".schema.json")
        }
        expected_schemas = set(SCHEMA_RELATIVE_PATHS)
        if packaged_schemas != expected_schemas:
            raise ValueError(
                f"{path.name} packaged schemas do not match canonical sources: "
                f"expected {sorted(expected_schemas)!r}, found {sorted(packaged_schemas)!r}"
            )
        for schema in sorted(packaged_schemas):
            _verify_schema(archive.read(schema), schema, path)


def _verify_sdist(path: Path) -> None:
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        duplicates = sorted(
            name for name, count in Counter(member.name for member in members).items() if count > 1
        )
        if duplicates:
            raise ValueError(f"{path.name} contains duplicate archive members: {duplicates!r}")
        names = {member.name for member in members}
        metadata_paths = [name for name in names if name.endswith("/PKG-INFO")]
        if len(metadata_paths) != 1:
            raise ValueError(f"{path.name} must contain exactly one PKG-INFO file")
        member = archive.getmember(metadata_paths[0])
        source = archive.extractfile(member)
        if source is None:
            raise ValueError(f"{path.name} cannot read {metadata_paths[0]}")
        _verify_metadata(_metadata(source.read(), path), path)
        packaged_schemas: dict[str, str] = {}
        marker = "/src/determa/state/data/"
        for name in names:
            if marker not in name or not name.endswith(".schema.json"):
                continue
            relative_path = f"determa/state/data/{name.split(marker, 1)[1]}"
            if relative_path in packaged_schemas:
                raise ValueError(f"{path.name} contains duplicate schema {relative_path}")
            packaged_schemas[relative_path] = name
        expected_schemas = set(SCHEMA_RELATIVE_PATHS)
        if set(packaged_schemas) != expected_schemas:
            raise ValueError(
                f"{path.name} packaged schemas do not match canonical sources: "
                f"expected {sorted(expected_schemas)!r}, found {sorted(packaged_schemas)!r}"
            )
        for schema, member_name in sorted(packaged_schemas.items()):
            schema_source = archive.extractfile(archive.getmember(member_name))
            if schema_source is None:
                raise ValueError(f"{path.name} cannot read {member_name}")
            _verify_schema(schema_source.read(), schema, path)


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
