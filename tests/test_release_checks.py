from __future__ import annotations

import io
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

from scripts.verify_distribution import SCHEMA_RELATIVE_PATHS, _verify_schema
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


@pytest.mark.parametrize(
    "tag",
    [
        "v0.1.0rc1",
        "v0.1.0.dev1",
        "v0.1.0+local",
        "0.1.0",
        "v00.1.0",
        "v0.01.0",
        "v0.1.00",
        "v0.1.0.0",
        "",
    ],
)
def test_release_tag_rejects_noncanonical_or_unstable_tags(tag: str) -> None:
    result = subprocess.run(
        [sys.executable, "scripts/verify_release_tag.py", tag],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "invalid release tag" in result.stderr


@pytest.mark.parametrize(
    "version",
    [
        "0.1.0rc1",
        "0.1.0.dev1",
        "0.1.0+local",
        "v0.1.0",
        "00.1.0",
        "0.01.0",
        "0.1.00",
        "0.1.0.0",
        "",
    ],
)
def test_package_version_rejects_noncanonical_or_unstable_versions(
    tmp_path: Path, version: str
) -> None:
    version_source = tmp_path / "__about__.py"
    version_source.write_text(f"__version__ = {version!r}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="exact stable X.Y.Z"):
        package_version(version_source)


def test_zero_stable_version_is_valid(tmp_path: Path) -> None:
    version_source = tmp_path / "__about__.py"
    version_source.write_text('__version__ = "0.0.0"\n', encoding="utf-8")

    assert package_version(version_source) == "0.0.0"


def _metadata() -> bytes:
    return (
        b"Metadata-Version: 2.4\n"
        b"Name: determa-state\n"
        b"Version: 0.1.0\n"
        b"Requires-Python: >=3.11\n\n"
    )


def _write_wheel(path: Path, *, corrupt_schema: str | None = None) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("determa_state-0.1.0.dist-info/METADATA", _metadata())
        for schema in SCHEMA_RELATIVE_PATHS:
            contents = (PROJECT_ROOT / "src" / schema).read_bytes()
            if schema == corrupt_schema:
                contents = contents.replace(b'"title"', b'"corrupted"', 1)
            archive.writestr(schema, contents)


def _write_sdist(path: Path, *, corrupt_schema: str | None = None) -> None:
    root = "determa_state-0.1.0"
    with tarfile.open(path, "w:gz") as archive:
        members = {f"{root}/PKG-INFO": _metadata()}
        for schema in SCHEMA_RELATIVE_PATHS:
            contents = (PROJECT_ROOT / "src" / schema).read_bytes()
            if schema == corrupt_schema:
                contents = contents.replace(b'"title"', b'"corrupted"', 1)
            members[f"{root}/src/{schema}"] = contents
        for name, contents in members.items():
            member = tarfile.TarInfo(name)
            member.size = len(contents)
            archive.addfile(member, io.BytesIO(contents))


def _run_distribution_verifier(directory: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "scripts/verify_distribution.py", str(directory)],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_distribution_verifier_accepts_canonical_schemas(tmp_path: Path) -> None:
    _write_wheel(tmp_path / "determa_state-0.1.0-py3-none-any.whl")
    _write_sdist(tmp_path / "determa_state-0.1.0.tar.gz")

    result = _run_distribution_verifier(tmp_path)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "contents",
    [b'{"not_json": NaN}', b'{"duplicate": true, "duplicate": false}'],
)
def test_distribution_verifier_requires_strict_json(contents: bytes) -> None:
    with pytest.raises(ValueError, match="invalid JSON"):
        _verify_schema(contents, SCHEMA_RELATIVE_PATHS[0], Path("corrupt.whl"))


def test_distribution_verifier_meta_validates_draft_2020_12_schema() -> None:
    contents = b'{"$schema": "https://json-schema.org/draft/2020-12/schema", "type": 42}'

    with pytest.raises(ValueError, match="invalid Draft 2020-12 schema"):
        _verify_schema(contents, SCHEMA_RELATIVE_PATHS[0], Path("corrupt.tar.gz"))


@pytest.mark.parametrize("artifact", ["wheel", "sdist"])
def test_distribution_verifier_rejects_corrupted_schema(
    tmp_path: Path, artifact: str
) -> None:
    corrupted_schema = SCHEMA_RELATIVE_PATHS[0]
    _write_wheel(
        tmp_path / "determa_state-0.1.0-py3-none-any.whl",
        corrupt_schema=corrupted_schema if artifact == "wheel" else None,
    )
    _write_sdist(
        tmp_path / "determa_state-0.1.0.tar.gz",
        corrupt_schema=corrupted_schema if artifact == "sdist" else None,
    )

    result = _run_distribution_verifier(tmp_path)

    assert result.returncode == 1
    assert "differs from canonical source" in result.stderr
