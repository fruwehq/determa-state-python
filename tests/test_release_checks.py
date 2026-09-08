from __future__ import annotations

import io
import shutil
import subprocess
import sys
import tarfile
import warnings
import zipfile
from pathlib import Path

import pytest
import yaml

from scripts.verify_distribution import SCHEMA_RELATIVE_PATHS, _verify_schema
from scripts.verify_release_tag import expected_tag, package_version

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_required_workflow_context_remains_stable() -> None:
    workflow = yaml.safe_load(
        (PROJECT_ROOT / ".github" / "workflows" / "test.yml").read_text(encoding="utf-8")
    )
    jobs = workflow["jobs"]

    baseline = jobs["test"]
    assert "name" not in baseline
    assert baseline["runs-on"] == "${{ matrix.os }}"
    assert baseline["strategy"]["matrix"] == {"os": ["ubuntu-24.04"]}
    assert baseline["steps"][1]["with"]["python-version"] == "3.13"

    compatibility = jobs["python-311-compatibility"]
    assert compatibility["name"] == "Python 3.11 compatibility"
    assert compatibility["runs-on"] == "ubuntu-24.04"
    assert compatibility["steps"][1]["with"]["python-version"] == "3.11"

    expected_gate_commands = ["ruff check .", "mypy src/determa", "pytest -q"]
    for job in (baseline, compatibility):
        commands = [step.get("run") for step in job["steps"]]
        assert all(command in commands for command in expected_gate_commands)


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


@pytest.fixture(scope="session")
def built_distributions(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    output = tmp_path_factory.mktemp("distribution-build")
    subprocess.run(
        [sys.executable, "-m", "build", "--no-isolation", "--outdir", str(output)],
        cwd=PROJECT_ROOT,
        check=True,
    )
    wheels = list(output.glob("*.whl"))
    sdists = list(output.glob("*.tar.gz"))
    assert len(wheels) == 1
    assert len(sdists) == 1
    return wheels[0], sdists[0]


def _copy_distributions(
    built_distributions: tuple[Path, Path], destination: Path
) -> tuple[Path, Path]:
    wheel, sdist = built_distributions
    return shutil.copy2(wheel, destination / wheel.name), shutil.copy2(
        sdist, destination / sdist.name
    )


def _corrupt_schema(contents: bytes) -> bytes:
    corrupted = contents.replace(b'"title"', b'"corrupted"', 1)
    assert corrupted != contents
    return corrupted


def _rewrite_wheel(path: Path, schema: str) -> None:
    rewritten = path.with_suffix(".rewritten.whl")
    with zipfile.ZipFile(path) as source, zipfile.ZipFile(rewritten, "w") as destination:
        for member in source.infolist():
            contents = source.read(member)
            destination.writestr(
                member,
                _corrupt_schema(contents) if member.filename == schema else contents,
            )
    rewritten.replace(path)


def _rewrite_sdist(path: Path, schema: str) -> None:
    rewritten = path.with_name(f"{path.name}.rewritten")
    marker = f"/src/{schema}"
    with tarfile.open(path, "r:gz") as source, tarfile.open(rewritten, "w:gz") as destination:
        for member in source.getmembers():
            extracted = source.extractfile(member) if member.isfile() else None
            contents = extracted.read() if extracted is not None else None
            if member.name.endswith(marker) and contents is not None:
                contents = _corrupt_schema(contents)
                member.size = len(contents)
            destination.addfile(member, io.BytesIO(contents) if contents is not None else None)
    rewritten.replace(path)


def _duplicate_wheel_member(path: Path, *, metadata: bool) -> None:
    with zipfile.ZipFile(path, "a") as archive:
        def selected(member: zipfile.ZipInfo) -> bool:
            if metadata:
                return member.filename.endswith(".dist-info/METADATA")
            return member.filename == SCHEMA_RELATIVE_PATHS[0]

        name = next(
            member.filename
            for member in archive.infolist()
            if selected(member)
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            archive.writestr(name, b"concealed duplicate corruption")


def _duplicate_sdist_member(path: Path, *, metadata: bool) -> None:
    rewritten = path.with_name(f"{path.name}.rewritten")
    with tarfile.open(path, "r:gz") as source, tarfile.open(rewritten, "w:gz") as destination:
        members = source.getmembers()
        for member in members:
            extracted = source.extractfile(member) if member.isfile() else None
            contents = extracted.read() if extracted is not None else None
            destination.addfile(member, io.BytesIO(contents) if contents is not None else None)
        def selected(member: tarfile.TarInfo) -> bool:
            if metadata:
                return member.name.endswith("/PKG-INFO")
            return member.name.endswith(f"/src/{SCHEMA_RELATIVE_PATHS[0]}")

        name = next(member.name for member in members if selected(member))
        duplicate = tarfile.TarInfo(name)
        duplicate.size = len(b"concealed duplicate corruption")
        destination.addfile(duplicate, io.BytesIO(b"concealed duplicate corruption"))
    rewritten.replace(path)


def _run_distribution_verifier(directory: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "scripts/verify_distribution.py", str(directory)],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_distribution_verifier_accepts_canonical_schemas(
    tmp_path: Path, built_distributions: tuple[Path, Path]
) -> None:
    _copy_distributions(built_distributions, tmp_path)

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
    tmp_path: Path, built_distributions: tuple[Path, Path], artifact: str
) -> None:
    corrupted_schema = SCHEMA_RELATIVE_PATHS[0]
    wheel, sdist = _copy_distributions(built_distributions, tmp_path)
    if artifact == "wheel":
        _rewrite_wheel(wheel, corrupted_schema)
    else:
        _rewrite_sdist(sdist, corrupted_schema)

    result = _run_distribution_verifier(tmp_path)

    assert result.returncode == 1
    assert "differs from canonical source" in result.stderr


@pytest.mark.parametrize("artifact", ["wheel", "sdist"])
@pytest.mark.parametrize("member_kind", ["schema", "metadata"])
def test_distribution_verifier_rejects_duplicate_members(
    tmp_path: Path,
    built_distributions: tuple[Path, Path],
    artifact: str,
    member_kind: str,
) -> None:
    wheel, sdist = _copy_distributions(built_distributions, tmp_path)
    if artifact == "wheel":
        _duplicate_wheel_member(wheel, metadata=member_kind == "metadata")
    else:
        _duplicate_sdist_member(sdist, metadata=member_kind == "metadata")

    result = _run_distribution_verifier(tmp_path)

    assert result.returncode == 1
    assert "duplicate archive members" in result.stderr
