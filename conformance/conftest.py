"""Fetch the immutable synchronized conformance and specification inputs."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from .pins import CONFORMANCE_CACHE, CONFORMANCE_COMMIT, SPEC_CACHE, SPEC_COMMIT


def _head(path: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, OSError):
        return None
    return result.stdout.strip()


def _ensure_checkout(path: Path, repository: str, commit: str) -> None:
    if _head(path) == commit:
        return
    path.mkdir(parents=True, exist_ok=True)
    try:
        if not (path / ".git").exists():
            subprocess.run(["git", "init", str(path)], check=True, capture_output=True)
        subprocess.run(
            ["git", "-C", str(path), "fetch", "--depth", "1", repository, commit],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "-C", str(path), "checkout", "--detach", "FETCH_HEAD"],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, OSError):
        return


if "DETERMA_CONFORMANCE_DIR" not in os.environ:
    _ensure_checkout(
        CONFORMANCE_CACHE,
        "https://github.com/fruwehq/determa-state-conformance.git",
        CONFORMANCE_COMMIT,
    )
    if _head(CONFORMANCE_CACHE) == CONFORMANCE_COMMIT:
        os.environ["DETERMA_CONFORMANCE_DIR"] = str(CONFORMANCE_CACHE)

if "DETERMA_SPEC_DIR" not in os.environ:
    _ensure_checkout(
        SPEC_CACHE,
        "https://github.com/fruwehq/determa-state-spec.git",
        SPEC_COMMIT,
    )
    if _head(SPEC_CACHE) == SPEC_COMMIT:
        os.environ["DETERMA_SPEC_DIR"] = str(SPEC_CACHE)
