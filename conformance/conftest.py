"""Fetch the language-agnostic conformance suite before test collection.

The suite lives in ``fruwehq/harel-conformance`` (no git submodule). It is cloned at the
release tag matching this package's version (falling back to ``main`` while the tag does
not yet exist) into a gitignored ``.cache/`` directory and reused. Override with a local
checkout via ``HAREL_CONFORMANCE_DIR`` for offline work. If the suite cannot be obtained
(offline, no override), the conformance tests skip rather than error.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import harel

_ROOT = Path(__file__).resolve().parent.parent
_CACHE = _ROOT / ".cache" / "harel-conformance"
_REPO = "https://github.com/fruwehq/harel-conformance.git"


def _ensure_conformance() -> None:
    if os.environ.get("HAREL_CONFORMANCE_DIR"):
        return  # caller provides a local checkout
    if (_CACHE / ".git").exists():
        return  # already fetched; reuse (force a refresh by deleting .cache/)
    _CACHE.parent.mkdir(parents=True, exist_ok=True)
    # Prefer the release tag matching our version; fall back to main (tags may not exist
    # yet pre-release). Network/tooling failure leaves the suite absent -> tests skip.
    for ref in (f"v{harel.__version__}", "main"):
        try:
            subprocess.run(
                ["git", "clone", "--depth", "1", "--branch", ref, _REPO, str(_CACHE)],
                check=True,
                capture_output=True,
            )
            return
        except (subprocess.CalledProcessError, OSError):
            continue


_ensure_conformance()
