"""Immutable synchronized specification and conformance inputs."""

from __future__ import annotations

from pathlib import Path

CONFORMANCE_COMMIT = "c6637066c1923e451edad62b7dc2ae73babfbec0"
SPEC_COMMIT = "318ef1f16ae024770090bd338c8b70056df2855b"

ROOT = Path(__file__).resolve().parent.parent
CONFORMANCE_CACHE = ROOT / ".cache" / f"determa-state-conformance-{CONFORMANCE_COMMIT[:12]}"
SPEC_CACHE = ROOT / ".cache" / f"determa-state-spec-{SPEC_COMMIT[:12]}"
