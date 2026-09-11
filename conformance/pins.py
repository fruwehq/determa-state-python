"""Immutable synchronized specification and conformance inputs."""

from __future__ import annotations

from pathlib import Path

CONFORMANCE_COMMIT = "8f6a4d9101fd6554e9a51da72ca48160368d4e83"
SPEC_COMMIT = "e22f9db295d632f3f46a9d1260c63b5af92efa7e"

ROOT = Path(__file__).resolve().parent.parent
CONFORMANCE_CACHE = ROOT / ".cache" / f"determa-state-conformance-{CONFORMANCE_COMMIT[:12]}"
SPEC_CACHE = ROOT / ".cache" / f"determa-state-spec-{SPEC_COMMIT[:12]}"
