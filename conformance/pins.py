"""Immutable pre-release specification and conformance inputs."""

from __future__ import annotations

from pathlib import Path

CONFORMANCE_COMMIT = "409bbdc6c2d4a4e9d50ddb1d994c5f5cd7d97762"
SPEC_COMMIT = "03771fac569a47b82f27891cd3700d4d1d876f8b"

ROOT = Path(__file__).resolve().parent.parent
CONFORMANCE_CACHE = ROOT / ".cache" / f"determa-state-conformance-{CONFORMANCE_COMMIT[:12]}"
SPEC_CACHE = ROOT / ".cache" / f"determa-state-spec-{SPEC_COMMIT[:12]}"
