"""Immutable pre-release specification and conformance inputs."""

from __future__ import annotations

from pathlib import Path

CONFORMANCE_COMMIT = "ffbc65cbce49733803119a7dabf02a9727819ba8"
SPEC_COMMIT = "4bd4d9588d11b75d376380b6120676a056a4bc45"

ROOT = Path(__file__).resolve().parent.parent
CONFORMANCE_CACHE = ROOT / ".cache" / f"determa-state-conformance-{CONFORMANCE_COMMIT[:12]}"
SPEC_CACHE = ROOT / ".cache" / f"determa-state-spec-{SPEC_COMMIT[:12]}"
