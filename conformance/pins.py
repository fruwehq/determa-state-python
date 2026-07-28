"""Immutable pre-release specification and conformance inputs."""

from __future__ import annotations

from pathlib import Path

CONFORMANCE_COMMIT = "fc4842010ab8d83bf4c5c6280a5627ca86829f7f"
SPEC_COMMIT = "4bd4d9588d11b75d376380b6120676a056a4bc45"

ROOT = Path(__file__).resolve().parent.parent
CONFORMANCE_CACHE = ROOT / ".cache" / f"determa-state-conformance-{CONFORMANCE_COMMIT[:12]}"
SPEC_CACHE = ROOT / ".cache" / f"determa-state-spec-{SPEC_COMMIT[:12]}"
