"""Immutable pre-release specification and conformance inputs."""

from __future__ import annotations

from pathlib import Path

CONFORMANCE_COMMIT = "707a49ce01c6f57f673c1959cdfe078bc8d0fc9a"
SPEC_COMMIT = "1502a58a780d837e05bfacb37680dfc92e3488b5"

ROOT = Path(__file__).resolve().parent.parent
CONFORMANCE_CACHE = ROOT / ".cache" / f"determa-state-conformance-{CONFORMANCE_COMMIT[:12]}"
SPEC_CACHE = ROOT / ".cache" / f"determa-state-spec-{SPEC_COMMIT[:12]}"
