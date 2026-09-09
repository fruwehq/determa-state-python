"""Immutable synchronized specification and conformance inputs."""

from __future__ import annotations

from pathlib import Path

CONFORMANCE_COMMIT = "5ba78c7ef90b8556e76de6481a18b23a3d0c2378"
SPEC_COMMIT = "cc4b0d734aa1c5953de75fb53b63e390a3b72761"

ROOT = Path(__file__).resolve().parent.parent
CONFORMANCE_CACHE = ROOT / ".cache" / f"determa-state-conformance-{CONFORMANCE_COMMIT[:12]}"
SPEC_CACHE = ROOT / ".cache" / f"determa-state-spec-{SPEC_COMMIT[:12]}"
