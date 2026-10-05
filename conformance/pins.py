"""Immutable synchronized specification and conformance inputs."""

from __future__ import annotations

from pathlib import Path

CONFORMANCE_COMMIT = "affe3fe3bcc4d13fa7c5374471568e94af36f0d1"
SPEC_COMMIT = "86bb88dd21cb1f799eefe5020b6e49dabf6e7225"

ROOT = Path(__file__).resolve().parent.parent
CONFORMANCE_CACHE = ROOT / ".cache" / f"determa-state-conformance-{CONFORMANCE_COMMIT[:12]}"
SPEC_CACHE = ROOT / ".cache" / f"determa-state-spec-{SPEC_COMMIT[:12]}"
