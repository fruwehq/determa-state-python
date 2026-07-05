"""Entry point for ``python -m determa.state`` (mirrors the ``determa-state`` console script)."""

from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
