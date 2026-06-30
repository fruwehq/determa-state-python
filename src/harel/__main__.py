"""Entry point for ``python -m harel`` (mirrors the ``harel`` console script)."""

from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
