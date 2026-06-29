"""CEL (Common Expression Language) evaluation for guards and action values.

Guards are CEL booleans; computed action values (an ``assign`` RHS, a published
payload value) are CEL over ``(esvs, event, id, parent)`` (SPEC §6). CEL is
side-effect-free and non-Turing-complete, which is what makes guards portable.

This module wraps ``cel-python`` (the ``celpy`` package). celpy requires its
own container types for field selection, so bindings are deep-converted on the
way in. Compiled programs are cached by expression text.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import celpy
import celpy.celtypes as celtypes


class CelError(Exception):
    """A CEL expression failed to compile or evaluate (e.g. division by zero)."""


_env = celpy.Environment()


@lru_cache(maxsize=2048)
def _program(expr: str) -> celpy.Runner:
    try:
        return _env.program(_env.compile(expr))
    except celpy.CELEvalError as exc:  # type: ignore[attr-defined]
        raise CelError(f"compile error: {expr!r}: {exc}") from exc


def _to_cel(value: Any) -> Any:
    if isinstance(value, dict):
        return celtypes.MapType({k: _to_cel(v) for k, v in value.items()})
    if isinstance(value, list):
        return celtypes.ListType([_to_cel(v) for v in value])
    return value


def evaluate(expr: str, bindings: dict[str, Any]) -> Any:
    """Evaluate a CEL expression against ``bindings`` (esvs + event/id/parent)."""
    try:
        return _program(expr).evaluate(_to_cel(bindings))
    except celpy.CELEvalError as exc:  # type: ignore[attr-defined]
        raise CelError(str(exc)) from exc
