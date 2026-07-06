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
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import celpy


class CelError(Exception):
    """A CEL expression failed to compile or evaluate (e.g. division by zero)."""


# celpy (with its lark + pendulum transitive deps) costs ~0.1s to import, but is only
# needed to *evaluate* an expression — not to load, inspect, snapshot, or step a machine.
# Import it lazily so the CLI and library stay fast on guard-free paths (SPEC §6).
_celpy: Any = None
_celtypes: Any = None
_env: Any = None


def _load() -> tuple[Any, Any, Any]:
    global _celpy, _celtypes, _env
    if _celpy is None:
        import celpy as celpy_mod
        import celpy.celtypes as celtypes_mod

        _celpy = celpy_mod
        _celtypes = celtypes_mod
        _env = celpy_mod.Environment()
    return _celpy, _celtypes, _env


@lru_cache(maxsize=2048)
def _program(expr: str) -> celpy.Runner:
    celpy_mod, _, env = _load()
    try:
        return cast("celpy.Runner", env.program(env.compile(expr)))
    except celpy_mod.CELEvalError as exc:
        raise CelError(f"compile error: {expr!r}: {exc}") from exc


def _to_cel(value: Any) -> Any:
    _, celtypes, _ = _load()
    if isinstance(value, dict):
        return celtypes.MapType({k: _to_cel(v) for k, v in value.items()})
    if isinstance(value, list):
        return celtypes.ListType([_to_cel(v) for v in value])
    return value


def _from_cel(value: Any) -> Any:
    """Normalize a celpy result to a canonical native/JSON Python value (SPEC §5.1).

    No guard-language wrapper type (``celpy.celtypes.*``) may cross the engine boundary,
    so every CEL result is coerced to its native equivalent here — the single choke point
    for esv assignments, published payloads, and spawn args.
    """
    _, celtypes, _ = _load()
    if isinstance(value, celtypes.BoolType):  # subclasses int — check before IntType
        return bool(value)
    if isinstance(value, (celtypes.IntType, celtypes.UintType)):
        return int(value)
    if isinstance(value, celtypes.DoubleType):
        return float(value)
    if isinstance(value, celtypes.StringType):
        return str(value)
    if isinstance(value, celtypes.BytesType):
        return bytes(value)
    if isinstance(value, dict):  # MapType (and native dict): normalize keys + values
        return {_from_cel(k): _from_cel(v) for k, v in value.items()}
    if isinstance(value, list):  # ListType (and native list)
        return [_from_cel(v) for v in value]
    return value


def evaluate(expr: str, bindings: dict[str, Any]) -> Any:
    """Evaluate a CEL expression, returning a canonical native/JSON value (§5.1)."""
    celpy_mod, _, _ = _load()
    try:
        return _from_cel(_program(expr).evaluate(_to_cel(bindings)))
    except celpy_mod.CELEvalError as exc:
        raise CelError(str(exc)) from exc
