"""Portable CEL profile checks and evaluation."""

from __future__ import annotations

import math
import re
from functools import lru_cache
from typing import TYPE_CHECKING, Any, cast

from .errors import CelError

if TYPE_CHECKING:
    import celpy

_celpy: Any = None
_celtypes: Any = None
_environment: Any = None
_INT_MIN = -(2**63)
_INT_MAX = 2**63 - 1
_ALLOWED_FUNCTIONS = frozenset({"size", "has", "double", "int", "string"})
_CEL_WORDS = frozenset({"true", "false", "null", "in"})


def _load() -> tuple[Any, Any, Any]:
    global _celpy, _celtypes, _environment
    if _celpy is None:
        import celpy as celpy_module
        import celpy.celtypes as celtypes_module

        _celpy = celpy_module
        _celtypes = celtypes_module
        _environment = celpy_module.Environment()
    return _celpy, _celtypes, _environment


@lru_cache(maxsize=4096)
def _program(expression: str) -> celpy.Runner:
    celpy_module, _, environment = _load()
    try:
        return cast("celpy.Runner", environment.program(environment.compile(expression)))
    except Exception as exc:
        raise CelError(f"invalid CEL expression: {expression}") from exc


def compile_expression(expression: str) -> None:
    """Parse an expression without evaluating it."""
    _program(expression)


def _without_strings(expression: str) -> str:
    pattern = r"""(?s)'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*\""""
    return re.sub(pattern, " ", expression)


def profile_error(expression: str, instance_reference_names: set[str] | None = None) -> bool:
    """Return whether an expression uses a construct outside the closed profile."""
    stripped = _without_strings(expression)
    if re.search(r"\.\s*[A-Za-z_][A-Za-z0-9_]*\s*\(", stripped):
        return True
    functions = set(re.findall(r"(?<![.\w])([A-Za-z_][A-Za-z0-9_]*)\s*\(", stripped))
    if functions - _ALLOWED_FUNCTIONS:
        return True
    if re.search(r"\b(?:uint|bytes|timestamp|duration)\s*\(", stripped):
        return True
    if re.search(r"\bint\s*\(\s*['\"]", expression):
        return True
    if re.search(r"\bevent\.(?!payload\b)", stripped):
        return True
    if re.search(r"\bowner\.(?!variables\b)", stripped):
        return True
    if re.search(r"\b[0-9]+\s*(?:==|!=|<=|>=|<|>|\+|-|\*|/|%)\s*[0-9]+\.[0-9]", stripped):
        return True
    if re.search(r"\b[0-9]+\.[0-9]\s*(?:==|!=|<=|>=|<|>|\+|-|\*|/|%)\s*[0-9]+\b", stripped):
        return True
    for name in instance_reference_names or set():
        if re.search(rf"\b{re.escape(name)}\s*\.", stripped):
            return True
        if re.search(rf"\bstring\s*\(\s*{re.escape(name)}\s*\)", stripped):
            return True
    return False


def referenced_names(expression: str) -> set[str]:
    """Conservatively collect bare activation identifiers."""
    stripped = _without_strings(expression)
    names = set(re.findall(r"(?<![.\w])([A-Za-z_][A-Za-z0-9_]*)\b", stripped))
    functions = set(re.findall(r"(?<![.\w])([A-Za-z_][A-Za-z0-9_]*)\s*\(", stripped))
    return names - functions - _CEL_WORDS


def infer_type(
    expression: str,
    scope: dict[str, str],
    *,
    event_fields: dict[str, str] | None = None,
    owner_fields: dict[str, str] | None = None,
) -> str:
    """Infer the portable type for the expression shapes used by format 1."""
    expr = expression.strip()
    if expr in scope:
        return scope[expr]
    event_match = re.fullmatch(r"event\.payload\.([A-Za-z_][A-Za-z0-9_]*)", expr)
    if event_match and event_fields is not None:
        return event_fields.get(event_match.group(1), "unknown")
    owner_match = re.fullmatch(r"owner\.variables\.([A-Za-z_][A-Za-z0-9_]*)", expr)
    if owner_match and owner_fields is not None:
        return owner_fields.get(owner_match.group(1), "unknown")
    if expr in {"true", "false"}:
        return "bool"
    if expr == "null":
        return "null"
    if re.fullmatch(r"-?(?:0|[1-9][0-9]*)", expr):
        return "int"
    if re.fullmatch(r"-?(?:0|[1-9][0-9]*)\.[0-9]+", expr):
        return "float"
    if re.fullmatch(r"""'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*\"""", expr):
        return "string"
    if expr.startswith("[") and expr.endswith("]"):
        return "list"
    if expr.startswith("{") and expr.endswith("}"):
        return "map"
    if re.match(r"^(?:size|int)\s*\(", expr):
        return "int"
    if re.match(r"^double\s*\(", expr):
        return "float"
    if re.match(r"^string\s*\(", expr):
        return "string"
    ternary = re.match(r"^.+\?(.+):(.+)$", expr)
    if ternary:
        left = infer_type(
            ternary.group(1).strip(),
            scope,
            event_fields=event_fields,
            owner_fields=owner_fields,
        )
        right = infer_type(
            ternary.group(2).strip(),
            scope,
            event_fields=event_fields,
            owner_fields=owner_fields,
        )
        return left if left == right else "unknown"
    if re.search(r"\[[^\]]+\]\s*$", expr):
        return "unknown"
    if (
        "==" in expr
        or "!=" in expr
        or re.search(r"(?:<=|>=|<|>)", expr)
        or "&&" in expr
        or "||" in expr
        or expr.startswith("!")
        or expr.startswith("has(")
        or re.search(r"\bin\b", expr)
    ):
        return "bool"
    for name, type_name in scope.items():
        if re.search(rf"\b{re.escape(name)}\b", expr):
            return type_name
    return "unknown"


def _to_cel(value: Any) -> Any:
    _, celtypes, _ = _load()
    if value is None:
        return None
    if isinstance(value, bool):
        return celtypes.BoolType(value)
    if isinstance(value, int):
        return celtypes.IntType(value)
    if isinstance(value, float):
        return celtypes.DoubleType(value)
    if isinstance(value, str):
        return celtypes.StringType(value)
    if isinstance(value, dict):
        return celtypes.MapType(
            {celtypes.StringType(key): _to_cel(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return celtypes.ListType([_to_cel(item) for item in value])
    raise CelError(f"unsupported CEL value: {type(value).__name__}")


def _from_cel(value: Any) -> Any:
    _, celtypes, _ = _load()
    if isinstance(value, celtypes.BoolType):
        return bool(value)
    if isinstance(value, (celtypes.IntType, celtypes.UintType)):
        integer = int(value)
        if not _INT_MIN <= integer <= _INT_MAX:
            raise CelError("integer overflow")
        return integer
    if isinstance(value, celtypes.DoubleType):
        double = float(value)
        if not math.isfinite(double):
            raise CelError("non-finite double")
        return 0.0 if double == 0.0 else double
    if isinstance(value, celtypes.StringType):
        return str(value)
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = _from_cel(key)
            if not isinstance(normalized_key, str):
                raise CelError("map key is not a string")
            result[normalized_key] = _from_cel(item)
        return result
    if isinstance(value, list):
        return [_from_cel(item) for item in value]
    if value is None or isinstance(value, bool | int | float | str):
        return value
    raise CelError(f"unsupported CEL result: {type(value).__name__}")


def evaluate(expression: str, bindings: dict[str, Any]) -> Any:
    """Evaluate one expression using only the explicit activation."""
    celpy_module, _, _ = _load()
    try:
        return _from_cel(_program(expression).evaluate(_to_cel(bindings)))
    except CelError:
        raise
    except (celpy_module.CELEvalError, ValueError, TypeError, KeyError) as exc:
        raise CelError(str(exc)) from exc
