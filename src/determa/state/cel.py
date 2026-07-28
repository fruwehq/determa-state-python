"""Portable CEL profile checks and evaluation."""

from __future__ import annotations

import ast
import math
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
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


class CelProfileError(CelError):
    """An expression uses a symbol or overload outside the portable profile."""


class CelTypeError(CelError):
    """An expression has an invalid activation, field, or destination type."""


@dataclass(frozen=True)
class StaticType:
    """One load-time type in the closed portable CEL profile."""

    kind: str
    element: StaticType | None = None
    fields: tuple[tuple[str, StaticType], ...] = ()
    record_name: str | None = None
    machine_id: str | None = None
    nullable: bool = False

    def field(self, name: str) -> StaticType | None:
        return dict(self.fields).get(name)


NULL = StaticType("null")
BOOL = StaticType("bool")
INT = StaticType("int")
FLOAT = StaticType("float")
STRING = StaticType("string")
DYNAMIC = StaticType("dynamic")
LIST = StaticType("list", element=DYNAMIC)
MAP = StaticType("map", element=DYNAMIC)


def type_from_name(name: str) -> StaticType:
    """Return the portable static type for one schema type name."""
    return {
        "bool": BOOL,
        "int": INT,
        "float": FLOAT,
        "string": STRING,
        "list": LIST,
        "map": MAP,
        "instance_reference": StaticType("instance_reference"),
    }[name]


def _merge_elements(types: list[StaticType]) -> StaticType:
    if not types:
        return DYNAMIC
    first = types[0]
    return first if all(item == first for item in types[1:]) else DYNAMIC


def _literal_type(value: Any) -> StaticType:
    if value is None:
        return NULL
    if isinstance(value, bool):
        return BOOL
    if isinstance(value, int):
        return INT
    if isinstance(value, float):
        return FLOAT
    if isinstance(value, str):
        return STRING
    if isinstance(value, list):
        return StaticType("list", element=_merge_elements([_literal_type(item) for item in value]))
    if isinstance(value, dict):
        fields = tuple((str(name), _literal_type(item)) for name, item in value.items())
        return StaticType(
            "map",
            element=_merge_elements([item_type for _, item_type in fields]),
            fields=fields,
        )
    return DYNAMIC


def type_from_declaration(
    declaration: Mapping[str, Any], *, refine_container: bool = False
) -> StaticType:
    """Build a static type, retaining safe literal container refinements."""
    kind = str(declaration["type"])
    if kind == "instance_reference":
        return StaticType(
            kind,
            machine_id=(
                str(declaration["machine_id"]) if declaration.get("machine_id") else None
            ),
            nullable=bool(declaration.get("nullable")),
        )
    declared = type_from_name(kind)
    if refine_container and kind in {"list", "map"} and "init" in declaration:
        literal = _literal_type(declaration["init"])
        if literal.kind == kind:
            return literal
    if refine_container and kind in {"list", "map"} and "default" in declaration:
        literal = _literal_type(declaration["default"])
        if literal.kind == kind:
            return literal
    return declared


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
def _tree(expression: str) -> Any:
    _, _, environment = _load()
    try:
        return environment.compile(expression)
    except Exception as exc:
        raise CelError(f"invalid CEL expression: {expression}") from exc


@lru_cache(maxsize=4096)
def _program(expression: str) -> celpy.Runner:
    _, _, environment = _load()
    try:
        return cast(
            "celpy.Runner",
            environment.program(
                _tree(expression),
                functions={
                    "double": _portable_double,
                    "int": _portable_int,
                    "string": _portable_string,
                },
            ),
        )
    except Exception as exc:
        raise CelError(f"invalid CEL expression: {expression}") from exc


def compile_expression(expression: str) -> None:
    """Parse an expression without evaluating it."""
    _tree(expression)


def _rule(node: Any) -> str:
    return str(node.data)


def _expect(condition: bool, message: str = "incompatible CEL type") -> None:
    if not condition:
        raise CelTypeError(message)


def _profile(condition: bool, message: str = "CEL overload is outside the profile") -> None:
    if not condition:
        raise CelProfileError(message)


def _assignable(actual: StaticType, expected: StaticType) -> bool:
    if actual.kind == "dynamic":
        return False
    if actual.kind == expected.kind:
        if actual.kind != "instance_reference":
            return True
        return expected.machine_id is None or actual.machine_id == expected.machine_id
    if expected.kind == "float" and actual.kind == "int":
        return True
    if (
        expected.kind == "instance_reference"
        and expected.nullable
        and actual.kind == "null"
    ):
        return True
    return False


def _references_compatible(left: StaticType, right: StaticType) -> bool:
    if left.kind == "null" and right.kind == "null":
        return True
    if left.kind == "null":
        return right.kind == "instance_reference" and right.nullable
    if right.kind == "null":
        return left.kind == "instance_reference" and left.nullable
    if left.kind != "instance_reference" or right.kind != "instance_reference":
        return False
    return (
        left.machine_id is None
        or right.machine_id is None
        or left.machine_id == right.machine_id
    )


def _common_type(left: StaticType, right: StaticType) -> StaticType | None:
    if left == right:
        return left
    if _references_compatible(left, right):
        reference = right if left.kind == "null" else left
        machine_id = reference.machine_id
        if left.kind == right.kind == "instance_reference":
            machine_id = left.machine_id if left.machine_id == right.machine_id else None
        return StaticType(
            "instance_reference",
            machine_id=machine_id,
            nullable=reference.nullable or left.kind == "null" or right.kind == "null",
        )
    return None


def _unwrap(node: Any) -> Any:
    wrappers = {
        "expr",
        "conditionalor",
        "conditionaland",
        "relation",
        "addition",
        "multiplication",
        "unary",
        "member",
        "primary",
        "paren_expr",
    }
    while (
        hasattr(node, "children")
        and _rule(node) in wrappers
        and len(node.children) == 1
    ):
        child = node.children[0]
        if not hasattr(child, "data"):
            break
        node = child
    return node


def _arguments(node: Any) -> list[Any]:
    if len(node.children) == 1:
        return []
    expression_list = node.children[1]
    return list(expression_list.children)


def _literal(node: Any) -> StaticType:
    token = node.children[0]
    token_type = str(token.type)
    text = str(token)
    if token_type == "NULL_LIT":
        return NULL
    if token_type == "BOOL_LIT":
        return BOOL
    if token_type == "INT_LIT":
        try:
            value = int(text, 10)
        except ValueError as exc:
            raise CelTypeError("invalid int literal") from exc
        _expect(_INT_MIN <= value <= _INT_MAX, "int literal is outside signed 64-bit range")
        return INT
    if token_type == "FLOAT_LIT":
        try:
            double_value = float(text)
        except ValueError as exc:
            raise CelTypeError("invalid double literal") from exc
        _expect(math.isfinite(double_value), "double literal is not finite")
        return FLOAT
    if token_type == "STRING_LIT":
        return STRING
    raise CelProfileError(f"{token_type} literal is outside the portable profile")


class _Checker:
    def __init__(
        self,
        scope: Mapping[str, StaticType],
        event_fields: Mapping[str, StaticType] | None,
        owner_fields: Mapping[str, StaticType] | None,
    ) -> None:
        self.scope = scope
        self.event_fields = event_fields
        self.owner_fields = owner_fields

    def check(self, node: Any) -> StaticType:
        rule = _rule(node)
        method = getattr(self, f"_check_{rule}", None)
        if method is None:
            raise CelProfileError(f"CEL syntax {rule} is outside the portable profile")
        return cast(StaticType, method(node))

    def _single(self, node: Any) -> StaticType:
        _profile(len(node.children) == 1)
        return self.check(node.children[0])

    _check_member = _single
    _check_primary = _single

    def _check_expr(self, node: Any) -> StaticType:
        if len(node.children) == 1:
            return self.check(node.children[0])
        _profile(len(node.children) == 3)
        condition = self.check(node.children[0])
        _profile(condition.kind == "bool")
        selected = self.check(node.children[1])
        unselected = self.check(node.children[2])
        result = _common_type(selected, unselected)
        _profile(result is not None)
        assert result is not None
        return result

    def _check_boolean(self, node: Any) -> StaticType:
        _profile(len(node.children) == 2)
        _profile(self.check(node.children[0]).kind == "bool")
        _profile(self.check(node.children[1]).kind == "bool")
        return BOOL

    def _check_conditionalor(self, node: Any) -> StaticType:
        return self._check_boolean(node) if len(node.children) == 2 else self._single(node)

    def _check_conditionaland(self, node: Any) -> StaticType:
        return self._check_boolean(node) if len(node.children) == 2 else self._single(node)

    def _binary_operator(self, node: Any) -> tuple[str, StaticType, StaticType]:
        _profile(len(node.children) == 2)
        operator = node.children[0]
        _profile(len(operator.children) == 1)
        return (
            _rule(operator),
            self.check(operator.children[0]),
            self.check(node.children[1]),
        )

    def _equality(self, left: StaticType, right: StaticType) -> StaticType:
        if left.kind == "instance_reference" or right.kind == "instance_reference":
            _profile(_references_compatible(left, right))
        else:
            _profile(left.kind == right.kind)
        return BOOL

    def _ordered_relation(self, left: StaticType, right: StaticType) -> StaticType:
        _profile(left.kind == right.kind and left.kind in {"int", "float", "string"})
        return BOOL

    def _membership(self, left: StaticType, right: StaticType) -> StaticType:
        if right.kind == "list":
            if right.element is not None and right.element.kind != "dynamic":
                _profile(_assignable(left, right.element))
            return BOOL
        if right.kind == "map":
            _profile(left.kind == "string")
            return BOOL
        raise CelProfileError("in requires a list or string-keyed map")

    def _check_relation(self, node: Any) -> StaticType:
        if len(node.children) == 1:
            return self._single(node)
        operator, left, right = self._binary_operator(node)
        if operator in {"relation_eq", "relation_ne"}:
            return self._equality(left, right)
        if operator in {
            "relation_lt",
            "relation_le",
            "relation_gt",
            "relation_ge",
        }:
            return self._ordered_relation(left, right)
        if operator == "relation_in":
            return self._membership(left, right)
        raise CelProfileError(f"operator {operator} is outside the portable profile")

    def _addition(self, left: StaticType, right: StaticType) -> StaticType:
        _profile(left.kind == right.kind)
        _profile(left.kind in {"int", "float", "string", "list"})
        if left.kind == "list":
            element = _common_type(left.element or DYNAMIC, right.element or DYNAMIC)
            if element is None:
                element = DYNAMIC
            return StaticType("list", element=element)
        return left

    def _numeric(
        self, left: StaticType, right: StaticType, *, modulo: bool = False
    ) -> StaticType:
        permitted = {"int"} if modulo else {"int", "float"}
        _profile(left.kind == right.kind and left.kind in permitted)
        return left

    def _check_addition(self, node: Any) -> StaticType:
        if len(node.children) == 1:
            return self._single(node)
        operator, left, right = self._binary_operator(node)
        if operator == "addition_add":
            return self._addition(left, right)
        if operator == "addition_sub":
            return self._numeric(left, right)
        raise CelProfileError(f"operator {operator} is outside the portable profile")

    def _check_multiplication(self, node: Any) -> StaticType:
        if len(node.children) == 1:
            return self._single(node)
        operator, left, right = self._binary_operator(node)
        if operator in {"multiplication_mul", "multiplication_div"}:
            return self._numeric(left, right)
        if operator == "multiplication_mod":
            return self._numeric(left, right, modulo=True)
        raise CelProfileError(f"operator {operator} is outside the portable profile")

    def _check_unary_not(self, node: Any) -> StaticType:
        _profile(len(node.children) == 0)
        return BOOL

    def _check_unary_neg(self, node: Any) -> StaticType:
        _profile(len(node.children) == 0)
        return StaticType("unary_negation_marker")

    def _check_unary(self, node: Any) -> StaticType:
        if len(node.children) == 1:
            return self.check(node.children[0])
        _profile(len(node.children) == 2)
        operator = self.check(node.children[0])
        operand = self.check(node.children[1])
        if operator.kind == "bool":
            _profile(operand.kind == "bool")
            return BOOL
        _profile(operator.kind == "unary_negation_marker")
        _profile(operand.kind in {"int", "float"})
        return operand

    def _check_ident(self, node: Any) -> StaticType:
        name = str(node.children[0])
        if name == "event":
            _expect(self.event_fields is not None, "event is unavailable in this context")
            payload = StaticType(
                "record",
                fields=tuple(cast(Mapping[str, StaticType], self.event_fields).items()),
                record_name="event_payload",
            )
            return StaticType(
                "record", fields=(("payload", payload),), record_name="event"
            )
        if name == "owner":
            _expect(self.owner_fields is not None, "owner is unavailable in this context")
            variables = StaticType(
                "record",
                fields=tuple(cast(Mapping[str, StaticType], self.owner_fields).items()),
                record_name="owner_variables",
            )
            return StaticType(
                "record", fields=(("variables", variables),), record_name="owner"
            )
        result = self.scope.get(name)
        _expect(result is not None, f"unknown CEL activation name: {name}")
        return cast(StaticType, result)

    def _check_literal(self, node: Any) -> StaticType:
        return _literal(node)

    def _check_paren_expr(self, node: Any) -> StaticType:
        return self._single(node)

    def _check_list_lit(self, node: Any) -> StaticType:
        if not node.children:
            return LIST
        values = [self.check(item) for item in node.children[0].children]
        return StaticType("list", element=_merge_elements(values))

    def _check_map_lit(self, node: Any) -> StaticType:
        if not node.children:
            return MAP
        members = node.children[0].children
        fields: list[tuple[str, StaticType]] = []
        values: list[StaticType] = []
        for index in range(0, len(members), 2):
            key_node = members[index]
            key_type = self.check(key_node)
            _profile(key_type.kind == "string")
            value_type = self.check(members[index + 1])
            values.append(value_type)
            key = _string_literal_value(key_node)
            if key is not None:
                fields.append((key, value_type))
        return StaticType(
            "map", element=_merge_elements(values), fields=tuple(fields)
        )

    def _check_member_dot(self, node: Any) -> StaticType:
        base = self.check(node.children[0])
        name = str(node.children[1])
        if base.kind == "record":
            result = base.field(name)
            if result is None:
                raise CelProfileError(f"record field {name} is outside the portable profile")
            return result
        if base.kind == "map":
            return base.field(name) or base.element or DYNAMIC
        if base.kind == "instance_reference":
            raise CelProfileError("instance_reference is opaque")
        raise CelProfileError("field selection requires a record or map")

    def _check_member_index(self, node: Any) -> StaticType:
        _profile(len(node.children) == 2)
        base = self.check(node.children[0])
        index = self.check(node.children[1])
        if base.kind == "list":
            _profile(index.kind == "int")
            return base.element or DYNAMIC
        if base.kind == "map":
            _profile(index.kind == "string")
            return base.element or DYNAMIC
        raise CelProfileError("indexing requires a list or string-keyed map")

    def _check_member_dot_arg(self, node: Any) -> StaticType:
        raise CelProfileError("receiver methods are outside the portable profile")

    def _check_member_object(self, node: Any) -> StaticType:
        raise CelProfileError("object construction is outside the portable profile")

    def _check_ident_arg(self, node: Any) -> StaticType:
        name = str(node.children[0])
        arguments = _arguments(node)
        if name == "has":
            _profile(len(arguments) == 1)
            _profile(_is_permitted_has(arguments[0], self))
            return BOOL
        argument_types = [self.check(item) for item in arguments]
        _profile(len(argument_types) == 1)
        argument = argument_types[0]
        if name == "size":
            _profile(argument.kind in {"string", "list", "map"})
            return INT
        if name == "double":
            _profile(argument.kind == "int")
            return FLOAT
        if name == "int":
            _profile(argument.kind == "float")
            return INT
        if name == "string":
            _profile(argument.kind in {"bool", "int", "float", "string"})
            return STRING
        raise CelProfileError(f"function {name} is outside the portable profile")


def _string_literal_value(node: Any) -> str | None:
    unwrapped = _unwrap(node)
    if _rule(unwrapped) != "literal":
        return None
    token = unwrapped.children[0]
    if str(token.type) != "STRING_LIT":
        return None
    try:
        value = ast.literal_eval(str(token))
    except (SyntaxError, ValueError):
        return None
    return value if isinstance(value, str) else None


def _is_permitted_has(node: Any, checker: _Checker) -> bool:
    unwrapped = _unwrap(node)
    if _rule(unwrapped) != "member_dot":
        return False
    base_node = unwrapped.children[0]
    base = checker.check(base_node)
    field_name = str(unwrapped.children[1])
    if base.kind == "map":
        return True
    return base.record_name == "event_payload" and base.field(field_name) is not None


def check_expression(
    expression: str,
    scope: Mapping[str, StaticType],
    *,
    expected: StaticType | str | None,
    event_fields: Mapping[str, StaticType] | None = None,
    owner_fields: Mapping[str, StaticType] | None = None,
) -> StaticType:
    """Parse and completely type-check one expression against the closed profile."""
    checker = _Checker(scope, event_fields, owner_fields)
    actual = checker.check(_tree(expression))
    if expected is not None:
        destination = type_from_name(expected) if isinstance(expected, str) else expected
        _expect(_assignable(actual, destination))
    return actual


def check_map_literal(
    expression: str,
    expected_fields: Mapping[str, StaticType],
    *,
    scope: Mapping[str, StaticType],
    event_fields: Mapping[str, StaticType] | None,
) -> None:
    """Validate an exact string-keyed map literal and each destination value."""
    tree = _tree(expression)
    map_node = _unwrap(tree)
    if _rule(map_node) != "map_lit":
        raise CelTypeError("expected a CEL map literal")
    if not map_node.children:
        supplied: dict[str, Any] = {}
    else:
        members = map_node.children[0].children
        supplied = {}
        for index in range(0, len(members), 2):
            key = _string_literal_value(members[index])
            if key is None or key in supplied:
                raise CelTypeError("map literal keys must be unique string literals")
            supplied[key] = members[index + 1]
    if not supplied or set(supplied) - set(expected_fields):
        raise CelTypeError("map literal has invalid fields")
    checker = _Checker(scope, event_fields, None)
    for name, value_node in supplied.items():
        actual = checker.check(value_node)
        _expect(_assignable(actual, expected_fields[name]))


def _portable_double(value: Any) -> Any:
    _, celtypes, _ = _load()
    if not isinstance(value, celtypes.IntType):
        raise TypeError("double requires int")
    integer = int(value)
    if not _INT_MIN <= integer <= _INT_MAX:
        raise ValueError("integer is outside signed 64-bit range")
    result = float(integer)
    if not math.isfinite(result):
        raise ValueError("double conversion is not finite")
    return celtypes.DoubleType(0.0 if result == 0.0 else result)


def _portable_int(value: Any) -> Any:
    _, celtypes, _ = _load()
    if not isinstance(value, celtypes.DoubleType):
        raise TypeError("int requires double")
    double = float(value)
    if not math.isfinite(double):
        raise ValueError("double conversion is not finite")
    integer = math.trunc(double)
    if not _INT_MIN <= integer <= _INT_MAX:
        raise ValueError("integer conversion is outside signed 64-bit range")
    return celtypes.IntType(integer)


def _jcs_number(value: float) -> str:
    if not math.isfinite(value):
        raise ValueError("non-finite double")
    if value == 0.0:
        return "0"
    raw = repr(value).lower()
    magnitude = abs(value)
    if 1e-6 <= magnitude < 1e21:
        fixed = format(Decimal(raw), "f")
        if "." in fixed:
            fixed = fixed.rstrip("0").rstrip(".")
        return fixed
    if "e" not in raw:
        raw = format(Decimal(raw).normalize(), "e")
    mantissa, exponent_text = raw.split("e", 1)
    if "." in mantissa:
        mantissa = mantissa.rstrip("0").rstrip(".")
    exponent = int(exponent_text)
    sign = "+" if exponent >= 0 else ""
    return f"{mantissa}e{sign}{exponent}"


def _portable_string(value: Any) -> Any:
    _, celtypes, _ = _load()
    if isinstance(value, celtypes.BoolType):
        return celtypes.StringType("true" if bool(value) else "false")
    if isinstance(value, celtypes.IntType):
        integer = int(value)
        if not _INT_MIN <= integer <= _INT_MAX:
            raise ValueError("integer is outside signed 64-bit range")
        return celtypes.StringType(str(integer))
    if isinstance(value, celtypes.DoubleType):
        return celtypes.StringType(_jcs_number(float(value)))
    if isinstance(value, celtypes.StringType):
        return value
    raise TypeError("string requires bool, int, double, or string")


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
    if isinstance(value, int) and not isinstance(value, bool):
        if not _INT_MIN <= value <= _INT_MAX:
            raise CelError("integer overflow")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CelError("non-finite double")
        return 0.0 if value == 0.0 else value
    if value is None or isinstance(value, bool | str):
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
