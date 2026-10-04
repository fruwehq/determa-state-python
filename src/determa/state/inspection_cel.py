"""Bounded CEL guard interpreter used only for semantic inspection.

This interpreter consumes the checked portable expression tree and never calls the
ordinary CEL runner. Charges are abstract specification steps, before each operation.
"""

from __future__ import annotations

import ast
import math
from typing import Any

from . import cel


class InspectionLimit(Exception):
    """A guard exceeded a source, snapshot, tree, or step bound."""


def value_units(value: Any) -> int:
    if isinstance(value, str):
        return 1 + len(value)
    if isinstance(value, list):
        return len(value) + sum(value_units(item) for item in value)
    if isinstance(value, dict):
        return sum(1 + len(key) + value_units(item) for key, item in value.items())
    return 1


def _map_cost(value: dict[str, Any], key: str) -> int:
    return 1 + len(key) + sum(1 + len(item) for item in value)


def _checked_number(value: Any) -> Any:
    if type(value) is int and not -(2**63) <= value < 2**63:
        raise ValueError("integer overflow")
    if type(value) is float and not math.isfinite(value):
        raise ValueError("non-finite double")
    return value


def _portable_equal(left: Any, right: Any) -> bool:
    """Portable recursive equality keeps booleans distinct from integers."""
    if type(left) is not type(right):
        return False
    if isinstance(left, list):
        return len(left) == len(right) and all(
            _portable_equal(item, other) for item, other in zip(left, right, strict=True)
        )
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(
            _portable_equal(left[key], right[key]) for key in left
        )
    return bool(left == right)


class _Interpreter:
    def __init__(self, bindings: dict[str, Any], limit: int) -> None:
        self.bindings = bindings
        self.limit = limit
        self.spent = 0

    def charge(self, count: int = 1) -> None:
        if self.spent + count > self.limit:
            raise InspectionLimit
        self.spent += count

    def eval(self, node: Any) -> Any:
        node = cel._unwrap(node)
        kind = str(node.data)
        self.charge()  # Each evaluated checked AST node.
        children = node.children
        if kind == "literal":
            token = children[0]
            token_kind = str(token.type)
            source = str(token)
            if token_kind == "BOOL_LIT":
                return source == "true"
            if token_kind == "NULL_LIT":
                return None
            if token_kind == "STRING_LIT":
                result = ast.literal_eval(source)
                self.charge(len(result))
                return result
            if token_kind == "INT_LIT":
                return int(source)
            if token_kind == "FLOAT_LIT":
                return float(source)
            raise ValueError("unsupported literal")
        if kind == "ident":
            return self.bindings[str(children[0])]
        if kind == "expr":
            return self.eval(children[1] if self.eval(children[0]) else children[2])
        if kind in {"conditionaland", "conditionalor"}:
            left: Any = None
            left_error: Exception | None = None
            try:
                left = self.eval(children[0])
            except InspectionLimit:
                raise
            except Exception as error:
                left_error = error
            right: Any = None
            right_error: Exception | None = None
            try:
                right = self.eval(children[1])
            except InspectionLimit:
                raise
            except Exception as error:
                right_error = error
            self.charge()
            if kind == "conditionaland" and (left is False or right is False):
                return False
            if kind == "conditionalor" and (left is True or right is True):
                return True
            if left_error is not None:
                raise left_error
            if right_error is not None:
                raise right_error
            return (left and right) if kind == "conditionaland" else (left or right)
        if kind == "unary":
            operand = self.eval(children[1])
            self.charge()
            return (
                not operand if str(children[0].data) == "unary_not" else _checked_number(-operand)
            )
        if kind in {"relation", "addition", "multiplication"}:
            operator = str(children[0].data)
            left = self.eval(children[0].children[0])
            right = self.eval(children[1])
            if operator in {"relation_eq", "relation_ne"}:
                self.charge(
                    value_units(left) + value_units(right)
                    if isinstance(left, (list, dict))
                    else len(left) + len(right)
                    if isinstance(left, str)
                    else 1
                )
                result = _portable_equal(left, right)
                return result if operator == "relation_eq" else not result
            if operator.startswith("relation_"):
                if operator == "relation_in":
                    if isinstance(right, list):
                        self.charge(value_units(right) + value_units(left))
                    else:
                        self.charge(_map_cost(right, left))
                    return any(_portable_equal(left, item) for item in right)
                self.charge(len(left) + len(right) if isinstance(left, str) else 1)
                return {
                    "relation_lt": lambda: left < right,
                    "relation_le": lambda: left <= right,
                    "relation_gt": lambda: left > right,
                    "relation_ge": lambda: left >= right,
                }[operator]()
            if operator == "addition_add":
                result = left + right
                if isinstance(result, str):
                    self.charge(len(left) + len(right) + len(result))
                elif isinstance(result, list):
                    self.charge(len(left) + len(right) + len(result))
                else:
                    self.charge()
                return _checked_number(result)
            self.charge()
            return _checked_number(
                {
                    "addition_sub": lambda: left - right,
                    "multiplication_mul": lambda: left * right,
                    "multiplication_div": lambda: left / right,
                    "multiplication_mod": lambda: left % right,
                }[operator]()
            )
        if kind == "member_dot":
            base = self.eval(children[0])
            key = str(children[1])
            # event and its payload are typed records; ordinary variables are maps.
            record = self._is_typed_record(children[0])
            self.charge(1 + len(key) + (0 if record else sum(1 + len(k) for k in base)))
            return base[key]
        if kind == "member_index":
            base = self.eval(children[0])
            key = self.eval(children[1])
            if isinstance(base, list):
                self.charge()
            else:
                self.charge(_map_cost(base, key))
            return base[key]
        if kind == "list_lit":
            result = [self.eval(item) for item in children[0].children] if children else []
            self.charge(value_units(result))
            return result
        if kind == "map_lit":
            members = children[0].children if children else []
            result = {
                self.eval(members[index]): self.eval(members[index + 1])
                for index in range(0, len(members), 2)
            }
            self.charge(value_units(result))
            return result
        if kind == "ident_arg":
            name = str(children[0])
            argument = children[1].children[0]
            if name == "has":
                member = cel._unwrap(argument)
                base = self.eval(member.children[0])
                key = str(member.children[1])
                self.charge(
                    1 + len(key)
                    if self._is_typed_record(member.children[0])
                    else _map_cost(base, key)
                )
                return key in base
            value = self.eval(argument)
            if name == "size":
                self.charge(
                    value_units(value)
                    if isinstance(value, dict)
                    else len(value)
                    if isinstance(value, str)
                    else 1
                )
                return len(value)
            if name == "string":
                if type(value) is bool:
                    result = "true" if value else "false"
                elif type(value) is int:
                    result = str(value)
                elif type(value) is float:
                    result = cel._jcs_number(value)
                else:
                    result = value
                self.charge(len(value) if isinstance(value, str) else 1)
                if not isinstance(value, str):
                    self.charge(len(result))
                return result
            self.charge()
            if name == "int":
                return _checked_number(int(value))
            if name == "double":
                return _checked_number(float(value))
            raise ValueError("unrecognized function")
        raise ValueError(f"unsupported checked AST node {kind}")

    def _is_typed_record(self, node: Any) -> bool:
        unwrapped = cel._unwrap(node)
        if str(unwrapped.data) == "ident":
            return str(unwrapped.children[0]) in {"event", "owner"}
        if str(unwrapped.data) == "member_dot":
            base = cel._unwrap(unwrapped.children[0])
            return str(base.data) == "ident" and (
                str(base.children[0]),
                str(unwrapped.children[1]),
            ) in {("event", "payload"), ("owner", "variables")}
        return False


def _node_count(node: Any) -> int:
    if not hasattr(node, "data"):
        return 0
    unwrapped = cel._unwrap(node)
    if str(unwrapped.data).startswith(("unary_", "relation_", "addition_", "multiplication_")):
        return sum(_node_count(child) for child in unwrapped.children)
    return 1 + sum(_node_count(child) for child in unwrapped.children)


def safe_evaluate(
    expression: str, bindings: dict[str, Any], limit: int, *, snapshot_units: int | None = None
) -> tuple[bool, int]:
    """Evaluate a portable guard under the shared source, snapshot and fuel caps."""
    if len(expression.encode("utf-8")) > 4096 or (snapshot_units or value_units(bindings)) > 65536:
        raise InspectionLimit
    tree = cel._tree(expression)
    if _node_count(tree) > 1024:
        raise InspectionLimit
    interpreter = _Interpreter(bindings, limit)
    result = interpreter.eval(tree)
    if type(result) is not bool:
        raise ValueError("guard result is not Boolean")
    if isinstance(result, float) and not math.isfinite(result):
        raise ValueError("invalid number")
    return result, interpreter.spent
