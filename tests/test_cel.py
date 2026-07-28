from __future__ import annotations

import pytest

from determa.state import CelError, cel


def test_error_absorbing_boolean_operators_are_commutative() -> None:
    assert cel.evaluate("false && (1 / 0 == 0)", {}) is False
    assert cel.evaluate("(1 / 0 == 0) && false", {}) is False
    assert cel.evaluate("true || (1 / 0 == 0)", {}) is True
    assert cel.evaluate("(1 / 0 == 0) || true", {}) is True


def test_integer_arithmetic_matches_portable_profile() -> None:
    assert cel.evaluate("-7 / 3", {}) == -2
    assert cel.evaluate("-7 % 3", {}) == -1
    with pytest.raises(CelError):
        cel.evaluate("9223372036854775807 + 1", {})


def test_unicode_is_not_normalized() -> None:
    assert cel.evaluate('size("\\u00e9")', {}) == 1
    assert cel.evaluate('size("e\\u0301")', {}) == 2
    assert cel.evaluate('"\\u00e9" == "e\\u0301"', {}) is False


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("integer_value + 1", cel.INT),
        ("double(integer_value)", cel.FLOAT),
        ("int(floating_value)", cel.INT),
        ("string(true)", cel.STRING),
        ("string(integer_value)", cel.STRING),
        ("string(floating_value)", cel.STRING),
        ("string(text)", cel.STRING),
        ("size(text)", cel.INT),
        ("size(numbers)", cel.INT),
        ("integer_value in numbers", cel.BOOL),
        ("has(attributes.present)", cel.BOOL),
        ("has(event.payload.optional)", cel.BOOL),
        ("event.payload.required == text", cel.BOOL),
        ("reference == null", cel.BOOL),
        ("reference != other_reference", cel.BOOL),
        (
            "flag ? reference : null",
            cel.StaticType("instance_reference", machine_id="worker", nullable=True),
        ),
    ],
)
def test_static_checker_accepts_only_declared_profile_overloads(
    expression: str, expected: cel.StaticType
) -> None:
    scope = {
        "integer_value": cel.INT,
        "floating_value": cel.FLOAT,
        "text": cel.STRING,
        "flag": cel.BOOL,
        "numbers": cel.StaticType("list", element=cel.INT),
        "attributes": cel.MAP,
        "reference": cel.StaticType(
            "instance_reference", machine_id="worker", nullable=True
        ),
        "other_reference": cel.StaticType(
            "instance_reference", machine_id="worker", nullable=True
        ),
    }
    event_fields = {"required": cel.STRING, "optional": cel.STRING}

    assert cel.check_expression(
        expression,
        scope,
        expected=expected,
        event_fields=event_fields,
    ) == expected


@pytest.mark.parametrize(
    "expression",
    [
        "integer_value != floating_value",
        "integer_value + floating_value",
        "integer_value < floating_value",
        "string(null)",
        "int(integer_value)",
        "double(floating_value)",
        "size(integer_value)",
        "matches(text, 'x')",
        "text.startsWith('x')",
        "numbers.map(value, value)",
        "uint(1)",
        "b'bytes'",
        "reference.instance_id",
        "string(reference)",
        "reference < other_reference",
        "has(text)",
        "has(owner.variables.text)",
        "event.event_id",
        "event.payload.missing",
        "owner.missing",
    ],
)
def test_static_checker_rejects_unavailable_symbols_and_overloads(expression: str) -> None:
    scope = {
        "integer_value": cel.INT,
        "floating_value": cel.FLOAT,
        "text": cel.STRING,
        "numbers": cel.StaticType("list", element=cel.INT),
        "reference": cel.StaticType(
            "instance_reference", machine_id="worker", nullable=True
        ),
        "other_reference": cel.StaticType(
            "instance_reference", machine_id="worker", nullable=True
        ),
    }

    with pytest.raises(cel.CelProfileError):
        cel.check_expression(
            expression,
            scope,
            expected=None,
            event_fields={"known": cel.STRING},
            owner_fields={"text": cel.STRING},
        )


@pytest.mark.parametrize(
    "expression",
    ["missing_name"],
)
def test_static_checker_rejects_unknown_activation_names_and_fields(expression: str) -> None:
    with pytest.raises(cel.CelTypeError):
        cel.check_expression(
            expression,
            {},
            expected=None,
            event_fields={"known": cel.STRING},
            owner_fields={"known": cel.STRING},
        )


def test_static_checker_does_not_flow_dynamic_values_to_concrete_destinations() -> None:
    with pytest.raises(cel.CelTypeError):
        cel.check_expression(
            "attributes['value']",
            {"attributes": cel.MAP},
            expected=cel.STRING,
        )


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("string(true)", "true"),
        ("string(false)", "false"),
        ("string(1.0)", "1"),
        ("string(-0.0)", "0"),
        ("string(1e-7)", "1e-7"),
        ("string(1e-6)", "0.000001"),
        ("string(1e20)", "100000000000000000000"),
        ("string(1e21)", "1e+21"),
        ("string(333333333.33333329)", "333333333.3333333"),
        ("string(4.50)", "4.5"),
        ("string(2e-3)", "0.002"),
        ("string(1e-27)", "1e-27"),
        ('string("\\u00e9")', "\u00e9"),
        ("string(9223372036854775807)", "9223372036854775807"),
        ("int(1.9)", 1),
        ("int(-1.9)", -1),
        ("int(-9223372036854775808.0)", -(2**63)),
        ("double(9007199254740993)", 9007199254740992.0),
    ],
)
def test_portable_conversion_vectors(expression: str, expected: object) -> None:
    assert cel.evaluate(expression, {}) == expected


@pytest.mark.parametrize(
    "expression",
    [
        "int(9.223372036854776e18)",
        "int(1e1000)",
        "1e308 * 1e308",
    ],
)
def test_portable_conversion_and_double_results_are_checked(expression: str) -> None:
    with pytest.raises(CelError):
        cel.evaluate(expression, {})


def test_static_destination_checking_has_only_the_documented_numeric_widening() -> None:
    assert cel.check_expression("integer_value", {"integer_value": cel.INT}, expected=cel.FLOAT)
    with pytest.raises(cel.CelTypeError):
        cel.check_expression("floating_value", {"floating_value": cel.FLOAT}, expected=cel.INT)
    with pytest.raises(cel.CelTypeError):
        cel.check_expression("integer_value", {"integer_value": cel.INT}, expected=cel.STRING)
