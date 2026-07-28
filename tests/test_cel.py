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
