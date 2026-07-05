"""YAML 1.2 core-schema resolution tests (SPEC §2).

PyYAML defaults to YAML 1.1 (yes/no/on/off are booleans, leading-zero octal,
sexagesimals). These pin the 1.2-core behaviour the engine relies on.
"""

from __future__ import annotations

import math

import pytest

from determa.state import yaml12

CASES = {
    # booleans — ONLY true/false (and capitalisations) are bool in 1.2 core.
    "true": True,
    "True": True,
    "TRUE": True,
    "false": False,
    "False": False,
    "FALSE": False,
    # 1.1 booleans must now be plain strings.
    "yes": "yes",
    "no": "no",
    "on": "on",
    "off": "off",
    "y": "y",
    "n": "n",
    "Y": "Y",
    "N": "N",
    # null forms.
    "null": None,
    "Null": None,
    "NULL": None,
    "~": None,
    "": None,
    # ints — plain decimal, no leading-zero octal, 0o/0x prefixes.
    "0": 0,
    "42": 42,
    "-7": -7,
    "+7": 7,
    "017": 17,  # NOT octal 15 (1.1 behaviour)
    "0o17": 15,
    "0x1F": 31,
    "-0x1F": -31,
    # floats.
    "3.14": 3.14,
    ".5": 0.5,
    "1.": 1.0,
    "1e3": 1000.0,
    "-2.5": -2.5,
    "0.0": 0.0,
    # plain date/time-like stays a string (no timestamp tag in 1.2 core).
    "2024-01-15": "2024-01-15",
    # sexagesimal is NOT an int in 1.2 core.
    "1:2:3": "1:2:3",
}


@pytest.mark.parametrize(("text", "expected"), sorted(CASES.items()))
def test_scalar_resolution(text: str, expected: object) -> None:
    assert yaml12.load(text) == expected


def test_inf_nan() -> None:
    assert math.isinf(yaml12.load(".inf"))
    assert yaml12.load(".inf") > 0
    assert yaml12.load("-.inf") < 0
    assert math.isnan(yaml12.load(".nan"))


def test_quoted_scalars_are_strings() -> None:
    assert yaml12.load("'true'") == "true"
    assert yaml12.load('"42"') == "42"
    assert yaml12.load("'yes'") == "yes"


def test_load_all_multidoc() -> None:
    docs = yaml12.load_all("id: a\n---\nid: b\n---\n# empty\n")
    assert [d["id"] for d in docs] == ["a", "b"]


def test_structures_roundtrip() -> None:
    doc = yaml12.load(
        "events:\n  coin: { payload: { amount: { type: int, required: true } } }\n"
        "list: [1, 2, yes]\n"
    )
    assert doc["events"]["coin"]["payload"]["amount"]["required"] is True
    assert doc["list"] == [1, 2, "yes"]
