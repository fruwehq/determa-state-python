"""YAML 1.2 core-schema loading (SPEC §2).

Determa State YAML MUST be parsed under the **YAML 1.2 core schema**, where only
``true``/``false`` (and capitalisations) are booleans. PyYAML defaults to YAML
1.1, in which ``yes``/``no``/``on``/``off``/``y``/``n`` are also booleans and
leading-zero / sexagesimal integers are parsed oddly. This module provides a
PyYAML loader that resolves scalars and constructs values strictly per the
YAML 1.2 core schema (https://yaml.org/spec/1.2.2/#102-core-schema).

Only the implicit resolvers and the int/float constructors are replaced; the
parser, composer, and (de)serialiser machinery are PyYAML's own.
"""

from __future__ import annotations

import re
from typing import Any

import yaml

# --- YAML 1.2 core-schema scalar patterns ----------------------------------
# Canonical regexes from the YAML 1.2 core schema resolution table.
_NULL_RE = re.compile(r"^(?:~|null|Null|NULL|)$")
_BOOL_RE = re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$")
_INT_RE = re.compile(r"^(?:[-+]?[0-9]+|[-+]?0o[0-7]+|[-+]?0x[0-9a-fA-F]+)$")
_FLOAT_RE = re.compile(
    r"^(?:"
    r"[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?"
    r"|[-+]?\.(?:inf|Inf|INF)"
    r"|\.(?:nan|NaN|NAN)"
    r")$"
)


class _CoreLoader(yaml.SafeLoader):
    """A SafeLoader whose implicit resolvers + int/float ctors are 1.2-core."""


# Replace the inherited (YAML 1.1) implicit-resolver table with a fresh one.
_CoreLoader.yaml_implicit_resolvers = {}

# Registration order matters: for a given first character, resolvers are tried
# in registration order and the first match wins.


def _add_resolver(tag: str, rx: re.Pattern[str], first: list[str]) -> None:
    _CoreLoader.add_implicit_resolver(tag, rx, first)  # type: ignore[no-untyped-call]


_add_resolver("tag:yaml.org,2002:null", _NULL_RE, list("~nN") + [""])
_add_resolver("tag:yaml.org,2002:bool", _BOOL_RE, list("tTfF"))
_add_resolver("tag:yaml.org,2002:int", _INT_RE, list("-+0123456789"))
_add_resolver("tag:yaml.org,2002:float", _FLOAT_RE, list("-+0123456789."))


def _construct_int(loader: yaml.Loader, node: yaml.ScalarNode) -> int:
    """YAML 1.2 core int: decimal, ``0o`` octal, ``0x`` hex. No leading-zero
    octal, no sexagesimals."""
    value = loader.construct_scalar(node)
    sign = ""
    if value[:1] in "+-":
        sign, value = value[0], value[1:]
    body = value.lower()
    if body[:2] == "0x":
        n = int(value, 16)
    elif body[:2] == "0o":
        n = int(value, 8)
    else:
        n = int(value, 10)
    return -n if sign == "-" else n


def _construct_float(loader: yaml.Loader, node: yaml.ScalarNode) -> float:
    """YAML 1.2 core float, including ``.inf``/``.nan`` forms."""
    value = loader.construct_scalar(node)
    lower = value.lower()
    if lower in {".inf", "+.inf"}:
        return float("inf")
    if lower == "-.inf":
        return float("-inf")
    if lower == ".nan":
        return float("nan")
    return float(value)


_CoreLoader.add_constructor("tag:yaml.org,2002:int", _construct_int)
_CoreLoader.add_constructor("tag:yaml.org,2002:float", _construct_float)


def load(text: str) -> Any:
    """Load a single YAML 1.2 document (``None`` if the stream is empty)."""
    return yaml.load(text, Loader=_CoreLoader)


def load_all(text: str) -> list[Any]:
    """Load all ``---``-separated YAML 1.2 documents (empty docs dropped)."""
    return [doc for doc in yaml.load_all(text, Loader=_CoreLoader) if doc is not None]
