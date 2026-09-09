"""Strict portable YAML/JSON source parsing from specification section 2."""

from __future__ import annotations

import math
import re
from typing import Any

import yaml

from .codes import MachineLoadFailureCode as LoadCode
from .errors import ValidationError

_JSON_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z")
_INTEGER = re.compile(r"-?(?:0|[1-9][0-9]*)\Z")
_NONPORTABLE_YAML_NUMBER = re.compile(
    r"""
    [+-]?(?:
        0[xX][0-9a-fA-F_]+
        |0[oO][0-7_]+
        |(?:[0-9][0-9_]*)(?:\.[0-9_]*)?(?:[eE][+-]?[0-9_]+)?
        |\.[0-9][0-9_]*(?:[eE][+-]?[0-9_]+)?
        |\.(?:inf|nan)
    )\Z
    """,
    re.IGNORECASE | re.VERBOSE,
)
_INVALID_BOOLEAN = frozenset({"True", "TRUE", "False", "FALSE"})
_INVALID_NULL = frozenset({"Null", "NULL", "~", ""})
_STRING_BOOLEAN_LIKE = frozenset(
    value
    for word in ("yes", "no", "on", "off", "y", "n")
    for value in (word, word.upper(), word.title())
)
_INT_MIN = -(2**63)
_INT_MAX = 2**63 - 1


def _has_invalid_unicode(value: str) -> bool:
    return any(0xD800 <= ord(char) <= 0xDFFF for char in value)


def validate_unicode(value: Any) -> bool:
    """Return whether every recursively contained string is a Unicode scalar sequence."""
    return _validate_unicode(value, set())


def _validate_unicode(value: Any, ancestors: set[int]) -> bool:
    if isinstance(value, str):
        return not _has_invalid_unicode(value)
    if isinstance(value, list):
        identity = id(value)
        if identity in ancestors:
            return False
        ancestors.add(identity)
        valid = all(_validate_unicode(item, ancestors) for item in value)
        ancestors.remove(identity)
        return valid
    if isinstance(value, dict):
        identity = id(value)
        if identity in ancestors:
            return False
        ancestors.add(identity)
        valid = all(
            isinstance(key, str)
            and _validate_unicode(key, ancestors)
            and _validate_unicode(item, ancestors)
            for key, item in value.items()
        )
        ancestors.remove(identity)
        return valid
    return True


def validate_portable_values(value: Any) -> None:
    """Reject host values outside the portable JSON scalar domain."""
    _validate_portable_values(value, set())


def normalize_portable_values(value: Any) -> Any:
    """Copy a portable host value and normalize every binary64 negative zero."""
    validate_portable_values(value)
    return _normalize_portable_values(value)


def _normalize_portable_values(value: Any) -> Any:
    if isinstance(value, float):
        return 0.0 if value == 0.0 else value
    if isinstance(value, list):
        return [_normalize_portable_values(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalize_portable_values(item) for key, item in value.items()}
    return value


def _validate_portable_values(value: Any, ancestors: set[int]) -> None:
    if value is None or isinstance(value, (str, bool)):
        return
    if isinstance(value, int):
        if not _INT_MIN <= value <= _INT_MAX:
            raise ValidationError(LoadCode.NUMERIC_VALUE_OUT_OF_RANGE)
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError(LoadCode.NUMERIC_VALUE_OUT_OF_RANGE)
        return
    if isinstance(value, list):
        identity = id(value)
        if identity in ancestors:
            raise ValidationError(LoadCode.NON_JSON_VALUE)
        ancestors.add(identity)
        for item in value:
            _validate_portable_values(item, ancestors)
        ancestors.remove(identity)
        return
    if isinstance(value, dict):
        identity = id(value)
        if identity in ancestors:
            raise ValidationError(LoadCode.NON_JSON_VALUE)
        ancestors.add(identity)
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValidationError(LoadCode.NON_STRING_MAP_KEY)
            _validate_portable_values(item, ancestors)
        ancestors.remove(identity)
        return
    raise ValidationError(LoadCode.NON_JSON_VALUE)


def _resolve_plain(value: str) -> Any:
    if value == "true":
        return True
    if value == "false":
        return False
    if value in _INVALID_BOOLEAN:
        raise ValidationError(LoadCode.INVALID_BOOLEAN_SYNTAX)
    if value == "null":
        return None
    if value in _INVALID_NULL:
        raise ValidationError(LoadCode.INVALID_NULL_SYNTAX)
    if value in _STRING_BOOLEAN_LIKE:
        return value
    if _JSON_NUMBER.fullmatch(value):
        if _INTEGER.fullmatch(value):
            integer = int(value, 10)
            if not _INT_MIN <= integer <= _INT_MAX:
                raise ValidationError(LoadCode.NUMERIC_VALUE_OUT_OF_RANGE)
            return integer
        try:
            double = float(value)
        except ValueError as exc:
            raise ValidationError(LoadCode.INVALID_NUMERIC_SYNTAX) from exc
        if not math.isfinite(double):
            raise ValidationError(LoadCode.NUMERIC_VALUE_OUT_OF_RANGE)
        return 0.0 if double == 0.0 else double
    if _NONPORTABLE_YAML_NUMBER.fullmatch(value):
        raise ValidationError(LoadCode.INVALID_NUMERIC_SYNTAX)
    return value


class _PortableLoader(yaml.BaseLoader):
    """A non-coercing loader with format-1 scalar and mapping construction."""


def _construct_scalar(loader: _PortableLoader, node: yaml.ScalarNode) -> Any:
    value = loader.construct_scalar(node)
    if _has_invalid_unicode(value):
        raise ValidationError(LoadCode.INVALID_UNICODE)
    if node.style is None:
        return _resolve_plain(value)
    return value


def _construct_mapping(
    loader: _PortableLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str):
            raise ValidationError(LoadCode.NON_STRING_MAP_KEY)
        if key in result:
            raise ValidationError(LoadCode.DUPLICATE_KEY)
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_PortableLoader.add_constructor("tag:yaml.org,2002:str", _construct_scalar)
_PortableLoader.add_constructor("tag:yaml.org,2002:map", _construct_mapping)


def _construct_sequence(loader: _PortableLoader, node: yaml.SequenceNode) -> list[Any]:
    return list(loader.construct_sequence(node))


_PortableLoader.add_constructor("tag:yaml.org,2002:seq", _construct_sequence)


def _reject_yaml_features(text: str) -> None:
    try:
        tokens = yaml.scan(text, Loader=_PortableLoader)
        for token in tokens:
            if isinstance(
                token, (yaml.tokens.AliasToken, yaml.tokens.AnchorToken, yaml.tokens.TagToken)
            ):
                raise ValidationError(LoadCode.UNSUPPORTED_YAML_FEATURE)
    except ValidationError:
        raise
    except (yaml.YAMLError, UnicodeError) as exc:
        raise ValidationError(LoadCode.NON_JSON_VALUE, message=str(exc)) from exc


def load(text: str) -> Any:
    """Parse exactly one portable format-1 source document."""
    if _has_invalid_unicode(text):
        raise ValidationError(LoadCode.INVALID_UNICODE)
    _reject_yaml_features(text)
    try:
        documents = list(yaml.load_all(text, Loader=_PortableLoader))
    except ValidationError:
        raise
    except (yaml.YAMLError, UnicodeError) as exc:
        raise ValidationError(LoadCode.NON_JSON_VALUE, message=str(exc)) from exc
    if len(documents) != 1:
        raise ValidationError(
            LoadCode.NON_JSON_VALUE,
            message="source must contain exactly one document",
        )
    document = documents[0]
    if not validate_unicode(document):
        raise ValidationError(LoadCode.INVALID_UNICODE)
    return document
