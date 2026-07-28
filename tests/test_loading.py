from __future__ import annotations

import math
from pathlib import Path

import pytest

from determa.state import ValidationError, load_bundle

ROOT = Path(__file__).resolve().parent.parent

FINGERPRINT_BUNDLE = """
format: 1
namespace: example.turnstile
events:
  tick:
    payload:
      amount: { type: float, default: 1 }
meta:
  large_integer: 9007199254740993
  integer_one: 1
  floating_one: 1.0
machines:
  - machine_id: turnstile
    events:
      local_notice:
        payload:
          value: { type: int, required: true }
    root:
      type: composite
      variables:
        attempts: { type: int, init: 0 }
      initial: { transition_to: locked }
      states:
        locked:
          type: parallel
          components:
            - component_id: left
              root: {}
            - component_id: right
              root: {}
          on_events:
            tick:
              transition_to: unlocked
              action:
                - send:
                    event: local_notice
                    payload: { value: "1" }
        unlocked: {}
"""


def test_normative_bundle_fingerprint_vector() -> None:
    bundle = load_bundle(FINGERPRINT_BUNDLE)

    assert bundle.fingerprint == (
        "sha256:7e48ad82ea5305c24b7730f4fd24c36ec196a0875c982b85eba5b3a5ddcbb92f"
    )


@pytest.mark.parametrize(
    ("source_fragment", "code"),
    [
        ("meta: { value: 0x10 }", "invalid_numeric_syntax"),
        ("meta: { value: 0o10 }", "invalid_numeric_syntax"),
        ("meta: { value: 1_000 }", "invalid_numeric_syntax"),
        ("meta: { value: +1 }", "invalid_numeric_syntax"),
        ("meta: { value: 01 }", "invalid_numeric_syntax"),
        ("meta: { value: .5 }", "invalid_numeric_syntax"),
        ("meta: { value: 1. }", "invalid_numeric_syntax"),
        ("meta: { value: .inf }", "invalid_numeric_syntax"),
        ("meta: { value: .NaN }", "invalid_numeric_syntax"),
        ("meta: { value: True }", "invalid_boolean_syntax"),
        ("meta: { value: NULL }", "invalid_null_syntax"),
        ("meta: { value: }", "invalid_null_syntax"),
        ("meta: &anchor { value: 1 }", "unsupported_yaml_feature"),
        ("meta: { value: !!str tagged }", "unsupported_yaml_feature"),
    ],
)
def test_nonportable_source_scalars_fail_before_schema(source_fragment: str, code: str) -> None:
    source = f"""
format: 1
namespace: example.scalar
{source_fragment}
machines:
  - machine_id: scalar
    root: {{}}
"""

    with pytest.raises(ValidationError, match=code) as caught:
        load_bundle(source)

    assert caught.value.code == code


@pytest.mark.parametrize(
    "value",
    [
        "1alpha",
        "1.2.3",
        "1e",
        ".release",
        "+release",
        "0xrelease",
    ],
)
def test_numeric_looking_plain_strings_remain_strings(value: str) -> None:
    bundle = load_bundle(
        f"""
format: 1
namespace: example.numeric_string
meta: {{ release: {value} }}
machines:
  - machine_id: numeric_string
    root: {{}}
"""
    )

    assert bundle.raw["meta"]["release"] == value
    assert type(bundle.raw["meta"]["release"]) is str


@pytest.mark.parametrize(
    "value",
    [
        "0x10",
        "0o10",
        "1_000",
        "+1",
        "01",
        ".5",
        "1.",
        ".inf",
        ".nan",
        "true",
        "null",
    ],
)
def test_quoted_scalar_forms_remain_strings(value: str) -> None:
    bundle = load_bundle(
        f"""
format: 1
namespace: example.quoted_scalar
meta: {{ value: "{value}" }}
machines:
  - machine_id: quoted_scalar
    root: {{}}
"""
    )

    assert bundle.raw["meta"]["value"] == value
    assert type(bundle.raw["meta"]["value"]) is str


def test_empty_mapping_and_sequence_are_values_but_empty_scalar_is_not() -> None:
    bundle = load_bundle(
        """
format: 1
namespace: example.empty_containers
meta:
  mapping: {}
  sequence: []
machines:
  - machine_id: empty_containers
    root: {}
"""
    )

    assert bundle.raw["meta"] == {"mapping": {}, "sequence": []}


def test_alias_tag_non_string_key_and_surrogate_source_forms_are_rejected() -> None:
    fragments = [
        "meta: { first: &value one, second: *value }",
        "meta: { value: !custom one }",
        "meta: { 1: one }",
        'meta: { value: "\\uD800" }',
    ]
    expected = [
        "unsupported_yaml_feature",
        "unsupported_yaml_feature",
        "non_string_map_key",
        "invalid_unicode",
    ]

    for fragment, code in zip(fragments, expected, strict=True):
        with pytest.raises(ValidationError) as caught:
            load_bundle(
                f"""
format: 1
namespace: example.invalid_source
{fragment}
machines:
  - machine_id: invalid_source
    root: {{}}
"""
            )
        assert caught.value.code == code


def test_duplicate_keys_do_not_use_last_value_wins() -> None:
    source = """
format: 1
namespace: example.duplicate
namespace: example.overwrite
machines:
  - machine_id: duplicate
    root: {}
"""

    with pytest.raises(ValidationError) as caught:
        load_bundle(source)

    assert caught.value.code == "duplicate_key"


def test_yaml_1_1_boolean_like_identifiers_remain_strings() -> None:
    bundle = load_bundle(
        """
format: 1
namespace: example.boolean_like
events:
  no: { direction: input }
machines:
  - machine_id: boolean_like
    root:
      type: composite
      initial: { transition_to: no }
      states:
        no: { on_events: { no: { transition_to: off } } }
        off: {}
"""
    )

    states = bundle.raw["machines"][0]["root"]["states"]
    assert list(states) == ["no", "off"]
    assert all(type(name) is str for name in states)


@pytest.mark.parametrize("value", [2**63, -(2**63) - 1, math.inf, math.nan, object()])
def test_native_mappings_use_the_same_portable_scalar_domain(value: object) -> None:
    document = {
        "format": 1,
        "namespace": "example.native",
        "meta": {"value": value},
        "machines": [{"machine_id": "native", "root": {}}],
    }

    with pytest.raises(ValidationError) as caught:
        load_bundle(document)

    assert caught.value.code in {"numeric_value_out_of_range", "non_json_value"}


def test_native_mapping_cycles_are_not_json_values() -> None:
    cycle: dict[str, object] = {}
    cycle["self"] = cycle
    document = {
        "format": 1,
        "namespace": "example.cycle",
        "meta": cycle,
        "machines": [{"machine_id": "cycle", "root": {}}],
    }

    with pytest.raises(ValidationError) as caught:
        load_bundle(document)

    assert caught.value.code == "non_json_value"


def test_checked_in_format_1_example_is_valid() -> None:
    bundle = load_bundle((ROOT / "examples" / "format-1.yaml").read_text(encoding="utf-8"))

    assert bundle.namespace == "example.counter"
    assert bundle.machine("counter") is not None


@pytest.mark.parametrize(
    ("guard", "assignment"),
    [
        ("integer_value != floating_value", "text"),
        ("true", "string(null)"),
    ],
)
def test_bundle_loading_rejects_invalid_cel_overload_types(
    guard: str, assignment: str
) -> None:
    source = f"""
format: 1
namespace: example.invalid_cel_types
events:
  go: {{ direction: input }}
machines:
  - machine_id: invalid_cel_types
    root:
      variables:
        integer_value: {{ type: int, init: 1 }}
        floating_value: {{ type: float, init: 1.0 }}
        text: {{ type: string, init: "" }}
      on_events:
        go:
          guard: "{guard}"
          action:
            - assign: {{ text: "{assignment}" }}
"""

    with pytest.raises(ValidationError) as caught:
        load_bundle(source)

    assert caught.value.code == "cel_profile_error"


def test_bundle_loading_accepts_checked_int_from_double_conversion() -> None:
    bundle = load_bundle(
        """
format: 1
namespace: example.valid_cel_conversion
events:
  go: { direction: input }
machines:
  - machine_id: valid_cel_conversion
    root:
      variables:
        integer_value: { type: int, init: 0 }
        floating_value: { type: float, init: 1.5 }
      on_events:
        go:
          action:
            - assign: { integer_value: "int(floating_value)" }
"""
    )

    assert bundle.machine("valid_cel_conversion") is not None


def test_mutable_container_literal_does_not_create_an_unsound_element_type() -> None:
    source = """
format: 1
namespace: example.mutable_container_type
events:
  replace: { direction: input }
machines:
  - machine_id: mutable_container_type
    root:
      variables:
        numbers: { type: list, init: [1] }
        selected: { type: int, init: 0 }
      on_events:
        replace:
          action:
            - assign: { numbers: "['not-an-integer']" }
            - assign: { selected: "numbers[0]" }
"""

    with pytest.raises(ValidationError) as caught:
        load_bundle(source)

    assert caught.value.code == "semantic_validation"


@pytest.mark.parametrize(
    "payload",
    [
        "",
        "payload: { optional_value: \"'provided'\" }",
    ],
)
def test_send_requires_every_required_payload_expression(payload: str) -> None:
    source = f"""
format: 1
namespace: example.required_send_payload
events:
  go: {{ direction: input }}
  notice:
    direction: internal
    payload:
      required_value: {{ type: string, required: true }}
      optional_value: {{ type: string }}
machines:
  - machine_id: required_send_payload
    root:
      on_events:
        go:
          action:
            - send:
                event: notice
                {payload}
"""

    with pytest.raises(ValidationError) as caught:
        load_bundle(source)

    assert caught.value.code == "semantic_validation"


@pytest.mark.parametrize(
    "payload_declaration",
    [
        "optional_value: { type: string }",
        "defaulted_value: { type: string, default: fallback }",
    ],
)
def test_send_allows_absent_optional_or_defaulted_payload_fields(
    payload_declaration: str,
) -> None:
    bundle = load_bundle(
        f"""
format: 1
namespace: example.optional_send_payload
events:
  go: {{ direction: input }}
  notice:
    direction: internal
    payload:
      {payload_declaration}
machines:
  - machine_id: optional_send_payload
    root:
      on_events:
        go:
          action:
            - send: {{ event: notice }}
"""
    )

    assert bundle.machine("optional_send_payload") is not None
