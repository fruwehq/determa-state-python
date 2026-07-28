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
        ("meta: { value: +1 }", "invalid_numeric_syntax"),
        ("meta: { value: True }", "invalid_boolean_syntax"),
        ("meta: { value: NULL }", "invalid_null_syntax"),
        ("meta: &anchor { value: 1 }", "unsupported_yaml_feature"),
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
