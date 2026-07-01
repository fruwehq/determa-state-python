"""Values crossing the boundary are canonical native/JSON types (SPEC §5.1).

CEL-produced values (assign RHS, published payloads, …) must not surface celpy wrapper
types (``IntType``/``DoubleType``/``BoolType``/``StringType``/``MapType``/``ListType``)
through the public API, snapshots, or `--json`.
"""

from __future__ import annotations

import json

from harel import Host, load_definitions

TYPES = """\
id: types
events:
  go: {}
top:
  esvs:
    i: { type: int, init: 0 }
    f: { type: float, init: 0.0 }
    b: { type: bool, init: false }
    s: { type: string, init: "" }
    m: { type: map, init: {} }
    l: { type: list, init: [] }
  initial: { transition_to: a }
  states:
    a:
      on_events:
        go:
          action:
            - { assign: { i: "1 + 2" } }
            - { assign: { f: "1.5 + 0.5" } }
            - { assign: { b: "1 < 2" } }
            - { assign: { s: "'a' + 'b'" } }
            - { assign: { m: "{'k': 1 + 1}" } }
            - { assign: { l: "[1, 2, 3]" } }
          transition_to: done_
    done_: {}
"""


def _run() -> object:
    host = Host()
    host.register_all(load_definitions(TYPES))
    inst = host.create_root(host.machines["types"], "r")
    host.run_to_quiescence()
    host.deliver("r", "go")
    host.run_to_quiescence()
    return inst


def test_cel_assignments_are_native_python() -> None:
    esvs = _run().resolved_esvs()
    assert type(esvs["i"]) is int
    assert type(esvs["f"]) is float
    assert type(esvs["b"]) is bool
    assert type(esvs["s"]) is str
    assert type(esvs["m"]) is dict
    assert type(esvs["l"]) is list
    # nested values too (no wrappers inside containers)
    assert type(esvs["m"]["k"]) is int and esvs["m"]["k"] == 2
    assert [type(x) for x in esvs["l"]] == [int, int, int]
    assert esvs["i"] == 3 and esvs["f"] == 2.0 and esvs["b"] is True and esvs["s"] == "ab"


def test_snapshot_contains_only_native_json_values() -> None:
    snap = _run().to_snapshot()
    json.dumps(snap)  # must be plain-serializable

    def _walk(v: object) -> None:
        # celpy wrappers subclass their natives, so json.dumps alone wouldn't catch them.
        assert "celpy" not in type(v).__module__, f"celpy type in snapshot: {type(v)}"
        if isinstance(v, dict):
            for k, val in v.items():
                _walk(k)
                _walk(val)
        elif isinstance(v, list):
            for x in v:
                _walk(x)

    _walk(snap)


def test_no_celpy_type_leaks_anywhere_in_esvs() -> None:
    esvs = _run().resolved_esvs()

    def _assert_native(v: object) -> None:
        assert type(v).__module__ == "builtins", f"non-native value leaked: {type(v)}"
        if isinstance(v, dict):
            for k, val in v.items():
                _assert_native(k)
                _assert_native(val)
        elif isinstance(v, list):
            for x in v:
                _assert_native(x)

    for val in esvs.values():
        _assert_native(val)
