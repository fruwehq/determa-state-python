# harel-python

Reference implementation (**Python**) of the [**harel**](https://github.com/fruwehq/harel)
statechart engine.

The normative `SPEC.md`, the JSON Schema for machine YAML, and the cross-language
**conformance suite** live in the spec repo. This repository implements that spec in
Python and is correct **iff it passes the conformance suite**.

Status: **passing the full conformance suite** — all 22 engine cases
(`conformance/01`–`22`) plus `conformance/cli/01`–`02`. Implements YAML 1.2 loading
+ validation, the full statechart semantics (RTC dispatch, hierarchy, orthogonal
regions + `done`, shallow/deep history, esvs, CEL guards, structured actions,
active objects + bus, defer, timers, faults), static contracts, snapshot
round-trip + safe-point migration, Mermaid `export`, and the §13 CLI. Built up
the build order in [issue #3][issue].

[issue]: https://github.com/fruwehq/harel-python/issues/3

## Conformance suite

The cross-language **conformance suite** is consumed as a pinned git submodule at
[`vendor/harel-conformance`](vendor/harel-conformance) (single source of truth — no
copy-paste drift); the harness in `tests/` discovers `conformance/*/` from there. The
normative `SPEC.md` and JSON Schema live in
[`fruwehq/harel`](https://github.com/fruwehq/harel), pinned at
[`vendor/harel`](vendor/harel) solely for the schema-drift check. This repository is
correct **iff it passes the suite**.

## Scope (per the spec)
- Load and validate machine YAML against `schema/machine.schema.json`, parsed under
  the **YAML 1.2 core schema** (only `true`/`false` are booleans).
- Execute statecharts per `SPEC.md`: run-to-completion; hierarchy; orthogonal regions
  (+ `done`); shallow/deep history; `initial` transitions; `esvs` (extended-state
  variables declared in states, hierarchical) including `external` esvs + the `env`
  event and `refresh`; `defer` (deferred-set, edge-triggered); timers via an injected
  clock; active-object spawning; `publish` (directed / by subscription / scoped); and
  faults (the `error` event).
- **Guards in CEL** (e.g. [`cel-python`](https://pypi.org/project/cel-python/));
  **structured actions** (`assign`/`publish`/`refresh`/`spawn`/`stop`) with CEL values.
- **Adapters** — bus / queue / clock / store / observer (SPEC §8), each with a simple
  in-memory default for tests.
- An **`export`** command that renders a machine (and an instance's current
  `state_config`) to **Mermaid** `stateDiagram-v2` (SPEC §12), behind a pluggable
  exporter interface so more formats (PlantUML, SCXML, …) can be added later.
- A test harness that runs the upstream conformance cases against this engine.

## Use as a library
The CLI (`harel …`) is a thin wrapper over a programmatic API; an engine can be
embedded in a host program **without** the CLI or the file-backed store (SPEC §2):

```python
import harel

defs = harel.load_definitions(open("gate.yaml").read())
harel.validate(defs[0].raw)                     # raises ValidationError if invalid

host = harel.Host()
host.register_all(defs)
inst = host.create_root(host.machines["gate"], "g1", external={"fare": 50})
host.run_to_quiescence()

host.deliver("g1", "coin", {"amount": 100})     # typed event; False if rejected
host.run_to_quiescence()
assert inst.active_leaf_names() == ["unlocked"]
assert inst.resolved_esvs()["fare"] == 50
assert inst.status is harel.Status.ACTIVE

host.advance("30s")                             # virtual clock
snaps = host.snapshot_all()                     # persist / round-trip (§8)
host.restore_all(snaps)
```

The public surface is everything exported from the top-level `harel` package
(`harel.__all__`): `Host`, `Instance`, `Definition`, `Machine`, `Status`, `Event`,
`load_definitions` / `load_definition`, `validate` / `collect_errors`, and the
error types. See [`tests/test_library_api.py`](tests/test_library_api.py).

## Layout
- `src/harel/` — the package.
- `tests/` — unit tests and the conformance harness.

## Develop
```
git submodule update --init      # fetch the conformance suite + schema (two submodules)
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
ruff check . && mypy src/harel && pytest
```

## License
MIT — see [LICENSE](LICENSE).
