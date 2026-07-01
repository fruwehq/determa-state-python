# harel-python

Reference implementation (**Python**) of the [**harel**](https://github.com/fruwehq/harel)
statechart engine.

The normative `SPEC.md`, the JSON Schema for machine YAML, and the cross-language
**conformance suite** live in the spec repo. This repository implements that spec in
Python and is correct **iff it passes the conformance suite**.

Implements the **harel spec v0.0.1** (early alpha; all fruwehq harel repos share one
[synchronized version](https://github.com/fruwehq/harel)).

Status: **passing the full conformance suite** — all 22 engine cases
(`conformance/01`–`22`) plus `conformance/cli/01`–`02`. Implements YAML 1.2 loading
+ validation, the full statechart semantics (RTC dispatch, hierarchy, orthogonal
regions + `done`, shallow/deep history, esvs, CEL guards, structured actions,
active objects + bus, defer, timers, faults), static contracts, snapshot
round-trip + safe-point migration, Mermaid `export`, and the §13 CLI. Built up
the build order in [issue #3][issue].

[issue]: https://github.com/fruwehq/harel-python/issues/3

## Conformance suite

The cross-language **conformance suite** is the single source of truth for correctness;
this repository is correct **iff it passes it**. The suite lives in
[`fruwehq/harel-conformance`](https://github.com/fruwehq/harel-conformance); the test
harness **fetches it at the matching release tag** (`v0.0.1`) into a gitignored
`.cache/` — no git submodule. The normative `SPEC.md` and JSON Schema live in
[`fruwehq/harel`](https://github.com/fruwehq/harel); the schema-drift test fetches the
schema at the same tag.

For **offline** work, point the harness at a local checkout:
```
export HAREL_CONFORMANCE_DIR=/path/to/harel-conformance   # the suite
export HAREL_SPEC_DIR=/path/to/harel                        # the schema (optional)
```

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

### Observing transitions (SPEC §8)
Pass an **observer** — a passive callback invoked once per RTC step (automatic *or*
manual) with `{ instance, event, transition, entered, exited, published, spawned,
faulted }`. Built-ins: `JsonlObserver(stream)` (a drop-in transition log) and
`CollectingObserver` (records to a list).

```python
import sys, harel
host = harel.Host(observer=harel.JsonlObserver(sys.stdout))  # one JSON line per step
```

The Observer is *domain* observability (what the machine did). For *operational*
diagnostics the engine also emits **standard-library logging** under the `harel` logger
(dispatch/transition at `DEBUG`, faults/dead-letter at `WARNING`). It is silent by
default (a `NullHandler` is attached); enable it from the host app:

```python
import logging
logging.basicConfig(level=logging.DEBUG)   # or logging.getLogger("harel").setLevel(...)
```

## Layout
- `src/harel/` — the package.
- `tests/` — the implementation's own **unit tests** (hermetic, offline).
- `conformance/` — the harness that runs the external **conformance suite** black-box
  against this implementation (kept separate from the unit tests).

## Develop
```
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'

make check        # ruff + mypy + unit tests (hermetic, offline) — the PR gate
make conformance  # download & run the language-agnostic conformance suite
```
Equivalently: `pytest` runs the unit tests only; `pytest conformance` runs the
conformance suite (it fetches `harel-conformance` into `.cache/` on first run — set
`HAREL_CONFORMANCE_DIR` to use a local checkout offline). The two are **separate**:
unit tests never touch the network; conformance is opt-in.

## License
MIT — see [LICENSE](LICENSE).
