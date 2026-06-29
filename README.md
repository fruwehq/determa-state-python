# harel-python

Reference implementation (**Python**) of the [**harel**](https://github.com/fruwehq/harel)
statechart engine.

The normative `SPEC.md`, the JSON Schema for machine YAML, and the cross-language
**conformance suite** live in the spec repo. This repository implements that spec in
Python and is correct **iff it passes the conformance suite**.

Status: **in progress** — YAML 1.2 loading + machine validation (SPEC §2/§4) are
implemented and gated against the full conformance suite. The engine is being
built up the build order in [issue #3][issue].

[issue]: https://github.com/fruwehq/harel-python/issues/3

## Conformance suite

The normative `SPEC.md`, JSON Schema, and cross-language **conformance suite**
are consumed as a pinned git submodule at [`vendor/harel`](vendor/harel)
(single source of truth — no copy-paste drift). The harness lives in `tests/`
and discovers `conformance/*/` from there. This repository is correct **iff it
passes the suite**.

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

## Layout
- `src/harel/` — the package.
- `tests/` — unit tests and the conformance harness.

## Develop
```
git submodule update --init      # fetch the conformance suite
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
ruff check . && mypy src/harel && pytest
```

## License
MIT — see [LICENSE](LICENSE).
