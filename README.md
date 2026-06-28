# harel-python

Reference implementation (**Python**) of the [**harel**](https://github.com/fruwehq/harel)
statechart engine.

The normative `SPEC.md`, the JSON Schema for machine YAML, and the cross-language
**conformance suite** live in the spec repo. This repository implements that spec in
Python and is correct **iff it passes the conformance suite**.

Status: **scaffold** — the engine is not yet implemented.

## Scope (per the spec)
- Load and validate machine YAML against `schema/machine.schema.json`, parsed under
  the **YAML 1.2 core schema** (so `on:` is a plain string, not a boolean).
- Execute statecharts per `SPEC.md`: run-to-completion, hierarchy, orthogonal
  regions, shallow/deep history, `defer` (deferred-set, edge-triggered), timers via
  an injected clock, active-object spawning + messaging, and action faults.
- **Guards in CEL** (e.g. [`cel-python`](https://pypi.org/project/cel-python/));
  **structured actions** with CEL-valued arguments.
- Storage / clock / observer adapters (SPEC §8).
- A test harness that runs the upstream conformance cases against this engine.

## Layout
- `src/harel/` — the package.
- `tests/` — unit tests and the conformance harness.

## Develop
```
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
pytest
```

## License
MIT — see [LICENSE](LICENSE).
