# AGENTS.md — determa-state-python

Guidance for coding agents working in this repository.

## Repository

This is the Python implementation of Determa State. The distribution is
`determa-state`; the import is `determa.state`. `src/determa` is a PEP 420 namespace
package so it can coexist with the umbrella `determa` launcher.

The implementation is conformant only when it passes the language-neutral suite.
Format-1 work currently uses these immutable pre-release inputs:

- specification: `1502a58a780d837e05bfacb37680dfc92e3488b5`;
- conformance: `707a49ce01c6f57f673c1959cdfe078bc8d0fc9a` (110 core cases plus
  persistence profiles).

The package metadata is `0.1.0` for the next synchronized release; the specification,
conformance suite, Python engine, and Rust engine version together.

Before this release is ready, replace the exact pre-release commit pins with and verify
them against the real synchronized specification and conformance `v0.1.0` tags after
those tags are created. Do not invent those tags or weaken pin checks.

## Boundaries

The implemented public API is `load_bundle`, `create`, and `dispatch`, plus validation
and error types exported by `determa.state`. It implements the exact `format: 1`
grammar. Do not restore abandoned draft field names or compatibility aliases.

The core is a pure foreground transform over one root ownership aggregate. It has no
hidden queues, timers, stores, snapshots, migration, enabled-event inspection, or
standardized execution CLI. Snapshot portability, machine definition migration or
hot-swap, package imports, and living tutorials are separate initiatives.

Layout:

- `src/determa/state/` — loader, validator, CEL profile, model, and engine;
- `src/determa/state/data/machine.schema.json` — exact pinned normative schema;
- `tests/` — hermetic implementation tests;
- `conformance/` — black-box format-1 harness and immutable pins;
- `.github/workflows/test.yml` — unit and pinned conformance gates;
- `.github/workflows/release.yml` — tag-triggered PyPI publication.

## Working Rules

- One issue to one PR, squash merge, linear history, and resolved review threads.
- Never put assistant attribution in commits, PRs, comments, or documentation.
- Behavioral work is specification, then conformance, then implementations. The
  conformance suite is the arbiter.
- Do not change the package version, tag, publish, or merge unless explicitly
  authorized as release work.
- Keep JSON/public identifiers unabbreviated and use only exact normative grammar.
- Unit tests remain hermetic and offline. Conformance may use its immutable checkouts.
- Preserve lazy CEL and JSON Schema imports where practical.

## Gates

```sh
python -m pip install -e '.[dev]'
ruff check .
mypy src/determa
pytest -q
DETERMA_CONFORMANCE_DIR=/path/to/conformance \
DETERMA_SPEC_DIR=/path/to/spec \
pytest conformance -q
```

`make check` runs lint, type checking, and unit tests. `make conformance` fetches or
reuses the immutable inputs recorded in `conformance/pins.py`.

## Release

A `vX.Y.Z` tag triggers `release.yml`, builds the distribution, and publishes to PyPI
using Trusted Publishing through the manually approved `pypi` environment. A tag
publishes, so do not create one during ordinary implementation work.
