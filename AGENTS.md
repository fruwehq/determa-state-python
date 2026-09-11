# AGENTS.md — determa-state-python

Guidance for coding agents working in this repository.

## Repository

This is the Python implementation of Determa State. The distribution is
`determa-state`; the import is `determa.state`. `src/determa` is a PEP 420 namespace
package so it can coexist with the umbrella `determa` launcher.

The implementation is conformant only when it passes the language-neutral suite.
The implementation uses these immutable inputs:

- specification: `2e33036563cb966b07124197db672159b4b7e1f4`;
- conformance: `531468c59c7a2dc32f5cbe92cfabf89805d27f6a` (114 core cases,
  108 persistence vectors, 12 persistence-profile steps, 99 execution-checkpoint
  vectors, 106 version-2 vectors, and 101 closed-code registry entries).

The package metadata remains `0.2.0`.

## Boundaries

The pure public API remains `load_bundle`, `create`, and `dispatch`, plus validation
and error types exported by `determa.state`. The optional synchronous `ExecutionHost`
and execution-store APIs wrap that core without changing its exact `format: 1`
grammar. Do not restore abandoned draft field names or compatibility aliases.

The core is a pure foreground transform over one root ownership aggregate. It has no
hidden queues, timers, stores, or standardized execution CLI. Portable aggregate
migration remains pure. The optional host owns checkpoint transactions, accepted
pending delivery, receipts, outbox state, retention, and tombstones. The CLI remains
validation-only.

Layout:

- `src/determa/state/` — loader, validator, CEL profile, model, and engine;
- `src/determa/state/checkpoint.py`, `host.py`, and `stores/` — optional portable
  checkpoint validation, synchronous host orchestration, registry, and adapters;
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
- Preserve lazy Psycopg import and explicit file/database schema setup. Never add
  checkpoint or root-marker deletion.
- Every execution-store transaction is root-bound. Shared application transactions
  use the host-owned callback API; never expose raw native/store transaction injection
  on portable host operations or return committed/pending responses before commit.
- Durable and retention profile checks use the configured store instance. SQLite and
  PostgreSQL schema health requires the exact explicit schema version and shape.

## Gates

```sh
python -m pip install -e '.[dev]'
ruff check .
mypy src/determa
pytest -q
DETERMA_CONFORMANCE_DIR=/path/to/conformance \
DETERMA_SPEC_DIR=/path/to/spec \
pytest conformance -q

# Optional, only with a configured service and installed postgresql extra
DETERMA_POSTGRESQL_DSN=postgresql://... pytest tests/test_postgresql_store.py -q
```

`make check` runs lint, type checking, and unit tests. `make conformance` fetches or
reuses the immutable inputs recorded in `conformance/pins.py`.

## Release

A `vX.Y.Z` tag triggers `release.yml`, builds the distribution, and publishes to PyPI
using Trusted Publishing through the manually approved `pypi` environment. A tag
publishes, so do not create one during ordinary implementation work.
