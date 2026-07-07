# AGENTS.md — determa-state-python

Guidance for AI/coding agents working in this repository. (Tool-agnostic; not specific to any one assistant.)

## What this repo is
The **Python reference implementation** of Determa State. Distribution name
**`determa-state`**; import name **`determa.state`** (a PEP 420 namespace package — there is
**no** `src/determa/__init__.py`, so it coexists with the `determa` launcher package). It is
correct **iff** it passes the conformance suite.

Layout:
- `src/determa/state/` — the engine package (`__about__.py` is the single version source).
- `tests/` — the implementation's own **unit tests** (hermetic, offline).
- `conformance/` — a **black-box harness** that fetches `determa-state-conformance` at the
  tag matching this package's version into `.cache/` (override with `DETERMA_CONFORMANCE_DIR`;
  spec schema override `DETERMA_SPEC_DIR`).
- `.github/workflows/` — `test.yml` (CI gate) and `release.yml` (tag → PyPI).

## Determa in one paragraph
**Determa** is a family for defining/running well-specified, verifiable behavior. **Determa
State** is a language-agnostic **statechart engine** (Harel/UML lineage, PSiCC RTC): one
YAML/JSON machine runs identically under any implementation, validated against a shared
conformance suite. Guards/action values are **CEL** (via the `cel-python`/`celpy` package,
lazily imported so the CLI starts fast). An umbrella `determa` launcher dispatches
`determa <product> …` → `determa-<product>` on PATH; this package also installs a
`determa-state-python` alias for explicit implementation selection.

## Repositories (org `fruwehq`, local folders `~/src/personal/`)
| Repo | Role |
|---|---|
| determa-state-spec | normative prose spec + schema. No CI. |
| determa-state-conformance | the conformance suite (arbiter). No CI. |
| **determa-state-python** (this) | Python impl — `determa-state` / `determa.state`. |
| determa-state-rust | Rust impl — crate `determa-state`. |
| determa | umbrella launcher (`python/`, `rust/`, `node/`). |

## Working rules (every Determa repo)
- **One issue → one PR**, branch → PR → **squash-merge**, linear history, resolve threads; `main` is protected (**and requires branches be up-to-date** — after a merge moves `main`, update other open PRs, which re-runs CI, before merging).
- **No AI/assistant attribution** anywhere (commits, PRs, comments, docs).
- **Conformance-first:** spec text → conformance case → this impl. Don't diverge from the pinned suite.
- **Synchronized SemVer** with spec + rust (currently **0.0.6**); bump `src/determa/state/__about__.py`.
- **No abbreviations** in JSON output / public identifiers (`definition` not `def`). Kept for now: `config`, machine-keywords (`esvs`, …), snapshot `def_id`/`def_version`, `spawn.def`.

## Gates (run before requesting review — this is the CI gate)
```sh
pip install -e '.[dev]'
ruff check .
mypy src/determa
pytest -q                 # unit tests (hermetic, offline) — CI job "test (ubuntu-24.04)"
pytest conformance        # conformance suite (network, or DETERMA_CONFORMANCE_DIR) — CI job "conformance"
# convenience: `make check` (gate) and `make conformance`
```
Keep CEL/`jsonschema` imports lazy (they dominate startup); unit tests must not touch the network.

## Releasing
Tag `vX.Y.Z` → `release.yml` builds sdist+wheel and publishes to **PyPI via Trusted
Publishing (OIDC)** — **gated on the `pypi` GitHub Environment (manual approval)**. The
PyPI project name is `determa-state`. **A tag publishes**, so only tag when you intend to
release. After a spec release, the conformance fetch auto-targets the new `v{version}` tag.

## Pointers
- Library API (SPEC §2): `Host`, `Instance`, `load_definitions` (accepts YAML **or** a dict/mapping), `validate`, etc. — see `README.md` and `tests/test_library_api.py`.
- CLI (SPEC §13/§14): `src/determa/state/cli.py`. Spec: `determa-state-spec/SPEC.md`.
