# Contributing to Determa State (Python)

`determa-state` is the Python implementation of the
[Determa State specification](https://github.com/fruwehq/determa-state-spec). The
[language-neutral conformance suite](https://github.com/fruwehq/determa-state-conformance)
is the executable arbiter of behavior.

## Development Setup

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
```

Python 3.11 or newer is supported. The distribution is `determa-state`; the import is
`determa.state`.

## Gates

Run the implementation gates and the full format-1 conformance suite before review:

```sh
ruff check .
mypy src/determa
pytest -q
pytest conformance -q
```

`tests/` is hermetic and offline. `conformance/` uses the approved immutable
specification and suite commits recorded in `conformance/pins.py`. The harness caches
those checkouts under `.cache/`. For offline or local cross-repository work:

```sh
DETERMA_CONFORMANCE_DIR=/path/to/determa-state-conformance \
DETERMA_SPEC_DIR=/path/to/determa-state-spec \
pytest conformance -q
```

CI checks out both immutable inputs directly and also verifies that the packaged schema
is byte-for-value equivalent to the pinned specification schema.

## Workflow

1. Read `AGENTS.md` and the linked specification/conformance changes first.
2. Create one branch and one pull request for one issue.
3. Never push directly to protected `main`.
4. Resolve every review thread and keep the branch current before squash-merging.
5. Do not add assistant attribution to commits, PRs, comments, or documentation.
6. Specify and add conformance behavior before changing an engine.

Do not reintroduce compatibility aliases for abandoned pre-format-1 grammar or public
behavior.

## Versioning And Release

`src/determa/state/__about__.py` is the single package version source. Determa State
specification, conformance, Python, and Rust versions are synchronized. The package
metadata is `0.1.0` for the next synchronized format 1 release.

The exact synchronized 0.1.0 specification and conformance commits recorded in
`conformance/pins.py` are the authoritative release inputs.

A `vX.Y.Z` tag triggers `release.yml` and publishes to PyPI through Trusted Publishing,
gated by the manually approved `pypi` environment. Version bumps, tags, and publication
are separate release work and require explicit authorization.

## License

Contributions are made under the [MIT license](LICENSE).
