# Contributing to harel-python

**harel-python** is the Python reference implementation of the
[harel](https://github.com/fruwehq/harel) statechart engine. It is correct **iff** it
passes the language-agnostic [conformance suite](https://github.com/fruwehq/harel-conformance).
The prose specification lives in [`fruwehq/harel`](https://github.com/fruwehq/harel)
(`SPEC.md`, the JSON Schema, and examples); the executable correctness target lives in
[`fruwehq/harel-conformance`](https://github.com/fruwehq/harel-conformance).

## Dev setup

```sh
python -m venv .venv
source .venv/bin/activate     # or `.venv\Scripts\activate` on Windows
pip install -e '.[dev]'
```

Python ≥ 3.11. The package is import-named `harel`, distribution-named `harel-python`.

## The gate

Before pushing, run all three and keep them clean (`make check` does exactly this):

```sh
ruff check .
mypy src
pytest            # unit tests only — hermetic, offline
```

CI runs `test (ubuntu-24.04)` on every PR and is **required** — a PR merges only once it
is green. This job runs the **unit tests only**.

## Unit tests vs. conformance — kept separate

This implementation has its **own unit tests** (`tests/`), which are hermetic and offline
— `pytest` (or `make test`) runs only these, and they never touch the network. That is the
required PR gate.

**Conformance is separate.** The language-agnostic suite is downloaded and run black-box
against the built CLI/engine, in its own directory (`conformance/`) and its own CI job
(`conformance`, non-blocking by default). Run it locally with:

```sh
make conformance   # == pytest conformance
```

The suite is **not** a submodule. `conformance/conftest.py` clones
`fruwehq/harel-conformance` at the release tag matching this package's version (falling
back to `main` while the tag does not yet exist) into a gitignored `.cache/` directory and
reuses it. To force a refresh, delete `.cache/`.

- **Offline / local edits:** point the tests at a local checkout with
  `HAREL_CONFORMANCE_DIR=/path/to/harel-conformance` (and `HAREL_SPEC_DIR=/path/to/harel`
  for the schema-parity test). If the suite cannot be obtained and no override is set, the
  conformance tests **skip** rather than error.
- **Black-box CLI conformance** runs the implementation's `harel` (via `python -m harel`)
  as a **subprocess** against `conformance/run_cli.py`, so packaging/entry-point regressions
  are caught (SPEC §13.6).

## Workflow

1. Branch from `main`, open a Pull Request, and **squash-merge** — `main` stays linear.
2. Resolve all review threads before merging.
3. **Never push to `main` directly.**
4. **No AI/assistant attribution anywhere** — not in commits, PR bodies, comments, or
   docs (no `Co-Authored-By:`, no "Generated with…"). Commits and PRs read as the
   author's own work.
5. One issue → one PR. A behavior change usually pairs with a `harel` spec edit and a
   `harel-conformance` case; link them from the PR.

## Versioning

The version source of truth is **`pyproject.toml`** (`version = …`); the package derives
`harel.__version__` from the installed distribution metadata (no second copy to keep in
sync). The package version **is** the implemented harel spec version.

> harel, harel-conformance, and harel-python share one synchronized SemVer version
> (currently pre-1.0 `0.0.x`). A release tags all three `vX.Y.Z` in lockstep; an
> implementation declares "implements harel spec vX.Y.Z" and pins the conformance suite
> at that tag.

### Releasing `vX.Y.Z` (lockstep)
1. Land all spec / conformance / implementation changes on the three `main` branches.
2. Bump the version in **`pyproject.toml`** (here) and the `VERSION` files in `harel` and
   `harel-conformance`.
3. Tag `vX.Y.Z` on **harel** and **harel-conformance** (`gh api -X POST
   repos/fruwehq/<repo>/git/refs -f ref=refs/tags/vX.Y.Z -f sha=$(gh api
   repos/fruwehq/<repo>/commits/main --jq .sha)`), so this package pins the matching
   conformance tag instead of falling back to `main`.
4. Tag **harel-python** `vX.Y.Z` only to publish to PyPI — it triggers `release.yml`
   (Trusted Publishing).

## License

Contributions are made under the project's [MIT license](LICENSE).
