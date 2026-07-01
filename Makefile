.PHONY: test conformance lint typecheck check all

# Unit tests — the implementation's own suite. Hermetic and offline.
test:
	pytest -q

# Conformance — the language-agnostic suite from fruwehq/harel-conformance, run
# black-box against this implementation. Downloads the suite (pinned to the release
# tag matching this package's version) into .cache/ on first run.
# Offline / against a local checkout:  HAREL_CONFORMANCE_DIR=/path/to/harel-conformance make conformance
conformance:
	pytest conformance -q

lint:
	ruff check .

typecheck:
	mypy src

# Everything a PR needs to pass locally (unit gate), plus conformance.
check: lint typecheck test

all: check conformance
