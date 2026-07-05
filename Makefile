.PHONY: test conformance lint typecheck check all sync-schema

# Unit tests — the implementation's own suite. Hermetic and offline.
test:
	pytest -q

# Conformance — the language-agnostic suite from fruwehq/determa-state-conformance, run
# black-box against this implementation. Downloads the suite (pinned to the release
# tag matching this package's version) into .cache/ on first run.
# Offline / against a local checkout:  DETERMA_CONFORMANCE_DIR=/path/to/determa-state-conformance make conformance
conformance:
	pytest conformance -q

# Refresh the bundled JSON Schema from fruwehq/determa-state-spec at the matching version tag
# (or DETERMA_SPEC_DIR=/path/to/determa-state-spec). The schema-drift test guards that they match.
sync-schema:
	python scripts/sync_schema.py

lint:
	ruff check .

typecheck:
	mypy src

# Everything a PR needs to pass locally (unit gate), plus conformance.
check: lint typecheck test

all: check conformance
