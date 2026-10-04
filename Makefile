.PHONY: test conformance postgresql-test lint format-check typecheck check all sync-schema

# Unit tests — the implementation's own suite. Hermetic and offline.
test:
	pytest -q

# Conformance — the language-agnostic format-1 core suite, pinned to the approved
# immutable pre-release commits in conformance/pins.py.
# Offline / against a local checkout:  DETERMA_CONFORMANCE_DIR=/path/to/determa-state-conformance make conformance
conformance:
	pytest conformance -q

# Optional live adapter test. Requires DETERMA_POSTGRESQL_DSN and the postgresql extra.
postgresql-test:
	pytest tests/test_postgresql_store.py -q

# Refresh the bundled JSON Schema from the immutable format-1 specification pin
# (or DETERMA_SPEC_DIR=/path/to/determa-state-spec).
sync-schema:
	python scripts/sync_schema.py

lint:
	ruff check .

format-check:
	ruff format --check .

typecheck:
	mypy src/determa

# Everything a PR needs to pass locally (unit gate), plus conformance.
check: lint format-check typecheck test

all: check conformance
