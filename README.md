# determa-state

Python implementation of [Determa State](https://github.com/fruwehq/determa-state-spec),
a language-agnostic statechart engine with a shared normative conformance suite.

This release implements Determa State `format: 1` at the synchronized specification
commit `318ef1f16ae024770090bd338c8b70056df2855b`. Correctness is determined by the
110-case core suite, persistence profiles, and 85-vector execution-checkpoint profile
at conformance commit `86cb08a98267371b96b8f4908409aee022e4b4fe`.

The package metadata is `0.1.0` for the next synchronized release of the specification,
conformance suite, Python engine, and Rust engine.

## Install

The published `0.0.7` distribution predates portable persistence and definition
migration. Until the synchronized `0.1.0` Determa State release is published, install
this release candidate from a checkout:

```sh
git clone https://github.com/fruwehq/determa-state-python.git
cd determa-state-python
python -m pip install -e .
```

The distribution is `determa-state`; the import is `determa.state`. It also installs
`determa-state` and `determa-state-python` commands.

PostgreSQL support is optional and imports Psycopg only when that adapter is used:

```sh
python -m pip install -e '.[postgresql]'
```

## Define A Bundle

Format 1 uses one self-contained bundle containing one or more machines:

```yaml
format: 1
namespace: example.counter
events:
  increment:
    direction: input
    payload:
      amount: { type: int, required: true }
  reset:
    direction: input
machines:
  - machine_id: counter
    version: 1
    root:
      type: composite
      variables:
        count: { type: int, init: 0 }
      initial: { transition_to: running }
      states:
        running:
          on_events:
            increment:
              action:
                - assign: { count: "count + event.payload.amount" }
            reset:
              action:
                - assign: { count: "0" }
```

The same bundle is available at [`examples/format-1.yaml`](examples/format-1.yaml).
Documents are parsed using the portable YAML 1.2 scalar rules, then checked against the
bundled normative JSON Schema and semantic validation rules. Abandoned draft grammar
names are not accepted.

## Use The Library

`create` and `dispatch` are pure foreground operations. They do not retain hidden
machine state or call queues, timers, databases, or remote services.

```python
from pathlib import Path

import determa.state as ds

bundle = ds.load_bundle(Path("examples/format-1.yaml").read_text())
created = ds.create(
    bundle,
    machine_id="counter",
    root_instance_id="counter-42",
    creation_id="create-counter-42",
    bindings={},
)
state = created["state"]

target = {
    "root": {
        "root_instance_id": state["root_instance_id"],
        "root_runtime_id": state["root_runtime_id"],
    }
}
result = ds.dispatch(
    bundle,
    state,
    {
        "input": {
            "event": "increment",
            "event_id": "counter-42:increment:1",
            "target": target,
            "payload": {"amount": 2},
        }
    },
)

assert result["status"] == "running"
assert result["disposition"] == "handled"
state = result["state"]
root = state["runtimes"][state["root_runtime_id"]]
assert root["scopes"]["root"]["count"] == 2
```

Both calls return all result fields: `status`, `disposition`, `state`, `emissions`,
`fault`, and `rejection` (`create` has a null disposition). The caller owns delivery:
the core processes at most one supplied envelope and does not place it in an internal
queue. Successful processing returns a new JSON-compatible logical aggregate while
leaving the supplied prior state unchanged. Rejections and unhandled deliveries return
the exact supplied state object.

`load_bundle` also accepts a native Python mapping through the same structural and
semantic validation path. Native values must satisfy the same portable Unicode and
numeric domain as source documents.

## Persist And Migrate

`serialize_aggregate` produces the canonical §16 aggregate artifact. Restoration
resolves its exact validated definition by fingerprint and fails closed when the
definition is absent or untrusted:

```python
resolver = ds.MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
encoded = ds.serialize_aggregate(bundle, state)
restored = ds.restore_aggregate(encoded, resolver)
```

`restore_aggregate_package` verifies a self-contained transport package and seeds a
mutable resolver without replacing existing content. `migrate_aggregate` applies an
exact trusted descriptor route as a pure operation. `migrate_and_dispatch` returns one
commit-ready migration, audit, dispatch, aggregate, and outbox-intent boundary. Failed
migrations return a deterministic `MigrationFailure` and do not mutate the supplied
artifact or resolver.

Definition and descriptor resolvers are protocols, so applications can back them with
an immutable registry or a transaction-local cache.

## Run A Checkpoint Host

`ExecutionHost` is an optional synchronous durable-host layer. It stores one strict
portable checkpoint per root and implements durable acceptance, committed receipts,
pending delivery, outbox lifecycle, keyed migration, bounded replay retention, CAS,
and terminal tombstones. Direct store injection does not require a registry:

```python
store = ds.SQLiteExecutionStore("state.db")
store.setup_schema()  # always explicit
resolver = ds.MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
host = ds.ExecutionHost(store, resolver)

created = host.create(
    bundle,
    machine_id="counter",
    root_instance_id="counter-42",
    creation_id="create-counter-42",
    bindings={},
)
checkpoint = host.read_checkpoint("counter-42").document
```

`MemoryExecutionStore` is ephemeral. `FileExecutionStore` provides locked atomic
replacement and restart persistence only. SQLite advertises durable single-writer
storage only with its verified transaction, journal, and synchronization settings.
The optional PostgreSQL adapter provides concurrent CAS and host-owned shared
application transactions. Every store transaction is bound to one exact root.

SQLite and PostgreSQL accept explicit `replay_retention="permanent"` and
`outbox_retention="strict" | "compact"` configuration. These settings add only the
retention capabilities they actually enforce. Database setup records that policy
immutably; reopening with a different policy is rejected, and database guards reject
native root-checkpoint deletion or policy mutation. `ExecutionHost` validates required
capabilities and composed profiles against the injected store:

```python
store = ds.SQLiteExecutionStore(
    "bank.db",
    replay_retention="permanent",
    outbox_retention="strict",
)
store.setup_schema()
host = ds.ExecutionHost(
    store,
    resolver,
    required_capabilities={
        ds.DURABLE_SINGLE_WRITER,
        ds.ROOT_IDENTITY_RETENTION,
        ds.PERMANENT_RECEIPT_RETENTION,
    },
    profile="exactly_once_committed_processing",
)
```

For PostgreSQL application composition, `run_shared_transaction` opens and owns one
native transaction. Its callback receives the Psycopg connection plus a root-bound
staging surface for exactly one host operation. That operation returns only
`StagedExecutionResult`; the portable committed or pending response is returned by
`run_shared_transaction` after the native transaction commits. Callback failure rolls
back both application writes and checkpoint work.

File and database schema setup is never implicit. SQLite and PostgreSQL validate an
explicit schema version and the exact required tables, columns, types, nullability,
primary keys, indexes, immutable policy rows, and deletion-protection triggers before
checkpoint use.

`ExecutionStoreRegistry` starts empty. `register_bundled_execution_stores` registers
`memory`, `file`, `sqlite`, and `postgresql` through the same public operation used by
third-party factories. URI resolution extracts only the scheme; each factory owns its
configuration. Root checkpoint deletion is unsupported.

## Implemented Surface

- strict format-1 loading, default materialization, bundle fingerprinting, and exact
  source-level scalar handling;
- portable CEL guards and action expressions;
- hierarchical dispatch, local and unmarked transitions, choices, shallow/deep
  history, entry/exit behavior, final states, and stop interruption;
- lexical typed variables, input/external bindings, `env` refresh, and typed payloads;
- explicit sends, isolated lifecycle-bound components, and deterministic routing;
- owned spawn, nominal instance references, binding, cancellation, completion,
  failure propagation, and cleanup cascades;
- atomic RTC rollback, deterministic identities/counters, pure inspection, and
  incompatible or malformed prior-state rejection;
- canonical aggregate serialization/restoration, portable typed values, package
  attachments, exact definition resolution, trusted lazy migration, deterministic
  audits, resource limits, and atomic migrate-and-dispatch results.
- strict portable execution-checkpoint parsing, canonical digests, semantic
  validation, synchronous transaction/CAS/replay orchestration, receipts, pending
  delivery, outbox lifecycle, replay retention, and root tombstones;
- public direct execution-store injection and explicit registration for memory, file,
  SQLite, optional PostgreSQL, and third-party adapters.

Format 1 deliberately does not define timers, a broker implementation, package
imports, standardized enabled-event inspection, or a standardized execution CLI.
Adapter storage schemas are implementation-owned and require explicit setup.

The implementation-local CLI only validates a bundle:

```sh
determa-state validate examples/format-1.yaml
```

It prints the normalized bundle fingerprint on success.

## Develop

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'

ruff check .
mypy src/determa
pytest -q
pytest conformance -q
```

Unit tests are hermetic and offline. The conformance harness uses the immutable commits
listed above, cached under `.cache/`; local checkouts can be supplied with
`DETERMA_CONFORMANCE_DIR` and `DETERMA_SPEC_DIR`.

## License

MIT. See [LICENSE](LICENSE).
