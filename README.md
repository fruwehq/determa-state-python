# determa-state

Python implementation of [Determa State](https://github.com/fruwehq/determa-state-spec),
a language-agnostic statechart engine with a shared normative conformance suite.

This implementation supports Determa State `format: 1` at specification
commit `86bb88dd21cb1f799eefe5020b6e49dabf6e7225`. Correctness is determined by
99 format-1 core cases, 162 version-1 vectors, 142 durable-host vectors, 49 core
inspection vectors, 47 extension negotiation vectors, 7 native guard-provider
inspection vectors, 30 runtime-provider vectors, and 476 manifest artifacts at
conformance commit `ce6c94ee1929597689403080d1790e2f48f8a5fb`. The native
runtime-provider profile is optional and is exercised through its production adapter.

The package metadata is `0.3.0`. Artifact and checkpoint schema version 1 is the
only supported portable artifact format. Machine YAML remains `format: 1`.

## Unreleased 0.3.0

This release adds portable event deferral to the queue-bearing aggregate and uses schema
version 1 as the sole portable aggregate, migration, and execution-checkpoint contract.
The synchronous host can persist accepted work and restore ready and deferred mailboxes
across restarts.
The version-1 checkpoint host returns exact receipt evidence for admission, processing,
and terminal replay. The optional public extension registry validates exact provider
references, loaded source closure, configured health, and currently proved claims
before evaluating a requested profile.

`ApplicationProjectionFacade` binds explicitly selected application rows to one
root. Supply an `ApplicationRowMapping` that reads and writes those rows through
the configured execution store's native shared transaction. The mapping provides
typed input, projects the proposed complete checkpoint into selected rows and
supplemental storage, and reconstructs it for a precommit round-trip check. `run`
returns the exact create, admit, or step result after commit; a retained delivery
returns its receipt before current row input is mapped. The optional aggregate-only
step path uses the mapping's `reconstruct_aggregate` and `project_aggregate`
methods when no checkpoint exists.

## Install

To try the `0.3.0` release candidate before publication, install it from a checkout:

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

`create`, `admit`, and `step` are pure foreground operations over explicit,
queue-bearing aggregate state. They do not retain hidden machine state or call
databases or remote services.

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

resolver = ds.MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})

target = {
    "root": {
        "root_instance_id": state["root_instance_id"],
        "root_runtime_id": state["root_runtime_id"],
    }
}
envelope = ds.portable_envelope(
    "increment",
    "counter-42:increment:1",
    target,
    {"amount": 2},
)
delivery = {
    "delivery_mode": "input",
    "envelope": envelope,
    "envelope_digest": ds.delivery_request_digest("counter-42", "input", envelope),
}
admitted = ds.admit(
    state,
    [delivery],
    resolver,
)
result = ds.step(admitted["state"], state["root_runtime_id"], resolver)

assert result["status"] == "running"
assert result["disposition"] == "handled"
state = result["state"]
root = next(
    runtime for runtime in state["runtimes"] if runtime["runtime_id"] == state["root_runtime_id"]
)
count = next(
    variable
    for variable in root["variables"]
    if variable["variable_declaration_pointer"].endswith("/count")
)
assert count["value"] == ["integer", "2"]
```

`create` initializes the aggregate, `admit` atomically appends accepted deliveries to
its runtime mailboxes, and `step` processes at most the selected runtime's ready head.
Ready and deferred mailboxes are literal portable aggregate state, so queued work
survives serialization and restoration. Each operation returns a new JSON-compatible
logical aggregate while leaving the supplied prior state unchanged.

`inspect_candidate(state, request, resolver)` predicts dispatch for one complete
normalized envelope at one exact runtime incarnation. It restores and validates the
aggregate, checks the
snapshot digest, and returns a closed result or failure without admitting the event.
Structural mode lists possible dispositions without executing a guard. Semantic mode
uses a separate bounded CEL inspector and returns evaluated guard evidence; pass
`semantic_enabled=False` when that optional capability is disabled. A result describes
only the supplied snapshot, so inspect again after any state change.

`load_bundle` also accepts a native Python mapping through the same structural and
semantic validation path. Native values must satisfy the same portable Unicode and
numeric domain as source documents.

Category-specific `StrEnum` definitions are used by production emitters.
`PORTABLE_CODE_SETS` is the immutable category-to-string mapping derived from those
definitions.

## Persist And Migrate

`serialize_aggregate` produces the canonical schema-v1 aggregate artifact. Restoration
resolves its exact validated definition by fingerprint and fails closed when the
definition is absent or untrusted:

```python
resolver = ds.MemoryArtifactResolver(definitions={bundle.fingerprint: bundle})
encoded = ds.serialize_aggregate(bundle, state)
restored = ds.restore_aggregate(encoded, resolver)
```

`restore_aggregate_package` verifies a self-contained transport package and seeds a
mutable resolver without replacing existing content. `migrate_aggregate_v1` applies an
exact trusted descriptor route as a pure operation. Failed migrations do not mutate the
supplied artifact or resolver.

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

created = host.create_v1(
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
capabilities and composed profiles against the injected store. Strong retention
capabilities are withheld before schema setup and whenever policy or guard validation
fails:

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

`ExtensionRegistry` is an optional common registration boundary for named providers.
Its `register` and `inject` operations validate the closed descriptor, and
`validate_configuration`, `capabilities`, `health`, `negotiate`, and
`evaluate_profile` evaluate the current configured instance. The embedding host
supplies a trusted source verifier; without one, the registry refuses to execute a
provider. A provider's claim is a candidate only. Unproved claims are omitted from
reports, and exact requirements fail closed. `bundled_extension_registry` installs
the four bundled stores through the same public `register` operation and accepts a
source verifier for additional providers. Its only positive claim here is memory's
`ephemeral`, which permits process loss. Category-specific guarantees need their
operational profile proof before the host can advertise them.

`RuntimeProviderRegistry` installs exact native guard and action providers through
that same common registration boundary. `SourceClosure` binds the loaded Python
callable and its declared dependency files to the reference and source digest.
The registry checks source-literal function defaults and the selected callable's
identity before use; mutable provider state may change in place. Hosts loading
providers with dynamic defaults or external executable dependencies can supply
`source_identity_verifier` when constructing the registry. That host callback must
independently attest the selected loaded executable and its source provenance;
provider self-assertions are not evidence.
`load_bundle(..., runtime_providers=registry)` preflights every native slot, including
unreachable declarations. The engine passes immutable typed snapshots, validates
ordered action proposals, and retains the ordinary atomic rollback and fault codes.
The registry reports effective guarantees from configured provider proof; unknown
purity discloses possible external I/O. A host may require guarantees at load with
`required_capabilities`. Native I/O can occur before a Determa commit and survives
a failed compare-and-swap; the engine does not retry it automatically.

`compile_language_source(source, registry, manifest=manifest)` verifies the exact
source and compiler closure, compiles disjoint regions in order, strictly loads the
generated format-1 bundle, and checks the manifest fingerprint and proved source
capabilities. A generated CEL bundle restores without an installed compiler.
Semantic inspection calls only a separately proved, bounded `inspect_guard` method;
structural inspection never invokes a native provider.

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
- atomic RTC rollback, deterministic identities/counters, and
  incompatible or malformed prior-state rejection;
- schema-v1 aggregate serialization/restoration, portable typed values, package
  attachments, exact definition resolution, trusted lazy migration, deterministic
  audits, and resource limits;
- strict portable execution-checkpoint parsing, canonical digests, semantic
  validation, synchronous transaction/CAS/replay orchestration, receipts, pending
  delivery, outbox lifecycle, replay retention, and root tombstones;
- public direct execution-store injection and explicit registration for memory, file,
  SQLite, optional PostgreSQL, and third-party adapters;
- exact public extension identity, loaded source checks, configured capability
  negotiation, and composition of guarantees and external-I/O hazards.
- exact-target structural inspection and optional bounded CEL semantic inspection of
  a restored portable aggregate, with no admission or action execution.

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
