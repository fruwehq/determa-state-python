# determa-state

Python implementation of [Determa State](https://github.com/fruwehq/determa-state-spec),
a language-agnostic statechart engine with a shared normative conformance suite.

This pre-release implements Determa State `format: 1` at the approved specification
commit `4bd4d9588d11b75d376380b6120676a056a4bc45`. Correctness is determined by the
88-case core suite at conformance commit
`ffbc65cbce49733803119a7dabf02a9727819ba8`.

The package version remains `0.0.6` until the specification, conformance suite, Python
engine, and Rust engine are released together.

## Install

The published `0.0.6` distribution predates format 1. Until the next synchronized
Determa State release, install this pre-release implementation from a checkout:

```sh
git clone https://github.com/fruwehq/determa-state-python.git
cd determa-state-python
python -m pip install -e .
```

The distribution is `determa-state`; the import is `determa.state`. It also installs
`determa-state` and `determa-state-python` commands.

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

## Implemented Core

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
  incompatible or malformed prior-state rejection.

Format 1 deliberately does not define native queues, timers, deferral, dead letters,
stores, snapshot wire encoding, machine hot-swap/migration, package imports,
standardized enabled-event inspection, or a standardized execution CLI. Hosts may
persist the returned logical aggregate in their own transaction, but portable
serialization and definition migration remain separate specification work.

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
