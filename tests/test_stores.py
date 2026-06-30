"""Store backends + the --store scheme (SPEC §8, §13.1).

All backends (``file``, ``mem``, ``sqlite``) round-trip an instance identically;
``sqlite:`` persists across CLI invocations; ``mem:`` is isolated per process (one
``run`` session). ``open_store`` parses the scheme.
"""

from __future__ import annotations

import io
import json

import pytest

import harel
import harel.cli as cli
from harel.store import FileStore, MemoryStore, SqliteStore, StoreState, open_store

TURNSTILE = """\
id: turnstile
events:
  coin: { payload: { amount: { type: int, required: true } } }
  push: {}
top:
  esvs:
    fare: { type: int, init: 50 }
  initial: { transition_to: locked }
  states:
    locked:
      on_events:
        coin: { transition_to: unlocked, guard: "event.payload.amount >= fare" }
    unlocked:
      on_events:
        push: { transition_to: locked }
"""


# --- open_store scheme parsing ----------------------------------------------
def test_open_store_parses_each_scheme(tmp_path) -> None:
    assert isinstance(open_store(str(tmp_path / "f")), FileStore)
    assert isinstance(open_store(f"file:{tmp_path / 'f2'}"), FileStore)
    assert isinstance(open_store("mem:"), MemoryStore)
    assert isinstance(open_store(f"sqlite:{tmp_path / 's.db'}"), SqliteStore)


def test_open_store_bare_path_is_file_for_backcompat(tmp_path) -> None:
    store = open_store(str(tmp_path / "bare"))
    assert isinstance(store, FileStore)


# --- round-trip parity across backends --------------------------------------
def _state() -> StoreState:
    host = harel.Host()
    host.register_all(harel.load_definitions(TURNSTILE))
    host.create_root(host.machines["turnstile"], "t1")
    host.run_to_quiescence()
    return StoreState(
        defs={"turnstile@1": TURNSTILE},
        instances=host.snapshot_all(),
        now=12_000,
        spawn_counters={"t1": 3},
        mode="manual",
    )


@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(lambda tmp: FileStore(tmp / "f"), id="file"),
        pytest.param(lambda tmp: MemoryStore(), id="mem"),
        pytest.param(lambda tmp: SqliteStore(tmp / "s.db"), id="sqlite"),
    ],
)
def test_round_trip_state_identical(factory, tmp_path) -> None:
    store = factory(tmp_path)
    store.save(_state())
    loaded = store.load()
    # the snapshot JSON (§8) is identical across backends.
    assert loaded.defs == {"turnstile@1": TURNSTILE}
    assert loaded.instances == _state().instances
    assert loaded.now == 12_000
    assert loaded.spawn_counters == {"t1": 3}
    assert loaded.mode == "manual"


def test_file_store_writes_snapshot_json_files(tmp_path) -> None:
    store = FileStore(tmp_path / "f")
    store.save(_state())
    files = sorted(p.name for p in (tmp_path / "f").iterdir())
    assert files == ["defs.json", "instances.json", "meta.json"]
    snap = json.loads((tmp_path / "f" / "instances.json").read_text())
    assert snap[0]["def_id"] == "turnstile"


def test_sqlite_store_persists_across_handles(tmp_path) -> None:
    path = tmp_path / "s.db"
    SqliteStore(path).save(_state())
    # a fresh handle on the same file reads it back (a new CLI invocation).
    loaded = SqliteStore(path).load()
    assert loaded.instances == _state().instances
    assert loaded.mode == "manual"


def test_memory_store_is_ephemeral_per_instance() -> None:
    a = MemoryStore()
    a.save(_state())
    b = MemoryStore()  # a separate process has no state
    assert b.load().instances == []


# --- end-to-end through the CLI ---------------------------------------------
def _run(
    store_spec: str,
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str]:
    rc = cli.main(["--store", store_spec, *argv])
    return rc, capsys.readouterr().out


def _run_batch(
    store_spec: str,
    lines: list[list[str]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, list[dict]]:
    stdin = "".join(json.dumps(line) + "\n" for line in lines)
    monkeypatch.setattr("sys.stdin", io.StringIO(stdin))
    rc = cli.main(["--store", store_spec, "run", "-"])
    out = capsys.readouterr().out
    return rc, [json.loads(x) for x in out.splitlines() if x.strip()]


def test_mem_store_holds_state_within_one_run_session(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    # one process: state persists across batch lines via the in-memory store.
    rc, results = _run_batch(
        "mem:",
        [
            ["new", "t1", str(machine)],
            ["send", "t1", "coin", "--payload", "amount=100"],
            ["state", "t1"],
        ],
        monkeypatch,
        capsys,
    )
    assert rc == 0
    assert results[0]["result"]["config"] == ["locked"]
    assert results[1]["result"]["config"] == ["unlocked"]
    assert results[2]["result"]["config"] == ["unlocked"]


def test_mem_store_does_not_persist_across_invocations(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    rc, _ = _run("mem:", ["new", "t1", str(machine)], monkeypatch, capsys)
    assert rc == 0
    # a separate process: the mem store is empty, so the instance is gone.
    rc, _ = _run("mem:", ["state", "t1"], monkeypatch, capsys)
    assert rc == 4  # not found


def test_sqlite_store_persists_across_cli_invocations(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    db = f"sqlite:{tmp_path / 's.db'}"
    rc, out = _run(db, ["new", "t1", str(machine), "--json"], monkeypatch, capsys)
    assert rc == 0 and json.loads(out)["config"] == ["locked"]
    rc, out = _run(
        db, ["send", "t1", "coin", "--payload", "amount=100", "--json"], monkeypatch, capsys
    )
    assert rc == 0 and json.loads(out)["config"] == ["unlocked"]
    # a third invocation reads the persisted state.
    rc, out = _run(db, ["state", "t1", "--json"], monkeypatch, capsys)
    assert rc == 0 and json.loads(out)["config"] == ["unlocked"]


def test_backends_produce_identical_cli_results(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)

    def drive(spec: str) -> list[dict]:
        _run(spec, ["new", "t1", str(machine)], monkeypatch, capsys)
        _, out = _run(
            spec, ["send", "t1", "coin", "--payload", "amount=100", "--json"], monkeypatch, capsys
        )
        return json.loads(out)

    file_result = drive(str(tmp_path / "file"))
    sqlite_result = drive(f"sqlite:{tmp_path / 's.db'}")
    assert file_result == sqlite_result
