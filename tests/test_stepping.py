"""Introspection + step-by-step execution (SPEC §14).

Covers the library primitives (``inject`` / ``step`` / ``inspect`` / manual mode)
and the CLI verbs (``mode`` / ``inject`` / ``step`` / ``inspect``), including the
manual-mode toggle where ``send`` enqueues without processing.
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

import harel
import harel.cli as cli

TURNSTILE = """\
id: turnstile
events:
  coin: { payload: { amount: { type: int, required: true } } }
  push: {}
  reset: {}
top:
  esvs:
    fare: { type: int, init: 50 }
  on_events:
    reset: { transition_to: locked }
  initial: { transition_to: locked }
  states:
    locked:
      on_events:
        coin: { transition_to: unlocked, guard: "event.payload.amount >= fare" }
        env: { transition_to: locked }
    unlocked:
      on_events:
        push: { transition_to: locked }
        error: { transition_to: unlocked }
"""


def _host() -> harel.Host:
    host = harel.Host()
    host.register_all(harel.load_definitions(TURNSTILE))
    host.create_root(host.machines["turnstile"], "t1")
    host.run_to_quiescence()
    return host


# --- library: inject / step / inspect ---------------------------------------
def test_inject_enqueues_without_processing() -> None:
    host = _host()
    inst = host.instances["t1"]
    assert inst.active_leaf_names() == ["locked"]

    accepted = host.inject("t1", "coin", {"amount": 100})
    assert accepted is True
    # enqueued, but the config is unchanged (nothing processed).
    assert inst.active_leaf_names() == ["locked"]
    assert [e.type for e in inst.queue] == ["coin"]


def test_inject_rejects_invalid_payload() -> None:
    host = _host()
    assert host.inject("t1", "coin", {"amount": "nope"}) is False
    assert len(host.instances["t1"].queue) == 0  # not enqueued


def test_step_returns_per_step_record_and_advances() -> None:
    host = _host()
    host.inject("t1", "coin", {"amount": 100})
    records = host.step("t1", 1)
    inst = host.instances["t1"]

    assert len(records) == 1
    rec = records[0]
    assert rec["event"] == "coin"
    assert rec["transition"] == "unlocked"
    assert rec["entered"] == ["unlocked"]
    assert rec["exited"] == ["locked"]
    assert rec["published"] == []
    assert rec["spawned"] == []
    assert rec["faulted"] is False
    assert inst.active_leaf_names() == ["unlocked"]


def test_step_drains_only_n_events() -> None:
    host = _host()
    host.inject("t1", "coin", {"amount": 100})  # -> unlocked
    host.inject("t1", "push")                    # -> locked
    records = host.step("t1", 1)                 # one RTC step only
    assert len(records) == 1
    assert host.instances["t1"].active_leaf_names() == ["unlocked"]
    # one event still pending.
    assert [e.type for e in host.instances["t1"].queue] == ["push"]


def test_step_with_empty_queue_returns_no_records() -> None:
    host = _host()
    assert host.step("t1", 5) == []


def test_inspect_exposes_full_internal_state() -> None:
    host = _host()
    host.inject("t1", "coin", {"amount": 100})
    info = host.inspect("t1")
    assert info["status"] == "active"
    assert info["config"] == ["locked"]
    assert info["esvs"] == {"fare": 50}
    assert info["enabled"] == ["coin", "reset"]
    assert [e["type"] for e in info["queue"]] == ["coin"]
    assert info["deferred"] == []
    assert info["timers"] == []
    assert info["history"] == {}


def test_enabled_events_are_declared_structural_and_lifecycle_filtered() -> None:
    host = _host()
    inst = host.instances["t1"]
    assert inst.enabled_events() == ["coin", "reset"]
    assert host.enabled_events("t1") == ["coin", "reset"]

    # Guard failure does not remove the structural handler.
    assert host.deliver("t1", "coin", {"amount": 0}) is True
    host.run_to_quiescence()
    assert host.enabled_events(inst) == ["coin", "reset"]

    assert host.deliver("t1", "coin", {"amount": 100}) is True
    host.run_to_quiescence()
    assert host.enabled_events("t1") == ["push", "reset"]


def test_manual_mode_send_enqueues_via_maybe_run() -> None:
    host = _host()
    host.mode = "manual"
    host.deliver("t1", "coin", {"amount": 100})
    host.maybe_run()  # manual -> does NOT run
    assert host.instances["t1"].active_leaf_names() == ["locked"]
    host.step("t1", 1)  # explicit step advances
    assert host.instances["t1"].active_leaf_names() == ["unlocked"]


# --- CLI verbs ---------------------------------------------------------------
def _run(
    tmp_path: Path,
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str]:
    rc = cli.main(["--store", str(tmp_path / "store"), *argv])
    out = capsys.readouterr().out
    return rc, out


def _run_batch(
    tmp_path: Path,
    lines: list[list[str]],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, list[dict]]:
    stdin = "".join(json.dumps(line) + "\n" for line in lines)
    monkeypatch.setattr("sys.stdin", io.StringIO(stdin))
    rc = cli.main(["--store", str(tmp_path / "store"), "run", "-"])
    out = capsys.readouterr().out
    return rc, [json.loads(x) for x in out.splitlines() if x.strip()]


def test_cli_mode_persists_and_toggles(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)

    rc, out = _run(tmp_path, ["mode", "--json"], monkeypatch, capsys)
    assert rc == 0 and json.loads(out) == {"mode": "auto"}

    rc, _ = _run(tmp_path, ["mode", "manual"], monkeypatch, capsys)
    assert rc == 0

    rc, out = _run(tmp_path, ["mode", "--json"], monkeypatch, capsys)
    assert json.loads(out) == {"mode": "manual"}


def test_cli_inject_enqueues_without_processing(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    _run(tmp_path, ["new", "t1", str(machine)], monkeypatch, capsys)

    rc, out = _run(
        tmp_path, ["inject", "t1", "coin", "--payload", "amount=100", "--json"],
        monkeypatch, capsys,
    )
    assert rc == 0
    obj = json.loads(out)
    assert obj["config"] == ["locked"]  # not processed

    _, out = _run(tmp_path, ["inspect", "t1", "--json"], monkeypatch, capsys)
    info = json.loads(out)
    assert [e["type"] for e in info["queue"]] == ["coin"]


def test_cli_step_advances_and_reports(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    _run(tmp_path, ["new", "t1", str(machine)], monkeypatch, capsys)
    _run(tmp_path, ["inject", "t1", "coin", "--payload", "amount=100"], monkeypatch, capsys)

    rc, out = _run(tmp_path, ["step", "t1", "--steps", "1", "--json"], monkeypatch, capsys)
    assert rc == 0
    obj = json.loads(out)
    assert obj["config"] == ["unlocked"]
    assert len(obj["steps"]) == 1
    assert obj["steps"][0]["transition"] == "unlocked"


def test_cli_inspect_full_shape(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    _run(tmp_path, ["new", "t1", str(machine)], monkeypatch, capsys)
    _run(tmp_path, ["inject", "t1", "coin", "--payload", "amount=100"], monkeypatch, capsys)

    rc, out = _run(tmp_path, ["inspect", "t1", "--json"], monkeypatch, capsys)
    assert rc == 0
    info = json.loads(out)
    assert info["instance"] == "t1"
    assert info["config"] == ["locked"]
    assert info["enabled"] == ["coin", "reset"]
    assert info["queue"] == [{"type": "coin", "payload": {"amount": 100}}]
    assert info["deferred"] == []
    assert info["timers"] == []


def test_cli_enabled_reports_current_events(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    _run(tmp_path, ["new", "t1", str(machine)], monkeypatch, capsys)

    rc, out = _run(tmp_path, ["enabled", "t1", "--json"], monkeypatch, capsys)
    assert rc == 0
    assert json.loads(out) == {"instance": "t1", "enabled": ["coin", "reset"]}

    _run(tmp_path, ["send", "t1", "coin", "--payload", "amount=100"], monkeypatch, capsys)
    rc, out = _run(tmp_path, ["enabled", "t1"], monkeypatch, capsys)
    assert rc == 0
    assert out.splitlines() == ["push", "reset"]


def test_cli_send_in_manual_mode_enqueues_only(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    _run(tmp_path, ["new", "t1", str(machine)], monkeypatch, capsys)
    _run(tmp_path, ["mode", "manual"], monkeypatch, capsys)

    # send enqueues but does not process -> config stays locked.
    rc, out = _run(
        tmp_path, ["send", "t1", "coin", "--payload", "amount=100", "--json"],
        monkeypatch, capsys,
    )
    assert rc == 0
    assert json.loads(out)["config"] == ["locked"]

    _, out = _run(tmp_path, ["inspect", "t1", "--json"], monkeypatch, capsys)
    assert [e["type"] for e in json.loads(out)["queue"]] == ["coin"]

    rc, out = _run(tmp_path, ["step", "t1", "--json"], monkeypatch, capsys)
    assert rc == 0 and json.loads(out)["config"] == ["unlocked"]


def test_cli_batch_stepping_session(tmp_path, monkeypatch, capsys):
    """The black-box §13.7 form: one run process, manual mode, step once."""
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    rc, results = _run_batch(
        tmp_path,
        [
            ["new", "t1", str(machine)],
            ["mode", "manual"],
            ["send", "t1", "coin", "--payload", "amount=100"],
            ["inspect", "t1"],
            ["step", "t1", "--steps", "1"],
            ["mode", "auto"],
        ],
        monkeypatch,
        capsys,
    )
    assert rc == 0
    assert [r["ok"] for r in results] == [True, True, True, True, True, True]
    assert results[1]["result"] == {"mode": "manual"}
    assert results[2]["result"]["config"] == ["locked"]
    assert [e["type"] for e in results[3]["result"]["queue"]] == ["coin"]
    assert results[4]["result"]["config"] == ["unlocked"]
    assert results[5]["result"] == {"mode": "auto"}
