"""Unit tests for the batch/streaming CLI mode (SPEC §13.7)."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

import determa.state.cli as cli

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
    results = [json.loads(x) for x in out.splitlines() if x.strip()]
    return rc, results


def test_stream_happy_path(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    rc, results = _run_batch(
        tmp_path,
        [
            ["new", "t1", str(machine)],
            ["send", "t1", "coin", "--payload", "amount=100"],
            ["state", "t1"],
        ],
        monkeypatch,
        capsys,
    )
    assert rc == 0
    assert [r["ok"] for r in results] == [True, True, True]
    assert results[0]["result"]["config"] == ["locked"]
    assert results[1]["result"]["config"] == ["unlocked"]
    assert results[1]["result"]["published"] == []
    assert results[2]["result"]["config"] == ["unlocked"]


def test_stream_enabled_command(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    rc, results = _run_batch(
        tmp_path,
        [
            ["new", "t1", str(machine)],
            ["enabled", "t1"],
            ["send", "t1", "coin", "--payload", "amount=100"],
            ["enabled", "t1"],
        ],
        monkeypatch,
        capsys,
    )
    assert rc == 0
    assert results[1]["result"] == {"instance": "t1", "enabled": ["coin"]}
    assert results[3]["result"] == {"instance": "t1", "enabled": ["push"]}


def test_stream_failure_does_not_abort(tmp_path, monkeypatch, capsys):
    machine = tmp_path / "m.yaml"
    machine.write_text(TURNSTILE)
    rc, results = _run_batch(
        tmp_path,
        [
            ["new", "t1", str(machine)],
            ["send", "missing", "push"],   # not found -> exit 4
            ["state", "t1"],               # still runs
        ],
        monkeypatch,
        capsys,
    )
    # process exit is the first non-zero line exit (§13.7).
    assert rc == 4
    assert results[1] == {
        "ok": False,
        "exit": 4,
        "result": None,
        "error": {"message": results[1]["error"]["message"]},
    }
    assert results[1]["error"]["message"]  # a diagnostic was captured
    assert results[2]["ok"] is True
    assert results[2]["result"]["config"] == ["locked"]


def test_stream_malformed_line(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO("not json\n"))
    rc = cli.main(["--store", str(tmp_path / "store"), "run", "-"])
    out = capsys.readouterr().out
    rec = json.loads(out.strip())
    assert rc == 2
    assert rec["ok"] is False and rec["exit"] == 2 and rec["result"] is None


def test_stream_rejects_nested_run(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(["run", "-"]) + "\n"))
    rc = cli.main(["--store", str(tmp_path / "store"), "run", "-"])
    rec = json.loads(capsys.readouterr().out.strip())
    assert rc == 2
    assert rec["ok"] is False and rec["exit"] == 2
