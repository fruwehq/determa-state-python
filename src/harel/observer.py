"""Observer adapter (SPEC §8): a passive per-step callback.

When a :class:`~harel.engine.Host` is given an observer, it is invoked once per
completed RTC step — for both automatic (run-to-quiescence) and manual (``step``)
processing — with a record::

    { instance, event, transition, entered, exited, published, spawned, faulted }

An observer is purely observational: it MUST NOT mutate engine state or influence
dispatch. It is the spec-native mechanism for transition logging and live
visualization, distinct from host-language diagnostic logging (``logging``).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, TextIO

# An observer is any callable taking one per-step record.
Observer = Callable[[dict[str, Any]], None]


class JsonlObserver:
    """Write one JSON record per line — a drop-in transition log.

    >>> import sys
    >>> host = Host(observer=JsonlObserver(sys.stdout))  # doctest: +SKIP
    """

    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    def __call__(self, record: dict[str, Any]) -> None:
        self._stream.write(json.dumps(record) + "\n")
        self._stream.flush()


class CollectingObserver:
    """Collect records into ``.records`` — handy for tests and inspection."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def __call__(self, record: dict[str, Any]) -> None:
        self.records.append(record)
