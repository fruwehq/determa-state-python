"""Host-owned nanosecond reads with no user callback at the commit boundary.

Controlled clocks are explicit native test/host sources. Their installer owns
sample updates and availability; they do not advance or schedule themselves.
"""

from __future__ import annotations

from time import time_ns as _system_time_ns


class NativeCommitClock:
    """Exact native clock handle, separate from portable request timestamps."""

    __slots__ = ("_sample", "_system")

    def __init__(self) -> None:
        self._system = True
        self._sample: tuple[int, bool] = (0, False)

    @classmethod
    def controlled(cls, now_ns: int) -> NativeCommitClock:
        clock = cls()
        clock._system = False
        clock.set_native_sample(now_ns)
        return clock

    def set_native_sample(self, now_ns: int, *, unavailable: bool = False) -> None:
        if type(now_ns) is not int or not -(2**63) <= now_ns < 2**63:
            raise ValueError("invalid native clock sample")
        if type(unavailable) is not bool or self._system:
            raise ValueError("controlled native clock required")
        self._sample = (now_ns, unavailable)


# Retain actual native member descriptors, not a dynamic attribute access which
# could invoke a property installed by a callback after proof verification.
_NATIVE_CLOCK_TYPE = NativeCommitClock
_SYSTEM_SLOT = NativeCommitClock.__dict__["_system"]
_SAMPLE_SLOT = NativeCommitClock.__dict__["_sample"]


def _native_clock_mode(clock: NativeCommitClock) -> bool:
    if type(clock) is not _NATIVE_CLOCK_TYPE:
        raise ValueError("native clock identity differs")
    namespace = type.__getattribute__(_NATIVE_CLOCK_TYPE, "__dict__")
    if namespace.get("_system") is not _SYSTEM_SLOT or namespace.get("_sample") is not _SAMPLE_SLOT:
        raise ValueError("native clock descriptors differ")
    mode = _SYSTEM_SLOT.__get__(clock, _NATIVE_CLOCK_TYPE)
    if type(mode) is not bool:
        raise ValueError("native clock configuration differs")
    return mode


def _read_native_clock(clock: NativeCommitClock, *, mode: bool) -> int:
    system = _native_clock_mode(clock)
    if system is not mode:
        raise ValueError("native clock source differs")
    if system:
        now = _system_time_ns()
    else:
        sample = _SAMPLE_SLOT.__get__(clock, _NATIVE_CLOCK_TYPE)
        if type(sample) is not tuple or len(sample) != 2 or sample[1] is not False:
            raise ValueError("native clock unavailable")
        now = sample[0]
    if type(now) is not int or not -(2**63) <= now < 2**63:
        raise ValueError("invalid native clock sample")
    return now
