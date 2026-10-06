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


def _read_native_clock(clock: NativeCommitClock) -> int:
    # Exact types and class-owned reads prevent overridden methods, numeric
    # conversions or an arbitrary supplied callable from running after proof.
    if type(clock) is not NativeCommitClock:
        raise ValueError("native clock identity differs")
    system = object.__getattribute__(clock, "_system")
    if system is True:
        now = _system_time_ns()
    elif system is False:
        sample = object.__getattribute__(clock, "_sample")
        if type(sample) is not tuple or len(sample) != 2 or sample[1] is not False:
            raise ValueError("native clock unavailable")
        now = sample[0]
    else:
        raise ValueError("native clock configuration differs")
    if type(now) is not int or not -(2**63) <= now < 2**63:
        raise ValueError("invalid native clock sample")
    return now
