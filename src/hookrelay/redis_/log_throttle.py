"""At-most-once-per-interval logging so a Redis outage does not flood stdout."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ThrottleDecision:
    log: bool
    suppressed: int


class LogThrottle:
    """Track how many times a named event was suppressed since the last emit."""

    def __init__(
        self, interval_s: float = 10.0, *, clock: Callable[[], float] | None = None
    ) -> None:
        self._interval_s = interval_s
        self._clock = clock or time.monotonic
        self._last: dict[str, float] = {}
        self._suppressed: dict[str, int] = {}

    def check(self, key: str) -> ThrottleDecision:
        now = self._clock()
        last = self._last.get(key)
        if last is None or now - last >= self._interval_s:
            suppressed = self._suppressed.pop(key, 0)
            self._last[key] = now
            return ThrottleDecision(log=True, suppressed=suppressed)
        self._suppressed[key] = self._suppressed.get(key, 0) + 1
        return ThrottleDecision(log=False, suppressed=0)
