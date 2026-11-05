"""Per-key leading + trailing edge coalescing for hot publish bursts."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class _Window:
    pending: bool = False
    handle: asyncio.TimerHandle | None = field(default=None, repr=False)


class KeyedCoalescer:
    """First trigger fires at once; triggers inside the window collapse into one trailing fire.

    A key therefore fires at most about twice per window however hot it is, and the last
    trigger is never dropped. Timers are created with ``call_later`` on the running loop so
    they do not keep the process alive by themselves once the coalescer is flushed.
    """

    def __init__(self, window_ms: float, fire: Callable[[str], None]) -> None:
        self._window_s = max(window_ms, 0) / 1000
        self._fire = fire
        self._windows: dict[str, _Window] = {}

    def trigger(self, key: str) -> None:
        window = self._windows.get(key)
        if window is not None:
            window.pending = True
            return
        self._fire(key)
        self._open(key)

    def flush(self) -> int:
        """Fire every pending trailing edge now and close all windows (graceful shutdown)."""
        fired = 0
        windows = list(self._windows.items())
        self._windows.clear()
        for key, window in windows:
            if window.handle is not None:
                window.handle.cancel()
            if window.pending:
                self._fire(key)
                fired += 1
        return fired

    @property
    def open_windows(self) -> int:
        return len(self._windows)

    def _open(self, key: str) -> None:
        window = _Window()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No loop (unit tests that only call flush): leave a marker with no timer.
            self._windows[key] = window
            return

        def _expire() -> None:
            current = self._windows.pop(key, None)
            if current is None:
                return
            if current.pending:
                self._fire(key)
                self._open(key)

        window.handle = loop.call_later(self._window_s, _expire)
        self._windows[key] = window
