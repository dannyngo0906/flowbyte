"""Thread-safe leaky bucket rate limiter synced with Haravan server state.

Haravan rate limit: 80 burst / 4 rps leak.
Client-side safety margin: stop at 70 to buffer 10 for server drift.
"""
from __future__ import annotations

import re
import threading
import time

_HEADER_RE = re.compile(r"^\s*(\d+)\s*/\s*(\d+)\s*$")


class HaravanTokenBucket:
    CAPACITY = 80
    LEAK_RATE = 4.0  # tokens/second
    SAFETY_MARGIN = 70  # stop acquiring above this

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tokens_used: float = 0.0
        self._last_leak: float = time.monotonic()

    # ── public ────────────────────────────────────────────────────────────────

    def acquire(self) -> None:
        """Block until a request slot is available."""
        while True:
            with self._lock:
                self._leak()
                if self._tokens_used < self.SAFETY_MARGIN:
                    self._tokens_used += 1.0
                    return
                wait = (self._tokens_used - self.SAFETY_MARGIN + 1) / self.LEAK_RATE
            time.sleep(min(wait, 1.0))

    def update_from_header(self, header_value: str | None) -> None:
        """Sync local count from X-Haravan-Api-Call-Limit response header."""
        if not header_value:
            return
        m = _HEADER_RE.match(header_value)
        if not m:
            # Malformed header — keep current state, don't crash
            return
        with self._lock:
            self._tokens_used = float(m.group(1))
            self._last_leak = time.monotonic()

    @property
    def tokens_used(self) -> float:
        with self._lock:
            self._leak()
            return self._tokens_used

    # ── private ───────────────────────────────────────────────────────────────

    def _leak(self) -> None:
        """Must be called with self._lock held."""
        now = time.monotonic()
        elapsed = now - self._last_leak
        self._tokens_used = max(0.0, self._tokens_used - elapsed * self.LEAK_RATE)
        self._last_leak = now
