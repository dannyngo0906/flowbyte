"""Alert deduplicator: suppress duplicate alerts within 5-min window, max 3/h/pipeline."""
from __future__ import annotations

import threading
from collections import defaultdict
from datetime import datetime, timezone

_WINDOW_SECONDS = 300   # 5 minutes
_MAX_PER_HOUR = 3


class AlertDeduper:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._recent: dict[str, datetime] = {}
        self._hourly: dict[str, list[datetime]] = defaultdict(list)

    def should_send(self, key: str, pipeline: str) -> bool:
        with self._lock:
            now = datetime.now(timezone.utc)

            # Dedup window
            last = self._recent.get(key)
            if last and (now - last).total_seconds() < _WINDOW_SECONDS:
                return False

            # Rate limit per pipeline per hour
            self._hourly[pipeline] = [
                t for t in self._hourly[pipeline]
                if (now - t).total_seconds() < 3600
            ]
            if len(self._hourly[pipeline]) >= _MAX_PER_HOUR:
                return False

            self._recent[key] = now
            self._hourly[pipeline].append(now)
            return True
