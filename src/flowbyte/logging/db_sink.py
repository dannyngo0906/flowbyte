"""Async bounded queue sink writing structlog events to sync_logs table."""
from __future__ import annotations

import json
import queue
import sys
import threading
import time
import traceback
from typing import Any

from sqlalchemy.engine import Engine

from flowbyte.logging.processors import deep_redact


class AsyncDBSink:
    """Non-blocking DB sink — drops on full queue (never blocks the caller)."""

    def __init__(
        self,
        engine: Engine,
        queue_size: int = 10_000,
        max_payload_bytes: int = 10_240,
        min_level: str = "INFO",
    ) -> None:
        self._engine = engine
        self._queue: queue.Queue[dict] = queue.Queue(maxsize=queue_size)
        self.dropped: int = 0
        self._max_payload = max_payload_bytes
        self._level_order = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
        self._min_level_int = self._level_order.get(min_level.upper(), 1)
        self._thread = threading.Thread(target=self._drain, daemon=True, name="flowbyte-log-sink")
        self._thread.start()

    def __call__(self, logger: Any, method_name: str, event_dict: dict) -> dict:
        level = event_dict.get("level", "INFO").upper()
        if self._level_order.get(level, 0) < self._min_level_int:
            return event_dict

        row = self._prepare_row(event_dict)
        try:
            self._queue.put_nowait(row)
        except queue.Full:
            self.dropped += 1
            sys.stderr.write(f"[flowbyte] log_sink_full: dropping {event_dict.get('event')}\n")

        return event_dict  # structlog chain continues

    _PII_KEYS: frozenset[str] = frozenset((
        "access_token", "master_key", "card_number", "secret_key",
        "password", "token", "api_key", "api_secret",
    ))

    _EXCLUDE_KEYS = frozenset((
        "timestamp", "level", "event", "sync_id", "pipeline", "resource",
        "exc_info", "_record", "_logger", "_name",
    ))

    def _prepare_row(self, event_dict: dict) -> dict:
        payload = {
            k: v
            for k, v in event_dict.items()
            if k not in self._EXCLUDE_KEYS and not k.startswith("_")
        }
        payload = {k: "***REDACTED***" if k in self._PII_KEYS else v for k, v in payload.items()}
        payload = deep_redact(payload)
        payload_json = json.dumps(payload, default=str)
        if len(payload_json) > self._max_payload:
            payload = {"_truncated": True, "original_size": len(payload_json)}

        exc_info = event_dict.get("exc_info")
        exc_text: str | None = None
        if exc_info and exc_info is not True:
            try:
                if isinstance(exc_info, tuple) and len(exc_info) == 3:
                    raw = "".join(traceback.format_exception(*exc_info))
                else:
                    raw = str(exc_info)
                exc_text = deep_redact(raw)
            except Exception:
                exc_text = repr(exc_info)

        return {
            "timestamp": event_dict.get("timestamp"),
            "level": event_dict.get("level", "INFO").upper(),
            "event": str(event_dict.get("event", "")),
            "sync_id": event_dict.get("sync_id"),
            "pipeline": event_dict.get("pipeline"),
            "resource": event_dict.get("resource"),
            "message": str(event_dict.get("event", "")),
            "payload": payload,
            "exc_info": exc_text,
        }

    def _drain(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            batch = [item]
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline and len(batch) < 100:
                try:
                    batch.append(self._queue.get(timeout=max(0.01, deadline - time.monotonic())))
                except queue.Empty:
                    break

            try:
                self._insert_batch(batch)
            except Exception as e:
                sys.stderr.write(f"[flowbyte] log_db_insert_failed: {e}\n")

    def _insert_batch(self, batch: list[dict]) -> None:
        from flowbyte.db.internal_schema import sync_logs

        with self._engine.begin() as conn:
            conn.execute(sync_logs.insert(), batch)
