"""Unit tests for US-008: Log Retention — cleanup logic, CLI, and AsyncDBSink hardening."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from flowbyte.cli.app import app
from flowbyte.logging.db_sink import AsyncDBSink

runner = CliRunner()


# ── helpers ───────────────────────────────────────────────────────────────────


def _make_cleanup_engine(row_counts: tuple[int, int, int, int, int]):
    """Mock engine for _run_cleanup: 5 scalar() queries, then deletes, then VACUUM conn."""
    success_cnt, error_cnt, val_cnt, runs_cnt, req_cnt = row_counts

    conn = MagicMock()
    conn.execute = MagicMock(
        side_effect=[
            MagicMock(scalar=MagicMock(return_value=success_cnt)),  # logs success
            MagicMock(scalar=MagicMock(return_value=error_cnt)),    # logs error
            MagicMock(scalar=MagicMock(return_value=val_cnt)),      # validation_results
            MagicMock(scalar=MagicMock(return_value=runs_cnt)),     # sync_runs
            MagicMock(scalar=MagicMock(return_value=req_cnt)),      # sync_requests
            MagicMock(),  # delete logs success
            MagicMock(),  # delete logs error
            MagicMock(),  # delete validation_results
            MagicMock(),  # delete sync_runs
            MagicMock(),  # delete sync_requests
        ]
    )

    @contextmanager
    def _begin():
        yield conn

    vacuum_conn = MagicMock()
    engine = MagicMock()
    engine.begin = _begin
    engine.execution_options.return_value.connect.return_value.__enter__ = MagicMock(
        return_value=vacuum_conn
    )
    engine.execution_options.return_value.connect.return_value.__exit__ = MagicMock(
        return_value=False
    )
    return engine, conn, vacuum_conn


def _make_sink() -> AsyncDBSink:
    """AsyncDBSink with a mock engine — drain thread starts but never inserts."""
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return AsyncDBSink(engine)


def _event_dict(**kwargs) -> dict:
    return {"event": "test_event", "level": "INFO", "timestamp": "2026-04-25T00:00:00Z", **kwargs}


# ── TestRetentionConstants ────────────────────────────────────────────────────


class TestRetentionConstants:
    def test_success_logs_90_days(self):
        from flowbyte.retention.cleanup import _SYNC_LOGS_SUCCESS_DAYS
        assert _SYNC_LOGS_SUCCESS_DAYS == 90

    def test_error_logs_30_days(self):
        from flowbyte.retention.cleanup import _SYNC_LOGS_ERROR_DAYS
        assert _SYNC_LOGS_ERROR_DAYS == 30

    def test_validation_results_30_days(self):
        from flowbyte.retention.cleanup import _VALIDATION_RESULTS_DAYS
        assert _VALIDATION_RESULTS_DAYS == 30

    def test_sync_runs_90_days(self):
        from flowbyte.retention.cleanup import _SYNC_RUNS_DAYS
        assert _SYNC_RUNS_DAYS == 90

    def test_sync_requests_30_days(self):
        from flowbyte.retention.cleanup import _SYNC_REQUESTS_DONE_DAYS
        assert _SYNC_REQUESTS_DONE_DAYS == 30


# ── TestDryRunCleanup ─────────────────────────────────────────────────────────


class TestDryRunCleanup:
    def test_dry_run_returns_stats_dict(self):
        from flowbyte.retention.cleanup import _run_cleanup

        engine, _, _ = _make_cleanup_engine((10, 5, 3, 7, 2))
        stats = _run_cleanup(engine, dry_run=True)

        assert isinstance(stats, dict)
        assert stats["dry_run"] is True

    def test_dry_run_counts_match_mocked_queries(self):
        from flowbyte.retention.cleanup import _run_cleanup

        engine, _, _ = _make_cleanup_engine((10, 5, 3, 7, 2))
        stats = _run_cleanup(engine, dry_run=True)

        assert stats["sync_logs_success_rows"] == 10
        assert stats["sync_logs_error_rows"] == 5
        assert stats["validation_results_rows"] == 3
        assert stats["sync_runs_rows"] == 7
        assert stats["sync_requests_rows"] == 2

    def test_dry_run_does_not_call_delete(self):
        from flowbyte.retention.cleanup import _run_cleanup

        engine, conn, _ = _make_cleanup_engine((10, 5, 3, 7, 2))
        _run_cleanup(engine, dry_run=True)

        # dry_run: only 5 SELECT COUNT calls, no DELETE calls
        assert conn.execute.call_count == 5

    def test_live_run_calls_deletes(self):
        from flowbyte.retention.cleanup import _run_cleanup

        engine, conn, _ = _make_cleanup_engine((10, 5, 3, 7, 2))
        _run_cleanup(engine, dry_run=False)

        # 5 SELECT COUNT + 5 DELETE = 10 calls
        assert conn.execute.call_count == 10

    def test_live_run_calls_vacuum(self):
        from flowbyte.retention.cleanup import _run_cleanup

        engine, _, vacuum_conn = _make_cleanup_engine((1, 1, 1, 1, 1))
        _run_cleanup(engine, dry_run=False)

        # VACUUM is called once per table (4 tables)
        assert vacuum_conn.execute.called
        assert vacuum_conn.execute.call_count == 4

    def test_dry_run_does_not_call_vacuum(self):
        from flowbyte.retention.cleanup import _run_cleanup

        engine, _, vacuum_conn = _make_cleanup_engine((1, 1, 1, 1, 1))
        _run_cleanup(engine, dry_run=True)

        assert not vacuum_conn.execute.called


# ── TestCleanupCLICommand ─────────────────────────────────────────────────────


class TestCleanupCLICommand:
    def test_dry_run_flag_shows_dry_run_header(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine, _, _ = _make_cleanup_engine((7, 3, 2, 5, 1))
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["cleanup", "--dry-run"])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output

    def test_dry_run_shows_all_five_table_lines(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine, _, _ = _make_cleanup_engine((0, 0, 0, 0, 0))
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["cleanup", "--dry-run"])
        assert result.exit_code == 0
        assert "sync_logs (success >90d)" in result.output
        assert "sync_logs (errors >30d)" in result.output
        assert "validation_results >30d" in result.output
        assert "sync_runs >90d" in result.output
        assert "sync_requests done >30d" in result.output

    def test_dry_run_shows_row_counts(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine, _, _ = _make_cleanup_engine((7, 3, 2, 5, 1))
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["cleanup", "--dry-run"])
        assert "7" in result.output
        assert "3" in result.output

    def test_live_cleanup_shows_success_message(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine, _, _ = _make_cleanup_engine((1, 1, 1, 1, 1))
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["cleanup"])
        assert result.exit_code == 0
        assert "Cleanup complete" in result.output


# ── TestAsyncDBSinkQueueSize ──────────────────────────────────────────────────


class TestAsyncDBSinkQueueSize:
    def test_default_queue_size_is_10000(self):
        sink = _make_sink()
        assert sink._queue.maxsize == 10_000

    def test_custom_queue_size_respected(self):
        engine = MagicMock()
        sink = AsyncDBSink(engine, queue_size=500)
        assert sink._queue.maxsize == 500


# ── TestAsyncDBSinkDroppedCounter ─────────────────────────────────────────────


class TestAsyncDBSinkDroppedCounter:
    def test_dropped_starts_at_zero(self):
        sink = _make_sink()
        assert sink.dropped == 0

    def test_dropped_increments_on_full_queue(self):
        engine = MagicMock()
        sink = AsyncDBSink(engine, queue_size=1)
        # Fill the queue directly so it is definitely full
        sink._queue.put_nowait({"event": "filler", "level": "INFO"})
        # This write should be dropped
        sink(None, "info", _event_dict())
        assert sink.dropped == 1

    def test_dropped_increments_multiple_times(self):
        engine = MagicMock()
        sink = AsyncDBSink(engine, queue_size=2)
        # Fill queue to capacity so all subsequent writes are dropped
        sink._queue.put_nowait({"event": "filler1", "level": "INFO"})
        sink._queue.put_nowait({"event": "filler2", "level": "INFO"})
        # These 3 writes should all be dropped (queue is full)
        for _ in range(3):
            sink(None, "info", _event_dict())
        assert sink.dropped >= 1  # drain thread may have freed a slot; at least 1 must drop

    def test_dropped_does_not_increment_on_normal_write(self):
        sink = AsyncDBSink(MagicMock(), queue_size=100)
        sink(None, "info", _event_dict())
        assert sink.dropped == 0


# ── TestPIIKeys ───────────────────────────────────────────────────────────────


class TestPIIKeys:
    def test_pii_keys_is_frozenset(self):
        assert isinstance(AsyncDBSink._PII_KEYS, frozenset)

    def test_pii_keys_contains_required_fields(self):
        required = {
            "access_token", "master_key", "card_number", "secret_key",
            "password", "token", "api_key", "api_secret",
        }
        assert required <= AsyncDBSink._PII_KEYS


# ── TestPrepareRowPIIRedaction ────────────────────────────────────────────────


class TestPrepareRowPIIRedaction:
    def _sink(self) -> AsyncDBSink:
        return _make_sink()

    def _row(self, sink: AsyncDBSink, **extra) -> dict:
        return sink._prepare_row(_event_dict(**extra))

    def test_access_token_redacted(self):
        sink = self._sink()
        row = self._row(sink, access_token="tok123")
        assert row["payload"].get("access_token") == "***REDACTED***"

    def test_master_key_redacted(self):
        sink = self._sink()
        row = self._row(sink, master_key="super-secret-key")
        assert row["payload"].get("master_key") == "***REDACTED***"

    def test_card_number_redacted(self):
        sink = self._sink()
        row = self._row(sink, card_number="4111111111111111")
        assert row["payload"].get("card_number") == "***REDACTED***"

    def test_password_redacted(self):
        sink = self._sink()
        row = self._row(sink, password="hunter2")
        assert row["payload"].get("password") == "***REDACTED***"

    def test_api_key_redacted(self):
        sink = self._sink()
        row = self._row(sink, api_key="sk-abc123")
        assert row["payload"].get("api_key") == "***REDACTED***"

    def test_api_secret_redacted(self):
        sink = self._sink()
        row = self._row(sink, api_secret="mysecret")
        assert row["payload"].get("api_secret") == "***REDACTED***"

    def test_non_pii_field_preserved(self):
        # pipeline + resource are in _EXCLUDE_KEYS; use custom fields not in that set
        sink = self._sink()
        row = self._row(sink, fetched_count=42, shop_name="test_shop")
        assert row["payload"].get("fetched_count") == 42
        assert row["payload"].get("shop_name") == "test_shop"

    def test_payload_truncation_still_works_after_redaction(self):
        sink = AsyncDBSink(MagicMock(), max_payload_bytes=50)
        big_value = "x" * 100
        row = sink._prepare_row(_event_dict(large_field=big_value))
        assert row["payload"].get("_truncated") is True
