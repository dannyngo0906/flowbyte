"""E2E tests for US-008: Log Retention — cleanup with real PostgreSQL.

Simulates the daily 3AM cleanup job with real data:
  Step 1: Seed expired + recent rows across retention tables
  Step 2: Run cleanup (live or dry-run)
  Step 3: Assert correct rows deleted / kept

Done-when coverage (PRD F8):
  ✅ Cleanup daily 3AM, idempotent
  ✅ sync_logs success: 90-day retention
  ✅ sync_logs errors: 30-day retention
  ✅ validation_results: 30-day retention
  ✅ --dry-run flag hoạt động (no rows deleted)
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import func, select, text
from typer.testing import CliRunner

from flowbyte.cli.app import app
from flowbyte.db.internal_schema import (
    sync_logs,
    sync_requests,
    sync_runs,
    validation_results,
)
from flowbyte.retention.cleanup import _run_cleanup

pytestmark = pytest.mark.integration

runner = CliRunner()

_PIPELINE = "shop_retention_test"


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def clean_tables(internal_engine):
    _truncate(internal_engine)
    yield
    _truncate(internal_engine)


def _truncate(engine):
    with engine.begin() as conn:
        conn.execute(
            text("TRUNCATE sync_logs, sync_runs, sync_requests, validation_results CASCADE")
        )


# ── Seed helpers ──────────────────────────────────────────────────────────────


def _insert_log(engine, level: str = "INFO", timestamp: datetime | None = None) -> None:
    with engine.begin() as conn:
        conn.execute(
            sync_logs.insert().values(
                timestamp=timestamp or datetime.now(timezone.utc),
                level=level,
                event="test_event",
                pipeline=_PIPELINE,
                resource="orders",
                message="test message",
            )
        )


def _insert_run(
    engine,
    started_at: datetime | None = None,
    status: str = "success",
) -> uuid.UUID:
    sync_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            sync_runs.insert().values(
                sync_id=sync_id,
                pipeline=_PIPELINE,
                resource="orders",
                mode="incremental",
                trigger="schedule",
                started_at=started_at or now - timedelta(minutes=5),
                finished_at=now,
                status=status,
            )
        )
    return sync_id


def _insert_validation(
    engine, sync_id: uuid.UUID, created_at: datetime | None = None
) -> None:
    with engine.begin() as conn:
        conn.execute(
            validation_results.insert().values(
                sync_id=sync_id,
                pipeline=_PIPELINE,
                resource="orders",
                rule="fetch_upsert_parity",
                status="ok",
                details={"note": "retention_test"},
                created_at=created_at or datetime.now(timezone.utc),
            )
        )


def _insert_sync_request(
    engine, status: str = "done", finished_at: datetime | None = None
) -> None:
    with engine.begin() as conn:
        conn.execute(
            sync_requests.insert().values(
                pipeline=_PIPELINE,
                mode="incremental",
                status=status,
                finished_at=finished_at,
            )
        )


def _count(engine, table) -> int:
    with engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(table)).scalar() or 0


# ── TestRetentionPolicies ─────────────────────────────────────────────────────


class TestRetentionPolicies:
    def test_success_logs_beyond_90d_deleted(self, internal_engine):
        now = datetime.now(timezone.utc)
        _insert_log(internal_engine, level="INFO", timestamp=now - timedelta(days=91))

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, sync_logs) == 0

    def test_success_logs_within_90d_kept(self, internal_engine):
        now = datetime.now(timezone.utc)
        _insert_log(internal_engine, level="INFO", timestamp=now - timedelta(days=89))

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, sync_logs) == 1

    def test_error_logs_beyond_30d_deleted(self, internal_engine):
        now = datetime.now(timezone.utc)
        _insert_log(internal_engine, level="ERROR", timestamp=now - timedelta(days=31))

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, sync_logs) == 0

    def test_error_logs_within_30d_kept(self, internal_engine):
        now = datetime.now(timezone.utc)
        _insert_log(internal_engine, level="ERROR", timestamp=now - timedelta(days=29))

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, sync_logs) == 1

    def test_validation_results_beyond_30d_deleted(self, internal_engine):
        now = datetime.now(timezone.utc)
        sync_id = _insert_run(internal_engine)
        _insert_validation(internal_engine, sync_id, created_at=now - timedelta(days=31))

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, validation_results) == 0

    def test_validation_results_within_30d_kept(self, internal_engine):
        now = datetime.now(timezone.utc)
        sync_id = _insert_run(internal_engine)
        _insert_validation(internal_engine, sync_id, created_at=now - timedelta(days=29))

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, validation_results) == 1

    def test_sync_runs_beyond_90d_deleted(self, internal_engine):
        now = datetime.now(timezone.utc)
        _insert_run(internal_engine, started_at=now - timedelta(days=91))

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, sync_runs) == 0

    def test_sync_runs_within_90d_kept(self, internal_engine):
        now = datetime.now(timezone.utc)
        _insert_run(internal_engine, started_at=now - timedelta(days=89))

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, sync_runs) == 1

    def test_sync_requests_done_beyond_30d_deleted(self, internal_engine):
        now = datetime.now(timezone.utc)
        _insert_sync_request(
            internal_engine, status="done", finished_at=now - timedelta(days=31)
        )

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, sync_requests) == 0

    def test_sync_requests_done_within_30d_kept(self, internal_engine):
        now = datetime.now(timezone.utc)
        _insert_sync_request(
            internal_engine, status="done", finished_at=now - timedelta(days=29)
        )

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, sync_requests) == 1

    def test_pending_sync_requests_never_deleted(self, internal_engine):
        """Pending requests (finished_at=NULL) must survive cleanup regardless of age."""
        _insert_sync_request(internal_engine, status="pending", finished_at=None)

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, sync_requests) == 1

    def test_mixed_age_logs_selective_deletion(self, internal_engine):
        """Only expired rows deleted; recent rows from both INFO and ERROR survive."""
        now = datetime.now(timezone.utc)
        _insert_log(internal_engine, level="INFO", timestamp=now - timedelta(days=91))  # deleted
        _insert_log(internal_engine, level="INFO", timestamp=now - timedelta(days=1))   # kept
        _insert_log(internal_engine, level="ERROR", timestamp=now - timedelta(days=31)) # deleted
        _insert_log(internal_engine, level="ERROR", timestamp=now - timedelta(days=1))  # kept

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, sync_logs) == 2


# ── TestDryRunE2E ─────────────────────────────────────────────────────────────


class TestDryRunE2E:
    def test_dry_run_returns_correct_counts(self, internal_engine):
        now = datetime.now(timezone.utc)
        for _ in range(3):
            _insert_log(internal_engine, level="INFO", timestamp=now - timedelta(days=91))

        stats = _run_cleanup(internal_engine, dry_run=True)

        assert stats["sync_logs_success_rows"] == 3
        assert stats["dry_run"] is True

    def test_dry_run_does_not_delete(self, internal_engine):
        now = datetime.now(timezone.utc)
        for _ in range(5):
            _insert_log(internal_engine, level="INFO", timestamp=now - timedelta(days=91))

        _run_cleanup(internal_engine, dry_run=True)

        assert _count(internal_engine, sync_logs) == 5

    def test_dry_run_error_logs_counted(self, internal_engine):
        now = datetime.now(timezone.utc)
        for _ in range(2):
            _insert_log(internal_engine, level="ERROR", timestamp=now - timedelta(days=31))

        stats = _run_cleanup(internal_engine, dry_run=True)

        assert stats["sync_logs_error_rows"] == 2
        assert _count(internal_engine, sync_logs) == 2  # not deleted

    def test_dry_run_stats_match_live_run_counts(self, internal_engine):
        now = datetime.now(timezone.utc)
        _insert_log(internal_engine, level="INFO", timestamp=now - timedelta(days=91))
        _insert_log(internal_engine, level="ERROR", timestamp=now - timedelta(days=31))

        dry_stats = _run_cleanup(internal_engine, dry_run=True)
        assert dry_stats["sync_logs_success_rows"] == 1
        assert dry_stats["sync_logs_error_rows"] == 1

        _run_cleanup(internal_engine, dry_run=False)

        assert _count(internal_engine, sync_logs) == 0


# ── TestIdempotency ───────────────────────────────────────────────────────────


class TestIdempotency:
    def test_cleanup_idempotent(self, internal_engine):
        now = datetime.now(timezone.utc)
        _insert_log(internal_engine, level="INFO", timestamp=now - timedelta(days=91))

        _run_cleanup(internal_engine, dry_run=False)
        _run_cleanup(internal_engine, dry_run=False)  # second run must not raise

        assert _count(internal_engine, sync_logs) == 0

    def test_cleanup_on_empty_tables_does_not_raise(self, internal_engine):
        _run_cleanup(internal_engine, dry_run=False)  # tables are already empty


# ── TestCLICleanupE2E ─────────────────────────────────────────────────────────


class TestCLICleanupE2E:
    def test_cleanup_dry_run_cli_shows_counts(self, internal_engine, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://unused/unused")
        now = datetime.now(timezone.utc)
        for _ in range(3):
            _insert_log(internal_engine, level="INFO", timestamp=now - timedelta(days=91))

        with patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine):
            result = runner.invoke(app, ["cleanup", "--dry-run"])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert "3" in result.output

    def test_cleanup_live_cli_deletes_rows(self, internal_engine, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://unused/unused")
        now = datetime.now(timezone.utc)
        for _ in range(2):
            _insert_log(internal_engine, level="INFO", timestamp=now - timedelta(days=91))

        with patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine):
            result = runner.invoke(app, ["cleanup"])

        assert result.exit_code == 0
        assert "Cleanup complete" in result.output
        assert _count(internal_engine, sync_logs) == 0


# ── TestVacuumNoRaise ─────────────────────────────────────────────────────────


class TestVacuumNoRaise:
    def test_cleanup_with_vacuum_does_not_raise(self, internal_engine):
        """VACUUM ANALYZE must run without error in the testcontainers Postgres instance."""
        now = datetime.now(timezone.utc)
        _insert_log(internal_engine, level="INFO", timestamp=now - timedelta(days=91))

        # Must not raise even though VACUUM runs in AUTOCOMMIT mode
        _run_cleanup(internal_engine, dry_run=False)
