"""Unit tests for US-007: CLI Observability — status, history, logs commands."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from flowbyte.cli.app import app

runner = CliRunner()

# ── mock factories ────────────────────────────────────────────────────────────

_VALID_CONFIG_JSON = {
    "name": "shop_main",
    "haravan_credentials_ref": "ref",
    "haravan_shop_domain": "test.myharavan.com",
    "destination": {
        "host": "localhost",
        "port": 5432,
        "user": "u",
        "database": "d",
        "credentials_ref": "pg",
    },
    "resources": {},
}


def _make_pipeline_row(name: str = "shop_main", enabled: bool = True, config_json=None):
    row = MagicMock()
    row.name = name
    row.enabled = enabled
    row.config_json = config_json if config_json is not None else _VALID_CONFIG_JSON
    return row


def _make_run_row(
    pipeline: str = "shop_main",
    resource: str = "orders",
    status: str = "success",
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    upserted_count: int = 42,
    error: str | None = None,
    fetched_count: int = 42,
    mode: str = "incremental",
    duration_seconds: float = 1.5,
):
    now = datetime.now(timezone.utc)
    row = MagicMock()
    row.pipeline = pipeline
    row.resource = resource
    row.status = status
    row.started_at = started_at or (now - timedelta(hours=1))
    row.finished_at = finished_at or now
    row.upserted_count = upserted_count
    row.fetched_count = fetched_count
    row.error = error
    row.mode = mode
    row.duration_seconds = duration_seconds
    return row


def _make_hb_row(age_seconds: float = 30.0, uptime_seconds: float = 3600 * 24 * 5 + 3600 * 3):
    now = datetime.now(timezone.utc)
    row = MagicMock()
    row.last_beat = now - timedelta(seconds=age_seconds)
    row.daemon_started_at = now - timedelta(seconds=uptime_seconds)
    return row


def _make_val_row(pipeline: str = "shop_main", resource: str = "orders", warn_count: int = 0, fail_count: int = 0):
    row = MagicMock()
    row.pipeline = pipeline
    row.resource = resource
    row.warn_count = warn_count
    row.fail_count = fail_count
    return row


def _make_status_engine(
    pipeline_rows=None,
    last_run_rows=None,
    val_rows=None,
    hb_row=None,
):
    """Mock engine for status() — 4 sequential execute() calls."""
    conn = MagicMock()
    conn.execute = MagicMock(
        side_effect=[
            MagicMock(all=MagicMock(return_value=pipeline_rows or [])),   # Q1: pipelines
            MagicMock(all=MagicMock(return_value=last_run_rows or [])),   # Q2: last runs
            MagicMock(all=MagicMock(return_value=val_rows or [])),        # Q3: val summary
            MagicMock(one_or_none=MagicMock(return_value=hb_row)),        # Q4: heartbeat
        ]
    )
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def _make_single_query_engine(rows):
    """Mock engine for history() and logs() — 1 execute() call."""
    conn = MagicMock()
    conn.execute = MagicMock(
        return_value=MagicMock(all=MagicMock(return_value=rows))
    )
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


# ── TestStatus ────────────────────────────────────────────────────────────────


class TestStatusCommand:
    def test_no_pipelines_shows_hint(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = _make_status_engine(pipeline_rows=[])
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "flowbyte init" in result.output

    def test_no_runs_shows_never(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        p = _make_pipeline_row()
        engine = _make_status_engine(pipeline_rows=[p], last_run_rows=[], hb_row=_make_hb_row())
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "⚪ NEVER" in result.output

    def test_success_shows_ok(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        p = _make_pipeline_row()
        run = _make_run_row(status="success")
        engine = _make_status_engine(pipeline_rows=[p], last_run_rows=[run], hb_row=_make_hb_row())
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🟢 OK" in result.output

    def test_failed_shows_fail(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        p = _make_pipeline_row()
        run = _make_run_row(status="failed", error="Connection timeout")
        engine = _make_status_engine(pipeline_rows=[p], last_run_rows=[run], hb_row=_make_hb_row())
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🔴 FAIL" in result.output

    def test_running_under_1h(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        p = _make_pipeline_row()
        now = datetime.now(timezone.utc)
        run = _make_run_row(
            status="running",
            started_at=now - timedelta(minutes=30),
            finished_at=None,
        )
        engine = _make_status_engine(pipeline_rows=[p], last_run_rows=[run], hb_row=_make_hb_row())
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🟡 RUNNING" in result.output
        assert "> 1h" not in result.output

    def test_running_over_1h_shows_warning(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        p = _make_pipeline_row()
        now = datetime.now(timezone.utc)
        run = _make_run_row(
            status="running",
            started_at=now - timedelta(hours=2),
            finished_at=None,
        )
        engine = _make_status_engine(pipeline_rows=[p], last_run_rows=[run], hb_row=_make_hb_row())
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🟡 RUNNING" in result.output
        assert "> 1h" in result.output

    def test_pipeline_disabled_shows_disabled(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        p = _make_pipeline_row(enabled=False)
        run = _make_run_row(status="success")
        engine = _make_status_engine(pipeline_rows=[p], last_run_rows=[run], hb_row=_make_hb_row())
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "⚪ DISABLED" in result.output

    def test_resource_disabled_in_config_shows_disabled(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        config_with_disabled = {
            **_VALID_CONFIG_JSON,
            "resources": {
                "orders": {
                    "enabled": False,
                    "sync_mode": "incremental",
                    "schedule": "0 */2 * * *",
                }
            },
        }
        p = _make_pipeline_row(enabled=True, config_json=config_with_disabled)
        engine = _make_status_engine(pipeline_rows=[p], last_run_rows=[], hb_row=_make_hb_row())
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "⚪ DISABLED" in result.output

    def test_scheduler_running_footer(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        p = _make_pipeline_row()
        engine = _make_status_engine(
            pipeline_rows=[p],
            last_run_rows=[],
            hb_row=_make_hb_row(age_seconds=30),
        )
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🟢 Running" in result.output

    def test_scheduler_dead_footer(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        p = _make_pipeline_row()
        engine = _make_status_engine(
            pipeline_rows=[p],
            last_run_rows=[],
            hb_row=_make_hb_row(age_seconds=200),
        )
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "❌ Dead" in result.output

    def test_scheduler_no_heartbeat_row(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        p = _make_pipeline_row()
        engine = _make_status_engine(pipeline_rows=[p], last_run_rows=[], hb_row=None)
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "❌ Dead" in result.output

    def test_validation_warnings_footer(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        p = _make_pipeline_row()
        val = _make_val_row(warn_count=2, fail_count=0)
        engine = _make_status_engine(
            pipeline_rows=[p],
            last_run_rows=[],
            val_rows=[val],
            hb_row=_make_hb_row(),
        )
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "⚠️" in result.output
        assert "warning" in result.output.lower()

    def test_validation_all_healthy_footer(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        p = _make_pipeline_row()
        engine = _make_status_engine(
            pipeline_rows=[p],
            last_run_rows=[],
            val_rows=[],
            hb_row=_make_hb_row(),
        )
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "✅" in result.output
        assert "healthy" in result.output.lower()


# ── TestHistory ───────────────────────────────────────────────────────────────


class TestHistoryCommand:
    def test_default_shows_rows(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        rows = [_make_run_row(resource=f"orders") for _ in range(5)]
        engine = _make_single_query_engine(rows)
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["history", "shop_main"])
        assert result.exit_code == 0
        assert "orders" in result.output

    def test_last_flag_accepted(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        rows = [_make_run_row()]
        engine = _make_single_query_engine(rows)
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["history", "shop_main", "--last", "5"])
        assert result.exit_code == 0

    def test_no_runs_shows_message(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = _make_single_query_engine([])
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["history", "shop_main"])
        assert result.exit_code == 0
        assert "No sync history" in result.output

    def test_success_shows_emoji(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = _make_single_query_engine([_make_run_row(status="success")])
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["history", "shop_main"])
        assert "🟢 OK" in result.output

    def test_failed_shows_emoji(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = _make_single_query_engine([_make_run_row(status="failed")])
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["history", "shop_main"])
        assert "🔴 FAIL" in result.output


# ── TestLogs ──────────────────────────────────────────────────────────────────


def _make_log_row(level: str = "INFO", event: str = "sync_ok", message: str = "done"):
    row = MagicMock()
    row.timestamp = datetime.now(timezone.utc)
    row.level = level
    row.event = event
    row.message = message
    return row


class TestLogsCommand:
    def test_errors_flag_returns_output(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = _make_single_query_engine([_make_log_row(level="ERROR", message="timeout")])
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["logs", "shop_main", "--errors"])
        assert result.exit_code == 0
        assert "ERROR" in result.output
        assert "timeout" in result.output

    def test_since_flag_accepted(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = _make_single_query_engine([_make_log_row()])
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["logs", "shop_main", "--since", "1h"])
        assert result.exit_code == 0

    def test_tail_flag_accepted(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = _make_single_query_engine([_make_log_row()])
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["logs", "shop_main", "--tail"])
        assert result.exit_code == 0

    def test_no_logs_empty_output(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = _make_single_query_engine([])
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["logs", "shop_main"])
        assert result.exit_code == 0


# ── TestParseSince ────────────────────────────────────────────────────────────


class TestParseSince:
    def test_hours(self):
        from flowbyte.cli.commands.observability import _parse_since

        assert _parse_since("1h") == timedelta(hours=1)
        assert _parse_since("2h") == timedelta(hours=2)

    def test_minutes(self):
        from flowbyte.cli.commands.observability import _parse_since

        assert _parse_since("30m") == timedelta(minutes=30)

    def test_days(self):
        from flowbyte.cli.commands.observability import _parse_since

        assert _parse_since("2d") == timedelta(days=2)

    def test_invalid_returns_none(self):
        from flowbyte.cli.commands.observability import _parse_since

        assert _parse_since("bad") is None
        assert _parse_since("") is None
        assert _parse_since("10x") is None
        assert _parse_since("abc") is None


# ── TestHelpers ───────────────────────────────────────────────────────────────


class TestResolveRowStatus:
    def test_pipeline_disabled(self):
        from flowbyte.cli.commands.observability import _resolve_row_status

        now = datetime.now(timezone.utc)
        assert _resolve_row_status(False, True, None, now) == "⚪ DISABLED"

    def test_resource_disabled(self):
        from flowbyte.cli.commands.observability import _resolve_row_status

        now = datetime.now(timezone.utc)
        assert _resolve_row_status(True, False, None, now) == "⚪ DISABLED"

    def test_no_run_shows_never(self):
        from flowbyte.cli.commands.observability import _resolve_row_status

        now = datetime.now(timezone.utc)
        assert _resolve_row_status(True, True, None, now) == "⚪ NEVER"

    def test_success(self):
        from flowbyte.cli.commands.observability import _resolve_row_status

        now = datetime.now(timezone.utc)
        run = MagicMock()
        run.status = "success"
        assert _resolve_row_status(True, True, run, now) == "🟢 OK"

    def test_failed(self):
        from flowbyte.cli.commands.observability import _resolve_row_status

        now = datetime.now(timezone.utc)
        run = MagicMock()
        run.status = "failed"
        assert _resolve_row_status(True, True, run, now) == "🔴 FAIL"

    def test_running_under_1h(self):
        from flowbyte.cli.commands.observability import _resolve_row_status

        now = datetime.now(timezone.utc)
        run = MagicMock()
        run.status = "running"
        run.started_at = now - timedelta(minutes=20)
        assert _resolve_row_status(True, True, run, now) == "🟡 RUNNING"

    def test_running_over_1h_returns_sentinel(self):
        from flowbyte.cli.commands.observability import _resolve_row_status

        now = datetime.now(timezone.utc)
        run = MagicMock()
        run.status = "running"
        run.started_at = now - timedelta(hours=2)
        result = _resolve_row_status(True, True, run, now)
        assert "🟡 RUNNING" in result
        assert "⚠️" in result


class TestComputeNextSync:
    def test_valid_cron_returns_timestamp(self):
        from flowbyte.cli.commands.observability import _compute_next_sync

        result = _compute_next_sync("0 */2 * * *")
        assert len(result) == 16
        assert result != "—"

    def test_invalid_cron_returns_dash(self):
        from flowbyte.cli.commands.observability import _compute_next_sync

        result = _compute_next_sync("not a cron")
        assert result == "—"

    def test_result_is_in_future(self):
        from flowbyte.cli.commands.observability import _compute_next_sync

        now = datetime.now(timezone.utc)
        result = _compute_next_sync("0 */2 * * *", from_dt=now)
        assert result != "—"
        result_dt = datetime.strptime(result, "%Y-%m-%d %H:%M")
        assert result_dt >= now.replace(tzinfo=None)


# ── Security: input validation ────────────────────────────────────────────────


class TestInputValidationSecurity:
    """Verify that CLI commands reject invalid inputs before querying the DB.

    Guards added as part of US-006/US-007 security review:
      - pipeline names validated against ^[a-z0-9_]{1,32}$
      - sync_id validated against ^[a-f0-9\\-]{1,36}$
      - --last capped at 100
    """

    # ── pipeline name guard ────────────────────────────────────────────────────

    def test_history_rejects_invalid_pipeline_name(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        result = runner.invoke(app, ["history", "INVALID NAME!"])
        assert result.exit_code == 2
        assert "Invalid pipeline name" in result.output

    def test_history_rejects_path_traversal(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        result = runner.invoke(app, ["history", "../../etc/passwd"])
        assert result.exit_code == 2

    def test_history_accepts_valid_pipeline_name(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = _make_single_query_engine([])
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["history", "shop_main"])
        assert result.exit_code == 0

    def test_logs_rejects_invalid_pipeline_name(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        result = runner.invoke(app, ["logs", "BAD NAME"])
        assert result.exit_code == 2
        assert "Invalid pipeline name" in result.output

    def test_logs_accepts_valid_pipeline_name(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = _make_single_query_engine([])
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["logs", "shop_main"])
        assert result.exit_code == 0

    # ── sync_id guard ──────────────────────────────────────────────────────────

    def test_inspect_rejects_wildcard_percent(self, monkeypatch):
        """% must be rejected — it would match all rows in a LIKE query."""
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        result = runner.invoke(app, ["inspect", "%"])
        assert result.exit_code == 2
        assert "Invalid sync_id" in result.output

    def test_inspect_rejects_non_uuid_characters(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        result = runner.invoke(app, ["inspect", "'; DROP TABLE sync_runs; --"])
        assert result.exit_code == 2

    def test_inspect_accepts_uuid_prefix(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = MagicMock()
        conn = MagicMock()
        conn.execute.return_value.one_or_none.return_value = None
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["inspect", "550e8400"])
        assert result.exit_code == 1  # exit 1 = not found (valid input, no row)

    def test_inspect_accepts_full_uuid(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = MagicMock()
        conn = MagicMock()
        conn.execute.return_value.one_or_none.return_value = None
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["inspect", "550e8400-e29b-41d4-a716-446655440000"])
        assert result.exit_code == 1  # exit 1 = not found (valid input, no row)

    # ── --last bound ───────────────────────────────────────────────────────────

    def test_history_last_exceeds_max_rejected(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        result = runner.invoke(app, ["history", "shop_main", "--last", "99999"])
        assert result.exit_code != 0

    def test_history_last_zero_rejected(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        result = runner.invoke(app, ["history", "shop_main", "--last", "0"])
        assert result.exit_code != 0

    def test_history_last_at_max_accepted(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        engine = _make_single_query_engine([])
        with patch("flowbyte.db.engine.get_internal_engine", return_value=engine):
            result = runner.invoke(app, ["history", "shop_main", "--last", "100"])
        assert result.exit_code == 0
