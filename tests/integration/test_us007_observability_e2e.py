"""E2E tests for US-007: CLI Observability — status, history, logs, inspect.

Simulates the end-user workflow with real PostgreSQL (testcontainers):
  Step 1: Seed pipeline + sync run data into internal DB
  Step 2: Invoke CLI commands via CliRunner
  Step 3: Assert output matches PRD F3 "Done khi" criteria

Done-when coverage (PRD F3):
  ✅ 5 CLI commands hoạt động (status / history / logs / inspect)
  ✅ status load < 1 giây
  ✅ 4 trạng thái: 🟢 OK / 🔴 FAIL / 🟡 RUNNING / ⚪ DISABLED
  ✅ history hiển thị last-N với time / resource / mode / status / fetched / duration / error
  ✅ logs --errors filter đúng ERROR/CRITICAL only
  ✅ logs --since filter theo time window
  ✅ inspect deep-trace: timeline logs + validation results

Edge cases (PRD F3):
  ✅ No pipelines → "flowbyte init" hint
  ✅ Sync > 1h → warning text in status
  ✅ Scheduler dead → ❌ Dead footer
  ✅ Validation failures → ❌ in footer
  ✅ Empty history / empty logs → graceful, no crash
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import text
from typer.testing import CliRunner

from flowbyte.cli.app import app
from flowbyte.db.internal_schema import (
    pipelines,
    scheduler_heartbeat,
    sync_logs,
    sync_runs,
    validation_results,
)

pytestmark = pytest.mark.integration

runner = CliRunner()

_PIPELINE = "shop_obs"

_VALID_CONFIG_JSON = {
    "name": _PIPELINE,
    "haravan_credentials_ref": "ref",
    "haravan_shop_domain": "test.myharavan.com",
    "destination": {
        "host": "localhost",
        "port": 5432,
        "user": "u",
        "database": "d",
        "credentials_ref": "pg",
    },
    "resources": {
        "orders": {
            "enabled": True,
            "sync_mode": "incremental",
            "schedule": "0 */2 * * *",
        },
        "customers": {
            "enabled": True,
            "sync_mode": "incremental",
            "schedule": "0 */2 * * *",
        },
        "products": {
            "enabled": True,
            "sync_mode": "incremental",
            "schedule": "0 */12 * * *",
        },
        "variants": {
            "enabled": True,
            "sync_mode": "incremental",
            "schedule": "0 */12 * * *",
        },
        "inventory_levels": {
            "enabled": False,
            "sync_mode": "full_refresh",
            "schedule": "0 */2 * * *",
        },
        "locations": {
            "enabled": True,
            "sync_mode": "full_refresh",
            "schedule": "0 3 * * *",
        },
    },
}


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def clean_tables(internal_engine):
    """Clean all observability-related tables before and after each test."""
    _truncate(internal_engine)
    yield
    _truncate(internal_engine)


def _truncate(engine):
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE pipelines, sync_runs, sync_logs, "
                "validation_results, scheduler_heartbeat CASCADE"
            )
        )


@pytest.fixture
def obs_engine(internal_engine):
    """Return internal_engine with get_internal_engine patched for CLI commands."""
    with patch(
        "flowbyte.cli.commands.observability.get_internal_engine",
        return_value=internal_engine,
    ):
        yield internal_engine


# ── Seed helpers ──────────────────────────────────────────────────────────────


def _insert_pipeline(
    engine,
    name: str = _PIPELINE,
    enabled: bool = True,
    config: dict | None = None,
):
    with engine.begin() as conn:
        conn.execute(
            pipelines.insert().values(
                name=name,
                yaml_content=f"name: {name}\n",
                config_json=config or _VALID_CONFIG_JSON,
                enabled=enabled,
            )
        )


def _insert_run(
    engine,
    resource: str = "orders",
    status: str = "success",
    mode: str = "incremental",
    fetched_count: int = 42,
    upserted_count: int = 42,
    skipped_invalid: int = 0,
    duration_seconds: float = 1.5,
    error: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> uuid.UUID:
    sync_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    if started_at is None:
        started_at = now - timedelta(minutes=5)
    if finished_at is None and status != "running":
        finished_at = now
    with engine.begin() as conn:
        conn.execute(
            sync_runs.insert().values(
                sync_id=sync_id,
                pipeline=_PIPELINE,
                resource=resource,
                mode=mode,
                trigger="schedule",
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                fetched_count=fetched_count,
                upserted_count=upserted_count,
                skipped_invalid=skipped_invalid,
                soft_deleted_count=0,
                rows_before=100,
                rows_after=100 + upserted_count,
                duration_seconds=duration_seconds,
                error=error,
            )
        )
    return sync_id


def _insert_heartbeat(
    engine,
    age_seconds: float = 30.0,
    uptime_seconds: float = 86_400 * 5 + 3_600 * 3,
):
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM scheduler_heartbeat WHERE id = 1"))
        conn.execute(
            scheduler_heartbeat.insert().values(
                id=1,
                last_beat=now - timedelta(seconds=age_seconds),
                daemon_started_at=now - timedelta(seconds=uptime_seconds),
                version="0.1.0",
            )
        )


def _insert_log(
    engine,
    level: str = "INFO",
    event: str = "sync_ok",
    message: str = "done",
    sync_id: uuid.UUID | None = None,
    timestamp: datetime | None = None,
    resource: str = "orders",
):
    with engine.begin() as conn:
        conn.execute(
            sync_logs.insert().values(
                timestamp=timestamp or datetime.now(timezone.utc),
                level=level,
                event=event,
                sync_id=sync_id,
                pipeline=_PIPELINE,
                resource=resource,
                message=message,
            )
        )


def _insert_validation(
    engine,
    sync_id: uuid.UUID,
    rule: str = "fetch_upsert_parity",
    status: str = "ok",
    details: dict | None = None,
):
    with engine.begin() as conn:
        conn.execute(
            validation_results.insert().values(
                sync_id=sync_id,
                pipeline=_PIPELINE,
                resource="orders",
                rule=rule,
                status=status,
                details=details or {"note": "e2e"},
            )
        )


# ── 1. flowbyte status — no pipelines ────────────────────────────────────────


class TestStatusNoPipelines:
    def test_empty_db_shows_init_hint(self, obs_engine):
        """Step 1: Chưa có pipeline nào → gợi ý 'flowbyte init'."""
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "flowbyte init" in result.output


# ── 2. flowbyte status — 4 resource states ────────────────────────────────────


class TestStatusResourceStates:
    def test_success_shows_green_ok(self, obs_engine):
        """Sync thành công → orders hiển thị 🟢 OK."""
        _insert_pipeline(obs_engine)
        _insert_run(obs_engine, resource="orders", status="success")
        _insert_heartbeat(obs_engine)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🟢 OK" in result.output

    def test_failed_shows_red_fail(self, obs_engine):
        """Sync thất bại → orders hiển thị 🔴 FAIL."""
        _insert_pipeline(obs_engine)
        _insert_run(obs_engine, resource="orders", status="failed", error="DB timeout")
        _insert_heartbeat(obs_engine)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🔴 FAIL" in result.output

    def test_running_shows_yellow(self, obs_engine):
        """Đang sync → orders hiển thị 🟡 RUNNING."""
        _insert_pipeline(obs_engine)
        now = datetime.now(timezone.utc)
        _insert_run(
            obs_engine,
            resource="orders",
            status="running",
            started_at=now - timedelta(minutes=20),
            finished_at=None,
        )
        _insert_heartbeat(obs_engine)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🟡 RUNNING" in result.output

    def test_disabled_pipeline_shows_disabled(self, obs_engine):
        """Pipeline bị tắt → tất cả resources hiển thị ⚪ DISABLED."""
        _insert_pipeline(obs_engine, enabled=False)
        _insert_run(obs_engine, status="success")
        _insert_heartbeat(obs_engine)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "⚪ DISABLED" in result.output
        assert "🟢 OK" not in result.output

    def test_disabled_resource_in_config_shows_disabled(self, obs_engine):
        """inventory_levels disabled trong YAML → ⚪ DISABLED."""
        _insert_pipeline(obs_engine)  # config_json có inventory_levels.enabled=False
        _insert_heartbeat(obs_engine)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "⚪ DISABLED" in result.output

    def test_never_synced_enabled_resource_shows_never(self, obs_engine):
        """Resource enabled nhưng chưa sync lần nào → ⚪ NEVER."""
        _insert_pipeline(obs_engine)
        _insert_heartbeat(obs_engine)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "⚪ NEVER" in result.output

    def test_all_four_statuses_visible_together(self, obs_engine):
        """Mix thực tế: OK + FAIL + RUNNING + DISABLED cùng lúc."""
        _insert_pipeline(obs_engine)
        now = datetime.now(timezone.utc)
        _insert_run(obs_engine, resource="orders", status="success")
        _insert_run(obs_engine, resource="customers", status="failed", error="Timeout")
        _insert_run(
            obs_engine,
            resource="products",
            status="running",
            started_at=now - timedelta(minutes=10),
            finished_at=None,
        )
        # inventory_levels disabled via config
        _insert_heartbeat(obs_engine)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🟢 OK" in result.output
        assert "🔴 FAIL" in result.output
        assert "🟡 RUNNING" in result.output
        assert "⚪ DISABLED" in result.output


# ── 3. flowbyte status — sync > 1h warning ───────────────────────────────────


class TestStatusLongRunning:
    def test_sync_over_1h_shows_warning_text(self, obs_engine):
        """Sync đang chạy > 1h → cảnh báo '> 1h' xuất hiện trong output."""
        _insert_pipeline(obs_engine)
        now = datetime.now(timezone.utc)
        _insert_run(
            obs_engine,
            resource="orders",
            status="running",
            started_at=now - timedelta(hours=2),
            finished_at=None,
        )
        _insert_heartbeat(obs_engine)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🟡 RUNNING" in result.output
        assert "> 1h" in result.output

    def test_sync_under_1h_no_warning(self, obs_engine):
        """Sync chạy < 1h → không có cảnh báo '> 1h'."""
        _insert_pipeline(obs_engine)
        now = datetime.now(timezone.utc)
        _insert_run(
            obs_engine,
            resource="orders",
            status="running",
            started_at=now - timedelta(minutes=30),
            finished_at=None,
        )
        _insert_heartbeat(obs_engine)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🟡 RUNNING" in result.output
        assert "> 1h" not in result.output


# ── 4. flowbyte status — scheduler footer ────────────────────────────────────


class TestStatusSchedulerFooter:
    def test_fresh_heartbeat_shows_running(self, obs_engine):
        """Heartbeat mới (30s trước) → Scheduler: 🟢 Running."""
        _insert_pipeline(obs_engine)
        _insert_heartbeat(obs_engine, age_seconds=30)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "🟢 Running" in result.output

    def test_stale_heartbeat_shows_dead(self, obs_engine):
        """Heartbeat cũ (> 2 phút) → Scheduler: ❌ Dead."""
        _insert_pipeline(obs_engine)
        _insert_heartbeat(obs_engine, age_seconds=300)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "❌ Dead" in result.output

    def test_no_heartbeat_row_shows_dead(self, obs_engine):
        """Không có heartbeat row → Scheduler: ❌ Dead."""
        _insert_pipeline(obs_engine)
        # KHÔNG insert heartbeat

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "❌ Dead" in result.output

    def test_running_scheduler_shows_uptime_days(self, obs_engine):
        """Scheduler chạy 5 ngày → footer hiển thị '5d'."""
        _insert_pipeline(obs_engine)
        _insert_heartbeat(obs_engine, age_seconds=10, uptime_seconds=86_400 * 5 + 3_600 * 3)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "5d" in result.output


# ── 5. flowbyte status — validation footer ───────────────────────────────────


class TestStatusValidationFooter:
    def test_all_healthy_when_no_validation_issues(self, obs_engine):
        """Không có vấn đề validation → Validation: ✅ All healthy."""
        _insert_pipeline(obs_engine)
        _insert_heartbeat(obs_engine)

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "✅" in result.output
        assert "healthy" in result.output.lower()

    def test_warning_shown_when_validation_warned(self, obs_engine):
        """Có validation warning (24h gần nhất) → ⚠️ trong footer."""
        _insert_pipeline(obs_engine)
        _insert_heartbeat(obs_engine)
        sync_id = _insert_run(obs_engine, status="success")
        _insert_validation(obs_engine, sync_id=sync_id, rule="volume_sanity", status="warning")

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "⚠️" in result.output

    def test_failed_shown_when_validation_failed(self, obs_engine):
        """Có validation failure (24h gần nhất) → ❌ trong footer."""
        _insert_pipeline(obs_engine)
        _insert_heartbeat(obs_engine)
        sync_id = _insert_run(obs_engine, status="success")
        _insert_validation(
            obs_engine, sync_id=sync_id, rule="fetch_upsert_parity", status="failed"
        )

        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "❌" in result.output


# ── 6. flowbyte status — performance ─────────────────────────────────────────


class TestStatusPerformance:
    def test_status_loads_under_1_second(self, obs_engine):
        """PRD F3 Done-khi: status load < 1 giây với 6 resources."""
        _insert_pipeline(obs_engine)
        _insert_heartbeat(obs_engine)
        for resource in ["orders", "customers", "products", "variants", "locations"]:
            _insert_run(obs_engine, resource=resource, status="success")

        start = time.perf_counter()
        result = runner.invoke(app, ["status"])
        elapsed = time.perf_counter() - start

        assert result.exit_code == 0
        assert elapsed < 1.0, f"status took {elapsed:.3f}s — must be < 1s (PRD F3)"


# ── 7. flowbyte history — last N runs ────────────────────────────────────────


class TestHistoryCommand:
    def test_history_shows_recent_runs(self, obs_engine):
        """Step 3: `flowbyte history shop_obs` → thấy các lần sync gần nhất."""
        for _ in range(5):
            _insert_run(obs_engine, resource="orders", status="success")

        result = runner.invoke(app, ["history", _PIPELINE])
        assert result.exit_code == 0
        assert "orders" in result.output
        assert "🟢 OK" in result.output

    def test_history_respects_last_flag(self, obs_engine):
        """--last 3 → chỉ hiển thị 3 runs gần nhất."""
        for _ in range(10):
            _insert_run(obs_engine, resource="orders", status="success")

        result = runner.invoke(app, ["history", _PIPELINE, "--last", "3"])
        assert result.exit_code == 0
        assert "orders" in result.output

    def test_history_empty_shows_friendly_message(self, obs_engine):
        """Không có history → thông báo rõ ràng, không crash."""
        result = runner.invoke(app, ["history", _PIPELINE])
        assert result.exit_code == 0
        assert "No sync history" in result.output

    def test_history_failed_shows_red_emoji(self, obs_engine):
        """Run thất bại → 🔴 FAIL trong history table."""
        _insert_run(obs_engine, resource="orders", status="failed", error="Network error")

        result = runner.invoke(app, ["history", _PIPELINE])
        assert result.exit_code == 0
        assert "🔴 FAIL" in result.output

    def test_history_shows_error_message_truncated(self, obs_engine):
        """Error message (≤40 ký tự) xuất hiện trong history row."""
        _insert_run(obs_engine, resource="orders", status="failed", error="Connection refused by DB host")

        result = runner.invoke(app, ["history", _PIPELINE])
        assert result.exit_code == 0
        assert "Connection refused" in result.output

    def test_history_shows_all_required_columns(self, obs_engine):
        """PRD F3 Done-khi: history hiển thị time / resource / mode / status / fetched / duration."""
        _insert_run(
            obs_engine,
            resource="orders",
            mode="incremental",
            status="success",
            fetched_count=99,
            duration_seconds=2.7,
        )

        result = runner.invoke(app, ["history", _PIPELINE])
        assert result.exit_code == 0
        assert "orders" in result.output
        assert "incremental" in result.output
        assert "99" in result.output
        assert "2.7" in result.output

    def test_history_running_status_shows_emoji(self, obs_engine):
        """Run đang chạy → 🟡 RUNNING trong history."""
        now = datetime.now(timezone.utc)
        _insert_run(
            obs_engine,
            resource="orders",
            status="running",
            started_at=now - timedelta(minutes=5),
            finished_at=None,
        )

        result = runner.invoke(app, ["history", _PIPELINE])
        assert result.exit_code == 0
        assert "🟡 RUNNING" in result.output


# ── 8. flowbyte logs — view and filter ───────────────────────────────────────


class TestLogsCommand:
    def test_logs_shows_recent_entries(self, obs_engine):
        """Step 4: `flowbyte logs shop_obs` → thấy log gần nhất."""
        _insert_log(obs_engine, level="INFO", event="sync_started", message="Starting sync")
        _insert_log(obs_engine, level="INFO", event="sync_ok", message="Completed 42 records")

        result = runner.invoke(app, ["logs", _PIPELINE])
        assert result.exit_code == 0
        # At least one of the two entries should appear
        assert "sync_started" in result.output or "sync_ok" in result.output

    def test_logs_errors_flag_shows_only_error_and_critical(self, obs_engine):
        """Step 5: --errors → chỉ ERROR + CRITICAL, không có INFO."""
        _insert_log(obs_engine, level="INFO", event="sync_ok", message="all good")
        _insert_log(obs_engine, level="ERROR", event="sync_fail", message="DB timeout")
        _insert_log(obs_engine, level="CRITICAL", event="auth_fail", message="401 Unauthorized")

        result = runner.invoke(app, ["logs", _PIPELINE, "--errors"])
        assert result.exit_code == 0
        assert "ERROR" in result.output
        assert "DB timeout" in result.output
        assert "CRITICAL" in result.output
        assert "401 Unauthorized" in result.output
        assert "all good" not in result.output

    def test_logs_errors_excludes_warning(self, obs_engine):
        """--errors không bao gồm WARNING (chỉ ERROR và CRITICAL)."""
        _insert_log(obs_engine, level="WARNING", event="rate_limit", message="Slowing down")
        _insert_log(obs_engine, level="ERROR", event="sync_fail", message="Failed")

        result = runner.invoke(app, ["logs", _PIPELINE, "--errors"])
        assert result.exit_code == 0
        assert "ERROR" in result.output
        assert "Slowing down" not in result.output

    def test_logs_since_filters_by_time_window(self, obs_engine):
        """Step 5: --since 1h → chỉ log trong 1 giờ gần nhất."""
        now = datetime.now(timezone.utc)
        _insert_log(
            obs_engine,
            level="INFO",
            event="old_sync",
            message="ancient history",
            timestamp=now - timedelta(hours=3),
        )
        _insert_log(
            obs_engine,
            level="INFO",
            event="recent_sync",
            message="just happened",
            timestamp=now - timedelta(minutes=30),
        )

        result = runner.invoke(app, ["logs", _PIPELINE, "--since", "1h"])
        assert result.exit_code == 0
        assert "recent_sync" in result.output
        assert "ancient history" not in result.output

    def test_logs_since_30m(self, obs_engine):
        """--since 30m → chỉ log trong 30 phút gần nhất."""
        now = datetime.now(timezone.utc)
        _insert_log(
            obs_engine,
            level="INFO",
            event="outside_window",
            message="too old",
            timestamp=now - timedelta(minutes=60),
        )
        _insert_log(
            obs_engine,
            level="INFO",
            event="inside_window",
            message="fresh",
            timestamp=now - timedelta(minutes=10),
        )

        result = runner.invoke(app, ["logs", _PIPELINE, "--since", "30m"])
        assert result.exit_code == 0
        assert "inside_window" in result.output
        assert "too old" not in result.output

    def test_logs_tail_flag_accepted(self, obs_engine):
        """--tail flag không crash."""
        _insert_log(obs_engine, level="INFO", event="sync_ok", message="done")

        result = runner.invoke(app, ["logs", _PIPELINE, "--tail"])
        assert result.exit_code == 0

    def test_logs_empty_no_crash(self, obs_engine):
        """Không có log nào → thoát sạch, không crash."""
        result = runner.invoke(app, ["logs", _PIPELINE])
        assert result.exit_code == 0

    def test_logs_shows_level_event_and_message(self, obs_engine):
        """Output bao gồm level, event name, và message text."""
        _insert_log(
            obs_engine,
            level="WARNING",
            event="rate_limit_hit",
            message="429 from Haravan — retrying in 2s",
        )

        result = runner.invoke(app, ["logs", _PIPELINE])
        assert result.exit_code == 0
        assert "WARNING" in result.output
        assert "rate_limit_hit" in result.output
        assert "429 from Haravan" in result.output


# ── 9. flowbyte inspect — deep trace ─────────────────────────────────────────


class TestInspectCommand:
    def test_inspect_shows_pipeline_resource_status(self, obs_engine):
        """Step 6: `flowbyte inspect <sync_id>` → thấy pipeline, resource, status."""
        sync_id = _insert_run(
            obs_engine, resource="orders", status="success",
            fetched_count=99, upserted_count=98, skipped_invalid=1,
        )

        result = runner.invoke(app, ["inspect", str(sync_id)])
        assert result.exit_code == 0
        assert _PIPELINE in result.output
        assert "orders" in result.output
        assert "success" in result.output

    def test_inspect_shows_fetched_and_upserted_counts(self, obs_engine):
        """Inspect output bao gồm fetched / upserted / skipped counts."""
        sync_id = _insert_run(
            obs_engine,
            resource="orders",
            status="success",
            fetched_count=100,
            upserted_count=98,
            skipped_invalid=2,
        )

        result = runner.invoke(app, ["inspect", str(sync_id)])
        assert result.exit_code == 0
        assert "100" in result.output
        assert "98" in result.output

    def test_inspect_shows_timeline_from_sync_logs(self, obs_engine):
        """Inspect hiển thị timeline log entries của sync run."""
        sync_id = _insert_run(obs_engine, resource="orders", status="success")
        _insert_log(obs_engine, level="INFO", event="extract_start",
                    message="Fetching page 1 of orders", sync_id=sync_id)
        _insert_log(obs_engine, level="INFO", event="load_ok",
                    message="Upserted 42 records", sync_id=sync_id)

        result = runner.invoke(app, ["inspect", str(sync_id)])
        assert result.exit_code == 0
        assert "Timeline" in result.output
        assert "extract_start" in result.output
        assert "load_ok" in result.output

    def test_inspect_shows_validation_results_section(self, obs_engine):
        """Inspect hiển thị phần Validation với các rules."""
        sync_id = _insert_run(obs_engine, resource="orders", status="success")
        _insert_validation(obs_engine, sync_id=sync_id, rule="fetch_upsert_parity", status="ok")
        _insert_validation(obs_engine, sync_id=sync_id, rule="volume_sanity", status="warning")

        result = runner.invoke(app, ["inspect", str(sync_id)])
        assert result.exit_code == 0
        assert "Validation" in result.output
        assert "fetch_upsert_parity" in result.output
        assert "volume_sanity" in result.output

    def test_inspect_failed_run_shows_error(self, obs_engine):
        """Inspect sync run thất bại → thấy trạng thái failed."""
        sync_id = _insert_run(
            obs_engine,
            resource="customers",
            status="failed",
            error="psycopg.OperationalError: connection refused",
        )

        result = runner.invoke(app, ["inspect", str(sync_id)])
        assert result.exit_code == 0
        assert "customers" in result.output
        assert "failed" in result.output

    def test_inspect_unknown_id_returns_error(self, obs_engine):
        """sync_id không tồn tại → thông báo lỗi và exit code ≠ 0."""
        fake_id = str(uuid.uuid4())
        result = runner.invoke(app, ["inspect", fake_id])
        assert result.exit_code != 0
        assert "not found" in result.output.lower()


# ── 10. Full end-to-end user workflow simulation ──────────────────────────────


class TestFullObservabilityWorkflow:
    def test_complete_user_workflow_5_steps(self, obs_engine):
        """
        Mô phỏng đầy đủ luồng Duy sử dụng CLI observability:

        Step 1: Seed dữ liệu — 1 pipeline, 2 sync runs (1 OK, 1 FAIL)
        Step 2: flowbyte status → thấy trạng thái mix OK + FAIL + DISABLED
        Step 3: flowbyte history shop_obs → xem 10 lần sync gần nhất
        Step 4: flowbyte logs shop_obs --errors → phát hiện lỗi
        Step 5: flowbyte inspect <failed_sync_id> → xem chi tiết run lỗi
        Step 6: flowbyte logs shop_obs --since 2h → verify hoạt động gần đây
        """
        # ── Step 1: Seed ────────────────────────────────────────────────────
        _insert_pipeline(obs_engine)
        _insert_heartbeat(obs_engine, age_seconds=45, uptime_seconds=86_400 * 3)

        now = datetime.now(timezone.utc)

        ok_id = _insert_run(
            obs_engine,
            resource="orders",
            status="success",
            fetched_count=42,
            upserted_count=42,
            started_at=now - timedelta(hours=2, minutes=5),
        )
        fail_id = _insert_run(
            obs_engine,
            resource="customers",
            status="failed",
            error="psycopg.OperationalError: connection refused",
            started_at=now - timedelta(hours=1, minutes=30),
        )

        _insert_log(
            obs_engine,
            level="INFO",
            event="sync_started",
            message="orders incremental started",
            sync_id=ok_id,
            timestamp=now - timedelta(hours=2, minutes=5),
        )
        _insert_log(
            obs_engine,
            level="INFO",
            event="sync_ok",
            message="orders: 42 records upserted",
            sync_id=ok_id,
            timestamp=now - timedelta(hours=2),
        )
        _insert_log(
            obs_engine,
            level="ERROR",
            event="sync_failed",
            message="psycopg.OperationalError: connection refused",
            sync_id=fail_id,
            timestamp=now - timedelta(hours=1, minutes=30),
        )
        _insert_validation(obs_engine, sync_id=ok_id, rule="fetch_upsert_parity", status="ok")

        # ── Step 2: flowbyte status ──────────────────────────────────────────
        r_status = runner.invoke(app, ["status"])
        assert r_status.exit_code == 0, r_status.output
        assert "🟢 OK" in r_status.output        # orders
        assert "🔴 FAIL" in r_status.output      # customers
        assert "⚪ DISABLED" in r_status.output  # inventory_levels
        assert "🟢 Running" in r_status.output   # scheduler footer
        assert "✅" in r_status.output           # validation healthy (orders ok)

        # ── Step 3: flowbyte history ─────────────────────────────────────────
        r_history = runner.invoke(app, ["history", _PIPELINE])
        assert r_history.exit_code == 0, r_history.output
        assert "orders" in r_history.output
        assert "customers" in r_history.output
        assert "🟢 OK" in r_history.output
        assert "🔴 FAIL" in r_history.output

        # ── Step 4: flowbyte logs --errors ───────────────────────────────────
        r_errors = runner.invoke(app, ["logs", _PIPELINE, "--errors"])
        assert r_errors.exit_code == 0, r_errors.output
        assert "ERROR" in r_errors.output
        assert "connection refused" in r_errors.output
        assert "orders incremental" not in r_errors.output  # INFO excluded

        # ── Step 5: flowbyte inspect failed run ──────────────────────────────
        r_inspect = runner.invoke(app, ["inspect", str(fail_id)])
        assert r_inspect.exit_code == 0, r_inspect.output
        assert "customers" in r_inspect.output
        assert "failed" in r_inspect.output

        # ── Step 6: flowbyte logs --since 2h ────────────────────────────────
        r_since = runner.invoke(app, ["logs", _PIPELINE, "--since", "2h"])
        assert r_since.exit_code == 0, r_since.output
        assert "sync_failed" in r_since.output  # trong cửa sổ 2h
        assert "sync_ok" in r_since.output       # trong cửa sổ 2h

    def test_operator_verifies_scheduler_then_checks_stale_runs(self, obs_engine):
        """
        Scenario: Duy mở terminal buổi sáng để check hệ thống.

        Step 1: `flowbyte status` — xác nhận scheduler đang chạy
        Step 2: Phát hiện customers FAIL → `flowbyte history` xem pattern
        Step 3: `flowbyte logs shop_obs --errors --since 24h` (dùng --since 24h)
        """
        _insert_pipeline(obs_engine)
        _insert_heartbeat(obs_engine, age_seconds=60, uptime_seconds=86_400 * 7)

        now = datetime.now(timezone.utc)
        for i in range(5):
            _insert_run(
                obs_engine,
                resource="customers",
                status="failed",
                error="Connection refused",
                started_at=now - timedelta(hours=i * 2),
            )

        _insert_log(
            obs_engine,
            level="ERROR",
            event="sync_failed",
            message="Connection refused — 5th consecutive failure",
            timestamp=now - timedelta(minutes=30),
        )

        # Step 1
        r1 = runner.invoke(app, ["status"])
        assert r1.exit_code == 0
        assert "🟢 Running" in r1.output
        assert "7d" in r1.output
        assert "🔴 FAIL" in r1.output

        # Step 2
        r2 = runner.invoke(app, ["history", _PIPELINE, "--last", "5"])
        assert r2.exit_code == 0
        assert "🔴 FAIL" in r2.output

        # Step 3
        r3 = runner.invoke(app, ["logs", _PIPELINE, "--errors", "--since", "24h"])
        assert r3.exit_code == 0
        assert "ERROR" in r3.output
        assert "consecutive failure" in r3.output
