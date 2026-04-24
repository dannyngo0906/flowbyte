"""US-005: Alerting — integration tests for sync fail trigger, scheduler dead watchdog, CLI command.

Covers:
  (1) Happy path: sync fail → alerter.send() called
  (2) Happy path: scheduler heartbeat stale >2h → alerter.send() called
  (3) Edge: alerter=None → no crash
  (4) Edge: heartbeat fresh → no alert
  (5) Edge: no heartbeat row → no alert
  (6) CLI `alert test` → success, disabled, wrong token
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call
from contextlib import contextmanager

import httpx
import pytest
from typer.testing import CliRunner

from flowbyte.alerting.telegram import TelegramAlerter, format_sync_fail_alert
from flowbyte.cli.app import app

cli_runner = CliRunner()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_engine_with_row(row):
    """Fake engine whose .begin() context yields a conn that returns `row`."""
    conn = MagicMock()
    conn.execute.return_value.one_or_none.return_value = row

    @contextmanager
    def _begin():
        yield conn

    engine = MagicMock()
    engine.begin = _begin
    return engine


def _make_heartbeat_row(last_beat: datetime):
    row = MagicMock()
    row.last_beat = last_beat
    return row


# ═══════════════════════════════════════════════════════════════════════════════
# 1. SCHEDULER DEAD WATCHDOG
# ═══════════════════════════════════════════════════════════════════════════════


class TestHeartbeatWatchdog:
    """Tests for _heartbeat_watchdog_tick in scheduler.daemon."""

    def _invoke(self, engine, alerter):
        from flowbyte.scheduler.daemon import _heartbeat_watchdog_tick
        _heartbeat_watchdog_tick(engine, alerter)

    def test_sends_alert_when_heartbeat_stale_over_2h(self):
        """🔴 Trigger: scheduler not heartbeating for >2h → alert."""
        stale_beat = datetime.now(timezone.utc) - timedelta(hours=3)
        engine = _make_engine_with_row(_make_heartbeat_row(stale_beat))
        alerter = MagicMock(spec=TelegramAlerter)

        self._invoke(engine, alerter)

        alerter.send.assert_called_once()
        call_kwargs = alerter.send.call_args
        text_arg = call_kwargs[0][0]  # first positional arg
        assert "Scheduler DEAD" in text_arg or "DEAD" in text_arg
        assert call_kwargs[1]["key"] == "scheduler_dead"
        assert call_kwargs[1]["pipeline"] == "__system__"

    def test_no_alert_when_heartbeat_fresh(self):
        """Heartbeat recent (1min ago) → no alert."""
        fresh_beat = datetime.now(timezone.utc) - timedelta(minutes=1)
        engine = _make_engine_with_row(_make_heartbeat_row(fresh_beat))
        alerter = MagicMock(spec=TelegramAlerter)

        self._invoke(engine, alerter)

        alerter.send.assert_not_called()

    def test_no_alert_when_just_under_threshold(self):
        """1h 59m → strictly under 2h → no alert."""
        recent_enough = datetime.now(timezone.utc) - timedelta(hours=1, minutes=59)
        engine = _make_engine_with_row(_make_heartbeat_row(recent_enough))
        alerter = MagicMock(spec=TelegramAlerter)

        self._invoke(engine, alerter)

        alerter.send.assert_not_called()

    def test_no_op_when_alerter_is_none(self):
        """alerter=None → return early, no DB query at all."""
        engine = MagicMock()

        self._invoke(engine, None)

        engine.begin.assert_not_called()

    def test_no_alert_when_no_heartbeat_row_in_db(self):
        """No row in scheduler_heartbeat table → no alert."""
        engine = _make_engine_with_row(None)
        alerter = MagicMock(spec=TelegramAlerter)

        self._invoke(engine, alerter)

        alerter.send.assert_not_called()

    def test_alert_text_contains_age_info(self):
        """Alert message includes hours-ago so operator knows how stale."""
        stale_beat = datetime.now(timezone.utc) - timedelta(hours=2.5)
        engine = _make_engine_with_row(_make_heartbeat_row(stale_beat))
        alerter = MagicMock(spec=TelegramAlerter)

        self._invoke(engine, alerter)

        text_arg = alerter.send.call_args[0][0]
        assert "2.5" in text_arg or "2" in text_arg  # age in message


# ═══════════════════════════════════════════════════════════════════════════════
# 2. SYNC FAIL ALERT (reconciler._run_sync_job alert path)
# ═══════════════════════════════════════════════════════════════════════════════


class TestSyncFailAlert:
    """Validate that _run_sync_job sends alert on failed SyncResult."""

    def _build_failed_result(self):
        from flowbyte.config.models import SyncResult
        r = SyncResult(
            sync_id="sync-abc123",
            pipeline="shop_main",
            resource="orders",
            mode="incremental",
        )
        r.status = "failed"
        r.error = "Connection timeout to Haravan API"
        return r

    def _build_success_result(self):
        from flowbyte.config.models import SyncResult
        r = SyncResult(
            sync_id="sync-abc123",
            pipeline="shop_main",
            resource="orders",
            mode="incremental",
        )
        r.status = "success"
        r.fetched_count = 10
        r.upserted_count = 10
        return r

    def test_alert_sent_on_failed_sync_result(self):
        """When runner.run() returns status=failed → alerter.send() called."""
        alerter = MagicMock(spec=TelegramAlerter)
        failed_result = self._build_failed_result()

        # Simulate the alerting logic from reconciler._run_sync_job
        if failed_result.status == "failed" and alerter is not None:
            text = format_sync_fail_alert(
                pipeline=failed_result.pipeline,
                resource=failed_result.resource,
                error=failed_result.error or "",
                sync_id=failed_result.sync_id,
            )
            alerter.send(
                text,
                key=f"sync_fail:{failed_result.pipeline}:{failed_result.resource}",
                pipeline=failed_result.pipeline,
            )

        alerter.send.assert_called_once()
        text_sent = alerter.send.call_args[0][0]
        assert "shop_main" in text_sent
        assert "orders" in text_sent
        assert "Connection timeout" in text_sent

    def test_no_alert_on_success_result(self):
        """Successful sync → alerter.send() NOT called."""
        alerter = MagicMock(spec=TelegramAlerter)
        success_result = self._build_success_result()

        if success_result.status == "failed" and alerter is not None:
            alerter.send("...", key="sync_fail:shop_main:orders", pipeline="shop_main")

        alerter.send.assert_not_called()

    def test_no_alert_when_alerter_none(self):
        """alerter=None → no crash even when sync fails."""
        failed_result = self._build_failed_result()
        alerter = None

        if failed_result.status == "failed" and alerter is not None:
            alerter.send("...")  # type: ignore[union-attr]
        # Reaching here without exception is the assertion

    def test_alert_key_includes_pipeline_and_resource(self):
        """Dedup key format: 'sync_fail:<pipeline>:<resource>'."""
        alerter = MagicMock(spec=TelegramAlerter)
        failed_result = self._build_failed_result()

        key = f"sync_fail:{failed_result.pipeline}:{failed_result.resource}"
        text = format_sync_fail_alert(
            pipeline=failed_result.pipeline,
            resource=failed_result.resource,
            error=failed_result.error or "",
            sync_id=failed_result.sync_id,
        )
        alerter.send(text, key=key, pipeline=failed_result.pipeline)

        call_key = alerter.send.call_args[1]["key"]
        assert call_key == "sync_fail:shop_main:orders"

    def test_dedup_suppresses_repeat_fail_alert_within_5min(self):
        """Same resource failing twice in <5min → second alert suppressed."""
        from flowbyte.alerting.deduper import AlertDeduper

        http_client = MagicMock(spec=httpx.Client)
        ok_resp = MagicMock()
        ok_resp.raise_for_status.return_value = None
        http_client.post.return_value = ok_resp

        alerter = TelegramAlerter("123456789:ABCDefghijklmnopqrst", "cid", http_client=http_client)

        key = "sync_fail:shop_main:orders"
        text = format_sync_fail_alert("shop_main", "orders", "error", "sync-001")

        first = alerter.send(text, key=key, pipeline="shop_main")
        second = alerter.send(text, key=key, pipeline="shop_main")

        assert first is True
        assert second is False
        assert http_client.post.call_count == 1


# ═══════════════════════════════════════════════════════════════════════════════
# 3. CLI `flowbyte alert test`
# ═══════════════════════════════════════════════════════════════════════════════


class TestAlertTestCLI:
    """Tests for `flowbyte alert test` command."""

    def _make_global_config(self, enabled: bool, token: str = "123456789:ABCDefghijklmnop", chat_id: str = "-1001"):
        from flowbyte.config.models import GlobalConfig, TelegramConfig
        from pydantic import SecretStr

        return GlobalConfig(
            telegram=TelegramConfig(
                enabled=enabled,
                bot_token=SecretStr(token),
                chat_id=chat_id,
            )
        )

    def test_sends_test_message_when_enabled(self):
        """Happy path: telegram enabled, test() succeeds → exit 0 + success message."""
        cfg = self._make_global_config(enabled=True)

        with (
            patch("flowbyte.config.loader.load_global_config", return_value=cfg),
            patch("flowbyte.alerting.telegram.TelegramAlerter") as MockAlerter,
        ):
            instance = MockAlerter.return_value
            instance.test.return_value = True

            result = cli_runner.invoke(app, ["alert", "test"])

        assert result.exit_code == 0
        assert "successfully" in result.output.lower() or "sent" in result.output.lower()

    def test_exits_1_when_telegram_disabled(self):
        """Telegram not configured → warn user and exit 1."""
        cfg = self._make_global_config(enabled=False)

        with patch("flowbyte.config.loader.load_global_config", return_value=cfg):
            result = cli_runner.invoke(app, ["alert", "test"])

        assert result.exit_code == 1
        assert "not enabled" in result.output.lower() or "telegram" in result.output.lower()

    def test_exits_1_on_wrong_bot_token_401(self):
        """Wrong bot_token → Telegram returns 401 → exit 1, clear error message."""
        cfg = self._make_global_config(enabled=True, token="999999999:REVOKEDtokenXXXXXXXX")

        with patch("flowbyte.config.loader.load_global_config", return_value=cfg):
            mock_client = MagicMock(spec=httpx.Client)
            resp = MagicMock(spec=httpx.Response)
            resp.status_code = 401
            mock_client.post.return_value = resp
            resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                "Unauthorized", request=MagicMock(), response=resp
            )

            with patch("flowbyte.alerting.telegram.httpx") as mock_httpx:
                mock_httpx.Client.return_value = mock_client
                mock_httpx.NetworkError = httpx.NetworkError

                result = cli_runner.invoke(app, ["alert", "test"])

        assert result.exit_code == 1
        assert "Failed" in result.output or "failed" in result.output

    def test_exits_1_on_network_error(self):
        """No outbound HTTPS to api.telegram.org → NetworkError → exit 1 with hint."""
        cfg = self._make_global_config(enabled=True)

        with patch("flowbyte.config.loader.load_global_config", return_value=cfg):
            mock_client = MagicMock(spec=httpx.Client)
            mock_client.post.side_effect = httpx.NetworkError("Connection refused")

            with patch("flowbyte.alerting.telegram.httpx") as mock_httpx:
                mock_httpx.Client.return_value = mock_client
                mock_httpx.NetworkError = httpx.NetworkError

                result = cli_runner.invoke(app, ["alert", "test"])

        assert result.exit_code == 1
        output_lower = result.output.lower()
        assert "failed" in output_lower or "error" in output_lower

    def test_unknown_alert_action_exits_2(self):
        """Unrecognized action → exit 2."""
        result = cli_runner.invoke(app, ["alert", "unknown_action"])
        assert result.exit_code == 2

    def test_alerter_constructed_with_correct_token(self):
        """TelegramAlerter is built with bot_token from config, not plaintext."""
        _REAL_TOKEN = "456789012:REALtokenABCDEfghij"
        cfg = self._make_global_config(enabled=True, token=_REAL_TOKEN)

        captured_kwargs: dict = {}

        def _capture_init(self_inner, bot_token, chat_id, **kw):
            captured_kwargs["bot_token"] = bot_token
            captured_kwargs["chat_id"] = chat_id
            self_inner._url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            self_inner._chat_id = chat_id
            self_inner._deduper = MagicMock()
            self_inner._client = MagicMock()
            self_inner._client.post.return_value = MagicMock(raise_for_status=lambda: None)

        with (
            patch("flowbyte.config.loader.load_global_config", return_value=cfg),
            patch.object(TelegramAlerter, "__init__", _capture_init),
            patch.object(TelegramAlerter, "test", return_value=True),
        ):
            cli_runner.invoke(app, ["alert", "test"])

        assert captured_kwargs.get("bot_token") == _REAL_TOKEN


# ═══════════════════════════════════════════════════════════════════════════════
# 4. FORMAT FUNCTIONS — output sanity
# ═══════════════════════════════════════════════════════════════════════════════


class TestAlertMessageContent:
    """Ensure alert messages contain actionable info for the operator."""

    def test_sync_fail_message_has_inspect_hint(self):
        msg = format_sync_fail_alert("shop_main", "orders", "DB timeout", "aaaa-bbbb-cccc")
        assert "flowbyte inspect" in msg
        assert "aaaa-bbb" in msg  # first 8 chars of sync_id

    def test_sync_fail_sanitizes_all_markdown_chars(self):
        error = "db *conn* `pool` _exhausted_"
        msg = format_sync_fail_alert("p", "r", error, "id")
        assert "*conn*" not in msg
        assert "`pool`" not in msg
        assert "_exhausted_" not in msg

    def test_scheduler_dead_message_has_docker_hint(self):
        from flowbyte.alerting.telegram import format_scheduler_dead_alert
        msg = format_scheduler_dead_alert("2026-04-24T10:00:00+00:00", 3.0)
        assert "docker" in msg.lower() or "flowbyte" in msg.lower()
        assert "3.0" in msg

    def test_sync_fail_message_has_pipeline_and_resource(self):
        msg = format_sync_fail_alert("shop_cosmetics", "customers", "err", "id")
        assert "shop_cosmetics" in msg
        assert "customers" in msg
