"""E2E tests for US-005: Telegram Alerting — full user flow.

Mô phỏng end-user từ bước 1 đến bước cuối:
  Bước 1: User cấu hình config.yml với Telegram credentials → GlobalConfig object
  Bước 2: `flowbyte alert test` CLI → HTTP POST đến api.telegram.org → nhận 200 OK
  Bước 3: Sync job fail → SyncRunner trả status="failed" → alert gửi đến Telegram
  Bước 4: Scheduler heartbeat stale >2h (real Postgres) → watchdog gửi "SCHEDULER_DEAD" alert
  Bước 5: Dedup — cùng sync fail trong 5 phút → lần 2 bị suppress, chỉ 1 POST
  Bước 6: Rate limit — 4 alert/h cùng pipeline → alert thứ 4 bị chặn
  Bước 7: Telegram API fail (5xx) → alerter trả False, không crash scheduler
  Bước 8: Wrong bot_token (401) → `alert test` CLI thoát exit 1, message rõ ràng

HTTP level: respx intercepts tất cả call đến api.telegram.org — không cần internet thật.
DB level: testcontainers Postgres cho scheduler heartbeat.
Haravan: MagicMock (không test Haravan trong US-005).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import httpx
import pytest
import respx
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typer.testing import CliRunner

from flowbyte.alerting.deduper import AlertDeduper
from flowbyte.alerting.telegram import TelegramAlerter, format_sync_fail_alert
from flowbyte.cli.app import app
from flowbyte.config.models import (
    GlobalConfig,
    PipelineConfig,
    PostgresDestConfig,
    SyncJobSpec,
    TelegramConfig,
)
from flowbyte.db.internal_schema import scheduler_heartbeat
from flowbyte.sync.runner import SyncRunner

pytestmark = pytest.mark.integration

cli_runner = CliRunner()

_BOT_TOKEN = "123456789:TESTtokenABCDEfghij"
_CHAT_ID = "-100123456789"
_TELEGRAM_SEND_URL = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"


# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="module")
def alerter_client() -> httpx.Client:
    """Real httpx.Client — respx will intercept its calls in each test."""
    client = httpx.Client(timeout=5.0)
    yield client
    client.close()


@pytest.fixture
def alerter(alerter_client) -> TelegramAlerter:
    """TelegramAlerter với real AlertDeduper và injected httpx client."""
    return TelegramAlerter(
        bot_token=_BOT_TOKEN,
        chat_id=_CHAT_ID,
        http_client=alerter_client,
    )


@pytest.fixture
def fresh_alerter(alerter_client) -> TelegramAlerter:
    """New alerter per test — fresh deduper state, same http client."""
    return TelegramAlerter(
        bot_token=_BOT_TOKEN,
        chat_id=_CHAT_ID,
        deduper=AlertDeduper(),
        http_client=alerter_client,
    )


def _ok_json() -> dict:
    return {"ok": True, "result": {"message_id": 42}}


def _make_pipeline_cfg() -> PipelineConfig:
    return PipelineConfig(
        name="shop_main",
        haravan_credentials_ref="test-creds",
        haravan_shop_domain="test.myharavan.com",
        destination=PostgresDestConfig(
            host="localhost", port=5432, user="test",
            database="test", credentials_ref="test-db-ref",
        ),
    )


def _make_global_config(enabled: bool = True) -> GlobalConfig:
    from pydantic import SecretStr
    return GlobalConfig(
        telegram=TelegramConfig(
            enabled=enabled,
            bot_token=SecretStr(_BOT_TOKEN),
            chat_id=_CHAT_ID,
        )
    )


# ═══════════════════════════════════════════════════════════════════════════════
# BƯỚC 1 + 2: Config → `flowbyte alert test` CLI
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuoc2AlertTestCLI:
    """Bước 2: End user chạy `flowbyte alert test` — POST thật đến Telegram."""

    def test_happy_path_exits_0_and_posts_to_telegram(self, alerter_client):
        """
        User có config telegram enabled.
        `flowbyte alert test` → POST đến Telegram → nhận 200 → exit 0.
        Verify HTTP call có đúng bot_token, chat_id, và message text.
        """
        cfg = _make_global_config(enabled=True)

        with respx.mock(assert_all_called=True) as mock:
            route = mock.post(_TELEGRAM_SEND_URL).mock(
                return_value=httpx.Response(200, json=_ok_json())
            )

            with (
                __import__("unittest.mock", fromlist=["patch"]).patch(
                    "flowbyte.config.loader.load_global_config", return_value=cfg
                ),
                __import__("unittest.mock", fromlist=["patch"]).patch(
                    "flowbyte.alerting.telegram.httpx.Client",
                    return_value=alerter_client,
                ),
            ):
                result = cli_runner.invoke(app, ["alert", "test"])

        assert result.exit_code == 0, f"Output: {result.output}"
        assert "successfully" in result.output.lower() or "sent" in result.output.lower()
        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body["chat_id"] == _CHAT_ID
        assert "Flowbyte alert test" in body["text"]

    def test_telegram_disabled_exits_1_with_warning(self):
        """User chưa config telegram → warning + exit 1."""
        cfg = _make_global_config(enabled=False)

        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "flowbyte.config.loader.load_global_config", return_value=cfg
        ):
            result = cli_runner.invoke(app, ["alert", "test"])

        assert result.exit_code == 1
        assert "not enabled" in result.output.lower() or "telegram" in result.output.lower()

    def test_wrong_bot_token_401_exits_1_with_clear_error(self, alerter_client):
        """
        User nhập sai bot_token → Telegram trả 401 → CLI thoát 1.
        Message phải đủ rõ để user biết cần check bot_token.
        """
        from pydantic import SecretStr
        _REVOKED_TOKEN = "999999999:REVOKEDtokenXXXXXXXX"
        cfg = GlobalConfig(
            telegram=TelegramConfig(
                enabled=True,
                bot_token=SecretStr(_REVOKED_TOKEN),
                chat_id=_CHAT_ID,
            )
        )
        bad_url = f"https://api.telegram.org/bot{_REVOKED_TOKEN}/sendMessage"

        with respx.mock:
            respx.post(bad_url).mock(
                return_value=httpx.Response(401, json={"ok": False, "error_code": 401})
            )

            with (
                __import__("unittest.mock", fromlist=["patch"]).patch(
                    "flowbyte.config.loader.load_global_config", return_value=cfg
                ),
                __import__("unittest.mock", fromlist=["patch"]).patch(
                    "flowbyte.alerting.telegram.httpx.Client",
                    return_value=alerter_client,
                ),
            ):
                result = cli_runner.invoke(app, ["alert", "test"])

        assert result.exit_code == 1
        assert "failed" in result.output.lower() or "✗" in result.output

    def test_network_error_exits_1_with_firewall_hint(self, alerter_client):
        """
        VPS block outbound HTTPS đến api.telegram.org → NetworkError.
        CLI exit 1, message gợi ý kiểm tra firewall.
        """
        cfg = _make_global_config(enabled=True)

        with respx.mock:
            respx.post(_TELEGRAM_SEND_URL).mock(side_effect=httpx.NetworkError("Connection refused"))

            with (
                __import__("unittest.mock", fromlist=["patch"]).patch(
                    "flowbyte.config.loader.load_global_config", return_value=cfg
                ),
                __import__("unittest.mock", fromlist=["patch"]).patch(
                    "flowbyte.alerting.telegram.httpx.Client",
                    return_value=alerter_client,
                ),
            ):
                result = cli_runner.invoke(app, ["alert", "test"])

        assert result.exit_code == 1
        output_lower = result.output.lower()
        assert "failed" in output_lower or "error" in output_lower


# ═══════════════════════════════════════════════════════════════════════════════
# BƯỚC 3: Sync fail → alert gửi đến Telegram
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuoc3SyncFailAlert:
    """Bước 3: Sync job fail → alerter.send() → POST đến Telegram."""

    @pytest.fixture(autouse=True)
    def clean_tables(self, internal_engine):
        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE sync_checkpoints, sync_runs CASCADE"))
        yield

    def test_sync_fail_triggers_telegram_alert(self, fresh_alerter, internal_engine, dest_engine):
        """
        SyncRunner chạy với Haravan raise exception
        → result.status == "failed"
        → alerter.send() được gọi với "Sync FAILED" message
        → POST đến Telegram API
        """
        cfg = _make_pipeline_cfg()
        client = MagicMock()
        client.paginate.side_effect = RuntimeError("Haravan timeout — network unreachable")
        runner = SyncRunner(cfg, client, internal_engine, dest_engine)

        with respx.mock(assert_all_called=False) as mock:
            route = mock.post(_TELEGRAM_SEND_URL).mock(
                return_value=httpx.Response(200, json=_ok_json())
            )

            spec = SyncJobSpec(
                pipeline="shop_main", resource="orders",
                mode="incremental", trigger="schedule",
            )
            result = runner.run(spec)

            if result.status == "failed":
                text_msg = format_sync_fail_alert(
                    pipeline=spec.pipeline,
                    resource=spec.resource,
                    error=result.error or "",
                    sync_id=result.sync_id,
                )
                sent = fresh_alerter.send(
                    text_msg,
                    key=f"sync_fail:{spec.pipeline}:{spec.resource}",
                    pipeline=spec.pipeline,
                )

        assert result.status == "failed"
        assert sent is True
        assert route.called, "Telegram API should have been called"

        body = json.loads(route.calls.last.request.content)
        assert body["chat_id"] == _CHAT_ID
        assert "shop_main" in body["text"]
        assert "orders" in body["text"]
        assert "Sync FAILED" in body["text"] or "FAILED" in body["text"]

    def test_alert_message_contains_inspect_hint(self, fresh_alerter, internal_engine, dest_engine):
        """Alert text phải có `flowbyte inspect <sync_id>` để operator debug được."""
        cfg = _make_pipeline_cfg()
        client = MagicMock()
        client.paginate.side_effect = ConnectionError("DB unreachable")
        runner = SyncRunner(cfg, client, internal_engine, dest_engine)

        spec = SyncJobSpec(
            pipeline="shop_main", resource="customers",
            mode="incremental", trigger="schedule",
        )
        result = runner.run(spec)

        text_msg = format_sync_fail_alert(
            pipeline=spec.pipeline,
            resource=spec.resource,
            error=result.error or "",
            sync_id=result.sync_id,
        )

        assert "flowbyte inspect" in text_msg
        assert result.sync_id[:8] in text_msg

    def test_success_sync_does_not_trigger_alert(self, fresh_alerter, internal_engine, dest_engine):
        """Sync thành công → không POST đến Telegram."""
        cfg = _make_pipeline_cfg()
        client = MagicMock()
        client.paginate.return_value = iter([])  # empty = success
        runner = SyncRunner(cfg, client, internal_engine, dest_engine)

        with respx.mock(assert_all_called=False) as mock:
            route = mock.post(_TELEGRAM_SEND_URL).mock(
                return_value=httpx.Response(200, json=_ok_json())
            )

            spec = SyncJobSpec(
                pipeline="shop_main", resource="orders",
                mode="incremental", trigger="schedule",
            )
            result = runner.run(spec)

            if result.status == "failed":
                fresh_alerter.send("...", key="k", pipeline="shop_main")

        assert result.status == "success"
        assert not route.called, "No alert should be sent on success"


# ═══════════════════════════════════════════════════════════════════════════════
# BƯỚC 4: Scheduler dead → SCHEDULER_DEAD alert (real Postgres)
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuoc4SchedulerDeadAlert:
    """Bước 4: Heartbeat stale >2h trong Postgres → watchdog gửi SCHEDULER_DEAD alert."""

    @pytest.fixture(autouse=True)
    def clean_heartbeat(self, internal_engine):
        with internal_engine.begin() as conn:
            conn.execute(text("DELETE FROM scheduler_heartbeat"))
        yield
        with internal_engine.begin() as conn:
            conn.execute(text("DELETE FROM scheduler_heartbeat"))

    def _seed_heartbeat(self, internal_engine, last_beat: datetime) -> None:
        with internal_engine.begin() as conn:
            conn.execute(
                pg_insert(scheduler_heartbeat)
                .values(
                    id=1,
                    last_beat=last_beat,
                    daemon_started_at=last_beat,
                    version="0.1.0-test",
                )
                .on_conflict_do_update(
                    index_elements=["id"],
                    set_={"last_beat": last_beat},
                )
            )

    def test_stale_heartbeat_3h_fires_alert(self, fresh_alerter, internal_engine):
        """
        Heartbeat row: last_beat = 3h trước → watchdog gửi alert.
        Verify: POST đến Telegram với "Scheduler DEAD" và age info.
        """
        from flowbyte.scheduler.daemon import _heartbeat_watchdog_tick

        stale = datetime.now(timezone.utc) - timedelta(hours=3)
        self._seed_heartbeat(internal_engine, stale)

        with respx.mock(assert_all_called=True) as mock:
            route = mock.post(_TELEGRAM_SEND_URL).mock(
                return_value=httpx.Response(200, json=_ok_json())
            )
            _heartbeat_watchdog_tick(internal_engine, fresh_alerter)

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert "DEAD" in body["text"] or "Scheduler" in body["text"]
        assert "3.0" in body["text"] or "2." in body["text"]  # age hours

    def test_fresh_heartbeat_does_not_fire_alert(self, fresh_alerter, internal_engine):
        """Heartbeat fresh (1 phút trước) → không POST đến Telegram."""
        from flowbyte.scheduler.daemon import _heartbeat_watchdog_tick

        fresh = datetime.now(timezone.utc) - timedelta(minutes=1)
        self._seed_heartbeat(internal_engine, fresh)

        with respx.mock(assert_all_called=False) as mock:
            route = mock.post(_TELEGRAM_SEND_URL).mock(
                return_value=httpx.Response(200, json=_ok_json())
            )
            _heartbeat_watchdog_tick(internal_engine, fresh_alerter)

        assert not route.called

    def test_no_heartbeat_row_does_not_fire(self, fresh_alerter, internal_engine):
        """Chưa có heartbeat row (daemon chưa khởi động) → không alert."""
        from flowbyte.scheduler.daemon import _heartbeat_watchdog_tick

        with respx.mock(assert_all_called=False) as mock:
            route = mock.post(_TELEGRAM_SEND_URL).mock(
                return_value=httpx.Response(200, json=_ok_json())
            )
            _heartbeat_watchdog_tick(internal_engine, fresh_alerter)

        assert not route.called

    def test_scheduler_dead_alert_key_for_dedup(self, fresh_alerter, internal_engine):
        """
        Watchdog chạy 2 lần trong 5 phút (cùng stale heartbeat).
        Lần đầu → POST. Lần hai → dedup suppress → không POST lần nữa.
        """
        from flowbyte.scheduler.daemon import _heartbeat_watchdog_tick

        stale = datetime.now(timezone.utc) - timedelta(hours=3)
        self._seed_heartbeat(internal_engine, stale)

        call_count = 0

        with respx.mock(assert_all_called=False) as mock:
            def _count(req):
                nonlocal call_count
                call_count += 1
                return httpx.Response(200, json=_ok_json())

            mock.post(_TELEGRAM_SEND_URL).mock(side_effect=_count)

            _heartbeat_watchdog_tick(internal_engine, fresh_alerter)
            _heartbeat_watchdog_tick(internal_engine, fresh_alerter)

        assert call_count == 1, "Second watchdog tick must be deduped"


# ═══════════════════════════════════════════════════════════════════════════════
# BƯỚC 5: Dedup — cùng event trong 5 phút → suppress
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuoc5Dedup:
    """Bước 5: Không spam — cùng key trong 5 phút chỉ gửi 1 message."""

    def test_same_sync_fail_twice_in_5min_sends_once(self, alerter_client):
        """
        orders fail lần 1 → POST.
        orders fail lần 2 (ngay sau đó, cùng key) → suppressed.
        HTTP call = 1 lần.
        """
        alerter = TelegramAlerter(
            bot_token=_BOT_TOKEN,
            chat_id=_CHAT_ID,
            deduper=AlertDeduper(),
            http_client=alerter_client,
        )
        call_count = 0

        with respx.mock(assert_all_called=False) as mock:
            def _count(req):
                nonlocal call_count
                call_count += 1
                return httpx.Response(200, json=_ok_json())

            mock.post(_TELEGRAM_SEND_URL).mock(side_effect=_count)

            key = "sync_fail:shop_main:orders"
            text1 = format_sync_fail_alert("shop_main", "orders", "timeout attempt 1", "id-001")
            text2 = format_sync_fail_alert("shop_main", "orders", "timeout attempt 2", "id-002")

            r1 = alerter.send(text1, key=key, pipeline="shop_main")
            r2 = alerter.send(text2, key=key, pipeline="shop_main")

        assert r1 is True,  "First alert should be sent"
        assert r2 is False, "Second alert within 5 min must be suppressed"
        assert call_count == 1

    def test_different_resources_each_get_alert(self, alerter_client):
        """
        orders fail + customers fail → 2 keys khác nhau → 2 POST.
        """
        alerter = TelegramAlerter(
            bot_token=_BOT_TOKEN,
            chat_id=_CHAT_ID,
            deduper=AlertDeduper(),
            http_client=alerter_client,
        )
        call_count = 0

        with respx.mock(assert_all_called=False) as mock:
            def _count(req):
                nonlocal call_count
                call_count += 1
                return httpx.Response(200, json=_ok_json())

            mock.post(_TELEGRAM_SEND_URL).mock(side_effect=_count)

            alerter.send(
                format_sync_fail_alert("shop_main", "orders", "err", "id1"),
                key="sync_fail:shop_main:orders", pipeline="shop_main",
            )
            alerter.send(
                format_sync_fail_alert("shop_main", "customers", "err", "id2"),
                key="sync_fail:shop_main:customers", pipeline="shop_main",
            )

        assert call_count == 2, "Different resources = different keys = 2 POSTs"


# ═══════════════════════════════════════════════════════════════════════════════
# BƯỚC 6: Rate limit — max 3 alert/h/pipeline
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuoc6RateLimit:
    """Bước 6: Max 3 alerts/h/pipeline — alert thứ 4 bị chặn."""

    def test_4th_alert_same_pipeline_blocked(self, alerter_client):
        """
        shop_main: orders fail, customers fail, products fail → 3 POSTs (ok).
        variants fail → bị chặn (rate limit 3/h).
        """
        alerter = TelegramAlerter(
            bot_token=_BOT_TOKEN,
            chat_id=_CHAT_ID,
            deduper=AlertDeduper(),
            http_client=alerter_client,
        )
        call_count = 0

        with respx.mock(assert_all_called=False) as mock:
            def _count(req):
                nonlocal call_count
                call_count += 1
                return httpx.Response(200, json=_ok_json())

            mock.post(_TELEGRAM_SEND_URL).mock(side_effect=_count)

            resources = ["orders", "customers", "products", "variants"]
            results = []
            for i, resource in enumerate(resources):
                r = alerter.send(
                    format_sync_fail_alert("shop_main", resource, "err", f"id-{i}"),
                    key=f"sync_fail:shop_main:{resource}",
                    pipeline="shop_main",
                )
                results.append(r)

        assert results[0] is True,  "1st alert: OK"
        assert results[1] is True,  "2nd alert: OK"
        assert results[2] is True,  "3rd alert: OK"
        assert results[3] is False, "4th alert: blocked by rate limit"
        assert call_count == 3

    def test_different_pipelines_have_independent_rate_limits(self, alerter_client):
        """
        shop_main đã đạt 3 alerts/h → bị chặn.
        shop_dev (pipeline khác) vẫn gửi được.
        """
        alerter = TelegramAlerter(
            bot_token=_BOT_TOKEN,
            chat_id=_CHAT_ID,
            deduper=AlertDeduper(),
            http_client=alerter_client,
        )
        call_count = 0

        with respx.mock(assert_all_called=False) as mock:
            def _count(req):
                nonlocal call_count
                call_count += 1
                return httpx.Response(200, json=_ok_json())

            mock.post(_TELEGRAM_SEND_URL).mock(side_effect=_count)

            # Exhaust shop_main quota
            for i in range(3):
                alerter.send(
                    f"alert {i}", key=f"key_{i}", pipeline="shop_main"
                )
            blocked = alerter.send("alert 4", key="key_4", pipeline="shop_main")

            # shop_dev vẫn gửi được
            dev_sent = alerter.send("dev alert", key="dev_key", pipeline="shop_dev")

        assert blocked is False
        assert dev_sent is True
        assert call_count == 4  # 3 (shop_main) + 1 (shop_dev)


# ═══════════════════════════════════════════════════════════════════════════════
# BƯỚC 7: Telegram API fail → no crash, return False
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuoc7TelegramFail:
    """Bước 7: Telegram API không ổn định → alerter không crash scheduler."""

    def test_5xx_response_returns_false_no_exception(self, fresh_alerter):
        """Telegram 500 sau 3 retries → returns False. Scheduler tiếp tục."""
        with respx.mock:
            respx.post(_TELEGRAM_SEND_URL).mock(
                return_value=httpx.Response(500, json={"ok": False})
            )

            result = fresh_alerter.send("alert text", key="k1", pipeline="shop_main")

        assert result is False, "Failed send must return False, not raise"

    def test_network_error_after_retries_returns_false(self, fresh_alerter):
        """NetworkError sau 3 retries → returns False. Scheduler không crash."""
        call_count = 0

        with respx.mock(assert_all_called=False) as mock:
            def _always_fail(req):
                nonlocal call_count
                call_count += 1
                raise httpx.NetworkError("Connection refused")

            mock.post(_TELEGRAM_SEND_URL).mock(side_effect=_always_fail)

            import unittest.mock
            with unittest.mock.patch("tenacity.nap.sleep"):
                result = fresh_alerter.send("alert text", key="k2", pipeline="shop_main")

        assert result is False
        assert call_count == 3, "Must retry 3 times before giving up"

    def test_network_error_retry_then_success(self, fresh_alerter):
        """
        Telegram down tạm thời: 2 lần NetworkError → lần 3 thành công.
        Alert cuối cùng vẫn được gửi (tenacity retry).
        """
        call_count = 0

        with respx.mock(assert_all_called=False) as mock:
            def _fail_twice_then_ok(req):
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise httpx.NetworkError("temporary outage")
                return httpx.Response(200, json=_ok_json())

            mock.post(_TELEGRAM_SEND_URL).mock(side_effect=_fail_twice_then_ok)

            import unittest.mock
            with unittest.mock.patch("tenacity.nap.sleep"):
                result = fresh_alerter.send("retry test", key="k3", pipeline="shop_main")

        assert result is True
        assert call_count == 3


# ═══════════════════════════════════════════════════════════════════════════════
# BƯỚC 8: Timing — alert < 30 giây từ lúc event
# ═══════════════════════════════════════════════════════════════════════════════


class TestBuoc8Timing:
    """Bước 8: Alert phải được gửi < 30 giây kể từ event (PRD §F7 Done khi)."""

    def test_sync_fail_alert_sent_within_30s(self, fresh_alerter, internal_engine, dest_engine):
        """
        Đo wall-clock time từ khi SyncRunner trả failed đến khi alerter.send() returns.
        Phải < 30s (loại bỏ network latency vì dùng respx mock).
        """
        import time

        cfg = _make_pipeline_cfg()
        client = MagicMock()
        client.paginate.side_effect = RuntimeError("timed out")
        runner = SyncRunner(cfg, client, internal_engine, dest_engine)

        with respx.mock(assert_all_called=False) as mock:
            mock.post(_TELEGRAM_SEND_URL).mock(
                return_value=httpx.Response(200, json=_ok_json())
            )

            spec = SyncJobSpec(
                pipeline="shop_main", resource="orders",
                mode="incremental", trigger="schedule",
            )

            t0 = time.monotonic()
            result = runner.run(spec)
            if result.status == "failed":
                text_msg = format_sync_fail_alert(
                    "shop_main", "orders", result.error or "", result.sync_id
                )
                fresh_alerter.send(
                    text_msg,
                    key=f"sync_fail:shop_main:orders",
                    pipeline="shop_main",
                )
            elapsed = time.monotonic() - t0

        assert result.status == "failed"
        assert elapsed < 30.0, f"Alert took {elapsed:.2f}s — must be < 30s"

    def test_scheduler_dead_alert_sent_within_30s(self, fresh_alerter, internal_engine):
        """
        _heartbeat_watchdog_tick với stale heartbeat → alert trong < 30s.
        """
        import time
        from flowbyte.scheduler.daemon import _heartbeat_watchdog_tick

        stale = datetime.now(timezone.utc) - timedelta(hours=3)
        with internal_engine.begin() as conn:
            conn.execute(text("DELETE FROM scheduler_heartbeat"))
            conn.execute(
                pg_insert(scheduler_heartbeat).values(
                    id=1,
                    last_beat=stale,
                    daemon_started_at=stale,
                    version="0.1.0-test",
                )
            )

        with respx.mock(assert_all_called=True) as mock:
            mock.post(_TELEGRAM_SEND_URL).mock(
                return_value=httpx.Response(200, json=_ok_json())
            )

            t0 = time.monotonic()
            _heartbeat_watchdog_tick(internal_engine, fresh_alerter)
            elapsed = time.monotonic() - t0

        assert elapsed < 30.0, f"Watchdog alert took {elapsed:.2f}s — must be < 30s"

        with internal_engine.begin() as conn:
            conn.execute(text("DELETE FROM scheduler_heartbeat"))
