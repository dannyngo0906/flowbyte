"""Unit tests for TelegramAlerter — send, retry, format functions, dedup integration."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from flowbyte.alerting.telegram import (
    TelegramAlerter,
    format_scheduler_dead_alert,
    format_sync_fail_alert,
)


def _ok_response() -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.raise_for_status.return_value = None
    return resp


def _error_response(status_code: int = 500) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "error", request=MagicMock(), response=resp
    )
    return resp


@pytest.fixture
def http_client() -> MagicMock:
    client = MagicMock(spec=httpx.Client)
    client.post.return_value = _ok_response()
    return client


_VALID_TOKEN = "123456789:ABCDefghijklmnopqrst"


@pytest.fixture
def alerter(http_client: MagicMock) -> TelegramAlerter:
    return TelegramAlerter(
        bot_token=_VALID_TOKEN,
        chat_id="-100123456",
        http_client=http_client,
    )


class TestSend:
    def test_returns_true_on_success(self, alerter, http_client):
        assert alerter.send("hello") is True
        http_client.post.assert_called_once()

    def test_deduped_returns_false(self, alerter, http_client):
        assert alerter.send("hello", key="k1", pipeline="p") is True
        assert alerter.send("hello", key="k1", pipeline="p") is False
        assert http_client.post.call_count == 1

    def test_no_key_always_sends(self, alerter, http_client):
        for _ in range(3):
            assert alerter.send("hello") is True
        assert http_client.post.call_count == 3

    def test_http_error_returns_false(self, http_client):
        http_client.post.return_value = _error_response(500)
        a = TelegramAlerter(_VALID_TOKEN, "cid", http_client=http_client)
        assert a.send("hello") is False

    def test_generic_error_returns_false(self, http_client):
        http_client.post.side_effect = RuntimeError("unexpected")
        a = TelegramAlerter(_VALID_TOKEN, "cid", http_client=http_client)
        assert a.send("hello") is False


class TestRetry:
    def test_retries_on_network_error(self, http_client):
        err = httpx.NetworkError("timeout")
        http_client.post.side_effect = [err, err, _ok_response()]
        a = TelegramAlerter(_VALID_TOKEN, "cid", http_client=http_client)
        with patch("tenacity.nap.sleep"):
            result = a.send("hello")
        assert result is True
        assert http_client.post.call_count == 3

    def test_exhausted_retries_returns_false(self, http_client):
        http_client.post.side_effect = httpx.NetworkError("persistent")
        a = TelegramAlerter(_VALID_TOKEN, "cid", http_client=http_client)
        with patch("tenacity.nap.sleep"):
            result = a.send("hello")
        assert result is False
        assert http_client.post.call_count == 3


class TestTestMethod:
    def test_calls_post_and_returns_true(self, alerter, http_client):
        assert alerter.test() is True
        http_client.post.assert_called_once()
        text_sent = http_client.post.call_args.kwargs["json"]["text"]
        assert "Flowbyte alert test" in text_sent

    def test_raises_on_network_error(self, http_client):
        http_client.post.side_effect = httpx.NetworkError("down")
        a = TelegramAlerter(_VALID_TOKEN, "cid", http_client=http_client)
        with patch("tenacity.nap.sleep"):
            with pytest.raises(httpx.NetworkError):
                a.test()


class TestFormatSyncFail:
    def test_contains_key_fields(self):
        msg = format_sync_fail_alert("shop_main", "orders", "Timeout error", "abc12345-xxxx")
        assert "shop_main" in msg
        assert "orders" in msg
        assert "Timeout error" in msg
        assert "abc12345" in msg

    def test_truncates_long_error(self):
        long_error = "x" * 400
        msg = format_sync_fail_alert("p", "r", long_error, "id")
        assert "x" * 300 in msg
        assert "x" * 301 not in msg

    def test_escapes_markdown(self):
        msg = format_sync_fail_alert("p", "r", "err `backtick` *bold* _italic_", "id")
        assert "`backtick`" not in msg
        assert "*bold*" not in msg
        assert "_italic_" not in msg


class TestFormatSchedulerDead:
    def test_contains_last_beat_and_age(self):
        msg = format_scheduler_dead_alert("2026-04-24T10:00:00+00:00", 2.5)
        assert "2026-04-24T10:00:00+00:00" in msg
        assert "2.5" in msg


class TestDedupIntegration:
    def test_dedup_within_5min_blocks_second(self, http_client):
        a = TelegramAlerter(_VALID_TOKEN, "cid", http_client=http_client)
        assert a.send("text", key="k", pipeline="p") is True
        assert a.send("text", key="k", pipeline="p") is False
        assert http_client.post.call_count == 1


class TestTokenValidation:
    def test_invalid_token_no_colon_raises(self, http_client):
        with pytest.raises(ValueError, match="bot_token"):
            TelegramAlerter("NOCOLON", "cid", http_client=http_client)

    def test_invalid_token_non_numeric_id_raises(self, http_client):
        with pytest.raises(ValueError, match="bot_token"):
            TelegramAlerter("botABC:ABCDefghijklmnop", "cid", http_client=http_client)

    def test_invalid_token_hash_too_short_raises(self, http_client):
        with pytest.raises(ValueError, match="bot_token"):
            TelegramAlerter("123456789:short", "cid", http_client=http_client)

    def test_invalid_token_path_traversal_raises(self, http_client):
        """/../ in token would silently redirect to a different Telegram endpoint."""
        with pytest.raises(ValueError, match="bot_token"):
            TelegramAlerter("123:TOKEN/../other", "cid", http_client=http_client)

    def test_valid_token_accepted(self, http_client):
        a = TelegramAlerter(_VALID_TOKEN, "cid", http_client=http_client)
        assert a is not None


class TestTokenMasking:
    def test_token_not_in_log_on_http_error(self, http_client, caplog):
        """bot_token must NOT appear in structured log when HTTP error occurs."""
        import logging
        http_client.post.return_value = _error_response(401)
        a = TelegramAlerter(_VALID_TOKEN, "cid", http_client=http_client)

        with caplog.at_level(logging.ERROR):
            a.send("hello")

        raw_token = _VALID_TOKEN.split(":")[1]
        for record in caplog.records:
            assert raw_token not in record.getMessage(), \
                "bot_token hash must not appear in log records"

    def test_text_truncated_at_4096_chars(self, alerter, http_client):
        """Messages longer than Telegram's 4096-char limit must be truncated."""
        long_text = "x" * 5000
        alerter.send(long_text)

        sent_body = http_client.post.call_args.kwargs["json"]
        assert len(sent_body["text"]) == 4096
