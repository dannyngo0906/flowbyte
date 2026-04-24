"""Unit tests for Haravan retry logic predicates."""
from __future__ import annotations

import pytest

from flowbyte.haravan.exceptions import (
    HaravanAuthError,
    HaravanClientError,
    HaravanNetworkError,
    HaravanRateLimited,
    HaravanServerError,
)
from flowbyte.haravan.client import should_retry


class TestShouldRetry:
    def test_rate_limited_retryable(self):
        assert should_retry(HaravanRateLimited(retry_after=5.0)) is True

    def test_server_error_retryable(self):
        assert should_retry(HaravanServerError(500, "Internal Server Error")) is True

    def test_network_error_retryable(self):
        import httpx
        assert should_retry(HaravanNetworkError(httpx.ConnectError("refused"))) is True

    def test_auth_error_not_retryable(self):
        assert should_retry(HaravanAuthError("401 Unauthorized")) is False

    def test_client_error_not_retryable(self):
        assert should_retry(HaravanClientError(404, "Not Found")) is False

    def test_generic_exception_not_retryable(self):
        assert should_retry(ValueError("unexpected")) is False

    def test_keyboard_interrupt_not_retryable(self):
        assert should_retry(KeyboardInterrupt()) is False


class TestExceptionAttributes:
    def test_rate_limited_has_retry_after(self):
        exc = HaravanRateLimited(retry_after=10.0)
        assert exc.retry_after == 10.0

    def test_server_error_has_status_code(self):
        exc = HaravanServerError(503, "Service Unavailable")
        assert exc.status_code == 503

    def test_client_error_has_status_code(self):
        exc = HaravanClientError(422, "Unprocessable Entity")
        assert exc.status_code == 422

    def test_auth_error_str(self):
        exc = HaravanAuthError("Unauthorized")
        assert "Unauthorized" in str(exc)
