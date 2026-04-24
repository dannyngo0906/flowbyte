"""Unit tests for AlertDeduper — time + rate-limit logic, no IO."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from flowbyte.alerting import deduper as deduper_module
from flowbyte.alerting.deduper import AlertDeduper

WINDOW_SECONDS = deduper_module._WINDOW_SECONDS
MAX_PER_HOUR = deduper_module._MAX_PER_HOUR


@pytest.fixture
def deduper():
    return AlertDeduper()


class TestDedupWindow:
    def test_first_send_allowed(self, deduper):
        assert deduper.should_send("key_a", "pipeline_x") is True

    def test_second_send_within_window_blocked(self, deduper):
        deduper.should_send("key_a", "pipeline_x")
        assert deduper.should_send("key_a", "pipeline_x") is False

    def test_different_key_allowed_in_window(self, deduper):
        deduper.should_send("key_a", "pipeline_x")
        assert deduper.should_send("key_b", "pipeline_x") is True

    def test_send_allowed_after_window_expires(self):
        d = AlertDeduper()
        t0 = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 4, 24, 10, 6, 0, tzinfo=timezone.utc)  # 6 min later

        with patch.object(deduper_module, "datetime") as mock_dt:
            mock_dt.now.return_value = t0
            d.should_send("key_a", "pipeline_x")

            mock_dt.now.return_value = t1
            result = d.should_send("key_a", "pipeline_x")
        assert result is True

    def test_send_blocked_just_before_window_expires(self):
        d = AlertDeduper()
        t0 = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 4, 24, 10, 4, 59, tzinfo=timezone.utc)  # 4m59s — still in window

        with patch.object(deduper_module, "datetime") as mock_dt:
            mock_dt.now.return_value = t0
            d.should_send("key_a", "pipeline_x")

            mock_dt.now.return_value = t1
            result = d.should_send("key_a", "pipeline_x")
        assert result is False


class TestHourlyRateLimit:
    def test_up_to_max_alerts_allowed(self, deduper):
        results = []
        for i in range(MAX_PER_HOUR):
            results.append(deduper.should_send(f"key_{i}", "pipeline_x"))
        assert all(results)

    def test_exceeding_max_blocked(self, deduper):
        for i in range(MAX_PER_HOUR):
            deduper.should_send(f"key_{i}", "pipeline_x")
        result = deduper.should_send("key_overflow", "pipeline_x")
        assert result is False

    def test_different_pipelines_have_separate_limits(self, deduper):
        for i in range(MAX_PER_HOUR):
            deduper.should_send(f"key_{i}", "pipeline_a")
        assert deduper.should_send("key_new", "pipeline_b") is True

    def test_rate_limit_resets_after_hour(self):
        d = AlertDeduper()
        t0 = datetime(2026, 4, 24, 10, 0, 0, tzinfo=timezone.utc)
        t_over = datetime(2026, 4, 24, 11, 1, 0, tzinfo=timezone.utc)

        with patch.object(deduper_module, "datetime") as mock_dt:
            mock_dt.now.return_value = t0
            for i in range(MAX_PER_HOUR):
                d.should_send(f"key_{i}", "pipeline_x")
            assert d.should_send("key_extra", "pipeline_x") is False

            mock_dt.now.return_value = t_over
            result = d.should_send("key_new", "pipeline_x")
        assert result is True

    def test_hourly_stale_entries_pruned(self):
        d = AlertDeduper()
        t0 = datetime(2026, 4, 24, 9, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 4, 24, 10, 0, 1, tzinfo=timezone.utc)  # >1h later

        with patch.object(deduper_module, "datetime") as mock_dt:
            mock_dt.now.return_value = t0
            for i in range(MAX_PER_HOUR):
                d.should_send(f"old_key_{i}", "pipeline_x")

            mock_dt.now.return_value = t1
            result = d.should_send("new_key", "pipeline_x")
        assert result is True


class TestConstants:
    def test_window_is_5_minutes(self):
        assert WINDOW_SECONDS == 300

    def test_max_per_hour_is_3(self):
        assert MAX_PER_HOUR == 3
