"""Unit tests for checkpoint: compute_watermark, load_checkpoint, save_checkpoint."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from flowbyte.sync.checkpoint import compute_watermark, load_checkpoint, save_checkpoint


def _ts(year: int = 2026, month: int = 1, day: int = 1, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def _mock_conn(one_or_none_result=None) -> MagicMock:
    conn = MagicMock()
    conn.execute.return_value.one_or_none.return_value = one_or_none_result
    return conn


# ── compute_watermark ────────────────────────────────────────────────────────


class TestComputeWatermark:
    def test_empty_batch_returns_none(self):
        assert compute_watermark([]) is None

    def test_single_record_returns_ts_and_id(self):
        batch = [{"id": 42, "updated_at": "2026-01-01T00:00:00+00:00"}]
        result = compute_watermark(batch)
        assert result is not None
        ts, id_ = result
        assert id_ == 42
        assert ts == _ts()

    def test_returns_max_updated_at(self):
        batch = [
            {"id": 100, "updated_at": "2026-01-01T00:00:00+00:00"},
            {"id": 200, "updated_at": "2026-01-02T00:00:00+00:00"},
        ]
        ts, id_ = compute_watermark(batch)
        assert id_ == 200
        assert ts == _ts(day=2)

    def test_same_ts_returns_higher_id(self):
        batch = [
            {"id": 100, "updated_at": "2026-01-01T00:00:00+00:00"},
            {"id": 200, "updated_at": "2026-01-01T00:00:00+00:00"},
        ]
        _, id_ = compute_watermark(batch)
        assert id_ == 200

    def test_z_suffix_timezone_parsed(self):
        batch = [{"id": 1, "updated_at": "2026-01-01T00:00:00Z"}]
        ts, _ = compute_watermark(batch)
        assert ts.tzinfo is not None

    def test_naive_datetime_gets_utc(self):
        batch = [{"id": 1, "updated_at": "2026-01-01T00:00:00"}]
        ts, _ = compute_watermark(batch)
        assert ts.tzinfo is not None
        assert ts.tzinfo == timezone.utc

    def test_skips_record_without_updated_at(self):
        batch = [{"id": 1}, {"id": 2, "updated_at": "2026-01-01T00:00:00+00:00"}]
        _, id_ = compute_watermark(batch)
        assert id_ == 2

    def test_all_records_no_updated_at_returns_none(self):
        batch = [{"id": 1}, {"id": 2}]
        assert compute_watermark(batch) is None

    def test_invalid_timestamp_string_skipped(self):
        batch = [
            {"id": 1, "updated_at": "not-a-date"},
            {"id": 2, "updated_at": "2026-01-01T00:00:00+00:00"},
        ]
        _, id_ = compute_watermark(batch)
        assert id_ == 2

    def test_null_updated_at_skipped(self):
        batch = [
            {"id": 1, "updated_at": None},
            {"id": 2, "updated_at": "2026-01-01T00:00:00+00:00"},
        ]
        _, id_ = compute_watermark(batch)
        assert id_ == 2

    def test_missing_id_defaults_to_zero(self):
        batch = [{"updated_at": "2026-01-01T00:00:00+00:00"}]
        _, id_ = compute_watermark(batch)
        assert id_ == 0


# ── load_checkpoint ──────────────────────────────────────────────────────────


class TestLoadCheckpoint:
    def test_returns_none_when_no_row(self):
        conn = _mock_conn(None)
        result = load_checkpoint(conn, "shop_main", "orders")
        assert result is None

    def test_returns_tuple_when_row_exists(self):
        row = MagicMock()
        row.last_updated_at = _ts(day=15)
        row.last_id = 999
        conn = _mock_conn(row)
        result = load_checkpoint(conn, "shop_main", "orders")
        assert result == (_ts(day=15), 999)

    def test_returns_zero_id_when_last_id_none(self):
        row = MagicMock()
        row.last_updated_at = _ts()
        row.last_id = None
        conn = _mock_conn(row)
        result = load_checkpoint(conn, "shop_main", "orders")
        assert result is not None
        assert result[1] == 0

    def test_returns_none_when_last_updated_at_none(self):
        row = MagicMock()
        row.last_updated_at = None
        row.last_id = 100
        conn = _mock_conn(row)
        result = load_checkpoint(conn, "shop_main", "orders")
        assert result is None


# ── save_checkpoint ──────────────────────────────────────────────────────────


class TestSaveCheckpoint:
    def test_does_nothing_when_watermark_none(self):
        conn = _mock_conn(None)
        save_checkpoint(conn, "shop_main", "orders", None, "sync-id")
        conn.execute.assert_not_called()

    def test_upsert_called_on_valid_watermark_no_prior(self):
        conn = _mock_conn(None)  # no prior checkpoint
        wm = (_ts(day=5), 100)
        save_checkpoint(conn, "shop_main", "orders", wm, "sync-id")
        # 1 SELECT (load_checkpoint) + 1 INSERT ON CONFLICT
        assert conn.execute.call_count == 2

    def test_regression_guard_skips_older_watermark(self):
        row = MagicMock()
        row.last_updated_at = _ts(month=2)  # existing is NEWER
        row.last_id = 200
        conn = _mock_conn(row)

        older_wm = (_ts(month=1), 100)  # new watermark is OLDER → regression guard fires
        save_checkpoint(conn, "shop_main", "orders", older_wm, "sync-id")

        # Only the SELECT (from load_checkpoint) should happen, upsert must NOT be called
        assert conn.execute.call_count == 1

    def test_newer_watermark_overwrites_older_checkpoint(self):
        row = MagicMock()
        row.last_updated_at = _ts(month=1)  # existing is OLDER
        row.last_id = 100
        conn = _mock_conn(row)

        newer_wm = (_ts(month=2), 200)  # new watermark is NEWER → proceed with upsert
        save_checkpoint(conn, "shop_main", "orders", newer_wm, "sync-id")

        # SELECT (load) + INSERT ON CONFLICT (upsert)
        assert conn.execute.call_count == 2

    def test_different_pipeline_resource_pair(self):
        conn = _mock_conn(None)
        wm = (_ts(month=3), 50)
        save_checkpoint(conn, "shop_main", "customers", wm, "sync-xyz")
        assert conn.execute.call_count == 2
