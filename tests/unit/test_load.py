"""Unit tests for sync/load.py: upsert_batch, count_rows, count_active_rows, sweep_soft_deletes.

Uses real SQLAlchemy Table definitions with mocked Connection objects.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, Text
from sqlalchemy import TIMESTAMP as SaTimestamp

from flowbyte.sync.load import (
    LoadStats,
    count_active_rows,
    count_rows,
    sweep_soft_deletes,
    upsert_batch,
)

# ── Minimal table fixtures ────────────────────────────────────────────────────

_meta = MetaData()

# Basic table: no _deleted_at (used for upsert + count_rows tests)
_simple_table = Table(
    "t_load_simple",
    _meta,
    Column("id", Integer, primary_key=True),
    Column("name", Text),
    Column("_sync_id", Text),
    Column("_synced_at", SaTimestamp),
)

# Table with _deleted_at: used for count_active_rows + sweep_soft_deletes
_full_table = Table(
    "t_load_full",
    _meta,
    Column("id", Integer, primary_key=True),
    Column("name", Text),
    Column("_deleted_at", SaTimestamp, nullable=True),
    Column("_sync_id", Text),
    Column("_synced_at", SaTimestamp),
)

# Table with _deleted_at but NO _sync_id: sweep should return 0
_no_sync_id_table = Table(
    "t_load_no_sync_id",
    _meta,
    Column("id", Integer, primary_key=True),
    Column("name", Text),
    Column("_deleted_at", SaTimestamp, nullable=True),
    Column("_synced_at", SaTimestamp),
)


# ── upsert_batch ──────────────────────────────────────────────────────────────


class TestUpsertBatch:
    def test_empty_records_returns_zero_stats(self):
        conn = MagicMock()
        stats = upsert_batch(conn, _simple_table, [], "sync-id")
        assert stats.upserted == 0
        assert stats.errors == 0
        conn.execute.assert_not_called()

    def test_returns_upserted_count(self):
        conn = MagicMock()
        records = [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
        stats = upsert_batch(conn, _simple_table, records, "sync-id")
        assert stats.upserted == 2

    def test_injects_sync_id_into_records(self):
        conn = MagicMock()
        records = [{"id": 1, "name": "test"}]
        upsert_batch(conn, _simple_table, records, "sync-xyz")
        assert records[0]["_sync_id"] == "sync-xyz"

    def test_injects_synced_at_into_records(self):
        conn = MagicMock()
        records = [{"id": 1, "name": "test"}]
        upsert_batch(conn, _simple_table, records, "sync-xyz")
        assert "_synced_at" in records[0]
        assert records[0]["_synced_at"] is not None

    def test_execute_called_once_for_small_batch(self):
        conn = MagicMock()
        records = [{"id": i, "name": f"r{i}"} for i in range(10)]
        upsert_batch(conn, _simple_table, records, "s")
        conn.execute.assert_called_once()

    def test_execute_error_increments_errors(self):
        conn = MagicMock()
        conn.execute.side_effect = Exception("DB error")
        records = [{"id": 1, "name": "test"}]
        stats = upsert_batch(conn, _simple_table, records, "sync-id")
        assert stats.errors == 1
        assert stats.upserted == 0


# ── count_rows ────────────────────────────────────────────────────────────────


class TestCountRows:
    def test_returns_scalar_value(self):
        conn = MagicMock()
        conn.execute.return_value.scalar.return_value = 42
        result = count_rows(conn, _simple_table)
        assert result == 42

    def test_returns_zero_when_scalar_none(self):
        conn = MagicMock()
        conn.execute.return_value.scalar.return_value = None
        result = count_rows(conn, _simple_table)
        assert result == 0


# ── count_active_rows ────────────────────────────────────────────────────────


class TestCountActiveRows:
    def test_table_without_deleted_at_falls_back_to_count_rows(self):
        conn = MagicMock()
        conn.execute.return_value.scalar.return_value = 10
        result = count_active_rows(conn, _simple_table)
        assert result == 10
        conn.execute.assert_called_once()

    def test_table_with_deleted_at_filters_non_null(self):
        conn = MagicMock()
        conn.execute.return_value.scalar.return_value = 7
        result = count_active_rows(conn, _full_table)
        assert result == 7
        conn.execute.assert_called_once()

    def test_returns_zero_when_scalar_none(self):
        conn = MagicMock()
        conn.execute.return_value.scalar.return_value = None
        result = count_active_rows(conn, _full_table)
        assert result == 0


# ── sweep_soft_deletes ───────────────────────────────────────────────────────


class TestSweepSoftDeletes:
    def test_table_without_deleted_at_returns_zero(self):
        conn = MagicMock()
        result = sweep_soft_deletes(conn, _simple_table, "sync-id")
        assert result == 0
        conn.execute.assert_not_called()

    def test_table_without_sync_id_returns_zero(self):
        conn = MagicMock()
        result = sweep_soft_deletes(conn, _no_sync_id_table, "sync-id")
        assert result == 0
        conn.execute.assert_not_called()

    def test_table_with_both_columns_executes_update(self):
        conn = MagicMock()
        conn.execute.return_value.rowcount = 3
        result = sweep_soft_deletes(conn, _full_table, "sync-id")
        assert result == 3
        conn.execute.assert_called_once()

    def test_returns_rowcount_from_execute(self):
        conn = MagicMock()
        conn.execute.return_value.rowcount = 12
        result = sweep_soft_deletes(conn, _full_table, "sync-id-abc")
        assert result == 12
