"""Unit tests for Raw EL pass-through transform."""
from __future__ import annotations

from datetime import UTC, datetime

from flowbyte.sync.transform import apply_transform


class TestApplyTransform:
    def test_preserves_full_raw(self):
        record = {"id": 1, "updated_at": "2026-01-01T00:00:00+00:00", "line_items": [{"id": 10}]}
        row = apply_transform(record, "orders")
        assert row["_raw"] is record

    def test_extracts_id_and_updated_at(self):
        record = {"id": 42, "updated_at": "2026-01-15T08:00:00+00:00", "email": "a@b.com"}
        row = apply_transform(record, "orders")
        assert row["id"] == 42
        assert row["updated_at"] == "2026-01-15T08:00:00+00:00"

    def test_synced_at_is_recent_utc(self):
        before = datetime.now(UTC)
        row = apply_transform({"id": 1, "updated_at": "2026-01-01"}, "customers")
        after = datetime.now(UTC)
        assert before <= row["_synced_at"] <= after

    def test_returns_none_when_no_id(self):
        record = {"name": "ghost", "updated_at": "2026-01-01"}
        assert apply_transform(record, "orders") is None

    def test_returns_none_when_id_is_null(self):
        assert apply_transform({"id": None, "updated_at": "2026-01-01"}, "customers") is None

    def test_inventory_levels_composite_key(self):
        record = {
            "inventory_item_id": 100,
            "location_id": 200,
            "available": 5,
            "updated_at": "2026-01-01",
        }
        row = apply_transform(record, "inventory_levels")
        assert row is not None
        assert row["inventory_item_id"] == 100
        assert row["location_id"] == 200
        assert row["_raw"] is record

    def test_inventory_levels_no_id_field_still_returns_row(self):
        record = {"inventory_item_id": 1, "location_id": 2}
        row = apply_transform(record, "inventory_levels")
        assert row is not None

    def test_no_extra_flatten_columns(self):
        record = {"id": 5, "updated_at": "2026-01-01", "financial_status": "paid"}
        row = apply_transform(record, "orders")
        assert "financial_status" not in row
        assert "email" not in row
