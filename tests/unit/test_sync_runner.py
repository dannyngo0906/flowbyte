"""Unit tests for SyncRunner: first-sync mode switching, result status, checkpoint invariant.

All external I/O (DB engines, Haravan client, extractors) is mocked.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import DEFAULT, MagicMock, patch

import pytest

from flowbyte.config.models import SyncJobSpec
from flowbyte.sync.load import LoadStats
from flowbyte.sync.runner import SyncRunner

# ── Minimal order record that passes apply_transform without error ────────────

_SAMPLE_ORDER = {
    "id": 1001,
    "order_number": 2001,
    "financial_status": "paid",
    "total_price": "100.00",
    "customer": None,
    "shipping_address": None,
    "transactions": [],
    "tax_lines": [],
    "discount_codes": [],
    "fulfillments": [],
    "note_attributes": [],
    "created_at": "2026-01-01T00:00:00+00:00",
    "updated_at": "2026-04-24T10:00:00+00:00",
    "line_items": [],
}

_SAMPLE_CUSTOMER = {
    "id": 9001,
    "email": "c@example.com",
    "phone": "0900000000",
    "first_name": "A",
    "last_name": "B",
    "total_spent": "500.00",
    "orders_count": 3,
    "tags": "",
    "created_at": "2025-01-01T00:00:00+00:00",
    "updated_at": "2026-01-10T00:00:00+00:00",
}

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_engine() -> tuple[MagicMock, MagicMock]:
    """Return (engine_mock, conn_mock) with context-manager support."""
    conn = MagicMock()
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _make_runner(extra_resources: dict | None = None) -> SyncRunner:
    cfg = MagicMock()
    cfg.name = "shop_main"
    cfg.resources = {
        "orders": MagicMock(spec=["enabled", "sync_mode", "schedule", "weekly_full_refresh"]),
        "customers": MagicMock(spec=["enabled", "sync_mode", "schedule", "weekly_full_refresh"]),
        **(extra_resources or {}),
    }
    haravan = MagicMock()
    internal_engine, _ = _make_engine()
    dest_engine, _ = _make_engine()
    return SyncRunner(cfg, haravan, internal_engine, dest_engine)


def _make_spec(resource: str = "orders", mode: str = "incremental") -> SyncJobSpec:
    return SyncJobSpec(pipeline="shop_main", resource=resource, mode=mode, trigger="test")


# ── Runner patches ─────────────────────────────────────────────────────────────

_RUNNER_MODULE = "flowbyte.sync.runner"
_RUNNER_PATCH_NAMES = [
    "get_table",
    "load_checkpoint",
    "save_checkpoint",
    "compute_watermark",
    "upsert_batch",
    "count_active_rows",
    "sweep_soft_deletes",
]


def _runner_patches(extra: dict | None = None):
    """patch.multiple context: all runner-level symbols as DEFAULT mocks."""
    return patch.multiple(_RUNNER_MODULE, **{n: DEFAULT for n in _RUNNER_PATCH_NAMES})


# ── TestSyncRunnerOrders ──────────────────────────────────────────────────────


class TestSyncRunnerOrders:
    """Tests exercising the incremental + full_refresh paths for orders."""

    def test_result_status_success_on_happy_path(self):
        with _runner_patches() as m, \
             patch("flowbyte.haravan.resources.orders.extract_orders",
                   side_effect=lambda *a, **kw: iter([_SAMPLE_ORDER])):
            m["load_checkpoint"].return_value = None
            m["upsert_batch"].return_value = LoadStats(upserted=1)
            m["count_active_rows"].return_value = 0
            m["sweep_soft_deletes"].return_value = 0
            m["compute_watermark"].return_value = None

            result = _make_runner().run(_make_spec())

        assert result.status == "success"

    def test_first_sync_no_checkpoint_triggers_soft_delete_sweep(self):
        """checkpoint=None + mode=incremental → effective_mode=full_refresh → sweep runs."""
        with _runner_patches() as m, \
             patch("flowbyte.haravan.resources.orders.extract_orders",
                   side_effect=lambda *a, **kw: iter([_SAMPLE_ORDER])):
            m["load_checkpoint"].return_value = None
            m["upsert_batch"].return_value = LoadStats()
            m["count_active_rows"].return_value = 0
            m["sweep_soft_deletes"].return_value = 0
            m["compute_watermark"].return_value = None

            _make_runner().run(_make_spec("orders", "incremental"))

        m["sweep_soft_deletes"].assert_called_once()

    def test_incremental_with_checkpoint_does_not_sweep(self):
        """Existing checkpoint + mode=incremental → stays incremental → no sweep."""
        cp = (datetime(2026, 1, 1, tzinfo=timezone.utc), 100)
        with _runner_patches() as m, \
             patch("flowbyte.haravan.resources.orders.extract_orders",
                   side_effect=lambda *a, **kw: iter([_SAMPLE_ORDER])):
            m["load_checkpoint"].return_value = cp
            m["upsert_batch"].return_value = LoadStats()
            m["count_active_rows"].return_value = 0
            m["sweep_soft_deletes"].return_value = 0
            m["compute_watermark"].return_value = None

            _make_runner().run(_make_spec("orders", "incremental"))

        m["sweep_soft_deletes"].assert_not_called()

    def test_full_refresh_mode_always_sweeps(self):
        """mode=full_refresh triggers sweep regardless of checkpoint."""
        cp = (datetime(2026, 1, 1, tzinfo=timezone.utc), 50)
        with _runner_patches() as m, \
             patch("flowbyte.haravan.resources.orders.extract_orders",
                   side_effect=lambda *a, **kw: iter([_SAMPLE_ORDER])):
            m["load_checkpoint"].return_value = cp
            m["upsert_batch"].return_value = LoadStats()
            m["count_active_rows"].return_value = 0
            m["sweep_soft_deletes"].return_value = 0
            m["compute_watermark"].return_value = None

            _make_runner().run(_make_spec("orders", "full_refresh"))

        m["sweep_soft_deletes"].assert_called_once()

    def test_result_pipeline_and_resource_fields(self):
        with _runner_patches() as m, \
             patch("flowbyte.haravan.resources.orders.extract_orders",
                   side_effect=lambda *a, **kw: iter([])):
            m["load_checkpoint"].return_value = None
            m["upsert_batch"].return_value = LoadStats()
            m["count_active_rows"].return_value = 0
            m["sweep_soft_deletes"].return_value = 0
            m["compute_watermark"].return_value = None

            result = _make_runner().run(_make_spec("orders"))

        assert result.pipeline == "shop_main"
        assert result.resource == "orders"

    def test_fetched_count_reflects_extracted_records(self):
        with _runner_patches() as m, \
             patch("flowbyte.haravan.resources.orders.extract_orders",
                   side_effect=lambda *a, **kw: iter([_SAMPLE_ORDER, _SAMPLE_ORDER])):
            m["load_checkpoint"].return_value = None
            m["upsert_batch"].return_value = LoadStats(upserted=2)
            m["count_active_rows"].return_value = 0
            m["sweep_soft_deletes"].return_value = 0
            m["compute_watermark"].return_value = None

            result = _make_runner().run(_make_spec("orders", "full_refresh"))

        assert result.fetched_count == 2


# ── TestCheckpointInvariant ───────────────────────────────────────────────────


class TestCheckpointInvariant:
    """Checkpoint must be saved after successful upsert; never on exception."""

    def test_checkpoint_saved_after_successful_sync(self):
        with _runner_patches() as m, \
             patch("flowbyte.haravan.resources.orders.extract_orders",
                   side_effect=lambda *a, **kw: iter([_SAMPLE_ORDER])):
            m["load_checkpoint"].return_value = None
            m["upsert_batch"].return_value = LoadStats()
            m["count_active_rows"].return_value = 0
            m["sweep_soft_deletes"].return_value = 0
            m["compute_watermark"].return_value = None

            result = _make_runner().run(_make_spec())

        assert result.status == "success"
        m["save_checkpoint"].assert_called_once()

    def test_checkpoint_not_saved_when_extractor_raises(self):
        with _runner_patches() as m, \
             patch("flowbyte.haravan.resources.orders.extract_orders",
                   side_effect=RuntimeError("Haravan timeout")):
            m["load_checkpoint"].return_value = None

            result = _make_runner().run(_make_spec())

        assert result.status == "failed"
        m["save_checkpoint"].assert_not_called()

    def test_checkpoint_not_saved_when_upsert_raises(self):
        with _runner_patches() as m, \
             patch("flowbyte.haravan.resources.orders.extract_orders",
                   side_effect=lambda *a, **kw: iter([_SAMPLE_ORDER])):
            m["load_checkpoint"].return_value = None
            m["upsert_batch"].side_effect = Exception("Postgres timeout")
            m["count_active_rows"].return_value = 0
            m["compute_watermark"].return_value = None

            result = _make_runner().run(_make_spec())

        assert result.status == "failed"
        m["save_checkpoint"].assert_not_called()


# ── TestSyncRunnerResultStatus ────────────────────────────────────────────────


class TestSyncRunnerResultStatus:
    def test_failed_status_on_extractor_error(self):
        with _runner_patches() as m, \
             patch("flowbyte.haravan.resources.orders.extract_orders",
                   side_effect=RuntimeError("API fail")):
            m["load_checkpoint"].return_value = None

            result = _make_runner().run(_make_spec())

        assert result.status == "failed"
        assert result.error is not None
        assert "API fail" in result.error

    def test_unknown_resource_results_in_failed(self):
        extra = {"unknown_xyz": MagicMock()}
        spec = _make_spec("unknown_xyz", "incremental")
        with _runner_patches() as m:
            m["load_checkpoint"].return_value = None

            result = _make_runner(extra).run(spec)

        assert result.status == "failed"

    def test_customers_resource_succeeds(self):
        with _runner_patches() as m, \
             patch("flowbyte.haravan.resources.customers.extract_customers",
                   side_effect=lambda *a, **kw: iter([_SAMPLE_CUSTOMER])):
            m["load_checkpoint"].return_value = None
            m["upsert_batch"].return_value = LoadStats(upserted=1)
            m["count_active_rows"].return_value = 0
            m["sweep_soft_deletes"].return_value = 0
            m["compute_watermark"].return_value = None

            result = _make_runner().run(_make_spec("customers", "incremental"))

        assert result.status == "success"
        assert result.resource == "customers"


# ── TestSyncRunnerInventoryLevels ─────────────────────────────────────────────

_SAMPLE_INVENTORY = {
    "inventory_item_id": 1001,
    "location_id": 501,
    "available": 10,
    "updated_at": "2026-01-01T00:00:00+00:00",
}


class TestSyncRunnerInventoryLevels:
    def test_skips_gracefully_when_no_variant_ids(self):
        """_get_variant_ids() returns [] → extract not called, status success, fetched=0."""
        with _runner_patches() as m, \
             patch("flowbyte.sync.runner.SyncRunner._get_location_ids", return_value=[501]), \
             patch("flowbyte.sync.runner.SyncRunner._get_variant_ids", return_value=[]):
            m["upsert_batch"].return_value = LoadStats()
            m["count_active_rows"].return_value = 0

            result = _make_runner().run(_make_spec("inventory_levels", "full_refresh"))

        assert result.status == "success"
        assert result.fetched_count == 0
        m["upsert_batch"].assert_not_called()

    def test_inventory_sync_success_with_variants(self):
        """variant_ids non-empty → extract called, upsert called, status success."""
        with _runner_patches() as m, \
             patch("flowbyte.sync.runner.SyncRunner._get_location_ids", return_value=[501]), \
             patch("flowbyte.sync.runner.SyncRunner._get_variant_ids", return_value=[10001]), \
             patch("flowbyte.haravan.resources.inventory.extract_inventory_levels",
                   side_effect=lambda *a, **kw: iter([_SAMPLE_INVENTORY])):
            m["upsert_batch"].return_value = LoadStats(upserted=1)
            m["count_active_rows"].return_value = 0

            result = _make_runner().run(_make_spec("inventory_levels", "full_refresh"))

        assert result.status == "success"
        assert result.fetched_count == 1
