"""Integration tests for US-002: Incremental Sync E2E with real PostgreSQL.

Haravan HTTP is mocked via MagicMock (paginate returns controlled data).
Internal + destination DBs are real PostgreSQL via testcontainers (conftest).
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import select, text

from flowbyte.config.models import PipelineConfig, PostgresDestConfig, SyncJobSpec
from flowbyte.db.destination_schema import get_table
from flowbyte.db.internal_schema import sync_checkpoints
from flowbyte.sync.runner import SyncRunner

pytestmark = pytest.mark.integration

_SHOP = "test_shop"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_order(order_id: int, updated_at: str = "2026-01-01T10:00:00+00:00", **kwargs) -> dict:
    base = {
        "id": order_id,
        "order_number": 1000 + order_id,
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
        "updated_at": updated_at,
        "line_items": [],
    }
    base.update(kwargs)
    return base


def _make_cfg() -> PipelineConfig:
    return PipelineConfig(
        name=_SHOP,
        haravan_credentials_ref="test-creds",
        haravan_shop_domain="test.myharavan.com",
        destination=PostgresDestConfig(
            host="localhost",
            port=5432,
            user="test",
            database="test",
            credentials_ref="test-db-ref",
        ),
    )


def _make_runner(internal_engine, dest_engine, records: list[dict]) -> SyncRunner:
    client = MagicMock()
    client.paginate.return_value = iter(records)
    return SyncRunner(_make_cfg(), client, internal_engine, dest_engine)


def _spec(mode: str = "incremental", resource: str = "orders") -> SyncJobSpec:
    return SyncJobSpec(pipeline=_SHOP, resource=resource, mode=mode, trigger="test")


@pytest.fixture(autouse=True)
def clean_tables(internal_engine, dest_engine):
    with internal_engine.begin() as conn:
        conn.execute(text("TRUNCATE sync_checkpoints, sync_runs CASCADE"))
    with dest_engine.begin() as conn:
        conn.execute(text("TRUNCATE orders CASCADE"))
    yield


# ── First sync ────────────────────────────────────────────────────────────────


class TestFirstSync:
    def test_first_sync_inserts_all_records(self, internal_engine, dest_engine):
        records = [_make_order(i) for i in range(1, 6)]
        result = _make_runner(internal_engine, dest_engine, records).run(_spec("incremental"))

        assert result.status == "success"
        assert result.fetched_count == 5
        with dest_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        assert count == 5

    def test_checkpoint_saved_after_first_sync(self, internal_engine, dest_engine):
        records = [_make_order(1, "2026-01-15T10:00:00+00:00")]
        _make_runner(internal_engine, dest_engine, records).run(_spec("full_refresh"))

        with internal_engine.connect() as conn:
            row = conn.execute(
                select(sync_checkpoints).where(
                    (sync_checkpoints.c.pipeline == _SHOP)
                    & (sync_checkpoints.c.resource == "orders")
                )
            ).one_or_none()
        assert row is not None
        assert row.last_id == 1

    def test_first_incremental_auto_full_refresh(self, internal_engine, dest_engine):
        """mode=incremental + no checkpoint → effective full_refresh → soft delete sweep runs."""
        # Seed 21 orders so the 5% sanity guard won't block deletion of 1 order
        seed = [_make_order(i) for i in range(1, 22)]
        _make_runner(internal_engine, dest_engine, seed).run(_spec("full_refresh"))

        # Clear checkpoint to simulate a truly first incremental run
        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE sync_checkpoints"))

        # Return only 20 orders → order 21 should be soft-deleted (1/21 ≈ 4.8% < 5%)
        run2 = [_make_order(i) for i in range(1, 21)]
        result = _make_runner(internal_engine, dest_engine, run2).run(_spec("incremental"))

        assert result.status == "success"
        with dest_engine.connect() as conn:
            deleted = conn.execute(
                text("SELECT COUNT(*) FROM orders WHERE _deleted_at IS NOT NULL")
            ).scalar()
        assert deleted == 1


# ── Incremental sync ──────────────────────────────────────────────────────────


class TestIncrementalSync:
    def test_second_sync_accumulates_records(self, internal_engine, dest_engine):
        """Incremental run appends new records without removing existing ones."""
        run1 = [_make_order(i, f"2026-01-{i:02d}T10:00:00+00:00") for i in range(1, 6)]
        _make_runner(internal_engine, dest_engine, run1).run(_spec("full_refresh"))

        run2 = [_make_order(6, "2026-01-10T10:00:00+00:00")]
        result = _make_runner(internal_engine, dest_engine, run2).run(_spec("incremental"))

        assert result.status == "success"
        assert result.fetched_count == 1
        with dest_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        assert count == 6

    def test_no_duplicates_after_repeated_syncs(self, internal_engine, dest_engine):
        """Running full_refresh twice on the same data must not duplicate rows."""
        records = [_make_order(i) for i in range(1, 4)]
        for _ in range(2):
            _make_runner(internal_engine, dest_engine, records).run(_spec("full_refresh"))

        with dest_engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
            distinct = conn.execute(text("SELECT COUNT(DISTINCT id) FROM orders")).scalar()
        assert total == distinct == 3

    def test_updated_record_is_upserted(self, internal_engine, dest_engine):
        """Changed field in second sync must be reflected in destination DB."""
        _make_runner(internal_engine, dest_engine, [_make_order(1)]).run(_spec("full_refresh"))

        updated = [_make_order(1, financial_status="refunded")]
        _make_runner(internal_engine, dest_engine, updated).run(_spec("full_refresh"))

        with dest_engine.connect() as conn:
            row = conn.execute(text("SELECT financial_status FROM orders WHERE id = 1")).one()
        assert row.financial_status == "refunded"


# ── Checkpoint invariant ──────────────────────────────────────────────────────


class TestCheckpointInvariant:
    def test_checkpoint_not_saved_on_api_error(self, internal_engine, dest_engine):
        """API error must not persist a checkpoint."""
        client = MagicMock()
        client.paginate.side_effect = RuntimeError("Haravan 503")
        runner = SyncRunner(_make_cfg(), client, internal_engine, dest_engine)
        result = runner.run(_spec("incremental"))

        assert result.status == "failed"
        with internal_engine.connect() as conn:
            row = conn.execute(
                select(sync_checkpoints).where(
                    (sync_checkpoints.c.pipeline == _SHOP)
                    & (sync_checkpoints.c.resource == "orders")
                )
            ).one_or_none()
        assert row is None

    def test_checkpoint_updated_on_second_sync(self, internal_engine, dest_engine):
        """Checkpoint last_id must advance after each successful sync."""
        run1 = [_make_order(1, "2026-01-01T10:00:00+00:00")]
        _make_runner(internal_engine, dest_engine, run1).run(_spec("full_refresh"))

        run2 = [_make_order(5, "2026-01-05T10:00:00+00:00")]
        _make_runner(internal_engine, dest_engine, run2).run(_spec("incremental"))

        with internal_engine.connect() as conn:
            row = conn.execute(
                select(sync_checkpoints).where(
                    (sync_checkpoints.c.pipeline == _SHOP)
                    & (sync_checkpoints.c.resource == "orders")
                )
            ).one()
        assert row.last_id == 5


# ── Soft delete sweep ─────────────────────────────────────────────────────────


class TestSoftDeleteSweep:
    def test_sweep_marks_missing_records(self, internal_engine, dest_engine):
        """Full refresh with 1 missing record soft-deletes it (≤5% sanity guard passes)."""
        # 21 orders → deleting 1 = 1/21 ≈ 4.8% < 5% guard
        seed = [_make_order(i) for i in range(1, 22)]
        _make_runner(internal_engine, dest_engine, seed).run(_spec("full_refresh"))

        run2 = [_make_order(i) for i in range(1, 21)]  # order 21 absent
        result = _make_runner(internal_engine, dest_engine, run2).run(_spec("full_refresh"))

        assert result.status == "success"
        assert result.soft_deleted_count == 1
        with dest_engine.connect() as conn:
            deleted = conn.execute(
                text("SELECT COUNT(*) FROM orders WHERE _deleted_at IS NOT NULL")
            ).scalar()
        assert deleted == 1

    def test_sanity_guard_aborts_large_deletion(self, internal_engine, dest_engine):
        """Deletion of >5% rows must be blocked by the sanity guard."""
        seed = [_make_order(i) for i in range(1, 6)]  # 5 orders
        _make_runner(internal_engine, dest_engine, seed).run(_spec("full_refresh"))

        # Only 3 orders returned → would delete 2/5 = 40% > 5% → aborted
        run2 = [_make_order(i) for i in range(1, 4)]
        result = _make_runner(internal_engine, dest_engine, run2).run(_spec("full_refresh"))

        assert result.status == "success"
        assert result.soft_deleted_count == 0
        with dest_engine.connect() as conn:
            deleted = conn.execute(
                text("SELECT COUNT(*) FROM orders WHERE _deleted_at IS NOT NULL")
            ).scalar()
        assert deleted == 0
