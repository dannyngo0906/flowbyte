"""Integration tests for US-006: Data Validation E2E with real PostgreSQL.

End-user flow under test:
  1. User runs flowbyte sync (mocked Haravan client, real Postgres)
  2. Validation runs automatically after successful sync
  3. Results persisted to validation_results (4 rules per sync)
  4. flowbyte status reports: Healthy / Warning / Failed

Cases:
  Happy path:
    1. Validation runs automatically after successful sync
    2. Results persisted to validation_results table (4 rows per sync)
    3. All rules ok/skipped on clean full_refresh → validation_failed=False
    4. Validation results linked to correct pipeline + resource + sync_id
    5. Validation not run when sync fails (no partial rows inserted)
    6. Incremental with positive fetch → volume_sanity=ok
    7. Validation completes under 5 seconds (SQL-only, no Haravan calls)

  Edge cases:
    8.  fetch_upsert_parity FAILED → data still in DB (no rollback)
    9.  soft_delete_sanity FAILED on full_refresh → sweep aborted, data safe
    10. incremental_volume_sanity WARNING after 3 zero-fetch streaks
    11. weekly_full_drift WARNING when full refresh count drifts >5%
    12. Validation executor crash does NOT propagate to runner
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select, text

from flowbyte.config.models import PipelineConfig, PostgresDestConfig, SyncJobSpec
from flowbyte.db.internal_schema import validation_results
from flowbyte.sync.runner import SyncRunner

pytestmark = pytest.mark.integration

_SHOP = "test_shop_val"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_order(order_id: int, updated_at: str = "2026-01-01T10:00:00+00:00", **kwargs) -> dict:
    base = {
        "id": order_id,
        "order_number": str(1000 + order_id),
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


def _count_validation_rows(internal_engine, sync_id: str | None = None) -> list[dict]:
    with internal_engine.connect() as conn:
        q = select(validation_results)
        if sync_id:
            from sqlalchemy import cast
            from sqlalchemy.dialects.postgresql import UUID
            q = q.where(
                validation_results.c.sync_id == cast(sync_id, UUID)
            )
        rows = conn.execute(q.order_by(validation_results.c.created_at.asc())).all()
    return [dict(r._mapping) for r in rows]


@pytest.fixture(autouse=True)
def clean_tables(internal_engine, dest_engine):
    with internal_engine.begin() as conn:
        conn.execute(text("TRUNCATE sync_checkpoints, sync_runs, validation_results CASCADE"))
    with dest_engine.begin() as conn:
        conn.execute(text("TRUNCATE orders CASCADE"))
    yield
    with internal_engine.begin() as conn:
        conn.execute(text("TRUNCATE sync_checkpoints, sync_runs, validation_results CASCADE"))
    with dest_engine.begin() as conn:
        conn.execute(text("TRUNCATE orders CASCADE"))


# ── 1. Happy path: validation auto-runs after success ─────────────────────────

class TestValidationHappyPath:
    def test_validation_runs_after_successful_sync(self, internal_engine, dest_engine):
        """After a successful sync, validation_results has rows (4 rules)."""
        records = [_make_order(i) for i in range(1, 6)]
        result = _make_runner(internal_engine, dest_engine, records).run(_spec("full_refresh"))

        assert result.status == "success"

        rows = _count_validation_rows(internal_engine)
        assert len(rows) == 4, f"Expected 4 validation rows, got {len(rows)}"

    def test_validation_results_have_correct_rules(self, internal_engine, dest_engine):
        records = [_make_order(i) for i in range(1, 6)]
        _make_runner(internal_engine, dest_engine, records).run(_spec("full_refresh"))

        rows = _count_validation_rows(internal_engine)
        rule_names = {r["rule"] for r in rows}
        assert rule_names == {
            "fetch_upsert_parity",
            "volume_sanity",
            "weekly_full_drift",
            "soft_delete_sanity",
        }

    def test_all_rules_ok_or_skipped_on_clean_full_refresh(self, internal_engine, dest_engine):
        records = [_make_order(i) for i in range(1, 6)]
        result = _make_runner(internal_engine, dest_engine, records).run(_spec("full_refresh"))

        assert result.validation_failed is False
        rows = _count_validation_rows(internal_engine)
        for row in rows:
            assert row["status"] in ("ok", "skipped"), (
                f"Rule {row['rule']} has unexpected status {row['status']}"
            )

    def test_validation_linked_to_correct_pipeline_resource(self, internal_engine, dest_engine):
        records = [_make_order(i) for i in range(1, 3)]
        _make_runner(internal_engine, dest_engine, records).run(_spec("full_refresh"))

        rows = _count_validation_rows(internal_engine)
        for row in rows:
            assert row["pipeline"] == _SHOP
            assert row["resource"] == "orders"

    def test_validation_not_run_when_sync_fails(self, internal_engine, dest_engine):
        """If sync fails, no validation rows should be inserted."""
        client = MagicMock()
        client.paginate.side_effect = RuntimeError("API down")
        runner = SyncRunner(_make_cfg(), client, internal_engine, dest_engine)
        result = runner.run(_spec("full_refresh"))

        assert result.status == "failed"
        rows = _count_validation_rows(internal_engine)
        assert len(rows) == 0, "Validation must not run after failed sync"

    def test_incremental_sync_volume_sanity_ok_when_fetched_positive(
        self, internal_engine, dest_engine
    ):
        """First sync (full_refresh), then incremental with records → volume_sanity=ok."""
        seed = [_make_order(i) for i in range(1, 6)]
        _make_runner(internal_engine, dest_engine, seed).run(_spec("full_refresh"))

        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE validation_results CASCADE"))

        new_record = [_make_order(6, "2026-02-01T10:00:00+00:00")]
        result = _make_runner(internal_engine, dest_engine, new_record).run(_spec("incremental"))

        assert result.status == "success"
        rows = _count_validation_rows(internal_engine)
        sanity = next(r for r in rows if r["rule"] == "volume_sanity")
        assert sanity["status"] == "ok"

    def test_validation_results_linked_to_sync_id(self, internal_engine, dest_engine):
        """Every validation_results row carries the same sync_id as the sync run."""
        records = [_make_order(i) for i in range(1, 4)]
        result = _make_runner(internal_engine, dest_engine, records).run(_spec("full_refresh"))

        assert result.status == "success"
        rows = _count_validation_rows(internal_engine, sync_id=result.sync_id)
        assert len(rows) == 4, "All 4 rules must be linked to the sync_id"
        for row in rows:
            assert str(row["sync_id"]) == result.sync_id

    def test_validation_completes_under_5_seconds(self, internal_engine, dest_engine):
        """Validation is SQL-only and must finish in < 5s (PRD §F5 Done khi)."""
        import time

        records = [_make_order(i) for i in range(1, 6)]
        start = time.perf_counter()
        result = _make_runner(internal_engine, dest_engine, records).run(_spec("full_refresh"))
        elapsed = time.perf_counter() - start

        assert result.status == "success"
        assert elapsed < 5.0, f"Sync+validation took {elapsed:.2f}s — must be < 5s"


# ── 2. Edge case: fetch_upsert_parity FAILED → data still present ─────────────

class TestFetchUpsertParityFailure:
    def test_data_not_rolled_back_when_parity_fails(self, internal_engine, dest_engine):
        """When fetch_upsert_parity fails, data must still be in destination DB."""
        records = [_make_order(i) for i in range(1, 6)]
        runner = _make_runner(internal_engine, dest_engine, records)

        # Make upsert_batch report fewer upserts than fetched
        from flowbyte.sync import runner as runner_module
        original_upsert = runner_module.upsert_batch

        def mock_upsert(conn, table, records, sync_id):
            stats = original_upsert(conn, table, records, sync_id)
            # Simulate: report fewer upserts (20% skip)
            from dataclasses import replace
            return replace(stats, upserted=max(0, stats.upserted - 1))

        with patch.object(runner_module, "upsert_batch", side_effect=mock_upsert):
            result = runner.run(_spec("full_refresh"))

        # Sync succeeds
        assert result.status == "success"

        # fetch_upsert_parity: 5 fetched, 4 reported → skip_pct=20% > 1% → failed
        rows = _count_validation_rows(internal_engine)
        parity = next((r for r in rows if r["rule"] == "fetch_upsert_parity"), None)
        assert parity is not None
        assert parity["status"] == "failed"
        assert parity["details"]["skipped_pct"] == 20.0
        # Data must still be present regardless of validation outcome
        with dest_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        assert count == 5, "All 5 records must be in DB regardless of validation outcome"


# ── 3. Edge case: soft_delete_sanity FAILED on mass-delete attempt ────────────

class TestSoftDeleteSanityFailure:
    def test_soft_delete_aborted_when_would_delete_more_than_5pct(
        self, internal_engine, dest_engine
    ):
        """Full refresh returning only 50 out of 100 records must NOT soft-delete 50%."""
        # Seed 100 orders
        seed = [_make_order(i) for i in range(1, 101)]
        _make_runner(internal_engine, dest_engine, seed).run(_spec("full_refresh"))

        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE validation_results, sync_runs, sync_checkpoints CASCADE"))

        # Full refresh returns only 50 orders (50% disappear → triggers sanity guard)
        partial = [_make_order(i) for i in range(1, 51)]
        result = _make_runner(internal_engine, dest_engine, partial).run(_spec("full_refresh"))

        # Sync itself succeeds (validation does not rollback data)
        assert result.status == "success"

        # soft_delete_sanity rule should be failed
        rows = _count_validation_rows(internal_engine)
        sanity = next(r for r in rows if r["rule"] == "soft_delete_sanity")
        assert sanity["status"] == "failed"
        assert "delete_pct" in sanity["details"]

        # Data integrity: the 100 original rows are still active (sweep was aborted)
        with dest_engine.connect() as conn:
            active = conn.execute(
                text("SELECT COUNT(*) FROM orders WHERE _deleted_at IS NULL")
            ).scalar()
        assert active == 100, "Soft-delete sweep must be aborted when > 5% would be deleted"


# ── 4. Edge case: incremental zero-fetch warning after 3 streaks ───────────────

class TestIncrementalVolumeSanityWarning:
    def test_warning_after_3_consecutive_zero_fetches(self, internal_engine, dest_engine):
        """Three consecutive zero-fetch incremental syncs → volume_sanity=warning."""
        # Seed records first
        seed = [_make_order(i) for i in range(1, 6)]
        _make_runner(internal_engine, dest_engine, seed).run(_spec("full_refresh"))

        # 3 zero-fetch incremental syncs
        for _ in range(3):
            _make_runner(internal_engine, dest_engine, []).run(_spec("incremental"))

        rows = _count_validation_rows(internal_engine)
        sanity_rows = [r for r in rows if r["rule"] == "volume_sanity"]
        # Last sync's volume_sanity should be warning
        last_sanity = sanity_rows[-1]
        assert last_sanity["status"] == "warning"
        assert last_sanity["details"].get("zero_fetch_streak") == 3


# ── 5. Edge case: weekly_full_drift WARNING between full refreshes ─────────────

class TestWeeklyFullDriftWarning:
    def test_drift_warning_when_fetch_count_increases_significantly(
        self, internal_engine, dest_engine
    ):
        """Full refresh with 10% more records than prev full refresh → drift warning."""
        # First full refresh: 100 records
        run1 = [_make_order(i, f"2026-01-{(i%28)+1:02d}T10:00:00+00:00") for i in range(1, 101)]
        _make_runner(internal_engine, dest_engine, run1).run(_spec("full_refresh"))

        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE validation_results CASCADE"))

        # Second full refresh: 110 records (10% more → warning territory 5-20%)
        run2 = [_make_order(i, f"2026-02-{(i%28)+1:02d}T10:00:00+00:00") for i in range(1, 111)]
        result = _make_runner(internal_engine, dest_engine, run2).run(_spec("full_refresh"))

        assert result.status == "success"
        rows = _count_validation_rows(internal_engine)
        drift = next(r for r in rows if r["rule"] == "weekly_full_drift")
        assert drift["status"] == "warning"
        assert 5.0 < drift["details"]["drift_pct"] <= 20.0


# ── 6. Edge case: executor crash does not crash runner ────────────────────────

class TestValidationExecutorCrashSafety:
    def test_executor_crash_does_not_fail_sync(self, internal_engine, dest_engine):
        """If ValidationExecutor.run() throws, sync result stays 'success'."""
        records = [_make_order(i) for i in range(1, 4)]
        runner = _make_runner(internal_engine, dest_engine, records)

        with patch(
            "flowbyte.validation.executor.ValidationExecutor.run",
            side_effect=RuntimeError("executor exploded"),
        ):
            result = runner.run(_spec("full_refresh"))

        assert result.status == "success"
        assert result.validation_failed is False
        # Data still persisted
        with dest_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        assert count == 3
