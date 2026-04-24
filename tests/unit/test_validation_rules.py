"""Unit tests for post-sync validation rules (§17.8)."""
from __future__ import annotations

import pytest

from flowbyte.validation.rules import (
    ValidationContext,
    fetch_upsert_parity,
    incremental_volume_sanity,
    run_all_validations,
    soft_delete_sanity,
    weekly_full_drift,
)


def _ctx(**kwargs):
    defaults = {
        "pipeline": "shop_main",
        "resource": "orders",
        "sync_id": "abc-123",
        "mode": "incremental",
        "fetched_count": 100,
        "upserted_count": 100,
        "rows_before": 5000,
        "rows_after": 5100,
        "prev_runs": [],
    }
    defaults.update(kwargs)
    return ValidationContext(**defaults)


class TestFetchUpsertParity:
    def test_ok_when_equal(self):
        r = fetch_upsert_parity(_ctx(fetched_count=100, upserted_count=100))
        assert r.status == "ok"

    def test_ok_when_skip_within_1_pct(self):
        # 1 skipped out of 100 = 1% → exactly at boundary, not > 1%
        r = fetch_upsert_parity(_ctx(fetched_count=100, upserted_count=99))
        assert r.status == "ok"

    def test_failed_when_skip_exceeds_1_pct(self):
        r = fetch_upsert_parity(_ctx(fetched_count=100, upserted_count=98))
        assert r.status == "failed"
        assert r.details["skipped_pct"] > 1.0

    def test_failed_details_have_expected_keys(self):
        r = fetch_upsert_parity(_ctx(fetched_count=100, upserted_count=90))
        assert "fetched" in r.details
        assert "upserted" in r.details
        assert "skipped_pct" in r.details

    def test_ok_when_fetched_zero(self):
        r = fetch_upsert_parity(_ctx(fetched_count=0, upserted_count=0))
        assert r.status == "ok"

    def test_rule_name(self):
        r = fetch_upsert_parity(_ctx())
        assert r.rule == "fetch_upsert_parity"


class TestIncrementalVolumeSanity:
    def test_skipped_for_full_refresh(self):
        r = incremental_volume_sanity(_ctx(mode="full_refresh"))
        assert r.status == "skipped"

    def test_ok_when_fetched_positive(self):
        r = incremental_volume_sanity(_ctx(fetched_count=5))
        assert r.status == "ok"

    def test_ok_when_fetched_zero_no_streak(self):
        r = incremental_volume_sanity(_ctx(fetched_count=0, prev_runs=[]))
        assert r.status == "ok"

    def test_warning_after_3_zero_fetches(self):
        prev = [{"fetched_count": 0}, {"fetched_count": 0}]
        r = incremental_volume_sanity(_ctx(fetched_count=0, prev_runs=prev))
        assert r.status == "warning"
        assert r.details["zero_fetch_streak"] == 3

    def test_ok_when_only_1_zero_previously(self):
        prev = [{"fetched_count": 0}]
        r = incremental_volume_sanity(_ctx(fetched_count=0, prev_runs=prev))
        assert r.status == "ok"

    def test_ok_when_previous_has_nonzero(self):
        prev = [{"fetched_count": 10}, {"fetched_count": 0}]
        r = incremental_volume_sanity(_ctx(fetched_count=0, prev_runs=prev))
        assert r.status == "ok"

    def test_hint_in_warning_details(self):
        prev = [{"fetched_count": 0}, {"fetched_count": 0}]
        r = incremental_volume_sanity(_ctx(fetched_count=0, prev_runs=prev))
        assert "hint" in r.details

    def test_rule_name(self):
        r = incremental_volume_sanity(_ctx())
        assert r.rule == "volume_sanity"


class TestWeeklyFullDrift:
    def test_skipped_for_incremental(self):
        r = weekly_full_drift(_ctx(mode="incremental"))
        assert r.status == "skipped"

    def test_skipped_when_no_previous_runs(self):
        r = weekly_full_drift(_ctx(mode="full_refresh", prev_runs=[]))
        assert r.status == "skipped"

    def test_skipped_when_no_previous_full_refresh(self):
        prev = [{"mode": "incremental", "fetched_count": 100}]
        r = weekly_full_drift(_ctx(mode="full_refresh", fetched_count=100, prev_runs=prev))
        assert r.status == "skipped"

    def test_ok_when_drift_within_5_pct(self):
        prev = [{"mode": "full_refresh", "fetched_count": 100}]
        r = weekly_full_drift(_ctx(mode="full_refresh", fetched_count=100, prev_runs=prev))
        assert r.status == "ok"

    def test_warning_when_drift_between_5_and_20_pct(self):
        prev = [{"mode": "full_refresh", "fetched_count": 100}]
        r = weekly_full_drift(_ctx(mode="full_refresh", fetched_count=110, prev_runs=prev))
        assert r.status == "warning"
        assert 5 < r.details["drift_pct"] <= 20

    def test_failed_when_drift_exceeds_20_pct(self):
        prev = [{"mode": "full_refresh", "fetched_count": 100}]
        r = weekly_full_drift(_ctx(mode="full_refresh", fetched_count=125, prev_runs=prev))
        assert r.status == "failed"
        assert r.details["drift_pct"] > 20

    def test_drift_pct_in_ok_details(self):
        prev = [{"mode": "full_refresh", "fetched_count": 100}]
        r = weekly_full_drift(_ctx(mode="full_refresh", fetched_count=103, prev_runs=prev))
        assert "drift_pct" in r.details

    def test_rule_name(self):
        r = weekly_full_drift(_ctx())
        assert r.rule == "weekly_full_drift"


class TestSoftDeleteSanity:
    def test_skipped_for_incremental(self):
        r = soft_delete_sanity(_ctx(mode="incremental"))
        assert r.status == "skipped"

    def test_ok_when_delete_pct_within_5_pct(self):
        r = soft_delete_sanity(_ctx(mode="full_refresh", rows_before=100, fetched_count=96))
        assert r.status == "ok"

    def test_ok_at_exact_5_pct_boundary(self):
        r = soft_delete_sanity(_ctx(mode="full_refresh", rows_before=100, fetched_count=95))
        assert r.status == "ok"

    def test_failed_when_delete_pct_exceeds_5_pct(self):
        r = soft_delete_sanity(_ctx(mode="full_refresh", rows_before=100, fetched_count=94))
        assert r.status == "failed"
        assert "delete_pct" in r.details

    def test_action_in_failed_details(self):
        r = soft_delete_sanity(_ctx(mode="full_refresh", rows_before=100, fetched_count=80))
        assert "action" in r.details

    def test_zero_rows_before_does_not_divide_by_zero(self):
        r = soft_delete_sanity(_ctx(mode="full_refresh", rows_before=0, fetched_count=0))
        assert r.status in ("ok", "failed")

    def test_rule_name(self):
        r = soft_delete_sanity(_ctx())
        assert r.rule == "soft_delete_sanity"


class TestRunAllValidations:
    def test_returns_list_of_4(self):
        results = run_all_validations(_ctx())
        assert len(results) == 4

    def test_all_have_status(self):
        results = run_all_validations(_ctx())
        for r in results:
            assert r.status in ("ok", "warning", "failed", "skipped")

    def test_all_have_rule_name(self):
        results = run_all_validations(_ctx())
        rules = {r.rule for r in results}
        assert rules == {"fetch_upsert_parity", "volume_sanity", "weekly_full_drift", "soft_delete_sanity"}
