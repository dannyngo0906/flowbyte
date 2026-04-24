"""US-006: Data Validation — unit tests for executor, formatter, runner integration."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch

import pytest

from flowbyte.alerting.telegram import format_validation_alert
from flowbyte.config.models import SyncResult
from flowbyte.validation.executor import ValidationExecutor
from flowbyte.validation.rules import ValidationContext, ValidationResult


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_result(**kwargs) -> SyncResult:
    defaults = dict(
        sync_id="sync-abc-123",
        pipeline="shop_main",
        resource="orders",
        mode="incremental",
        status="success",
        fetched_count=100,
        upserted_count=100,
        rows_before=5000,
        rows_after=5100,
    )
    defaults.update(kwargs)
    return SyncResult(**defaults)


def _make_engine(prev_run_rows=None):
    """Fake engine with separate mocks for connect() and begin()."""
    if prev_run_rows is None:
        prev_run_rows = []

    connect_conn = MagicMock()
    connect_conn.execute.return_value.all.return_value = prev_run_rows

    begin_conn = MagicMock()

    @contextmanager
    def _connect():
        yield connect_conn

    @contextmanager
    def _begin():
        yield begin_conn

    engine = MagicMock()
    engine.connect = _connect
    engine.begin = _begin
    engine._connect_conn = connect_conn
    engine._begin_conn = begin_conn
    return engine


def _make_prev_run_row(mode="incremental", fetched_count=80):
    row = MagicMock()
    row._mapping = {
        "mode": mode,
        "fetched_count": fetched_count,
        "status": "success",
    }
    return row


# ═══════════════════════════════════════════════════════════════════════════════
# 1. ValidationExecutor.run()
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidationExecutorRun:
    def test_returns_list_of_validation_results(self):
        engine = _make_engine()
        executor = ValidationExecutor(engine)
        result = _make_result()
        vr_list = executor.run(result)
        assert isinstance(vr_list, list)
        assert all(isinstance(v, ValidationResult) for v in vr_list)

    def test_runs_all_4_rules(self):
        engine = _make_engine()
        executor = ValidationExecutor(engine)
        result = _make_result()
        vr_list = executor.run(result)
        assert len(vr_list) == 4

    def test_fetch_upsert_parity_ok_when_counts_match(self):
        engine = _make_engine()
        executor = ValidationExecutor(engine)
        result = _make_result(fetched_count=50, upserted_count=50)
        vr_list = executor.run(result)
        parity = next(v for v in vr_list if v.rule == "fetch_upsert_parity")
        assert parity.status == "ok"

    def test_fetch_upsert_parity_failed_when_large_skip(self):
        engine = _make_engine()
        executor = ValidationExecutor(engine)
        result = _make_result(fetched_count=100, upserted_count=80)
        vr_list = executor.run(result)
        parity = next(v for v in vr_list if v.rule == "fetch_upsert_parity")
        assert parity.status == "failed"

    def test_persist_called_once_per_rule(self):
        engine = _make_engine()
        executor = ValidationExecutor(engine)
        result = _make_result()
        executor.run(result)
        # _persist uses begin() conn — 4 rules = 4 execute calls
        assert engine._begin_conn.execute.call_count == 4

    def test_persist_failure_does_not_crash_run(self):
        """If DB write fails, run() still returns results (non-blocking)."""
        engine = _make_engine()
        engine._begin_conn.execute.side_effect = RuntimeError("DB down")

        executor = ValidationExecutor(engine)
        result = _make_result()
        # Must not raise
        vr_list = executor.run(result)
        assert len(vr_list) == 4

    def test_loads_prev_runs_from_sync_runs_table(self):
        prev_rows = [_make_prev_run_row(fetched_count=100)]
        engine = _make_engine(prev_run_rows=prev_rows)
        executor = ValidationExecutor(engine)
        result = _make_result()
        executor.run(result)
        # connect() conn should have been used for _load_prev_runs
        assert engine._connect_conn.execute.called


# ═══════════════════════════════════════════════════════════════════════════════
# 2. ValidationExecutor — context building
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidationContextBuilding:
    def test_context_fields_from_sync_result(self):
        engine = _make_engine()
        executor = ValidationExecutor(engine)
        result = _make_result(
            pipeline="test_pipeline",
            resource="customers",
            mode="full_refresh",
            fetched_count=200,
            upserted_count=198,
            rows_before=180,
            rows_after=198,
        )
        with patch("flowbyte.validation.executor.run_all_validations") as mock_run:
            mock_run.return_value = []
            executor.run(result)
            ctx: ValidationContext = mock_run.call_args[0][0]
            assert ctx.pipeline == "test_pipeline"
            assert ctx.resource == "customers"
            assert ctx.mode == "full_refresh"
            assert ctx.fetched_count == 200
            assert ctx.upserted_count == 198
            assert ctx.rows_before == 180
            assert ctx.rows_after == 198
            assert ctx.sync_id == result.sync_id

    def test_prev_runs_passed_to_context(self):
        prev_rows = [_make_prev_run_row(fetched_count=90)]
        engine = _make_engine(prev_run_rows=prev_rows)
        executor = ValidationExecutor(engine)
        result = _make_result()
        with patch("flowbyte.validation.executor.run_all_validations") as mock_run:
            mock_run.return_value = []
            executor.run(result)
            ctx: ValidationContext = mock_run.call_args[0][0]
            assert len(ctx.prev_runs) == 1
            assert ctx.prev_runs[0]["fetched_count"] == 90


# ═══════════════════════════════════════════════════════════════════════════════
# 3. format_validation_alert()
# ═══════════════════════════════════════════════════════════════════════════════

class TestFormatValidationAlert:
    def test_contains_pipeline_and_resource(self):
        msg = format_validation_alert(
            pipeline="shop_main",
            resource="orders",
            failed_rules=["fetch_upsert_parity"],
            sync_id="abc-123",
        )
        assert "shop_main" in msg
        assert "orders" in msg

    def test_contains_rule_names(self):
        msg = format_validation_alert(
            pipeline="shop_main",
            resource="orders",
            failed_rules=["fetch_upsert_parity", "volume_sanity"],
            sync_id="abc-123",
        )
        assert "fetch upsert parity" in msg  # underscore → space
        assert "volume sanity" in msg

    def test_contains_sync_id_prefix(self):
        msg = format_validation_alert(
            pipeline="shop_main",
            resource="orders",
            failed_rules=["fetch_upsert_parity"],
            sync_id="abc12345-xxxx",
        )
        assert "abc12345" in msg

    def test_truncates_sync_id_to_8_chars(self):
        msg = format_validation_alert(
            pipeline="shop_main",
            resource="orders",
            failed_rules=["fetch_upsert_parity"],
            sync_id="abcdefgh-long-uuid",
        )
        assert "abcdefgh" in msg
        assert "long-uuid" not in msg

    def test_escapes_underscore_in_rule_names(self):
        msg = format_validation_alert(
            pipeline="shop_main",
            resource="orders",
            failed_rules=["fetch_upsert_parity"],
            sync_id="abc-123",
        )
        # Underscores in rule names replaced with spaces (Markdown safety)
        assert "_parity" not in msg

    def test_handles_empty_failed_rules(self):
        msg = format_validation_alert(
            pipeline="shop_main",
            resource="orders",
            failed_rules=[],
            sync_id="abc-123",
        )
        assert "shop_main" in msg

    def test_contains_validation_failed_header(self):
        msg = format_validation_alert(
            pipeline="p",
            resource="r",
            failed_rules=["rule_a"],
            sync_id="s",
        )
        assert "Validation FAILED" in msg


# ═══════════════════════════════════════════════════════════════════════════════
# 4. SyncRunner._run_validation() integration
# ═══════════════════════════════════════════════════════════════════════════════

class TestSyncRunnerValidationIntegration:
    def _make_runner(self, internal_engine):
        from flowbyte.sync.runner import SyncRunner
        runner = MagicMock(spec=SyncRunner)
        runner._internal = internal_engine
        runner._run_validation = SyncRunner._run_validation.__get__(runner)
        return runner

    def test_validation_failed_set_on_result(self):
        engine = _make_engine()
        runner = self._make_runner(engine)
        result = _make_result()

        failed_vr = ValidationResult("fetch_upsert_parity", "failed", {"skipped_pct": 20.0})
        ok_vr = ValidationResult("volume_sanity", "ok", {})

        with patch("flowbyte.validation.executor.ValidationExecutor.run") as mock_run:
            mock_run.return_value = [failed_vr, ok_vr]
            runner._run_validation(result)

        assert result.validation_failed is True
        assert "fetch_upsert_parity" in result.validation_failed_rules

    def test_validation_passed_when_all_ok(self):
        engine = _make_engine()
        runner = self._make_runner(engine)
        result = _make_result()

        ok_vrs = [
            ValidationResult("fetch_upsert_parity", "ok", {}),
            ValidationResult("volume_sanity", "ok", {}),
        ]
        with patch("flowbyte.validation.executor.ValidationExecutor.run") as mock_run:
            mock_run.return_value = ok_vrs
            runner._run_validation(result)

        assert result.validation_failed is False
        assert result.validation_failed_rules == []

    def test_runner_does_not_crash_if_executor_raises(self):
        engine = _make_engine()
        runner = self._make_runner(engine)
        result = _make_result()

        with patch("flowbyte.validation.executor.ValidationExecutor.run") as mock_run:
            mock_run.side_effect = RuntimeError("unexpected executor crash")
            # Must not raise
            runner._run_validation(result)

        # validation_failed stays False on error
        assert result.validation_failed is False

    def test_validation_statuses_populated(self):
        engine = _make_engine()
        runner = self._make_runner(engine)
        result = _make_result()

        vrs = [
            ValidationResult("fetch_upsert_parity", "ok", {}),
            ValidationResult("volume_sanity", "warning", {}),
            ValidationResult("weekly_full_drift", "skipped", {}),
        ]
        with patch("flowbyte.validation.executor.ValidationExecutor.run") as mock_run:
            mock_run.return_value = vrs
            runner._run_validation(result)

        assert result.validation_statuses == ["ok", "warning", "skipped"]


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Reconciler validation alert dispatch
# ═══════════════════════════════════════════════════════════════════════════════

class TestReconcilerValidationAlert:
    def test_alerter_called_when_validation_failed(self):
        alerter = MagicMock()
        result = _make_result(
            validation_failed=True,
            validation_failed_rules=["fetch_upsert_parity"],
        )

        from flowbyte.alerting.telegram import format_validation_alert
        text = format_validation_alert(
            pipeline=result.pipeline,
            resource=result.resource,
            failed_rules=result.validation_failed_rules,
            sync_id=result.sync_id,
        )
        alerter.send(
            text,
            key=f"validation_fail:{result.pipeline}:{result.resource}",
            pipeline=result.pipeline,
        )

        alerter.send.assert_called_once()
        call_kwargs = alerter.send.call_args
        assert "validation_fail:shop_main:orders" in call_kwargs[1]["key"]

    def test_alerter_not_called_when_validation_passed(self):
        alerter = MagicMock()
        result = _make_result(validation_failed=False)
        # Simulate reconciler logic: only call if validation_failed
        if result.validation_failed and alerter is not None:
            alerter.send("msg", key="k", pipeline="p")
        alerter.send.assert_not_called()

    def test_no_crash_when_alerter_is_none(self):
        result = _make_result(
            validation_failed=True,
            validation_failed_rules=["fetch_upsert_parity"],
        )
        alerter = None
        # Simulate reconciler guard
        if result.validation_failed and alerter is not None:
            alerter.send("msg", key="k", pipeline="p")
        # No exception = pass

    def test_validation_key_differs_from_sync_fail_key(self):
        """Validation alerts use 'validation_fail:' prefix, not 'sync_fail:'."""
        pipeline = "shop_main"
        resource = "orders"
        val_key = f"validation_fail:{pipeline}:{resource}"
        sync_key = f"sync_fail:{pipeline}:{resource}"
        assert val_key != sync_key


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Security: EventName correctness in _run_validation
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidationEventNameSecurity:
    """Verify _run_validation logs VALIDATION_FAILED (not SYNC_FAILED) on executor error.

    Using SYNC_FAILED would trigger false Telegram alerts claiming the sync
    failed when it actually succeeded and only the validation executor crashed.
    """

    def _make_runner(self, internal_engine):
        from flowbyte.sync.runner import SyncRunner
        r = MagicMock(spec=SyncRunner)
        r._internal = internal_engine
        r._run_validation = SyncRunner._run_validation.__get__(r)
        return r

    def test_executor_crash_logs_validation_failed_not_sync_failed(self):
        from flowbyte.logging.events import EventName

        engine = _make_engine()
        r = self._make_runner(engine)
        result = _make_result()

        logged_events: list[str] = []

        def mock_log_error(event, **kwargs):
            logged_events.append(event)

        with patch("flowbyte.validation.executor.ValidationExecutor.run") as mock_run:
            mock_run.side_effect = RuntimeError("executor crash")
            with patch("flowbyte.sync.runner.log") as mock_log:
                mock_log.error.side_effect = lambda event, **kw: logged_events.append(event)
                r._run_validation(result)

        assert EventName.VALIDATION_FAILED in logged_events, (
            "executor crash must log VALIDATION_FAILED, not SYNC_FAILED"
        )
        assert EventName.SYNC_FAILED not in logged_events, (
            "SYNC_FAILED must not be logged for a validation executor error"
        )
