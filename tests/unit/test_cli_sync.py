"""Security + input validation tests for the 'run' sync command."""
from __future__ import annotations

import pytest
from typer.testing import CliRunner

from flowbyte.cli.app import app

runner = CliRunner()


class TestRunCommandInputValidation:
    """sync.py run command: pipeline name, resource, and mode must be validated
    before touching the DB to prevent bad data and confusing SQL errors."""

    def test_invalid_pipeline_name_path_traversal_rejected(self):
        result = runner.invoke(app, ["run", "../../etc/passwd"])
        assert result.exit_code == 2

    def test_invalid_pipeline_name_uppercase_rejected(self):
        result = runner.invoke(app, ["run", "Shop_Main"])
        assert result.exit_code == 2

    def test_invalid_pipeline_name_too_long_rejected(self):
        result = runner.invoke(app, ["run", "a" * 33])
        assert result.exit_code == 2

    def test_invalid_resource_rejected(self):
        result = runner.invoke(app, ["run", "shop_main", "--resource", "payments"])
        assert result.exit_code == 2
        assert "Unknown resource" in result.output or "Valid" in result.output

    def test_invalid_mode_rejected(self):
        result = runner.invoke(app, ["run", "shop_main", "--mode", "streaming"])
        assert result.exit_code == 2
        assert "Invalid mode" in result.output or "Use:" in result.output

    def test_valid_resource_passes_validation(self, monkeypatch):
        from unittest.mock import MagicMock, patch

        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        mock_result = MagicMock()
        mock_result.scalar.return_value = "00000000-0000-0000-0000-000000000001"
        conn_ctx.execute = MagicMock(return_value=mock_result)
        mock_engine = MagicMock()
        mock_engine.begin = MagicMock(return_value=conn_ctx)

        with patch("flowbyte.db.engine.get_internal_engine", return_value=mock_engine):
            result = runner.invoke(app, ["run", "shop_main", "--resource", "orders"])
        assert result.exit_code == 0
        assert "queued" in result.output.lower()

    def test_valid_mode_full_refresh_passes_validation(self, monkeypatch):
        from unittest.mock import MagicMock, patch

        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        mock_result = MagicMock()
        mock_result.scalar.return_value = "00000000-0000-0000-0000-000000000002"
        conn_ctx.execute = MagicMock(return_value=mock_result)
        mock_engine = MagicMock()
        mock_engine.begin = MagicMock(return_value=conn_ctx)

        with patch("flowbyte.db.engine.get_internal_engine", return_value=mock_engine):
            result = runner.invoke(app, ["run", "shop_main", "--mode", "full_refresh"])
        assert result.exit_code == 0


class TestTelegramFormatEscaping:
    """Telegram Markdown formatting: backticks in error strings must be escaped
    to prevent breaking the code block and injecting unintended Markdown."""

    def test_backtick_in_error_is_replaced(self):
        from flowbyte.alerting.telegram import format_sync_fail_alert

        msg = format_sync_fail_alert(
            pipeline="shop_main",
            resource="orders",
            error="Unexpected token `null` at position 0",
            sync_id="abc12345",
        )
        assert "`null`" not in msg
        assert "'null'" in msg

    def test_error_without_backticks_unchanged(self):
        from flowbyte.alerting.telegram import format_sync_fail_alert

        error = "Connection refused: host=localhost port=5432"
        msg = format_sync_fail_alert("shop_main", "orders", error, "abc12345")
        assert error in msg

    def test_error_truncated_to_300_chars(self):
        from flowbyte.alerting.telegram import format_sync_fail_alert

        long_error = "x" * 500
        msg = format_sync_fail_alert("shop_main", "orders", long_error, "abc12345")
        assert "x" * 301 not in msg
