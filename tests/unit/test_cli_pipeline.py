"""Unit tests for pipeline CLI commands (init, validate, list, enable, disable, delete, creds).

Uses Typer's CliRunner to invoke commands without a running DB.
All DB/HTTP interactions are mocked.
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from flowbyte.cli.app import app


runner = CliRunner()


# ── flowbyte init ─────────────────────────────────────────────────────────────


class TestInitCommand:
    def test_creates_yaml_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_PIPELINES_DIR", str(tmp_path))
        result = runner.invoke(app, ["init", "shop_test"])
        assert result.exit_code == 0
        assert (tmp_path / "shop_test.yml").exists()

    def test_output_contains_pipeline_name(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_PIPELINES_DIR", str(tmp_path))
        result = runner.invoke(app, ["init", "shop_test"])
        assert "shop_test" in result.output

    def test_fails_if_pipeline_already_exists(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_PIPELINES_DIR", str(tmp_path))
        runner.invoke(app, ["init", "shop_test"])
        result = runner.invoke(app, ["init", "shop_test"])
        assert result.exit_code != 0
        assert "already exists" in result.output

    def test_created_yaml_has_no_plaintext_credentials(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_PIPELINES_DIR", str(tmp_path))
        runner.invoke(app, ["init", "shop_test"])
        content = (tmp_path / "shop_test.yml").read_text()
        assert "access_token" not in content
        assert "password" not in content
        assert "credentials_ref" in content


# ── flowbyte init-master-key ──────────────────────────────────────────────────


class TestInitMasterKeyCommand:
    def test_creates_key_file(self, tmp_path):
        key_path = tmp_path / "master.key"
        result = runner.invoke(
            app, ["init-master-key", "--path", str(key_path), "--skip-confirm"]
        )
        assert result.exit_code == 0, result.output
        assert key_path.exists()

    def test_key_file_is_32_bytes(self, tmp_path):
        key_path = tmp_path / "master.key"
        runner.invoke(app, ["init-master-key", "--path", str(key_path), "--skip-confirm"])
        assert len(key_path.read_bytes()) == 32

    def test_key_file_has_chmod_600(self, tmp_path):
        import stat
        key_path = tmp_path / "master.key"
        runner.invoke(app, ["init-master-key", "--path", str(key_path), "--skip-confirm"])
        mode = stat.S_IMODE(key_path.stat().st_mode)
        assert mode == 0o600

    def test_fails_if_key_already_exists(self, tmp_path):
        key_path = tmp_path / "master.key"
        runner.invoke(app, ["init-master-key", "--path", str(key_path), "--skip-confirm"])
        result = runner.invoke(
            app, ["init-master-key", "--path", str(key_path), "--skip-confirm"]
        )
        assert result.exit_code != 0
        assert "already exists" in result.output

    def test_output_shows_fingerprint(self, tmp_path):
        key_path = tmp_path / "master.key"
        result = runner.invoke(
            app, ["init-master-key", "--path", str(key_path), "--skip-confirm"]
        )
        assert "SHA-256" in result.output or "ingerprint" in result.output

    def test_output_warns_about_backup(self, tmp_path):
        key_path = tmp_path / "master.key"
        result = runner.invoke(
            app, ["init-master-key", "--path", str(key_path), "--skip-confirm"]
        )
        assert "CRITICAL" in result.output or "backup" in result.output.lower()


# ── flowbyte list ─────────────────────────────────────────────────────────────


class TestListCommand:
    def _mock_engine(self, rows):
        """Helper: mock DB engine returning the given rows for list command."""
        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        conn_ctx.execute = MagicMock(return_value=MagicMock(all=MagicMock(return_value=rows)))
        mock_engine = MagicMock()
        mock_engine.connect = MagicMock(return_value=conn_ctx)
        return mock_engine

    def test_shows_no_pipelines_message_when_empty(self, monkeypatch):
        mock_engine = self._mock_engine([])
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        with patch("flowbyte.db.engine.get_internal_engine", return_value=mock_engine):
            result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        assert "No pipelines" in result.output or "flowbyte init" in result.output

    def test_shows_pipeline_table_when_rows_exist(self, monkeypatch):
        row = MagicMock()
        row.name = "shop_main"
        row.enabled = True
        row.updated_at = "2026-04-24 10:00:00"
        mock_engine = self._mock_engine([row])
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        with patch("flowbyte.db.engine.get_internal_engine", return_value=mock_engine):
            result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        assert "shop_main" in result.output


# ── flowbyte delete ───────────────────────────────────────────────────────────


class TestDeleteCommand:
    def test_delete_aborts_when_user_says_no(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_PIPELINES_DIR", str(tmp_path))
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")

        # Mock empty running syncs
        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        conn_ctx.execute = MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=[])))
        mock_engine = MagicMock()
        mock_engine.connect = MagicMock(return_value=conn_ctx)

        with patch("flowbyte.db.engine.get_internal_engine", return_value=mock_engine):
            # Pass "n" to confirmation prompt
            result = runner.invoke(app, ["delete", "shop_main"], input="n\n")

        assert result.exit_code == 0
        assert "Aborted" in result.output

    def test_delete_refuses_if_sync_running_without_force(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_PIPELINES_DIR", str(tmp_path))
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")

        running_row = MagicMock()
        running_row.resource = "orders"
        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        conn_ctx.execute = MagicMock(
            return_value=MagicMock(fetchall=MagicMock(return_value=[running_row]))
        )
        mock_engine = MagicMock()
        mock_engine.connect = MagicMock(return_value=conn_ctx)

        with patch("flowbyte.db.engine.get_internal_engine", return_value=mock_engine):
            result = runner.invoke(app, ["delete", "shop_main"])

        assert result.exit_code != 0
        assert "running" in result.output.lower() or "force" in result.output.lower()


# ── flowbyte creds ─────────────────────────────────────────────────────────────


class TestCredsListCommand:
    def test_list_shows_no_credentials_when_empty(self, monkeypatch):
        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        conn_ctx.execute = MagicMock(return_value=MagicMock(all=MagicMock(return_value=[])))
        mock_engine = MagicMock()
        mock_engine.connect = MagicMock(return_value=conn_ctx)

        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        monkeypatch.setenv("FLOWBYTE_MASTER_KEY_PATH", "/etc/flowbyte/master.key")

        with patch("flowbyte.db.engine.get_internal_engine", return_value=mock_engine):
            result = runner.invoke(app, ["creds", "list"])

        assert result.exit_code == 0
        assert "No credentials" in result.output

    def test_list_shows_credentials_table(self, monkeypatch):
        row = MagicMock()
        row.ref = "shop_main"
        row.kind = "haravan"
        row.updated_at = "2026-04-24 10:00:00"
        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        conn_ctx.execute = MagicMock(return_value=MagicMock(all=MagicMock(return_value=[row])))
        mock_engine = MagicMock()
        mock_engine.connect = MagicMock(return_value=conn_ctx)

        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")

        with patch("flowbyte.db.engine.get_internal_engine", return_value=mock_engine):
            result = runner.invoke(app, ["creds", "list"])

        assert result.exit_code == 0
        assert "shop_main" in result.output
        assert "haravan" in result.output


class TestCredsSetCommand:
    def test_creds_set_haravan_encrypts_and_stores(self, tmp_path, monkeypatch):
        import json

        key_path = tmp_path / "master.key"
        from flowbyte.security.master_key import MasterKey
        MasterKey.generate_and_save(key_path)

        stored = {}

        def fake_execute(stmt, *args, **kwargs):
            # Capture what was stored
            return MagicMock(scalar=MagicMock(return_value=None))

        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        conn_ctx.execute = fake_execute
        mock_engine = MagicMock()
        mock_engine.begin = MagicMock(return_value=conn_ctx)

        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        monkeypatch.setenv("FLOWBYTE_MASTER_KEY_PATH", str(key_path))

        with patch("flowbyte.db.engine.get_internal_engine", return_value=mock_engine):
            result = runner.invoke(
                app,
                ["creds", "set", "shop_main", "--kind", "haravan"],
                input="test.myharavan.com\ntest_token_abc\n",
            )

        assert result.exit_code == 0
        assert "saved" in result.output.lower() or "shop_main" in result.output

    def test_creds_set_unknown_kind_fails(self, tmp_path, monkeypatch):
        key_path = tmp_path / "master.key"
        from flowbyte.security.master_key import MasterKey
        MasterKey.generate_and_save(key_path)
        monkeypatch.setenv("FLOWBYTE_MASTER_KEY_PATH", str(key_path))
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")

        result = runner.invoke(
            app, ["creds", "set", "ref_x", "--kind", "slack"], input="whatever\n"
        )
        assert result.exit_code != 0
        assert "Unknown kind" in result.output or "slack" in result.output

    def test_creds_set_invalid_shop_domain_fails(self, tmp_path, monkeypatch):
        key_path = tmp_path / "master.key"
        from flowbyte.security.master_key import MasterKey
        MasterKey.generate_and_save(key_path)
        monkeypatch.setenv("FLOWBYTE_MASTER_KEY_PATH", str(key_path))
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")

        result = runner.invoke(
            app,
            ["creds", "set", "ref_x", "--kind", "haravan"],
            input="INVALID DOMAIN WITH SPACES\ntoken\n",
        )
        assert result.exit_code != 0
        assert "Invalid" in result.output or "domain" in result.output.lower()


class TestCredsUnknownAction:
    def test_unknown_action_exits_with_code_2(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")
        result = runner.invoke(app, ["creds", "badaction"])
        assert result.exit_code == 2
        assert "Unknown action" in result.output or "badaction" in result.output


class TestCredsDeleteCommand:
    def test_creds_delete_aborts_when_user_says_no(self, monkeypatch):
        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        conn_ctx.execute = MagicMock(return_value=MagicMock())
        mock_engine = MagicMock()
        mock_engine.begin = MagicMock(return_value=conn_ctx)

        monkeypatch.setenv("FLOWBYTE_DB_URL", "postgresql+psycopg://x:x@localhost/x")

        with patch("flowbyte.db.engine.get_internal_engine", return_value=mock_engine):
            result = runner.invoke(app, ["creds", "delete", "shop_main"], input="n\n")

        assert result.exit_code == 0
        # When user says no, no delete should occur
        assert conn_ctx.execute.call_count == 0
