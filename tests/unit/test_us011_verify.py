"""Unit tests for US-011: flowbyte verify command.

Done-when (PRD US-011 AC-3.4):
  ✅ T-11.3  flowbyte verify <pipeline> so sánh count ±tolerance%
  ✅ --resource lọc đúng 1 resource
  ✅ --tolerance tuỳ chỉnh ngưỡng
  ✅ remote=0 không divide-by-zero
  ✅ Pipeline name invalid → exit 2, không query DB
  ✅ Deviation vượt threshold → exit 1
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from flowbyte.cli.app import app

runner = CliRunner()

_PIPELINE = "shop_main"
_DB_URL = "postgresql+psycopg://x:x@localhost/x"

_VALID_CONFIG_JSON = {
    "name": _PIPELINE,
    "haravan_credentials_ref": "shop_main",
    "haravan_shop_domain": "test.myharavan.com",
    "destination": {
        "host": "localhost",
        "port": 5432,
        "user": "flowbyte",
        "database": "flowbyte_data",
        "credentials_ref": "pg_main",
    },
    "resources": {},
}


def _make_pipeline_row(name: str = _PIPELINE, config_json: dict | None = None):
    row = MagicMock()
    row.name = name
    row.config_json = config_json if config_json is not None else _VALID_CONFIG_JSON
    return row


def _make_internal_engine(pipeline_row=None):
    """Mock internal engine that returns pipeline_row from SELECT pipelines."""
    if pipeline_row is None:
        pipeline_row = _make_pipeline_row()

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.one_or_none.return_value = pipeline_row
    mock_engine.connect.return_value = mock_conn
    return mock_engine


def _make_dest_engine(local_counts: dict[str, int]):
    """Mock dest engine that returns counts per resource in call order."""
    counts_iter = iter(
        local_counts.get(r, 0) for r in ("orders", "customers", "products")
    )

    mock_dest_engine = MagicMock()
    mock_dest_conn = MagicMock()
    mock_dest_conn.__enter__ = lambda s: mock_dest_conn
    mock_dest_conn.__exit__ = MagicMock(return_value=False)
    mock_dest_conn.execute.return_value.scalar.side_effect = lambda: next(counts_iter)
    mock_dest_engine.connect.return_value = mock_dest_conn
    return mock_dest_engine


def _make_dest_engine_for_resource(local_count: int):
    """Mock dest engine returning a single count value (for --resource tests)."""
    mock_dest_engine = MagicMock()
    mock_dest_conn = MagicMock()
    mock_dest_conn.__enter__ = lambda s: mock_dest_conn
    mock_dest_conn.__exit__ = MagicMock(return_value=False)
    mock_dest_conn.execute.return_value.scalar.return_value = local_count
    mock_dest_engine.connect.return_value = mock_dest_conn
    return mock_dest_engine


def _make_haravan_client(counts: dict[str, int]):
    mock_client = MagicMock()
    mock_client.get_count.side_effect = lambda res, params=None: counts.get(res, 0)
    return mock_client


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestVerifyAllResourcesPass:
    """T-11.3: all 3 resources within 0.1% → exit 0."""

    def test_exit_code_zero(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        mock_client = _make_haravan_client(
            {"orders": 10000, "customers": 5000, "products": 2000}
        )
        internal_engine = _make_internal_engine()
        dest_engine = _make_dest_engine(
            {"orders": 10000, "customers": 5000, "products": 2000}
        )

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.db.engine.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials", return_value={"access_token": "tok", "password": "pw"}),
            patch("flowbyte.scheduler.reconciler._build_dest_url", return_value="postgresql+psycopg://u:p@localhost/d"),
            patch("flowbyte.haravan.client.HaravanClient", return_value=mock_client),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert result.exit_code == 0, f"Expected exit 0, got:\n{result.output}"

    def test_output_contains_ok_for_all(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        mock_client = _make_haravan_client(
            {"orders": 10000, "customers": 5000, "products": 2000}
        )
        internal_engine = _make_internal_engine()
        dest_engine = _make_dest_engine(
            {"orders": 9999, "customers": 4999, "products": 1999}
        )

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.db.engine.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials", return_value={"access_token": "tok", "password": "pw"}),
            patch("flowbyte.scheduler.reconciler._build_dest_url", return_value="postgresql+psycopg://u:p@localhost/d"),
            patch("flowbyte.haravan.client.HaravanClient", return_value=mock_client),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert "✓ OK" in result.output
        assert "✗ FAIL" not in result.output

    def test_deviation_shown_in_table(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        mock_client = _make_haravan_client(
            {"orders": 1000, "customers": 500, "products": 200}
        )
        internal_engine = _make_internal_engine()
        dest_engine = _make_dest_engine(
            {"orders": 1000, "customers": 500, "products": 200}
        )

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.db.engine.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials", return_value={"access_token": "tok", "password": "pw"}),
            patch("flowbyte.scheduler.reconciler._build_dest_url", return_value="postgresql+psycopg://u:p@localhost/d"),
            patch("flowbyte.haravan.client.HaravanClient", return_value=mock_client),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert "0.00%" in result.output


class TestVerifyOneResourceFails:
    """Deviation > tolerance → exit 1."""

    def test_exit_code_one_when_orders_fails(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        # orders: remote=10000, local=9800 → deviation=2% > 0.1%
        mock_client = _make_haravan_client(
            {"orders": 10000, "customers": 500, "products": 200}
        )
        internal_engine = _make_internal_engine()
        dest_engine = _make_dest_engine(
            {"orders": 9800, "customers": 500, "products": 200}
        )

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.db.engine.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials", return_value={"access_token": "tok", "password": "pw"}),
            patch("flowbyte.scheduler.reconciler._build_dest_url", return_value="postgresql+psycopg://u:p@localhost/d"),
            patch("flowbyte.haravan.client.HaravanClient", return_value=mock_client),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert result.exit_code == 1

    def test_fail_status_appears_in_output(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        mock_client = _make_haravan_client(
            {"orders": 10000, "customers": 500, "products": 200}
        )
        internal_engine = _make_internal_engine()
        dest_engine = _make_dest_engine(
            {"orders": 9000, "customers": 500, "products": 200}
        )

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.db.engine.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials", return_value={"access_token": "tok", "password": "pw"}),
            patch("flowbyte.scheduler.reconciler._build_dest_url", return_value="postgresql+psycopg://u:p@localhost/d"),
            patch("flowbyte.haravan.client.HaravanClient", return_value=mock_client),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert "FAIL" in result.output


class TestVerifySingleResourceFlag:
    """--resource customers → only checks customers."""

    def test_only_customers_checked(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        mock_client = _make_haravan_client({"customers": 500})
        internal_engine = _make_internal_engine()
        dest_engine = _make_dest_engine_for_resource(500)

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.db.engine.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials", return_value={"access_token": "tok", "password": "pw"}),
            patch("flowbyte.scheduler.reconciler._build_dest_url", return_value="postgresql+psycopg://u:p@localhost/d"),
            patch("flowbyte.haravan.client.HaravanClient", return_value=mock_client),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE, "--resource", "customers"])

        assert result.exit_code == 0
        # get_count called once only (for customers)
        mock_client.get_count.assert_called_once_with("customers")
        # orders and products not in output table
        assert "orders" not in result.output
        assert "products" not in result.output
        assert "customers" in result.output


class TestVerifyZeroRemoteCount:
    """Remote count = 0, local = 0 → deviation=0%, pass (no divide-by-zero)."""

    def test_zero_remote_zero_local_passes(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        mock_client = _make_haravan_client(
            {"orders": 0, "customers": 0, "products": 0}
        )
        internal_engine = _make_internal_engine()
        dest_engine = _make_dest_engine(
            {"orders": 0, "customers": 0, "products": 0}
        )

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.db.engine.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials", return_value={"access_token": "tok", "password": "pw"}),
            patch("flowbyte.scheduler.reconciler._build_dest_url", return_value="postgresql+psycopg://u:p@localhost/d"),
            patch("flowbyte.haravan.client.HaravanClient", return_value=mock_client),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert result.exit_code == 0
        assert "0.00%" in result.output


class TestVerifyInvalidPipelineName:
    """Invalid pipeline name → exit 2, no DB query."""

    def test_slash_in_name(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        with patch("flowbyte.db.engine.get_internal_engine") as mock_eng:
            result = runner.invoke(app, ["verify", "shop/main"])

        assert result.exit_code == 2
        mock_eng.assert_not_called()

    def test_uppercase_in_name(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        with patch("flowbyte.db.engine.get_internal_engine") as mock_eng:
            result = runner.invoke(app, ["verify", "Shop_Main"])

        assert result.exit_code == 2
        mock_eng.assert_not_called()


class TestVerifyCustomTolerance:
    """--tolerance 1.0 → deviation 0.5% should pass."""

    def test_half_percent_deviation_passes_at_1_pct_tolerance(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        # orders: remote=1000, local=995 → deviation=0.5%
        mock_client = _make_haravan_client(
            {"orders": 1000, "customers": 500, "products": 200}
        )
        internal_engine = _make_internal_engine()
        dest_engine = _make_dest_engine(
            {"orders": 995, "customers": 500, "products": 200}
        )

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.db.engine.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials", return_value={"access_token": "tok", "password": "pw"}),
            patch("flowbyte.scheduler.reconciler._build_dest_url", return_value="postgresql+psycopg://u:p@localhost/d"),
            patch("flowbyte.haravan.client.HaravanClient", return_value=mock_client),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE, "--tolerance", "1.0"])

        assert result.exit_code == 0, f"Expected exit 0, got:\n{result.output}"
        assert "✓ OK" in result.output

    def test_half_percent_deviation_fails_at_default_tolerance(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        # Same 0.5% deviation but default 0.1% tolerance → fail
        mock_client = _make_haravan_client(
            {"orders": 1000, "customers": 500, "products": 200}
        )
        internal_engine = _make_internal_engine()
        dest_engine = _make_dest_engine(
            {"orders": 995, "customers": 500, "products": 200}
        )

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.db.engine.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials", return_value={"access_token": "tok", "password": "pw"}),
            patch("flowbyte.scheduler.reconciler._build_dest_url", return_value="postgresql+psycopg://u:p@localhost/d"),
            patch("flowbyte.haravan.client.HaravanClient", return_value=mock_client),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert result.exit_code == 1


class TestVerifyCredentialSafetyInErrors:
    """Security: credentials must never appear in CLI error output."""

    _SECRET_PW = "Super_Secret_Pw_12345"
    _SECRET_TOKEN = "tok_haravan_secret_xyz"

    def test_dest_db_connection_error_does_not_leak_password(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        # Use a named exception so type(e).__name__ is "OperationalError"
        class OperationalError(Exception):
            pass

        internal_engine = _make_internal_engine()

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials", return_value={
                "access_token": self._SECRET_TOKEN,
                "password": self._SECRET_PW,
            }),
            patch("flowbyte.scheduler.reconciler._build_dest_url",
                  return_value=f"postgresql+psycopg://flowbyte:{self._SECRET_PW}@localhost/db"),
            patch("flowbyte.haravan.client.HaravanClient"),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
            patch("flowbyte.db.engine.get_dest_engine",
                  side_effect=OperationalError(
                      f"postgresql+psycopg://flowbyte:{self._SECRET_PW}@localhost/db"
                  )),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert result.exit_code == 1
        assert self._SECRET_PW not in result.output, "Password must not appear in CLI output"
        assert "OperationalError" in result.output  # error type (safe) is shown

    def test_dest_db_query_error_does_not_leak_password(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        class InterfaceError(Exception):
            pass

        mock_client = _make_haravan_client({"orders": 1000, "customers": 500, "products": 200})
        internal_engine = _make_internal_engine()

        mock_dest_engine = MagicMock()
        mock_dest_conn = MagicMock()
        mock_dest_conn.__enter__ = lambda s: mock_dest_conn
        mock_dest_conn.__exit__ = MagicMock(return_value=False)
        mock_dest_conn.execute.side_effect = InterfaceError(
            f"postgresql+psycopg://flowbyte:{self._SECRET_PW}@localhost/db closed"
        )
        mock_dest_engine.connect.return_value = mock_dest_conn

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.db.engine.get_dest_engine", return_value=mock_dest_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials", return_value={
                "access_token": self._SECRET_TOKEN,
                "password": self._SECRET_PW,
            }),
            patch("flowbyte.scheduler.reconciler._build_dest_url",
                  return_value=f"postgresql+psycopg://flowbyte:{self._SECRET_PW}@localhost/db"),
            patch("flowbyte.haravan.client.HaravanClient", return_value=mock_client),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert self._SECRET_PW not in result.output, "Password must not appear in CLI output"
        assert "InterfaceError" in result.output  # error type (safe) is shown

    def test_haravan_creds_error_does_not_leak_token(self, monkeypatch):
        monkeypatch.setenv("FLOWBYTE_DB_URL", _DB_URL)

        class NoResultFound(Exception):
            pass

        internal_engine = _make_internal_engine()

        with (
            patch("flowbyte.db.engine.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.scheduler.reconciler._load_credentials",
                  side_effect=NoResultFound(f"ref={self._SECRET_TOKEN}")),
            patch("flowbyte.haravan.token_bucket.HaravanTokenBucket"),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert result.exit_code == 1
        assert self._SECRET_TOKEN not in result.output, "Token must not appear in CLI output"
        assert "NoResultFound" in result.output  # error type (safe) is shown
