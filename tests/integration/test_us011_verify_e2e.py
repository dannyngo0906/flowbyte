"""E2E tests for US-011: flowbyte verify command — count comparison.

Mô phỏng end-user flow:
  Bước 1: Seed pipeline config + credentials vào internal DB
  Bước 2: Upsert N records vào destination orders/customers/products tables
  Bước 3: Mock Haravan /count.json endpoints qua HaravanClient.get_count
  Bước cuối: flowbyte verify <pipeline> → exit 0, deviation % hiển thị đúng

Done-when (PRD US-011 AC-3.4):
  ✅ T-11.3  count orders khớp Haravan ±0.1%
  ✅ flowbyte verify exit 0 khi all resources within tolerance
  ✅ flowbyte verify exit 1 khi một resource vượt tolerance
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import text
from typer.testing import CliRunner

from flowbyte.cli.app import app
from flowbyte.db.destination_schema import destination_metadata
from flowbyte.db.internal_schema import credentials, pipelines

pytestmark = pytest.mark.integration

runner = CliRunner()

_PIPELINE = "shop_verify_e2e"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _db_url(pg_container) -> str:
    raw = pg_container.get_connection_url()
    return (
        raw.replace("postgresql+psycopg2://", "postgresql+psycopg://")
           .replace("postgresql://", "postgresql+psycopg://")
    )


_VALID_CONFIG = {
    "name": _PIPELINE,
    "haravan_credentials_ref": f"{_PIPELINE}_haravan",
    "haravan_shop_domain": "e2e.myharavan.com",
    "destination": {
        "host": "localhost",
        "port": 5432,
        "user": "flowbyte",
        "database": "flowbyte_data",
        "credentials_ref": f"{_PIPELINE}_pg",
    },
    "resources": {},
}


def _seed_pipeline(engine, master_key, encryptor) -> None:
    """Insert pipeline + credentials into internal DB."""
    from flowbyte.db.internal_schema import credentials, pipelines
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    haravan_plain = json.dumps(
        {"shop_domain": "e2e.myharavan.com", "access_token": "tok_e2e_test"}
    )
    haravan_ct = encryptor.encrypt(
        haravan_plain, associated_data=f"{_PIPELINE}_haravan".encode()
    )
    pg_plain = json.dumps({"password": "secret"})
    pg_ct = encryptor.encrypt(pg_plain, associated_data=f"{_PIPELINE}_pg".encode())

    with engine.begin() as conn:
        conn.execute(
            pg_insert(credentials).values(
                ref=f"{_PIPELINE}_haravan", kind="haravan", ciphertext=haravan_ct.serialize()
            ).on_conflict_do_nothing()
        )
        conn.execute(
            pg_insert(credentials).values(
                ref=f"{_PIPELINE}_pg", kind="postgres", ciphertext=pg_ct.serialize()
            ).on_conflict_do_nothing()
        )
        conn.execute(
            pg_insert(pipelines)
            .values(
                name=_PIPELINE,
                yaml_content="",
                config_json=_VALID_CONFIG,
                enabled=True,
            )
            .on_conflict_do_update(
                index_elements=["name"],
                set_={"config_json": _VALID_CONFIG, "enabled": True},
            )
        )


def _seed_destination_records(dest_engine, resource: str, count: int) -> None:
    """Insert N active records into the destination table."""
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(1, count + 1):
        rows.append(
            {
                "id": i,
                "updated_at": now,
                "_raw": json.dumps({"id": i}),
                "_synced_at": now,
                "_deleted_at": None,
                "_sync_id": str(uuid.uuid4()),
            }
        )
    with dest_engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {resource}"))
        if rows:
            conn.execute(
                text(
                    f"INSERT INTO {resource} (id, updated_at, _raw, _synced_at, _deleted_at, _sync_id) "
                    f"VALUES (:id, :updated_at, :_raw::jsonb, :_synced_at, :_deleted_at, :_sync_id::uuid)"
                ),
                rows,
            )


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def setup_destination_schema(dest_engine):
    """Ensure destination tables exist."""
    destination_metadata.create_all(dest_engine)
    yield
    # Clean up after each test
    with dest_engine.begin() as conn:
        for tbl in ("orders", "customers", "products"):
            conn.execute(text(f"DELETE FROM {tbl}"))


@pytest.fixture(autouse=True)
def clean_internal(internal_engine):
    yield
    with internal_engine.begin() as conn:
        conn.execute(text(f"DELETE FROM pipelines WHERE name = '{_PIPELINE}'"))
        conn.execute(
            text(f"DELETE FROM credentials WHERE ref LIKE '{_PIPELINE}_%'")
        )


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestVerifyE2EPass:
    """flowbyte verify passes when local counts match Haravan within tolerance."""

    def test_all_resources_within_tolerance(
        self, internal_engine, dest_engine, master_key, encryptor, tmp_path
    ):
        _seed_pipeline(internal_engine, master_key, encryptor)
        _seed_destination_records(dest_engine, "orders", 1000)
        _seed_destination_records(dest_engine, "customers", 500)
        _seed_destination_records(dest_engine, "products", 200)

        db_url = str(internal_engine.url)
        dest_url = str(dest_engine.url)

        mock_client = _build_mock_client(
            counts={"orders": 1000, "customers": 500, "products": 200}
        )

        with (
            patch("flowbyte.cli.commands.verify.HaravanClient", return_value=mock_client),
            patch("flowbyte.cli.commands.verify.HaravanTokenBucket"),
            patch("flowbyte.cli.commands.verify.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.cli.commands.verify.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.cli.commands.verify._build_dest_url", return_value=dest_url),
            patch("flowbyte.security.master_key.MasterKey.load", return_value=master_key),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert result.exit_code == 0, f"Expected exit 0, got:\n{result.output}"
        assert "✓ OK" in result.output
        assert "orders" in result.output
        assert "customers" in result.output
        assert "products" in result.output


class TestVerifyE2EFail:
    """flowbyte verify fails when one resource exceeds tolerance."""

    def test_orders_deviation_exceeds_threshold(
        self, internal_engine, dest_engine, master_key, encryptor, tmp_path
    ):
        _seed_pipeline(internal_engine, master_key, encryptor)
        # Local: 900 orders, Haravan says 1000 → 10% deviation > 0.1%
        _seed_destination_records(dest_engine, "orders", 900)
        _seed_destination_records(dest_engine, "customers", 500)
        _seed_destination_records(dest_engine, "products", 200)

        dest_url = str(dest_engine.url)

        mock_client = _build_mock_client(
            counts={"orders": 1000, "customers": 500, "products": 200}
        )

        with (
            patch("flowbyte.cli.commands.verify.HaravanClient", return_value=mock_client),
            patch("flowbyte.cli.commands.verify.HaravanTokenBucket"),
            patch("flowbyte.cli.commands.verify.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.cli.commands.verify.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.cli.commands.verify._build_dest_url", return_value=dest_url),
            patch("flowbyte.security.master_key.MasterKey.load", return_value=master_key),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE])

        assert result.exit_code == 1, f"Expected exit 1, got:\n{result.output}"
        assert "FAIL" in result.output


class TestVerifyE2ESingleResource:
    """--resource orders checks only orders."""

    def test_single_resource_orders(
        self, internal_engine, dest_engine, master_key, encryptor, tmp_path
    ):
        _seed_pipeline(internal_engine, master_key, encryptor)
        _seed_destination_records(dest_engine, "orders", 1000)

        dest_url = str(dest_engine.url)

        mock_client = _build_mock_client(counts={"orders": 1000})

        with (
            patch("flowbyte.cli.commands.verify.HaravanClient", return_value=mock_client),
            patch("flowbyte.cli.commands.verify.HaravanTokenBucket"),
            patch("flowbyte.cli.commands.verify.get_internal_engine", return_value=internal_engine),
            patch("flowbyte.cli.commands.verify.get_dest_engine", return_value=dest_engine),
            patch("flowbyte.cli.commands.verify._build_dest_url", return_value=dest_url),
            patch("flowbyte.security.master_key.MasterKey.load", return_value=master_key),
        ):
            result = runner.invoke(app, ["verify", _PIPELINE, "--resource", "orders"])

        assert result.exit_code == 0, f"Expected exit 0, got:\n{result.output}"
        # get_count called only once for "orders"
        mock_client.get_count.assert_called_once_with("orders")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _build_mock_client(counts: dict[str, int]):
    mock = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
    mock.get_count.side_effect = lambda res, params=None: counts.get(res, 0)
    return mock
