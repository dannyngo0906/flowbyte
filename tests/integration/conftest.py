"""Integration test fixtures — real Postgres via testcontainers."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from flowbyte.db.internal_schema import internal_metadata
from flowbyte.db.destination_schema import destination_metadata

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "haravan"


@pytest.fixture(scope="session")
def pg_container():
    with PostgresContainer("postgres:14-alpine") as pg:
        # Enable pgcrypto for gen_random_uuid()
        engine = create_engine(pg.get_connection_url())
        with engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        yield pg


@pytest.fixture(scope="session")
def internal_engine(pg_container):
    """Session-scoped engine with schema migrated to head."""
    import subprocess
    import os

    url = pg_container.get_connection_url()
    # Replace psycopg2:// with psycopg+psycopg:// for SA 2.0
    sa_url = url.replace("postgresql://", "postgresql+psycopg://")
    env = {**os.environ, "FLOWBYTE_DB_URL": sa_url}

    subprocess.run(
        ["alembic", "upgrade", "head"],
        env=env,
        check=True,
        cwd=Path(__file__).parent.parent.parent,
    )
    return create_engine(sa_url)


@pytest.fixture(scope="session")
def dest_engine(pg_container):
    """Separate engine for destination DB (same PG instance, different schema)."""
    url = pg_container.get_connection_url()
    sa_url = url.replace("postgresql://", "postgresql+psycopg://")
    engine = create_engine(sa_url)

    # Create destination tables
    destination_metadata.create_all(engine)
    return engine


@pytest.fixture
def pg_conn(internal_engine):
    """Transaction-per-test: BEGIN → yield → ROLLBACK."""
    with internal_engine.connect() as conn:
        trans = conn.begin()
        yield conn
        trans.rollback()


@pytest.fixture
def dest_conn(dest_engine):
    with dest_engine.connect() as conn:
        trans = conn.begin()
        yield conn
        trans.rollback()


@pytest.fixture
def master_key(tmp_path):
    from flowbyte.security.master_key import MasterKey

    path = tmp_path / "master.key"
    return MasterKey.generate_and_save(path)


@pytest.fixture
def encryptor(master_key):
    from flowbyte.security.encryption import Encryptor

    return Encryptor(master_key.raw)


@pytest.fixture
def fake_orders():
    path = FIXTURES_DIR / "orders_sample.json"
    if path.exists():
        return json.loads(path.read_text())
    # Minimal inline fixture for CI
    return [
        {
            "id": i,
            "order_number": 1000 + i,
            "email": f"test{i}@example.com",
            "total_price": "100.00",
            "financial_status": "paid",
            "customer_id": 9000 + i,
            "customer": {"email": f"test{i}@example.com", "phone": ""},
            "shipping_address": {"city": "HCM", "province": "HCM", "country_code": "VN", "phone": ""},
            "created_at": "2026-01-01T00:00:00+07:00",
            "updated_at": f"2026-04-{i+1:02d}T10:00:00+07:00",
            "line_items": [],
        }
        for i in range(1, 11)
    ]
