"""Integration tests for DB schema — verifies migration creates all expected
tables, indexes, and the sync_rollup_daily materialized view.

All tests use real Postgres via testcontainers (no mocking).
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).parent.parent.parent


# ── Schema introspection helpers ──────────────────────────────────────────────

def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = :t"
        ),
        {"t": table_name},
    ).fetchone()
    return row is not None


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    # pg_attribute covers both tables and materialized views;
    # information_schema.columns omits matview columns in PostgreSQL.
    row = conn.execute(
        text(
            "SELECT 1 FROM pg_catalog.pg_attribute a "
            "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' "
            "AND c.relname = :t AND a.attname = :c "
            "AND a.attnum > 0 AND NOT a.attisdropped"
        ),
        {"t": table_name, "c": column_name},
    ).fetchone()
    return row is not None


def _column_data_type(conn, table_name: str, column_name: str) -> str | None:
    row = conn.execute(
        text(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' "
            "AND table_name = :t AND column_name = :c"
        ),
        {"t": table_name, "c": column_name},
    ).fetchone()
    return row[0] if row else None


def _index_exists(conn, index_name: str) -> bool:
    row = conn.execute(
        text(
            "SELECT 1 FROM pg_catalog.pg_indexes "
            "WHERE schemaname = 'public' AND indexname = :i"
        ),
        {"i": index_name},
    ).fetchone()
    return row is not None


def _index_is_partial(conn, index_name: str) -> bool:
    row = conn.execute(
        text(
            "SELECT indexdef FROM pg_catalog.pg_indexes "
            "WHERE schemaname = 'public' AND indexname = :i"
        ),
        {"i": index_name},
    ).fetchone()
    if row is None:
        return False
    return "WHERE" in row[0].upper()


def _matview_exists(conn, view_name: str) -> bool:
    row = conn.execute(
        text(
            "SELECT 1 FROM pg_catalog.pg_matviews "
            "WHERE schemaname = 'public' AND matviewname = :v"
        ),
        {"v": view_name},
    ).fetchone()
    return row is not None


# ── Internal schema tables ─────────────────────────────────────────────────────

class TestInternalSchemaTables:
    EXPECTED_TABLES = [
        "pipelines",
        "credentials",
        "sync_requests",
        "sync_logs",
        "sync_checkpoints",
        "sync_runs",
        "validation_results",
        "scheduler_heartbeat",
        "deployment_events",
        "backup_events",
        "master_key_metadata",
    ]

    @pytest.mark.parametrize("table_name", EXPECTED_TABLES)
    def test_table_exists(self, pg_conn, table_name):
        assert _table_exists(pg_conn, table_name), (
            f"Internal table '{table_name}' missing after alembic upgrade head"
        )


# ── Column structure validation ────────────────────────────────────────────────

class TestInternalSchemaColumns:
    def test_sync_checkpoints_last_id_bigint(self, pg_conn):
        # §17.5 composite cursor tie-breaker requires BigInteger
        assert _column_exists(pg_conn, "sync_checkpoints", "last_id")
        assert _column_data_type(pg_conn, "sync_checkpoints", "last_id") == "bigint"

    def test_sync_requests_recovery_count_smallint(self, pg_conn):
        # §17.4 recovery tracking
        assert _column_exists(pg_conn, "sync_requests", "recovery_count")
        assert _column_data_type(pg_conn, "sync_requests", "recovery_count") == "smallint"

    def test_sync_requests_wait_timeout_at_timestamptz(self, pg_conn):
        assert _column_exists(pg_conn, "sync_requests", "wait_timeout_at")
        assert (
            _column_data_type(pg_conn, "sync_requests", "wait_timeout_at")
            == "timestamp with time zone"
        )

    @pytest.mark.parametrize("col", [
        "fetched_count",
        "upserted_count",
        "skipped_invalid",
        "soft_deleted_count",
        "rows_before",
        "rows_after",
        "duration_seconds",
    ])
    def test_sync_runs_metric_columns_exist(self, pg_conn, col):
        assert _column_exists(pg_conn, "sync_runs", col), (
            f"sync_runs missing metric column '{col}'"
        )

    def test_credentials_ciphertext_is_text(self, pg_conn):
        # Encryptor.serialize() returns base64 string → stored as TEXT, not BYTEA
        assert _column_data_type(pg_conn, "credentials", "ciphertext") == "text"


# ── Index verification ─────────────────────────────────────────────────────────

class TestIndexes:
    def test_idx_sync_requests_pending_is_partial(self, pg_conn):
        assert _index_exists(pg_conn, "idx_sync_requests_pending")
        assert _index_is_partial(pg_conn, "idx_sync_requests_pending")

    def test_idx_sync_logs_errors_is_partial(self, pg_conn):
        assert _index_exists(pg_conn, "idx_sync_logs_errors")
        assert _index_is_partial(pg_conn, "idx_sync_logs_errors")

    def test_idx_sync_runs_status_is_partial(self, pg_conn):
        assert _index_exists(pg_conn, "idx_sync_runs_status")
        assert _index_is_partial(pg_conn, "idx_sync_runs_status")


# ── Materialized view ──────────────────────────────────────────────────────────

class TestMaterializedView:
    EXPECTED_COLUMNS = [
        "day",
        "pipeline",
        "resource",
        "mode",
        "success_count",
        "failure_count",
        "avg_duration_s",
        "p95_duration_s",
        "total_fetched",
        "total_upserted",
    ]

    def test_sync_rollup_daily_exists(self, pg_conn):
        assert _matview_exists(pg_conn, "sync_rollup_daily"), (
            "Materialized view 'sync_rollup_daily' not found — "
            "check that SYNC_ROLLUP_DAILY_VIEW_SQL is executed in upgrade()"
        )

    @pytest.mark.parametrize("col", EXPECTED_COLUMNS)
    def test_sync_rollup_daily_has_column(self, pg_conn, col):
        # PostgreSQL exposes materialized view columns in information_schema.columns
        assert _column_exists(pg_conn, "sync_rollup_daily", col), (
            f"sync_rollup_daily missing column '{col}'"
        )

    def test_sync_rollup_daily_unique_index_exists(self, pg_conn):
        assert _index_exists(pg_conn, "idx_sync_rollup_daily_pk")


# ── Destination tables ─────────────────────────────────────────────────────────

class TestDestinationTables:
    EXPECTED_TABLES = [
        "orders",
        "order_line_items",
        "customers",
        "products",
        "variants",
        "inventory_levels",
        "locations",
    ]
    # Tables using _meta_columns() which includes _deleted_at
    SOFT_DELETE_TABLES = ["orders", "customers", "products", "locations"]
    # Tables with explicit columns only — no _deleted_at
    NO_SOFT_DELETE_TABLES = ["order_line_items", "variants", "inventory_levels"]

    @pytest.mark.parametrize("table_name", EXPECTED_TABLES)
    def test_destination_table_exists(self, dest_conn, table_name):
        assert _table_exists(dest_conn, table_name)

    @pytest.mark.parametrize("table_name", EXPECTED_TABLES)
    def test_destination_table_has_raw_column(self, dest_conn, table_name):
        assert _column_exists(dest_conn, table_name, "_raw")

    @pytest.mark.parametrize("table_name", EXPECTED_TABLES)
    def test_destination_table_has_synced_at(self, dest_conn, table_name):
        assert _column_exists(dest_conn, table_name, "_synced_at")

    @pytest.mark.parametrize("table_name", EXPECTED_TABLES)
    def test_destination_table_has_sync_id(self, dest_conn, table_name):
        assert _column_exists(dest_conn, table_name, "_sync_id")

    @pytest.mark.parametrize("table_name", SOFT_DELETE_TABLES)
    def test_soft_delete_table_has_deleted_at(self, dest_conn, table_name):
        assert _column_exists(dest_conn, table_name, "_deleted_at"), (
            f"Soft-delete table '{table_name}' missing '_deleted_at'"
        )

    @pytest.mark.parametrize("table_name", NO_SOFT_DELETE_TABLES)
    def test_non_soft_delete_table_lacks_deleted_at(self, dest_conn, table_name):
        assert not _column_exists(dest_conn, table_name, "_deleted_at"), (
            f"Table '{table_name}' should NOT have '_deleted_at' per destination_schema.py"
        )

    def test_idx_orders_sync_id_active_is_partial(self, dest_conn):
        # §17.1 partial index for weekly full refresh sweep
        assert _index_exists(dest_conn, "idx_orders_sync_id_active")
        assert _index_is_partial(dest_conn, "idx_orders_sync_id_active")


# ── Migration roundtrip ────────────────────────────────────────────────────────

class TestMigrationRoundtrip:
    """Verifies upgrade() and downgrade() are complete and correct.

    Uses a separate DB on the running container to avoid interfering
    with the session-scoped internal_engine fixture.

    Tests must run sequentially (pytest default). If pytest-xdist is
    introduced, mark these @pytest.mark.serial.
    """

    @pytest.fixture(scope="class")
    def roundtrip_db_url(self, pg_container):
        base_url = pg_container.get_connection_url()
        sa_base = (base_url
                   .replace("postgresql+psycopg2://", "postgresql+psycopg://")
                   .replace("postgresql://", "postgresql+psycopg://"))

        rt_db = "flowbyte_roundtrip"
        admin_engine = create_engine(sa_base)
        with admin_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS {rt_db}"))
            conn.execute(text(f"CREATE DATABASE {rt_db}"))
        admin_engine.dispose()

        rt_url = sa_base.rsplit("/", 1)[0] + f"/{rt_db}"

        rt_engine = create_engine(rt_url)
        with rt_engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        rt_engine.dispose()

        yield rt_url

    def _alembic(self, command: str, db_url: str) -> None:
        alembic_bin = str(Path(sys.executable).parent / "alembic")
        env = {**os.environ, "FLOWBYTE_DB_URL": db_url}
        subprocess.run(
            [alembic_bin, *command.split()],
            env=env,
            check=True,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )

    def _check_tables(self, db_url: str, tables: list[str]) -> dict[str, bool]:
        engine = create_engine(db_url)
        result = {}
        with engine.connect() as conn:
            for t in tables:
                result[t] = _table_exists(conn, t)
        engine.dispose()
        return result

    def _check_view(self, db_url: str) -> bool:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            exists = _matview_exists(conn, "sync_rollup_daily")
        engine.dispose()
        return exists

    ALL_INTERNAL = [
        "pipelines", "credentials", "sync_requests", "sync_logs",
        "sync_checkpoints", "sync_runs", "validation_results",
        "scheduler_heartbeat", "deployment_events", "backup_events",
        "master_key_metadata",
    ]

    def test_upgrade_creates_all_tables_and_view(self, roundtrip_db_url):
        self._alembic("upgrade head", roundtrip_db_url)
        existence = self._check_tables(roundtrip_db_url, self.ALL_INTERNAL)
        missing = [t for t, ok in existence.items() if not ok]
        assert not missing, f"Tables missing after upgrade head: {missing}"
        assert self._check_view(roundtrip_db_url), (
            "sync_rollup_daily view missing after upgrade head"
        )

    def test_downgrade_drops_all(self, roundtrip_db_url):
        self._alembic("downgrade base", roundtrip_db_url)
        existence = self._check_tables(roundtrip_db_url, self.ALL_INTERNAL)
        remaining = [t for t, ok in existence.items() if ok]
        assert not remaining, f"Tables still present after downgrade base: {remaining}"
        assert not self._check_view(roundtrip_db_url), (
            "sync_rollup_daily still exists after downgrade base"
        )

    def test_re_upgrade_restores_schema(self, roundtrip_db_url):
        self._alembic("upgrade head", roundtrip_db_url)
        existence = self._check_tables(roundtrip_db_url, self.ALL_INTERNAL)
        missing = [t for t, ok in existence.items() if not ok]
        assert not missing, f"Tables missing after second upgrade head: {missing}"
        assert self._check_view(roundtrip_db_url), (
            "sync_rollup_daily missing after second upgrade head"
        )
