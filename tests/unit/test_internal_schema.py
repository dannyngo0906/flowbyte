"""Unit tests for internal_schema.py — Table definitions and SYNC_ROLLUP_DAILY_VIEW_SQL.

Pure-Python structural tests — no DB connection needed.
Verify that every SQLAlchemy Table object matches the TDD spec so that
mistakes are caught at import-time, before any migration runs.

Coverage:
  - internal_metadata contains exactly the 11 expected tables
  - Column Python types, nullability, primary-key membership
  - Check constraints are defined with correct SQL text
  - Partial indexes carry the correct WHERE clause
  - SYNC_ROLLUP_DAILY_VIEW_SQL is well-formed and idempotent
"""
from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

from flowbyte.db.internal_schema import (
    SYNC_ROLLUP_DAILY_VIEW_SQL,
    backup_events,
    credentials,
    deployment_events,
    internal_metadata,
    master_key_metadata,
    pipelines,
    scheduler_heartbeat,
    sync_checkpoints,
    sync_logs,
    sync_requests,
    sync_runs,
    validation_results,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _col(table: sa.Table, name: str) -> sa.Column:
    return table.c[name]


def _check_texts(table: sa.Table) -> list[str]:
    return [
        str(c.sqltext)
        for c in table.constraints
        if isinstance(c, sa.CheckConstraint)
    ]


def _index(table: sa.Table, name: str) -> sa.Index | None:
    return next((i for i in table.indexes if i.name == name), None)


def _partial_where(table: sa.Table, index_name: str) -> str | None:
    idx = _index(table, index_name)
    if idx is None:
        return None
    return idx.dialect_kwargs.get("postgresql_where")


# ── metadata registry ─────────────────────────────────────────────────────────

class TestMetadataRegistry:
    EXPECTED = {
        "pipelines", "credentials", "sync_requests", "sync_logs",
        "sync_checkpoints", "sync_runs", "validation_results",
        "scheduler_heartbeat", "deployment_events", "backup_events",
        "master_key_metadata",
    }

    def test_all_expected_tables_registered(self):
        names = set(internal_metadata.tables)
        assert self.EXPECTED.issubset(names)

    def test_no_extra_tables(self):
        names = set(internal_metadata.tables)
        assert names == self.EXPECTED

    def test_table_objects_are_sa_table(self):
        for name in self.EXPECTED:
            assert isinstance(internal_metadata.tables[name], sa.Table)


# ── pipelines ─────────────────────────────────────────────────────────────────

class TestPipelinesTable:
    def test_name_is_primary_key(self):
        assert _col(pipelines, "name").primary_key

    def test_name_max_length(self):
        assert _col(pipelines, "name").type.length == 32

    def test_yaml_content_is_text_not_nullable(self):
        col = _col(pipelines, "yaml_content")
        assert isinstance(col.type, sa.Text)
        assert not col.nullable

    def test_config_json_is_jsonb(self):
        assert isinstance(_col(pipelines, "config_json").type, JSONB)

    def test_enabled_is_boolean_not_nullable(self):
        col = _col(pipelines, "enabled")
        assert isinstance(col.type, sa.Boolean)
        assert not col.nullable

    def test_enabled_has_server_default(self):
        assert _col(pipelines, "enabled").server_default is not None

    def test_timestamps_are_timestamptz_not_nullable(self):
        for name in ("created_at", "updated_at"):
            col = _col(pipelines, name)
            assert isinstance(col.type, sa.DateTime)
            assert col.type.timezone is True
            assert not col.nullable
            assert col.server_default is not None


# ── credentials ───────────────────────────────────────────────────────────────

class TestCredentialsTable:
    def test_ref_is_primary_key(self):
        assert _col(credentials, "ref").primary_key

    def test_ref_max_length(self):
        assert _col(credentials, "ref").type.length == 64

    def test_ciphertext_is_text_not_nullable(self):
        # §17.4: Encryptor.serialize() returns base64 str → TEXT, not BYTEA
        col = _col(credentials, "ciphertext")
        assert isinstance(col.type, sa.Text)
        assert not col.nullable

    def test_kind_is_string_not_nullable(self):
        col = _col(credentials, "kind")
        assert isinstance(col.type, sa.String)
        assert not col.nullable

    def test_kind_check_constraint_defined(self):
        texts = _check_texts(credentials)
        assert any("haravan" in t and "postgres" in t for t in texts), (
            "credentials_kind_check constraint missing"
        )

    def test_timestamps_have_server_defaults(self):
        for name in ("created_at", "updated_at"):
            assert _col(credentials, name).server_default is not None


# ── sync_requests ─────────────────────────────────────────────────────────────

class TestSyncRequestsTable:
    def test_id_is_uuid_primary_key(self):
        col = _col(sync_requests, "id")
        assert isinstance(col.type, UUID)
        assert col.primary_key

    def test_id_has_server_default(self):
        assert _col(sync_requests, "id").server_default is not None

    def test_pipeline_not_nullable(self):
        assert not _col(sync_requests, "pipeline").nullable

    def test_resource_is_nullable(self):
        # NULL = "sync all resources"
        assert _col(sync_requests, "resource").nullable

    def test_mode_not_nullable_has_default(self):
        col = _col(sync_requests, "mode")
        assert not col.nullable
        assert col.server_default is not None

    def test_status_not_nullable_has_default(self):
        col = _col(sync_requests, "status")
        assert not col.nullable
        assert col.server_default is not None

    def test_recovery_count_is_smallinteger(self):
        # §17.4: SmallInteger, not Integer — low max value deliberate
        col = _col(sync_requests, "recovery_count")
        assert isinstance(col.type, sa.SmallInteger)

    def test_recovery_count_not_nullable_with_default(self):
        col = _col(sync_requests, "recovery_count")
        assert not col.nullable
        assert col.server_default is not None

    def test_wait_timeout_at_is_nullable_timestamptz(self):
        # Optional field — NULL = no timeout configured
        col = _col(sync_requests, "wait_timeout_at")
        assert col.nullable
        assert isinstance(col.type, sa.DateTime)
        assert col.type.timezone is True

    def test_claimed_at_and_finished_at_nullable(self):
        for name in ("claimed_at", "finished_at"):
            assert _col(sync_requests, name).nullable

    def test_mode_check_constraint(self):
        texts = _check_texts(sync_requests)
        assert any("incremental" in t and "full_refresh" in t for t in texts)

    def test_status_check_constraint_all_6_values(self):
        texts = _check_texts(sync_requests)
        expected = {"pending", "claimed", "running", "done", "failed", "cancelled"}
        assert any(all(v in t for v in expected) for t in texts)

    def test_pending_partial_index_defined(self):
        idx = _index(sync_requests, "idx_sync_requests_pending")
        assert idx is not None

    def test_pending_partial_index_has_where_clause(self):
        where = _partial_where(sync_requests, "idx_sync_requests_pending")
        assert where is not None
        assert "pending" in where


# ── sync_logs ─────────────────────────────────────────────────────────────────

class TestSyncLogsTable:
    def test_id_is_uuid_primary_key_with_default(self):
        col = _col(sync_logs, "id")
        assert isinstance(col.type, UUID)
        assert col.primary_key
        assert col.server_default is not None

    def test_level_not_nullable(self):
        assert not _col(sync_logs, "level").nullable

    def test_event_not_nullable(self):
        assert not _col(sync_logs, "event").nullable

    def test_sync_id_pipeline_resource_nullable(self):
        # Logs can exist without a pipeline context (daemon-level events)
        for name in ("sync_id", "pipeline", "resource"):
            assert _col(sync_logs, name).nullable, f"{name} should be nullable"

    def test_payload_is_jsonb(self):
        assert isinstance(_col(sync_logs, "payload").type, JSONB)

    def test_level_check_constraint(self):
        texts = _check_texts(sync_logs)
        expected_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        assert any(all(lv in t for lv in expected_levels) for t in texts)

    def test_errors_partial_index_defined(self):
        idx = _index(sync_logs, "idx_sync_logs_errors")
        assert idx is not None

    def test_errors_partial_index_filters_errors_only(self):
        where = _partial_where(sync_logs, "idx_sync_logs_errors")
        assert where is not None
        assert "ERROR" in where
        assert "CRITICAL" in where

    def test_pipeline_ts_index_defined(self):
        assert _index(sync_logs, "idx_sync_logs_pipeline_ts") is not None

    def test_sync_id_index_defined(self):
        assert _index(sync_logs, "idx_sync_logs_sync_id") is not None


# ── sync_checkpoints ──────────────────────────────────────────────────────────

class TestSyncCheckpointsTable:
    def test_composite_primary_key(self):
        # Both pipeline AND resource form the composite PK
        assert _col(sync_checkpoints, "pipeline").primary_key
        assert _col(sync_checkpoints, "resource").primary_key

    def test_last_id_is_biginteger(self):
        # §17.5: composite cursor tie-breaker — must be BigInteger, not Integer
        assert isinstance(_col(sync_checkpoints, "last_id").type, sa.BigInteger)

    def test_last_id_is_nullable(self):
        # NULL on first sync (no checkpoint yet)
        assert _col(sync_checkpoints, "last_id").nullable

    def test_last_updated_at_is_nullable_timestamptz(self):
        col = _col(sync_checkpoints, "last_updated_at")
        assert col.nullable
        assert isinstance(col.type, sa.DateTime)
        assert col.type.timezone is True

    def test_last_sync_id_is_uuid_nullable(self):
        col = _col(sync_checkpoints, "last_sync_id")
        assert isinstance(col.type, UUID)
        assert col.nullable

    def test_last_status_is_nullable(self):
        assert _col(sync_checkpoints, "last_status").nullable


# ── sync_runs ─────────────────────────────────────────────────────────────────

class TestSyncRunsTable:
    def test_sync_id_is_uuid_primary_key(self):
        col = _col(sync_runs, "sync_id")
        assert isinstance(col.type, UUID)
        assert col.primary_key

    def test_pipeline_resource_mode_trigger_not_nullable(self):
        for name in ("pipeline", "resource", "mode", "trigger"):
            assert not _col(sync_runs, name).nullable, f"{name} should be NOT NULL"

    def test_started_at_not_nullable(self):
        assert not _col(sync_runs, "started_at").nullable

    def test_finished_at_nullable(self):
        assert _col(sync_runs, "finished_at").nullable

    def test_status_not_nullable_with_running_default(self):
        col = _col(sync_runs, "status")
        assert not col.nullable
        assert col.server_default is not None

    @pytest.mark.parametrize("col_name", [
        "fetched_count", "upserted_count", "skipped_invalid",
        "soft_deleted_count", "rows_before", "rows_after",
    ])
    def test_metric_integer_columns_are_nullable(self, col_name):
        # Nullable: not yet set until sync completes
        col = _col(sync_runs, col_name)
        assert isinstance(col.type, sa.Integer)
        assert col.nullable

    def test_duration_seconds_is_numeric_not_integer(self):
        # Sub-second precision required
        col = _col(sync_runs, "duration_seconds")
        assert isinstance(col.type, sa.Numeric)
        assert col.nullable

    def test_partial_index_for_failed_cancelled(self):
        idx = _index(sync_runs, "idx_sync_runs_status")
        assert idx is not None
        where = idx.dialect_kwargs.get("postgresql_where", "")
        assert "failed" in where
        assert "cancelled" in where

    def test_recent_index_defined(self):
        assert _index(sync_runs, "idx_sync_runs_recent") is not None


# ── validation_results ────────────────────────────────────────────────────────

class TestValidationResultsTable:
    def test_id_is_uuid_primary_key_with_default(self):
        col = _col(validation_results, "id")
        assert isinstance(col.type, UUID)
        assert col.primary_key
        assert col.server_default is not None

    def test_sync_id_not_nullable(self):
        assert not _col(validation_results, "sync_id").nullable

    def test_pipeline_resource_rule_not_nullable(self):
        for name in ("pipeline", "resource", "rule"):
            assert not _col(validation_results, name).nullable

    def test_status_not_nullable(self):
        assert not _col(validation_results, "status").nullable

    def test_status_check_constraint_4_values(self):
        texts = _check_texts(validation_results)
        for value in ("ok", "warning", "failed", "skipped"):
            assert any(value in t for t in texts), f"'{value}' missing from status CHECK"

    def test_details_is_jsonb_nullable(self):
        col = _col(validation_results, "details")
        assert isinstance(col.type, JSONB)
        assert col.nullable

    def test_sync_id_index_defined(self):
        assert _index(validation_results, "idx_validation_sync") is not None


# ── scheduler_heartbeat ───────────────────────────────────────────────────────

class TestSchedulerHeartbeatTable:
    def test_id_is_integer_primary_key(self):
        col = _col(scheduler_heartbeat, "id")
        assert isinstance(col.type, sa.Integer)
        assert col.primary_key

    def test_singleton_check_constraint(self):
        # §17.9: only id=1 allowed, prevents second daemon row
        texts = _check_texts(scheduler_heartbeat)
        assert any("id = 1" in t or "id=1" in t for t in texts)

    def test_last_beat_not_nullable_with_default(self):
        col = _col(scheduler_heartbeat, "last_beat")
        assert not col.nullable
        assert col.server_default is not None

    def test_daemon_started_at_not_nullable_no_default(self):
        # Explicitly set on daemon boot, never auto-filled
        col = _col(scheduler_heartbeat, "daemon_started_at")
        assert not col.nullable
        assert col.server_default is None

    def test_version_not_nullable(self):
        assert not _col(scheduler_heartbeat, "version").nullable


# ── deployment_events ─────────────────────────────────────────────────────────

class TestDeploymentEventsTable:
    def test_id_autoincrement_primary_key(self):
        col = _col(deployment_events, "id")
        assert isinstance(col.type, sa.Integer)
        assert col.primary_key
        assert col.autoincrement is True

    def test_event_not_nullable(self):
        assert not _col(deployment_events, "event").nullable

    def test_version_alembic_cols_nullable(self):
        for name in ("version", "alembic_from", "alembic_to"):
            assert _col(deployment_events, name).nullable

    def test_details_is_jsonb_nullable(self):
        col = _col(deployment_events, "details")
        assert isinstance(col.type, JSONB)
        assert col.nullable

    def test_at_not_nullable_with_default(self):
        col = _col(deployment_events, "at")
        assert not col.nullable
        assert col.server_default is not None


# ── backup_events ─────────────────────────────────────────────────────────────

class TestBackupEventsTable:
    def test_id_autoincrement_primary_key(self):
        col = _col(backup_events, "id")
        assert isinstance(col.type, sa.Integer)
        assert col.primary_key
        assert col.autoincrement is True

    def test_started_at_not_nullable_with_default(self):
        col = _col(backup_events, "started_at")
        assert not col.nullable
        assert col.server_default is not None

    def test_finished_at_nullable(self):
        assert _col(backup_events, "finished_at").nullable

    def test_status_not_nullable(self):
        assert not _col(backup_events, "status").nullable

    def test_size_bytes_is_biginteger(self):
        # Backup files can exceed 2GB — Integer would overflow
        assert isinstance(_col(backup_events, "size_bytes").type, sa.BigInteger)

    def test_sha256_is_string_64(self):
        col = _col(backup_events, "sha256")
        assert isinstance(col.type, sa.String)
        assert col.type.length == 64


# ── master_key_metadata ───────────────────────────────────────────────────────

class TestMasterKeyMetadataTable:
    def test_id_is_integer_primary_key(self):
        col = _col(master_key_metadata, "id")
        assert isinstance(col.type, sa.Integer)
        assert col.primary_key

    def test_singleton_check_constraint(self):
        texts = _check_texts(master_key_metadata)
        assert any("id = 1" in t or "id=1" in t for t in texts)

    def test_fingerprint_sha256_not_nullable_string_64(self):
        col = _col(master_key_metadata, "fingerprint_sha256")
        assert not col.nullable
        assert isinstance(col.type, sa.String)
        assert col.type.length == 64

    def test_bootstrapped_at_not_nullable_timestamptz(self):
        col = _col(master_key_metadata, "bootstrapped_at")
        assert not col.nullable
        assert isinstance(col.type, sa.DateTime)
        assert col.type.timezone is True

    def test_user_confirmed_backup_boolean_default_false(self):
        col = _col(master_key_metadata, "user_confirmed_backup")
        assert isinstance(col.type, sa.Boolean)
        assert not col.nullable
        assert col.server_default is not None

    def test_last_backup_reminder_at_nullable(self):
        assert _col(master_key_metadata, "last_backup_reminder_at").nullable


# ── SYNC_ROLLUP_DAILY_VIEW_SQL ────────────────────────────────────────────────

class TestSyncRollupViewSQL:
    SQL = SYNC_ROLLUP_DAILY_VIEW_SQL

    def test_is_non_empty_string(self):
        assert isinstance(self.SQL, str) and len(self.SQL) > 0

    def test_idempotent_create(self):
        # Must not fail if view already exists
        assert "IF NOT EXISTS" in self.SQL

    def test_view_name(self):
        assert "sync_rollup_daily" in self.SQL

    def test_uses_asia_ho_chi_minh_timezone(self):
        # §17.11: TDD requires Asia/Ho_Chi_Minh, NOT UTC
        assert "Asia/Ho_Chi_Minh" in self.SQL
        assert "UTC" not in self.SQL

    def test_does_not_load_data_on_create(self):
        # WITH NO DATA: view is empty until first REFRESH
        assert "WITH NO DATA" in self.SQL

    def test_filters_schedule_trigger_only(self):
        # §17.11: manual syncs excluded from rollup metrics
        assert "trigger = 'schedule'" in self.SQL

    def test_requires_finished_at(self):
        # In-progress syncs (NULL finished_at) must be excluded
        assert "finished_at IS NOT NULL" in self.SQL

    def test_groups_by_day_pipeline_resource_mode(self):
        assert "GROUP BY" in self.SQL

    @pytest.mark.parametrize("col_alias", [
        "day", "pipeline", "resource", "mode",
        "success_count", "failure_count",
        "avg_duration_s", "p95_duration_s",
        "total_fetched", "total_upserted",
    ])
    def test_output_column_aliases(self, col_alias):
        import re
        assert re.search(rf"\b{col_alias}\b", self.SQL), (
            f"Expected output column '{col_alias}' not found in view SQL"
        )

    def test_success_count_filters_success_status(self):
        assert "status = 'success'" in self.SQL

    def test_failure_count_filters_failed_status(self):
        assert "status = 'failed'" in self.SQL

    def test_percentile_for_p95(self):
        assert "0.95" in self.SQL

    def test_unique_index_defined(self):
        assert "CREATE UNIQUE INDEX" in self.SQL
        assert "idx_sync_rollup_daily_pk" in self.SQL

    def test_unique_index_covers_4_columns(self):
        # day, pipeline, resource, mode
        assert "day, pipeline, resource, mode" in self.SQL

    def test_reads_from_sync_runs(self):
        assert "FROM sync_runs" in self.SQL
