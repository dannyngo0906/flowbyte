"""Init internal schema — all control plane tables.

Revision ID: 001
Revises:
Create Date: 2026-04-24
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # ── pipelines ─────────────────────────────────────────────────────────────
    op.create_table(
        "pipelines",
        sa.Column("name", sa.String(32), primary_key=True),
        sa.Column("yaml_content", sa.Text, nullable=False),
        sa.Column("config_json", JSONB, nullable=False),
        sa.Column("enabled", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default="NOW()"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default="NOW()"),
    )

    # ── credentials ───────────────────────────────────────────────────────────
    op.create_table(
        "credentials",
        sa.Column("ref", sa.String(64), primary_key=True),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("ciphertext", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default="NOW()"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default="NOW()"),
        sa.CheckConstraint("kind IN ('haravan', 'postgres')", name="credentials_kind_check"),
    )

    # ── sync_requests ─────────────────────────────────────────────────────────
    op.create_table(
        "sync_requests",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default="gen_random_uuid()"),
        sa.Column("pipeline", sa.String(32), nullable=False),
        sa.Column("resource", sa.String(32)),
        sa.Column("mode", sa.String(16), nullable=False, server_default="'incremental'"),
        sa.Column("status", sa.String(16), nullable=False, server_default="'pending'"),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False, server_default="NOW()"),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("error", sa.Text),
        sa.Column("recovery_count", sa.SmallInteger, nullable=False, server_default="0"),
        sa.Column("wait_timeout_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("mode IN ('incremental', 'full_refresh')", name="sync_requests_mode_check"),
        sa.CheckConstraint(
            "status IN ('pending', 'claimed', 'running', 'done', 'failed', 'cancelled')",
            name="sync_requests_status_check",
        ),
    )
    op.create_index(
        "idx_sync_requests_pending", "sync_requests", ["status", "requested_at"],
        postgresql_where="status = 'pending'",
    )

    # ── sync_logs ─────────────────────────────────────────────────────────────
    op.create_table(
        "sync_logs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default="gen_random_uuid()"),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False, server_default="NOW()"),
        sa.Column("level", sa.String(16), nullable=False),
        sa.Column("event", sa.String(64), nullable=False),
        sa.Column("sync_id", UUID(as_uuid=True)),
        sa.Column("pipeline", sa.String(32)),
        sa.Column("resource", sa.String(32)),
        sa.Column("message", sa.Text),
        sa.Column("payload", JSONB),
        sa.Column("exc_info", sa.Text),
        sa.CheckConstraint(
            "level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')",
            name="sync_logs_level_check",
        ),
    )
    op.create_index("idx_sync_logs_pipeline_ts", "sync_logs", ["pipeline", "resource", "timestamp"])
    op.create_index("idx_sync_logs_sync_id", "sync_logs", ["sync_id"])
    op.create_index(
        "idx_sync_logs_errors", "sync_logs", ["level", "timestamp"],
        postgresql_where="level IN ('ERROR', 'CRITICAL')",
    )

    # ── sync_checkpoints ──────────────────────────────────────────────────────
    op.create_table(
        "sync_checkpoints",
        sa.Column("pipeline", sa.String(32), primary_key=True),
        sa.Column("resource", sa.String(32), primary_key=True),
        sa.Column("last_updated_at", sa.DateTime(timezone=True)),
        sa.Column("last_id", sa.BigInteger),
        sa.Column("last_sync_id", UUID(as_uuid=True)),
        sa.Column("last_sync_at", sa.DateTime(timezone=True)),
        sa.Column("last_status", sa.String(16)),
    )

    # ── sync_runs ─────────────────────────────────────────────────────────────
    op.create_table(
        "sync_runs",
        sa.Column("sync_id", UUID(as_uuid=True), primary_key=True),
        sa.Column("pipeline", sa.String(32), nullable=False),
        sa.Column("resource", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("trigger", sa.String(16), nullable=False),
        sa.Column("request_id", UUID(as_uuid=True)),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(16), nullable=False, server_default="'running'"),
        sa.Column("fetched_count", sa.Integer),
        sa.Column("upserted_count", sa.Integer),
        sa.Column("skipped_invalid", sa.Integer),
        sa.Column("soft_deleted_count", sa.Integer),
        sa.Column("rows_before", sa.Integer),
        sa.Column("rows_after", sa.Integer),
        sa.Column("duration_seconds", sa.Numeric(10, 2)),
        sa.Column("error", sa.Text),
    )
    op.create_index("idx_sync_runs_recent", "sync_runs", ["pipeline", "resource", "started_at"])
    op.create_index(
        "idx_sync_runs_status", "sync_runs", ["status", "started_at"],
        postgresql_where="status IN ('failed', 'cancelled')",
    )

    # ── validation_results ────────────────────────────────────────────────────
    op.create_table(
        "validation_results",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default="gen_random_uuid()"),
        sa.Column("sync_id", UUID(as_uuid=True), nullable=False),
        sa.Column("pipeline", sa.String(32), nullable=False),
        sa.Column("resource", sa.String(32), nullable=False),
        sa.Column("rule", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("details", JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default="NOW()"),
        sa.CheckConstraint(
            "status IN ('ok', 'warning', 'failed', 'skipped')",
            name="validation_results_status_check",
        ),
    )
    op.create_index("idx_validation_sync", "validation_results", ["sync_id"])

    # ── scheduler_heartbeat ───────────────────────────────────────────────────
    op.create_table(
        "scheduler_heartbeat",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("last_beat", sa.DateTime(timezone=True), nullable=False, server_default="NOW()"),
        sa.Column("daemon_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.String(32), nullable=False),
        sa.CheckConstraint("id = 1", name="scheduler_heartbeat_singleton"),
    )

    # ── deployment_events ─────────────────────────────────────────────────────
    op.create_table(
        "deployment_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("event", sa.String(32), nullable=False),
        sa.Column("version", sa.String(32)),
        sa.Column("alembic_from", sa.String(32)),
        sa.Column("alembic_to", sa.String(32)),
        sa.Column("details", JSONB),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False, server_default="NOW()"),
    )

    # ── backup_events ─────────────────────────────────────────────────────────
    op.create_table(
        "backup_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default="NOW()"),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("output_path", sa.Text),
        sa.Column("size_bytes", sa.BigInteger),
        sa.Column("sha256", sa.String(64)),
        sa.Column("verify_status", sa.String(16)),
        sa.Column("error", sa.Text),
        sa.Column("retention_tier", sa.String(16)),
    )

    # ── master_key_metadata ───────────────────────────────────────────────────
    op.create_table(
        "master_key_metadata",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("fingerprint_sha256", sa.String(64), nullable=False),
        sa.Column("bootstrapped_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("user_confirmed_backup", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("last_backup_reminder_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("id = 1", name="master_key_metadata_singleton"),
    )


def downgrade() -> None:
    for table in [
        "master_key_metadata", "backup_events", "deployment_events",
        "scheduler_heartbeat", "validation_results", "sync_runs",
        "sync_checkpoints", "sync_logs", "sync_requests", "credentials", "pipelines",
    ]:
        op.drop_table(table)
