"""SQLAlchemy Table definitions for Flowbyte internal DB (control plane)."""
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Table,
    Text,
    UniqueConstraint,
    text as sa_text,
)

_NOW = sa_text("NOW()")
from sqlalchemy.dialects.postgresql import JSONB, UUID

internal_metadata = MetaData()

# ── pipelines ─────────────────────────────────────────────────────────────────

pipelines = Table(
    "pipelines",
    internal_metadata,
    Column("name", String(32), primary_key=True),
    Column("yaml_content", Text, nullable=False),
    Column("config_json", JSONB, nullable=False),
    Column("enabled", Boolean, nullable=False, server_default="false"),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=_NOW),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=_NOW),
)

# ── credentials ───────────────────────────────────────────────────────────────

credentials = Table(
    "credentials",
    internal_metadata,
    Column("ref", String(64), primary_key=True),
    Column(
        "kind",
        String(32),
        nullable=False,
        # CHECK constraint defined via DDL in migration
    ),
    Column("ciphertext", Text, nullable=False),  # base64(nonce||tag||ct)
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=_NOW),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=_NOW),
    CheckConstraint("kind IN ('haravan', 'postgres')", name="credentials_kind_check"),
)

# ── sync_requests ─────────────────────────────────────────────────────────────

sync_requests = Table(
    "sync_requests",
    internal_metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default="gen_random_uuid()"),
    Column("pipeline", String(32), nullable=False),
    Column("resource", String(32)),  # NULL = all resources
    Column(
        "mode",
        String(16),
        nullable=False,
        server_default="'incremental'",
    ),
    Column("status", String(16), nullable=False, server_default="'pending'"),
    Column("requested_at", DateTime(timezone=True), nullable=False, server_default=_NOW),
    Column("claimed_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("error", Text),
    Column("recovery_count", SmallInteger, nullable=False, server_default="0"),
    Column("wait_timeout_at", DateTime(timezone=True)),
    CheckConstraint(
        "mode IN ('incremental', 'full_refresh')", name="sync_requests_mode_check"
    ),
    CheckConstraint(
        "status IN ('pending', 'claimed', 'running', 'done', 'failed', 'cancelled')",
        name="sync_requests_status_check",
    ),
    Index("idx_sync_requests_pending", "status", "requested_at", postgresql_where="status = 'pending'"),
)

# ── sync_logs ─────────────────────────────────────────────────────────────────

sync_logs = Table(
    "sync_logs",
    internal_metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default="gen_random_uuid()"),
    Column("timestamp", DateTime(timezone=True), nullable=False, server_default=_NOW),
    Column("level", String(16), nullable=False),
    Column("event", String(64), nullable=False),
    Column("sync_id", UUID(as_uuid=True)),
    Column("pipeline", String(32)),
    Column("resource", String(32)),
    Column("message", Text),
    Column("payload", JSONB),
    Column("exc_info", Text),
    CheckConstraint(
        "level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')",
        name="sync_logs_level_check",
    ),
    Index("idx_sync_logs_pipeline_ts", "pipeline", "resource", "timestamp"),
    Index("idx_sync_logs_sync_id", "sync_id"),
    Index(
        "idx_sync_logs_errors",
        "level",
        "timestamp",
        postgresql_where="level IN ('ERROR', 'CRITICAL')",
    ),
)

# ── sync_checkpoints ──────────────────────────────────────────────────────────

sync_checkpoints = Table(
    "sync_checkpoints",
    internal_metadata,
    Column("pipeline", String(32), primary_key=True),
    Column("resource", String(32), primary_key=True),
    Column("last_updated_at", DateTime(timezone=True)),
    Column("last_id", BigInteger),  # composite cursor tie-breaker (§17.5)
    Column("last_sync_id", UUID(as_uuid=True)),
    Column("last_sync_at", DateTime(timezone=True)),
    Column("last_status", String(16)),
)

# ── sync_runs ─────────────────────────────────────────────────────────────────

sync_runs = Table(
    "sync_runs",
    internal_metadata,
    Column("sync_id", UUID(as_uuid=True), primary_key=True),
    Column("pipeline", String(32), nullable=False),
    Column("resource", String(32), nullable=False),
    Column("mode", String(16), nullable=False),
    Column("trigger", String(16), nullable=False),  # schedule | manual
    Column("request_id", UUID(as_uuid=True)),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("status", String(16), nullable=False, server_default="'running'"),
    Column("fetched_count", Integer),
    Column("upserted_count", Integer),
    Column("skipped_invalid", Integer),
    Column("soft_deleted_count", Integer),
    Column("rows_before", Integer),
    Column("rows_after", Integer),
    Column("duration_seconds", Numeric(10, 2)),
    Column("error", Text),
    Index("idx_sync_runs_recent", "pipeline", "resource", "started_at"),
    Index(
        "idx_sync_runs_status",
        "status",
        "started_at",
        postgresql_where="status IN ('failed', 'cancelled')",
    ),
)

# ── validation_results ────────────────────────────────────────────────────────

validation_results = Table(
    "validation_results",
    internal_metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, server_default="gen_random_uuid()"),
    Column("sync_id", UUID(as_uuid=True), nullable=False),
    Column("pipeline", String(32), nullable=False),
    Column("resource", String(32), nullable=False),
    Column("rule", String(64), nullable=False),
    Column("status", String(16), nullable=False),
    Column("details", JSONB),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=_NOW),
    CheckConstraint(
        "status IN ('ok', 'warning', 'failed', 'skipped')",
        name="validation_results_status_check",
    ),
    Index("idx_validation_sync", "sync_id"),
)

# ── scheduler_heartbeat ───────────────────────────────────────────────────────

scheduler_heartbeat = Table(
    "scheduler_heartbeat",
    internal_metadata,
    Column("id", Integer, primary_key=True),  # singleton, id=1
    Column("last_beat", DateTime(timezone=True), nullable=False, server_default=_NOW),
    Column("daemon_started_at", DateTime(timezone=True), nullable=False),
    Column("version", String(32), nullable=False),
    CheckConstraint("id = 1", name="scheduler_heartbeat_singleton"),
)

# ── deployment_events ─────────────────────────────────────────────────────────

deployment_events = Table(
    "deployment_events",
    internal_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("event", String(32), nullable=False),
    Column("version", String(32)),
    Column("alembic_from", String(32)),
    Column("alembic_to", String(32)),
    Column("details", JSONB),
    Column("at", DateTime(timezone=True), nullable=False, server_default=_NOW),
)

# ── backup_events ─────────────────────────────────────────────────────────────

backup_events = Table(
    "backup_events",
    internal_metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=_NOW),
    Column("finished_at", DateTime(timezone=True)),
    Column("status", String(16), nullable=False),
    Column("output_path", Text),
    Column("size_bytes", BigInteger),
    Column("sha256", String(64)),
    Column("verify_status", String(16)),
    Column("error", Text),
    Column("retention_tier", String(16)),
)

# ── master_key_metadata ───────────────────────────────────────────────────────

master_key_metadata = Table(
    "master_key_metadata",
    internal_metadata,
    Column("id", Integer, primary_key=True),  # singleton, id=1
    Column("fingerprint_sha256", String(64), nullable=False),
    Column("bootstrapped_at", DateTime(timezone=True), nullable=False),
    Column("user_confirmed_backup", Boolean, nullable=False, server_default="false"),
    Column("last_backup_reminder_at", DateTime(timezone=True)),
    CheckConstraint("id = 1", name="master_key_metadata_singleton"),
)

# ── sync_rollup_daily (materialized view — defined in migration) ──────────────
# Not a Table, but referenced by queries in observability.
SYNC_ROLLUP_DAILY_VIEW_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS sync_rollup_daily AS
SELECT
  DATE_TRUNC('day', finished_at AT TIME ZONE 'Asia/Ho_Chi_Minh') AS day,
  pipeline,
  resource,
  mode,
  COUNT(*) FILTER (WHERE status = 'success') AS success_count,
  COUNT(*) FILTER (WHERE status = 'failed') AS failure_count,
  AVG(duration_seconds) AS avg_duration_s,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_seconds) AS p95_duration_s,
  SUM(fetched_count) AS total_fetched,
  SUM(upserted_count) AS total_upserted
FROM sync_runs
WHERE finished_at IS NOT NULL AND trigger = 'schedule'
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_rollup_daily_pk
  ON sync_rollup_daily (day, pipeline, resource, mode);
"""
