"""Pydantic configuration models for Flowbyte pipelines and global settings."""
from __future__ import annotations

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# ── Default schedules (staggered to avoid misfire at max_workers=1) ──────────

RESOURCE_SCHEDULE_DEFAULTS: dict[str, str] = {
    "orders": "0 */2 * * *",
    "customers": "5 */2 * * *",
    "products": "10 */12 * * *",
    "inventory_levels": "15 */2 * * *",
    "locations": "20 0 * * *",
}

RESOURCE_WEEKLY_FULL_DEFAULTS: dict[str, str] = {
    "orders": "0 3 * * 0",
    "customers": "30 3 * * 0",
    "products": "0 4 * * 0",
    "inventory_levels": "30 4 * * 0",
    "locations": "0 5 * * 0",
}

RESOURCE_SYNC_MODE_DEFAULTS: dict[str, str] = {
    "orders": "incremental",
    "customers": "incremental",
    "products": "incremental",
    "inventory_levels": "full_refresh",
    "locations": "full_refresh",
}

PHASE_1_RESOURCES = ["orders", "customers", "products", "inventory_levels", "locations"]


# ── Resource ──────────────────────────────────────────────────────────────────


class WeeklyFullRefreshConfig(BaseModel):
    enabled: bool = True
    cron: str = "0 2 * * 0"

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v: str) -> str:
        if not croniter.is_valid(v):
            raise ValueError(f"Invalid cron expression: {v!r}")
        return v


class ResourceConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")  # silently ignore legacy transform: section in YAML

    enabled: bool = True
    sync_mode: str = "incremental"
    schedule: str = "0 */2 * * *"
    weekly_full_refresh: WeeklyFullRefreshConfig = Field(
        default_factory=WeeklyFullRefreshConfig
    )

    @field_validator("sync_mode")
    @classmethod
    def validate_sync_mode(cls, v: str) -> str:
        if v not in ("incremental", "full_refresh"):
            raise ValueError(f"sync_mode must be 'incremental' or 'full_refresh', got {v!r}")
        return v

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, v: str) -> str:
        if not croniter.is_valid(v):
            raise ValueError(f"Invalid cron expression: {v!r}")
        return v


# ── Pipeline ──────────────────────────────────────────────────────────────────


class PostgresDestConfig(BaseModel):
    host: str = "localhost"
    port: int = 5432
    user: str
    database: str
    credentials_ref: str

    @property
    def dsn_without_password(self) -> str:
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"


class PipelineConfig(BaseModel):
    name: str = Field(pattern=r"^[a-z0-9_]{1,32}$")
    haravan_credentials_ref: str
    haravan_shop_domain: str
    destination: PostgresDestConfig
    resources: dict[str, ResourceConfig] = Field(default_factory=dict)

    @model_validator(mode="after")
    def apply_resource_defaults(self) -> "PipelineConfig":
        for res in PHASE_1_RESOURCES:
            if res not in self.resources:
                self.resources[res] = ResourceConfig(
                    sync_mode=RESOURCE_SYNC_MODE_DEFAULTS.get(res, "incremental"),
                    schedule=RESOURCE_SCHEDULE_DEFAULTS.get(res, "0 */2 * * *"),
                    weekly_full_refresh=WeeklyFullRefreshConfig(
                        cron=RESOURCE_WEEKLY_FULL_DEFAULTS.get(res, "0 2 * * 0")
                    ),
                )
        return self

    def detect_schedule_collisions(self) -> dict[str, list[str]]:
        """Return mapping of cron-minute → list of resource names that collide."""
        buckets: dict[str, list[str]] = {}
        for name, cfg in self.resources.items():
            if not cfg.enabled:
                continue
            minute = cfg.schedule.split()[0]
            buckets.setdefault(minute, []).append(name)
        return {k: v for k, v in buckets.items() if len(v) > 1}


# ── Global config (config.yml) ────────────────────────────────────────────────


class TelegramConfig(BaseModel):
    enabled: bool = False
    bot_token: SecretStr = Field(default=SecretStr(""))
    chat_id: str = ""


class ValidationThresholds(BaseModel):
    fetch_upsert_parity_max_skip_pct: float = 1.0
    volume_sanity_zero_streak: int = 3
    weekly_full_drift_warn_pct: float = 5.0
    weekly_full_drift_fail_pct: float = 20.0
    soft_delete_sanity_max_pct: float = 5.0


class LoggingDBSinkConfig(BaseModel):
    enabled: bool = True
    min_level: str = "INFO"
    retention_days: int = 14
    max_payload_bytes: int = 10_240
    on_sink_failure: str = "drop"
    queue_size: int = 1000


class LoggingConfig(BaseModel):
    db_sink: LoggingDBSinkConfig = Field(default_factory=LoggingDBSinkConfig)
    batch_events_every_n: int = 10


class BackupConfig(BaseModel):
    enabled: bool = False
    schedule: str = "0 4 * * *"
    output_dir: str = "/backups/flowbyte"
    retention_daily: int = 7
    retention_weekly: int = 4
    retention_monthly: int = 6
    include_data: bool = True
    verify_after: bool = True


class ObservabilityConfig(BaseModel):
    prometheus_enabled: bool = True
    prometheus_port: int = 9091
    prometheus_bind: str = "127.0.0.1"


class GlobalConfig(BaseModel):
    timezone: str = "Asia/Ho_Chi_Minh"
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    validation: ValidationThresholds = Field(default_factory=ValidationThresholds)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    backup: BackupConfig = Field(default_factory=BackupConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except (ZoneInfoNotFoundError, KeyError):
            raise ValueError(f"Unknown timezone: {v!r}")
        return v

    def get_zoneinfo(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)


# ── App-level settings (env vars) ─────────────────────────────────────────────


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FLOWBYTE_",
        env_file=".env",
        case_sensitive=False,
    )

    db_url: str = "postgresql+psycopg://flowbyte:flowbyte@localhost:5432/flowbyte_internal"
    dest_db_url: str = ""
    log_level: str = "INFO"
    env: str = "prod"
    master_key_path: str = "/etc/flowbyte/master.key"
    pipelines_dir: str = "/etc/flowbyte/pipelines"
    scheduler_poll_interval_seconds: int = 10
    heartbeat_interval_seconds: int = 60
    skip_migration: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    config_path: str = "/etc/flowbyte/config.yml"


# ── Sync job spec / result (used by runner) ───────────────────────────────────


class SyncJobSpec(BaseModel):
    pipeline: str
    resource: str
    mode: str
    sync_id: str = ""
    request_id: str | None = None
    trigger: str = "schedule"


class SyncResult(BaseModel):
    sync_id: str
    pipeline: str
    resource: str
    mode: str
    status: str = ""
    fetched_count: int = 0
    upserted_count: int = 0
    skipped_invalid: int = 0
    soft_deleted_count: int = 0
    rows_before: int = 0
    rows_after: int = 0
    duration_seconds: float = 0.0
    error: str | None = None
    validation_failed: bool = False
    validation_statuses: list[str] = Field(default_factory=list)
    validation_failed_rules: list[str] = Field(default_factory=list)
