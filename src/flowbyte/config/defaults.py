"""Compile-time constants for Flowbyte.

Schedule defaults and resource lists live in config/models.py (tightly
coupled to Pydantic validation). This module holds runtime sizing and
threshold constants shared across multiple packages.
"""

# ── Haravan API ────────────────────────────────────────────────────────────────
DEFAULT_PAGE_SIZE: int = 250
INCREMENTAL_OVERLAP_MINUTES: int = 5

# ── Rate limit (mirrors HaravanTokenBucket constants) ─────────────────────────
HARAVAN_BUCKET_CAPACITY: int = 80
HARAVAN_BUCKET_LEAK_RATE: float = 4.0
HARAVAN_BUCKET_SAFETY_MARGIN: int = 70

# ── Retry ──────────────────────────────────────────────────────────────────────
DEFAULT_RETRY_ATTEMPTS: int = 5
DEFAULT_RETRY_MAX_WAIT_SECONDS: int = 60

# ── Scheduler ─────────────────────────────────────────────────────────────────
MISFIRE_GRACE_TIME_SECONDS: int = 1800       # 30 min
STALE_HEARTBEAT_SECONDS: int = 7200          # 2 h → triggers dead-scheduler alert
STALE_CLAIM_SECONDS: int = 300               # 5 min before reconciler releases claim
MAX_RECOVERY_COUNT: int = 3

# ── Retention ─────────────────────────────────────────────────────────────────
SYNC_LOGS_SUCCESS_RETENTION_DAYS: int = 90
SYNC_LOGS_ERROR_RETENTION_DAYS: int = 30
VALIDATION_RESULTS_RETENTION_DAYS: int = 30

# ── Validation thresholds ──────────────────────────────────────────────────────
ROW_COUNT_DELTA_WARN_PCT: float = 10.0       # flag if row count drops > 10%
SOFT_DELETE_SANITY_MAX_PCT: float = 5.0      # alert if > 5% records soft-deleted in one run

# ── Telegram alerting ─────────────────────────────────────────────────────────
ALERT_DEDUP_WINDOW_MINUTES: int = 5
ALERT_MAX_PER_HOUR_PER_PIPELINE: int = 3
