"""Structured event names for all Flowbyte log events."""
from enum import StrEnum


class EventName(StrEnum):
    # ── Sync lifecycle ────────────────────────────────────────────────────────
    SYNC_STARTED = "sync_started"
    SYNC_COMPLETED = "sync_completed"
    SYNC_FAILED = "sync_failed"
    SYNC_SKIPPED = "sync_skipped"

    # ── Extract ───────────────────────────────────────────────────────────────
    EXTRACT_PAGE_FETCHED = "extract_page_fetched"
    EXTRACT_DONE = "extract_done"
    PAGINATION_STUCK = "pagination_stuck"
    FULL_REFRESH_EXTRACT_DONE = "full_refresh_extract_done"

    # ── Transform ─────────────────────────────────────────────────────────────
    RECORD_SKIPPED = "record_skipped"
    TYPE_OVERRIDE_FAILED = "type_override_failed"

    # ── Load ──────────────────────────────────────────────────────────────────
    UPSERT_BATCH_DONE = "upsert_batch_done"
    UPSERT_CONFLICT = "upsert_conflict"

    # ── Checkpoint ────────────────────────────────────────────────────────────
    CHECKPOINT_SAVED = "checkpoint_saved"
    CHECKPOINT_LOADED = "checkpoint_loaded"
    WATERMARK_EMPTY = "watermark_empty"
    WATERMARK_REGRESSION = "watermark_regression"

    # ── Soft delete ───────────────────────────────────────────────────────────
    SOFT_DELETE_SWEEP_DONE = "soft_delete_sweep_done"
    SOFT_DELETE_SWEEP_ABORTED = "soft_delete_sweep_aborted"

    # ── Validation ────────────────────────────────────────────────────────────
    VALIDATION_DONE = "validation_done"
    VALIDATION_FAILED = "validation_failed"

    # ── Scheduler ─────────────────────────────────────────────────────────────
    SCHEDULER_STARTED = "scheduler_started"
    SCHEDULER_STOPPED = "scheduler_stopped"
    RECONCILER_TICK = "reconciler_tick"
    HEARTBEAT_WRITTEN = "heartbeat_written"
    JOB_MISFIRE = "job_misfire"

    # ── Sync requests ─────────────────────────────────────────────────────────
    SYNC_REQUEST_QUEUED = "sync_request_queued"
    SYNC_REQUEST_CLAIMED = "sync_request_claimed"
    SYNC_REQUEST_RECOVERED = "sync_request_recovered"
    SYNC_REQUEST_AUTO_FAILED = "sync_request_auto_failed"

    # ── Haravan API ───────────────────────────────────────────────────────────
    RATE_LIMIT_HIT = "rate_limit_hit"
    BUCKET_PRIMED = "bucket_primed"
    BUCKET_PRIME_FAILED = "bucket_prime_failed"
    BUCKET_HEADER_MALFORMED = "bucket_header_malformed"

    # ── Alert ─────────────────────────────────────────────────────────────────
    ALERT_SENT = "alert_sent"
    ALERT_SKIPPED_DEDUP = "alert_skipped_dedup"
    ALERT_FAILED = "alert_failed"
    SCHEDULER_DEAD = "scheduler_dead"

    # ── Cleanup / retention ───────────────────────────────────────────────────
    CLEANUP_STARTED = "cleanup_started"
    CLEANUP_DONE = "cleanup_done"

    # ── Credentials ───────────────────────────────────────────────────────────
    CREDENTIAL_SAVED = "credential_saved"
    CREDENTIAL_LOADED = "credential_loaded"

    # ── Daemon / bootstrap ────────────────────────────────────────────────────
    DAEMON_STARTED = "daemon_started"
    DAEMON_STOPPED = "daemon_stopped"
    MIGRATION_DONE = "migration_done"
    BOOTSTRAP_DONE = "bootstrap_done"
