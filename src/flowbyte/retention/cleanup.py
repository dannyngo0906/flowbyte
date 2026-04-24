"""Daily 3AM cleanup: delete expired sync_logs + validation_results, then VACUUM."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, func, select, text
from sqlalchemy.engine import Engine

from flowbyte.db.internal_schema import sync_logs, validation_results
from flowbyte.logging import EventName, get_logger

log = get_logger()

_SYNC_LOGS_SUCCESS_DAYS = 90
_SYNC_LOGS_ERROR_DAYS = 30
_VALIDATION_RESULTS_DAYS = 30


def cleanup_tick(internal_engine: Engine) -> None:
    log.info(EventName.CLEANUP_STARTED)
    try:
        stats = _run_cleanup(internal_engine, dry_run=False)
        log.info(EventName.CLEANUP_DONE, **stats)
    except Exception as e:
        log.error(EventName.CLEANUP_DONE, error=str(e), exc_info=True)


def dry_run_cleanup(internal_engine: Engine) -> dict:
    return _run_cleanup(internal_engine, dry_run=True)


def _run_cleanup(internal_engine: Engine, dry_run: bool) -> dict:
    now = datetime.now(timezone.utc)
    success_cutoff = now - timedelta(days=_SYNC_LOGS_SUCCESS_DAYS)
    error_cutoff = now - timedelta(days=_SYNC_LOGS_ERROR_DAYS)
    validation_cutoff = now - timedelta(days=_VALIDATION_RESULTS_DAYS)

    stats: dict = {}

    with internal_engine.begin() as conn:
        # Count rows to delete (for dry-run reporting)
        logs_success_count = conn.execute(
            select(func.count()).select_from(sync_logs).where(
                sync_logs.c.level.notin_(["ERROR", "CRITICAL"]),
                sync_logs.c.timestamp < success_cutoff,
            )
        ).scalar() or 0

        logs_error_count = conn.execute(
            select(func.count()).select_from(sync_logs).where(
                sync_logs.c.level.in_(["ERROR", "CRITICAL"]),
                sync_logs.c.timestamp < error_cutoff,
            )
        ).scalar() or 0

        val_count = conn.execute(
            select(func.count()).select_from(validation_results).where(
                validation_results.c.created_at < validation_cutoff
            )
        ).scalar() or 0

        stats = {
            "sync_logs_success_rows": logs_success_count,
            "sync_logs_error_rows": logs_error_count,
            "validation_results_rows": val_count,
            "dry_run": dry_run,
        }

        if dry_run:
            return stats

        conn.execute(
            delete(sync_logs).where(
                sync_logs.c.level.notin_(["ERROR", "CRITICAL"]),
                sync_logs.c.timestamp < success_cutoff,
            )
        )
        conn.execute(
            delete(sync_logs).where(
                sync_logs.c.level.in_(["ERROR", "CRITICAL"]),
                sync_logs.c.timestamp < error_cutoff,
            )
        )
        conn.execute(
            delete(validation_results).where(
                validation_results.c.created_at < validation_cutoff
            )
        )

    # VACUUM outside of transaction
    with internal_engine.connect() as conn:
        conn.execute(text("VACUUM ANALYZE sync_logs"))
        conn.execute(text("VACUUM ANALYZE validation_results"))

    return stats
