"""APScheduler daemon: runs reconciler, heartbeat, cleanup, and sync jobs."""
from __future__ import annotations

import signal
import sys
from datetime import datetime, timezone

from apscheduler.events import EVENT_JOB_MISSED
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler

from flowbyte.config.models import AppSettings
from flowbyte.db.engine import get_internal_engine
from flowbyte.haravan.token_bucket import HaravanTokenBucket
from flowbyte.logging import EventName, get_logger

log = get_logger()

# Singleton token bucket shared across all sync jobs (1 shop, 1 pipeline MVP)
GLOBAL_TOKEN_BUCKET = HaravanTokenBucket()


def build_scheduler() -> BlockingScheduler:
    return BlockingScheduler(
        executors={
            "default": ThreadPoolExecutor(max_workers=1),   # sync jobs queue serially
            "internal": ThreadPoolExecutor(max_workers=2),  # reconciler + heartbeat
        },
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 1800,  # 30 min (§17.7)
        },
    )


def start_daemon() -> None:
    from flowbyte.scheduler.reconciler import reconciler_tick
    from flowbyte.retention.cleanup import cleanup_tick

    settings = AppSettings()
    internal_engine = get_internal_engine(settings.db_url)
    scheduler = build_scheduler()

    # ── Misfire alert listener ─────────────────────────────────────────────────
    def on_misfire(event):
        log.error(
            EventName.JOB_MISFIRE,
            job_id=event.job_id,
            scheduled_run_time=event.scheduled_run_time.isoformat()
            if event.scheduled_run_time
            else None,
        )

    scheduler.add_listener(on_misfire, EVENT_JOB_MISSED)

    # ── Internal jobs ─────────────────────────────────────────────────────────
    scheduler.add_job(
        reconciler_tick,
        trigger="interval",
        seconds=settings.scheduler_poll_interval_seconds,
        id="_reconciler",
        executor="internal",
        args=[scheduler, internal_engine],
    )
    scheduler.add_job(
        _heartbeat_tick,
        trigger="interval",
        seconds=settings.heartbeat_interval_seconds,
        id="_heartbeat",
        executor="internal",
        args=[internal_engine],
    )
    scheduler.add_job(
        cleanup_tick,
        trigger="cron",
        hour=3,
        minute=0,
        id="_cleanup",
        executor="internal",
        args=[internal_engine],
    )

    # ── Signal handling ────────────────────────────────────────────────────────
    def _shutdown(signum, frame):
        log.info(EventName.DAEMON_STOPPED, signal=signum)
        scheduler.shutdown(wait=True)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    # ── Bootstrap heartbeat row ────────────────────────────────────────────────
    from flowbyte import __version__
    from flowbyte.db.internal_schema import scheduler_heartbeat
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    with internal_engine.begin() as conn:
        conn.execute(
            pg_insert(scheduler_heartbeat)
            .values(
                id=1,
                last_beat=datetime.now(timezone.utc),
                daemon_started_at=datetime.now(timezone.utc),
                version=__version__,
            )
            .on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "last_beat": datetime.now(timezone.utc),
                    "daemon_started_at": datetime.now(timezone.utc),
                    "version": __version__,
                },
            )
        )

    log.info(EventName.DAEMON_STARTED, version=__version__)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


def _heartbeat_tick(internal_engine) -> None:
    from flowbyte.db.internal_schema import scheduler_heartbeat

    with internal_engine.begin() as conn:
        conn.execute(
            scheduler_heartbeat.update()
            .where(scheduler_heartbeat.c.id == 1)
            .values(last_beat=datetime.now(timezone.utc))
        )
    log.debug(EventName.HEARTBEAT_WRITTEN)
