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
    from flowbyte.config.loader import load_global_config
    from flowbyte.alerting.telegram import TelegramAlerter, format_scheduler_dead_alert

    settings = AppSettings()
    internal_engine = get_internal_engine(settings.db_url)
    scheduler = build_scheduler()

    # ── Build Telegram alerter (None when disabled) ───────────────────────────
    global_cfg = load_global_config(settings.config_path)
    alerter: TelegramAlerter | None = None
    if global_cfg.telegram.enabled:
        alerter = TelegramAlerter(
            bot_token=global_cfg.telegram.bot_token.get_secret_value(),
            chat_id=global_cfg.telegram.chat_id,
        )

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
        args=[scheduler, internal_engine, alerter],
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
    scheduler.add_job(
        _heartbeat_watchdog_tick,
        trigger="interval",
        minutes=5,
        id="_heartbeat_watchdog",
        executor="internal",
        args=[internal_engine, alerter],
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


_STALE_HEARTBEAT_HOURS = 2.0


def _heartbeat_watchdog_tick(internal_engine, alerter) -> None:
    """Send SCHEDULER_DEAD alert if heartbeat row is stale > 2 hours."""
    from flowbyte.db.internal_schema import scheduler_heartbeat
    from flowbyte.alerting.telegram import format_scheduler_dead_alert
    from sqlalchemy import select

    if alerter is None:
        return

    with internal_engine.begin() as conn:
        row = conn.execute(
            select(scheduler_heartbeat).where(scheduler_heartbeat.c.id == 1)
        ).one_or_none()

    if row is None:
        return

    now = datetime.now(timezone.utc)
    age_hours = (now - row.last_beat).total_seconds() / 3600
    if age_hours > _STALE_HEARTBEAT_HOURS:
        log.critical(
            EventName.SCHEDULER_DEAD,
            last_beat=row.last_beat.isoformat(),
            age_hours=round(age_hours, 2),
        )
        text = format_scheduler_dead_alert(
            last_beat=row.last_beat.isoformat(),
            age_hours=age_hours,
        )
        alerter.send(text, key="scheduler_dead", pipeline="__system__")


def _heartbeat_tick(internal_engine) -> None:
    from flowbyte.db.internal_schema import scheduler_heartbeat

    with internal_engine.begin() as conn:
        conn.execute(
            scheduler_heartbeat.update()
            .where(scheduler_heartbeat.c.id == 1)
            .values(last_beat=datetime.now(timezone.utc))
        )
    log.debug(EventName.HEARTBEAT_WRITTEN)
