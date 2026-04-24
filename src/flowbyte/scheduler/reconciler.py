"""Reconciler: every N seconds, diff desired vs actual APScheduler jobs.

Steps:
  0. Recover stale sync_requests (claimed but no in-flight job)
  1. Diff enabled pipelines → add/remove scheduled jobs
  2. Claim + schedule next pending manual sync_request
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from urllib.parse import quote as _urlquote
from uuid import UUID

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from flowbyte.config.loader import load_pipeline_config
from flowbyte.config.models import PipelineConfig
from flowbyte.db.internal_schema import pipelines, sync_requests
from flowbyte.logging import EventName, get_logger

log = get_logger()

_STALE_CLAIM_MINUTES = 2
_MAX_RECOVERIES = 3
_INTERNAL_JOB_PREFIXES = ("_",)


def reconciler_tick(
    scheduler: BlockingScheduler,
    internal_engine: Engine,
    alerter=None,
) -> None:
    try:
        _reconciler_tick_inner(scheduler, internal_engine, alerter)
    except Exception as e:
        log.error(EventName.RECONCILER_TICK, error=str(e), exc_info=True)


def _reconciler_tick_inner(
    scheduler: BlockingScheduler,
    internal_engine: Engine,
    alerter=None,
) -> None:
    with internal_engine.begin() as conn:
        # ── Step 0: Recover stale claimed requests ─────────────────────────
        _recover_stale_claims(conn, scheduler)

        # ── Step 1: Reconcile scheduled jobs ──────────────────────────────
        desired: set[str] = set()
        enabled_pipelines = _list_enabled_pipelines(conn)

        for pipeline_row in enabled_pipelines:
            cfg = PipelineConfig.model_validate(pipeline_row.config_json)
            tz = _get_timezone(cfg)

            for resource_name, resource_cfg in cfg.resources.items():
                if not resource_cfg.enabled:
                    continue

                # Primary trigger
                job_id = f"{cfg.name}__{resource_name}__{resource_cfg.sync_mode}"
                desired.add(job_id)
                scheduler.add_job(
                    _run_sync_job,
                    trigger=CronTrigger.from_crontab(resource_cfg.schedule, timezone=tz),
                    id=job_id,
                    executor="default",
                    args=[cfg.name, resource_name, resource_cfg.sync_mode, internal_engine, alerter],
                    replace_existing=True,
                )

                # Weekly full refresh
                if resource_cfg.weekly_full_refresh.enabled:
                    weekly_id = f"{cfg.name}__{resource_name}__weekly_full"
                    desired.add(weekly_id)
                    scheduler.add_job(
                        _run_sync_job,
                        trigger=CronTrigger.from_crontab(
                            resource_cfg.weekly_full_refresh.cron, timezone=tz
                        ),
                        id=weekly_id,
                        executor="default",
                        args=[cfg.name, resource_name, "full_refresh", internal_engine, alerter],
                        replace_existing=True,
                    )

        # Remove stale jobs
        enabled_pipeline_names = {r.name for r in enabled_pipelines}
        for job in scheduler.get_jobs():
            if any(job.id.startswith(p) for p in _INTERNAL_JOB_PREFIXES):
                continue
            if job.id.startswith("_manual_"):
                # Cancel manual job if pipeline is now disabled
                pipeline_name = job.id.split("__")[0].removeprefix("_manual_")
                if pipeline_name not in enabled_pipeline_names:
                    scheduler.remove_job(job.id)
                continue
            if job.id not in desired:
                scheduler.remove_job(job.id)

        # ── Step 2: Claim next pending manual sync_request ─────────────────
        claimed = _claim_next_pending(conn)
        if claimed:
            scheduler.add_job(
                _run_sync_job,
                trigger="date",
                run_date=datetime.now(timezone.utc),
                id=f"_manual_{claimed['id']}",
                executor="default",
                args=[
                    claimed["pipeline"],
                    claimed["resource"],
                    claimed["mode"],
                    internal_engine,
                    alerter,
                ],
                kwargs={"request_id": str(claimed["id"])},
            )
            log.info(
                EventName.SYNC_REQUEST_CLAIMED,
                request_id=str(claimed["id"]),
                pipeline=claimed["pipeline"],
                resource=claimed["resource"],
            )

    log.debug(EventName.RECONCILER_TICK, desired_jobs=len(desired))


def _run_sync_job(
    pipeline_name: str,
    resource: str,
    mode: str,
    internal_engine: Engine,
    alerter=None,
    request_id: str | None = None,
) -> None:
    from flowbyte.config.models import AppSettings, SyncJobSpec
    from flowbyte.db.engine import get_dest_engine
    from flowbyte.haravan.client import HaravanClient
    from flowbyte.scheduler.daemon import GLOBAL_TOKEN_BUCKET
    from flowbyte.sync.runner import SyncRunner
    from uuid import uuid4

    sync_id = str(uuid4())
    bound_log = log.bind(sync_id=sync_id, pipeline=pipeline_name, resource=resource)

    try:
        with internal_engine.begin() as conn:
            row = conn.execute(
                select(pipelines).where(pipelines.c.name == pipeline_name)
            ).one()
        cfg = PipelineConfig.model_validate(row.config_json)
        resource_cfg = cfg.resources.get(resource)
        if resource_cfg is None or not resource_cfg.enabled:
            bound_log.info(EventName.SYNC_SKIPPED, reason="resource_disabled")
            return

        creds = _load_credentials(internal_engine, cfg.haravan_credentials_ref)
        pg_creds = _load_credentials(internal_engine, cfg.destination.credentials_ref)
        dest_url = _build_dest_url(cfg, pg_creds)

        haravan = HaravanClient(
            shop_domain=cfg.haravan_shop_domain,
            access_token=creds["access_token"],
            bucket=GLOBAL_TOKEN_BUCKET,
        )
        dest_engine = get_dest_engine(dest_url)
        runner = SyncRunner(cfg, haravan, internal_engine, dest_engine)

        spec = SyncJobSpec(
            pipeline=pipeline_name,
            resource=resource,
            mode=mode,
            sync_id=sync_id,
            request_id=request_id,
            trigger="manual" if request_id else "schedule",
        )
        result = runner.run(spec)

        if result.status == "failed" and alerter is not None:
            from flowbyte.alerting.telegram import format_sync_fail_alert
            text = format_sync_fail_alert(
                pipeline=pipeline_name,
                resource=resource,
                error=result.error or "",
                sync_id=sync_id,
            )
            alerter.send(
                text,
                key=f"sync_fail:{pipeline_name}:{resource}",
                pipeline=pipeline_name,
            )

        if result.validation_failed and alerter is not None:
            from flowbyte.alerting.telegram import format_validation_alert
            text = format_validation_alert(
                pipeline=pipeline_name,
                resource=resource,
                failed_rules=result.validation_failed_rules,
                sync_id=sync_id,
            )
            alerter.send(
                text,
                key=f"validation_fail:{pipeline_name}:{resource}",
                pipeline=pipeline_name,
            )

        if request_id:
            _update_request_status(
                internal_engine,
                request_id,
                "done" if result.status == "success" else "failed",
                result.error,
            )

    except Exception as e:
        bound_log.error(EventName.SYNC_FAILED, error=str(e), exc_info=True)
        if request_id:
            _update_request_status(internal_engine, request_id, "failed", str(e))


def _list_enabled_pipelines(conn):
    return conn.execute(select(pipelines).where(pipelines.c.enabled == True)).all()


def _recover_stale_claims(conn, scheduler: BlockingScheduler) -> None:
    stale_cutoff = datetime.now(timezone.utc) - timedelta(minutes=_STALE_CLAIM_MINUTES)
    running_ids = {j.id for j in scheduler.get_jobs()}

    stale = conn.execute(
        select(sync_requests).where(
            sync_requests.c.status == "claimed",
            sync_requests.c.claimed_at < stale_cutoff,
        )
    ).all()

    for row in stale:
        manual_job_id = f"_manual_{row.id}"
        if manual_job_id in running_ids:
            continue  # actually running, OK

        if row.recovery_count >= _MAX_RECOVERIES:
            conn.execute(
                sync_requests.update()
                .where(sync_requests.c.id == row.id)
                .values(
                    status="failed",
                    error=f"Auto-failed after {_MAX_RECOVERIES} recoveries",
                    finished_at=datetime.now(timezone.utc),
                )
            )
            log.critical(EventName.SYNC_REQUEST_AUTO_FAILED, request_id=str(row.id))
        else:
            conn.execute(
                sync_requests.update()
                .where(sync_requests.c.id == row.id)
                .values(
                    status="pending",
                    claimed_at=None,
                    recovery_count=row.recovery_count + 1,
                )
            )
            log.warning(
                EventName.SYNC_REQUEST_RECOVERED,
                request_id=str(row.id),
                attempt=row.recovery_count + 1,
            )


def _claim_next_pending(conn) -> dict | None:
    row = conn.execute(
        select(sync_requests)
        .where(sync_requests.c.status == "pending")
        .order_by(sync_requests.c.requested_at)
        .limit(1)
        .with_for_update(skip_locked=True)
    ).one_or_none()

    if row is None:
        return None

    conn.execute(
        sync_requests.update()
        .where(sync_requests.c.id == row.id)
        .values(status="claimed", claimed_at=datetime.now(timezone.utc))
    )
    return dict(row._mapping)


def _update_request_status(engine: Engine, request_id: str, status: str, error: str | None) -> None:
    with engine.begin() as conn:
        conn.execute(
            sync_requests.update()
            .where(sync_requests.c.id == request_id)
            .values(
                status=status,
                finished_at=datetime.now(timezone.utc),
                error=error,
            )
        )


def _load_credentials(engine: Engine, ref: str) -> dict:
    from flowbyte.db.internal_schema import credentials as creds_table
    from flowbyte.config.models import AppSettings
    from flowbyte.security import Encryptor, MasterKey
    import json

    settings = AppSettings()
    master_key = MasterKey.load(settings.master_key_path)
    enc = Encryptor(master_key.raw)

    with engine.begin() as conn:
        row = conn.execute(
            select(creds_table).where(creds_table.c.ref == ref)
        ).one()

    from flowbyte.security.encryption import Ciphertext
    ct = Ciphertext.deserialize(row.ciphertext)
    plaintext = enc.decrypt(ct, associated_data=ref.encode())
    return json.loads(plaintext)


def _build_dest_url(cfg: PipelineConfig, pg_creds: dict) -> str:
    d = cfg.destination
    password = _urlquote(pg_creds.get("password", ""), safe="")
    user = _urlquote(d.user, safe="")
    return f"postgresql+psycopg://{user}:{password}@{d.host}:{d.port}/{d.database}"


def _get_timezone(cfg: PipelineConfig):
    from flowbyte.config.loader import load_global_config
    global_cfg = load_global_config()
    return global_cfg.get_zoneinfo()
