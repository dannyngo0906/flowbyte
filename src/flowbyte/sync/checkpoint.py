"""Checkpoint management: high-watermark composite cursor (updated_at, id).

Invariant: checkpoint is saved AFTER destination upsert commits.
Crash between upsert-commit and checkpoint-save → safe re-process (upsert idempotent).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection

from flowbyte.db.internal_schema import sync_checkpoints
from flowbyte.logging import EventName, get_logger

log = get_logger()


def load_checkpoint(
    conn: Connection,
    pipeline: str,
    resource: str,
) -> tuple[datetime, int] | None:
    """Return (last_updated_at, last_id) or None if no checkpoint exists."""
    row = conn.execute(
        select(
            sync_checkpoints.c.last_updated_at,
            sync_checkpoints.c.last_id,
        ).where(
            sync_checkpoints.c.pipeline == pipeline,
            sync_checkpoints.c.resource == resource,
        )
    ).one_or_none()

    if row is None or row.last_updated_at is None:
        log.info(EventName.CHECKPOINT_LOADED, pipeline=pipeline, resource=resource, checkpoint=None)
        return None

    cp = (row.last_updated_at, row.last_id or 0)
    log.info(EventName.CHECKPOINT_LOADED, pipeline=pipeline, resource=resource, checkpoint=str(cp))
    return cp


def save_checkpoint(
    conn: Connection,
    pipeline: str,
    resource: str,
    new_watermark: tuple[datetime, int] | None,
    sync_id: UUID | str,
) -> None:
    """Upsert checkpoint atomically (call within same transaction as destination upsert)."""
    if new_watermark is None:
        log.warning(
            EventName.WATERMARK_EMPTY,
            pipeline=pipeline,
            resource=resource,
            message="No valid watermark in batch — keeping previous checkpoint",
        )
        return

    new_ts, new_id = new_watermark

    # Regression guard: don't overwrite with older watermark
    current = load_checkpoint(conn, pipeline, resource)
    if current is not None:
        cur_ts, _ = current
        if new_ts < cur_ts:
            log.critical(
                EventName.WATERMARK_REGRESSION,
                pipeline=pipeline,
                resource=resource,
                current=cur_ts.isoformat(),
                new=new_ts.isoformat(),
            )
            return  # Keep current, do NOT overwrite

    stmt = insert(sync_checkpoints).values(
        pipeline=pipeline,
        resource=resource,
        last_updated_at=new_ts,
        last_id=new_id,
        last_sync_id=sync_id,
        last_sync_at=datetime.now(timezone.utc),
        last_status="success",
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["pipeline", "resource"],
        set_={
            "last_updated_at": stmt.excluded.last_updated_at,
            "last_id": stmt.excluded.last_id,
            "last_sync_id": stmt.excluded.last_sync_id,
            "last_sync_at": stmt.excluded.last_sync_at,
            "last_status": stmt.excluded.last_status,
        },
    )
    conn.execute(stmt)
    log.info(
        EventName.CHECKPOINT_SAVED,
        pipeline=pipeline,
        resource=resource,
        watermark=new_ts.isoformat(),
        last_id=new_id,
    )


def compute_watermark(batch: list[dict]) -> tuple[datetime, int] | None:
    """Return MAX(updated_at, id) from a batch, or None if batch has no valid timestamps."""
    valid: list[tuple[datetime, int]] = []
    for r in batch:
        ts_raw = r.get("updated_at")
        r_id = r.get("id", 0) or 0
        if ts_raw:
            try:
                ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                valid.append((ts, r_id))
            except (ValueError, TypeError):
                pass
    if not valid:
        return None
    return max(valid)
