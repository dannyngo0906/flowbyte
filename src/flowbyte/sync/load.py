"""Upsert loader: batch INSERT ... ON CONFLICT DO UPDATE into destination tables."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import Table, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection

from flowbyte.logging import EventName, get_logger

log = get_logger()

_BATCH_SIZE = 500


@dataclass
class LoadStats:
    upserted: int = 0
    skipped: int = 0
    errors: int = 0


def upsert_batch(
    conn: Connection,
    table: Table,
    records: list[dict],
    sync_id: UUID | str,
    primary_key: str | list[str] = "id",
) -> LoadStats:
    """INSERT ... ON CONFLICT DO UPDATE for all records in batches of 500.

    Idempotent: safe to re-run with overlapping records (checkpoint overlap).
    """
    if not records:
        return LoadStats()

    now = datetime.now(timezone.utc)
    stats = LoadStats()

    pk_cols = [primary_key] if isinstance(primary_key, str) else primary_key

    for i in range(0, len(records), _BATCH_SIZE):
        chunk = records[i : i + _BATCH_SIZE]
        for r in chunk:
            r["_sync_id"] = sync_id
            r["_synced_at"] = now

        try:
            stmt = insert(table).values(chunk)
            update_cols = {
                c.name: stmt.excluded[c.name]
                for c in table.c
                if c.name not in pk_cols
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=pk_cols,
                set_=update_cols,
            )
            conn.execute(stmt)
            stats.upserted += len(chunk)
            log.debug(
                EventName.UPSERT_BATCH_DONE,
                table=table.name,
                batch_size=len(chunk),
                offset=i,
            )
        except Exception as e:
            log.error(EventName.UPSERT_CONFLICT, table=table.name, error=str(e), exc_info=True)
            stats.errors += len(chunk)

    return stats


def count_rows(conn: Connection, table: Table) -> int:
    return conn.execute(select(func.count()).select_from(table)).scalar() or 0


def count_active_rows(conn: Connection, table: Table) -> int:
    """Count rows where _deleted_at IS NULL (if column exists)."""
    if "_deleted_at" not in {c.name for c in table.c}:
        return count_rows(conn, table)
    return (
        conn.execute(
            select(func.count()).select_from(table).where(table.c._deleted_at.is_(None))
        ).scalar()
        or 0
    )


def sweep_soft_deletes(
    conn: Connection,
    table: Table,
    current_sync_id: UUID | str,
) -> int:
    """Mark records not touched by current sync as soft-deleted.

    Only call after full paginate() completes successfully (§17.1).
    """
    if "_deleted_at" not in {c.name for c in table.c}:
        return 0
    if "_sync_id" not in {c.name for c in table.c}:
        return 0

    result = conn.execute(
        table.update()
        .where(table.c._sync_id != str(current_sync_id))
        .where(table.c._deleted_at.is_(None))
        .values(_deleted_at=func.now())
    )
    return result.rowcount
