"""Transform Haravan JSON records to destination DB rows.

Raw EL Strategy: extract PK/index fields + store entire record in _raw JSONB.
No flatten, no rename, no type_override. dbt handles transformation in a later phase.
"""
from __future__ import annotations

from datetime import UTC, datetime

from flowbyte.logging import get_logger

log = get_logger()


def apply_transform(record: dict, resource: str) -> dict | None:
    """Raw EL: extract PK fields and store entire record in _raw.

    Returns None (and logs a warning) when a non-inventory record has no id.
    """
    if record.get("id") is None and resource not in {"inventory_levels"}:
        log.warning("transform_skip_no_id", resource=resource, keys=list(record.keys()))
        return None

    row: dict = {
        "_raw": record,
        "_synced_at": datetime.now(UTC),
    }

    if "id" in record:
        row["id"] = record["id"]
    if "updated_at" in record:
        row["updated_at"] = record["updated_at"]

    if resource == "inventory_levels":
        row["inventory_item_id"] = record.get("inventory_item_id")
        row["location_id"] = record.get("location_id")

    return row
