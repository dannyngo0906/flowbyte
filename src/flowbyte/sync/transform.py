"""Transform Haravan JSON records to destination DB rows.

Pipeline: flatten top-level + nested objects → apply rename/skip/type_override → _raw backup.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from flowbyte.config.models import TransformConfig
from flowbyte.haravan.exceptions import InvalidRecordError
from flowbyte.logging import EventName, get_logger

log = get_logger()

# ── Flattening rules per resource ─────────────────────────────────────────────

_FLATTENING_RULES: dict[str, dict] = {
    "orders": {
        "flat_fields": [
            "id", "order_number", "name", "email", "financial_status",
            "fulfillment_status", "total_price", "subtotal_price", "total_tax",
            "currency", "customer_id", "created_at", "updated_at",
            "cancelled_at", "closed_at",
        ],
        "nested_flatten": {
            "customer": ["email", "phone"],            # → customer_email, customer_phone
            "shipping_address": ["city", "province", "country_code", "phone"],
        },
        "nested_jsonb": [
            "transactions", "tax_lines", "discount_codes", "fulfillments", "note_attributes"
        ],
        "nested_prefix": {
            "shipping_address": "shipping",
            "customer": "customer",
        },
    },
    "order_line_items": {
        "flat_fields": [
            "id", "order_id", "product_id", "variant_id", "sku",
            "title", "quantity", "price", "total_discount",
        ],
        "nested_flatten": {},
        "nested_jsonb": [],
        "nested_prefix": {},
    },
    "customers": {
        "flat_fields": [
            "id", "email", "phone", "first_name", "last_name",
            "total_spent", "orders_count", "accepts_marketing", "tags",
            "created_at", "updated_at",
        ],
        "nested_flatten": {},
        "nested_jsonb": [],
        "nested_prefix": {},
    },
    "products": {
        "flat_fields": [
            "id", "title", "vendor", "product_type", "handle",
            "tags", "status", "created_at", "updated_at", "published_at",
        ],
        "nested_flatten": {},
        "nested_jsonb": [],
        "nested_prefix": {},
    },
    "variants": {
        "flat_fields": [
            "id", "product_id", "sku", "title", "price", "compare_at_price",
            "inventory_item_id", "inventory_quantity", "created_at", "updated_at",
        ],
        "nested_flatten": {},
        "nested_jsonb": [],
        "nested_prefix": {},
    },
    "inventory_levels": {
        "flat_fields": ["inventory_item_id", "location_id", "available", "updated_at"],
        "nested_flatten": {},
        "nested_jsonb": [],
        "nested_prefix": {},
    },
    "locations": {
        "flat_fields": [
            "id", "name", "address1", "address2", "city", "province",
            "country_code", "phone", "active", "created_at", "updated_at",
        ],
        "nested_flatten": {},
        "nested_jsonb": [],
        "nested_prefix": {},
    },
}


def apply_transform(
    record: dict,
    transform_config: TransformConfig,
    resource: str,
) -> dict:
    """Pure function: Haravan JSON dict → destination row dict.

    Raises InvalidRecordError if id is None.
    Logs and skips (returns None) on type_override failure per field.
    """
    if not record.get("id") and resource not in ("inventory_levels",):
        raise InvalidRecordError(f"Record missing id in resource {resource!r}")

    rules = _FLATTENING_RULES.get(resource, {
        "flat_fields": list(record.keys()),
        "nested_flatten": {},
        "nested_jsonb": [],
        "nested_prefix": {},
    })

    out: dict[str, Any] = {"_raw": record}

    # ── Flat fields ───────────────────────────────────────────────────────────
    for field in rules["flat_fields"]:
        if field in transform_config.skip:
            continue
        target = transform_config.rename.get(field, field)
        value = record.get(field)

        if field in transform_config.type_override and value is not None:
            value = _cast(value, transform_config.type_override[field], field)

        out[target] = value

    # ── Nested → flattened columns ────────────────────────────────────────────
    prefix_map = rules.get("nested_prefix", {})
    for parent, children in rules["nested_flatten"].items():
        nested = record.get(parent) or {}
        prefix = prefix_map.get(parent, parent)
        for child in children:
            col_name = f"{prefix}_{child}"
            if col_name not in transform_config.skip:
                out[col_name] = nested.get(child)

    # ── Nested arrays → JSONB columns ─────────────────────────────────────────
    for field in rules["nested_jsonb"]:
        if field in transform_config.skip:
            continue
        target = transform_config.rename.get(field, field)
        out[target] = record.get(field)

    return out


def _cast(value: Any, pg_type: str, field_name: str) -> Any:
    """Attempt type conversion for type_override. Returns original if fails."""
    pg_type = pg_type.lower().strip()
    try:
        if pg_type.startswith("numeric") or pg_type in ("decimal", "float", "double precision"):
            return Decimal(str(value))
        if pg_type in ("integer", "int", "bigint", "smallint"):
            return int(value)
        if pg_type == "boolean":
            if isinstance(value, bool):
                return value
            return str(value).lower() in ("true", "1", "yes")
        if "timestamp" in pg_type:
            if isinstance(value, datetime):
                return value
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc)
    except (ValueError, InvalidOperation, TypeError) as e:
        log.warning(EventName.TYPE_OVERRIDE_FAILED, field=field_name, type=pg_type, error=str(e))
    return None
