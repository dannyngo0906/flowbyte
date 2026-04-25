"""SQLAlchemy Table definitions for the destination DB (data plane).

Tables are auto-created by DestinationSchemaManager on first sync.
NOT managed by Alembic — schema is owned by the SyncRunner.

Raw EL Strategy: each table stores the full Haravan API record in _raw JSONB.
No flatten columns. dbt handles transformation in a later phase.
"""
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    MetaData,
    Table,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

destination_metadata = MetaData()


# ── orders ────────────────────────────────────────────────────────────────────

orders = Table(
    "orders",
    destination_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("_raw", JSONB, nullable=False),
    Column("_synced_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
    Column("_deleted_at", DateTime(timezone=True)),
    Column("_sync_id", UUID(as_uuid=True), nullable=True),
    Index("idx_orders_updated_at", "updated_at"),
    Index("idx_orders_active", "id", postgresql_where="_deleted_at IS NULL"),
)

# ── customers ─────────────────────────────────────────────────────────────────

customers = Table(
    "customers",
    destination_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("_raw", JSONB, nullable=False),
    Column("_synced_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
    Column("_deleted_at", DateTime(timezone=True)),
    Column("_sync_id", UUID(as_uuid=True), nullable=True),
    Index("idx_customers_updated_at", "updated_at"),
    Index("idx_customers_active", "id", postgresql_where="_deleted_at IS NULL"),
)

# ── products ──────────────────────────────────────────────────────────────────
# variants[] stay nested inside _raw — dbt will unnest via jsonb_array_elements

products = Table(
    "products",
    destination_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("_raw", JSONB, nullable=False),
    Column("_synced_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
    Column("_deleted_at", DateTime(timezone=True)),
    Column("_sync_id", UUID(as_uuid=True), nullable=True),
    Index("idx_products_updated_at", "updated_at"),
    Index("idx_products_active", "id", postgresql_where="_deleted_at IS NULL"),
)

# ── inventory_levels ──────────────────────────────────────────────────────────
# Composite PK; no _deleted_at (no soft-delete for inventory levels)
# updated_at nullable — Haravan may omit it

inventory_levels = Table(
    "inventory_levels",
    destination_metadata,
    Column("inventory_item_id", BigInteger, primary_key=True),
    Column("location_id", BigInteger, primary_key=True),
    Column("updated_at", DateTime(timezone=True), nullable=True),
    Column("_raw", JSONB, nullable=False),
    Column("_synced_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
    Column("_sync_id", UUID(as_uuid=True), nullable=True),
)

# ── locations ─────────────────────────────────────────────────────────────────

locations = Table(
    "locations",
    destination_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("updated_at", DateTime(timezone=True), nullable=True),
    Column("_raw", JSONB, nullable=False),
    Column("_synced_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
    Column("_deleted_at", DateTime(timezone=True)),
    Column("_sync_id", UUID(as_uuid=True), nullable=True),
    Index("idx_locations_updated_at", "updated_at"),
    Index("idx_locations_active", "id", postgresql_where="_deleted_at IS NULL"),
)

# ── Registry: resource name → Table ──────────────────────────────────────────

DESTINATION_TABLES: dict[str, Table] = {
    "orders": orders,
    "customers": customers,
    "products": products,
    "inventory_levels": inventory_levels,
    "locations": locations,
}


def get_table(resource: str) -> Table:
    try:
        return DESTINATION_TABLES[resource]
    except KeyError:
        raise ValueError(f"Unknown resource: {resource!r}. Known: {list(DESTINATION_TABLES)}")
