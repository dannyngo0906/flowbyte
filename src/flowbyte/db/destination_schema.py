"""SQLAlchemy Table definitions for the destination DB (data plane).

Tables are auto-created by DestinationSchemaManager on first sync.
NOT managed by Alembic — schema is owned by the SyncRunner.
"""
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

destination_metadata = MetaData()

# ── Common meta columns added to every data table ────────────────────────────

def _meta_columns() -> list[Column]:
    return [
        Column("_raw", JSONB, nullable=False),
        Column("_synced_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
        Column("_deleted_at", DateTime(timezone=True)),
        Column("_sync_id", UUID(as_uuid=True), nullable=False),
    ]


# ── orders ────────────────────────────────────────────────────────────────────

orders = Table(
    "orders",
    destination_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("order_number", String(32)),
    Column("name", String(64)),
    Column("email", String(256)),
    Column("financial_status", String(32)),
    Column("fulfillment_status", String(32)),
    Column("total_price", Numeric(12, 2)),
    Column("subtotal_price", Numeric(12, 2)),
    Column("total_tax", Numeric(12, 2)),
    Column("currency", String(8)),
    Column("customer_id", BigInteger),
    # flattened shipping_address
    Column("shipping_city", String(128)),
    Column("shipping_province", String(128)),
    Column("shipping_country_code", String(8)),
    Column("shipping_phone", String(32)),
    # flattened customer
    Column("customer_email", String(256)),
    Column("customer_phone", String(32)),
    # nested arrays as JSONB
    Column("transactions", JSONB),
    Column("tax_lines", JSONB),
    Column("discount_codes", JSONB),
    Column("fulfillments", JSONB),
    Column("note_attributes", JSONB),
    # timestamps
    Column("created_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
    Column("cancelled_at", DateTime(timezone=True)),
    Column("closed_at", DateTime(timezone=True)),
    *_meta_columns(),
    Index("idx_orders_customer", "customer_id"),
    Index("idx_orders_updated", "updated_at"),
    Index("idx_orders_active", "id", postgresql_where="_deleted_at IS NULL"),
    Index("idx_orders_sync_id_active", "_sync_id", postgresql_where="_deleted_at IS NULL"),
)

# ── order_line_items ──────────────────────────────────────────────────────────

order_line_items = Table(
    "order_line_items",
    destination_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("order_id", BigInteger, nullable=False),
    Column("product_id", BigInteger),
    Column("variant_id", BigInteger),
    Column("sku", String(128)),
    Column("title", Text),
    Column("quantity", Integer),
    Column("price", Numeric(12, 2)),
    Column("total_discount", Numeric(12, 2)),
    Column("_raw", JSONB, nullable=False),
    Column("_synced_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
    Column("_sync_id", UUID(as_uuid=True), nullable=False),
    Index("idx_order_items_order", "order_id"),
    Index("idx_order_items_product", "product_id"),
    Index("idx_order_items_variant", "variant_id"),
)

# ── customers ─────────────────────────────────────────────────────────────────

customers = Table(
    "customers",
    destination_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("email", String(256)),
    Column("phone", String(32)),
    Column("first_name", String(128)),
    Column("last_name", String(128)),
    Column("total_spent", Numeric(12, 2)),
    Column("orders_count", Integer),
    Column("accepts_marketing", Boolean),
    Column("tags", Text),
    Column("created_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
    *_meta_columns(),
    Index("idx_customers_email", "email"),
    Index("idx_customers_updated", "updated_at"),
    Index("idx_customers_sync_id_active", "_sync_id", postgresql_where="_deleted_at IS NULL"),
)

# ── products ──────────────────────────────────────────────────────────────────

products = Table(
    "products",
    destination_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("title", Text),
    Column("vendor", String(128)),
    Column("product_type", String(128)),
    Column("handle", String(256)),
    Column("tags", Text),
    Column("status", String(32)),
    Column("created_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
    Column("published_at", DateTime(timezone=True)),
    *_meta_columns(),
    Index("idx_products_updated", "updated_at"),
    Index("idx_products_vendor", "vendor"),
    Index("idx_products_sync_id_active", "_sync_id", postgresql_where="_deleted_at IS NULL"),
)

# ── variants ──────────────────────────────────────────────────────────────────

variants = Table(
    "variants",
    destination_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("product_id", BigInteger, nullable=False),
    Column("sku", String(128)),
    Column("title", Text),
    Column("price", Numeric(12, 2)),
    Column("compare_at_price", Numeric(12, 2)),
    Column("inventory_item_id", BigInteger),
    Column("inventory_quantity", Integer),
    Column("created_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
    Column("_raw", JSONB, nullable=False),
    Column("_synced_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
    Column("_sync_id", UUID(as_uuid=True), nullable=False),
    Index("idx_variants_product", "product_id"),
    Index("idx_variants_sku", "sku"),
    Index("idx_variants_inv_item", "inventory_item_id"),
)

# ── inventory_levels ──────────────────────────────────────────────────────────

inventory_levels = Table(
    "inventory_levels",
    destination_metadata,
    Column("inventory_item_id", BigInteger, primary_key=True),
    Column("location_id", BigInteger, primary_key=True),
    Column("available", Integer),
    Column("updated_at", DateTime(timezone=True)),
    Column("_raw", JSONB, nullable=False),
    Column("_synced_at", DateTime(timezone=True), nullable=False, server_default="NOW()"),
    Column("_sync_id", UUID(as_uuid=True), nullable=False),
)

# ── locations ─────────────────────────────────────────────────────────────────

locations = Table(
    "locations",
    destination_metadata,
    Column("id", BigInteger, primary_key=True),
    Column("name", String(256)),
    Column("address1", Text),
    Column("address2", Text),
    Column("city", String(128)),
    Column("province", String(128)),
    Column("country_code", String(8)),
    Column("phone", String(32)),
    Column("active", Boolean),
    Column("created_at", DateTime(timezone=True)),
    Column("updated_at", DateTime(timezone=True)),
    *_meta_columns(),
    Index("idx_locations_sync_id_active", "_sync_id", postgresql_where="_deleted_at IS NULL"),
)

# ── Registry: resource name → Table ──────────────────────────────────────────

DESTINATION_TABLES: dict[str, Table] = {
    "orders": orders,
    "order_line_items": order_line_items,
    "customers": customers,
    "products": products,
    "variants": variants,
    "inventory_levels": inventory_levels,
    "locations": locations,
}


def get_table(resource: str) -> Table:
    try:
        return DESTINATION_TABLES[resource]
    except KeyError:
        raise ValueError(f"Unknown resource: {resource!r}. Known: {list(DESTINATION_TABLES)}")
