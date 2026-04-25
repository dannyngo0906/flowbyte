"""Integration tests for US-004: E2E sync for customers, products/variants, inventory_levels, locations.

Haravan HTTP is mocked via MagicMock (paginate / get return controlled data).
Internal + destination DBs are real PostgreSQL via testcontainers (conftest).
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import select, text

from flowbyte.config.models import PipelineConfig, PostgresDestConfig, SyncJobSpec
from flowbyte.db.internal_schema import sync_checkpoints
from flowbyte.haravan.exceptions import HaravanRateLimited
from flowbyte.sync.runner import SyncRunner

pytestmark = pytest.mark.integration

_SHOP = "test_shop"


# ── Helper factories ──────────────────────────────────────────────────────────


def _make_customer(
    customer_id: int,
    updated_at: str = "2026-01-01T10:00:00+00:00",
    **kwargs: object,
) -> dict:
    base: dict = {
        "id": customer_id,
        "email": f"customer{customer_id}@example.com",
        "phone": f"+8490{customer_id:07d}",
        "first_name": f"First{customer_id}",
        "last_name": f"Last{customer_id}",
        "total_spent": "500.00",
        "orders_count": 2,
        "accepts_marketing": True,
        "tags": "",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": updated_at,
    }
    base.update(kwargs)
    return base


def _make_product(
    product_id: int,
    updated_at: str = "2026-01-01T10:00:00+00:00",
    **kwargs: object,
) -> dict:
    """Returns a product with 1 nested variant (matching Haravan /products.json payload)."""
    base: dict = {
        "id": product_id,
        "title": f"Product {product_id}",
        "vendor": "Test Vendor",
        "product_type": "Type A",
        "handle": f"product-{product_id}",
        "tags": "test",
        "status": "active",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": updated_at,
        "published_at": "2026-01-01T00:00:00+00:00",
        "variants": [
            {
                "id": product_id * 100 + 1,
                "product_id": product_id,
                "sku": f"SKU-{product_id}-1",
                "title": "Default Title",
                "price": "99.00",
                "compare_at_price": None,
                "inventory_item_id": product_id * 1000,
                "inventory_quantity": 10,
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": updated_at,
            }
        ],
    }
    base.update(kwargs)
    return base


def _make_inventory_level(
    inventory_item_id: int,
    location_id: int,
    available: int = 10,
) -> dict:
    return {
        "inventory_item_id": inventory_item_id,
        "location_id": location_id,
        "available": available,
        "updated_at": "2026-01-01T00:00:00+00:00",
    }


def _make_location(location_id: int, active: bool = True) -> dict:
    return {
        "id": location_id,
        "name": f"Location {location_id}",
        "address1": f"{location_id} Test Street",
        "address2": None,
        "city": "Ho Chi Minh",
        "province": "Ho Chi Minh",
        "country_code": "VN",
        "phone": "",
        "active": active,
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }


# ── Shared helpers ────────────────────────────────────────────────────────────


def _make_cfg() -> PipelineConfig:
    return PipelineConfig(
        name=_SHOP,
        haravan_credentials_ref="test-creds",
        haravan_shop_domain="test.myharavan.com",
        destination=PostgresDestConfig(
            host="localhost",
            port=5432,
            user="test",
            database="test",
            credentials_ref="test-db-ref",
        ),
    )


def _spec(mode: str, resource: str) -> SyncJobSpec:
    return SyncJobSpec(pipeline=_SHOP, resource=resource, mode=mode, trigger="test")


def _make_runner(
    internal_engine,
    dest_engine,
    records: list[dict],
    resource: str = "customers",
    client_get_response: dict | None = None,
) -> SyncRunner:
    client = MagicMock()
    client.paginate.return_value = iter(records)
    if client_get_response is not None:
        client.get.return_value = client_get_response
    return SyncRunner(_make_cfg(), client, internal_engine, dest_engine)


# ── TestCustomersSync ─────────────────────────────────────────────────────────


class TestCustomersSync:
    @pytest.fixture(autouse=True)
    def clean_tables(self, internal_engine, dest_engine):
        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE sync_checkpoints, sync_runs CASCADE"))
        with dest_engine.begin() as conn:
            conn.execute(text("TRUNCATE customers CASCADE"))
        yield

    def test_first_sync_inserts_all_customers(self, internal_engine, dest_engine):
        records = [_make_customer(i) for i in range(1, 6)]
        result = _make_runner(internal_engine, dest_engine, records).run(
            _spec("full_refresh", "customers")
        )

        assert result.status == "success"
        assert result.fetched_count == 5
        with dest_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
        assert count == 5

    def test_incremental_adds_new_customers(self, internal_engine, dest_engine):
        run1 = [_make_customer(i, f"2026-01-{i:02d}T10:00:00+00:00") for i in range(1, 6)]
        _make_runner(internal_engine, dest_engine, run1).run(_spec("full_refresh", "customers"))

        run2 = [_make_customer(6, "2026-01-10T10:00:00+00:00")]
        result = _make_runner(internal_engine, dest_engine, run2).run(
            _spec("incremental", "customers")
        )

        assert result.status == "success"
        with dest_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
        assert count == 6

    def test_updated_customer_is_upserted(self, internal_engine, dest_engine):
        _make_runner(internal_engine, dest_engine, [_make_customer(1)]).run(
            _spec("full_refresh", "customers")
        )

        updated = [_make_customer(1, email="new@example.com")]
        _make_runner(internal_engine, dest_engine, updated).run(
            _spec("full_refresh", "customers")
        )

        with dest_engine.connect() as conn:
            row = conn.execute(text("SELECT email FROM customers WHERE id = 1")).one()
        assert row.email == "new@example.com"

    def test_checkpoint_saved_after_customers_sync(self, internal_engine, dest_engine):
        records = [_make_customer(1, "2026-02-01T10:00:00+00:00")]
        _make_runner(internal_engine, dest_engine, records).run(
            _spec("full_refresh", "customers")
        )

        with internal_engine.connect() as conn:
            row = conn.execute(
                select(sync_checkpoints).where(
                    (sync_checkpoints.c.pipeline == _SHOP)
                    & (sync_checkpoints.c.resource == "customers")
                )
            ).one_or_none()
        assert row is not None
        assert row.last_id == 1


# ── TestProductsAndVariantsSync ───────────────────────────────────────────────


class TestProductsAndVariantsSync:
    @pytest.fixture(autouse=True)
    def clean_tables(self, internal_engine, dest_engine):
        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE sync_checkpoints, sync_runs CASCADE"))
        with dest_engine.begin() as conn:
            conn.execute(text("TRUNCATE variants, products CASCADE"))
        yield

    def _runner(self, internal_engine, dest_engine, products: list[dict]) -> SyncRunner:
        client = MagicMock()
        client.paginate.return_value = iter(products)
        return SyncRunner(_make_cfg(), client, internal_engine, dest_engine)

    def test_first_sync_inserts_products_and_variants(self, internal_engine, dest_engine):
        products = [_make_product(i) for i in range(1, 4)]
        result = self._runner(internal_engine, dest_engine, products).run(
            _spec("full_refresh", "products")
        )

        assert result.status == "success"
        with dest_engine.connect() as conn:
            p_count = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
            v_count = conn.execute(text("SELECT COUNT(*) FROM variants")).scalar()
        assert p_count == 3
        assert v_count == 3  # 1 variant per product

    def test_variants_linked_to_product_id(self, internal_engine, dest_engine):
        products = [_make_product(10)]
        self._runner(internal_engine, dest_engine, products).run(
            _spec("full_refresh", "products")
        )

        with dest_engine.connect() as conn:
            row = conn.execute(text("SELECT product_id FROM variants WHERE id = 1001")).one()
        assert row.product_id == 10

    def test_no_duplicate_products_on_repeated_sync(self, internal_engine, dest_engine):
        products = [_make_product(i) for i in range(1, 4)]
        for _ in range(2):
            self._runner(internal_engine, dest_engine, products).run(
                _spec("full_refresh", "products")
            )

        with dest_engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
            distinct = conn.execute(text("SELECT COUNT(DISTINCT id) FROM products")).scalar()
        assert total == distinct == 3

    def test_full_refresh_soft_deletes_missing_products(self, internal_engine, dest_engine):
        # Seed 21 products so removing 1 = 1/21 ≈ 4.8% < 5% sanity guard
        seed = [_make_product(i) for i in range(1, 22)]
        self._runner(internal_engine, dest_engine, seed).run(_spec("full_refresh", "products"))

        run2 = [_make_product(i) for i in range(1, 21)]  # product 21 absent
        result = self._runner(internal_engine, dest_engine, run2).run(
            _spec("full_refresh", "products")
        )

        assert result.status == "success"
        assert result.soft_deleted_count == 1
        with dest_engine.connect() as conn:
            deleted = conn.execute(
                text("SELECT COUNT(*) FROM products WHERE _deleted_at IS NOT NULL")
            ).scalar()
        assert deleted == 1

    def test_variants_not_soft_deleted_directly(self, internal_engine, dest_engine):
        """Variants table has no _deleted_at — sweep returns 0 for variants."""
        seed = [_make_product(i) for i in range(1, 22)]
        self._runner(internal_engine, dest_engine, seed).run(_spec("full_refresh", "products"))

        run2 = [_make_product(i) for i in range(1, 21)]
        self._runner(internal_engine, dest_engine, run2).run(_spec("full_refresh", "products"))

        with dest_engine.connect() as conn:
            # Variants for all 21 products still present (no cascade soft delete)
            v_count = conn.execute(text("SELECT COUNT(*) FROM variants")).scalar()
        assert v_count == 21


# ── TestInventoryLevelsSync ───────────────────────────────────────────────────


class TestInventoryLevelsSync:
    @pytest.fixture(autouse=True)
    def clean_tables(self, internal_engine, dest_engine):
        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE sync_checkpoints, sync_runs CASCADE"))
        with dest_engine.begin() as conn:
            conn.execute(text("TRUNCATE inventory_levels, products CASCADE"))
        # Seed 1 product with variant so _get_variant_ids() → [10001]
        with dest_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO products (id, updated_at, _raw, _synced_at) "
                    "VALUES (1, NOW(), CAST(:raw AS jsonb), NOW())"
                ),
                {"raw": '{"variants":[{"id":10001}]}'},
            )
        yield

    def _runner(
        self,
        internal_engine,
        dest_engine,
        inventory_records: list[dict],
        location_ids: list[int] | None = None,
    ) -> SyncRunner:
        """Mock client.get for locations + inventory_locations endpoint."""
        loc_ids = location_ids or [501]
        client = MagicMock()

        def mock_get(path, **kwargs):
            if "inventory_locations" in path:
                return {"inventory_locations": inventory_records}
            return {"locations": [{"id": lid} for lid in loc_ids]}

        client.get.side_effect = mock_get
        return SyncRunner(_make_cfg(), client, internal_engine, dest_engine)

    def test_full_refresh_inserts_inventory_levels(self, internal_engine, dest_engine):
        records = [
            _make_inventory_level(1001, 501, available=50),
            _make_inventory_level(1002, 501, available=30),
        ]
        result = self._runner(internal_engine, dest_engine, records).run(
            _spec("full_refresh", "inventory_levels")
        )

        assert result.status == "success"
        assert result.fetched_count == 2
        with dest_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM inventory_levels")).scalar()
        assert count == 2

    def test_composite_pk_upsert_no_duplicate(self, internal_engine, dest_engine):
        records = [_make_inventory_level(1001, 501, available=50)]
        for _ in range(2):
            self._runner(internal_engine, dest_engine, records).run(
                _spec("full_refresh", "inventory_levels")
            )

        with dest_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM inventory_levels")).scalar()
        assert count == 1

    def test_updated_inventory_is_reflected(self, internal_engine, dest_engine):
        self._runner(
            internal_engine, dest_engine, [_make_inventory_level(1001, 501, available=50)]
        ).run(_spec("full_refresh", "inventory_levels"))

        self._runner(
            internal_engine, dest_engine, [_make_inventory_level(1001, 501, available=99)]
        ).run(_spec("full_refresh", "inventory_levels"))

        with dest_engine.connect() as conn:
            row = conn.execute(
                text("SELECT (_raw->>'available')::int FROM inventory_levels WHERE inventory_item_id=1001")
            ).one()
        assert row[0] == 99


# ── TestLocationsSync ─────────────────────────────────────────────────────────


class TestLocationsSync:
    @pytest.fixture(autouse=True)
    def clean_tables(self, internal_engine, dest_engine):
        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE sync_checkpoints, sync_runs CASCADE"))
        with dest_engine.begin() as conn:
            conn.execute(text("TRUNCATE locations CASCADE"))
        yield

    def _runner(self, internal_engine, dest_engine, locations: list[dict]) -> SyncRunner:
        client = MagicMock()
        client.get.return_value = {"locations": locations}
        return SyncRunner(_make_cfg(), client, internal_engine, dest_engine)

    def test_first_sync_inserts_locations(self, internal_engine, dest_engine):
        locations = [_make_location(i) for i in range(501, 504)]
        result = self._runner(internal_engine, dest_engine, locations).run(
            _spec("full_refresh", "locations")
        )

        assert result.status == "success"
        assert result.fetched_count == 3
        with dest_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM locations")).scalar()
        assert count == 3

    def test_full_refresh_soft_deletes_missing_location(self, internal_engine, dest_engine):
        # 21 locations → remove 1 = 1/21 ≈ 4.8% < 5% sanity guard
        seed = [_make_location(500 + i) for i in range(1, 22)]
        self._runner(internal_engine, dest_engine, seed).run(_spec("full_refresh", "locations"))

        run2 = [_make_location(500 + i) for i in range(1, 21)]  # location 521 absent
        result = self._runner(internal_engine, dest_engine, run2).run(
            _spec("full_refresh", "locations")
        )

        assert result.status == "success"
        assert result.soft_deleted_count == 1
        with dest_engine.connect() as conn:
            deleted = conn.execute(
                text("SELECT COUNT(*) FROM locations WHERE _deleted_at IS NOT NULL")
            ).scalar()
        assert deleted == 1


# ── TestRateLimitFailure ──────────────────────────────────────────────────────


class TestRateLimitFailure:
    @pytest.fixture(autouse=True)
    def clean_tables(self, internal_engine, dest_engine):
        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE sync_checkpoints, sync_runs CASCADE"))
        with dest_engine.begin() as conn:
            conn.execute(text("TRUNCATE orders CASCADE"))
        yield

    def test_runner_fails_gracefully_on_rate_limit_error(self, internal_engine, dest_engine):
        client = MagicMock()
        client.paginate.side_effect = HaravanRateLimited(retry_after=0.0)
        runner = SyncRunner(_make_cfg(), client, internal_engine, dest_engine)
        result = runner.run(_spec("incremental", "orders"))

        assert result.status == "failed"
        assert result.error is not None
        assert "Rate limited" in result.error

    def test_checkpoint_not_saved_on_rate_limit_error(self, internal_engine, dest_engine):
        client = MagicMock()
        client.paginate.side_effect = HaravanRateLimited(retry_after=0.0)
        runner = SyncRunner(_make_cfg(), client, internal_engine, dest_engine)
        runner.run(_spec("incremental", "orders"))

        with internal_engine.connect() as conn:
            row = conn.execute(
                select(sync_checkpoints).where(
                    (sync_checkpoints.c.pipeline == _SHOP)
                    & (sync_checkpoints.c.resource == "orders")
                )
            ).one_or_none()
        assert row is None
