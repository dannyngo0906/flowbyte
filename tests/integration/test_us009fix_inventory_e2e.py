"""E2E tests for US-009-FIX: inventory_levels endpoint fix.

Mô phỏng end-user flow từ bước 1 đến bước cuối:
  Bước 1: Sync products → variant IDs lưu trong products._raw['variants'][]['id']
  Bước 2: Sync inventory_levels → gọi /inventory_locations.json (endpoint đúng)
  Bước cuối: SELECT COUNT(*) FROM inventory_levels > 0

Done-when (PRD US-009-FIX):
  ✅ T-9F.1  Endpoint /inventory_locations.json thay vì /inventory_levels.json
  ✅ T-9F.3  Batch 200 variant IDs/request
  ✅ T-9F.4  Graceful skip khi products chưa sync (variant_ids = [])
  ✅ T-9F.8  SELECT COUNT(*) FROM inventory_levels > 0 sau sync
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

from flowbyte.config.models import PipelineConfig, PostgresDestConfig, SyncJobSpec
from flowbyte.sync.runner import SyncRunner

pytestmark = pytest.mark.integration

_SHOP = "shop_e2e_inventory"


# ── Helpers ───────────────────────────────────────────────────────────────────


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


def _spec(resource: str, mode: str = "full_refresh") -> SyncJobSpec:
    return SyncJobSpec(pipeline=_SHOP, resource=resource, mode=mode, trigger="test")


def _make_product(product_id: int, variant_ids: list[int] | None = None) -> dict:
    ids = variant_ids or [product_id * 100 + 1]
    variants = [
        {
            "id": vid,
            "product_id": product_id,
            "sku": f"SKU-{product_id}-{i}",
            "title": f"Variant {i}",
            "price": "99.00",
            "compare_at_price": None,
            "inventory_item_id": vid * 10,
            "inventory_quantity": 5,
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T10:00:00+00:00",
        }
        for i, vid in enumerate(ids, start=1)
    ]
    return {
        "id": product_id,
        "title": f"Product {product_id}",
        "vendor": "Vendor",
        "product_type": "Type",
        "handle": f"product-{product_id}",
        "tags": "",
        "status": "active",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T10:00:00+00:00",
        "published_at": "2026-01-01T00:00:00+00:00",
        "variants": variants,
    }


def _make_inventory(inventory_item_id: int, location_id: int, available: int = 10) -> dict:
    return {
        "inventory_item_id": inventory_item_id,
        "location_id": location_id,
        "available": available,
        "updated_at": "2026-01-01T00:00:00+00:00",
    }


def _products_runner(
    internal_engine, dest_engine, products: list[dict]
) -> SyncRunner:
    client = MagicMock()
    client.paginate.return_value = iter(products)
    return SyncRunner(_make_cfg(), client, internal_engine, dest_engine)


def _inventory_runner(
    internal_engine,
    dest_engine,
    inventory_records: list[dict],
    location_ids: list[int] | None = None,
) -> SyncRunner:
    """Mock client.get: dispatch by path — locations vs inventory_locations."""
    loc_ids = location_ids or [501]
    client = MagicMock()

    def mock_get(path, **kwargs):
        if "inventory_locations" in path:
            return {"inventory_locations": inventory_records}
        return {"locations": [{"id": lid} for lid in loc_ids]}

    client.get.side_effect = mock_get
    return SyncRunner(_make_cfg(), client, internal_engine, dest_engine)


_ALLOWED_TABLES = frozenset({"inventory_levels", "products", "variants"})


def _count_rows(engine, table_name: str) -> int:
    if table_name not in _ALLOWED_TABLES:
        raise ValueError(f"table not in allowlist: {table_name!r}")
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()


# ── TestInventoryEndpointFixE2E ───────────────────────────────────────────────


class TestInventoryEndpointFixE2E:
    """Full pipeline: products sync → inventory sync → verify count > 0."""

    @pytest.fixture(autouse=True)
    def clean_tables(self, internal_engine, dest_engine):
        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE sync_checkpoints, sync_runs CASCADE"))
        with dest_engine.begin() as conn:
            conn.execute(text("TRUNCATE inventory_levels, variants, products CASCADE"))
        yield
        with internal_engine.begin() as conn:
            conn.execute(text("TRUNCATE sync_checkpoints, sync_runs CASCADE"))
        with dest_engine.begin() as conn:
            conn.execute(text("TRUNCATE inventory_levels, variants, products CASCADE"))

    # ── Bước 1 → Bước cuối ───────────────────────────────────────────────────

    def test_full_pipeline_inventory_count_gt_0(self, internal_engine, dest_engine):
        """Bước 1: sync products. Bước 2: sync inventory. Bước cuối: count > 0."""
        # Bước 1 — sync products (variant IDs embedded in _raw)
        products = [_make_product(1), _make_product(2)]
        p_result = _products_runner(internal_engine, dest_engine, products).run(
            _spec("products")
        )
        assert p_result.status == "success"
        assert p_result.fetched_count == 2

        # Bước 2 — sync inventory_levels
        variant_1 = 1 * 100 + 1  # 101
        variant_2 = 2 * 100 + 1  # 201
        inventory = [
            _make_inventory(variant_1, 501, available=15),
            _make_inventory(variant_2, 501, available=8),
        ]
        inv_result = _inventory_runner(internal_engine, dest_engine, inventory).run(
            _spec("inventory_levels")
        )
        assert inv_result.status == "success"
        assert inv_result.fetched_count == 2

        # Bước cuối — AC-3.2 (PRD): count > 0
        count = _count_rows(dest_engine, "inventory_levels")
        assert count > 0
        assert count == 2

    # ── T-9F.1 Endpoint đúng ─────────────────────────────────────────────────

    def test_calls_inventory_locations_not_inventory_levels(
        self, internal_engine, dest_engine
    ):
        """Xác nhận runner gọi /inventory_locations.json, không phải /inventory_levels.json."""
        products = [_make_product(1)]
        _products_runner(internal_engine, dest_engine, products).run(_spec("products"))

        loc_ids = [501]
        client = MagicMock()
        called_paths: list[str] = []

        def mock_get(path, **kwargs):
            called_paths.append(path)
            if "inventory_locations" in path:
                return {"inventory_locations": [_make_inventory(101, 501)]}
            return {"locations": [{"id": lid} for lid in loc_ids]}

        client.get.side_effect = mock_get
        runner = SyncRunner(_make_cfg(), client, internal_engine, dest_engine)
        runner.run(_spec("inventory_levels"))

        inventory_calls = [p for p in called_paths if "inventory_locations" in p]
        assert len(inventory_calls) >= 1
        assert all("/inventory_locations.json" in p for p in inventory_calls)
        assert not any("inventory_levels" in p for p in inventory_calls)

    def test_location_ids_and_variant_ids_passed_as_params(
        self, internal_engine, dest_engine
    ):
        """Params location_ids và variant_ids được truyền vào request."""
        products = [_make_product(1)]
        _products_runner(internal_engine, dest_engine, products).run(_spec("products"))

        captured_params: list[dict] = []
        client = MagicMock()

        def mock_get(path, **kwargs):
            if "inventory_locations" in path:
                captured_params.append(kwargs.get("params", {}))
                return {"inventory_locations": []}
            return {"locations": [{"id": 501}]}

        client.get.side_effect = mock_get
        SyncRunner(_make_cfg(), client, internal_engine, dest_engine).run(
            _spec("inventory_levels")
        )

        assert len(captured_params) >= 1
        first = captured_params[0]
        assert "location_ids" in first
        assert "variant_ids" in first
        # variant ID 101 (from product 1's variant) must be in the param string
        assert "101" in first["variant_ids"]

    # ── T-9F.3 Batch 200 ─────────────────────────────────────────────────────

    def test_201_variants_trigger_two_api_calls(self, internal_engine, dest_engine):
        """201 variant IDs → endpoint gọi 2 lần (batch 200 + batch 1)."""
        # Build 1 product with 201 variants (IDs 1..201)
        big_product = _make_product(1, variant_ids=list(range(1, 202)))
        _products_runner(internal_engine, dest_engine, [big_product]).run(_spec("products"))

        inventory_call_count = 0
        client = MagicMock()

        def mock_get(path, **kwargs):
            nonlocal inventory_call_count
            if "inventory_locations" in path:
                inventory_call_count += 1
                return {"inventory_locations": []}
            return {"locations": [{"id": 501}]}

        client.get.side_effect = mock_get
        SyncRunner(_make_cfg(), client, internal_engine, dest_engine).run(
            _spec("inventory_levels")
        )

        assert inventory_call_count == 2

    def test_all_records_across_batches_loaded(self, internal_engine, dest_engine):
        """201 variants, 2 inventory records/batch → 4 records total in DB."""
        big_product = _make_product(1, variant_ids=list(range(1, 202)))
        _products_runner(internal_engine, dest_engine, [big_product]).run(_spec("products"))

        batch_call = 0
        client = MagicMock()

        def mock_get(path, **kwargs):
            nonlocal batch_call
            if "inventory_locations" in path:
                batch_call += 1
                # Each batch returns 2 distinct records keyed by batch number
                base = batch_call * 1000
                return {
                    "inventory_locations": [
                        _make_inventory(base + 1, 501, available=5),
                        _make_inventory(base + 2, 501, available=10),
                    ]
                }
            return {"locations": [{"id": 501}]}

        client.get.side_effect = mock_get
        result = SyncRunner(_make_cfg(), client, internal_engine, dest_engine).run(
            _spec("inventory_levels")
        )

        assert result.status == "success"
        assert result.fetched_count == 4
        assert _count_rows(dest_engine, "inventory_levels") == 4

    # ── T-9F.4 Graceful skip ─────────────────────────────────────────────────

    def test_inventory_skips_gracefully_when_products_not_synced(
        self, internal_engine, dest_engine
    ):
        """Khi products chưa sync (variant_ids=[]), inventory sync skip, status=success, count=0."""
        # Không sync products → products table rỗng → _get_variant_ids() = []
        result = _inventory_runner(
            internal_engine, dest_engine, [_make_inventory(101, 501)]
        ).run(_spec("inventory_levels"))

        assert result.status == "success"
        assert result.fetched_count == 0
        assert _count_rows(dest_engine, "inventory_levels") == 0

    def test_inventory_skips_gracefully_after_all_products_deleted(
        self, internal_engine, dest_engine
    ):
        """Tất cả products bị soft-delete → _get_variant_ids() = [] → skip gracefully."""
        # Seed product rồi soft-delete
        products = [_make_product(i) for i in range(1, 22)]
        _products_runner(internal_engine, dest_engine, products).run(_spec("products"))

        # Full refresh với 0 products → soft-delete >5% → sweep aborted, nhưng ta cần _deleted_at
        # Thay vì dùng sweep, ta set _deleted_at trực tiếp để isolate test
        with dest_engine.begin() as conn:
            conn.execute(text("UPDATE products SET _deleted_at = NOW()"))

        result = _inventory_runner(
            internal_engine, dest_engine, [_make_inventory(101, 501)]
        ).run(_spec("inventory_levels"))

        assert result.status == "success"
        assert result.fetched_count == 0

    # ── Idempotency & upsert ─────────────────────────────────────────────────

    def test_composite_pk_no_duplicates_on_repeated_sync(
        self, internal_engine, dest_engine
    ):
        """Sync 2 lần cùng data → không có bản ghi trùng (composite PK)."""
        products = [_make_product(1)]
        _products_runner(internal_engine, dest_engine, products).run(_spec("products"))

        inventory = [_make_inventory(101, 501, available=20)]
        for _ in range(2):
            _inventory_runner(internal_engine, dest_engine, inventory).run(
                _spec("inventory_levels")
            )

        assert _count_rows(dest_engine, "inventory_levels") == 1

    def test_updated_quantity_reflected_after_resync(
        self, internal_engine, dest_engine
    ):
        """available thay đổi → giá trị mới được phản ánh trong DB."""
        products = [_make_product(1)]
        _products_runner(internal_engine, dest_engine, products).run(_spec("products"))

        _inventory_runner(
            internal_engine, dest_engine, [_make_inventory(101, 501, available=50)]
        ).run(_spec("inventory_levels"))

        _inventory_runner(
            internal_engine, dest_engine, [_make_inventory(101, 501, available=99)]
        ).run(_spec("inventory_levels"))

        with dest_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT (_raw->>'available')::int FROM inventory_levels "
                    "WHERE inventory_item_id = 101 AND location_id = 501"
                )
            ).one()
        assert row[0] == 99

    # ── Multi-location ────────────────────────────────────────────────────────

    def test_multi_location_ids_all_passed_to_api(
        self, internal_engine, dest_engine
    ):
        """Nhiều location_ids → được nối thành chuỗi comma trong request param."""
        products = [_make_product(1)]
        _products_runner(internal_engine, dest_engine, products).run(_spec("products"))

        captured_params: list[dict] = []
        client = MagicMock()

        def mock_get(path, **kwargs):
            if "inventory_locations" in path:
                captured_params.append(kwargs.get("params", {}))
                return {"inventory_locations": []}
            # Trả về 3 locations
            return {"locations": [{"id": 501}, {"id": 502}, {"id": 503}]}

        client.get.side_effect = mock_get
        SyncRunner(_make_cfg(), client, internal_engine, dest_engine).run(
            _spec("inventory_levels")
        )

        assert len(captured_params) >= 1
        loc_param = captured_params[0]["location_ids"]
        loc_ids_parsed = set(loc_param.split(","))
        assert {"501", "502", "503"} == loc_ids_parsed

    def test_inventory_records_from_multiple_locations_stored(
        self, internal_engine, dest_engine
    ):
        """Records từ nhiều location → đều được lưu vào DB (composite PK = item+location)."""
        products = [_make_product(1)]
        _products_runner(internal_engine, dest_engine, products).run(_spec("products"))

        # Cùng variant_id 101, nhưng 3 location khác nhau
        inventory = [
            _make_inventory(101, 501, available=5),
            _make_inventory(101, 502, available=10),
            _make_inventory(101, 503, available=0),
        ]
        result = _inventory_runner(
            internal_engine, dest_engine, inventory, location_ids=[501, 502, 503]
        ).run(_spec("inventory_levels"))

        assert result.status == "success"
        assert result.fetched_count == 3
        assert _count_rows(dest_engine, "inventory_levels") == 3

    # ── Observability ─────────────────────────────────────────────────────────

    def test_sync_run_recorded_in_internal_db(self, internal_engine, dest_engine):
        """sync_runs ghi nhận run với status=success sau khi inventory sync xong."""
        products = [_make_product(1)]
        _products_runner(internal_engine, dest_engine, products).run(_spec("products"))

        _inventory_runner(
            internal_engine, dest_engine, [_make_inventory(101, 501)]
        ).run(_spec("inventory_levels"))

        with internal_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT status, fetched_count FROM sync_runs "
                    "WHERE pipeline = :p AND resource = 'inventory_levels' "
                    "ORDER BY started_at DESC LIMIT 1"
                ),
                {"p": _SHOP},
            ).one_or_none()

        assert row is not None
        assert row.status == "success"
        assert row.fetched_count == 1

    def test_products_sync_run_also_recorded(self, internal_engine, dest_engine):
        """sync_runs ghi nhận cả products run (dependency của inventory)."""
        products = [_make_product(i) for i in range(1, 4)]
        _products_runner(internal_engine, dest_engine, products).run(_spec("products"))

        with internal_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT status, fetched_count FROM sync_runs "
                    "WHERE pipeline = :p AND resource = 'products' "
                    "ORDER BY started_at DESC LIMIT 1"
                ),
                {"p": _SHOP},
            ).one_or_none()

        assert row is not None
        assert row.status == "success"
        assert row.fetched_count == 3
