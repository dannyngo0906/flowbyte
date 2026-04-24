"""Unit tests for ETL transform layer — pure dict-in / dict-out, no IO."""
from __future__ import annotations

import pytest

from flowbyte.config.models import TransformConfig
from flowbyte.sync.transform import InvalidRecordError, apply_transform, validate_transform_for_resource


def _base_order(**overrides):
    record = {
        "id": 1001,
        "order_number": 2001,
        "name": "#2001",
        "email": "test@example.com",
        "financial_status": "paid",
        "fulfillment_status": None,
        "total_price": "149.99",
        "subtotal_price": "130.00",
        "total_tax": "19.99",
        "currency": "VND",
        "customer_id": 9001,
        "customer": {"email": "test@example.com", "phone": "0901234567"},
        "shipping_address": {
            "city": "Ho Chi Minh",
            "province": "Ho Chi Minh",
            "country_code": "VN",
            "phone": "",
        },
        "transactions": [{"id": 10, "amount": "149.99"}],
        "tax_lines": [],
        "discount_codes": [],
        "fulfillments": [],
        "note_attributes": [{"name": "source", "value": "web"}],
        "created_at": "2026-01-01T00:00:00+07:00",
        "updated_at": "2026-04-24T10:00:00+07:00",
        "cancelled_at": None,
        "closed_at": None,
        "line_items": [
            {
                "id": 5001,
                "product_id": 7001,
                "variant_id": 8001,
                "sku": "PROD-001",
                "title": "Product A",
                "quantity": 2,
                "price": "65.00",
                "total_discount": "0.00",
            }
        ],
    }
    record.update(overrides)
    return record


def _no_transform():
    return TransformConfig()


class TestApplyTransformOrders:
    def test_flat_fields_copied(self):
        out = apply_transform(_base_order(), _no_transform(), "orders")
        assert out["id"] == 1001
        assert out["email"] == "test@example.com"
        assert out["financial_status"] == "paid"

    def test_raw_always_present(self):
        record = _base_order()
        out = apply_transform(record, _no_transform(), "orders")
        assert "_raw" in out
        assert out["_raw"]["id"] == 1001

    def test_nested_customer_flattened(self):
        out = apply_transform(_base_order(), _no_transform(), "orders")
        assert out["customer_email"] == "test@example.com"
        assert out["customer_phone"] == "0901234567"

    def test_nested_shipping_flattened(self):
        out = apply_transform(_base_order(), _no_transform(), "orders")
        assert out["shipping_city"] == "Ho Chi Minh"
        assert out["shipping_province"] == "Ho Chi Minh"
        assert out["shipping_country_code"] == "VN"

    def test_nested_jsonb_preserved(self):
        out = apply_transform(_base_order(), _no_transform(), "orders")
        assert out["transactions"] == [{"id": 10, "amount": "149.99"}]
        assert out["note_attributes"] == [{"name": "source", "value": "web"}]

    def test_line_items_not_in_orders_output(self):
        out = apply_transform(_base_order(), _no_transform(), "orders")
        assert "line_items" not in out

    def test_missing_customer_defaults_to_none(self):
        record = _base_order(customer=None)
        out = apply_transform(record, _no_transform(), "orders")
        assert out["customer_email"] is None
        assert out["customer_phone"] is None

    def test_missing_shipping_defaults_to_none(self):
        record = _base_order(shipping_address=None)
        out = apply_transform(record, _no_transform(), "orders")
        assert out["shipping_city"] is None

    def test_rename_field(self):
        cfg = TransformConfig(rename={"total_price": "revenue"})
        out = apply_transform(_base_order(), cfg, "orders")
        assert "revenue" in out
        assert "total_price" not in out
        assert out["revenue"] == "149.99"

    def test_skip_field(self):
        cfg = TransformConfig(skip=["note_attributes"])
        out = apply_transform(_base_order(), cfg, "orders")
        assert "note_attributes" not in out

    def test_type_override_numeric(self):
        cfg = TransformConfig(type_override={"total_price": "numeric(12,2)"})
        out = apply_transform(_base_order(), cfg, "orders")
        from decimal import Decimal
        assert out["total_price"] == Decimal("149.99")

    def test_type_override_numeric_with_rename(self):
        cfg = TransformConfig(
            rename={"total_price": "revenue"},
            type_override={"total_price": "numeric(12,2)"},
        )
        out = apply_transform(_base_order(), cfg, "orders")
        from decimal import Decimal
        assert out["revenue"] == Decimal("149.99")
        assert "total_price" not in out

    def test_id_null_raises_invalid_record_error(self):
        record = _base_order(id=None)
        with pytest.raises(InvalidRecordError, match="missing id"):
            apply_transform(record, _no_transform(), "orders")

    def test_id_zero_raises_invalid_record_error(self):
        record = _base_order(id=0)
        with pytest.raises(InvalidRecordError, match="missing id"):
            apply_transform(record, _no_transform(), "orders")


class TestApplyTransformCustomers:
    def _base_customer(self, **overrides):
        record = {
            "id": 9001,
            "email": "customer@example.com",
            "phone": "0901234567",
            "first_name": "Nguyen",
            "last_name": "An",
            "total_spent": "1500.00",
            "orders_count": 5,
            "accepts_marketing": True,
            "tags": "vip,loyal",
            "created_at": "2025-01-15T08:00:00+07:00",
            "updated_at": "2026-04-20T09:30:00+07:00",
        }
        record.update(overrides)
        return record

    def test_basic_fields(self):
        out = apply_transform(self._base_customer(), _no_transform(), "customers")
        assert out["id"] == 9001
        assert out["email"] == "customer@example.com"
        assert out["first_name"] == "Nguyen"

    def test_raw_present(self):
        out = apply_transform(self._base_customer(), _no_transform(), "customers")
        assert out["_raw"]["id"] == 9001


class TestApplyTransformProducts:
    def _base_product(self, **overrides):
        record = {
            "id": 7001,
            "title": "Moisturizer SPF50",
            "vendor": "Brand X",
            "product_type": "skincare",
            "handle": "moisturizer-spf50",
            "tags": "spf,moisturizer",
            "status": "active",
            "created_at": "2025-06-01T00:00:00+07:00",
            "updated_at": "2026-03-10T12:00:00+07:00",
            "published_at": "2025-06-01T00:00:00+07:00",
            "variants": [],
        }
        record.update(overrides)
        return record

    def test_basic_fields(self):
        out = apply_transform(self._base_product(), _no_transform(), "products")
        assert out["id"] == 7001
        assert out["title"] == "Moisturizer SPF50"
        assert "variants" not in out


class TestTransformConfigValidation:
    def test_skip_id_raises(self):
        with pytest.raises(ValueError, match="'id'"):
            TransformConfig(skip=["id"])

    def test_rename_creates_duplicate_raises(self):
        with pytest.raises(ValueError, match="duplicate"):
            TransformConfig(rename={"email": "my_alias", "phone": "my_alias"})

    def test_empty_config_valid(self):
        cfg = TransformConfig()
        assert cfg.rename == {}
        assert cfg.skip == []
        assert cfg.type_override == {}


class TestTypeOverrideCasting:
    def test_numeric_string_to_decimal(self):
        cfg = TransformConfig(type_override={"total_price": "numeric(12,2)"})
        record = _base_order(total_price="99.50")
        out = apply_transform(record, cfg, "orders")
        from decimal import Decimal
        assert out["total_price"] == Decimal("99.50")

    def test_none_value_stays_none(self):
        cfg = TransformConfig(type_override={"total_price": "numeric(12,2)"})
        record = _base_order(total_price=None)
        out = apply_transform(record, cfg, "orders")
        assert out["total_price"] is None

    def test_invalid_numeric_string_does_not_crash(self):
        cfg = TransformConfig(type_override={"total_price": "numeric(12,2)"})
        record = _base_order(total_price="not_a_number")
        # Should return None or raise — currently returns None (skip on cast failure)
        out = apply_transform(record, cfg, "orders")
        assert out["total_price"] is None

    def test_type_override_integer(self):
        cfg = TransformConfig(type_override={"orders_count": "integer"})
        record = {
            "id": 9001, "email": "c@example.com", "phone": "090",
            "first_name": "A", "last_name": "B",
            "total_spent": "100.00", "orders_count": "5",
            "accepts_marketing": True, "tags": "",
            "created_at": "2025-01-15T08:00:00+07:00",
            "updated_at": "2026-04-20T09:30:00+07:00",
        }
        out = apply_transform(record, cfg, "customers")
        assert out["orders_count"] == 5
        assert isinstance(out["orders_count"], int)

    def test_type_override_boolean_string_true(self):
        cfg = TransformConfig(type_override={"accepts_marketing": "boolean"})
        record = {
            "id": 9001, "email": "c@example.com", "phone": "090",
            "first_name": "A", "last_name": "B",
            "total_spent": "0", "orders_count": 1,
            "accepts_marketing": "true",
            "tags": "", "created_at": "2025-01-15T08:00:00+07:00",
            "updated_at": "2026-04-20T09:30:00+07:00",
        }
        out = apply_transform(record, cfg, "customers")
        assert out["accepts_marketing"] is True

    def test_type_override_boolean_string_false(self):
        cfg = TransformConfig(type_override={"accepts_marketing": "boolean"})
        record = {
            "id": 9001, "email": "c@example.com", "phone": "090",
            "first_name": "A", "last_name": "B",
            "total_spent": "0", "orders_count": 1,
            "accepts_marketing": "0",
            "tags": "", "created_at": "2025-01-15T08:00:00+07:00",
            "updated_at": "2026-04-20T09:30:00+07:00",
        }
        out = apply_transform(record, cfg, "customers")
        assert out["accepts_marketing"] is False

    def test_type_override_timestamp_iso8601(self):
        from datetime import datetime, timezone
        cfg = TransformConfig(type_override={"created_at": "timestamp with time zone"})
        out = apply_transform(_base_order(), cfg, "orders")
        assert isinstance(out["created_at"], datetime)
        assert out["created_at"].tzinfo == timezone.utc


class TestSkipAndRenameOnNestedColumns:
    """Skip/rename on nested-flatten and JSONB columns."""

    def test_skip_nested_flatten_column_customer_email(self):
        cfg = TransformConfig(skip=["customer_email"])
        out = apply_transform(_base_order(), cfg, "orders")
        assert "customer_email" not in out
        assert "customer_phone" in out  # sibling column unaffected

    def test_skip_nested_flatten_column_shipping_city(self):
        cfg = TransformConfig(skip=["shipping_city"])
        out = apply_transform(_base_order(), cfg, "orders")
        assert "shipping_city" not in out
        assert "shipping_province" in out

    def test_rename_jsonb_column(self):
        cfg = TransformConfig(rename={"note_attributes": "notes"})
        out = apply_transform(_base_order(), cfg, "orders")
        assert "notes" in out
        assert "note_attributes" not in out
        assert out["notes"] == [{"name": "source", "value": "web"}]

    def test_skip_multiple_fields_mixed_types(self):
        cfg = TransformConfig(skip=["email", "note_attributes", "customer_email"])
        out = apply_transform(_base_order(), cfg, "orders")
        assert "email" not in out
        assert "note_attributes" not in out
        assert "customer_email" not in out
        assert "id" in out  # id always present


class TestValidateTransformForResource:
    def test_rename_to_existing_column_returns_error(self):
        cfg = TransformConfig(rename={"total_price": "email"})
        errors = validate_transform_for_resource(cfg, "orders")
        assert len(errors) == 1
        assert "email" in errors[0]

    def test_rename_to_unique_name_returns_no_errors(self):
        cfg = TransformConfig(rename={"total_price": "revenue"})
        errors = validate_transform_for_resource(cfg, "orders")
        assert errors == []

    def test_rename_to_another_renamed_source_returns_no_errors(self):
        # Chain rename: total_price → email (OK because email itself is renamed away)
        cfg = TransformConfig(rename={"total_price": "email", "email": "contact_email"})
        errors = validate_transform_for_resource(cfg, "orders")
        assert errors == []

    def test_unknown_resource_returns_no_errors(self):
        cfg = TransformConfig(rename={"foo": "bar"})
        errors = validate_transform_for_resource(cfg, "nonexistent_resource")
        assert errors == []
