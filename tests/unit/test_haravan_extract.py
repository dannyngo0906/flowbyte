"""Unit tests for Haravan resource extractors: extract_orders, extract_customers.

Verifies incremental mode adds 5-min overlap param; full_refresh does not.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from flowbyte.haravan.resources.customers import extract_customers
from flowbyte.haravan.resources.orders import extract_orders


def _ts(hour: int = 10, minute: int = 0) -> datetime:
    return datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc)


def _make_client(records: list[dict] | None = None) -> MagicMock:
    client = MagicMock()
    client.paginate.return_value = iter(records or [])
    return client


def _get_params(client: MagicMock) -> dict:
    """Return the `params` kwarg from the most recent paginate() call."""
    return client.paginate.call_args[1]["params"]


# ── extract_orders ───────────────────────────────────────────────────────────


class TestExtractOrders:
    def test_full_refresh_no_checkpoint_no_updated_at_min(self):
        client = _make_client()
        list(extract_orders(client, None, "full_refresh"))
        assert "updated_at_min" not in _get_params(client)

    def test_incremental_no_checkpoint_no_updated_at_min(self):
        client = _make_client()
        list(extract_orders(client, None, "incremental"))
        assert "updated_at_min" not in _get_params(client)

    def test_incremental_with_checkpoint_adds_updated_at_min(self):
        client = _make_client()
        cp = (_ts(hour=10), 100)
        list(extract_orders(client, cp, "incremental"))
        assert "updated_at_min" in _get_params(client)

    def test_overlap_is_exactly_5_minutes(self):
        client = _make_client()
        cp_ts = _ts(hour=10, minute=0)
        list(extract_orders(client, (cp_ts, 100), "incremental"))

        raw_param = _get_params(client)["updated_at_min"]
        actual = datetime.fromisoformat(raw_param.replace("Z", "+00:00"))
        expected = _ts(hour=9, minute=55)
        assert actual == expected

    def test_full_refresh_with_checkpoint_ignores_checkpoint(self):
        client = _make_client()
        cp = (_ts(hour=8), 50)
        list(extract_orders(client, cp, "full_refresh"))
        assert "updated_at_min" not in _get_params(client)

    def test_sort_order_always_set(self):
        client = _make_client()
        list(extract_orders(client, None, "full_refresh"))
        assert _get_params(client)["order"] == "updated_at asc, id asc"

    def test_sort_order_set_for_incremental(self):
        client = _make_client()
        list(extract_orders(client, (_ts(), 0), "incremental"))
        assert _get_params(client)["order"] == "updated_at asc, id asc"

    def test_paginate_called_with_orders_resource(self):
        client = _make_client()
        list(extract_orders(client, None, "full_refresh"))
        resource_arg = client.paginate.call_args[0][0]
        assert resource_arg == "orders"

    def test_page_size_is_250(self):
        client = _make_client()
        list(extract_orders(client, None, "full_refresh"))
        assert client.paginate.call_args[1]["page_size"] == 250

    def test_yields_records_from_paginate(self):
        client = _make_client([{"id": 1}, {"id": 2}])
        records = list(extract_orders(client, None, "full_refresh"))
        assert records == [{"id": 1}, {"id": 2}]

    def test_checkpoint_passed_to_paginate(self):
        client = _make_client()
        cp = (_ts(), 99)
        list(extract_orders(client, cp, "incremental"))
        assert client.paginate.call_args[1]["checkpoint"] == cp

    def test_none_checkpoint_passed_to_paginate(self):
        client = _make_client()
        list(extract_orders(client, None, "incremental"))
        assert client.paginate.call_args[1]["checkpoint"] is None


# ── extract_inventory_levels (inventory_locations endpoint) ──────────────────


from flowbyte.haravan.resources.inventory import extract_inventory_levels


def _make_inv_client(response: dict | None = None) -> MagicMock:
    client = MagicMock()
    client.get.return_value = response or {"inventory_locations": []}
    return client


class TestExtractInventoryLocations:
    def test_no_call_if_location_ids_empty(self):
        client = _make_inv_client()
        records = list(extract_inventory_levels(client, [], [10001]))
        client.get.assert_not_called()
        assert records == []

    def test_no_call_if_variant_ids_empty(self):
        client = _make_inv_client()
        records = list(extract_inventory_levels(client, [501], []))
        client.get.assert_not_called()
        assert records == []

    def test_calls_correct_endpoint(self):
        client = _make_inv_client()
        list(extract_inventory_levels(client, [501], [10001]))
        client.get.assert_called_once()
        path_arg = client.get.call_args[0][0]
        assert path_arg == "/inventory_locations.json"

    def test_location_ids_passed_as_comma_string(self):
        client = _make_inv_client()
        list(extract_inventory_levels(client, [501, 502], [10001]))
        params = client.get.call_args[1]["params"]
        assert params["location_ids"] == "501,502"

    def test_variant_ids_batched_by_200(self):
        client = _make_inv_client()
        variant_ids = list(range(1, 202))  # 201 variant IDs → 2 batches
        list(extract_inventory_levels(client, [501], variant_ids))
        assert client.get.call_count == 2

    def test_yields_from_inventory_locations_key(self):
        r1 = {"inventory_item_id": 1001, "location_id": 501, "available": 10}
        r2 = {"inventory_item_id": 1002, "location_id": 501, "available": 5}
        client = _make_inv_client({"inventory_locations": [r1, r2]})
        records = list(extract_inventory_levels(client, [501], [10001]))
        assert records == [r1, r2]

    def test_missing_key_yields_nothing(self):
        client = _make_inv_client({})
        records = list(extract_inventory_levels(client, [501], [10001]))
        assert records == []


# ── extract_customers ────────────────────────────────────────────────────────


class TestExtractCustomers:
    def test_incremental_with_checkpoint_adds_updated_at_min(self):
        client = _make_client()
        cp = (_ts(hour=10), 200)
        list(extract_customers(client, cp, "incremental"))
        assert "updated_at_min" in _get_params(client)

    def test_full_refresh_no_updated_at_min(self):
        client = _make_client()
        list(extract_customers(client, None, "full_refresh"))
        assert "updated_at_min" not in _get_params(client)

    def test_incremental_no_checkpoint_no_updated_at_min(self):
        client = _make_client()
        list(extract_customers(client, None, "incremental"))
        assert "updated_at_min" not in _get_params(client)

    def test_overlap_is_exactly_5_minutes(self):
        client = _make_client()
        cp_ts = _ts(hour=12, minute=30)
        list(extract_customers(client, (cp_ts, 200), "incremental"))

        raw_param = _get_params(client)["updated_at_min"]
        actual = datetime.fromisoformat(raw_param.replace("Z", "+00:00"))
        expected = _ts(hour=12, minute=25)
        assert actual == expected

    def test_paginate_called_with_customers_resource(self):
        client = _make_client()
        list(extract_customers(client, None, "full_refresh"))
        resource_arg = client.paginate.call_args[0][0]
        assert resource_arg == "customers"

    def test_page_size_is_250(self):
        client = _make_client()
        list(extract_customers(client, None, "incremental"))
        assert client.paginate.call_args[1]["page_size"] == 250

    def test_yields_records_from_paginate(self):
        client = _make_client([{"id": 9001}, {"id": 9002}])
        records = list(extract_customers(client, None, "full_refresh"))
        assert records == [{"id": 9001}, {"id": 9002}]
