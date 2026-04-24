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
