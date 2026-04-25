"""Extract products from Haravan API.

Variants stay nested inside each product dict (in _raw) — dbt will unnest them
via jsonb_array_elements(_raw->'variants'). No separate variants table.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterator

from flowbyte.haravan.client import HaravanClient


def extract_products_and_variants(
    client: HaravanClient,
    checkpoint: tuple[datetime, int] | None,
    mode: str,
) -> Iterator[dict]:
    """Yield product dicts. Variants stay nested in each product's 'variants' key."""
    params: dict = {"order": "updated_at asc, id asc"}
    paginate_checkpoint = None
    if mode == "incremental" and checkpoint:
        last_ts, _ = checkpoint
        params["updated_at_min"] = (last_ts - timedelta(minutes=5)).isoformat()
        paginate_checkpoint = checkpoint

    for product in client.paginate(
        "products", params=params, page_size=250, checkpoint=paginate_checkpoint
    ):
        yield product
