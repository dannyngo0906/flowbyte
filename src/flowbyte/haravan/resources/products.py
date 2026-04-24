"""Extract products + variants (variants are nested in products response)."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterator

from flowbyte.haravan.client import HaravanClient


def extract_products_and_variants(
    client: HaravanClient,
    checkpoint: tuple[datetime, int] | None,
    mode: str,
) -> tuple[Iterator[dict], Iterator[dict]]:
    """Returns (products_iter, variants_iter). Variants inject product_id FK."""
    products_buf: list[dict] = []
    variants_buf: list[dict] = []

    params: dict = {"order": "updated_at asc, id asc"}
    if mode == "incremental" and checkpoint:
        last_ts, _ = checkpoint
        params["updated_at_min"] = (last_ts - timedelta(minutes=5)).isoformat()

    for product in client.paginate("products", params=params, page_size=250, checkpoint=checkpoint):
        # Separate out variants before yielding product
        product_copy = {k: v for k, v in product.items() if k != "variants"}
        products_buf.append(product_copy)
        for variant in product.get("variants", []):
            variant["product_id"] = product["id"]
            variants_buf.append(variant)

    return iter(products_buf), iter(variants_buf)
