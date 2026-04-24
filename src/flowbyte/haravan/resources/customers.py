"""Extract customers from Haravan with keyset pagination."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterator

from flowbyte.haravan.client import HaravanClient


def extract_customers(
    client: HaravanClient,
    checkpoint: tuple[datetime, int] | None,
    mode: str,
) -> Iterator[dict]:
    params: dict = {"order": "updated_at asc, id asc"}
    if mode == "incremental" and checkpoint:
        last_ts, _ = checkpoint
        params["updated_at_min"] = (last_ts - timedelta(minutes=5)).isoformat()

    yield from client.paginate("customers", params=params, page_size=250, checkpoint=checkpoint)
