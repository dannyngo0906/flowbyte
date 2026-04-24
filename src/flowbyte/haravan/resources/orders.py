"""Extract orders from Haravan with keyset pagination."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterator

from flowbyte.haravan.client import HaravanClient


def extract_orders(
    client: HaravanClient,
    checkpoint: tuple[datetime, int] | None,
    mode: str,
) -> Iterator[dict]:
    params: dict = {"order": "updated_at asc, id asc"}
    last_ts, last_id = checkpoint if checkpoint else (None, 0)

    if mode == "incremental" and last_ts:
        # 5-min look-back overlap to catch records delayed between extract/load
        params["updated_at_min"] = (last_ts - timedelta(minutes=5)).isoformat()

    yield from client.paginate(
        "orders",
        params=params,
        page_size=250,
        checkpoint=checkpoint,
    )
