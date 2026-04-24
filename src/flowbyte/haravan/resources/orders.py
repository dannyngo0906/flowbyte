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
    paginate_checkpoint = None
    if mode == "incremental" and checkpoint:
        last_ts, _ = checkpoint
        params["updated_at_min"] = (last_ts - timedelta(minutes=5)).isoformat()
        paginate_checkpoint = checkpoint

    yield from client.paginate(
        "orders",
        params=params,
        page_size=250,
        checkpoint=paginate_checkpoint,
    )
