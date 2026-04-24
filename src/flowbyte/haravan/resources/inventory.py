"""Extract inventory_levels (full_refresh only — no updated_at filter)."""
from __future__ import annotations

from typing import Iterator

from flowbyte.haravan.client import HaravanClient


def extract_inventory_levels(
    client: HaravanClient,
    location_ids: list[int],
) -> Iterator[dict]:
    for loc_id in location_ids:
        yield from client.paginate(
            "inventory_levels",
            params={"location_ids": str(loc_id)},
            page_size=250,
            url_suffix="",
        )
