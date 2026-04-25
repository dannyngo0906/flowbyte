"""Extract inventory location balances via /inventory_locations.json."""
from __future__ import annotations

from typing import Iterator

from flowbyte.haravan.client import HaravanClient

_VARIANT_BATCH_SIZE = 200


def extract_inventory_levels(
    client: HaravanClient,
    location_ids: list[int],
    variant_ids: list[int],
) -> Iterator[dict]:
    if not location_ids or not variant_ids:
        return
    loc_param = ",".join(str(i) for i in location_ids)
    for start in range(0, len(variant_ids), _VARIANT_BATCH_SIZE):
        batch = variant_ids[start : start + _VARIANT_BATCH_SIZE]
        var_param = ",".join(str(v) for v in batch)
        response = client.get(
            "/inventory_locations.json",
            params={"location_ids": loc_param, "variant_ids": var_param},
        )
        yield from response.get("inventory_locations", [])
