"""Extract locations (full_refresh only — small volume, no pagination needed)."""
from __future__ import annotations

from typing import Iterator

from flowbyte.haravan.client import HaravanClient


def extract_locations(client: HaravanClient) -> Iterator[dict]:
    response = client.get("/locations.json")
    yield from response.get("locations", [])
