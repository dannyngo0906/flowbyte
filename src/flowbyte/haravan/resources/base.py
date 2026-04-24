"""Base class for all Haravan resource handlers."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterator

from flowbyte.haravan.client import HaravanClient

_OVERLAP_MINUTES = 5


class BaseResourceHandler:
    """Override `name`, `endpoint`, and optionally `extract` in each subclass."""

    name: str
    endpoint: str
    supports_incremental: bool = True
    page_size: int = 250

    def extract(
        self,
        client: HaravanClient,
        checkpoint: tuple[datetime, int] | None,
        mode: str,
    ) -> Iterator[dict]:
        params: dict[str, object] = {"order": "updated_at asc, id asc"}
        if mode == "incremental" and checkpoint:
            last_ts, _ = checkpoint
            params["updated_at_min"] = (
                last_ts - timedelta(minutes=_OVERLAP_MINUTES)
            ).isoformat()
        yield from client.paginate(
            self.endpoint,
            params=params,
            page_size=self.page_size,
            checkpoint=checkpoint if mode == "incremental" else None,
        )

    def extract_full(self, client: HaravanClient) -> Iterator[dict]:
        yield from client.paginate(
            self.endpoint,
            params={"order": "updated_at asc, id asc"},
            page_size=self.page_size,
        )
