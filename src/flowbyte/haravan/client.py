"""Haravan REST API HTTP client with rate limiting, retry, and keyset pagination."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import httpx
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from flowbyte.haravan.exceptions import (
    HaravanAuthError,
    HaravanClientError,
    HaravanNetworkError,
    HaravanRateLimited,
    HaravanServerError,
)
from flowbyte.haravan.token_bucket import HaravanTokenBucket

logger = logging.getLogger(__name__)

_RETRY_ATTEMPTS = 5
_RATE_LIMIT_HEADER = "X-Haravan-Api-Call-Limit"
_RETRY_AFTER_HEADER = "Retry-After"


def should_retry(exc: BaseException) -> bool:
    return isinstance(exc, (HaravanRateLimited, HaravanServerError, HaravanNetworkError))


_should_retry = should_retry


class HaravanClient:
    def __init__(
        self,
        shop_domain: str,
        access_token: str,
        bucket: HaravanTokenBucket,
        http_client: httpx.Client | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = f"https://{shop_domain}/admin"
        self._bucket = bucket
        self._client = http_client or httpx.Client(
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
        )
        self._prime_bucket()

    # ── Public API ────────────────────────────────────────────────────────────

    def paginate(
        self,
        resource: str,
        params: dict[str, Any] | None = None,
        page_size: int = 250,
        checkpoint: tuple[datetime, int] | None = None,
    ) -> Iterator[dict]:
        """Keyset pagination with composite cursor (updated_at, id).

        Yields records one at a time, stable under concurrent source writes.
        """
        p = {**(params or {}), "limit": page_size}
        last_ts, last_id = checkpoint or (None, 0)

        while True:
            response = self._request_with_retry("GET", f"/{resource}.json", params=p)
            records: list[dict] = response.json().get(resource, [])
            if not records:
                return

            new_any = False
            for r in records:
                r_ts_raw = r.get("updated_at")
                r_id = r.get("id", 0)

                if r_ts_raw and last_ts:
                    r_ts = _parse_iso(r_ts_raw)
                    if (r_ts, r_id) <= (last_ts, last_id):
                        continue  # already processed (5-min overlap dedup)

                yield r
                new_any = True
                if r_ts_raw:
                    last_ts = _parse_iso(r_ts_raw)
                    last_id = r_id

            if len(records) < page_size:
                return
            if not new_any:
                # Full page was tie-breaker overlap — avoid infinite loop
                logger.warning("pagination_stuck: full page already processed, cursor=%s", last_ts)
                return

            # Advance cursor for next page
            if last_ts:
                p["updated_at_min"] = last_ts.isoformat()
            p.pop("page", None)  # cursor-based: don't mix with page

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict:
        response = self._request_with_retry("GET", path, params=params)
        return response.json()

    def test_connection(self) -> dict:
        """Call /shop.json to verify credentials. Returns shop info."""
        return self.get("/shop.json")

    # ── Private ───────────────────────────────────────────────────────────────

    def _prime_bucket(self) -> None:
        """Sync token bucket from server state on startup (cold start prime)."""
        try:
            response = self._client.get(f"{self.base_url}/shop.json", timeout=5)
            self._bucket.update_from_header(response.headers.get(_RATE_LIMIT_HEADER))
        except Exception as e:
            logger.warning("bucket_prime_failed: %s — assuming 50/80", e)
            # Conservative fallback to avoid 429 storm on restart
            with self._bucket._lock:
                self._bucket._tokens_used = 50.0

    def _request_with_retry(
        self, method: str, path: str, **kwargs: Any
    ) -> httpx.Response:
        def _log_retry(state: RetryCallState) -> None:
            exc = state.outcome.exception()
            logger.warning(
                "haravan_retry attempt=%d error=%s", state.attempt_number, exc
            )

        for attempt in Retrying(
            retry=retry_if_exception(_should_retry),
            wait=wait_exponential(multiplier=1, min=2, max=60),
            stop=stop_after_attempt(_RETRY_ATTEMPTS),
            before_sleep=_log_retry,
            reraise=True,
        ):
            with attempt:
                self._bucket.acquire()
                try:
                    response = self._client.request(
                        method, f"{self.base_url}{path}", **kwargs
                    )
                except httpx.NetworkError as e:
                    raise HaravanNetworkError(str(e)) from e
                except httpx.TimeoutException as e:
                    raise HaravanNetworkError(f"Timeout: {e}") from e

                self._bucket.update_from_header(
                    response.headers.get(_RATE_LIMIT_HEADER)
                )
                return self._raise_for_status(response)

        raise RuntimeError("unreachable")  # tenacity reraise=True handles it

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> httpx.Response:
        sc = response.status_code
        if sc == 429:
            retry_after = float(response.headers.get(_RETRY_AFTER_HEADER, "5"))
            raise HaravanRateLimited(retry_after=retry_after)
        if sc in (401, 403):
            raise HaravanAuthError(response.text[:300])
        if 500 <= sc < 600:
            raise HaravanServerError(sc, response.text)
        if 400 <= sc < 500:
            raise HaravanClientError(sc, response.text)
        return response


def _parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
