"""Haravan API exception hierarchy."""


class HaravanError(Exception):
    """Base class for all Haravan errors."""


class HaravanAuthError(HaravanError):
    """401 / 403 — credentials invalid or expired. Do NOT retry."""


class HaravanRateLimited(HaravanError):
    """429 — rate limit hit. Retry after Retry-After header."""

    def __init__(self, retry_after: float = 5.0) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limited, retry after {retry_after}s")


class HaravanServerError(HaravanError):
    """5xx — server-side error. Retry with backoff."""

    def __init__(self, status_code: int, body: str = "") -> None:
        self.status_code = status_code
        super().__init__(f"Server error {status_code}: {body[:200]}")


class HaravanClientError(HaravanError):
    """4xx (except 401/403/429) — bad request. Do NOT retry."""

    def __init__(self, status_code: int, body: str = "") -> None:
        self.status_code = status_code
        super().__init__(f"Client error {status_code}: {body[:200]}")


class HaravanNetworkError(HaravanError):
    """Network-level failure (timeout, connection refused). Retry."""


class InvalidRecordError(Exception):
    """Record from Haravan is missing required fields (id, etc.)."""
