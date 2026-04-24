"""Prometheus metrics for Flowbyte (~50 lines, §17.11)."""
from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

_registry = CollectorRegistry()


@dataclass(frozen=True)
class _Metrics:
    sync_total: Counter = Counter(
        "flowbyte_sync_total",
        "Total sync attempts",
        ["pipeline", "resource", "mode", "status"],
        registry=_registry,
    )
    sync_duration: Histogram = Histogram(
        "flowbyte_sync_duration_seconds",
        "Sync duration in seconds",
        ["pipeline", "resource", "mode"],
        buckets=(1, 5, 10, 30, 60, 180, 600, 1800, 7200),
        registry=_registry,
    )
    records_upserted: Counter = Counter(
        "flowbyte_records_upserted_total",
        "Records upserted",
        ["pipeline", "resource"],
        registry=_registry,
    )
    haravan_api_calls: Counter = Counter(
        "flowbyte_haravan_api_calls_total",
        "Haravan API calls",
        ["endpoint", "status_code"],
        registry=_registry,
    )
    token_bucket_used: Gauge = Gauge(
        "flowbyte_token_bucket_used",
        "Token bucket usage (0-80)",
        registry=_registry,
    )
    heartbeat_age: Gauge = Gauge(
        "flowbyte_heartbeat_age_seconds",
        "Age of last scheduler heartbeat",
        registry=_registry,
    )
    reconciler_tick_age: Gauge = Gauge(
        "flowbyte_reconciler_tick_age_seconds",
        "Seconds since last reconciler tick",
        registry=_registry,
    )
    stuck_requests: Gauge = Gauge(
        "flowbyte_stuck_sync_requests",
        "Number of stuck claimed requests",
        registry=_registry,
    )
    job_misfire_total: Counter = Counter(
        "flowbyte_job_misfire_total",
        "APScheduler job misfire events",
        ["job_id"],
        registry=_registry,
    )


metrics = _Metrics()


def start_metrics_server(port: int = 9091, bind: str = "127.0.0.1") -> None:
    start_http_server(port, addr=bind, registry=_registry)
