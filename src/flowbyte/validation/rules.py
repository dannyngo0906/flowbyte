"""Post-sync validation rules (4 rules as per TDD §17.8)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ValidationContext:
    pipeline: str
    resource: str
    sync_id: str
    mode: str
    fetched_count: int
    upserted_count: int
    rows_before: int
    rows_after: int
    prev_runs: list[dict] = field(default_factory=list)

    @property
    def is_weekly_full_refresh(self) -> bool:
        return self.mode == "full_refresh"


@dataclass
class ValidationResult:
    rule: str
    status: str  # ok | warning | failed | skipped
    details: dict[str, Any] = field(default_factory=dict)


def fetch_upsert_parity(ctx: ValidationContext) -> ValidationResult:
    """R1: Detect silent record skip (fetched >> upserted)."""
    diff = ctx.fetched_count - ctx.upserted_count
    skip_pct = diff / max(ctx.fetched_count, 1) * 100
    if skip_pct > 1.0:
        return ValidationResult(
            "fetch_upsert_parity",
            "failed",
            {
                "fetched": ctx.fetched_count,
                "upserted": ctx.upserted_count,
                "skipped_pct": round(skip_pct, 2),
            },
        )
    return ValidationResult("fetch_upsert_parity", "ok", {})


def incremental_volume_sanity(ctx: ValidationContext) -> ValidationResult:
    """R2: Zero-fetch streak detection (possible stale/expired token)."""
    if ctx.mode != "incremental":
        return ValidationResult("volume_sanity", "skipped", {})
    if ctx.fetched_count > 0:
        return ValidationResult("volume_sanity", "ok", {})

    recent_zeros = sum(1 for r in ctx.prev_runs[:2] if (r.get("fetched_count") or 0) == 0)
    if recent_zeros >= 2:
        return ValidationResult(
            "volume_sanity",
            "warning",
            {"zero_fetch_streak": 3, "hint": "Check access_token — may be expired"},
        )
    return ValidationResult("volume_sanity", "ok", {})


def weekly_full_drift(ctx: ValidationContext) -> ValidationResult:
    """R3: Compare full refresh count vs previous full refresh."""
    if ctx.mode != "full_refresh" or not ctx.prev_runs:
        return ValidationResult("weekly_full_drift", "skipped", {})

    prev_full = next(
        (r for r in ctx.prev_runs if r.get("mode") == "full_refresh"), None
    )
    if prev_full is None:
        return ValidationResult("weekly_full_drift", "skipped", {})

    prev_count = prev_full.get("fetched_count") or 0
    delta = abs(ctx.fetched_count - prev_count)
    drift_pct = delta / max(prev_count, 1) * 100

    if drift_pct > 20.0:
        return ValidationResult(
            "weekly_full_drift", "failed", {"drift_pct": round(drift_pct, 2)}
        )
    if drift_pct > 5.0:
        return ValidationResult(
            "weekly_full_drift", "warning", {"drift_pct": round(drift_pct, 2)}
        )
    return ValidationResult("weekly_full_drift", "ok", {"drift_pct": round(drift_pct, 2)})


def soft_delete_sanity(ctx: ValidationContext) -> ValidationResult:
    """R4: Guard against mass soft-delete on full refresh (§17.1)."""
    if ctx.mode != "full_refresh":
        return ValidationResult("soft_delete_sanity", "skipped", {})

    would_delete = ctx.rows_before - ctx.fetched_count
    delete_pct = would_delete / max(ctx.rows_before, 1) * 100

    if delete_pct > 5.0:
        return ValidationResult(
            "soft_delete_sanity",
            "failed",
            {
                "delete_pct": round(delete_pct, 2),
                "action": "sweep ABORTED — manual review required",
            },
        )
    return ValidationResult(
        "soft_delete_sanity", "ok", {"delete_pct": round(delete_pct, 2)}
    )


def run_all_validations(ctx: ValidationContext) -> list[ValidationResult]:
    return [
        fetch_upsert_parity(ctx),
        incremental_volume_sanity(ctx),
        weekly_full_drift(ctx),
        soft_delete_sanity(ctx),
    ]
