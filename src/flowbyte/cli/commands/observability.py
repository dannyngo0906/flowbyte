"""Observability commands: status, history, logs, inspect."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table

app = typer.Typer()
console = Console()

STATUS_EMOJI: dict[str, str] = {
    "success": "🟢 OK",
    "failed": "🔴 FAIL",
    "running": "🟡 RUNNING",
}

# hex + hyphens only → excludes LIKE wildcards (% and _) for inspect prefix lookup
_UUID_PREFIX_RE = re.compile(r"^[a-f0-9\-]{1,36}$")
_PIPELINE_NAME_RE = re.compile(r"^[a-z0-9_]{1,32}$")
_HISTORY_MAX = 100


def _assert_valid_pipeline(name: str) -> None:
    if not _PIPELINE_NAME_RE.match(name):
        console.print(
            f"[red]✗ Invalid pipeline name '{escape(name)}'.[/red] "
            "Use only lowercase letters, digits, underscores (max 32 chars)."
        )
        raise typer.Exit(2)


def _assert_valid_sync_id(sync_id: str) -> None:
    """Validate sync_id is hex + hyphens only — prevents LIKE wildcard injection."""
    if not _UUID_PREFIX_RE.match(sync_id):
        console.print(
            f"[red]✗ Invalid sync_id '{escape(sync_id)}'.[/red] "
            "Expected a UUID or hex prefix (e.g. 550e8400)."
        )
        raise typer.Exit(2)


# ── helpers ───────────────────────────────────────────────────────────────────


def _compute_next_sync(cron_expr: str, from_dt: datetime | None = None) -> str:
    try:
        from croniter import croniter

        base = (from_dt or datetime.now(timezone.utc)).replace(tzinfo=None)
        return str(croniter(cron_expr, base).get_next(datetime))[:16]
    except Exception:
        return "—"


def _format_scheduler_footer(hb_row: object) -> str:
    now = datetime.now(timezone.utc)
    if hb_row is None:
        return "Scheduler: ❌ Dead (no heartbeat)"
    age_s = (now - hb_row.last_beat).total_seconds()
    if age_s > 120:
        return f"Scheduler: ❌ Dead (last beat {int(age_s)}s ago)"
    uptime_s = int((now - hb_row.daemon_started_at).total_seconds())
    days, rem = divmod(uptime_s, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days > 0:
        uptime_str = f"{days}d {hours}h"
    elif hours > 0:
        uptime_str = f"{hours}h {minutes}m"
    else:
        uptime_str = f"{minutes}m"
    return f"Scheduler: 🟢 Running (uptime {uptime_str})"


def _resolve_row_status(
    pipeline_enabled: bool,
    resource_enabled: bool,
    run: object | None,
    now: datetime,
) -> str:
    if not pipeline_enabled or not resource_enabled:
        return "⚪ DISABLED"
    if run is None:
        return "⚪ NEVER"
    if run.status == "running":
        elapsed = (now - run.started_at).total_seconds()
        if elapsed > 3600:
            return "🟡 RUNNING ⚠️"
        return "🟡 RUNNING"
    if run.status == "success":
        return "🟢 OK"
    if run.status == "failed":
        return "🔴 FAIL"
    return f"⚪ {run.status}"


# ── commands ──────────────────────────────────────────────────────────────────


@app.command()
def status() -> None:
    """Show sync status overview for all pipelines × resources."""
    from flowbyte.config.models import PHASE_1_RESOURCES, AppSettings, PipelineConfig
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import pipelines, scheduler_heartbeat
    from sqlalchemy import select, text

    settings = AppSettings()
    engine = get_internal_engine(settings.db_url)
    now = datetime.now(timezone.utc)

    with engine.connect() as conn:
        # Q1: all pipelines
        pipeline_rows = conn.execute(select(pipelines)).all()

        if not pipeline_rows:
            console.print(
                "No pipelines configured."
                " Run [bold]flowbyte init <name>[/bold] to create one."
            )
            return

        # Q2: last run per (pipeline, resource) via DISTINCT ON
        last_run_rows = conn.execute(
            text(
                "SELECT DISTINCT ON (pipeline, resource)"
                " pipeline, resource, status, started_at,"
                " finished_at, upserted_count, error"
                " FROM sync_runs"
                " ORDER BY pipeline, resource, started_at DESC"
            )
        ).all()
        runs_by_key = {(r.pipeline, r.resource): r for r in last_run_rows}

        # Q3: validation summary (last 24h aggregated)
        val_rows = conn.execute(
            text(
                "SELECT pipeline, resource,"
                " COUNT(*) FILTER (WHERE status = 'warning') AS warn_count,"
                " COUNT(*) FILTER (WHERE status = 'failed')  AS fail_count"
                " FROM validation_results"
                " WHERE created_at >= NOW() - INTERVAL '24 hours'"
                " GROUP BY pipeline, resource"
            )
        ).all()

        # Q4: scheduler heartbeat singleton
        hb_row = conn.execute(
            select(scheduler_heartbeat).where(scheduler_heartbeat.c.id == 1)
        ).one_or_none()

    table = Table("PIPELINE", "RESOURCE", "LAST SYNC", "STATUS", "RECORDS", "NEXT SYNC")
    long_running_warnings: list[str] = []

    for p in pipeline_rows:
        try:
            cfg = PipelineConfig.model_validate(p.config_json)
        except Exception:
            cfg = None

        for resource in PHASE_1_RESOURCES:
            res_cfg = cfg.resources.get(resource) if cfg else None
            resource_enabled = res_cfg.enabled if res_cfg else True
            cron_expr = res_cfg.schedule if res_cfg else None

            run = runs_by_key.get((p.name, resource))
            status_str = _resolve_row_status(p.enabled, resource_enabled, run, now)

            if "⚠️" in status_str:
                long_running_warnings.append(
                    f"⚠️  {escape(p.name)}/{escape(resource)}: sync running > 1h"
                    f" (started {str(run.started_at)[:16]})"
                )
                status_str = "🟡 RUNNING"

            last_sync = str(run.finished_at)[:16] if run and run.finished_at else "—"
            records = str(run.upserted_count or 0) if run else "—"
            next_sync = _compute_next_sync(cron_expr, now) if cron_expr else "—"
            table.add_row(escape(p.name), escape(resource), last_sync, status_str, records, next_sync)

    console.print(table)
    for w in long_running_warnings:
        console.print(w)
    console.print(_format_scheduler_footer(hb_row))

    total_warn = sum(r.warn_count for r in val_rows)
    total_fail = sum(r.fail_count for r in val_rows)
    if total_fail > 0:
        console.print(f"Validation: ❌ {total_fail} failed rule(s)")
    elif total_warn > 0:
        console.print(f"Validation: ⚠️ {total_warn} warning(s)")
    else:
        console.print("Validation: ✅ All healthy")


@app.command()
def history(
    pipeline: str = typer.Argument(...),
    last: int = typer.Option(10, "--last", "-n", min=1, max=_HISTORY_MAX),
) -> None:
    """Show recent sync history for a pipeline."""
    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import sync_runs
    from sqlalchemy import select

    _assert_valid_pipeline(pipeline)
    settings = AppSettings()
    engine = get_internal_engine(settings.db_url)

    with engine.connect() as conn:
        rows = conn.execute(
            select(sync_runs)
            .where(sync_runs.c.pipeline == pipeline)
            .order_by(sync_runs.c.started_at.desc())
            .limit(last)
        ).all()

    if not rows:
        # pipeline is already validated to ^[a-z0-9_]{1,32}$ — no Rich markup chars
        console.print(f"No sync history for pipeline '{pipeline}'.")
        return

    table = Table("TIME", "RESOURCE", "MODE", "STATUS", "FETCHED", "DURATION", "ERROR")
    for r in rows:
        # escape error text — may contain arbitrary content from Haravan API responses
        error_short = escape((r.error or "")[:40])
        status_display = STATUS_EMOJI.get(r.status, f"⚪ {r.status}")
        table.add_row(
            str(r.started_at)[:16],
            escape(r.resource),
            r.mode,
            status_display,
            str(r.fetched_count or 0),
            f"{r.duration_seconds or 0:.1f}s",
            error_short,
        )
    console.print(table)


@app.command()
def logs(
    pipeline: str = typer.Argument(...),
    tail: bool = typer.Option(False, "--tail"),
    errors: bool = typer.Option(False, "--errors"),
    since: str = typer.Option("", "--since", help="e.g. 1h, 30m, 2d"),
) -> None:
    """View sync logs for a pipeline."""
    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import sync_logs
    from sqlalchemy import select

    _assert_valid_pipeline(pipeline)
    settings = AppSettings()
    engine = get_internal_engine(settings.db_url)

    query = select(sync_logs).where(sync_logs.c.pipeline == pipeline)

    if errors:
        query = query.where(sync_logs.c.level.in_(["ERROR", "CRITICAL"]))

    if since:
        delta = _parse_since(since)
        if delta:
            cutoff = datetime.now(timezone.utc) - delta
            query = query.where(sync_logs.c.timestamp >= cutoff)

    query = query.order_by(sync_logs.c.timestamp.desc()).limit(100)

    with engine.connect() as conn:
        rows = conn.execute(query).all()

    for r in reversed(rows):
        level_color = {"ERROR": "red", "CRITICAL": "bold red", "WARNING": "yellow"}.get(
            r.level, "white"
        )
        # escape event/message — may contain brackets from API error payloads
        console.print(
            f"[{level_color}]{str(r.timestamp)[:19]}  {r.level:8}  {escape(r.event)}"
            f"  {escape(r.message or '')}[/{level_color}]"
        )


@app.command()
def inspect(sync_id: str = typer.Argument(...)) -> None:
    """Deep trace of a single sync run."""
    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import sync_logs, sync_runs, validation_results
    from sqlalchemy import select, String

    # validate before DB query — hex/hyphen-only input cannot contain LIKE wildcards
    _assert_valid_sync_id(sync_id)
    settings = AppSettings()
    engine = get_internal_engine(settings.db_url)

    with engine.connect() as conn:
        run = conn.execute(
            select(sync_runs).where(sync_runs.c.sync_id.cast(String).like(f"{sync_id}%"))
        ).one_or_none()
        if run is None:
            # sync_id already validated to hex+hyphens — safe, but escape defensively
            console.print(f"[red]Sync run not found: {escape(sync_id)}[/red]")
            raise typer.Exit(1)

        logs_rows = conn.execute(
            select(sync_logs)
            .where(sync_logs.c.sync_id == run.sync_id)
            .order_by(sync_logs.c.timestamp)
        ).all()

        val_rows = conn.execute(
            select(validation_results).where(validation_results.c.sync_id == run.sync_id)
        ).all()

    console.print(f"[bold]SYNC {escape(str(run.sync_id))}[/bold]")
    console.print(f"Pipeline: {escape(run.pipeline)}  Resource: {escape(run.resource)}  Mode: {run.mode}")
    console.print(f"Status: {run.status}  Duration: {run.duration_seconds}s")
    console.print(
        f"Fetched: {run.fetched_count}  Upserted: {run.upserted_count}"
        f"  Skipped: {run.skipped_invalid}"
    )

    if logs_rows:
        console.print("\n[bold]Timeline:[/bold]")
        for log_entry in logs_rows:
            # escape log content — may contain brackets from Haravan error messages
            console.print(
                f"  {str(log_entry.timestamp)[11:19]}  {escape(log_entry.event)}"
                f"  {escape(log_entry.message or '')}"
            )

    if val_rows:
        console.print("\n[bold]Validation:[/bold]")
        for v in val_rows:
            icon = "✓" if v.status == "ok" else ("⚠" if v.status == "warning" else "✗")
            console.print(f"  {icon} {escape(v.rule)}: {v.status}")


def _parse_since(value: str) -> timedelta | None:
    try:
        if value.endswith("h"):
            return timedelta(hours=float(value[:-1]))
        if value.endswith("m"):
            return timedelta(minutes=float(value[:-1]))
        if value.endswith("d"):
            return timedelta(days=float(value[:-1]))
    except (ValueError, OverflowError):
        pass
    return None
