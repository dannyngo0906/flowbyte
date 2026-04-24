"""Observability commands: status, history, logs, inspect."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer()
console = Console()


@app.command()
def status():
    """Show sync status overview for all pipelines × resources."""
    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import pipelines, sync_runs, validation_results
    from sqlalchemy import select

    settings = AppSettings()
    engine = get_internal_engine(settings.db_url)

    with engine.connect() as conn:
        pipeline_rows = conn.execute(select(pipelines)).all()

    if not pipeline_rows:
        console.print("No pipelines. Run [bold]flowbyte init <name>[/bold] to create one.")
        return

    table = Table("PIPELINE", "RESOURCE", "LAST SYNC", "STATUS", "RECORDS", "VALIDATION", "NEXT SYNC")

    with engine.connect() as conn:
        for p in pipeline_rows:
            for resource in ["orders", "customers", "inventory_levels", "products", "variants", "locations"]:
                last_run = conn.execute(
                    select(sync_runs)
                    .where(
                        sync_runs.c.pipeline == p.name,
                        sync_runs.c.resource == resource,
                    )
                    .order_by(sync_runs.c.started_at.desc())
                    .limit(1)
                ).one_or_none()

                if last_run is None:
                    table.add_row(p.name, resource, "—", "⚪ DISABLED" if not p.enabled else "⚪ NEVER", "—", "—", "—")
                    continue

                status_icon = {
                    "success": "🟢 OK",
                    "failed": "🔴 FAIL",
                    "running": "🟡 RUNNING",
                }.get(last_run.status, "⚪ UNKNOWN")

                val_rows = conn.execute(
                    select(validation_results)
                    .where(validation_results.c.sync_id == last_run.sync_id)
                ).all()
                if not val_rows:
                    val_icon = "—"
                elif any(r.status == "failed" for r in val_rows):
                    val_icon = "❌ Failed"
                elif any(r.status == "warning" for r in val_rows):
                    val_icon = "⚠️ Warning"
                else:
                    val_icon = "✅ Healthy"

                last_sync = str(last_run.finished_at)[:16] if last_run.finished_at else "—"
                records = str(last_run.upserted_count or 0)
                table.add_row(p.name, resource, last_sync, status_icon, records, val_icon, "—")

    console.print(table)


@app.command()
def history(
    pipeline: str = typer.Argument(...),
    last: int = typer.Option(10, "--last", "-n"),
):
    """Show recent sync history for a pipeline."""
    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import sync_runs
    from sqlalchemy import select

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
        console.print(f"No sync history for pipeline '{pipeline}'.")
        return

    table = Table("TIME", "RESOURCE", "MODE", "STATUS", "FETCHED", "DURATION", "ERROR")
    for r in rows:
        error_short = (r.error or "")[:40]
        table.add_row(
            str(r.started_at)[:16],
            r.resource,
            r.mode,
            r.status,
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
    since: str = typer.Option("", "--since", help="e.g. 1h, 30m"),
):
    """View sync logs for a pipeline."""
    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import sync_logs
    from sqlalchemy import select

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
        level_color = {"ERROR": "red", "CRITICAL": "bold red", "WARNING": "yellow"}.get(r.level, "white")
        console.print(f"[{level_color}]{str(r.timestamp)[:19]}  {r.level:8}  {r.event}  {r.message or ''}[/{level_color}]")


@app.command()
def inspect(sync_id: str = typer.Argument(...)):
    """Deep trace of a single sync run."""
    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import sync_runs, sync_logs, validation_results
    from sqlalchemy import select

    settings = AppSettings()
    engine = get_internal_engine(settings.db_url)

    with engine.connect() as conn:
        run = conn.execute(
            select(sync_runs).where(sync_runs.c.sync_id.cast(str).like(f"{sync_id}%"))
        ).one_or_none()
        if run is None:
            console.print(f"[red]Sync run not found: {sync_id}[/red]")
            raise typer.Exit(1)

        logs_rows = conn.execute(
            select(sync_logs)
            .where(sync_logs.c.sync_id == run.sync_id)
            .order_by(sync_logs.c.timestamp)
        ).all()

        val_rows = conn.execute(
            select(validation_results).where(validation_results.c.sync_id == run.sync_id)
        ).all()

    console.print(f"[bold]SYNC {run.sync_id}[/bold]")
    console.print(f"Pipeline: {run.pipeline}  Resource: {run.resource}  Mode: {run.mode}")
    console.print(f"Status: {run.status}  Duration: {run.duration_seconds}s")
    console.print(f"Fetched: {run.fetched_count}  Upserted: {run.upserted_count}  Skipped: {run.skipped_invalid}")

    if logs_rows:
        console.print("\n[bold]Timeline:[/bold]")
        for log_entry in logs_rows:
            console.print(f"  {str(log_entry.timestamp)[11:19]}  {log_entry.event}  {log_entry.message or ''}")

    if val_rows:
        console.print("\n[bold]Validation:[/bold]")
        for v in val_rows:
            icon = "✓" if v.status == "ok" else ("⚠" if v.status == "warning" else "✗")
            console.print(f"  {icon} {v.rule}: {v.status}")


def _parse_since(value: str) -> timedelta | None:
    try:
        if value.endswith("h"):
            return timedelta(hours=float(value[:-1]))
        if value.endswith("m"):
            return timedelta(minutes=float(value[:-1]))
        if value.endswith("d"):
            return timedelta(days=float(value[:-1]))
    except ValueError:
        pass
    return None
