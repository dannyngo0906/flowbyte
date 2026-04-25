"""System commands: health, cleanup, daemon (internal)."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

import typer
from rich.console import Console
from rich.markup import escape

app = typer.Typer()
console = Console()


@app.command()
def health(strict: bool = typer.Option(False, "--strict")):
    """Multi-signal health check (exit 3 if unhealthy — used by Docker healthcheck)."""
    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import scheduler_heartbeat, sync_requests
    from sqlalchemy import select, func

    settings = AppSettings()

    try:
        engine = get_internal_engine(settings.db_url)
    except Exception as e:
        console.print(f"[red]✗ DB unreachable: {escape(str(e))}[/red]")
        raise typer.Exit(3)

    checks: dict[str, bool] = {}

    try:
        with engine.connect() as conn:
            # Heartbeat check
            row = conn.execute(select(scheduler_heartbeat)).one_or_none()
            if row:
                age = (datetime.now(timezone.utc) - row.last_beat).total_seconds()
                checks["heartbeat_fresh"] = age < 120
            else:
                checks["heartbeat_fresh"] = False

            # Stuck requests
            stuck = conn.execute(
                select(func.count()).select_from(sync_requests).where(
                    sync_requests.c.status == "claimed",
                    sync_requests.c.claimed_at < datetime.now(timezone.utc) - timedelta(minutes=5),
                )
            ).scalar() or 0
            checks["no_stuck_requests"] = stuck == 0
    except Exception as e:
        console.print(f"[red]✗ DB unreachable: {escape(str(e))}[/red]")
        raise typer.Exit(3)

    unhealthy = [k for k, ok in checks.items() if not ok]

    for name, ok in checks.items():
        icon = "[green]✓[/green]" if ok else "[red]✗[/red]"
        console.print(f"  {icon} {name}")

    if unhealthy:
        console.print(f"\n[red]UNHEALTHY: {', '.join(unhealthy)}[/red]")
        raise typer.Exit(3)

    console.print("\n[green]Healthy[/green]")


@app.command()
def cleanup(dry_run: bool = typer.Option(False, "--dry-run")):
    """Run retention cleanup (daily 3AM automatic, or manual with --dry-run)."""
    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.retention.cleanup import dry_run_cleanup, cleanup_tick

    settings = AppSettings()
    engine = get_internal_engine(settings.db_url)

    if dry_run:
        stats = dry_run_cleanup(engine)
        console.print("[bold]DRY RUN — nothing deleted:[/bold]")
        console.print(f"  sync_logs (success >90d): {stats['sync_logs_success_rows']} rows")
        console.print(f"  sync_logs (errors >30d):  {stats['sync_logs_error_rows']} rows")
        console.print(f"  validation_results >30d:  {stats['validation_results_rows']} rows")
        console.print(f"  sync_runs >90d:           {stats['sync_runs_rows']} rows")
        console.print(f"  sync_requests done >30d:  {stats['sync_requests_rows']} rows")
    else:
        cleanup_tick(engine)
        console.print("[green]✓ Cleanup complete.[/green]")


@app.command(name="log-deployment")
def log_deployment(
    version: str = typer.Option("unknown", "--version"),
) -> None:
    """Record a deployment event in the internal DB (called by entrypoint.sh)."""
    if len(version) > 32:
        console.print("[red]✗ --version exceeds 32 characters[/red]")
        raise typer.Exit(1)

    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import deployment_events

    settings = AppSettings()
    engine = get_internal_engine(settings.db_url)
    with engine.begin() as conn:
        conn.execute(
            deployment_events.insert().values(
                event="deploy",
                version=version,
            )
        )
    console.print(f"[green]✓ Deployment event logged (version={escape(version)})[/green]")


@app.command()
def daemon():
    """Start the background daemon (called by entrypoint.sh)."""
    from flowbyte.config.models import AppSettings
    from flowbyte.config.loader import load_global_config
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.logging.config import configure_logging
    from flowbyte.logging.db_sink import AsyncDBSink
    from flowbyte.scheduler.daemon import start_daemon

    settings = AppSettings()
    global_cfg = load_global_config(settings.config_path)
    db_sink: AsyncDBSink | None = None
    if global_cfg.logging.db_sink.enabled:
        engine = get_internal_engine(settings.db_url)
        db_sink = AsyncDBSink(
            engine,
            queue_size=global_cfg.logging.db_sink.queue_size,
            max_payload_bytes=global_cfg.logging.db_sink.max_payload_bytes,
            min_level=global_cfg.logging.db_sink.min_level,
        )
    configure_logging(log_level=settings.log_level, env=settings.env, db_sink=db_sink)
    start_daemon()
