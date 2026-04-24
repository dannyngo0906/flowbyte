"""System commands: health, cleanup, daemon (internal)."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

import typer
from rich.console import Console

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
        console.print(f"[red]✗ DB unreachable: {e}[/red]")
        raise typer.Exit(3)

    checks: dict[str, bool] = {}

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
        console.print(f"  sync_logs (success): {stats['sync_logs_success_rows']} rows")
        console.print(f"  sync_logs (errors): {stats['sync_logs_error_rows']} rows")
        console.print(f"  validation_results: {stats['validation_results_rows']} rows")
    else:
        cleanup_tick(engine)
        console.print("[green]✓ Cleanup complete.[/green]")


@app.command()
def daemon():
    """Start the background daemon (called by entrypoint.sh)."""
    from flowbyte.config.models import AppSettings
    from flowbyte.logging.config import configure_logging
    from flowbyte.scheduler.daemon import start_daemon

    settings = AppSettings()
    configure_logging(log_level=settings.log_level, env=settings.env)
    start_daemon()
