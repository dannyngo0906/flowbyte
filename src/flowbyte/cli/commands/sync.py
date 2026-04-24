"""Sync commands: run."""
from __future__ import annotations

import sys
import time

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def run(
    pipeline: str = typer.Argument(...),
    resource: str = typer.Option(None, "--resource", "-r"),
    mode: str = typer.Option("incremental", "--mode", "-m"),
    wait: bool = typer.Option(False, "--wait", help="Block until sync completes"),
    timeout: int = typer.Option(300, "--timeout", help="Max seconds to wait"),
):
    """Trigger a manual sync (queued via sync_requests table)."""
    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import sync_requests
    from sqlalchemy import select

    settings = AppSettings()
    engine = get_internal_engine(settings.db_url)

    with engine.begin() as conn:
        result = conn.execute(
            sync_requests.insert().values(
                pipeline=pipeline,
                resource=resource,
                mode=mode,
                status="pending",
            ).returning(sync_requests.c.id)
        )
        req_id = str(result.scalar())

    console.print(f"Sync request [bold]{req_id[:8]}[/bold] queued.")

    if not wait:
        return

    console.print("Waiting for sync to complete...")
    deadline = time.time() + timeout

    while time.time() < deadline:
        with engine.connect() as conn:
            row = conn.execute(
                select(sync_requests.c.status, sync_requests.c.error, sync_requests.c.finished_at)
                .where(sync_requests.c.id == req_id)
            ).one()

        if row.status in ("done", "failed", "cancelled"):
            if row.status == "done":
                console.print(f"[green]✓ Sync done.[/green]")
            else:
                console.print(f"[red]✗ Sync {row.status}: {row.error}[/red]")
                raise typer.Exit(1)
            return

        time.sleep(2)

    console.print(f"[yellow]Timeout after {timeout}s. Request still running.[/yellow]")
    console.print(f"  Check: flowbyte inspect {req_id[:8]}")
    raise typer.Exit(4)
