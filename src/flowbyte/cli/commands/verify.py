"""Verify command: compare local DB record counts against Haravan API counts."""
from __future__ import annotations

import re

import typer
from rich.console import Console
from rich.markup import escape
from rich.table import Table

app = typer.Typer()
console = Console()

_PIPELINE_NAME_RE = re.compile(r"^[a-z0-9_]{1,32}$")

# Resources that have a /count.json endpoint on Haravan
_COUNTABLE_RESOURCES = ("orders", "customers", "products")


def _assert_valid_pipeline(name: str) -> None:
    if not _PIPELINE_NAME_RE.match(name):
        console.print(
            f"[red]✗ Invalid pipeline name '{escape(name)}'.[/red] "
            "Use only lowercase letters, digits, underscores (max 32 chars)."
        )
        raise typer.Exit(2)


@app.command()
def verify(
    pipeline: str = typer.Argument(..., help="Pipeline name"),
    resource: str = typer.Option(
        "all",
        "--resource",
        "-r",
        help="Resource to check: orders, customers, products, or all",
    ),
    tolerance: float = typer.Option(
        0.1,
        "--tolerance",
        help="Max acceptable deviation % (default: 0.1)",
        min=0.0,
        max=100.0,
    ),
) -> None:
    """Compare local DB record counts against Haravan API (AC-3.4: ±0.1%)."""
    _assert_valid_pipeline(pipeline)

    if resource != "all" and resource not in _COUNTABLE_RESOURCES:
        console.print(
            f"[red]✗ Unknown resource '{escape(resource)}'.[/red] "
            f"Supported: {', '.join(_COUNTABLE_RESOURCES)}, all"
        )
        raise typer.Exit(2)

    resources_to_check = (
        list(_COUNTABLE_RESOURCES) if resource == "all" else [resource]
    )

    from flowbyte.config.models import AppSettings, PipelineConfig
    from flowbyte.db.destination_schema import get_table
    from flowbyte.db.engine import get_internal_engine, get_dest_engine
    from flowbyte.db.internal_schema import pipelines
    from flowbyte.haravan.client import HaravanClient
    from flowbyte.haravan.token_bucket import HaravanTokenBucket
    from flowbyte.scheduler.reconciler import _load_credentials, _build_dest_url
    from sqlalchemy import select, func

    settings = AppSettings()
    internal_engine = get_internal_engine(settings.db_url)

    # Load pipeline config from DB
    with internal_engine.connect() as conn:
        row = conn.execute(
            select(pipelines).where(pipelines.c.name == pipeline)
        ).one_or_none()

    if row is None:
        console.print(f"[red]✗ Pipeline '{escape(pipeline)}' not found in DB.[/red]")
        console.print(f"  Hint: run [bold]flowbyte enable {escape(pipeline)}[/bold] first.")
        raise typer.Exit(1)

    try:
        cfg = PipelineConfig.model_validate(row.config_json)
    except Exception as e:
        console.print(f"[red]✗ Pipeline config invalid: {escape(str(e))}[/red]")
        raise typer.Exit(1)

    # Build Haravan client
    try:
        haravan_creds = _load_credentials(internal_engine, cfg.haravan_credentials_ref)
        client = HaravanClient(
            cfg.haravan_shop_domain,
            haravan_creds["access_token"],
            HaravanTokenBucket(),
        )
    except Exception as e:
        console.print(f"[red]✗ Failed to load Haravan credentials ({type(e).__name__}).[/red]")
        console.print("  Hint: check haravan_credentials_ref in pipeline config.")
        raise typer.Exit(1)

    # Build destination DB engine — never print str(e) here: SQLAlchemy exceptions
    # include the full connection URL (with plaintext password) in their message.
    try:
        pg_creds = _load_credentials(internal_engine, cfg.destination.credentials_ref)
        dest_url = _build_dest_url(cfg, pg_creds)
        dest_engine = get_dest_engine(dest_url)
    except Exception as e:
        console.print(f"[red]✗ Failed to build destination DB connection ({type(e).__name__}).[/red]")
        console.print("  Hint: check host, port, user, database in pipeline config.")
        raise typer.Exit(1)

    console.print(
        f"\n[bold]Record Count Verification[/bold] — {escape(pipeline)}"
        f"  (tolerance: {tolerance}%)\n"
    )

    table = Table("Resource", "Haravan", "Local DB", "Deviation", "Status")
    all_pass = True

    for res in resources_to_check:
        # Fetch remote count
        try:
            remote_count = client.get_count(res)
        except Exception as e:
            console.print(f"[red]✗ Failed to fetch {escape(res)} count from Haravan: {escape(str(e))}[/red]")
            all_pass = False
            table.add_row(
                escape(res), "—", "—", "—", "[red]✗ API ERR[/red]"
            )
            continue

        # Fetch local count — use SQLAlchemy Table object, never string interpolation
        try:
            tbl = get_table(res)
            with dest_engine.connect() as conn:
                local_count = conn.execute(
                    select(func.count()).select_from(tbl).where(tbl.c._deleted_at.is_(None))
                ).scalar() or 0
        except Exception as e:
            # Do not print str(e): connection errors may include the DB URL with password.
            console.print(f"[red]✗ Failed to query {escape(res)} count from DB ({type(e).__name__}).[/red]")
            all_pass = False
            table.add_row(
                escape(res),
                f"{remote_count:,}",
                "—",
                "—",
                "[red]✗ DB ERR[/red]",
            )
            continue

        deviation_pct = abs(local_count - remote_count) / max(remote_count, 1) * 100
        passed = deviation_pct <= tolerance

        if not passed:
            all_pass = False

        status_str = (
            "[green]✓ OK[/green]" if passed else "[red]✗ FAIL[/red]"
        )
        table.add_row(
            escape(res),
            f"{remote_count:,}",
            f"{local_count:,}",
            f"{deviation_pct:.2f}%",
            status_str,
        )

    console.print(table)
    console.print()

    if all_pass:
        n = len(resources_to_check)
        label = f"{'All ' if n > 1 else ''}{n} resource{'s' if n > 1 else ''}"
        console.print(f"[green]{label} within {tolerance}% tolerance.[/green]")
    else:
        console.print(f"[red]One or more resources exceed {tolerance}% tolerance.[/red]")
        raise typer.Exit(1)
