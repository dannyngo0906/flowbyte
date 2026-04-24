"""Flowbyte CLI entry point — all commands registered here."""
import typer
from rich.console import Console

from flowbyte.cli.commands import (
    alerting,
    observability,
    pipeline,
    sync as sync_cmd,
    system,
)

app = typer.Typer(
    name="flowbyte",
    help="ETL tool syncing Haravan → PostgreSQL",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
console = Console()

# ── Register command groups ───────────────────────────────────────────────────

app.add_typer(pipeline.app, name="")   # inline — flowbyte init, list, enable, etc.
app.add_typer(sync_cmd.app, name="")
app.add_typer(observability.app, name="")
app.add_typer(alerting.app, name="")
app.add_typer(system.app, name="")


def main() -> None:
    app()
