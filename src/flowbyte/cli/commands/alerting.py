"""Alert commands: alert test."""
from __future__ import annotations

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def alert(action: str = typer.Argument(..., help="test")):
    """Alert management."""
    if action == "test":
        _alert_test()
    else:
        console.print(f"[red]Unknown action: {action}[/red]")
        raise typer.Exit(2)


def _alert_test() -> None:
    from flowbyte.config.loader import load_global_config
    from flowbyte.alerting.telegram import TelegramAlerter

    cfg = load_global_config()
    tg = cfg.telegram

    if not tg.enabled:
        console.print("[yellow]⚠ Telegram not enabled in config.yml[/yellow]")
        raise typer.Exit(1)

    alerter = TelegramAlerter(
        bot_token=tg.bot_token.get_secret_value(),
        chat_id=tg.chat_id,
    )
    try:
        alerter.test()
        console.print("[green]✓ Telegram test message sent successfully.[/green]")
    except Exception as e:
        console.print(f"[red]✗ Failed to send Telegram alert: {e}[/red]")
        console.print("  Check: bot_token, chat_id, and outbound HTTPS to api.telegram.org")
        raise typer.Exit(1)
