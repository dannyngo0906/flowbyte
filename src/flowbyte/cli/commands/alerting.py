"""Alert commands: alert test."""
from __future__ import annotations

import typer
from rich.console import Console
from rich.markup import escape

app = typer.Typer()
console = Console()


@app.command()
def alert(action: str = typer.Argument(..., help="test")):
    """Alert management."""
    if action == "test":
        _alert_test()
    else:
        console.print(f"[red]Unknown action: {escape(action)}[/red]")
        raise typer.Exit(2)


def _alert_test() -> None:
    from flowbyte.config.loader import load_global_config
    from flowbyte.config.models import AppSettings
    from flowbyte.alerting.telegram import TelegramAlerter

    settings = AppSettings()
    cfg = load_global_config(settings.config_path)
    tg = cfg.telegram

    if not tg.enabled:
        console.print("[yellow]⚠ Telegram not enabled in config.yml[/yellow]")
        raise typer.Exit(1)

    raw_token = tg.bot_token.get_secret_value()
    alerter = TelegramAlerter(bot_token=raw_token, chat_id=tg.chat_id)
    try:
        alerter.test()
        console.print("[green]✓ Telegram test message sent successfully.[/green]")
    except Exception as e:
        # Mask bot_token in error message before printing to terminal.
        tok_id = raw_token.split(":")[0] if ":" in raw_token else "***"
        safe_msg = str(e).replace(raw_token, f"{tok_id}:***")
        console.print(f"[red]✗ Failed to send Telegram alert: {escape(safe_msg)}[/red]")
        console.print("  Check: bot_token, chat_id, and outbound HTTPS to api.telegram.org")
        raise typer.Exit(1)
