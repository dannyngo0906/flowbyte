"""Pipeline management commands: init, validate, list, enable, disable, delete, creds."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from flowbyte.config.loader import PIPELINE_TEMPLATE, load_global_config, load_pipeline_config
from flowbyte.config.models import AppSettings

app = typer.Typer(help="Pipeline management")
console = Console()


@app.command()
def bootstrap():
    """First-time setup: generate master.key and config templates."""
    from flowbyte.bootstrap.init import run_bootstrap
    run_bootstrap()


@app.command("init-master-key")
def init_master_key(
    path: str = typer.Option("", "--path", "-p", help="Override master key path"),
    skip_confirm: bool = typer.Option(False, "--skip-confirm", help="Skip backup confirmation (CI/testing)"),
):
    """Generate master key at /etc/flowbyte/master.key (or FLOWBYTE_MASTER_KEY_PATH)."""
    from flowbyte.config.models import AppSettings
    from flowbyte.security.master_key import MasterKey, MasterKeyError

    settings = AppSettings()
    key_path = Path(path) if path else Path(settings.master_key_path)

    try:
        key = MasterKey.generate_and_save(key_path)
    except MasterKeyError as e:
        console.print(f"[red]✗ {e}[/red]")
        raise typer.Exit(1)

    console.print(f"[green]✓ master.key created at {key_path}[/green]")
    console.print(f"  Fingerprint (SHA-256): {key.fingerprint}")
    console.print()
    console.print("[bold yellow]⚠️  CRITICAL: Back up master.key NOW.[/bold yellow]")
    console.print("   All encrypted credentials are unrecoverable without it.")
    console.print(f"   Backup command:  sudo cp {key_path} /your/secure/backup/location")

    if not skip_confirm:
        confirmed = typer.prompt("\nType 'BACKED-UP' when done", default="")
        if confirmed.strip() != "BACKED-UP":
            console.print("[yellow]Warning: Continuing without confirmed backup.[/yellow]")


@app.command()
def init(name: str = typer.Argument(..., help="Pipeline name (lowercase, underscores allowed)")):
    """Create a new pipeline YAML template."""
    settings = AppSettings()
    pipelines_dir = Path(settings.pipelines_dir)
    pipelines_dir.mkdir(parents=True, exist_ok=True)
    path = pipelines_dir / f"{name}.yml"

    if path.exists():
        console.print(f"[red]✗ Pipeline '{name}' already exists at {path}[/red]")
        raise typer.Exit(1)

    content = PIPELINE_TEMPLATE.format(name=name)
    path.write_text(content)
    console.print(f"[green]✓ Created {path}[/green]")
    console.print()
    console.print(f"  Edit YAML to configure resources + schedule, then:")
    console.print(f"    flowbyte creds set {name} --kind haravan")
    console.print(f"    flowbyte validate {name}")
    console.print(f"    flowbyte enable {name}")


@app.command()
def validate(name: str = typer.Argument(...)):
    """Test Haravan + Postgres connection for a pipeline (<5s)."""
    settings = AppSettings()
    path = Path(settings.pipelines_dir) / f"{name}.yml"

    if not path.exists():
        console.print(f"[red]✗ Pipeline not found: {path}[/red]")
        raise typer.Exit(1)

    try:
        cfg = load_pipeline_config(path)
    except Exception as e:
        console.print(f"[red]✗ YAML validation failed: {e}[/red]")
        raise typer.Exit(2)

    # Schedule collision check
    collisions = cfg.detect_schedule_collisions()
    if collisions:
        console.print("[yellow]⚠ Schedule collisions detected:[/yellow]")
        for minute, resources in collisions.items():
            console.print(f"  Minute '{minute}': {', '.join(resources)}")
        console.print("  → Risk of misfire with max_workers=1. Consider staggering schedules.")

    # Test Haravan connection
    console.print("Testing Haravan connection...", end="  ")
    try:
        from flowbyte.scheduler.reconciler import _load_credentials
        from flowbyte.haravan.client import HaravanClient
        from flowbyte.haravan.exceptions import HaravanAuthError
        from flowbyte.haravan.token_bucket import HaravanTokenBucket
        from flowbyte.db.engine import get_internal_engine

        engine = get_internal_engine(settings.db_url)
        creds = _load_credentials(engine, cfg.haravan_credentials_ref)
        client = HaravanClient(cfg.haravan_shop_domain, creds["access_token"], HaravanTokenBucket())
        shop = client.test_connection()
        console.print(f"[green]✓ OK[/green] (shop: {shop.get('shop', {}).get('name', 'unknown')})")
    except HaravanAuthError:
        console.print("[red]✗ FAILED[/red]: 401/403 Unauthorized")
        console.print("  Hint: Check access token at Haravan Admin → Apps → Private apps")
        raise typer.Exit(3)
    except Exception as e:
        console.print(f"[red]✗ FAILED[/red]: {e}")
        raise typer.Exit(1)

    # Test Postgres destination
    console.print("Testing Postgres destination...", end="  ")
    try:
        from flowbyte.scheduler.reconciler import _load_credentials, _build_dest_url
        from flowbyte.db.engine import get_dest_engine
        from sqlalchemy import text

        pg_creds = _load_credentials(engine, cfg.destination.credentials_ref)
        dest_url = _build_dest_url(cfg, pg_creds)
        dest_engine = get_dest_engine(dest_url)
        with dest_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        console.print("[green]✓ OK[/green]")
    except Exception as e:
        console.print(f"[red]✗ FAILED[/red]: {e}")
        console.print("  Hint: check host, port, user, database and pg_hba.conf/firewall")
        raise typer.Exit(1)

    console.print()
    console.print("[green]Summary: Pipeline ready to enable.[/green]")
    console.print(f"  flowbyte enable {name}")


@app.command("list")
def list_pipelines():
    """List all pipelines with enabled state."""
    settings = AppSettings()
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import pipelines
    from sqlalchemy import select

    engine = get_internal_engine(settings.db_url)
    with engine.connect() as conn:
        rows = conn.execute(select(pipelines).order_by(pipelines.c.name)).all()

    if not rows:
        console.print("No pipelines found.")
        console.print("  Run [bold]flowbyte init <name>[/bold] to create one.")
        return

    table = Table("NAME", "ENABLED", "UPDATED")
    for r in rows:
        enabled = "[green]yes[/green]" if r.enabled else "[dim]no[/dim]"
        table.add_row(r.name, enabled, str(r.updated_at)[:16])
    console.print(table)


@app.command()
def enable(name: str = typer.Argument(...)):
    """Enable scheduler for a pipeline."""
    _set_enabled(name, True)


@app.command()
def disable(name: str = typer.Argument(...)):
    """Disable scheduler for a pipeline."""
    _set_enabled(name, False)


@app.command()
def delete(
    name: str = typer.Argument(...),
    force: bool = typer.Option(False, "--force", help="Kill running sync and delete immediately"),
):
    """Delete a pipeline (refuses if sync is running unless --force)."""
    settings = AppSettings()
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import pipelines, sync_runs
    from sqlalchemy import delete as sql_delete, select

    engine = get_internal_engine(settings.db_url)

    with engine.connect() as conn:
        running_rows = conn.execute(
            select(sync_runs.c.resource).where(
                sync_runs.c.pipeline == name,
                sync_runs.c.status == "running",
            )
        ).fetchall()

    if running_rows and not force:
        resources = ", ".join(r.resource for r in running_rows)
        console.print(f"[yellow]⚠ Sync is currently running for: {resources}[/yellow]")
        console.print("  Wait for it to finish, or use [bold]--force[/bold] to kill and delete.")
        raise typer.Exit(1)

    if not force:
        confirmed = typer.confirm(f"Delete pipeline '{name}'?", default=False)
        if not confirmed:
            console.print("Aborted.")
            return

    with engine.begin() as conn:
        result = conn.execute(sql_delete(pipelines).where(pipelines.c.name == name))

    if result.rowcount == 0:
        console.print(f"[red]✗ Pipeline '{name}' not found.[/red]")
        raise typer.Exit(1)

    path = Path(settings.pipelines_dir) / f"{name}.yml"
    if path.exists():
        path.unlink()

    console.print(f"[green]✓ Pipeline '{name}' deleted.[/green]")


@app.command("creds")
def creds(
    action: str = typer.Argument(..., help="set | list | delete"),
    ref: str = typer.Argument("", help="Credential reference name"),
    kind: str = typer.Option("haravan", "--kind", help="haravan | postgres"),
):
    """Manage encrypted credentials."""
    if action == "set":
        _creds_set(ref, kind)
    elif action == "list":
        _creds_list()
    elif action == "delete":
        _creds_delete(ref)
    else:
        console.print(f"[red]Unknown action: {action}[/red]")
        raise typer.Exit(2)


# ── Internal helpers ──────────────────────────────────────────────────────────


def _set_enabled(name: str, enabled: bool) -> None:
    settings = AppSettings()
    from flowbyte.config.loader import load_pipeline_config
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import pipelines
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from datetime import datetime, timezone

    path = Path(settings.pipelines_dir) / f"{name}.yml"
    if not path.exists():
        console.print(f"[red]✗ Pipeline YAML not found: {path}[/red]")
        raise typer.Exit(1)

    cfg = load_pipeline_config(path)
    engine = get_internal_engine(settings.db_url)
    with engine.begin() as conn:
        conn.execute(
            pg_insert(pipelines)
            .values(
                name=name,
                yaml_content=path.read_text(),
                config_json=cfg.model_dump(mode="json"),
                enabled=enabled,
            )
            .on_conflict_do_update(
                index_elements=["name"],
                set_={
                    "enabled": enabled,
                    "yaml_content": path.read_text(),
                    "config_json": cfg.model_dump(mode="json"),
                    "updated_at": datetime.now(timezone.utc),
                },
            )
        )

    state = "[green]enabled[/green]" if enabled else "[dim]disabled[/dim]"
    console.print(f"✓ Pipeline '{name}' {state}")
    if enabled:
        console.print("  Reconciler will pick up within 10s.")


def _creds_set(ref: str, kind: str) -> None:
    import getpass, json
    from flowbyte.config.models import AppSettings
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import credentials
    from flowbyte.security import Encryptor, MasterKey
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from datetime import datetime, timezone

    settings = AppSettings()

    if kind == "haravan":
        shop_domain = typer.prompt("Haravan shop domain")
        _SHOP_DOMAIN_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9\-\.]{1,61}[a-zA-Z0-9]$")
        if not _SHOP_DOMAIN_RE.match(shop_domain):
            console.print("[red]Invalid shop domain. Use format: myshop.myharavan.com[/red]")
            raise typer.Exit(2)
        access_token = getpass.getpass("Haravan access token: ")
        plaintext = json.dumps({"shop_domain": shop_domain, "access_token": access_token})
    elif kind == "postgres":
        password = getpass.getpass("Postgres password: ")
        plaintext = json.dumps({"password": password})
    else:
        console.print(f"[red]Unknown kind: {kind}[/red]")
        raise typer.Exit(2)

    master_key = MasterKey.load(settings.master_key_path)
    enc = Encryptor(master_key.raw)
    ct = enc.encrypt(plaintext, associated_data=ref.encode())

    engine = get_internal_engine(settings.db_url)
    with engine.begin() as conn:
        conn.execute(
            pg_insert(credentials)
            .values(ref=ref, kind=kind, ciphertext=ct.serialize())
            .on_conflict_do_update(
                index_elements=["ref"],
                set_={"ciphertext": ct.serialize(), "updated_at": datetime.now(timezone.utc)},
            )
        )

    console.print(f"[green]✓ Credentials saved as '{ref}' (kind={kind})[/green]")


def _creds_list() -> None:
    settings = AppSettings()
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import credentials
    from sqlalchemy import select

    engine = get_internal_engine(settings.db_url)
    with engine.connect() as conn:
        rows = conn.execute(select(credentials.c.ref, credentials.c.kind, credentials.c.updated_at)).all()

    if not rows:
        console.print("No credentials stored.")
        return

    table = Table("REF", "KIND", "UPDATED")
    for r in rows:
        table.add_row(r.ref, r.kind, str(r.updated_at)[:16])
    console.print(table)


def _creds_delete(ref: str) -> None:
    settings = AppSettings()
    from flowbyte.db.engine import get_internal_engine
    from flowbyte.db.internal_schema import credentials
    from sqlalchemy import delete as sql_delete

    confirmed = typer.confirm(f"Delete credentials '{ref}'?", default=False)
    if not confirmed:
        return

    engine = get_internal_engine(settings.db_url)
    with engine.begin() as conn:
        conn.execute(sql_delete(credentials).where(credentials.c.ref == ref))
    console.print(f"[green]✓ Credentials '{ref}' deleted.[/green]")
