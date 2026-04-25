"""Unit tests for YAML config loader — filesystem-backed but no DB/HTTP."""
from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from flowbyte.config.loader import PIPELINE_TEMPLATE, load_global_config, load_pipeline_config
from flowbyte.config.models import GlobalConfig, PipelineConfig


# ── Global config ─────────────────────────────────────────────────────────────


def test_load_global_config_returns_defaults_when_missing(tmp_path):
    cfg = load_global_config(tmp_path / "nonexistent.yml")
    assert isinstance(cfg, GlobalConfig)
    assert cfg.timezone == "Asia/Ho_Chi_Minh"


def test_load_global_config_reads_timezone(tmp_path):
    p = tmp_path / "config.yml"
    p.write_text("timezone: UTC\n")
    cfg = load_global_config(p)
    assert cfg.timezone == "UTC"


def test_load_global_config_reads_telegram_section(tmp_path):
    p = tmp_path / "config.yml"
    p.write_text("telegram:\n  enabled: true\n  bot_token: 'tok'\n  chat_id: '123'\n")
    cfg = load_global_config(p)
    assert cfg.telegram.enabled is True
    assert cfg.telegram.chat_id == "123"


def test_load_global_config_empty_file_returns_defaults(tmp_path):
    p = tmp_path / "config.yml"
    p.write_text("")
    cfg = load_global_config(p)
    assert cfg.timezone == "Asia/Ho_Chi_Minh"


# ── Pipeline config ───────────────────────────────────────────────────────────


def _write_minimal_pipeline(tmp_path: Path, name: str = "shop_main") -> Path:
    path = tmp_path / f"{name}.yml"
    path.write_text(f"""\
name: {name}
haravan_credentials_ref: "{name}"
haravan_shop_domain: "test.myharavan.com"
destination:
  host: localhost
  port: 5432
  user: flowbyte
  database: flowbyte_data
  credentials_ref: pg_main
""")
    return path


def test_load_pipeline_config_minimal(tmp_path):
    path = _write_minimal_pipeline(tmp_path)
    cfg = load_pipeline_config(path)
    assert cfg.name == "shop_main"
    assert cfg.haravan_shop_domain == "test.myharavan.com"


def test_load_pipeline_config_applies_resource_defaults(tmp_path):
    path = _write_minimal_pipeline(tmp_path)
    cfg = load_pipeline_config(path)
    for res in ["orders", "customers", "products", "inventory_levels", "locations"]:
        assert res in cfg.resources, f"Missing resource: {res}"
    assert "variants" not in cfg.resources


def test_load_pipeline_config_invalid_yaml_raises(tmp_path):
    path = tmp_path / "bad.yml"
    path.write_text("name: [unclosed bracket\n")
    with pytest.raises(Exception):
        load_pipeline_config(path)


def test_load_pipeline_config_missing_required_field_raises(tmp_path):
    path = tmp_path / "incomplete.yml"
    path.write_text("name: shop_main\n")  # missing haravan_credentials_ref etc.
    with pytest.raises(ValidationError):
        load_pipeline_config(path)


def test_load_pipeline_config_with_legacy_transform_section_is_silently_ignored(tmp_path):
    """YAML files with old transform: sections must still load without errors (backward compat)."""
    path = tmp_path / "shop.yml"
    path.write_text("""\
name: shop_main
haravan_credentials_ref: "shop_main"
haravan_shop_domain: "test.myharavan.com"
destination:
  host: localhost
  port: 5432
  user: flowbyte
  database: flowbyte_data
  credentials_ref: pg_main
resources:
  orders:
    enabled: true
    sync_mode: incremental
    schedule: "0 */2 * * *"
    transform:
      rename:
        total_price: revenue
      skip:
        - note_attributes
      type_override:
        total_price: "numeric(12,2)"
""")
    cfg = load_pipeline_config(path)
    assert cfg.resources["orders"].enabled is True
    assert cfg.resources["orders"].sync_mode == "incremental"
    assert not hasattr(cfg.resources["orders"], "transform")


def test_load_pipeline_config_invalid_cron_raises(tmp_path):
    path = tmp_path / "shop.yml"
    path.write_text("""\
name: shop_main
haravan_credentials_ref: "shop_main"
haravan_shop_domain: "test.myharavan.com"
destination:
  host: localhost
  port: 5432
  user: flowbyte
  database: flowbyte_data
  credentials_ref: pg_main
resources:
  orders:
    schedule: "not a cron"
""")
    with pytest.raises(ValidationError):
        load_pipeline_config(path)


def test_load_pipeline_config_does_not_contain_plaintext_credentials(tmp_path):
    path = _write_minimal_pipeline(tmp_path)
    content = path.read_text()
    assert "access_token" not in content
    assert "password" not in content


# ── Pipeline template ─────────────────────────────────────────────────────────


def test_pipeline_template_is_valid_yaml_when_parsed(tmp_path):
    content = PIPELINE_TEMPLATE.format(name="test_shop")
    path = tmp_path / "test_shop.yml"
    path.write_text(content)
    # Should parse without errors
    cfg = load_pipeline_config(path)
    assert cfg.name == "test_shop"


def test_pipeline_template_has_no_plaintext_secret(tmp_path):
    content = PIPELINE_TEMPLATE.format(name="myshop")
    # Template uses credentials_ref, not actual credentials
    assert "access_token:" not in content
    assert "password:" not in content
    assert "credentials_ref" in content


def test_pipeline_template_has_all_phase1_resources():
    content = PIPELINE_TEMPLATE.format(name="shop")
    for resource in ["orders", "customers", "products", "inventory_levels", "locations"]:
        assert resource in content
    assert "variants" not in content


# ── save_pipeline_config ──────────────────────────────────────────────────────


def test_save_pipeline_config_writes_valid_yaml(tmp_path):
    from flowbyte.config.loader import save_pipeline_config

    path = _write_minimal_pipeline(tmp_path)
    original = load_pipeline_config(path)
    save_pipeline_config(original, path)

    reloaded = load_pipeline_config(path)
    assert reloaded.name == original.name
    assert reloaded.haravan_shop_domain == original.haravan_shop_domain


def test_save_pipeline_config_creates_file_if_not_exists(tmp_path):
    from flowbyte.config.loader import save_pipeline_config
    from flowbyte.config.models import PostgresDestConfig

    cfg = PipelineConfig(
        name="new_shop",
        haravan_credentials_ref="new_shop",
        haravan_shop_domain="new.myharavan.com",
        destination=PostgresDestConfig(
            host="localhost", port=5432, user="fb", database="fb_data", credentials_ref="pg"
        ),
    )
    new_path = tmp_path / "new_shop.yml"
    save_pipeline_config(cfg, new_path)
    assert new_path.exists()
    reloaded = load_pipeline_config(new_path)
    assert reloaded.name == "new_shop"
