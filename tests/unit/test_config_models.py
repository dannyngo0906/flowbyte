"""Unit tests for Pydantic config models."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from flowbyte.config.models import (
    GlobalConfig,
    PipelineConfig,
    PostgresDestConfig,
    ResourceConfig,
    WeeklyFullRefreshConfig,
)


def _dest():
    return PostgresDestConfig(
        host="localhost",
        port=5432,
        user="flowbyte",
        database="flowbyte_data",
        credentials_ref="pg_main",
    )


def _minimal_pipeline(**overrides):
    defaults = {
        "name": "shop_main",
        "haravan_credentials_ref": "shop_main",
        "haravan_shop_domain": "myshop.myharavan.com",
        "destination": _dest(),
    }
    defaults.update(overrides)
    return defaults


class TestWeeklyFullRefreshConfig:
    def test_default_enabled_with_cron(self):
        cfg = WeeklyFullRefreshConfig()
        assert cfg.enabled is True
        assert cfg.cron is not None

    def test_custom_cron_valid(self):
        cfg = WeeklyFullRefreshConfig(cron="0 4 * * 0")
        assert cfg.cron == "0 4 * * 0"

    def test_invalid_cron_raises(self):
        with pytest.raises(ValidationError):
            WeeklyFullRefreshConfig(cron="not a cron")

    def test_disabled(self):
        cfg = WeeklyFullRefreshConfig(enabled=False)
        assert cfg.enabled is False


class TestResourceConfig:
    def test_default_enabled_incremental(self):
        cfg = ResourceConfig()
        assert cfg.enabled is True
        assert cfg.sync_mode == "incremental"

    def test_full_refresh_mode(self):
        cfg = ResourceConfig(sync_mode="full_refresh")
        assert cfg.sync_mode == "full_refresh"

    def test_invalid_sync_mode_raises(self):
        with pytest.raises(ValidationError):
            ResourceConfig(sync_mode="delta")

    def test_schedule_validation(self):
        cfg = ResourceConfig(schedule="*/30 * * * *")
        assert cfg.schedule == "*/30 * * * *"

    def test_invalid_schedule_raises(self):
        with pytest.raises(ValidationError):
            ResourceConfig(schedule="not valid cron")


class TestPipelineConfig:
    def test_minimal_config_applies_defaults(self):
        cfg = PipelineConfig(**_minimal_pipeline())
        assert "orders" in cfg.resources
        assert "customers" in cfg.resources
        assert "products" in cfg.resources
        assert "inventory_levels" in cfg.resources
        assert "locations" in cfg.resources
        assert "variants" not in cfg.resources

    def test_all_phase1_resources_enabled_by_default(self):
        cfg = PipelineConfig(**_minimal_pipeline())
        for name in ["orders", "customers", "products", "inventory_levels", "locations"]:
            assert cfg.resources[name].enabled is True

    def test_detect_no_collisions_with_staggered_defaults(self):
        cfg = PipelineConfig(**_minimal_pipeline())
        collisions = cfg.detect_schedule_collisions()
        assert collisions == {}

    def test_detect_collision_when_same_minute(self):
        cfg = PipelineConfig(
            **_minimal_pipeline(
                resources={
                    "orders": ResourceConfig(schedule="0 */2 * * *"),
                    "customers": ResourceConfig(schedule="0 */2 * * *"),
                }
            )
        )
        collisions = cfg.detect_schedule_collisions()
        assert len(collisions) > 0

    def test_override_single_resource(self):
        cfg = PipelineConfig(
            **_minimal_pipeline(
                resources={"orders": ResourceConfig(enabled=False)}
            )
        )
        assert cfg.resources["orders"].enabled is False
        assert cfg.resources["customers"].enabled is True  # default applied

    def test_name_pattern_allows_underscores(self):
        cfg = PipelineConfig(**_minimal_pipeline(name="shop_main_2"))
        assert cfg.name == "shop_main_2"

    def test_name_pattern_rejects_uppercase(self):
        with pytest.raises(ValidationError):
            PipelineConfig(**_minimal_pipeline(name="Shop_Main"))

    def test_name_pattern_rejects_spaces(self):
        with pytest.raises(ValidationError):
            PipelineConfig(**_minimal_pipeline(name="shop main"))


class TestGlobalConfig:
    def test_default_timezone(self):
        cfg = GlobalConfig()
        assert cfg.timezone == "Asia/Ho_Chi_Minh"

    def test_custom_timezone(self):
        cfg = GlobalConfig(timezone="UTC")
        assert cfg.timezone == "UTC"

    def test_telegram_not_enabled_by_default(self):
        cfg = GlobalConfig()
        # Either telegram is None or has enabled=False
        telegram = cfg.telegram
        assert telegram is None or telegram.enabled is False

    def test_validation_thresholds_have_defaults(self):
        cfg = GlobalConfig()
        assert cfg.validation.fetch_upsert_parity_max_skip_pct > 0
        assert cfg.validation.soft_delete_sanity_max_pct > 0

    def test_soft_delete_sanity_threshold(self):
        cfg = GlobalConfig()
        assert cfg.validation.soft_delete_sanity_max_pct == 5.0

    def test_weekly_full_drift_thresholds(self):
        cfg = GlobalConfig()
        assert cfg.validation.weekly_full_drift_warn_pct == 5.0
        assert cfg.validation.weekly_full_drift_fail_pct == 20.0
