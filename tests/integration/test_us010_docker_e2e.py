"""E2E tests for US-010: Docker + systemd deployment.

Mô phỏng end-user flow từ bước 1 đến bước cuối:
  Bước 1: Container khởi động → entrypoint.sh chạy alembic upgrade head
  Bước 2: flowbyte log-deployment --version ghi nhận deployment_events
  Bước 3: flowbyte daemon khởi động (scheduler beat)
  Bước cuối: flowbyte health --strict → exit 0 (healthy)

Done-when (PRD US-010):
  ✅ T-10.1  Multi-stage Dockerfile (builder + slim runtime)
  ✅ T-10.2  entrypoint.sh: alembic upgrade head trước daemon + Telegram alert on ERR
  ✅ T-10.3  docker-compose.yml: flowbyte service + postgres service
  ✅ T-10.4  flowbyte.service systemd: User=1000 / non-root
  ✅ T-10.5  Master key mount host :ro trong docker-compose.yml
  ✅ T-10.6  Memory limit 1.5GB
  ✅ T-10.7  flowbyte health --strict (exit 0 healthy / exit 3 unhealthy)
  ✅ T-10.8  README có deployment section
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text
from typer.testing import CliRunner

from flowbyte.cli.app import app
from flowbyte.db.internal_schema import deployment_events, scheduler_heartbeat, sync_requests

pytestmark = pytest.mark.integration

_PROJECT_ROOT = Path(__file__).parent.parent.parent
runner = CliRunner()

_PIPELINE = "shop_deploy_test"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _db_url(pg_container) -> str:
    raw = pg_container.get_connection_url()
    return (
        raw.replace("postgresql+psycopg2://", "postgresql+psycopg://")
           .replace("postgresql://", "postgresql+psycopg://")
    )


def _seed_fresh_heartbeat(engine) -> None:
    """Insert a scheduler_heartbeat row with last_beat = now (simulates running daemon)."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM scheduler_heartbeat"))
        conn.execute(
            text(
                "INSERT INTO scheduler_heartbeat (id, last_beat, daemon_started_at, version) "
                "VALUES (1, NOW(), NOW(), '1.0.0')"
            )
        )


def _seed_stale_heartbeat(engine, hours_ago: int = 3) -> None:
    """Insert a heartbeat that is stale (last_beat = N hours ago)."""
    stale_ts = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM scheduler_heartbeat"))
        conn.execute(
            text(
                "INSERT INTO scheduler_heartbeat (id, last_beat, daemon_started_at, version) "
                "VALUES (1, :ts, :ts, '1.0.0')"
            ),
            {"ts": stale_ts},
        )


def _seed_stuck_request(engine) -> None:
    """Insert a sync_request claimed >5 min ago (triggers no_stuck_requests check)."""
    stuck_ts = datetime.now(timezone.utc) - timedelta(minutes=10)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO sync_requests (id, pipeline, mode, status, requested_at, claimed_at) "
                "VALUES (gen_random_uuid(), :p, 'incremental', 'claimed', NOW(), :ts)"
            ),
            {"p": _PIPELINE, "ts": stuck_ts},
        )


def _truncate_health_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE scheduler_heartbeat, sync_requests, deployment_events CASCADE"))


# ── T-10.1 / T-10.2 / T-10.3 / T-10.4 / T-10.5 / T-10.6 / T-10.7 / T-10.8
#    Artifact Validation — no DB required ─────────────────────────────────────


class TestDockerArtifactValidation:
    """Validate Docker build artifacts on disk (no container/DB required)."""

    # T-10.1 Multi-stage Dockerfile
    def test_dockerfile_exists(self):
        assert (_PROJECT_ROOT / "Dockerfile").is_file()

    def test_dockerfile_has_builder_stage(self):
        content = (_PROJECT_ROOT / "Dockerfile").read_text()
        assert "AS builder" in content

    def test_dockerfile_has_slim_runtime_stage(self):
        content = (_PROJECT_ROOT / "Dockerfile").read_text()
        stages = [line for line in content.splitlines() if line.startswith("FROM")]
        assert len(stages) >= 2, "Must have at least 2 FROM stages (multi-stage)"
        assert any("slim" in s for s in stages), "Runtime stage should use slim image"

    def test_dockerfile_copies_venv_from_builder(self):
        content = (_PROJECT_ROOT / "Dockerfile").read_text()
        assert "COPY --from=builder" in content

    # T-10.4 Non-root UID 1000
    def test_dockerfile_creates_non_root_user_uid_1000(self):
        content = (_PROJECT_ROOT / "Dockerfile").read_text()
        assert "-u 1000" in content or "useradd -u 1000" in content

    def test_dockerfile_sets_user_flowbyte(self):
        content = (_PROJECT_ROOT / "Dockerfile").read_text()
        assert "USER flowbyte" in content or "USER 1000" in content

    # T-10.7 Healthcheck
    def test_dockerfile_has_healthcheck(self):
        content = (_PROJECT_ROOT / "Dockerfile").read_text()
        assert "HEALTHCHECK" in content

    def test_dockerfile_healthcheck_uses_flowbyte_health_strict(self):
        content = (_PROJECT_ROOT / "Dockerfile").read_text()
        assert "flowbyte health --strict" in content

    # T-10.2 Entrypoint
    def test_entrypoint_script_exists(self):
        assert (_PROJECT_ROOT / "docker" / "entrypoint.sh").is_file()

    def test_entrypoint_runs_alembic_before_daemon(self):
        content = (_PROJECT_ROOT / "docker" / "entrypoint.sh").read_text()
        alembic_pos = content.find("alembic")
        daemon_pos = content.find("flowbyte daemon")
        assert alembic_pos != -1, "entrypoint.sh must call alembic"
        assert daemon_pos != -1, "entrypoint.sh must call flowbyte daemon"
        assert alembic_pos < daemon_pos, "alembic must run BEFORE flowbyte daemon"

    def test_entrypoint_sends_telegram_alert_on_error(self):
        content = (_PROJECT_ROOT / "docker" / "entrypoint.sh").read_text()
        assert "trap" in content
        assert "TELEGRAM" in content or "send_alert" in content or "_send_alert" in content

    def test_entrypoint_logs_deployment_event(self):
        content = (_PROJECT_ROOT / "docker" / "entrypoint.sh").read_text()
        assert "log-deployment" in content

    def test_entrypoint_exits_on_alembic_failure(self):
        content = (_PROJECT_ROOT / "docker" / "entrypoint.sh").read_text()
        assert "set -euo pipefail" in content or "exit 1" in content

    # T-10.3 docker-compose.yml
    def test_docker_compose_exists(self):
        assert (_PROJECT_ROOT / "docker-compose.yml").is_file()

    def test_docker_compose_has_flowbyte_service(self):
        cfg = yaml.safe_load((_PROJECT_ROOT / "docker-compose.yml").read_text())
        assert "flowbyte" in cfg["services"]

    def test_docker_compose_has_postgres_service(self):
        cfg = yaml.safe_load((_PROJECT_ROOT / "docker-compose.yml").read_text())
        assert "postgres" in cfg["services"]

    # T-10.4 docker-compose UID 1000
    def test_docker_compose_flowbyte_runs_as_uid_1000(self):
        cfg = yaml.safe_load((_PROJECT_ROOT / "docker-compose.yml").read_text())
        svc = cfg["services"]["flowbyte"]
        user = str(svc.get("user", ""))
        assert "1000" in user, f"flowbyte service user must be UID 1000, got: {user!r}"

    # T-10.5 Master key mount :ro
    def test_docker_compose_master_key_mount_readonly(self):
        content = (_PROJECT_ROOT / "docker-compose.yml").read_text()
        assert ":ro" in content
        assert "master_key" in content or "master.key" in content

    # T-10.6 Memory limit 1.5GB
    def test_docker_compose_memory_limit_1_5gb(self):
        content = (_PROJECT_ROOT / "docker-compose.yml").read_text()
        assert "1.5g" in content or "1536m" in content

    # T-10.4 systemd service
    def test_systemd_service_exists(self):
        assert (_PROJECT_ROOT / "docker" / "flowbyte.service").is_file()

    def test_systemd_service_has_user_1000(self):
        content = (_PROJECT_ROOT / "docker" / "flowbyte.service").read_text()
        assert "User=1000" in content

    def test_systemd_service_requires_docker(self):
        content = (_PROJECT_ROOT / "docker" / "flowbyte.service").read_text()
        assert "docker" in content.lower()

    def test_systemd_service_restart_on_failure(self):
        content = (_PROJECT_ROOT / "docker" / "flowbyte.service").read_text()
        assert "Restart=" in content

    # T-10.8 README deployment section
    def test_readme_has_deployment_section(self):
        readme = _PROJECT_ROOT / "README.md"
        assert readme.is_file()
        content = readme.read_text()
        assert "## Deployment" in content or "# Deployment" in content or "deployment" in content.lower()


# ── T-10.7 flowbyte health --strict — với testcontainers DB ──────────────────


class TestHealthCommandE2E:
    """E2E tests for `flowbyte health --strict` using real Postgres.

    Mỗi test simulate một state khác nhau của container sau khi boot.
    """

    @pytest.fixture(autouse=True)
    def clean(self, internal_engine):
        _truncate_health_tables(internal_engine)
        yield
        _truncate_health_tables(internal_engine)

    # ── Happy path ─────────────────────────────────────────────────────────────

    def test_health_strict_exits_0_when_heartbeat_fresh(self, internal_engine, pg_container):
        """Bước cuối: daemon chạy, heartbeat fresh → health --strict exit 0."""
        _seed_fresh_heartbeat(internal_engine)
        db_url = _db_url(pg_container)

        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})

        assert result.exit_code == 0, f"Expected 0, got {result.exit_code}\n{result.output}"
        assert "Healthy" in result.output

    def test_health_without_strict_exits_0_when_healthy(self, internal_engine, pg_container):
        """health (no --strict) cũng trả 0 khi hệ thống healthy."""
        _seed_fresh_heartbeat(internal_engine)
        db_url = _db_url(pg_container)

        result = runner.invoke(app, ["health"], env={"FLOWBYTE_DB_URL": db_url})

        assert result.exit_code == 0

    def test_health_output_shows_check_results(self, internal_engine, pg_container):
        """Output phải liệt kê từng check (heartbeat_fresh, no_stuck_requests)."""
        _seed_fresh_heartbeat(internal_engine)
        db_url = _db_url(pg_container)

        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})

        assert "heartbeat_fresh" in result.output
        assert "no_stuck_requests" in result.output

    # ── Edge case: No heartbeat ────────────────────────────────────────────────

    def test_health_strict_exits_3_when_no_heartbeat(self, pg_container):
        """Daemon chưa start (không có heartbeat row) → health exit 3."""
        db_url = _db_url(pg_container)

        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})

        assert result.exit_code == 3, f"Expected 3, got {result.exit_code}\n{result.output}"
        assert "UNHEALTHY" in result.output

    def test_health_strict_shows_failing_check_when_no_heartbeat(self, pg_container):
        """Output phải chỉ rõ heartbeat_fresh là check bị fail."""
        db_url = _db_url(pg_container)

        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})

        assert "heartbeat_fresh" in result.output

    # ── Edge case: Stale heartbeat ────────────────────────────────────────────

    def test_health_strict_exits_3_when_heartbeat_stale(self, internal_engine, pg_container):
        """Heartbeat 3h cũ → scheduler dead → health exit 3."""
        _seed_stale_heartbeat(internal_engine, hours_ago=3)
        db_url = _db_url(pg_container)

        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})

        assert result.exit_code == 3
        assert "UNHEALTHY" in result.output

    def test_health_strict_exits_3_when_heartbeat_just_over_2min(
        self, internal_engine, pg_container
    ):
        """Heartbeat cũ 3 phút (vừa qua ngưỡng 2 phút) → unhealthy."""
        three_min_ago = datetime.now(timezone.utc) - timedelta(minutes=3)
        with internal_engine.begin() as conn:
            conn.execute(text("DELETE FROM scheduler_heartbeat"))
            conn.execute(
                text(
                    "INSERT INTO scheduler_heartbeat (id, last_beat, daemon_started_at, version) "
                    "VALUES (1, :ts, :ts, '1.0.0')"
                ),
                {"ts": three_min_ago},
            )
        db_url = _db_url(pg_container)

        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})

        assert result.exit_code == 3

    # ── Edge case: Stuck requests ─────────────────────────────────────────────

    def test_health_strict_exits_3_when_stuck_request(self, internal_engine, pg_container):
        """Có sync_request ở trạng thái claimed > 5 phút → unhealthy."""
        _seed_fresh_heartbeat(internal_engine)
        _seed_stuck_request(internal_engine)
        db_url = _db_url(pg_container)

        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})

        assert result.exit_code == 3
        assert "no_stuck_requests" in result.output

    def test_health_strict_exits_0_when_request_recently_claimed(
        self, internal_engine, pg_container
    ):
        """Sync_request claimed 1 phút trước (< 5 phút) → không stuck → healthy."""
        _seed_fresh_heartbeat(internal_engine)
        recent_ts = datetime.now(timezone.utc) - timedelta(minutes=1)
        with internal_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO sync_requests (id, pipeline, mode, status, requested_at, claimed_at) "
                    "VALUES (gen_random_uuid(), :p, 'incremental', 'claimed', NOW(), :ts)"
                ),
                {"p": _PIPELINE, "ts": recent_ts},
            )
        db_url = _db_url(pg_container)

        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})

        assert result.exit_code == 0

    def test_health_strict_exits_0_when_done_requests_only(
        self, internal_engine, pg_container
    ):
        """Chỉ có done requests (không bao giờ stuck) → healthy."""
        _seed_fresh_heartbeat(internal_engine)
        with internal_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO sync_requests (id, pipeline, mode, status, requested_at) "
                    "VALUES (gen_random_uuid(), :p, 'incremental', 'done', NOW())"
                ),
                {"p": _PIPELINE},
            )
        db_url = _db_url(pg_container)

        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})

        assert result.exit_code == 0

    # ── Edge case: DB unreachable ─────────────────────────────────────────────

    def test_health_strict_exits_3_when_db_unreachable(self):
        """DB không kết nối được → health exit 3 (không crash với stack trace)."""
        bad_url = "postgresql+psycopg://bad:bad@localhost:19999/nonexistent"

        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": bad_url})

        assert result.exit_code == 3
        assert "DB unreachable" in result.output or "UNHEALTHY" in result.output


# ── T-10.2 Deployment boot sequence — alembic → log-deployment → health ───────


class TestDeploymentBootSequenceE2E:
    """E2E test mô phỏng entrypoint.sh boot sequence với real DB.

    Bước 1: Migrations đã chạy (internal_engine fixture)
    Bước 2: log-deployment --version ghi event
    Bước 3: Seed heartbeat (daemon running)
    Bước cuối: health --strict → exit 0
    """

    @pytest.fixture(autouse=True)
    def clean(self, internal_engine):
        _truncate_health_tables(internal_engine)
        yield
        _truncate_health_tables(internal_engine)

    def test_full_boot_sequence_health_passes(self, internal_engine, pg_container):
        """Full boot: migration → log-deployment → heartbeat → health --strict exit 0."""
        db_url = _db_url(pg_container)

        # Bước 2: log-deployment (entrypoint.sh gọi sau alembic upgrade head)
        result = runner.invoke(
            app,
            ["log-deployment", "--version=1.2.3"],
            env={"FLOWBYTE_DB_URL": db_url},
        )
        assert result.exit_code == 0, f"log-deployment failed: {result.output}"

        # Bước 3: daemon khởi động → seed heartbeat
        _seed_fresh_heartbeat(internal_engine)

        # Bước cuối: health --strict phải trả 0
        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})
        assert result.exit_code == 0, f"health failed: {result.output}"
        assert "Healthy" in result.output

    def test_log_deployment_records_event_in_db(self, internal_engine, pg_container):
        """T-10.2: log-deployment tạo row trong deployment_events."""
        db_url = _db_url(pg_container)

        runner.invoke(
            app,
            ["log-deployment", "--version=0.9.5"],
            env={"FLOWBYTE_DB_URL": db_url},
        )

        with internal_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT event, version FROM deployment_events "
                    "ORDER BY at DESC LIMIT 1"
                )
            ).one_or_none()

        assert row is not None, "deployment_events must have a row after log-deployment"
        assert row.event == "deploy"
        assert row.version == "0.9.5"

    def test_log_deployment_records_correct_version(self, internal_engine, pg_container):
        """Version string đúng được lưu trong deployment_events."""
        db_url = _db_url(pg_container)
        version = "2.0.0-beta"

        runner.invoke(
            app,
            [f"log-deployment", f"--version={version}"],
            env={"FLOWBYTE_DB_URL": db_url},
        )

        with internal_engine.connect() as conn:
            row = conn.execute(
                text("SELECT version FROM deployment_events ORDER BY at DESC LIMIT 1")
            ).one_or_none()

        assert row is not None
        assert row.version == version

    def test_log_deployment_idempotent_multiple_deploys(self, internal_engine, pg_container):
        """Multiple deploys → mỗi deploy có 1 row riêng trong deployment_events."""
        db_url = _db_url(pg_container)

        for v in ["1.0.0", "1.1.0", "1.2.0"]:
            runner.invoke(
                app,
                [f"log-deployment", f"--version={v}"],
                env={"FLOWBYTE_DB_URL": db_url},
            )

        with internal_engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM deployment_events WHERE event = 'deploy'")
            ).scalar()

        assert count == 3

    def test_log_deployment_cli_output_confirms_success(self, internal_engine, pg_container):
        """CLI output phải confirm version được log."""
        db_url = _db_url(pg_container)

        result = runner.invoke(
            app,
            ["log-deployment", "--version=3.0.0"],
            env={"FLOWBYTE_DB_URL": db_url},
        )

        assert result.exit_code == 0
        assert "3.0.0" in result.output

    def test_health_strict_fails_before_daemon_starts(self, pg_container):
        """Sau migration + log-deployment nhưng TRƯỚC khi daemon start → health exit 3."""
        db_url = _db_url(pg_container)

        runner.invoke(
            app,
            ["log-deployment", "--version=1.0.0"],
            env={"FLOWBYTE_DB_URL": db_url},
        )

        # Daemon chưa start → không có heartbeat
        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})
        assert result.exit_code == 3

    def test_health_strict_passes_after_daemon_beats(self, internal_engine, pg_container):
        """Sau khi daemon đã beat → health exit 0."""
        db_url = _db_url(pg_container)

        runner.invoke(
            app,
            ["log-deployment", "--version=1.0.0"],
            env={"FLOWBYTE_DB_URL": db_url},
        )
        _seed_fresh_heartbeat(internal_engine)

        result = runner.invoke(app, ["health", "--strict"], env={"FLOWBYTE_DB_URL": db_url})
        assert result.exit_code == 0

    # ── Alembic migration (via internal_engine fixture) ───────────────────────

    def test_all_internal_tables_exist_after_migration(self, internal_engine):
        """T-10.2: alembic upgrade head tạo đầy đủ bảng internal schema."""
        expected_tables = [
            "pipelines",
            "credentials",
            "sync_requests",
            "sync_logs",
            "sync_checkpoints",
            "sync_runs",
            "validation_results",
            "scheduler_heartbeat",
            "deployment_events",
        ]
        with internal_engine.connect() as conn:
            for table in expected_tables:
                count = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_name = :t"
                    ),
                    {"t": table},
                ).scalar()
                assert count == 1, f"Table {table!r} missing after alembic upgrade head"

    def test_migration_is_idempotent(self, pg_container):
        """alembic upgrade head chạy 2 lần không lỗi (idempotent)."""
        import os
        import subprocess
        import sys

        db_url = _db_url(pg_container)
        env = {**os.environ, "FLOWBYTE_DB_URL": db_url}
        alembic_bin = str(Path(sys.executable).parent / "alembic")
        project_root = str(_PROJECT_ROOT)

        # Run twice — should not error
        for _ in range(2):
            result = subprocess.run(
                [alembic_bin, "upgrade", "head"],
                env=env,
                capture_output=True,
                text=True,
                cwd=project_root,
            )
            assert result.returncode == 0, f"alembic upgrade head failed:\n{result.stderr}"
