"""Root pytest conftest — shared fixtures available to all test tiers."""
from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "haravan"


# ── Lightweight crypto fixtures (used by unit + integration) ──────────────────

@pytest.fixture
def master_key_bytes() -> bytes:
    """32-byte key for AES-256-GCM tests. Pure in-memory, no filesystem."""
    return b"flowbyte-test-master-key-32bytes"


@pytest.fixture
def encryptor(master_key_bytes: bytes):
    from flowbyte.security.encryption import Encryptor
    return Encryptor(master_key_bytes)


# ── Fixture file helpers ───────────────────────────────────────────────────────

@pytest.fixture
def haravan_fixture_dir() -> Path:
    return FIXTURES_DIR
