"""Unit tests for master key lifecycle: generate, load, permissions."""
from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from flowbyte.security.master_key import MasterKey, MasterKeyError


def test_generate_and_save_creates_file(tmp_path):
    path = tmp_path / "master.key"
    key = MasterKey.generate_and_save(path)
    assert path.exists()
    assert len(path.read_bytes()) == 32


def test_generate_sets_chmod_600(tmp_path):
    path = tmp_path / "master.key"
    MasterKey.generate_and_save(path)
    mode = stat.S_IMODE(path.stat().st_mode)
    assert mode == 0o600


def test_generate_returns_masterkey_instance(tmp_path):
    path = tmp_path / "master.key"
    key = MasterKey.generate_and_save(path)
    assert isinstance(key, MasterKey)
    assert len(key.raw) == 32


def test_generate_fails_if_key_already_exists(tmp_path):
    path = tmp_path / "master.key"
    path.write_bytes(b"x" * 32)
    os.chmod(path, 0o600)
    with pytest.raises(MasterKeyError, match="already exists"):
        MasterKey.generate_and_save(path)


def test_load_returns_masterkey(tmp_path):
    path = tmp_path / "master.key"
    original = MasterKey.generate_and_save(path)
    loaded = MasterKey.load(path)
    assert loaded.raw == original.raw


def test_load_fails_if_missing(tmp_path):
    path = tmp_path / "nonexistent.key"
    with pytest.raises(MasterKeyError, match="not found"):
        MasterKey.load(path)


def test_load_fails_if_wrong_permissions(tmp_path):
    path = tmp_path / "master.key"
    path.write_bytes(os.urandom(32))
    os.chmod(path, 0o644)  # too permissive
    with pytest.raises(MasterKeyError, match="Insecure permissions"):
        MasterKey.load(path)


def test_load_fails_if_wrong_length(tmp_path):
    path = tmp_path / "master.key"
    path.write_bytes(b"short")
    os.chmod(path, 0o600)
    with pytest.raises(MasterKeyError, match="Invalid key size"):
        MasterKey.load(path)


def test_fingerprint_is_sha256_hex(tmp_path):
    path = tmp_path / "master.key"
    key = MasterKey.generate_and_save(path)
    assert len(key.fingerprint) == 64
    assert all(c in "0123456789abcdef" for c in key.fingerprint)


def test_two_generated_keys_are_different(tmp_path):
    p1 = tmp_path / "key1.key"
    p2 = tmp_path / "key2.key"
    k1 = MasterKey.generate_and_save(p1)
    k2 = MasterKey.generate_and_save(p2)
    assert k1.raw != k2.raw


def test_raw_property_returns_bytes(tmp_path):
    path = tmp_path / "master.key"
    key = MasterKey.generate_and_save(path)
    assert isinstance(key.raw, bytes)


def test_generate_creates_parent_dirs(tmp_path):
    path = tmp_path / "subdir" / "deep" / "master.key"
    MasterKey.generate_and_save(path)
    assert path.exists()
