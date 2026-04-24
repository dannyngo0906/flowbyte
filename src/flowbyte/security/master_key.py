"""Master key lifecycle: generate, load, validate permissions."""
from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path

_KEY_SIZE = 32
_REQUIRED_MODE = 0o600


class MasterKeyError(Exception):
    pass


class MasterKey:
    def __init__(self, raw: bytes) -> None:
        if len(raw) != _KEY_SIZE:
            raise MasterKeyError(f"Invalid key size: {len(raw)}, expected {_KEY_SIZE}")
        self._raw = raw

    @property
    def raw(self) -> bytes:
        return self._raw

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self._raw).hexdigest()

    @classmethod
    def generate_and_save(cls, path: Path | str) -> "MasterKey":
        p = Path(path)
        if p.exists():
            raise MasterKeyError(
                f"Master key already exists at {p}. "
                "Remove it manually if you intend to rotate (this will invalidate all credentials)."
            )
        key = os.urandom(_KEY_SIZE)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(key)
        os.chmod(p, _REQUIRED_MODE)
        return cls(key)

    @classmethod
    def load(cls, path: Path | str = "/etc/flowbyte/master.key") -> "MasterKey":
        p = Path(path)
        if not p.exists():
            raise MasterKeyError(
                f"Master key not found at {p}.\n"
                "Run `flowbyte bootstrap` to generate one, "
                "or restore from backup and place it at this path."
            )
        mode = stat.S_IMODE(p.stat().st_mode)
        if mode != _REQUIRED_MODE:
            raise MasterKeyError(
                f"Insecure permissions on {p}: {oct(mode)}. "
                f"Run: chmod 600 {p}"
            )
        raw = p.read_bytes()
        return cls(raw)
