"""AES-256-GCM encryption for credentials at-rest.

Wire format: base64( nonce(12) || tag(16) || ciphertext(N) )
AAD (associated data): credential ref name — binding prevents cross-ref reuse.
"""
from __future__ import annotations

import base64
import os
from dataclasses import dataclass

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_NONCE_SIZE = 12
_TAG_SIZE = 16


@dataclass(frozen=True)
class Ciphertext:
    nonce: bytes  # 12 bytes
    tag: bytes  # 16 bytes (embedded in cryptography lib's output)
    data: bytes  # variable length

    def serialize(self) -> str:
        """Return base64-encoded string suitable for DB TEXT column."""
        raw = self.nonce + self.data  # tag is appended to data by AESGCM
        return base64.b64encode(raw).decode()

    @classmethod
    def deserialize(cls, encoded: str) -> "Ciphertext":
        raw = base64.b64decode(encoded)
        nonce = raw[:_NONCE_SIZE]
        # AESGCM appends 16-byte tag to ciphertext internally
        data = raw[_NONCE_SIZE:]
        return cls(nonce=nonce, tag=b"", data=data)


class Encryptor:
    def __init__(self, master_key: bytes) -> None:
        if len(master_key) != 32:
            raise ValueError("Master key must be 32 bytes (AES-256)")
        self._aesgcm = AESGCM(master_key)

    def encrypt(self, plaintext: str, associated_data: bytes) -> Ciphertext:
        """Encrypt plaintext string. Returns Ciphertext with fresh nonce."""
        nonce = os.urandom(_NONCE_SIZE)
        ct_with_tag = self._aesgcm.encrypt(nonce, plaintext.encode(), associated_data)
        return Ciphertext(nonce=nonce, tag=b"", data=ct_with_tag)

    def decrypt(self, ct: Ciphertext, associated_data: bytes) -> str:
        """Decrypt and return plaintext string. Raises InvalidTag on tamper."""
        plaintext_bytes = self._aesgcm.decrypt(ct.nonce, ct.data, associated_data)
        return plaintext_bytes.decode()
