"""Unit tests for AES-256-GCM encryption — pure crypto, no IO.

Note: Encryptor.encrypt() takes str, decrypt() returns str.
The DB stores encrypted JSON strings (credentials payload).
"""
from __future__ import annotations

import pytest
from cryptography.exceptions import InvalidTag

from flowbyte.security.encryption import Ciphertext, Encryptor


@pytest.fixture
def key_32() -> bytes:
    return b"0" * 32


@pytest.fixture
def enc(key_32) -> Encryptor:
    return Encryptor(key_32)


def test_encrypt_returns_ciphertext(enc):
    ct = enc.encrypt("hello", associated_data=b"aad")
    assert isinstance(ct, Ciphertext)


def test_roundtrip(enc):
    plaintext = "secret credentials"
    ct = enc.encrypt(plaintext, associated_data=b"ref_name")
    recovered = enc.decrypt(ct, associated_data=b"ref_name")
    assert recovered == plaintext


def test_roundtrip_json_payload(enc):
    import json
    payload = json.dumps({"access_token": "tok_abc123", "shop_domain": "test.myharavan.com"})
    ct = enc.encrypt(payload, associated_data=b"shop_main")
    recovered = enc.decrypt(ct, associated_data=b"shop_main")
    assert json.loads(recovered) == {"access_token": "tok_abc123", "shop_domain": "test.myharavan.com"}


def test_fresh_nonce_per_encrypt(enc):
    ct1 = enc.encrypt("same", associated_data=b"aad")
    ct2 = enc.encrypt("same", associated_data=b"aad")
    assert ct1.nonce != ct2.nonce


def test_different_ciphertext_same_plaintext(enc):
    ct1 = enc.encrypt("same", associated_data=b"aad")
    ct2 = enc.encrypt("same", associated_data=b"aad")
    assert ct1.data != ct2.data  # nonce makes ciphertext unique


def test_wrong_aad_fails_decrypt(enc):
    ct = enc.encrypt("secret", associated_data=b"ref_a")
    with pytest.raises(InvalidTag):
        enc.decrypt(ct, associated_data=b"ref_b")


def test_tampered_ciphertext_fails_decrypt(enc):
    ct = enc.encrypt("secret", associated_data=b"ref")
    tampered_data = bytearray(ct.data)
    tampered_data[0] ^= 0xFF
    tampered = Ciphertext(nonce=ct.nonce, tag=ct.tag, data=bytes(tampered_data))
    with pytest.raises(InvalidTag):
        enc.decrypt(tampered, associated_data=b"ref")


def test_tampered_nonce_fails_decrypt(enc):
    ct = enc.encrypt("secret", associated_data=b"ref")
    tampered_nonce = bytearray(ct.nonce)
    tampered_nonce[0] ^= 0xFF
    tampered = Ciphertext(nonce=bytes(tampered_nonce), tag=ct.tag, data=ct.data)
    with pytest.raises(InvalidTag):
        enc.decrypt(tampered, associated_data=b"ref")


def test_wrong_key_fails_decrypt(enc, key_32):
    ct = enc.encrypt("secret", associated_data=b"ref")
    other_enc = Encryptor(b"1" * 32)
    with pytest.raises(InvalidTag):
        other_enc.decrypt(ct, associated_data=b"ref")


def test_nonce_length_is_12(enc):
    ct = enc.encrypt("x", associated_data=b"y")
    assert len(ct.nonce) == 12


def test_serialization_roundtrip(enc):
    ct = enc.encrypt("payload", associated_data=b"aad")
    serialized = ct.serialize()
    assert isinstance(serialized, str)
    restored = Ciphertext.deserialize(serialized)
    assert restored.nonce == ct.nonce
    assert restored.data == ct.data


def test_deserialized_ct_decrypts_correctly(enc):
    plaintext = "roundtrip test"
    ct = enc.encrypt(plaintext, associated_data=b"ref")
    serialized = ct.serialize()
    restored = Ciphertext.deserialize(serialized)
    recovered = enc.decrypt(restored, associated_data=b"ref")
    assert recovered == plaintext


def test_empty_plaintext_roundtrip(enc):
    ct = enc.encrypt("", associated_data=b"ref")
    assert enc.decrypt(ct, associated_data=b"ref") == ""


def test_large_payload_roundtrip(enc):
    large = "x" * 100_000
    ct = enc.encrypt(large, associated_data=b"ref")
    assert enc.decrypt(ct, associated_data=b"ref") == large


def test_key_must_be_32_bytes():
    with pytest.raises(ValueError, match="32 bytes"):
        Encryptor(b"short")
